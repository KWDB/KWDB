// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package memo

import (
	"hash/fnv"
	"sort"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/lib/pq/oid"
)

// ExprPos type of expr position
// used for bo push down logical
// flag the expr position, for example ExprPosSelect flag expr exists
// selectExpr filters. ExprPosProjList flag expr exists projectExpr
// projections. push down can use it for can push down to bo
type ExprPos uint32

// flag of the expr position
const (
	ExprPosSelect   = 0
	ExprPosProjList = 1
	ExprPosGroupBy  = 2
	ExprPosNone     = 3
)

// ExprType all expr type
// used for bo push down logical
// push down can use it for can push down to bo
type ExprType uint32

// expr type flag
const (
	ExprTypConst   = 0  // const scalar, a value
	ExprTypCol     = 1  // single column, an original column
	ExprTypeFuncOp = 2  // function, a func expr
	ExprTypBinOp   = 3  // binary operate
	ExprTypCompOp  = 4  // compare operate
	ExprTypeAggOp  = 5  // aggregation operate
	ExprTypeNone   = 10 // unexpected flag
)

// ExprInfo to facilitate pushdown logic during SQL compilation
type ExprInfo struct {
	Alias        string   //name of expr
	Type         ExprType //type of expr
	Pos          ExprPos  //the position where expr appear, for whitelist checking
	Hash         uint32   //hash value for searching in white list
	IsTimeBucket bool     //IsTimeBucket is true when the expr is time_bucket function.
}

// PushHelper to associate column ID and expressions
type PushHelper struct {
	MetaMap MetaInfoMap

	// lock used when operate on map
	lock syncutil.Mutex
}

// Find finds the exprInfo obj from PushHelper.
// id is the key of MetaMap.
func (p *PushHelper) Find(id opt.ColumnID) (ExprInfo, bool) {
	p.lock.Lock()
	if v, ok := p.MetaMap[id]; ok {
		p.lock.Unlock()
		return v, ok
	}
	p.lock.Unlock()
	return ExprInfo{}, false
}

// MetaInfoMap records all logical column info
type MetaInfoMap map[opt.ColumnID]ExprInfo

// StringHash get hashcode from string
func StringHash(s string) uint32 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		panic(err)
	}
	return h.Sum32()
}

// GetDataTypeName get data type name
// string family need detailed type string, other need family string
func GetDataTypeName(t *types.T) string {
	return strconv.Itoa(int(t.Oid()))
}

func getChildStr(src opt.Expr) string {
	str := ""
	if src.ChildCount() > 0 {
		for i := 0; i < src.ChildCount(); i++ {
			param, ok := src.Child(i).(opt.ScalarExpr)
			if !ok {
				return ""
			}
			// We need to handle Coalece expressions specifically here,
			// because the time series engine does not support more than two parameters
			if _, ok1 := src.(*CoalesceExpr); ok1 {
				if args, ok2 := param.(*ScalarListExpr); ok2 && len(*args) >= 3 {
					return ""
				}
			}
			// if any of the children is of time data type, check if it's interval data type
			// if it IS interval data type and includes month/year, we cannot push it down
			if param.DataType().SupportTimeCalc() {
				if timeCanPush(param) {
					str += GetDataTypeName(param.DataType())
				} else {
					str += GetDataTypeName(types.Interval)
				}
			} else if value, ok1 := param.(*TupleExpr); ok1 {
				for c := 0; c < value.Elems.ChildCount(); c++ {
					str += GetDataTypeName(value.Elems.Child(0).(opt.ScalarExpr).DataType())
				}
			} else {
				str += GetDataTypeName(param.DataType())
			}
		}
	}

	return str
}

// timeCanPush return true if interval type does not have month or year.
func timeCanPush(param opt.ScalarExpr) bool {
	if conExpr, ok := param.(*ConstExpr); ok {
		if interval, ok := conExpr.Value.(*tree.DInterval); ok && interval.Months != 0 {
			return false
		}
	}
	return true
}

// GetExprString gets the expr type name of itself and its children nodes, and return as string
func GetExprString(src opt.Expr) string {
	var str string
	if opt.IsComparisonOp(src) || opt.IsBinaryOp(src) {
		if opt.IsComparisonOp(src) {
			treeOp := opt.ComparisonOpReverseMap[src.Op()]
			str += treeOp.String()
		} else if opt.IsBinaryOp(src) {
			treeOp := opt.BinaryOpReverseMap[src.Op()]
			str += treeOp.String()
		}
		return str + getChildStr(src)
	} else if opt.IsAggregateOp(src) {
		return opt.AggregateOpReverseMap[src.Op()] + getChildStr(src)
	}

	switch source := src.(type) {
	case *VariableExpr:
		str = "column"
	case *ConstExpr:
		str = GetDataTypeName(source.Typ)
	case *FunctionExpr:
		str = source.FunctionPrivate.Name
		for _, param := range source.Args {
			str += GetDataTypeName(param.DataType())
		}
	default:
		str = src.Op().String()

		str += getChildStr(src)
	}

	return str
}

// GetExprHash get expr hash code
func GetExprHash(src opt.Expr) uint32 {
	if val, ok := src.(opt.ScalarExpr); ok {
		str := GetExprString(val)
		return StringHash(str)
	}
	return 0
}

// GetExprType get expr type code
func GetExprType(src opt.Expr) ExprType {
	if opt.IsConstValueOp(src) {
		return ExprTypConst
	} else if src.Op() == opt.VariableOp {
		return ExprTypCol
	} else if opt.IsBinaryOp(src) {
		return ExprTypBinOp
	} else if opt.IsComparisonOp(src) {
		return ExprTypCompOp
	} else if opt.IsAggregateOp(src) {
		return ExprTypeAggOp
	} else if src.Op() == opt.FunctionOp {
		if e, ok := src.(*FunctionExpr); ok && e.IsConstForLogicPlan {
			return ExprTypConst
		}
		return ExprTypeFuncOp
	}

	return ExprTypeNone
}

// CheckFunc is used in whitelist checking for given function
type CheckFunc func(key uint32, pos uint32) bool

// These types support exec in ts engine in castExpr
var castSupportType = []oid.Oid{oid.T_timestamp, oid.T_timestamptz, oid.T_int2, oid.T_int4, oid.T_int8,
	oid.T_float4, oid.T_float8, oid.T_bool, oid.T_bpchar, types.T_nchar, oid.T_varchar, types.T_nvarchar, oid.T_varbytea, oid.T_interval, oid.T_text}

// check whether type of expr support exec in ts engine
func checkAESupportType(id oid.Oid) bool {
	typFlag := false
	// check returnType
	for _, typ := range castSupportType {
		if typ == id {
			typFlag = true
			break
		}
	}
	return typFlag
}

// CheckExprCanExecInTSEngine checks whether expr and it's children exprs can be pushed down
// tupleCanExecInTSEngine is true when the tuple use in InExpr or NotInExpr, tuple expr only can exec in ts engine in this case,
// such as col in (1,2,3) or col in (col1,col2); other case such as COUNT(DISTINCT (channel, companyid)) can not exec in ts engine.
// add param onlyOnePTagValue: filters have one pTag only
func CheckExprCanExecInTSEngine(
	src opt.Expr, pos int8, f CheckFunc, tupleCanExecInTSEngine bool, onlyOnePTagValue bool,
) (bool, uint32) {
	switch src.Op() {
	case opt.VariableOp, opt.ConstOp, opt.FalseOp, opt.NullOp, opt.TrueOp, opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		// const and column can exec in ts engine.
		// SubqueryOp, ExistsOp, AnyOp are the scalar subquery, set to be able to exec in ts engine.
		return true, 0
	case opt.TupleOp:
		// only const tuple can exec in ts engine.
		if src.ChildCount() > 0 && src.Child(0).ChildCount() > 0 && tupleCanExecInTSEngine {
			return true, 0
		}
	case opt.InOp, opt.NotInOp:
		for i := 0; i < src.ChildCount(); i++ {
			can, _ := CheckExprCanExecInTSEngine(src.Child(i), pos, f, true, onlyOnePTagValue)
			if !can { // can not push down
				return false, 0
			}
		}
		treeOp := opt.ComparisonOpReverseMap[src.Op()]
		str := treeOp.String()
		if src.ChildCount() > 1 {
			if param, ok := src.Child(1).(*TupleExpr); ok && param.ChildCount() > 0 {
				val := param.Child(0)
				if list, ok1 := val.(*ScalarListExpr); ok1 {
					str += getChildStr(list.Child(0))
				}
			}
		}

		hashCode := StringHash(str)
		if nil != f && f(hashCode, uint32(pos)) {
			return true, hashCode
		}
		return false, 0
	}

	switch source := src.(type) {
	case *ScalarListExpr:
		for i := 0; i < len(*source); i++ {
			can, _ := CheckExprCanExecInTSEngine((*source)[i], pos, f, false, onlyOnePTagValue)
			if !can { // can not push down
				return false, 0
			}
		}
		return true, 0
	case *FunctionExpr:
		if source.Properties.ForbiddenExecInTSEngine {
			if _, ok := CheckGroupWindowExist(src); ok && !onlyOnePTagValue {
				return false, 0
			}
		}

	case *CastExpr:
		if !checkAESupportType(source.Typ.Oid()) {
			return false, 0
		}
	}

	if scalar, ok := src.(opt.ScalarExpr); ok && scalar.CheckConstDeductionEnabled() && checkAESupportType(scalar.DataType().Oid()) {
		return true, 0
	}

	// check white list to find out if src can be pushed down
	hashCode := GetExprHash(src)
	if nil != f && !f(hashCode, uint32(pos)) { // can not push down
		return false, hashCode
	}

	// check whether child exprs can exec in ts engine
	for i := 0; i < src.ChildCount(); i++ {
		if can, _ := CheckExprCanExecInTSEngine(src.Child(i), pos, f, false, onlyOnePTagValue); !can { // can not push down
			return false, hashCode
		}
	}
	return true, hashCode
}

// CheckFilterExprCanExecInTSEngine  check filter condition can exec on ts engine
//
// params:
// - src: the expr that filter expr
// - pos: reference positions referencing this expr in where/having expressions
// - f: check func, for function checking in white list
//
// return
// - can execute on ts engine
func CheckFilterExprCanExecInTSEngine(
	src opt.Expr, pos ExprPos, f CheckFunc, onlyOnePTagValue bool,
) bool {
	// operator and , or , range check is child can exec on ts engine
	if src.Op() == opt.AndOp || src.Op() == opt.OrOp || src.Op() == opt.RangeOp {
		var i int
		for i = 0; i < src.ChildCount(); i++ {
			if !CheckFilterExprCanExecInTSEngine(src.Child(i), pos, f, onlyOnePTagValue) {
				return false
			}
		}

		return true
	}

	// check white list that can support by ts engine
	if can, _ := CheckExprCanExecInTSEngine(src, int8(pos), f, false, onlyOnePTagValue); !can {
		return false
	}

	return true
}

// GetPrimaryTagFilterValue splits the filtering conditions into tag filter conditions, primary tag filter
// conditions,ordinary column filter conditions and then return them.
// private record HINT and PrimaryTagValues.
// e is the original filter expr.
// m record the table cache.
func GetPrimaryTagFilterValue(
	private *TSScanPrivate, e opt.Expr, m *Memo,
) (opt.Expr, []opt.Expr, []opt.Expr) {
	tabID := private.Table

	table := m.Metadata().TableMeta(tabID).Table
	colMap := make(TagColMap, 1)
	primaryColMap := make(TagColMap, 1)
	for i := 0; i < table.ColumnCount(); i++ {
		if table.Column(i).IsTagCol() {
			colMap[tabID.ColumnID(i)] = struct{}{}
		}

		if table.Column(i).IsPrimaryTagCol() {
			primaryColMap[tabID.ColumnID(i)] = struct{}{}
		}
	}

	// 1. spilt original filter expr to ordinary column filter conditions and tag filter conditions
	// 2. spilt tag filter conditions to ordinary tag filter conditions, primary tag filter conditions
	leave, tagFilter := m.SplitTagExpr(e, colMap)

	// hint control use TagTable
	if tagFilter != nil && private.HintType == keys.ForceTagTableHint {
		return leave, tagFilter, nil
	}
	var tags []opt.Expr
	var PrimaryTags []opt.Expr
	private.PrimaryTagValues = make(map[uint32][]string, 1)
	for i := 0; i < len(tagFilter); i++ {
		if !GetPrimaryTagValues(tagFilter[i], primaryColMap, &private.PrimaryTagValues) {
			tags = append(tags, tagFilter[i])
		} else {
			PrimaryTags = append(PrimaryTags, tagFilter[i])
		}
	}

	// check primary tag
	if !CheckPrimaryTagCanUse(m.Metadata().TableMeta(tabID).PrimaryTagCount, &private.PrimaryTagValues) {
		tags = append(tags, PrimaryTags...)
		PrimaryTags = nil
		private.PrimaryTagValues = nil
	}
	return leave, tags, PrimaryTags
}

// CheckPrimaryTagCanUse checks if the number of values matches the number of primary tag columns.
// primaryTagCount is the number of primary tag column.
// value is the primary tag values.
func CheckPrimaryTagCanUse(primaryTagCount int, value *PTagValues) bool {
	// all primary tag value count in filter can not use index
	if len(*value) != primaryTagCount {
		return false
	}

	count := 0
	first := true
	for _, val := range *value {
		if first {
			count = len(val)
			first = false
		} else {
			// all primary tag value count is not equal can not use index
			if count != len(val) {
				return false
			}
		}
	}

	return true
}

// GetAccessMode get access mode by filter expr.
// hasPrimaryFilter is true when there have primary tag filter.
// hasTagFilter is true when there have ordinary tag filter.
// sel is the SelectExpr.
// m record the table cache.
func GetAccessMode(
	hasPrimaryFilter bool, hasTagFilter bool, hasTagIndex bool, private *TSScanPrivate, m *Memo,
) (accessMode int) {
	if private.AccessMode != -1 {
		return private.AccessMode
	}
	var hasNormalTags, outputHasTag, outputHasPTag bool
	hasPrimaryTags := hasPrimaryFilter

	private.Cols.ForEach(func(colID opt.ColumnID) {
		colMeta := m.Metadata().ColumnMeta(colID)
		outputHasTag = outputHasTag || colMeta.IsNormalTag()
		outputHasPTag = outputHasPTag || colMeta.IsPrimaryTag()
	})
	if hasTagFilter || outputHasTag {
		hasNormalTags = true
	}
	if hasPrimaryTags {
		if !hasNormalTags {
			accessMode = int(execinfrapb.TSTableReadMode_tagIndex)
		} else {
			accessMode = int(execinfrapb.TSTableReadMode_tagIndexTable)
		}
	} else if hasTagIndex {
		if outputHasPTag || hasTagFilter {
			accessMode = int(execinfrapb.TSTableReadMode_tagIndexTable)
		} else {
			accessMode = int(execinfrapb.TSTableReadMode_tagHashIndex)
		}
	} else if hasNormalTags {
		accessMode = int(execinfrapb.TSTableReadMode_tableTableMeta)
	} else { // !hasPrimaryTags && !hasNormalTags
		accessMode = int(execinfrapb.TSTableReadMode_metaTable)
	}
	return accessMode
}

// SplitTagExpr find all ts engine tag filter expr for ts engine execute tag index
// eg: create table t1(k_timestamp timestamptz not null, a int, b int) tags (type int not null, location varchar(10))
//
//	primary tags(type)
//	select sum(a) from t1 where type=1 and a = 10;
//	the expr is  [type=1 and location = 'shanghai']
//	 src is                                 AndExpr
//	            left: EqExpr (type=1)[tag filter]           right: EqExpr (a = 10)[leave]
//	  left:VariableExpr right:constExpr[int]        left:VariableExpr right:constExpr[int]
//	 leave expr is EqExpr (a = 10)[leave], tag filter is EqExpr (type=1)
//
// params:
// - src: memo expr struct
// - f: check func, for check in white list
//
// returns:
// - leave: leave expr for exec at up engine
// - tagFilter: tag expr filter, eg a > 10, b < 100, that can compose andexpr
func (m *Memo) SplitTagExpr(src opt.Expr, colMap TagColMap) (leave opt.Expr, tagFilter []opt.Expr) {
	// split and expr
	switch source := src.(type) {
	case *FiltersExpr:
		switch len(*source) {
		case 0:
			return src, nil
		default:
			var lev FiltersExpr
			for i := 0; i < len(*source); i++ {
				lea, tags := m.SplitTagExpr((*source)[i].Condition.(opt.Expr), colMap)
				if lea != nil {
					(*source)[i].Condition = lea.(opt.ScalarExpr)
					lev = append(lev, (*source)[i])
				}

				tagFilter = append(tagFilter, tags...)
			}
			if lev == nil {
				return nil, tagFilter
			}
			return &lev, tagFilter
		}
	case *FiltersItem:
		// Pass through the call.
		return m.SplitTagExpr(source.Condition, colMap)
	case *RangeExpr:
		return m.SplitTagExpr(source.And, colMap)
	case *AndExpr:
		lLeave, lTag := m.SplitTagExpr(source.Left, colMap)
		rLeave, rTag := m.SplitTagExpr(source.Right, colMap)
		if lLeave == nil && rLeave == nil {
			lTag = append(lTag, rTag...)
			return nil, lTag
		}
		for i := 0; i < len(lTag); i++ {
			tagFilter = append(tagFilter, lTag[i])
		}
		for i := 0; i < len(rTag); i++ {
			tagFilter = append(tagFilter, rTag[i])
		}
		if nil == lLeave {
			leave = rLeave
		} else if nil == rLeave {
			leave = lLeave
		} else {
			source.Left = lLeave.(opt.ScalarExpr)
			source.Right = rLeave.(opt.ScalarExpr)
			leave = source
		}
		return leave, tagFilter
	case *OrExpr:
		lLeave, _ := m.SplitTagExpr(source.Left, colMap)
		rLeave, _ := m.SplitTagExpr(source.Right, colMap)
		if lLeave == nil && rLeave == nil {
			// all or is a tag filter
			return nil, []opt.Expr{source}
		}

		return source, nil
	default:
		// check white list that can push down
		if push, _ := CheckExprCanExecInTSEngine(src, ExprPosSelect, m.GetWhiteList().CheckWhiteListParam, false, m.CheckOnlyOnePTagValue()); !push {
			return source, nil
		}

		mode := checkTagExpr(src, colMap)
		canSplit := mode == 1<<hasTag || mode == (1<<hasTag+1<<hasConst)
		if m.CheckFlag(opt.ScalarSubQueryPush) {
			canSplit = canSplit || mode == (1<<hasTag+1<<hasSubQuery)
		}
		if canSplit {
			return nil, []opt.Expr{source}
		}

		return source, nil
	}
}

// constValueToString get const value to string slice
func constValueToString(expr *ConstExpr) string {
	resValue := expr.Value.String()
	switch expr := expr.Value.(type) {
	case *tree.DString:

		resValue = string(*expr)
	}
	return resValue
}

// appendToPTagValues append string to primary tag values
func appendToPTagValues(col uint32, expr *ConstExpr, val *PTagValues) {
	if val != nil {
		(*val)[col] = append((*val)[col], constValueToString(expr))
	}
}

// addConstValueToPTagValues get const value and add to PTagValues
// returning true if append happens, else false
func addConstValueToPTagValues(
	src1 opt.Expr, src2 opt.Expr, primaryTagCol TagColMap, val *PTagValues,
) bool {
	if col, ok := src1.(*VariableExpr); ok {
		switch src := src2.(type) {
		case *ConstExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			appendToPTagValues(uint32(col.Col), src, val)
			return true
		case *FalseExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolFalse.String())
			return true
		case *TrueExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolTrue.String())
			return true
		}
		return false
	}
	return false
}

// GetPrimaryTagValues get primary tag value
//
// params:
// - src: memo expr struct
// - primaryTagCol: primary tag col map, key is column id[logical] value is empty
// - PTagValues: primary tag value map, key is column id[logical] value is string
//
// returns:
// - get primary tag flag
func GetPrimaryTagValues(src opt.Expr, primaryTagCol TagColMap, val *PTagValues) bool {
	switch source := src.(type) {
	case *EqExpr:
		return addConstValueToPTagValues(source.Left, source.Right, primaryTagCol, val) ||
			addConstValueToPTagValues(source.Right, source.Left, primaryTagCol, val)
	case *InExpr:
		if col, ok := source.Left.(*VariableExpr); ok {
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			if tuple, ok1 := source.Right.(*TupleExpr); ok1 {
				var values []string
				for i := 0; i < len(tuple.Elems); i++ {
					if col1, ok1 := tuple.Elems[i].(*ConstExpr); ok1 {
						values = append(values, constValueToString(col1))
					} else {
						return false
					}
				}
				if val != nil {
					(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], values...)
				}
				return true
			}
		}
	}

	return false
}

// tagExprMode tag expr mode
type tagExprMode uint32

// TagColMap tag col map
type TagColMap map[opt.ColumnID]struct{}

const (
	// hasConst expr of const flag
	hasConst = 1
	// hasTag expr of tag flag
	hasTag = 2
	// hasColumn expr of column flag
	hasColumn = 3
	// hasSubQuery sub query expr flag
	hasSubQuery = 4
)

// checkTagExpr check if expr can push down, including src expr self and children
func checkTagExpr(src opt.Expr, colMap TagColMap) tagExprMode {
	switch source := src.(type) {
	case *VariableExpr:
		if _, ok := colMap[source.Col]; ok {
			return 1 << hasTag
		}

		return 1 << hasColumn
	case *ConstExpr:
		return 1 << hasConst
	case *SubqueryExpr:
		return 1 << hasSubQuery
	default:
		var rMode tagExprMode
		for i := 0; i < src.ChildCount(); i++ {
			mode := checkTagExpr(src.Child(i), colMap)
			rMode |= mode
		}
		return rMode
	}
}

// CheckAggCanParallel checks if the agg can parallel execute.
// expr is the expr of agg function.
// returns:
// param1: return true when the agg can be parallel execute.
// param2: return true when the agg with distinct.
func CheckAggCanParallel(expr opt.Expr) (bool, bool) {
	switch t := expr.(type) {
	case *MaxExpr, *MinExpr, *SumExpr, *AvgExpr, *CountExpr, *CountRowsExpr,
		*FirstExpr, *FirstRowExpr, *FirstTimeStampExpr, *FirstRowTimeStampExpr,
		*LastExpr, *LastTimeStampExpr, *LastRowExpr, *LastRowTimeStampExpr, *ConstAggExpr:
		return true, false
	case *AggDistinctExpr:
		ok, _ := CheckAggCanParallel(t.Input)
		return ok, true
	}
	return false, false
}

// GetTagIndexKeyAndFilter get tag index key and tag index filters by metadata.
func GetTagIndexKeyAndFilter(
	private *TSScanPrivate, tagFilters *[]opt.Expr, m *Memo, filterCount int,
) []opt.Expr {
	tabID := private.Table
	table := m.Metadata().TableMeta(tabID).Table

	// Early return if there's no index to use
	if table.IndexCount() == 1 {
		return nil
	}

	tagColMap, primaryTagColMap := buildTagColumnMaps(table, tabID)

	// GET tag index info from metadata
	var tagIndexColumnIDs [][]uint32
	mapTagIndexIDs := make([]map[sqlbase.IndexID][]uint32, table.IndexCount())
	for i := 1; i < table.IndexCount(); i++ {
		var metaIDs []uint32
		tagIndex := table.Index(i)
		for i := 0; i < tagIndex.ColumnCount()-1; i++ {
			index := tagIndex.Column(i).Ordinal
			metaIDs = append(metaIDs, uint32(tabID.ColumnID(index)))
		}
		mapTagIndexIDs[i] = make(map[sqlbase.IndexID][]uint32)
		tagIndexColumnIDs = append(tagIndexColumnIDs, metaIDs)
		mapTagIndexIDs[i][sqlbase.IndexID(table.Index(i).ID())] = metaIDs
	}

	tagValues := make(map[uint32][]string)
	tagEqFilters, tagOrFilter, filterMap := populateFilters(tagFilters, &tagValues, tagColMap, primaryTagColMap, filterCount)

	// Attempt to use tag index
	tagIndexFilters, tagIndexValues, canUseTagIndex := CheckAndGetTagIndex(tagIndexColumnIDs, tagValues, tagEqFilters, filterMap)
	if canUseTagIndex {
		if len(tagIndexFilters) == 1 && tagIndexFilters[0].Op() == opt.InOp {
			private.TagIndex.UnionType = Combine
		} else {
			private.TagIndex.UnionType = Intersection
		}
		private.TagIndex.TagIndexValues = tagIndexValues
		private.TagIndex.IndexID = make([]uint32, len(tagIndexValues))
		for i, mapTagColumnIDs := range tagIndexValues {
			var TagColumnIDs []uint32
			for k := range mapTagColumnIDs {
				TagColumnIDs = append(TagColumnIDs, k)
			}

			for _, indexMap := range mapTagIndexIDs {
				for indexID, colIDs := range indexMap {
					if equalColumns(TagColumnIDs, colIDs) {
						private.TagIndex.IndexID[i] = uint32(indexID)
						break
					}
				}
			}
		}
	} else {
		primaryTagValues, tagIndexValues, canUseTagIndexByOrFilter := CheckAndGetTagIndexFromOrExpr(tagIndexColumnIDs, tagValues, primaryTagColMap)
		if canUseTagIndexByOrFilter {
			private.PrimaryTagValues = primaryTagValues
			private.TagIndex.UnionType = Combine
			private.TagIndex.TagIndexValues = tagIndexValues
			private.TagIndex.IndexID = make([]uint32, len(tagIndexValues))
			for i, mapTagColumnIDs := range tagIndexValues {
				var TagColumnIDs []uint32
				for k := range mapTagColumnIDs {
					TagColumnIDs = append(TagColumnIDs, k)
				}

				for _, indexMap := range mapTagIndexIDs {
					for indexID, colIDs := range indexMap {
						if equalColumns(TagColumnIDs, colIDs) {
							private.TagIndex.IndexID[i] = uint32(indexID)
							break
						}
					}
				}
			}
			tagIndexFilters = tagOrFilter
		}
	}

	used := make(map[opt.Expr]struct{})
	for _, filter := range tagIndexFilters {
		used[filter] = struct{}{}
	}
	var newFilters []opt.Expr
	for _, filter := range *tagFilters {
		if _, exists := used[filter]; !exists {
			newFilters = append(newFilters, filter)
		}
	}

	*tagFilters = newFilters

	return tagIndexFilters
}

// buildTagColumnMaps used to build tag and primary tag colMap.
func buildTagColumnMaps(table cat.Table, tabID opt.TableID) (TagColMap, TagColMap) {
	tagColMap := make(TagColMap, 1)
	primaryTagColMap := make(TagColMap, 1)
	for i := 0; i < table.ColumnCount(); i++ {
		if table.Column(i).IsOrdinaryTagCol() {
			tagColMap[tabID.ColumnID(i)] = struct{}{}
		}

		if table.Column(i).IsPrimaryTagCol() {
			primaryTagColMap[tabID.ColumnID(i)] = struct{}{}
		}
	}
	return tagColMap, primaryTagColMap
}

// populateFilters used to get all equal and or filter.
func populateFilters(
	filters *[]opt.Expr,
	tagValues *map[uint32][]string,
	tagColMap, primaryTagColMap TagColMap,
	filterCount int,
) ([]opt.Expr, []opt.Expr, map[int]uint32) {
	var tagEqFilters, tagOrFilter []opt.Expr
	filterMap := make(map[int]uint32) // Initialize filterMap
	for _, filter := range *filters {
		tempTagValues := make(map[uint32][]string)
		valid, isSingleOr := GetTagValues(filter, primaryTagColMap, tagColMap, &tempTagValues, len(*filters), filterCount)
		if !valid {
			continue
		}
		if isSingleOr {
			tagOrFilter = append(tagOrFilter, filter)
		} else {
			tagEqFilters = append(tagEqFilters, filter)
		}
		for k, val := range tempTagValues {
			(*tagValues)[k] = val
			filterMap[len(tagEqFilters)-1] = k // Map index of tagEqFilters to column ID
		}
	}
	return tagEqFilters, tagOrFilter, filterMap
}

// GetTagValues get tag index key
//
// params:
// - src: memo expr struct
// - tagCol: tag col map, key is column id[logical] value is empty
// - tagIndexValues: tag value map, key is column id[logical] value is string
//
// returns:
// - get tag flag
func GetTagValues(
	src opt.Expr,
	primaryTagColMap, tagCol TagColMap,
	val *map[uint32][]string,
	tagFiltersCount, filtersCount int,
) (bool, bool) {
	switch source := src.(type) {
	case *EqExpr:
		if col, ok := source.Left.(*VariableExpr); ok {
			switch right := source.Right.(type) {
			case *ConstExpr:
				if _, hasTagIndex := tagCol[col.Col]; !hasTagIndex {
					if _, find := primaryTagColMap[col.Col]; !find {
						return false, false
					}
				}
				(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], constValueToString(right))
				return true, false
			case *FalseExpr:
				if _, hasTagIndex := tagCol[col.Col]; !hasTagIndex {
					if _, find := primaryTagColMap[col.Col]; !find {
						return false, false
					}
				}
				(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolFalse.String())
				return true, false
			case *TrueExpr:
				if _, hasTagIndex := tagCol[col.Col]; !hasTagIndex {
					if _, find := primaryTagColMap[col.Col]; !find {
						return false, false
					}
				}
				(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolTrue.String())
				return true, false
			}
			return false, false
		}
	case *InExpr:
		// Only process inExpr if there is exactly one filter
		if tagFiltersCount == 1 {
			if col, ok := source.Left.(*VariableExpr); ok {
				if _, hasTagIndex := tagCol[col.Col]; !hasTagIndex {
					if _, find := primaryTagColMap[col.Col]; !find {
						return false, false
					}
				}

				if tuple, isTupleExpr := source.Right.(*TupleExpr); isTupleExpr {
					var values []string
					for i := 0; i < len(tuple.Elems); i++ {
						switch e := tuple.Elems[i].(type) {
						case *ConstExpr:
							values = append(values, constValueToString(e))
						case *TrueExpr:
							values = append(values, tree.DBoolTrue.String())
						case *FalseExpr:
							values = append(values, tree.DBoolFalse.String())
						default:
							return false, false
						}
					}
					if val != nil {
						(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], values...)
					}
					return true, false
				}
			}
		}
	case *OrExpr:
		// Only process OrExpr if there is exactly one filter
		if filtersCount == 1 {
			leftValid, _ := GetTagValues(source.Left, primaryTagColMap, tagCol, val, tagFiltersCount, filtersCount)
			rightValid, _ := GetTagValues(source.Right, primaryTagColMap, tagCol, val, tagFiltersCount, filtersCount)
			return leftValid && rightValid, true
		}
		return false, false
	}

	return false, false
}

// CheckAndGetTagIndex evaluates the applicability of tag indexes based on the provided conditions.
// The function aims to find the best set of indices that cover the maximum possible scope of the given filters.
//
// Parameters:
// - tagIndexIDs: index set.
// - tagValues: mapping of column and values for each tag filter.
// - tagEqFilters: all tag equivalent filters.
// - filterMap: mapping of (index positions in tagEqFilters) and tag IDs.
//
// Returns:
// - selected tag filters.
// - whether any index can be effectively utilized based on the input conditions.
func CheckAndGetTagIndex(
	tagIndexIDs [][]uint32,
	tagValues map[uint32][]string,
	tagEqFilters []opt.Expr,
	filterMap map[int]uint32,
) ([]opt.Expr, []map[uint32][]string, bool) {
	if len(tagValues) == 0 || len(tagIndexIDs) == 0 || len(tagEqFilters) == 0 {
		return nil, nil, false
	}

	colIDs := make(map[uint32]bool)
	for k := range tagValues {
		colIDs[k] = true
	}

	// Select all indexes that cover at least one filter condition.
	// eg:
	// - index: [1,2,3],[1],[2],[3]
	// - tagEqFilters: 1 = 'a' and 2 = 'b' and 3 = 'c'
	// - selectedIndices: [{1,2,3},{1},{2},{3}]
	selectedIndices := make([][]uint32, 0)
	for _, indexIDs := range tagIndexIDs {
		allMatched := true
		count := 0
		first := true
		for _, id := range indexIDs {
			if !colIDs[id] {
				allMatched = false
				break
			}
			if first {
				count = len(tagValues[id])
				first = false
			} else {
				// All tag index value count is not equal can not use index
				if count != len(tagValues[id]) {
					allMatched = false
					break
				}
			}
		}
		if allMatched {
			selectedIndices = append(selectedIndices, indexIDs)
		}
	}

	// Find the index that covers the most query filter by the greedy method.
	finalIndices := selectOptimalIndices(selectedIndices)

	// Builds a filter expression that uses the selected index
	newTagIndexFilters := make([]opt.Expr, 0)
	tagIndexValues := make([]map[uint32][]string, len(finalIndices))
	for i, idx := range finalIndices {
		tagIndexValues[i] = make(map[uint32][]string, 0)
		for _, id := range idx {
			for j, filter := range tagEqFilters {
				colID := filterMap[j]
				if colID == id {
					newTagIndexFilters = append(newTagIndexFilters, filter)
					tagIndexValues[i][colID] = tagValues[colID]
				}
			}
		}
	}

	return newTagIndexFilters, tagIndexValues, len(newTagIndexFilters) > 0
}

// selectOptimalIndices selects the optimal indices from a list of candidate indices
// eg:
// - indices: [{1,2,3},{1},{4,5,6},{5,6}]
// - return: [{1,2,3},{4,5,6}]
func selectOptimalIndices(indices [][]uint32) [][]uint32 {
	if len(indices) == 0 {
		return nil
	}

	// Sort indices based on the sum of unique coverages they provide
	// Indices that cover more unique columns are prioritized
	sort.SliceStable(indices, func(i, j int) bool {
		return len(indices[i]) > len(indices[j])
	})

	// Select indices that cover the most unique column IDs
	selectedIndices := make([][]uint32, 0)
	usedCols := make(map[uint32]bool)
	for _, index := range indices {
		if addsUniqueCoverage(index, usedCols) {
			selectedIndices = append(selectedIndices, index)
			// Mark these columns as used
			for _, id := range index {
				usedCols[id] = true
			}
		}
	}

	return selectedIndices
}

// addsUniqueCoverage checks if the index adds any unique column ID that has not been covered yet
func addsUniqueCoverage(index []uint32, usedCols map[uint32]bool) bool {
	for _, id := range index {
		if usedCols[id] {
			return false
		}
	}
	return true
}

// CheckAndGetTagIndexFromOrExpr evaluates the applicability of tag indexes based on the or condition.
func CheckAndGetTagIndexFromOrExpr(
	tagIndexIDs [][]uint32, tagValues map[uint32][]string, primaryColMap TagColMap,
) (map[uint32][]string, []map[uint32][]string, bool) {
	if len(tagValues) == 0 || len(tagIndexIDs) == 0 {
		return nil, nil, false
	}

	primaryTagValues := make(map[uint32][]string)
	tagIndexValues := make([]map[uint32][]string, 0)
	// Determine if primaryColMap contains exactly one entry and capture that ID
	var singlePrimaryColID uint32
	if len(primaryColMap) == 1 {
		for colID := range primaryColMap {
			singlePrimaryColID = uint32(colID)
		}
	}

	for colID, val := range tagValues {
		isSingleIndexed := false
		for _, index := range tagIndexIDs {
			// Check if index is a single column index and matches the column id
			if len(index) == 1 && index[0] == colID {
				isSingleIndexed = true
				tagIndexValue := make(map[uint32][]string)
				tagIndexValue[colID] = tagValues[colID]
				tagIndexValues = append(tagIndexValues, tagIndexValue)
				break
			}
		}

		// Additionally, check if the colID matches the single entry in primaryColMap
		if colID == singlePrimaryColID && len(primaryColMap) == 1 {
			isSingleIndexed = true
			primaryTagValues[colID] = append(primaryTagValues[colID], val...)
		}

		if !isSingleIndexed {
			// If any column ID does not have a single column index, return false
			return nil, nil, false
		}
	}

	return primaryTagValues, tagIndexValues, true
}

// equalColumns checks whether two uint32 slices contain the same elements.
func equalColumns(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[uint32]int)
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		if counts[v] == 0 {
			return false
		}
		counts[v]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}
