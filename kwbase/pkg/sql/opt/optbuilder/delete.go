// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/gogo/protobuf/sortkeys"
)

// buildDelete builds a memo group for a DeleteOp expression, which deletes all
// rows projected by the input expression. All columns from the deletion table
// are projected, including mutation columns (the optimizer may later prune the
// columns if they are not needed).
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Delete operator).
func (b *Builder) buildDelete(del *tree.Delete, inScope *scope) (outScope *scope) {
	// UX friendliness safeguard.
	if del.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.DangerousStatementf("DELETE without WHERE clause"))
	}

	if del.OrderBy != nil && del.Limit == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"DELETE statement requires LIMIT when ORDER BY is used"))
	}

	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(del.Table, privilege.DELETE)

	if refColumns != nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"cannot specify a list of column IDs with DELETE"))
	}

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(depName, tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "delete", tab, alias)
	// template does not support delete operation
	if tab.GetTableType() == tree.TemplateTable {
		panic(sqlbase.TemplateUnsupportedError("delete"))
	} else if tab.GetTableType() == tree.InstanceTable || tab.GetTableType() == tree.TimeseriesTable {
		_, ok := del.Returning.(*tree.NoReturningClause)
		// time series and instance does not support operator except filter
		if del.OrderBy != nil || del.Limit != nil || !ok || del.With != nil {
			panic(sqlbase.UnsupportedDeleteConditionError("only supported where expression"))
		} else {
			if tab.GetTableType() == tree.InstanceTable {
				cn, err := sqlbase.GetInstNamespaceByName(b.ctx, b.evalCtx.Txn, alias.Catalog(), alias.Table())
				if err != nil {
					panic(err)
				}

				return b.buildTSDelete(inScope, tab, del, alias, cn.InstTableID)
			}
			return b.buildTSDelete(inScope, tab, del, alias, sqlbase.ID(tab.ID()))
		}
	}

	// Build the input expression that selects the rows that will be deleted:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the delete table will be projected.
	mb.buildInputForDelete(inScope, del.Table, del.Where, del.Limit, del.OrderBy)

	// Build the final delete statement, including any returned expressions.
	if resultsNeeded(del.Returning) {
		mb.buildDelete(*del.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildDelete(nil /* returning */)
	}

	// check if there's other type of table referenced
	mb.CheckMixedTableRefWithTs()

	return mb.outScope
}

// buildDelete constructs a Delete operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildDelete(returning tree.ReturningExprs) {
	mb.buildFKChecksForDelete()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructDelete(mb.outScope.expr, mb.checks, private)

	mb.buildReturning(returning)
}

// checkDeleteFilter check if unsupported exprs such as cast expr exist in delete stmt
func checkDeleteFilter(expr tree.Expr) error {
	switch exp := expr.(type) {
	case *tree.AndExpr:
		if err := checkDeleteFilter(exp.Left); err != nil {
			return err
		}
		if err := checkDeleteFilter(exp.Right); err != nil {
			return err
		}
	case *tree.OrExpr:
		if err := checkDeleteFilter(exp.Left); err != nil {
			return err
		}
		if err := checkDeleteFilter(exp.Right); err != nil {
			return err
		}
	case *tree.ComparisonExpr:
		if err := checkDeleteFilter(exp.Left); err != nil {
			return err
		}
		if err := checkDeleteFilter(exp.Right); err != nil {
			return err
		}
	case *tree.ParenExpr:
		if err := checkDeleteFilter(exp.Expr); err != nil {
			return err
		}
	case *tree.UnresolvedName, *tree.NumVal, *tree.StrVal, *tree.DBool, *tree.Placeholder, *tree.Tuple, *tree.UserDefinedVar:
	default:
		return sqlbase.UnsupportedDeleteConditionError("unsupported filter expression")
	}
	return nil
}

// lowerLimitOfTimestamp is the lower boundary of ts delete
const lowerLimitOfTimestamp = tree.TsMinTimestamp - 1

// TsMaxTimestamp is the upper boundary of ts delete
const upperLimitOfTimestamp = tree.TsMaxTimestamp + 1

const (
	// TsDeleteData means delete data
	TsDeleteData = 3
	// TsDeleteEntities means delete entities
	TsDeleteEntities = 4
)

// buildTSDelete build delete outScope of time-series table
// input: inScope, table, delete, TableName, table ID
// output: outScope
// build delete outScope of time-series table through table and delete message
func (b *Builder) buildTSDelete(
	inScope *scope, table cat.Table, del *tree.Delete, alias tree.TableName, tblID sqlbase.ID,
) (outScope *scope) {
	id := b.factory.Metadata().AddTable(table, &alias)
	b.DisableMemoReuse = true
	var spans []opt.TsSpan
	if del.Where == nil {
		spans = append(spans, opt.TsSpan{
			Start: lowerLimitOfTimestamp,
			End:   upperLimitOfTimestamp,
		})
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructTSDelete(
			&memo.TSDeletePrivate{
				DeleteType: int(execinfrapb.OperatorType_TsDeleteMultiEntitiesData),
				ID:         opt.TableID(tblID),
				STable:     id,
				Spans:      spans,
			})
		return outScope
	}

	if err := checkDeleteFilter(del.Where.Expr); err != nil {
		panic(err)
	}
	colCount := table.ColumnCount()
	priTagCols := make([]*sqlbase.ColumnDescriptor, 0)
	colMap := make(map[int]int, 0)
	primaryTagIDs := make(map[int]struct{})

	for i := 0; i < colCount; i++ {
		if table.Column(i).IsPrimaryTagCol() {
			priTagCols = append(priTagCols, table.Column(i).(*sqlbase.ColumnDescriptor))
			// build primary tags' map，and check if scope column in filter meets the conditions
			primaryTagIDs[int(table.Column(i).ColID())] = struct{}{}
		}
	}

	// only one set of primary tags or ts columns of the time series table
	// or ts columns of the instance table are supported for filtering in delete
	exprs := make(map[int]tree.Expr, len(priTagCols))
	var filter tree.Exprs
	var filters []tree.Exprs

	filterTyp := initType
	// instance table only support delete data
	if table.GetTableType() == tree.InstanceTable {
		filterTyp = onlyTS
	} else if table.GetTableType() == tree.TimeseriesTable {
		filterTyp = onlyTag
	}

	var indexFlags *tree.IndexFlags
	if source, ok := del.Table.(*tree.AliasedTableExpr); ok && source.IndexFlags != nil {
		indexFlags = source.IndexFlags
	}
	inScope = b.buildScan(
		b.addTable(table, &alias),
		nil, /* ordinals */
		indexFlags,
		noRowLocking,
		includeMutations,
		inScope,
	)
	var hasTypeHints bool
	for i := range b.semaCtx.Placeholders.TypeHints {
		if b.semaCtx.Placeholders.TypeHints[i] != nil {
			hasTypeHints = true
			b.semaCtx.Placeholders.TypeHints[i] = nil
		}
	}
	// resolve filter expr to get scope column and it's column id
	texpr := inScope.resolveType(del.Where.Expr, types.Bool)
	meta := b.factory.Metadata()
	// check if filter supported
	spans = checkTSDeleteFilter(b.evalCtx, inScope, texpr, exprs, &filterTyp, primaryTagIDs, meta, spans)
	if filterTyp == unsupportedType {
		panic(sqlbase.UnsupportedDeleteConditionError("unsupported binary operator or mismatching tag filter"))
	}

	if filterTyp == tagAndTS && len(exprs) == 0 {
		if len(spans) == 0 {
			spans = append(spans, opt.TsSpan{
				Start: upperLimitOfTimestamp,
				End:   upperLimitOfTimestamp,
			})
		}
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructTSDelete(
			&memo.TSDeletePrivate{
				ColsMap:    colMap,
				DeleteType: int(execinfrapb.OperatorType_TsDeleteMultiEntitiesData),
				ID:         opt.TableID(tblID),
				STable:     id,
				Spans:      spans,
			})
		return outScope
	}

	if filterTyp == onlyTag || filterTyp == tagAndTS {
		for i, col := range priTagCols {
			// check if primary tags completed
			if exp, ok := exprs[int(col.ColID())]; ok {
				filter = append(filter, exp)
				colMap[int(col.ID)] = i
			} else {
				panic(sqlbase.UnsupportedDeleteConditionError("incomplete tag filter"))
			}
		}
		filters = append(filters, filter)
	}
	for colID, expr := range exprs {
		if v, ok := expr.(*tree.Placeholder); ok {
			if hasTypeHints {
				b.semaCtx.Placeholders.TypeHints[v.Idx] = meta.ColumnMeta(opt.ColumnID(colID)).Type
			}
			b.semaCtx.Placeholders.Types[v.Idx] = meta.ColumnMeta(opt.ColumnID(colID)).Type
		}
	}
	// get delete type
	var delTyp int
	if filterTyp == onlyTS || filterTyp == tagAndTS {
		delTyp = TsDeleteData
	} else if filterTyp == onlyTag {
		delTyp = TsDeleteEntities
		b.DisableMemoReuse = false
	}

	if delTyp == TsDeleteData && len(spans) == 0 {
		spans = append(spans, opt.TsSpan{
			Start: upperLimitOfTimestamp,
			End:   upperLimitOfTimestamp,
		})
	}

	outScope = inScope.push()
	outScope.expr = b.factory.ConstructTSDelete(
		&memo.TSDeletePrivate{
			InputRows:  filters,
			ColsMap:    colMap,
			DeleteType: delTyp,
			ID:         opt.TableID(tblID),
			STable:     id,
			Spans:      spans,
		})
	return outScope
}

const (
	initType filterType = iota
	// onlyTag means only tag filter in instance table
	onlyTag
	// onlyTS means only timestamp filter in time-series table
	onlyTS
	// tagAndTS means tags and timestamp filter in time-series table
	tagAndTS
	// unsupportedType unsupported filter in time-series table
	unsupportedType
)

// tsColumnID means first timestamp column id
const tsColumnID = 1

type filterType int

// checkTSDeleteFilter check if filter in delete only
// support one piece of primary tags or ts column
func checkTSDeleteFilter(
	evalCtx *tree.EvalContext,
	inScope *scope,
	filter tree.Expr,
	exprs map[int]tree.Expr,
	typ *filterType,
	primaryTagIDs map[int]struct{},
	meta *opt.Metadata,
	spans []opt.TsSpan,
) []opt.TsSpan {
	if *typ == unsupportedType {
		return nil
	}

	switch f := filter.(type) {
	case *tree.AndExpr:
		rightSpans := checkTSDeleteFilter(evalCtx, inScope, f.Right, exprs, typ, primaryTagIDs, meta, nil)
		leftSpans := checkTSDeleteFilter(evalCtx, inScope, f.Left, exprs, typ, primaryTagIDs, meta, nil)
		spans = mergeAndSpans(leftSpans, rightSpans, spans)
	case *tree.OrExpr:
		leftPri, rightPri := make(map[int]tree.Expr), make(map[int]tree.Expr)
		rightSpans := checkTSDeleteFilter(evalCtx, inScope, f.Right, rightPri, typ, primaryTagIDs, meta, nil)
		right := checkPTagIsComplete(rightPri, primaryTagIDs)
		leftSpans := checkTSDeleteFilter(evalCtx, inScope, f.Left, leftPri, typ, primaryTagIDs, meta, nil)
		left := checkPTagIsComplete(leftPri, primaryTagIDs)
		if (*typ == onlyTag || *typ == tagAndTS) && right && left {
			for key, value := range leftPri {
				if rightPri[key].String() != value.String() {
					*typ = unsupportedType
					return nil
				}
				exprs[key] = value
			}
		}
		spans = mergeOrSpans(leftSpans, rightSpans, spans)
	case *tree.ComparisonExpr:
		if f.Operator > tree.NotIn {
			*typ = unsupportedType
			return nil
		}
		if udv, ok := f.Left.(*tree.UserDefinedVar); ok {
			exp, err := udv.Eval(evalCtx)
			if err != nil {
				panic(err)
			}
			f.Left = exp
		}
		if udv, ok := f.Right.(*tree.UserDefinedVar); ok {
			exp, err := udv.Eval(evalCtx)
			if err != nil {
				panic(err)
			}
			f.Right = exp
		}
		spans = checkComExpr(evalCtx, f.Left, f.Right, exprs, typ, primaryTagIDs, f.Operator, meta, spans)
	case *tree.DBool:
		spans = handleDBool(f, spans, typ)
	default:
		*typ = unsupportedType
		return nil
	}
	return spans
}

// mergeAndSpans merge spans in and expr
func mergeAndSpans(leftSpans, rightSpans, spans []opt.TsSpan) []opt.TsSpan {
	if leftSpans == nil {
		return append(spans, rightSpans...)
	}
	if rightSpans == nil {
		return append(spans, leftSpans...)
	}
	for _, leftSpan := range leftSpans {
		for _, rightSpan := range rightSpans {
			if rightSpan.End >= leftSpan.Start && rightSpan.Start <= leftSpan.End {
				spans = append(spans, opt.TsSpan{
					Start: max(rightSpan.Start, leftSpan.Start),
					End:   min(rightSpan.End, leftSpan.End),
				})
			}
		}
	}
	return spans
}

// mergeOrSpans merge spans in or expr
func mergeOrSpans(leftSpans, rightSpans, spans []opt.TsSpan) []opt.TsSpan {
	if leftSpans == nil {
		return append(spans, rightSpans...)
	}
	if rightSpans == nil {
		return append(spans, leftSpans...)
	}
	spans = append(spans, mergeSpans(leftSpans, rightSpans)...)

	return spans
}

// handleDBool handle spans when filter type is bool
func handleDBool(f *tree.DBool, spans []opt.TsSpan, typ *filterType) []opt.TsSpan {
	if *typ == onlyTag {
		*typ = tagAndTS
	}
	var span opt.TsSpan
	if *f {
		span = opt.TsSpan{Start: lowerLimitOfTimestamp, End: upperLimitOfTimestamp}
	} else {
		span = opt.TsSpan{Start: upperLimitOfTimestamp, End: upperLimitOfTimestamp}
	}
	return append(spans, span)
}

// handlePrimaryTagColumn check if primary tag in filter is supported
func handlePrimaryTagColumn(
	exprs map[int]tree.Expr,
	id int,
	expr tree.Expr,
	op tree.ComparisonOperator,
	typ *filterType,
	primaryTagIDs map[int]struct{},
) {
	if op != tree.EQ {
		*typ = unsupportedType
	}
	if datum, ok := expr.(tree.Datum); ok {
		if val, ok := exprs[id]; ok && val.String() != datum.String() {
			*typ = unsupportedType
		}
		checkFilterType(id, typ, primaryTagIDs)
		exprs[id] = datum
	} else {
		*typ = unsupportedType
	}
}

// checkComExpr resolve comparison expr and get durition and primary tag values
// filter in delete only support one piece of primary tags or ts column
func checkComExpr(
	evalCtx *tree.EvalContext,
	left, right tree.Expr,
	exprs map[int]tree.Expr,
	typ *filterType,
	primaryTagIDs map[int]struct{},
	op tree.ComparisonOperator,
	meta *opt.Metadata,
	spans []opt.TsSpan,
) []opt.TsSpan {
	if *typ == unsupportedType {
		return nil
	}

	leftScopeCol, leftIsScopeCol := left.(*scopeColumn)
	rightScopeCol, rightIsScopeCol := right.(*scopeColumn)

	if leftIsScopeCol && !rightIsScopeCol {
		// id in expr is column's logical id, need to get column's physical id
		t := meta.ColumnMeta(leftScopeCol.id).Table
		index := t.ColumnOrdinal(leftScopeCol.id)
		id := meta.Table(t).Column(index).ColID()
		if id == tsColumnID {
			if timeSpan, ok := right.(*tree.Placeholder); ok {
				v := tree.UnwrapDatum(evalCtx, timeSpan)
				right = v
			}
			switch rightExp := right.(type) {
			case *tree.DTimestampTZ:
				time := rightExp.UnixMilli()
				var start, end int64
				switch op {
				case tree.GT:
					if time > upperLimitOfTimestamp {
						start = upperLimitOfTimestamp
					} else {
						start = time + 1
					}
					end = upperLimitOfTimestamp
				case tree.GE:
					if time > upperLimitOfTimestamp {
						start = upperLimitOfTimestamp
					} else {
						start = time
					}
					end = upperLimitOfTimestamp
				case tree.EQ:
					start, end = time, time
				case tree.LE:
					start = lowerLimitOfTimestamp
					if time < lowerLimitOfTimestamp {
						end = lowerLimitOfTimestamp
					} else {
						end = time
					}
				case tree.LT:
					start = lowerLimitOfTimestamp
					if time < lowerLimitOfTimestamp {
						end = lowerLimitOfTimestamp
					} else {
						end = time - 1
					}
				case tree.NE:
					if time < lowerLimitOfTimestamp || time > upperLimitOfTimestamp {
						start, end = lowerLimitOfTimestamp, upperLimitOfTimestamp
					} else {
						spans = append(spans, opt.TsSpan{
							Start: lowerLimitOfTimestamp,
							End:   time - 1,
						})
						start, end = time+1, upperLimitOfTimestamp
					}
				default:
					*typ = unsupportedType
					return nil
				}
				spans = append(spans, opt.TsSpan{
					Start: start,
					End:   end,
				})
			case *tree.Tuple:
				resolveTupleSpanForDelete(evalCtx, rightExp.Exprs, op, &spans, typ)
			}
			if *typ == onlyTag {
				*typ = tagAndTS
			}
			return spans
		}
		handlePrimaryTagColumn(exprs, int(id), right, op, typ, primaryTagIDs)
	} else if !leftIsScopeCol && rightIsScopeCol {
		// id in expr is column's logical id, need to get column's physical id
		t := meta.ColumnMeta(rightScopeCol.id).Table
		id := t.ColumnOrdinal(rightScopeCol.id) + 1
		if id == tsColumnID {
			if timeSpan, ok := left.(*tree.Placeholder); ok {
				v := tree.UnwrapDatum(evalCtx, timeSpan)
				left = v
			}
			switch leftExp := left.(type) {
			case *tree.DTimestampTZ:
				time := leftExp.UnixMilli()
				var start, end int64
				switch op {
				case tree.GT:
					start = lowerLimitOfTimestamp
					if time < lowerLimitOfTimestamp {
						end = lowerLimitOfTimestamp
					} else {
						end = time - 1
					}
				case tree.GE:
					start = lowerLimitOfTimestamp
					if time < lowerLimitOfTimestamp {
						end = lowerLimitOfTimestamp
					} else {
						end = time
					}
				case tree.EQ:
					start, end = time, time
				case tree.LE:
					if time > upperLimitOfTimestamp {
						start = upperLimitOfTimestamp
					} else {
						start = time
					}
					end = upperLimitOfTimestamp
				case tree.LT:
					if time > upperLimitOfTimestamp {
						start = upperLimitOfTimestamp
					} else {
						start = time + 1
					}
					end = upperLimitOfTimestamp
				case tree.NE:
					if time < lowerLimitOfTimestamp || time > upperLimitOfTimestamp {
						start, end = lowerLimitOfTimestamp, upperLimitOfTimestamp
					} else {
						spans = append(spans, opt.TsSpan{
							Start: lowerLimitOfTimestamp,
							End:   time - 1,
						})
						start, end = time+1, upperLimitOfTimestamp
					}
				default:
					*typ = unsupportedType
					return nil
				}
				spans = append(spans, opt.TsSpan{
					Start: start,
					End:   end,
				})
			case *tree.Tuple:
				resolveTupleSpanForDelete(evalCtx, leftExp.Exprs, op, &spans, typ)
			}
			if *typ == onlyTag {
				*typ = tagAndTS
			}
			return spans
		}
		handlePrimaryTagColumn(exprs, id, left, op, typ, primaryTagIDs)
	} else {
		*typ = unsupportedType
		return nil
	}

	return spans
}

// checkFilterType check if filter supported
func checkFilterType(id int, typ *filterType, primaryTagIDs map[int]struct{}) {
	if id == 0 && *typ == onlyTS {
		*typ = onlyTS
	} else if id == 0 && *typ == onlyTag {
		*typ = tagAndTS
	} else if _, ok := primaryTagIDs[id]; ok && *typ == onlyTag {
		*typ = onlyTag
	} else if _, ok := primaryTagIDs[id]; ok && *typ == tagAndTS {
		*typ = tagAndTS
	} else {
		*typ = unsupportedType
	}
}

func checkPTagIsComplete(exprs map[int]tree.Expr, primaryTagIDs map[int]struct{}) bool {
	for key := range primaryTagIDs {
		if _, ok := exprs[key]; !ok {
			return false
		}
	}
	return true
}

// max return the maximum of two integers.
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// min return the minimum of two integers.
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// mergeSpans Merge 2 sorted time span slices.
func mergeSpans(left, right []opt.TsSpan) []opt.TsSpan {
	i, j := 0, 0
	var spans []opt.TsSpan
	for i < len(right) || j < len(left) {
		if i < len(right) && j < len(left) {
			if right[i].Start <= left[j].End && right[i].End >= left[j].Start {
				spans = append(spans, opt.TsSpan{
					Start: min(right[i].Start, left[j].Start),
					End:   max(right[i].End, left[j].End),
				})
				i++
				j++
			} else if right[i].End < left[j].Start {
				spans = append(spans, right[i])
				i++
			} else if right[i].Start > left[j].End {
				spans = append(spans, left[j])
				j++
			}
		}
		if i < len(right) && j >= len(left) {
			if right[i].Start > left[j-1].End {
				spans = append(spans, right[i])
				i++
			} else {
				i++
			}
		}
		if i >= len(right) && j < len(left) {
			if right[i-1].End < left[j].Start {
				spans = append(spans, left[j])
				j++
			} else {
				j++
			}
		}
	}
	return spans
}

// resolveTupleSpanForDelete resolve time spans in operator of in and not in.
func resolveTupleSpanForDelete(
	evalCtx *tree.EvalContext,
	exprs tree.Exprs,
	op tree.ComparisonOperator,
	spans *[]opt.TsSpan,
	typ *filterType,
) {
	var timeArray []int64
	for _, exp := range exprs {
		if timeSpan, ok := exp.(*tree.Placeholder); ok {
			v := tree.UnwrapDatum(evalCtx, timeSpan)
			exp = v
		}
		if datum, ok := exp.(*tree.DTimestampTZ); ok {
			time := datum.UnixMilli()
			if time > lowerLimitOfTimestamp && time < upperLimitOfTimestamp {
				timeArray = append(timeArray, datum.UnixMilli())
			} else {
				timeArray = append(timeArray, lowerLimitOfTimestamp)
			}
		}
		if _, ok := exp.(tree.DNullExtern); ok && op == tree.NotIn {
			return
		}
	}
	sortkeys.Int64s(timeArray)
	switch op {
	case tree.In:
		for _, time := range timeArray {
			*spans = append(*spans, opt.TsSpan{
				Start: time,
				End:   time,
			})
		}
	case tree.NotIn:
		for i, time := range timeArray {
			if i == 0 {
				*spans = append(*spans, opt.TsSpan{
					Start: lowerLimitOfTimestamp,
					End:   time - 1,
				})
			} else {
				*spans = append(*spans, opt.TsSpan{
					Start: timeArray[i-1] + 1,
					End:   time - 1,
				})
			}
			if i == len(timeArray)-1 {
				*spans = append(*spans, opt.TsSpan{
					Start: time + 1,
					End:   upperLimitOfTimestamp,
				})
			}
		}
	default:
		*typ = unsupportedType
	}
}
