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

package sql

import (
	"context"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

var tsScanNodePool = sync.Pool{
	New: func() interface{} {
		return &tsScanNode{}
	},
}

// A tsScanNode handles scanning over the timeseries pairs for a table and
// reconstructing them into rows.
type tsScanNode struct {
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	// table metadata.
	Table cat.Table

	// the physical ID set of the columns that need to be scanned.
	ScanSource util.FastIntSet

	// time frame
	tsSpans []execinfrapb.TsSpan

	// tsSpansPre represents the precision of the tsSpans
	tsSpansPre int32

	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns sqlbase.ResultColumns

	// filter that can be evaluated using only this table; it contains
	// tree.IndexedVar leaves generated using filterVars.
	filter     tree.TypedExpr
	filterVars tree.IndexedVarHelper

	// AccessMode is table-read mode
	AccessMode execinfrapb.TSTableReadMode

	// ScanAggArray is flag for use statistic reader
	ScanAggArray bool

	// Filter conditions for regular tag columns
	TagFilterArray []tree.TypedExpr

	// Filter conditions for primary tag columns
	PrimaryTagFilterArray []tree.TypedExpr
	PrimaryTagValues      map[uint32][]string

	// TagIndexFilterArray conditions for index tag columns
	TagIndexFilterArray []tree.TypedExpr
	// TagIndex for tag hash index
	TagIndex memo.TagIndexInfo

	// HintType is Hint type of scan table
	HintType keys.ScanMethodHintType

	// RelInfo for hash tagscan
	RelInfo RelationalInfo

	// ts table ordered type
	orderedType opt.OrderedTableType

	// ts table id
	TableMetaID opt.TableID

	// estimatedRowCount is the estimated number of rows that this tsScanNode will
	// output. When there are no statistics to make the estimation, it will be
	// set to zero.
	estimatedRowCount uint64
}

// RelationalInfo contains relational information from the other side of
// BatchLookupJoin needed for hash tag scan
type RelationalInfo struct {

	// columns meta for pushed down data
	RelationalCols *[]sqlbase.TSCol

	// probe side column ids
	ProbeColIds *[]uint32

	// hashtag side column ids
	HashColIds *[]uint32
}

// scanNode implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &tsScanNode{}

func (n *tsScanNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("scanNode can't be run in local mode")
}

func (n *tsScanNode) IndexedVarResolvedType(idx int) *types.T {
	return n.resultColumns[idx].Typ
}

func (n *tsScanNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return (*tree.Name)(&n.resultColumns[idx].Name)
}

func (n *tsScanNode) startExec(params runParams) error {
	return pgerror.New(pgcode.Warning, "time series query is not supported in subquery")
}

func (n *tsScanNode) Close(context.Context) {
	*n = tsScanNode{}
	tsScanNodePool.Put(n)
}

func (n *tsScanNode) Next(params runParams) (bool, error) {
	panic("scanNode can't be run in local mode")
}

func (n *tsScanNode) Values() tree.Datums {
	panic("scanNode can't be run in local mode")
}

func (p *planner) TSScan() *tsScanNode {
	n := tsScanNodePool.Get().(*tsScanNode)
	return n
}

// synchronizerNode represents a node that can merge input plan
type synchronizerNode struct {
	// resultColumns
	columns sqlbase.ResultColumns
	plan    planNode

	// parallel degree
	degree int32
}

func (n *synchronizerNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("scanNode can't be run in local mode")
}

func (n *synchronizerNode) IndexedVarResolvedType(idx int) *types.T {
	return n.columns[idx].Typ
}

func (n *synchronizerNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return (*tree.Name)(&n.columns[idx].Name)
}

func (n *synchronizerNode) startExec(params runParams) error {
	return pgerror.New(pgcode.Warning, "time series query is not supported in subquery")
}

func (n *synchronizerNode) Close(ctx context.Context) {
	if n.plan != nil {
		n.plan.Close(ctx)
	}
}

func (n *synchronizerNode) Next(params runParams) (bool, error) {
	panic("scanNode can't be run in local mode")
}

func (n *synchronizerNode) Values() tree.Datums {
	panic("scanNode can't be run in local mode")
}

var tsInsertSelectNodePool = sync.Pool{
	New: func() interface{} {
		return &tsInsertSelectNode{}
	},
}

// tsInsertSelectNode insert into select
type tsInsertSelectNode struct {
	plan planNode

	// TableID insert table id
	TableID uint64

	// TableName insert table name
	TableName string

	// DBID database of insert table
	DBID uint64

	Cols      []int32
	ColIdxs   []int32
	TableType int32
}

func (t *tsInsertSelectNode) startExec(params runParams) error {
	return nil
}

func (t *tsInsertSelectNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (t *tsInsertSelectNode) Values() tree.Datums {
	return nil
}

func (t *tsInsertSelectNode) Close(ctx context.Context) {
	if t.plan != nil {
		t.plan.Close(ctx)
	}
	*t = tsInsertSelectNode{}
	tsInsertSelectNodePool.Put(t)
}

var _ planNode = &tsInsertSelectNode{}
