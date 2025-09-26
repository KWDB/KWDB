// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"
	"strconv"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type valuesNode struct {
	columns sqlbase.ResultColumns
	tuples  [][]tree.TypedExpr

	// specifiedInQuery is set if the valuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a valuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	valuesRun
}

var tsInsertNodePool = sync.Pool{
	New: func() interface{} {
		return &tsInsertNode{}
	},
}

type tsInsertNode struct {
	nodeIDs             []roachpb.NodeID
	allNodePayloadInfos [][]*sqlbase.SinglePayloadInfo
}

// FastPathResults implements the planNodeFastPath interface.
func (t *tsInsertNode) FastPathResults() (int, bool) {
	var rownums uint32
	for _, perNodeInfos := range t.allNodePayloadInfos {
		for _, value := range perNodeInfos {
			rownums += value.RowNum
		}
	}
	return int(rownums), true
}

func (t *tsInsertNode) startExec(params runParams) error {
	//if err := params.p.txn.Commit(params.ctx); err != nil {
	//	return err
	//}
	//return masterengine.SendInsertToAe(params.ctx, masterengine.GetConnectIDFromCtx(params.ctx), t.payload)
	return nil
}

func (t *tsInsertNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (t *tsInsertNode) Values() tree.Datums {
	return nil
}

func (t *tsInsertNode) Close(ctx context.Context) {
	*t = tsInsertNode{}
	tsInsertNodePool.Put(t)
}

type tsInsertWithCDCNode struct {
	tsInsertNode
	CDCData *sqlbase.CDCData
}

func (t *tsInsertWithCDCNode) Close(ctx context.Context) {
	*t = tsInsertWithCDCNode{}
	tsInsertWithCDCNodePool.Put(t)
}

var tsInsertWithCDCNodePool = sync.Pool{
	New: func() interface{} {
		return &tsInsertWithCDCNode{}
	},
}

var _ planNode = &tsInsertNode{}
var _ planNodeFastPath = &tsInsertNode{}

var tsDeleteNodePool = sync.Pool{
	New: func() interface{} {
		return &tsDeleteNode{}
	},
}

// FastPathResults implements the planNodeFastPath interface.
func (t *tsDeleteNode) FastPathResults() (int, bool) {
	return 0, true
}

type tsDeleteNode struct {
	nodeIDs         []roachpb.NodeID
	tableID         uint64
	hashNum         uint64
	primaryTagKey   [][]byte
	primaryTagValue [][]byte
	delTyp          uint8
	spans           []execinfrapb.Span
	// if primary tag of type int out of range, we will return delete 0 directly
	wrongPTag bool
}

func (t *tsDeleteNode) startExec(params runParams) error {
	return nil
}

func (t *tsDeleteNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (t *tsDeleteNode) Values() tree.Datums {
	return nil
}

func (t *tsDeleteNode) Close(ctx context.Context) {
	*t = tsDeleteNode{}
	tsDeleteNodePool.Put(t)
}

var _ planNode = &tsDeleteNode{}
var _ planNodeFastPath = &tsDeleteNode{}

var tsTagUpdateNodePool = sync.Pool{
	New: func() interface{} {
		return &tsTagUpdateNode{}
	},
}

// FastPathResults implements the planNodeFastPath interface.
func (t *tsTagUpdateNode) FastPathResults() (int, bool) {
	return 0, true
}

type tsTagUpdateNode struct {
	nodeIDs       []roachpb.NodeID
	tableID       uint64
	groupID       uint64
	operateTyp    uint8
	primaryTagKey [][]byte
	TagValue      [][]byte
	// if primary tag of type int out of range, we will return UPDATE 0 directly
	wrongPTag bool
	startKey  roachpb.Key
	endKey    roachpb.Key
	osnID     uint64
}

func (t *tsTagUpdateNode) startExec(params runParams) error {
	return nil
}

func (t *tsTagUpdateNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (t *tsTagUpdateNode) Values() tree.Datums {
	return nil
}

func (t *tsTagUpdateNode) Close(ctx context.Context) {
	*t = tsTagUpdateNode{}
	tsTagUpdateNodePool.Put(t)
}

var _ planNode = &tsTagUpdateNode{}
var _ planNodeFastPath = &tsTagUpdateNode{}

type operateDataNode struct {
	operateType int32
	nodeID      []roachpb.NodeID
	desc        []sqlbase.TableDescriptor
}

func (c *operateDataNode) startExec(params runParams) error {
	return nil
}

func (c *operateDataNode) Next(runParams) (bool, error) { return false, nil }

func (c *operateDataNode) Values() tree.Datums { return nil }

func (c *operateDataNode) Close(ctx context.Context) {}

// Values implements the VALUES clause.
func (p *planner) Values(
	ctx context.Context, origN tree.Statement, desiredTypes []*types.T,
) (planNode, error) {
	v := &valuesNode{
		specifiedInQuery: true,
	}

	// If we have names, extract them.
	var n *tree.ValuesClause
	switch t := origN.(type) {
	case *tree.ValuesClauseWithNames:
		n = &t.ValuesClause
	case *tree.ValuesClause:
		n = t
	default:
		return nil, errors.AssertionFailedf("unhandled case in values: %T %v", origN, origN)
	}

	if len(n.Rows) == 0 {
		return v, nil
	}

	numCols := len(n.Rows[0])

	v.tuples = make([][]tree.TypedExpr, 0, len(n.Rows))
	tupleBuf := make([]tree.TypedExpr, len(n.Rows)*numCols)

	v.columns = make(sqlbase.ResultColumns, 0, numCols)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	p.semaCtx.Properties.Require("VALUES", tree.RejectSpecial)

	for num, tuple := range n.Rows {
		if a, e := len(tuple), numCols; a != e {
			return nil, newValuesListLenErr(e, a)
		}

		// Chop off prefix of tupleBuf and limit its capacity.
		tupleRow := tupleBuf[:numCols:numCols]
		tupleBuf = tupleBuf[numCols:]

		for i, expr := range tuple {
			desired := types.Any
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}

			// Clear the properties so we can check them below.
			typedExpr, err := p.analyzeExpr(ctx, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if num == 0 {
				v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ.Family() == types.UnknownFamily {
				v.columns[i].Typ = typ
			} else if typ.Family() != types.UnknownFamily && !typ.Equivalent(v.columns[i].Typ) {
				return nil, pgerror.Newf(pgcode.DatatypeMismatch,
					"VALUES types %s and %s cannot be matched", typ, v.columns[i].Typ)
			}

			tupleRow[i] = typedExpr
		}
		v.tuples = append(v.tuples, tupleRow)
	}
	return v, nil
}

func (p *planner) newContainerValuesNode(columns sqlbase.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		valuesRun: valuesRun{
			rows: rowcontainer.NewRowContainer(
				p.EvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), capacity,
			),
		},
	}
}

// valuesRun is the run-time state of a valuesNode during local execution.
type valuesRun struct {
	rows    *rowcontainer.RowContainer
	nextRow int // The index of the next row.
}

func (n *valuesNode) startExec(params runParams) error {
	if n.rows != nil {
		// n.rows was already created in newContainerValuesNode.
		// Nothing to do here.
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluating.
	// This may run subqueries.
	n.rows = rowcontainer.NewRowContainer(
		params.extendedEvalCtx.Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			var err error
			row[i], err = typedExpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (n *valuesNode) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

func (n *valuesNode) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *valuesNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
		n.rows = nil
	}
	n.nextRow = 0
}

func newValuesListLenErr(exp, got int) error {
	return pgerror.Newf(
		pgcode.Syntax,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}
