// Copyright 2017 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

var _ PlanNode = &scatterNode{}

type scatterNode struct {
	OptColumnsSlot

	run scatterRun
}

// Scatter moves ranges to random stores
// (`ALTER TABLE/INDEX ... SCATTER ...` statement)
// Privileges: INSERT on table.
func (p *GenericPlanner) Scatter(ctx context.Context, n *tree.Scatter) (PlanNode, error) {
	tableDesc, index, err := p.GetTableAndIndex(ctx, &n.TableOrIndex, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	if tableDesc.IsTSTable() {
		return nil, sqlbase.TSUnsupportedError("scatter")
	}

	var span roachpb.Span
	if n.From == nil {
		// No FROM/TO specified; the span is the entire table/index.
		span = tableDesc.IndexSpan(index.ID)
	} else {
		switch {
		case len(n.From) == 0:
			return nil, errors.Errorf("no columns in SCATTER FROM expression")
		case len(n.From) > len(index.ColumnIDs):
			return nil, errors.Errorf("too many columns in SCATTER FROM expression")
		case len(n.To) == 0:
			return nil, errors.Errorf("no columns in SCATTER TO expression")
		case len(n.To) > len(index.ColumnIDs):
			return nil, errors.Errorf("too many columns in SCATTER TO expression")
		}

		// Calculate the desired types for the select statement:
		//  - column values; it is OK if the select statement returns fewer columns
		//  (the relevant prefix is used).
		desiredTypes := make([]*types.T, len(index.ColumnIDs))
		for i, colID := range index.ColumnIDs {
			c, err := tableDesc.FindColumnByID(colID)
			if err != nil {
				return nil, err
			}
			desiredTypes[i] = &c.Type
		}
		fromVals := make([]tree.Datum, len(n.From))
		for i, expr := range n.From {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, tree.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			fromVals[i], err = typedExpr.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
		}
		toVals := make([]tree.Datum, len(n.From))
		for i, expr := range n.To {
			typedExpr, err := p.analyzeExpr(
				ctx, expr, nil, tree.IndexedVarHelper{}, desiredTypes[i], true, "SCATTER",
			)
			if err != nil {
				return nil, err
			}
			toVals[i], err = typedExpr.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
		}

		span.Key, err = getRowKey(tableDesc.TableDesc(), index, fromVals)
		if err != nil {
			return nil, err
		}
		span.EndKey, err = getRowKey(tableDesc.TableDesc(), index, toVals)
		if err != nil {
			return nil, err
		}
		// Tolerate reversing FROM and TO; this can be useful for descending
		// indexes.
		if cmp := span.Key.Compare(span.EndKey); cmp > 0 {
			span.Key, span.EndKey = span.EndKey, span.Key
		} else if cmp == 0 {
			// Key==EndKey is invalid, so special-case when the user's FROM and
			// TO are the same tuple.
			span.EndKey = span.EndKey.Next()
		}
	}

	return &scatterNode{
		run: scatterRun{
			span: span,
		},
	}, nil
}

// scatterRun contains the run-time state of scatterNode during local execution.
type scatterRun struct {
	span roachpb.Span

	rangeIdx int
	ranges   []roachpb.AdminScatterResponse_Range
}

func (n *scatterNode) StartExec(params RunParams) error {
	db := params.p.ExecCfg().DB
	req := &roachpb.AdminScatterRequest{
		RequestHeader:   roachpb.RequestHeader{Key: n.run.span.Key, EndKey: n.run.span.EndKey},
		RandomizeLeases: true,
	}
	log.VEventf(params.Ctx, 3, "send AdminScatterRequest to [%v, %v]", n.run.span.Key, n.run.span.EndKey)
	res, pErr := kv.SendWrapped(params.Ctx, db.NonTransactionalSender(), req)
	if pErr != nil {
		return pErr.GoError()
	}
	n.run.rangeIdx = -1
	n.run.ranges = res.(*roachpb.AdminScatterResponse).Ranges
	return nil
}

func (n *scatterNode) Next(params RunParams) (bool, error) {
	n.run.rangeIdx++
	hasNext := n.run.rangeIdx < len(n.run.ranges)
	return hasNext, nil
}

func (n *scatterNode) Values() tree.Datums {
	r := n.run.ranges[n.run.rangeIdx]
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(r.Span.Key)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, r.Span.Key)),
	}
}

func (*scatterNode) Close(ctx context.Context) {}
