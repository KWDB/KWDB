// Copyright 2016 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/pkg/errors"
)

var _ PlanNode = &splitNode{}

type splitNode struct {
	OptColumnsSlot

	tableDesc      *sqlbase.TableDescriptor
	index          *sqlbase.IndexDescriptor
	rows           PlanNode
	run            splitRun
	expirationTime hlc.Timestamp
}

// splitRun contains the run-time state of splitNode during local execution.
type splitRun struct {
	lastSplitKey       []byte
	lastExpirationTime hlc.Timestamp
}

func (n *splitNode) StartExec(params RunParams) error {
	if n.tableDesc.IsTSTable() {
		return sqlbase.TSUnsupportedError("split")
	}
	return nil
}

func (n *splitNode) Next(params RunParams) (bool, error) {
	// TODO(radu): instead of performing the splits sequentially, accumulate all
	// the split keys and then perform the splits in parallel (e.g. split at the
	// middle key and recursively to the left and right).

	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	rowKey, err := getRowKey(n.tableDesc, n.index, n.rows.Values())
	if err != nil {
		return false, err
	}

	if err := params.extendedEvalCtx.ExecCfg.DB.AdminSplit(params.Ctx, rowKey, rowKey, n.expirationTime); err != nil {
		return false, err
	}

	n.run.lastSplitKey = rowKey
	n.run.lastExpirationTime = n.expirationTime

	return true, nil
}

func (n *splitNode) Values() tree.Datums {
	splitEnforcedUntil := tree.DNull
	if (n.run.lastExpirationTime != hlc.Timestamp{}) {
		splitEnforcedUntil = tree.TimestampToInexactDTimestamp(n.run.lastExpirationTime)
	}
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastSplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastSplitKey)),
		splitEnforcedUntil,
	}
}

func (n *splitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

// getRowKey generates a key that corresponds to a row (or prefix of a row) in a table or index.
// Both tableDesc and index are required (index can be the primary index).
func getRowKey(
	tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, values []tree.Datum,
) ([]byte, error) {
	if len(index.ColumnIDs) < len(values) {
		return nil, pgerror.Newf(pgcode.Syntax, "excessive number of values provided: expected %d, got %d", len(index.ColumnIDs), len(values))
	}
	colMap := make(map[sqlbase.ColumnID]int)
	for i := range values {
		colMap[index.ColumnIDs[i]] = i
	}
	prefix := sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID)
	key, _, err := sqlbase.EncodePartialIndexKey(
		tableDesc, index, len(values), colMap, values, prefix,
	)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// parseExpriationTime parses an expression into a hlc.Timestamp representing
// the expiration time of the split.
func parseExpirationTime(
	evalCtx *tree.EvalContext, expireExpr tree.TypedExpr,
) (hlc.Timestamp, error) {
	if !tree.IsConst(evalCtx, expireExpr) {
		return hlc.Timestamp{}, errors.Errorf("SPLIT AT: only constant expressions are allowed for expiration")
	}
	d, err := expireExpr.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if d == tree.DNull {
		return hlc.MaxTimestamp, nil
	}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ts, err := tree.DatumToHLC(evalCtx, stmtTimestamp, d)
	if err != nil {
		return ts, errors.Wrap(err, "SPLIT AT")
	}
	if ts.GoTime().Before(stmtTimestamp) {
		return ts, errors.Errorf("SPLIT AT: expiration time should be greater than or equal to current time")
	}
	return ts, nil
}
