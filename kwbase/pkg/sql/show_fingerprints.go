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
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type showFingerprintsNode struct {
	optColumnsSlot

	tableDesc *sqlbase.ImmutableTableDescriptor
	indexes   []*sqlbase.IndexDescriptor

	run showFingerprintsRun
}

// ShowFingerprints statement fingerprints the data in each index of a table.
// For each index, a full index scan is run to hash every row with the fnv64
// hash. For the primary index, all table columns are included in the hash,
// whereas for secondary indexes, the index cols + the primary index cols + the
// STORING cols are included. The hashed rows are all combined with XOR using
// distsql.
//
// Our hash functions expect input of type BYTES (or string but we use bytes
// here), so we have to convert any datums that are not BYTES. This is currently
// done by round tripping through the string representation of the column
// (`::string::bytes`) and is an obvious area for improvement in the next
// version.
//
// To extract the fingerprints at some point in the past, the following
// query can be used:
//
//	SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE foo] AS OF SYSTEM TIME xxx
func (p *planner) ShowFingerprints(
	ctx context.Context, n *tree.ShowFingerprints,
) (planNode, error) {
	// timeseries check
	_, found, err := sqlbase.ResolveInstanceName(ctx, p.txn, p.CurrentDatabase(), n.Table.Parts[0])
	if err != nil {
		return nil, err
	}
	if found {
		return nil, sqlbase.TSUnsupportedError("show fingerprints")
	}
	// We avoid the cache so that we can observe the fingerprints without
	// taking a lease, like other SHOW commands.
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(
		ctx, n.Table, true /*required*/, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc.IsTSTable() {
		return nil, sqlbase.TSUnsupportedError("show fingerprints")
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	return &showFingerprintsNode{
		tableDesc: tableDesc,
		indexes:   tableDesc.AllNonDropIndexes(),
	}, nil
}

// showFingerprintsRun contains the run-time state of
// showFingerprintsNode during local execution.
type showFingerprintsRun struct {
	rowIdx int
	// values stores the current row, updated by Next().
	values []tree.Datum
}

func (n *showFingerprintsNode) startExec(params runParams) error {
	n.run.values = []tree.Datum{tree.DNull, tree.DNull}
	return nil
}

func (n *showFingerprintsNode) Next(params runParams) (bool, error) {
	if n.run.rowIdx >= len(n.indexes) {
		return false, nil
	}
	index := n.indexes[n.run.rowIdx]

	cols := make([]string, 0, len(n.tableDesc.Columns))
	addColumn := func(col *sqlbase.ColumnDescriptor) {
		// TODO(dan): This is known to be a flawed way to fingerprint. Any datum
		// with the same string representation is fingerprinted the same, even
		// if they're different types.
		switch col.Type.Family() {
		case types.BytesFamily:
			cols = append(cols, fmt.Sprintf("%s:::bytes", tree.NameStringP(&col.Name)))
		default:
			cols = append(cols, fmt.Sprintf("%s::string::bytes", tree.NameStringP(&col.Name)))
		}
	}

	if index.ID == n.tableDesc.PrimaryIndex.ID {
		for i := range n.tableDesc.Columns {
			addColumn(&n.tableDesc.Columns[i])
		}
	} else {
		colsByID := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor)
		for i := range n.tableDesc.Columns {
			col := &n.tableDesc.Columns[i]
			colsByID[col.ID] = col
		}
		colIDs := append(append(index.ColumnIDs, index.ExtraColumnIDs...), index.StoreColumnIDs...)
		for _, colID := range colIDs {
			addColumn(colsByID[colID])
		}
	}

	// The fnv64 hash was chosen mostly due to speed. I did an AS OF SYSTEM TIME
	// fingerprint over 31GiB on a 4 node production cluster (with no other
	// traffic to try and keep things comparable). The cluster was restarted in
	// between each run. Resulting times:
	//
	//  fnv => 17m
	//  sha512 => 1h6m
	//  sha265 => 1h6m
	//  fnv64 (again) => 17m
	//
	// TODO(dan): If/when this ever loses its EXPERIMENTAL prefix and gets
	// exposed to users, consider adding a version to the fingerprint output.
	sql := fmt.Sprintf(`SELECT
	  xor_agg(fnv64(%s))::string AS fingerprint
	  FROM [%d AS t]@{FORCE_INDEX=[%d]}
	`, strings.Join(cols, `,`), n.tableDesc.ID, index.ID)
	// If were'in in an AOST context, propagate it to the inner statement so that
	// the inner statement gets planned with planner.avoidCachedDescriptors set,
	// like the outter one.
	if params.p.semaCtx.AsOfTimestamp != nil {
		ts := params.p.txn.ReadTimestamp()
		sql = sql + " AS OF SYSTEM TIME " + ts.AsOfSystemTime()
	}

	fingerprintCols, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx, "hash-fingerprint",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sql,
	)
	if err != nil {
		return false, err
	}

	if len(fingerprintCols) != 1 {
		return false, errors.AssertionFailedf(
			"unexpected number of columns returned: 1 vs %d",
			len(fingerprintCols))
	}
	fingerprint := fingerprintCols[0]

	n.run.values[0] = tree.NewDString(index.Name)
	n.run.values[1] = fingerprint
	n.run.rowIdx++
	return true, nil
}

func (n *showFingerprintsNode) Values() tree.Datums     { return n.run.values }
func (n *showFingerprintsNode) Close(_ context.Context) {}
