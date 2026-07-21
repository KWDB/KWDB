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

package ddl

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &dropIndexNode{}

type dropIndexNode struct {
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//
//	Notes: postgres allows only the index owner to DROP an index.
//	       mysql requires the INDEX privilege on the table.
func DropIndex(ctx context.Context, p *GenericPlanner, n *tree.DropIndex) (sql.PlanNode, error) {
	// Keep a track of the indexes that exist to check. When the IF EXISTS
	// options are provided, we will simply not include any indexes that
	// don't exist and continue execution.
	idxNames := make([]fullIndexName, 0, len(n.IndexList))
	for _, index := range n.IndexList {
		tn, tableDesc, err := sql.ExpandMutableIndexName(ctx, p, index, !n.IfExists /* requireTable */)
		if err != nil {
			// Error or table did not exist.
			return nil, err
		}

		if tableDesc == nil {
			// IfExists specified and table did not exist.
			continue
		}

		// drop multiple tag indexes at once is not supported
		if tableDesc.IsTSTable() && len(n.IndexList) > 1 {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "drop multiple tag indexes at once is not supported")
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}

		idxNames = append(idxNames, fullIndexName{tn: tn, idxName: index.Index})
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because DROP INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropIndexNode) ReadingOwnWrites() {}

func (n *dropIndexNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("index"))

	if n.n.Concurrently {
		params.GetPlanner().SendClientNotice(
			params.Ctx,
			pgerror.Noticef("CONCURRENTLY is not required as all indexes are dropped concurrently"),
		)
	}

	ctx := params.Ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, err := params.GetPlanner().ResolveMutableTableDescriptor(
			ctx, index.tn, true /*required*/, sql.ResolveRequireTableOrViewDesc)
		if err != nil {
			// Somehow the descriptor we had during planning is not there
			// any more.
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"table descriptor for %q became unavailable within same txn",
				tree.ErrString(index.tn))
		}

		// can not drop index on view which is not a materialized view.
		if tableDesc.IsView() && !tableDesc.MaterializedView() {
			return pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
		}

		// If we couldn't find the index by name, this is either a legitimate error or
		// this statement contains an 'IF EXISTS' qualifier. Both of these cases are
		// handled by `dropIndexByName()` below so we just ignore the error here.
		idxDesc, dropped, _ := tableDesc.FindIndexByName(string(index.idxName))
		var shardColName string
		// If we're dropping a sharded index, record the name of its shard column to
		// potentially drop it if no other index refers to it.
		if idxDesc != nil && idxDesc.IsSharded() && !dropped {
			shardColName = idxDesc.Sharded.Name
		}

		if err := sql.DropIndexByName(ctx, params.GetPlanner(),
			index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, sqlconst.CheckIdxConstraint,
			tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}

		if shardColName != "" {
			if err := n.maybeDropShardColumn(params, tableDesc, shardColName); err != nil {
				return err
			}
		}
	}

	return nil
}

// dropShardColumnAndConstraint drops the given shard column and its associated check
// constraint.
func (n *dropIndexNode) dropShardColumnAndConstraint(
	params RunParams, tableDesc *MutableTableDescriptor, shardColDesc *sqlbase.ColumnDescriptor,
) error {
	validChecks := tableDesc.Checks[:0]
	for _, check := range tableDesc.AllActiveAndInactiveChecks() {
		if used, err := check.UsesColumn(tableDesc.TableDesc(), shardColDesc.ID); err != nil {
			return err
		} else if used {
			if check.Validity == sqlbase.ConstraintValidity_Validating {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"referencing constraint %q in the middle of being added, try again later", check.Name)
			}
		} else {
			validChecks = append(validChecks, check)
		}
	}

	if len(validChecks) != len(tableDesc.Checks) {
		tableDesc.Checks = validChecks
	}

	tableDesc.AddColumnMutation(shardColDesc, sqlbase.DescriptorMutation_DROP)
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].ID == shardColDesc.ID {
			// Note the third slice parameter which will force a copy of the backing
			// array if the column being removed is not the last column.
			tableDesc.Columns = append(tableDesc.Columns[:i:i],
				tableDesc.Columns[i+1:]...)
			break
		}
	}

	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}
	mutationID := tableDesc.ClusterVersion.NextMutationID
	if err := params.GetPlanner().WriteSchemaChange(
		params.Ctx, tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

// maybeDropShardColumn drops the given shard column, if there aren't any other indexes
// referring to it.
//
// Assumes that the given index is sharded.
func (n *dropIndexNode) maybeDropShardColumn(
	params RunParams, tableDesc *MutableTableDescriptor, shardColName string,
) error {
	shardColDesc, dropped, err := tableDesc.FindColumnByName(tree.Name(shardColName))
	if err != nil {
		return err
	}
	if dropped {
		return nil
	}
	shouldDropShardColumn := true
	for _, otherIdx := range tableDesc.AllNonDropIndexes() {
		if otherIdx.ContainsColumnID(shardColDesc.ID) {
			shouldDropShardColumn = false
			break
		}
	}
	if !shouldDropShardColumn {
		return nil
	}
	return n.dropShardColumnAndConstraint(params, tableDesc, shardColDesc)
}

func (*dropIndexNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropIndexNode) Close(context.Context)        {}

type fullIndexName struct {
	tn      *tree.TableName
	idxName tree.UnrestrictedName
}
