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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ sql.PlanNode = &createIndexNode{}
var _ sql.PlanNodeReadingOwnWrites = &createIndexNode{}

type createIndexNode struct {
	n                  *tree.CreateIndex
	tableDesc          *MutableTableDescriptor
	isOrdinaryTagIndex bool
}

// NewCreateIndexNode creates a new createIndexNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateIndexNode(
	n *tree.CreateIndex, tableDesc *MutableTableDescriptor, isOrdinaryTagIndex bool,
) *createIndexNode {
	return &createIndexNode{
		n:                  n,
		tableDesc:          tableDesc,
		isOrdinaryTagIndex: isOrdinaryTagIndex,
	}
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires INDEX on the table.
func CreateIndex(
	ctx context.Context, p *GenericPlanner, n *tree.CreateIndex,
) (sql.PlanNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, sql.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}
	// can not create index on view which is not a materialized view.
	if tableDesc.IsView() && !tableDesc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
	}

	if tableDesc.MaterializedView() {
		if n.Interleave != nil {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition,
				"cannot create interleaved index on materialized view %s", tableDesc.Name)
		}
		if n.Sharded != nil {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition,
				"cannot create hash sharded index on materialized view %s", tableDesc.Name)
		}
		if n.Unique {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition,
				"cannot create unique index on materialized view %s", tableDesc.Name)
		}
	}

	if tableDesc != nil && tableDesc.IsReplTable && tableDesc.ReplicateFrom != "" {
		return nil, errors.Errorf("Can not create index on replicated table %s . ", tableDesc.Name)
	}

	var isTagIndex bool
	if tableDesc.IsTSTable() {
		if err := tsTableUnsupportedFeatureOfIndex(n); err != nil {
			return nil, err
		}
		isTagIndex = true
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{tableDesc: tableDesc, n: n, isOrdinaryTagIndex: isTagIndex}, nil
}

func tsTableUnsupportedFeatureOfIndex(n *tree.CreateIndex) error {
	if n.Unique {
		return sqlbase.TSUnsupportedError("create unique index")
	}
	if n.Inverted {
		return sqlbase.TSUnsupportedError("create index of inverted")
	}
	if n.Sharded != nil {
		return sqlbase.TSUnsupportedError("create hash sharded index")
	}
	if len(n.Storing) > 0 {
		return sqlbase.TSUnsupportedError("create index with extra columns")
	}
	if n.Interleave != nil {
		return sqlbase.TSUnsupportedError("create interleave index")
	}
	if n.PartitionBy != nil {
		return sqlbase.TSUnsupportedError("create index of partitioning")
	}
	if n.Concurrently {
		return sqlbase.TSUnsupportedError("concurrently create index")
	}
	return nil
}

// setupFamilyAndConstraintForShard adds a newly-created shard column into its appropriate
// family (see comment above GetColumnFamilyForShard) and adds a check constraint ensuring
// that the shard column's value is within [0..ShardBuckets-1]. This method is called when
// a `CREATE INDEX` statement is issued for the creation of a sharded index that *does
// not* re-use a pre-existing shard column.
func setupFamilyAndConstraintForShard(
	ctx context.Context,
	p *GenericPlanner,
	tableDesc *MutableTableDescriptor,
	shardCol *sqlbase.ColumnDescriptor,
	idxColumns []string,
	buckets int32,
) error {
	family := sqlbase.GetColumnFamilyForShard(tableDesc, idxColumns)
	if family == "" {
		return errors.AssertionFailedf("could not find column family for the first column in the index column set")
	}
	// Assign shard column to the family of the first column in its index set, and do it
	// before `AllocateIDs()` assigns it to the primary column family.
	if err := tableDesc.AddColumnToFamilyMaybeCreate(shardCol.Name, family, false, false); err != nil {
		return err
	}
	// Assign an ID to the newly-added shard column, which is needed for the creation
	// of a valid check constraint.
	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}

	ckDef, err := schema.MakeShardCheckConstraintDef(tableDesc, int(buckets), shardCol)
	if err != nil {
		return err
	}
	info, err := tableDesc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}

	inuseNames := make(map[string]struct{}, len(info))
	for k := range info {
		inuseNames[k] = struct{}{}
	}

	ckName, err := schema.GenerateMaybeDuplicateNameForCheckConstraint(tableDesc, ckDef.Expr)
	if err != nil {
		return err
	}
	// Avoid creating duplicate check constraints.
	if _, ok := inuseNames[ckName]; !ok {
		ck, err := sql.MakeCheckConstraint(ctx, tableDesc, ckDef, inuseNames,
			p.GetSemaCtx(), p.GetTableName())
		if err != nil {
			return err
		}
		ck.Validity = sqlbase.ConstraintValidity_Validating
		tableDesc.AddCheckMutation(ck, sqlbase.DescriptorMutation_ADD)
	}
	return nil
}

// MakeIndexDescriptor creates an index descriptor from a CreateIndex node and optionally
// adds a hidden computed shard column (along with its check constraint) in case the index
// is hash sharded. Note that `tableDesc` will be modified when this method is called for
// a hash sharded index.
func MakeIndexDescriptor(
	params RunParams, n *tree.CreateIndex, tableDesc *MutableTableDescriptor, isTgaIndex bool,
) (*IndexDescriptor, error) {
	// Ensure that the columns we want to index exist before trying to create the
	// index.
	if err := validateIndexColumnsExist(tableDesc, n.Columns, isTgaIndex); err != nil {
		return nil, err
	}
	indexDesc := IndexDescriptor{
		Name:              string(n.Name),
		Unique:            n.Unique,
		StoreColumnNames:  n.Storing.ToStrings(),
		CreatedExplicitly: true,
	}

	if n.Inverted {
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support interleaved tables")
		}

		if n.PartitionBy != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support partitioning")
		}

		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns")
		}

		if n.Unique {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique")
		}
		indexDesc.Type = sqlbase.IndexDescriptor_INVERTED
		telemetry.Inc(sqltelemetry.InvertedIndexCounter)
	}

	if n.Sharded != nil {
		if n.PartitionBy != nil {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		shardCol, newColumn, err := schema.SetupShardedIndex(
			params.Ctx,
			params.EvalContext().Settings,
			params.SessionData().HashShardedIndexesEnabled,
			&n.Columns,
			n.Sharded.ShardBuckets,
			tableDesc,
			&indexDesc,
			false /* isNewTable */)
		if err != nil {
			return nil, err
		}
		if newColumn {
			if err := setupFamilyAndConstraintForShard(params.Ctx, params.GetPlanner(), tableDesc, shardCol,
				indexDesc.Sharded.ColumnNames, indexDesc.Sharded.ShardBuckets); err != nil {
				return nil, err
			}
		}
		telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
	}

	if err := indexDesc.FillColumns(n.Columns); err != nil {
		return nil, err
	}
	return &indexDesc, nil
}

// validateIndexColumnsExists validates that the columns for an index exist
// in the table and are not being dropped prior to attempting to add the index.
func validateIndexColumnsExist(
	desc *MutableTableDescriptor, columns tree.IndexElemList, isTgaIndex bool,
) error {
	if isTgaIndex && len(columns) > 4 {
		return pgerror.Newf(pgcode.FeatureNotSupported, "cannot create index with more than 4 tags in the table")
	}

	for _, column := range columns {
		col, dropping, err := desc.FindColumnByName(column.Column)
		if err != nil {
			return err
		}
		if dropping {
			return sqlbase.NewUndefinedColumnError(string(column.Column))
		}
		if isTgaIndex {
			if !col.IsOrdinaryTagCol() {
				return sqlbase.TSUnsupportedError(fmt.Sprintf("creating index on column or primary tag \"%s\"", col.Name))
			}

			if col.Type.InternalType.Oid == oid.T_varbytea || col.Type.InternalType.Oid == oid.T_varchar {
				return sqlbase.TSUnsupportedError(fmt.Sprintf("creating index on tag \"%s\" with type varchar/varbytes", col.Name))
			}
		}
	}
	return nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because CREATE INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createIndexNode) ReadingOwnWrites() {}

func (n *createIndexNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("index"))
	_, dropped, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if dropped {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	if n.n.Concurrently {
		params.GetPlanner().SendClientNotice(
			params.Ctx,
			pgerror.Noticef("CONCURRENTLY is not required as all indexes are created concurrently"),
		)
	}

	// Warn against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	if n.n.PartitionBy == nil && n.tableDesc.PrimaryIndex.Partitioning.NumColumns > 0 {
		params.GetPlanner().SendClientNotice(
			params.Ctx,
			errors.WithHint(
				pgerror.Noticef("creating non-partitioned index on partitioned table may not be performant"),
				"Consider modifying the index such that it is also partitioned.",
			),
		)
	}

	if n.n.Interleave != nil && n.n.PartitionBy != nil {
		return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot be partitioned")
	}

	indexDesc, err := MakeIndexDescriptor(params, n.n, n.tableDesc, n.isOrdinaryTagIndex)
	if err != nil {
		return err
	}

	// Increment the counter if this index could be storing data across multiple column families.
	if len(indexDesc.StoreColumnNames) > 1 && len(n.tableDesc.Families) > 1 {
		telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
	}

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into the index descriptor.
	encodingVersion := sqlbase.BaseIndexFormatVersion
	if params.EvalContext().Settings.Version.IsActive(params.Ctx, clusterversion.VersionSecondaryIndexColumnFamilies) {
		encodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
	}
	indexDesc.Version = encodingVersion

	if n.n.PartitionBy != nil {
		return errors.AssertionFailedf(
			"unsupported create index partition:  %+v", n.n.PartitionBy)
		//partitioning, err := NewPartitioningDescriptor(params.Ctx,
		//	params.EvalContext(), n.tableDesc, indexDesc, n.n.PartitionBy)
		//if err != nil {
		//	return err
		//}
		//indexDesc.Partitioning = partitioning
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}
	// The index name may have changed as a result of
	// AllocateIDs(). Retrieve it for the event log below.
	index := n.tableDesc.Mutations[mutationIdx].GetIndex()

	if n.n.Interleave != nil {
		if err := sql.AddInterleave(params.Ctx, params.GetTxn(), params.GetPlanner(), n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := sql.FinalizeInterleave(params.Ctx, params.GetPlanner(), n.tableDesc, index); err != nil {
			return err
		}
	}

	mutationID := n.tableDesc.ClusterVersion.NextMutationID

	// creates and exec create tag index job
	if n.isOrdinaryTagIndex {
		syncDetail := jobspb.SyncMetaCacheDetails{
			Type:                  sql.CreateTagIndex,
			SNTable:               n.tableDesc.TableDescriptor,
			CreateOrAlterTagIndex: *indexDesc,
			MutationID:            mutationID,
		}
		jobID, err := sql.CreateTSSchemaChangeJob(params.Ctx, params.GetPlanner(), syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.PlannerTxn())
		if err != nil {
			return err
		}
		if mutationID != sqlbase.InvalidMutationID {
			n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: jobID})
		}
		if err = sql.WriteTableDesc(params.Ctx, params.GetPlanner(), n.tableDesc); err != nil {
			return err
		}
		// Actively commit a transaction, and read/write system table operations
		// need to be performed before this.
		if err = params.PlannerTxn().Commit(params.Ctx); err != nil {
			return err
		}

		// After the transaction commits successfully, execute the Job and wait for it to complete.
		if err = params.PlannerExecCfg().JobRegistry.Run(
			params.Ctx,
			params.ExtEvalContext().InternalExecutor.(*sql.InternalExecutor),
			[]int64{jobID},
		); err != nil {
			return err
		}
		//log.Infof(params.Ctx, "create tag index %s 1st txn finished, id: %d", n.n.Name.String(), indexDesc.ID)
	} else {
		if err := params.GetPlanner().WriteSchemaChange(
			params.Ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			log.Info(params.Ctx, err)
			return err
		}
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	params.GetPlanner().SetAuditTarget(uint32(index.ID), index.Name, nil)
	return nil
}

func (*createIndexNode) Next(RunParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}
