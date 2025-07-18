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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type dropIndexNode struct {
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//
//	Notes: postgres allows only the index owner to DROP an index.
//	       mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(ctx context.Context, n *tree.DropIndex) (planNode, error) {
	// Keep a track of the indexes that exist to check. When the IF EXISTS
	// options are provided, we will simply not include any indexes that
	// don't exist and continue execution.
	idxNames := make([]fullIndexName, 0, len(n.IndexList))
	for _, index := range n.IndexList {
		tn, tableDesc, err := expandMutableIndexName(ctx, p, index, !n.IfExists /* requireTable */)
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

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropIndexNode) ReadingOwnWrites() {}

func (n *dropIndexNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("index"))

	if n.n.Concurrently {
		params.p.SendClientNotice(
			params.ctx,
			pgerror.Noticef("CONCURRENTLY is not required as all indexes are dropped concurrently"),
		)
	}

	ctx := params.ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, err := params.p.ResolveMutableTableDescriptor(
			ctx, index.tn, true /*required*/, ResolveRequireTableOrViewDesc)
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

		if err := params.p.dropIndexByName(
			ctx, index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkIdxConstraint,
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
	params runParams,
	tableDesc *sqlbase.MutableTableDescriptor,
	shardColDesc *sqlbase.ColumnDescriptor,
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
	if err := params.p.writeSchemaChange(
		params.ctx, tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
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
	params runParams, tableDesc *sqlbase.MutableTableDescriptor, shardColName string,
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

func (*dropIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*dropIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropIndexNode) Close(context.Context)        {}

type fullIndexName struct {
	tn      *tree.TableName
	idxName tree.UnrestrictedName
}

// dropIndexConstraintBehavior is used when dropping an index to signal whether
// it is okay to do so even if it is in use as a constraint (outbound FK or
// unique). This is a subset of what is implied by DropBehavior CASCADE, which
// implies dropping *all* dependencies. This is used e.g. when the element
// constrained is being dropped anyway.
type dropIndexConstraintBehavior bool

const (
	checkIdxConstraint  dropIndexConstraintBehavior = true
	ignoreIdxConstraint dropIndexConstraintBehavior = false
)

func (p *planner) dropIndexByName(
	ctx context.Context,
	tn *tree.TableName,
	idxName tree.UnrestrictedName,
	tableDesc *sqlbase.MutableTableDescriptor,
	ifExists bool,
	behavior tree.DropBehavior,
	constraintBehavior dropIndexConstraintBehavior,
	jobDesc string,
) error {
	idx, dropped, err := tableDesc.FindIndexByName(string(idxName))
	if err != nil {
		// Only index names of the form "table@idx" throw an error here if they
		// don't exist.
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return err
	}
	if dropped {
		return nil
	}

	// Check if requires CCL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(ctx, p.txn, uint32(tableDesc.ID), nil, "", false)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(idx.ID) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), tableDesc.TableDesc(), zone.Subzones, false /* newSubzones */)
			break
		}
	}

	// Remove all foreign key references and backreferences from the index.
	// TODO (lucy): This is incorrect for two reasons: The first is that FKs won't
	// be restored if the DROP INDEX is rolled back, and the second is that
	// validated constraints should be dropped in the schema changer in multiple
	// steps to avoid inconsistencies. We should be queuing a mutation to drop the
	// FK instead. The reason why the FK is removed here is to keep the index
	// state consistent with the removal of the reference on the other table
	// involved in the FK, in case of rollbacks (#38733).

	// TODO (rohany): switching all the checks from checking the legacy ID's to
	//  checking if the index has a prefix of the columns needed for the foreign
	//  key might result in some false positives for this index while it is in
	//  a mixed version cluster, but we have to remove all reads of the legacy
	//  explicit index fields.

	// Construct a list of all the remaining indexes, so that we can see if there
	// is another index that could replace the one we are deleting for a given
	// foreign key constraint.
	remainingIndexes := make([]*sqlbase.IndexDescriptor, 0, len(tableDesc.Indexes)+1)
	remainingIndexes = append(remainingIndexes, &tableDesc.PrimaryIndex)
	for i := range tableDesc.Indexes {
		index := &tableDesc.Indexes[i]
		if index.ID != idx.ID {
			remainingIndexes = append(remainingIndexes, index)
		}
	}

	// indexHasReplacementCandidate runs isValidIndex on each index in remainingIndexes and returns
	// true if at least one index satisfies isValidIndex.
	indexHasReplacementCandidate := func(isValidIndex func(*sqlbase.IndexDescriptor) bool) bool {
		foundReplacement := false
		for _, index := range remainingIndexes {
			if isValidIndex(index) {
				foundReplacement = true
				break
			}
		}
		return foundReplacement
	}
	// If we aren't at the cluster version where we have removed explicit foreign key IDs
	// from the foreign key descriptors, fall back to the existing drop index logic.
	// That means we pretend that we can never find replacements for any indexes.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionNoExplicitForeignKeyIndexIDs) {
		indexHasReplacementCandidate = func(func(*sqlbase.IndexDescriptor) bool) bool {
			return false
		}
	}

	// Check for foreign key mutations referencing this index.
	for _, m := range tableDesc.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
			// If the index being deleted could be used as a index for this outbound
			// foreign key mutation, then make sure that we have another index that
			// could be used for this mutation.
			idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs) &&
			!indexHasReplacementCandidate(func(idx *sqlbase.IndexDescriptor) bool {
				return idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs)
			}) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"referencing constraint %q in the middle of being added, try again later", c.ForeignKey.Name)
		}
	}

	if err := p.MaybeUpgradeDependentOldForeignKeyVersionTables(ctx, tableDesc); err != nil {
		return err
	}

	// Index for updating the FK slices in place when removing FKs.
	sliceIdx := 0
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[sliceIdx] = tableDesc.OutboundFKs[i]
		sliceIdx++
		fk := &tableDesc.OutboundFKs[i]
		canReplace := func(idx *sqlbase.IndexDescriptor) bool {
			return idx.IsValidOriginIndex(fk.OriginColumnIDs)
		}
		// The index being deleted could be used as the origin index for this foreign key.
		if idx.IsValidOriginIndex(fk.OriginColumnIDs) && !indexHasReplacementCandidate(canReplace) {
			if behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
				return errors.Errorf("index %q is in use as a foreign key constraint", idx.Name)
			}
			sliceIdx--
			if err := p.removeFKBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	tableDesc.OutboundFKs = tableDesc.OutboundFKs[:sliceIdx]

	sliceIdx = 0
	for i := range tableDesc.InboundFKs {
		tableDesc.InboundFKs[sliceIdx] = tableDesc.InboundFKs[i]
		sliceIdx++
		fk := &tableDesc.InboundFKs[i]
		canReplace := func(idx *sqlbase.IndexDescriptor) bool {
			return idx.IsValidReferencedIndex(fk.ReferencedColumnIDs)
		}
		// The index being deleted could potentially be the referenced index for this fk.
		if idx.IsValidReferencedIndex(fk.ReferencedColumnIDs) &&
			// If we haven't found a replacement candidate for this foreign key, then
			// we need a cascade to delete this index.
			!indexHasReplacementCandidate(canReplace) {
			// If we found haven't found a replacement, then we check that the drop behavior is cascade.
			if err := p.canRemoveFKBackreference(ctx, idx.Name, fk, behavior); err != nil {
				return err
			}
			sliceIdx--
			if err := p.removeFKForBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	tableDesc.InboundFKs = tableDesc.InboundFKs[:sliceIdx]

	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}
	for _, ref := range idx.InterleavedBy {
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if idx.Unique && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint && !idx.CreatedExplicitly {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", idx.Name)
	}

	var droppedViews []string
	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, skipped, err := p.getViewDescForCascade(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			if skipped {
				// In the table whose index is being removed, filter out all back-references
				// that refer to the view that's being removed.
				tableDesc.DependedOnBy = removeMatchingReferences(tableDesc.DependedOnBy, tableRef.ID)
				continue
			}
			viewJobDesc := fmt.Sprintf("removing view %q dependent on index %q which is being dropped",
				viewDesc.Name, idx.Name)
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc, viewJobDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the idx object we collected above. Make a copy.
	idxCopy := *idx
	idx = &idxCopy
	if !tableDesc.IsTSTable() {
		found := false
		for i, idxEntry := range tableDesc.Indexes {
			if idxEntry.ID == idx.ID {
				// Unsplit all manually split ranges in the index so they can be
				// automatically merged by the merge queue.
				span := tableDesc.IndexSpan(idxEntry.ID)
				ranges, err := ScanMetaKVs(ctx, p.txn, span)
				if err != nil {
					return err
				}
				for _, r := range ranges {
					var desc roachpb.RangeDescriptor
					if err := r.ValueProto(&desc); err != nil {
						return err
					}
					// We have to explicitly check that the range descriptor's start key
					// lies within the span of the index since ScanMetaKVs returns all
					// intersecting spans.
					if (desc.GetStickyBit() != hlc.Timestamp{}) && span.Key.Compare(desc.StartKey.AsRawKey()) <= 0 {
						// Swallow "key is not the start of a range" errors because it would
						// mean that the sticky bit was removed and merged concurrently. DROP
						// INDEX should not fail because of this.
						if err := p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
							return err
						}
					}
				}

				// the idx we picked up with FindIndexByID at the top may not
				// contain the same field any more due to other schema changes
				// intervening since the initial lookup. So we send the recent
				// copy idxEntry for drop instead.
				if err := tableDesc.AddIndexMutation(&idxEntry, sqlbase.DescriptorMutation_DROP); err != nil {
					return err
				}
				tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
		}

		if err := p.removeIndexComment(ctx, tableDesc.ID, idx.ID); err != nil {
			return err
		}
	} else {
		found := false
		for i, idxEntry := range tableDesc.Indexes {
			if idxEntry.ID == idx.ID {
				if err := tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_DROP); err != nil {
					return err
				}
				tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
		}
	}

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}
	mutationID := tableDesc.ClusterVersion.NextMutationID

	if tableDesc.IsTSTable() {
		// creates and exec drop tag index job
		syncDetail := jobspb.SyncMetaCacheDetails{
			Type:                  dropTagIndex,
			SNTable:               tableDesc.TableDescriptor,
			CreateOrAlterTagIndex: *idx,
			MutationID:            mutationID,
		}
		jobID, err := p.createTSSchemaChangeJob(ctx, syncDetail, p.stmt.SQL, p.txn)
		if err != nil {
			return err
		}
		if mutationID != sqlbase.InvalidMutationID {
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: jobID})
		}
		if err = p.writeTableDesc(ctx, tableDesc); err != nil {
			return err
		}
		// Actively commit a transaction, and read/write system table operations
		// need to be performed before this.
		if err = p.txn.Commit(ctx); err != nil {
			return err
		}

		//After the transaction commits successfully, execute the Job and wait for it to complete.
		if err = p.ExecCfg().JobRegistry.Run(
			ctx,
			p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
			[]int64{jobID},
		); err != nil {
			return err
		}
		log.Infof(ctx, "drop tag index %s 1st txn finished, id: %d", string(idxName), idx.ID)
	} else {
		if err := p.writeSchemaChange(ctx, tableDesc, mutationID, jobDesc); err != nil {
			return err
		}
		p.SendClientNotice(ctx,
			errors.WithHint(
				pgerror.Noticef("the data for dropped indexes is reclaimed asynchronously"),
				"The reclamation delay can be customized in the zone configuration for the table."))
	}

	p.SetAuditTarget(uint32(idx.ID), idx.Name, droppedViews)
	return nil
}
