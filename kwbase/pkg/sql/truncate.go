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

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// TableTruncateChunkSize is the maximum number of keys deleted per chunk
// during a table truncation.
const TableTruncateChunkSize = indexTruncateChunkSize

var _ PlanNode = &truncateNode{}

type truncateNode struct {
	n *tree.Truncate
}

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//
//	Notes: postgres requires TRUNCATE.
//	       mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *GenericPlanner) Truncate(ctx context.Context, n *tree.Truncate) (PlanNode, error) {
	return &truncateNode{n: n}, nil
}

func (t *truncateNode) StartExec(params RunParams) error {
	p := params.p
	n := t.n
	ctx := params.Ctx

	// Since truncation may cascade to a given table any number of times, start by
	// building the unique set (ID->name) of tables to truncate.
	toTruncate := make(map[sqlbase.ID]string, len(n.Tables))
	// toTraverse is the list of tables whose references need to be traversed
	// while constructing the list of tables that should be truncated.
	toTraverse := make([]sqlbase.MutableTableDescriptor, 0, len(n.Tables))

	for i := range n.Tables {
		tn := &n.Tables[i]
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, ResolveRequireTableDesc)
		if err != nil {
			return err
		}
		if tableDesc.IsTSTable() {
			return sqlbase.TSUnsupportedError("truncate")
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
			return err
		}

		toTruncate[tableDesc.ID] = tn.FQString()
		toTraverse = append(toTraverse, *tableDesc)
	}

	// Check that any referencing tables are contained in the set, or, if CASCADE
	// requested, add them all to the set.
	for _, tb := range toTraverse {
		if tb.TableDescriptor.ReplicateFrom != "" {
			return errors.Errorf("TRUNCATE REPLICATION TABLE %v IS NOT ALLOWED", tb.TableDescriptor.Name)
		}
	}
	for len(toTraverse) > 0 {
		// Pick last element.
		idx := len(toTraverse) - 1
		tableDesc := toTraverse[idx]
		toTraverse = toTraverse[:idx]

		maybeEnqueue := func(tableID sqlbase.ID, msg string) error {
			// Check if we're already truncating the referencing table.
			if _, ok := toTruncate[tableID]; ok {
				return nil
			}
			other, err := p.Tables().GetMutableTableVersionByID(ctx, tableID, p.txn)
			if err != nil {
				return err
			}

			if n.DropBehavior != tree.DropCascade {
				return errors.Errorf("%q is %s table %q", tableDesc.Name, msg, other.Name)
			}
			if err := p.CheckPrivilege(ctx, other, privilege.DROP); err != nil {
				return err
			}
			otherName, err := GetQualifiedTableName(ctx, p.Txn(), other.TableDesc())
			if err != nil {
				return err
			}
			toTruncate[other.ID] = otherName
			toTraverse = append(toTraverse, *other)
			return nil
		}

		for i := range tableDesc.InboundFKs {
			fk := &tableDesc.InboundFKs[i]
			if err := maybeEnqueue(fk.OriginTableID, "referenced by foreign key from"); err != nil {
				return err
			}
		}
		for _, idx := range tableDesc.AllNonDropIndexes() {
			for _, ref := range idx.InterleavedBy {
				if err := maybeEnqueue(ref.Table, "interleaved by"); err != nil {
					return err
				}
			}
		}
	}

	// Mark this query as non-cancellable if autocommitting.
	if err := p.cancelChecker.Check(); err != nil {
		return err
	}

	traceKV := p.extendedEvalCtx.Tracing.KVTracingEnabled()
	for id, name := range toTruncate {
		if err := p.truncateTable(ctx, id, tree.AsStringWithFQNames(t.n, params.Ann()), traceKV); err != nil {
			return err
		}

		// Log a Truncate Table event for this table.
		params.GetPlanner().SetAuditTarget(uint32(id), name, nil)
	}

	return nil
}

func (t *truncateNode) Next(RunParams) (bool, error) { return false, nil }
func (t *truncateNode) Values() tree.Datums          { return tree.Datums{} }
func (t *truncateNode) Close(context.Context)        {}

// truncateTable truncates the data of a table in a single transaction. It
// drops the table and recreates it with a new ID. The dropped table is
// GC-ed later through an asynchronous schema change.
func (p *GenericPlanner) truncateTable(
	ctx context.Context, id sqlbase.ID, jobDesc string, traceKV bool,
) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Tables().GetMutableTableVersionByID(ctx, id, p.txn)
	if err != nil {
		return err
	}

	// todo(fxy/zhuodong):
	// 1. We can report err with tbl name later
	if p.txn != nil {
		isReplTbl := tableDesc.IsReplTable && tableDesc.ParentID > 1
		if isReplTbl {
			p.txn.ModifyReplTbl = true
		} else {
			p.txn.ModifyNonReplTbl = true
		}
		if p.txn.ModifyReplTbl && p.txn.ModifyNonReplTbl {
			return errors.Errorf("Txn don't support modify repl tables and non repl tables!")
		}
	}

	// tableDesc.DropJobID = dropJobID
	newTableDesc := sqlbase.NewMutableCreatedTableDescriptor(tableDesc.TableDescriptor)
	newTableDesc.ReplacementOf = sqlbase.TableDescriptor_Replacement{
		ID: id,
		// NB: Time is just used for debugging purposes. See the comment on the
		// field for more details.
		Time: p.txn.ReadTimestamp(),
	}
	newTableDesc.SetID(0)
	newTableDesc.Version = 1

	// Remove old name -> id map.
	// This is a violation of consistency because once the TRUNCATE commits
	// some nodes in the cluster can have cached the old name to id map
	// for the table and applying operations using the old table id.
	// This violation is needed because it is not uncommon for TRUNCATE
	// to be used along with other CRUD commands that follow it in the
	// same transaction. Commands that follow the TRUNCATE in the same
	// transaction will use the correct descriptor (through uncommittedTables)
	// See the comment about problem 3 related to draining names in
	// structured.proto
	//
	// TODO(vivek): Fix properly along with #12123.
	zoneKey := config.MakeZoneKey(uint32(tableDesc.ID))
	key := sqlbase.MakeObjectNameKey(
		ctx, p.ExecCfg().Settings,
		newTableDesc.ParentID,
		newTableDesc.GetParentSchemaID(),
		newTableDesc.Name,
	).Key()

	// Remove the old namespace entry.
	if err := sqlbase.RemoveObjectNamespaceEntry(
		ctx, p.txn,
		tableDesc.ParentID, tableDesc.GetParentSchemaID(), tableDesc.GetName(),
		traceKV); err != nil {
		return err
	}

	// Drop table.
	if err := p.initiateDropTable(ctx, tableDesc, true /* queueJob */, jobDesc, false /* drainName */); err != nil {
		return err
	}

	newID, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// update all the references to this table.
	tables, err := p.findAllReferences(ctx, *tableDesc)
	if err != nil {
		return err
	}
	if changed, err := reassignReferencedTables(tables, tableDesc.ID, newID); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	for _, table := range tables {
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := p.WriteSchemaChange(
			ctx, table, sqlbase.InvalidMutationID, "updating reference for truncated table",
		); err != nil {
			return err
		}
	}

	// Reassign all self references.
	if changed, err := reassignReferencedTables(
		[]*sqlbase.MutableTableDescriptor{newTableDesc}, tableDesc.ID, newID,
	); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	// Resolve all outstanding mutations. Make all new schema elements
	// public because the table is empty and doesn't need to be backfilled.
	for _, m := range newTableDesc.Mutations {
		if err := newTableDesc.MakeMutationComplete(m); err != nil {
			return err
		}
	}
	newTableDesc.Mutations = nil
	newTableDesc.GCMutations = nil
	// NB: Set the modification time to a zero value so that it is interpreted
	// as the commit timestamp for the new descriptor. See the comment on
	// sqlbase.Descriptor.Table().
	newTableDesc.ModificationTime = hlc.Timestamp{}
	// TODO (lucy): Have more consistent/informative names for dependent jobs.
	if err := p.CreateDescriptorWithID(
		ctx, key, newID, newTableDesc, p.ExtendedEvalContext().Settings,
		"creating new descriptor for truncated table",
	); err != nil {
		return err
	}

	// Reassign comments on the table, columns and indexes.
	if err := reassignComments(ctx, p, tableDesc, newTableDesc); err != nil {
		return err
	}

	// Copy the zone config.
	b := &kv.Batch{}
	b.Get(zoneKey)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	val := b.Results[0].Rows[0].Value
	if val == nil {
		return nil
	}
	zoneCfg, err := val.GetBytes()
	if err != nil {
		return err
	}
	const insertZoneCfg = `INSERT INTO system.zones (id, config) VALUES ($1, $2)`
	_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx, "insert-zone", p.txn, insertZoneCfg, newID, zoneCfg)
	return err
}

// For all the references from a table
func (p *GenericPlanner) findAllReferences(
	ctx context.Context, table sqlbase.MutableTableDescriptor,
) ([]*sqlbase.MutableTableDescriptor, error) {
	refs, err := table.FindAllReferences()
	if err != nil {
		return nil, err
	}
	tables := make([]*sqlbase.MutableTableDescriptor, 0, len(refs))
	for id := range refs {
		if id == table.ID {
			continue
		}
		t, err := p.Tables().GetMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// reassign all the references from oldID to newID.
func reassignReferencedTables(
	tables []*sqlbase.MutableTableDescriptor, oldID, newID sqlbase.ID,
) (bool, error) {
	changed := false
	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for j, a := range index.Interleave.Ancestors {
				if a.TableID == oldID {
					index.Interleave.Ancestors[j].TableID = newID
					changed = true
				}
			}
			for j, c := range index.InterleavedBy {
				if c.Table == oldID {
					index.InterleavedBy[j].Table = newID
					changed = true
				}
			}
			return nil
		}); err != nil {
			return false, err
		}
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if fk.ReferencedTableID == oldID {
				fk.ReferencedTableID = newID
				changed = true
			}
		}
		for i := range table.InboundFKs {
			fk := &table.InboundFKs[i]
			if fk.OriginTableID == oldID {
				fk.OriginTableID = newID
				changed = true
			}
		}

		for i, dest := range table.DependsOn {
			if dest == oldID {
				table.DependsOn[i] = newID
				changed = true
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if ref.ID == oldID {
				ref.ID = newID
				changed = true
			}
			table.DependedOnBy = append(table.DependedOnBy, ref)
		}
	}
	return changed, nil
}

// reassignComments reassign all comments on the table, indexes and columns.
func reassignComments(
	ctx context.Context,
	p *GenericPlanner,
	oldTableDesc, newTableDesc *sqlbase.MutableTableDescriptor,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"update-table-comments",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.comments SET object_id=$1 WHERE object_id=$2`,
		newTableDesc.ID,
		oldTableDesc.ID,
	)
	return err
}

// ClearTableDataInChunks truncates the data of a table in chunks. It deletes a
// range of data for the table, which includes the PK and all indexes.
// The table has already been marked for deletion and has been purged from the
// descriptor cache on all nodes.
//
// TODO(vivek): No node is reading/writing data on the table at this stage,
// therefore the entire table can be deleted with no concern for conflicts (we
// can even eliminate the need to use a transaction for each chunk at a later
// stage if it proves inefficient).
func ClearTableDataInChunks(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, db *kv.DB, traceKV bool,
) error {
	const chunkSize = TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &sqlbase.DatumAlloc{}
	for rowIdx, done := 0, false; !done; rowIdx += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.Name, rowIdx, resume)
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			rd, err := row.MakeDeleter(
				ctx,
				txn,
				sqlbase.NewImmutableTableDescriptor(*tableDesc),
				nil,
				nil, /* requestedCols */
				row.SkipFKs,
				nil, /* *tree.EvalContext */
				alloc,
			)
			if err != nil {
				return err
			}
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(ctx, txn, nil /* *tree.EvalContext */); err != nil {
				return err
			}
			resume, err = td.deleteAllRows(ctx, resumeAt, chunkSize, traceKV)
			return err
		}); err != nil {
			return err
		}
		done = resume.Key == nil
	}
	return nil
}

// canClearRangeForDrop returns if an index can be deleted by deleting every
// key from a single span.
// This determines whether an index is dropped during a schema change, or if
// it is only deleted upon GC.
func canClearRangeForDrop(index *sqlbase.IndexDescriptor) bool {
	return !index.IsInterleaved()
}
