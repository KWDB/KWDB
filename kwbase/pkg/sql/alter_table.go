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
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schemachange"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/lib/pq/oid"
)

const (
	// MaxInt16StrLen represents the minimum type width when converting int2 to varchar
	MaxInt16StrLen = 6
	// MaxInt32StrLen represents the minimum type width when converting int4 to varchar
	MaxInt32StrLen = 11
	// MaxInt64StrLen represents the minimum type width when converting int8 to varchar
	MaxInt64StrLen = 20
	// MaxDoubleStrLen represents the minimum type width when converting float4/float8 to varchar
	MaxDoubleStrLen = 30
	// DefaultNVarcharLen represents the default width of nvarchar
	DefaultNVarcharLen = 63
	// DefaultVarcharLen represents the default width of varchar
	DefaultVarcharLen = 254
)

type alterTableNode struct {
	n         *tree.AlterTable
	tableDesc *MutableTableDescriptor
	// statsData is populated with data for "alter table inject statistics"
	// commands - the JSON stats expressions.
	// It is parallel with n.Cmds (for the inject stats commands).
	statsData map[int]tree.TypedExpr
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(ctx context.Context, n *tree.AlterTable) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Table, !n.IfExists, ResolveRequireTableDesc,
	)
	if errors.Is(err, errNoPrimaryKey) {
		if len(n.Cmds) > 0 && isAlterCmdValidWithoutPrimaryKey(n.Cmds[0]) {
			tableDesc, err = p.ResolveMutableTableDescriptorExAllowNoPrimaryKey(
				ctx, n.Table, !n.IfExists, ResolveRequireTableDesc,
			)
		}
	}
	if err != nil {
		return nil, err
	}

	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if !p.extendedEvalCtx.TxnImplicit && tableDesc.IsTSTable() {
		return nil, sqlbase.UnsupportedTSExplicitTxnError()
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	n.HoistAddColumnConstraints()

	// See if there's any "inject statistics" in the query and type check the
	// expressions.
	statsData := make(map[int]tree.TypedExpr)
	for i, cmd := range n.Cmds {
		injectStats, ok := cmd.(*tree.AlterTableInjectStats)
		if !ok {
			continue
		}
		typedExpr, err := p.analyzeExpr(
			ctx, injectStats.Stats,
			nil, /* sources - no name resolution */
			tree.IndexedVarHelper{},
			types.Jsonb, true, /* requireType */
			"INJECT STATISTICS" /* typingContext */)
		if err != nil {
			return nil, err
		}
		statsData[i] = typedExpr
	}

	return &alterTableNode{
		n:         n,
		tableDesc: tableDesc,
		statsData: statsData,
	}, nil
}

func isAlterCmdValidWithoutPrimaryKey(cmd tree.AlterTableCmd) bool {
	switch t := cmd.(type) {
	case *tree.AlterTableAlterPrimaryKey:
		return true
	case *tree.AlterTableAddConstraint:
		cs, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
		if ok && cs.PrimaryKey {
			return true
		}
	default:
		return false
	}
	return false
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because ALTER TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterTableNode) ReadingOwnWrites() {}

func (n *alterTableNode) startExec(params runParams) error {
	if n.tableDesc.IsTSTable() && len(n.n.Cmds) > 1 {
		return pgerror.New(pgcode.FeatureNotSupported, "alter timeseries table with multiple commands is not supported")
	}
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("table"))

	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)
	var droppedViews []string
	tn := params.p.ResolvedName(n.n.Table)

	for i, cmd := range n.n.Cmds {
		telemetry.Inc(cmd.TelemetryCounter())

		if !n.tableDesc.HasPrimaryKey() && !isAlterCmdValidWithoutPrimaryKey(cmd) {
			return errors.Newf("table %q does not have a primary key, cannot perform%s", n.tableDesc.Name, tree.AsString(cmd))
		}

		switch t := cmd.(type) {
		case *tree.AlterTableAddColumn:
			d := t.ColumnDef
			if n.tableDesc.IsTSTable() {
				log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
				// Parameter validation for ts table.
				if n.tableDesc.TableType == tree.InstanceTable {
					return pgerror.New(pgcode.FeatureNotSupported, "add column is not supported for instance table")
				}
				if d.Nullable.Nullability == tree.NotNull {
					return pgerror.Newf(pgcode.FeatureNotSupported, "add NOT-NULL column %s for timeseries table is not supported", d.Name)
				}
				if len(n.tableDesc.Columns)+1 > MaxTSDataColumns {
					return pgerror.Newf(pgcode.TooManyColumns,
						"on table %s, the number of columns/tags exceeded the maximum value %d", n.tableDesc.Name, MaxTSDataColumns)
				}
				if err := checkTSColValidity(d); err != nil {
					return err
				}
				_, _, err := n.tableDesc.FindColumnByName(d.Name)
				if err == nil {
					// The column name is already exists
					if t.IfNotExists {
						continue
					} else {
						return pgerror.Newf(pgcode.DuplicateColumn, "duplicate column name: %q", d.Name)
					}
				}
				TSColumn, _, err := sqlbase.MakeTSColumnDefDescs(string(d.Name), d.Type, true, n.tableDesc.TsTable.GetSde(), sqlbase.ColumnType_TYPE_DATA, d.DefaultExpr.Expr, &params.p.semaCtx)
				if err != nil {
					return err
				}
				n.tableDesc.AddColumnMutation(TSColumn, sqlbase.DescriptorMutation_ADD)
				// Allocate IDs now, so new IDs are available to subsequent commands
				if err := n.tableDesc.AllocateIDs(); err != nil {
					return err
				}
				mutationID := n.tableDesc.ClusterVersion.NextMutationID
				// Create a Job to perform the second stage of ts DDL.
				syncDetail := jobspb.SyncMetaCacheDetails{
					Type:       alterKwdbAddColumn,
					SNTable:    n.tableDesc.TableDescriptor,
					AlterTag:   *TSColumn,
					MutationID: mutationID,
				}
				jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
				if err != nil {
					return err
				}
				if mutationID != sqlbase.InvalidMutationID {
					n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
						MutationID: mutationID, JobID: jobID})
				}
				if err = params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
					return err
				}
				// Actively commit a transaction, and read/write system table operations
				//need to be performed before this.
				if err = params.p.txn.Commit(params.ctx); err != nil {
					return err
				}
				// After the transaction commits successfully, execute the Job and wait for it to complete.
				if err = params.p.ExecCfg().JobRegistry.Run(
					params.ctx,
					params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
					[]int64{jobID},
				); err != nil {
					log.Info(params.ctx, err)
				}
				log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
				return nil
			}
			if d.HasFKConstraint() {
				return unimplemented.NewWithIssue(32917,
					"adding a REFERENCES constraint while also adding a column via ALTER not supported")
			}
			version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
			if supported, err := isTypeSupportedInVersion(version, d.Type); err != nil {
				return err
			} else if !supported {
				return pgerror.Newf(
					pgcode.FeatureNotSupported,
					"type %s is not supported until version upgrade is finalized",
					d.Type.SQLString(),
				)
			}

			newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn, n.tableDesc.IsTSTable())
			if err != nil {
				return err
			}
			if seqName != nil {
				if err := doCreateSequence(
					params,
					n.n.String(),
					seqDbDesc,
					n.tableDesc.GetParentSchemaID(),
					seqName,
					n.tableDesc.Temporary,
					seqOpts,
					tree.AsStringWithFQNames(n.n, params.Ann()),
					d.IsSerial,
				); err != nil {
					return err
				}
			}
			d = newDef
			incTelemetryForNewColumn(d)

			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, &params.p.semaCtx, tree.RelationalTable)
			if err != nil {
				return err
			}
			// If the new column has a DEFAULT expression that uses a sequence, add references between
			// its descriptor and this column descriptor.
			if d.HasDefaultExpr() {
				changedSeqDescs, err := maybeAddSequenceDependencies(
					params.ctx, params.p, n.tableDesc, col, expr, nil,
				)
				if err != nil {
					return err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					if err := params.p.writeSchemaChange(
						params.ctx, changedSeqDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
					); err != nil {
						return err
					}
				}
			}

			// We're checking to see if a user is trying add a non-nullable column without a default to a
			// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
			if !col.Nullable && (col.DefaultExpr == nil && !col.IsComputed()) {
				kvs, err := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
				if err != nil {
					return err
				}
				if len(kvs) > 0 {
					return sqlbase.NewNonNullViolationError(col.Name)
				}
			}
			_, dropped, err := n.tableDesc.FindColumnByName(d.Name)
			if err == nil {
				if dropped {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"column %q being dropped, try again later", col.Name)
				}
				if t.IfNotExists {
					continue
				}
			}

			n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_ADD)
			if idx != nil {
				if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}
			}
			if d.HasColumnFamily() {
				err := n.tableDesc.AddColumnToFamilyMaybeCreate(
					col.Name, string(d.Family.Name), d.Family.Create,
					d.Family.IfNotExists)
				if err != nil {
					return err
				}
			}

			if d.IsComputed() {
				if err := validateComputedColumn(n.tableDesc, d, &params.p.semaCtx); err != nil {
					return err
				}
			}

		case *tree.AlterTableAddConstraint:
			if n.tableDesc.IsTSTable() {
				return pgerror.New(pgcode.FeatureNotSupported, "adding constraint to timeseries table is not supported")
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			inuseNames := make(map[string]struct{}, len(info))
			for k := range info {
				inuseNames[k] = struct{}{}
			}
			switch d := t.ConstraintDef.(type) {
			case *tree.UniqueConstraintTableDef:
				if d.PrimaryKey {
					// We only support "adding" a primary key when we are using the
					// default rowid primary index or if a DROP PRIMARY KEY statement
					// was processed before this statement. If a DROP PRIMARY KEY
					// statement was processed, then n.tableDesc.HasPrimaryKey() = false.
					if n.tableDesc.HasPrimaryKey() && !n.tableDesc.IsPrimaryIndexDefaultRowID() {
						return pgerror.Newf(pgcode.InvalidTableDefinition,
							"multiple primary keys for table %q are not allowed", n.tableDesc.Name)
					}

					// Translate this operation into an ALTER PRIMARY KEY command.
					alterPK := &tree.AlterTableAlterPrimaryKey{
						Columns:    d.Columns,
						Sharded:    d.Sharded,
						Interleave: d.Interleave,
					}
					if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK); err != nil {
						return err
					}
					continue
				}
				idx := sqlbase.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					StoreColumnNames: d.Storing.ToStrings(),
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}
				if d.PartitionBy != nil {
					partitioning, err := CreatePartitioning(
						params.ctx, params.p.ExecCfg().Settings,
						params.EvalContext(), n.tableDesc, &idx, d.PartitionBy)
					if err != nil {
						return err
					}
					idx.Partitioning = partitioning
				}
				_, dropped, err := n.tableDesc.FindIndexByName(string(d.Name))
				if err == nil {
					if dropped {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}

			case *tree.CheckConstraintTableDef:
				ck, err := MakeCheckConstraint(params.ctx,
					n.tableDesc, d, inuseNames, &params.p.semaCtx, *tn)
				if err != nil {
					return err
				}
				if t.ValidationBehavior == tree.ValidationDefault {
					ck.Validity = sqlbase.ConstraintValidity_Validating
				} else {
					ck.Validity = sqlbase.ConstraintValidity_Unvalidated
				}
				n.tableDesc.AddCheckMutation(ck, sqlbase.DescriptorMutation_ADD)

			case *tree.ForeignKeyConstraintTableDef:
				for _, colName := range d.FromCols {
					col, err := n.tableDesc.FindActiveColumnByName(string(colName))
					if err != nil {
						if _, dropped, inactiveErr := n.tableDesc.FindColumnByName(colName); inactiveErr == nil && !dropped {
							return unimplemented.NewWithIssue(32917,
								"adding a REFERENCES constraint while the column is being added not supported")
						}
						return err
					}

					if err := col.CheckCanBeFKRef(); err != nil {
						return err
					}
				}
				affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

				// If there are any FKs, we will need to update the table descriptor of the
				// depended-on table (to register this table against its DependedOnBy field).
				// This descriptor must be looked up uncached, and we'll allow FK dependencies
				// on tables that were just added. See the comment at the start of
				// the global-scope resolveFK().
				// TODO(vivek): check if the cache can be used.
				params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Check whether the table is empty, and pass the result to resolveFK(). If
					// the table is empty, then resolveFK will automatically add the necessary
					// index for a fk constraint if the index does not exist.
					kvs, scanErr := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
					if scanErr != nil {
						err = scanErr
						return
					}
					var tableState FKTableState
					if len(kvs) == 0 {
						tableState = EmptyTable
					} else {
						tableState = NonEmptyTable
					}
					err = params.p.resolveFK(params.ctx, n.tableDesc, d, affected, tableState, t.ValidationBehavior)
				})
				if err != nil {
					return err
				}
				descriptorChanged = true
				for _, updated := range affected {
					if err := params.p.writeSchemaChange(
						params.ctx, updated, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
					); err != nil {
						return err
					}
				}
				// TODO(lucy): Validate() can't be called here because it reads the
				// referenced table descs, which may have to be upgraded to the new FK
				// representation. That requires reading the original table descriptor
				// (which the backreference points to) from KV, but we haven't written
				// the updated table desc yet. We can restore the call to Validate()
				// after running a migration of all table descriptors, making it
				// unnecessary to read the original table desc from KV.
				// if err := n.tableDesc.Validate(params.ctx, params.p.txn); err != nil {
				// 	return err
				// }

			default:
				return errors.AssertionFailedf(
					"unsupported constraint: %T", t.ConstraintDef)
			}

		case *tree.AlterTableAlterPrimaryKey:
			if n.tableDesc.IsTSTable() {
				return pgerror.New(pgcode.WrongObjectType, "alter primary key on timeseries table is not supported")
			}
			if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, t); err != nil {
				return err
			}
			// Mark descriptorChanged so that a mutation job is scheduled at the end of startExec.
			descriptorChanged = true

		case *tree.AlterTableDropColumn:
			if params.SessionData().SafeUpdates {
				return pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will remove all data in that column")
			}

			colToDrop, dropped, err := n.tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return err
			}
			if dropped {
				continue
			}

			if n.tableDesc.IsTSTable() {
				if colToDrop.IsTagCol() {
					return pgerror.Newf(pgcode.WrongObjectType, "%q is a tag, not a column, try use \"drop tag\" clause in DDL", colToDrop.ColName())
				}
				log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
				if colToDrop.ID == 1 {
					return pgerror.Newf(pgcode.InvalidColumnDefinition, "cannot drop the first timestamp column %s", colToDrop.Name)
				}
				var columnCount int
				found := false
				for _, column := range n.tableDesc.Columns {
					if !column.IsTagCol() {
						columnCount++
					}
					if column.ID == colToDrop.ID {
						found = true
					}
				}
				if columnCount == 2 {
					return pgerror.Newf(
						pgcode.InvalidTableDefinition,
						"cannot drop the only data column besides the timestamp column on table %s", n.tableDesc.Name)
				}
				if !found {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"column %q in the middle of being added, try again later", t.Column)
				}

				// n.tableDesc.State = sqlbase.TableDescriptor_ALTER
				n.tableDesc.AddColumnMutation(colToDrop, sqlbase.DescriptorMutation_DROP)
				mutationID := n.tableDesc.ClusterVersion.NextMutationID
				// Create a Job to perform the second stage of ts DDL.
				syncDetail := jobspb.SyncMetaCacheDetails{
					Type:       alterKwdbDropColumn,
					SNTable:    n.tableDesc.TableDescriptor,
					AlterTag:   *colToDrop,
					MutationID: mutationID,
				}
				jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
				if err != nil {
					return err
				}
				if mutationID != sqlbase.InvalidMutationID {
					n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
						MutationID: mutationID, JobID: jobID})
				}
				if err = params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
					return err
				}
				// Actively commit a transaction, and read/write system table operations
				// need to be performed before this.
				if err = params.p.txn.Commit(params.ctx); err != nil {
					return err
				}

				// After the transaction commits successfully, execute the Job and wait for it to complete.
				if err = params.p.ExecCfg().JobRegistry.Run(
					params.ctx,
					params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
					[]int64{jobID},
				); err != nil {
					log.Info(params.ctx, err)
				}
				log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
				return nil
			}

			// If the dropped column uses a sequence, remove references to it from that sequence.
			if len(colToDrop.UsesSequenceIds) > 0 {
				if err := params.p.removeSequenceDependencies(params.ctx, n.tableDesc, colToDrop); err != nil {
					return err
				}
			}

			// You can't remove a column that owns a sequence that is depended on
			// by another column
			if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, n.tableDesc, colToDrop, t.DropBehavior); err != nil {
				return err
			}

			if err := params.p.dropSequencesOwnedByCol(params.ctx, colToDrop, true /* queueJob */); err != nil {
				return err
			}

			// You can't drop a column depended on by a view unless CASCADE was
			// specified.
			for _, ref := range n.tableDesc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == colToDrop.ID {
						found = true
						break
					}
				}
				if !found {
					continue
				}
				err := params.p.canRemoveDependentViewGeneric(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				viewDesc, err := params.p.getViewDescForCascade(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref.ID, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				jobDesc := fmt.Sprintf("removing view %q dependent on column %q which is being dropped",
					viewDesc.Name, colToDrop.ColName())
				droppedViews, err = params.p.removeDependentView(params.ctx, n.tableDesc, viewDesc, jobDesc)
				if err != nil {
					return err
				}
			}

			if n.tableDesc.PrimaryIndex.ContainsColumnID(colToDrop.ID) {
				return pgerror.Newf(pgcode.InvalidColumnReference,
					"column %q is referenced by the primary key", colToDrop.Name)
			}
			for _, idx := range n.tableDesc.AllNonDropIndexes() {
				// We automatically drop indexes on that column that only
				// index that column (and no other columns). If CASCADE is
				// specified, we also drop other indices that refer to this
				// column.  The criteria to determine whether an index "only
				// indexes that column":
				//
				// Assume a table created with CREATE TABLE foo (a INT, b INT).
				// Then assume the user issues ALTER TABLE foo DROP COLUMN a.
				//
				// INDEX i1 ON foo(a) -> i1 deleted
				// INDEX i2 ON foo(a) STORING(b) -> i2 deleted
				// INDEX i3 ON foo(a, b) -> i3 not deleted unless CASCADE is specified.
				// INDEX i4 ON foo(b) STORING(a) -> i4 not deleted unless CASCADE is specified.

				// containsThisColumn becomes true if the index is defined
				// over the column being dropped.
				containsThisColumn := false
				// containsOnlyThisColumn becomes false if the index also
				// includes non-PK columns other than the one being dropped.
				containsOnlyThisColumn := true

				// Analyze the index.
				for _, id := range idx.ColumnIDs {
					if id == colToDrop.ID {
						containsThisColumn = true
					} else {
						containsOnlyThisColumn = false
					}
				}
				for _, id := range idx.ExtraColumnIDs {
					if n.tableDesc.PrimaryIndex.ContainsColumnID(id) {
						// All secondary indices necessary contain the PK
						// columns, too. (See the comments on the definition of
						// IndexDescriptor). The presence of a PK column in the
						// secondary index should thus not be seen as a
						// sufficient reason to reject the DROP.
						continue
					}
					if id == colToDrop.ID {
						containsThisColumn = true
					}
				}
				// The loop above this comment is for the old STORING encoding. The
				// loop below is for the new encoding (where the STORING columns are
				// always in the value part of a KV).
				for _, id := range idx.StoreColumnIDs {
					if id == colToDrop.ID {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					if containsOnlyThisColumn || t.DropBehavior == tree.DropCascade {
						jobDesc := fmt.Sprintf("removing index %q dependent on column %q which is being"+
							" dropped; full details: %s", idx.Name, colToDrop.ColName(),
							tree.AsStringWithFQNames(n.n, params.Ann()))
						if err := params.p.dropIndexByName(
							params.ctx, tn, tree.UnrestrictedName(idx.Name), n.tableDesc, false,
							t.DropBehavior, ignoreIdxConstraint, jobDesc,
						); err != nil {
							return err
						}
					} else {
						return pgerror.Newf(pgcode.InvalidColumnReference,
							"column %q is referenced by existing index %q", colToDrop.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			// Note that foreign key constraints are dropped as part of dropping
			// indexes on the column. In the future, when FKs no longer depend on
			// indexes in the same way, FKs will have to be dropped separately here.
			validChecks := n.tableDesc.Checks[:0]
			for _, check := range n.tableDesc.AllActiveAndInactiveChecks() {
				if used, err := check.UsesColumn(n.tableDesc.TableDesc(), colToDrop.ID); err != nil {
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

			if len(validChecks) != len(n.tableDesc.Checks) {
				n.tableDesc.Checks = validChecks
				descriptorChanged = true
			}

			if err != nil {
				return err
			}
			if err := params.p.removeColumnComment(
				params.ctx,
				params.p.txn,
				n.tableDesc.ID,
				colToDrop.ID,
			); err != nil {
				return err
			}

			found := false
			for i := range n.tableDesc.Columns {
				if n.tableDesc.Columns[i].ID == colToDrop.ID {
					n.tableDesc.AddColumnMutation(colToDrop, sqlbase.DescriptorMutation_DROP)
					// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
					// to ColumnDescriptors may unexpectedly change.
					n.tableDesc.Columns = append(n.tableDesc.Columns[:i:i], n.tableDesc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being added, try again later", t.Column)
			}
			if err := n.tableDesc.Validate(params.ctx, params.p.txn); err != nil {
				return err
			}

		case *tree.AlterTableDropConstraint:
			if n.tableDesc.IsTSTable() {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not drop constraint on timeseries table \"%s\"", n.tableDesc.Name)
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			details, ok := info[name]
			if !ok {
				if t.IfExists {
					continue
				}
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist on table %s", t.Constraint, n.tableDesc.Name)
			}
			if err := n.tableDesc.DropConstraint(
				params.ctx,
				name, details,
				func(desc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint) error {
					return params.p.removeFKBackReference(params.ctx, desc, ref)
				}, params.ExecCfg().Settings); err != nil {
				return err
			}
			descriptorChanged = true
			if err := n.tableDesc.Validate(params.ctx, params.p.txn); err != nil {
				return err
			}

		case *tree.AlterTableValidateConstraint:
			if n.tableDesc.IsTSTable() {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not validate constraint on timeseries table \"%s\"", n.tableDesc.Name)
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist on table %s", t.Constraint, n.tableDesc.Name)
			}
			if !constraint.Unvalidated {
				continue
			}
			switch constraint.Kind {
			case sqlbase.ConstraintTypeCheck:
				found := false
				var ck *sqlbase.TableDescriptor_CheckConstraint
				for _, c := range n.tableDesc.Checks {
					// If the constraint is still being validated, don't allow VALIDATE CONSTRAINT to run
					if c.Name == name && c.Validity != sqlbase.ConstraintValidity_Validating {
						found = true
						ck = c
						break
					}
				}
				if !found {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint %q in the middle of being added, try again later", t.Constraint)
				}
				if err := validateCheckInTxn(
					params.ctx, params.p.LeaseMgr(), params.EvalContext(), n.tableDesc, params.EvalContext().Txn, ck.Expr,
				); err != nil {
					return err
				}
				ck.Validity = sqlbase.ConstraintValidity_Validated

			case sqlbase.ConstraintTypeFK:
				var foundFk *sqlbase.ForeignKeyConstraint
				for i := range n.tableDesc.OutboundFKs {
					fk := &n.tableDesc.OutboundFKs[i]
					// If the constraint is still being validated, don't allow VALIDATE CONSTRAINT to run
					if fk.Name == name && fk.Validity != sqlbase.ConstraintValidity_Validating {
						foundFk = fk
						break
					}
				}
				if foundFk == nil {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint %q in the middle of being added, try again later", t.Constraint)
				}
				if err := validateFkInTxn(
					params.ctx, params.p.LeaseMgr(), params.EvalContext(), n.tableDesc, params.EvalContext().Txn, name,
				); err != nil {
					return err
				}
				foundFk.Validity = sqlbase.ConstraintValidity_Validated

			default:
				return pgerror.Newf(pgcode.WrongObjectType,
					"constraint %q of relation %q is not a foreign key or check constraint",
					tree.ErrString(&t.Constraint), tree.ErrString(n.n.Table))
			}
			descriptorChanged = true

		case tree.ColumnMutationCmd:
			_, isTSAlterColumnType := t.(*tree.AlterTableAlterColumnType)
			_, isTSAlterColumnDefault := t.(*tree.AlterTableSetDefault)
			if !n.tableDesc.IsTSTable() {
				isTSAlterColumnType = false
			}
			if n.tableDesc.IsTSTable() && !isTSAlterColumnType && !isTSAlterColumnDefault {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not modify an existing column on ts table \"%s\"", n.tableDesc.Name)
			}
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being dropped", t.GetColumn())
			}
			isOnlyMetaChange := false
			// Apply mutations to copy of column descriptor.
			if isOnlyMetaChange, err = applyColumnMutation(n.tableDesc, col, t, params); err != nil {
				return err
			}
			if isTSAlterColumnType && !isOnlyMetaChange {
				return nil
			}
			descriptorChanged = true

		case *tree.AlterTableAlterTagType:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not alter tag type on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not alter tag type on instance table \"%s\"", n.tableDesc.Name)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			tagColumn, dropped, err := n.tableDesc.FindColumnByName(t.Tag)
			if err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					return sqlbase.NewUndefinedTagError(string(t.Tag))
				}
				return err
			}
			if dropped {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being dropped", t.Tag)
			}
			if err = checkIndexOnTag(n, tagColumn); err != nil {
				return err
			}

			if !tagColumn.IsTagCol() {
				return pgerror.Newf(pgcode.WrongObjectType, "%s is not a tag", tagColumn.Name)
			}
			if tagColumn.IsPrimaryTagCol() {
				return pgerror.Newf(pgcode.WrongObjectType,
					"tag %q is a primary tag", tagColumn.Name)
			}
			// type cast validation
			newType := prepareAlterType(t.ToType)
			if tagColumn.Type.Identical(newType) {
				return nil
			}
			isDoNothing, err := validateAlterTSType(tagColumn.Name, &tagColumn.Type, newType, sqlbase.ColumnType_TYPE_TAG)
			if err != nil {
				return err
			}
			if isDoNothing {
				return nil
			}

			alteringTag, _, err := sqlbase.MakeTSColumnDefDescs(tagColumn.Name, newType, tagColumn.Nullable, false, sqlbase.ColumnType_TYPE_TAG, nil, &params.p.semaCtx)
			if err != nil {
				return err
			}
			alteringTag.ID = tagColumn.ID
			// Check that newType is compatible with mutationColType already present in mutation.
			if err = checkTSMutationColumnType(n.tableDesc, alteringTag); err != nil {
				return err
			}
			if tagColumn.HasDefault() {
				err := castDefaultFuncExpr(*tagColumn.DefaultExpr, alteringTag, params, newType)
				if err != nil {
					return err
				}
			}
			//n.tableDesc.State = sqlbase.TableDescriptor_ALTER
			n.tableDesc.AddColumnMutation(alteringTag, sqlbase.DescriptorMutation_NONE)
			mutationID := n.tableDesc.ClusterVersion.NextMutationID
			// Create a Job to perform the second stage of ts DDL.
			syncDetail := jobspb.SyncMetaCacheDetails{
				Type:         alterKwdbAlterTagType,
				SNTable:      n.tableDesc.TableDescriptor,
				AlterTag:     *alteringTag,
				OriginColumn: *tagColumn,
				MutationID:   mutationID,
			}
			jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
			if err != nil {
				return err
			}
			if mutationID != sqlbase.InvalidMutationID {
				n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID, JobID: jobID})
			}
			if err = params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
				return err
			}
			// Actively commit a transaction, and read/write system table operations
			// need to be performed before this.
			if err = params.p.txn.Commit(params.ctx); err != nil {
				return err
			}

			// After the transaction commits successfully, execute the Job and wait for it to complete.
			if err = params.p.ExecCfg().JobRegistry.Run(
				params.ctx,
				params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
				[]int64{jobID},
			); err != nil {
				log.Info(params.ctx, err)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			return nil

		case *tree.AlterTablePartitionBy:
			if n.tableDesc.IsTSTable() && t.HashPoint == nil {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not partition ts table \"%s\"", n.tableDesc.Name)
			}
			partitioning, err := CreatePartitioning(
				params.ctx, params.p.ExecCfg().Settings,
				params.EvalContext(),
				n.tableDesc, &n.tableDesc.PrimaryIndex, t.PartitionBy)
			if err != nil {
				return err
			}
			descriptorChanged = !proto.Equal(
				&n.tableDesc.PrimaryIndex.Partitioning,
				&partitioning,
			)
			err = deleteRemovedPartitionZoneConfigs(
				params.ctx, params.p.txn,
				n.tableDesc.TableDesc(), &n.tableDesc.PrimaryIndex, &n.tableDesc.PrimaryIndex.Partitioning,
				&partitioning, params.extendedEvalCtx.ExecCfg,
			)
			if err != nil {
				return err
			}
			n.tableDesc.PrimaryIndex.Partitioning = partitioning

		/*case *tree.AlterTableSetAudit:
		  var err error
		  descriptorChanged, err = params.p.setAuditMode(params.ctx, n.tableDesc.TableDesc(), t.Mode)
		  if err != nil {
		  	return err
		  }*/

		case *tree.AlterTableInjectStats:
			sd, ok := n.statsData[i]
			if !ok {
				return errors.AssertionFailedf("missing stats data")
			}
			if !params.p.EvalContext().TxnImplicit {
				return errors.New("cannot inject statistics in an explicit transaction")
			}
			if err := injectTableStats(params, n.tableDesc.TableDesc(), sd); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			const allowRenameOfShardColumn = false
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc,
				&t.Column, &t.NewName, allowRenameOfShardColumn, false)
			if err != nil {
				return err
			}
			descriptorChanged = descChanged

		case *tree.AlterTableRenameConstraint:
			if n.tableDesc.IsTSTable() {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not rename constraint on timeseries table \"%s\"", n.tableDesc.Name)
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			details, ok := info[string(t.Constraint)]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist on table %s", tree.ErrString(&t.Constraint), n.tableDesc.Name)
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}

			if _, ok := info[string(t.NewName)]; ok {
				return pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", tree.ErrString(&t.NewName))
			}

			if err := params.p.CheckPrivilege(params.ctx, n.tableDesc, privilege.CREATE); err != nil {
				return err
			}

			depViewRenameError := func(objType string, refTableID sqlbase.ID) error {
				return params.p.dependentViewRenameError(params.ctx,
					objType, tree.ErrString(&t.NewName), n.tableDesc.ParentID, refTableID)
			}

			if err := n.tableDesc.RenameConstraint(
				details, string(t.Constraint), string(t.NewName), depViewRenameError, func(desc *MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint, newName string) error {
					return params.p.updateFKBackReferenceName(params.ctx, desc, ref, newName)
				}); err != nil {
				return err
			}
			descriptorChanged = true

		case *tree.AlterTableAddTag:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not add tag on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not add tag on instance table \"%s\"", n.tableDesc.Name)
			}
			if len(n.tableDesc.Columns)+1 > MaxTSDataColumns {
				return pgerror.Newf(pgcode.TooManyColumns,
					"on table %s, the number of columns/tags exceeded the maximum value %d", n.tableDesc.Name, MaxTSDataColumns)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			tagNum := 0
			for _, column := range n.tableDesc.Columns {
				if column.IsTagCol() {
					tagNum++
				}
			}
			if tagNum >= sqlbase.MaxTagNum {
				return sqlbase.ErrTooManyTags
			}

			if len(string(t.Tag.TagName)) > MaxTagNameLength {
				return sqlbase.NewTSNameOutOfLengthError("tag", string(t.Tag.TagName), MaxTagNameLength)
			}
			if !t.Tag.Nullable {
				return pgerror.Newf(pgcode.FeatureNotSupported, "cannot add a NOT NULL tag in timeseries table (tag %s)", string(t.Tag.TagName))
			}
			_, _, err := n.tableDesc.FindColumnByName(t.Tag.TagName)
			if err == nil {
				return pgerror.Newf(pgcode.DuplicateColumn, "duplicate tag name: %q", t.Tag.TagName)
			}
			if t.Tag.IsSerial {
				return pgerror.Newf(
					pgcode.FeatureNotSupported, "serial tag %s is not supported in timeseries table", t.Tag.TagName)
			}
			// Tag type check
			tagType, err := checkTagType(t.Tag.TagName, t.Tag.TagType)
			if err != nil {
				return err
			}
			t.Tag.TagType = tagType
			tagCol, _, err := sqlbase.MakeTSColumnDefDescs(string(t.Tag.TagName), t.Tag.TagType, t.Tag.Nullable, false, sqlbase.ColumnType_TYPE_TAG, nil, &params.p.semaCtx)
			if err != nil {
				return err
			}
			if tagCol.ID == 0 {
				tagCol.ID = n.tableDesc.GetNextColumnID()
				n.tableDesc.NextColumnID++
			}
			//n.tableDesc.State = sqlbase.TableDescriptor_ALTER
			n.tableDesc.AddColumnMutation(tagCol, sqlbase.DescriptorMutation_ADD)
			// Allocate IDs now, so new IDs are available to subsequent commands
			if err := n.tableDesc.AllocateIDs(); err != nil {
				return err
			}
			mutationID := n.tableDesc.ClusterVersion.NextMutationID
			// Create a Job to perform the second stage of ts DDL.
			syncDetail := jobspb.SyncMetaCacheDetails{
				Type:       alterKwdbAddTag,
				SNTable:    n.tableDesc.TableDescriptor,
				AlterTag:   *tagCol,
				MutationID: mutationID,
			}
			jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
			if err != nil {
				log.Info(params.ctx, err)
			}
			if mutationID != sqlbase.InvalidMutationID {
				n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID, JobID: jobID})
			}
			if err = params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
				return err
			}
			// Actively commit a transaction, and read/write system table operations
			// need to be performed before this.
			if err = params.p.txn.Commit(params.ctx); err != nil {
				return err
			}

			// After the transaction commits successfully, execute the Job and wait for it to complete.
			if err = params.p.ExecCfg().JobRegistry.Run(
				params.ctx,
				params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
				[]int64{jobID},
			); err != nil {
				log.Info(params.ctx, err)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			return nil

		case *tree.AlterTableDropTag:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not drop tag on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not drop tag on instance table \"%s\"", n.tableDesc.Name)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			tagColumn, dropped, err := n.tableDesc.FindColumnByName(t.TagName)
			if err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					return sqlbase.NewUndefinedTagError(string(t.TagName))
				}
				return err
			}
			if !tagColumn.IsTagCol() {
				return pgerror.Newf(pgcode.WrongObjectType,
					"%q is not a tag", tagColumn.Name)
			}

			if tagColumn.IsPrimaryTagCol() {
				return pgerror.Newf(pgcode.WrongObjectType,
					"tag %q is a primary tag", tagColumn.Name)
			}
			if dropped {
				continue
			}
			if err = checkIndexOnTag(n, tagColumn); err != nil {
				return err
			}
			var tagCount int
			found := false
			for _, tag := range n.tableDesc.Columns {
				if tag.IsTagCol() {
					tagCount++
				}
				if tag.ID == tagColumn.ID {
					found = true
				}
			}
			if tagCount == 1 && n.tableDesc.TableType == tree.TemplateTable {
				return pgerror.Newf(pgcode.InvalidTableDefinition, "cannot drop the only tag on table %s", n.tableDesc.Name)
			}
			if !found {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"tag %q in the middle of being added, try again later", tagColumn.Name)
			}
			//n.tableDesc.State = sqlbase.TableDescriptor_ALTER
			n.tableDesc.AddColumnMutation(tagColumn, sqlbase.DescriptorMutation_DROP)
			mutationID := n.tableDesc.ClusterVersion.NextMutationID
			// Create a Job to perform the second stage of ts DDL.
			syncDetail := jobspb.SyncMetaCacheDetails{
				Type:       alterKwdbDropTag,
				SNTable:    n.tableDesc.TableDescriptor,
				AlterTag:   *tagColumn,
				MutationID: mutationID,
			}
			jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
			if err != nil {
				return err
			}
			if mutationID != sqlbase.InvalidMutationID {
				n.tableDesc.MutationJobs = append(n.tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID, JobID: jobID})
			}
			if err = params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
				return err
			}
			// Actively commit a transaction, and read/write system table operations
			// need to be performed before this.
			if err = params.p.txn.Commit(params.ctx); err != nil {
				return err
			}

			// After the transaction commits successfully, execute the Job and wait for it to complete.
			if err = params.p.ExecCfg().JobRegistry.Run(
				params.ctx,
				params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
				[]int64{jobID},
			); err != nil {
				log.Info(params.ctx, err)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			return nil

		case *tree.AlterTableRenameTag:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not rename tag on relational table \"%s\"", n.tableDesc.Name)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
			const allowRenameOfShardColumn = false
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc,
				&t.OldName, &t.NewName, allowRenameOfShardColumn, true)
			if err != nil {
				return err
			}
			descriptorChanged = descChanged

		case *tree.AlterTableSetTag:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set tag on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.TemplateTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set tag on template table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.TimeseriesTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set tag on time series table \"%s\"", n.tableDesc.Name)
			}

			// get instance table id
			db, err := getDatabaseDescByID(params.ctx, params.p.txn, n.tableDesc.ParentID)
			if err != nil {
				return err
			}
			insTable, found, err := sqlbase.ResolveInstanceName(params.ctx, params.p.txn, db.Name, n.n.Table.Parts[0])
			if err != nil {
				return err
			} else if !found {
				return sqlbase.NewUndefinedTableError(n.n.Table.Parts[0])
			}
			if err != nil {
				return err
			}
			cTableName := insTable.InstName
			insTable.State = sqlbase.ChildDesc_ALTER
			var tagName []string
			var tagVal string
			for _, name := range t.Expr.Names {
				col, _, err := n.tableDesc.FindColumnByName(name)
				if err != nil {
					return err
				}
				datum, err := checkTagValue(params, t.Expr.Expr, col.Type, col.Nullable, col.Name)
				if err != nil {
					return err
				}
				tagVal = sqlbase.DatumToString(datum)
				tagName = append(tagName, string(name))
			}
			// Create a Job to perform the second stage of ts DDL.
			syncDetail := jobspb.SyncMetaCacheDetails{
				Type:    alterKwdbSetTagValue,
				SNTable: n.tableDesc.TableDescriptor,
				SetTag:  jobspb.SetTag{TableName: cTableName, TableId: int64(insTable.InstTableID), DbName: db.Name, TagName: tagName, TagValue: tagVal},
			}
			jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
			if err != nil {
				return err
			}
			if err := writeInstTableMeta(params.ctx, params.p.txn, []sqlbase.InstNameSpace{insTable}, true); err != nil {
				return err
			}

			// Actively commit a transaction, and read/write system table operations
			// need to be performed before this.
			if err = params.p.txn.Commit(params.ctx); err != nil {
				return err
			}

			// After the transaction commits successfully, execute the Job and wait for it to complete.
			if err = params.p.ExecCfg().JobRegistry.Run(
				params.ctx,
				params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
				[]int64{jobID},
			); err != nil {
				log.Info(params.ctx, err)
			}
			return nil

		case *tree.AlterTableSetRetentions:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set retentions on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set retentions on instance table \"%s\"", n.tableDesc.Name)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			lifeTime := getTimeFromTimeInput(t.TimeInput)
			if lifeTime < 0 || lifeTime > MaxLifeTime {
				return pgerror.Newf(pgcode.InvalidParameterValue, "parameter %d %s is invalid",
					t.TimeInput.Value, t.TimeInput.Unit)
			}
			n.tableDesc.TsTable.Lifetime = uint64(lifeTime)
			var downsampling []string
			downsampling = append(downsampling, timeInputToString(t.TimeInput))
			n.tableDesc.TsTable.Downsampling = downsampling
			descriptorChanged = true

		case *tree.AlterTableSetActivetime:
			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set retentions on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set retentions on instance table \"%s\"", n.tableDesc.Name)
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			activeTime := getTimeFromTimeInput(t.TimeInput)
			if activeTime < 0 || activeTime > MaxLifeTime {
				return pgerror.Newf(pgcode.InvalidParameterValue, "active time %d %s is invalid",
					t.TimeInput.Value, t.TimeInput.Unit)
			}
			activeTimeInput := timeInputToString(t.TimeInput)
			n.tableDesc.TsTable.ActiveTime = uint32(activeTime)
			n.tableDesc.TsTable.ActiveTimeInput = &activeTimeInput
			descriptorChanged = true
		case *tree.AlterPartitionInterval:
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			if n.tableDesc.TableType == tree.RelationalTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set partition interval on relational table \"%s\"", n.tableDesc.Name)
			}
			if n.tableDesc.TableType == tree.InstanceTable {
				return pgerror.Newf(
					pgcode.WrongObjectType, "can not set partition interval on instance table \"%s\"", n.tableDesc.Name)
			}
			switch t.TimeInput.Unit {
			case "s", "second", "m", "minute", "h", "hour":
				return pgerror.Newf(pgcode.InvalidParameterValue, "unsupported partition interval unit: %s",
					t.TimeInput.Unit)
			}
			partitionInterval := getTimeFromTimeInput(t.TimeInput)
			if partitionInterval <= 0 || partitionInterval > MaxLifeTime {
				return pgerror.Newf(pgcode.InvalidParameterValue, "partition interval %d %s is invalid, the time range is [1day, 1000year]",
					t.TimeInput.Value, t.TimeInput.Unit)
			}
			partitionIntervalInput := timeInputToString(t.TimeInput)
			n.tableDesc.TsTable.PartitionInterval = uint64(partitionInterval)
			n.tableDesc.TsTable.PartitionIntervalInput = &partitionIntervalInput
			// Create a Job to perform the second stage of ts DDL.
			syncDetail := jobspb.SyncMetaCacheDetails{
				Type:    alterKwdbAlterPartitionInterval,
				SNTable: n.tableDesc.TableDescriptor,
			}
			jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
			if err != nil {
				return err
			}
			// Actively commit a transaction, and read/write system table operations
			// need to be performed before this.
			if err = params.p.txn.Commit(params.ctx); err != nil {
				return err
			}
			// After the transaction commits successfully, execute the Job and wait for it to complete.
			if err = params.p.ExecCfg().JobRegistry.Run(
				params.ctx,
				params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
				[]int64{jobID},
			); err != nil {
				return err
			}
			log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)

			return nil
		default:
			return errors.AssertionFailedf("unsupported alter command: %T", cmd)
		}

		// Allocate IDs now, so new IDs are available to subsequent commands
		if err := n.tableDesc.AllocateIDs(); err != nil {
			return err
		}
	}
	// Were some changes made?
	//
	// This is only really needed for the unittests that add dummy mutations
	// before calling ALTER TABLE commands. We do not want to apply those
	// dummy mutations. Most tests trigger errors above
	// this line, but tests that run redundant operations like dropping
	// a column when it's already dropped will hit this condition and exit.
	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		return nil
	}

	mutationID := sqlbase.InvalidMutationID
	if addedMutations {
		mutationID = n.tableDesc.ClusterVersion.NextMutationID
	}
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	params.p.SetAuditTarget(uint32(n.tableDesc.GetID()), n.tableDesc.GetName(), droppedViews)
	if descriptorChanged {
		log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", n.n.Table.String(), n.tableDesc.ID, n.n.Cmds)
	}
	return nil
}

// checkTSMutationColumnType Checks that newCol's type is compatible with mutationColType already present in mutation.
func checkTSMutationColumnType(
	tableDesc *MutableTableDescriptor, newCol *sqlbase.ColumnDescriptor,
) error {
	tagOrColumn := sqlbase.ColumnType_TYPE_DATA
	if newCol.IsTagCol() {
		tagOrColumn = sqlbase.ColumnType_TYPE_TAG
	}
	for i := range tableDesc.Mutations {
		mutation := tableDesc.Mutations[i]
		if muCol := mutation.GetColumn(); muCol != nil {
			if muCol.ID == newCol.ID && mutation.Direction == sqlbase.DescriptorMutation_NONE {
				if muCol.Type.Identical(&newCol.Type) {
					continue
				}
				_, err := validateAlterTSType(muCol.Name, &muCol.Type, &newCol.Type, tagOrColumn)
				if err != nil {
					return errors.Wrap(err, "blocked by existing ALTER TYPE: ")
				}
			}
		}
	}
	return nil
}

/*
func (p *planner) setAuditMode(
	ctx context.Context, desc *sqlbase.TableDescriptor, auditMode tree.AuditMode,
) (bool, error) {
	// An auditing config change is itself auditable!
	// We record the event even if the permission check below fails:
	// auditing wants to know who tried to change the settings.
	p.curPlan.auditEvents = append(p.curPlan.auditEvents,
		auditEvent{desc: desc, writing: true})

	// We require root for now. Later maybe use a different permission?
	if err := p.RequireAdminRole(ctx, "change auditing settings on a table"); err != nil {
		return false, err
	}

	telemetry.Inc(sqltelemetry.SchemaSetAuditModeCounter(auditMode.TelemetryName()))

	return desc.SetAuditMode(auditMode)
}*/

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableNode) Close(context.Context)        {}

// addIndexMutationWithSpecificPrimaryKey adds an index mutation into the given table descriptor, but sets up
// the index with ExtraColumnIDs from the given index, rather than the table's primary key.
func addIndexMutationWithSpecificPrimaryKey(
	table *sqlbase.MutableTableDescriptor,
	toAdd *sqlbase.IndexDescriptor,
	primary *sqlbase.IndexDescriptor,
) error {
	// Reset the ID so that a call to AllocateIDs will set up the index.
	toAdd.ID = 0
	if err := table.AddIndexMutation(toAdd, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := table.AllocateIDs(); err != nil {
		return err
	}
	// Use the columns in the given primary index to construct this indexes ExtraColumnIDs list.
	toAdd.ExtraColumnIDs = nil
	for _, colID := range primary.ColumnIDs {
		if !toAdd.ContainsColumnID(colID) {
			toAdd.ExtraColumnIDs = append(toAdd.ExtraColumnIDs, colID)
		}
	}
	return nil
}

// applyColumnMutation applies the mutation specified in `mut` to the given
// columnDescriptor, and saves the containing table descriptor. If the column's
// dependencies on sequences change, it updates them as well.
func applyColumnMutation(
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	mut tree.ColumnMutationCmd,
	params runParams,
) (bool, error) {
	isOnlyMetaChanged := false
	switch t := mut.(type) {
	case *tree.AlterTableAlterColumnType:
		typ := t.ToType
		// can not alter column type if any table or view depends on this column
		for _, tableRef := range tableDesc.DependedOnBy {
			for _, colID := range tableRef.ColumnIDs {
				if colID == col.ID {
					return false, pgerror.New(
						pgcode.DependentObjectsStillExist,
						"can not alter column type because there are objects that depend on it",
					)
				}
			}
		}
		if tableDesc.IsTSTable() {
			log.Infof(params.ctx, "alter ts table %s 1st txn start, id: %d, content: %s", tableDesc.Name, tableDesc.ID, mut)
			if t.Using != nil || t.Collation != "" {
				return false, pgerror.New(pgcode.Syntax, "column and tag in timeseries table does not support USING or COLLATION")
			}
			if tableDesc.TableType == tree.InstanceTable {
				return false, pgerror.Newf(
					pgcode.WrongObjectType, "can not alter tag type on instance table \"%s\"", tableDesc.Name)
			}
			if col.IsTagCol() {
				return false, pgerror.Newf(pgcode.WrongObjectType, "%s is a tag", col.Name)
			}
			if col.ID == 1 {
				return false, pgerror.Newf(pgcode.InvalidColumnDefinition, "cannot alter the first ts column %s", col.Name)
			}

			newType := prepareAlterType(t.ToType)
			if col.Type.Identical(newType) {
				return false, nil
			}
			// type cast validation
			// if converting timestamp to timestamptz or reverse, we will not send this to AE.
			isStorageDoNothing, err := validateAlterTSType(col.Name, &col.Type, newType, sqlbase.ColumnType_TYPE_DATA)
			if err != nil {
				return false, err
			}
			if isStorageDoNothing {
				isOnlyMetaChanged = true
			}

			if !isOnlyMetaChanged {
				//var alteringTag sqlbase.ColumnDescriptor
				alteringCol, _, err := sqlbase.MakeTSColumnDefDescs(col.Name, newType, col.Nullable, false, sqlbase.ColumnType_TYPE_DATA, nil, &params.p.semaCtx)
				if err != nil {
					return false, err
				}
				alteringCol.ID = col.ID
				// Check that newType is compatible with mutationColType already present in mutation.
				if err = checkTSMutationColumnType(tableDesc, alteringCol); err != nil {
					return false, err
				}
				if col.HasDefault() {
					err := castDefaultFuncExpr(*col.DefaultExpr, alteringCol, params, typ)
					if err != nil {
						return false, err
					}
				}
				// TODO(ZXY): Temporary use DescriptorMutation_NONE for alter type mutation
				tableDesc.AddColumnMutation(alteringCol, sqlbase.DescriptorMutation_NONE)
				mutationID := tableDesc.ClusterVersion.NextMutationID
				//tableDesc.State = sqlbase.TableDescriptor_ALTER
				// Create a Job to perform the second stage of ts DDL.
				syncDetail := jobspb.SyncMetaCacheDetails{
					Type:         alterKwdbAlterColumnType,
					SNTable:      tableDesc.TableDescriptor,
					AlterTag:     *alteringCol,
					OriginColumn: *col,
					MutationID:   mutationID,
				}
				jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, params.p.stmt.SQL, params.p.txn)
				if err != nil {
					return false, err
				}
				if mutationID != sqlbase.InvalidMutationID {
					tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
						MutationID: mutationID, JobID: jobID})
				}
				if err = params.p.writeTableDesc(params.ctx, tableDesc); err != nil {
					return false, err
				}
				// Actively commit a transaction, and read/write system table operations
				// need to be performed before this.
				if err = params.p.txn.Commit(params.ctx); err != nil {
					return false, err
				}

				// After the transaction commits successfully, execute the Job and wait for it to complete.
				if err = params.p.ExecCfg().JobRegistry.Run(
					params.ctx,
					params.p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
					[]int64{jobID},
				); err != nil {
					log.Info(params.ctx, err)
					return false, nil
				}
				log.Infof(params.ctx, "alter ts table %s 1st txn finished, id: %d, content: %s", tableDesc.Name, tableDesc.ID, mut)
				return false, nil
			}
		}

		version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
		if supported, err := isTypeSupportedInVersion(version, typ); err != nil {
			return false, err
		} else if !supported {
			return false, pgerror.Newf(
				pgcode.FeatureNotSupported,
				"type %s is not supported until version upgrade is finalized",
				typ.SQLString(),
			)
		}
		// Special handling for STRING COLLATE xy to verify that we recognize the language.
		if t.Collation != "" {
			if types.IsStringType(typ) {
				typ = types.MakeCollatedString(typ, t.Collation)
			} else {
				return false, pgerror.New(pgcode.Syntax, "COLLATE can only be used with string types")
			}
		}

		typ = sqlbase.UpdateTimeColPrecision(typ, tableDesc.TableType)
		err := sqlbase.ValidateColumnDefType(typ, tableDesc.TableType)
		if err != nil {
			return false, err
		}

		// No-op if the types are Identical.  We don't use Equivalent here because
		// the user may be trying to change the type of the column without changing
		// the type family.
		if col.Type.Identical(typ) {
			return false, nil
		}

		kind, err := schemachange.ClassifyConversion(&col.Type, typ)
		if err != nil {
			return false, err
		}

		switch kind {
		case schemachange.ColumnConversionDangerous, schemachange.ColumnConversionImpossible:
			// We're not going to make it impossible for the user to perform
			// this conversion, but we do want them to explicit about
			// what they're going for.
			return false, pgerror.Newf(pgcode.CannotCoerce,
				"the requested type conversion (%s -> %s) requires an explicit USING expression",
				col.Type.SQLString(), typ.SQLString())
		case schemachange.ColumnConversionTrivial:
			col.Type = *typ
			if col.HasDefault() {
				err := castDefaultFuncExpr(*col.DefaultExpr, col, params, typ)
				if err != nil {
					return false, err
				}
			}
		case schemachange.ColumnConversionGeneral:
			return false, unimplemented.NewWithIssueDetailf(
				9851,
				fmt.Sprintf("%s->%s", col.Type.SQLString(), typ.SQLString()),
				"type conversion from %s to %s requires overwriting existing values which is not yet implemented",
				col.Type.SQLString(),
				typ.SQLString(),
			)
		default:
			return false, unimplemented.NewWithIssueDetail(9851,
				fmt.Sprintf("%s->%s", col.Type.SQLString(), typ.SQLString()),
				"type conversion not yet implemented")
		}

	case *tree.AlterTableSetDefault:
		isTSTable := tableDesc.IsTSTable()
		if len(col.UsesSequenceIds) > 0 {
			if err := params.p.removeSequenceDependencies(params.ctx, tableDesc, col); err != nil {
				return false, err
			}
		}
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			isFuncDefault := false
			colDatumType := &col.Type
			expr, err := sqlbase.SanitizeVarFreeExpr(
				t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, true /* allowImpure */, isTSTable, col.Name,
			)
			if err != nil {
				return false, err
			}
			if expr == tree.DNull && !col.Nullable {
				return false, pgerror.Newf(pgcode.InvalidColumnDefinition,
					"default value NULL violates NOT NULL constraint on column %s", col.Name)
			}
			if isTSTable {
				if funcExpr, ok := expr.(*tree.FuncExpr); ok {
					switch col.Type.Oid() {
					case oid.T_timestamptz, oid.T_timestamp:
						if strings.ToLower(funcExpr.Func.String()) != "now" {
							return false, pgerror.Newf(pgcode.InvalidColumnDefinition,
								"column %s with type %s can only be set now() as default function", col.Name, col.Type.SQLString())
						}
					default:
						return false, pgerror.Newf(pgcode.FeatureNotSupported,
							"column with type %s can only be set constant as default value", col.Type.SQLString())
					}
				}
			} else {
				if _, ok := expr.(*tree.FuncExpr); ok {
					isFuncDefault = true
				}
			}
			if expr != tree.DNull {
				if isFuncDefault {
					t.Default = expr
					s := tree.Serialize(t.Default)
					col.DefaultExpr = &s
				} else {
					s := tree.Serialize(t.Default)
					col.DefaultExpr = &s
				}
			} else {
				col.DefaultExpr = nil
			}

			// Add references to the sequence descriptors this column is now using.
			changedSeqDescs, err := maybeAddSequenceDependencies(
				params.ctx, params.p, tableDesc, col, expr, nil, /* backrefs */
			)
			if err != nil {
				return false, err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				// TODO (lucy): Have more consistent/informative names for dependent jobs.
				if err := params.p.writeSchemaChange(
					params.ctx, changedSeqDesc, sqlbase.InvalidMutationID, "updating dependent sequence",
				); err != nil {
					return false, err
				}
			}
		}

	case *tree.AlterTableSetNotNull:
		if !col.Nullable {
			return false, nil
		}
		// See if there's already a mutation to add a not null constraint
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.ID {
				if tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_ADD {
					return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added")
				}
				return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped, try again later")
			}
		}

		info, err := tableDesc.GetConstraintInfo(params.ctx, nil)
		if err != nil {
			return false, err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		check := sqlbase.MakeNotNullCheckConstraint(col.Name, col.ID, inuseNames, sqlbase.ConstraintValidity_Validating)
		tableDesc.AddNotNullMutation(check, sqlbase.DescriptorMutation_ADD)

	case *tree.AlterTableDropNotNull:
		if col.Nullable {
			return false, nil
		}

		// Prevent a column in a primary key from becoming non-null.
		if tableDesc.PrimaryIndex.ContainsColumnID(col.ID) {
			return false, pgerror.Newf(pgcode.InvalidTableDefinition,
				`column "%s" is in a primary index`, col.Name)
		}

		// See if there's already a mutation to add/drop a not null constraint.
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.ID {
				if tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_ADD {
					return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added, try again later")
				}
				return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped")
			}
		}
		info, err := tableDesc.GetConstraintInfo(params.ctx, nil)
		if err != nil {
			return false, err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		col.Nullable = true

		// Add a check constraint equivalent to the non-null constraint and drop
		// it in the schema changer.
		check := sqlbase.MakeNotNullCheckConstraint(col.Name, col.ID, inuseNames, sqlbase.ConstraintValidity_Dropping)
		tableDesc.Checks = append(tableDesc.Checks, check)
		tableDesc.AddNotNullMutation(check, sqlbase.DescriptorMutation_DROP)

	case *tree.AlterTableDropStored:
		if !col.IsComputed() {
			return false, pgerror.Newf(pgcode.InvalidColumnDefinition,
				"column %q is not a computed column", col.Name)
		}
		col.ComputeExpr = nil
	}
	return isOnlyMetaChanged, nil
}

func castDefaultFuncExpr(
	originDefault string, col *sqlbase.ColumnDescriptor, params runParams, typ *types.T,
) error {
	parsedExpr, err := parser.ParseExpr(originDefault)
	if err != nil {
		return err
	}
	if tmp, ok := parsedExpr.(*tree.AnnotateTypeExpr); ok {
		if innerExpr, isFunc := tmp.TypedInnerExpr().(*tree.FuncExpr); isFunc {
			newDefaultExpr, err := innerExpr.TypeCheck(&params.p.semaCtx, typ)
			if err != nil {
				return err
			}
			s := tree.Serialize(newDefaultExpr)
			col.DefaultExpr = &s
		}
	}
	return nil
}

func labeledRowValues(cols []sqlbase.ColumnDescriptor, values tree.Datums) string {
	var s bytes.Buffer
	for i := range cols {
		if i != 0 {
			s.WriteString(`, `)
		}
		s.WriteString(cols[i].Name)
		s.WriteString(`=`)
		s.WriteString(values[i].String())
	}
	return s.String()
}

// injectTableStats implements the INJECT STATISTICS command, which deletes any
// existing statistics on the table and replaces them with the statistics in the
// given json object (in the same format as the result of SHOW STATISTICS USING
// JSON). This is useful for reproducing planning issues without importing the
// data.
func injectTableStats(
	params runParams, desc *sqlbase.TableDescriptor, statsExpr tree.TypedExpr,
) error {
	val, err := statsExpr.Eval(params.EvalContext())
	if err != nil {
		return err
	}
	if val == tree.DNull {
		return pgerror.New(pgcode.Syntax,
			"statistics cannot be NULL")
	}
	jsonStr := val.(*tree.DJSON).JSON.String()
	var jsonStats []stats.JSONStatistic
	if err := gojson.Unmarshal([]byte(jsonStr), &jsonStats); err != nil {
		return err
	}

	// First, delete all statistics for the table.
	if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"delete-stats",
		params.EvalContext().Txn,
		`DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.ID,
	); err != nil {
		return errors.Wrapf(err, "failed to delete old stats")
	}

	// Insert each statistic.
	for i := range jsonStats {
		s := &jsonStats[i]
		var h *stats.HistogramData
		var err error
		h, err = s.GetHistogram(params.EvalContext())
		if err != nil {
			return err
		}
		if len(s.SortHistogramBuckets) > 0 {
			h, err = s.GetSortHistogram(params.EvalContext())
			if err != nil {
				return err
			}
		}
		// histogram will be passed to the INSERT statement; we want it to be a
		// nil interface{} if we don't generate a histogram.
		var histogram interface{}
		if h != nil {
			histogram, err = protoutil.Marshal(h)
			if err != nil {
				return err
			}
		}

		columnIDs := tree.NewDArray(types.Int)
		for _, colName := range s.Columns {
			colDesc, _, err := desc.FindColumnByName(tree.Name(colName))
			if err != nil {
				return err
			}
			if err := columnIDs.Append(tree.NewDInt(tree.DInt(colDesc.ID))); err != nil {
				return err
			}
		}
		var name interface{}
		if s.Name != "" {
			name = s.Name
		}
		if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"insert-stats",
			params.EvalContext().Txn,
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			desc.ID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			histogram,
		); err != nil {
			return errors.Wrapf(err, "failed to insert stats")
		}
	}

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (whereas the gossip
	// update is handled asynchronously).
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.ID)

	// Use Gossip to refresh the caches on other nodes.
	return stats.GossipTableStatAdded(params.extendedEvalCtx.ExecCfg.Gossip, desc.ID)
}

func (p *planner) removeColumnComment(
	ctx context.Context, txn *kv.Txn, tableID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-column-comment",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ColumnCommentType,
		tableID,
		columnID)

	return err
}

// updateFKBackReferenceName updates the name of a foreign key reference on
// the referenced table descriptor.
// TODO (lucy): This method is meant to be analogous to removeFKBackReference,
// in that it only updates the backreference, but we should refactor/unify all
// the places where we update both FKs and their backreferences, so that callers
// don't have to manually take care of updating both table descriptors.
func (p *planner) updateFKBackReferenceName(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	ref *sqlbase.ForeignKeyConstraint,
	newName string,
) error {
	var referencedTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ref.ReferencedTableID, err)
		}
		referencedTableDesc = lookup
	}
	if referencedTableDesc.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	for i := range referencedTableDesc.InboundFKs {
		backref := &referencedTableDesc.InboundFKs[i]
		if backref.Name == ref.Name && backref.OriginTableID == tableDesc.ID {
			backref.Name = newName
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			return p.writeSchemaChange(
				ctx, referencedTableDesc, sqlbase.InvalidMutationID, "updating referenced table",
			)
		}
	}
	return errors.Errorf("missing backreference for foreign key %s", ref.Name)
}

// validateAlterTSType validates ALTER COLUMN/TAG TYPE command.
// Parameters:
// - oldType types.T: Old column type
// - newType types.T: New column type
// - colType: data column or tag column
//
// Returns:
// - doNothing bool: Return true if the storage structure does not need to be modified
// - error: type cannot be converted error msg
func validateAlterTSType(
	colName string, oldType, newType *types.T, colType sqlbase.ColumnType,
) (bool, error) {
	switch oldType.Oid() {
	// Numeric type
	case oid.T_int2:
		return false, validateNumType(colName, oldType, newType, []oid.Oid{oid.T_int4, oid.T_int8, oid.T_varchar})
	case oid.T_int4:
		return false, validateNumType(colName, oldType, newType, []oid.Oid{oid.T_int8, oid.T_varchar})
	case oid.T_int8, oid.T_float8:
		return false, validateNumType(colName, oldType, newType, []oid.Oid{oid.T_varchar})
	case oid.T_float4:
		return false, validateNumType(colName, oldType, newType, []oid.Oid{oid.T_float8, oid.T_varchar})
		// Character type
	case oid.T_bpchar, oid.T_varchar, types.T_nchar, types.T_nvarchar:
		return false, validateTextType(colName, oldType, newType, colType)
		// timestamp type
	case oid.T_timestamp, oid.T_timestamptz:
		newType = sqlbase.UpdateTimeColPrecision(newType, tree.TimeseriesTable)
		if newType.Precision() != oldType.Precision() {
			return false, newTypeConvertError(colName, oldType, newType)
		}
		if newType.Oid() == oid.T_timestamp || newType.Oid() == oid.T_timestamptz {
			return true, nil
		}
		return false, newTypeConvertError(colName, oldType, newType)
	default:
		return false, newTypeConvertError(colName, oldType, newType)
	}
}

// validateNumType validates ALTER COLUMN/TAG TYPE command for numeric types.
func validateNumType(colName string, oldType, newType *types.T, validTypes []oid.Oid) error {
	for _, validType := range validTypes {
		if newType.Oid() == validType {
			if newType.Oid() == oid.T_varchar {
				if newType.Width() > MaxVariableLen {
					return newTypeExceedError(colName, newType, MaxVariableLen)
				}
				switch oldType.Oid() {
				case oid.T_int2:
					if newType.Width() < MaxInt16StrLen {
						return pgerror.Newf(
							pgcode.InvalidColumnDefinition,
							"column \"%s\": when %s is converted to VARCHAR, the minimum length is %d",
							colName, oldType.SQLString(), MaxInt16StrLen)
					}
				case oid.T_int4:
					if newType.Width() < MaxInt32StrLen {
						return pgerror.Newf(
							pgcode.InvalidColumnDefinition,
							"column \"%s\": when %s is converted to VARCHAR, the minimum length is %d",
							colName, oldType.SQLString(), MaxInt32StrLen)
					}
				case oid.T_int8:
					if newType.Width() < MaxInt64StrLen {
						return pgerror.Newf(
							pgcode.InvalidColumnDefinition,
							"column \"%s\": when %s is converted to VARCHAR, the minimum length is %d",
							colName, oldType.SQLString(), MaxInt64StrLen)
					}
				case oid.T_float4, oid.T_float8:
					if newType.Width() < MaxDoubleStrLen {
						return pgerror.Newf(
							pgcode.InvalidColumnDefinition,
							"column \"%s\": when %s is converted to VARCHAR, the minimum length is %d",
							colName, oldType.SQLString(), MaxDoubleStrLen)
					}
				}
			}
			return nil
		}
	}
	return newTypeConvertError(colName, oldType, newType)
}

// validateTextType validates ALTER COLUMN/TAG TYPE command for text types.
func validateTextType(colName string, oldType, newType *types.T, colType sqlbase.ColumnType) error {
	if oldType.Oid() == oid.T_varchar {
		switch newType.Oid() {
		case oid.T_int2, oid.T_int4, oid.T_int8, oid.T_float4, oid.T_float8:
			return nil
		}
	}
	switch newType.Oid() {
	case oid.T_bpchar:
		if newType.Width() >= MaxFixedLen {
			return newTypeExceedError(colName, newType, MaxFixedLen)
		}
		if oldType.Oid() == oid.T_bpchar || oldType.Oid() == oid.T_varchar {
			if newType.Width() < oldType.Width() {
				return newTypeConvertError(colName, oldType, newType)
			}
		} else if oldType.Oid() == types.T_nchar || oldType.Oid() == types.T_nvarchar {
			if newType.Width() < oldType.Width()*4 {
				return newTypeConvertError(colName, oldType, newType)
			}
		}

	case oid.T_varchar:
		if newType.Width() > MaxVariableLen {
			return newTypeExceedError(colName, newType, MaxVariableLen)
		}
		if oldType.Oid() == oid.T_bpchar || oldType.Oid() == oid.T_varchar {
			if newType.Width() < oldType.Width() {
				return newTypeConvertError(colName, oldType, newType)
			}
		} else if oldType.Oid() == types.T_nchar || oldType.Oid() == types.T_nvarchar {
			if newType.Width() < oldType.Width()*4 {
				return newTypeConvertError(colName, oldType, newType)
			}
		}

	case types.T_nchar:
		if newType.Width() > MaxNCharLen {
			return newTypeExceedError(colName, newType, MaxNCharLen)
		}
		if oldType.Oid() == oid.T_bpchar || oldType.Oid() == oid.T_varchar {
			if float64(newType.Width()) < float64(oldType.Width())/4 {
				return newTypeConvertError(colName, oldType, newType)
			}
		} else if oldType.Oid() == types.T_nchar || oldType.Oid() == types.T_nvarchar {
			if newType.Width() < oldType.Width() {
				return newTypeConvertError(colName, oldType, newType)
			}
		}
	case types.T_nvarchar:
		if colType == sqlbase.ColumnType_TYPE_TAG {
			return pgerror.Newf(pgcode.InvalidColumnDefinition, "tag %s does not support type %s", colName, newType.SQLString())
		}
		if newType.Width() > MaxVariableLen {
			return newTypeExceedError(colName, newType, MaxVariableLen)
		}
		if oldType.Oid() == oid.T_bpchar || oldType.Oid() == oid.T_varchar {
			if float64(newType.Width()) < float64(oldType.Width())/4 {
				return newTypeConvertError(colName, oldType, newType)
			}
		} else if oldType.Oid() == types.T_nchar || oldType.Oid() == types.T_nvarchar {
			if newType.Width() < oldType.Width() {
				return newTypeConvertError(colName, oldType, newType)
			}
		}
	default:
		return newTypeConvertError(colName, oldType, newType)
	}
	return nil
}

// newTypeConvertError returns an error when old type cannot convert to new type.
func newTypeConvertError(colName string, oldType, newType *types.T) error {
	return pgerror.Newf(pgcode.CannotCoerce, "column: \"%s\": cannot convert %s to %s", colName, oldType.SQLString(), newType.SQLString())
}

// newTypeExceedError returns an error when new type exceeds the maximum width.
func newTypeExceedError(colName string, newType *types.T, maxWidth int) error {
	return pgerror.Newf(pgcode.CannotCoerce, "column: \"%s\": %s exceeds the maximum width: %d", colName, newType.SQLString(), maxWidth)
}

// prepareAlterType assigns the correct width for varchar and nvarchar.
func prepareAlterType(t *types.T) *types.T {
	switch t.Oid() {
	case oid.T_varchar:
		if t.InternalType.Width == 0 {
			return types.MakeVarChar(DefaultVariableLEN, t.TypeEngine())
		}
	case types.T_nvarchar:
		if t.InternalType.Width == 0 {
			return types.MakeNVarChar(DefaultNVarcharLen)
		}
	default:
	}
	return t
}

// checkIndexOnTag checks whether the index exists on the tag.
func checkIndexOnTag(n *alterTableNode, colToDrop *sqlbase.ColumnDescriptor) error {
	for _, idx := range n.tableDesc.AllNonDropIndexes() {

		// containsThisColumn becomes true if the index is defined
		// over the column being dropped.
		containsThisColumn := false

		// Analyze the index.
		for _, id := range idx.ColumnIDs {
			if id == colToDrop.ID {
				containsThisColumn = true
			}
		}

		if containsThisColumn {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"column %q is referenced by existing index %q", colToDrop.Name, idx.Name)
		}
	}
	return nil
}
