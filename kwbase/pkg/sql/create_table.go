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
	"bytes"
	"context"
	"fmt"
	"go/constant"
	"sort"
	"strconv"
	"strings"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Max length of fixed and indefinite length type.
const (
	MaxFixedLen                   = 1024
	MaxVariableLen                = 64 * 1024
	MaxNCharLen                   = 254
	DefaultTypeWithLength         = 0
	DefaultFixedLen               = 1
	DefaultVariableLEN            = 254
	MaxPrimaryTagWidth            = 128
	DefaultPrimaryTagVarcharWidth = 64
	MaxTSDataColumns              = 4096 // the maximum number of data columns for ts table.
	MaxVariableTupleLen           = 255  // the max length of variable-length type in tuple mode
)

type createTableNode struct {
	n          *tree.CreateTable
	dbDesc     *sqlbase.DatabaseDescriptor
	sourcePlan planNode

	run createTableRun
}

// createTableRun contains the run-time state of createTableNode
// during local execution.
type createTableRun struct {
	autoCommit autoCommitOpt

	// synthRowID indicates whether an input column needs to be synthesized to
	// provide the default value for the hidden rowid column. The optimizer's plan
	// already includes this column if a user specified PK does not exist (so
	// synthRowID is false), whereas the heuristic planner's plan does not in this
	// case (so synthRowID is true).
	synthRowID bool

	// fromHeuristicPlanner indicates whether the planning was performed by the
	// heuristic planner instead of the optimizer. This is used to determine
	// whether or not a row_id was synthesized as part of the planning stage, if a
	// user defined PK is not specified.
	fromHeuristicPlanner bool
}

type createMultiInstTableNode struct {
	optColumnsSlot
	ns         []*tree.CreateTable
	dbDescs    map[string]*sqlbase.DatabaseDescriptor
	run        createTableRun
	sourcePlan planNode
	res        [3]int // created, failed, skipped
}

// createMultiInstTableNode contains the logic to create multiple instance tables
func (ct *createMultiInstTableNode) startExec(params runParams) error {
	logAndNotice := func(tblName string, err error) {
		msg := fmt.Sprintf("create table %s failed: %s", tblName, err.Error())
		log.Error(params.ctx, msg)
	}

	for i, instTbl := range ct.ns {
		if i != 0 {
			params.resetNewTxn()
		}
		schKey := instTbl.Table.Catalog() + "_" + instTbl.Table.Schema()
		dbDesc, exists := ct.dbDescs[schKey]
		var err error
		if !exists {
			err = errors.Newf("unresolved table name prefix '%s'.", instTbl.Table.TableNamePrefix.String())
			logAndNotice(instTbl.Table.FQString(), err)
			ct.res[1]++
			continue
		}
		if dbDesc.EngineType != tree.EngineTypeTimeseries {
			err = errors.Newf("can not create timeseries table '%s' in relational database.", instTbl.Table.Table())
			logAndNotice(instTbl.Table.FQString(), err)
			ct.res[1]++
			continue
		}

		// check whether there exists instance table with identical name
		err = checkChildTable(params.ctx, params.p.txn, dbDesc.Name, instTbl.Table)
		if err != nil {
			logAndNotice(instTbl.Table.FQString(), err)
			ct.res[2]++
			params.p.txn.CleanupOnError(params.ctx, err)
			continue
		}

		if err := createInstanceTable(params, instTbl, dbDesc); err != nil {
			logAndNotice(instTbl.Table.FQString(), err)
			if strings.HasSuffix(err.Error(), "already exists") {
				ct.res[2]++
			} else {
				ct.res[1]++
			}
			params.p.txn.CleanupOnError(params.ctx, err)

		} else {
			ct.res[0]++
		}
	}
	return nil
}

func (ct *createMultiInstTableNode) Next(runParams) (bool, error) { return false, nil }

func (ct *createMultiInstTableNode) Values() tree.Datums { return tree.Datums{} }

func (ct *createMultiInstTableNode) Close(ctx context.Context) {
	if ct.sourcePlan != nil {
		ct.sourcePlan.Close(ctx)
		ct.sourcePlan = nil
	}
}

func (ct *createMultiInstTableNode) ReadingOwnWrites() {}

// storageParamType indicates the required type of a storage parameter.
type storageParamType int

// storageParamType values
const (
	storageParamBool storageParamType = iota
	storageParamInt
	storageParamFloat
	storageParamUnimplemented
)

// MaxTSTableNameLength represents the maximum length of timeseries table name.
const MaxTSTableNameLength = 128

// MaxTagNameLength represents the maximum length of tag name.
const MaxTagNameLength = 128

var storageParamExpectedTypes = map[string]storageParamType{
	`fillfactor`:                                  storageParamInt,
	`toast_tuple_target`:                          storageParamUnimplemented,
	`parallel_workers`:                            storageParamUnimplemented,
	`autovacuum_enabled`:                          storageParamUnimplemented,
	`toast.autovacuum_enabled`:                    storageParamUnimplemented,
	`autovacuum_vacuum_threshold`:                 storageParamUnimplemented,
	`toast.autovacuum_vacuum_threshold`:           storageParamUnimplemented,
	`autovacuum_vacuum_scale_factor`:              storageParamUnimplemented,
	`toast.autovacuum_vacuum_scale_factor`:        storageParamUnimplemented,
	`autovacuum_analyze_threshold`:                storageParamUnimplemented,
	`autovacuum_analyze_scale_factor`:             storageParamUnimplemented,
	`autovacuum_vacuum_cost_delay`:                storageParamUnimplemented,
	`toast.autovacuum_vacuum_cost_delay`:          storageParamUnimplemented,
	`autovacuum_vacuum_cost_limit`:                storageParamUnimplemented,
	`autovacuum_freeze_min_age`:                   storageParamUnimplemented,
	`toast.autovacuum_freeze_min_age`:             storageParamUnimplemented,
	`autovacuum_freeze_max_age`:                   storageParamUnimplemented,
	`toast.autovacuum_freeze_max_age`:             storageParamUnimplemented,
	`autovacuum_freeze_table_age`:                 storageParamUnimplemented,
	`toast.autovacuum_freeze_table_age`:           storageParamUnimplemented,
	`autovacuum_multixact_freeze_min_age`:         storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_min_age`:   storageParamUnimplemented,
	`autovacuum_multixact_freeze_max_age`:         storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_max_age`:   storageParamUnimplemented,
	`autovacuum_multixact_freeze_table_age`:       storageParamUnimplemented,
	`toast.autovacuum_multixact_freeze_table_age`: storageParamUnimplemented,
	`log_autovacuum_min_duration`:                 storageParamUnimplemented,
	`toast.log_autovacuum_min_duration`:           storageParamUnimplemented,
	`user_catalog_table`:                          storageParamUnimplemented,
}

// minimumTypeUsageVersions defines the minimum version needed for a new
// data type.
var minimumTypeUsageVersions = map[types.Family]clusterversion.VersionKey{
	types.TimeTZFamily: clusterversion.VersionTimeTZType,
}

// isTypeSupportedInVersion returns whether a given type is supported in the given version.
func isTypeSupportedInVersion(v clusterversion.ClusterVersion, t *types.T) (bool, error) {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}

	switch t.Family() {
	case types.TimeFamily, types.TimestampFamily, types.TimestampTZFamily, types.TimeTZFamily:
		if t.Precision() != 6 && !v.IsActive(clusterversion.VersionTimePrecision) {
			return false, nil
		}
	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return false, err
		}
		if (t.Precision() != 6 || itm.DurationField != types.IntervalDurationField{}) &&
			!v.IsActive(clusterversion.VersionTimePrecision) {
			return false, nil
		}
	}
	minVersion, ok := minimumTypeUsageVersions[t.Family()]
	if !ok {
		return true, nil
	}
	return v.IsActive(minVersion), nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createTableNode) ReadingOwnWrites() {}

// getTableCreateParams returns the table key needed for the new table,
// as well as the schema id.
func getTableCreateParams(
	params runParams, dbID sqlbase.ID, isTemporary bool, tableName tree.TableName,
) (tKey sqlbase.DescriptorKey, schemaID sqlbase.ID, err error) {
	if isTemporary {
		if !params.SessionData().TempTablesEnabled {
			return nil, 0, errors.WithTelemetry(
				pgerror.WithCandidateCode(
					errors.WithHint(
						errors.WithIssueLink(
							errors.Newf("temporary tables are only supported experimentally"),
							errors.IssueLink{IssueURL: unimplemented.MakeURL(46260)},
						),
						"You can enable temporary tables by running `SET experimental_enable_temp_tables = 'on'`.",
					),
					pgcode.FeatureNotSupported,
				),
				"sql.schema.temp_tables_disabled",
			)
		}

		var err error
		schemaID, err = params.p.getOrCreateTemporarySchema(params.ctx, dbID)
		if err != nil {
			return nil, 0, err
		}
		tKey = sqlbase.NewTableKey(dbID, schemaID, tableName.Table())
	} else {
		if IsVirtualSchemaName(tableName.Schema()) {
			return nil, sqlbase.InvalidID, pgerror.Newf(pgcode.InvalidName,
				"schema cannot be modified: %q", tree.ErrString(&tableName.SchemaName))
		}
		if strings.HasPrefix(tableName.Schema(), sessiondata.PgTempSchemaName) {
			return nil, sqlbase.InvalidID, errors.AssertionFailedf("invalid schema %s for CreateTable", tableName.Schema())
		}
		// Otherwise, find the ID of the schema to create the table within.
		var err error
		var found bool
		found, schemaID, err = params.p.Tables().resolveSchemaID(params.ctx, params.p.Txn(), dbID, tableName.Schema())
		if err != nil {
			return nil, sqlbase.InvalidID, err
		}
		if !found {
			return nil, sqlbase.InvalidID, sqlbase.NewUndefinedSchemaError(tableName.Schema())
		}
		tKey = sqlbase.MakeObjectNameKey(params.ctx, params.ExecCfg().Settings, dbID, schemaID, tableName.Table())
	}

	// Check permissions on the schema.
	if err := params.p.canCreateOnSchema(params.ctx, tableName.Schema(), dbID, skipCheckPublicSchema); err != nil {
		return nil, 0, err
	}

	exists, _, err := sqlbase.LookupObjectID(params.ctx, params.p.txn, dbID, schemaID, tableName.Table())
	if err == nil && exists {
		return nil, sqlbase.InvalidID, sqlbase.NewRelationAlreadyExistsError(tableName.Table())
	} else if err != nil {
		return nil, 0, err
	}
	return tKey, schemaID, nil
}

// checkEngineType check if features in different engines are supported
func checkEngineType(n *createTableNode) error {
	if n.dbDesc.EngineType == tree.EngineTypeRelational && n.n.TableType == tree.RelationalTable {
		if n.n.DownSampling != nil || n.n.Sde {
			return pgerror.Newf(pgcode.WrongObjectType, "downsampling feature is not supported on relational table \"%s\"", n.n.Table.TableName)
		}
	}
	if n.dbDesc.EngineType == tree.EngineTypeRelational && n.n.TableType != tree.RelationalTable {
		return pgerror.Newf(pgcode.WrongObjectType, "can not create timeseries table in relational database \"%s\"", n.dbDesc.Name)
	}
	if n.dbDesc.EngineType == tree.EngineTypeTimeseries && n.n.TableType == tree.RelationalTable {
		return pgerror.Newf(pgcode.WrongObjectType, "can not create relational table in timeseries database \"%s\"", n.dbDesc.Name)
	}
	if n.dbDesc.EngineType == tree.EngineTypeTimeseries {
		if sqlbase.ContainsNonAlphaNumSymbol(n.n.Table.String()) {
			return sqlbase.NewTSNameInvalidError(n.n.Table.String())
		}
		if len(n.n.Table.Table()) > MaxTSTableNameLength {
			return sqlbase.NewTSNameOutOfLengthError("table", n.n.Table.Table(), MaxTSTableNameLength)
		}
	}
	return nil
}

// startExec exec create table node including make table desc, write table desc and exec create table job
func (n *createTableNode) startExec(params runParams) error {
	if err := checkEngineType(n); err != nil {
		return err
	}
	log.Infof(params.ctx, "create table %s 1st txn start, type: %s", n.n.Table.Table(), tree.TableTypeName(n.n.TableType))
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("table"))

	isTemporary := n.n.Temporary

	// check if there are child tables with identical name
	err := checkChildTable(params.ctx, params.p.txn, n.dbDesc.Name, n.n.Table)
	if err != nil {
		return err
	}

	tKey, schemaID, err := getTableCreateParams(params, n.dbDesc.ID, isTemporary, n.n.Table)
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}
	// create instance table.
	if n.n.TableType == tree.InstanceTable {
		return createInstanceTable(params, n.n, n.dbDesc)
	}

	if n.n.Interleave != nil {
		if n.n.IsTS() {
			return sqlbase.TSUnsupportedError("interleave")
		}
		telemetry.Inc(sqltelemetry.CreateInterleavedTableCounter)
	}
	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempTableCounter)

		// TODO(#46556): support ON COMMIT DROP and DELETE ROWS on TEMPORARY TABLE.
		// If we do this, the n.n.OnCommit variable should probably be stored on the
		// table descriptor.
		// Note UNSET / PRESERVE ROWS behave the same way so we do not need to do that for now.
		switch n.n.OnCommit {
		case tree.CreateTableOnCommitUnset, tree.CreateTableOnCommitPreserveRows:
		default:
			return errors.AssertionFailedf("ON COMMIT value %d is unrecognized", n.n.OnCommit)
		}
	} else if n.n.OnCommit != tree.CreateTableOnCommitUnset {
		return pgerror.New(
			pgcode.InvalidTableDefinition,
			"ON COMMIT can only be used on temporary tables",
		)
	}

	// Warn against creating non-partitioned indexes on a partitioned table,
	// which is undesirable in most cases.
	if n.n.PartitionBy != nil {
		if n.n.IsTS() {
			return sqlbase.TSUnsupportedError("partition")
		}
		for _, def := range n.n.Defs {
			if d, ok := def.(*tree.IndexTableDef); ok {
				if d.PartitionBy == nil {
					params.p.SendClientNotice(
						params.ctx,
						errors.WithHint(
							pgerror.Noticef("creating non-partitioned index on partitioned table may not be performant"),
							"Consider modifying the index such that it is also partitioned.",
						),
					)
				}
			}
		}
	}

	// generate ID for instance table
	childID, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	privs := n.dbDesc.GetPrivileges()
	if n.dbDesc.ID == keys.SystemDatabaseID {
		privs = sqlbase.NewDefaultPrivilegeDescriptor()
	}

	var asCols sqlbase.ResultColumns
	var desc sqlbase.MutableTableDescriptor
	var affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor
	creationTime, err := params.creationTimeForNewTableDescriptor()
	if err != nil {
		return err
	}
	if n.n.As() {
		asCols = planColumns(n.sourcePlan)
		if !n.run.fromHeuristicPlanner && !n.n.AsHasUserSpecifiedPrimaryKey() {
			// rowID column is already present in the input as the last column if it
			// was planned by the optimizer and the user did not specify a PRIMARY
			// KEY. So ignore it for the purpose of creating column metadata (because
			// makeTableDescIfAs does it automatically).
			asCols = asCols[:len(asCols)-1]
		}

		desc, err = makeTableDescIfAs(params,
			n.n, n.dbDesc.ID, schemaID, childID, creationTime, asCols, privs, params.p.EvalContext(), isTemporary)
		if err != nil {
			return err
		}

		// If we have an implicit txn we want to run CTAS async, and consequently
		// ensure it gets queued as a SchemaChange.
		if params.p.ExtendedEvalContext().TxnImplicit {
			desc.State = sqlbase.TableDescriptor_ADD
		}
	} else {
		affected = make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
		desc, err = makeTableDesc(params, n.n, n.dbDesc.ID, schemaID, childID, creationTime, privs, affected, isTemporary)
		if err != nil {
			return err
		}

		if desc.Adding() {
			// if this table and all its references are created in the same
			// transaction it can be made PUBLIC.
			refs, err := desc.FindAllReferences()
			if err != nil {
				return err
			}
			var foundExternalReference bool
			for id := range refs {
				if t := params.p.Tables().getUncommittedTableByID(id).MutableTableDescriptor; t == nil || !t.IsNewTable() {
					foundExternalReference = true
					break
				}
			}
			if !foundExternalReference {
				desc.State = sqlbase.TableDescriptor_PUBLIC
			}
		}
	}
	if desc.IsTSTable() {
		desc.State = sqlbase.TableDescriptor_ADD
		if desc.TsTable.Lifetime == InvalidLifetime {
			desc.TsTable.Lifetime = n.dbDesc.TsDb.Lifetime
		}
		if desc.TsTable.PartitionInterval == InvalidLifetime {
			desc.TsTable.PartitionInterval = n.dbDesc.TsDb.PartitionInterval
		}
	}

	// Descriptor written to store here.
	if err := params.p.createDescriptorWithID(
		params.ctx, tKey.Key(), childID, &desc, params.EvalContext().Settings,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	for _, updated := range affected {
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := params.p.writeSchemaChange(
			params.ctx, updated, sqlbase.InvalidMutationID, "updating referenced table",
		); err != nil {
			return err
		}
	}

	for _, index := range desc.AllNonDropIndexes() {
		if len(index.Interleave.Ancestors) > 0 {
			if err := params.p.finalizeInterleave(params.ctx, &desc, index); err != nil {
				return err
			}
		}
	}

	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	params.p.SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)

	// If we are in an explicit txn or the source has placeholders, we execute the
	// CTAS query synchronously.
	if n.n.As() && !params.p.ExtendedEvalContext().TxnImplicit {
		err = func() error {
			// The data fill portion of CREATE AS must operate on a read snapshot,
			// so that it doesn't end up observing its own writes.
			prevMode := params.p.Txn().ConfigureStepping(params.ctx, kv.SteppingEnabled)
			defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()

			// This is a very simplified version of the INSERT logic: no CHECK
			// expressions, no FK checks, no arbitrary insertion order, no
			// RETURNING, etc.

			// Instantiate a row inserter and table writer. It has a 1-1
			// mapping to the definitions in the descriptor.
			ri, err := row.MakeInserter(
				params.ctx,
				params.p.txn,
				sqlbase.NewImmutableTableDescriptor(*desc.TableDesc()),
				desc.Columns,
				row.SkipFKs,
				nil, /* fkTables */
				&params.p.alloc)
			if err != nil {
				return err
			}
			ti := tableInserterPool.Get().(*tableInserter)
			*ti = tableInserter{ri: ri}
			tw := tableWriter(ti)
			if n.run.autoCommit == autoCommitEnabled {
				tw.enableAutoCommit()
			}
			defer func() {
				tw.close(params.ctx)
				*ti = tableInserter{}
				tableInserterPool.Put(ti)
			}()
			if err := tw.init(params.ctx, params.p.txn, params.p.EvalContext()); err != nil {
				return err
			}

			// Prepare the buffer for row values. At this point, one more column has
			// been added by ensurePrimaryKey() to the list of columns in sourcePlan, if
			// a PRIMARY KEY is not specified by the user.
			rowBuffer := make(tree.Datums, len(desc.Columns))
			pkColIdx := len(desc.Columns) - 1

			// The optimizer includes the rowID expression as part of the input
			// expression. But the heuristic planner does not do this, so construct
			// a rowID expression to be evaluated separately.
			var defTypedExpr tree.TypedExpr
			if n.run.synthRowID {
				// Prepare the rowID expression.
				defExprSQL := *desc.Columns[pkColIdx].DefaultExpr
				defExpr, err := parser.ParseExpr(defExprSQL)
				if err != nil {
					return err
				}
				defTypedExpr, err = params.p.analyzeExpr(
					params.ctx,
					defExpr,
					nil, /*sources*/
					tree.IndexedVarHelper{},
					types.Any,
					false, /*requireType*/
					"CREATE TABLE AS")
				if err != nil {
					return err
				}
			}

			for {
				if err := params.p.cancelChecker.Check(); err != nil {
					return err
				}
				if next, err := n.sourcePlan.Next(params); !next {
					if err != nil {
						return err
					}
					_, err := tw.finalize(
						params.ctx, params.extendedEvalCtx.Tracing.KVTracingEnabled())
					if err != nil {
						return err
					}
					break
				}

				// Populate the buffer and generate the PK value.
				copy(rowBuffer, n.sourcePlan.Values())
				if n.run.synthRowID {
					rowBuffer[pkColIdx], err = defTypedExpr.Eval(params.p.EvalContext())
					if err != nil {
						return err
					}
				}

				if err := tw.row(params.ctx, rowBuffer, params.extendedEvalCtx.Tracing.KVTracingEnabled()); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	if desc.IsTSTable() {
		if err = createAndExecCreateTSTableJob(params, desc, n); err != nil {
			return err
		}
		var splitInfo []roachpb.AdminSplitInfoForTs
		if splitInfo, err = distributeAndDuplicateOfCreateTSTable(params, desc); err != nil {
			return err
		}
		if splitInfo != nil && params.ExecCfg().StartMode == StartSingleReplica {
			var wg sync.WaitGroup
			log.Infof(params.ctx, "will relocate for MPP mode, location: %+v", splitInfo)
			for i := range splitInfo {
				wg.Add(1)
				go func(info *roachpb.AdminSplitInfoForTs) {
					target := []roachpb.ReplicationTarget{{
						NodeID:  info.PreDist[0].NodeID,
						StoreID: info.PreDist[0].StoreID,
					}}
					if err = params.extendedEvalCtx.ExecCfg.DB.AdminRelocateRange(params.ctx, info.SplitKey, target); err != nil {
						log.Errorf(params.ctx, "failed relocate range for key %v, target %+v, err: %v", info.SplitKey, target, err)
					}
					wg.Done()
				}(&splitInfo[i])
			}
			wg.Wait()
			log.Infof(params.ctx, "done relocate for MPP mode")
		}

		if updateErr := params.p.ExecCfg().DB.Txn(params.ctx, func(ctx context.Context, txn *kv.Txn) error {
			desc.State = sqlbase.TableDescriptor_PUBLIC
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}
			b := txn.NewBatch()
			err := writeDescToBatch(ctx, params.p.extendedEvalCtx.Tracing.KVTracingEnabled(), params.p.execCfg.Settings, b, desc.GetID(), desc.TableDesc())
			if err != nil {
				return err
			}
			return txn.Run(params.ctx, b)
		}); updateErr != nil {
			return updateErr
		}
	}

	return nil
}

// createAndExecCreateTSTableJob creates and exec create time-series table job
func createAndExecCreateTSTableJob(
	params runParams, desc sqlbase.MutableTableDescriptor, n *createTableNode,
) error {
	// Create a Job to perform the second stage of ts DDL.
	syncDetail := jobspb.SyncMetaCacheDetails{
		Type:     createKwdbTsTable,
		SNTable:  desc.TableDescriptor,
		Database: *n.dbDesc,
	}
	_, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n.n, params.Ann()), params.p.txn)
	if err != nil {
		return errors.Wrap(err, "createSyncMetaCacheJob failed")
	}
	// Actively commit a transaction, and read/write system table operations
	// need to be performed before this.
	if err := params.p.txn.Commit(params.ctx); err != nil {
		return err
	}

	// After the transaction commits successfully, execute the Job and wait for it to complete.
	//if err = params.ExecCfg().JobRegistry.Run(
	//	params.ctx,
	//	params.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
	//	[]int64{jobID},
	//); err != nil {
	//	return errors.Wrap(err, "createSyncMetaCacheJob run failed")
	//}
	return nil
}

func (*createTableNode) Next(runParams) (bool, error) { return false, nil }
func (*createTableNode) Values() tree.Datums          { return tree.Datums{} }

func (n *createTableNode) Close(ctx context.Context) {
	if n.sourcePlan != nil {
		n.sourcePlan.Close(ctx)
		n.sourcePlan = nil
	}
}

// resolveFK on the planner calls resolveFK() on the current txn.
//
// The caller must make sure the planner is configured to look up
// descriptors without caching. See the comment on resolveFK().
func (p *planner) resolveFK(
	ctx context.Context,
	tbl *sqlbase.MutableTableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	ts FKTableState,
	validationBehavior tree.ValidationBehavior,
) error {
	return ResolveFK(ctx, p.txn, p, tbl, d, backrefs, ts, validationBehavior, p.ExecCfg().Settings)
}

func qualifyFKColErrorWithDB(
	ctx context.Context, txn *kv.Txn, tbl *sqlbase.TableDescriptor, col string,
) string {
	if txn == nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}

	// TODO(solon): this ought to use a database cache.
	db, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tbl.ParentID)
	if err != nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}
	schema, err := schema.ResolveNameByID(ctx, txn, db.ID, tbl.GetParentSchemaID())
	if err != nil {
		return tree.ErrString(tree.NewUnresolvedName(tbl.Name, col))
	}
	return tree.ErrString(tree.NewUnresolvedName(db.Name, schema, tbl.Name, col))
}

// FKTableState is the state of the referencing table resolveFK() is called on.
type FKTableState int

const (
	// NewTable represents a new table, where the FK constraint is specified in the
	// CREATE TABLE
	NewTable FKTableState = iota
	// EmptyTable represents an existing table that is empty
	EmptyTable
	// NonEmptyTable represents an existing non-empty table
	NonEmptyTable
)

// MaybeUpgradeDependentOldForeignKeyVersionTables upgrades the on-disk foreign key descriptor
// version of all table descriptors that have foreign key relationships with desc. This is intended
// to catch upgrade 19.1 version table descriptors that haven't been upgraded yet before an operation
// like drop index which could cause them to lose FK information in the old representation.
func (p *planner) MaybeUpgradeDependentOldForeignKeyVersionTables(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor,
) error {
	// In order to avoid having old version foreign key descriptors that depend on this
	// index lose information when this index is dropped, ensure that they get updated.
	maybeUpgradeFKRepresentation := func(id sqlbase.ID) error {
		// Read the referenced table and see if the foreign key representation has changed. If it has, write
		// the upgraded descriptor back to disk.
		tbl, didUpgrade, err := sqlbase.GetTableDescFromIDWithFKsChanged(ctx, p.txn, id)
		if err != nil {
			return err
		}
		if didUpgrade {
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			err := p.writeSchemaChange(
				ctx, sqlbase.NewMutableExistingTableDescriptor(*tbl), sqlbase.InvalidMutationID,
				"updating foreign key references on table",
			)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for i := range desc.OutboundFKs {
		if err := maybeUpgradeFKRepresentation(desc.OutboundFKs[i].ReferencedTableID); err != nil {
			return err
		}
	}
	for i := range desc.InboundFKs {
		if err := maybeUpgradeFKRepresentation(desc.InboundFKs[i].OriginTableID); err != nil {
			return err
		}
	}
	return nil
}

// ResolveFK looks up the tables and columns mentioned in a `REFERENCES`
// constraint and adds metadata representing that constraint to the descriptor.
// It may, in doing so, add to or alter descriptors in the passed in `backrefs`
// map of other tables that need to be updated when this table is created.
// Constraints that are not known to hold for existing data are created
// "unvalidated", but when table is empty (e.g. during creation), no existing
// data implies no existing violations, and thus the constraint can be created
// without the unvalidated flag.
//
// The caller should pass an instance of fkSelfResolver as
// SchemaResolver, so that FK references can find the newly created
// table for self-references.
//
// The caller must also ensure that the SchemaResolver is configured to
// bypass caching and enable visibility of just-added descriptors.
// If there are any FKs, the descriptor of the depended-on table must
// be looked up uncached, and we'll allow FK dependencies on tables
// that were just added.
//
// The passed Txn is used to lookup databases to qualify names in error messages
// but if nil, will result in unqualified names in those errors.
//
// The passed validationBehavior is used to determine whether or not preexisting
// entries in the table need to be validated against the foreign key being added.
// This only applies for existing tables, not new tables.
func ResolveFK(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	tbl *sqlbase.MutableTableDescriptor,
	d *tree.ForeignKeyConstraintTableDef,
	backrefs map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	ts FKTableState,
	validationBehavior tree.ValidationBehavior,
	settings *cluster.Settings,
) error {
	originColumnIDs := make(sqlbase.ColumnIDs, len(d.FromCols))
	for i, col := range d.FromCols {
		col, _, err := tbl.FindColumnByName(col)
		if err != nil {
			return err
		}
		if err := col.CheckCanBeFKRef(); err != nil {
			return err
		}
		originColumnIDs[i] = col.ID
	}

	target, err := ResolveMutableExistingObject(ctx, sc, &d.Table, true /*required*/, ResolveRequireTableDesc)
	if err != nil {
		return err
	}
	if tbl.IsReplTable != target.IsReplTable {
		return errors.Errorf("Cannot create foreign keys between replicated and non-replicated tables")
	}
	if tbl.Temporary != target.Temporary {
		tablePersistenceType := "permanent"
		if tbl.Temporary {
			tablePersistenceType = "temporary"
		}
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"constraints on %s tables may reference only %s tables",
			tablePersistenceType,
			tablePersistenceType,
		)
	}
	if target.ID == tbl.ID {
		// When adding a self-ref FK to an _existing_ table, we want to make sure
		// we edit the same copy.
		target = tbl
	} else {
		// Since this FK is referencing another table, this table must be created in
		// a non-public "ADD" state and made public only after all leases on the
		// other table are updated to include the backref, if it does not already
		// exist.
		if ts == NewTable {
			tbl.State = sqlbase.TableDescriptor_ADD
		}

		// If we resolve the same table more than once, we only want to edit a
		// single instance of it, so replace target with previously resolved table.
		if prev, ok := backrefs[target.ID]; ok {
			target = prev
		} else {
			backrefs[target.ID] = target
		}
	}

	srcCols, err := tbl.FindActiveColumnsByNames(d.FromCols)
	if err != nil {
		return err
	}

	targetColNames := d.ToCols
	// If no columns are specified, attempt to default to PK.
	if len(targetColNames) == 0 {
		targetColNames = make(tree.NameList, len(target.PrimaryIndex.ColumnNames))
		for i, n := range target.PrimaryIndex.ColumnNames {
			targetColNames[i] = tree.Name(n)
		}
	}

	targetCols, err := target.FindActiveColumnsByNames(targetColNames)
	if err != nil {
		return err
	}

	if len(targetCols) != len(srcCols) {
		return pgerror.Newf(pgcode.Syntax,
			"%d columns must reference exactly %d columns in referenced table (found %d)",
			len(srcCols), len(srcCols), len(targetCols))
	}

	for i := range srcCols {
		if s, t := srcCols[i], targetCols[i]; !s.Type.Equivalent(&t.Type) {
			return pgerror.Newf(pgcode.DatatypeMismatch,
				"type of %q (%s) does not match foreign key %q.%q (%s)",
				s.Name, s.Type.String(), target.Name, t.Name, t.Type.String())
		}
	}

	// Verify we are not writing a constraint over the same name.
	// This check is done in Verify(), but we must do it earlier
	// or else we can hit other checks that break things with
	// undesired error codes, e.g. #42858.
	// It may be removable after #37255 is complete.
	constraintInfo, err := tbl.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}
	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = sqlbase.GenerateUniqueConstraintName(
			fmt.Sprintf("fk_%s_ref_%s", string(d.FromCols[0]), target.Name),
			func(p string) bool {
				_, ok := constraintInfo[p]
				return ok
			},
		)
	} else {
		if _, ok := constraintInfo[constraintName]; ok {
			return pgerror.Newf(pgcode.DuplicateObject, "duplicate constraint name: %q", constraintName)
		}
	}

	targetColIDs := make(sqlbase.ColumnIDs, len(targetCols))
	for i := range targetCols {
		targetColIDs[i] = targetCols[i].ID
	}

	// Don't add a SET NULL action on an index that has any column that is NOT
	// NULL.
	if d.Actions.Delete == tree.SetNull || d.Actions.Update == tree.SetNull {
		for _, sourceColumn := range srcCols {
			if !sourceColumn.Nullable {
				col := qualifyFKColErrorWithDB(ctx, txn, tbl.TableDesc(), sourceColumn.Name)
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET NULL cascading action on column %q which has a NOT NULL constraint", col,
				)
			}
		}
	}

	// Don't add a SET DEFAULT action on an index that has any column that has
	// a DEFAULT expression of NULL and a NOT NULL constraint.
	if d.Actions.Delete == tree.SetDefault || d.Actions.Update == tree.SetDefault {
		for _, sourceColumn := range srcCols {
			// Having a default expression of NULL, and a constraint of NOT NULL is a
			// contradiction and should never be allowed.
			if sourceColumn.DefaultExpr == nil && !sourceColumn.Nullable {
				col := qualifyFKColErrorWithDB(ctx, txn, tbl.TableDesc(), sourceColumn.Name)
				return pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET DEFAULT cascading action on column %q which has a "+
						"NOT NULL constraint and a NULL default expression", col,
				)
			}
		}
	}

	var legacyOriginIndexID sqlbase.IndexID
	// Search for an index on the origin table that matches. If one doesn't exist,
	// we create one automatically if the table to alter is new or empty.
	originIdx, err := sqlbase.FindFKOriginIndex(tbl.TableDesc(), originColumnIDs)
	if err == nil {
		// If there was no error, we found a suitable index.
		legacyOriginIndexID = originIdx.ID
	} else {
		// No existing suitable index was found.
		if ts == NonEmptyTable {
			var colNames bytes.Buffer
			colNames.WriteString(`("`)
			for i, id := range originColumnIDs {
				if i != 0 {
					colNames.WriteString(`", "`)
				}
				col, err := tbl.TableDesc().FindColumnByID(id)
				if err != nil {
					return err
				}
				colNames.WriteString(col.Name)
			}
			colNames.WriteString(`")`)
			return pgerror.Newf(pgcode.ForeignKeyViolation,
				"foreign key requires an existing index on columns %s", colNames.String())
		}
		id, err := addIndexForFK(tbl, srcCols, constraintName, ts)
		if err != nil {
			return err
		}
		legacyOriginIndexID = id
	}

	referencedIdx, err := sqlbase.FindFKReferencedIndex(target.TableDesc(), targetColIDs)
	if err != nil {
		return err
	}
	legacyReferencedIndexID := referencedIdx.ID

	var validity sqlbase.ConstraintValidity
	if ts != NewTable {
		if validationBehavior == tree.ValidationSkip {
			validity = sqlbase.ConstraintValidity_Unvalidated
		} else {
			validity = sqlbase.ConstraintValidity_Validating
		}
	}

	ref := sqlbase.ForeignKeyConstraint{
		OriginTableID:         tbl.ID,
		OriginColumnIDs:       originColumnIDs,
		ReferencedColumnIDs:   targetColIDs,
		ReferencedTableID:     target.ID,
		Name:                  constraintName,
		Validity:              validity,
		OnDelete:              sqlbase.ForeignKeyReferenceActionValue[d.Actions.Delete],
		OnUpdate:              sqlbase.ForeignKeyReferenceActionValue[d.Actions.Update],
		Match:                 sqlbase.CompositeKeyMatchMethodValue[d.Match],
		LegacyOriginIndex:     legacyOriginIndexID,
		LegacyReferencedIndex: legacyReferencedIndexID,
	}

	if ts == NewTable {
		tbl.OutboundFKs = append(tbl.OutboundFKs, ref)
		target.InboundFKs = append(target.InboundFKs, ref)
	} else {
		tbl.AddForeignKeyMutation(&ref, sqlbase.DescriptorMutation_ADD)
	}

	return nil
}

// Adds an index to a table descriptor (that is in the process of being created)
// that will support using `srcCols` as the referencing (src) side of an FK.
func addIndexForFK(
	tbl *sqlbase.MutableTableDescriptor,
	srcCols []sqlbase.ColumnDescriptor,
	constraintName string,
	ts FKTableState,
) (sqlbase.IndexID, error) {
	// No existing index for the referencing columns found, so we add one.
	idx := sqlbase.IndexDescriptor{
		Name:             fmt.Sprintf("%s_auto_index_%s", tbl.Name, constraintName),
		ColumnNames:      make([]string, len(srcCols)),
		ColumnDirections: make([]sqlbase.IndexDescriptor_Direction, len(srcCols)),
	}
	for i, c := range srcCols {
		idx.ColumnDirections[i] = sqlbase.IndexDescriptor_ASC
		idx.ColumnNames[i] = c.Name
	}

	if ts == NewTable {
		if err := tbl.AddIndex(idx, false); err != nil {
			return 0, err
		}
		if err := tbl.AllocateIDs(); err != nil {
			return 0, err
		}
		added := tbl.Indexes[len(tbl.Indexes)-1]
		return added.ID, nil
	}

	// TODO (lucy): In the EmptyTable case, we add an index mutation, making this
	// the only case where a foreign key is added to an index being added.
	// Allowing FKs to be added to other indexes/columns also being added should
	// be a generalization of this special case.
	if err := tbl.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
		return 0, err
	}
	if err := tbl.AllocateIDs(); err != nil {
		return 0, err
	}
	id := tbl.Mutations[len(tbl.Mutations)-1].GetIndex().ID
	return id, nil
}

func (p *planner) addInterleave(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *tree.InterleaveDef,
) error {
	return addInterleave(ctx, p.txn, p, desc, index, interleave)
}

// addInterleave marks an index as one that is interleaved in some parent data
// according to the given definition.
func addInterleave(
	ctx context.Context,
	txn *kv.Txn,
	vt SchemaResolver,
	desc *sqlbase.MutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	interleave *tree.InterleaveDef,
) error {
	if interleave.DropBehavior != tree.DropDefault {
		return unimplemented.NewWithIssuef(
			7854, "unsupported shorthand %s", interleave.DropBehavior)
	}

	parentTable, err := ResolveExistingObject(
		ctx, vt, &interleave.Parent, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc,
	)
	if err != nil {
		return err
	}
	parentIndex := parentTable.PrimaryIndex

	// typeOfIndex is used to give more informative error messages.
	var typeOfIndex string
	if index.ID == desc.PrimaryIndex.ID {
		typeOfIndex = "primary key"
	} else {
		typeOfIndex = "index"
	}

	if len(interleave.Fields) != len(parentIndex.ColumnIDs) {
		return pgerror.Newf(
			pgcode.InvalidSchemaDefinition,
			"declared interleaved columns (%s) must match the parent's primary index (%s)",
			&interleave.Fields,
			strings.Join(parentIndex.ColumnNames, ", "),
		)
	}
	if len(interleave.Fields) > len(index.ColumnIDs) {
		return pgerror.Newf(
			pgcode.InvalidSchemaDefinition,
			"declared interleaved columns (%s) must be a prefix of the %s columns being interleaved (%s)",
			&interleave.Fields,
			typeOfIndex,
			strings.Join(index.ColumnNames, ", "),
		)
	}

	for i, targetColID := range parentIndex.ColumnIDs {
		targetCol, err := parentTable.FindColumnByID(targetColID)
		if err != nil {
			return err
		}
		col, err := desc.FindColumnByID(index.ColumnIDs[i])
		if err != nil {
			return err
		}
		if string(interleave.Fields[i]) != col.Name {
			return pgerror.Newf(
				pgcode.InvalidSchemaDefinition,
				"declared interleaved columns (%s) must refer to a prefix of the %s column names being interleaved (%s)",
				&interleave.Fields,
				typeOfIndex,
				strings.Join(index.ColumnNames, ", "),
			)
		}
		if !col.Type.Identical(&targetCol.Type) || index.ColumnDirections[i] != parentIndex.ColumnDirections[i] {
			return pgerror.Newf(
				pgcode.InvalidSchemaDefinition,
				"declared interleaved columns (%s) must match type and sort direction of the parent's primary index (%s)",
				&interleave.Fields,
				strings.Join(parentIndex.ColumnNames, ", "),
			)
		}
	}

	ancestorPrefix := append(
		[]sqlbase.InterleaveDescriptor_Ancestor(nil), parentIndex.Interleave.Ancestors...)
	intl := sqlbase.InterleaveDescriptor_Ancestor{
		TableID:         parentTable.ID,
		IndexID:         parentIndex.ID,
		SharedPrefixLen: uint32(len(parentIndex.ColumnIDs)),
	}
	for _, ancestor := range ancestorPrefix {
		intl.SharedPrefixLen -= ancestor.SharedPrefixLen
	}
	index.Interleave = sqlbase.InterleaveDescriptor{Ancestors: append(ancestorPrefix, intl)}

	desc.State = sqlbase.TableDescriptor_ADD
	return nil
}

// finalizeInterleave creates backreferences from an interleaving parent to the
// child data being interleaved.
func (p *planner) finalizeInterleave(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, index *sqlbase.IndexDescriptor,
) error {
	// TODO(dan): This is similar to finalizeFKs. Consolidate them
	if len(index.Interleave.Ancestors) == 0 {
		return nil
	}
	// Only the last ancestor needs the backreference.
	ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
	var ancestorTable *sqlbase.MutableTableDescriptor
	if ancestor.TableID == desc.ID {
		ancestorTable = desc
	} else {
		var err error
		ancestorTable, err = p.Tables().getMutableTableVersionByID(ctx, ancestor.TableID, p.txn)
		if err != nil {
			return err
		}
	}
	ancestorIndex, err := ancestorTable.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	ancestorIndex.InterleavedBy = append(ancestorIndex.InterleavedBy,
		sqlbase.ForeignKeyReference{Table: desc.ID, Index: index.ID})

	// TODO (lucy): Have more consistent/informative names for dependent jobs.
	if err := p.writeSchemaChange(
		ctx, ancestorTable, sqlbase.InvalidMutationID, "updating ancestor table",
	); err != nil {
		return err
	}

	if desc.State == sqlbase.TableDescriptor_ADD {
		desc.State = sqlbase.TableDescriptor_PUBLIC

		// No job description, since this is presumably part of some larger schema change.
		if err := p.writeSchemaChange(
			ctx, desc, sqlbase.InvalidMutationID, "",
		); err != nil {
			return err
		}
	}

	return nil
}

// CreatePartitioning constructs the partitioning descriptor for an index that
// is partitioned into ranges, each addressable by zone configs.
func CreatePartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	if partBy == nil {
		// No CCL necessary if we're looking at PARTITION BY NOTHING.
		return sqlbase.PartitioningDescriptor{}, nil
	}
	return CreatePartitioningCCL(ctx, st, evalCtx, tableDesc, indexDesc, partBy)
}

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL = func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	return sqlbase.PartitioningDescriptor{}, sqlbase.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
}

// InitTableDescriptor returns a blank TableDescriptor.
func InitTableDescriptor(
	id, parentID, parentSchemaID sqlbase.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	temporary bool,
	tblType tree.TableType,
	creator string,
) sqlbase.MutableTableDescriptor {
	return *sqlbase.NewMutableCreatedTableDescriptor(sqlbase.TableDescriptor{
		ID:                      id,
		Name:                    name,
		ParentID:                parentID,
		UnexposedParentSchemaID: parentSchemaID,
		FormatVersion:           sqlbase.InterleavedFormatVersion,
		Version:                 1,
		ModificationTime:        creationTime,
		Privileges:              privileges,
		CreateAsOfTime:          creationTime,
		Temporary:               temporary,
		TableType:               tblType,
		Creator:                 creator,
		CreateTime:              creationTime,
	})
}

func getFinalSourceQuery(source *tree.Select, evalCtx *tree.EvalContext) string {
	// Ensure that all the table names pretty-print as fully qualified, so we
	// store that in the table descriptor.
	//
	// The traversal will update the TableNames in-place, so the changes are
	// persisted in n.n.AsSource. We exploit the fact that planning step above
	// has populated any missing db/schema details in the table names in-place.
	// We use tree.FormatNode merely as a traversal method; its output buffer is
	// discarded immediately after the traversal because it is not needed
	// further.
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.SetReformatTableNames(
		func(_ *tree.FmtCtx, tn *tree.TableName) {
			// Persist the database prefix expansion.
			if tn.SchemaName != "" {
				// All CTE or table aliases have no schema
				// information. Those do not turn into explicit.
				tn.ExplicitSchema = true
				tn.ExplicitCatalog = true
			}
		},
	)
	f.FormatNode(source)
	f.Close()

	// Substitute placeholders with their values.
	ctx := tree.NewFmtCtx(tree.FmtParsable)
	ctx.SetPlaceholderFormat(func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
		d, err := placeholder.Eval(evalCtx)
		if err != nil {
			panic(fmt.Sprintf("failed to serialize placeholder: %s", err))
		}
		d.Format(ctx)
	})
	ctx.FormatNode(source)

	return ctx.CloseAndGetString()
}

// makeTableDescIfAs is the MakeTableDesc method for when we have a table
// that is created with the CREATE AS format.
func makeTableDescIfAs(
	params runParams,
	p *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	resultColumns []sqlbase.ResultColumn,
	privileges *sqlbase.PrivilegeDescriptor,
	evalContext *tree.EvalContext,
	temporary bool,
) (desc sqlbase.MutableTableDescriptor, err error) {
	colResIndex := 0
	// TableDefs for a CREATE TABLE ... AS AST node comprise of a ColumnTableDef
	// for each column, and a ConstraintTableDef for any constraints on those
	// columns.
	for _, defs := range p.Defs {
		var d *tree.ColumnTableDef
		var ok bool
		if d, ok = defs.(*tree.ColumnTableDef); ok {
			d.Type = resultColumns[colResIndex].Typ
			colResIndex++
		}
	}

	// If there are no TableDefs defined by the parser, then we construct a
	// ColumnTableDef for each column using resultColumns.
	if len(p.Defs) == 0 {
		for _, colRes := range resultColumns {
			var d *tree.ColumnTableDef
			var ok bool
			var tableDef tree.TableDef = &tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colRes.Typ}
			if d, ok = tableDef.(*tree.ColumnTableDef); !ok {
				return desc, errors.Errorf("failed to cast type to ColumnTableDef\n")
			}
			d.Nullable.Nullability = tree.SilentNull
			p.Defs = append(p.Defs, tableDef)
		}
	}

	desc, err = makeTableDesc(
		params,
		p,
		parentID, parentSchemaID, id,
		creationTime,
		privileges,
		nil, /* affected */
		temporary,
	)
	desc.CreateQuery = getFinalSourceQuery(p.AsSource, evalContext)
	return desc, err
}

func dequalifyColumnRefs(
	ctx context.Context, source *sqlbase.DataSourceInfo, expr tree.Expr,
) (tree.Expr, error) {
	resolver := sqlbase.ColumnResolver{Source: source}
	return tree.SimpleVisit(
		expr,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			if vBase, ok := expr.(tree.VarName); ok {
				v, err := vBase.NormalizeVarName()
				if err != nil {
					return false, nil, err
				}
				if c, ok := v.(*tree.ColumnItem); ok {
					_, err := c.Resolve(ctx, &resolver)
					if err != nil {
						return false, nil, err
					}
					colIdx := resolver.ResolverState.ColIdx
					col := source.SourceColumns[colIdx]
					return false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}, nil
				}
			}
			return true, expr, err
		},
	)
}

// buildTSTableDesc checks if object in create table is available and build time-series table descriptor
func buildTSTableDesc(
	desc *sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	n *tree.CreateTable,
	allTagDesc *[]*sqlbase.ColumnDescriptor,
	user string,
) error {
	allTagName := make(map[tree.Name]*sqlbase.ColumnDescriptor, len(n.Tags)+1)
	desc.TsTable.Sde = n.Sde
	if len(n.StorageParams) > 0 {
		return sqlbase.TSUnsupportedError("storage params is not accepted for timeseries table")
	}
	if len(n.Defs) < 2 {
		return pgerror.New(pgcode.InvalidTableDefinition, "ts table must have at least 2 columns")
	}
	if len(n.Defs) > MaxTSDataColumns {
		return pgerror.Newf(pgcode.TooManyColumns, "table %s has too many columns,"+
			" each timeseries table can have maximum 4096 columns", n.Table.Table())
	}
	var activeTime int64
	if n.ActiveTime != nil {
		activeTime = getTimeFromTimeInput(*n.ActiveTime)
		if activeTime < 0 || activeTime > MaxLifeTime {
			return pgerror.Newf(pgcode.InvalidParameterValue, "active time %d%s is invalid",
				n.ActiveTime.Value, n.ActiveTime.Unit)
		}
		activeTimeInput := timeInputToString(*n.ActiveTime)
		desc.TsTable.ActiveTimeInput = &activeTimeInput
	} else {
		activeTime = DefaultActiveTime
	}
	desc.TsTable.ActiveTime = uint32(activeTime)
	var partitionInterval int64
	if n.PartitionInterval != nil {
		switch n.PartitionInterval.Unit {
		case "s", "second", "m", "minute", "h", "hour":
			return pgerror.Newf(pgcode.InvalidParameterValue, "unsupported partition interval unit: %s",
				n.PartitionInterval.Unit)
		}
		partitionInterval = getTimeFromTimeInput(*n.PartitionInterval)
		if partitionInterval <= 0 || partitionInterval > MaxLifeTime {
			return pgerror.Newf(pgcode.InvalidParameterValue, "partition interval %d%s is invalid, time range between 1day and 1000year is accepted",
				n.PartitionInterval.Value, n.PartitionInterval.Unit)
		}
		partitionIntervalInput := timeInputToString(*n.PartitionInterval)
		desc.TsTable.PartitionIntervalInput = &partitionIntervalInput
	} else {
		partitionInterval = InvalidLifetime
	}
	desc.TsTable.PartitionInterval = uint64(partitionInterval)
	desc.TsTable.TsVersion = 1
	desc.TsTable.NextTsVersion = desc.TsTable.TsVersion + 1

	// The default primary tag for template tables is the instance table name
	if len(n.PrimaryTagList) == 0 {
		hiddenTag := sqlbase.ColumnDescriptor{
			Name:     "pTag",
			Type:     *types.MakeChar(63),
			Nullable: false,
			Hidden:   true,
			TsCol: sqlbase.TSCol{
				ColumnType:         sqlbase.ColumnType_TYPE_PTAG,
				StorageType:        sqlbase.DataType_CHAR,
				StorageLen:         63,
				VariableLengthType: sqlbase.VariableLengthType_ColStorageTypeTuple,
			},
		}
		*allTagDesc = append(*allTagDesc, &hiddenTag)
		allTagName[tree.Name(hiddenTag.Name)] = &hiddenTag
	}

	primaryTagName := make(map[tree.Name]struct{}, len(n.PrimaryTagList))
	for _, pt := range n.PrimaryTagList {
		primaryTagName[pt] = struct{}{}
	}
	for i := range n.Tags {
		columnType := sqlbase.ColumnType_TYPE_TAG
		if _, ok := allTagName[n.Tags[i].TagName]; ok {
			return pgerror.Newf(pgcode.DuplicateColumn, "duplicate tag name: %q", n.Tags[i].TagName)
		}
		if _, ok := primaryTagName[n.Tags[i].TagName]; ok {
			columnType = sqlbase.ColumnType_TYPE_PTAG
			if n.Tags[i].TagType.Width() == DefaultTypeWithLength && n.Tags[i].TagType.Oid() == oid.T_varchar {
				n.Tags[i].TagType = types.MakeVarChar(DefaultPrimaryTagVarcharWidth, n.Tags[i].TagType.TypeEngine())
			}
		}
		if len(string(n.Tags[i].TagName)) > MaxTagNameLength {
			return sqlbase.NewTSNameOutOfLengthError("tag", string(n.Tags[i].TagName), MaxTagNameLength)
		}
		if n.Tags[i].IsSerial {
			return pgerror.Newf(
				pgcode.FeatureNotSupported, "serial type for tag %s is not supported in timeseries table", n.Tags[i].TagName)
		}
		tagType, err := checkTagType(n.Tags[i].TagName, n.Tags[i].TagType)
		if err != nil {
			return err
		}
		n.Tags[i].TagType = tagType
		// Building columnDesc for tags
		tagColumn, _, err := sqlbase.MakeTSColumnDefDescs(string(n.Tags[i].TagName), n.Tags[i].TagType, n.Tags[i].Nullable, false, columnType, nil, semaCtx)
		if err != nil {
			return err
		}
		*allTagDesc = append(*allTagDesc, tagColumn)
		allTagName[n.Tags[i].TagName] = tagColumn
	}
	// Check if the primary tag meets the requirements of the primary tag
	// 1. Cannot exceed four
	// 2. Floating point types and variable length types other than varchar are not supported
	// 3. The maximum length of varchar type is 128, with a default of 64
	// 4. The primary tag must be not null
	if len(n.PrimaryTagList) > sqlbase.MaxPrimaryTagNum {
		return pgerror.Newf(pgcode.ProgramLimitExceeded, "the max number of primary tags is %d", sqlbase.MaxPrimaryTagNum)
	}
	for _, pt := range n.PrimaryTagList {
		if tagColumn, ok := allTagName[pt]; ok {
			if err := checkPrimaryTag(*tagColumn); err != nil {
				return err
			}
		} else {
			return pgerror.Newf(pgcode.InvalidName, "primary tag %s is not a tag", string(pt))
		}
	}
	if n.DownSampling != nil {
		reten, err := checkRetentionForCreate(n.Defs, *n.DownSampling)
		if err != nil {
			return err
		}
		desc.TsTable.Resolution = reten.resolution
		desc.TsTable.KeepDuration = reten.keepDuration
		desc.TsTable.Sample = reten.samples
		desc.TsTable.Downsampling = reten.originRetention
		desc.TsTable.Lifetime = reten.lifetime
		desc.TsTable.DownsamplingCreator = user
	} else {
		desc.TsTable.Lifetime = InvalidLifetime
	}
	return nil
}

// checkColumnDef checks if object in column definition is available
func checkColumnDef(
	ctx context.Context,
	d *tree.ColumnTableDef,
	desc *sqlbase.MutableTableDescriptor,
	n *tree.CreateTable,
	st *cluster.Settings,
	sessionData *sessiondata.SessionData,
	columnDefaultExprs *[]tree.TypedExpr,
) error {
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if !desc.IsVirtualTable() {
		switch d.Type.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"VECTOR column types are unsupported",
			)
		}
	}
	if supported, err := isTypeSupportedInVersion(version, d.Type); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			d.Type.SQLString(),
		)
	}
	if d.PrimaryKey.Sharded {
		// This function can sometimes be called when `st` is nil,
		// and also before the version has been initialized. We only
		// allow hash sharded indexes to be created if we know for
		// certain that it supported by the cluster.
		if st == nil {
			return invalidClusterForShardedIndexError
		}
		if version == (clusterversion.ClusterVersion{}) ||
			!version.IsActive(clusterversion.VersionHashShardedIndexes) {
			return invalidClusterForShardedIndexError
		}

		if !sessionData.HashShardedIndexesEnabled {
			return hashShardedIndexesDisabledError
		}
		if n.PartitionBy != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		if n.Interleave != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		buckets, err := tree.EvalShardBucketCount(d.PrimaryKey.ShardBuckets)
		if err != nil {
			return err
		}
		shardCol, _, err := maybeCreateAndAddShardCol(int(buckets), desc,
			[]string{string(d.Name)}, true /* isNewTable */)
		if err != nil {
			return err
		}
		checkConstraint, err := makeShardCheckConstraintDef(desc, int(buckets), shardCol)
		if err != nil {
			return err
		}
		// Add the shard's check constraint to the list of TableDefs to treat it
		// like it's been "hoisted" like the explicitly added check constraints.
		// It'll then be added to this table's resulting table descriptor below in
		// the constraint pass.
		n.Defs = append(n.Defs, checkConstraint)
		*columnDefaultExprs = append(*columnDefaultExprs, nil)
	}
	return nil
}

// checkAndMakeTSColDesc checks if the first column type is timestamptz and make ts column descriptor
func checkAndMakeTSColDesc(
	d *tree.ColumnTableDef,
	semaCtx *tree.SemaContext,
	col **sqlbase.ColumnDescriptor,
	desc *sqlbase.MutableTableDescriptor,
	isFirstTSCol bool,
) (tree.TypedExpr, error) {
	var err error
	var expr tree.TypedExpr
	if isFirstTSCol {
		if d.Type.Family() != types.TimestampFamily && d.Type.Family() != types.TimestampTZFamily {
			return nil, pgerror.Newf(pgcode.DatatypeMismatch, "column %s: the 1st column's type in timeseries table must be TimestampTZ", d.Name)
		} else if d.Nullable.Nullability != tree.NotNull {
			return nil, pgerror.Newf(pgcode.NotNullViolation, "the 1st TimestampTZ column %s must be not null", string(d.Name))
		}
		if d.Type.InternalType.TimePrecisionIsSet {
			d.Type = types.MakeTimestampTZ(d.Type.Precision())
		} else {
			d.Type = types.MakeTimestampTZ(3)
		}
	} else {
		d.Type = sqlbase.UpdateTimeColPrecision(d.Type, tree.TimeseriesTable)
	}
	if err = checkTSColValidity(d); err != nil {
		return nil, err
	}
	nullable := d.Nullable.Nullability != tree.NotNull
	*col, expr, err = sqlbase.MakeTSColumnDefDescs(string(d.Name), d.Type, nullable, desc.TsTable.Sde, sqlbase.ColumnType_TYPE_DATA, d.DefaultExpr.Expr, semaCtx)
	if err != nil {
		return nil, err
	}
	if isFirstTSCol {
		// The first timestamp column in the data column needs to reserve 16 bytes for LSN
		(*col).TsCol.StorageLen = sqlbase.FirstTsDataColSize
		// Add a unique constraint to the first column of the timeseries table to prevent a validate error
		tsPK := sqlbase.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(tsPK, true); err != nil {
			return nil, err
		}
	}
	return expr, nil
}

// buildIndexForDesc builds index descriptor for column descriptor
func buildIndexForDesc(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	d *tree.IndexTableDef,
	desc *sqlbase.MutableTableDescriptor,
	setupShardedIndexForNewTable func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
) error {
	idx := sqlbase.IndexDescriptor{
		Name:             string(d.Name),
		StoreColumnNames: d.Storing.ToStrings(),
		Version:          indexEncodingVersion,
	}
	if d.Inverted {
		idx.Type = sqlbase.IndexDescriptor_INVERTED
	}
	if d.Sharded != nil {
		if d.Interleave != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		if err := setupShardedIndexForNewTable(d, &idx); err != nil {
			return err
		}
	}
	if err := idx.FillColumns(d.Columns); err != nil {
		return err
	}
	if d.PartitionBy != nil {
		partitioning, err := CreatePartitioning(ctx, st, evalCtx, desc, &idx, d.PartitionBy)
		if err != nil {
			return err
		}
		idx.Partitioning = partitioning
	}

	if err := desc.AddIndex(idx, false); err != nil {
		return err
	}
	if d.Interleave != nil {
		return unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
	}
	return nil
}

// buildUniqueForDesc builds unique descriptor for column descriptor
func buildUniqueForDesc(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	d *tree.UniqueConstraintTableDef,
	desc *sqlbase.MutableTableDescriptor,
	n *tree.CreateTable,
	setupShardedIndexForNewTable func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
	primaryIndexColumnSet *map[string]struct{},
) error {
	idx := sqlbase.IndexDescriptor{
		Name:             string(d.Name),
		Unique:           true,
		StoreColumnNames: d.Storing.ToStrings(),
		Version:          indexEncodingVersion,
	}
	if d.Sharded != nil {
		if n.Interleave != nil && d.PrimaryKey {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		if err := setupShardedIndexForNewTable(&d.IndexTableDef, &idx); err != nil {
			return err
		}
	}
	if err := idx.FillColumns(d.Columns); err != nil {
		return err
	}
	if d.PartitionBy != nil {
		partitioning, err := CreatePartitioning(ctx, st, evalCtx, desc, &idx, d.PartitionBy)
		if err != nil {
			return err
		}
		idx.Partitioning = partitioning
	}
	if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
		return err
	}
	if d.PrimaryKey {
		if d.Interleave != nil {
			return unimplemented.NewWithIssue(
				45710,
				"interleave not supported in primary key constraint definition",
			)
		}
		*primaryIndexColumnSet = make(map[string]struct{})
		for _, c := range d.Columns {
			(*primaryIndexColumnSet)[string(c.Column)] = struct{}{}
		}
	}
	if d.Interleave != nil {
		return unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
	}
	return nil
}

// buildFamilyForDesc builds column family descriptor for table descriptor
func buildFamilyForDesc(
	d *tree.FamilyTableDef,
	desc *sqlbase.MutableTableDescriptor,
	columnsInExplicitFamilies *map[string]bool,
) {
	fam := sqlbase.ColumnFamilyDescriptor{
		Name:        string(d.Name),
		ColumnNames: d.Columns.ToStrings(),
	}
	for _, c := range fam.ColumnNames {
		(*columnsInExplicitFamilies)[c] = true
	}
	desc.AddFamily(fam)
}

// addColToTblDesc resolves column table define to column desc and add column desc to table desc
func addColToTblDesc(
	semaCtx *tree.SemaContext,
	d *tree.ColumnTableDef,
	num int,
	desc *sqlbase.MutableTableDescriptor,
	columnDefaultExprs *[]tree.TypedExpr,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
) error {
	var err error
	var col *sqlbase.ColumnDescriptor
	var idx *sqlbase.IndexDescriptor
	var expr tree.TypedExpr
	if desc.IsTSTable() {
		isFirstTSCol := num == 0
		if expr, err = checkAndMakeTSColDesc(d, semaCtx, &col, desc, isFirstTSCol); err != nil {
			return err
		}
	} else {
		col, idx, expr, err = sqlbase.MakeColumnDefDescs(d, semaCtx, tree.RelationalTable)
		if err != nil {
			return err
		}
	}

	desc.AddColumn(col)
	if d.HasDefaultExpr() {
		// This resolution must be delayed until ColumnIDs have been populated.
		(*columnDefaultExprs)[num] = expr
	} else {
		(*columnDefaultExprs)[num] = nil
	}

	if idx != nil {
		idx.Version = indexEncodingVersion
		if err := desc.AddIndex(*idx, d.PrimaryKey.IsPrimaryKey); err != nil {
			return err
		}
	}

	if d.HasColumnFamily() {
		// Pass true for `create` and `ifNotExists` because when we're creating
		// a table, we always want to create the specified family if it doesn't
		// exist.
		err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkColFamily checks if primary key column is in column family when cluster setting is nil
func checkColFamily(
	ctx context.Context, st *cluster.Settings, desc *sqlbase.MutableTableDescriptor,
) error {
	if version := st.Version.ActiveVersionOrEmpty(ctx); version != (clusterversion.ClusterVersion{}) &&
		!version.IsActive(clusterversion.VersionPrimaryKeyColumnsOutOfFamilyZero) {
		var colsInFamZero util.FastIntSet
		for _, colID := range desc.Families[0].ColumnIDs {
			colsInFamZero.Add(int(colID))
		}
		for _, colID := range desc.PrimaryIndex.ColumnIDs {
			if !colsInFamZero.Contains(int(colID)) {
				return errors.Errorf("primary key column %d is not in column family 0", colID)
			}
		}
	}
	return nil
}

// parseAndSerializeComputeCol parses compute expr of col and serialize result expr
func parseAndSerializeComputeCol(
	ctx context.Context, n *tree.CreateTable, desc *sqlbase.MutableTableDescriptor,
) error {
	// Now that we've constructed our columns, we pop into any of our computed
	// columns so that we can dequalify any column references.
	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		n.Table, sqlbase.ResultColumnsFromColDescs(desc.GetID(), desc.Columns),
	)

	for i := range desc.Columns {
		col := &desc.Columns[i]
		if col.IsComputed() {
			expr, err := parser.ParseExpr(*col.ComputeExpr)
			if err != nil {
				return err
			}

			expr, err = dequalifyColumnRefs(ctx, sourceInfo, expr)
			if err != nil {
				return err
			}
			serialized := tree.Serialize(expr)
			col.ComputeExpr = &serialized
		}
	}
	return nil
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
//
// txn and vt can be nil if the table to be created does not contain references
// to other tables (e.g. foreign keys or interleaving). This is useful at
// bootstrap when creating descriptors for virtual tables.
//
// parentID refers to the databaseID under which the descriptor is being
// created,and parentSchemaID refers to the schemaID of the schema under which
// the descriptor is being created.
//
// evalCtx can be nil if the table to be created has no default expression for
// any of the columns and no partitioning expression.
//
// semaCtx can be nil if the table to be created has no default expression on
// any of the columns and no check constraints.
//
// The caller must also ensure that the SchemaResolver is configured
// to bypass caching and enable visibility of just-added descriptors.
// This is used to resolve sequence and FK dependencies. Also see the
// comment at the start of the global scope resolveFK().
//
// If the table definition *may* use the SERIAL type, the caller is
// also responsible for processing serial types using
// processSerialInColumnDef() on every column definition, and creating
// the necessary sequences in KV before calling MakeTableDesc().
func MakeTableDesc(
	ctx context.Context,
	txn *kv.Txn,
	vt SchemaResolver,
	st *cluster.Settings,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	sessionData *sessiondata.SessionData,
	temporary bool,
) (sqlbase.MutableTableDescriptor, error) {
	// Used to delay establishing Column/Sequence dependency until ColumnIDs have
	// been populated.
	var err error
	columnDefaultExprs := make([]tree.TypedExpr, len(n.Defs))
	desc := InitTableDescriptor(
		id, parentID, parentSchemaID, n.Table.Table(), creationTime, privileges, temporary, n.TableType, sessionData.User,
	)
	var allTagDesc []*sqlbase.ColumnDescriptor
	if desc.IsTSTable() {
		if err = buildTSTableDesc(&desc, semaCtx, n, &allTagDesc, sessionData.User); err != nil {
			return desc, err
		}
	}

	if err = checkStorageParameters(semaCtx, n.StorageParams, storageParamExpectedTypes); err != nil {
		return desc, err
	}

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into new index descriptors.
	indexEncodingVersion := sqlbase.BaseIndexFormatVersion
	// We can't use st.Version.IsActive because this method is used during
	// server setup before the cluster version has been initialized.
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if version != (clusterversion.ClusterVersion{}) &&
		version.IsActive(clusterversion.VersionSecondaryIndexColumnFamilies) {
		indexEncodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
	}

	for i, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if err = checkColumnDef(ctx, d, &desc, n, st, sessionData, &columnDefaultExprs); err != nil {
				return desc, err
			}
			if err = addColToTblDesc(semaCtx, d, i, &desc, &columnDefaultExprs, indexEncodingVersion); err != nil {
				return desc, err
			}
		}
	}

	if n.IsTS() {
		generateTableFormatMetadata(&desc.TsTable, &desc.Columns)
		for _, tagColumn := range allTagDesc {
			desc.AddColumn(tagColumn)
		}
	}

	if err = parseAndSerializeComputeCol(ctx, n, &desc); err != nil {
		return desc, err
	}

	var primaryIndexColumnSet map[string]struct{}
	setupShardedIndexForNewTable := func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error {
		if n.PartitionBy != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		shardCol, newColumn, err := setupShardedIndex(
			ctx,
			st,
			sessionData.HashShardedIndexesEnabled,
			&d.Columns,
			d.Sharded.ShardBuckets,
			&desc,
			idx,
			true /* isNewTable */)
		if err != nil {
			return err
		}
		if newColumn {
			buckets, err := tree.EvalShardBucketCount(d.Sharded.ShardBuckets)
			if err != nil {
				return err
			}
			checkConstraint, err := makeShardCheckConstraintDef(&desc, int(buckets), shardCol)
			if err != nil {
				return err
			}
			n.Defs = append(n.Defs, checkConstraint)
			columnDefaultExprs = append(columnDefaultExprs, nil)
		}
		return nil
	}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// pass, handled above.
		case *tree.IndexTableDef:
			if desc.IsTSTable() {
				return desc, sqlbase.TSUnsupportedError("table def: index")
			}
			if err = buildIndexForDesc(ctx, st, evalCtx, d, &desc, setupShardedIndexForNewTable, indexEncodingVersion); err != nil {
				return desc, err
			}
		case *tree.UniqueConstraintTableDef:
			if desc.IsTSTable() {
				return desc, sqlbase.TSUnsupportedError("table def: unique")
			}
			if err = buildUniqueForDesc(ctx, st, evalCtx, d, &desc, n, setupShardedIndexForNewTable, indexEncodingVersion, &primaryIndexColumnSet); err != nil {
				return desc, err
			}
		case *tree.CheckConstraintTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("check constraint")
			}
		case *tree.ForeignKeyConstraintTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("referenced constraint")
			}
		case *tree.FamilyTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("family")
			}
			// handled of relational table below.
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// If explicit primary keys are required, error out since a primary key was not supplied.
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() && evalCtx != nil &&
		evalCtx.SessionData != nil && evalCtx.SessionData.RequireExplicitPrimaryKeys {
		return desc, errors.Errorf(
			"no primary key specified for table %s (require_explicit_primary_keys = true)", desc.Name)
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[desc.Columns[i].Name]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	columnsInExplicitFamilies := map[string]bool{}
	for _, def := range n.Defs {
		if d, ok := def.(*tree.FamilyTableDef); ok {
			buildFamilyForDesc(d, &desc, &columnsInExplicitFamilies)
		}
	}

	// Assign any implicitly added shard columns to the column family of the first column
	// in their corresponding set of index columns.
	for _, index := range desc.AllNonDropIndexes() {
		if index.IsSharded() && !columnsInExplicitFamilies[index.Sharded.Name] {
			// Ensure that the shard column wasn't explicitly assigned a column family
			// during table creation (this will happen when a create statement is
			// "roundtripped", for example).
			family := sqlbase.GetColumnFamilyForShard(&desc, index.Sharded.ColumnNames)
			if family != "" {
				if err := desc.AddColumnToFamilyMaybeCreate(index.Sharded.Name, family, false, false); err != nil {
					return desc, err
				}
			}
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return desc, err
	}

	// If any nodes are not at version VersionPrimaryKeyColumnsOutOfFamilyZero, then return an error
	// if a primary key column is not in column family 0.
	if st != nil {
		if err = checkColFamily(ctx, st, &desc); err != nil {
			return desc, err
		}
	}

	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		// Increment the counter if this index could be storing data across multiple column families.
		if len(idx.StoreColumnNames) > 1 && len(desc.Families) > 1 {
			telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
		}
	}

	if n.Interleave != nil {
		if err := addInterleave(ctx, txn, vt, &desc, &desc.PrimaryIndex, n.Interleave); err != nil {
			return desc, err
		}
	}

	if n.PartitionBy != nil {
		partitioning, err := CreatePartitioning(
			ctx, st, evalCtx, &desc, &desc.PrimaryIndex, n.PartitionBy)
		if err != nil {
			return desc, err
		}
		desc.PrimaryIndex.Partitioning = partitioning
	}

	// Once all the IDs have been allocated, we can add the Sequence dependencies
	// as maybeAddSequenceDependencies requires ColumnIDs to be correct.
	// Elements in n.Defs are not necessarily column definitions, so use a separate
	// counter to map ColumnDefs to columns.
	colIdx := 0
	for i := range n.Defs {
		if _, ok := n.Defs[i].(*tree.ColumnTableDef); ok {
			if expr := columnDefaultExprs[i]; expr != nil {
				changedSeqDescs, err := maybeAddSequenceDependencies(ctx, vt, &desc, &desc.Columns[colIdx], expr, affected)
				if err != nil {
					return desc, err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					affected[changedSeqDesc.ID] = changedSeqDesc
				}
			}
			colIdx++
		}
	}

	// With all structural elements in place and IDs allocated, we can resolve the
	// constraints and qualifications.
	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs. Resolution also
	// accumulates updates to other tables (adding backreferences) in the passed
	// map -- anything in that map should be saved when the table is created.
	//

	// We use a fkSelfResolver so that name resolution can find the newly created
	// table.
	fkResolver := &fkSelfResolver{
		SchemaResolver: vt,
		newTableDesc:   desc.TableDesc(),
		newTableName:   &n.Table,
	}

	generatedNames := map[string]struct{}{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// Check after all ResolveFK calls.
		case *tree.IndexTableDef, *tree.UniqueConstraintTableDef, *tree.FamilyTableDef:
			// Pass, handled above.
		case *tree.CheckConstraintTableDef:
			ck, err := MakeCheckConstraint(ctx, &desc, d, generatedNames, semaCtx, n.Table)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)
		case *tree.ForeignKeyConstraintTableDef:
			if err := ResolveFK(ctx, txn, fkResolver, &desc, d, affected, NewTable, tree.ValidationDefault, st); err != nil {
				return desc, err
			}
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// Now that we have all the other columns set up, we can validate
	// any computed columns.
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.IsComputed() {
				if err := validateComputedColumn(&desc, d, semaCtx); err != nil {
					return desc, err
				}
			}
		}
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err = desc.AllocateIDs()

	// Record the types of indexes that the table has.
	if err := desc.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		if idx.IsSharded() {
			telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
		}
		if idx.Type == sqlbase.IndexDescriptor_INVERTED {
			telemetry.Inc(sqltelemetry.InvertedIndexCounter)
		}
		return nil
	}); err != nil {
		return desc, err
	}

	return desc, err
}

func checkStorageParameters(
	semaCtx *tree.SemaContext, params tree.StorageParams, expectedTypes map[string]storageParamType,
) error {
	for _, sp := range params {
		k := string(sp.Key)
		validate, ok := expectedTypes[k]
		if !ok {
			return errors.Errorf("invalid storage parameter %q", k)
		}
		if sp.Value == nil {
			return errors.Errorf("storage parameter %q requires a value", k)
		}
		var expectedType *types.T
		if validate == storageParamBool {
			expectedType = types.Bool
		} else if validate == storageParamInt {
			expectedType = types.Int
		} else if validate == storageParamFloat {
			expectedType = types.Float
		} else {
			return unimplemented.NewWithIssuef(43299, "storage parameter %q", k)
		}

		_, err := tree.TypeCheckAndRequire(sp.Value, semaCtx, expectedType, k)
		if err != nil {
			return err
		}
	}
	return nil
}

// makeTableDesc creates a table descriptor from a CreateTable statement.
func makeTableDesc(
	params runParams,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	temporary bool,
) (ret sqlbase.MutableTableDescriptor, err error) {
	// Process any SERIAL columns to remove the SERIAL type,
	// as required by MakeTableDesc.
	createStmt := n
	ensureCopy := func() {
		if createStmt == n {
			newCreateStmt := *n
			n.Defs = append(tree.TableDefs(nil), n.Defs...)
			createStmt = &newCreateStmt
		}
	}
	for i, def := range n.Defs {
		d, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		// Do not include virtual tables in these statistics.
		if !sqlbase.IsVirtualTable(id) {
			incTelemetryForNewColumn(d)
		}
		newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, &n.Table, n.IsTS())
		if err != nil {
			return ret, err
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if seqName != nil {
			if err := doCreateSequence(
				params,
				n.String(),
				seqDbDesc,
				parentSchemaID,
				seqName,
				temporary,
				seqOpts,
				"creating sequence",
				d.IsSerial,
			); err != nil {
				return ret, err
			}
		}
		if d != newDef {
			ensureCopy()
			n.Defs[i] = newDef
		}
	}

	// We need to run MakeTableDesc with caching disabled, because
	// it needs to pull in descriptors from FK depended-on tables
	// and interleaved parents using their current state in KV.
	// See the comment at the start of MakeTableDesc() and resolveFK().
	params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
		ret, err = MakeTableDesc(
			params.ctx,
			params.p.txn,
			params.p,
			params.p.ExecCfg().Settings,
			n,
			parentID,
			parentSchemaID,
			id,
			creationTime,
			privileges,
			affected,
			&params.p.semaCtx,
			params.EvalContext(),
			params.SessionData(),
			temporary,
		)
	})
	return ret, err
}

// dummyColumnItem is used in MakeCheckConstraint to construct an expression
// that can be both type-checked and examined for variable expressions.
type dummyColumnItem struct {
	typ *types.T
	// name is only used for error-reporting.
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumnItem) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumnItem) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumnItem) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumnItem) TypeCheck(_ *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumnItem) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumnItem) ResolvedType() *types.T {
	return d.typ
}

// makeShardColumnDesc returns a new column descriptor for a hidden computed shard column
// based on all the `colNames`.
func makeShardColumnDesc(colNames []string, buckets int) (*sqlbase.ColumnDescriptor, error) {
	col := &sqlbase.ColumnDescriptor{
		Hidden:   true,
		Nullable: false,
		Type:     *types.Int4,
	}
	col.Name = sqlbase.GetShardColumnName(colNames, int32(buckets))
	col.ComputeExpr = makeHashShardComputeExpr(colNames, buckets)
	return col, nil
}

// makeHashShardComputeExpr creates the serialized computed expression for a hash shard
// column based on the column names and the number of buckets. The expression will be
// of the form:
//
//	mod(fnv32(colNames[0]::STRING)+fnv32(colNames[1])+...,buckets)
func makeHashShardComputeExpr(colNames []string, buckets int) *string {
	unresolvedFunc := func(funcName string) tree.ResolvableFunctionReference {
		return tree.ResolvableFunctionReference{
			FunctionReference: &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{funcName},
			},
		}
	}
	hashedColumnExpr := func(colName string) tree.Expr {
		return &tree.FuncExpr{
			Func: unresolvedFunc("fnv32"),
			Exprs: tree.Exprs{
				// NB: We have created the hash shard column as NOT NULL so we need
				// to coalesce NULLs into something else. There's a variety of different
				// reasonable choices here. We could pick some outlandish value, we
				// could pick a zero value for each type, or we can do the simple thing
				// we do here, however the empty string seems pretty reasonable. At worst
				// we'll have a collision for every combination of NULLable string
				// columns. That seems just fine.
				&tree.CoalesceExpr{
					Name: "COALESCE",
					Exprs: tree.Exprs{
						&tree.CastExpr{
							Type: types.String,
							Expr: &tree.ColumnItem{ColumnName: tree.Name(colName)},
						},
						tree.NewDString(""),
					},
				},
			},
		}
	}

	// Construct an expression which is the sum of all of the casted and hashed
	// columns.
	var expr tree.Expr
	for i := len(colNames) - 1; i >= 0; i-- {
		c := colNames[i]
		if expr == nil {
			expr = hashedColumnExpr(c)
		} else {
			expr = &tree.BinaryExpr{
				Left:     hashedColumnExpr(c),
				Operator: tree.Plus,
				Right:    expr,
			}
		}
	}
	str := tree.Serialize(&tree.FuncExpr{
		Func: unresolvedFunc("mod"),
		Exprs: tree.Exprs{
			expr,
			tree.NewDInt(tree.DInt(buckets)),
		},
	})
	return &str
}

// generateMaybeDuplicateNameForCheckConstraint generates a name, the given check
// constraint expression, which may already be taken by another object in the table
// descriptor.
func generateMaybeDuplicateNameForCheckConstraint(
	desc *MutableTableDescriptor, expr tree.Expr,
) (string, error) {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	if err := iterColDescriptorsInExpr(desc, expr, func(c *sqlbase.ColumnDescriptor) error {
		nameBuf.WriteByte('_')
		nameBuf.WriteString(c.Name)
		return nil
	}); err != nil {
		return "", err
	}
	return nameBuf.String(), nil
}

// generateNameForCheckConstraint generates a unique name for the given check constraint.
func generateNameForCheckConstraint(
	desc *MutableTableDescriptor, expr tree.Expr, inuseNames map[string]struct{},
) (string, error) {

	name, err := generateMaybeDuplicateNameForCheckConstraint(desc, expr)
	if err != nil {
		return "", err
	}
	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name.
	if _, ok := inuseNames[name]; ok {
		i := 1
		for {
			appended := fmt.Sprintf("%s%d", name, i)
			if _, ok := inuseNames[appended]; !ok {
				name = appended
				break
			}
			i++
		}
	}
	if inuseNames != nil {
		inuseNames[name] = struct{}{}
	}

	return name, nil
}

func makeShardCheckConstraintDef(
	desc *MutableTableDescriptor, buckets int, shardCol *sqlbase.ColumnDescriptor,
) (*tree.CheckConstraintTableDef, error) {
	values := &tree.Tuple{}
	for i := 0; i < buckets; i++ {
		const negative = false
		values.Exprs = append(values.Exprs, tree.NewNumVal(
			constant.MakeInt64(int64(i)),
			strconv.Itoa(i),
			negative))
	}
	return &tree.CheckConstraintTableDef{
		Expr: &tree.ComparisonExpr{
			Operator: tree.In,
			Left: &tree.ColumnItem{
				ColumnName: tree.Name(shardCol.Name),
			},
			Right: values,
		},
		Hidden: true,
	}, nil
}

func iterColDescriptorsInExpr(
	desc *sqlbase.MutableTableDescriptor, rootExpr tree.Expr, f func(*sqlbase.ColumnDescriptor) error,
) error {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, pgerror.Newf(pgcode.InvalidTableDefinition,
				"column %q not found, referenced in %q",
				c.ColumnName, rootExpr)
		}

		if err := f(col); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	return err
}

// validateComputedColumn checks that a computed column satisfies a number of
// validity constraints, for instance, that it typechecks.
func validateComputedColumn(
	desc *sqlbase.MutableTableDescriptor, d *tree.ColumnTableDef, semaCtx *tree.SemaContext,
) error {
	if d.HasDefaultExpr() {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"computed column %s cannot have default values", d.Name,
		)
	}

	dependencies := make(map[sqlbase.ColumnID]struct{})
	// First, check that no column in the expression is a computed column.
	if err := iterColDescriptorsInExpr(desc, d.Computed.Expr, func(c *sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				"computed column %s cannot reference other computed columns", d.Name)
		}
		dependencies[c.ID] = struct{}{}

		return nil
	}); err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		for _, id := range fk.OriginColumnIDs {
			if _, ok := dependencies[id]; !ok {
				// We don't depend on this column.
				continue
			}
			for _, action := range []sqlbase.ForeignKeyReference_Action{
				fk.OnDelete,
				fk.OnUpdate,
			} {
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE,
					sqlbase.ForeignKeyReference_SET_NULL,
					sqlbase.ForeignKeyReference_SET_DEFAULT:
					return pgerror.New(pgcode.InvalidTableDefinition,
						"computed columns cannot reference non-restricted FK columns")
				}
			}
		}
	}

	// Replace column references with typed dummies to allow typechecking.
	replacedExpr, _, err := replaceVars(desc, d.Computed.Expr)
	if err != nil {
		return err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(
		replacedExpr, d.Type, "computed column", semaCtx, false /* allowImpure */, false, string(d.Name),
	); err != nil {
		return err
	}

	return nil
}

// replaceVars replaces the occurrences of column names in an expression with
// dummies containing their type, so that they may be typechecked. It returns
// this new expression tree alongside a set containing the ColumnID of each
// column seen in the expression.
func replaceVars(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr,
) (tree.Expr, map[sqlbase.ColumnID]struct{}, error) {
	colIDs := make(map[sqlbase.ColumnID]struct{})
	newExpr, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, expr.String())
		}
		colIDs[col.ID] = struct{}{}
		// Convert to a dummy node of the correct type.
		return false, &dummyColumnItem{typ: &col.Type, name: c.ColumnName}, nil
	})
	return newExpr, colIDs, err
}

// MakeCheckConstraint makes a descriptor representation of a check from a def.
func MakeCheckConstraint(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	d *tree.CheckConstraintTableDef,
	inuseNames map[string]struct{},
	semaCtx *tree.SemaContext,
	tableName tree.TableName,
) (*sqlbase.TableDescriptor_CheckConstraint, error) {
	name := string(d.Name)

	if name == "" {
		var err error
		name, err = generateNameForCheckConstraint(desc, d.Expr, inuseNames)
		if err != nil {
			return nil, err
		}
	}

	expr, colIDsUsed, err := replaceVars(desc, d.Expr)
	if err != nil {
		return nil, err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(
		expr, types.Bool, "CHECK", semaCtx, true /* allowImpure */, false, "",
	); err != nil {
		return nil, err
	}

	colIDs := make([]sqlbase.ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		colIDs = append(colIDs, colID)
	}
	sort.Sort(sqlbase.ColumnIDs(colIDs))

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		tableName, sqlbase.ResultColumnsFromColDescs(
			desc.GetID(),
			desc.TableDesc().AllNonDropColumns(),
		),
	)

	expr, err = dequalifyColumnRefs(ctx, sourceInfo, d.Expr)
	if err != nil {
		return nil, err
	}

	return &sqlbase.TableDescriptor_CheckConstraint{
		Expr:      tree.Serialize(expr),
		Name:      name,
		ColumnIDs: colIDs,
		Hidden:    d.Hidden,
	}, nil
}

// incTelemetryForNewColumn increments relevant telemetry every time a new column
// is added to a table.
func incTelemetryForNewColumn(d *tree.ColumnTableDef) {
	telemetry.Inc(sqltelemetry.SchemaNewTypeCounter(d.Type.TelemetryName()))
	if d.IsComputed() {
		telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("computed"))
	}
	if d.HasDefaultExpr() {
		telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("default_expr"))
	}
	if d.Unique {
		telemetry.Inc(sqltelemetry.SchemaNewColumnTypeQualificationCounter("unique"))
	}
}

// generate storage type for table record in memory.
func generateTableFormatMetadata(tableMeta *sqlbase.TSTable, cols *[]sqlbase.ColumnDescriptor) {
	var zColOffset uint64
	for i := 0; i < len(*cols); i++ {
		(*cols)[i].TsCol.ColOffset = zColOffset
		if (*cols)[i].TsCol.VariableLengthType == sqlbase.StorageIndependentPage {
			zColOffset += 16
		} else {
			zColOffset += (*cols)[i].TsCol.StorageLen
		}
	}

	// count how many bytes is needed for bitmap based on the column number
	bitmapLen := (len(*cols) + 8 - 1) / 8
	// calculate the offset value for bitmap array in metadata
	tableMeta.BitmapOffset = zColOffset
	// total row size includes sum of column storage lengths and bitmap length
	tableMeta.RowSize = zColOffset + uint64(bitmapLen)
}

// checkTSColValidity checks the column options in DDL statements for TimeSeries tables
func checkTSColValidity(d *tree.ColumnTableDef) error {
	makeTsErr := func(msg string) error {
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s is not supported in timeseries table", msg)
	}
	if d.Type.Family() == types.DecimalFamily || d.Type.Oid() == oid.T_bytea {
		return pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported column type %s in timeseries table", d.Name, d.Type.Name())
	}

	if d.IsSerial {
		return makeTsErr("serial column")
	}
	if d.PrimaryKey.IsPrimaryKey {
		return makeTsErr("primary key")
	}
	if d.Unique {
		return makeTsErr("unique constraint")
	}
	//if d.HasDefaultExpr() {
	//	return makeTsErr("default Expr")
	//}
	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return makeTsErr("check constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return makeTsErr("referenced constraint")
	}
	if d.IsComputed() {
		return makeTsErr("computed column")
	}
	return nil
}

// createInstanceTable creates instance table including tag check, create and exec job
func createInstanceTable(
	params runParams, n *tree.CreateTable, db *sqlbase.DatabaseDescriptor,
) error {
	if n.UsingSource.ExplicitSchema {
		if n.UsingSource.ExplicitCatalog {
			if n.UsingSource.Schema() != tree.PublicSchema {
				return sqlbase.NewUndefinedRelationError(&n.UsingSource)
			}
			if n.UsingSource.Catalog() != db.Name {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"can not create instance table %s in another database %s",
					n.UsingSource.Catalog(), db.Name)
			}
		} else {
			if n.UsingSource.Schema() != tree.PublicSchema && n.UsingSource.Schema() != db.Name {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"can not create instance table %s in another database %s",
					n.UsingSource.Catalog(), db.Name)
			}
		}
	}
	// get the template table ID
	exists, tmplTblID, err := sqlbase.LookupObjectID(params.ctx, params.p.txn, db.ID, keys.PublicSchemaID, n.UsingSource.Table())
	if !exists {
		if err != nil {
			return err
		}
		return sqlbase.NewUndefinedRelationError(&n.UsingSource)

	}

	// get template table desc based on its ID
	tmplTbl, err := sqlbase.GetTableDescFromID(params.ctx, params.p.txn, tmplTblID)
	if err != nil {
		return err
	}

	if tmplTbl.TableType == tree.TimeseriesTable {
		return pgerror.Newf(pgcode.WrongObjectType, "can not create instance table use timeseries table: %s", tmplTbl.Name)
	}
	if err = tmplTbl.CheckTSTableStateValid(); err != nil {
		return err
	}
	// Check permissions to create instance table.
	if err := params.p.CheckPrivilege(params.ctx, tmplTbl, privilege.CREATE); err != nil {
		return err
	}

	tagValueForSet := make(map[string]string)
	var tagMeta []tree.Tag
	var cols []*sqlbase.ColumnDescriptor
	// inputRow is used to insert a row of data into the storage tag
	var inputRow tree.Exprs
	// colIndexs record the position of each tag corresponding to the specified value
	colIndexs := make(map[int]int, len(tmplTbl.Columns))
	inputRow = append(inputRow, tree.NewStrVal(n.Table.Table()))
	for i, tagColumn := range tmplTbl.GetColumns() {
		if tagColumn.TsCol.ColumnType == sqlbase.ColumnType_TYPE_TAG {
			tag := tree.Tag{
				TagName:  tree.Name(tagColumn.Name),
				TagType:  &tmplTbl.Columns[i].Type,
				Nullable: tagColumn.Nullable,
				ColID:    int(tagColumn.ID),
			}
			tagMeta = append(tagMeta, tag)
		}
		if tagColumn.IsTagCol() {
			cols = append(cols, &tmplTbl.Columns[i])
		}
		if tagColumn.IsPrimaryTagCol() {
			colIndexs[int(tagColumn.ID)] = 0
		}
	}
	// Check if the tag satisfies the following conditions when creating instance table
	//1.When a tag name is not specified, the default tag value is assigned in the order in which the template table was created, and cannot be defaulted.
	//2.When specifying a tag name, if not all are specified, the default tag assignment is NULL.
	//3.Check if the specified tag name exists on the template table
	//4.Check if the type of the tag matches
	//5.Check if the tag satisfies non-null constraints
	for i := range n.Tags {
		var tagType types.T
		var nullable bool
		// Only when no name is specified will there be a situation where TagName is empty
		if n.Tags[i].TagName == "" {
			if len(n.Tags) != len(tagMeta) {
				return pgerror.Newf(pgcode.Syntax, "Tags number mismatch: %d tags, %d values", len(tagMeta), len(n.Tags))
			}
			n.Tags[i].TagName = tagMeta[i].TagName
			tagType = *tagMeta[i].TagType
			nullable = tagMeta[i].Nullable
			inputRow = append(inputRow, n.Tags[i].TagVal)
			colIndexs[tagMeta[i].ColID] = i + 1
			// partial and fully specified names
		} else {
			var find bool
			for _, stag := range tagMeta {
				if n.Tags[i].TagName == stag.TagName {
					find = true
					tagType = *tagMeta[i].TagType
					nullable = stag.Nullable
					inputRow = append(inputRow, n.Tags[i].TagVal)
					colIndexs[stag.ColID] = i + 1
					break
				}
			}
			if !find {
				return pgerror.Newf(pgcode.UndefinedObject, "Tag %s does not exist on table %s", n.Tags[i].TagName, tmplTbl.Name)
			}
		}
		// if the tag no specified value, use NULL
		for _, stag := range tagMeta {
			if _, ok := colIndexs[stag.ColID]; !ok {
				colIndexs[stag.ColID] = -1
			}
		}
		// tag type check
		datum, err := checkTagValue(params, n.Tags[i].TagVal, tagType, nullable, string(n.Tags[i].TagName))
		if err != nil {
			return err
		}
		// check if have duplicate tag name
		if _, ok := tagValueForSet[string(n.Tags[i].TagName)]; ok {
			return pgerror.Newf(pgcode.DuplicateObject, "duplicate tag name: %s", n.Tags[i].TagName)
		}
		tagValueForSet[string(n.Tags[i].TagName)] = sqlbase.DatumToString(datum)
	}
	// generate instance table id
	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}
	crCtable := sqlbase.CreateCTable{
		DatabaseId: uint32(tmplTbl.ParentID),
		CTable: sqlbase.KWDBCTable{
			Id:   uint32(id),
			Name: n.Table.Table(),
		},
		StableId: uint32(tmplTblID),
	}

	var inputRows opt.RowsValue

	inputRows = append(inputRows, inputRow)

	payloadNodeMap, err := execbuilder.BuildInputForTSInsert(
		params.EvalContext(),
		inputRows,
		cols,
		colIndexs,
		uint32(db.ID),
		uint32(tmplTblID),
		false,
		uint32(tmplTbl.TsTable.TsVersion),
	)
	if err != nil {
		return err
	}
	for _, payloadVals := range payloadNodeMap {
		crCtable.CTable.NodeIDs = []int32{int32(payloadVals.NodeID)}
		crCtable.CTable.Payloads = [][]byte{payloadVals.PerNodePayloads[0].Payload}
		crCtable.CTable.PrimaryKeys = [][]byte{payloadVals.PerNodePayloads[0].PrimaryTagKey}
	}

	time, err := params.creationTimeForNewTableDescriptor()
	if err != nil {
		return err
	}
	cTbNameSpace := InitInstDescriptor(id, tmplTblID, n.Table.Table(), db.Name, tmplTbl.Name, time)
	cTbNameSpace.State = sqlbase.ChildDesc_ADD

	// clear the cache to avoid using cache when querying the template table after creating instance table
	params.p.execCfg.QueryCache.Clear()

	if err := writeInstTableMeta(params.ctx, params.p.Txn(), []sqlbase.InstNameSpace{cTbNameSpace}, false); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return sqlbase.NewRelationAlreadyExistsError(n.Table.Table())
		}
		return err
	}

	// Create a Job to perform the second stage of ts DDL.
	syncDetail := jobspb.SyncMetaCacheDetails{
		Type:     createKwdbInsTable,
		SNTable:  *tmplTbl,
		CTable:   crCtable,
		Database: *db,
	}
	jobID, err := params.p.createTSSchemaChangeJob(params.ctx, syncDetail, tree.AsStringWithFQNames(n, params.Ann()), params.p.txn)
	if err != nil {
		return err
	}

	// Actively commit a transaction, and read/write system table operations
	// need to be performed before this.
	if err := params.p.txn.Commit(params.ctx); err != nil {
		return err
	}

	// After the transaction commits successfully, execute the Job and wait for it to complete.
	if err = params.ExecCfg().JobRegistry.Run(
		params.ctx,
		params.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
		[]int64{jobID},
	); err != nil {
		return err
	}
	params.p.SetAuditTarget(uint32(id), n.Table.Table(), nil)
	return nil
}

// checkTagValue checks if input of tag value accord with tag type
func checkTagValue(
	params runParams, tagVal tree.Expr, tagType types.T, nullable bool, tagName string,
) (tree.Datum, error) {
	MatchErr := pgerror.Newf(pgcode.DatatypeMismatch,
		"value %s doesn't match type %s of column %q",
		tagVal.String(), tagType.String(), tree.ErrNameString(tagName))
	var val tree.Datum
	var err error
	switch v := tagVal.(type) {
	case *tree.NumVal:
		val, err = v.TSTypeCheck(tagName, &tagType)
	case *tree.StrVal:
		val, err = v.TSTypeCheck(tagName, &tagType, &params.p.semaCtx)
	case *tree.DBool:
		switch tagType.Family() {
		case types.BoolFamily:
			val = v
		case types.IntFamily:
			// Convert a bool value to a numeric value.
			if v == tree.DBoolTrue {
				val = tree.NewDInt(tree.DInt(1))
			} else {
				val = tree.NewDInt(tree.DInt(0))
			}
		default:
			return nil, MatchErr
		}
	case tree.DNullExtern:
		if !nullable {
			return nil, pgerror.Newf(pgcode.NotNullViolation, "can not use null tag value with not null constraint (tag %s)", tagName)
		}
		val = v

	default:
		return nil, MatchErr
	}
	if err != nil {
		return nil, err
	}

	datum, ok := val.(tree.Datum)
	if !ok {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "tag %s: wrong input attribute/tag data type", tagName)
	}
	return datum, nil
}

// checkTagType checks whether tag type is supported
func checkTagType(tagName tree.Name, tagType *types.T) (*types.T, error) {
	switch tagType.Oid() {
	case oid.T_bool, oid.T_float4, oid.T_float8, oid.T_int2, oid.T_int4, oid.T_int8:

	case oid.T_bpchar:
		if tagType.Width() == DefaultTypeWithLength {
			tagType = types.MakeChar(DefaultFixedLen)
		}
		if tagType.Width() >= MaxFixedLen {
			return nil, pgerror.Newf(
				pgcode.InvalidColumnDefinition,
				"tag %s: %d exceeded the maximum width limit of the type: %s",
				tagName, tagType.Width(), tagType.String())
		}
	case oid.T_varchar, types.T_varbytea:
		if tagType.Width() == DefaultTypeWithLength {
			if tagType.Oid() == oid.T_varbytea {
				tagType = types.MakeVarBytes(DefaultVariableLEN, tagType.TypeEngine())
			} else {
				tagType = types.MakeVarChar(DefaultVariableLEN, tagType.TypeEngine())
			}
		}
		if tagType.Width() > MaxVariableLen {
			return nil, pgerror.Newf(
				pgcode.InvalidColumnDefinition,
				"tag %s: %d exceeded the maximum width limit of the type: %s",
				tagName, tagType.Width(), tagType.String())
		}
	case types.T_nchar:
		if tagType.Width() == DefaultTypeWithLength {
			tagType = types.MakeNChar(DefaultFixedLen)
		}
		if tagType.Width() > MaxNCharLen {
			return nil, pgerror.Newf(
				pgcode.InvalidColumnDefinition,
				"tag %s: %d exceeded the maximum width limit of the type: %s",
				tagName, tagType.Width(), tagType.String())
		}
	default:
		return nil, pgerror.Newf(
			pgcode.WrongObjectType,
			"tag %s: unsupported tag type %s in timeseries table",
			tagName, tagType.String())
	}
	return tagType, nil
}

func checkChildTable(ctx context.Context, txn *kv.Txn, dbName string, tn tree.TableName) error {
	_, found, err := sqlbase.ResolveInstanceName(ctx, txn, dbName, tn.Table())
	if err != nil {
		return err
	}
	if found {
		return sqlbase.NewRelationAlreadyExistsError(tn.Table())
	}
	return nil
}

// MaxResolution means max MaxResolution is 24 hours.
const MaxResolution = 24 * 3600

// InvalidLifetime is used internally
const InvalidLifetime = MaxLifeTime + 1

// MaxLifeTime means max lifetime on table which is 1000 years.
const MaxLifeTime = 1000 * 365 * 24 * 3600

// DefaultActiveTime means default activetime on table which is 1 day.
const DefaultActiveTime = 24 * 3600

// DefaultPartitionInterval means default Partition Interval on table which is 10 day.
const DefaultPartitionInterval = 10 * 24 * 3600

// retention includes original definition of retention,
// and parsed resolution, keep duration, sample method and lifetime.
// info above is used to fill in some fields in descriptor.
type retention struct {
	originRetention []string //original definition of retention, used for SHOW CREATE
	resolution      []uint64 //sampling(aggregation) interval
	keepDuration    []uint64 //the time range of the data to be downsampled
	samples         []string //down sample method
	lifetime        uint64   //lifetime of data
}

// columnDef saves the name and type of column, used for column validation
type columnDef struct {
	name string
	typ  oid.Oid
}

// buildColumnDef retrieves the column name and type from TableDefs.
func buildColumnDef(defs tree.TableDefs) ([]columnDef, map[string]oid.Oid) {
	var colDefs []columnDef
	var colTyp = make(map[string]oid.Oid)
	// loop TableDefs, retrieves column name and type.
	for i, def := range defs {
		if de, ok := def.(*tree.ColumnTableDef); ok {
			if i == 0 && de.Type == types.Timestamp {
				continue
			}
			var col columnDef
			col.name = de.Name.String()
			col.typ = de.Type.Oid()
			colDefs = append(colDefs, col)
			colTyp[col.name] = col.typ
		}
	}
	return colDefs, colTyp
}

// checkRetentionForCreate validates the definition of downSampling in CREATE TABLE AST.
func checkRetentionForCreate(defs tree.TableDefs, d tree.DownSampling) (retention, error) {
	defArray, colTyp := buildColumnDef(defs)
	ret, err := checkRetention(defArray, colTyp, d)
	if err != nil {
		return retention{}, err
	}
	return ret, nil
}

// checkRetention is used for parsing and validating retention-related information,
// including checking and parsing keep duration, resolution, sample, lifetime, etc.
//
// Parameters:
// - colDefs: definition of columns
// - colTyp: type of columns
// - d: AST of downSampling message
//
// Returns:
// - retention: retention information used for build descriptor.
// - error
func checkRetention(
	colDefs []columnDef, colTyp map[string]oid.Oid, d tree.DownSampling,
) (retention, error) {
	var ret retention
	// oriRetention represents user-input retention
	var oriRetention string
	oriRetention = timeInputToString(d.KeepDurationOrLifetime)

	timeFirstKeep := getTimeFromTimeInput(d.KeepDurationOrLifetime)
	if timeFirstKeep < 0 || timeFirstKeep > MaxLifeTime {
		return ret, pgerror.Newf(pgcode.InvalidParameterValue, "retention %d%s is out of range",
			d.KeepDurationOrLifetime.Value, d.KeepDurationOrLifetime.Unit)
	}

	// if user only provides lifetime
	if d.Retentions == nil {
		ret.originRetention = append(ret.originRetention, oriRetention)
		ret.lifetime = uint64(timeFirstKeep)
		// only lifetime provided, and the sample list is not nil, should return error
		if d.Methods != nil {
			return ret, pgerror.New(pgcode.InvalidParameterValue, "sample list should be provided")
		}
		return ret, nil
	}
	return ret, pgerror.New(pgcode.FeatureNotSupported, "only support lifeTime for now")
}

func timeInputToString(input tree.TimeInput) string {
	return strconv.Itoa(int(input.Value)) + input.Unit
}

// getTimeFromTimeInput convert time in different unit to second
func getTimeFromTimeInput(input tree.TimeInput) int64 {

	switch input.Unit {
	case "s", "second":
		return input.Value
	case "m", "minute":
		return input.Value * 60
	case "h", "hour":
		return input.Value * 60 * 60
	case "d", "day":
		return input.Value * 24 * 60 * 60
	case "w", "week":
		return input.Value * 24 * 60 * 60 * 7
	case "mon", "month":
		return input.Value * 24 * 60 * 60 * 30
	case "y", "year":
		return input.Value * 24 * 60 * 60 * 365
	default:
		return -1
	}
}

// checkPrimaryTag validates the definition of primary tag
func checkPrimaryTag(tagColumn sqlbase.ColumnDescriptor) error {
	if tagColumn.Nullable {
		return pgerror.Newf(pgcode.NotNullViolation, "tag %s can not be a nullable tag as primary tag", tagColumn.Name)
	}
	switch tagColumn.Type.Oid() {
	case oid.T_float4, oid.T_float8, oid.T_varbytea:
		return pgerror.Newf(pgcode.WrongObjectType,
			"data type %s is not supported for primary tag %s", tagColumn.Type.String(), tagColumn.Name)
	case oid.T_varchar:
		if tagColumn.Type.Width() > MaxPrimaryTagWidth {
			return pgerror.Newf(pgcode.InvalidColumnDefinition,
				"tag %s: %d exceeded the maximum width limit of the type %s as primary tag",
				tagColumn.Name, tagColumn.Type.Width(), tagColumn.Type.String())
		}
	}
	return nil
}

// distributeAndDuplicateOfCreateTSTable makes distribute and duplicate jobs when creating time-series table
// which including getting node id, hash partitions and predistribution, relocate
func distributeAndDuplicateOfCreateTSTable(
	params runParams, desc sqlbase.MutableTableDescriptor,
) ([]roachpb.AdminSplitInfoForTs, error) {
	var preDistReplicas []roachpb.ReplicaDescriptor
	partitions, err := api.GetDistributeInfo(params.ctx, uint32(desc.ID))
	if err != nil {
		return nil, errors.Wrap(err, "PreDistributionError: get distribute info failed")
	}
	if params.ExecCfg().StartMode == StartSingleReplica {
		preDist, err := api.PreDistributeBySingleReplica(params.ctx, params.p.txn, partitions)
		if err != nil {
			return nil, errors.Wrap(err, "PreDistributionError: get pre distribute info failed")
		}
		preDistReplicas = preDist
	}

	type pointGroup struct {
		point     int32
		timestamp int64
	}
	var pointGroups []pointGroup
	var splitInfo []roachpb.AdminSplitInfoForTs
	for index, hashPartition := range partitions {
		startPoint := hashPartition.StartPoint
		var info roachpb.AdminSplitInfoForTs
		pointGroups = append(pointGroups, pointGroup{int32(startPoint), hashPartition.StartTimeStamp})
		splitKey := sqlbase.MakeTsRangeKey(desc.ID, uint64(startPoint), hashPartition.StartTimeStamp)
		if params.ExecCfg().StartMode == StartSingleReplica {
			info = roachpb.AdminSplitInfoForTs{
				SplitKey: splitKey,
				PreDist:  []roachpb.ReplicaDescriptor{preDistReplicas[index]},
			}
		} else {
			info = roachpb.AdminSplitInfoForTs{
				SplitKey: splitKey,
			}
		}

		splitInfo = append(splitInfo, info)
	}

	// split ts range
	sort.Slice(pointGroups, func(i, j int) bool { return pointGroups[i].point < pointGroups[j].point })
	for _, p := range pointGroups {
		spanKey := sqlbase.MakeTsRangeKey(desc.ID, uint64(p.point), p.timestamp)
		// TODO(kang): send split key
		var tmp = []int32{p.point}
		var timestamps = []int64{p.timestamp}
		if err := params.extendedEvalCtx.ExecCfg.DB.AdminSplitTs(params.ctx, spanKey, uint32(desc.ID), tmp, timestamps, false); err != nil {
			return nil, errors.Wrap(err, "PreDistributionError: split failed")
		}
	}

	// return split info for relocation
	if params.extendedEvalCtx.ExecCfg.StartMode == StartSingleReplica {
		return splitInfo, nil
	}
	return nil, nil
}
