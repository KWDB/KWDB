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
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/querycache"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/transform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
)

// extendedEvalContext extends tree.EvalContext with fields that are needed for
// distsql planning.
type extendedEvalContext struct {
	tree.EvalContext

	SessionMutator *sessionDataMutator

	// SessionID for this connection.
	SessionID ClusterWideID

	// VirtualSchemas can be used to access virtual tables.
	VirtualSchemas VirtualTabler

	// Tracing provides access to the session's tracing interface. Changes to the
	// tracing state should be done through the sessionDataMutator.
	Tracing *SessionTracing

	// StatusServer gives access to the Status service. Used to cancel queries.
	StatusServer serverpb.StatusServer

	// MemMetrics represent the group of metrics to which execution should
	// contribute.
	MemMetrics *MemoryMetrics

	// Tables points to the Session's table collection (& cache).
	Tables *TableCollection

	ExecCfg *ExecutorConfig

	DistSQLPlanner *DistSQLPlanner

	TxnModesSetter txnModesSetter

	// Jobs refers to jobs in extraTxnState. Jobs is a pointer to a jobsCollection
	// which is a slice because we need calls to resetExtraTxnState to reset the
	// jobsCollection.
	Jobs *jobsCollection

	// SchemaChangeJobCache refers to schemaChangeJobsCache in extraTxnState.
	SchemaChangeJobCache map[sqlbase.ID]*jobs.Job

	schemaAccessors *schemaInterface

	sqlStatsCollector *sqlStatsCollector

	// IsInternalSQL marks whether the SQL statement is executed internally
	IsInternalSQL bool

	// IsDisplayed is true when the flow spec of the SQL had displayed.
	IsDisplayed bool
}

// copy returns a deep copy of ctx.
func (ctx *extendedEvalContext) copy() *extendedEvalContext {
	cpy := *ctx
	cpy.EvalContext = *ctx.EvalContext.Copy()
	return &cpy
}

// QueueJob creates a new job from record and queues it for execution after
// the transaction commits.
func (ctx *extendedEvalContext) QueueJob(record jobs.Record) (*jobs.Job, error) {
	job, err := ctx.ExecCfg.JobRegistry.CreateJobWithTxn(
		ctx.Context,
		record,
		ctx.Txn,
	)
	if err != nil {
		return nil, err
	}
	*ctx.Jobs = append(*ctx.Jobs, *job.ID())
	return job, nil
}

// schemaInterface provides access to the database and table descriptors.
// See schema_accessors.go.
type schemaInterface struct {
	physical SchemaAccessor
	logical  SchemaAccessor
}

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution. It is logically
// scoped to the execution of a single statement, and should not be used to
// execute multiple statements. It is not safe to use the same planner from
// multiple goroutines concurrently.
//
// planners are usually created by using the newPlanner method on a Session.
// If one needs to be created outside of a Session, use makeInternalPlanner().
type planner struct {
	txn *kv.Txn

	// Reference to the corresponding sql Statement for this query.
	stmt *Statement

	// Contexts for different stages of planning and execution.
	semaCtx         tree.SemaContext
	extendedEvalCtx extendedEvalContext

	// sessionDataMutator is used to mutate the session variables. Read
	// access to them is provided through evalCtx.
	sessionDataMutator *sessionDataMutator

	// execCfg is used to access the server configuration for the Executor.
	execCfg *ExecutorConfig

	preparedStatements preparedStatementsAccessor

	// avoidCachedDescriptors, when true, instructs all code that
	// accesses table/view descriptors to force reading the descriptors
	// within the transaction. This is necessary to read descriptors
	// from the store for:
	// 1. Descriptors that are part of a schema change but are not
	// modified by the schema change. (reading a table in CREATE VIEW)
	// 2. Disable the use of the table cache in tests.
	avoidCachedDescriptors bool

	// If set, the planner should skip checking for the SELECT privilege when
	// initializing plans to read from a table. This should be used with care.
	skipSelectPrivilegeChecks bool

	// autoCommit indicates whether we're planning for an implicit transaction.
	// If autoCommit is true, the plan is allowed (but not required) to commit the
	// transaction along with other KV operations. Committing the txn might be
	// beneficial because it may enable the 1PC optimization.
	//
	// NOTE: plan node must be configured appropriately to actually perform an
	// auto-commit. This is dependent on information from the optimizer.
	autoCommit bool

	// discardRows is set if we want to discard any results rather than sending
	// them back to the client. Used for testing/benchmarking. Note that the
	// resulting schema or the plan are not affected.
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// cancelChecker is used by planNodes to check for cancellation of the associated
	// query.
	cancelChecker *sqlbase.CancelChecker

	// collectBundle is set when we are collecting a diagnostics bundle for a
	// statement; it triggers saving of extra information like the plan string.
	collectBundle bool

	// isPreparing is true if this planner is currently preparing.
	isPreparing bool

	// curPlan collects the properties of the current plan being prepared. This state
	// is undefined at the beginning of the planning of each new statement, and cannot
	// be reused for an old prepared statement after a new statement has been prepared.
	curPlan planTop

	// Avoid allocations by embedding commonly used objects and visitors.
	txCtx                 transform.ExprTransformContext
	nameResolutionVisitor sqlbase.NameResolutionVisitor
	tableName             tree.TableName

	// Use a common datum allocator across all the plan nodes. This separates the
	// plan lifetime from the lifetime of returned results allowing plan nodes to
	// be pool allocated.
	alloc sqlbase.DatumAlloc

	// optPlanningCtx stores the optimizer planning context, which contains
	// data structures that can be reused between queries (for efficiency).
	optPlanningCtx optPlanningCtx

	// noticeSender allows the sending of notices.
	// Do not use this object directly; use the SendClientNotice() method
	// instead.
	noticeSender noticeSender

	queryCacheSession querycache.Session

	//ShortCircuit is true when stable without child tables.
	ShortCircuit bool

	// TsInScopeFlag is true if there is a TS table in query
	TsInScopeFlag bool
}

// ExecutorConfig implements Planner interface.
func (p *planner) ExecutorConfig() interface{} {
	return p.execCfg
}

func (ctx *extendedEvalContext) setSessionID(sessionID ClusterWideID) {
	ctx.SessionID = sessionID
}

// noteworthyInternalMemoryUsageBytes is the minimum size tracked by each
// internal SQL pool before the pool starts explicitly logging overall usage
// growth in the log.
var noteworthyInternalMemoryUsageBytes = envutil.EnvOrDefaultInt64("KWBASE_NOTEWORTHY_INTERNAL_MEMORY_USAGE", 1<<20 /* 1 MB */)

// NewInternalPlanner is an exported version of newInternalPlanner. It
// returns an interface{} so it can be used outside of the sql package.
func NewInternalPlanner(
	opName string, txn *kv.Txn, user string, memMetrics *MemoryMetrics, execCfg *ExecutorConfig,
) (interface{}, func()) {
	return newInternalPlanner(opName, txn, user, memMetrics, execCfg)
}

// newInternalPlanner creates a new planner instance for internal usage. This
// planner is not associated with a sql session.
//
// Since it can't be reset, the planner can be used only for planning a single
// statement.
//
// Returns a cleanup function that must be called once the caller is done with
// the planner.
func newInternalPlanner(
	opName string, txn *kv.Txn, user string, memMetrics *MemoryMetrics, execCfg *ExecutorConfig,
) (*planner, func()) {
	// We need a context that outlives all the uses of the planner (since the
	// planner captures it in the EvalCtx, and so does the cleanup function that
	// we're going to return. We just create one here instead of asking the caller
	// for a ctx with this property. This is really ugly, but the alternative of
	// asking the caller for one is hard to explain. What we need is better and
	// separate interfaces for planning and running plans, which could take
	// suitable contexts.
	ctx := logtags.AddTag(context.Background(), opName, "")

	sd := &sessiondata.SessionData{
		SearchPath:    sqlbase.DefaultSearchPath,
		User:          user,
		Database:      "system",
		SequenceState: sessiondata.NewSequenceState(),
		DataConversion: sessiondata.DataConversionConfig{
			Location: time.UTC,
		},
		UserDefinedVars: make(map[string]interface{}),
	}
	// The table collection used by the internal planner does not rely on the
	// databaseCache and there are no subscribers to the databaseCache, so we can
	// leave it uninitialized.
	tables := &TableCollection{
		leaseMgr: execCfg.LeaseManager,
		settings: execCfg.Settings,
	}
	dataMutator := &sessionDataMutator{
		data: sd,
		defaults: SessionDefaults{SessionDefaultsMp: map[string]string{
			"application_name": "kwdb-internal",
			"database":         "system",
		}},
		settings:           execCfg.Settings,
		paramStatusUpdater: &noopParamStatusUpdater{},
		setCurTxnReadOnly:  func(bool) {},
	}

	var ts time.Time
	if txn != nil {
		readTimestamp := txn.ReadTimestamp()
		if readTimestamp == (hlc.Timestamp{}) {
			panic("makeInternalPlanner called with a transaction without timestamps")
		}
		ts = readTimestamp.GoTime()
	}

	p := &planner{execCfg: execCfg}

	p.txn = txn
	p.stmt = nil
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.Location = &sd.DataConversion.Location
	p.semaCtx.SearchPath = sd.SearchPath
	p.semaCtx.UserDefinedVars = sd.UserDefinedVars

	plannerMon := mon.MakeUnlimitedMonitor(ctx,
		fmt.Sprintf("internal-planner.%s.%s", user, opName),
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		noteworthyInternalMemoryUsageBytes, execCfg.Settings)

	p.extendedEvalCtx = internalExtendedEvalCtx(
		ctx, sd, dataMutator, tables, txn, ts, ts, execCfg, &plannerMon,
	)
	p.extendedEvalCtx.Planner = p
	p.extendedEvalCtx.PrivilegedAccessor = p
	p.extendedEvalCtx.SessionAccessor = p
	p.extendedEvalCtx.ClientNoticeSender = p
	p.extendedEvalCtx.Sequence = p
	p.extendedEvalCtx.ClusterID = execCfg.ClusterID()
	p.extendedEvalCtx.ClusterName = execCfg.RPCContext.ClusterName()
	p.extendedEvalCtx.NodeID = execCfg.NodeID.Get()
	p.extendedEvalCtx.Locality = execCfg.Locality

	p.sessionDataMutator = dataMutator
	p.autoCommit = false

	p.extendedEvalCtx.MemMetrics = memMetrics
	p.extendedEvalCtx.ExecCfg = execCfg
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.extendedEvalCtx.Tables = tables

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)

	return p, func() {
		// Note that we capture ctx here. This is only valid as long as we create
		// the context as explained at the top of the method.
		plannerMon.Stop(ctx)
	}
}

// internalExtendedEvalCtx creates an evaluation context for an "internal
// planner". Since the eval context is supposed to be tied to a session and
// there's no session to speak of here, different fields are filled in here to
// keep the tests using the internal planner passing.
func internalExtendedEvalCtx(
	ctx context.Context,
	sd *sessiondata.SessionData,
	dataMutator *sessionDataMutator,
	tables *TableCollection,
	txn *kv.Txn,
	txnTimestamp time.Time,
	stmtTimestamp time.Time,
	execCfg *ExecutorConfig,
	plannerMon *mon.BytesMonitor,
) extendedEvalContext {
	var evalContextTestingKnobs tree.EvalContextTestingKnobs
	var statusServer serverpb.StatusServer
	evalContextTestingKnobs = execCfg.EvalContextTestingKnobs
	statusServer = execCfg.StatusServer

	return extendedEvalContext{
		EvalContext: tree.EvalContext{
			Txn:              txn,
			SessionData:      sd,
			TxnReadOnly:      false,
			TxnImplicit:      true,
			Settings:         execCfg.Settings,
			Context:          ctx,
			Mon:              plannerMon,
			TestingKnobs:     evalContextTestingKnobs,
			StmtTimestamp:    stmtTimestamp,
			TxnTimestamp:     txnTimestamp,
			InternalExecutor: execCfg.InternalExecutor,
		},
		SessionMutator:  dataMutator,
		VirtualSchemas:  execCfg.VirtualSchemas,
		Tracing:         &SessionTracing{},
		StatusServer:    statusServer,
		Tables:          tables,
		ExecCfg:         execCfg,
		schemaAccessors: newSchemaInterface(tables, execCfg.VirtualSchemas),
		DistSQLPlanner:  execCfg.DistSQLPlanner,
	}
}

func (p *planner) PhysicalSchemaAccessor() SchemaAccessor {
	return p.extendedEvalCtx.schemaAccessors.physical
}

func (p *planner) LogicalSchemaAccessor() SchemaAccessor {
	return p.extendedEvalCtx.schemaAccessors.logical
}

// Note: if the context will be modified, use ExtendedEvalContextCopy instead.
func (p *planner) ExtendedEvalContext() *extendedEvalContext {
	return &p.extendedEvalCtx
}

func (p *planner) ExtendedEvalContextCopy() *extendedEvalContext {
	return p.extendedEvalCtx.copy()
}

func (p *planner) CurrentDatabase() string {
	return p.SessionData().Database
}

func (p *planner) CurrentSearchPath() sessiondata.SearchPath {
	return p.SessionData().SearchPath
}

// EvalContext() provides convenient access to the planner's EvalContext().
func (p *planner) EvalContext() *tree.EvalContext {
	return &p.extendedEvalCtx.EvalContext
}

func (p *planner) Tables() *TableCollection {
	return p.extendedEvalCtx.Tables
}

// GetStmt get the sql from planner if the stmt not nil.
func (p *planner) GetStmt() string {
	if p.stmt != nil {
		return p.stmt.SQL
	}
	return ""
}

// ExecCfg implements the PlanHookState interface.
func (p *planner) ExecCfg() *ExecutorConfig {
	return p.extendedEvalCtx.ExecCfg
}

func (p *planner) LeaseMgr() *LeaseManager {
	return p.Tables().leaseMgr
}

func (p *planner) Txn() *kv.Txn {
	return p.txn
}

func (p *planner) User() string {
	return p.SessionData().User
}

func (p *planner) TemporarySchemaName() string {
	return temporarySchemaName(p.ExtendedEvalContext().SessionID)
}

// DistSQLPlanner returns the DistSQLPlanner
func (p *planner) DistSQLPlanner() *DistSQLPlanner {
	return p.extendedEvalCtx.DistSQLPlanner
}

// ParseType implements the tree.EvalPlanner interface.
// We define this here to break the dependency from eval.go to the parser.
func (p *planner) ParseType(sql string) (*types.T, error) {
	return parser.ParseType(sql)
}

// ParseQualifiedTableName implements the tree.EvalDatabase interface.
// This exists to get around a circular dependency between sql/sem/tree and
// sql/parser. sql/parser depends on tree to make objects, so tree cannot import
// ParseQualifiedTableName even though some builtins need that function.
// TODO(jordan): remove this once builtins can be moved outside of sql/sem/tree.
func (p *planner) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return parser.ParseQualifiedTableName(sql)
}

// ResolveTableName implements the tree.EvalDatabase interface.
func (p *planner) ResolveTableName(ctx context.Context, tn *tree.TableName) (tree.ID, error) {
	desc, err := ResolveExistingObject(ctx, p, tn, tree.ObjectLookupFlagsWithRequired(), ResolveAnyDescType)
	if err != nil {
		return 0, err
	}
	if desc.TableType == tree.InstanceTable {
		// get child table ID
		cn, found, err := sqlbase.ResolveInstanceName(ctx, p.Txn(), tn.Catalog(), tn.Table())
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, sqlbase.NewUndefinedTableError(tn.Table())
		}
		return tree.ID(cn.InstTableID), nil
	}
	return tree.ID(desc.ID), nil
}

// LookupTableByID looks up a table, by the given descriptor ID. Based on the
// CommonLookupFlags, it could use or skip the TableCollection cache. See
// TableCollection.getTableVersionByID for how it's used.
func (p *planner) LookupTableByID(ctx context.Context, tableID sqlbase.ID) (row.TableEntry, error) {
	flags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{AvoidCached: p.avoidCachedDescriptors}}
	table, err := p.Tables().getTableVersionByID(ctx, p.txn, tableID, flags)
	if err != nil {
		if err == errTableAdding {
			return row.TableEntry{IsAdding: true}, nil
		}
		return row.TableEntry{}, err
	}
	return row.TableEntry{Desc: table}, nil
}

// TypeAsString enforces (not hints) that the given expression typechecks as a
// string and returns a function that can be called to get the string value
// during (planNode).Start.
// To also allow NULLs to be returned, use TypeAsStringOrNull() instead.
func (p *planner) TypeAsString(e tree.Expr, op string) (func() (string, error), error) {
	typedE, err := tree.TypeCheckAndRequire(e, &p.semaCtx, types.String, op)
	if err != nil {
		return nil, err
	}
	evalFn := p.makeStringEvalFn(typedE)
	return func() (string, error) {
		isNull, str, err := evalFn()
		if err != nil {
			return "", err
		}
		if isNull {
			return "", errors.Errorf("expected string, got NULL")
		}
		return str, nil
	}, nil
}

// TypeAsStringOrNull is like TypeAsString but allows NULLs.
func (p *planner) TypeAsStringOrNull(e tree.Expr, op string) (func() (bool, string, error), error) {
	typedE, err := tree.TypeCheckAndRequire(e, &p.semaCtx, types.String, op)
	if err != nil {
		return nil, err
	}
	return p.makeStringEvalFn(typedE), nil
}

func (p *planner) makeStringEvalFn(typedE tree.TypedExpr) func() (bool, string, error) {
	return func() (bool, string, error) {
		d, err := typedE.Eval(p.EvalContext())
		if err != nil {
			return false, "", err
		}
		if d == tree.DNull {
			return true, "", nil
		}
		str, ok := d.(*tree.DString)
		if !ok {
			return false, "", errors.Errorf("failed to cast %T to string", d)
		}
		return false, string(*str), nil
	}
}

// KVStringOptValidate indicates the requested validation of a TypeAsStringOpts
// option.
type KVStringOptValidate string

// KVStringOptValidate values
const (
	KVStringOptAny            KVStringOptValidate = `any`
	KVStringOptRequireNoValue KVStringOptValidate = `no-value`
	KVStringOptRequireValue   KVStringOptValidate = `value`
)

// evalStringOptions evaluates the KVOption values as strings and returns them
// in a map. Options with no value have an empty string.
func evalStringOptions(
	evalCtx *tree.EvalContext, opts []exec.KVOption, optValidate map[string]KVStringOptValidate,
) (map[string]string, error) {
	res := make(map[string]string, len(opts))
	for _, opt := range opts {
		k := opt.Key
		validate, ok := optValidate[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}
		val, err := opt.Value.Eval(evalCtx)
		if err != nil {
			return nil, err
		}
		if val == tree.DNull {
			if validate == KVStringOptRequireValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			res[k] = ""
		} else {
			if validate == KVStringOptRequireNoValue {
				return nil, errors.Errorf("option %q does not take a value", k)
			}
			str, ok := val.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected string value, got %T", val)
			}
			res[k] = string(*str)
		}
	}
	return res, nil
}

// TypeAsStringOpts enforces (not hints) that the given expressions
// typecheck as strings, and returns a function that can be called to
// get the string value during (planNode).Start.
func (p *planner) TypeAsStringOpts(
	opts tree.KVOptions, optValidate map[string]KVStringOptValidate,
) (func() (map[string]string, error), error) {
	typed := make(map[string]tree.TypedExpr, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		validate, ok := optValidate[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}

		if opt.Value == nil {
			if validate == KVStringOptRequireValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			typed[k] = nil
			continue
		}
		if validate == KVStringOptRequireNoValue {
			return nil, errors.Errorf("option %q does not take a value", k)
		}
		r, err := tree.TypeCheckAndRequire(opt.Value, &p.semaCtx, types.String, k)
		if err != nil {
			return nil, err
		}
		if _, ok := typed[k]; ok {
			return nil, errors.Errorf("specifying the same option %q is not supported", k)
		}
		typed[k] = r
	}
	fn := func() (map[string]string, error) {
		res := make(map[string]string, len(typed))
		for name, e := range typed {
			if e == nil {
				res[name] = ""
				continue
			}
			d, err := e.Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
			str, ok := d.(*tree.DString)
			if !ok {
				return res, errors.Errorf("failed to cast %T to string", d)
			}
			res[name] = string(*str)
		}
		return res, nil
	}
	return fn, nil
}

// TypeAsStringArray enforces (not hints) that the given expressions all typecheck as
// strings and returns a function that can be called to get the string values
// during (planNode).Start.
func (p *planner) TypeAsStringArray(exprs tree.Exprs, op string) (func() ([]string, error), error) {
	typedExprs := make([]tree.TypedExpr, len(exprs))
	for i := range exprs {
		typedE, err := tree.TypeCheckAndRequire(exprs[i], &p.semaCtx, types.String, op)
		if err != nil {
			return nil, err
		}
		typedExprs[i] = typedE
	}
	fn := func() ([]string, error) {
		strs := make([]string, len(exprs))
		for i := range exprs {
			d, err := typedExprs[i].Eval(p.EvalContext())
			if err != nil {
				return nil, err
			}
			str, ok := d.(*tree.DString)
			if !ok {
				return strs, errors.Errorf("failed to cast %T to string", d)
			}
			strs[i] = string(*str)
		}
		return strs, nil
	}
	return fn, nil
}

// SessionData is part of the PlanHookState interface.
func (p *planner) SessionData() *sessiondata.SessionData {
	return p.EvalContext().SessionData
}

// txnModesSetter is an interface used by SQL execution to influence the current
// transaction.
type txnModesSetter interface {
	// setTransactionModes updates some characteristics of the current
	// transaction.
	// asOfTs, if not empty, is the evaluation of modes.AsOf.
	setTransactionModes(modes tree.TransactionModes, asOfTs hlc.Timestamp) error
}

// MakeNewPlanAndRunForTsInsert
/* @Description：create an internal planner as the planner and run;
 * @In nodeIDs: node of exec ts insert;
 * @In payloads: insert data;
 * @In rowNums: number of entries written to each node
 * @In primaryTagKey: primary tag
 * @Return 1: rowAffectNum
 * @Return 2: error
 */
func (p *planner) MakeNewPlanAndRunForTsInsert(
	ctx context.Context, evalCtx *tree.EvalContext, param tree.TSInsertSelectParam,
) (int, error) {
	if payloadNodeMap, ok := param.(*map[int]*sqlbase.PayloadForDistTSInsert); ok {
		tsIns := tsInsertNodePool.Get().(*tsInsertNode)
		for _, payloadVals := range *payloadNodeMap {
			tsIns.nodeIDs = append(tsIns.nodeIDs, payloadVals.NodeID)
			tsIns.allNodePayloadInfos = append(tsIns.allNodePayloadInfos, payloadVals.PerNodePayloads)
		}
		return p.makeNewPlanAndRun(ctx, evalCtx.Txn, tsIns)
	}
	return 0, nil
}

// GetNodeIDNumber return node id.
func (p *planner) GetNodeIDNumber() int32 {
	if p.execCfg != nil && p.execCfg.NodeID != nil {
		return int32(p.execCfg.NodeID.Get())
	}
	return 0
}
