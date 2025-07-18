// Copyright 2018 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/querycache"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var queryCacheEnabled = settings.RegisterBoolSetting(
	"sql.query_cache.enabled", "enable the query cache", true,
)

// cluster leve switch for get hints
var autonomicOptimizationEnable = settings.RegisterBoolSetting(
	"sql.autonomic_optimization.enabled", "enable the autonomic_optimization", true,
)

// prepareUsingOptimizer builds a memo for a prepared statement and populates
// the following stmt.Prepared fields:
//   - Columns
//   - Types
//   - AnonymizedStr
//   - Memo (for reuse during exec, if appropriate).
func (p *planner) prepareUsingOptimizer(ctx context.Context) (planFlags, error) {
	stmt := p.stmt

	opc := &p.optPlanningCtx
	opc.reset()

	stmt.Prepared.AnonymizedStr = anonymizeStmt(stmt.AST)

	switch stmt.AST.(type) {
	case *tree.AlterIndex, *tree.AlterTable, *tree.AlterSequence, *tree.AlterSchedule,
		*tree.BeginTransaction,
		*tree.CommentOnColumn, *tree.CommentOnDatabase, *tree.CommentOnIndex, *tree.CommentOnTable,
		*tree.CommentOnProcedure,
		*tree.CommitTransaction,
		*tree.CopyFrom, *tree.CreateDatabase, *tree.CreateFunction, *tree.CreateIndex, *tree.CreateView,
		*tree.CreateSchedule,
		*tree.CreateSequence,
		*tree.CreateStats,
		*tree.Deallocate, *tree.Discard, *tree.DropDatabase, *tree.DropIndex,
		*tree.DropTable, *tree.DropView, *tree.DropSequence,
		*tree.Execute,
		*tree.Grant, *tree.GrantRole,
		*tree.Prepare, *tree.PauseSchedule,
		*tree.RebalanceTsData,
		*tree.ReleaseSavepoint, *tree.RenameColumn, *tree.RenameDatabase,
		*tree.RenameIndex, *tree.RenameTable, *tree.Revoke, *tree.RevokeRole, *tree.ResumeSchedule,
		*tree.ReplicateSetSecondary, *tree.ReplicateSetRole,
		*tree.ReplicationControl,
		*tree.RollbackToSavepoint, *tree.RollbackTransaction,
		*tree.Savepoint, *tree.SetTransaction, *tree.SetTracing, *tree.SetSessionAuthorizationDefault,
		*tree.SetSessionCharacteristics:
		// These statements do not have result columns and do not support placeholders
		// so there is no need to do anything during prepare.
		//
		// Some of these statements (like BeginTransaction) aren't supported by the
		// optbuilder so they would error out. Others (like CreateIndex) have planning
		// code that can introduce unnecessary txn retries (because of looking up
		// descriptors and such).
		return opc.flags, nil

	case *tree.ExplainAnalyzeDebug:
		// This statement returns result columns but does not support placeholders,
		// and we don't want to do anything during prepare.
		if len(p.semaCtx.Placeholders.Types) != 0 {
			return 0, errors.Errorf("%s does not support placeholders", stmt.AST.StatementTag())
		}
		stmt.Prepared.Columns = sqlbase.ExplainAnalyzeDebugColumns
		return opc.flags, nil
	}

	if opc.useCache {
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, stmt.SQL)
		if ok && cachedData.PrepareMetadata != nil {
			pm := cachedData.PrepareMetadata
			// Check that the type hints match (the type hints affect type checking).
			if !pm.TypeHints.Equals(p.semaCtx.Placeholders.TypeHints) {
				opc.log(ctx, "query cache hit but type hints don't match")
			} else {
				isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog)
				if err != nil {
					return 0, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.AnonymizedStr = pm.AnonymizedStr
					stmt.Prepared.Columns = pm.Columns
					stmt.Prepared.Types = pm.Types
					stmt.Prepared.Memo = cachedData.Memo
					stmt.Prepared.needTSTypeCheck = cachedData.Memo.CheckFlag(opt.NeedTSTypeCheck)
					return opc.flags, nil
				}
				opc.log(ctx, "query cache hit but memo is stale (prepare)")
			}
		} else if ok {
			opc.log(ctx, "query cache hit but there is no prepare metadata")
		} else {
			opc.log(ctx, "query cache miss")
		}
		opc.flags.Set(planFlagOptCacheMiss)
	}

	memo, err := opc.buildReusableMemo(ctx)
	if err != nil {
		return 0, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	var resultCols sqlbase.ResultColumns
	if len(opc.procedureCols) == 0 {
		resultCols = make(sqlbase.ResultColumns, len(physical.Presentation))
		for i, col := range physical.Presentation {
			resultCols[i].Name = col.Alias
			resultCols[i].Typ = md.ColumnMeta(col.ID).Type
			if err := checkResultType(resultCols[i].Typ); err != nil {
				return 0, err
			}
		}
	} else {
		resultCols = opc.procedureCols
	}
	// Verify that all placeholder types have been set.
	if err := p.semaCtx.Placeholders.Types.AssertAllSet(); err != nil {
		return 0, err
	}

	stmt.Prepared.Columns = resultCols
	stmt.Prepared.Types = p.semaCtx.Placeholders.Types
	stmt.Prepared.needTSTypeCheck = memo.CheckFlag(opt.NeedTSTypeCheck)
	// jdbc case: when the stable without ctable, there is no need for caching,
	// and it will return empty directly
	if opc.allowMemoReuse && !p.ShortCircuit {
		stmt.Prepared.Memo = memo
		if opc.useCache {
			// execPrepare sets the PrepareMetadata.InferredTypes field after this
			// point. However, once the PrepareMetadata goes into the cache, it
			// can't be modified without causing race conditions. So make a copy of
			// it now.
			// TODO(radu): Determine if the extra object allocation is really
			// necessary.
			pm := stmt.Prepared.PrepareMetadata
			cachedData := querycache.CachedData{
				SQL:             stmt.SQL,
				Memo:            memo,
				PrepareMetadata: &pm,
			}
			p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		}
	}
	return opc.flags, nil
}

// makeOptimizerPlan generates a plan using the cost-based optimizer.
// On success, it populates p.curPlan.
func (p *planner) makeOptimizerPlan(ctx context.Context) error {
	stmt := p.stmt

	if p.ExecCfg().StartMode != StartSingleNode {
		p.EvalContext().StartDistributeMode = true
	}

	opc := &p.optPlanningCtx
	opc.reset()

	var execMemo *memo.Memo
	var layerType tree.PhysicalLayerType
	var err error
	useProcedureCache := stmt.AST.StatOp() == "CALL" && opt.CheckOptMode(opt.TSQueryOptMode.Get(&p.ExecCfg().Settings.SV), opt.EnableProcedureCache)

	if useProcedureCache {
		// Try to find the cached memo in the procedure cache
		execMemo, layerType, err = p.handleProcedureCache(ctx, stmt, opc)
	} else {
		execMemo, layerType, err = opc.buildExecMemo(ctx)
	}
	if err != nil {
		return err
	}

	// Build the plan tree.
	root := execMemo.RootExpr()
	execFactory := makeExecFactory(p)
	bld := execbuilder.New(&execFactory, execMemo, &opc.catalog, root, p.EvalContext())
	bld.PhysType = layerType

	plan, err := bld.Build(true)
	if err != nil {
		return err
	}

	result := plan.(*planTop)
	result.mem = execMemo
	result.catalog = &opc.catalog
	result.stmt = stmt
	result.flags = opc.flags
	if bld.IsDDL {
		result.flags.Set(planFlagIsDDL)
	}
	if layerType == tree.TS {
		result.planType = tree.General
	}

	cols := planColumns(result.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			// Because procedure supports multiple result sets and
			// the type of the returned result cannot be determined,
			// this error is skipped.
			_, isCreateProc := root.(*memo.CreateProcedureExpr)
			_, isCallProc := root.(*memo.CallProcedureExpr)
			if !isCreateProc && !isCallProc {
				return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
			}
		}
	}

	result.auditInfo = p.curPlan.auditInfo

	p.curPlan = *result

	return nil
}

type optPlanningCtx struct {
	p *planner

	// catalog is initialized once, and reset for each query. This allows the
	// catalog objects to be reused across queries in the same session.
	catalog optCatalog

	// -- Fields below are reinitialized for each query ---

	optimizer xform.Optimizer

	// When set, we are allowed to reuse a memo, or store a memo for later reuse.
	allowMemoReuse bool

	autonomicOptimizationFlag bool

	// When set, we consult and update the query cache. Never set if
	// allowMemoReuse is false.
	useCache bool

	flags planFlags

	// procedureCols saves procedure output column
	procedureCols sqlbase.ResultColumns
}

// init performs one-time initialization of the planning context; reset() must
// also be called before each use.
func (opc *optPlanningCtx) init(p *planner) {
	opc.p = p
	opc.catalog.init(p)
}

// reset initializes the planning context for the statement in the planner.
func (opc *optPlanningCtx) reset() {
	p := opc.p
	opc.catalog.reset()
	opc.optimizer.Init(p.EvalContext(), &opc.catalog)
	opc.flags = 0
	opc.procedureCols = nil

	// We only allow memo caching for SELECT/INSERT/UPDATE/DELETE. We could
	// support it for all statements in principle, but it would increase the
	// surface of potential issues (conditions we need to detect to invalidate a
	// cached memo).
	switch p.stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause, *tree.UnionClause, *tree.ValuesClause,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CannedOptPlan:
		// If the current transaction has uncommitted DDL statements, we cannot rely
		// on descriptor versions for detecting a "stale" memo. This is because
		// descriptor versions are bumped at most once per transaction, even if there
		// are multiple DDL operations; and transactions can be aborted leading to
		// potential reuse of versions. To avoid these issues, we prevent saving a
		// memo (for prepare) or reusing a saved memo (for execute).
		opc.allowMemoReuse = !p.Tables().hasUncommittedTables()
		opc.useCache = opc.allowMemoReuse && queryCacheEnabled.Get(&p.execCfg.Settings.SV)
		// cluster level switch for get hints
		opc.autonomicOptimizationFlag = autonomicOptimizationEnable.Get(&p.execCfg.Settings.SV)

		if _, isCanned := p.stmt.AST.(*tree.CannedOptPlan); isCanned {
			// It's unsafe to use the cache, since PREPARE AS OPT PLAN doesn't track
			// dependencies and check permissions.
			opc.useCache = false
		}

	case *tree.Explain:
		opc.allowMemoReuse = false
		opc.useCache = false
		opc.autonomicOptimizationFlag = autonomicOptimizationEnable.Get(&p.execCfg.Settings.SV)

	default:
		opc.allowMemoReuse = false
		opc.useCache = false
	}
}

func (opc *optPlanningCtx) log(ctx context.Context, msg string) {
	if log.VDepth(1, 1) {
		log.InfofDepth(ctx, 1, "%s: %s", msg, opc.p.stmt)
	} else {
		log.Event(ctx, msg)
	}
}

// buildReusableMemo builds the statement into a memo that can be stored for
// prepared statements and can later be used as a starting point for
// optimization. The returned memo is fully detached from the planner and can be
// used with reuseMemo independently and concurrently by multiple threads.
func (opc *optPlanningCtx) buildReusableMemo(ctx context.Context) (_ *memo.Memo, _ error) {
	p := opc.p

	_, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan)
	if isCanned {
		if !p.EvalContext().SessionData.AllowPrepareAsOptPlan {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN is a testing facility that should not be used directly",
			)
		}

		if p.SessionData().User != security.RootUser {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN may only be used by root",
			)
		}
	}

	if p.SessionData().SaveTablesPrefix != "" && p.SessionData().User != security.RootUser {
		return nil, pgerror.New(pgcode.InsufficientPrivilege,
			"sub-expression tables creation may only be used by root",
		)
	}

	// Build the Memo (optbuild) and apply normalization rules to it. If the
	// query contains placeholders, values are not assigned during this phase,
	// as that only happens during the EXECUTE phase. If the query does not
	// contain placeholders, then also apply exploration rules to the Memo so
	// that there's even less to do during the EXECUTE phase.
	//
	f := opc.optimizer.Factory()
	f.TSWhiteListMap = p.ExecCfg().TSWhiteListMap
	f.Memo().SetWhiteList(p.ExecCfg().TSWhiteListMap)

	if p.stmt.Prepared != nil && p.stmt.NumPlaceholders != 0 {
		f.TSFlags |= opt.IsPrepare
	}

	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	bld.KeepPlaceholders = true
	// find sql of select in insert...select...
	if err := bld.Build(); err != nil {
		return nil, err
	}
	if len(bld.OutPutCols) > 0 {
		resultCols := make(sqlbase.ResultColumns, len(bld.OutPutCols))
		for i, col := range bld.OutPutCols {
			resultCols[i].Name = string(col.GetName())
			resultCols[i].Typ = col.GetType()
			if err := checkResultType(resultCols[i].Typ); err != nil {
				return nil, err
			}
		}
		opc.procedureCols = resultCols
	}
	if opc.optimizer.Memo().QueryType == memo.MultiModel {
		if len(opc.optimizer.Memo().Metadata().AllTables()) > 1 {
			opc.optimizer.CheckMultiModel()
		} else {
			opc.optimizer.Memo().QueryType = memo.Unset
		}
	}
	opc.p.TsInScopeFlag = bld.PhysType == tree.TS
	f.Memo().ColsUsage = bld.ColsUsage

	opc.InitTS(ctx)
	f.GetPushHelperValue(opc.optimizer.Memo().GetPushHelperAddress())
	if opt.CheckTsProperty(bld.TSInfo.TSProp, optbuilder.TSPropSTableWithoutChild) {
		p.ShortCircuit = false
	}
	if opt.CheckTsProperty(bld.TSInfo.TSProp, optbuilder.TSPropNeedTSTypeCheck) {
		f.Memo().SetFlag(opt.NeedTSTypeCheck)
	}
	if bld.DisableMemoReuse {
		opc.allowMemoReuse = false
		opc.useCache = false
	}

	if isCanned {
		if f.Memo().HasPlaceholders() {
			// We don't support placeholders inside the canned plan. The main reason
			// is that they would be invisible to the parser (which is reports the
			// number of placeholders, used to initialize the relevant structures).
			return nil, pgerror.Newf(pgcode.Syntax,
				"placeholders are not supported with PREPARE AS OPT PLAN")
		}
		// With a canned plan, the memo is already optimized.
	} else {
		// If the memo doesn't have placeholders, then fully optimize it, since
		// it can be reused without further changes to build the execution tree.
		if !f.Memo().HasPlaceholders() {
			// fix query errors during prepare
			opc.log(ctx, "optimizing (no placeholders)")
			if _, err := opc.optimizer.Optimize(); err != nil {
				return nil, err
			}
		}
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return opc.optimizer.DetachMemo(), nil
}

// InitTS initializes some elements to determine if the memo exprs can be executed in the TS engine.
func (opc *optPlanningCtx) InitTS(ctx context.Context) {
	m := opc.optimizer.Memo()
	sv := &opc.p.EvalContext().Settings.SV
	if IsSingleNodeMode(opc.p.execCfg.StartMode) {
		m.SetFlag(opt.SingleMode)
	}

	if opt.CheckOptMode(opt.TSQueryOptMode.Get(sv), opt.PushScalarSubQuery) {
		// ScalarSubQueryPush is set when the optimization of SubQuery is opened.
		m.SetFlag(opt.ScalarSubQueryPush)
	}

	// init degree of parallelism
	m.SetTsDop(uint32(opt.TSParallelDegree.Get(sv)))

	// init CheckHelper
	m.InitCheckHelper(opc.p.ExecCfg().TSWhiteListMap)
	m.InitCheckHelper(ctx)
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
//
// The cached memo is not modified; it is safe to call reuseMemo on the same
// cachedMemo from multiple threads concurrently.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) reuseMemo(cachedMemo *memo.Memo) (*memo.Memo, error) {
	if !cachedMemo.HasPlaceholders() {
		// If there are no placeholders, the query was already fully optimized
		// (see buildReusableMemo).
		return cachedMemo, nil
	}
	f := opc.optimizer.Factory()
	// Finish optimization by assigning any remaining placeholders and
	// applying exploration rules. Reinitialize the optimizer and construct a
	// new memo that is copied from the prepared memo, but with placeholders
	// assigned.
	if err := f.AssignPlaceholders(cachedMemo); err != nil {
		return nil, err
	}
	opc.optimizer.Memo().SetWhiteList(cachedMemo.GetWhiteList())
	opc.optimizer.Memo().SetAllFlag(cachedMemo.GetAllFlag())
	opc.optimizer.Memo().SetTsDop(cachedMemo.GetTsDop())
	if opc.optimizer.Memo().QueryType == memo.MultiModel &&
		!opt.CheckOptMode(opt.TSQueryOptMode.Get(&opc.p.ExecCfg().Settings.SV), opt.OutsideInUseCBO) {
		if len(opc.optimizer.Memo().Metadata().AllTables()) > 1 {
			opc.optimizer.CheckMultiModel()
		} else {
			opc.optimizer.Memo().QueryType = memo.Unset
		}
	}
	if _, err := opc.optimizer.Optimize(); err != nil {
		return nil, err
	}
	return f.Memo(), nil
}

// buildExecMemo creates a fully optimized memo, possibly reusing a previously
// cached memo as a starting point.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) buildExecMemo(
	ctx context.Context,
) (_ *memo.Memo, _ tree.PhysicalLayerType, _ error) {
	prepared := opc.p.stmt.Prepared
	p := opc.p
	f := opc.optimizer.Factory()
	f.TSWhiteListMap = p.ExecCfg().TSWhiteListMap
	f.Memo().SetWhiteList(p.ExecCfg().TSWhiteListMap)

	if p.stmt.Prepared != nil && p.stmt.NumPlaceholders != 0 {
		f.TSFlags |= opt.IsPrepare
	}
	if opc.allowMemoReuse && prepared != nil && prepared.Memo != nil {
		// We are executing a previously prepared statement and a reusable memo is
		// available.

		// If the prepared memo has been invalidated by schema or other changes,
		// re-prepare it.
		if isStale, err := prepared.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
			return nil, tree.Invalid, err
		} else if isStale {
			prepared.Memo, err = opc.buildReusableMemo(ctx)
			opc.log(ctx, "rebuilding cached memo")
			if err != nil {
				return nil, tree.Invalid, err
			}
		}
		opc.log(ctx, "reusing cached memo")
		prepared.Memo.SetWhiteList(p.ExecCfg().TSWhiteListMap)
		memo, err := opc.reuseMemo(prepared.Memo)
		return memo, tree.Invalid, err
	}

	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
				return nil, tree.Invalid, err
			} else if isStale {
				cachedData.Memo, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, tree.Invalid, err
				}
				// Update the plan in the cache. If the cache entry had PrepareMetadata
				// populated, it may no longer be valid.
				cachedData.PrepareMetadata = nil
				p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
				opc.log(ctx, "query cache hit but needed update")
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			cachedData.Memo.SetWhiteList(p.ExecCfg().TSWhiteListMap)
			memo, err := opc.reuseMemo(cachedData.Memo)
			return memo, tree.Invalid, err
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	} else {
		opc.log(ctx, "not using query cache")
	}

	// We are executing a statement for which there is no reusable memo
	// available.
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)

	//computer queryingerPrint for a select sql,and get its external hint from pseudo catalog cache
	//or pseudo catalog table.
	bld.CleanIncludeTSTableFlag()
	if err := bld.Build(); err != nil {
		return nil, tree.Invalid, err
	}

	// check if the query satisfies the requirements for multiple model processing.
	if opc.optimizer.Memo().QueryType == memo.MultiModel &&
		!opt.CheckOptMode(opt.TSQueryOptMode.Get(&opc.p.ExecCfg().Settings.SV), opt.OutsideInUseCBO) {
		if len(opc.optimizer.Memo().Metadata().AllTables()) > 1 {
			opc.optimizer.CheckMultiModel()
		} else {
			opc.optimizer.Memo().QueryType = memo.Unset
		}
	}

	if opt.CheckTsProperty(bld.TSInfo.TSProp, optbuilder.TSPropSTableWithoutChild) {
		p.ShortCircuit = false
	}
	opc.p.TsInScopeFlag = bld.PhysType == tree.TS
	opc.InitTS(ctx)

	if _, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan); !isCanned {
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, tree.Invalid, err
		}
	}

	// If this statement doesn't have placeholders, add it to the cache. Note
	// that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders && !bld.DisableMemoReuse && !opt.CheckTsProperty(bld.TSInfo.TSProp, optbuilder.TSPropSTableWithoutChild) && opc.optimizer.Memo().QueryType != memo.MultiModel {
		memo := opc.optimizer.DetachMemo()
		//memo.PhysType = bld.PhysType
		cachedData := querycache.CachedData{
			SQL:  opc.p.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		opc.log(ctx, "query cache add")
		memo.ColsUsage = bld.ColsUsage
		return memo, bld.PhysType, nil
	}

	f.Memo().ColsUsage = bld.ColsUsage
	return f.Memo(), bld.PhysType, nil
}

// handleProcedureCache handles the procedure cache lookup, validation, and update logic.
func (p *planner) handleProcedureCache(
	ctx context.Context, stmt *Statement, opc *optPlanningCtx,
) (execMemo *memo.Memo, layerType tree.PhysicalLayerType, err error) {
	procedureName := stmt.AST.(*tree.CallProcedure).Name
	found, desc, err := ResolveProcedureObject(ctx, p, &procedureName)
	if err != nil {
		return nil, tree.Invalid, err
	}
	if !found {
		return nil, tree.Invalid, sqlbase.NewUndefinedProcedureError(&procedureName)
	}

	procedureInfo := ProcedureInfo{ProcedureID: uint32(desc.ID)}

	// Try to find the cached data in the procedure cache
	if cachedData := p.execCfg.ProcedureCache.Find(procedureInfo); cachedData != nil {
		// Verify exec privilege for the procedure
		if err := p.CheckPrivilege(ctx, desc, privilege.EXECUTE); err != nil {
			return nil, tree.Invalid, err
		}

		// Check if cached plan is stale
		isStale, err := cachedData.ProcedureFn.ProcedureCacheIsStale(ctx, p.EvalContext(), &opc.catalog)
		if err != nil {
			return nil, tree.Invalid, err
		}

		if isStale {
			// ReCompile memo if stale
			if _, _, err = opc.buildExecMemo(ctx); err != nil {
				return nil, tree.Invalid, err
			}

			// Update cache with fresh memo
			memo := opc.optimizer.DetachMemo()
			memo.ProcCacheHelper.ExecViaProcedureCache = true
			memo.ProcCacheHelper.ProcedureStmtType = stmt.AST.StatementType()
			p.execCfg.ProcedureCache.Add(&CachedData{
				ProcedureInfo: procedureInfo,
				ProcedureFn:   memo,
			})
		}

		// Replace the parameter values to avoid repeated compilation
		cp := stmt.AST.(*tree.CallProcedure)
		if len(cp.Exprs) != len(desc.Parameters) {
			return nil, tree.Invalid, pgerror.New(pgcode.InvalidParameterValue, "Number of arguments does not match stored procedure definition")
		}

		if err = cachedData.ProcedureFn.ReplaceProcedureParam(&p.semaCtx, cp.Exprs, desc.Parameters); err != nil {
			return nil, tree.Invalid, err
		}

		// Return cached memo
		return cachedData.ProcedureFn, tree.Invalid, nil
	}

	// Cache miss so compile new memo
	execMemo, layerType, err = opc.buildExecMemo(ctx)
	if err != nil {
		return nil, tree.Invalid, err
	}

	memo := opc.optimizer.DetachMemo()
	memo.ProcCacheHelper.ExecViaProcedureCache = true
	memo.ProcCacheHelper.ProcedureStmtType = stmt.AST.StatementType()
	p.execCfg.ProcedureCache.Add(&CachedData{
		ProcedureInfo: procedureInfo,
		ProcedureFn:   memo,
	})

	return execMemo, layerType, nil
}
