// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

type callProcedureNode struct {
	procName string
	procCall string
	// run procedure params
	// execCtx execute context
	execCtx procedure.SpExecContext

	// ins execute block
	ins procedure.Instruction

	// params saves run param for next result execute
	params RunParams

	// err saves error
	err error

	// fn saves function that optimize and build
	fn exec.ProcedurePlanFn
}

func (n *callProcedureNode) endTransaction(txnImplicit bool) {
	// the explicit transaction was not properly completed.
	if txnImplicit && n.execCtx.GetProcedureTxn() != tree.ProcedureTransactionDefault {
		if err := n.params.p.txn.Rollback(n.params.Ctx); err != nil {
			n.err = err
		}
		n.execCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
		if n.err == nil {
			panic(pgerror.Newf(pgcode.Syntax,
				"procedure explicit transaction has not been ended, please check explicit transaction start statements."))
		}
	}
}

func (n *callProcedureNode) StartExec(params RunParams) error {
	// Ensure all nodes are the correct version.
	if !params.ExecCfg().Settings.Version.IsActive(params.Ctx, clusterversion.VersionUDR) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"all nodes are not at the correct version to use Stored Procedures")
	}
	if !params.p.extendedEvalCtx.TxnImplicit {
		return pgerror.New(pgcode.FeatureNotSupported, "Call Procedure statement is not supported in explicit transaction")
	}
	n.execCtx.Init()
	n.execCtx.Fn = n.fn
	n.execCtx.GetResultFn = GetPlanResultColumn
	n.execCtx.RunPlanFn = RunPlanInsideProcedure
	n.execCtx.StartPlanFn = StartPlanInsideProcedure
	n.params = params
	if err := n.ins.Execute(&params, &n.execCtx); err != nil {
		if params.p.extendedEvalCtx.TxnImplicit && n.execCtx.GetProcedureTxn() != tree.ProcedureTransactionDefault {
			if err := params.p.txn.Rollback(params.Ctx); err != nil {
				n.err = err
				return err
			}
		}
		n.err = err
		return err
	}
	return nil
}

func (n *callProcedureNode) ReadingOwnWrites() {}

// CheckResultExist checks result exist
func (n *callProcedureNode) CheckResultExist() bool {
	return n.execCtx.CheckResultExist()
}

func (n *callProcedureNode) Next(params RunParams) (bool, error) {
	return n.execCtx.Next()
}

func (n *callProcedureNode) Values() tree.Datums {
	return n.execCtx.Values()
}

func (n *callProcedureNode) Close(ctx context.Context) {
	n.execCtx.Close(ctx)
	n.ins.Close()
}

// HasNextResult checks next result for procedure . The stored procedure will generate multiple result sets,
// and it is necessary to use this interface to determine whether there are result sets
func (n *callProcedureNode) HasNextResult(txnImplicit bool) (bool, error) {
	isExecHandler := false
	var returnHandler *procedure.HandlerHelper
	var exceptionErr error
	// since the ExitHandler execution ends with the termination of the procedure,
	// if the result set is generated from the ExitHandler, we only need to return to the ExitHandler and continue executing.
	// returnHandler record the ExitHandler.
	if n.execCtx.ReturnLabel() == procedure.HandlerLabel && n.execCtx.GetReturnHandler() != nil {
		isExecHandler = true
		returnHandler = n.execCtx.GetReturnHandler()
		exceptionErr = n.execCtx.GetExceptionErr()
	}
	n.execCtx.Close(n.params.Ctx)
	if n.err != nil {
		return false, n.err
	}
	n.execCtx.InitResult()
	// if label is 'Handler', we should continue executing returnHandler.
	if isExecHandler {
		if err := returnHandler.ExecHandler(&n.params, &n.execCtx, tree.ModeExit); err != nil {
			return false, err
		}
		if n.execCtx.NeedReturn() && n.execCtx.ReturnLabel() == procedure.EndLabel && exceptionErr != nil {
			n.err = exceptionErr
			n.endTransaction(txnImplicit)
			return false, n.err
		}
	} else if err := n.ins.Execute(&n.params, &n.execCtx); err != nil {
		n.err = err
		n.endTransaction(txnImplicit)
		return false, n.err
	}

	ret := n.execCtx.CheckResultExist()

	if !ret {
		n.endTransaction(txnImplicit)
	}

	return ret, n.err
}

// GetNextResultCols get next result columns
func (n *callProcedureNode) GetNextResultCols() sqlbase.ResultColumns {
	return n.execCtx.GetNextResultCols()
}

// GetResultTypeAndAffected get next result query type
func (n *callProcedureNode) GetResultTypeAndAffected() (tree.StatementType, int) {
	return n.execCtx.GetResultTypeAndAffected()
}

// SQLUDFCallProcedure executes SQL UDF by procedure and returns
// the first row and first column as the scalar function result.
func (p *GenericPlanner) SQLUDFCallProcedure(
	ctx context.Context, funcName string, returnType *types.T, args tree.Datums,
) (ret tree.Datum, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ok, e := errorutil.ShouldCatch(r); ok {
				ret = nil
				err = e
				return
			}
			panic(r)
		}
	}()

	ctx, err = pushSQLUDFCall(ctx, funcName)
	if err != nil {
		return nil, err
	}

	udfPlanner, cleanup := p.makeSQLUDFInternalPlanner(ctx, funcName)
	defer cleanup()

	cp, params, err := udfPlanner.makeCallProcedureNodeForSQLUDF(ctx, funcName, args)
	if err != nil {
		return nil, err
	}
	defer cp.Close(ctx)

	if err := cp.StartExec(params); err != nil {
		return nil, err
	}

	rows, err := collectFirstProcedureResultSetForSQLUDF(ctx, params, cp, funcName)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 || len(rows) > 1 || len(rows[0]) != 1 {
		return nil, pgerror.Newf(pgcode.ErrorInAssignment,
			"sql function %s does not return a scalar result", funcName)
	}

	rawRet := rows[0][0]
	if rawRet == tree.DNull {
		return tree.DNull, nil
	}
	if !rawRet.ResolvedType().Equivalent(returnType) {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"sql function %s returned type %s, expected %s",
			funcName, rawRet.ResolvedType().SQLString(), returnType.SQLString(),
		)
	}
	retName := funcName + "()"
	outVal, err := sqlbase.LimitValueWidth(returnType, rawRet, &retName)
	if err != nil {
		return nil, err
	}

	return outVal, nil
}

// makeSQLUDFInternalPlanner creates an isolated planner for one SQL UDF call.
//
// SQL UDF execution is triggered during scalar expression evaluation. If it
// reuses the outer planner, nested UDF calls can share the outer memo/metadata
// and cause ColumnID mismatch. Using a fresh internal planner makes every SQL
// UDF call behave like an independent internal CALL statement.
func (p *GenericPlanner) makeSQLUDFInternalPlanner(
	ctx context.Context, funcName string,
) (*GenericPlanner, func()) {
	user := p.User()
	opName := "sql-udf-" + funcName

	memMetrics := p.extendedEvalCtx.MemMetrics
	if memMetrics == nil {
		memMetrics = &MemoryMetrics{}
	}

	udfPlanner, cleanup := newInternalPlanner(
		opName,
		p.txn,
		user,
		memMetrics,
		p.execCfg,
	)

	outerSD := p.SessionData()
	innerSD := udfPlanner.SessionData()

	*innerSD = *outerSD
	if outerSD.UserDefinedVars != nil {
		innerSD.UserDefinedVars = make(map[string]interface{}, len(outerSD.UserDefinedVars))
		for k, v := range outerSD.UserDefinedVars {
			innerSD.UserDefinedVars[k] = v
		}
	} else {
		innerSD.UserDefinedVars = make(map[string]interface{})
	}

	udfPlanner.txn = p.txn
	udfPlanner.execCfg = p.execCfg
	udfPlanner.resolveSQLFunctionAsProcedure = true
	udfPlanner.cancelChecker = p.cancelChecker
	udfPlanner.AvoidCachedDescriptors = p.AvoidCachedDescriptors
	udfPlanner.skipSelectPrivilegeChecks = p.skipSelectPrivilegeChecks
	udfPlanner.ShortCircuit = p.ShortCircuit
	udfPlanner.TsInScopeFlag = p.TsInScopeFlag
	udfPlanner.forceFilterInME = p.forceFilterInME
	udfPlanner.inStream = p.inStream

	udfPlanner.semaCtx.Location = &innerSD.DataConversion.Location
	udfPlanner.semaCtx.SearchPath = innerSD.SearchPath
	udfPlanner.semaCtx.UserDefinedVars = innerSD.UserDefinedVars
	udfPlanner.semaCtx.SQLUDFFunctionHandler = udfPlanner

	udfPlanner.extendedEvalCtx.Context = ctx
	udfPlanner.extendedEvalCtx.Txn = p.txn
	udfPlanner.extendedEvalCtx.TxnImplicit = p.extendedEvalCtx.TxnImplicit
	udfPlanner.extendedEvalCtx.DB = p.extendedEvalCtx.DB
	if udfPlanner.extendedEvalCtx.DB == nil {
		udfPlanner.extendedEvalCtx.DB = p.execCfg.DB
	}
	udfPlanner.extendedEvalCtx.ClientNoticeSender = p.extendedEvalCtx.ClientNoticeSender
	udfPlanner.extendedEvalCtx.Sequence = p.extendedEvalCtx.Sequence
	udfPlanner.extendedEvalCtx.TsDBAccessor = p.extendedEvalCtx.TsDBAccessor
	udfPlanner.extendedEvalCtx.CollationEnv = p.extendedEvalCtx.CollationEnv
	udfPlanner.extendedEvalCtx.Mon = p.extendedEvalCtx.Mon
	udfPlanner.extendedEvalCtx.ReCache = p.extendedEvalCtx.ReCache
	udfPlanner.extendedEvalCtx.StmtTimestamp = p.extendedEvalCtx.StmtTimestamp
	udfPlanner.extendedEvalCtx.TxnTimestamp = p.extendedEvalCtx.TxnTimestamp
	udfPlanner.extendedEvalCtx.InternalExecutor = p.extendedEvalCtx.InternalExecutor
	udfPlanner.extendedEvalCtx.Placeholders = &udfPlanner.semaCtx.Placeholders
	udfPlanner.extendedEvalCtx.TriggerColHolders = &udfPlanner.semaCtx.TriggerColHolders
	udfPlanner.extendedEvalCtx.Annotations = &udfPlanner.semaCtx.Annotations
	udfPlanner.extendedEvalCtx.SQLUDFFunctionHandler = udfPlanner
	udfPlanner.extendedEvalCtx.SessionAccessor = p.extendedEvalCtx.SessionAccessor
	udfPlanner.extendedEvalCtx.schemaAccessors = p.extendedEvalCtx.schemaAccessors
	udfPlanner.extendedEvalCtx.PrivilegedAccessor = p.extendedEvalCtx.PrivilegedAccessor
	udfPlanner.extendedEvalCtx.VirtualSchemas = p.extendedEvalCtx.VirtualSchemas
	udfPlanner.extendedEvalCtx.Tables = p.extendedEvalCtx.Tables
	udfPlanner.extendedEvalCtx.IsInternalSQL = p.extendedEvalCtx.IsInternalSQL
	udfPlanner.extendedEvalCtx.IsDisplayed = p.extendedEvalCtx.IsDisplayed
	udfPlanner.extendedEvalCtx.GroupWindow = p.extendedEvalCtx.GroupWindow

	return udfPlanner, cleanup
}

// makeCallProcedureNodeForSQLUDF builds an internal CALL plan for executing a
// SQL UDF backing procedure.
func (p *GenericPlanner) makeCallProcedureNodeForSQLUDF(
	ctx context.Context, funcName string, args tree.Datums,
) (*callProcedureNode, RunParams, error) {
	callStmt := &tree.CallProcedure{
		Name:  tree.MakeUnqualifiedTableName(tree.Name(funcName)),
		Exprs: datumsToExprs(args),
	}

	// Save the outer planner state for SELECT func(...).
	oldStmt := p.stmt
	oldCurPlan := p.curPlan
	oldEvalIsProcedure := p.extendedEvalCtx.IsProcedure
	oldResolveSQLFunctionAsProcedure := p.resolveSQLFunctionAsProcedure

	defer func() {
		p.stmt = oldStmt
		p.curPlan = oldCurPlan
		p.extendedEvalCtx.IsProcedure = oldEvalIsProcedure
		p.resolveSQLFunctionAsProcedure = oldResolveSQLFunctionAsProcedure
	}()

	// Resolve the CALL target from the SQL function namespace.
	p.resolveSQLFunctionAsProcedure = true

	// Build an internal CALL statement and reuse the normal CALL planning path.
	p.stmt = &Statement{
		Statement: parser.Statement{
			AST: callStmt,
			SQL: tree.AsStringWithFlags(callStmt, tree.FmtSimple),
		},
	}

	if err := p.makeOptimizerPlan(ctx); err != nil {
		return nil, RunParams{}, err
	}

	cp, ok := unwrapCallProcedureNode(p.curPlan.plan)
	if !ok {
		return nil, RunParams{}, pgerror.Newf(pgcode.Warning,
			"expected CALL procedure plan for SQL UDF %s, got %T",
			funcName,
			p.curPlan.plan,
		)
	}

	params := NewRunParams(ctx, p.ExtendedEvalContext(), p)
	return cp, *params, nil
}

// unwrapCallProcedureNode extracts a callProcedureNode from a planned PlanNode.
func unwrapCallProcedureNode(plan PlanNode) (*callProcedureNode, bool) {
	cp, ok := plan.(*callProcedureNode)
	return cp, ok
}

// datumsToExprs converts Datums into expression nodes for building
// an internal CALL statement.
func datumsToExprs(args tree.Datums) tree.Exprs {
	exprs := make(tree.Exprs, 0, len(args))
	for _, d := range args {
		if d == nil {
			exprs = append(exprs, tree.DNull)
		} else {
			exprs = append(exprs, d)
		}
	}
	return exprs
}

type sqlUDFCallStackKey struct{}

// pushSQLUDFCall returns a child context with funcName appended to the current
// SQL IDF call stack. It rejects recursive SQL UDF calls in the same execution
// chain.
func pushSQLUDFCall(ctx context.Context, funcName string) (context.Context, error) {
	stack, _ := ctx.Value(sqlUDFCallStackKey{}).([]string)

	for _, name := range stack {
		if name == funcName {
			return ctx, pgerror.New(pgcode.InvalidFunctionDefinition, "recursive stored functions are not allowed")
		}
	}

	newStack := append(append([]string{}, stack...), funcName)
	return context.WithValue(ctx, sqlUDFCallStackKey{}, newStack), nil
}

// collectFirstProcedureResultSetForSQLUDF collects the result rows produced by
// the first result set of a procedure.
func collectFirstProcedureResultSetForSQLUDF(
	ctx context.Context, params RunParams, cp *callProcedureNode, funcName string,
) ([]tree.Datums, error) {
	var rows []tree.Datums

	defer cp.endTransaction(params.p.extendedEvalCtx.TxnImplicit)

	if !cp.CheckResultExist() {
		return nil, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ok, err := cp.Next(params)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		rows = append(rows, append(tree.Datums(nil), cp.Values()...))
	}

	hasNext, err := cp.HasNextResult(params.p.extendedEvalCtx.TxnImplicit)
	if err != nil {
		return nil, err
	}
	if hasNext {
		return nil, pgerror.Newf(pgcode.ErrorInAssignment,
			"sql function %s does not return a scalar result", funcName)
	}

	return rows, nil
}

// ResolveFunctionFromCatalog loads a persisted user defined function
// from system.user_defined_routine and registers it into tree.ConcurrentFunDefs.
func (p *GenericPlanner) ResolveFunctionFromCatalog(
	name *tree.UnresolvedName, searchPath sessiondata.SearchPath,
) (*tree.FunctionDefinition, bool, error) {
	if name == nil || name.Star || name.NumParts == 0 || len(name.Parts[0]) == 0 {
		return nil, false, nil
	}

	funcName := name.Parts[0]

	return p.loadUDF(funcName)
}

// loadUDF loads a user-defined function by function_name.
// It supports both existing Lua UDFs and SQL UDFs stored in
// system.user_defined_routine.
func (p *GenericPlanner) loadUDF(funcName string) (*tree.FunctionDefinition, bool, error) {
	query := fmt.Sprintf(`
    SELECT name, descriptor, routine_type
    FROM system.user_defined_routine
    WHERE name = $1
    AND routine_type IN (%d, %d)`,
		sqlbase.LUAFunction,
		sqlbase.SQLFunction,
	)

	rows, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		p.extendedEvalCtx.Context,
		"load-udf-from-catalog",
		p.txn,
		query,
		funcName,
	)
	if err != nil {
		return nil, false, err
	}

	if len(rows) == 0 {
		return nil, false, nil
	}

	if len(rows) > 1 {
		return nil, false, pgerror.Newf(pgcode.AmbiguousFunction, "function %s is ambiguous", funcName)
	}

	// Only after the catalog confirms that the function still exists, try the
	// udf cache. This avoids returning stale UDF definitions that were
	// already dropped from system.user_defined_routine.
	if fd, ok := tree.ConcurrentFunDefs.LookupFunc(funcName); ok {
		return fd, true, nil
	}

	fd, err := registerUDFRowFromCatalog(rows[0])
	if err != nil {
		return nil, false, err
	}

	return fd, true, nil
}

// registerUDFRowFromCatalog converts one system.user_defined_routine row into
// an in-memory FunctionDefinition and registers it in ConcurrentFunDefs.
func registerUDFRowFromCatalog(row tree.Datums) (*tree.FunctionDefinition, error) {
	if len(row) < 3 {
		return nil, pgerror.Newf(pgcode.Warning,
			"invalid user_defined_routine row: expected at least 3 columns, got %d", len(row))
	}

	funcName := string(*row[0].(*tree.DString))
	routineType := int64(*row[2].(*tree.DInt))

	switch routineType {
	case int64(sqlbase.LUAFunction):
		// Existing Lua UDF path.
		fd, err := builtins.RegisterLuaUDFs(tree.Datums{row[1]})
		if err != nil {
			return nil, err
		}

		tree.ConcurrentFunDefs.RegisterFunc(funcName, fd)
		return fd, nil

	case int64(sqlbase.SQLFunction):
		// New SQL function body path.
		descBytes := tree.MustBeDBytes(row[1])

		var procDesc sqlbase.ProcedureDescriptor
		if err := protoutil.Unmarshal([]byte(descBytes), &procDesc); err != nil {
			return nil, err
		}

		wrapper, err := makeSQLFunctionWrapperFromProcDesc(&procDesc)
		if err != nil {
			return nil, err
		}

		fd, err := MakeSQLUDFDefinition(wrapper)
		if err != nil {
			return nil, err
		}

		tree.ConcurrentFunDefs.RegisterFunc(funcName, fd)
		return fd, nil

	default:
		return nil, pgerror.Newf(
			pgcode.InvalidFunctionDefinition,
			"unsupported routine type for function resolution: %d",
			routineType,
		)
	}
}

// makeSQLFunctionWrapperFromProcDesc rebuilds a SQLFunctionWrapper from a
// persisted ProcedureDescriptor.
func makeSQLFunctionWrapperFromProcDesc(
	desc *sqlbase.ProcedureDescriptor,
) (*tree.SQLFunctionWrapper, error) {
	args := make(tree.FuncArgDefs, 0, len(desc.Parameters))
	for _, p := range desc.Parameters {
		typ := p.Type

		args = append(args, tree.FuncArgDef{
			ArgName: tree.Name(p.Name),
			ArgType: &typ,
		})
	}

	return &tree.SQLFunctionWrapper{
		FunctionName: tree.Name(desc.Name),
		Arguments:    args,
		ReturnType:   &desc.ReturnType,
	}, nil
}

// MakeSQLUDFDefinition builds a FunctionDefinition for a SQL UDF.
func MakeSQLUDFDefinition(fn *tree.SQLFunctionWrapper) (*tree.FunctionDefinition, error) {
	funcName := string(fn.FunctionName)

	paramTypes := make(tree.ArgTypes, len(fn.Arguments))
	for i, p := range fn.Arguments {
		paramTypes[i].Name = ""
		paramTypes[i].Typ = p.ArgType
	}

	overload := tree.Overload{
		Types:      paramTypes,
		ReturnType: tree.FixedReturnType(fn.ReturnType),
		Fn: createSQLFunctionWrapper(
			funcName,
			fn.ReturnType,
		),
	}

	return tree.GetUdfFunctionDefinition(
		funcName,
		&tree.FunctionProperties{
			NullableArgs:            true,
			ForbiddenExecInTSEngine: true,
			DistsqlBlacklist:        true,
		},
		[]tree.Overload{overload},
	), nil
}

// createSQLFunctionWrapper creates the runtime function body for a SQL UDF.
// It delegates execution to EvalContext.SQLUDFHandler, which runs the procedure and
// returns a scalar Datum.
func createSQLFunctionWrapper(
	funcName string, returnType *types.T,
) func(*tree.EvalContext, tree.Datums) (tree.Datum, error) {
	return func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return evalCtx.SQLUDFFunctionHandler.SQLUDFCallProcedure(
			evalCtx.Context,
			funcName,
			returnType,
			args,
		)
	}
}

// CheckUDFNameExists checks udf exist in user_defined_routine.
func CheckUDFNameExists(p *GenericPlanner, funcName string) error {
	const query = `
  SELECT name
  FROM system.user_defined_routine
  WHERE name = $1 AND routine_type IN ($2, $3) LIMIT 1`

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		p.EvalContext().Context,
		"get-functions",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		funcName,
		sqlbase.LUAFunction,
		sqlbase.SQLFunction,
	)
	if err != nil {
		return err
	}

	if len(rows) > 0 {
		return pgerror.Newf(pgcode.DuplicateObject, "function named '%s' already exists. Please choose a different name", funcName)
	}

	return nil
}
