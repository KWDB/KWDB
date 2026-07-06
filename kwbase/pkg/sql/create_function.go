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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	luaast "github.com/yuin/gopher-lua/ast"
	goluaparse "github.com/yuin/gopher-lua/golua-parse"
)

// user define functions use the default schemaID and databaseID
// for storage and parsing currently
const (
	UDFFunctionDBID     int = 0
	UDFFunctionSchemaID int = 0
)

type createFunctionNode struct {
	n *tree.CreateFunction
	p *planner
}

// CreateFunction creates a function.
func (p *planner) CreateFunction(ctx context.Context, n *tree.CreateFunction) (planNode, error) {
	if !p.extendedEvalCtx.TxnImplicit {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "Create Function statement is not supported in explicit transaction")
	}
	return &createFunctionNode{
		n: n,
		p: p,
	}, nil
}

// startExec is interface implementation, which execute the event of creating function(s).
func (n *createFunctionNode) startExec(params runParams) error {
	if err := n.CheckUdf(params); err != nil {
		return err
	}

	rows := make([]tree.Datums, 0)
	creator := params.p.sessionDataMutator.data.User
	funcName := string(n.n.FunctionName)

	// assign unique id
	id, err := GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Generate funcDesc
	desc, err := n.makeFuncDesc(params.SessionData())
	if err != nil {
		return err
	}
	descValue, err := protoutil.Marshal(&desc)
	if err != nil {
		return err
	}

	var ext []byte
	row := tree.Datums{
		tree.NewDString(string(n.n.FunctionName)),
		tree.NewDInt(tree.DInt(UDFFunctionDBID)),
		tree.NewDInt(tree.DInt(UDFFunctionSchemaID)),
		tree.NewDBytes(tree.DBytes(descValue)),
		tree.NewDInt(tree.DInt(id)),
		tree.NewDInt(tree.DInt(sqlbase.LUAFunction)),
		tree.NewDString(creator),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.NewDInt(tree.DInt(1)),
		tree.DBoolTrue,
		tree.NewDBytes(tree.DBytes(ext)),
	}
	rows = append(rows, row)
	// system.user_defined_routine
	if err := WriteKWDBDesc(params.ctx, params.p.txn, sqlbase.UDRTable, rows, false); err != nil {
		return err
	}

	fd, err := builtins.RegisterLuaUDFs(tree.Datums{tree.NewDBytes(tree.DBytes(descValue))})
	if err != nil {
		return err
	}

	tree.ConcurrentFunDefs.RegisterFunc(funcName, fd)

	if err := GossipUdfAdded(params.p.execCfg.Gossip, funcName); err != nil {
		return err
	}
	return nil
}

// makeFuncDesc constructs and returns a new funcDescriptor
func (n *createFunctionNode) makeFuncDesc(
	sessionData *sessiondata.SessionData,
) (sqlbase.FunctionDescriptor, error) {
	argTypes, returnTypes, typeLens, err := getTypesAndLength(n.n.Arguments, n.n.ReturnType)
	if err != nil {
		return sqlbase.FunctionDescriptor{}, err
	}
	encodeArgTypes := make([]uint32, len(argTypes.Array))
	encodeReturnTypes := make([]uint32, len(returnTypes.Array))
	encodeTypeLens := make([]uint32, len(typeLens.Array))
	for i, v := range argTypes.Array {
		encodeArgTypes[i] = uint32(*v.(*tree.DInt))
	}
	for i, v := range returnTypes.Array {
		encodeReturnTypes[i] = uint32(*v.(*tree.DInt))
	}
	for i, v := range typeLens.Array {
		encodeTypeLens[i] = uint32(*v.(*tree.DInt))
	}
	argumentNames := make([]string, 0, len(n.n.Arguments))
	for _, arg := range n.n.Arguments {
		argumentNames = append(argumentNames, string(arg.ArgName))
	}

	return sqlbase.FunctionDescriptor{
		Name:          string(n.n.FunctionName),
		ArgumentTypes: encodeArgTypes,
		ReturnType:    encodeReturnTypes,
		TypesLength:   encodeTypeLens,
		FunctionBody:  n.n.FuncBody,
		FunctionType:  uint32(sqlbase.DefinedFunction),
		Language:      "LUA",
		DbName:        sessionData.Database,
		ArgumentNames: argumentNames,
	}, nil
}

// CheckUdf is used to check whether the parameters, return type, function body
// are legal
func (n *createFunctionNode) CheckUdf(params runParams) error {
	L := sqlbase.NewRestrictedLuaState()
	defer L.Close()

	// Check lua syntax
	if err := L.ParseString(n.n.FuncBody, strings.ToLower(string(n.n.FunctionName)), len(n.n.Arguments)); err != nil {
		return err
	}

	// Reject unsafe syntax in Lua UDF body.
	if err := checkLuaUDFNoUnsafeSyntax(n.n.FuncBody, strings.ToLower(string(n.n.FunctionName))); err != nil {
		return err
	}

	return nil
}

// checkLuaUDFNoUnsafeSyntax rejects unsafe syntax in Lua UDF body.
func checkLuaUDFNoUnsafeSyntax(source string, udfName string) error {
	stmts, err := parseLuaChunk(source, udfName)
	if err != nil {
		return err
	}

	return checkLuaStmtsNoUnsafeSyntax(stmts, true)
}

// parseLuaChunk parses lua source into ast statements.
func parseLuaChunk(source string, udfName string) ([]luaast.Stmt, error) {
	return goluaparse.Parse(strings.NewReader(source), udfName)
}

// checkLuaStmtsNoUnsafeSyntax checks unsafe syntax for lua stmts.
func checkLuaStmtsNoUnsafeSyntax(stmts []luaast.Stmt, topLevel bool) error {
	for _, stmt := range stmts {
		if err := checkLuaStmtNoUnsafeSyntax(stmt, topLevel); err != nil {
			return err
		}
	}
	return nil
}

// checkLuaStmtNoUnsafeSyntax checks unsafe syntax for lua stmt.
func checkLuaStmtNoUnsafeSyntax(stmt luaast.Stmt, topLevel bool) error {
	switch s := stmt.(type) {
	case nil:
		return nil

	case *luaast.AssignStmt:
		for _, expr := range s.Lhs {
			if err := checkLuaExprNoUnsafeSyntax(expr); err != nil {
				return err
			}
		}
		for _, expr := range s.Rhs {
			if err := checkLuaExprNoUnsafeSyntax(expr); err != nil {
				return err
			}
		}
		return nil

	case *luaast.LocalAssignStmt:
		for _, expr := range s.Exprs {
			if err := checkLuaExprNoUnsafeSyntax(expr); err != nil {
				return err
			}
		}
		return nil

	case *luaast.FuncCallStmt:
		return luaUDFFunctionCallNotAllowedError()

	case *luaast.DoBlockStmt:
		return checkLuaStmtsNoUnsafeSyntax(s.Stmts, false)

	case *luaast.WhileStmt:
		if err := checkLuaExprNoUnsafeSyntax(s.Condition); err != nil {
			return err
		}
		return checkLuaStmtsNoUnsafeSyntax(s.Stmts, false)

	case *luaast.RepeatStmt:
		if err := checkLuaStmtsNoUnsafeSyntax(s.Stmts, false); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(s.Condition)

	case *luaast.IfStmt:
		if err := checkLuaExprNoUnsafeSyntax(s.Condition); err != nil {
			return err
		}
		if err := checkLuaStmtsNoUnsafeSyntax(s.Then, false); err != nil {
			return err
		}
		return checkLuaStmtsNoUnsafeSyntax(s.Else, false)

	case *luaast.NumberForStmt:
		if err := checkLuaExprNoUnsafeSyntax(s.Init); err != nil {
			return err
		}
		if err := checkLuaExprNoUnsafeSyntax(s.Limit); err != nil {
			return err
		}
		if err := checkLuaExprNoUnsafeSyntax(s.Step); err != nil {
			return err
		}
		return checkLuaStmtsNoUnsafeSyntax(s.Stmts, false)

	case *luaast.GenericForStmt:
		for _, expr := range s.Exprs {
			if err := checkLuaExprNoUnsafeSyntax(expr); err != nil {
				return err
			}
		}
		return checkLuaStmtsNoUnsafeSyntax(s.Stmts, false)

	case *luaast.FuncDefStmt:
		if !topLevel {
			return luaUDFNestedFunctionNotAllowedError()
		}
		if s.Func == nil {
			return nil
		}

		// Top-level function definition itself is allowed, but its body must be
		// checked. Do not pass FunctionExpr into checkLuaExprNoUnsafeSyntax here,
		// because FunctionExpr is normally rejected as nested/anonymous function.
		return checkLuaStmtsNoUnsafeSyntax(s.Func.Stmts, false)

	case *luaast.ReturnStmt:
		for _, expr := range s.Exprs {
			if err := checkLuaExprNoUnsafeSyntax(expr); err != nil {
				return err
			}
		}
		return nil

	case *luaast.BreakStmt:
		return nil

	case *luaast.LabelStmt:
		// Label itself does not call functions.
		return nil

	case *luaast.GotoStmt:
		// Goto itself does not call functions.
		// If you want a stricter subset, reject it here.
		return nil

	default:
		// Security code should not silently allow unknown syntax.
		return luaUDFUnsupportedSyntaxError(stmt)
	}
}

// checkLuaExprNoUnsafeSyntax checks unsafe syntax for lua expr.
func checkLuaExprNoUnsafeSyntax(expr luaast.Expr) error {
	switch e := expr.(type) {
	case nil:
		return nil

	case *luaast.FuncCallExpr:
		return luaUDFFunctionCallNotAllowedError()

	case *luaast.FunctionExpr:
		return luaUDFNestedFunctionNotAllowedError()

	case *luaast.AttrGetExpr:
		if err := checkLuaExprNoUnsafeSyntax(e.Object); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(e.Key)

	case *luaast.TableExpr:
		for _, field := range e.Fields {
			if field == nil {
				continue
			}
			if err := checkLuaExprNoUnsafeSyntax(field.Key); err != nil {
				return err
			}
			if err := checkLuaExprNoUnsafeSyntax(field.Value); err != nil {
				return err
			}
		}
		return nil

	case *luaast.UnaryMinusOpExpr:
		return checkLuaExprNoUnsafeSyntax(e.Expr)

	case *luaast.UnaryNotOpExpr:
		return checkLuaExprNoUnsafeSyntax(e.Expr)

	case *luaast.UnaryLenOpExpr:
		return checkLuaExprNoUnsafeSyntax(e.Expr)

	case *luaast.LogicalOpExpr:
		if err := checkLuaExprNoUnsafeSyntax(e.Lhs); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(e.Rhs)

	case *luaast.RelationalOpExpr:
		if err := checkLuaExprNoUnsafeSyntax(e.Lhs); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(e.Rhs)

	case *luaast.StringConcatOpExpr:
		if err := checkLuaExprNoUnsafeSyntax(e.Lhs); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(e.Rhs)

	case *luaast.ArithmeticOpExpr:
		if err := checkLuaExprNoUnsafeSyntax(e.Lhs); err != nil {
			return err
		}
		return checkLuaExprNoUnsafeSyntax(e.Rhs)

	case *luaast.IdentExpr,
		*luaast.NilExpr,
		*luaast.TrueExpr,
		*luaast.FalseExpr,
		*luaast.NumberExpr,
		*luaast.StringExpr,
		*luaast.Comma3Expr:
		return nil

	default:
		return luaUDFUnsupportedSyntaxError(expr)
	}
}

// luaUDFUnsupportedSyntaxError return call function error.
func luaUDFFunctionCallNotAllowedError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "lua udf is not allowed to call functions")
}

// luaUDFUnsupportedSyntaxError return define nested error.
func luaUDFNestedFunctionNotAllowedError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "lua udf is not allowed to define nested functions")
}

// luaUDFUnsupportedSyntaxError return unknown syntax error.
func luaUDFUnsupportedSyntaxError(n interface{}) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported lua udf syntax %T", n)
}

func (*createFunctionNode) Next(runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums          { return tree.Datums{} }
func (*createFunctionNode) Close(context.Context)        {}

// getFuncDataTypeAndLen returns the type and length used by the defined function.
func getFuncDataTypeAndLen(typ *types.T) (sqlbase.DataType, int32) {
	//if typ.InternalType.TypeEngine != 0 && !typ.IsTypeEngineSet(types.TIMESERIES) {
	//	return sqlbase.DataType_UNKNOWN
	//}
	switch typ.Name() {
	case "timestamp":
		return sqlbase.DataType_TIMESTAMP, 0
	case "int2":
		return sqlbase.DataType_SMALLINT, 0
	case "int4":
		return sqlbase.DataType_INT, 0
	case "int":
		return sqlbase.DataType_BIGINT, 0
	case "float4":
		return sqlbase.DataType_FLOAT, 0
	case "float":
		return sqlbase.DataType_DOUBLE, 0
	case "char":
		return sqlbase.DataType_CHAR, typ.Width()
	case "nchar":
		return sqlbase.DataType_NCHAR, typ.Width()
	case "varchar":
		return sqlbase.DataType_VARCHAR, typ.Width()
	case "nvarchar":
		return sqlbase.DataType_NVARCHAR, typ.Width()
	default:
		return sqlbase.DataType_UNKNOWN, 0
	}
}

// getTypesAndLength return defined function's argTypes, returnTypes, typeLens
// Input:   None
// Output:
//
//	1.argTypes     - An array of function's argument type
//	2.returnTypes  - An array of function's return type
//	3.typeLens     - An array of function's argument and return type length
func getTypesAndLength(
	args tree.FuncArgDefs, ret *types.T,
) (*tree.DArray, *tree.DArray, *tree.DArray, error) {
	typeLens := tree.NewDArray(types.Int)
	var lengthArray tree.Datums

	argTypes := tree.NewDArray(types.Int)
	var argTypeArray tree.Datums
	for _, val := range args {
		argType, argLen := getFuncDataTypeAndLen(val.ArgType)
		if argType == sqlbase.DataType_UNKNOWN {
			return nil, nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "argument type %s is not supported", val.ArgType.SQLString())
		}
		argTypeArray = append(argTypeArray, tree.NewDInt(tree.DInt(argType)))
		lengthArray = append(lengthArray, tree.NewDInt(tree.DInt(argLen)))
	}
	argTypes.Array = argTypeArray

	returnTypes := tree.NewDArray(types.Int)
	var returnTypeArray tree.Datums
	returnType, returnLen := getFuncDataTypeAndLen(ret)
	if returnType == sqlbase.DataType_UNKNOWN {
		return nil, nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "return type %s is not supported", ret.SQLString())
	}
	returnTypeArray = append(returnTypeArray, tree.NewDInt(tree.DInt(returnType)))
	lengthArray = append(lengthArray, tree.NewDInt(tree.DInt(returnLen)))
	returnTypes.Array = returnTypeArray
	typeLens.Array = lengthArray

	return argTypes, returnTypes, typeLens, nil
}

// RegisterSQLFunction registers a SQL UDF into the ConcurrentFunDefs
// and notifies other nodes through gossip.
func RegisterSQLFunction(params runParams, fn *tree.SQLFunctionWrapper) error {
	funcName := string(fn.FunctionName)

	udfDef, err := makeSQLUDFDefinition(fn)
	if err != nil {
		return err
	}

	tree.ConcurrentFunDefs.RegisterFunc(funcName, udfDef)

	if err := GossipUdfAdded(params.p.execCfg.Gossip, funcName); err != nil {
		return err
	}

	return nil
}

// makeSQLUDFDefinition builds a FunctionDefinition for a SQL UDF.
func makeSQLUDFDefinition(fn *tree.SQLFunctionWrapper) (*tree.FunctionDefinition, error) {
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

// SQLUDFCallProcedure executes SQL UDF by procedure and returns
// the first row and first column as the scalar function result.
func (p *planner) SQLUDFCallProcedure(
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

	if err := cp.startExec(params); err != nil {
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
func (p *planner) makeSQLUDFInternalPlanner(
	ctx context.Context, funcName string,
) (*planner, func()) {
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
	udfPlanner.avoidCachedDescriptors = p.avoidCachedDescriptors
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

// collectFirstProcedureResultSetForSQLUDF collects the result rows produced by
// the first result set of a procedure.
func collectFirstProcedureResultSetForSQLUDF(
	ctx context.Context, params runParams, cp *callProcedureNode, funcName string,
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

// makeCallProcedureNodeForSQLUDF builds an internal CALL plan for executing a
// SQL UDF backing procedure.
func (p *planner) makeCallProcedureNodeForSQLUDF(
	ctx context.Context, funcName string, args tree.Datums,
) (*callProcedureNode, runParams, error) {
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
		return nil, runParams{}, err
	}

	cp, ok := unwrapCallProcedureNode(p.curPlan.plan)
	if !ok {
		return nil, runParams{}, pgerror.Newf(pgcode.Warning,
			"expected CALL procedure plan for SQL UDF %s, got %T",
			funcName,
			p.curPlan.plan,
		)
	}

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: p.ExtendedEvalContext(),
		p:               p,
	}
	return cp, params, nil
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

// unwrapCallProcedureNode extracts a callProcedureNode from a planned planNode.
func unwrapCallProcedureNode(plan planNode) (*callProcedureNode, bool) {
	cp, ok := plan.(*callProcedureNode)
	return cp, ok
}

// ResolveFunctionFromCatalog loads a persisted user defined function
// from system.user_defined_routine and registers it into tree.ConcurrentFunDefs.
func (p *planner) ResolveFunctionFromCatalog(
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
func (p *planner) loadUDF(funcName string) (*tree.FunctionDefinition, bool, error) {
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

		fd, err := makeSQLUDFDefinition(wrapper)
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
