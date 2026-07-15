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

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildCreateFunction(cf *tree.CreateFunction, inScope *scope) (outScope *scope) {
	if err := CheckUdfName(cf, b.evalCtx); err != nil {
		panic(err)
	}

	switch cf.Language {
	case tree.FunctionLangSQL:
		for _, param := range cf.Arguments {
			if err := checkFuncParameterType(param.ArgType); err != nil {
				panic(err)
			}
		}
		if err := checkFuncReturnType(cf.ReturnType); err != nil {
			panic(err)
		}

		// CREATE FUNCTION ... LANGUAGE SQL BEGIN ... END
		cp, err := makeCreateProcedureFromSQLFunction(cf)
		if err != nil {
			panic(err)
		}
		return b.buildCreateProcedure(cp, inScope)

	case tree.FunctionLangLua:
		// Keep existing LUA UDF behavior:
		// buildOpaque -> planner.CreateFunction -> createFunctionNode.
		if outScope := b.tryBuildOpaque(cf, inScope); outScope != nil {
			b.DisableMemoReuse = true
			return outScope
		}
		panic(errors.AssertionFailedf("failed to create language lua function: unexpected create function routing"))

	default:
		panic(pgerror.Newf(pgcode.FeatureNotSupported, "unsupported function language: %s", cf.Language))
	}
}

// makeCreateProcedureFromSQLFunction makes procedure stmt by function stmt.
func makeCreateProcedureFromSQLFunction(fn *tree.CreateFunction) (*tree.CreateProcedure, error) {
	params, err := makeProcParamsFromFuncArgs(fn.Arguments)
	if err != nil {
		return nil, err
	}

	procName := makeProcNameFromFuncName(fn.FunctionName)

	return &tree.CreateProcedure{
		Name:       procName,
		Parameters: params,
		Block:      *fn.Block,

		SQLFunction: &tree.SQLFunctionWrapper{
			FunctionName: fn.FunctionName,
			Arguments:    fn.Arguments,
			ReturnType:   fn.ReturnType,
		},
	}, nil
}

// makeProcParamsFromFuncArgs makes procedure parameters by function parameters.
func makeProcParamsFromFuncArgs(args tree.FuncArgDefs) ([]*tree.ProcedureParameter, error) {
	params := make([]*tree.ProcedureParameter, 0, len(args))

	for _, arg := range args {
		param := &tree.ProcedureParameter{
			Name: arg.ArgName,
			Type: arg.ArgType,
		}
		params = append(params, param)
	}

	return params, nil
}

// makeProcNameFromFuncName makes procedure name by function name.
func makeProcNameFromFuncName(name tree.Name) tree.TableName {
	return tree.MakeUnqualifiedTableName(name)
}

// CheckUdfName is used to check whether the function name is legal
func CheckUdfName(cf *tree.CreateFunction, evalCtx *tree.EvalContext) error {
	funcName := string(cf.FunctionName)
	if funcName == "" {
		return pgerror.New(pgcode.Syntax, "function name cannot be empty when creating a new function")
	}

	// check if there is already a function with the same name
	// by looking up the all builtins function.
	if _, ok := tree.FunDefs[funcName]; ok {
		return pgerror.Newf(pgcode.DuplicateObject, "function named '%s' already exists. Please choose a different name", funcName)
	}

	return nil
}

// checkFuncParameterType checks SQL function parameter type.
func checkFuncParameterType(typ *types.T) error {
	switch typ.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily, types.StringFamily, types.CollatedStringFamily, types.TimestampFamily, types.TimestampTZFamily:
	default:
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "unsupported parameter type %s in sql function", typ.SQLString())
	}
	return nil
}

// checkFuncReturnType checks SQL function return type.
func checkFuncReturnType(typ *types.T) error {
	switch typ.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily, types.StringFamily, types.CollatedStringFamily, types.TimestampFamily, types.TimestampTZFamily:
	default:
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "unsupported return type %s in sql function", typ.SQLString())
	}
	return nil
}

// checkFuncBodyType checks type of SQL function boy.
func checkFuncBodyType(typ *types.T) error {
	switch typ.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily, types.StringFamily, types.CollatedStringFamily, types.TimestampFamily, types.TimestampTZFamily:
	default:
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "unsupported type %s in sql function", typ.SQLString())
	}
	return nil
}
