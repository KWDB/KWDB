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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func (b *Builder) buildCallProcedure(cp *tree.CallProcedure, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	b.insideObjectDef.SetFlags(InsideProcedureDef)
	b.trackViewDeps = true
	b.qualifyDataSourceNamesInAST = true

	// reset fields which is used specifically while compiling procedure
	// and recover panic
	defer func() {
		b.insideObjectDef.ClearFlags(InsideProcedureDef)
		b.trackViewDeps = false
		b.viewDeps = nil
		b.qualifyDataSourceNamesInAST = false

		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			panic(recErr)
		default:
			panic(recErr)
		}
	}()

	// resolve procDesc
	found, source, err := b.catalog.ResolveProcCatalog(b.ctx, &cp.Name, true)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(pgerror.Newf(
			pgcode.UndefinedObject, "procedure %q does not exist", tree.ErrString(&cp.Name)))
	}

	// check parameter count
	var inputParams []tree.Statement
	if len(cp.Exprs) != len(source.Parameters) {
		panic(pgerror.New(pgcode.InvalidParameterValue, "Number of arguments does not match stored procedure definition"))
	}
	for i := range source.Parameters {
		param := source.Parameters[i]
		inputParams = append(inputParams, &tree.Declaration{
			Typ:      tree.DeclVariable,
			Variable: tree.DeclareVar{VarName: string(param.Name), Typ: param.Type, DefaultExpr: cp.Exprs[i], IsParameter: true},
		})
	}

	// get AST from metadata
	createProcStmt, err := ParseCreateProcedure(source.BodyStr)
	if err != nil {
		panic(err)
	}

	blockComm := b.getProcCommand(&createProcStmt.Block, nil, inScope, inputParams, nil)
	if b.IsProcStatementTypeRows {
		cp.IsRows = true
	}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.FormatNode(&source.Name)
	f.WriteString("(")
	f.FormatNode(&cp.Exprs)
	f.WriteString(")")
	callStr := f.CloseAndGetString()

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCallProcedure(
		&memo.CallProcedurePrivate{
			ProcName: string(source.Name.TableName),
			ProcCall: callStr,
			ProcComm: blockComm,
		},
	)
	return outScope
}

func checkProcParamType(typ *types.T) error {
	switch typ.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily, types.StringFamily, types.TimestampFamily, types.TimestampTZFamily:
	default:
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "unsupported type %s inside stored procedure", typ.SQLString())
	}
	return nil
}
