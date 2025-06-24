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
)

func (b *Builder) buildCreateProcedure(cv *tree.CreateProcedure, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	b.insideProcDef = true
	b.trackViewDeps = true
	b.qualifyDataSourceNamesInAST = true

	defer func() {
		b.insideProcDef = false
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
	oriName := cv.Name
	sch, resName := b.resolveSchemaForCreate(&cv.Name, tree.RelationalTable)
	cv.Name.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)
	// resolve procDesc
	found, _, err := b.catalog.ResolveProcCatalog(b.ctx, &cv.Name, false)
	if err != nil {
		panic(err)
	}
	if found {
		panic(pgerror.Newf(pgcode.DuplicateRelation, "procedure %q already exists", tree.ErrString(&oriName)))
	}

	var input memo.RelExpr
	input = b.factory.ConstructZeroValues()
	// check duplicated params
	paramNames := make(map[tree.Name]struct{})
	for _, param := range cv.Parameters {
		if _, ok := paramNames[param.Name]; ok {
			// Argument names cannot be used more than once.
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition, "parameter name %q used more than once", param.Name,
			))
		}
		paramNames[param.Name] = struct{}{}
	}

	var inputParams []tree.Statement
	for i := range cv.Parameters {
		param := cv.Parameters[i]
		// check unsupported types in stored procedure
		if err := checkProcParamType(param.Type); err != nil {
			panic(err)
		}
		// collect user defined parameters, treat it as declared variables
		inputParams = append(inputParams, &tree.Declaration{
			Typ:      tree.DeclVariable,
			Variable: tree.DeclareVar{VarName: string(param.Name), Typ: param.Type, IsParameter: true},
		})
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
	b.getProcCommand(&cv.Block, nil, inScope, inputParams)
	fmtCtx.FormatNode(cv)
	cv.BodyStr = fmtCtx.CloseAndGetString()

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateProcedure(
		input,
		&memo.CreateProcedurePrivate{Schema: schID, Syntax: cv, Deps: b.viewDeps},
	)
	return outScope
}
