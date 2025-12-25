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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func (b *Builder) buildCreateProcedure(cv *tree.CreateProcedure, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	b.insideObjectDef.SetFlags(InsideProcedureDef)
	b.trackViewDeps = true
	b.qualifyDataSourceNamesInAST = true

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
	oriName := cv.Name
	sch, resName := b.resolveSchemaForCreate(&cv.Name, tree.RelationalTable)
	cv.Name.TableNamePrefix = resName
	// resolve procDesc
	found, _, err := b.catalog.ResolveProcCatalog(b.ctx, &cv.Name, false)
	if err != nil {
		panic(err)
	}
	if found {
		panic(pgerror.Newf(pgcode.DuplicateRelation, "procedure %q already exists", tree.ErrString(&oriName)))
	}

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
	labels := make(map[string]struct{}, 0)
	b.getProcCommand(&cv.Block, nil, inScope, inputParams, labels)
	fmtCtx.FormatNode(cv)
	bodyStr := fmtCtx.CloseAndGetString()
	cv.BodyStr = bodyStr
	if strings.Contains(bodyStr, "::") {
		cv.BodyStr = b.OriginalSQL
	}

	var input memo.RelExpr
	schID := b.factory.Metadata().AddSchema(sch)
	input = b.factory.ConstructZeroValues()

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateProcedure(
		input,
		&memo.CreateProcedurePrivate{Schema: schID, Syntax: cv, Deps: b.viewDeps},
	)
	return outScope
}

// buildCreateProcedurePG
func (b *Builder) buildCreateProcedurePG(
	cv *tree.CreateProcedurePG, inScope *scope,
) (outScope *scope) {
	ctx := tree.NewFmtCtx(tree.FmtSimple)
	ctx.WriteString("CREATE ")
	ctx.WriteString("PROCEDURE ")
	ctx.FormatNode(&cv.Name)
	ctx.WriteString("(")
	for i, arg := range cv.Parameters {
		lex.EncodeRestrictedSQLIdent(&ctx.Buffer, string(arg.Name), lex.EncNoFlags)
		ctx.WriteString(" ")
		ctx.WriteString(arg.Type.SQLString())
		if i < len(cv.Parameters)-1 {
			ctx.WriteString(", ")
		}
	}
	ctx.WriteString(") ")
	ctx.WriteString(cv.BodyStr)
	newSQL := ctx.String()
	newStmt, err := parser.Parse(newSQL)
	if err != nil {
		panic(err)
	}
	if len(newStmt) != 1 {
		panic(pgerror.Newf(pgcode.InvalidFunctionDefinition, "please put the statement into the body of BEGIN...END"))
	}
	cp, ok := newStmt[0].AST.(*tree.CreateProcedure)
	if !ok {
		panic(pgerror.Newf(pgcode.Syntax, "cannot parse \"%s\" as CREATE PROCEDURE statement", newStmt[0].AST.String()))
	}
	return b.buildCreateProcedure(cp, inScope)
}
