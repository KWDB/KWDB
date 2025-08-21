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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// buildCreateTrigger does semantic check on trigger bounded table and all sql stmts\
// inside trigger definition
func (b *Builder) buildCreateTrigger(ct *tree.CreateTrigger, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	b.insideObjectDef.SetFlags(InsideTriggerDef)
	b.qualifyDataSourceNamesInAST = true
	defer func() {
		b.insideObjectDef.ClearFlags(InsideTriggerDef)
		b.qualifyDataSourceNamesInAST = false
		b.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = nil

		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			panic(recErr)
		default:
			panic(recErr)
		}
	}()

	// Find which table we're working on, check the permissions.
	tab, _, _, _ := b.resolveTableForMutation(&ct.TableName, privilege.CREATE)
	if tab.GetTableType() != tree.RelationalTable {
		panic(pgerror.Newf(pgcode.InvalidCatalogName, "unsupported table type: %s in trigger", tree.TableTypeName(tab.GetTableType())))
	}

	// record column metas in builder
	TriggerTableCols := make([]opt.ColumnMeta, 0)
	pTyp := make(tree.PlaceholderTypes, 0)
	for i := 0; i < tab.ColumnCount(); i++ {
		TriggerTableCols = append(TriggerTableCols, opt.ColumnMeta{
			MetaID: opt.ColumnID(tab.Column(i).ColID()),
			Type:   tab.Column(i).DatumType(),
			Table:  opt.TableID(tab.ID()),
			Name:   string(tab.Column(i).ColName()),
		})
		pTyp = append(pTyp, tab.Column(i).DatumType())
	}
	b.TriggerInfo.TriggerTab[tab.ID()] = TriggerTab{
		ColMetas:       TriggerTableCols,
		CurTriggerTyp:  ct.Event,
		CurActionTime:  ct.ActionTime,
		PlaceholderTyp: pTyp,
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
	fmtCtx.FormatNode(ct)
	ct.BodyStr = fmtCtx.CloseAndGetString()
	// semantic check for database objects inside this trigger
	for _, stmt := range ct.Body.Body {
		b.TriggerInfo.CurTriggerTabID = tab.ID()
		b.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = b.TriggerInfo.TriggerTab[tab.ID()].PlaceholderTyp
		b.buildProcCommand(stmt, nil /* desiredTypes */, inScope, nil)
	}
	// validate NEW/OLD in different DML
	if b.TriggerInfo.TriggerNewOldTyp&oldCol != 0 && ct.Event == tree.TriggerEventInsert {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "cannot use OLD in INSERT event trigger"))
	}
	if b.TriggerInfo.TriggerNewOldTyp&newCol != 0 && ct.Event == tree.TriggerEventDelete {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "cannot use NEW in DELETE event trigger"))
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateTrigger(
		&memo.CreateTriggerPrivate{Syntax: ct},
	)

	return outScope
}

// buildCreateTriggerPG receives a createTriggerStmt in PG style ($$ trigger_body $$), which is
// used to be compatible with JDBC interface.
// This function converts SQL from pgStyle to normal st
func (b *Builder) buildCreateTriggerPG(ct *tree.CreateTriggerPG, inScope *scope) (outScope *scope) {
	// reformat createTrigger SQL to support JDBC
	ctx := tree.NewFmtCtx(tree.FmtSimple)
	ctx.WriteString("CREATE TRIGGER ")
	if ct.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&ct.Name)
	ctx.WriteString(" ")
	ctx.FormatNode(&ct.ActionTime)
	ctx.WriteString(" ")
	ctx.FormatNode(&ct.Event)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&ct.TableName)
	ctx.WriteString(" FOR EACH ROW ")
	if ct.Order != nil {
		ctx.FormatNode(ct.Order)
	}
	ctx.WriteString(ct.BodyStr)
	newSQL := ctx.String()
	newStmt, err := parser.Parse(newSQL)
	if err != nil {
		panic(err)
	}
	if len(newStmt) != 1 {
		panic(pgerror.Newf(pgcode.InvalidObjectDefinition, "please put the statement into the body of BEGIN...END"))
	}
	cp, ok := newStmt[0].AST.(*tree.CreateTrigger)
	if !ok {
		panic(pgerror.Newf(pgcode.Syntax, "cannot parse \"%s\" as CREATE TRIGGER statement", newStmt[0].AST.String()))
	}
	return b.buildCreateTrigger(cp, inScope)
}
