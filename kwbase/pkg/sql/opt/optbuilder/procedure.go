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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// The buildXXX functions are used to compile the SQL statements dedicated to stored procedures,
// generate ProcCommand and store it in outscope for the subsequent generation of execution plans.
// For example：
// the original procedure is:
// begin
//    declare a int;
//    set a = 10;
//    if a > 5 then
//        declare b int;
//        select a1 into b from t1 where a1 > a;
//        set a = b;
//    endif;
//    select * from t2;
//    select a;
// end;
//
// the corresponding ProcCommand is like:
// BlockCommand
// ···DeclareCommand: (d int;)
// ···DeclareCommand: (a int;)
// ···SetCommand: (a = 10;)
// ···IfCommand: (a > 5)
// ······DeclareCommand: (b int;)
// ······IntoCommand: (select a1 into b from t1 where a1 > a;)
// ·········StmtCommand: (select a1 from t1 where a1 > a;)
// ············memo.RelExpr
// ······SetCommand: (set a = b;)
// ···StmtCommand: (select * from t2;)
// ······memo.RelExpr
// ···StmtCommand: select a;
// ······memo.RelExpr

// buildStatements builds all statements to proc command, return ArrayCommand
func (b *Builder) buildStatements(
	states []tree.Statement, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	var result memo.ArrayCommand
	// put parent scope's scopeColumn into current scope
	thenScope := inScope.push()
	thenScope.cols = make([]scopeColumn, len(inScope.cols))
	copy(thenScope.cols, inScope.cols)
	for i := range states {
		// build stmts in THEN. If there exists variable declaration, append it into current scopeColumn
		procComm := b.buildProcCommand(states[i], nil, thenScope, labels)
		result.Bodys = append(result.Bodys, procComm)
	}

	return &result
}

// buildIf builds if command that have condition and block
func (b *Builder) buildIf(
	condition tree.Expr, then []tree.Statement, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	if condition == nil {
		return b.buildStatements(then, inScope, labels)
	}
	var ifCommand memo.IfCommand
	b.isConditionContainsSubquery = true
	cond := b.resolveAndBuildScalar(
		condition,
		types.Bool,
		exprTypeProcedure,
		tree.RejectGenerators|tree.RejectWindowApplications|tree.RejectSubqueries,
		inScope,
	)
	b.isConditionContainsSubquery = false
	ifCommand.Cond = cond
	ifCommand.Then = b.buildStatements(then, inScope, labels)

	return &ifCommand
}

// buildProcIf compiles proc_if_stmt (see sql.y)
func (b *Builder) buildProcIf(
	t *tree.ProcIf, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	b.ensureScopeHasExpr(inScope)
	var Array memo.ArrayCommand
	Array.Bodys = append(Array.Bodys, b.buildIf(t.Condition, t.ThenBody, inScope, labels))
	for i := range t.ElseIfList {
		Array.Bodys = append(Array.Bodys, b.buildIf(t.ElseIfList[i].Condition, t.ElseIfList[i].Stmts, inScope, labels))
	}

	Array.Bodys = append(Array.Bodys, b.buildIf(nil, t.ElseBody, inScope, labels))
	return &Array
}

// buildProcWhile compiles proc_while_stmt (see sql.y)
func (b *Builder) buildProcWhile(
	t *tree.ProcWhile, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	var whileComm memo.WhileCommand
	whileComm.Label = t.Label
	if labels != nil {
		if _, ok := labels[t.Label]; ok && t.Label != "" {
			panic(pgerror.Newf(pgcode.UndefinedObject, "Label %s already exist", t.Label))
		}
		labels[t.Label] = struct{}{}
		defer func() {
			delete(labels, t.Label)
		}()
	}
	// build condition of WHILE
	cond := b.buildSQLExpr(t.Condition, types.Bool, inScope)
	whileComm.Cond = cond

	// inherits scopeColumns from parent scope
	thenScope := inScope.push()
	thenScope.cols = make([]scopeColumn, len(inScope.cols))
	copy(thenScope.cols, inScope.cols)
	for i := range t.Body {
		// build stmts in WHILE body. If there exists variable declaration, append it into current scopeColumn
		proc := b.buildProcCommand(t.Body[i], nil, thenScope, labels)
		whileComm.Then = append(whileComm.Then, proc)
	}
	return &whileComm
}

func makeDefaultExpr(ctx tree.ParseTimeContext, t *types.T) tree.Expr {
	switch t.Family() {
	case types.TimestampFamily:
		d, err := tree.ParseDTimestamp(ctx, "1970-1-1", time.Second)
		if err != nil {
			panic(err)
		}
		return d
	case types.TimestampTZFamily:
		d, err := tree.ParseDTimestampTZ(ctx, "1970-1-1", time.Second)
		if err != nil {
			panic(err)
		}
		return d
	case types.IntFamily:
		return tree.NewDInt(0)
	case types.FloatFamily:
		return tree.NewDFloat(0)
	case types.DecimalFamily:
		d, _ := tree.ParseDDecimal("0")
		return d
	case types.StringFamily:
		return tree.NewStrVal("")
	default:
		panic(errors.AssertionFailedf("unsupported expr"))
	}
}

// buildBlock compiles block statement
func (b *Builder) buildBlock(
	block *tree.Block, desiredTypes []*types.T, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	blockExpr := memo.BlockCommand{Label: block.Label}

	cursorMap := make(map[string]interface{})
	for i := range block.Body {
		if declare, ok := block.Body[i].(*tree.Declaration); ok {
			if declare.IsCursor() {
				e := cursorMap[string(declare.Cursor.CurName)]
				if e != nil {
					panic(pgerror.Newf(pgcode.DuplicateColumn, "duplicate cursor: %q", string(declare.Cursor.CurName)))
				}
				cursorMap[string(declare.Cursor.CurName)] = true
			}
		}
		if controlCur, ok := block.Body[i].(*tree.ControlCursor); ok {
			e := cursorMap[string(controlCur.CurName)]
			if e == nil {
				panic(pgerror.Newf(pgcode.UndefinedObject, "variable name: %s does not exist", string(controlCur.CurName)))
			}
		}
		blockExpr.Body.Bodys = append(blockExpr.Body.Bodys, b.buildProcCommand(block.Body[i], desiredTypes, inScope, labels))
	}

	return &blockExpr
}

// buildControlCursor compiles cursor related statements:
// close_cursor_stmt
// fetch_cursor_stmt
// open_cursor_stmt (see sql.y)
func (b *Builder) buildControlCursor(
	cursorStmt *tree.ControlCursor, inScope *scope,
) memo.ProcCommand {
	if cursorStmt.Command == tree.OpenCursor {
		return b.buildOpenCursor(cursorStmt.CurName, inScope)
	} else if cursorStmt.Command == tree.FetchCursor {
		return b.buildFetchCursor(cursorStmt.CurName, cursorStmt.FetchInto, inScope)
	}
	return b.buildCloseCursor(cursorStmt.CurName, inScope)
}

func (b *Builder) buildOpenCursor(cursorName tree.Name, inScope *scope) memo.ProcCommand {
	var cmd memo.OpenCursorCommand
	cmd.Name = cursorName
	return &cmd
}

func (b *Builder) buildFetchCursor(
	cursorName tree.Name, varName []string, inScope *scope,
) memo.ProcCommand {
	var cmd memo.FetchCursorCommand
	var dstVar []int
	var isFind bool
	for _, name := range varName {
		for _, col := range inScope.cols {
			if string(col.name) == name {
				dstVar = append(dstVar, b.factory.Metadata().ColumnMeta(col.id).RealIdx)
				isFind = true
			}
		}
		if !isFind {
			panic(pgerror.Newf(pgcode.UndefinedObject, "variable name: %s does not exist", name))
		}
	}
	cmd.Name = cursorName
	cmd.DstVar = dstVar
	return &cmd
}

func (b *Builder) buildCloseCursor(cursorName tree.Name, inScope *scope) memo.ProcCommand {
	var cmd memo.CloseCursorCommand
	cmd.Name = cursorName
	return &cmd
}

// buildProcDeclare compiles declare_stmt, which involved variable, handler and cursor (see sql.y)
func (b *Builder) buildProcDeclare(
	declStmt *tree.Declaration, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	if declStmt.IsVar() {
		return b.buildDeclareVar(declStmt.Variable, inScope)
	} else if declStmt.IsCursor() {
		return b.buildDeclareCursor(declStmt.Cursor, inScope)
	} else {
		return b.buildDeclareHandler(declStmt.Handler, inScope, labels)
	}
}

func (b *Builder) buildDeclareVar(dv tree.DeclareVar, inScope *scope) memo.ProcCommand {
	nextIdx := 0
	overWrite := -1
	for i := range inScope.cols {
		if inScope.cols[i].isDeclared {
			if dv.VarName == string(inScope.cols[i].name) {
				if !inScope.cols[i].isPara {
					panic(pgerror.Newf(pgcode.DuplicateColumn, "duplicate variable name: %q", dv.VarName))
				}
				overWrite = i
				break
			} else {
				nextIdx++
			}
		}
	}
	if err := checkProcParamType(dv.Typ); err != nil {
		panic(err)
	}

	var typedExp tree.TypedExpr
	var err error
	if dv.DefaultExpr == nil {
		//dv.DefaultExpr = makeDefaultExpr(b.semaCtx, dv.Typ)
		dv.DefaultExpr = tree.DNull
	}

	opName := "DEFAULT"
	if dv.IsParameter {
		opName = "Parameter"
	}
	if typedExp, err = sqlbase.SanitizeVarFreeExpr(
		dv.DefaultExpr, dv.Typ, opName, b.semaCtx, true /* allowImpure */, false, dv.VarName,
	); err != nil {
		panic(err)
	}

	declComm := &memo.DeclareVarCommand{Col: memo.DeclareCol{
		Name:    tree.Name(dv.VarName),
		Typ:     dv.Typ,
		Default: typedExp,
		Idx:     nextIdx,
	}}
	b.synthesizeDeclareColumn(inScope, dv.VarName, dv.Typ, typedExp, nextIdx, dv.IsParameter, overWrite)
	return declComm
}

func (b *Builder) buildDeclareCursor(dc tree.DeclareCursor, inScope *scope) memo.ProcCommand {
	var curComm memo.DeclCursorCommand
	curComm.Body = b.buildProcCommand(dc.Body, nil, inScope, nil)
	curComm.Name = dc.CurName
	return &curComm
}

func (b *Builder) buildDeclareHandler(
	dh tree.DeclareHandler, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	var handComm memo.DeclHandlerCommand
	switch dh.HandlerOp {
	case tree.HandlerOpContinue:
		handComm.HandlerOp = tree.ModeContinue
	case tree.HandlerOpExit:
		handComm.HandlerOp = tree.ModeExit
	}

	for i := range dh.HandlerFor {
		switch dh.HandlerFor[i] {
		case tree.HandlerForNotFound:
			handComm.HandlerFor |= tree.NOTFOUND
		case tree.HandlerForException:
			handComm.HandlerFor |= tree.SQLEXCEPTION
		}
	}

	handComm.Block = b.buildBlock(&dh.Block, nil, inScope, labels)
	return &handComm
}

// buildProcSet compiles proc_set_stmt (see sql.y)
func (b *Builder) buildProcSet(procSet *tree.ProcSet, inScope *scope) memo.ProcCommand {
	var typedExp tree.TypedExpr
	var col *scopeColumn

	for i := range inScope.cols {
		if string(inScope.cols[i].name) == procSet.Name &&
			inScope.cols[i].isDeclared {
			col = &inScope.cols[i]
			break
		}
	}
	if col == nil {
		panic(sqlbase.NewUndefinedVarColumnError(procSet.Name))
	}
	typedExp = b.buildSQLExpr(procSet.Value, col.typ, inScope)
	// Type check the input column against the corresponding column.
	if !typedExp.ResolvedType().Equivalent(col.typ) {
		panic(pgerror.Newf(pgcode.DatatypeMismatch,
			"value type %s doesn't match type %s of column %q",
			typedExp.ResolvedType(), col.typ, tree.ErrNameString(string(col.name))))
	}

	declComm := &memo.DeclareVarCommand{Col: memo.DeclareCol{
		Name:    tree.Name(procSet.Name),
		Typ:     col.typ,
		Default: typedExp,
	}}

	return declComm
}

// buildProcLeave compiles proc_leave_stmt (see sql.y)
func (b *Builder) buildProcLeave(
	t *tree.ProcLeave, inScope *scope, labels map[string]struct{},
) memo.ProcCommand {
	if labels != nil {
		if _, ok := labels[t.Label]; !ok {
			panic(pgerror.Newf(pgcode.UndefinedObject, "Label %s does not exist", t.Label))
		}
	}
	return &memo.LeaveCommand{Label: t.Label}
}

func (b *Builder) buildProcedureLoop(cv *tree.ProcLoop, inScope *scope) memo.ProcCommand {
	return nil
}

// buildProcStmt compiles basic SQL statements like insert, update, delete, select and so on. (see sql.y)
func (b *Builder) buildProcStmt(
	stmt tree.Statement, desiredTypes []*types.T, inScope *scope,
) memo.ProcCommand {
	stmtComm := &memo.StmtCommand{}
	outScope := b.buildStmtAtRoot(stmt, desiredTypes, inScope)
	stmtComm.Name = stmt.String()
	stmtComm.Body = outScope.expr
	stmtComm.PhysicalProp = outScope.makePhysicalProps()
	stmtComm.Typ = stmt.StatementType()

	if stmt.StatementType() == tree.Rows && !b.IsProcStatementTypeRows {
		// if there exists one stmt whose statementType is ROWS,
		// then the statementType of whole callProcedure stmt is ROWS as WELL,
		// otherwise it's DDL
		b.IsProcStatementTypeRows = true
		b.OutPutCols = outScope.cols
	}
	return stmtComm
}

// buildProcedureTransaction compiles transaction related statements inside procedures (see sql.y)
func (b *Builder) buildProcedureTransaction(stmt tree.Statement) memo.ProcCommand {
	stmtComm := &memo.ProcedureTxnCommand{}
	switch stmt.(type) {
	case *tree.BeginTransaction:
		stmtComm.TxnType = tree.ProcedureTransactionStart
	case *tree.CommitTransaction:
		stmtComm.TxnType = tree.ProcedureTransactionCommit
	case *tree.RollbackTransaction:
		stmtComm.TxnType = tree.ProcedureTransactionRollBack
	}
	return stmtComm
}

// buildIntoCommand compiles select/delete/update into statements inside procedures (see sql.y)
func (b *Builder) buildIntoCommand(
	selInto tree.Statement, targets tree.SelectIntoTargets, inScope *scope,
) memo.ProcCommand {
	stmtComm := &memo.IntoCommand{}
	// ids represents variables' idx which will be assigned by INTO
	var ids []int
	for _, target := range targets {
		found := false
		for _, col := range inScope.cols {
			if string(col.name) == target.DeclareVar {
				ids = append(ids, b.factory.Metadata().ColumnMeta(col.id).RealIdx)
				found = true
				break
			}
		}
		if !found {
			panic(pgerror.Newf(
				pgcode.UndefinedObject, "%s does not exist", target.DeclareVar,
			))
		}
	}
	outScope := b.buildStmtAtRoot(selInto, nil, inScope)
	if len(outScope.cols) != len(targets) {
		panic(pgerror.Newf(
			pgcode.Syntax, "The used SELECT statements have a different number of columns",
		))
	}
	stmtComm.Name = selInto.String()
	stmtComm.PhysicalProp = outScope.makePhysicalProps()
	stmtComm.Body = outScope.expr
	stmtComm.Idx = ids
	return stmtComm
}

// buildSQLExpr type-checks and builds the given SQL expression into a
// ScalarExpr within the given scope.
func (b *Builder) buildSQLExpr(expr tree.Expr, typ *types.T, s *scope) tree.TypedExpr {
	b.isConditionContainsSubquery = true

	// Save any outer CTEs before building the expression, which may have
	// subqueries with inner CTEs.
	prevCTEs := b.cteStack
	b.cteStack = nil
	defer func() {
		b.cteStack = prevCTEs
	}()
	expr, _ = tree.WalkExpr(s, expr)
	typedExpr, err := expr.TypeCheck(b.semaCtx, typ)
	if err != nil {
		panic(err)
	}
	h := &ReplaceProcCond{}
	// Replace the scopeColumn in the procStmt condition with IndexedVar
	typedExpr = h.Replace(typedExpr)
	b.isConditionContainsSubquery = false
	return typedExpr
}

// coerceType implements PLpgSQL type-coercion behavior.
func (b *Builder) coerceType(scalar opt.ScalarExpr, typ *types.T) opt.ScalarExpr {
	resolved := scalar.DataType()
	if !resolved.Identical(typ) {
		scalar = b.factory.ConstructCast(scalar, typ)
		// if CastExpr can convert ConstExpr, set flag to true, else set flag to false
		if _, ok := scalar.(*memo.ConstExpr); ok {
			scalar = b.setConstDeductionEnabled(scalar, true)
		} else {
			scalar.SetConstDeductionEnabled(false)
		}
	}
	return scalar
}

func (b *Builder) ensureScopeHasExpr(s *scope) {
	if s.expr == nil {
		s.expr = b.factory.ConstructZeroValues()
	}
}
