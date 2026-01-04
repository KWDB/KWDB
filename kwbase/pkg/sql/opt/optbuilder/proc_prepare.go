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
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// buildProcPrepare compiles proc_prepare_stmt (see sql.y)
func (b *Builder) buildProcPrepare(procPrepare *tree.Prepare) memo.ProcCommand {
	if b.PrepareHelper.Check == nil || b.PrepareHelper.Exec == nil || b.PrepareHelper.Add == nil {
		err := pgerror.Newf(pgcode.Internal, "system error, call back check function is nil")
		panic(err)
	}
	b.factory.Memo().SetProcPrepare(true)

	typeHints, err := b.PrepareHelper.Check(procPrepare, procPrepare.NumPlaceholders)
	if err != nil {
		panic(err)
	}

	res, err1 := b.PrepareHelper.Exec(b.ctx, parser.Statement{
		SQL:             tree.AsStringWithFlags(procPrepare.Statement, tree.FmtParsable),
		AST:             procPrepare.Statement,
		NumPlaceholders: procPrepare.NumPlaceholders,
		NumAnnotations:  procPrepare.NumAnnotations,
	}, typeHints, 2, b.semaCtx.ProcUserDefinedVars, InsidePrepareOfProcDef)
	if err1 != nil {
		panic(err1)
	}

	if b.PrepareNamespaces == nil {
		b.PrepareNamespaces = make(map[string]interface{})
	}
	b.PrepareNamespaces[string(procPrepare.Name)] = res

	return &memo.PrepareCommand{Name: procPrepare.Name, Res: res, AddFn: b.PrepareHelper.Add}
}

// buildProcExecute compiles proc_execute_stmt (see sql.y)
func (b *Builder) buildProcExecute(procExecute *tree.Execute) memo.ProcCommand {
	if b.ExecuteHelper.Check == nil || b.ExecuteHelper.GetReplacedMemo == nil ||
		b.ExecuteHelper.GetPlaceholder == nil || b.ExecuteHelper.GetStatement == nil {
		err := pgerror.Newf(pgcode.Internal, "system error, call back check function is nil")
		panic(err)
	}
	b.factory.Memo().SetProcPrepare(true)

	ps, err := b.ExecuteHelper.Check(procExecute)
	if err != nil {
		if ps1, ok := b.PrepareNamespaces[string(procExecute.Name)]; !ok {
			panic(err)
		} else {
			ps = ps1
		}
	}

	pInfo, err1 := b.ExecuteHelper.GetPlaceholder(ps, string(procExecute.Name), procExecute.Params, b.semaCtx)
	if err1 != nil {
		panic(err1)
	}

	statementType, sqlStr := b.ExecuteHelper.GetStatement(ps)
	if statementType == int(tree.Rows) && !b.IsProcStatementTypeRows {
		b.IsProcStatementTypeRows = true
	}

	return &memo.ExecuteCommand{
		Execute:       procExecute,
		PhInfo:        pInfo,
		GetMemoFn:     b.ExecuteHelper.GetReplacedMemo,
		StatementType: statementType,
		SQLStr:        sqlStr,
	}
}

// buildProcPrepare compiles proc_deallocate_stmt (see sql.y)
func (b *Builder) buildProcDeallocate(s *tree.Deallocate) memo.ProcCommand {
	if b.DeallocateHelper.Check == nil {
		err := pgerror.Newf(pgcode.Internal, "system error, call back check function is nil")
		panic(err)
	}
	b.factory.Memo().SetProcPrepare(true)
	if s.Name == "" {
		b.PrepareNamespaces = nil
		return &memo.DeallocateCommand{}
	}

	ok := b.DeallocateHelper.Check(string(s.Name))
	_, ok1 := b.PrepareNamespaces[string(s.Name)]
	if ok || ok1 {
		delete(b.PrepareNamespaces, string(s.Name))
		return &memo.DeallocateCommand{Name: string(s.Name)}
	}
	err := pgerror.Newf(pgcode.InvalidSQLStatementName, "prepared statement %q does not exist", s.Name)
	panic(err)
}
