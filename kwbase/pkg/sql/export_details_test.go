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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/distsql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCheckBeforeExport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	srv := s.SQLServer().(*Server)
	stmtBuf := NewStmtBuf()
	flushed := make(chan []resWithPos)
	clientComm := &internalClientComm{
		sync: func(res []resWithPos) {
			flushed <- res
		},
	}
	connHandler, err := srv.SetupConn(ctx, SessionArgs{User: security.RootUser}, stmtBuf, clientComm, MemoryMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	ast := &tree.Export{}
	conn := connHandler.ex
	res := conn.clientComm.CreateStatementResult(
		ast,
		NeedRowDesc,
		0,
		nil, /* formatCodes */
		conn.sessionData.DataConversion,
		0,  /* limit */
		"", /* portalName */
		conn.implicitTxn(),
	)

	p := makeTestPlanner()
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	conn.planner.ExtendedEvalContext().TxnImplicit = true
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)
	ast.FileFormat = "CSV"
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	p.txn = db.NewTxn(ctx, "test")
	conn.planner.txn = p.txn
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.NoError(t, err)

	ast.Options = tree.KVOptions{{Key: "test", Value: nil}}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{{Key: "delimiter", Value: tree.NewDInt(1)}}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString("1")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.NoError(t, err)

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.FileFormat = "SQL"
	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.FileFormat = "CSV"
	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString("£")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "meta_only", Value: nil},
		{Key: "data_only", Value: nil},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "column_name", Value: nil},
	}
	ast.FileFormat = "SQL"
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "chunk_rows", Value: tree.NewDString("10")},
	}
	ast.FileFormat = "SQL"
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.FileFormat = "CSV"
	ast.Options = tree.KVOptions{
		{Key: "chunk_rows", Value: tree.NewDString("100001")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "chunk_rows", Value: tree.NewDString("-1")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "chunk_rows", Value: tree.NewDString("a")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "meta_only", Value: nil},
		{Key: "chunk_rows", Value: tree.NewDString("a")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.FileFormat = "SQL"
	ast.Options = tree.KVOptions{
		{Key: "nullas", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "enclosed", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.FileFormat = "CSV"
	ast.Options = tree.KVOptions{
		{Key: "enclosed", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "enclosed", Value: tree.NewDString("b")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "enclosed", Value: tree.NewDString("£")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	// sql not support
	ast.FileFormat = "SQL"
	ast.Options = tree.KVOptions{
		{Key: "escaped", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	// only support single char
	ast.FileFormat = "CSV"
	ast.Options = tree.KVOptions{
		{Key: "escaped", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	// value not support
	ast.Options = tree.KVOptions{
		{Key: "escaped", Value: tree.NewDString("b")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	// value out of range 1 ~ 127
	ast.Options = tree.KVOptions{
		{Key: "escaped", Value: tree.NewDString("£")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	// char set invalid charset value, value must be 'GBK''GB18030''BIG5''UTF-8'
	ast.Options = tree.KVOptions{
		{Key: "charset", Value: tree.NewDString("10")},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, err)

	ast.Options = tree.KVOptions{
		{Key: "comment", Value: nil},
		{Key: "privileges", Value: nil},
	}
	ast.Query = &tree.Select{Select: &tree.SelectClause{}}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, res.Err())

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
		{Key: "enclosed", Value: tree.NewDString(",")},
	}
	ast.Query = &tree.Select{Select: &tree.SelectClause{}}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, res.Err())

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
		{Key: "escaped", Value: tree.NewDString(",")},
	}
	ast.Query = &tree.Select{Select: &tree.SelectClause{}}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.Error(t, res.Err())

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
		{Key: "chunk_rows", Value: tree.NewDString("10")},
		{Key: "privileges", Value: nil},
		{Key: "foreign_key", Value: nil},
		{Key: "column_name", Value: nil},
		{Key: "comment", Value: nil},
	}
	_, err = checkBeforeExport(ctx, p, res, conn, ast)
	require.NoError(t, err)
}

func execSQL(ctx context.Context, connHandler ConnectionHandler, sql string) error {
	sqlStmt, err := parser.ParseOne(sql)
	if err != nil {
		return err
	}
	err = connHandler.PushStmt(ctx, ExecStmt{Statement: sqlStmt})
	if err != nil {
		return err
	}
	_, err = connHandler.ExecRestfulStmt(ctx)
	if err != nil {
		return err
	}
	return nil
}

func TestExportRelationalAndTsDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	srv := s.SQLServer().(*Server)
	stmtBuf := NewStmtBuf()
	flushed := make(chan []resWithPos)
	clientComm := &internalClientComm{
		sync: func(res []resWithPos) {
			flushed <- res
		},
	}
	connHandler, err := srv.SetupConn(ctx, SessionArgs{User: security.RootUser}, stmtBuf, clientComm, MemoryMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	connHandler.NewStmt()
	if err := execSQL(ctx, connHandler, "CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatal(err)
	}
	if err := execSQL(ctx, connHandler, "COMMENT ON database test is 'database for comment';"); err != nil {
		t.Fatal(err)
	}
	if err := execSQL(ctx, connHandler, "CREATE TABLE if not exists test.t1 (id int) comment 'testtest'"); err != nil {
		t.Fatal(err)
	}
	conn := connHandler.ex
	ast := &tree.Export{}
	res := conn.clientComm.CreateStatementResult(
		ast,
		NeedRowDesc,
		0,
		nil, /* formatCodes */
		conn.sessionData.DataConversion,
		0,  /* limit */
		"", /* portalName */
		conn.implicitTxn(),
	)
	conn.planner.ExtendedEvalContext().TxnImplicit = true

	p := makeTestPlanner()
	p.txn = db.NewTxn(ctx, "test")
	conn.planner.txn = p.txn
	ast.FileFormat = "CSV"
	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
		{Key: "chunk_rows", Value: tree.NewDString("10")},
		{Key: "foreign_key", Value: nil},
		{Key: "column_name", Value: nil},
		{Key: "comment", Value: nil},
	}
	opt, err := checkBeforeExport(ctx, p, res, conn, ast)
	if err != nil {
		t.Fatal(err)
	}

	err = exportRelationalAndTsDatabase(ctx, p, res, conn, ast, opt)
	require.Error(t, err)

	ast.Database = "test"
	ast.File = tree.NewStrVal("nodelocal://01/test.csv")
	p.stmt = &Statement{Statement: parser.Statement{AST: ast}}
	p.execCfg.InternalExecutor = s.InternalExecutor().(*InternalExecutor)
	p.extendedEvalCtx.ExecCfg.DistSQLSrv = s.DistSQLServer().(*distsql.ServerImpl)
	err = exportRelationalAndTsDatabase(ctx, p, res, conn, ast, opt)
	require.Error(t, res.Err())

	ast.Options = tree.KVOptions{
		{Key: "delimiter", Value: tree.NewDString(",")},
		{Key: "chunk_rows", Value: tree.NewDString("10")},
		{Key: "foreign_key", Value: nil},
		{Key: "column_name", Value: nil},
	}
	opt, err = checkBeforeExport(ctx, p, res, conn, ast)
	if err != nil {
		t.Fatal(err)
	}
	err = exportRelationalAndTsDatabase(ctx, p, res, conn, ast, opt)
	require.NoError(t, err)
}
