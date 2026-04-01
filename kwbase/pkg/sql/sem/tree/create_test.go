// Copyright 2012, Google Inc. All rights reserved.
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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateDatabaseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateDatabase
		expected string
	}{
		{
			name: "basic relational",
			node: &CreateDatabase{
				Name:       "testdb",
				EngineType: EngineTypeRelational,
			},
			expected: `CREATE DATABASE testdb`,
		},
		{
			name: "with if not exists",
			node: &CreateDatabase{
				Name:        "testdb",
				IfNotExists: true,
				EngineType:  EngineTypeRelational,
			},
			expected: `CREATE DATABASE IF NOT EXISTS testdb`,
		},
		{
			name: "with all options",
			node: &CreateDatabase{
				Name:       "testdb",
				Template:   "tpl",
				Encoding:   "utf8",
				Collate:    "en_US.utf8",
				CType:      "en_US.utf8",
				EngineType: EngineTypeRelational,
				Comment:    "my comment",
			},
			expected: `CREATE DATABASE testdb TEMPLATE = 'tpl' ENCODING = 'utf8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8' COMMENT = 'my comment'`,
		},
		{
			name: "timeseries database",
			node: &CreateDatabase{
				Name:       "tsdb",
				EngineType: EngineTypeTimeseries,
				TSDatabase: TSDatabase{
					DownSampling: &DownSampling{
						KeepDurationOrLifetime: TimeInput{Value: 30, Unit: "d"},
					},
					PartitionInterval: &TimeInput{Value: 10, Unit: "d"},
				},
			},
			expected: `CREATE TS DATABASE tsdb RETENTIONS 30d PARTITION INTERVAL 10d`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestCreateScheduleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateSchedule
		expected string
	}{
		{
			name: "basic schedule",
			node: &CreateSchedule{
				SQL:          " SELECT 1",
				ScheduleName: NewStrVal("my_sched"),
				Recurrence:   NewStrVal("@daily"),
			},
			expected: `CREATE SCHEDULE 'my_sched' FOR SQL SELECT 1 RECURRING '@daily'`,
		},
		{
			name: "never recurring with if not exists",
			node: &CreateSchedule{
				SQL:         " SELECT 2",
				IfNotExists: true,
			},
			expected: `CREATE SCHEDULE IF NOT EXISTS FOR SQL SELECT 2 RECURRING NEVER`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestAlterScheduleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &AlterSchedule{
		ScheduleName: "sched1",
		Recurrence:   NewStrVal("@hourly"),
		IfExists:     true,
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `ALTER SCHEDULE IF EXISTS sched1 RECURRING '@hourly'`, ctx.CloseAndGetString())
}

func TestCreateIndexFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateIndex
		expected string
	}{
		{
			name: "basic index",
			node: &CreateIndex{
				Name:  "idx1",
				Table: MakeTableName("db", "tbl"),
				Columns: IndexElemList{
					{Column: "c1", Direction: Ascending},
					{Column: "c2", Direction: Descending, NullsOrder: NullsFirst},
				},
			},
			expected: `CREATE INDEX idx1 ON db.public.tbl (c1 ASC, c2 DESC NULLS FIRST)`,
		},
		{
			name: "unique inverted concurrent if not exists",
			node: &CreateIndex{
				Name:         "idx2",
				Table:        MakeTableName("db", "tbl"),
				Unique:       true,
				Inverted:     true,
				Concurrently: true,
				IfNotExists:  true,
				Columns: IndexElemList{
					{Column: "c1"},
				},
				Storing: NameList{"c3", "c4"},
			},
			expected: `CREATE UNIQUE INVERTED INDEX CONCURRENTLY IF NOT EXISTS idx2 ON db.public.tbl (c1) STORING (c3, c4)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestCreateTableFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	colDef, _ := NewColumnTableDef("c1", types.Int, false, nil)
	colDef2, _ := NewColumnTableDef("c2", types.String, false, []NamedColumnQualification{
		{Qualification: NotNullConstraint{}},
	})

	testCases := []struct {
		name     string
		node     *CreateTable
		expected string
	}{
		{
			name: "basic relational table",
			node: &CreateTable{
				Table:     MakeTableName("db", "tbl"),
				TableType: RelationalTable,
				Defs:      TableDefs{colDef, colDef2},
				Comment:   "'test table'",
			},
			expected: `CREATE TABLE db.public.tbl (c1 INT8, c2 STRING NOT NULL) COMMENT = 'test table'`,
		},
		{
			name: "timeseries table",
			node: &CreateTable{
				Table:     MakeTableName("db", "ts_tbl"),
				TableType: TimeseriesTable,
				Defs:      TableDefs{colDef},
				Tags: Tags{
					{TagName: "tag1", TagType: types.Int},
				},
				PrimaryTagList: NameList{"tag1"},
				ActiveTime:     &TimeInput{Value: 1, Unit: "h"},
			},
			expected: `CREATE TABLE db.public.ts_tbl (c1 INT8) TAGS (tag1 INT8 NOT NULL) PRIMARY TAGS(tag1) ACTIVETIME 1h`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestCreateSchemaFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateSchema{
		Schema:      "my_schema",
		IfNotExists: true,
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE SCHEMA IF NOT EXISTS my_schema`, ctx.CloseAndGetString())
}

func TestCreateSequenceFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateSequence{
		Name:        MakeTableName("db", "seq"),
		IfNotExists: true,
		Temporary:   true,
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE TEMPORARY SEQUENCE IF NOT EXISTS db.public.seq`, ctx.CloseAndGetString())
}

func TestCreateRoleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateRole{
		Name:        NewStrVal("my_role"),
		IsRole:      true,
		IfNotExists: true,
		KVOptions: KVOptions{
			{Key: "PASSWORD", Value: NewStrVal("pass")},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE ROLE IF NOT EXISTS 'my_role' WITH "PASSWORD" 'pass'`, ctx.CloseAndGetString())
}

func TestAlterRoleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &AlterRole{
		Name:     NewStrVal("my_user"),
		IsRole:   false,
		IfExists: true,
		KVOptions: KVOptions{
			{Key: "LOGIN", Value: nil}, // true/false flag
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `ALTER USER IF EXISTS 'my_user' WITH "LOGIN"`, ctx.CloseAndGetString())
}

func TestCreateViewFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateView{
		Name:         MakeTableName("db", "v1"),
		ColumnNames:  NameList{"a", "b"},
		Materialized: true,
		AsSource: &Select{
			Select: &SelectClause{
				Exprs: SelectExprs{
					{Expr: &UnresolvedName{NumParts: 1, Parts: NameParts{"a"}}},
					{Expr: &UnresolvedName{NumParts: 1, Parts: NameParts{"b"}}},
				},
				From: From{
					Tables: TableExprs{
						&AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
					},
				},
			},
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE MATERIALIZED VIEW db.public.v1 (a, b) AS SELECT a, b FROM db.public.tbl`, ctx.CloseAndGetString())
}

func TestRefreshMaterializedViewFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	n, _ := NewUnresolvedObjectName(1, [3]string{"mv1"}, NoAnnotation)
	node := &RefreshMaterializedView{
		Name: n,
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `REFRESH MATERIALIZED VIEW mv1`, ctx.CloseAndGetString())
}

func TestCreateStatsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateStats{
		Name:        "stat1",
		ColumnNames: NameList{"c1", "c2"},
		Table:       &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
		Options: CreateStatsOptions{
			Throttling: 0.5,
		},
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE STATISTICS stat1 ON c1, c2 FROM db.public.tbl WITH OPTIONS THROTTLING 0.5`, ctx.CloseAndGetString())
}

func TestCreateAuditFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateAudit{
		Name: "aud1",
		Target: AuditTarget{
			Type: "TABLE ",
			Name: MakeTableName("db", "tbl"),
		},
		Operations: NameList{"SELECT", "INSERT"},
		Operators:  NameList{"user1"},
		Level:      NewStrVal("HIGH"),
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE AUDIT aud1 ON TABLE db.public.tbl FOR "SELECT", "INSERT" TO user1 LEVEL 'HIGH'`, ctx.CloseAndGetString())
}

func TestCreateFunctionFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &CreateFunction{
		FunctionName: "my_func",
		Arguments: FuncArgDefs{
			{ArgName: "a", ArgType: types.Int},
			{ArgName: "b", ArgType: types.String},
		},
		ReturnType: types.Bool,
		FuncBody:   "return a > 0",
	}

	ctx := NewFmtCtx(FmtSimple)
	node.Format(ctx)
	require.Equal(t, `CREATE FUNCTION my_func (a INT8, b STRING) RETURNS BOOL LUA BEGIN 'return a > 0' END`, ctx.CloseAndGetString())
}
