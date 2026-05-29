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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateAuditIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_audit_db`)
	require.NoError(t, err)

	// create a test table
	_, err = db.Exec(`CREATE TABLE test_audit_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create object audit for table
	_, err = db.Exec(`CREATE AUDIT my_table_audit ON TABLE test_audit_db.test_table FOR ALL TO ALL`)
	require.NoError(t, err)

	// create object audit with specific operations
	_, err = db.Exec(`CREATE AUDIT my_table_audit_insert ON TABLE test_audit_db.test_table FOR INSERT TO ALL`)
	require.NoError(t, err)

	// create object audit with multiple operations
	_, err = db.Exec(`CREATE AUDIT my_table_audit_multi ON TABLE test_audit_db.test_table FOR INSERT, DELETE, UPDATE TO ALL`)
	require.NoError(t, err)

	// create statement audit for table
	_, err = db.Exec(`CREATE AUDIT my_table_stmt_audit ON TABLE FOR CREATE, DROP TO ALL`)
	require.NoError(t, err)

	// create duplicate audit with IF NOT EXISTS
	_, err = db.Exec(`CREATE AUDIT IF NOT EXISTS my_table_audit ON TABLE test_audit_db.test_table FOR ALL TO ALL`)
	require.NoError(t, err)

	// create duplicate audit without IF NOT EXISTS
	_, err = db.Exec(`CREATE AUDIT my_table_audit ON TABLE test_audit_db.test_table FOR ALL TO ALL`)
	require.Error(t, err)
}

func TestCreateSharedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec(`SET experimental_enable_hash_sharded_indexes = true`)
	require.NoError(t, err)

	// create a test database and table
	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_shared_idx_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_shared_idx_db.test_table (id INT PRIMARY KEY, name STRING, age INT)`)
	require.NoError(t, err)

	// create a basic shared index
	_, err = db.Exec(`CREATE INDEX idx_basic ON test_shared_idx_db.test_table (name)`)
	require.NoError(t, err)

	// create UNIQUE index
	_, err = db.Exec(`CREATE UNIQUE INDEX idx_unique ON test_shared_idx_db.test_table (name)`)
	require.NoError(t, err)

	// create CONCURRENTLY index
	_, err = db.Exec(`CREATE INDEX CONCURRENTLY idx_concurrent ON test_shared_idx_db.test_table (age)`)
	require.NoError(t, err)

	// create index with IF NOT EXISTS
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_basic ON test_shared_idx_db.test_table (name)`)
	require.NoError(t, err)

	// create index with explicit name
	_, err = db.Exec(`CREATE INDEX my_custom_idx ON test_shared_idx_db.test_table (name, age)`)
	require.NoError(t, err)

	// create index with USING BTREE
	_, err = db.Exec(`CREATE INDEX idx_btree ON test_shared_idx_db.test_table USING BTREE (name)`)
	require.NoError(t, err)

	// create index with USING GIN (if supported)
	// _, err = db.Exec(`CREATE INDEX idx_gin ON test_shared_idx_db.test_table USING GIN (name)`)
	// require.NoError(t, err)

	// create index with HASH SHARDED option
	_, err = db.Exec(`CREATE INDEX idx_sharded ON test_shared_idx_db.test_table (name) USING HASH WITH BUCKET_COUNT = 4`)
	require.NoError(t, err)

	// create index with STORING columns
	_, err = db.Exec(`CREATE INDEX idx_storing ON test_shared_idx_db.test_table (name) STORING (age)`)
	require.NoError(t, err)

	// create index with composite columns
	_, err = db.Exec(`CREATE INDEX idx_composite ON test_shared_idx_db.test_table (name, age)`)
	require.NoError(t, err)

	// create index with ASC order (default)
	_, err = db.Exec(`CREATE INDEX idx_asc ON test_shared_idx_db.test_table (name ASC)`)
	require.NoError(t, err)

	// create index with DESC order
	_, err = db.Exec(`CREATE INDEX idx_desc ON test_shared_idx_db.test_table (name DESC)`)
	require.NoError(t, err)
}

func TestCreateAuditNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createAuditNode{
		ifNotExists: false,
		name:        "test_audit",
		eventType:   "TABLE",
		id:          1,
		operations:  []string{"ALL"},
		operators:   tree.NameList{"ALL"},
		whenever:    "ALL",
		condition:   0,
		action:      0,
		level:       0,
	}
	p := makeTestPlanner()
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}

	// Test Next method
	hasNext, err := node.Next(runParam)
	require.NoError(t, err)
	require.False(t, hasNext)

	// Test Values method
	values := node.Values()
	require.Nil(t, values)

	// Test Close method
	node.Close(context.Background())

	// Test SetUpsert method
	err = node.SetUpsert()
	require.NoError(t, err)
}

func TestCreateAuditStatementTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_stmt_audit_db`)
	require.NoError(t, err)

	// test DATABASE statement audit
	_, err = db.Exec(`CREATE AUDIT db_audit_alter ON DATABASE FOR ALTER TO ALL`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT db_audit_create ON DATABASE FOR CREATE TO ALL`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT db_audit_drop ON DATABASE FOR DROP TO ALL`)
	require.NoError(t, err)

	// test TABLE statement audit
	_, err = db.Exec(`CREATE AUDIT table_audit_all ON TABLE FOR ALL TO ALL`)
	require.NoError(t, err)

	// test INDEX statement audit
	_, err = db.Exec(`CREATE AUDIT index_audit ON INDEX FOR CREATE, DROP TO ALL`)
	require.NoError(t, err)

	// test VIEW statement audit
	_, err = db.Exec(`CREATE AUDIT view_audit ON VIEW FOR ALTER, CREATE, DROP TO ALL`)
	require.NoError(t, err)

	// test USER statement audit
	_, err = db.Exec(`CREATE AUDIT user_audit ON USER FOR ALTER, CREATE, DROP TO ALL`)
	require.NoError(t, err)

	// test ROLE statement audit
	_, err = db.Exec(`CREATE AUDIT role_audit ON ROLE FOR CREATE, DROP, GRANT, REVOKE TO ALL`)
	require.NoError(t, err)

	// test JOB statement audit
	_, err = db.Exec(`CREATE AUDIT job_audit ON JOB FOR CANCEL, PAUSE, RESUME TO ALL`)
	require.NoError(t, err)

	// test SCHEDULE statement audit
	_, err = db.Exec(`CREATE AUDIT schedule_audit ON SCHEDULE FOR ALTER, PAUSE, RESUME TO ALL`)
	require.NoError(t, err)

	// test PRIVILEGE statement audit
	_, err = db.Exec(`CREATE AUDIT priv_audit ON PRIVILEGE FOR GRANT, REVOKE TO ALL`)
	require.NoError(t, err)

	// test AUDIT statement audit
	_, err = db.Exec(`CREATE AUDIT audit_audit ON AUDIT FOR ALTER, CREATE, DROP TO ALL`)
	require.NoError(t, err)
}

func TestCreateAuditObjectTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and tables
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_obj_audit_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_obj_audit_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE VIEW test_obj_audit_db.test_view AS SELECT * FROM test_obj_audit_db.test_table`)
	require.NoError(t, err)

	// test TABLE object audit with all operations
	_, err = db.Exec(`CREATE AUDIT table_obj_audit_all ON TABLE test_obj_audit_db.test_table FOR ALL TO ALL`)
	require.NoError(t, err)

	// test TABLE object audit with specific operations
	_, err = db.Exec(`CREATE AUDIT table_obj_audit_select ON TABLE test_obj_audit_db.test_table FOR SELECT TO ALL`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT table_obj_audit_insert ON TABLE test_obj_audit_db.test_table FOR INSERT TO ALL`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT table_obj_audit_update ON TABLE test_obj_audit_db.test_table FOR UPDATE TO ALL`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT table_obj_audit_delete ON TABLE test_obj_audit_db.test_table FOR DELETE TO ALL`)
	require.NoError(t, err)

	// test VIEW object audit with all operations
	_, err = db.Exec(`CREATE AUDIT view_obj_audit_all ON VIEW test_obj_audit_db.test_view FOR ALL TO ALL`)
	require.NoError(t, err)

	// test VIEW object audit with specific operations
	_, err = db.Exec(`CREATE AUDIT view_obj_audit_select ON VIEW test_obj_audit_db.test_view FOR SELECT TO ALL`)
	require.NoError(t, err)
}

func TestCreateAuditOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_op_audit_db`)
	require.NoError(t, err)

	// create a test table
	_, err = db.Exec(`CREATE TABLE test_op_audit_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// test audit to specific users
	_, err = db.Exec(`CREATE AUDIT audit_to_user1 ON TABLE test_op_audit_db.test_table FOR ALL TO user1`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE AUDIT audit_to_users ON TABLE test_op_audit_db.test_table FOR ALL TO user1, user2`)
	require.NoError(t, err)
}

func TestCreateDatabaseNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createDatabaseNode{
		n: &tree.CreateDatabase{
			Name: "test_db",
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestCreateDatabaseIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a regular database
	_, err := db.Exec(`CREATE DATABASE test_db`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE SCHEMA sc1`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE SCHEMA sc1`)
	require.Error(t, err)

	_, err = db.Exec(`CREATE TABLE test_db.t1 (id INT primary key)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE t2 AS SELECT * FROM test_db.t1`)
	require.NoError(t, err)

	// create a TS database
	_, err = db.Exec(`CREATE TS DATABASE test_ts_db`)
	require.NoError(t, err)

	// create a TS database with retention
	_, err = db.Exec(`CREATE TS DATABASE test_ts_db_retention retentions 30d`)
	require.NoError(t, err)

	// create duplicate database with IF NOT EXISTS
	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_db`)
	require.NoError(t, err)

	// create duplicate database without IF NOT EXISTS
	_, err = db.Exec(`CREATE DATABASE test_db`)
	require.Error(t, err)
}

func TestCreateDatabaseErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// test create database with empty name
	_, err := db.Exec(`CREATE DATABASE ''`)
	require.Error(t, err)

	// test create TS database with invalid name (contains special characters)
	_, err = db.Exec(`CREATE TS DATABASE 'test-db-123'`)
	require.Error(t, err)

	// test create TS database with name too long
	longName := "a"
	for i := 0; i < 70; i++ {
		longName += "a"
	}
	_, err = db.Exec(`CREATE TS DATABASE ` + longName)
	require.Error(t, err)

	// test create database with unsupported template
	_, err = db.Exec(`CREATE DATABASE test_template_db TEMPLATE template1`)
	require.Error(t, err)

	// test create database with unsupported encoding
	_, err = db.Exec(`CREATE DATABASE test_encoding_db ENCODING 'LATIN1'`)
	require.Error(t, err)

	// test create database with unsupported collation
	_, err = db.Exec(`CREATE DATABASE test_collate_db COLLATE 'en_US.UTF-8'`)
	require.Error(t, err)

	// test create database with unsupported ctype
	_, err = db.Exec(`CREATE DATABASE test_ctype_db LC_CTYPE 'en_US.UTF-8'`)
	require.Error(t, err)

	// test create TS database with invalid partition interval unit (seconds)
	_, err = db.Exec(`CREATE TS DATABASE test_invalid_partition WITH (partition_interval = '30s')`)
	require.Error(t, err)

	// test create TS database with invalid partition interval unit (minutes)
	_, err = db.Exec(`CREATE TS DATABASE test_invalid_partition2 WITH (partition_interval = '30m')`)
	require.Error(t, err)

	// test create TS database with invalid partition interval unit (hours)
	_, err = db.Exec(`CREATE TS DATABASE test_invalid_partition3 WITH (partition_interval = '2h')`)
	require.Error(t, err)

	// test create TS database with partition interval out of range (0)
	_, err = db.Exec(`CREATE TS DATABASE test_invalid_partition4 WITH (partition_interval = '0d')`)
	require.Error(t, err)

	// test create TS database with partition interval out of range (too large)
	_, err = db.Exec(`CREATE TS DATABASE test_invalid_partition5 WITH (partition_interval = '2000y')`)
	require.Error(t, err)
}

func TestCreateDatabaseOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// test create database with supported encoding
	_, err := db.Exec(`CREATE DATABASE test_encoding_db ENCODING 'UTF8'`)
	require.NoError(t, err)

	// test create database with supported collation
	_, err = db.Exec(`CREATE DATABASE test_collate_db LC_COLLATE 'C'`)
	require.NoError(t, err)

	// test create database with supported ctype
	_, err = db.Exec(`CREATE DATABASE test_ctype_db LC_CTYPE 'C'`)
	require.NoError(t, err)

	// test create database with template0
	_, err = db.Exec(`CREATE DATABASE test_template_db TEMPLATE template0`)
	require.NoError(t, err)

	// test create TS database with different partition interval units
	_, err = db.Exec(`CREATE TS DATABASE test_partition_day partition interval 1d`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TS DATABASE test_partition_week partition interval 1w`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TS DATABASE test_partition_month partition interval 1mon`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TS DATABASE test_partition_year partition interval 1y`)
	require.NoError(t, err)

	// test create TS database with comment
	_, err = db.Exec(`CREATE TS DATABASE test_comment_db COMMENT 'test comment'`)
	require.NoError(t, err)
}

func TestCreateFunctionNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createFunctionNode{
		n: &tree.CreateFunction{
			FunctionName: "test_func",
			FuncBody:     "return 1",
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestCreateFunctionIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_func_db`)
	require.NoError(t, err)
	_, err = db.Exec(`SET database = test_func_db`)
	require.NoError(t, err)

	// create a simple function
	_, err = db.Exec(`CREATE FUNCTION test_simple() RETURNS INT LANGUAGE LUA BEGIN 'function test_simple() return 1 end' END`)
	require.NoError(t, err)

	// create function with arguments
	_, err = db.Exec(`CREATE FUNCTION test_add(a INT, b INT) RETURNS INT LANGUAGE LUA BEGIN 'function test_add(a, b) return a + b end' END`)
	require.NoError(t, err)

	// create function with timestamp parameter
	_, err = db.Exec(`CREATE FUNCTION test_timestamp(ts TIMESTAMP) RETURNS TIMESTAMP LANGUAGE LUA BEGIN 'function test_timestamp(ts) return ts end' END`)
	require.NoError(t, err)

	// create function with float parameter
	_, err = db.Exec(`CREATE FUNCTION test_float(f FLOAT) RETURNS FLOAT LANGUAGE LUA BEGIN 'function test_float(f) return f end' END`)
	require.NoError(t, err)

	// create function with varchar parameter
	_, err = db.Exec(`CREATE FUNCTION test_varchar(name VARCHAR) RETURNS VARCHAR LANGUAGE LUA BEGIN 'function test_varchar(name) return name end' END`)
	require.NoError(t, err)

	// create duplicate function
	_, err = db.Exec(`CREATE FUNCTION test_simple() RETURNS INT LANGUAGE LUA BEGIN 'function test_simple() return 1 end' END`)
	require.Error(t, err)
}

func TestCreateFunctionErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_func_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`SET database = test_func_err_db`)
	require.NoError(t, err)

	// test create function with invalid Lua syntax
	_, err = db.Exec(`CREATE FUNCTION test_invalid_lua() RETURNS INT LANGUAGE LUA BEGIN 'function test_invalid_lua() invalid lua syntax {{}} end' END`)
	require.Error(t, err)

	// test create function with unsupported argument type
	_, err = db.Exec(`CREATE FUNCTION test_unsupported_arg(bad_type BLOB) RETURNS INT LANGUAGE LUA BEGIN 'function test_unsupported_arg() return 1 end' END`)
	require.Error(t, err)

	// test create function with unsupported return type
	_, err = db.Exec(`CREATE FUNCTION test_unsupported_return() RETURNS BLOB LANGUAGE LUA BEGIN 'function test_unsupported_return() return 1 end' END`)
	require.Error(t, err)

	// test create function with empty function body
	_, err = db.Exec(`CREATE FUNCTION test_empty_body() RETURNS INT LANGUAGE LUA BEGIN 'function test_empty_body()  end' END`)
	require.NoError(t, err)

	// test create function with existing builtin function name
	_, err = db.Exec(`CREATE FUNCTION now() RETURNS TIMESTAMP LANGUAGE LUA BEGIN 'function now() return 1 end' END`)
	require.Error(t, err)

	// test create function with existing user defined function name
	_, err = db.Exec(`CREATE FUNCTION test_func_exists() RETURNS INT LANGUAGE LUA BEGIN 'function test_func_exists() return 1 end' END`)
	require.NoError(t, err)

	// test create duplicate function
	_, err = db.Exec(`CREATE FUNCTION test_func_exists() RETURNS INT LANGUAGE LUA BEGIN 'function test_func_exists() return 2 end' END`)
	require.Error(t, err)
}

func TestCreateFunctionTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_func_types_db`)
	require.NoError(t, err)
	_, err = db.Exec(`SET database = test_func_types_db`)
	require.NoError(t, err)

	// test with INT arguments
	_, err = db.Exec(`CREATE FUNCTION func_int(a INT) RETURNS INT LANGUAGE LUA BEGIN 'function func_int(a) return a end' END`)
	require.NoError(t, err)

	// test with SMALLINT (INT2) arguments
	_, err = db.Exec(`CREATE FUNCTION func_int2(a INT2) RETURNS INT2 LANGUAGE LUA BEGIN 'function func_int2(a) return a end' END`)
	require.NoError(t, err)

	// test with INTEGER (INT4) arguments
	_, err = db.Exec(`CREATE FUNCTION func_int4(a INT4) RETURNS INT4 LANGUAGE LUA BEGIN 'function func_int4(a) return a end' END`)
	require.NoError(t, err)

	// test with BIGINT arguments
	_, err = db.Exec(`CREATE FUNCTION func_bigint(a BIGINT) RETURNS BIGINT LANGUAGE LUA BEGIN 'function func_bigint(a) return a end' END`)
	require.NoError(t, err)

	// test with FLOAT4 arguments
	_, err = db.Exec(`CREATE FUNCTION func_float4(a FLOAT4) RETURNS FLOAT4 LANGUAGE LUA BEGIN 'function func_float4(a) return a end' END`)
	require.NoError(t, err)

	// test with FLOAT (DOUBLE) arguments
	_, err = db.Exec(`CREATE FUNCTION func_float(a FLOAT) RETURNS FLOAT LANGUAGE LUA BEGIN 'function func_float(a) return a end' END`)
	require.NoError(t, err)

	// test with CHAR arguments
	_, err = db.Exec(`CREATE FUNCTION func_char(a CHAR) RETURNS CHAR LANGUAGE LUA BEGIN 'function func_char(a) return a end' END`)
	require.NoError(t, err)

	// test with NCHAR arguments
	_, err = db.Exec(`CREATE FUNCTION func_nchar(a NCHAR) RETURNS NCHAR LANGUAGE LUA BEGIN 'function func_nchar(a) return a end' END`)
	require.NoError(t, err)

	// test with VARCHAR arguments
	_, err = db.Exec(`CREATE FUNCTION func_varchar(a VARCHAR) RETURNS VARCHAR LANGUAGE LUA BEGIN 'function func_varchar(a) return a end' END`)
	require.NoError(t, err)

	// test with NVARCHAR arguments
	_, err = db.Exec(`CREATE FUNCTION func_nvarchar(a NVARCHAR) RETURNS NVARCHAR LANGUAGE LUA BEGIN 'function func_nvarchar(a) return a end' END`)
	require.NoError(t, err)

	// test with TIMESTAMP arguments
	_, err = db.Exec(`CREATE FUNCTION func_timestamp(a TIMESTAMP) RETURNS TIMESTAMP LANGUAGE LUA BEGIN 'function func_timestamp(a) return a end' END`)
	require.NoError(t, err)
}

func TestCreateFunctionComplexLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_func_logic_db`)
	require.NoError(t, err)
	_, err = db.Exec(`SET database = test_func_logic_db`)
	require.NoError(t, err)

	// test function with conditional logic
	_, err = db.Exec(`CREATE FUNCTION test_condition(a INT) RETURNS INT LANGUAGE LUA BEGIN 'function test_condition(a) if a > 0 then return a else return 0 end end' END`)
	require.NoError(t, err)

	// test function with loop
	_, err = db.Exec(`CREATE FUNCTION test_loop(n INT) RETURNS INT LANGUAGE LUA BEGIN 'function test_loop(n) local sum = 0 for i = 1, n do sum = sum + i end return sum end' END`)
	require.NoError(t, err)

	// test function with multiple arguments
	_, err = db.Exec(`CREATE FUNCTION test_multi_args(a INT, b INT, c INT) RETURNS INT LANGUAGE LUA BEGIN 'function test_multi_args(a, b, c) return a + b + c end' END`)
	require.NoError(t, err)

	// test function with string manipulation
	_, err = db.Exec(`CREATE FUNCTION test_string(name VARCHAR) RETURNS VARCHAR LANGUAGE LUA BEGIN 'function test_string(name) return "Hello, " .. name end' END`)
	require.NoError(t, err)
}

func TestCreateIndexNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createIndexNode{
		n: &tree.CreateIndex{
			Name:    "test_idx",
			Table:   tree.TableName{},
			Columns: tree.IndexElemList{},
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())

	// Test ReadingOwnWrites method
	node.ReadingOwnWrites()
}

func TestCreateIndexIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_idx_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_idx_db.test_table (id INT PRIMARY KEY, name STRING, age INT)`)
	require.NoError(t, err)

	// create a simple index
	_, err = db.Exec(`CREATE INDEX idx_name ON test_idx_db.test_table (name)`)
	require.NoError(t, err)

	// create an index with IF NOT EXISTS
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_name ON test_idx_db.test_table (name)`)
	require.NoError(t, err)

	// create duplicate index without IF NOT EXISTS
	_, err = db.Exec(`CREATE INDEX idx_name ON test_idx_db.test_table (name)`)
	require.Error(t, err)

	// create composite index
	_, err = db.Exec(`CREATE INDEX idx_composite ON test_idx_db.test_table (name, age)`)
	require.NoError(t, err)

	// create index with ascending order
	_, err = db.Exec(`CREATE INDEX idx_asc ON test_idx_db.test_table (name ASC)`)
	require.NoError(t, err)

	// create index with descending order
	_, err = db.Exec(`CREATE INDEX idx_desc ON test_idx_db.test_table (name DESC)`)
	require.NoError(t, err)
}

func TestCreateIndexErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_idx_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_idx_err_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create index on non-existent column
	_, err = db.Exec(`CREATE INDEX idx_invalid ON test_idx_err_db.test_table (non_existent_col)`)
	require.Error(t, err)

	// create index on non-existent table
	_, err = db.Exec(`CREATE INDEX idx_invalid ON test_idx_err_db.non_existent_table (id)`)
	require.Error(t, err)

	// create index on non-existent database
	_, err = db.Exec(`CREATE INDEX idx_invalid ON non_existent_db.test_table (id)`)
	require.Error(t, err)

	// create index on view (not materialized)
	_, err = db.Exec(`CREATE VIEW test_idx_err_db.test_view AS SELECT * FROM test_idx_err_db.test_table`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE INDEX idx_on_view ON test_idx_err_db.test_view (id)`)
	require.Error(t, err)
}

func TestCreateUniqueIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_unique_idx_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_unique_idx_db.test_table (id INT PRIMARY KEY, email STRING)`)
	require.NoError(t, err)

	// create a unique index
	_, err = db.Exec(`CREATE UNIQUE INDEX idx_unique ON test_unique_idx_db.test_table (email)`)
	require.NoError(t, err)

	// create unique index with IF NOT EXISTS
	_, err = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique ON test_unique_idx_db.test_table (email)`)
	require.NoError(t, err)

	// insert some data
	_, err = db.Exec(`INSERT INTO test_unique_idx_db.test_table (id, email) VALUES (1, 'test@example.com')`)
	require.NoError(t, err)

	// insert duplicate email (should fail due to unique index)
	_, err = db.Exec(`INSERT INTO test_unique_idx_db.test_table (id, email) VALUES (2, 'test@example.com')`)
	require.Error(t, err)
}

func TestCreateTAGIndexIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_tga_idx_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_tga_idx_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create tag index on TS table
	_, err = db.Exec(`CREATE INDEX tga_idx1 ON test_tga_idx_db.test_ts_table (tag1)`)
	require.Error(t, err)

	// create multiple tag indexes on TS table
	_, err = db.Exec(`CREATE TABLE test_tga_idx_db.test_ts_table2 (ts timestamp not null, a int) tags(tag1 int not null, tag2 int not null, tag3 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX tga_idx2 ON test_tga_idx_db.test_ts_table2 (tag2)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX tga_idx3 ON test_tga_idx_db.test_ts_table2 (tag3)`)
	require.NoError(t, err)
}

func TestCreateTAGIndexErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_tga_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_tga_err_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create unique index on TS table (unsupported)
	_, err = db.Exec(`CREATE UNIQUE INDEX idx_unique ON test_tga_err_db.test_ts_table (tag1)`)
	require.Error(t, err)

	// create inverted index on TS table (unsupported)
	_, err = db.Exec(`CREATE INVERTED INDEX idx_inverted ON test_tga_err_db.test_ts_table (tag1)`)
	require.Error(t, err)

	// create index with storing columns on TS table (unsupported)
	_, err = db.Exec(`CREATE INDEX idx_storing ON test_tga_err_db.test_ts_table (tag1) STORING (a)`)
	require.Error(t, err)

	// create index on timestamp column in TS table (unsupported - must be tag column)
	_, err = db.Exec(`CREATE INDEX idx_ts ON test_tga_err_db.test_ts_table (ts)`)
	require.Error(t, err)

	// create hash sharded index on TS table (unsupported)
	_, err = db.Exec(`CREATE INDEX idx_sharded ON test_tga_err_db.test_ts_table (tag1) USING HASH WITH (shard_buckets = 4)`)
	require.Error(t, err)
}

func TestCreateScheduleNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createScheduleNode{
		n: &tree.CreateSchedule{
			ScheduleName: tree.NewDString("test_schedule"),
			Recurrence:   tree.NewDString("@daily"),
			SQL:          "INSERT INTO test_table VALUES (1)",
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestCreateScheduleIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create a schedule with name
	_, err = db.Exec(`CREATE SCHEDULE test_schedule1 FOR SQL 'INSERT INTO test_schedule_db.test_table(id, name) VALUES(1, ''test1'')' RECURRING '@daily'`)
	require.NoError(t, err)

	// create a schedule without name (auto-generated name)
	_, err = db.Exec(`CREATE SCHEDULE FOR SQL 'INSERT INTO test_schedule_db.test_table(id, name) VALUES(2, ''test2'')' RECURRING '@hourly'`)
	require.NoError(t, err)

	// create schedule with IF NOT EXISTS (duplicate should succeed)
	_, err = db.Exec(`CREATE SCHEDULE IF NOT EXISTS test_schedule1 FOR SQL 'INSERT INTO test_schedule_db.test_table(id, name) VALUES(3, ''test3'')' RECURRING '@daily'`)
	require.NoError(t, err)

	// create duplicate schedule without IF NOT EXISTS
	_, err = db.Exec(`CREATE SCHEDULE test_schedule1 FOR SQL 'INSERT INTO test_schedule_db.test_table(id, name) VALUES(4, ''test4'')' RECURRING '@daily'`)
	require.Error(t, err)
}

func TestCreateScheduleErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_err_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create schedule without RECURRING clause
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_no_recurring FOR SQL 'INSERT INTO test_schedule_err_db.test_table VALUES(1, ''test'')'`)
	require.Error(t, err)

	// create schedule with invalid recurrence expression
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_invalid_cron FOR SQL 'INSERT INTO test_schedule_err_db.test_table VALUES(1, ''test'')' RECURRING 'invalid_cron'`)
	require.Error(t, err)

	// create schedule with unsupported SQL statement (CREATE TABLE is not supported)
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_unsupported FOR SQL 'CREATE TABLE test_schedule_err_db.new_table (id INT)' RECURRING '@daily'`)
	require.Error(t, err)

	// create schedule with unsupported SQL statement (SELECT is not supported)
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_select FOR SQL 'SELECT * FROM test_schedule_err_db.test_table' RECURRING '@daily'`)
	require.Error(t, err)

	// create schedule with placeholder (not supported)
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_placeholder FOR SQL 'INSERT INTO test_schedule_err_db.test_table VALUES($1, ''test'')' RECURRING '@daily'`)
	require.Error(t, err)

	// create schedule with invalid SQL syntax
	_, err = db.Exec(`CREATE SCHEDULE test_schedule_invalid_sql FOR SQL 'INSERT INVALID SYNTAX' RECURRING '@daily'`)
	require.Error(t, err)
}

func TestCreateScheduleOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_opts_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_opts_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create schedule with on_execution_failure = retry
	_, err = db.Exec(`CREATE SCHEDULE schedule_retry FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(1, ''retry'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_execution_failure = 'retry')`)
	require.NoError(t, err)

	// create schedule with on_execution_failure = reschedule
	_, err = db.Exec(`CREATE SCHEDULE schedule_reschedule FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(2, ''reschedule'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_execution_failure = 'reschedule')`)
	require.NoError(t, err)

	// create schedule with on_execution_failure = pause
	_, err = db.Exec(`CREATE SCHEDULE schedule_pause FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(3, ''pause'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_execution_failure = 'pause')`)
	require.NoError(t, err)

	// create schedule with on_previous_running = start
	_, err = db.Exec(`CREATE SCHEDULE schedule_start FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(4, ''start'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_previous_running = 'start')`)
	require.NoError(t, err)

	// create schedule with on_previous_running = skip
	_, err = db.Exec(`CREATE SCHEDULE schedule_skip FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(5, ''skip'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_previous_running = 'skip')`)
	require.NoError(t, err)

	// create schedule with on_previous_running = wait
	_, err = db.Exec(`CREATE SCHEDULE schedule_wait FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(6, ''wait'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_previous_running = 'wait')`)
	require.NoError(t, err)

	// create schedule with multiple options
	_, err = db.Exec(`CREATE SCHEDULE schedule_multi_opts FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(7, ''multi'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_execution_failure = 'retry', on_previous_running = 'start')`)
	require.NoError(t, err)

	// create schedule with invalid on_execution_failure value
	_, err = db.Exec(`CREATE SCHEDULE schedule_invalid_failure FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(8, ''invalid'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_execution_failure = 'invalid_value')`)
	require.Error(t, err)

	// create schedule with invalid on_previous_running value
	_, err = db.Exec(`CREATE SCHEDULE schedule_invalid_running FOR SQL 'INSERT INTO test_schedule_opts_db.test_table(id, name) VALUES(9, ''invalid'')' RECURRING '@daily' WITH SCHEDULE OPTIONS (on_previous_running = 'invalid_value')`)
	require.Error(t, err)
}

func TestCreateScheduleRecurrence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_recurring_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_recurring_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create schedule with @yearly
	_, err = db.Exec(`CREATE SCHEDULE schedule_yearly FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(1, ''yearly'')' RECURRING '@yearly'`)
	require.NoError(t, err)

	// create schedule with @monthly
	_, err = db.Exec(`CREATE SCHEDULE schedule_monthly FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(2, ''monthly'')' RECURRING '@monthly'`)
	require.NoError(t, err)

	// create schedule with @weekly
	_, err = db.Exec(`CREATE SCHEDULE schedule_weekly FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(3, ''weekly'')' RECURRING '@weekly'`)
	require.NoError(t, err)

	// create schedule with @daily
	_, err = db.Exec(`CREATE SCHEDULE schedule_daily FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(4, ''daily'')' RECURRING '@daily'`)
	require.NoError(t, err)

	// create schedule with @hourly
	_, err = db.Exec(`CREATE SCHEDULE schedule_hourly FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(5, ''hourly'')' RECURRING '@hourly'`)
	require.NoError(t, err)

	// create schedule with @minutely
	_, err = db.Exec(`CREATE SCHEDULE schedule_minutely FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(6, ''minutely'')' RECURRING '@minutely'`)
	require.Error(t, err)

	// create schedule with @secondly
	_, err = db.Exec(`CREATE SCHEDULE schedule_secondly FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(7, ''secondly'')' RECURRING '@secondly'`)
	require.Error(t, err)

	// create schedule with cron expression (0 0 * * *)
	_, err = db.Exec(`CREATE SCHEDULE schedule_cron FOR SQL 'INSERT INTO test_schedule_recurring_db.test_table(id, name) VALUES(8, ''cron'')' RECURRING '0 0 * * *'`)
	require.NoError(t, err)
}

func TestCreateScheduleSupportedSQLTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and tables
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_sql_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_sql_db.src_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_sql_db.dst_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create schedule with INSERT statement
	_, err = db.Exec(`CREATE SCHEDULE schedule_insert FOR SQL 'INSERT INTO test_schedule_sql_db.dst_table(id, name) VALUES(1, ''test'')' RECURRING '@daily'`)
	require.NoError(t, err)

	// create schedule with UPDATE statement
	_, err = db.Exec(`CREATE SCHEDULE schedule_update FOR SQL 'UPDATE test_schedule_sql_db.dst_table SET name = ''updated'' WHERE id = 1' RECURRING '@daily'`)
	require.NoError(t, err)

	// create schedule with DELETE statement
	_, err = db.Exec(`CREATE SCHEDULE schedule_delete FOR SQL 'DELETE FROM test_schedule_sql_db.dst_table WHERE id = 1' RECURRING '@daily'`)
	require.NoError(t, err)

	// create schedule with INSERT...SELECT statement
	_, err = db.Exec(`CREATE SCHEDULE schedule_insert_select FOR SQL 'INSERT INTO test_schedule_sql_db.dst_table SELECT * FROM test_schedule_sql_db.src_table' RECURRING '@daily'`)
	require.NoError(t, err)
}

func TestCreateStatsNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createStatsNode{
		CreateStats: tree.CreateStats{
			Name:  "test_stats",
			Table: &tree.UnresolvedObjectName{},
		},
	}

	// Test Values method
	values := node.Values()
	require.Nil(t, values)

	// Test Close method
	node.Close(context.Background())
}

func TestCreateStatsIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_stats_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stats_db.test_table (id INT PRIMARY KEY, name STRING, age INT)`)
	require.NoError(t, err)

	// insert some data
	_, err = db.Exec(`INSERT INTO test_stats_db.test_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)`)
	require.NoError(t, err)

	// create statistics on a table
	_, err = db.Exec(`CREATE STATISTICS test_stats1 ON id FROM test_stats_db.test_table`)
	require.NoError(t, err)

	// create statistics on multiple columns
	_, err = db.Exec(`CREATE STATISTICS test_stats2 ON id, name, age FROM test_stats_db.test_table`)
	require.NoError(t, err)

	// create statistics without column names (uses default columns)
	_, err = db.Exec(`CREATE STATISTICS test_stats3 FROM test_stats_db.test_table`)
	require.NoError(t, err)
}

func TestCreateStatsErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_stats_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stats_err_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create statistics on non-existent column
	_, err = db.Exec(`CREATE STATISTICS test_stats_invalid ON non_existent_col FROM test_stats_err_db.test_table`)
	require.Error(t, err)

	// create statistics on non-existent table
	_, err = db.Exec(`CREATE STATISTICS test_stats_invalid ON id FROM test_stats_err_db.non_existent_table`)
	require.Error(t, err)

	// create statistics on non-existent database
	_, err = db.Exec(`CREATE STATISTICS test_stats_invalid ON id FROM non_existent_db.test_table`)
	require.Error(t, err)

	// create statistics on a view (not supported)
	_, err = db.Exec(`CREATE VIEW test_stats_err_db.test_view AS SELECT * FROM test_stats_err_db.test_table`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STATISTICS test_stats_view ON id FROM test_stats_err_db.test_view`)
	require.Error(t, err)

	// create statistics with JSON column (not supported)
	_, err = db.Exec(`CREATE TABLE test_stats_err_db.json_table (id INT PRIMARY KEY, data JSON)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STATISTICS test_stats_json ON data FROM test_stats_err_db.json_table`)
	require.Error(t, err)
}

func TestCreateStatsTSTableIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_stats_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_stats_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null, tag2 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create statistics on tag columns
	_, err = db.Exec(`CREATE STATISTICS ts_stats_tag ON tag1 FROM test_ts_stats_db.test_ts_table`)
	require.NoError(t, err)

	// create statistics on multiple tag columns
	_, err = db.Exec(`CREATE STATISTICS ts_stats_tags ON tag1, tag2 FROM test_ts_stats_db.test_ts_table`)
	require.Error(t, err)

	// create statistics without column names (uses default columns)
	_, err = db.Exec(`CREATE STATISTICS ts_stats_default FROM test_ts_stats_db.test_ts_table`)
	require.NoError(t, err)
}

func TestCreateStatsTSTableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_stats_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_stats_err_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create statistics with AS OF SYSTEM TIME on TS table (not supported)
	_, err = db.Exec(`CREATE STATISTICS ts_stats_asof ON tag1 FROM test_ts_stats_err_db.test_ts_table AS OF SYSTEM TIME '10s'`)
	require.Error(t, err)

	// create multi-column statistics with non-PTAG columns (not supported)
	_, err = db.Exec(`CREATE STATISTICS ts_stats_multi ON tag1, a FROM test_ts_stats_err_db.test_ts_table`)
	require.Error(t, err)

	// create statistics on non-existent tag column
	_, err = db.Exec(`CREATE STATISTICS ts_stats_invalid ON non_existent_tag FROM test_ts_stats_err_db.test_ts_table`)
	require.Error(t, err)

	// create statistics on timestamp column (should use tag columns instead)
	_, err = db.Exec(`CREATE STATISTICS ts_stats_ts ON ts FROM test_ts_stats_err_db.test_ts_table`)
	require.NoError(t, err)

	// create statistics with sorted histogram
	_, err = db.Exec(`CREATE STATISTICS test_sorted_hist ON tag1 FROM test_ts_stats_err_db.test_ts_table WITH OPTIONS COLLECT_SORTED_HISTOGRAM`)
	require.Error(t, err)
}

func TestCreateStatsOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_stats_opts_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stats_opts_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// insert some data
	_, err = db.Exec(`INSERT INTO test_stats_opts_db.test_table VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)
	// create statistics with sorted histogram
	_, err = db.Exec(`CREATE STATISTICS test_sorted_hist ON id FROM test_stats_opts_db.test_table WITH OPTIONS COLLECT_SORTED_HISTOGRAM`)
	require.NoError(t, err)
	// create statistics with THROTTLING option
	_, err = db.Exec(`CREATE STATISTICS test_stats_throttle ON id FROM test_stats_opts_db.test_table WITH OPTIONS THROTTLING 0.5`)
	require.NoError(t, err)
}

func TestCreateStreamNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &createStreamNode{
		n: &tree.CreateStream{
			StreamName: "test_stream",
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestCreateStreamIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create a stream with enable option off
	_, err = db.Exec(`CREATE STREAM test_stream1 INTO test_stream_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_db.source_table`)
	require.NoError(t, err)

	// create a stream with IF NOT EXISTS (duplicate should succeed)
	_, err = db.Exec(`CREATE STREAM IF NOT EXISTS test_stream1 INTO test_stream_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_db.source_table`)
	require.NoError(t, err)

	// create duplicate stream without IF NOT EXISTS
	_, err = db.Exec(`CREATE STREAM test_stream1 INTO test_stream_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_db.source_table`)
	require.Error(t, err)
}

func TestCreateStreamErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_err_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_err_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create stream with non-existent source table
	_, err = db.Exec(`CREATE STREAM test_stream_err1 INTO test_stream_err_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_err_db.non_existent_table`)
	require.Error(t, err)

	// create stream with non-existent target table
	_, err = db.Exec(`CREATE STREAM test_stream_err2 INTO test_stream_err_db.non_existent_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_err_db.source_table`)
	require.Error(t, err)

	// create stream with invalid stream name
	_, err = db.Exec(`CREATE STREAM '' INTO test_stream_err_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_err_db.source_table`)
	require.Error(t, err)
}

func TestCreateStreamOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_opts_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_opts_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_opts_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create stream with enable option on
	_, err = db.Exec(`CREATE STREAM test_stream_enable INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'on') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with max_delay option
	_, err = db.Exec(`CREATE STREAM test_stream_delay INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', max_delay = '24h') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with sync_time option
	_, err = db.Exec(`CREATE STREAM test_stream_sync INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', sync_time = '1h') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with process_history option
	_, err = db.Exec(`CREATE STREAM test_stream_history INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', process_history = 'on') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with ignore_expired option
	_, err = db.Exec(`CREATE STREAM test_stream_expired INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', ignore_expired = 'off') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with ignore_update option
	_, err = db.Exec(`CREATE STREAM test_stream_update INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', ignore_update = 'on') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with max_retries option
	_, err = db.Exec(`CREATE STREAM test_stream_retries INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', max_retries = '5') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with checkpoint_interval option
	_, err = db.Exec(`CREATE STREAM test_stream_checkpoint INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', checkpoint_interval = '10s') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with heartbeat_interval option
	_, err = db.Exec(`CREATE STREAM test_stream_heartbeat INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', heartbeat_interval = '2s') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with recalculate_delay_rounds option
	_, err = db.Exec(`CREATE STREAM test_stream_rounds INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', recalculate_delay_rounds = '10') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with buffer_size option
	_, err = db.Exec(`CREATE STREAM test_stream_buffer INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', buffer_size = '2GiB') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)

	// create stream with multiple options
	_, err = db.Exec(`CREATE STREAM test_stream_multi INTO test_stream_opts_db.target_table WITH OPTIONS (enable = 'off', max_delay = '48h', sync_time = '1h', process_history = 'on') AS SELECT * FROM test_stream_opts_db.source_table`)
	require.NoError(t, err)
}

func TestCreateStreamWithQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_query_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_query_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_query_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create stream with WHERE clause
	_, err = db.Exec(`CREATE STREAM test_stream_where INTO test_stream_query_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_query_db.source_table WHERE tag1 > 10`)
	require.NoError(t, err)

	// create stream with aggregation (group by)
	_, err = db.Exec(`CREATE STREAM test_stream_agg INTO test_stream_query_db.target_table WITH OPTIONS (enable = 'off') AS SELECT count(*) FROM test_stream_query_db.source_table GROUP BY tag1`)
	require.Error(t, err)
}
