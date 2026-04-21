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

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAlterAuditIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_db.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// create audit
	_, err = db.Exec(`CREATE AUDIT my_audit ON TABLE test_db.test_table FOR ALL TO ALL`)
	require.NoError(t, err)

	// enable audit
	_, err = db.Exec(`ALTER AUDIT my_audit ENABLE`)
	require.NoError(t, err)

	// disable audit
	_, err = db.Exec(`ALTER AUDIT my_audit DISABLE`)
	require.NoError(t, err)

	// rename audit
	_, err = db.Exec(`ALTER AUDIT my_audit RENAME TO renamed_audit`)
	require.NoError(t, err)

	// alter non-existent audit
	_, err = db.Exec(`ALTER AUDIT non_existent_audit ENABLE`)
	require.Error(t, err)

	// alter non-existent audit with IF EXISTS
	_, err = db.Exec(`ALTER AUDIT IF EXISTS non_existent_audit ENABLE`)
	require.NoError(t, err)
}

func TestAlterIndexIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_index_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_index_db.test_table (id INT PRIMARY KEY, name STRING, age INT)`)
	require.NoError(t, err)

	// create an index
	_, err = db.Exec(`CREATE INDEX idx_name ON test_index_db.test_table (name)`)
	require.NoError(t, err)

	// alter index partition (this should fail as it's unsupported)
	_, err = db.Exec(`ALTER INDEX test_index_db.test_table@idx_name PARTITION BY LIST (name) (PARTITION p1 VALUES IN ('a'))`)
	require.Error(t, err)
}

func TestAlterScheduleIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create a schedule for SQL
	_, err = db.Exec(`CREATE SCHEDULE test_sql_schedule FOR SQL 'INSERT INTO test_schedule_db.test_table(id, name) VALUES(1, ''test'')' RECURRING '0 0 * * *'`)
	require.NoError(t, err)

	// alter schedule recurrence
	_, err = db.Exec(`ALTER SCHEDULE test_sql_schedule RECURRING '0 1 * * *'`)
	require.NoError(t, err)

	// alter non-existent schedule
	_, err = db.Exec(`ALTER SCHEDULE non_existent_schedule RECURRING '0 2 * * *'`)
	require.Error(t, err)

	// alter schedule with IF EXISTS for non-existent schedule
	_, err = db.Exec(`ALTER SCHEDULE IF EXISTS non_existent_schedule RECURRING '0 3 * * *'`)
	require.NoError(t, err)
}

func TestAlterScheduleNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &alterScheduleNode{
		n: &tree.AlterSchedule{
			ScheduleName: "test_schedule",
			Recurrence:   tree.NewDString("0 0 * * *"),
			IfExists:     false,
		},
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
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestUpdateScheduleFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}
	schedule := &jobs.ScheduledJob{}
	require.NoError(t, updateSchedule(runParam, schedule))
}

func TestAlterScheduleOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_schedule_options_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_schedule_options_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// create a schedule with on_execution_failure option
	_, err = db.Exec(`CREATE SCHEDULE test_schedule1 FOR SQL 'INSERT INTO test_schedule_options_db.test_table(id, name) VALUES(1, ''test1'')' RECURRING '0 0 * * *' WITH  SCHEDULE OPTIONS (on_execution_failure = 'retry')`)
	require.NoError(t, err)

	// create a schedule with on_previous_running option
	_, err = db.Exec(`CREATE SCHEDULE test_schedule2 FOR SQL 'INSERT INTO test_schedule_options_db.test_table(id, name) VALUES(2, ''test2'')' RECURRING '0 0 * * *' WITH  SCHEDULE OPTIONS (on_previous_running = 'skip')`)
	require.NoError(t, err)

	// alter schedule with on_execution_failure option
	_, err = db.Exec(`ALTER SCHEDULE test_schedule1 RECURRING '0 1 * * *' WITH  SCHEDULE OPTIONS (on_execution_failure = 'pause')`)
	require.NoError(t, err)

	// alter schedule with on_previous_running option
	_, err = db.Exec(`ALTER SCHEDULE test_schedule2 RECURRING '0 1 * * *' WITH  SCHEDULE OPTIONS (on_previous_running = 'wait')`)
	require.NoError(t, err)

	// alter schedule with multiple options
	_, err = db.Exec(`ALTER SCHEDULE test_schedule1 RECURRING '0 2 * * *' WITH  SCHEDULE OPTIONS (on_execution_failure = 'reschedule', on_previous_running = 'start')`)
	require.NoError(t, err)

	// test invalid on_execution_failure option
	_, err = db.Exec(`ALTER SCHEDULE test_schedule1 RECURRING '0 3 * * *' WITH  SCHEDULE OPTIONS (on_execution_failure = 'invalid_option')`)
	require.Error(t, err)

	// test invalid on_previous_running option
	_, err = db.Exec(`ALTER SCHEDULE test_schedule2 RECURRING '0 3 * * *' WITH  SCHEDULE OPTIONS (on_previous_running = 'invalid_option')`)
	require.Error(t, err)
}

func TestAlterStreamNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &alterStreamNode{
		StreamMetadata: StreamMetadata{
			name:   "test_stream",
			status: "Enable",
		},
		n: &tree.AlterStream{
			StreamName: "test_stream",
			Options:    nil,
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestAlterStreamIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create a stream with correct syntax
	_, err = db.Exec(`CREATE STREAM test_stream INTO test_stream_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_db.source_table`)
	require.NoError(t, err)

	// alter stream enable option
	_, err = db.Exec(`ALTER STREAM test_stream SET OPTIONS (enable = 'on')`)
	require.NoError(t, err)

	// alter stream with multiple options
	_, err = db.Exec(`ALTER STREAM test_stream SET OPTIONS (enable = 'off', max_delay = '24h')`)
	require.NoError(t, err)

	// alter non-existent stream
	_, err = db.Exec(`ALTER STREAM non_existent_stream SET OPTIONS (enable = 'off')`)
	require.Error(t, err)

	// alter stream with invalid option
	_, err = db.Exec(`ALTER STREAM test_stream SET OPTIONS (invalid_option = 'value')`)
	require.Error(t, err)
}

func TestAlterStreamOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.source_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_stream_db.target_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// create a stream with all options
	_, err = db.Exec(`CREATE STREAM test_stream_all_opts INTO test_stream_db.target_table WITH OPTIONS (enable = 'off') AS SELECT * FROM test_stream_db.source_table`)
	require.NoError(t, err)

	// alter stream with max_delay option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (max_delay = '48h')`)
	require.NoError(t, err)

	// alter stream with sync_time option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (sync_time = '5m')`)
	require.NoError(t, err)

	// alter stream with process_history option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (process_history = 'on')`)
	require.NoError(t, err)

	// alter stream with ignore_expired option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (ignore_expired = 'off')`)
	require.NoError(t, err)

	// alter stream with ignore_update option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (ignore_update = 'off')`)
	require.NoError(t, err)

	// alter stream with max_retries option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (max_retries = '10')`)
	require.NoError(t, err)

	// alter stream with checkpoint_interval option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (checkpoint_interval = '30s')`)
	require.NoError(t, err)

	// alter stream with heartbeat_interval option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (heartbeat_interval = '5s')`)
	require.NoError(t, err)

	// alter stream with recalculate_delay_rounds option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (recalculate_delay_rounds = '20')`)
	require.NoError(t, err)

	// alter stream with buffer_size option
	_, err = db.Exec(`ALTER STREAM test_stream_all_opts SET OPTIONS (buffer_size = '4GiB')`)
	require.NoError(t, err)
}

func TestAlterTableNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &alterTableNode{
		n: &tree.AlterTable{
			Table: &tree.UnresolvedObjectName{},
			Cmds:  tree.AlterTableCmds{},
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestAlterTableIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and regular table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_alter_table_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_alter_table_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// add column
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.test_table ADD COLUMN age INT`)
	require.NoError(t, err)

	// alter column set not null
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.test_table alter COLUMN age set not null`)
	require.NoError(t, err)

	// alter column set null
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.test_table alter COLUMN age drop not null`)
	require.NoError(t, err)
	// rename column
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.test_table RENAME COLUMN name TO full_name`)
	require.NoError(t, err)

	// drop column
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.test_table DROP COLUMN age`)
	require.NoError(t, err)

	// alter non-existent table
	_, err = db.Exec(`ALTER TABLE test_alter_table_db.non_existent_table ADD COLUMN new_col INT`)
	require.Error(t, err)

	// alter non-existent table with IF EXISTS
	_, err = db.Exec(`ALTER TABLE IF EXISTS test_alter_table_db.non_existent_table ADD COLUMN new_col INT`)
	require.NoError(t, err)
}

func TestAlterTSTableIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_alter_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_alter_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// add tag
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table ADD TAG tag2 int`)
	require.NoError(t, err)

	// add column
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table ADD COLUMN b int`)
	require.NoError(t, err)

	// alter column type
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table alter COLUMN b type int64`)
	require.NoError(t, err)

	// drop column
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table DROP COLUMN b`)
	require.NoError(t, err)

	// rename tag
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table RENAME TAG tag2 TO tag3`)
	require.NoError(t, err)

	// alter tag type
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table alter TAG tag3 type int64`)
	require.NoError(t, err)

	// set retention
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table SET RETENTIONS = 30d`)
	require.NoError(t, err)

	// set activetime
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table SET ACTIVETIME = 7d`)
	require.NoError(t, err)

	// drop tag
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.test_ts_table DROP TAG tag3`)
	require.NoError(t, err)

	// alter non-existent TS table
	_, err = db.Exec(`ALTER TABLE test_ts_alter_db.non_existent_ts_table ADD TAG new_tag int`)
	require.Error(t, err)

	// alter non-existent TS table with IF EXISTS
	_, err = db.Exec(`ALTER TABLE IF EXISTS test_ts_alter_db.non_existent_ts_table ADD TAG new_tag int`)
	require.NoError(t, err)
}

func TestAlterTSDatabaseNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	node := &alterTSDatabaseNode{
		n: &tree.AlterTSDatabase{
			Database: "test_db",
		},
	}

	// Test Values method
	values := node.Values()
	require.NotNil(t, values)
	require.Equal(t, 0, len(values))

	// Test Close method
	node.Close(context.Background())
}

func TestAlterTSDatabaseIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_db`)
	require.NoError(t, err)

	// alter TS database with lifetime
	_, err = db.Exec(`ALTER TS DATABASE test_ts_db SET RETENTIONS = 30d`)
	require.NoError(t, err)

	// alter non-existent TS database
	_, err = db.Exec(`ALTER TS DATABASE non_existent_ts_db SET RETENTIONS = 30d`)
	require.Error(t, err)
}
