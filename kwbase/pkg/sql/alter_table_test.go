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
// WITHOUT WARRANTIES OF CONDITIONS OF ANY KIND, either express or implied.
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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAlterTableAddColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_add_col_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_add_col_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// add a new column
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN age INT`)
	require.NoError(t, err)

	// add column with IF NOT EXISTS
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN IF NOT EXISTS age INT`)
	require.NoError(t, err)

	// add column with default value
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN address STRING DEFAULT 'unknown'`)
	require.NoError(t, err)

	// add column with NOT NULL constraint
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN status INT NOT NULL DEFAULT 0`)
	require.NoError(t, err)

	// add column with PRIMARY KEY constraint
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN new_id INT PRIMARY KEY`)
	require.NoError(t, err)

	// add column with CHECK constraint
	_, err = db.Exec(`ALTER TABLE test_add_col_db.test_table ADD COLUMN score INT CHECK (score >= 0)`)
	require.NoError(t, err)
}

func TestAlterTableDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_drop_col_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_drop_col_db.test_table (id INT PRIMARY KEY, name STRING, age INT)`)
	require.NoError(t, err)

	// drop a column
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table DROP COLUMN age`)
	require.NoError(t, err)

	// drop column with IF EXISTS
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table DROP COLUMN IF EXISTS age`)
	require.NoError(t, err)

	// drop non-existent column
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table DROP COLUMN non_existent_col`)
	require.Error(t, err)

	// drop primary key column
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table DROP COLUMN id`)
	require.Error(t, err)

	// drop column with CASCADE
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table ADD COLUMN temp_col INT`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE test_drop_col_db.test_table DROP COLUMN temp_col CASCADE`)
	require.NoError(t, err)
}

func TestAlterTableRenameColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_rename_col_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_rename_col_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// rename a column
	_, err = db.Exec(`ALTER TABLE test_rename_col_db.test_table RENAME COLUMN name TO full_name`)
	require.NoError(t, err)

	// rename to existing column name
	_, err = db.Exec(`ALTER TABLE test_rename_col_db.test_table RENAME COLUMN full_name TO id`)
	require.Error(t, err)

	// rename non-existent column
	_, err = db.Exec(`ALTER TABLE test_rename_col_db.test_table RENAME COLUMN non_existent TO new_name`)
	require.Error(t, err)
}

func TestAlterTableAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_alter_type_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_alter_type_db.test_table (id INT PRIMARY KEY, age INT)`)
	require.NoError(t, err)

	// alter column type
	_, err = db.Exec(`ALTER TABLE test_alter_type_db.test_table ALTER COLUMN age SET DATA TYPE BIGINT`)
	require.NoError(t, err)

	// alter column type to string
	_, err = db.Exec(`ALTER TABLE test_alter_type_db.test_table ALTER COLUMN age SET DATA TYPE STRING`)
	require.Error(t, err)

	// alter non-existent column
	_, err = db.Exec(`ALTER TABLE test_alter_type_db.test_table ALTER COLUMN non_existent SET DATA TYPE STRING`)
	require.Error(t, err)
}

func TestAlterTableSetNotNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_set_notnull_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_set_notnull_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// add nullable column
	_, err = db.Exec(`ALTER TABLE test_set_notnull_db.test_table ADD COLUMN age INT`)
	require.NoError(t, err)

	// set NOT NULL
	_, err = db.Exec(`ALTER TABLE test_set_notnull_db.test_table ALTER COLUMN age SET NOT NULL`)
	require.NoError(t, err)

	// set NOT NULL on column with NULL values
	_, err = db.Exec(`ALTER TABLE test_set_notnull_db.test_table ADD COLUMN nullable_col INT`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE test_set_notnull_db.test_table ALTER COLUMN nullable_col SET NOT NULL`)
	require.NoError(t, err)
}

func TestAlterTableDropNotNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_drop_notnull_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_drop_notnull_db.test_table (id INT PRIMARY KEY, name STRING NOT NULL)`)
	require.NoError(t, err)

	// drop NOT NULL
	_, err = db.Exec(`ALTER TABLE test_drop_notnull_db.test_table ALTER COLUMN name DROP NOT NULL`)
	require.NoError(t, err)
}

func TestAlterTableSetDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_set_default_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_set_default_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// set default value
	_, err = db.Exec(`ALTER TABLE test_set_default_db.test_table ALTER COLUMN name SET DEFAULT 'default_name'`)
	require.NoError(t, err)

	// drop default value
	_, err = db.Exec(`ALTER TABLE test_set_default_db.test_table ALTER COLUMN name DROP DEFAULT`)
	require.NoError(t, err)

	// set default with expression
	_, err = db.Exec(`ALTER TABLE test_set_default_db.test_table ALTER COLUMN id SET DEFAULT 100`)
	require.NoError(t, err)
}

func TestAlterTableAddConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_add_const_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_add_const_db.test_table (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	// add CHECK constraint
	_, err = db.Exec(`ALTER TABLE test_add_const_db.test_table ADD CONSTRAINT check_id CHECK (id > 0)`)
	require.NoError(t, err)

	// add UNIQUE constraint
	_, err = db.Exec(`ALTER TABLE test_add_const_db.test_table ADD CONSTRAINT unique_name UNIQUE (name)`)
	require.NoError(t, err)
}

func TestAlterTableDropConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table with constraint
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_drop_const_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_drop_const_db.test_table (id INT PRIMARY KEY, name STRING, CONSTRAINT test_check CHECK (id > 0))`)
	require.NoError(t, err)

	// drop constraint
	_, err = db.Exec(`ALTER TABLE test_drop_const_db.test_table DROP CONSTRAINT test_check`)
	require.NoError(t, err)

	// drop non-existent constraint
	_, err = db.Exec(`ALTER TABLE test_drop_const_db.test_table DROP CONSTRAINT non_existent_constraint`)
	require.Error(t, err)

	// drop constraint with IF EXISTS
	_, err = db.Exec(`ALTER TABLE test_drop_const_db.test_table DROP CONSTRAINT IF EXISTS non_existent_constraint`)
	require.NoError(t, err)
}

func TestAlterTableRenameConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table with constraint
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_rename_const_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_rename_const_db.test_table (id INT PRIMARY KEY, CONSTRAINT old_name CHECK (id > 0))`)
	require.NoError(t, err)

	// rename constraint
	_, err = db.Exec(`ALTER TABLE test_rename_const_db.test_table RENAME CONSTRAINT old_name TO new_name`)
	require.NoError(t, err)
}

func TestAlterTableRenameTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_rename_tbl_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_rename_tbl_db.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// rename table
	_, err = db.Exec(`ALTER TABLE test_rename_tbl_db.test_table RENAME TO test_table_new`)
	require.NoError(t, err)

	// rename to existing table name
	_, err = db.Exec(`CREATE TABLE test_rename_tbl_db.test_table2 (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE test_rename_tbl_db.test_table_new RENAME TO test_table2`)
	require.Error(t, err)
}

func TestAlterTableSetPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_set_pk_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_set_pk_db.test_table (id INT NOT NULL, name STRING)`)
	require.NoError(t, err)

	// alter primary key
	_, err = db.Exec(`ALTER TABLE test_set_pk_db.test_table ALTER PRIMARY KEY USING COLUMNS (id)`)
	require.NoError(t, err)
}

func TestAlterTSTableAddTag(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_add_tag_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_add_tag_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// add a new tag
	_, err = db.Exec(`ALTER TABLE test_ts_add_tag_db.test_ts_table ADD TAG tag2 int`)
	require.NoError(t, err)

	// add multiple tags
	_, err = db.Exec(`ALTER TABLE test_ts_add_tag_db.test_ts_table ADD TAG tag3 string, ADD TAG tag4 int`)
	require.Error(t, err)
}

func TestAlterTSTableDropTag(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_drop_tag_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_drop_tag_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null, tag2 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// drop a tag
	_, err = db.Exec(`ALTER TABLE test_ts_drop_tag_db.test_ts_table DROP TAG tag2`)
	require.NoError(t, err)

	// drop non-existent tag
	_, err = db.Exec(`ALTER TABLE test_ts_drop_tag_db.test_ts_table DROP TAG non_existent_tag`)
	require.Error(t, err)

	// drop primary tag
	_, err = db.Exec(`ALTER TABLE test_ts_drop_tag_db.test_ts_table DROP TAG tag1`)
	require.Error(t, err)
}

func TestAlterTSTableRenameTag(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_rename_tag_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_rename_tag_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null, tag2 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// rename a tag
	_, err = db.Exec(`ALTER TABLE test_ts_rename_tag_db.test_ts_table RENAME TAG tag2 TO tag3`)
	require.NoError(t, err)

	// rename to existing tag name
	_, err = db.Exec(`ALTER TABLE test_ts_rename_tag_db.test_ts_table RENAME TAG tag3 TO tag1`)
	require.Error(t, err)

	// rename non-existent tag
	_, err = db.Exec(`ALTER TABLE test_ts_rename_tag_db.test_ts_table RENAME TAG non_existent TO new_name`)
	require.Error(t, err)
}

func TestAlterTSTableAlterTagType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_alter_tag_type_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_alter_tag_type_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// alter primary tag type
	_, err = db.Exec(`ALTER TABLE test_ts_alter_tag_type_db.test_ts_table ALTER TAG tag1 SET DATA TYPE BIGINT`)
	require.Error(t, err)
}

func TestAlterTSTableSetRetentions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_retentions_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_retentions_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// set retentions
	_, err = db.Exec(`ALTER TABLE test_ts_retentions_db.test_ts_table SET RETENTIONS = 30d`)
	require.NoError(t, err)

	// set retentions with different units
	_, err = db.Exec(`ALTER TABLE test_ts_retentions_db.test_ts_table SET RETENTIONS = 720h`)
	require.NoError(t, err)

	// set retentions with weeks
	_, err = db.Exec(`ALTER TABLE test_ts_retentions_db.test_ts_table SET RETENTIONS = 4w`)
	require.NoError(t, err)

	// set retentions with months
	_, err = db.Exec(`ALTER TABLE test_ts_retentions_db.test_ts_table SET RETENTIONS = 6m`)
	require.NoError(t, err)
}

func TestAlterTSTableSetActivetime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test TS database and table
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_ts_activetime_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_ts_activetime_db.test_ts_table (ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1)`)
	require.NoError(t, err)

	// set activetime
	_, err = db.Exec(`ALTER TABLE test_ts_activetime_db.test_ts_table SET ACTIVETIME = 7d`)
	require.NoError(t, err)

	// set activetime with hours
	_, err = db.Exec(`ALTER TABLE test_ts_activetime_db.test_ts_table SET ACTIVETIME = 168h`)
	require.NoError(t, err)
}

func TestAlterTableIfExists(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_if_exists_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_if_exists_db.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// alter non-existent table with IF EXISTS
	_, err = db.Exec(`ALTER TABLE IF EXISTS test_if_exists_db.non_existent_table ADD COLUMN new_col INT`)
	require.NoError(t, err)

	// alter non-existent table without IF EXISTS
	_, err = db.Exec(`ALTER TABLE test_if_exists_db.non_existent_table ADD COLUMN new_col INT`)
	require.Error(t, err)
}

func TestAlterTableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test database and table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_alter_err_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_alter_err_db.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// alter non-existent database
	_, err = db.Exec(`ALTER TABLE non_existent_db.test_table ADD COLUMN new_col INT`)
	require.Error(t, err)

	// alter non-existent table
	_, err = db.Exec(`ALTER TABLE test_alter_err_db.non_existent_table ADD COLUMN new_col INT`)
	require.Error(t, err)

	// add duplicate column
	_, err = db.Exec(`ALTER TABLE test_alter_err_db.test_table ADD COLUMN id INT`)
	require.Error(t, err)

	// drop non-existent column
	_, err = db.Exec(`ALTER TABLE test_alter_err_db.test_table DROP COLUMN non_existent_col`)
	require.Error(t, err)
}
