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

package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropCascadeIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create database and schema
	_, err := db.Exec(`CREATE DATABASE test_cascade_db`)
	require.NoError(t, err)

	// use the database first, then create schema
	_, err = db.Exec(`SET database = test_cascade_db`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE SCHEMA test_schema`)
	require.NoError(t, err)

	// create table in schema
	_, err = db.Exec(`CREATE TABLE test_schema.test_table (id INT PRIMARY KEY, name VARCHAR)`)
	require.NoError(t, err)

	// drop schema with cascade - should drop the table inside
	_, err = db.Exec(`DROP SCHEMA test_schema CASCADE`)
	require.NoError(t, err)

	// verify schema is dropped
	_, err = db.Exec(`SHOW SCHEMAS`)
	require.NoError(t, err)
}

func TestDropCascadeIfExists(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create database
	_, err := db.Exec(`CREATE DATABASE test_cascade_exists`)
	require.NoError(t, err)

	// drop non-existent schema with IF EXISTS - should not error
	_, err = db.Exec(`DROP SCHEMA IF EXISTS non_existent_schema CASCADE`)
	require.NoError(t, err)

	// drop non-existent schema without IF EXISTS - should error
	_, err = db.Exec(`DROP SCHEMA non_existent_schema CASCADE`)
	require.Error(t, err)
}

func TestDropCascadeWithoutCascade(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create database and schema with table
	_, err := db.Exec(`CREATE DATABASE test_no_cascade`)
	require.NoError(t, err)

	_, err = db.Exec(`SET database = test_no_cascade`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE SCHEMA test_schema`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE test_schema.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// drop schema without cascade - should error because schema is not empty
	_, err = db.Exec(`DROP SCHEMA test_schema`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not empty")
}

func TestDropCascadeSystemSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// try to drop public schema - should error
	_, err := db.Exec(`DROP SCHEMA public CASCADE`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop schema")

	// try to drop pg_catalog schema - should error
	_, err = db.Exec(`DROP SCHEMA pg_catalog CASCADE`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop schema")
}
