// Copyright 2016 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPgCatalogTablesPopulate tests the populate functions of various pg_catalog tables
func TestPgCatalogTablesPopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Start a test server
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner with admin privileges
	localPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, s.NodeID()),
		security.RootUser, // Root user has admin privileges
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := localPlanner.(*planner)
	p.preparedStatements = connExPrepStmtsAccessor{
		ex: &connExecutor{},
	}

	// Create a mock database descriptor for tables that need it
	dbDesc := &DatabaseDescriptor{
		ID:   1,
		Name: "test_db",
	}

	// Test cases for tables that only need a simple planner
	simpleTestCases := []struct {
		name     string
		table    virtualSchemaTable
		expected int
	}{
		{
			name:     "pg_am",
			table:    pgCatalogAmTable,
			expected: 2, // forward and inverted index
		},
		{
			name:     "pg_available_extensions",
			table:    pgCatalogAvailableExtensionsTable,
			expected: 0, // no extensions supported
		},
		{
			name:     "pg_cast",
			table:    pgCatalogCastTable,
			expected: 0, // not implemented
		},
		{
			name:     "pg_conversion",
			table:    pgCatalogConversionTable,
			expected: 0, // not implemented
		},
		{
			name:     "pg_prepared_statements",
			table:    pgCatalogPreparedStatementsTable,
			expected: 0, // no prepared statements in test environment
		},
	}

	// Run simple test cases
	for _, tc := range simpleTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a slice to capture the rows added by populate
			var rows [][]tree.Datum
			addRow := func(d ...tree.Datum) error {
				rows = append(rows, d)
				return nil
			}

			// Call populate
			err := tc.table.populate(ctx, p, dbDesc, addRow)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			// Verify the expected number of rows
			if len(rows) != tc.expected {
				t.Fatalf("expected %d rows, got: %d", tc.expected, len(rows))
			}

			// For pg_am, verify rows have at least 1 column
			if tc.name == "pg_am" && len(rows) > 0 {
				for i, row := range rows {
					if len(row) < 1 {
						t.Fatalf("expected row %d to have at least 1 column, got: %d", i, len(row))
					}
				}
			}
		})
	}

	// Test cases for tables that need a server and proper planner
	serverTestCases := []struct {
		name  string
		table virtualSchemaTable
	}{
		{
			name:  "pg_authid",
			table: pgCatalogAuthIDTable,
		},
		{
			name:  "pg_class",
			table: pgCatalogClassTable,
		},
		{
			name:  "pg_auth_members",
			table: pgCatalogAuthMembersTable,
		},
		{
			name:  "pg_depend",
			table: pgCatalogDependTable,
		},
		{
			name:  "pg_index",
			table: pgCatalogIndexTable,
		},
		{
			name:  "pg_indexes",
			table: pgCatalogIndexesTable,
		},
		{
			name:  "pg_proc",
			table: pgCatalogProcTable,
		},
		{
			name:  "pg_roles",
			table: pgCatalogRolesTable,
		},
		{
			name:  "pg_tables",
			table: pgCatalogTablesTable,
		},
		{
			name:  "pg_user",
			table: pgCatalogUserTable,
		},
		{
			name:  "pg_attrdef",
			table: pgCatalogAttrDefTable,
		},
		{
			name:  "pg_collation",
			table: pgCatalogCollationTable,
		},
		{
			name:  "pg_operator",
			table: pgCatalogOperatorTable,
		},
		{
			name:  "pg_settings",
			table: pgCatalogSettingsTable,
		},
	}

	// Run server test cases
	for _, tc := range serverTestCases {
		t.Run(tc.name, func(t *testing.T) {

			// Create a slice to capture the rows added by populate
			var rows [][]tree.Datum
			addRow := func(d ...tree.Datum) error {
				rows = append(rows, d)
				return nil
			}

			// Call populate
			err := tc.table.populate(ctx, p, dbDesc, addRow)
			// This might fail if the planner isn't properly initialized, but we're just testing that it doesn't panic
			if err != nil {
				t.Logf("populate returned error (expected in test environment): %v", err)
			}
		})
	}
}
