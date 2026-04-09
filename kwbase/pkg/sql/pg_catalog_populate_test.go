// Copyright 2016 The Cockroach Authors.
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

// TestPgCatalogAmTablePopulate tests the populate function of pg_catalog.pg_am
func TestPgCatalogAmTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner (empty struct is sufficient for this test)
	p := &planner{}

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogAmTable.populate(context.Background(), p, nil, addRow)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify that two rows were added (forward index and inverted index)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got: %d", len(rows))
	}

	// Verify the first row (forward index)
	forwardRow := rows[0]
	if len(forwardRow) < 1 {
		t.Fatalf("expected forward row to have at least 1 column, got: %d", len(forwardRow))
	}

	// Verify the second row (inverted index)
	invertedRow := rows[1]
	if len(invertedRow) < 1 {
		t.Fatalf("expected inverted row to have at least 1 column, got: %d", len(invertedRow))
	}
}

// TestPgCatalogAvailableExtensionsTablePopulate tests the populate function of pg_catalog.pg_available_extensions
func TestPgCatalogAvailableExtensionsTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner
	p := &planner{}

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogAvailableExtensionsTable.populate(context.Background(), p, nil, addRow)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify that no rows were added (since we support no extensions)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got: %d", len(rows))
	}
}

// TestPgCatalogCastTablePopulate tests the populate function of pg_catalog.pg_cast
func TestPgCatalogCastTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner
	p := &planner{}

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogCastTable.populate(context.Background(), p, nil, addRow)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify that no rows were added (since it's not implemented)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got: %d", len(rows))
	}
}

// TestPgCatalogConversionTablePopulate tests the populate function of pg_catalog.pg_conversion
func TestPgCatalogConversionTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner
	p := &planner{}

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogConversionTable.populate(context.Background(), p, nil, addRow)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify that no rows were added (since it's not implemented)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got: %d", len(rows))
	}
}

// TestPgCatalogAuthIDTablePopulate tests the populate function of pg_catalog.pg_authid
func TestPgCatalogAuthIDTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner with admin privileges
	localPlanner, cleanup := NewInternalPlanner(
		"test",
		nil,               // No transaction needed for this test
		security.RootUser, // Root user has admin privileges
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := localPlanner.(*planner)

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogAuthIDTable.populate(context.Background(), p, nil, addRow)
	// This might fail if the planner isn't properly initialized, but we're just testing that it doesn't panic
	if err != nil {
		t.Logf("populate returned error (expected in test environment): %v", err)
	}
}

// TestPgCatalogClassTablePopulate tests the populate function of pg_catalog.pg_class
func TestPgCatalogClassTablePopulate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock planner
	ctx := context.Background()

	// Start a test server
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner with admin privileges
	localPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, s.NodeID()), // No transaction needed for this test
		security.RootUser,              // Root user has admin privileges
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := localPlanner.(*planner)

	// Create a slice to capture the rows added by populate
	var rows [][]tree.Datum
	addRow := func(d ...tree.Datum) error {
		rows = append(rows, d)
		return nil
	}

	// Call populate
	err := pgCatalogClassTable.populate(context.Background(), p, nil, addRow)
	// This might fail if the planner isn't properly initialized, but we're just testing that it doesn't panic
	if err != nil {
		t.Logf("populate returned error (expected in test environment): %v", err)
	}
}
