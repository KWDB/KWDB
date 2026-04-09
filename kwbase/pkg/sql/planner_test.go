// Copyright 2017 The Cockroach Authors.
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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func TestTypeAsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}
	testData := []struct {
		expr        tree.Expr
		expected    string
		expectedErr bool
	}{
		{expr: tree.NewDString("foo"), expected: "foo"},
		{
			expr: &tree.BinaryExpr{
				Operator: tree.Concat, Left: tree.NewDString("foo"), Right: tree.NewDString("bar")},
			expected: "foobar",
		},
		{expr: tree.NewDInt(3), expectedErr: true},
	}

	t.Run("TypeAsString", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsString(td.expr, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			s, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			if s != td.expected {
				t.Fatalf("expected %s; got %s", td.expected, s)
			}
		}
	})

	t.Run("TypeAsStringArray", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsStringArray([]tree.Expr{td.expr, td.expr}, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			a, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			expected := []string{td.expected, td.expected}
			if !reflect.DeepEqual(a, expected) {
				t.Fatalf("expected %s; got %s", expected, a)
			}
		}
	})
}

// TestRelocateRange tests the RelocateRange method
func TestRelocateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
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
	// Test case 1: RelocateRange with invalid range ID
	t.Run("RelocateRange with invalid range ID", func(t *testing.T) {
		// Test RelocateRange with a range ID that doesn't exist
		err := p.RelocateRange(ctx, 999999, 1, 2)
		// The function should return an error because the range doesn't exist
		if err == nil {
			t.Error("RelocateRange should return error for invalid range ID")
		}
	})
}

// TestGetRangeDebugInfo tests the GetRangeDebugInfo method
func TestGetRangeDebugInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
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
	// Test case 1: GetRangeDebugInfo with invalid range ID
	t.Run("GetRangeDebugInfo with invalid range ID", func(t *testing.T) {
		// Test GetRangeDebugInfo with a range ID that doesn't exist
		_, err := p.GetRangeDebugInfo(ctx, 999999)
		// The function should return an error because the range doesn't exist
		if err == nil {
			t.Error("GetRangeDebugInfo should return error for invalid range ID")
		}
	})
}
