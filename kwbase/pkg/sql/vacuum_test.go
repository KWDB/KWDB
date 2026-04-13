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
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPlannerVacuum tests the Vacuum method of planner
func TestPlannerVacuum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		setupFunc   func() (*planner, *tree.Vacuum)
		expectedErr bool
	}{
		{
			name: "test Vacuum returns valid node",
			setupFunc: func() (*planner, *tree.Vacuum) {
				return &planner{}, &tree.Vacuum{}
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, vacuumStmt := tt.setupFunc()
			
			result, err := p.Vacuum(context.Background(), vacuumStmt)
			
			if tt.expectedErr {
				if err == nil {
					t.Error("Vacuum() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Vacuum() unexpected error: %v", err)
				}
				if result == nil {
					t.Error("Vacuum() returned nil")
				}
				
				// Verify it's a vacuumNode
				if _, ok := result.(*vacuumNode); !ok {
					t.Errorf("Vacuum() returned %T, want *vacuumNode", result)
				}
			}
		})
	}
}

// TestVacuumNodeStartExec tests the startExec method of vacuumNode
func TestVacuumNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *vacuumNode
		testFunc  func(t *testing.T, n *vacuumNode)
	}{
		{
			name: "test startExec returns nil",
			setupNode: func() *vacuumNode {
				return &vacuumNode{}
			},
			testFunc: func(t *testing.T, n *vacuumNode) {
				params := runParams{
					ctx: context.Background(),
				}
				err := n.startExec(params)
				if err != nil {
					t.Errorf("startExec() returned error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestVacuumNodeNext tests the Next method of vacuumNode
func TestVacuumNodeNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *vacuumNode
		testFunc  func(t *testing.T, n *vacuumNode)
	}{
		{
			name: "test Next returns false",
			setupNode: func() *vacuumNode {
				return &vacuumNode{}
			},
			testFunc: func(t *testing.T, n *vacuumNode) {
				params := runParams{
					ctx: context.Background(),
				}
				hasNext, err := n.Next(params)
				if err != nil {
					t.Errorf("Next() returned error: %v", err)
				}
				if hasNext {
					t.Error("Next() = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestVacuumNodeValues tests the Values method of vacuumNode
func TestVacuumNodeValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *vacuumNode
		testFunc  func(t *testing.T, n *vacuumNode)
	}{
		{
			name: "test Values returns empty Datums",
			setupNode: func() *vacuumNode {
				return &vacuumNode{}
			},
			testFunc: func(t *testing.T, n *vacuumNode) {
				result := n.Values()
				if result == nil {
					t.Error("Values() returned nil")
				}
				if len(result) != 0 {
					t.Errorf("Values() length = %d, want 0", len(result))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestVacuumNodeClose tests the Close method of vacuumNode
func TestVacuumNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *vacuumNode
		testFunc  func(t *testing.T, n *vacuumNode)
	}{
		{
			name: "test Close does not panic",
			setupNode: func() *vacuumNode {
				return &vacuumNode{}
			},
			testFunc: func(t *testing.T, n *vacuumNode) {
				// Should not panic
				n.Close(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}