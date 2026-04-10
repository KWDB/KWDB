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

// TestSetSessionAuthorizationDefault tests the SetSessionAuthorizationDefault method
func TestSetSessionAuthorizationDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a test planner
	p := &planner{}

	// Test SetSessionAuthorizationDefault
	node, err := p.SetSessionAuthorizationDefault()
	if err != nil {
		t.Errorf("SetSessionAuthorizationDefault should not return error, got %v", err)
	}

	// Verify the returned node is a zeroNode
	_, ok := node.(*zeroNode)
	if !ok {
		t.Error("SetSessionAuthorizationDefault should return a zeroNode")
	}
}

// TestSetSessionCharacteristics tests the SetSessionCharacteristics method
func TestSetSessionCharacteristics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a test planner with sessionDataMutator
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	plan, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	p := plan.(*planner)
	defer cleanup()

	// Test cases for different isolation levels and read write modes
	testCases := []struct {
		name          string
		isolation     tree.IsolationLevel
		readWriteMode tree.ReadWriteMode
		expectedError bool
	}{
		{"Serializable isolation, read write", tree.SerializableIsolation, tree.ReadWrite, false},
		{"Read committed isolation, read only", tree.ReadCommittedIsolation, tree.ReadOnly, false},
		{"Repeated read isolation, read write", tree.RepeatedReadIsolation, tree.ReadWrite, false},
		{"Unspecified isolation, read write", tree.UnspecifiedIsolation, tree.ReadWrite, false},
		{"Unsupported isolation", tree.IsolationLevel(999), tree.ReadWrite, true},
		{"Unsupported read write mode", tree.SerializableIsolation, tree.ReadWriteMode(999), true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create SetSessionCharacteristics node
			n := &tree.SetSessionCharacteristics{
				Modes: tree.TransactionModes{
					Isolation:     tc.isolation,
					ReadWriteMode: tc.readWriteMode,
				},
			}

			// Test SetSessionCharacteristics
			node, err := p.SetSessionCharacteristics(n)
			if (err != nil) != tc.expectedError {
				t.Errorf("SetSessionCharacteristics for %s: expected error = %t, got error = %v", tc.name, tc.expectedError, err)
			}

			if !tc.expectedError {
				// Verify the returned node is a zeroNode
				_, ok := node.(*zeroNode)
				if !ok {
					t.Error("SetSessionCharacteristics should return a zeroNode")
				}
			}
		})
	}

	// Test user priority (should return unimplemented error)
	t.Run("User priority", func(t *testing.T) {
		// Create SetSessionCharacteristics node with user priority
		n := &tree.SetSessionCharacteristics{
			Modes: tree.TransactionModes{
				Isolation:     tree.SerializableIsolation,
				ReadWriteMode: tree.ReadWrite,
				UserPriority:  tree.UserPriority(1),
			},
		}

		// Test SetSessionCharacteristics
		node, err := p.SetSessionCharacteristics(n)
		if err == nil {
			t.Error("SetSessionCharacteristics should return error for user priority")
		}

		if node != nil {
			t.Error("SetSessionCharacteristics should return nil node for user priority")
		}
	})
}
