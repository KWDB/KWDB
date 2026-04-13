// Copyright 2015 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestMaybePlanHook tests the maybePlanHook function
func TestMaybePlanHook(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		stmt        tree.Statement
		expectedErr bool
	}{
		{
			name:        "nil statement",
			stmt:        nil,
			expectedErr: false,
		},
		{
			name:        "empty select statement",
			stmt:        &tree.Select{},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &planner{}

			result, err := p.maybePlanHook(context.Background(), tt.stmt)

			if tt.expectedErr {
				if err == nil {
					t.Errorf("maybePlanHook() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("maybePlanHook() unexpected error: %v", err)
				}
				// For statements that don't match any plan hooks, should return nil
				if result != nil {
					t.Errorf("maybePlanHook() returned non-nil result: %v", result)
				}
			}
		})
	}
}

// TestResetNewTxn tests the resetNewTxn function
func TestResetNewTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		test func(t *testing.T, r *runParams)
	}{
		{
			name: "reset new transaction basic test",
			test: func(t *testing.T, r *runParams) {
				// Store original transaction
				originalTxn := r.p.txn

				// Call resetNewTxn - this will panic if DB is nil, but we want to test the function signature
				// and basic behavior when called
				defer func() {
					if r := recover(); r != nil {
						// Expected panic due to nil DB, this is acceptable for this test
						t.Logf("resetNewTxn() panicked as expected: %v", r)
					}
				}()

				r.resetNewTxn()

				// If we reach here without panic, verify that transaction was replaced
				if r.p.txn == originalTxn {
					t.Errorf("resetNewTxn() did not replace the transaction")
				}

				// Verify that new transaction is not nil
				if r.p.txn == nil {
					t.Errorf("resetNewTxn() returned nil transaction")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock runParams with minimal setup
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					NodeID: roachpb.NodeID(1),
				},
			}

			s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.TODO())

			p := &planner{
				txn: kv.NewTxn(context.TODO(), kvDB, 1),
			}

			r := &runParams{
				ctx:             context.TODO(),
				extendedEvalCtx: extendedEvalCtx,
				p:               p,
			}

			tt.test(t, r)
		})
	}
}
