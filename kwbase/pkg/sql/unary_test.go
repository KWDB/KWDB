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

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestUnaryNodeStartExec tests the startExec method of unaryNode
func TestUnaryNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *unaryNode
		testFunc  func(t *testing.T, u *unaryNode)
	}{
		{
			name: "test startExec returns nil",
			setupNode: func() *unaryNode {
				return &unaryNode{}
			},
			testFunc: func(t *testing.T, u *unaryNode) {
				params := runParams{
					ctx: context.Background(),
				}
				err := u.startExec(params)
				if err != nil {
					t.Errorf("startExec() returned error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.setupNode()
			tt.testFunc(t, u)
		})
	}
}

// TestUnaryNodeValues tests the Values method of unaryNode
func TestUnaryNodeValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *unaryNode
		testFunc  func(t *testing.T, u *unaryNode)
	}{
		{
			name: "test Values returns nil",
			setupNode: func() *unaryNode {
				return &unaryNode{}
			},
			testFunc: func(t *testing.T, u *unaryNode) {
				result := u.Values()
				if result != nil {
					t.Errorf("Values() = %v, want nil", result)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.setupNode()
			tt.testFunc(t, u)
		})
	}
}

// TestUnaryNodeNext tests the Next method of unaryNode
func TestUnaryNodeNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *unaryNode
		testFunc  func(t *testing.T, u *unaryNode)
	}{
		{
			name: "test Next returns true then false",
			setupNode: func() *unaryNode {
				return &unaryNode{}
			},
			testFunc: func(t *testing.T, u *unaryNode) {
				params := runParams{
					ctx: context.Background(),
				}
				
				// First call should return true
				hasNext, err := u.Next(params)
				if err != nil {
					t.Errorf("Next() first call returned error: %v", err)
				}
				if !hasNext {
					t.Error("Next() first call = false, want true")
				}
				
				// Second call should return false
				hasNext, err = u.Next(params)
				if err != nil {
					t.Errorf("Next() second call returned error: %v", err)
				}
				if hasNext {
					t.Error("Next() second call = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.setupNode()
			tt.testFunc(t, u)
		})
	}
}

// TestUnaryNodeClose tests the Close method of unaryNode
func TestUnaryNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *unaryNode
		testFunc  func(t *testing.T, u *unaryNode)
	}{
		{
			name: "test Close does not panic",
			setupNode: func() *unaryNode {
				return &unaryNode{}
			},
			testFunc: func(t *testing.T, u *unaryNode) {
				// Should not panic
				u.Close(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.setupNode()
			tt.testFunc(t, u)
		})
	}
}