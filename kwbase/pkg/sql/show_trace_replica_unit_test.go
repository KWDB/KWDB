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
	"strconv"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// mockPlanNode is a mock planNode implementation for testing
type mockPlanNode struct {
	rows    [][]tree.Datum
	current int
	closed  bool
}

func (m *mockPlanNode) startExec(params runParams) error {
	return nil
}

func (m *mockPlanNode) Next(params runParams) (bool, error) {
	if m.current >= len(m.rows) {
		return false, nil
	}
	m.current++
	return true, nil
}

func (m *mockPlanNode) Values() tree.Datums {
	if m.current == 0 || m.current > len(m.rows) {
		return nil
	}
	return m.rows[m.current-1]
}

func (m *mockPlanNode) Close(ctx context.Context) {
	m.closed = true
}

// These are additional required methods for the planNode interface, not under test
func (m *mockPlanNode) maybeReveal(_ runParams) {}

func TestShowTraceReplicaHelpers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("TestNodeStoreRangeRE", func(t *testing.T) {
		testCases := []struct {
			name        string
			input       string
			expected    []string
			shouldMatch bool
		}{
			{
				name:        "valid tag format",
				input:       "[n1,s2,r3/...",
				expected:    []string{"[n1,s2,r3/", "1", "2", "3"},
				shouldMatch: true,
			},
			{
				name:        "another valid tag",
				input:       "[n10,s20,r30/abc",
				expected:    []string{"[n10,s20,r30/", "10", "20", "30"},
				shouldMatch: true,
			},
			{
				name:        "invalid format - missing r",
				input:       "[n1,s2,3/",
				shouldMatch: false,
			},
			{
				name:        "invalid format - missing brackets",
				input:       "n1,s2,r3/",
				shouldMatch: false,
			},
			{
				name:        "empty string",
				input:       "",
				shouldMatch: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				matches := nodeStoreRangeRE.FindStringSubmatch(tc.input)
				if tc.shouldMatch {
					require.NotNil(t, matches)
					require.Equal(t, tc.expected, matches)
				} else {
					require.Nil(t, matches)
				}
			})
		}
	})

	t.Run("TestReplicaMsgRE", func(t *testing.T) {
		testCases := []struct {
			name        string
			input       string
			shouldMatch bool
		}{
			{
				name:        "read-write path",
				input:       "read-write path",
				shouldMatch: true,
			},
			{
				name:        "read-only path",
				input:       "read-only path",
				shouldMatch: true,
			},
			{
				name:        "admin path",
				input:       "admin path",
				shouldMatch: true,
			},
			{
				name:        "not a match - other message",
				input:       "some other message",
				shouldMatch: false,
			},
			{
				name:        "not a match - prefix",
				input:       "prefix read-write path",
				shouldMatch: false,
			},
			{
				name:        "not a match - suffix",
				input:       "read-write path suffix",
				shouldMatch: false,
			},
			{
				name:        "empty string",
				input:       "",
				shouldMatch: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				matches := replicaMsgRE.MatchString(tc.input)
				if tc.shouldMatch {
					require.True(t, matches)
				} else {
					require.False(t, matches)
				}
			})
		}
	})

	t.Run("TestParseNodeStoreRange", func(t *testing.T) {
		testCases := []struct {
			name          string
			tag           string
			expectedNode  int
			expectedStore int
			expectedRange int
			shouldErr     bool
		}{
			{
				name:          "valid tag",
				tag:           "[n1,s2,r3/",
				expectedNode:  1,
				expectedStore: 2,
				expectedRange: 3,
				shouldErr:     false,
			},
			{
				name:          "another valid tag",
				tag:           "[n10,s20,r30/",
				expectedNode:  10,
				expectedStore: 20,
				expectedRange: 30,
				shouldErr:     false,
			},
			{
				name:      "invalid tag format",
				tag:       "invalid",
				shouldErr: true,
			},
			{
				name:      "invalid node number",
				tag:       "[nabc,s2,r3/",
				shouldErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				matches := nodeStoreRangeRE.FindStringSubmatch(tc.tag)
				if tc.shouldErr && matches == nil {
					return // Expected error
				}
				if !tc.shouldErr {
					require.NotNil(t, matches)
					nodeID, err := strconv.Atoi(matches[1])
					require.NoError(t, err)
					storeID, err := strconv.Atoi(matches[2])
					require.NoError(t, err)
					rangeID, err := strconv.Atoi(matches[3])
					require.NoError(t, err)
					require.Equal(t, tc.expectedNode, nodeID)
					require.Equal(t, tc.expectedStore, storeID)
					require.Equal(t, tc.expectedRange, rangeID)
				}
			})
		}
	})
}

func TestShowTraceReplicaNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mockPlan := &mockPlanNode{}
	node := &showTraceReplicaNode{
		plan: mockPlan,
	}

	ctx := context.Background()
	node.Close(ctx)

	require.True(t, mockPlan.closed)
}

func TestShowTraceReplicaNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	node := &showTraceReplicaNode{}
	params := runParams{}
	err := node.startExec(params)
	require.NoError(t, err)
}

func TestShowTraceReplicaNodeValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	node := &showTraceReplicaNode{}
	// We only verify no panic occurs, not the actual content
	require.NotPanics(t, func() {
		node.Values()
	})
}

// TestParseNodeStoreRange tests the logic for parsing node, store, and range IDs from tags
func TestParseNodeStoreRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name          string
		tag           string
		expectedNode  int
		expectedStore int
		expectedRange int
		shouldErr     bool
		errContains   string
	}{
		{
			name:          "valid tag",
			tag:           "[n1,s2,r3/",
			expectedNode:  1,
			expectedStore: 2,
			expectedRange: 3,
			shouldErr:     false,
		},
		{
			name:          "another valid tag",
			tag:           "[n10,s20,r30/",
			expectedNode:  10,
			expectedStore: 20,
			expectedRange: 30,
			shouldErr:     false,
		},
		{
			name:        "invalid tag - no match",
			tag:         "invalid tag",
			shouldErr:   true,
			errContains: "could not extract node, store, range from",
		},
		{
			name:      "invalid node number",
			tag:       "[nabc,s2,r3/",
			shouldErr: true,
		},
		{
			name:      "invalid store number",
			tag:       "[n1,sabc,r3/",
			shouldErr: true,
		},
		{
			name:      "invalid range number",
			tag:       "[n1,s2,rabc/",
			shouldErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			matches := nodeStoreRangeRE.FindStringSubmatch(tc.tag)
			if tc.shouldErr && matches == nil {
				// Test case for regex mismatch scenario
				return
			}

			if matches == nil {
				if !tc.shouldErr {
					t.Fatalf("expected matches, got nil")
				}
				return
			}

			nodeID, err := strconv.Atoi(matches[1])
			if tc.shouldErr && err != nil {
				return
			}
			require.NoError(t, err)

			storeID, err := strconv.Atoi(matches[2])
			if tc.shouldErr && err != nil {
				return
			}
			require.NoError(t, err)

			rangeID, err := strconv.Atoi(matches[3])
			if tc.shouldErr && err != nil {
				return
			}
			require.NoError(t, err)

			if !tc.shouldErr {
				require.Equal(t, tc.expectedNode, nodeID)
				require.Equal(t, tc.expectedStore, storeID)
				require.Equal(t, tc.expectedRange, rangeID)
			}
		})
	}
}
