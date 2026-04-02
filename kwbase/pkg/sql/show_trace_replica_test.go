// Copyright 2017 The Cockroach Authors.
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
	"regexp"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestNodeStoreRangeRE tests the nodeStoreRangeRE regular expression
func TestNodeStoreRangeRE(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		input    string
		expected []string
		match    bool
	}{
		{
			name:     "standard format",
			input:    "[n1,s2,r3/4:some-range-info]",
			expected: []string{"[n1,s2,r3/", "1", "2", "3"},
			match:    true,
		},
		{
			name:     "only node information",
			input:    "[n10,s20,r30/40]",
			expected: []string{"[n10,s20,r30/", "10", "20", "30"},
			match:    true,
		},
		{
			name:     "invalid format",
			input:    "some random text",
			expected: nil,
			match:    false,
		},
		{
			name:     "partial match",
			input:    "[n1,s2]",
			expected: nil,
			match:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := nodeStoreRangeRE.FindStringSubmatch(tt.input)
			if tt.match {
				require.NotNil(t, matches)
				require.Equal(t, tt.expected, matches)
			} else {
				require.Nil(t, matches)
			}
		})
	}
}

// TestReplicaMsgRE tests the replicaMsgRE regular expression
func TestReplicaMsgRE(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name  string
		input string
		match bool
	}{
		{
			name:  "read-write path",
			input: "read-write path",
			match: true,
		},
		{
			name:  "read-only path",
			input: "read-only path",
			match: true,
		},
		{
			name:  "admin path",
			input: "admin path",
			match: true,
		},
		{
			name:  "other message",
			input: "some other message",
			match: false,
		},
		{
			name:  "empty string",
			input: "",
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match := replicaMsgRE.MatchString(tt.input)
			require.Equal(t, tt.match, match)
		})
	}
}

// TestNodeStoreRangeRECompile tests regex compilation
func TestNodeStoreRangeRECompile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify regex compiles correctly
	require.NotNil(t, nodeStoreRangeRE)
	require.Equal(t, "^\\[n(\\d+),s(\\d+),r(\\d+)/", nodeStoreRangeRE.String())
}

// TestReplicaMsgRECompile tests regex compilation
func TestReplicaMsgRECompile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify regex compiles correctly
	require.NotNil(t, replicaMsgRE)
	require.Equal(t, "^read-write path$|^read-only path$|^admin path$", replicaMsgRE.String())
}

// TestShowTraceReplicaNodeMethods tests methods of showTraceReplicaNode
func TestShowTraceReplicaNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock showTraceReplicaNode
	node := &showTraceReplicaNode{}

	// Test Values method (should return nil before startExec)
	values := node.Values()
	require.Nil(t, values)
}

// TestNodeStoreRangeREExtract tests extracting node, store, and range IDs
func TestNodeStoreRangeREExtract(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		input           string
		expectedNodeID  int
		expectedStoreID int
		expectedRangeID int
		expectError     bool
	}{
		{
			input:           "[n1,s2,r3/4:test]",
			expectedNodeID:  1,
			expectedStoreID: 2,
			expectedRangeID: 3,
			expectError:     false,
		},
		{
			input:           "[n100,s200,r300/400]",
			expectedNodeID:  100,
			expectedStoreID: 200,
			expectedRangeID: 300,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			matches := nodeStoreRangeRE.FindStringSubmatch(tt.input)
			if tt.expectError {
				require.Nil(t, matches)
			} else {
				require.NotNil(t, matches)
				require.Len(t, matches, 4)
				// matches[1] = nodeID, matches[2] = storeID, matches[3] = rangeID
				require.NotEmpty(t, matches[1])
				require.NotEmpty(t, matches[2])
				require.NotEmpty(t, matches[3])
			}
		})
	}
}

// TestReplicaMsgREComplex tests complex matching of replicaMsgRE
func TestReplicaMsgREComplex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name  string
		input string
		match bool
	}{
		{
			name:  "contains read-write path",
			input: "read-write path",
			match: true,
		},
		{
			name:  "contains read-only path",
			input: "read-only path",
			match: true,
		},
		{
			name:  "contains admin path",
			input: "admin path",
			match: true,
		},
		{
			name:  "prefix match failed",
			input: "pre-read-write path",
			match: false,
		},
		{
			name:  "suffix match failed",
			input: "read-write path-suffix",
			match: false,
		},
		{
			name:  "case sensitive",
			input: "READ-WRITE PATH",
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match := replicaMsgRE.MatchString(tt.input)
			require.Equal(t, tt.match, match, "input: %s", tt.input)
		})
	}
}

// TestRegexpPatterns tests regex patterns
func TestRegexpPatterns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test nodeStoreRangeRE pattern
	t.Run("nodeStoreRangeRE pattern", func(t *testing.T) {
		pattern := `^\[n(\d+),s(\d+),r(\d+)/`
		re := regexp.MustCompile(pattern)

		testCases := []string{
			"[n1,s1,r1/1:test]",
			"[n123,s456,r789/012:range]",
		}

		for _, tc := range testCases {
			matches := re.FindStringSubmatch(tc)
			require.NotNil(t, matches, "should match: %s", tc)
			require.Len(t, matches, 4)
		}
	})

	// Test replicaMsgRE pattern
	t.Run("replicaMsgRE pattern", func(t *testing.T) {
		pattern := `^read-write path$|^read-only path$|^admin path$`
		re := regexp.MustCompile(pattern)

		positiveCases := []string{
			"read-write path",
			"read-only path",
			"admin path",
		}

		for _, tc := range positiveCases {
			require.True(t, re.MatchString(tc), "should match: %s", tc)
		}

		negativeCases := []string{
			"read-write",
			"write path",
			"admin",
		}

		for _, tc := range negativeCases {
			require.False(t, re.MatchString(tc), "should not match: %s", tc)
		}
	})
}
