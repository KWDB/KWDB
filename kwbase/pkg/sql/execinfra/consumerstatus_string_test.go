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

package execinfra

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestConsumerStatusString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		status   ConsumerStatus
		expected string
	}{
		{
			name:     "NeedMoreRows",
			status:   NeedMoreRows,
			expected: "NeedMoreRows",
		},
		{
			name:     "DrainRequested",
			status:   DrainRequested,
			expected: "DrainRequested",
		},
		{
			name:     "ConsumerClosed",
			status:   ConsumerClosed,
			expected: "ConsumerClosed",
		},
		{
			name:     "Invalid status 3",
			status:   ConsumerStatus(3),
			expected: "ConsumerStatus(3)",
		},
		{
			name:     "Invalid status 100",
			status:   ConsumerStatus(100),
			expected: "ConsumerStatus(100)",
		},
		{
			name:     "Invalid status max uint32",
			status:   ConsumerStatus(0xFFFFFFFF),
			expected: "ConsumerStatus(4294967295)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.String()
			if got != tt.expected {
				t.Errorf("ConsumerStatus.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConsumerStatusStringBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		status   ConsumerStatus
		expected string
	}{
		{
			name:     "Minimum valid status",
			status:   ConsumerStatus(0),
			expected: "NeedMoreRows",
		},
		{
			name:     "Maximum valid status",
			status:   ConsumerStatus(2),
			expected: "ConsumerClosed",
		},
		{
			name:     "Just above maximum valid status",
			status:   ConsumerStatus(3),
			expected: "ConsumerStatus(3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.String()
			if got != tt.expected {
				t.Errorf("ConsumerStatus.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConsumerStatusStringConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	statuses := []ConsumerStatus{NeedMoreRows, DrainRequested, ConsumerClosed}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			str := status.String()
			parsedStatus := ConsumerStatus(0)
			switch str {
			case "NeedMoreRows":
				parsedStatus = NeedMoreRows
			case "DrainRequested":
				parsedStatus = DrainRequested
			case "ConsumerClosed":
				parsedStatus = ConsumerClosed
			default:
				t.Errorf("Unexpected string representation: %s", str)
				return
			}
			if parsedStatus != status {
				t.Errorf("String() returned %s which does not map back to original status", str)
			}
		})
	}
}

func TestConsumerStatusStringUnique(t *testing.T) {
	defer leaktest.AfterTest(t)()
	statuses := []ConsumerStatus{NeedMoreRows, DrainRequested, ConsumerClosed}
	strings := make(map[string]bool)

	for _, status := range statuses {
		str := status.String()
		if strings[str] {
			t.Errorf("Duplicate string representation: %s", str)
		}
		strings[str] = true
	}

	if len(strings) != len(statuses) {
		t.Errorf("Expected %d unique string representations, got %d", len(statuses), len(strings))
	}
}

func TestConsumerStatusStringFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		status   ConsumerStatus
		contains string
	}{
		{
			name:     "NeedMoreRows format",
			status:   NeedMoreRows,
			contains: "NeedMoreRows",
		},
		{
			name:     "DrainRequested format",
			status:   DrainRequested,
			contains: "DrainRequested",
		},
		{
			name:     "ConsumerClosed format",
			status:   ConsumerClosed,
			contains: "ConsumerClosed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.String()
			if got != tt.contains {
				t.Errorf("ConsumerStatus.String() = %v, want exact match %v", got, tt.contains)
			}
		})
	}
}
