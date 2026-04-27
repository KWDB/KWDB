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

package ordering

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

// TestMutationCanProvideOrdering tests the mutationCanProvideOrdering function
func TestMutationCanProvideOrdering(t *testing.T) {
	testCases := []struct {
		name     string
		required string
	}{
		{
			name:     "simple-ordering",
			required: "+1",
		},
		{
			name:     "complex-ordering",
			required: "+1,-2,+3",
		},
		{
			name:     "empty-ordering",
			required: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			required := physical.ParseOrderingChoice(tc.required)

			result := mutationCanProvideOrdering(nil, &required)
			if !result {
				t.Errorf("mutationCanProvideOrdering should always return true, got %v", result)
			}
		})
	}
}
