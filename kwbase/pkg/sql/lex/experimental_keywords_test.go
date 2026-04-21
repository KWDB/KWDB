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

package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllowedExperimental(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"ranges is allowed", "ranges", true},
		{"other keyword is not allowed", "select", false},
		{"empty string is not allowed", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := AllowedExperimental[tt.input]
			require.Equal(t, tt.want, got)
		})
	}
}
