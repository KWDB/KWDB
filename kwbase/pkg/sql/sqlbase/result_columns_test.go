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

package sqlbase

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestResultColumnsTypesEqual(t *testing.T) {
	tests := []struct {
		r, o  ResultColumns
		equal bool
	}{
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.String}},
			equal: false,
		},
		{
			r:     ResultColumns{{Typ: types.Unknown}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: false,
		},
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Unknown}},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Int}, {Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: false,
		},
		{
			r:     ResultColumns{},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v-%v", tc.r, tc.o), func(t *testing.T) {
			eq := tc.r.TypesEqual(tc.o)
			if eq != tc.equal {
				t.Fatalf("expected %v, got %v", tc.equal, eq)
			}
		})
	}
}
