// Copyright 2018 The Cockroach Authors.
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

package opt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnID_Index(t *testing.T) {
	tests := []struct {
		name string
		id   ColumnID
		want int
	}{
		{"ColumnID 1", 1, 0},
		{"ColumnID 2", 2, 1},
		{"ColumnID 10", 10, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.id.index()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColList_ToSet(t *testing.T) {
	tests := []struct {
		name string
		list ColList
		want ColSet
	}{
		{"empty list", ColList{}, ColSet{}},
		{"single column", ColList{1}, MakeColSet(1)},
		{"multiple columns", ColList{1, 2, 3}, MakeColSet(1, 2, 3)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.list.ToSet()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColList_Find(t *testing.T) {
	tests := []struct {
		name    string
		list    ColList
		col     ColumnID
		wantIdx int
		wantOk  bool
	}{
		{"empty list", ColList{}, 1, -1, false},
		{"column not found", ColList{1, 2, 3}, 4, -1, false},
		{"column found at index 0", ColList{1, 2, 3}, 1, 0, true},
		{"column found at index 1", ColList{1, 2, 3}, 2, 1, true},
		{"column found at index 2", ColList{1, 2, 3}, 3, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, gotOk := tt.list.Find(tt.col)
			require.Equal(t, tt.wantIdx, gotIdx)
			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestColList_Equals(t *testing.T) {
	tests := []struct {
		name  string
		list1 ColList
		list2 ColList
		want  bool
	}{
		{"empty lists", ColList{}, ColList{}, true},
		{"same lists", ColList{1, 2, 3}, ColList{1, 2, 3}, true},
		{"different lengths", ColList{1, 2}, ColList{1, 2, 3}, false},
		{"different elements", ColList{1, 2, 3}, ColList{1, 2, 4}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.list1.Equals(tt.list2)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColSetToList(t *testing.T) {
	tests := []struct {
		name string
		set  ColSet
		want ColList
	}{
		{"empty set", ColSet{}, ColList{}},
		{"single column", MakeColSet(1), ColList{1}},
		{"multiple columns", MakeColSet(3, 1, 2), ColList{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ColSetToList(tt.set)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColumnMeta_IsNormalCol(t *testing.T) {
	tests := []struct {
		name   string
		tsType int
		want   bool
	}{
		{"normal column", ColNormal, true},
		{"TS normal column", TSColNormal, false},
		{"TS tag column", TSColTag, false},
		{"TS primary tag column", TSColPrimaryTag, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ColumnMeta{TSType: tt.tsType}
			got := cm.IsNormalCol()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColumnMeta_IsTag(t *testing.T) {
	tests := []struct {
		name   string
		tsType int
		want   bool
	}{
		{"normal column", ColNormal, false},
		{"TS normal column", TSColNormal, false},
		{"TS tag column", TSColTag, true},
		{"TS primary tag column", TSColPrimaryTag, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ColumnMeta{TSType: tt.tsType}
			got := cm.IsTag()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColumnMeta_IsNormalTag(t *testing.T) {
	tests := []struct {
		name   string
		tsType int
		want   bool
	}{
		{"normal column", ColNormal, false},
		{"TS normal column", TSColNormal, false},
		{"TS tag column", TSColTag, true},
		{"TS primary tag column", TSColPrimaryTag, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ColumnMeta{TSType: tt.tsType}
			got := cm.IsNormalTag()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColumnMeta_IsPrimaryTag(t *testing.T) {
	tests := []struct {
		name   string
		tsType int
		want   bool
	}{
		{"normal column", ColNormal, false},
		{"TS normal column", TSColNormal, false},
		{"TS tag column", TSColTag, false},
		{"TS primary tag column", TSColPrimaryTag, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ColumnMeta{TSType: tt.tsType}
			got := cm.IsPrimaryTag()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAddTSProperty(t *testing.T) {
	tests := []struct {
		name string
		prop int
		flag int
		want int
	}{
		{"add flag to zero property", 0, 1, 1},
		{"add flag to existing property", 1, 2, 3},
		{"add same flag multiple times", 1, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AddTSProperty(tt.prop, tt.flag)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDelTsProperty(t *testing.T) {
	tests := []struct {
		name string
		prop int
		flag int
		want int
	}{
		{"remove flag from property", 3, 1, 2},
		{"remove non-existent flag", 2, 1, 2},
		{"remove all flags", 3, 3, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DelTsProperty(tt.prop, tt.flag)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckTsProperty(t *testing.T) {
	tests := []struct {
		name string
		prop int
		flag int
		want bool
	}{
		{"flag present", 3, 1, true},
		{"flag not present", 2, 1, false},
		{"multiple flags", 3, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckTsProperty(tt.prop, tt.flag)
			require.Equal(t, tt.want, got)
		})
	}
}
