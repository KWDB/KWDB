// Copyright 2019 The Cockroach Authors.
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

package opt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFastSet_Add(t *testing.T) {
	tests := []struct {
		name     string
		initial  FastSet
		toAdd    int
		expected FastSet
	}{
		{"add to empty set", FastSet{}, 1, FastSet{small: 1 << 1}},
		{"add existing element", FastSet{small: 1 << 1}, 1, FastSet{small: 1 << 1}},
		{"add multiple elements", FastSet{small: 1 << 1}, 2, FastSet{small: (1 << 1) | (1 << 2)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial
			got.Add(tt.toAdd)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Remove(t *testing.T) {
	tests := []struct {
		name     string
		initial  FastSet
		toRemove int
		expected FastSet
	}{
		{"remove from empty set", FastSet{}, 1, FastSet{}},
		{"remove existing element", FastSet{small: 1 << 1}, 1, FastSet{}},
		{"remove non-existing element", FastSet{small: 1 << 1}, 2, FastSet{small: 1 << 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial
			got.Remove(tt.toRemove)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Contains(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		toCheck  int
		expected bool
	}{
		{"empty set", FastSet{}, 1, false},
		{"element exists", FastSet{small: 1 << 1}, 1, true},
		{"element does not exist", FastSet{small: 1 << 1}, 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Contains(tt.toCheck)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Empty(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		expected bool
	}{
		{"empty set", FastSet{}, true},
		{"non-empty set", FastSet{small: 1 << 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Empty()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Len(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		expected int
	}{
		{"empty set", FastSet{}, 0},
		{"single element", FastSet{small: 1 << 1}, 1},
		{"multiple elements", FastSet{small: (1 << 1) | (1 << 2) | (1 << 3)}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Len()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Next(t *testing.T) {
	tests := []struct {
		name       string
		set        FastSet
		startVal   int
		expected   int
		expectedOk bool
	}{
		{"empty set", FastSet{}, 0, 9223372036854775807, false},
		{"start before first element", FastSet{small: 1 << 1}, 0, 1, true},
		{"start at first element", FastSet{small: 1 << 1}, 1, 1, true},
		{"start after last element", FastSet{small: 1 << 1}, 2, 9223372036854775807, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := tt.set.Next(tt.startVal)
			require.Equal(t, tt.expected, got)
			require.Equal(t, tt.expectedOk, gotOk)
		})
	}
}

func TestFastSet_ForEach(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		expected []int
	}{
		{"single element", FastSet{small: 1 << 1}, []int{1}},
		{"multiple elements", FastSet{small: (1 << 1) | (1 << 2) | (1 << 3)}, []int{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int
			tt.set.ForEach(func(i int) {
				got = append(got, i)
			})
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Ordered(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		expected []int
	}{
		{"empty set", FastSet{}, nil},
		{"single element", FastSet{small: 1 << 1}, []int{1}},
		{"multiple elements", FastSet{small: (1 << 3) | (1 << 1) | (1 << 2)}, []int{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Ordered()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Copy(t *testing.T) {
	tests := []struct {
		name string
		set  FastSet
	}{
		{"empty set", FastSet{}},
		{"non-empty set", FastSet{small: 1 << 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Copy()
			require.Equal(t, tt.set, got)
			// Modify the copy to ensure it's independent
			got.Add(2)
			require.NotEqual(t, tt.set, got)
		})
	}
}

func TestFastSet_UnionWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{small: 1 << 1}},
		{"non-empty sets", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, FastSet{small: (1 << 1) | (1 << 2)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.UnionWith(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Union(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{small: 1 << 1}},
		{"non-empty sets", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, FastSet{small: (1 << 1) | (1 << 2)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Union(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_IntersectionWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{}},
		{"non-empty sets with intersection", FastSet{small: (1 << 1) | (1 << 2)}, FastSet{small: (1 << 2) | (1 << 3)}, FastSet{small: 1 << 2}},
		{"non-empty sets without intersection", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, FastSet{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.IntersectionWith(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Intersection(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{}},
		{"non-empty sets with intersection", FastSet{small: (1 << 1) | (1 << 2)}, FastSet{small: (1 << 2) | (1 << 3)}, FastSet{small: 1 << 2}},
		{"non-empty sets without intersection", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, FastSet{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Intersection(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Intersects(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected bool
	}{
		{"empty sets", FastSet{}, FastSet{}, false},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, false},
		{"non-empty sets with intersection", FastSet{small: (1 << 1) | (1 << 2)}, FastSet{small: (1 << 2) | (1 << 3)}, true},
		{"non-empty sets without intersection", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Intersects(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_DifferenceWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{}},
		{"non-empty sets", FastSet{small: (1 << 1) | (1 << 2) | (1 << 3)}, FastSet{small: (1 << 2) | (1 << 4)}, FastSet{small: (1 << 1) | (1 << 3)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.DifferenceWith(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Difference(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected FastSet
	}{
		{"empty sets", FastSet{}, FastSet{}, FastSet{}},
		{"empty and non-empty", FastSet{}, FastSet{small: 1 << 1}, FastSet{}},
		{"non-empty sets", FastSet{small: (1 << 1) | (1 << 2) | (1 << 3)}, FastSet{small: (1 << 2) | (1 << 4)}, FastSet{small: (1 << 1) | (1 << 3)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Difference(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_Equals(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected bool
	}{
		{"empty sets", FastSet{}, FastSet{}, true},
		{"same non-empty sets", FastSet{small: (1 << 1) | (1 << 2)}, FastSet{small: (1 << 1) | (1 << 2)}, true},
		{"different non-empty sets", FastSet{small: 1 << 1}, FastSet{small: 1 << 2}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Equals(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_SubsetOf(t *testing.T) {
	tests := []struct {
		name     string
		set1     FastSet
		set2     FastSet
		expected bool
	}{
		{"empty set subset of empty set", FastSet{}, FastSet{}, true},
		{"empty set subset of non-empty set", FastSet{}, FastSet{small: 1 << 1}, true},
		{"non-empty set subset of same set", FastSet{small: 1 << 1}, FastSet{small: 1 << 1}, true},
		{"non-empty set subset of superset", FastSet{small: 1 << 1}, FastSet{small: (1 << 1) | (1 << 2)}, true},
		{"non-empty set not subset of smaller set", FastSet{small: (1 << 1) | (1 << 2)}, FastSet{small: 1 << 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.SubsetOf(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFastSet_String(t *testing.T) {
	tests := []struct {
		name     string
		set      FastSet
		expected string
	}{
		{"empty set", FastSet{}, "()"},
		{"single element", FastSet{small: 1 << 1}, "(1)"},
		{"multiple elements", FastSet{small: (1 << 1) | (1 << 2) | (1 << 3)}, "(1-3)"},
		{"non-consecutive elements", FastSet{small: (1 << 1) | (1 << 3)}, "(1,3)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.String()
			require.Equal(t, tt.expected, got)
		})
	}
}
