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

func TestIntSet_Add(t *testing.T) {
	tests := []struct {
		name     string
		initial  *IntSet
		toAdd    int
		expected *IntSet
	}{
		{"add to empty set", &IntSet{}, 1, func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
		{"add existing element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 1, func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
		{"add multiple elements", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 2, func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial
			got.Add(tt.toAdd)
			require.True(t, got.Equals(tt.expected), "IntSet.Add(%d) failed: got %v, expected %v", tt.toAdd, got, tt.expected)
		})
	}
}

func TestIntSet_Remove(t *testing.T) {
	tests := []struct {
		name     string
		initial  *IntSet
		toRemove int
		expected *IntSet
	}{
		{"remove from empty set", &IntSet{}, 1, &IntSet{}},
		{"remove existing element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 1, &IntSet{}},
		{"remove non-existing element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 2, func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial
			got.Remove(tt.toRemove)
			require.True(t, got.Equals(tt.expected), "IntSet.Remove(%d) failed: got %v, expected %v", tt.toRemove, got, tt.expected)
		})
	}
}

func TestIntSet_Contains(t *testing.T) {
	tests := []struct {
		name     string
		set      *IntSet
		toCheck  int
		expected bool
	}{
		{"empty set", &IntSet{}, 1, false},
		{"element exists", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 1, true},
		{"element does not exist", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Contains(tt.toCheck)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_Empty(t *testing.T) {
	tests := []struct {
		name     string
		set      *IntSet
		expected bool
	}{
		{"empty set", &IntSet{}, true},
		{"non-empty set", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Empty()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_Len(t *testing.T) {
	tests := []struct {
		name     string
		set      *IntSet
		expected int
	}{
		{"empty set", &IntSet{}, 0},
		{"single element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 1},
		{"multiple elements", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); s.Add(3); return s }(), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Len()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_Next(t *testing.T) {
	tests := []struct {
		name       string
		set        *IntSet
		startVal   int
		expected   int
		expectedOk bool
	}{
		{"empty set", &IntSet{}, 0, 9223372036854775807, false},
		{"start before first element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 0, 1, true},
		{"start at first element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 1, 1, true},
		{"start after last element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), 2, 9223372036854775807, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := tt.set.Next(tt.startVal)
			require.Equal(t, tt.expected, got)
			require.Equal(t, tt.expectedOk, gotOk)
		})
	}
}

func TestIntSet_ForEach(t *testing.T) {
	tests := []struct {
		name     string
		set      *IntSet
		expected []int
	}{
		{"single element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), []int{1}},
		{"multiple elements", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); s.Add(3); return s }(), []int{1, 2, 3}},
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

func TestIntSet_Ordered(t *testing.T) {
	tests := []struct {
		name     string
		set      *IntSet
		expected []int
	}{
		{"empty set", &IntSet{}, nil},
		{"single element", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), []int{1}},
		{"multiple elements", func() *IntSet { s := &IntSet{}; s.Add(3); s.Add(1); s.Add(2); return s }(), []int{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.Ordered()
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_Copy(t *testing.T) {
	tests := []struct {
		name string
		set  *IntSet
	}{
		{"non-empty set", func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
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

func TestIntSet_UnionWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
		{"non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.UnionWith(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.UnionWith failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_Union(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); return s }()},
		{"non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Union(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.Union failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_IntersectionWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), &IntSet{}},
		{"non-empty sets with intersection", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); s.Add(3); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }()},
		{"non-empty sets without intersection", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), &IntSet{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.IntersectionWith(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.IntersectionWith failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_Intersection(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), &IntSet{}},
		{"non-empty sets with intersection", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); s.Add(3); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }()},
		{"non-empty sets without intersection", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), &IntSet{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Intersection(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.Intersection failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_Intersects(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected bool
	}{
		{"empty sets", &IntSet{}, &IntSet{}, false},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), false},
		{"non-empty sets with intersection", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); s.Add(3); return s }(), true},
		{"non-empty sets without intersection", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Intersects(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_DifferenceWith(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), &IntSet{}},
		{"non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); s.Add(3); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); s.Add(4); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(3); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1
			got.DifferenceWith(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.DifferenceWith failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_Difference(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected *IntSet
	}{
		{"empty sets", &IntSet{}, &IntSet{}, &IntSet{}},
		{"empty and non-empty", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), &IntSet{}},
		{"non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); s.Add(3); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); s.Add(4); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(3); return s }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Difference(tt.set2)
			require.True(t, got.Equals(tt.expected), "IntSet.Difference failed: got %v, expected %v", got, tt.expected)
		})
	}
}

func TestIntSet_Equals(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected bool
	}{
		{"empty sets", &IntSet{}, &IntSet{}, true},
		{"same non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), true},
		{"different non-empty sets", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(2); return s }(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.Equals(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIntSet_SubsetOf(t *testing.T) {
	tests := []struct {
		name     string
		set1     *IntSet
		set2     *IntSet
		expected bool
	}{
		{"empty set subset of empty set", &IntSet{}, &IntSet{}, true},
		{"empty set subset of non-empty set", &IntSet{}, func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), true},
		{"non-empty set subset of same set", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), true},
		{"non-empty set subset of superset", func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), true},
		{"non-empty set not subset of smaller set", func() *IntSet { s := &IntSet{}; s.Add(1); s.Add(2); return s }(), func() *IntSet { s := &IntSet{}; s.Add(1); return s }(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set1.SubsetOf(tt.set2)
			require.Equal(t, tt.expected, got)
		})
	}
}
