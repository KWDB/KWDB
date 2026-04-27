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

package sqlbase_test

import (
	"context"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type intSlice []int

func (p intSlice) Len() int           { return len(p) }
func (p intSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p intSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type reverseIntSlice []int

func (p reverseIntSlice) Len() int           { return len(p) }
func (p reverseIntSlice) Less(i, j int) bool { return p[i] > p[j] }
func (p reverseIntSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type stringSlice []string

func (p stringSlice) Len() int           { return len(p) }
func (p stringSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p stringSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func TestSortEmpty(t *testing.T) {
	var data intSlice = nil
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if data != nil {
		t.Error("Expected nil slice after sorting")
	}
}

func TestSortEmptySlice(t *testing.T) {
	data := intSlice{}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if len(data) != 0 {
		t.Error("Expected empty slice after sorting")
	}
}

func TestSortSingleElement(t *testing.T) {
	data := intSlice{42}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if len(data) != 1 || data[0] != 42 {
		t.Error("Single element should remain unchanged")
	}
}

func TestSortTwoElements(t *testing.T) {
	data := intSlice{2, 1}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if data[0] != 1 || data[1] != 2 {
		t.Errorf("Expected [1 2], got %v", data)
	}
}

func TestSortAlreadySorted(t *testing.T) {
	data := intSlice{1, 2, 3, 4, 5}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Already sorted slice should remain sorted")
	}
}

func TestSortReverseOrder(t *testing.T) {
	data := intSlice{5, 4, 3, 2, 1}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestSortRandomOrder(t *testing.T) {
	data := intSlice{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestSortWithDuplicates(t *testing.T) {
	data := intSlice{3, 3, 3, 1, 1, 2, 2, 5, 5}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestSortAllDuplicates(t *testing.T) {
	data := intSlice{7, 7, 7, 7, 7}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("All duplicates should be sorted")
	}
}

func TestSortLargeSlice(t *testing.T) {
	data := make(intSlice, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = 999 - i
	}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Large slice should be sorted")
	}
}

func TestSortStrings(t *testing.T) {
	data := stringSlice{"banana", "apple", "cherry", "date"}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted strings, got %v", data)
	}
	if data[0] != "apple" || data[3] != "date" {
		t.Errorf("Expected [apple banana cherry date], got %v", data)
	}
}

func TestSortNegativeNumbers(t *testing.T) {
	data := intSlice{-5, 3, -2, 0, 7, -1, 4}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
	if data[0] != -5 || data[6] != 7 {
		t.Errorf("Expected [-5 -2 -1 0 3 4 7], got %v", data)
	}
}

func TestSortMaxDepth(t *testing.T) {
	data := intSlice{5, 2, 8, 1, 9, 3, 7, 4, 6}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Slice should be sorted")
	}
}

func TestStableSortEmpty(t *testing.T) {
	var data intSlice = nil
	sqlbase.StableSort(data)
	if data != nil {
		t.Error("Expected nil slice after sorting")
	}
}

func TestStableSortEmptySlice(t *testing.T) {
	data := intSlice{}
	sqlbase.StableSort(data)
	if len(data) != 0 {
		t.Error("Expected empty slice after sorting")
	}
}

func TestStableSortSingleElement(t *testing.T) {
	data := intSlice{42}
	sqlbase.StableSort(data)
	if len(data) != 1 || data[0] != 42 {
		t.Error("Single element should remain unchanged")
	}
}

func TestStableSortTwoElements(t *testing.T) {
	data := intSlice{2, 1}
	sqlbase.StableSort(data)
	if data[0] != 1 || data[1] != 2 {
		t.Errorf("Expected [1 2], got %v", data)
	}
}

func TestStableSortAlreadySorted(t *testing.T) {
	data := intSlice{1, 2, 3, 4, 5}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Error("Already sorted slice should remain sorted")
	}
}

func TestStableSortReverseOrder(t *testing.T) {
	data := intSlice{5, 4, 3, 2, 1}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestStableSortRandomOrder(t *testing.T) {
	data := intSlice{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestStableSortWithDuplicates(t *testing.T) {
	data := intSlice{3, 3, 3, 1, 1, 2, 2, 5, 5}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestStableSortLargeSlice(t *testing.T) {
	data := make(intSlice, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = 999 - i
	}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Error("Large slice should be sorted")
	}
}

func TestStableSortStrings(t *testing.T) {
	data := stringSlice{"banana", "apple", "cherry", "date"}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted strings, got %v", data)
	}
}

func TestSortWithCancelCheckerNotCancelled(t *testing.T) {
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	data := intSlice{5, 2, 8, 1, 9, 3}
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Slice should be sorted when cancel checker is not canceled")
	}
}

func TestSortBoundary12Elements(t *testing.T) {
	data := intSlice{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("12 elements should be sorted")
	}
}

func TestSortBoundary13Elements(t *testing.T) {
	data := intSlice{13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("13 elements should be sorted")
	}
}

func TestStableSortBoundaryCases(t *testing.T) {
	for _, n := range []int{1, 2, 19, 20, 21, 39, 40, 41} {
		data := make(intSlice, n)
		for i := 0; i < n; i++ {
			data[i] = n - 1 - i
		}
		sqlbase.StableSort(data)
		if !sort.IsSorted(data) {
			t.Errorf("%d elements should be sorted", n)
		}
	}
}

func TestSortSliceSize40(t *testing.T) {
	data := make(intSlice, 40)
	for i := 0; i < 40; i++ {
		data[i] = 39 - i
	}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("40 elements should be sorted")
	}
}

func TestSortVerySmallSlice(t *testing.T) {
	data := intSlice{1}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Single element should be sorted")
	}
}

func TestStableSortWithNegatives(t *testing.T) {
	data := intSlice{-5, 3, -2, 0, 7, -1, 4}
	sqlbase.StableSort(data)
	if !sort.IsSorted(data) {
		t.Errorf("Expected sorted, got %v", data)
	}
}

func TestSortManyDuplicates(t *testing.T) {
	data := intSlice{1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Many duplicates should be sorted")
	}
}

func TestSortSortedWithCancelChecker(t *testing.T) {
	data := intSlice{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(data, cancelChecker)
	if !sort.IsSorted(data) {
		t.Error("Already sorted slice should remain sorted with cancel checker")
	}
}
