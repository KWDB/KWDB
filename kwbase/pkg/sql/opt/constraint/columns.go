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

package constraint

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
)

// Columns identifies the columns which correspond to the values in a Key (and
// consequently the columns of a Span or Constraint).
//
// The columns have directions; a descending column inverts the order of the
// values on that column (in other words, inverts the result of any Datum
// comparisons on that column).
//
// The internal representation optimizes for the common case of a single-column
// constraint by storing the first column inline and the rest in a slice.
type Columns struct {
	// firstCol holds the first column id and otherCols hold any ids beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-column constraint.
	firstCol  opt.OrderingColumn
	otherCols []opt.OrderingColumn
}

// Init initializes the Columns structure with a slice of ordering columns.
// The first element of cols is stored inline in firstCol for efficiency;
// remaining elements are stored in otherCols via slice aliasing.
func (c *Columns) Init(cols []opt.OrderingColumn) {
	c.firstCol = cols[0]
	c.otherCols = cols[1:]
}

// InitSingle is a more efficient version of Init for the common case of a
// single column. It avoids allocating the otherCols slice.
func (c *Columns) InitSingle(col opt.OrderingColumn) {
	c.firstCol = col
	c.otherCols = nil
}

var _ = (*Columns).InitSingle

// Count returns the number of constrained columns (always at least one).
func (c *Columns) Count() int {
	// There's always at least one column (firstCol).
	return 1 + len(c.otherCols)
}

// Get returns the nth column and direction. Together with the
// Count method, Get allows iteration over the list of constrained
// columns (since there is no method to return a slice of columns).
func (c *Columns) Get(nth int) opt.OrderingColumn {
	// There's always at least one column.
	if nth == 0 {
		return c.firstCol
	}
	return c.otherCols[nth-1]
}

// Equals returns true if the two lists of columns are identical.
func (c *Columns) Equals(other *Columns) bool {
	if !c.hasSameFirstAndCount(other) {
		return false
	}
	// Fast path: single column or shared underlying slice.
	if c.otherColsAreAliased(other, 0) {
		return true
	}
	return c.otherColsMatch(other.otherCols)
}

// IsStrictSuffixOf returns true if the columns in c are a strict suffix of the
// columns in other. For example, columns [2,3] are a strict suffix of [1,2,3],
// but columns [2,3] are not a suffix of [1,2] or [2,3].
func (c *Columns) IsStrictSuffixOf(other *Columns) bool {
	offset := other.Count() - c.Count()
	if offset <= 0 {
		return false
	}
	if c.firstCol != other.otherCols[offset-1] {
		return false
	}
	// Fast path when the slices are aliased (shared underlying array).
	if c.otherColsAreAliased(other, offset) {
		return true
	}
	return c.otherColsMatch(other.otherCols[offset:])
}

// ColSet returns the columns as a ColSet for set operations.
func (c *Columns) ColSet() opt.ColSet {
	return c.collectIntoColSet()
}

func (c Columns) String() string {
	var b strings.Builder

	for i := 0; i < c.Count(); i++ {
		b.WriteRune('/')
		b.WriteString(fmt.Sprintf("%d", c.Get(i)))
	}
	return b.String()
}

// InitFirst initializes firstCol of Columns, only use in make TSSpan.
func (c *Columns) InitFirst() {
	c.firstCol = 1
}

// -------- internal helpers --------

// hasSameFirstAndCount checks whether two Columns have the same first column
// and the same total number of columns. Used as a quick equality pre-check
// before comparing the remaining columns element-by-element.
func (c *Columns) hasSameFirstAndCount(other *Columns) bool {
	return c.Count() == other.Count() && c.firstCol == other.firstCol
}

// otherColsAreAliased returns true if c.otherCols and the slice starting at
// offset in other.otherCols share the same underlying array, or if c has no
// otherCols (single-column case). This provides an efficient short-cut for
// column comparison.
func (c *Columns) otherColsAreAliased(other *Columns, offset int) bool {
	n := c.Count()
	if n == 1 {
		return true
	}
	if len(c.otherCols) == 0 {
		return true
	}
	return &c.otherCols[0] == &other.otherCols[offset]
}

// otherColsMatch compares c.otherCols element-by-element with the given slice.
// The caller must ensure the slices have the same length. A bounds-check
// elimination hint is provided by slicing cmpCols to exactly len(c.otherCols).
func (c *Columns) otherColsMatch(cmpCols []opt.OrderingColumn) bool {
	// Hint for the compiler to eliminate bounds check inside the loop.
	cmpCols = cmpCols[:len(c.otherCols)]
	for i, v := range c.otherCols {
		if v != cmpCols[i] {
			return false
		}
	}
	return true
}

// collectIntoColSet accumulates all column IDs from this Columns into a single
// ColSet.
func (c *Columns) collectIntoColSet() opt.ColSet {
	var r opt.ColSet
	r.Add(c.firstCol.ID())
	for _, col := range c.otherCols {
		r.Add(col.ID())
	}
	return r
}
