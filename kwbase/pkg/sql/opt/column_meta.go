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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// ColumnID uniquely identifies the usage of a column within the scope of a
// query. ColumnID 0 is reserved to mean "unknown column". See the comment for
// Metadata for more details.
type ColumnID int32

// ColumnUsage represents an access pattern for a column
type ColumnUsage struct {
	// Unique identifier of Column
	ID ColumnID
	// The accessed rank
	UsageType string
	// If the Usage Type is Sorting, then the sorting type for each column (ASC/DESC)
	Ordering string
	// If the Usage Type is not SORTING, it is the predicate identifier for each column,
	// pointing to the predicate table
	PredicateNo int32
}

// index returns the index of the column in Metadata.cols. It's biased by 1, so
// that ColumnID 0 can be be reserved to mean "unknown column".
func (c ColumnID) index() int {
	return int(c - 1)
}

// ColList is a list of column ids.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList []ColumnID

// ColIdxs is a list of column indexs.
type ColIdxs []int

// ColumnMeta stores information about one of the columns stored in the
// metadata.
type ColumnMeta struct {
	// MetaID is the identifier for this column that is unique within the query
	// metadata.
	MetaID ColumnID

	// Alias is the best-effort name of this column. Since the same column in a
	// query can have multiple names (using aliasing), one of those is chosen to
	// be used for pretty-printing and debugging. This might be different than
	// what is stored in the physical properties and is presented to end users.
	Alias string

	// Type is the scalar SQL type of this column.
	Type *types.T

	// Table is the base table to which this column belongs.
	// If the column was synthesized (i.e. no base table), then it is 0.
	Table TableID

	// Expr record the column typedExpr
	Expr tree.TypedExpr

	// TSType property of time series column(0: normal col; 1: tag col 2: primary tag)
	TSType int
}

// IsNormalCol identify this column as a normal column
func (c *ColumnMeta) IsNormalCol() bool {
	return c.TSType == ColNormal
}

// IsTag identify this column as a tag column
func (c *ColumnMeta) IsTag() bool {
	return c.TSType == TSColTag || c.TSType == TSColPrimaryTag
}

// IsNormalTag identify this column as a normal tag column
func (c *ColumnMeta) IsNormalTag() bool {
	return c.TSType == TSColTag
}

// IsPrimaryTag identify this column as a primary tag column.
func (c *ColumnMeta) IsPrimaryTag() bool {
	return c.TSType == TSColPrimaryTag
}

// property of time series column(0: normal col; 1: tag col 2: primary tag)
const (
	// ColNormal is for normal column in relational table
	ColNormal = 0
	// TSColNormal means normal column in time series table
	TSColNormal = 1
	// TSColTag means tag col in timeseries table
	TSColTag = 2
	// TSColPrimaryTag means primary tag col in timeseries table
	TSColPrimaryTag = 3
)

// ToSet converts a column id list to a column id set.
func (cl ColList) ToSet() ColSet {
	var r ColSet
	for _, col := range cl {
		r.Add(col)
	}
	return r
}

// Find searches for a column in the list and returns its index in the list (if
// successful).
func (cl ColList) Find(col ColumnID) (idx int, ok bool) {
	for i := range cl {
		if cl[i] == col {
			return i, true
		}
	}
	return -1, false
}

// Equals returns true if this column list has the same columns as the given
// column list, in the same order.
func (cl ColList) Equals(other ColList) bool {
	if len(cl) != len(other) {
		return false
	}
	for i := range cl {
		if cl[i] != other[i] {
			return false
		}
	}
	return true
}

// ColSetToList converts a column id set to a list, in column id order.
func ColSetToList(set ColSet) ColList {
	res := make(ColList, 0, set.Len())
	set.ForEach(func(x ColumnID) {
		res = append(res, x)
	})
	return res
}

// ColMap provides a 1:1 mapping from one column id to another. It is used by
// operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// AliasedColumn specifies the label and id of a column.
type AliasedColumn struct {
	Alias string
	ID    ColumnID

	Typ *types.T
}

// AddTSPropertyBool add flag to time series property
func AddTSPropertyBool(prop int, flag int) int {
	return DelTsProperty(prop, flag)
}

// AddTSProperty add flag to time series property
func AddTSProperty(prop int, flag int) int {
	return prop | flag
}

// DelTsProperty delete flag from time series property
func DelTsProperty(prop int, flag int) int {
	return prop &^ flag
}

// CheckTsProperty check whether flag is in prop.
func CheckTsProperty(prop int, flag int) bool {
	return prop&flag > 0
}

// VarNames is an array of user defined variables.
type VarNames []string
