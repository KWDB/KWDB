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

package cat

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// Column is an interface to a table column, exposing only the information
// needed by the query optimizer.
type Column interface {
	// ColID is the unique, stable identifier for this column within its table.
	// Each new column in the table will be assigned a new ID that is different
	// than every column allocated before or after. This is true even if a column
	// is dropped and then re-added with the same name; the new column will have
	// a different ID. See the comment for StableID for more detail.
	ColID() StableID

	// ColName returns the name of the column.
	ColName() tree.Name

	// DatumType returns the data type of the column.
	DatumType() *types.T

	// ColTypePrecision returns the precision of the column's SQL data type. This
	// is only defined for the Decimal data type and represents the max number of
	// decimal digits in the decimal (including fractional digits). If precision
	// is 0, then the decimal has no max precision.
	ColTypePrecision() int

	// ColTypeWidth returns the width of the column's SQL data type. This has
	// different meanings depending on the data type:
	//
	//   Decimal  : scale
	//   Int      : # bits (16, 32, 64, etc)
	//   Bit Array: # bits
	//   String   : rune count
	//
	// TODO(andyk): Switch calling code to use DatumType.
	ColTypeWidth() int

	// ColTypeStr returns the SQL data type of the column, as a string. Note that
	// this is sometimes different than DatumType().String(), since datum types
	// are a subset of column types.
	ColTypeStr() string

	// IsNullable returns true if the column is nullable.
	IsNullable() bool

	// IsTagCol returns true if the column is a tag.
	IsTagCol() bool

	// IsOrdinaryTagCol returns true if the column is an ordinary tag.
	IsOrdinaryTagCol() bool

	// TsColStorgeLen is part of the cat.Column interface.
	TsColStorgeLen() uint64

	// IsPrimaryTagCol returns true if the column is a primary tag.
	IsPrimaryTagCol() bool

	// IsHidden returns true if the column is hidden (e.g., there is always a
	// hidden column called rowid if there is no primary key on the table).
	IsHidden() bool

	// HasDefault returns true if the column has a default value. DefaultExprStr
	// will be set to the SQL expression string in that case.
	HasDefault() bool

	// DefaultExprStr is set to the SQL expression string that describes the
	// column's default value. It is used when the user does not provide a value
	// for the column when inserting a row. Default values cannot depend on other
	// columns.
	DefaultExprStr() string

	// IsComputed returns true if the column is a computed value. ComputedExprStr
	// will be set to the SQL expression string in that case.
	IsComputed() bool

	// ComputedExprStr is set to the SQL expression string that describes the
	// column's computed value. It is always used to provide the column's value
	// when inserting or updating a row. Computed values cannot depend on other
	// computed columns, but they can depend on all other columns, including
	// columns with default values.
	ComputedExprStr() string
}

// IsMutationColumn is a convenience function that returns true if the column at
// the given ordinal position is a mutation column.
func IsMutationColumn(table Table, ord int) bool {
	return ord >= table.ColumnCount()
}
