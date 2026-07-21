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

package physical

import (
	"bytes"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
)

// ============================================================================
// Required
// ============================================================================

// Required properties are interesting characteristics of an expression that
// impact its layout, presentation, or location, but not its logical content.
// Examples include row order, column naming, and data distribution (physical
// location of data ranges). Physical properties exist outside of the relational
// algebra, and arise from both the SQL query itself (e.g. the non-relational
// ORDER BY operator) and by the selection of specific implementations during
// optimization (e.g. a merge join requires the inputs to be sorted in a
// particular order).
//
// Required properties are derived top-to-bottom - there is a required physical
// property on the root, and each expression can require physical properties on
// one or more of its operands. When an expression is optimized, it is always
// with respect to a particular set of required physical properties. The goal
// is to find the lowest cost expression that provides those properties while
// still remaining logically equivalent.
type Required struct {
	// Presentation specifies the naming, membership (including duplicates),
	// and order of result columns. If Presentation is not defined, then no
	// particular column presentation is required or provided.
	Presentation Presentation

	// Ordering specifies the sort order of result rows. Rows can be sorted by
	// one or more columns, each of which can be sorted in either ascending or
	// descending order. If Ordering is not defined, then no particular ordering
	// is required or provided.
	Ordering OrderingChoice

	// LimitHint specifies a "soft limit" to the number of result rows that may
	// be required of the expression. If requested, an expression will still need
	// to return all result rows, but it can be optimized based on the assumption
	// that only the hinted number of rows will be needed.
	// A LimitHint of 0 indicates "no limit". The LimitHint is an intermediate
	// float64 representation, and can be converted to an integer number of rows
	// using math.Ceil.
	LimitHint float64

	// MustAddSort is true when a sort node must be added regardless of
	// other properties.
	MustAddSort bool
}

// MinRequired are the default physical properties that require nothing and
// provide nothing.
var MinRequired = &Required{}

// Defined is true if any physical property is defined. If none is defined, then
// this is an instance of MinRequired.
func (p *Required) Defined() bool {
	return !p.Presentation.Any() || !p.Ordering.Any() || p.LimitHint != 0
}

// ColSet returns the set of columns used by any of the physical properties.
func (p *Required) ColSet() opt.ColSet {
	colSet := p.Ordering.ColSet()
	for _, col := range p.Presentation {
		colSet.Add(col.ID)
	}
	return colSet
}

// Equals returns true if the two physical properties are identical.
func (p *Required) Equals(rhs *Required) bool {
	return p.Presentation.Equals(rhs.Presentation) &&
		p.Ordering.Equals(&rhs.Ordering) &&
		p.LimitHint == rhs.LimitHint
}

// String returns a compact human-readable summary of the required properties.
// Example: "[presentation: a:1,b:2] [ordering: +1,+5] [limit hint: 10.00]"
func (p *Required) String() string {
	return formatRequiredString(p)
}

// ============================================================================
// Presentation
// ============================================================================

// Presentation specifies the naming, membership (including duplicates), and
// order of result columns that are required of or provided by an operator.
// While it cannot add unique columns, Presentation can rename, reorder,
// duplicate and discard columns. If Presentation is not defined, then no
// particular column presentation is required or provided. For example:
//
//	a.y:2 a.x:1 a.y:2 column1:3
type Presentation []opt.AliasedColumn

// Any is true if any column presentation is allowed or can be provided.
func (p Presentation) Any() bool {
	return p == nil
}

// Equals returns true iff this presentation exactly matches the given
// presentation.
func (p Presentation) Equals(rhs Presentation) bool {
	// The 0 column presentation is not the same as the nil presentation.
	if p.Any() != rhs.Any() {
		return false
	}
	if len(p) != len(rhs) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] != rhs[i] {
			return false
		}
	}
	return true
}

// String returns "a:1,b:2" style representation.
func (p Presentation) String() string {
	var buf bytes.Buffer
	p.format(&buf)
	return buf.String()
}

// format writes each "alias:id" pair separated by commas.
func (p Presentation) format(buf *bytes.Buffer) {
	for i, col := range p {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(buf, "%s:%d", col.Alias, col.ID)
	}
}

// ============================================================================
// String formatting helpers (placed at end to keep exported API near the top)
// ============================================================================

// formatRequiredString produces the bracketed human-readable representation of
// a Required properties set. It uses a local output helper to avoid repetition
// across the three property kinds (presentation / ordering / limit hint).
func formatRequiredString(p *Required) string {
	var buf bytes.Buffer

	// output is a small helper that writes a named property section.
	output := func(name string, fn func(*bytes.Buffer)) {
		if buf.Len() != 0 {
			buf.WriteByte(' ')
		}
		buf.WriteByte('[')
		buf.WriteString(name)
		buf.WriteString(": ")
		fn(&buf)
		buf.WriteByte(']')
	}

	if !p.Presentation.Any() {
		output("presentation", p.Presentation.format)
	}
	if !p.Ordering.Any() {
		output("ordering", p.Ordering.Format)
	}
	if p.LimitHint != 0 {
		output("limit hint", func(buf *bytes.Buffer) { fmt.Fprintf(buf, "%.2f", p.LimitHint) })
	}

	// Handle empty properties case.
	if buf.Len() == 0 {
		return "[]"
	}
	return buf.String()
}
