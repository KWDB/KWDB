// Copyright 2019 The Cockroach Authors.
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

package memo

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

// ExprNameGenerator is used to generate a unique name for each relational
// expression in a query tree. See GenerateName for details.
type ExprNameGenerator struct {
	prefix    string
	exprCount int
}

// NewExprNameGenerator creates a new instance of ExprNameGenerator,
// initialized with the given prefix.
func NewExprNameGenerator(prefix string) *ExprNameGenerator {
	return &ExprNameGenerator{prefix: prefix}
}

// GenerateName generates a name for a relational expression with the given
// operator. It is used to generate names for each relational expression
// in a query tree, corresponding to the tables that will be created if the
// session variable `save_tables_prefix` is non-empty.
//
// Each invocation of GenerateName is guaranteed to produce a unique name for
// a given instance of ExprNameGenerator. This works because each name is
// appended with a unique, auto-incrementing number. For readability, the
// generated names also contain a common prefix and the name of the relational
// operator separated with underscores. For example: my_query_scan_2.
//
// Since the names are generated with an auto-incrementing number, the order
// of invocation is important. For a given query, the number assigned to each
// relational subexpression corresponds to the order in which the expression
// was encountered during tree traversal. Thus, in order to generate a
// consistent name, always call GenerateName in a pre-order traversal of the
// expression tree.
//
func (g *ExprNameGenerator) GenerateName(op opt.Operator) string {
	// Replace all instances of "-" in the operator name with "_" in order to
	// create a legal table name.
	operator := strings.Replace(op.String(), "-", "_", -1)
	g.exprCount++
	return fmt.Sprintf("%s_%s_%d", g.prefix, operator, g.exprCount)
}

// ColumnNameGenerator is used to generate a unique name for each column of a
// relational expression. See GenerateName for details.
type ColumnNameGenerator struct {
	e    RelExpr
	pres physical.Presentation
	seen map[string]int
}

// NewColumnNameGenerator creates a new instance of ColumnNameGenerator,
// initialized with the given relational expression.
func NewColumnNameGenerator(e RelExpr) *ColumnNameGenerator {
	return &ColumnNameGenerator{
		e:    e,
		pres: e.RequiredPhysical().Presentation,
		seen: make(map[string]int, e.Relational().OutputCols.Len()),
	}
}

// GenerateName generates a unique name for each column in a relational
// expression. This function is used to generate consistent, unique names
// for the columns in the table that will be created if the session
// variable `save_tables_prefix` is non-empty.
func (g *ColumnNameGenerator) GenerateName(col opt.ColumnID) string {
	colMeta := g.e.Memo().Metadata().ColumnMeta(col)
	colName := colMeta.Alias

	// Check whether the presentation has a different name for this column, and
	// use it if available.
	for i := range g.pres {
		if g.pres[i].ID == col {
			colName = g.pres[i].Alias
			break
		}
	}

	// Every column name must be unique.
	if cnt, ok := g.seen[colName]; ok {
		g.seen[colName]++
		colName = fmt.Sprintf("%s_%d", colName, cnt)
	} else {
		g.seen[colName] = 1
	}

	return colName
}