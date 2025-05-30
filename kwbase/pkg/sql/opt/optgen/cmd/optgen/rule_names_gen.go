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

package main

import (
	"fmt"
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/lang"
)

// ruleNamesGen generates an enumeration containing the names of all the rules.
type ruleNamesGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
	unique   map[lang.StringExpr]struct{}
}

func (g *ruleNamesGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w
	g.unique = make(map[lang.StringExpr]struct{})

	fmt.Fprintf(g.w, "package opt\n\n")

	fmt.Fprintf(g.w, "const (\n")
	fmt.Fprintf(g.w, "  startAutoRule RuleName = iota + NumManualRuleNames\n\n")

	g.genRuleNameEnumByTag("Normalize")
	fmt.Fprintf(g.w, "  // startExploreRule tracks the number of normalization rules;\n")
	fmt.Fprintf(g.w, "  // all rules greater than this value are exploration rules.\n")
	fmt.Fprintf(g.w, "  startExploreRule\n\n")
	g.genRuleNameEnumByTag("Explore")

	fmt.Fprintf(g.w, "  // NumRuleNames tracks the total count of rule names.\n")
	fmt.Fprintf(g.w, "  NumRuleNames\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *ruleNamesGen) genRuleNameEnumByTag(tag string) {
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	fmt.Fprintf(g.w, "  // %s Rule Names\n", tag)
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	for _, rule := range g.compiled.Rules {
		if !rule.Tags.Contains(tag) {
			continue
		}

		// Only include each unique rule name once, not once per operator.
		if _, ok := g.unique[rule.Name]; ok {
			continue
		}
		g.unique[rule.Name] = struct{}{}

		fmt.Fprintf(g.w, "  %s\n", rule.Name)
	}
	fmt.Fprintf(g.w, "\n")
}
