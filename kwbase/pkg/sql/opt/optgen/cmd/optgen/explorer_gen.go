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
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/lang"
)

// explorerGen generates code for the explorer, which searches for logically
// equivalent expressions and adds them to the memo.
type explorerGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        *matchWriter
	ruleGen  newRuleGen
}

func (g *explorerGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "xform")
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.md, g.w)

	g.w.writeIndent("package xform\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"gitee.com/kwbasedb/kwbase/pkg/sql/opt\"\n")
	g.w.writeIndent("\"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree\"\n")
	g.w.unnest(")\n\n")

	g.genDispatcher()
	g.genRuleFuncs()
}

// genDispatcher generates a switch statement that calls an exploration method
// for each define statement that has an explore rule defined. The code is
// similar to this:
//
//	func (_e *explorer) exploreGroupMember(
//	  state *exploreState,
//	  member memo.RelExpr,
//	  ordinal int,
//	) (_fullyExplored bool) {
//	  switch t := member.(type) {
//	    case *memo.ScanNode:
//	      return _e.exploreScan(state, t, ordinal)
//	    case *memo.SelectNode:
//	      return _e.exploreSelect(state, t, ordinal)
//	  }
//
//	  // No rules for other operator types.
//	  return true
//	}
func (g *explorerGen) genDispatcher() {
	g.w.nestIndent("func (_e *explorer) exploreGroupMember(\n")
	g.w.writeIndent("state *exploreState,\n")
	g.w.writeIndent("member memo.RelExpr,\n")
	g.w.writeIndent("ordinal int,\n")
	g.w.unnest(") (_fullyExplored bool)")
	g.w.nest(" {\n")
	g.w.writeIndent("switch t := member.(type) {\n")

	for _, define := range g.compiled.Defines {
		// Only include exploration rules.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Explore")
		if len(rules) > 0 {
			opTyp := g.md.typeOf(define)
			format := ""
			format = "case *%s: return _e.explore%s(state, t, ordinal)\n"

			g.w.writeIndent(format, opTyp.name, define.Name)
		}
	}

	g.w.writeIndent("}\n\n")
	g.w.writeIndent("// No rules for other operator types.\n")
	g.w.writeIndent("return true\n")
	g.w.unnest("}\n\n")
}

// genRuleFuncs generates a method for each operator that has at least one
// explore rule defined. The code is similar to this:
//
//	func (_e *explorer) exploreScan(
//	  _rootState *exploreState,
//	  _root *memo.ScanNode,
//	  _rootOrd int,
//	) (_fullyExplored bool) {
//	  _fullyExplored = true
//
//	  ... exploration rule code goes here ...
//
//	  return _fullyExplored
//	}
func (g *explorerGen) genRuleFuncs() {
	for _, define := range g.compiled.Defines {
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Explore")
		if len(rules) == 0 {
			continue
		}

		opTyp := g.md.typeOf(define)

		g.w.nestIndent("func (_e *explorer) explore%s(\n", define.Name)
		g.w.writeIndent("_rootState *exploreState,\n")
		g.w.writeIndent("_root *%s,\n", opTyp.name)
		g.w.writeIndent("_rootOrd int,\n")
		g.w.unnest(") (_fullyExplored bool)")
		g.w.nest(" {\n")
		g.w.writeIndent("_fullyExplored = true\n\n")

		sortRulesByPriority(rules)
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}

		g.w.writeIndent("return _fullyExplored\n")
		g.w.unnest("}\n\n")
	}
}
