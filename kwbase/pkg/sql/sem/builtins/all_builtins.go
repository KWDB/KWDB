// Copyright 2017 The Cockroach Authors.
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

package builtins

import (
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// AllBuiltinNames is an array containing all the built-in function
// names, sorted in alphabetical order. This can be used for a
// deterministic walk through the Builtins map.
var AllBuiltinNames []string

// AllAggregateBuiltinNames is an array containing the subset of
// AllBuiltinNames that corresponds to aggregate functions.
var AllAggregateBuiltinNames []string

// AllWindowBuiltinNames is an array containing the subset of
// AllBuiltinNames that corresponds to window functions.
var AllWindowBuiltinNames []string

func init() {
	initializeBuiltinSubtypes()
	registerAllBuiltinFunctions()
	populateMissingCategories()
	sortBuiltinNameArrays()
}

// initializeBuiltinSubtypes calls the initialization routines for each builtin
// category: aggregates, window functions, generators, and PG-compat functions.
func initializeBuiltinSubtypes() {
	initAggregateBuiltins()
	initWindowBuiltins()
	initGeneratorBuiltins()
	initPGBuiltins()
	initPubAndSubBuiltins()
}

// registerAllBuiltinFunctions iterates over the builtins map, creates
// tree.FunctionDefinition entries in tree.FunDefs, and populates the
// AllBuiltinNames, AllAggregateBuiltinNames, and AllWindowBuiltinNames slices.
func registerAllBuiltinFunctions() {
	AllBuiltinNames = make([]string, 0, len(builtins))
	AllAggregateBuiltinNames = make([]string, 0, len(aggregates))
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)

	for name, def := range builtins {
		fDef := tree.NewFunctionDefinition(name, &def.props, def.overloads)
		tree.FunDefs[name] = fDef
		if fDef.Private {
			// Avoid listing help for private functions.
			continue
		}
		AllBuiltinNames = append(AllBuiltinNames, name)
		classifyBuiltinByName(name, def)
	}
}

// classifyBuiltinByName adds the named builtin to the appropriate category-specific
// name slice based on its function class.
func classifyBuiltinByName(name string, def builtinDefinition) {
	if def.props.Class == tree.AggregateClass {
		AllAggregateBuiltinNames = append(AllAggregateBuiltinNames, name)
	} else if def.props.Class == tree.WindowClass {
		AllWindowBuiltinNames = append(AllWindowBuiltinNames, name)
	}
}

// populateMissingCategories fills in any empty Category fields on builtins by
// deriving them from the function's overload types.
func populateMissingCategories() {
	for _, name := range AllBuiltinNames {
		def := builtins[name]
		if def.props.Category == "" {
			def.props.Category = getCategory(def.overloads)
			builtins[name] = def
		}
	}
}

// sortBuiltinNameArrays sorts all builtin name slices for deterministic iteration.
func sortBuiltinNameArrays() {
	sort.Strings(AllBuiltinNames)
	sort.Strings(AllAggregateBuiltinNames)
	sort.Strings(AllWindowBuiltinNames)
}

func getCategory(b []tree.Overload) string {
	// If single argument attempt to categorize by the type of the argument.
	for _, ovl := range b {
		switch typ := ovl.Types.(type) {
		case tree.ArgTypes:
			if len(typ) == 1 {
				return categorizeType(typ[0].Typ)
			}
		}
		// Fall back to categorizing by return type.
		if retType := ovl.FixedReturnType(); retType != nil {
			return categorizeType(retType)
		}
	}
	return ""
}

func collectOverloads(
	props tree.FunctionProperties, types []*types.T, gens ...func(*types.T) tree.Overload,
) builtinDefinition {
	r := make([]tree.Overload, 0, len(types)*len(gens))
	for _, f := range gens {
		for _, t := range types {
			r = append(r, f(t))
		}
	}
	return builtinDefinition{
		props:     props,
		overloads: r,
	}
}
