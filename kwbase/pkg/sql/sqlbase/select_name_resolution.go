// Copyright 2016 The Cockroach Authors.
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
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sqlbase

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	lua "github.com/yuin/gopher-lua"
)

// NameResolutionVisitor is a tree.Visitor implementation used to
// resolve the column names in an expression.
type NameResolutionVisitor struct {
	err        error
	iVarHelper tree.IndexedVarHelper
	searchPath sessiondata.SearchPath
	resolver   ColumnResolver

	// foundDependentVars is set to true during the analysis if an
	// expression was found which can change values between rows of the
	// same data source, for example IndexedVars and calls to the
	// random() function.
	foundDependentVars bool
}

var _ tree.Visitor = &NameResolutionVisitor{}

// VisitPre implements tree.Visitor.
func (v *NameResolutionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.IndexedVar:
		// If the indexed var is a standalone ordinal reference, ensure it
		// becomes a fully bound indexed var.
		t, v.err = v.iVarHelper.BindIfUnbound(t)
		if v.err != nil {
			return false, expr
		}

		v.foundDependentVars = true
		return false, t

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *tree.ColumnItem:
		_, err := t.Resolve(context.TODO(), &v.resolver)
		if err != nil {
			v.err = err
			return false, expr
		}

		colIdx := v.resolver.ResolverState.ColIdx
		ivar := v.iVarHelper.IndexedVar(colIdx)
		v.foundDependentVars = true
		return true, ivar

	case *tree.FuncExpr:
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			v.err = err
			return false, expr
		}

		if fd.NeedsRepeatedEvaluation {
			v.foundDependentVars = true
		}

		// Check for invalid use of *, which, if it is an argument, is the only argument.
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(tree.VarName)
		if !ok {
			break
		}
		vn, v.err = vn.NormalizeVarName()
		if v.err != nil {
			return false, expr
		}
		// Save back to avoid re-doing the work later.
		t.Exprs[0] = vn
		return true, t
	}

	return true, expr
}

// VisitPost implements tree.Visitor.
func (*NameResolutionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// ResolveNames is a wrapper around ResolveNamesUsingVisitor.
func ResolveNames(
	expr tree.Expr,
	source *DataSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, bool, error) {
	var v NameResolutionVisitor
	return ResolveNamesUsingVisitor(&v, expr, source, ivarHelper, searchPath)
}

// ResolveNamesUsingVisitor resolves the names in the given expression. It
// returns the resolved expression, whether it found dependent vars, and
// whether it found stars.
func ResolveNamesUsingVisitor(
	v *NameResolutionVisitor,
	expr tree.Expr,
	source *DataSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, bool, error) {
	*v = NameResolutionVisitor{
		iVarHelper: ivarHelper,
		searchPath: searchPath,
		resolver: ColumnResolver{
			Source: source,
		},
	}

	expr, _ = tree.WalkExpr(v, expr)
	return expr, v.foundDependentVars, v.err
}

// NewRestrictedLuaState creates a Lua VM for UDF execution.
//
// Do not load the default Lua standard libraries. In particular, this prevents
// Lua UDFs from accessing dangerous modules such as os, io, package, and debug.
func NewRestrictedLuaState() *lua.LState {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: true,
	})

	// Defense in depth. These globals should not exist when SkipOpenLibs is
	// true, but explicitly clearing them protects us if safe libraries are added
	// later.
	for _, name := range []string{
		"os",
		"io",
		"package",
		"debug",
		"require",
		"load",
		"loadfile",
		"dofile",
		"loadstring",
		"collectgarbage",
		"getfenv",
		"setfenv",
		"module",
		"newproxy",
		"print",
		"coroutine",
		"channel",
	} {
		L.SetGlobal(name, lua.LNil)
	}

	return L
}
