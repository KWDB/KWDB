// Copyright 2020 The Cockroach Authors.
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

// Package nocopy defines an Analyzer that detects invalid uses of util.NoCopy.
package nocopy

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for invalid uses of util.NoCopy`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "nocopy",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const noCopyType = "gitee.com/kwbasedb/kwbase/pkg/util.NoCopy"

// nocopy ensures that the util.NoCopy type is not misused. Specifically, it
// ensures that the type is always embedded without a name as the first field in
// a parent struct like:
//
//	type s struct {
//	    _ util.NoCopy
//	    ...
//	}
//
// We lint against including the type in other positions in structs both for
// uniformity and because it can have runtime performance effects. Specifically,
// if util.NoCopy is included as the last field in a parent struct then it will
// increase the size of the parent struct even though util.NoCopy is zero-sized.
// This is explained in detail in https://github.com/golang/go/issues/9401 and
// is demonstrated in https://play.golang.org/p/jwB2Az5owcm.
func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	inspect.Preorder([]ast.Node{
		(*ast.StructType)(nil),
	}, func(n ast.Node) {
		str := n.(*ast.StructType)
		if str.Fields == nil {
			return
		}
		for i, f := range str.Fields.List {
			tv, ok := pass.TypesInfo.Types[f.Type]
			if !ok {
				continue
			}
			if tv.Type.String() != noCopyType {
				continue
			}
			switch {
			case i != 0:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - must be first field in struct")
			case len(f.Names) == 0:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should not be embedded")
			case len(f.Names) > 1:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should be included only once")
			case f.Names[0].Name != "_":
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should be unnamed")
			default:
				// Valid use.
			}
		}
	})
	return nil, nil
}
