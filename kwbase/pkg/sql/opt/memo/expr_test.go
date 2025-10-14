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

package memo_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/exprgen"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/opttester"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	_ "gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TestExprIsNeverNull runs data-driven testcases of the form
//
//	<command> [<args>]...
//	<SQL statement or expression>
//	----
//	<expected results>
//
// See OptTester.Handle for supported commands. In addition to those, we
// support:
//
//   - scalar-is-not-nullable [args]
//
//     Builds a scalar expression using the input and performs a best-effort
//     check to see if the scalar expression is nullable. It outputs this
//     result as a boolean.
//
//     The supported args (in addition to the ones supported by OptTester):
//
//   - vars=(type1,type2,...)
//
//     Adding a !null suffix on a var type is used to mark that var as
//     non-nullable.
func TestExprIsNeverNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/expr", func(t *testing.T, path string) {
		catalog := testcat.New()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var varTypes []*types.T
			var err error

			tester := opttester.New(catalog, d.Input)
			switch d.Cmd {
			case "scalar-is-not-nullable":
				var notNullCols opt.ColSet
				for _, arg := range d.CmdArgs {
					key, vals := arg.Key, arg.Vals
					switch key {
					case "vars":
						for i := 0; i < len(vals); i++ {
							if strings.HasSuffix(strings.ToLower(vals[i]), "!null") {
								vals[i] = strings.TrimSuffix(strings.ToLower(vals[i]), "!null")
								notNullCols.Add(opt.ColumnID(i + 1))
							}
						}
						varTypes, err = exprgen.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

					default:
						if err := tester.Flags.Set(arg); err != nil {
							d.Fatalf(t, "%s", err)
						}
					}
				}

				expr, err := parser.ParseExpr(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				ctx := context.Background()
				semaCtx := tree.MakeSemaContext()
				evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

				var o xform.Optimizer
				o.Init(&evalCtx, catalog)
				for i, typ := range varTypes {
					o.Memo().Metadata().AddColumn(fmt.Sprintf("@%d", i+1), typ)
				}
				b := optbuilder.NewScalar(ctx, &semaCtx, &evalCtx, o.Factory())
				err = b.Build(expr)
				if err != nil {
					return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
				}
				return fmt.Sprintf("%t\n", memo.ExprIsNeverNull(o.Memo().RootExpr().(opt.ScalarExpr), notNullCols))

			default:
				return tester.RunCommand(t, d)
			}
		})
	})
}
