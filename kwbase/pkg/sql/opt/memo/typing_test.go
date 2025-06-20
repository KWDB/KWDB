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

package memo_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestTyping(t *testing.T) {
	runDataDrivenTest(t, "testdata/typing",
		memo.ExprFmtHideMiscProps|
			memo.ExprFmtHideConstraints|
			memo.ExprFmtHideFuncDeps|
			memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideStats|
			memo.ExprFmtHideCost|
			memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars,
	)
}

func TestBinaryOverloadExists(t *testing.T) {
	test := func(expected, actual bool) {
		if expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}

	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Date, types.Int))
	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Date, types.Unknown))
	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Unknown, types.Int))
	test(false, memo.BinaryOverloadExists(opt.MinusOp, types.Int, types.Date))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.IntArray, types.Int))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.Unknown, types.IntArray))
}

func TestBinaryAllowsNullArgs(t *testing.T) {
	test := func(expected, actual bool) {
		if expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}

	test(false, memo.BinaryAllowsNullArgs(opt.PlusOp, types.Int, types.Int))
	test(false, memo.BinaryAllowsNullArgs(opt.PlusOp, types.Int, types.Unknown))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.IntArray, types.Int))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.Unknown, types.IntArray))
}

// TestTypingUnaryAssumptions ensures that unary overloads conform to certain
// assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operand.
func TestTypingUnaryAssumptions(t *testing.T) {
	for name, overloads := range tree.UnaryOps {
		for i, overload := range overloads {
			op := overload.(*tree.UnaryOp)

			// Check for basic ambiguity where two different unary op overloads
			// both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				op2 := overload2.(*tree.UnaryOp)
				if op.Typ.Equivalent(op2.Typ) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}
		}
	}
}

// TestTypingBinaryAssumptions ensures that binary overloads conform to certain
// assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operands.
//  2. When of the operands is null, and if NullableArgs is true, then the
//     return type can be inferred from just the non-null operand.
func TestTypingBinaryAssumptions(t *testing.T) {
	for name, overloads := range tree.BinOps {
		for i, overload := range overloads {
			op := overload.(*tree.BinOp)

			// Check for basic ambiguity where two different binary op overloads
			// both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				op2 := overload2.(*tree.BinOp)
				if op.LeftType.Equivalent(op2.LeftType) && op.RightType.Equivalent(op2.RightType) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}

			// Handle ops that allow null operands. Check for ambiguity where
			// the return type cannot be inferred from the non-null operand.
			if op.NullableArgs {
				for i2, overload2 := range overloads {
					if i == i2 {
						continue
					}

					op2 := overload2.(*tree.BinOp)
					if !op2.NullableArgs {
						continue
					}

					if op.LeftType == op2.LeftType && op.ReturnType != op2.ReturnType {
						t.Errorf("found null operand ambiguity for %s:\n%+v\n%+v", name, op, op2)
					}

					if op.RightType == op2.RightType && op.ReturnType != op2.ReturnType {
						t.Errorf("found null operand ambiguity for %s:\n%+v\n%+v", name, op, op2)
					}
				}
			}
		}
	}
}

// TestTypingComparisonAssumptions ensures that comparison overloads conform to
// certain assumptions we're making in the type inference code:
//  1. All comparison ops will be present in tree.CmpOps after being mapped
//     with NormalizeComparison.
//  2. The overload can be inferred from the operator type and the data
//     types of its operands.
func TestTypingComparisonAssumptions(t *testing.T) {
	for _, op := range opt.ComparisonOperators {
		newOp, _, _ := memo.NormalizeComparison(op)
		comp := opt.ComparisonOpReverseMap[newOp]
		if _, ok := tree.CmpOps[comp]; !ok {
			t.Errorf("could not find overload for %v", op)
		}
	}
	for name, overloads := range tree.CmpOps {
		for i, overload := range overloads {
			op := overload.(*tree.CmpOp)

			// Check for basic ambiguity where two different comparison op overloads
			// both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				op2 := overload2.(*tree.CmpOp)
				if op.LeftType.Equivalent(op2.LeftType) && op.RightType.Equivalent(op2.RightType) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}
		}
	}
}

// TestTypingAggregateAssumptions ensures that aggregate overloads conform to
// certain assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operand.
//  2. The return type of overloads is fixed.
//  3. The return type for min/max aggregates is same as type of argument.
func TestTypingAggregateAssumptions(t *testing.T) {
	for _, name := range builtins.AllAggregateBuiltinNames {
		if name == builtins.AnyNotNull {
			// any_not_null is treated as a special case.
			continue
		}
		_, overloads := builtins.GetBuiltinProperties(name)
		for i, overload := range overloads {
			// Check for basic ambiguity where two different aggregate function
			// overloads both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				if overload.Types.Match(overload2.Types.Types()) {
					format := "found equivalent operand type ambiguity for %s: %+v"
					t.Errorf(format, name, overload.Types.Types())
				}
			}

			// Check for fixed return types.
			retType := overload.ReturnType(nil)

			// As per rule 3, max and min have slightly different rules. We allow
			// max and min to have non-fixed return types to allow defining aggregate
			// overloads that have container types as arguments.
			if name == "min" || name == "max" || name == "first" || name == "last" || name == "last_row" || name == "first_row" {
				// Evaluate the return typer.
				types := overload.Types.Types()
				retType = overload.InferReturnTypeFromInputArgTypes(types)
				if retType != types[0] {
					t.Errorf("return type differs from arg type for %s: %+v", name, overload.Types.Types())
				}
				continue
			}

			if name == "min_extend" || name == "max_extend" {
				// Evaluate the return typer.
				types := overload.Types.Types()
				retType = overload.InferReturnTypeFromInputArgTypes(types)
				if retType != types[1] {
					t.Errorf("return type differs from arg type for %s: %+v", name, overload.Types.Types())
				}
				continue
			}
			if retType == tree.UnknownReturnType {
				t.Errorf("return type is not fixed for %s: %+v", name, overload.Types.Types())
			}
		}
	}
}
