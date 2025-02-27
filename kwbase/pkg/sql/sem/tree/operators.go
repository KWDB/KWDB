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

package tree

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// This file implements the generation of unique names for every
// operator overload.
//
// The historical first purpose of generating these names is to be used
// as telemetry keys, for feature usage reporting.

// Detailed counter name generation follows.
//
// We pre-allocate the counter objects upfront here and later use
// Inc(), to avoid the hash map lookup in telemetry.Count upon type
// checking every scalar operator node.

// The logic that follows is also associated with a related feature in
// PostgreSQL, which may be implemented by CockroachDB in the future:
// exposing all the operators as unambiguous, non-overloaded built-in
// functions.  For example, in PostgreSQL, one can use `SELECT
// int8um(123)` to apply the int8-specific unary minus operator.
// This feature can be considered in the future for two reasons:
//
// 1. some pg applications may simply require the ability to use the
//    pg native operator built-ins. If/when this compatibility is
//    considered, care should be taken to tweak the string maps below
//    to ensure that the operator names generated here coincide with
//    those used in the postgres library.
//
// 2. since the operator built-in functions are non-overloaded, they
//    remove the requirement to disambiguate the type of operands
//    with the ::: (annotate_type) operator. This may be useful
//    to simplify/accelerate the serialization of scalar expressions
//    in distsql.
//

func init() {
	// Label the unary operators.
	for op, overloads := range UnaryOps {
		if int(op) >= len(unaryOpName) || unaryOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := unaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*UnaryOp)
			o.counter = sqltelemetry.UnaryOpCounter(opName, o.Typ.String())
		}
	}

	// Label the comparison operators.
	for op, overloads := range CmpOps {
		if int(op) >= len(comparisonOpName) || comparisonOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := comparisonOpName[op]
		for _, impl := range overloads {
			o := impl.(*CmpOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			o.counter = sqltelemetry.CmpOpCounter(opName, lname, rname)
		}
	}

	// Label the binary operators.
	for op, overloads := range BinOps {
		if int(op) >= len(binaryOpName) || binaryOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := binaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*BinOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			o.counter = sqltelemetry.BinOpCounter(opName, lname, rname)
		}
	}
}

// annotateCast produces an array of cast types decorated with cast
// type telemetry counters.
func annotateCast(toType *types.T, fromTypes []*types.T) []castInfo {
	ci := make([]castInfo, len(fromTypes))
	for i, fromType := range fromTypes {
		ci[i].fromT = fromType
	}
	rname := toType.String()

	for i, fromType := range fromTypes {
		lname := fromType.String()
		ci[i].counter = sqltelemetry.CastOpCounter(lname, rname)
	}
	return ci
}
