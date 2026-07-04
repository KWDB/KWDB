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

package constraint

import (
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ParseConstraint parses a constraint in the format of Constraint.String, e.g:
//
//	"/1/2/3: [/1 - /2]".
func ParseConstraint(evalCtx *tree.EvalContext, str string) Constraint {
	s := strings.SplitN(str, ": ", 2)
	if len(s) != 2 {
		panic(str)
	}
	var cols []opt.OrderingColumn
	for _, v := range parseIntPath(s[0]) {
		cols = append(cols, opt.OrderingColumn(v))
	}
	var c Constraint
	c.Columns.Init(cols)
	c.Spans = parseSpans(evalCtx, s[1])
	return c
}

// parseSpans parses a list of spans with integer values like:
//
//	"[/1 - /2] [/5 - /6]".
func parseSpans(evalCtx *tree.EvalContext, str string) Spans {
	if str == "" {
		return Spans{}
	}
	s := strings.Split(str, " ")
	// Each span has three pieces.
	if len(s)%piecesPerSpan != 0 {
		panic(str)
	}
	var result Spans
	for i := 0; i < len(s)/piecesPerSpan; i++ {
		sp := ParseSpan(evalCtx, strings.Join(s[i*piecesPerSpan:i*piecesPerSpan+piecesPerSpan], " "), types.IntFamily)
		result.Append(&sp)
	}
	return result
}

// piecesPerSpan is the number of tokens in a span string: "[", key, "-", key, "]"
const piecesPerSpan = 3

// ParseSpan parses a span in the format of Span.String, e.g: [/1 - /2].
func ParseSpan(evalCtx *tree.EvalContext, str string, typ types.Family) Span {
	if len(str) < minSpanStringLen {
		panic(str)
	}
	s, e := str[0], str[len(str)-1]
	if !isValidBoundaryRune(s) || !isValidBoundaryRune(e) {
		panic(str)
	}

	keys := strings.Split(str[1:len(str)-1], keySeparator)
	if len(keys) != expectedKeyCount {
		panic(str)
	}
	startVals := parseDatumPath(evalCtx, keys[0], typ)
	endVals := parseDatumPath(evalCtx, keys[1], typ)

	var sp Span
	sp.Init(
		MakeCompositeKey(startVals...), runeToBoundary(s),
		MakeCompositeKey(endVals...), runeToBoundary(e),
	)
	return sp
}

// minSpanStringLen is the minimum length of a span string representation.
const minSpanStringLen = len("[ - ]")

// keySeparator separates start and end keys in a span string.
const keySeparator = " - "

// expectedKeyCount is the number of keys expected after splitting a span string.
const expectedKeyCount = 2

// runeToBoundary maps a boundary rune to its SpanBoundary value.
func runeToBoundary(r byte) SpanBoundary {
	switch r {
	case '[', ']':
		return IncludeBoundary
	case '(', ')':
		return ExcludeBoundary
	default:
		return IncludeBoundary
	}
}

// isValidBoundaryRune returns true if the byte is a valid span boundary marker.
func isValidBoundaryRune(r byte) bool {
	return r == '[' || r == ']' || r == '(' || r == ')'
}

// parseIntPath parses a string like "/1/2/3" into a list of integers.
func parseIntPath(str string) []int {
	var res []int
	for _, valStr := range parsePath(str) {
		val, err := strconv.Atoi(valStr)
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}

// parseDatumPath parses a span key string like "/1/2/3".
// Only NULL and a subset of types are currently supported.
func parseDatumPath(evalCtx *tree.EvalContext, str string, typ types.Family) []tree.Datum {
	var res []tree.Datum
	for _, valStr := range parsePath(str) {
		if valStr == nullMarker {
			res = append(res, tree.DNull)
			continue
		}
		val, err := parseDatumByType(evalCtx, valStr, typ)
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}

// nullMarker is the string used to represent NULL in datum paths.
const nullMarker = "NULL"

// parseDatumByType dispatches datum parsing to the appropriate type-specific
// parser based on the type family.
func parseDatumByType(evalCtx *tree.EvalContext, valStr string, typ types.Family) (tree.Datum, error) {
	switch typ {
	case types.BoolFamily:
		return tree.ParseDBool(valStr)
	case types.IntFamily:
		return tree.ParseDInt(valStr)
	case types.FloatFamily:
		return tree.ParseDFloat(valStr)
	case types.DecimalFamily:
		return tree.ParseDDecimal(valStr)
	case types.DateFamily:
		return tree.ParseDDate(evalCtx, valStr)
	case types.TimestampFamily:
		return tree.ParseDTimestamp(evalCtx, valStr, time.Microsecond)
	case types.TimestampTZFamily:
		return tree.ParseDTimestampTZ(evalCtx, valStr, time.Microsecond)
	case types.StringFamily:
		return tree.NewDString(valStr), nil
	default:
		return nil, errors.AssertionFailedf("type %s not supported", typ.String())
	}
}

// parsePath splits a string of the form "/foo/bar" into strings ["foo", "bar"].
// An empty string is allowed, otherwise the string must start with /.
func parsePath(str string) []string {
	if str == "" {
		return nil
	}
	if str[0] != pathSeparator {
		panic(str)
	}
	return strings.Split(str, string(pathSeparator))[1:]
}

// pathSeparator is the character used to separate path components.
const pathSeparator = '/'
