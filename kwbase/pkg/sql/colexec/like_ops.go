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

package colexec

import (
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// likeOpType is an enum that describes all of the different variants of LIKE
// that we support.
type likeOpType int

const (
	likeConstant likeOpType = iota + 1
	likeConstantNegate
	likeNeverMatch
	likeAlwaysMatch
	likeSuffix
	likeSuffixNegate
	likePrefix
	likePrefixNegate
	likeRegexp
	likeRegexpNegate
)

func getLikeOperatorType(pattern string, negate bool) (likeOpType, string, error) {
	if pattern == "" {
		if negate {
			return likeConstantNegate, "", nil
		}
		return likeConstant, "", nil
	}
	if pattern == "%" {
		if negate {
			return likeNeverMatch, "", nil
		}
		return likeAlwaysMatch, "", nil
	}
	if len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
		// There are no wildcards in the middle of the string, so we only need to
		// use a regular expression if both the first and last characters are
		// wildcards.
		firstChar := pattern[0]
		lastChar := pattern[len(pattern)-1]
		if !isWildcard(firstChar) && !isWildcard(lastChar) {
			// No wildcards, so this is just an exact string match.
			if negate {
				return likeConstantNegate, pattern, nil
			}
			return likeConstant, pattern, nil
		}
		if firstChar == '%' && !isWildcard(lastChar) {
			suffix := pattern[1:]
			if negate {
				return likeSuffixNegate, suffix, nil
			}
			return likeSuffix, suffix, nil
		}
		if lastChar == '%' && !isWildcard(firstChar) {
			prefix := pattern[:len(pattern)-1]
			if negate {
				return likePrefixNegate, prefix, nil
			}
			return likePrefix, prefix, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	if negate {
		return likeRegexpNegate, pattern, nil
	}
	return likeRegexp, pattern, nil
}

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern, or NOT LIKE if the negate argument is true. The implementation
// varies depending on the complexity of the pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, pattern string, negate bool,
) (Operator, error) {
	likeOpType, pattern, err := getLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	base := selConstOpBase{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
	}
	switch likeOpType {
	case likeConstant:
		return &selEQBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likeConstantNegate:
		return &selNEBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likeNeverMatch:
		// Use an empty not-prefix operator to get correct NULL behavior.
		return &selNotPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       []byte{},
		}, nil
	case likeAlwaysMatch:
		// Use an empty prefix operator to get correct NULL behavior.
		return &selPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       []byte{},
		}, nil
	case likeSuffix:
		return &selSuffixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likeSuffixNegate:
		return &selNotSuffixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likePrefix:
		return &selPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likePrefixNegate:
		return &selNotPrefixBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       pat,
		}, nil
	case likeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selRegexpBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       re,
		}, nil
	case likeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &selNotRegexpBytesBytesConstOp{
			selConstOpBase: base,
			constArg:       re,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}

// GetLikeProjectionOperator returns a projection operator which projects the
// result of the specified LIKE pattern, or NOT LIKE if the negate argument is
// true. The implementation varies depending on the complexity of the pattern.
func GetLikeProjectionOperator(
	allocator *Allocator,
	ctx *tree.EvalContext,
	input Operator,
	colIdx int,
	resultIdx int,
	pattern string,
	negate bool,
) (Operator, error) {
	likeOpType, pattern, err := getLikeOperatorType(pattern, negate)
	if err != nil {
		return nil, err
	}
	pat := []byte(pattern)
	input = newVectorTypeEnforcer(allocator, input, coltypes.Bool, resultIdx)
	base := projConstOpBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    resultIdx,
	}
	switch likeOpType {
	case likeConstant:
		return &projEQBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likeConstantNegate:
		return &projNEBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likeNeverMatch:
		// Use an empty not-prefix operator to get correct NULL behavior.
		return &projNotPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        []byte{},
		}, nil
	case likeAlwaysMatch:
		// Use an empty prefix operator to get correct NULL behavior.
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        []byte{},
		}, nil
	case likeSuffix:
		return &projSuffixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likeSuffixNegate:
		return &projNotSuffixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likePrefix:
		return &projPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likePrefixNegate:
		return &projNotPrefixBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        pat,
		}, nil
	case likeRegexp:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projRegexpBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        re,
		}, nil
	case likeRegexpNegate:
		re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
		if err != nil {
			return nil, err
		}
		return &projNotRegexpBytesBytesConstOp{
			projConstOpBase: base,
			constArg:        re,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unsupported like op type %d", likeOpType)
	}
}
