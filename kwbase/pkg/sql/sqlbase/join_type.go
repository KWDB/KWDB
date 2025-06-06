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

package sqlbase

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// Prettier aliases for JoinType values. See the original types for
// descriptions.
const (
	InnerJoin        = JoinType_INNER
	LeftOuterJoin    = JoinType_LEFT_OUTER
	RightOuterJoin   = JoinType_RIGHT_OUTER
	FullOuterJoin    = JoinType_FULL_OUTER
	LeftSemiJoin     = JoinType_LEFT_SEMI
	LeftAntiJoin     = JoinType_LEFT_ANTI
	IntersectAllJoin = JoinType_INTERSECT_ALL
	ExceptAllJoin    = JoinType_EXCEPT_ALL
)

// JoinTypeFromAstString takes a join string as found in a SQL
// statement (e.g. "INNER JOIN") and returns the JoinType.
func JoinTypeFromAstString(joinStr string) JoinType {
	switch joinStr {
	case "", tree.AstInner, tree.AstCross:
		return InnerJoin

	case tree.AstLeft:
		return LeftOuterJoin

	case tree.AstRight:
		return RightOuterJoin

	case tree.AstFull:
		return FullOuterJoin

	default:
		panic(fmt.Sprintf("unknown join string %s", joinStr))
	}
}

// IsSetOpJoin returns true if this join is a set operation.
func (j JoinType) IsSetOpJoin() bool {
	return j == IntersectAllJoin || j == ExceptAllJoin
}
