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

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// buildLimit adds Limit and Offset operators according to the Limit clause.
//
// parentScope is the scope for the LIMIT/OFFSET expressions; this is not the
// same as inScope, because statements like:
//
//	SELECT k FROM kv LIMIT k
//
// are not valid.
func (b *Builder) buildLimit(limit *tree.Limit, parentScope, inScope *scope) {
	if limit.Offset != nil {
		input := inScope.expr
		offset := b.resolveAndBuildScalar(
			limit.Offset, types.Int, exprTypeOffset, tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructOffset(input, offset, inScope.makeOrderingChoice())
	}
	if limit.Count != nil {
		input := inScope.expr
		limit := b.resolveAndBuildScalar(
			limit.Count, types.Int, exprTypeLimit, tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructLimit(input, limit, inScope.makeOrderingChoice())
	}
	// ordering is stored by limit or offset if rootExpr are limit or offset, we need not handle orderBy in fromSubquery.
	// there exists some RBO rule that enables Limit push, eg: PushLimitIntoWindow
	switch inScope.expr.(type) {
	case *memo.LimitExpr, *memo.OffsetExpr:
		inScope.noHandleOrderInFrom = true
	}
}
