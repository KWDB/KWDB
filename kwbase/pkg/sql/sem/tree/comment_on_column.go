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

package tree

import "strings"

// CommentOnColumn represents an COMMENT ON COLUMN statement.
type CommentOnColumn struct {
	*ColumnItem
	Comment *string
}

// commentOnColumnKeyword is the SQL keyword prefix for COMMENT ON COLUMN.
const commentOnColumnKeyword = "COMMENT ON COLUMN "

// Format implements the NodeFormatter interface.
func (n *CommentOnColumn) Format(ctx *FmtCtx) {
	formatCommentStatement(ctx, commentOnColumnKeyword, n.ColumnItem, n.Comment)
}

// formatCommentStatement formats a COMMENT ON <object> IS <value> statement.
// The nameProvider provides the object name to format, and comment is the
// optional comment text (or NULL).
func formatCommentStatement(
	ctx *FmtCtx, keyword string, nameProvider NodeFormatter, comment *string,
) {
	ctx.WriteString(keyword)
	ctx.FormatNode(nameProvider)
	ctx.WriteString(" IS ")
	writeCommentValue(ctx, comment)
}

// writeCommentValue outputs a comment string value, properly escaped, or NULL.
func writeCommentValue(ctx *FmtCtx, comment *string) {
	if comment != nil {
		tmp := *comment
		ctx.WriteString("'" + strings.Replace(tmp, "'", "''", -1) + "'")
	} else {
		ctx.WriteString("NULL")
	}
}
