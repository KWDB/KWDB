// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

// CommentOnProcedure represents an COMMENT ON PROCEDURE statement.
type CommentOnProcedure struct {
	Name    TableName
	Comment *string
}

// Format implements the NodeFormatter interface.
func (n *CommentOnProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMENT ON PROCEDURE ")
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" IS ")
	if n.Comment != nil {
		tmp := *n.Comment
		//lex.EncodeSQLStringWithFlags(&ctx.Buffer, *n.Comment, ctx.flags.EncodeFlags())
		ctx.WriteString("'" + strings.Replace(tmp, "'", "''", -1) + "'")
	} else {
		ctx.WriteString("NULL")
	}
}
