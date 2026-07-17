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

// CommentOnProcedure represents an COMMENT ON PROCEDURE statement.
type CommentOnProcedure struct {
	Name    TableName
	Comment *string
}

// commentOnProcedureKeyword is the SQL keyword prefix for COMMENT ON PROCEDURE.
const commentOnProcedureKeyword = "COMMENT ON PROCEDURE "

// Format implements the NodeFormatter interface.
func (n *CommentOnProcedure) Format(ctx *FmtCtx) {
	formatCommentStatement(ctx, commentOnProcedureKeyword, &n.Name, n.Comment)
}
