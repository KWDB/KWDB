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

// SecondaryType indicates that secondary operation type.
type SecondaryType uint8

// ReplicateSetSecondary represents a CHANGE REPLICATION SECONDARY statement.
type ReplicateSetSecondary struct {
	Host PartitionedBackup
}

// replicateSetSecondaryKeyword is the SQL keyword prefix for the statement.
const replicateSetSecondaryKeyword = "REPLICATE SET SECONDARY"

// Format implements the NodeFormatter interface.
func (node *ReplicateSetSecondary) Format(ctx *FmtCtx) {
	ctx.WriteString(replicateSetSecondaryKeyword)
	node.formatHostNode(ctx)
}

// formatHostNode writes the serialized Host field to the format context.
func (node *ReplicateSetSecondary) formatHostNode(ctx *FmtCtx) {
	ctx.FormatNode(&node.Host)
}
