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

// replicationAlterPrefix and replicationToClauseKeyword are the SQL fragments
// used to format the ALTER ... REPLICATE TO statement.
const (
	replicationAlterPrefix     = "ALTER "
	replicationToClauseKeyword = "REPLICATE TO "
)

// ReplicationInformation represents an ALTER REPLICATE TO statement.
type ReplicationInformation struct {
	Targets TargetList
	To      PartitionedBackup
	Options KVOptions
	IsTs    bool
}

// Format implements the NodeFormatter interface.
func (node *ReplicationInformation) Format(ctx *FmtCtx) {
	node.writeAlterTargets(ctx)
	node.maybeWriteReplicateToClause(ctx)
}

// writeAlterTargets outputs the ALTER prefix with the target list.
func (node *ReplicationInformation) writeAlterTargets(ctx *FmtCtx) {
	ctx.WriteString(replicationAlterPrefix)
	ctx.FormatNode(&node.Targets)
}

// maybeWriteReplicateToClause outputs the REPLICATE TO clause when a
// destination is specified.
func (node *ReplicationInformation) maybeWriteReplicateToClause(ctx *FmtCtx) {
	if node.To != nil {
		ctx.WriteString(replicationToClauseKeyword)
		ctx.FormatNode(&node.To)
	}
}
