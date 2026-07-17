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

// AlterTSDatabase represents an ALTER TS DATABASE statement.
type AlterTSDatabase struct {
	Database          Name
	LifeTime          *TimeInput
	PartitionInterval *TimeInput
}

// alterTSDatabaseKeyword is the SQL keyword prefix for ALTER TS DATABASE SET.
const alterTSDatabaseKeyword = "ALTER TS DATABASE SET"

// compile-time interface conformance check
var _ Statement = &AlterTSDatabase{}

// Format implements the NodeFormatter interface.
func (node *AlterTSDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString(alterTSDatabaseKeyword)
	node.maybeWriteLifeTimeClause(ctx)
	node.maybeWritePartitionIntervalClause(ctx)
}

// maybeWriteLifeTimeClause emits the LIFETIME setting when configured.
func (node *AlterTSDatabase) maybeWriteLifeTimeClause(ctx *FmtCtx) {
	if node.LifeTime != nil {
		ctx.WriteString(" LIFETIME = ")
		ctx.FormatNode(node.LifeTime)
	}
}

// maybeWritePartitionIntervalClause emits the PARTITION INTERVAL setting
// when configured.
func (node *AlterTSDatabase) maybeWritePartitionIntervalClause(ctx *FmtCtx) {
	if node.PartitionInterval != nil {
		ctx.WriteString(" PARTITION INTERVAL = ")
		ctx.FormatNode(node.PartitionInterval)
	}
}
