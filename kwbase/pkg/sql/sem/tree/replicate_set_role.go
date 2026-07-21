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

// RoleType indicates edge node role type.
type RoleType uint8

const (
	// RoleTypeDefault default node
	RoleTypeDefault RoleType = iota
	// RoleTypePrimary primary node
	RoleTypePrimary
	// RoleTypeSecondary secondary node
	RoleTypeSecondary
)

// ReplicateSetRole represents a SET REPLICA ROLE statement.
type ReplicateSetRole struct {
	RoleType RoleType
}

// replicateSetRoleKeyword is the SQL keyword for REPLICATE SET ROLE.
const replicateSetRoleKeyword = "REPLICATE SET ROLE"

// roleTypeSuffixes maps RoleType values to their SQL keyword suffixes.
var roleTypeSuffixes = map[RoleType]string{
	RoleTypeDefault:   " DEFAULT",
	RoleTypePrimary:   " PRIMARY",
	RoleTypeSecondary: " SECONDARY",
}

// Format implements the NodeFormatter interface.
func (node *ReplicateSetRole) Format(ctx *FmtCtx) {
	ctx.WriteString(replicateSetRoleKeyword)
	node.writeRoleTypeSuffix(ctx)
}

// writeRoleTypeSuffix appends the role type suffix based on the configured role.
func (node *ReplicateSetRole) writeRoleTypeSuffix(ctx *FmtCtx) {
	if suffix, ok := roleTypeSuffixes[node.RoleType]; ok {
		ctx.WriteString(suffix)
	}
}
