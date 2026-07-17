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

// AlterAudit represents a ALTER AUDIT statement.
type AlterAudit struct {
	Name     Name
	Enable   bool
	NewName  Name
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *AlterAudit) Format(ctx *FmtCtx) {
	n.writeAlterAuditPrefix(ctx)
	n.maybeWriteIfExistsClause(ctx)
	n.writeAuditName(ctx)
	n.writeAuditActionClause(ctx)
}

// writeAlterAuditPrefix emits the ALTER AUDIT keyword.
func (n *AlterAudit) writeAlterAuditPrefix(ctx *FmtCtx) {
	ctx.WriteString("ALTER AUDIT ")
}

// maybeWriteIfExistsClause emits IF EXISTS when the flag is set.
func (n *AlterAudit) maybeWriteIfExistsClause(ctx *FmtCtx) {
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
}

// writeAuditName outputs the audit name node.
func (n *AlterAudit) writeAuditName(ctx *FmtCtx) {
	ctx.FormatNode(&n.Name)
}

// writeAuditActionClause determines and emits the appropriate action clause
// for the ALTER AUDIT statement: RENAME TO, ENABLE, or DISABLE.
func (n *AlterAudit) writeAuditActionClause(ctx *FmtCtx) {
	switch {
	case n.NewName != "":
		ctx.WriteString(" RENAME TO ")
		ctx.FormatNode(&n.NewName)
	case n.Enable:
		ctx.WriteString(" ENABLE")
	default:
		ctx.WriteString(" DISABLE")
	}
}
