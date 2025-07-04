// Copyright 2012, Google Inc. All rights reserved.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import "gitee.com/kwbasedb/kwbase/pkg/sql/privilege"

// Grant represents a GRANT statement.
type Grant struct {
	Privileges privilege.List
	Targets    TargetList
	Grantees   NameList
}

// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases  NameList
	Schemas    NameList
	Tables     TablePatterns
	Procedures TableNames

	// ForRoles and Roles are used internally in the parser and not used
	// in the AST. Therefore they do not participate in pretty-printing,
	// etc.
	ForRoles bool
	Roles    NameList
}

// Format implements the NodeFormatter interface.
func (tl *TargetList) Format(ctx *FmtCtx) {
	if tl.Databases != nil {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&tl.Databases)
	} else if tl.Schemas != nil {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.Tables != nil {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&tl.Tables)
	} else {
		ctx.WriteString("PROCEDURE ")
		ctx.FormatNode(&tl.Procedures)
	}
}

// Format implements the NodeFormatter interface.
func (node *Grant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	node.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Grantees)
}

// GrantRole represents a GRANT <role> KWDBGrantstatement.
type GrantRole struct {
	Roles       NameList
	Members     NameList
	AdminOption bool
}

// Format implements the NodeFormatter interface.
func (node *GrantRole) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	ctx.FormatNode(&node.Roles)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Members)
	if node.AdminOption {
		ctx.WriteString(" WITH ADMIN OPTION")
	}
}
