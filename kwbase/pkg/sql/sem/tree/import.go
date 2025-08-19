// Copyright 2017 The Cockroach Authors.
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

// Import represents a IMPORT statement.
type Import struct {
	Table      *TableName
	IsDatabase bool
	Into       bool
	IntoCols   NameList
	CreateFile Expr
	CreateDefs TableDefs
	FileFormat string
	Files      Exprs
	Options    KVOptions
	OnlyMeta   bool
	Settings   bool
	Users      bool
}

var _ Statement = &Import{}

// Format implements the NodeFormatter interface.
func (node *Import) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT ")

	if node.Into {
		ctx.WriteString("INTO ")
		ctx.FormatNode(node.Table)
		ctx.WriteString(" ")
	} else if node.IsDatabase {
		ctx.WriteString("DATABASE ")
	} else {
		ctx.WriteString("TABLE ")
		if node.Table != nil {
			ctx.FormatNode(node.Table)
		}

		if node.CreateFile != nil {
			ctx.WriteString("CREATE USING ")
			ctx.FormatNode(node.CreateFile)
			ctx.WriteString(" ")
		} else {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.CreateDefs)
			ctx.WriteString(") ")
		}
	}
	ctx.WriteString(node.FileFormat)
	ctx.WriteString(" DATA (")
	ctx.FormatNode(&node.Files)
	ctx.WriteString(")")

	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// ImportPortal represents a IMPORT KWDB Schema statement.
type ImportPortal struct {
	File Expr
}

var _ Statement = &ImportPortal{}

// Format implements the NodeFormatter interface.
func (node *ImportPortal) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT ")
	ctx.WriteString("PORTAL ")
	ctx.WriteString("( ")
	ctx.FormatNode(node.File)
	ctx.WriteString(") ")
}
