// Copyright 2016 The Cockroach Authors.
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

package tree

// CopyFrom represents a COPY FROM statement.
type CopyFrom struct {
	Table   TableName
	Columns NameList
	Stdin   bool
	Options KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CopyFrom) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	ctx.FormatNode(&node.Table)
	node.maybeWriteColumnList(ctx)
	ctx.WriteString(" FROM ")
	node.writeCopySource(ctx)
	node.maybeWriteOptions(ctx)
}

// maybeWriteColumnList outputs the parenthesized column list when columns
// are specified.
func (node *CopyFrom) maybeWriteColumnList(ctx *FmtCtx) {
	if len(node.Columns) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Columns)
		ctx.WriteString(")")
	}
}

// writeCopySource outputs the copy source, either STDIN or another source.
func (node *CopyFrom) writeCopySource(ctx *FmtCtx) {
	if node.Stdin {
		ctx.WriteString("STDIN")
	}
}

// maybeWriteOptions appends the WITH options clause when present.
func (node *CopyFrom) maybeWriteOptions(ctx *FmtCtx) {
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}
