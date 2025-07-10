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

// ReturningClause represents the returning clause on a statement.
type ReturningClause interface {
	NodeFormatter
	// statementType returns the StatementType of statements that include
	// the implementors variant of a RETURNING clause.
	statementType() StatementType
	returningClause()
}

var _ ReturningClause = &ReturningExprs{}
var _ ReturningClause = &ReturningNothing{}
var _ ReturningClause = &NoReturningClause{}

// ReturningIntoClause represents RETURNING expressions.
type ReturningIntoClause struct {
	SelectClause ReturningExprs
	Targets      SelectIntoTargets
}

// Format implements the NodeFormatter interface.
func (r *ReturningIntoClause) Format(ctx *FmtCtx) {
	ctx.FormatNode(&r.SelectClause)
	ctx.WriteString(" INTO")
	for i := range r.Targets {
		if i > 0 {
			ctx.WriteString(",")
		}
		ctx.WriteString(" ")
		ctx.WriteString(r.Targets[i].DeclareVar)
	}
}

// ReturningExprs represents RETURNING expressions.
type ReturningExprs SelectExprs

// Format implements the NodeFormatter interface.
func (r *ReturningExprs) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURNING ")
	ctx.FormatNode((*SelectExprs)(r))
}

// ReturningNothingClause is a shared instance to avoid unnecessary allocations.
var ReturningNothingClause = &ReturningNothing{}

// ReturningNothing represents RETURNING NOTHING.
type ReturningNothing struct{}

// Format implements the NodeFormatter interface.
func (*ReturningNothing) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURNING NOTHING")
}

// AbsentReturningClause is a ReturningClause variant representing the absence of
// a RETURNING clause.
var AbsentReturningClause = &NoReturningClause{}

// NoReturningClause represents the absence of a RETURNING clause.
type NoReturningClause struct{}

// Format implements the NodeFormatter interface.
func (*NoReturningClause) Format(_ *FmtCtx) {}

// used by parent statements to determine their own StatementType.
func (*ReturningExprs) statementType() StatementType      { return Rows }
func (*ReturningNothing) statementType() StatementType    { return RowsAffected }
func (*NoReturningClause) statementType() StatementType   { return RowsAffected }
func (*ReturningIntoClause) statementType() StatementType { return RowsAffected }

func (*ReturningExprs) returningClause()      {}
func (*ReturningNothing) returningClause()    {}
func (*NoReturningClause) returningClause()   {}
func (*ReturningIntoClause) returningClause() {}

// HasReturningClause determines if a ReturningClause is present, given a
// variant of the ReturningClause interface.
func HasReturningClause(clause ReturningClause) bool {
	_, ok := clause.(*NoReturningClause)
	return !ok
}
