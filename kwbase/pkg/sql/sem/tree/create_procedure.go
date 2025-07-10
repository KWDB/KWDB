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

import (
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// CreateProcedure represents the definition of stored procedures.
type CreateProcedure struct {
	Name         TableName
	Parameters   []*ProcedureParameter
	Block        Block
	ReturnedType *types.T
	BodyStr      string

	//just for resolve
	ProcID   int32
	DBID     int32
	SchemaID int32
}

// Format formats create procedure string
func (node *CreateProcedure) Format(ctx *FmtCtx) {
	f := ctx.flags
	ctx.WriteString("CREATE ")
	ctx.WriteString("PROCEDURE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString("(")
	for i, arg := range node.Parameters {
		//ctx.WriteString(getProcParamDirection(arg.Direction))
		//ctx.WriteString(" ")
		lex.EncodeRestrictedSQLIdent(&ctx.Buffer, string(arg.Name), f.EncodeFlags())
		ctx.WriteString(" ")
		ctx.WriteString(arg.Type.SQLString())
		if i < len(node.Parameters)-1 {
			ctx.WriteString(", ")
		}
	}
	ctx.WriteString(")\n")
	ctx.WriteString("BEGIN\n")
	for _, arg := range node.Block.Body {
		ctx.WriteString("\t")
		ctx.FormatNode(arg)
		//if i < len(node.Body)-1 {
		ctx.WriteString(";\n")
		//}
	}
	ctx.WriteString("END")
}

// CreateProcedurePG represents the definition of stored procedures.
type CreateProcedurePG struct {
	Name       TableName
	Parameters []*ProcedureParameter
	BodyStr    string
}

// Format formats create procedure string
func (node *CreateProcedurePG) Format(ctx *FmtCtx) {
	f := ctx.flags
	ctx.WriteString("CREATE ")
	ctx.WriteString("PROCEDURE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString("(")
	for i, arg := range node.Parameters {
		lex.EncodeRestrictedSQLIdent(&ctx.Buffer, string(arg.Name), f.EncodeFlags())
		ctx.WriteString(" ")
		ctx.WriteString(arg.Type.SQLString())
		if i < len(node.Parameters)-1 {
			ctx.WriteString(", ")
		}
	}
	ctx.WriteString(") ")
	ctx.WriteString("$$")
	ctx.WriteString(node.BodyStr)
	ctx.WriteString("$$")
}

// ProcedureParameter 表示存储过程的参数
type ProcedureParameter struct {
	Direction ProcDirection
	Name      Name
	Type      *types.T
}

// ProcDirection flags params direction（IN/OUT/INOUT）
type ProcDirection int

const (
	// InDirection represents IN Direction type of parameter
	InDirection ProcDirection = iota
	// OutDirection represents OUT Direction type of parameter
	OutDirection
	// InOutDirection represents INOUT Direction type of parameter
	InOutDirection
)

// Block represents statements in a procedure body.
type Block struct {
	Label string
	Body  []Statement
}

// Format implements the NodeFormatter interface.
func (s *Block) Format(ctx *FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
	ctx.WriteString("BEGIN\n")
	for _, childStmt := range s.Body {
		ctx.FormatNode(childStmt)
	}
	ctx.WriteString("END")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	ctx.WriteString(";\n")
}

// ProcedureReturn represent a RETURN statement in a procedure body.
type ProcedureReturn struct {
	ReturnVal Expr
}

// Format implements the NodeFormatter interface.
func (node *ProcedureReturn) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURN ")
	ctx.FormatNode(node.ReturnVal)
}

// Declaration represents a Declare statement in a procedure body.
type Declaration struct {
	Typ      DeclareTyp
	Variable DeclareVar
	Cursor   DeclareCursor
	Handler  DeclareHandler
}

// DeclareTyp represents Declaration type
type DeclareTyp int

const (
	// DeclVariable represents Variable Declaration type
	DeclVariable DeclareTyp = iota
	// DeclCursor represents Cursor Declaration type
	DeclCursor
	// DeclHandler represents Handler Declaration type
	DeclHandler
)

// DeclareVar represents a Declare Variable statement in a procedure body.
type DeclareVar struct {
	VarName     string
	Typ         *types.T
	DefaultExpr Expr
	// When DeclareVar is a parameter, it is true.
	IsParameter bool
}

// DeclareCursor represents a Declare Cursor statement in a procedure body.
type DeclareCursor struct {
	CurName Name
	Body    Statement
}

// HandlerMode represents Handler Mode
type HandlerMode uint8

const (
	// ModeExit represents Exit Handler Mode
	ModeExit HandlerMode = iota
	// ModeContinue represents Continue Handler Mode
	ModeContinue
	// ModeMax represents Max Handler Mode
	ModeMax
)

// HandlerType represents Handler Type
type HandlerType uint8

const (
	// NOTFOUND represents Handler not found
	NOTFOUND HandlerType = 1 << iota
	// SQLEXCEPTION represents Handler exception
	SQLEXCEPTION
)

// ControlCursor represents a Control Cursor statement in a procedure body.
type ControlCursor struct {
	CurName   Name
	Command   CursorCommand
	FetchInto []string
}

// CursorCommand represents type of Cursor Command
type CursorCommand int

const (
	// OpenCursor represents Open Cursor
	OpenCursor CursorCommand = iota
	// FetchCursor represents Fetch Cursor
	FetchCursor
	// CloseCursor represents Close Cursor
	CloseCursor
)

// CursorCommandToStatement translates a cursor command integer to a statement prefix.
var CursorCommandToStatement = map[CursorCommand]string{
	OpenCursor:  "OPEN",
	FetchCursor: "FETCH",
	CloseCursor: "CLOSE",
}

// Format implements the NodeFormatter interface.
func (n *ControlCursor) Format(ctx *FmtCtx) {
	ctx.WriteString(CursorCommandToStatement[n.Command])
	ctx.WriteString(" ")
	ctx.FormatNode(&n.CurName)
	if len(n.FetchInto) != 0 {
		ctx.WriteString(" INTO ")
		sep := ""
		for _, str := range n.FetchInto {
			ctx.WriteString(sep)
			ctx.WriteString(str)
			sep = ", "
		}
	}
}

const (
	// HandlerOpContinue represents continue operator
	HandlerOpContinue = "CONTINUE"
	// HandlerOpExit represents exit operator
	HandlerOpExit = "EXIT"
	// HandlerForNotFound represents operator not found
	HandlerForNotFound = "NOT FOUND"
	// HandlerForException represents exception operator
	HandlerForException = "SQLEXCEPTION"
)

// DeclareHandler represents a Declare Handler statement in a procedure body.
type DeclareHandler struct {
	HandlerOp  string
	HandlerFor []string
	Block      Block
}

// Format implements the NodeFormatter interface.
func (n *Declaration) Format(ctx *FmtCtx) {
	f := ctx.flags
	ctx.WriteString("DECLARE ")
	if n.IsVar() {
		lex.EncodeRestrictedSQLIdent(&ctx.Buffer, n.Variable.VarName, f.EncodeFlags())
		ctx.WriteString(" ")
		ctx.WriteString(n.Variable.Typ.SQLString())
		if n.Variable.DefaultExpr != nil {
			ctx.WriteString(" DEFAULT ")
			ctx.FormatNode(n.Variable.DefaultExpr)
		}
	} else if n.IsCursor() {
		ctx.FormatNode(&n.Cursor.CurName)
		ctx.WriteString(" CURSOR FOR ")
		ctx.FormatNode(n.Cursor.Body)
	} else {
		ctx.WriteString(n.Handler.HandlerOp)
		ctx.WriteString(" HANDLER FOR ")
		ctx.WriteString(strings.Join(n.Handler.HandlerFor, ","))
		ctx.WriteString("\n\t")
		if n.Handler.Block.Label != "" {
			ctx.WriteString("LABEL ")
			ctx.WriteString(n.Handler.Block.Label)
			ctx.WriteString(":\n\t")
		}
		ctx.WriteString("BEGIN")
		for i := range n.Handler.Block.Body {
			ctx.WriteString("\n\t\t")
			ctx.FormatNode(n.Handler.Block.Body[i])
			ctx.WriteString(";")
		}
		ctx.WriteString("\n\tENDHANDLER")
	}
}

// IsVar return true if the type of Declaration if variable
func (n *Declaration) IsVar() bool {
	return n.Typ == DeclVariable
}

// IsCursor return true if the type of Declaration if cursor
func (n *Declaration) IsCursor() bool {
	return n.Typ == DeclCursor
}

// IsHandler return true if the type of Declaration if handler
func (n *Declaration) IsHandler() bool {
	return n.Typ == DeclHandler
}

// ProcSet represents a SET statement.
type ProcSet struct {
	Name  string
	Value Expr
}

// Format implements the NodeFormatter interface.
func (node *ProcSet) Format(ctx *FmtCtx) {
	f := ctx.flags
	ctx.WriteString("SET ")
	lex.EncodeRestrictedSQLIdent(&ctx.Buffer, node.Name, f.EncodeFlags())
	ctx.WriteString(" = ")
	ctx.FormatNode(node.Value)
}

// CallProcedure represents a Call Procedure statement in a procedure body.
type CallProcedure struct {
	Name         TableName
	Exprs        Exprs
	ReturnedType *types.T
	Parameters   []ProcedureParameter
	IsRows       bool
}

// Format implements the NodeFormatter interface.
func (node *CallProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString("(")
	ctx.FormatNode(&node.Exprs)
	ctx.WriteString(")")
}

// ProcIf is a conditional control statement used in procedure.
type ProcIf struct {
	Condition  Expr
	ThenBody   []Statement
	ElseIfList []ElseIf
	ElseBody   []Statement
}

// Format implements the NodeFormatter interface.
func (node *ProcIf) Format(ctx *FmtCtx) {
	ctx.WriteString("IF ")
	ctx.FormatNode(node.Condition)
	ctx.WriteString(" THEN")
	for _, stmt := range node.ThenBody {
		// TODO: Pretty Print with spaces, not tabs.
		ctx.WriteString("\n\t\t")
		ctx.FormatNode(stmt)
		ctx.WriteString(";")
	}
	for _, elsifStmt := range node.ElseIfList {
		ctx.FormatNode(&elsifStmt)
	}
	for i, elseStmt := range node.ElseBody {
		if i == 0 {
			ctx.WriteString("\n\tELSE")
		}
		ctx.WriteString("\n\t\t")
		ctx.FormatNode(elseStmt)
		ctx.WriteString(";")
	}
	ctx.WriteString("\n\tENDIF")
}

// ElseIf represents an Else If statement in a procedure body.
type ElseIf struct {
	Condition Expr
	Stmts     []Statement
}

// Format implements the NodeFormatter interface.
func (node *ElseIf) Format(ctx *FmtCtx) {
	ctx.WriteString("\n\tELSIF ")
	ctx.FormatNode(node.Condition)
	ctx.WriteString(" THEN")
	for _, stmt := range node.Stmts {
		ctx.WriteString("\n\t\t")
		ctx.FormatNode(stmt)
		ctx.WriteString(";")
	}
}

// ProcWhile is a loop control statement used in procedure.
type ProcWhile struct {
	Label     string
	Condition Expr
	Body      []Statement
}

// Format implements the NodeFormatter interface.
func (s *ProcWhile) Format(ctx *FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("LABEL ")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(":\n\t")
	}
	ctx.WriteString("WHILE ")
	ctx.FormatNode(s.Condition)
	ctx.WriteString(" DO")
	for _, stmt := range s.Body {
		ctx.WriteString("\n\t\t")
		ctx.FormatNode(stmt)
		ctx.WriteString("; ")
	}
	ctx.WriteString("\n\tENDWHILE")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
}

// ProcLoop is a loop control statement used in procedure.
type ProcLoop struct {
	Label string
	Body  []Statement
}

// Format implements the NodeFormatter interface.
func (s *ProcLoop) Format(ctx *FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
	ctx.WriteString("LOOP\n")
	for _, stmt := range s.Body {
		ctx.FormatNode(stmt)
	}
	ctx.WriteString("END LOOP")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	ctx.WriteString(";\n")
}

// ProcCase represents a Case statement in a procedure body.
type ProcCase struct {
	TestExpr     Expr
	CaseWhenList []*CaseWhen
	HaveElse     bool
	ElseStmts    []Statement
}

// Format implements the NodeFormatter interface.
func (s *ProcCase) Format(ctx *FmtCtx) {
	ctx.WriteString("CASE")
	s.TestExpr.Format(ctx)
	ctx.WriteString("\n")
	for _, when := range s.CaseWhenList {
		ctx.FormatNode(when)
	}
	if s.HaveElse {
		ctx.WriteString("ELSE\n")
		for _, stmt := range s.ElseStmts {
			ctx.WriteString("  ")
			ctx.FormatNode(stmt)
		}
	}
	ctx.WriteString("END CASE\n")
}

// CaseWhen represents a Case When statement in a procedure body.
type CaseWhen struct {
	Expr  Expr
	Stmts []Statement
}

// Format implements the NodeFormatter interface.
func (s *CaseWhen) Format(ctx *FmtCtx) {
	ctx.WriteString("WHEN ")
	s.Expr.Format(ctx)
	ctx.WriteString(" THEN\n")
	for i, stmt := range s.Stmts {
		ctx.WriteString("  ")
		ctx.FormatNode(stmt)
		if i != len(s.Stmts)-1 {
			ctx.WriteString("\n")
		}
	}
}

// ProcLeave represents a Leave statement in a procedure body.
type ProcLeave struct {
	Label string
}

// Format implements the NodeFormatter interface.
func (s *ProcLeave) Format(ctx *FmtCtx) {
	ctx.WriteString("LEAVE ")
	ctx.WriteString(s.Label)
}

// ProcedureTxnState is state of transaction
type ProcedureTxnState int

const (
	// ProcedureTransactionDefault Default
	ProcedureTransactionDefault ProcedureTxnState = iota
	// ProcedureTransactionStart Commit
	ProcedureTransactionStart
	// ProcedureTransactionCommit Commit
	ProcedureTransactionCommit
	// ProcedureTransactionRollBack RollBack
	ProcedureTransactionRollBack
)
