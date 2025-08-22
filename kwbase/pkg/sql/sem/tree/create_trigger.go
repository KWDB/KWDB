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

// SetValueMode represents value mode
type SetValueMode uint8

const (
	// DeclareValue represents procedure declare value
	DeclareValue SetValueMode = iota
	// InternalValue represents procedure set local value
	InternalValue
	// ExternalValue represents set external value, for trigger(new/old)
	ExternalValue
)

// CreateTrigger represents a CREATE TRIGGER statement.
type CreateTrigger struct {
	Name        Name
	IfNotExists bool
	ActionTime  TriggerActionTime
	Event       TriggerEvent
	TableName   TableName
	Order       *TriggerOrder
	Body        TriggerBody
	BodyStr     string
}

// Format implements the NodeFormatter interface.
func (node *CreateTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE TRIGGER ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	ctx.FormatNode(&node.ActionTime)
	ctx.WriteString(" ")
	ctx.FormatNode(&node.Event)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.TableName)
	ctx.WriteString(" FOR EACH ROW ")
	if node.Order != nil {
		ctx.FormatNode(node.Order)
	}
	if len(node.Body.Body) == 1 {
		ctx.WriteString("\nBEGIN\n")
		ctx.FormatNode(node.Body.Body[0])
		ctx.WriteString(";\n")
		ctx.WriteString("END;")
	} else {
		ctx.WriteString("\nBEGIN\n")
		for idx := range node.Body.Body {
			ctx.FormatNode(node.Body.Body[idx])
			ctx.WriteString(";\n")
		}
		ctx.WriteString("END;")
	}
}

// CreateTriggerPG represents a CREATE TRIGGER stmt in PG style ($$ trigger_body $$), which is
// used to be compatible with JDBC interface
type CreateTriggerPG struct {
	Name        Name
	IfNotExists bool
	ActionTime  TriggerActionTime
	Event       TriggerEvent
	TableName   TableName
	Order       *TriggerOrder
	BodyStr     string
}

// Format implements the NodeFormatter interface.
func (node *CreateTriggerPG) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE TRIGGER ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	ctx.FormatNode(&node.ActionTime)
	ctx.WriteString(" ")
	ctx.FormatNode(&node.Event)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.TableName)
	ctx.WriteString(" FOR EACH ROW ")
	if node.Order != nil {
		ctx.FormatNode(node.Order)
	}
	ctx.WriteString("$$")
	ctx.WriteString(node.BodyStr)
	ctx.WriteString("$$")
}

// TriggerActionTime represents trigger's action time(BEFORE/AFTER).
type TriggerActionTime uint8

const (
	// TriggerActionTimeUnknown is default
	TriggerActionTimeUnknown TriggerActionTime = iota
	// TriggerActionTimeBefore is BEFORE
	TriggerActionTimeBefore
	// TriggerActionTimeAfter is AFTER
	TriggerActionTimeAfter
)

// Format implements the NodeFormatter interface.
func (node *TriggerActionTime) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerActionTimeUnknown:
		ctx.WriteString("UNKNOWN")
	case TriggerActionTimeBefore:
		ctx.WriteString("BEFORE")
	case TriggerActionTimeAfter:
		ctx.WriteString("AFTER")
	}
}

// TriggerEvent represents trigger's event(INSERT/UPDATE/DELETE).
type TriggerEvent uint8

const (
	// TriggerEventTypeUnknown is default.
	TriggerEventTypeUnknown TriggerEvent = iota
	// TriggerEventInsert is INSERT
	TriggerEventInsert
	// TriggerEventUpdate is UPDATE
	TriggerEventUpdate
	// TriggerEventDelete is DELETE
	TriggerEventDelete
)

// Format implements the NodeFormatter interface.
func (node *TriggerEvent) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerEventTypeUnknown:
		ctx.WriteString("UNKNOWN")
	case TriggerEventInsert:
		ctx.WriteString("INSERT")
	case TriggerEventUpdate:
		ctx.WriteString("UPDATE")
	case TriggerEventDelete:
		ctx.WriteString("DELETE")
	}
}

// String implements the Stringer interface.
func (node *TriggerEvent) String() string {
	return AsString(node)
}

// TriggerOrderType represents trigger's order (FOLLOW/PRECEDE)
type TriggerOrderType uint8

const (
	// TriggerOrderTypeUnknown is default
	TriggerOrderTypeUnknown TriggerOrderType = iota
	// TriggerOrderTypeFollow is FOLLOW
	TriggerOrderTypeFollow
	// TriggerOrderTypePrecede is PRECEDE
	TriggerOrderTypePrecede
)

// Format implements the NodeFormatter interface.
func (node *TriggerOrderType) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerOrderTypeUnknown:
		ctx.WriteString("UNKNOWN")
	case TriggerOrderTypeFollow:
		ctx.WriteString("FOLLOWS")
	case TriggerOrderTypePrecede:
		ctx.WriteString("PRECEDES")
	}
}

// TriggerOrder represents trigger order in CREATE TRIGGER statement.
type TriggerOrder struct {
	// OrderType is FOLLOW/PRECEDE
	OrderType TriggerOrderType
	// OtherTrigger is the referenced trigger's name.
	OtherTrigger Name
}

// Format implements the NodeFormatter interface.
func (node *TriggerOrder) Format(ctx *FmtCtx) {
	node.OrderType.Format(ctx)
	ctx.WriteString(" " + node.OtherTrigger.String())
}

// TriggerBody represents trigger's body in CREATE TRIGGER statement.
type TriggerBody struct {
	// Body stores sql stmts in BEGIN ... END
	Body []Statement
	//  Body stores sql string
	BodyStr string
}
