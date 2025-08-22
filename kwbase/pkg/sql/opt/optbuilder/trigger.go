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

package optbuilder

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

const triggerColNew = "new"
const triggerColOld = "old"

type colNewOldType int

const (
	// 0000 0001  -> newCol
	// 0000 0010  -> oldCol
	// 0000 0011  -> newCol, oldCol
	newCol colNewOldType = 1 << iota
	oldCol
)

// TriggerTab represents the metadata of the target table. It is used to resolve NEW/OLD.
type TriggerTab struct {
	ColMetas       []opt.ColumnMeta
	CurTriggerTyp  tree.TriggerEvent
	CurActionTime  tree.TriggerActionTime
	PlaceholderTyp tree.PlaceholderTypes
}

// build each stmt in trigger body
func (mb *mutationBuilder) buildProcCommandForTriggers(
	eventType tree.TriggerEvent,
) *memo.ArrayCommand {
	eventTriggers := mb.tab.GetTriggers(eventType)
	var currentTabID cat.StableID
	var triggerCommands memo.ArrayCommand
	if len(eventTriggers) == 0 {
		return &triggerCommands
	}
	currentTabID = mb.tab.ID()
	if _, ok := mb.b.TriggerInfo.TriggerTab[currentTabID]; ok {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, fmt.Sprintf("Can't update table %s in trigger because"+
			" it is already used by statement which invoked this trigger.", string(mb.tab.Name()))))
	}

	mb.b.insideObjectDef.SetFlags(InsideTriggerDef)
	// store table meta in current building phase
	TriggerTableCols := make([]opt.ColumnMeta, 0)
	pTyp := make(tree.PlaceholderTypes, 0)
	for i := 0; i < mb.tab.ColumnCount(); i++ {
		TriggerTableCols = append(TriggerTableCols, opt.ColumnMeta{
			MetaID: opt.ColumnID(mb.tab.Column(i).ColID()),
			Type:   mb.tab.Column(i).DatumType(),
			Table:  opt.TableID(currentTabID),
			Name:   string(mb.tab.Column(i).ColName()),
		})
		pTyp = append(pTyp, mb.tab.Column(i).DatumType())
	}
	mb.b.TriggerInfo.TriggerTab[currentTabID] = TriggerTab{
		ColMetas:       TriggerTableCols,
		CurTriggerTyp:  eventType,
		PlaceholderTyp: pTyp,
	}

	// convert AST to procCommand，which is used to generate instructions (in execution phase)
	for _, trig := range eventTriggers {
		blockComm := &memo.ArrayCommand{}
		stmt, err := parser.ParseOne(trig.Body)
		if err != nil {
			panic(err)
		}
		createTriggerStmt, ok := stmt.AST.(*tree.CreateTrigger)
		if !ok {
			panic(stmt.SQL)
		}
		for _, sql := range createTriggerStmt.Body.Body {
			mb.b.TriggerInfo.CurTriggerTabID = currentTabID
			mb.b.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = mb.b.TriggerInfo.TriggerTab[currentTabID].PlaceholderTyp
			blockComm.Bodys = append(blockComm.Bodys, mb.b.buildProcCommand(sql, nil, mb.outScope, nil))
		}
		triggerCommands.Bodys = append(triggerCommands.Bodys, blockComm)
	}
	delete(mb.b.TriggerInfo.TriggerTab, currentTabID)
	mb.b.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = nil
	return &triggerCommands
}

func (b *Builder) isTriggerUpdate() bool {
	return b.TriggerInfo.TriggerTab[b.TriggerInfo.CurTriggerTabID].CurTriggerTyp == tree.TriggerEventUpdate
}
