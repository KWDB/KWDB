// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

var (
	alters               = append(altersTableExistence, altersExistingTable...)
	altersTableExistence = []statementWeight{
		{10, makeCreateTable},
		{1, makeDropTable},
	}
	altersExistingTable = []statementWeight{
		{5, makeRenameTable},

		{10, makeAddColumn},
		{10, makeJSONComputedColumn},
		{10, makeAlterPrimaryKey},
		{1, makeDropColumn},
		{5, makeRenameColumn},
		{5, makeAlterColumnType},

		{10, makeCreateIndex},
		{1, makeDropIndex},
		{5, makeRenameIndex},
	}
)

func makeAlter(s *Smither) (tree.Statement, bool) {
	if s.canRecurse() {
		// Schema changes aren't visible immediately, so try to
		// sync the change from the last alter before trying the
		// next one. This is instead of running ReloadSchemas right
		// after the alter succeeds (which doesn't always pick
		// up the change). This has the added benefit of leaving
		// behind old column references for a bit, which should
		// test some additional logic.
		_ = s.ReloadSchemas()
		for i := 0; i < retryCount; i++ {
			stmt, ok := s.alterSampler.Next()(s)
			if ok {
				return stmt, ok
			}
		}
	}
	return nil, false
}

func makeCreateTable(s *Smither) (tree.Statement, bool) {
	table := sqlbase.RandCreateTable(s.rnd, "", 0)
	table.Table = tree.MakeUnqualifiedTableName(s.name("tab"))
	return table, true
}

func makeDropTable(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	return &tree.DropTable{
		Names:        tree.TableNames{*tableRef.TableName},
		DropBehavior: s.randDropBehavior(),
	}, true
}

func makeRenameTable(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	newName, err := tree.NewUnresolvedObjectName(
		1 /* numParts */, [3]string{string(s.name("tab"))}, tree.NoAnnotation,
	)
	if err != nil {
		return nil, false
	}

	return &tree.RenameTable{
		Name:    tableRef.TableName.ToUnresolvedObjectName(),
		NewName: newName,
	}, true
}

func makeRenameColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.RenameColumn{
		Table:   *tableRef.TableName,
		Name:    col.Name,
		NewName: s.name("col"),
	}, true
}

func makeAlterColumnType(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	typ := sqlbase.RandColumnType(s.rnd)
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAlterColumnType{
				Column: col.Name,
				ToType: typ,
			},
		},
	}, true
}

func makeAddColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, colRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	colRefs.stripTableName()
	t := sqlbase.RandColumnType(s.rnd)
	col, err := tree.NewColumnTableDef(s.name("col"), t, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Nullable.Nullability = s.randNullability()
	if s.coin() {
		col.DefaultExpr.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, nil)}
	} else if s.coin() {
		col.Computed.Computed = true
		col.Computed.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, colRefs)}
	}
	for s.coin() {
		col.CheckExprs = append(col.CheckExprs, tree.ColumnTableDefCheckExpr{
			Expr: makeBoolExpr(s, colRefs),
		})
	}

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: col,
			},
		},
	}, true
}

func makeJSONComputedColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, colRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	colRefs.stripTableName()
	// Shuffle columns and find the first one that's JSON.
	s.rnd.Shuffle(len(colRefs), func(i, j int) {
		colRefs[i], colRefs[j] = colRefs[j], colRefs[i]
	})
	var ref *colRef
	for _, c := range colRefs {
		if c.typ.Family() == types.JsonFamily {
			ref = c
			break
		}
	}
	// If we didn't find any JSON columns, return.
	if ref == nil {
		return nil, false
	}
	col, err := tree.NewColumnTableDef(s.name("col"), types.Jsonb, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Computed.Computed = true
	col.Computed.Expr = tree.NewTypedBinaryExpr(tree.JSONFetchText, ref.typedExpr(), sqlbase.RandDatumSimple(s.rnd, types.String), types.String)

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: col,
			},
		},
	}, true
}

func makeDropColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableDropColumn{
				Column:       col.Name,
				DropBehavior: s.randDropBehavior(),
			},
		},
	}, true
}

func makeAlterPrimaryKey(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	// Collect all columns that are NOT NULL to be candidate new primary keys.
	var candidateColumns tree.IndexElemList
	for _, c := range tableRef.Columns {
		if c.Nullable.Nullability == tree.NotNull {
			candidateColumns = append(candidateColumns, tree.IndexElem{Column: c.Name})
		}
	}
	if len(candidateColumns) == 0 {
		return nil, false
	}
	s.rnd.Shuffle(len(candidateColumns), func(i, j int) {
		candidateColumns[i], candidateColumns[j] = candidateColumns[j], candidateColumns[i]
	})
	// Pick some randomly short prefix of the candidate columns as a potential new primary key.
	i := 1
	for len(candidateColumns) > i && s.rnd.Intn(2) == 0 {
		i++
	}
	candidateColumns = candidateColumns[:i]
	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAlterPrimaryKey{
				Columns: candidateColumns,
			},
		},
	}, true
}

func makeCreateIndex(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	var cols tree.IndexElemList
	seen := map[tree.Name]bool{}
	inverted := false
	unique := s.coin()
	for len(cols) < 1 || s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		// If this is the first column and it's invertable (i.e., JSONB), make an inverted index.
		if len(cols) == 0 && sqlbase.ColumnTypeIsInvertedIndexable(col.Type) {
			inverted = true
			unique = false
			cols = append(cols, tree.IndexElem{
				Column: col.Name,
			})
			break
		}
		if sqlbase.ColumnTypeIsIndexable(col.Type) {
			cols = append(cols, tree.IndexElem{
				Column:    col.Name,
				Direction: s.randDirection(),
			})
		}
	}
	var storing tree.NameList
	for !inverted && s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		storing = append(storing, col.Name)
	}

	return &tree.CreateIndex{
		Name:         s.name("idx"),
		Table:        *tableRef.TableName,
		Unique:       unique,
		Columns:      cols,
		Storing:      storing,
		Inverted:     inverted,
		Concurrently: s.coin(),
	}, true
}

func makeDropIndex(s *Smither) (tree.Statement, bool) {
	tin, _, _, ok := s.getRandIndex()
	return &tree.DropIndex{
		IndexList:    tree.TableIndexNames{tin},
		DropBehavior: s.randDropBehavior(),
		Concurrently: s.coin(),
	}, ok
}

func makeRenameIndex(s *Smither) (tree.Statement, bool) {
	tin, _, _, ok := s.getRandIndex()
	return &tree.RenameIndex{
		Index:   tin,
		NewName: tree.UnrestrictedName(s.name("idx")),
	}, ok
}
