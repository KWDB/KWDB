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

package sql

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

var errEmptyColumnName = pgerror.New(pgcode.Syntax, "empty column name")

type renameColumnNode struct {
	n         *tree.RenameColumn
	tableDesc *sqlbase.MutableTableDescriptor
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	// Check if table exists.
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &renameColumnNode{n: n, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME COLUMN performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameColumnNode) ReadingOwnWrites() {}

func (n *renameColumnNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc

	const allowRenameOfShardColumn = false
	descChanged, err := params.p.renameColumn(params.ctx, tableDesc, &n.n.Name,
		&n.n.NewName, allowRenameOfShardColumn, false)
	if err != nil {
		return err
	}

	if !descChanged {
		return nil
	}

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

// renameColumn will rename the column in tableDesc from oldName to newName.
// If allowRenameOfShardColumn is false, this method will return an error if
// the column being renamed is a generated column for a hash sharded index.
func (p *planner) renameColumn(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	oldName, newName *tree.Name,
	allowRenameOfShardColumn bool,
	isTag bool,
) (changed bool, err error) {

	if tableDesc.TableType == tree.InstanceTable {
		return false, pgerror.Newf(
			pgcode.WrongObjectType, "can not rename column/tag on instance table \"%s\"", tableDesc.Name)
	}
	if tableDesc.IsTSTable() {
		if len(string(*newName)) > MaxTagNameLength {
			return false, sqlbase.NewTSNameOutOfLengthError("column/tag", string(*newName), MaxTagNameLength)
		}
		if sqlbase.ContainsNonAlphaNumSymbol(string(*newName)) {
			return false, sqlbase.NewTSColInvalidError(string(*newName))
		}
	}

	if *newName == "" {
		return false, errEmptyColumnName
	}

	col, _, err := tableDesc.FindColumnByName(*oldName)
	if err != nil {
		return false, err
	}
	typName := "column"
	if isTag {
		if !col.IsTagCol() {
			return false, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a tag", col.Name)
		}
		typName = "tag"
	} else {
		if col.IsTagCol() {
			return false, pgerror.Newf(pgcode.WrongObjectType,
				"%q is a tag", col.Name)
		}
	}
	if col.IsPrimaryTagCol() {
		return false, pgerror.Newf(pgcode.WrongObjectType, "can not rename primary tag name: %s", oldName)
	}
	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return false, p.dependentViewRenameError(
				ctx, typName, oldName.String(), tableDesc.ParentID, tableRef.ID)
		}
	}
	if *oldName == *newName {
		// Noop.
		return false, nil
	}
	isShardColumn := tableDesc.IsShardColumn(col)
	if isShardColumn && !allowRenameOfShardColumn {
		return false, pgerror.Newf(pgcode.ReservedName, "cannot rename shard column: %s", col)
	}

	if _, _, err := tableDesc.FindColumnByName(*newName); err == nil {
		return false, fmt.Errorf("column/tag name %q already exists", tree.ErrString(newName))
	}

	// preFn is used to change name in computed expr of column desc
	// e.g. create table test(a int as (b + c), b int, c int);
	// alter table test rename b to d
	// result: column a int as (b + c) --> column a int as (d + c)
	preFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(*oldName) {
					c.ColumnName = *newName
				}
			}
			return false, v, nil
		}
		return true, expr, nil
	}

	renameIn := func(expression string) (string, error) {
		parsed, err := parser.ParseExpr(expression)
		if err != nil {
			return "", err
		}

		renamed, err := tree.SimpleVisit(parsed, preFn)
		if err != nil {
			return "", err
		}

		return renamed.String(), nil
	}

	// Rename the column in CHECK constraints.
	// Renaming columns that are being referenced by checks that are being added is not allowed.
	for i := range tableDesc.Checks {
		var err error
		tableDesc.Checks[i].Expr, err = renameIn(tableDesc.Checks[i].Expr)
		if err != nil {
			return false, err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if otherCol := &tableDesc.Columns[i]; otherCol.IsComputed() {
			newExpr, err := renameIn(*otherCol.ComputeExpr)
			if err != nil {
				return false, err
			}
			otherCol.ComputeExpr = &newExpr
		}
	}

	// Rename the column in hash-sharded index descriptors. Potentially rename the
	// shard column too if we haven't already done it.
	shardColumnsToRename := make(map[tree.Name]tree.Name) // map[oldShardColName]newShardColName
	maybeUpdateShardedDesc := func(shardedDesc *sqlbase.ShardedDescriptor) {
		if !shardedDesc.IsSharded {
			return
		}
		oldShardColName := tree.Name(sqlbase.GetShardColumnName(
			shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
		var changed bool
		for i, c := range shardedDesc.ColumnNames {
			if c == string(*oldName) {
				changed = true
				shardedDesc.ColumnNames[i] = string(*newName)
			}
		}
		if !changed {
			return
		}
		newName, alreadyRenamed := shardColumnsToRename[oldShardColName]
		if !alreadyRenamed {
			newName = tree.Name(sqlbase.GetShardColumnName(
				shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
			shardColumnsToRename[oldShardColName] = newName
		}
		// Keep the shardedDesc name in sync with the column name.
		shardedDesc.Name = string(newName)
	}
	for _, idx := range tableDesc.AllNonDropIndexes() {
		maybeUpdateShardedDesc(&idx.Sharded)
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(*newName))

	// Rename any shard columns which need to be renamed because their name was
	// based on this column.
	for oldShardColName, newShardColName := range shardColumnsToRename {
		// Recursively call p.renameColumn. We don't need to worry about deeper than
		// one recursive call because shard columns cannot refer to each other.
		const allowRenameOfShardColumn = true
		_, err = p.renameColumn(ctx, tableDesc, &oldShardColName, &newShardColName,
			allowRenameOfShardColumn, isTag)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}
