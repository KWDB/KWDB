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

package sql

import (
	"context"
	"io/ioutil"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// CreateImportPortal returns a importPortalNode.
func (p *GenericPlanner) CreateImportPortal(
	ctx context.Context, n *tree.ImportPortal,
) (PlanNode, error) {
	return &spoolNode{source: &serializeNode{
		source: &importPortalNode{
			ip:      *n,
			columns: sqlbase.ImportPortalColumns,
		}}}, nil
}

type importPortalNode struct {
	ip      tree.ImportPortal
	columns sqlbase.ResultColumns
	rows    *rowcontainer.RowContainer
	// values is going to be returned
	values []tree.Datums
	// Whether all values have been returned
	done bool
}

func (n *importPortalNode) StartExec(params RunParams) error {
	ctx, p := params.Ctx, params.p
	createFileFn, err := TypeAsString(p, n.ip.File, "IMPORT")
	if err != nil {
		return err
	}
	filename, err := createFileFn()
	SQLCounts := 0
	if err != nil {
		return err
	}
	store, err := p.execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, filename)
	if err != nil {
		return err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	stmts, err := parser.Parse(string(tableDefStr))
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		isKwdbSchema := true
		switch stmt.AST.(type) {
		default:
			isKwdbSchema = false
		}
		if isKwdbSchema {
			SQLCounts++
		}
	}

	// Construct the result set returned by the client
	n.values = append(n.values, tree.Datums{
		tree.NewDInt(tree.DInt(SQLCounts)), //kwdb schema SQL
	})
	n.rows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns), 0)
	return nil
}

func (n *importPortalNode) Next(params RunParams) (bool, error) { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (n *importPortalNode) BatchedNext(params RunParams) (bool, error) {
	// Advance one batch. First, clear the current batch.
	if n.done {
		return false, nil
	}
	if n.rows != nil {
		n.rows.Clear(params.Ctx)
	}
	// If result rows need to be accumulated, do it.
	if n.rows != nil {
		for _, value := range n.values {
			_, err := n.rows.AddRow(params.Ctx, value)
			if err != nil {
				return false, err
			}
		}
		n.done = true
	}
	return n.done, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (n *importPortalNode) BatchedCount() int { return len(n.values) }

// BatchedCount implements the batchedPlanNode interface.
func (n *importPortalNode) BatchedValues(rowIdx int) tree.Datums { return n.rows.At(rowIdx) }

func (n *importPortalNode) Values() tree.Datums { panic("not valid") }

func (n *importPortalNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
		n.rows = nil
	}
}
