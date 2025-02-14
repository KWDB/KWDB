// Copyright 2018 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// optTableUpserter implements the upsert operation when it is planned by the
// cost-based optimizer (CBO). The CBO can use a much simpler upserter because
// it incorporates conflict detection, update and computed column evaluation,
// and other upsert operations into the input query, rather than requiring the
// upserter to do it. For example:
//
//	CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//	INSERT INTO abc VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b=10
//
// The CBO will generate an input expression similar to this:
//
//	SELECT ins_a, ins_b, ins_c, fetch_a, fetch_b, fetch_c, 10 AS upd_b
//	FROM (VALUES (1, 2, NULL)) AS ins(ins_a, ins_b, ins_c)
//	LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//	ON ins_a = fetch_a
//
// The other non-CBO upserters perform custom left lookup joins. However, that
// doesn't allow sharing of optimization rules and doesn't work with correlated
// SET expressions.
//
// For more details on how the CBO compiles UPSERT statements, see the block
// comment on Builder.buildInsert in opt/optbuilder/insert.go.
type optTableUpserter struct {
	tableWriterBase

	ri row.Inserter

	// Should we collect the rows for a RETURNING clause?
	collectRows bool

	// Rows returned if collectRows is true.
	rowsUpserted *rowcontainer.RowContainer

	// A mapping of column IDs to the return index used to shape the resulting
	// rows to those required by the returning clause. Only required if
	// collectRows is true.
	colIDToReturnIndex map[sqlbase.ColumnID]int

	// Do the result rows have a different order than insert rows. Only set if
	// collectRows is true.
	insertReorderingRequired bool

	// rowsInLastProcessedBatch tracks the number of upserts that were
	// performed in the last processed batch. If collectRows is true, it will
	// be equal to rowsUpserted.Len() after the batch has been created.
	rowsInLastProcessedBatch int

	// fetchCols indicate which columns need to be fetched from the target table,
	// in order to detect whether a conflict has occurred, as well as to provide
	// existing values for updates.
	fetchCols []sqlbase.ColumnDescriptor

	// updateCols indicate which columns need an update during a conflict.
	updateCols []sqlbase.ColumnDescriptor

	// returnCols indicate which columns need to be returned by the Upsert.
	returnCols []sqlbase.ColumnDescriptor

	// canaryOrdinal is the ordinal position of the column within the input row
	// that is used to decide whether to execute an insert or update operation.
	// If the canary column is null, then an insert will be performed; otherwise,
	// an update is performed. This column will always be one of the fetchCols.
	canaryOrdinal int

	// resultRow is a reusable slice of Datums used to store result rows.
	resultRow tree.Datums

	// fkTables is used for foreign key checks in the update case.
	fkTables row.FkTableMetadata

	// ru is used when updating rows.
	ru row.Updater

	// tabColIdxToRetIdx is the mapping from the columns in the table to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the table column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column of the table is
	// to be returned.
	tabColIdxToRetIdx []int
}

var _ tableWriter = &optTableUpserter{}

// init is part of the tableWriter interface.
func (tu *optTableUpserter) init(
	ctx context.Context, txn *kv.Txn, evalCtx *tree.EvalContext,
) error {
	tu.tableWriterBase.init(txn)

	// collectRows, set upon initialization, indicates whether or not we want
	// rows returned from the operation.
	if tu.collectRows {
		tu.resultRow = make(tree.Datums, len(tu.returnCols))
		tu.rowsUpserted = rowcontainer.NewRowContainer(
			evalCtx.Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromColDescs(tu.returnCols),
			0, /* rowCapacity */
		)

		// Create the map from colIds to the expected columns.
		// Note that this map will *not* contain any mutation columns - that's
		// because even though we might insert values into mutation columns, we
		// never return them back to the user.
		tu.colIDToReturnIndex = map[sqlbase.ColumnID]int{}
		tableDesc := tu.tableDesc()
		for i := range tableDesc.Columns {
			id := tableDesc.Columns[i].ID
			tu.colIDToReturnIndex[id] = i
		}

		if len(tu.ri.InsertColIDtoRowIndex) == len(tu.colIDToReturnIndex) {
			for colID, insertIndex := range tu.ri.InsertColIDtoRowIndex {
				resultIndex, ok := tu.colIDToReturnIndex[colID]
				if !ok || resultIndex != insertIndex {
					tu.insertReorderingRequired = true
					break
				}
			}
		} else {
			tu.insertReorderingRequired = true
		}
	}

	return nil
}

// flushAndStartNewBatch is part of the tableWriter interface.
func (tu *optTableUpserter) flushAndStartNewBatch(ctx context.Context) error {
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// close is part of the tableWriter interface.
func (tu *optTableUpserter) close(ctx context.Context) {
	if tu.rowsUpserted != nil {
		tu.rowsUpserted.Close(ctx)
	}
}

// finalize is part of the tableWriter interface.
func (tu *optTableUpserter) finalize(
	ctx context.Context, traceKV bool,
) (*rowcontainer.RowContainer, error) {
	return nil, tu.tableWriterBase.finalize(ctx, tu.tableDesc())
}

// makeResultFromRow reshapes a row that was inserted or updated to a row
// suitable for storing for a RETURNING clause, shaped by the target table's
// descriptor.
// There are two main examples of this reshaping:
// 1) A row may not contain values for nullable columns, so insert those NULLs.
// 2) Don't return values we wrote into non-public mutation columns.
func (tu *optTableUpserter) makeResultFromRow(
	row tree.Datums, colIDToRowIndex map[sqlbase.ColumnID]int,
) tree.Datums {
	resultRow := make(tree.Datums, len(tu.colIDToReturnIndex))
	for colID, returnIndex := range tu.colIDToReturnIndex {
		rowIndex, ok := colIDToRowIndex[colID]
		if ok {
			resultRow[returnIndex] = row[rowIndex]
		} else {
			// If the row doesn't have all columns filled out. Fill the columns that
			// weren't included with NULLs. This will only be true for nullable
			// columns.
			resultRow[returnIndex] = tree.DNull
		}
	}
	return resultRow
}

// desc is part of the tableWriter interface.
func (*optTableUpserter) desc() string { return "opt upserter" }

// row is part of the tableWriter interface.
func (tu *optTableUpserter) row(ctx context.Context, row tree.Datums, traceKV bool) error {
	tu.batchSize++

	// Consult the canary column to determine whether to insert or update. For
	// more details on how canary columns work, see the block comment on
	// Builder.buildInsert in opt/optbuilder/insert.go.
	insertEnd := len(tu.ri.InsertCols)
	if tu.canaryOrdinal == -1 {
		// No canary column means that existing row should be overwritten (i.e.
		// the insert and update columns are the same, so no need to choose).
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], true /* overwrite */, traceKV)
	}
	if row[tu.canaryOrdinal] == tree.DNull {
		// No conflict, so insert a new row.
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], false /* overwrite */, traceKV)
	}

	// If no columns need to be updated, then possibly collect the unchanged row.
	fetchEnd := insertEnd + len(tu.fetchCols)
	if len(tu.updateCols) == 0 {
		if !tu.collectRows {
			return nil
		}
		_, err := tu.rowsUpserted.AddRow(ctx, row[insertEnd:fetchEnd])
		return err
	}

	// Update the row.
	updateEnd := fetchEnd + len(tu.updateCols)
	return tu.updateConflictingRow(
		ctx,
		tu.b,
		row[insertEnd:fetchEnd],
		row[fetchEnd:updateEnd],
		tu.tableDesc(),
		traceKV,
	)
}

// atBatchEnd is part of the tableWriter interface.
func (tu *optTableUpserter) atBatchEnd(ctx context.Context, traceKV bool) error {
	// Nothing to do, because the row method does everything.
	return nil
}

// insertNonConflictingRow inserts the given source row into the table when
// there was no conflict. If the RETURNING clause was specified, then the
// inserted row is stored in the rowsUpserted collection.
func (tu *optTableUpserter) insertNonConflictingRow(
	ctx context.Context, b *kv.Batch, insertRow tree.Datums, overwrite, traceKV bool,
) error {
	// Perform the insert proper.
	if err := tu.ri.InsertRow(
		ctx, b, insertRow, overwrite, row.CheckFKs, traceKV, tu.txn); err != nil {
		return err
	}

	if !tu.collectRows {
		return nil
	}

	// Reshape the row if needed.
	if tu.insertReorderingRequired {
		tableRow := tu.makeResultFromRow(insertRow, tu.ri.InsertColIDtoRowIndex)

		// TODO(ridwanmsharif): Why didn't they update the value of tu.resultRow
		//  before? Is it safe to be doing it now?
		// Map the upserted columns into the result row before adding it.
		for tabIdx := range tableRow {
			if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
				tu.resultRow[retIdx] = tableRow[tabIdx]
			}
		}
		_, err := tu.rowsUpserted.AddRow(ctx, tu.resultRow)
		return err
	}

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range insertRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = insertRow[tabIdx]
		}
	}
	_, err := tu.rowsUpserted.AddRow(ctx, tu.resultRow)
	return err
}

// updateConflictingRow updates an existing row in the table when there was a
// conflict. The existing values from the row are provided in fetchRow, and the
// updated values are provided in updateValues. The updater is assumed to
// already be initialized with the descriptors for the fetch and update values.
// If the RETURNING clause was specified, then the updated row is stored in the
// rowsUpserted collection.
func (tu *optTableUpserter) updateConflictingRow(
	ctx context.Context,
	b *kv.Batch,
	fetchRow tree.Datums,
	updateValues tree.Datums,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	// Enforce the column constraints.
	// Note: the column constraints are already enforced for fetchRow,
	// because:
	// - for the insert part, they were checked upstream in upsertNode
	//   via GenerateInsertRow().
	// - for the fetched part, we assume that the data in the table is
	//   correct already.
	if err := enforceLocalColumnConstraints(updateValues, tu.updateCols); err != nil {
		return err
	}

	// Queue the update in KV. This also returns an "update row"
	// containing the updated values for every column in the
	// table. This is useful for RETURNING, which we collect below.
	_, err := tu.ru.UpdateRow(ctx, b, fetchRow, updateValues, row.CheckFKs, traceKV, &kv.Txn{})
	if err != nil {
		return err
	}

	// We only need a result row if we're collecting rows.
	if !tu.collectRows {
		return nil
	}

	// We now need a row that has the shape of the result row with
	// the appropriate return columns. Make sure all the fetch columns
	// are present.
	tableRow := tu.makeResultFromRow(fetchRow, tu.ru.FetchColIDtoRowIndex)

	// Make sure all the updated columns are present.
	for colID, returnIndex := range tu.colIDToReturnIndex {
		// If an update value for a given column exists, use that; else use the
		// existing value of that column if it has been fetched.
		rowIndex, ok := tu.ru.UpdateColIDtoRowIndex[colID]
		if ok {
			tableRow[returnIndex] = updateValues[rowIndex]
		}
	}

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range tableRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = tableRow[tabIdx]
		}
	}

	// The resulting row may have nil values for columns that aren't
	// being upserted, updated or fetched.
	_, err = tu.rowsUpserted.AddRow(ctx, tu.resultRow)
	return err
}

// tableDesc is part of the tableWriter interface.
func (tu *optTableUpserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (tu *optTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
}
