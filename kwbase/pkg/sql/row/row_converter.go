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

package row

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/transform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// KVInserter implements the putter interface.
type KVInserter func(roachpb.KeyValue)

// CPut is not implmented.
func (i KVInserter) CPut(key, value interface{}, expValue *roachpb.Value) {
	panic("unimplemented")
}

// Del is not implemented.
func (i KVInserter) Del(key ...interface{}) {
	// This is called when there are multiple column families to ensure that
	// existing data is cleared. With the exception of IMPORT INTO, the entire
	// existing keyspace in any IMPORT is guaranteed to be empty, so we don't have
	// to worry about it.
	//
	// IMPORT INTO disallows overwriting an existing row, so we're also okay here.
	// The reason this works is that row existence is precisely defined as whether
	// column family 0 exists, meaning that we write column family 0 even if all
	// the non-pk columns in it are NULL. It follows that either the row does
	// exist and the imported column family 0 will conflict (and the IMPORT INTO
	// will fail) or the row does not exist (and thus the column families are all
	// empty).
}

// Put method of the putter interface.
func (i KVInserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// InitPut method of the putter interface.
func (i KVInserter) InitPut(key, value interface{}, failOnTombstones bool) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// GenerateInsertRow prepares a row tuple for insertion. It fills in default
// expressions, verifies non-nullable columns, and checks column widths.
//
// The result is a row tuple providing values for every column in insertCols.
// This results contains:
//
//   - the values provided by rowVals, the tuple of source values. The
//     caller ensures this provides values 1-to-1 to the prefix of
//     insertCols that was specified explicitly in the INSERT statement.
//   - the default values for any additional columns in insertCols that
//     have default values in defaultExprs.
//   - the computed values for any additional columns in insertCols
//     that are computed. The mapping in rowContainerForComputedCols
//     maps the indexes of the comptuedCols/computeExpr slices
//     back into indexes in the result row tuple.
func GenerateInsertRow(
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	insertCols []sqlbase.ColumnDescriptor,
	computedCols []sqlbase.ColumnDescriptor,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	rowVals tree.Datums,
	rowContainerForComputedVals *sqlbase.RowIndexedVarContainer,
) (tree.Datums, error) {
	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions. This will not happen if the row tuple was produced
	// by a ValuesClause, because all default expressions will have been populated
	// already by fillDefaults.
	if len(rowVals) < len(insertCols) {
		// It's not cool to append to the slice returned by a node; make a copy.
		oldVals := rowVals
		rowVals = make(tree.Datums, len(insertCols))
		copy(rowVals, oldVals)

		for i := len(oldVals); i < len(insertCols); i++ {
			if defaultExprs == nil {
				rowVals[i] = tree.DNull
				continue
			}
			d, err := defaultExprs[i].Eval(evalCtx)
			if err != nil {
				return nil, err
			}
			rowVals[i] = d
		}
	}

	// Generate the computed values, if needed.
	if len(computeExprs) > 0 {
		rowContainerForComputedVals.CurSourceRow = rowVals
		evalCtx.PushIVarContainer(rowContainerForComputedVals)
		for i := range computedCols {
			// Note that even though the row is not fully constructed at this point,
			// since we disallow computed columns from referencing other computed
			// columns, all the columns which could possibly be referenced *are*
			// available.
			d, err := computeExprs[i].Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err, "computed column %s", tree.ErrString((*tree.Name)(&computedCols[i].Name)))
			}
			rowVals[rowContainerForComputedVals.Mapping[computedCols[i].ID]] = d
		}
		evalCtx.PopIVarContainer()
	}

	// Verify the column constraints.
	//
	// We would really like to use enforceLocalColumnConstraints() here,
	// but this is not possible because of some brain damage in the
	// Insert() constructor, which causes insertCols to contain
	// duplicate columns descriptors: computed columns are listed twice,
	// one will receive a NULL value and one will receive a comptued
	// value during execution. It "works out in the end" because the
	// latter (non-NULL) value overwrites the earlier, but
	// enforceLocalColumnConstraints() does not know how to reason about
	// this.
	//
	// In the end it does not matter much, this code is going away in
	// favor of the (simpler, correct) code in the CBO.

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.WritableColumns() {
		if !col.Nullable {
			if i, ok := rowContainerForComputedVals.Mapping[col.ID]; !ok || rowVals[i] == tree.DNull {
				return nil, sqlbase.NewNonNullViolationError(col.Name)
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := 0; i < len(insertCols); i++ {
		outVal, err := sqlbase.LimitValueWidth(&insertCols[i].Type, rowVals[i], &insertCols[i].Name)
		if err != nil {
			return nil, err
		}
		rowVals[i] = outVal
	}

	return rowVals, nil
}

// KVBatch represents a batch of KVs generated from converted rows.
type KVBatch struct {
	// DataFileIdx is where the row data in the batch came from.
	DataFileIdx int32
	// LastRow is the index of the low watermark for the emitted rows across all workers.
	LastRow int64
	// KVs is the actual converted KV data.
	KVs []roachpb.KeyValue

	// RowsVal is timeSeries data record.
	Payload map[int]*sqlbase.PayloadForDistTSInsert
	// NodeRowsMap record the mapping of payload and data.
	NodeRowsMap map[int][]int
	// Data is reading from csv file.
	Data []interface{}
}

// DatumRowConverter converts Datums into kvs and streams it to the destination
// channel.
type DatumRowConverter struct {
	// current row buf
	Datums []tree.Datum

	// kv destination and current batch
	KvCh     chan<- KVBatch
	KvBatch  KVBatch
	BatchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// Tracks which column indices in the set of visible columns are part of the
	// user specified target columns. This can be used before populating Datums
	// to filter out unwanted column data.
	IsTargetCol map[int]struct{}

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    Inserter
	EvalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	VisibleCols           []sqlbase.ColumnDescriptor
	VisibleColTypes       []*types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer

	// FractionFn is used to set the progress header in KVBatches.
	CompletedRowFn func() int64
}

// KvDatumRowConverterBatchSize means the size of one converter batch.
var KvDatumRowConverterBatchSize = 5000

// TestingSetDatumRowConverterBatchSize sets KvDatumRowConverterBatchSize and returns function to
// reset this setting back to its old value.
func TestingSetDatumRowConverterBatchSize(newSize int) func() {
	KvDatumRowConverterBatchSize = newSize
	return func() {
		KvDatumRowConverterBatchSize = 5000
	}
}

// NewDatumRowConverter returns an instance of a DatumRowConverter.
func NewDatumRowConverter(
	ctx context.Context,
	spec *execinfrapb.ReadImportDataSpec,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
	kvCh chan<- KVBatch,
) (*DatumRowConverter, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc)
	c := &DatumRowConverter{
		tableDesc: immutDesc,
		KvCh:      kvCh,
		EvalCtx:   evalCtx,
	}
	var err error
	var targetColDescriptors []sqlbase.ColumnDescriptor
	if spec != nil && len(spec.Table.IntoCols) > 0 {
		var targetColNames tree.NameList
		for _, col := range spec.Table.IntoCols {
			targetColNames = append(targetColNames, tree.Name(col))
		}
		if targetColDescriptors, err = sqlbase.ProcessTargetColumns(immutDesc, targetColNames,
			true /* ensureColumns */, false /* allowMutations */); err != nil {
			return nil, err
		}
	} else {
		targetColDescriptors = immutDesc.VisibleColumns()
	}
	isTargetColID := make(map[sqlbase.ColumnID]struct{})
	for _, col := range targetColDescriptors {
		isTargetColID[col.ID] = struct{}{}
	}

	c.IsTargetCol = make(map[int]struct{})
	for i, col := range targetColDescriptors {
		if _, ok := isTargetColID[col.ID]; !ok {
			continue
		}
		c.IsTargetCol[i] = struct{}{}
	}

	var txCtx transform.ExprTransformContext
	// We do not currently support DEFAULT expressions on target or non-target
	// columns. All non-target columns must be nullable and will be set to NULL
	// during import. We do however support DEFAULT on hidden columns (which is
	// only the default _rowid one). This allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(targetColDescriptors, immutDesc, &txCtx, c.EvalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}

	ri, err := MakeInserter(
		ctx,
		nil, /* txn */
		immutDesc,
		cols,
		SkipFKs,
		nil, /* fkTables */
		&sqlbase.DatumAlloc{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}

	c.ri = ri
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.VisibleCols = targetColDescriptors
	c.VisibleColTypes = make([]*types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].DatumType()
	}

	c.Datums = make([]tree.Datum, len(targetColDescriptors), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i := range cols {
		col := &cols[i]
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				return nil, errors.Newf("unexpected hidden column %s", col.Name)
			}
			c.hidden = i
			c.Datums = append(c.Datums, nil)
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("columns does not match: unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.BatchCap = KvDatumRowConverterBatchSize + padding
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

const rowIDBits = 64 - builtins.NodeIDBits

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *DatumRowConverter) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because it
		// is not idempotent and has unfortunate overlapping of output spans since
		// it puts the uniqueness-ensuring per-generator part (nodeID) in the
		// low-bits. Instead, make our own IDs that attempt to keep each generator
		// (sourceID) writing to its own key-space with sequential rowIndexes
		// mapping to sequential unique IDs, by putting the rowID in the lower
		// bits. To avoid collisions with the SQL-genenerated IDs (at least for a
		// very long time) we also flip the top bit to 1.
		//
		// Producing sequential keys in non-overlapping spans for each source yields
		// observed improvements in ingestion performance of ~2-3x and even more
		// significant reductions in required compactions during IMPORT.
		//
		// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
		// used on a table more than once) offset their rowIndex by a wall-time at
		// which their overall job is run, so that subsequent ingestion jobs pick
		// different row IDs for the i'th row and don't collide. However such
		// time-offset rowIDs mean each row imported consumes some unit of time that
		// must then elapse before the next IMPORT could run without colliding e.g.
		// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
		// likely that IMPORT's write-rate is still the limiting factor, but this
		// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
		// Finding an alternative scheme for avoiding collisions (like sourceID *
		// fileIndex*desc.Version) could improve on this. For now, if this
		// best-effort collision avoidance scheme doesn't work in some cases we can
		// just recommend an explicit PK as a workaround.
		avoidCollisionsWithSQLsIDs := uint64(1 << 63)
		rowID := (uint64(sourceID) << rowIDBits) ^ uint64(rowIndex)
		c.Datums[c.hidden] = tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | rowID))
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, c.EvalCtx, c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	if err := c.ri.InsertRow(
		ctx,
		KVInserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch.KVs = append(c.KvBatch.KVs, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		SkipFKs,
		false, /* traceKV */
		&kv.Txn{},
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch.KVs) >= KvDatumRowConverterBatchSize {
		if err := c.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SendBatch streams kv operations from the current KvBatch to the destination
// channel, and resets the KvBatch to empty.
func (c *DatumRowConverter) SendBatch(ctx context.Context) error {
	if len(c.KvBatch.KVs) == 0 {
		return nil
	}
	if c.CompletedRowFn != nil {
		c.KvBatch.LastRow = c.CompletedRowFn()
	}
	select {
	case c.KvCh <- c.KvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)
	return nil
}
