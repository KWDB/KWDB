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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var _ checkOperation = &physicalCheckOperation{}

// physicalCheckOperation is a check on an indexes physical data.
type physicalCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.ImmutableTableDescriptor
	indexDesc *sqlbase.IndexDescriptor

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []*sqlbase.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run physicalCheckRun
}

// physicalCheckRun contains the run-time state for
// physicalCheckOperation during local execution.
type physicalCheckRun struct {
	started  bool
	rows     *rowcontainer.RowContainer
	rowIndex int
}

func newPhysicalCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
) *physicalCheckOperation {
	return &physicalCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		indexDesc: indexDesc,
	}
}

// Start implements the checkOperation interface.
// It will plan and run the physical data check using the distSQL
// execution engine.
func (o *physicalCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	// Collect all of the columns, their types, and their IDs.
	var columnIDs []tree.ColumnID
	colIDToIdx := make(map[sqlbase.ColumnID]int, len(o.tableDesc.Columns))
	columns := make([]*sqlbase.ColumnDescriptor, len(columnIDs))
	for i := range o.tableDesc.Columns {
		colIDToIdx[o.tableDesc.Columns[i].ID] = i
	}

	// Collect all of the columns being scanned.
	if o.indexDesc.ID == o.tableDesc.PrimaryIndex.ID {
		for i := range o.tableDesc.Columns {
			columnIDs = append(columnIDs, tree.ColumnID(o.tableDesc.Columns[i].ID))
		}
	} else {
		for _, id := range o.indexDesc.ColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
		for _, id := range o.indexDesc.ExtraColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
		for _, id := range o.indexDesc.StoreColumnIDs {
			columnIDs = append(columnIDs, tree.ColumnID(id))
		}
	}

	for i := range columnIDs {
		idx := colIDToIdx[sqlbase.ColumnID(columnIDs[i])]
		columns = append(columns, &o.tableDesc.Columns[idx])
	}

	// Find the row indexes for all of the primary index columns.
	primaryColIdxs, err := getPrimaryColIdxs(o.tableDesc, columns)
	if err != nil {
		return err
	}

	indexFlags := &tree.IndexFlags{
		IndexID:     tree.IndexID(o.indexDesc.ID),
		NoIndexJoin: true,
	}
	scan := params.p.Scan()
	scan.isCheck = true
	colCfg := scanColumnsConfig{wantedColumns: columnIDs, addUnwantedAsHidden: true}
	if err := scan.initTable(ctx, params.p, o.tableDesc, indexFlags, colCfg); err != nil {
		return err
	}
	scan.index = scan.specifiedIndex
	sb := span.MakeBuilder(o.tableDesc.TableDesc(), o.indexDesc)
	scan.spans, err = sb.UnconstrainedSpans(false /* forDelete */)
	if err != nil {
		return err
	}

	planCtx := params.extendedEvalCtx.DistSQLPlanner.NewPlanningCtx(ctx, params.extendedEvalCtx, params.p.txn)
	physPlan, err := params.extendedEvalCtx.DistSQLPlanner.createScrubPhysicalCheck(
		planCtx, scan, *o.tableDesc.TableDesc(), *o.indexDesc, params.p.ExecCfg().Clock.Now())
	if err != nil {
		return err
	}

	o.primaryColIdxs = primaryColIdxs
	o.columns = columns
	o.run.started = true
	rows, err := scrubRunDistSQL(ctx, planCtx, params.p, &physPlan, rowexec.ScrubTypes)
	if err != nil {
		rows.Close(ctx)
		return err
	}
	o.run.rows = rows
	return nil
}

// Next implements the checkOperation interface.
func (o *physicalCheckOperation) Next(params runParams) (tree.Datums, error) {
	row := o.run.rows.At(o.run.rowIndex)
	o.run.rowIndex++

	timestamp := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)

	details, ok := row[2].(*tree.DJSON)
	if !ok {
		return nil, errors.Errorf("expected row value 3 to be DJSON, got: %T", row[2])
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		row[0],     /* errorType */
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		row[1], /* primaryKey */
		timestamp,
		tree.DBoolFalse,
		details,
	}, nil
}

// Started implements the checkOperation interface.
func (o *physicalCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *physicalCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= o.run.rows.Len()
}

// Close implements the checkOperation interface.
func (o *physicalCheckOperation) Close(ctx context.Context) {
	if o.run.rows != nil {
		o.run.rows.Close(ctx)
	}
}
