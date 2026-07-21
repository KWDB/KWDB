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

	"gitee.com/kwbasedb/kwbase/pkg/sql/scrub"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// sqlForeignKeyCheckOperation is a check on an indexes physical data.
type sqlForeignKeyCheckOperation struct {
	tableName  *tree.TableName
	tableDesc  *sqlbase.ImmutableTableDescriptor
	constraint *sqlbase.ConstraintDetail
	asOf       hlc.Timestamp

	colIDToRowIdx map[sqlbase.ColumnID]int

	run sqlForeignKeyConstraintCheckRun
}

// sqlForeignKeyConstraintCheckRun contains the run-time state for
// sqlForeignKeyConstraintCheckOperation during local execution.
type sqlForeignKeyConstraintCheckRun struct {
	started  bool
	rows     []tree.Datums
	rowIndex int
}

func newSQLForeignKeyCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	constraint sqlbase.ConstraintDetail,
	asOf hlc.Timestamp,
) *sqlForeignKeyCheckOperation {
	return &sqlForeignKeyCheckOperation{
		tableName:  tableName,
		tableDesc:  tableDesc,
		constraint: &constraint,
		asOf:       asOf,
	}
}

// Start implements the checkOperation interface.
// It creates a query string and generates a plan from it, which then
// runs in the distSQL execution engine.
func (o *sqlForeignKeyCheckOperation) Start(params RunParams) error {
	ctx := params.Ctx

	checkQuery, _, err := nonMatchingRowQuery(
		&o.tableDesc.TableDescriptor,
		o.constraint.FK,
		o.constraint.ReferencedTable,
		false, /* limitResults */
	)
	if err != nil {
		return err
	}

	rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		ctx, "scrub-fk", params.p.txn, checkQuery,
	)
	if err != nil {
		return err
	}
	o.run.rows = rows

	if len(o.constraint.FK.OriginColumnIDs) > 1 && o.constraint.FK.Match == sqlbase.ForeignKeyReference_FULL {
		// Check if there are any disallowed references where some columns are NULL
		// and some aren't.
		checkNullsQuery, _, err := matchFullUnacceptableKeyQuery(
			&o.tableDesc.TableDescriptor,
			o.constraint.FK,
			false, /* limitResults */
		)
		if err != nil {
			return err
		}
		rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
			ctx, "scrub-fk", params.p.txn, checkNullsQuery,
		)
		if err != nil {
			return err
		}
		o.run.rows = append(o.run.rows, rows...)
	}

	// Collect the expected types for the query results. This is all
	// columns and extra columns in the secondary index used for foreign
	// key referencing. This also implicitly includes all primary index
	// columns.
	columnsByID := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor, len(o.tableDesc.Columns))
	for i := range o.tableDesc.Columns {
		columnsByID[o.tableDesc.Columns[i].ID] = &o.tableDesc.Columns[i]
	}

	// Get primary key columns not included in the FK.
	var colIDs []sqlbase.ColumnID
	colIDs = append(colIDs, o.constraint.FK.OriginColumnIDs...)
	for _, pkColID := range o.tableDesc.PrimaryIndex.ColumnIDs {
		found := false
		for _, id := range o.constraint.FK.OriginColumnIDs {
			if pkColID == id {
				found = true
				break
			}
		}
		if !found {
			colIDs = append(colIDs, pkColID)
		}
	}

	o.colIDToRowIdx = make(map[sqlbase.ColumnID]int, len(colIDs))
	for i, id := range colIDs {
		o.colIDToRowIdx[id] = i
	}

	o.run.started = true
	return nil
}

// Next implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Next(params RunParams) (tree.Datums, error) {
	row := o.run.rows[o.run.rowIndex]
	o.run.rowIndex++

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["constraint_name"] = o.constraint.FK.Name

	// Collect the primary index values for generating the primary key
	// pretty string.
	primaryKeyDatums := make(tree.Datums, 0, len(o.tableDesc.PrimaryIndex.ColumnIDs))
	for _, id := range o.tableDesc.PrimaryIndex.ColumnIDs {
		idx := o.colIDToRowIdx[id]
		primaryKeyDatums = append(primaryKeyDatums, row[idx])
	}

	// Collect all of the values fetched from the index to generate a
	// pretty JSON dictionary for row_data.
	for _, id := range o.constraint.FK.OriginColumnIDs {
		idx := o.colIDToRowIdx[id]
		col, err := o.tableDesc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		rowDetails[col.Name] = row[idx].String()
	}
	for _, id := range o.tableDesc.PrimaryIndex.ColumnIDs {
		found := false
		for _, fkID := range o.constraint.FK.OriginColumnIDs {
			if id == fkID {
				found = true
				break
			}
		}
		if !found {
			idx := o.colIDToRowIdx[id]
			col, err := o.tableDesc.FindActiveColumnByID(id)
			if err != nil {
				return nil, err
			}
			rowDetails[col.Name] = row[idx].String()
		}
	}

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		tree.NewDString(scrub.ForeignKeyConstraintViolation),
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(primaryKeyDatums.String()),
		tree.MakeDTimestamp(params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond),
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= len(o.run.rows)
}

// Close implements the checkOperation interface.
func (o *sqlForeignKeyCheckOperation) Close(ctx context.Context) {
	o.run.rows = nil
}
