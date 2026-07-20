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
	"go/constant"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/scrub"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// sqlCheckConstraintCheckOperation is a check which validates a SQL
// CHECK constraint on a table.
type sqlCheckConstraintCheckOperation struct {
	tableName *tree.TableName
	tableDesc *sqlbase.ImmutableTableDescriptor
	checkDesc *sqlbase.TableDescriptor_CheckConstraint
	asOf      hlc.Timestamp

	// columns is a list of the columns returned in the query result
	// tree.Datums.
	columns []*sqlbase.ColumnDescriptor
	// primaryColIdxs maps PrimaryIndex.Columns to the row
	// indexes in the query result tree.Datums.
	primaryColIdxs []int

	run sqlCheckConstraintCheckRun
}

// sqlCheckConstraintCheckRun contains the run-time state for
// sqlCheckConstraintCheckOperation during local execution.
type sqlCheckConstraintCheckRun struct {
	started  bool
	rows     []tree.Datums
	rowIndex int
}

func newSQLCheckConstraintCheckOperation(
	tableName *tree.TableName,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	checkDesc *sqlbase.TableDescriptor_CheckConstraint,
	asOf hlc.Timestamp,
) *sqlCheckConstraintCheckOperation {
	return &sqlCheckConstraintCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		checkDesc: checkDesc,
		asOf:      asOf,
	}
}

// Start implements the checkOperation interface.
// It creates a SELECT expression and generates a plan from it, which
// then runs in the distSQL execution engine.
func (o *sqlCheckConstraintCheckOperation) Start(params RunParams) error {
	ctx := params.Ctx
	expr, err := parser.ParseExpr(o.checkDesc.Expr)
	if err != nil {
		return err
	}
	// Generate a query of the form:
	//    SELECT a,b,c FROM db.t WHERE NOT (condition)
	// We always fully qualify the table in the query.
	tn := *o.tableName
	tn.ExplicitCatalog = true
	tn.ExplicitSchema = true
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(o.tableDesc.Columns),
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
		Where: &tree.Where{
			Type: tree.AstWhere,
			Expr: &tree.NotExpr{Expr: expr},
		},
	}
	if o.asOf != hlc.MaxTimestamp {
		sel.From.AsOf = tree.AsOfClause{
			Expr: tree.NewNumVal(
				constant.MakeInt64(o.asOf.WallTime),
				"", /* origString */
				false /* negative */),
		}
	}

	rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		ctx, "check-constraint", params.p.txn, tree.AsStringWithFlags(sel, tree.FmtParsable),
	)
	if err != nil {
		return err
	}

	o.run.started = true
	o.run.rows = rows

	// Collect all the columns.
	for i := range o.tableDesc.Columns {
		o.columns = append(o.columns, &o.tableDesc.Columns[i])
	}
	// Find the row indexes for all of the primary index columns.
	o.primaryColIdxs, err = getPrimaryColIdxs(o.tableDesc, o.columns)
	return err
}

// Next implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Next(params RunParams) (tree.Datums, error) {
	row := o.run.rows[o.run.rowIndex]
	o.run.rowIndex++
	timestamp := tree.MakeDTimestamp(
		params.extendedEvalCtx.GetStmtTimestamp(), time.Nanosecond)

	var primaryKeyDatums tree.Datums
	for _, rowIdx := range o.primaryColIdxs {
		primaryKeyDatums = append(primaryKeyDatums, row[rowIdx])
	}

	details := make(map[string]interface{})
	rowDetails := make(map[string]interface{})
	details["row_data"] = rowDetails
	details["constraint_name"] = o.checkDesc.Name
	for rowIdx, col := range o.columns {
		// TODO(joey): We should maybe try to get the underlying type.
		rowDetails[col.Name] = row[rowIdx].String()
	}
	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		// TODO(joey): Add the job UUID once the SCRUB command uses jobs.
		tree.DNull, /* job_uuid */
		tree.NewDString(scrub.CheckConstraintViolation),
		tree.NewDString(o.tableName.Catalog()),
		tree.NewDString(o.tableName.Table()),
		tree.NewDString(primaryKeyDatums.String()),
		timestamp,
		tree.DBoolFalse,
		detailsJSON,
	}, nil
}

// Started implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Started() bool {
	return o.run.started
}

// Done implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Done(ctx context.Context) bool {
	return o.run.rows == nil || o.run.rowIndex >= len(o.run.rows)
}

// Close implements the checkOperation interface.
func (o *sqlCheckConstraintCheckOperation) Close(ctx context.Context) {
	o.run.rows = nil
}
