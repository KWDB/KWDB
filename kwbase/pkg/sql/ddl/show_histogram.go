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

package ddl

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// ShowHistogramColumns is a template of show histogram result
// Ideally, we would want upper_bound to have the type of the column the
// histogram is on. However, we don't want to have a SHOW statement for which
// the schema depends on its parameters.
var ShowHistogramColumns = sqlbase.ResultColumns{
	{Name: "upper_bound", Typ: types.String},
	{Name: "range_rows", Typ: types.Int},
	{Name: "distinct_range_rows", Typ: types.Float},
	{Name: "equal_rows", Typ: types.Int},
}

// dummy data struct
type showHistogramNode struct {
	_ string
}

// ShowHistogram returns a SHOW HISTOGRAM statement.
// Privileges: Any privilege on the respective table.
func ShowHistogram(
	ctx context.Context, p *GenericPlanner, n *tree.ShowHistogram,
) (sql.PlanNode, error) {
	return sql.NewDelayedNode(
			fmt.Sprintf("SHOW HISTOGRAM %d", n.HistogramID),
			ShowHistogramColumns,
			func(ctx context.Context, p *GenericPlanner) (sql.PlanNode, error) {
				row, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
					ctx,
					"read-histogram",
					p.Txn(),
					InternalExecutorSessionDataOverride{User: security.RootUser},
					`SELECT histogram
				 FROM system.table_statistics
				 WHERE "statisticID" = $1`,
					n.HistogramID,
				)
				if err != nil {
					return nil, err
				}
				if row == nil {
					return nil, fmt.Errorf("histogram %d not found", n.HistogramID)
				}
				if len(row) != 1 {
					return nil, errors.AssertionFailedf("expected 1 column from internal query")
				}
				if row[0] == tree.DNull {
					// We found a statistic, but it has no histogram.
					return nil, fmt.Errorf("histogram %d not found", n.HistogramID)
				}

				histogram := &stats.HistogramData{}
				histData := *row[0].(*tree.DBytes)
				if err := protoutil.Unmarshal([]byte(histData), histogram); err != nil {
					return nil, err
				}

				v := p.NewContainerValuesNode(ShowHistogramColumns, 0)
				for _, b := range histogram.Buckets {
					ed, _, err := sqlbase.EncDatumFromBuffer(
						&histogram.ColumnType, sqlbase.DatumEncoding_ASCENDING_KEY, b.UpperBound,
					)
					if err != nil {
						v.Close(ctx)
						return nil, err
					}
					row := tree.Datums{
						tree.NewDString(ed.String(&histogram.ColumnType)),
						tree.NewDInt(tree.DInt(b.NumRange)),
						tree.NewDFloat(tree.DFloat(b.DistinctRange)),
						tree.NewDInt(tree.DInt(b.NumEq)),
					}
					if _, err := v.AddRowToValueNode(ctx, row); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
				return v, nil
			},
			nil),
		nil
}
