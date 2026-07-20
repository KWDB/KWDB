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

// ShowSortHistogramColumns is represents Sort Histogram result columns.
var ShowSortHistogramColumns = sqlbase.ResultColumns{
	{Name: "upper_bound", Typ: types.String},
	{Name: "row_count", Typ: types.Int},
	{Name: "unordered_row_count", Typ: types.Int},
	{Name: "ordered_entities_count", Typ: types.Float},
	{Name: "unordered_entities_count", Typ: types.Float},
}

// dummy
type showSortHistogramNode struct {
	_ string
}

// ShowSortHistogram returns a SHOW HISTOGRAM statement.
// Privileges: Any privilege on the respective table.
func ShowSortHistogram(
	ctx context.Context, p *GenericPlanner, n *tree.ShowSortHistogram,
) (sql.PlanNode, error) {
	useTableName := n.Table != nil
	return sql.NewDelayedNode(
			fmt.Sprintf("SHOW SORT HISTOGRAM %d", n.HistogramID),
			ShowSortHistogramColumns,
			func(ctx context.Context, p *GenericPlanner) (sql.PlanNode, error) {
				var row tree.Datums
				var err error
				var desc *sql.ImmutableTableDescriptor
				if useTableName {
					desc, err = p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true /*required*/, sql.ResolveRequireTableDesc)
					if err != nil {
						return nil, err
					}
					row, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
						ctx,
						"read-sort-histogram",
						p.Txn(),
						InternalExecutorSessionDataOverride{User: security.RootUser},
						`SELECT histogram
				 FROM system.table_statistics
				 WHERE "tableID" = $1 and histogram is not null order by "createdAt" desc limit 1`,
						desc.ID,
					)
				} else {
					row, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
						ctx,
						"read-sort-histogram",
						p.Txn(),
						InternalExecutorSessionDataOverride{User: security.RootUser},
						`SELECT histogram
				 FROM system.table_statistics
				 WHERE "statisticID" = $1`,
						n.HistogramID,
					)
				}

				if err != nil {
					return nil, err
				}
				if row == nil {
					return nil, fmt.Errorf("sort histogram %d not found", n.HistogramID)
				}
				if len(row) != 1 {
					return nil, errors.AssertionFailedf("expected 1 column from internal query")
				}
				if row[0] == tree.DNull {
					if useTableName {
						return nil, fmt.Errorf("sort histogram not found for table %s", desc.TableDescriptor.Name)
					}
					// We found a statistic, but it has no histogram.
					return nil, fmt.Errorf("sort histogram %d not found", n.HistogramID)
				}

				histogram := &stats.HistogramData{}
				histData := *row[0].(*tree.DBytes)
				if err := protoutil.Unmarshal([]byte(histData), histogram); err != nil {
					return nil, err
				}

				v := p.NewContainerValuesNode(ShowSortHistogramColumns, 0)
				for _, b := range histogram.SortedBuckets {
					ed, _, err := sqlbase.EncDatumFromBuffer(
						types.TimestampTZ, sqlbase.DatumEncoding_ASCENDING_KEY, b.UpperBound,
					)
					if err != nil {
						v.Close(ctx)
						return nil, err
					}
					row := tree.Datums{
						tree.NewDString(ed.String(types.TimestampTZ)),
						tree.NewDInt(tree.DInt(b.RowCount)),
						tree.NewDInt(tree.DInt(b.UnorderedRowCount)),
						tree.NewDFloat(tree.DFloat(b.OrderedEntities)),
						tree.NewDFloat(tree.DFloat(b.UnorderedEntities)),
					}
					if _, err := v.AddRowToValueNode(ctx, row); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
				return v, nil
			}, nil),
		nil
}
