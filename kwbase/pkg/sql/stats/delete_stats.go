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

package stats

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

const (
	// keepCount is the number of automatic statistics to keep for a given
	// table and set of columns when deleting old stats. The purpose of keeping
	// several old automatic statistics is to be able to track the amount of
	// time between refreshes. See comments in automatic_stats.go for more
	// details.
	keepCount = 4
)

// DeleteOldStatsForColumns deletes old statistics from the
// system.table_statistics table. For the given tableID and columnIDs,
// DeleteOldStatsForColumns keeps the most recent keepCount automatic
// statistics and deletes all the others.
func DeleteOldStatsForColumns(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableID sqlbase.ID,
	columnIDs []sqlbase.ColumnID,
) error {
	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return err
		}
	}

	// This will delete all old statistics for the given table and columns,
	// including stats created manually (except for a few automatic statistics,
	// which are identified by the name AutoStatsName).
	_, err := executor.Exec(
		ctx, "delete-statistics", txn,
		`DELETE FROM system.table_statistics
               WHERE "tableID" = $1
               AND "columnIDs" = $3
               AND "statisticID" NOT IN (
                   SELECT "statisticID" FROM system.table_statistics
                   WHERE "tableID" = $1
                   AND "name" = $2
                   AND "columnIDs" = $3
                   ORDER BY "createdAt" DESC
                   LIMIT $4
               )`,
		tableID,
		AutoStatsName,
		columnIDsVal,
		keepCount,
	)
	return err
}
