// Copyright 2016 The Cockroach Authors.
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
//
// This source file is part of the database statistics management subsystem for KaiwuDB.
// The primary purpose of this file is to implement the logic for refreshing time-series
// statistics on database tables, helping maintain optimal query performance and efficient
// data management.
//
// The main logic in this file revolves around the Refresher struct and its methods,
// which manage the conditional refreshing of statistics based on database activity and
// specific conditions dictated by the system's usage patterns. This includes:
// - Checking if statistics for a table are outdated or missing.
// - Determining which statistics need refreshing based on table modifications.
// - Managing concurrent statistics creation across different nodes in a cluster.
//
// Design Philosophy:
// The design of this subsystem is guided by the principle of least surprise, meaning
// it aims to automatically manage statistics with minimal need for manual intervention.
// It contrasts with the base origin statistics collection logic by introducing more
// sophisticated heuristics tailored for KaiwuDB's advanced time-series data handling
// and by supporting a broader set of data types and table structures.
//
// Overall Design:
// - The `maybeRefreshTsStats` function serves as the entry point for checking and
//   potentially initiating a refresh of statistics based on the current state of a table.
// - Specific helper functions like `HandleTsColumnStats`, `HandleTsTagStats`, and
//   `HandleTsPrimaryTagStats` encapsulate the logic for handling different types of
//   columns within a table (e.g., normal columns, tag columns, and primary tag columns).
// - These functions use statistical thresholds and timing information to decide
//   when to refresh the statistics and coordinate this process across potentially
//   many nodes in a cluster environment.
// - Error handling is robust, ensuring that temporary issues like concurrency conflicts
//   do not prevent the system from eventually achieving consistency in its statistics.

package stats

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/shirou/gopsutil/cpu"
)

// maybeRefreshTsStats implements the core logic described in the comment for
// Refresher. It is called by the background Refresher thread.
func (r *Refresher) maybeRefreshTsStats(
	ctx context.Context,
	tableID sqlbase.ID,
	tabDesc *sqlbase.TableDescriptor,
	affected Affected,
	asOf time.Duration,
) {
	// Get system cpu usage
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Errorf(ctx, "failed to get cpu usage when refreshing statistics")
		return
	}

	// If cpu usage exceeds 30%, update is skipped
	if len(cpuPercent) > 0 && cpuPercent[0] > 30.0 {
		log.Warningf(ctx, "cpu usage is too high (%f%%), skipping table statistics update", cpuPercent[0])
		return
	}
	enableMetricAutomaticCollection := AutomaticTsStatisticsClusterMode.Get(&r.st.SV)
	enableTagAutomaticCollection := AutomaticTagStatisticsClusterMode.Get(&r.st.SV)
	enableTSOrderedTable := opt.TSOrderedTable.Get(&r.st.SV)
	colsNewDesc := tabDesc.Columns
	tableStats, err := r.cache.GetTableStats(ctx, tableID)
	if err != nil {
		log.Errorf(ctx, "failed to get table statistics cache for table (ID: %d) when refreshing statistics: %v", tableID, err)
		return
	}
	// Here, we need to refresh the statistics for the first time
	if len(tableStats) == 0 {
		// Sample Metric and tag table
		if enableMetricAutomaticCollection && enableTagAutomaticCollection {
			err := r.firstRefreshStats(ctx, tableID, asOf)
			if err != nil {
				log.Warningf(ctx, "failed to create statistics on table (ID: %d): %v when refreshing statistic for the first time", tableID, err)
			}
		}

		// Sample tag table statistics and metric table statistics of row count
		if !enableMetricAutomaticCollection && (enableTagAutomaticCollection || enableTSOrderedTable) {
			err := r.firstRefreshTagStats(ctx, tableID, colsNewDesc, enableTSOrderedTable, tabDesc)
			if err != nil {
				log.Warningf(ctx, "failed to create statistics on table (ID: %d): %v when refreshing tag statistic for the first time", tableID, err)
			}
		}
		return
	}

	// Determine the column type based on all column IDs in tableID and tableStats
	tsMetricStats := make([]*TableStatistic, 0)
	tsTagStats := make([]*TableStatistic, 0)

	colDescMap := make(map[sqlbase.ColumnID]sqlbase.ColumnDescriptor)
	for _, desc := range colsNewDesc {
		colDescMap[desc.ID] = desc
	}
	// Here we need to judge the logic based on the actual column id and type.
	// Since only single column statistics are currently supported, index uses 0
	for _, stat := range tableStats {
		colID := stat.ColumnIDs[0]
		if desc, ok := colDescMap[colID]; ok {
			switch desc.TsCol.ColumnType {
			// Currently, the normal column, tag column use the same sampling method,
			// and the PTag columns are special.
			case sqlbase.ColumnType_TYPE_DATA:
				tsMetricStats = append(tsMetricStats, stat)
			case sqlbase.ColumnType_TYPE_TAG, sqlbase.ColumnType_TYPE_PTAG:
				tsTagStats = append(tsTagStats, stat)
			default:
				log.Errorf(ctx, "unknown column type %v when refreshing statistic for table (ID: %d)", desc.TsCol.ColumnType.String(), tableID)
				return
			}
		}
	}

	// Statistics refresh ploy for metric columns
	if len(tsMetricStats) >= 0 && (enableMetricAutomaticCollection || enableTagAutomaticCollection) {
		r.HandleTsMetricStats(ctx, tsMetricStats, tableID, affected, colsNewDesc, tabDesc, enableTagAutomaticCollection)
	}

	// Statistics refresh ploy for Tag columns
	if len(tsTagStats) > 0 && (enableTagAutomaticCollection || enableTSOrderedTable) {
		r.HandleTsTagStats(ctx, tsTagStats, tableID, affected, colsNewDesc, enableTSOrderedTable)
	}
}

// refreshTsStats is used to refresh tag table statistic
func (r *Refresher) refreshTagStats(
	ctx context.Context,
	tableID sqlbase.ID,
	colsNewDesc []sqlbase.ColumnDescriptor,
	enableTSOrderedTable bool,
) error {
	var columnIDList strings.Builder

	for _, colDesc := range colsNewDesc {
		if colDesc.TsCol.ColumnType != sqlbase.ColumnType_TYPE_DATA {
			if columnIDList.Len() > 0 {
				columnIDList.WriteString(",")
			}
			columnIDList.WriteString(strconv.Itoa(int(colDesc.ColID())))
		}
	}

	var autoCollectionSQL string
	if enableTSOrderedTable {
		autoCollectionSQL = fmt.Sprintf(
			"CREATE STATISTICS %s ON [%s] FROM [%d] with options collect_sorted_histogram",
			AutoStatsName,
			columnIDList.String(),
			tableID,
		)
	} else {
		autoCollectionSQL = fmt.Sprintf(
			"CREATE STATISTICS %s ON [%s] FROM [%d]",
			AutoStatsName,
			columnIDList.String(),
			tableID,
		)
	}

	// Create statistics for specify the id of the columns on the given table.
	_ /* rows */, err := r.ex.Exec(
		ctx,
		"create-stats",
		nil, /* txn */
		autoCollectionSQL,
	)
	return err
}

// refreshMetricStats is used to refresh metric table statistic
func (r *Refresher) refreshMetricStats(
	ctx context.Context,
	tableID sqlbase.ID,
	colsNewDesc []sqlbase.ColumnDescriptor,
	tabDesc *sqlbase.TableDescriptor,
	enableTagAutomaticCollection bool,
) error {
	var err error

	if enableTagAutomaticCollection {
		// Collect metric statistics for specify the id of the columns on the given table.
		err = r.updateTableStatistics(ctx, tableID, colsNewDesc, tabDesc)
	} else {
		var columnIDList strings.Builder

		for _, colDesc := range colsNewDesc {
			if colDesc.TsCol.ColumnType == sqlbase.ColumnType_TYPE_DATA {
				if columnIDList.Len() > 0 {
					columnIDList.WriteString(",")
				}
				columnIDList.WriteString(strconv.Itoa(int(colDesc.ColID())))
			}
		}
		// Create statistics for specify the id of the columns on the given table.
		_ /* rows */, err = r.ex.Exec(
			ctx,
			"create-stats",
			nil, /* txn */
			fmt.Sprintf(
				"CREATE STATISTICS %s ON [%s] FROM [%d]",
				AutoStatsName,
				columnIDList.String(),
				tableID,
			),
		)
	}

	return err
}

// HandleTsMetricStats is used to make sure whether refresh normal columns statistic
// If the refresh requirements are met, the refresh will then be performed
func (r *Refresher) HandleTsMetricStats(
	ctx context.Context,
	tsColumnStats []*TableStatistic,
	tableID sqlbase.ID,
	affected Affected,
	colsNewDesc []sqlbase.ColumnDescriptor,
	tabDesc *sqlbase.TableDescriptor,
	enableTagAutomaticCollection bool,
) {
	var rowCount float64
	mustRefresh := false
	if stat := mostRecentAutomaticStat(tsColumnStats); stat != nil {
		// Check if too much time has passed since the last refresh.
		// This check is in place to corral statistical outliers and avoid a
		// case where a significant portion of the data in a table has changed but
		// the stats haven't been refreshed. Randomly add some extra time to the
		// limit check to avoid having multiple nodes trying to create stats at
		// the same time.
		//
		// Note that this can cause some unnecessary runs of CREATE STATISTICS
		// in the case where there is a heavy write load followed by a very light
		// load. For example, suppose the average refresh time is 1 hour during
		// the period of heavy writes, and the average refresh time should be 1
		// week during the period of light load. It could take ~16 refreshes over
		// 3-4 weeks before the average settles at around 1 week. (Assuming the
		// refresh happens at exactly 4x the current average, and the average
		// refresh time is calculated from the most recent 4 refreshes. See the
		// comment in stats/delete_stats.go.)
		maxTimeBetweenRefreshes := stat.CreatedAt.Add(2*avgTsColumnRefreshTime(tsColumnStats) + r.extraTime)
		if timeutil.Now().After(maxTimeBetweenRefreshes) {
			mustRefresh = true
		}
		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	targetRows := int64(rowCount*AutomaticStatisticsFractionStaleRows.Get(&r.st.SV)) +
		4*AutomaticStatisticsMinStaleRows.Get(&r.st.SV)
	if !mustRefresh && affected.rowsAffected < math.MaxInt32 && targetRows > affected.rowsAffected {
		// No refresh is happening this time.
		return
	}

	if err := r.refreshMetricStats(ctx, tableID, colsNewDesc, tabDesc, enableTagAutomaticCollection); err != nil {
		if errors.Is(err, ConcurrentCreateStatsError) {
			// Another stats job was already running. Attempt to reschedule this
			// refresh.
			if mustRefresh {
				// For the cases where mustRefresh=true (stats don't yet exist or it
				// has been 2x the average time since a refresh), we want to make sure
				// that maybeRefreshStats is called on this table during the next
				// cycle so that we have another chance to trigger a refresh. We pass
				// rowsAffected=0 so that we don't force a refresh if another node has
				// already done it.
				r.mutations <- mutation{tableID: tableID, rowsAffected: 0}
			} else {
				// If this refresh was caused by a "dice roll", we want to make sure
				// that the refresh is rescheduled so that we adhere to the
				// AutomaticStatisticsFractionStaleRows statistical ideal. We
				// ensure that the refresh is triggered during the next cycle by
				// passing a very large number for rowsAffected.
				r.mutations <- mutation{tableID: tableID, rowsAffected: math.MaxInt32}
			}
			return
		}

		// Log other errors but don't automatically reschedule the refresh, since
		// that could lead to endless retries.
		log.Warningf(ctx, "failed to create statistics for table (ID: %d) with error message: %v", tableID, err)
		return
	}
}

// HandleTsTagStats is used to make sure whether refresh normal columns statistic
// If the refresh requirements are met, the refresh will then be performed
func (r *Refresher) HandleTsTagStats(
	ctx context.Context,
	tsTagStats []*TableStatistic,
	tableID sqlbase.ID,
	affected Affected,
	colsNewDesc []sqlbase.ColumnDescriptor,
	enableTSOrderedTable bool,
) {
	var rowCount float64
	mustRefresh := false
	if stat := mostRecentAutomaticStat(tsTagStats); stat != nil {
		maxTimeBetweenRefreshes := stat.CreatedAt.Add(avgTsPrimaryTagRefreshTime(tsTagStats))
		if timeutil.Now().After(maxTimeBetweenRefreshes) {
			mustRefresh = true
		}
		rowCount = float64(stat.RowCount)
	} else {
		// If there are no statistics available on this table, we must perform a
		// refresh.
		mustRefresh = true
	}

	var targetUnorderedRows int64
	var collectSortHistogram bool
	if enableTSOrderedTable {
		targetUnorderedRows = int64(rowCount*AutomaticTsUnorderedFractionStaleRows) +
			int64(AutomaticTsUnorderedMinStaleRows)
		collectSortHistogram = targetUnorderedRows > affected.unorderedAffected
	}

	// Tag table uses a threshold based on the fraction of rows*1/2 and a minimum number of rows threshold*1/5
	targetEntitiesRows := int64(rowCount*AutomaticStatisticsFractionStaleRows.Get(&r.st.SV))/2 +
		AutomaticStatisticsMinStaleRows.Get(&r.st.SV)/5
	if !mustRefresh && affected.entitiesAffected < math.MaxInt32 && (targetEntitiesRows > affected.entitiesAffected && !collectSortHistogram) {
		// No refresh is happening this time.
		return
	}

	if err := r.refreshTagStats(ctx, tableID, colsNewDesc, enableTSOrderedTable); err != nil {
		if errors.Is(err, ConcurrentCreateStatsError) {
			// Another stats job was already running. Attempt to reschedule this
			// refresh.
			if mustRefresh {
				// For the cases where mustRefresh=true (stats don't yet exist or it
				// has been 2x the average time since a refresh), we want to make sure
				// that maybeRefreshStats is called on this table during the next
				// cycle so that we have another chance to trigger a refresh. We pass
				// rowsAffected=0 so that we don't force a refresh if another node has
				// already done it.
				r.mutations <- mutation{tableID: tableID, rowsAffected: 0}
			} else {
				// If this refresh was caused by a "dice roll", we want to make sure
				// that the refresh is rescheduled so that we adhere to the
				// AutomaticStatisticsFractionStaleRows statistical ideal. We
				// ensure that the refresh is triggered during the next cycle by
				// passing a very large number for rowsAffected.
				r.mutations <- mutation{tableID: tableID, entitiesAffected: math.MaxInt32}
			}
			return
		}

		// Log other errors but don't automatically reschedule the refresh, since
		// that could lead to endless retries.
		log.Warningf(ctx, "failed to create statistics for table (ID: %d) with error message: %v", tableID, err)
		return
	}
}

// avgTsColumnRefreshTime returns the average time between automatic statistics
// refreshes given a list of tableStats from one ts table. It does so by finding
// the most recent automatically generated statistic (identified by the name
// AutoStatsName), and then finds all previously generated automatic stats on
// those same columns. The average is calculated as the average time between
// each consecutive stat.
//
// If there are not at least two automatically generated statistics on the same
// columns, the default value defaultAverageTimeBetweenRefreshes is returned.
func avgTsColumnRefreshTime(tableStats []*TableStatistic) time.Duration {
	var reference *TableStatistic
	var sum time.Duration
	var count int
	for _, stat := range tableStats {
		if stat.Name != AutoStatsName {
			continue
		}
		if reference == nil {
			reference = stat
			continue
		}
		if !areEqual(stat.ColumnIDs, reference.ColumnIDs) {
			continue
		}
		// Stats are sorted with the most recent first.
		sum += reference.CreatedAt.Sub(stat.CreatedAt)
		count++
		reference = stat
	}
	if count == 0 {
		return defaultAverageTimeBetweenRefreshes
	}
	return sum / time.Duration(count)
}

// avgTsPrimaryTagRefreshTime returns the average time between automatic statistics
// refreshes given a list of tableStats from one ts table. It does so by finding
// the most recent automatically generated statistic (identified by the name
// AutoStatsName), and then finds all previously generated automatic stats on
// those same columns. The average is calculated as the average time between
// each consecutive stat.
//
// If there are not at least two automatically generated statistics on the same
// columns, the default value defaultAverageTimeBetweenRefreshes is returned.
func avgTsPrimaryTagRefreshTime(tableStats []*TableStatistic) time.Duration {
	var reference *TableStatistic
	var sum time.Duration
	var count int
	for _, stat := range tableStats {
		if stat.Name != AutoStatsName {
			continue
		}
		if reference == nil {
			reference = stat
			continue
		}
		if !areEqual(stat.ColumnIDs, reference.ColumnIDs) {
			continue
		}
		// Stats are sorted with the most recent first.
		sum += reference.CreatedAt.Sub(stat.CreatedAt)
		count++
		reference = stat
	}
	if count == 0 {
		return defaultAverageTimeBetweenTsRefreshes
	}
	return sum / time.Duration(count)
}

// firstRefreshTagStats is used to create statistic on every for the first time
func (r *Refresher) firstRefreshTagStats(
	ctx context.Context,
	tableID sqlbase.ID,
	colsDesc []sqlbase.ColumnDescriptor,
	enableTSOrderedTable bool,
	tabDesc *sqlbase.TableDescriptor,
) error {
	var columnIDList strings.Builder

	for _, colDesc := range colsDesc {
		if colDesc.TsCol.ColumnType != sqlbase.ColumnType_TYPE_DATA {
			colID := colDesc.ColID()
			if columnIDList.Len() > 0 {
				columnIDList.WriteString(",")
			}
			columnIDList.WriteString(strconv.Itoa(int(colID)))
		}
	}

	var autoCollectionSQL string
	if enableTSOrderedTable {
		autoCollectionSQL = fmt.Sprintf(
			"CREATE STATISTICS %s ON [%s] FROM [%d] with options collect_sorted_histogram",
			AutoStatsName,
			columnIDList.String(),
			tableID,
		)
	} else {
		autoCollectionSQL = fmt.Sprintf(
			"CREATE STATISTICS %s ON [%s] FROM [%d]",
			AutoStatsName,
			columnIDList.String(),
			tableID,
		)
	}

	// Create tag statistics for specify the id of the columns on the given table.
	_ /* rows */, err := r.ex.Exec(
		ctx,
		"create-stats",
		nil, /* txn */
		autoCollectionSQL,
	)

	// Collect metric statistics for specify the id of the columns on the given table.
	err = r.updateTableStatistics(ctx, tableID, colsDesc, tabDesc)

	if err != nil {
		if errors.Is(err, ConcurrentCreateStatsError) {
			// For the cases where mustRefresh=true (stats don't yet exist or it
			// has been 2x the average time since a refresh), we want to make sure
			// that maybeRefreshStats is called on this table during the next
			// cycle so that we have another chance to trigger a refresh. We pass
			// rowsAffected=0 so that we don't force a refresh if another node has
			// already done it.
			r.mutations <- mutation{tableID: tableID, rowsAffected: 0, entitiesAffected: 0}
		}
		return err
	}
	return nil
}

// firstRefreshStats is used to create statistic on every for the first time
func (r *Refresher) firstRefreshStats(
	ctx context.Context, tableID sqlbase.ID, asOf time.Duration,
) error {
	if err := r.firstRefreshTsStats(ctx, tableID); err != nil {
		if errors.Is(err, ConcurrentCreateStatsError) {
			// For the cases where mustRefresh=true (stats don't yet exist or it
			// has been 2x the average time since a refresh), we want to make sure
			// that maybeRefreshStats is called on this table during the next
			// cycle so that we have another chance to trigger a refresh. We pass
			// rowsAffected=0 so that we don't force a refresh if another node has
			// already done it.
			r.mutations <- mutation{tableID: tableID, rowsAffected: 0}
		}
		return err
	}
	return nil
}

// updateTableStatistics updates the statistics for the given table and columns.
func (r *Refresher) updateTableStatistics(
	ctx context.Context,
	tableID sqlbase.ID,
	colsDesc []sqlbase.ColumnDescriptor,
	tabDesc *sqlbase.TableDescriptor,
) error {
	// Get database description.
	dbDesc, dbErr := sqlbase.GetDatabaseDescFromID(ctx, r.cache.ClientDB, tabDesc.ParentID)
	if dbErr != nil {
		return dbErr
	}

	// Query to get the total row count of the table.
	rows, err := r.ex.QueryRow(
		ctx,
		"select-rowCount",
		nil, /* txn */
		fmt.Sprintf(
			`select count(1) from %s.%s`,
			dbDesc.Name,
			tabDesc.Name,
		),
	)
	if err != nil {
		return err
	}

	var tsRowCount, distinctCount, nullCount uint64
	if len(rows) > 0 {
		tsRowCount = uint64(*rows[0].(*tree.DInt))
	} else {
		return pgerror.Newf(pgcode.Warning, "failed to get the total row count by internal query")
	}
	if tsRowCount > 0 {
		// Assuming 10% distinct count.
		distinctCount = uint64(math.Max(float64(tsRowCount)*0.1, 1))
		// Assuming 1% null count.
		nullCount = uint64(math.Max(float64(tsRowCount)*0.01, 1))
	}

	for _, colDesc := range colsDesc {
		if colDesc.IsTagCol() {
			// Skip tag columns.
			continue
		}

		columnIDsVal := tree.NewDArray(types.Int)
		if err = columnIDsVal.Append(tree.NewDInt(tree.DInt(colDesc.ID))); err != nil {
			return err
		}

		// This will delete all old statistics for the given table and columns,
		// including stats created manually (except for a few automatic statistics,
		// which are identified by the name AutoStatsName).
		_, err = r.ex.Exec(
			ctx, "delete-statistics", nil,
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
		if err != nil {
			return err
		}

		// Insert new statistics for the column.
		_, err = r.ex.Exec(
			ctx, "insert-statistic", nil,
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, NULL)`,
			tableID,
			AutoStatsName,
			columnIDsVal,
			tsRowCount,
			distinctCount,
			nullCount,
		)
		if err != nil {
			return err
		}
		// update cache
		err = GossipTableStatAdded(r.cache.Gossip, tableID)
		if err != nil {
			log.Errorf(ctx, "Gossip Table Stat err: %v", err)
		}
	}

	return nil
}
