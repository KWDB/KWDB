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
	"fmt"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func TestDeleteOldStatsForColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ex := s.InternalExecutor().(sqlutil.InternalExecutor)
	cache := NewTableStatisticsCache(10 /* cacheSize */, s.GossipI().(*gossip.Gossip), db, ex)

	// The test data must be ordered by CreatedAt DESC so the calculated set of
	// expected deleted stats is correct.
	testData := []TableStatisticProto{
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   1,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-1 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   2,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-2 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   3,
			Name:          "stat_100_1",
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-3 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   4,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-4 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   5,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-5 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   6,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-6 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     0,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   7,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-7 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   8,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-8 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   9,
			Name:          "stat_100_2_3",
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-9 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   10,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-10 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   11,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-11 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   12,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-12 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   13,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2},
			CreatedAt:     timeutil.Now().Add(-13 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   14,
			Name:          "stat_100_1_3",
			ColumnIDs:     []sqlbase.ColumnID{1, 3},
			CreatedAt:     timeutil.Now().Add(-14 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(100),
			StatisticID:   15,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{3, 2},
			CreatedAt:     timeutil.Now().Add(-15 * time.Hour),
			RowCount:      1000,
			DistinctCount: 1000,
			NullCount:     5,
		},
		{
			TableID:       sqlbase.ID(101),
			StatisticID:   16,
			Name:          "stat_101_1",
			ColumnIDs:     []sqlbase.ColumnID{1},
			CreatedAt:     timeutil.Now().Add(-16 * time.Hour),
			RowCount:      320000,
			DistinctCount: 300000,
			NullCount:     100,
		},
		{
			TableID:       sqlbase.ID(102),
			StatisticID:   17,
			Name:          AutoStatsName,
			ColumnIDs:     []sqlbase.ColumnID{2, 3},
			CreatedAt:     timeutil.Now().Add(-17 * time.Hour),
			RowCount:      0,
			DistinctCount: 0,
			NullCount:     0,
		},
	}

	for i := range testData {
		stat := &testData[i]
		if err := insertTableStat(ctx, db, ex, stat); err != nil {
			t.Fatal(err)
		}
	}

	// findStat searches for a statistic in the given list of stats and returns
	// an error if expectDeleted is true but the statistic is found. Likewise, it
	// returns an error if expectDeleted is false but the statistic is not found.
	findStat := func(
		stats []*TableStatistic, tableID sqlbase.ID, statisticID uint64, expectDeleted bool,
	) error {
		for j := range stats {
			if stats[j].StatisticID == statisticID {
				if expectDeleted {
					return fmt.Errorf(
						"expected statistic %d in table %d to be deleted, but it was not",
						statisticID, tableID,
					)
				}
				return nil
			}
		}

		if !expectDeleted {
			return fmt.Errorf(
				"expected statistic %d in table %d not to be deleted, but it was",
				statisticID, tableID,
			)
		}
		return nil
	}

	// checkDelete deletes old statistics for the given table and column IDs and
	// checks that only the statisticIDs contained in expectDeleted have been
	// deleted.
	checkDelete := func(
		tableID sqlbase.ID, columnIDs []sqlbase.ColumnID, expectDeleted map[uint64]struct{},
	) error {
		if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return DeleteOldStatsForColumns(ctx, ex, txn, tableID, columnIDs)
		}); err != nil {
			return err
		}

		cache.RefreshTableStats(ctx, tableID)
		for i := range testData {
			stat := &testData[i]
			if stat.TableID != tableID {
				cache.RefreshTableStats(ctx, stat.TableID)
			}
		}

		return testutils.SucceedsSoonError(func() error {
			tableStats, err := cache.GetTableStats(ctx, tableID)
			if err != nil {
				return err
			}

			for i := range testData {
				stat := &testData[i]
				if stat.TableID != tableID {
					stats, err := cache.GetTableStats(ctx, stat.TableID)
					if err != nil {
						return err
					}
					// No stats from other tables should be deleted.
					if err := findStat(
						stats, stat.TableID, stat.StatisticID, false, /* expectDeleted */
					); err != nil {
						return err
					}
					continue
				}

				// Check whether this stat should have been deleted.
				_, expectDeleted := expectDeleted[stat.StatisticID]
				if err := findStat(tableStats, tableID, stat.StatisticID, expectDeleted); err != nil {
					return err
				}
			}

			return nil
		})
	}

	expectDeleted := make(map[uint64]struct{}, len(testData))
	getExpectDeleted := func(tableID sqlbase.ID, columnIDs []sqlbase.ColumnID) {
		keptStats := 0
		for i := range testData {
			stat := &testData[i]
			if stat.TableID != tableID {
				continue
			}
			if !reflect.DeepEqual(stat.ColumnIDs, columnIDs) {
				continue
			}
			if stat.Name == AutoStatsName && keptStats < keepCount {
				keptStats++
				continue
			}
			expectDeleted[stat.StatisticID] = struct{}{}
		}
	}

	// Delete stats for column 1 in table 100.
	tableID := sqlbase.ID(100)
	columnIDs := []sqlbase.ColumnID{1}
	getExpectDeleted(tableID, columnIDs)
	if err := checkDelete(tableID, columnIDs, expectDeleted); err != nil {
		t.Fatal(err)
	}

	// Delete stats for columns {2, 3} in table 100.
	tableID = sqlbase.ID(100)
	columnIDs = []sqlbase.ColumnID{2, 3}
	getExpectDeleted(tableID, columnIDs)
	if err := checkDelete(tableID, columnIDs, expectDeleted); err != nil {
		t.Fatal(err)
	}
}
