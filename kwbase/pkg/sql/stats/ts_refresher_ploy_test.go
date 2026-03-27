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

package stats

import (
	"bytes"
	"context"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// fakeTSRefresherPolicy is a test double used to control time, CPU usage,
// and refresh behavior without changing production logic.
type fakeTSRefresherPolicy struct {
	nowFn func() time.Time
	cpuFn func(time.Duration, bool) ([]float64, error)

	firstRefreshStatsFn func(context.Context, *Refresher, sqlbase.ID, time.Duration) error
	firstRefreshTagFn   func(context.Context, *Refresher, sqlbase.ID, []sqlbase.ColumnDescriptor, bool, *sqlbase.TableDescriptor) error
	refreshMetricFn     func(context.Context, *Refresher, sqlbase.ID, []sqlbase.ColumnDescriptor, *sqlbase.TableDescriptor, bool) error
	refreshTagFn        func(context.Context, *Refresher, sqlbase.ID, []sqlbase.ColumnDescriptor, bool) error
}

func (f fakeTSRefresherPolicy) now() time.Time {
	if f.nowFn != nil {
		return f.nowFn()
	}
	return timeutil.Unix(0, 0)
}

func (f fakeTSRefresherPolicy) cpuPercent(interval time.Duration, percpu bool) ([]float64, error) {
	if f.cpuFn != nil {
		return f.cpuFn(interval, percpu)
	}
	return []float64{0}, nil
}

func (f fakeTSRefresherPolicy) firstRefreshStats(
	ctx context.Context, r *Refresher, tableID sqlbase.ID, asOf time.Duration,
) error {
	if f.firstRefreshStatsFn != nil {
		return f.firstRefreshStatsFn(ctx, r, tableID, asOf)
	}
	return nil
}

func (f fakeTSRefresherPolicy) firstRefreshTagStats(
	ctx context.Context,
	r *Refresher,
	tableID sqlbase.ID,
	colsNewDesc []sqlbase.ColumnDescriptor,
	enableTSOrderedTable bool,
	tabDesc *sqlbase.TableDescriptor,
) error {
	if f.firstRefreshTagFn != nil {
		return f.firstRefreshTagFn(ctx, r, tableID, colsNewDesc, enableTSOrderedTable, tabDesc)
	}
	return nil
}

func (f fakeTSRefresherPolicy) refreshMetricStats(
	ctx context.Context,
	r *Refresher,
	tableID sqlbase.ID,
	colsNewDesc []sqlbase.ColumnDescriptor,
	tabDesc *sqlbase.TableDescriptor,
	enableTagAutomaticCollection bool,
) error {
	if f.refreshMetricFn != nil {
		return f.refreshMetricFn(ctx, r, tableID, colsNewDesc, tabDesc, enableTagAutomaticCollection)
	}
	return nil
}

func (f fakeTSRefresherPolicy) refreshTagStats(
	ctx context.Context,
	r *Refresher,
	tableID sqlbase.ID,
	colsNewDesc []sqlbase.ColumnDescriptor,
	enableTSOrderedTable bool,
) error {
	if f.refreshTagFn != nil {
		return f.refreshTagFn(ctx, r, tableID, colsNewDesc, enableTSOrderedTable)
	}
	return nil
}

func setTestTSRefreshPolicy(t *testing.T, p tsRefresherPolicy) {
	old := tsRefreshPolicy
	tsRefreshPolicy = p
	t.Cleanup(func() {
		tsRefreshPolicy = old
	})
}

// makeTSStat creates a minimal statistic row for refresh-time helper tests.
func makeTSStat(
	name string, createdAt time.Time, rowCount uint64, colIDs ...sqlbase.ColumnID,
) *TableStatistic {
	return &TableStatistic{
		TableStatisticProto: TableStatisticProto{
			Name:      name,
			CreatedAt: createdAt,
			RowCount:  rowCount,
			ColumnIDs: colIDs,
		},
	}
}

// newTestRefresher creates a lightweight refresher for branch-level unit tests.
func newTestRefresher() *Refresher {
	st := cluster.MakeTestingClusterSettings()
	// Use deterministic thresholds in tests.
	AutomaticStatisticsFractionStaleRows.Override(&st.SV, 0.2)
	AutomaticStatisticsMinStaleRows.Override(&st.SV, 5)
	return &Refresher{
		st:        st,
		extraTime: 0,
		mutations: make(chan mutation, 8),
	}
}

// TestAvgTsColumnRefreshTime_DefaultWhenNoMatchingStats verifies that the
// metric helper falls back to the default interval if no usable auto-stats exist.
func TestAvgTsColumnRefreshTime_DefaultWhenNoMatchingStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	stats := []*TableStatistic{
		makeTSStat("manual", base, 100, sqlbase.ColumnID(1)),
		makeTSStat("manual", base.Add(-time.Hour), 100, sqlbase.ColumnID(1)),
	}

	got := avgTsColumnRefreshTime(stats)
	if got != defaultAverageTimeBetweenRefreshes {
		t.Fatalf("expected %v, got %v", defaultAverageTimeBetweenRefreshes, got)
	}
}

// TestAvgTsColumnRefreshTime_ComputesAverage verifies the normal averaging path
// for automatic stats on the same metric column.
func TestAvgTsColumnRefreshTime_ComputesAverage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(1)),
		makeTSStat(AutoStatsName, base.Add(-2*time.Hour), 100, sqlbase.ColumnID(1)),
		makeTSStat(AutoStatsName, base.Add(-5*time.Hour), 100, sqlbase.ColumnID(1)),
	}

	want := 150 * time.Minute
	got := avgTsColumnRefreshTime(stats)
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestAvgTsColumnRefreshTime_IgnoresDifferentColumns verifies that only the
// reference column-set contributes to the rolling average.
func TestAvgTsColumnRefreshTime_IgnoresDifferentColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(10)),
		makeTSStat(AutoStatsName, base.Add(-time.Hour), 100, sqlbase.ColumnID(99)),
		makeTSStat(AutoStatsName, base.Add(-4*time.Hour), 100, sqlbase.ColumnID(10)),
	}

	want := 4 * time.Hour
	got := avgTsColumnRefreshTime(stats)
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestAvgTsPrimaryTagRefreshTime_DefaultWhenNoMatchingStats verifies that the
// tag helper falls back to the tag-specific default interval.
func TestAvgTsPrimaryTagRefreshTime_DefaultWhenNoMatchingStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	stats := []*TableStatistic{
		makeTSStat("manual", base, 100, sqlbase.ColumnID(7)),
	}

	got := avgTsPrimaryTagRefreshTime(stats)
	if got != defaultAverageTimeBetweenTsRefreshes {
		t.Fatalf("expected %v, got %v", defaultAverageTimeBetweenTsRefreshes, got)
	}
}

// TestAvgTsPrimaryTagRefreshTime_ComputesAverage verifies the tag average path.
func TestAvgTsPrimaryTagRefreshTime_ComputesAverage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(7)),
		makeTSStat(AutoStatsName, base.Add(-1*time.Hour), 100, sqlbase.ColumnID(7)),
		makeTSStat(AutoStatsName, base.Add(-4*time.Hour), 100, sqlbase.ColumnID(7)),
	}

	want := 2 * time.Hour
	got := avgTsPrimaryTagRefreshTime(stats)
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestHandleTsMetricStats_NoStatsForcesRefresh verifies that missing stats
// always force a metric refresh attempt.
func TestHandleTsMetricStats_NoStatsForcesRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	r.HandleTsMetricStats(
		context.Background(),
		nil,
		sqlbase.ID(42),
		Affected{},
		nil,
		nil,
		false,
	)

	if called != 1 {
		t.Fatalf("expected refreshMetricStats to be called once, got %d", called)
	}
}

// TestHandleTsMetricStats_SmallMutationSkipsRefresh verifies that a small
// mutation count below the stale threshold does not trigger refresh.
func TestHandleTsMetricStats_SmallMutationSkipsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(1)),
	}

	r.HandleTsMetricStats(
		context.Background(),
		stats,
		sqlbase.ID(42),
		Affected{rowsAffected: 1},
		nil,
		nil,
		false,
	)

	if called != 0 {
		t.Fatalf("expected refreshMetricStats not to be called, got %d", called)
	}
}

// TestHandleTsMetricStats_TimeBasedRefresh verifies that old auto-stats
// force a refresh even when affected rows are small.
func TestHandleTsMetricStats_TimeBasedRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	createdAt := base.Add(-(2*defaultAverageTimeBetweenRefreshes + time.Minute))

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, createdAt, 100, sqlbase.ColumnID(1)),
	}

	r.HandleTsMetricStats(
		context.Background(),
		stats,
		sqlbase.ID(42),
		Affected{rowsAffected: 0},
		nil,
		nil,
		false,
	)

	if called != 1 {
		t.Fatalf("expected refreshMetricStats to be called once, got %d", called)
	}
}

// TestHandleTsMetricStats_ConcurrentErrorMustRefreshRequeuesZero verifies the
// requeue path for concurrent refresh on a must-refresh case.
func TestHandleTsMetricStats_ConcurrentErrorMustRefreshRequeuesZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			return ConcurrentCreateStatsError
		},
	})

	r.HandleTsMetricStats(
		context.Background(),
		nil,
		sqlbase.ID(42),
		Affected{},
		nil,
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(42) || m.rowsAffected != 0 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected a rescheduled mutation")
	}
}

// TestHandleTsMetricStats_ConcurrentErrorDiceRollRequeuesMax verifies the
// statistical-ideal requeue path for a non-mandatory refresh.
func TestHandleTsMetricStats_ConcurrentErrorDiceRollRequeuesMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			return ConcurrentCreateStatsError
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(1)),
	}

	r.HandleTsMetricStats(
		context.Background(),
		stats,
		sqlbase.ID(42),
		Affected{rowsAffected: math.MaxInt32},
		nil,
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(42) || m.rowsAffected != math.MaxInt32 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected a rescheduled mutation")
	}
}

// TestHandleTsMetricStats_NormalErrorDoesNotRequeue verifies that a normal
// refresh error is logged but not re-enqueued.
func TestHandleTsMetricStats_NormalErrorDoesNotRequeue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshMetricFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ *sqlbase.TableDescriptor, _ bool,
		) error {
			return context.DeadlineExceeded
		},
	})

	r.HandleTsMetricStats(
		context.Background(),
		nil,
		sqlbase.ID(42),
		Affected{},
		nil,
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		t.Fatalf("unexpected requeue: %+v", m)
	default:
	}
}

// TestHandleTsTagStats_NoStatsForcesRefresh verifies that missing tag stats
// always force a refresh attempt.
func TestHandleTsTagStats_NoStatsForcesRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	r.HandleTsTagStats(
		context.Background(),
		nil,
		sqlbase.ID(77),
		Affected{},
		nil,
		false,
	)

	if called != 1 {
		t.Fatalf("expected refreshTagStats to be called once, got %d", called)
	}
}

// TestHandleTsTagStats_SmallEntityMutationSkipsRefresh verifies the normal
// no-refresh path when neither stale entities nor unordered rows exceed limits.
func TestHandleTsTagStats_SmallEntityMutationSkipsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(7)),
	}

	r.HandleTsTagStats(
		context.Background(),
		stats,
		sqlbase.ID(77),
		Affected{entitiesAffected: 1, unorderedAffected: 0},
		nil,
		false,
	)

	if called != 0 {
		t.Fatalf("expected refreshTagStats not to be called, got %d", called)
	}
}

// TestHandleTsTagStats_TimeBasedRefresh verifies that old tag stats force refresh.
func TestHandleTsTagStats_TimeBasedRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	createdAt := base.Add(-(defaultAverageTimeBetweenTsRefreshes + time.Minute))

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, createdAt, 100, sqlbase.ColumnID(7)),
	}

	r.HandleTsTagStats(
		context.Background(),
		stats,
		sqlbase.ID(77),
		Affected{entitiesAffected: 0},
		nil,
		false,
	)

	if called != 1 {
		t.Fatalf("expected refreshTagStats to be called once, got %d", called)
	}
}

// TestHandleTsTagStats_OrderedTableUsesUnorderedThreshold verifies the special
// ordered-table histogram branch.
func TestHandleTsTagStats_OrderedTableUsesUnorderedThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	called := 0
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			called++
			return nil
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(7)),
	}

	r.HandleTsTagStats(
		context.Background(),
		stats,
		sqlbase.ID(77),
		Affected{entitiesAffected: 0, unorderedAffected: 0},
		nil,
		true, // enableTSOrderedTable
	)

	if called != 1 {
		t.Fatalf("expected refreshTagStats to be called once, got %d", called)
	}
}

// TestHandleTsTagStats_ConcurrentErrorMustRefreshRequeuesZero verifies the
// mandatory-refresh concurrent requeue path for tag stats.
func TestHandleTsTagStats_ConcurrentErrorMustRefreshRequeuesZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			return ConcurrentCreateStatsError
		},
	})

	r.HandleTsTagStats(
		context.Background(),
		nil,
		sqlbase.ID(77),
		Affected{},
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(77) || m.rowsAffected != 0 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected a rescheduled mutation")
	}
}

// TestHandleTsTagStats_ConcurrentErrorDiceRollRequeuesMax verifies the
// non-mandatory concurrent requeue path for tag stats.
func TestHandleTsTagStats_ConcurrentErrorDiceRollRequeuesMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()
	base := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		nowFn: func() time.Time { return base },
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			return ConcurrentCreateStatsError
		},
	})

	stats := []*TableStatistic{
		makeTSStat(AutoStatsName, base, 100, sqlbase.ColumnID(7)),
	}

	r.HandleTsTagStats(
		context.Background(),
		stats,
		sqlbase.ID(77),
		Affected{entitiesAffected: math.MaxInt32, unorderedAffected: math.MaxInt32},
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(77) || m.entitiesAffected != math.MaxInt32 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected a rescheduled mutation")
	}
}

// TestHandleTsTagStats_NormalErrorDoesNotRequeue verifies that non-concurrent
// tag refresh errors do not enqueue another attempt.
func TestHandleTsTagStats_NormalErrorDoesNotRequeue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newTestRefresher()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		refreshTagFn: func(
			_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool,
		) error {
			return context.DeadlineExceeded
		},
	})

	r.HandleTsTagStats(
		context.Background(),
		nil,
		sqlbase.ID(77),
		Affected{},
		nil,
		false,
	)

	select {
	case m := <-r.mutations:
		t.Fatalf("unexpected requeue: %+v", m)
	default:
	}
}

type execCall struct {
	opName string
	query  string
	args   []interface{}
}

type fakeInternalExecutor struct {
	execCalls []execCall

	queryRowRes tree.Datums
	queryRes    []tree.Datums

	execErr     error
	queryRowErr error
	queryErr    error

	execHook     func(opName, query string, args ...interface{}) error
	queryRowHook func(opName, query string, args ...interface{}) (tree.Datums, error)
	queryHook    func(opName, query string, args ...interface{}) ([]tree.Datums, error)
}

func (f *fakeInternalExecutor) Exec(
	_ context.Context, opName string, _ *kv.Txn, query string, args ...interface{},
) (int, error) {
	f.execCalls = append(f.execCalls, execCall{opName: opName, query: query, args: args})
	if f.execHook != nil {
		if err := f.execHook(opName, query, args...); err != nil {
			return 0, err
		}
	}
	return 0, f.execErr
}

func (f *fakeInternalExecutor) ExecEx(
	_ context.Context,
	opName string,
	_ *kv.Txn,
	_ sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	f.execCalls = append(f.execCalls, execCall{opName: opName, query: stmt, args: qargs})
	if f.execHook != nil {
		if err := f.execHook(opName, stmt, qargs...); err != nil {
			return 0, err
		}
	}
	return 0, f.execErr
}

func (f *fakeInternalExecutor) QueryRow(
	_ context.Context, opName string, _ *kv.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	if f.queryRowHook != nil {
		return f.queryRowHook(opName, statement, qargs...)
	}
	return f.queryRowRes, f.queryRowErr
}

func (f *fakeInternalExecutor) QueryRowEx(
	_ context.Context,
	opName string,
	_ *kv.Txn,
	_ sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	if f.queryRowHook != nil {
		return f.queryRowHook(opName, stmt, qargs...)
	}
	return f.queryRowRes, f.queryRowErr
}

func (f *fakeInternalExecutor) Query(
	_ context.Context, opName string, _ *kv.Txn, statement string, qargs ...interface{},
) ([]tree.Datums, error) {
	if f.queryHook != nil {
		return f.queryHook(opName, statement, qargs...)
	}
	return f.queryRes, f.queryErr
}

func (f *fakeInternalExecutor) QueryEx(
	_ context.Context,
	opName string,
	_ *kv.Txn,
	_ sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	if f.queryHook != nil {
		return f.queryHook(opName, stmt, qargs...)
	}
	return f.queryRes, f.queryErr
}

func (f *fakeInternalExecutor) QueryWithCols(
	_ context.Context,
	opName string,
	_ *kv.Txn,
	_ sqlbase.InternalExecutorSessionDataOverride,
	statement string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	if f.queryHook != nil {
		rows, err := f.queryHook(opName, statement, qargs...)
		return rows, nil, err
	}
	return f.queryRes, nil, f.queryErr
}

func makeColumn(id sqlbase.ColumnID, typ sqlbase.ColumnType) sqlbase.ColumnDescriptor {
	return sqlbase.ColumnDescriptor{
		ID: id,
		TsCol: sqlbase.TSCol{
			ColumnType: typ,
		},
		Type: *types.Int,
	}
}

func newRefresherForMoreTests() *Refresher {
	st := cluster.MakeTestingClusterSettings()
	AutomaticStatisticsFractionStaleRows.Override(&st.SV, 0.2)
	AutomaticStatisticsMinStaleRows.Override(&st.SV, 5)
	return &Refresher{
		st:        st,
		extraTime: 0,
		mutations: make(chan mutation, 16),
	}
}

func setGetTableStatsFn(
	t *testing.T,
	fn func(ctx context.Context, r *Refresher, tableID sqlbase.ID) ([]*TableStatistic, error),
) {
	old := getTableStatsFn
	getTableStatsFn = fn
	t.Cleanup(func() { getTableStatsFn = old })
}

func setFirstRefreshTsStatsFn(
	t *testing.T, fn func(ctx context.Context, r *Refresher, tableID sqlbase.ID) error,
) {
	old := firstRefreshTsStatsFn
	firstRefreshTsStatsFn = fn
	t.Cleanup(func() { firstRefreshTsStatsFn = old })
}

func setGetDatabaseDescFromIDFn(
	t *testing.T,
	fn func(context.Context, *Refresher, sqlbase.ID) (*sqlbase.DatabaseDescriptor, error),
) {
	old := getDatabaseDescFromIDFn
	getDatabaseDescFromIDFn = fn
	t.Cleanup(func() { getDatabaseDescFromIDFn = old })
}

func setGossipTableStatAddedFn(t *testing.T, fn func(*Refresher, sqlbase.ID) error) {
	old := gossipTableStatAddedFn
	gossipTableStatAddedFn = fn
	t.Cleanup(func() { gossipTableStatAddedFn = old })
}

// ---- tests for maybeRefreshTsStats ----

func TestMaybeRefreshTsStats_CPUError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		cpuFn: func(time.Duration, bool) ([]float64, error) {
			return nil, errors.New("cpu error")
		},
	})

	r.maybeRefreshTsStats(
		context.Background(),
		sqlbase.ID(1),
		&sqlbase.TableDescriptor{},
		Affected{},
		0,
	)
}

func TestMaybeRefreshTsStats_CPUTooHigh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		cpuFn: func(time.Duration, bool) ([]float64, error) {
			return []float64{80.0}, nil
		},
	})

	r.maybeRefreshTsStats(
		context.Background(),
		sqlbase.ID(1),
		&sqlbase.TableDescriptor{},
		Affected{},
		0,
	)
}

func TestMaybeRefreshTsStats_GetTableStatsError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		cpuFn: func(time.Duration, bool) ([]float64, error) {
			return []float64{10.0}, nil
		},
	})
	setGetTableStatsFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) ([]*TableStatistic, error) {
		return nil, errors.New("cache error")
	})

	r.maybeRefreshTsStats(
		context.Background(),
		sqlbase.ID(1),
		&sqlbase.TableDescriptor{},
		Affected{},
		0,
	)
}

func TestMaybeRefreshTsStats_FirstRefreshStatsPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	called := 0

	AutomaticTsStatisticsClusterMode.Override(&r.st.SV, true)
	AutomaticTagStatisticsClusterMode.Override(&r.st.SV, true)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		cpuFn: func(time.Duration, bool) ([]float64, error) {
			return []float64{10.0}, nil
		},
		firstRefreshStatsFn: func(_ context.Context, _ *Refresher, _ sqlbase.ID, _ time.Duration) error {
			called++
			return nil
		},
	})
	setGetTableStatsFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) ([]*TableStatistic, error) {
		return nil, nil
	})

	r.maybeRefreshTsStats(
		context.Background(),
		sqlbase.ID(1),
		&sqlbase.TableDescriptor{Columns: []sqlbase.ColumnDescriptor{makeColumn(1, sqlbase.ColumnType_TYPE_DATA)}},
		Affected{},
		0,
	)

	if called != 1 {
		t.Fatalf("expected firstRefreshStats to be called once, got %d", called)
	}
}

func TestMaybeRefreshTsStats_FirstRefreshTagStatsPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	called := 0

	AutomaticTsStatisticsClusterMode.Override(&r.st.SV, false)
	AutomaticTagStatisticsClusterMode.Override(&r.st.SV, true)

	setTestTSRefreshPolicy(t, fakeTSRefresherPolicy{
		cpuFn: func(time.Duration, bool) ([]float64, error) {
			return []float64{10.0}, nil
		},
		firstRefreshTagFn: func(_ context.Context, _ *Refresher, _ sqlbase.ID, _ []sqlbase.ColumnDescriptor, _ bool, _ *sqlbase.TableDescriptor) error {
			called++
			return nil
		},
	})
	setGetTableStatsFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) ([]*TableStatistic, error) {
		return nil, nil
	})

	r.maybeRefreshTsStats(
		context.Background(),
		sqlbase.ID(1),
		&sqlbase.TableDescriptor{
			Columns: []sqlbase.ColumnDescriptor{
				makeColumn(1, sqlbase.ColumnType_TYPE_TAG),
				makeColumn(2, sqlbase.ColumnType_TYPE_DATA),
			},
		},
		Affected{},
		0,
	)

	if called != 1 {
		t.Fatalf("expected firstRefreshTagStats to be called once, got %d", called)
	}
}

// ---- tests for refreshTagStats / refreshMetricStats ----

func TestRefreshTagStats_SQLWithoutOrderedHistogram(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	err := r.refreshTagStats(
		context.Background(),
		sqlbase.ID(99),
		[]sqlbase.ColumnDescriptor{
			makeColumn(1, sqlbase.ColumnType_TYPE_TAG),
			makeColumn(2, sqlbase.ColumnType_TYPE_PTAG),
			makeColumn(3, sqlbase.ColumnType_TYPE_DATA),
		},
		false,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fakeEx.execCalls) != 1 {
		t.Fatalf("expected 1 exec call, got %d", len(fakeEx.execCalls))
	}
	sql := fakeEx.execCalls[0].query
	if !strings.Contains(sql, "CREATE STATISTICS") || !strings.Contains(sql, "ON [1,2] FROM [99]") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if strings.Contains(sql, "collect_sorted_histogram") {
		t.Fatalf("unexpected ordered histogram option in sql: %s", sql)
	}
}

func TestRefreshTagStats_SQLWithOrderedHistogram(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	err := r.refreshTagStats(
		context.Background(),
		sqlbase.ID(99),
		[]sqlbase.ColumnDescriptor{
			makeColumn(1, sqlbase.ColumnType_TYPE_TAG),
			makeColumn(2, sqlbase.ColumnType_TYPE_DATA),
		},
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql := fakeEx.execCalls[0].query
	if !strings.Contains(sql, "collect_sorted_histogram") {
		t.Fatalf("expected ordered histogram option, sql=%s", sql)
	}
}

func TestRefreshMetricStats_SQLForDataColumnsOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	err := r.refreshMetricStats(
		context.Background(),
		sqlbase.ID(123),
		[]sqlbase.ColumnDescriptor{
			makeColumn(10, sqlbase.ColumnType_TYPE_DATA),
			makeColumn(11, sqlbase.ColumnType_TYPE_TAG),
			makeColumn(12, sqlbase.ColumnType_TYPE_DATA),
		},
		nil,
		false,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql := fakeEx.execCalls[0].query
	if !strings.Contains(sql, "ON [10,12] FROM [123]") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

func TestRefreshMetricStats_UpdateTableStatisticsPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	setGetDatabaseDescFromIDFn(
		t,
		func(_ context.Context, _ *Refresher, _ sqlbase.ID,
		) (*sqlbase.DatabaseDescriptor, error) {
			return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
		})

	setGossipTableStatAddedFn(t, func(_ *Refresher, _ sqlbase.ID) error {
		return nil
	})

	fakeEx.queryRowRes = tree.Datums{tree.NewDInt(100)}

	err := r.refreshMetricStats(
		context.Background(),
		sqlbase.ID(123),
		[]sqlbase.ColumnDescriptor{
			makeColumn(10, sqlbase.ColumnType_TYPE_DATA),
			makeColumn(11, sqlbase.ColumnType_TYPE_TAG),
		},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fakeEx.execCalls) < 2 {
		t.Fatalf("expected delete+insert exec calls, got %d", len(fakeEx.execCalls))
	}
}

// ---- tests for firstRefreshTagStats / firstRefreshStats ----

func TestFirstRefreshTagStats_ConcurrentErrorRequeues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	setGetDatabaseDescFromIDFn(t, func(
		_ context.Context, _ *Refresher, _ sqlbase.ID,
	) (*sqlbase.DatabaseDescriptor, error) {
		return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
	})

	setGossipTableStatAddedFn(t, func(_ *Refresher, _ sqlbase.ID) error {
		return nil
	})

	// tag create succeeds; metric update fails with concurrent error
	fakeEx.queryRowRes = []tree.Datum{tree.NewDInt(10)}
	fakeEx.execHook = func(opName, query string, args ...interface{}) error {
		if opName == "insert-statistic" {
			return ConcurrentCreateStatsError
		}
		return nil
	}

	err := r.firstRefreshTagStats(
		context.Background(),
		sqlbase.ID(9),
		[]sqlbase.ColumnDescriptor{
			makeColumn(1, sqlbase.ColumnType_TYPE_TAG),
			makeColumn(2, sqlbase.ColumnType_TYPE_DATA),
		},
		false,
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if !errors.Is(err, ConcurrentCreateStatsError) {
		t.Fatalf("expected concurrent error, got %v", err)
	}

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(9) || m.rowsAffected != 0 || m.entitiesAffected != 0 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected mutation requeue")
	}
}

func TestFirstRefreshStats_ConcurrentErrorRequeues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()

	setFirstRefreshTsStatsFn(
		t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) error {
			return ConcurrentCreateStatsError
		})

	err := r.firstRefreshStats(context.Background(), sqlbase.ID(8), 0)
	if !errors.Is(err, ConcurrentCreateStatsError) {
		t.Fatalf("expected concurrent error, got %v", err)
	}

	select {
	case m := <-r.mutations:
		if m.tableID != sqlbase.ID(8) || m.rowsAffected != 0 {
			t.Fatalf("unexpected mutation: %+v", m)
		}
	default:
		t.Fatal("expected mutation requeue")
	}
}

// ---- tests for updateTableStatistics ----

func TestUpdateTableStatistics_DBDescError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()

	setGetDatabaseDescFromIDFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) (*sqlbase.DatabaseDescriptor, error) {
		return nil, errors.New("db desc err")
	})

	err := r.updateTableStatistics(
		context.Background(),
		sqlbase.ID(1),
		[]sqlbase.ColumnDescriptor{makeColumn(10, sqlbase.ColumnType_TYPE_DATA)},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if err == nil || !strings.Contains(err.Error(), "db desc err") {
		t.Fatalf("expected db desc error, got %v", err)
	}
}

func TestUpdateTableStatistics_QueryRowError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{queryRowErr: errors.New("query row err")}
	r.ex = fakeEx

	setGetDatabaseDescFromIDFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) (*sqlbase.DatabaseDescriptor, error) {
		return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
	})

	err := r.updateTableStatistics(
		context.Background(),
		sqlbase.ID(1),
		[]sqlbase.ColumnDescriptor{makeColumn(10, sqlbase.ColumnType_TYPE_DATA)},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if err == nil || !strings.Contains(err.Error(), "query row err") {
		t.Fatalf("expected query row error, got %v", err)
	}
}

func TestUpdateTableStatistics_EmptyQueryRowWarning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{}
	r.ex = fakeEx

	setGetDatabaseDescFromIDFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) (*sqlbase.DatabaseDescriptor, error) {
		return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
	})

	err := r.updateTableStatistics(
		context.Background(),
		sqlbase.ID(1),
		[]sqlbase.ColumnDescriptor{makeColumn(10, sqlbase.ColumnType_TYPE_DATA)},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if err == nil {
		t.Fatal("expected warning error for empty row result")
	}
}

func TestUpdateTableStatistics_SuccessSkipsTagColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{
		queryRowRes: []tree.Datum{tree.NewDInt(100)},
	}
	r.ex = fakeEx

	gossipCalled := 0
	setGetDatabaseDescFromIDFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) (*sqlbase.DatabaseDescriptor, error) {
		return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
	})
	setGossipTableStatAddedFn(t, func(_ *Refresher, _ sqlbase.ID) error {
		gossipCalled++
		return nil
	})

	err := r.updateTableStatistics(
		context.Background(),
		sqlbase.ID(1),
		[]sqlbase.ColumnDescriptor{
			makeColumn(10, sqlbase.ColumnType_TYPE_DATA),
			makeColumn(11, sqlbase.ColumnType_TYPE_TAG),
			makeColumn(12, sqlbase.ColumnType_TYPE_DATA),
		},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 两个 data 列，每列 delete + insert，共 4 次 exec
	if len(fakeEx.execCalls) != 4 {
		t.Fatalf("expected 4 exec calls, got %d", len(fakeEx.execCalls))
	}
	if gossipCalled != 2 {
		t.Fatalf("expected gossip called twice, got %d", gossipCalled)
	}
}

func TestUpdateTableStatistics_InsertExecError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := newRefresherForMoreTests()
	fakeEx := &fakeInternalExecutor{
		queryRowRes: []tree.Datum{tree.NewDInt(100)},
		execHook: func(opName, query string, args ...interface{}) error {
			if opName == "insert-statistic" {
				return errors.New("insert err")
			}
			return nil
		},
	}
	r.ex = fakeEx

	setGetDatabaseDescFromIDFn(t, func(_ context.Context, _ *Refresher, _ sqlbase.ID) (*sqlbase.DatabaseDescriptor, error) {
		return &sqlbase.DatabaseDescriptor{Name: "db1"}, nil
	})

	err := r.updateTableStatistics(
		context.Background(),
		sqlbase.ID(1),
		[]sqlbase.ColumnDescriptor{makeColumn(10, sqlbase.ColumnType_TYPE_DATA)},
		&sqlbase.TableDescriptor{Name: "t1", ParentID: sqlbase.ID(50)},
	)
	if err == nil || !strings.Contains(err.Error(), "insert err") {
		t.Fatalf("expected insert err, got %v", err)
	}
}

func makeTestHistogramData() *HistogramData {
	return &HistogramData{
		ColumnType: *types.Int,
		Buckets: []HistogramData_Bucket{
			{
				NumEq:         10,
				NumRange:      20,
				DistinctRange: 3.5,
				UpperBound:    []byte{0x01, 0x02, 0x03},
			},
			{
				NumEq:         5,
				NumRange:      8,
				DistinctRange: 1.25,
				UpperBound:    []byte{0x09, 0x08},
			},
		},
		SortedBuckets: []HistogramData_SortedHistogramBucket{
			{
				RowCount:          100,
				UnorderedRowCount: 20,
				UnorderedEntities: 1.5,
				OrderedEntities:   3.25,
				UpperBound:        []byte{0xaa, 0xbb},
			},
		},
	}
}

func TestHistogramData_MarshalUnmarshal_RoundTrip(t *testing.T) {
	orig := makeTestHistogramData()

	data, err := protoutil.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var decoded HistogramData
	if err := protoutil.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if !reflect.DeepEqual(orig.Buckets, decoded.Buckets) {
		t.Fatalf("Buckets mismatch:\n got=%v\nwant=%v", decoded.Buckets, orig.Buckets)
	}
	if !reflect.DeepEqual(orig.SortedBuckets, decoded.SortedBuckets) {
		t.Fatalf("SortedBuckets mismatch:\n got=%v\nwant=%v", decoded.SortedBuckets, orig.SortedBuckets)
	}
	if orig.ColumnType.String() != decoded.ColumnType.String() {
		t.Fatalf("ColumnType mismatch: got=%s want=%s", decoded.ColumnType.String(), orig.ColumnType.String())
	}
}

func TestHistogramData_SizeMatchesMarshalLength(t *testing.T) {
	m := makeTestHistogramData()

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	if got, want := len(data), m.Size(); got != want {
		t.Fatalf("len(Marshal()) != Size(): got=%d want=%d", got, want)
	}
}

func TestHistogramData_MarshalToMatchesMarshal(t *testing.T) {
	m := makeTestHistogramData()

	data1, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() error: %v", err)
	}
	data2 := buf[:n]

	if !bytes.Equal(data1, data2) {
		t.Fatalf("Marshal() and MarshalTo() differ:\nmarshal=%v\nmarshalTo=%v", data1, data2)
	}
}

func TestHistogramData_Bucket_MarshalUnmarshal_RoundTrip(t *testing.T) {
	orig := &HistogramData_Bucket{
		NumEq:         7,
		NumRange:      9,
		DistinctRange: 2.75,
		UpperBound:    []byte{0x10, 0x11, 0x12},
	}

	data, err := protoutil.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var decoded HistogramData_Bucket
	if err := protoutil.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if !reflect.DeepEqual(*orig, decoded) {
		t.Fatalf("bucket mismatch:\n got=%+v\nwant=%+v", decoded, *orig)
	}
}

func TestHistogramData_SortedBucket_MarshalUnmarshal_RoundTrip(t *testing.T) {
	orig := &HistogramData_SortedHistogramBucket{
		RowCount:          123,
		UnorderedRowCount: 45,
		UnorderedEntities: 1.25,
		OrderedEntities:   4.75,
		UpperBound:        []byte{0x21, 0x22},
	}

	data, err := protoutil.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var decoded HistogramData_SortedHistogramBucket
	if err := protoutil.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if !reflect.DeepEqual(*orig, decoded) {
		t.Fatalf("sorted bucket mismatch:\n got=%+v\nwant=%+v", decoded, *orig)
	}
}

func TestHistogramData_Unmarshal_WrongWireTypeForBuckets(t *testing.T) {
	// field 1 (Buckets) should use wire type 2, here we intentionally use 0.
	data := []byte{0x08, 0x01}

	var m HistogramData
	err := protoutil.Unmarshal(data, &m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHistogramData_Bucket_Unmarshal_WrongWireTypeForDistinctRange(t *testing.T) {
	// field 4 (DistinctRange) should use wire type 1, here we intentionally use 0.
	// tag = (4 << 3) | 0 = 0x20
	data := []byte{0x20, 0x01}

	var m HistogramData_Bucket
	err := protoutil.Unmarshal(data, &m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHistogramData_SortedBucket_Unmarshal_WrongWireTypeForUpperBound(t *testing.T) {
	// field 5 (UpperBound) should use wire type 2, here we intentionally use 1.
	// tag = (5 << 3) | 1 = 0x29, followed by 8 bytes because wire type 1 is fixed64.
	data := []byte{0x29, 1, 2, 3, 4, 5, 6, 7, 8}

	var m HistogramData_SortedHistogramBucket
	err := protoutil.Unmarshal(data, &m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHistogramData_Unmarshal_UnexpectedEOF(t *testing.T) {
	m := makeTestHistogramData()

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if len(data) < 2 {
		t.Fatal("encoded data too short for truncation test")
	}

	truncated := data[:len(data)-1]

	var decoded HistogramData
	err = protoutil.Unmarshal(truncated, &decoded)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected %v, got %v", io.ErrUnexpectedEOF, err)
	}
}

func TestSkipHistogram_Varint(t *testing.T) {
	// field 1, wire type 0, value 150
	data := []byte{0x08, 0x96, 0x01}

	n, err := skipHistogram(data)
	if err != nil {
		t.Fatalf("skipHistogram() error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected skip length %d, got %d", len(data), n)
	}
}

func TestSkipHistogram_Fixed64(t *testing.T) {
	// field 1, wire type 1, then 8 bytes payload
	data := []byte{0x09, 1, 2, 3, 4, 5, 6, 7, 8}

	n, err := skipHistogram(data)
	if err != nil {
		t.Fatalf("skipHistogram() error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected skip length %d, got %d", len(data), n)
	}
}

func TestSkipHistogram_LengthDelimited(t *testing.T) {
	// field 1, wire type 2, len=3, payload=3 bytes
	data := []byte{0x0a, 0x03, 0x11, 0x22, 0x33}

	n, err := skipHistogram(data)
	if err != nil {
		t.Fatalf("skipHistogram() error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected skip length %d, got %d", len(data), n)
	}
}

func TestSkipHistogram_UnexpectedEOF(t *testing.T) {
	data := []byte{0x80}

	_, err := skipHistogram(data)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected %v, got %v", io.ErrUnexpectedEOF, err)
	}
}

func TestSkipHistogram_IllegalWireType(t *testing.T) {
	// wire type 7 is illegal
	data := []byte{0x0f}

	_, err := skipHistogram(data)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSkipHistogram_IntOverflow(t *testing.T) {
	// varint that never terminates within 64 bits
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}

	_, err := skipHistogram(data)
	if err != ErrIntOverflowHistogram {
		t.Fatalf("expected %v, got %v", ErrIntOverflowHistogram, err)
	}
}

func TestEncodeVarintHistogram_Simple(t *testing.T) {
	buf := make([]byte, 10)
	n := encodeVarintHistogram(buf, 0, 300)

	// 300 => 0xac 0x02
	want := []byte{0xac, 0x02}
	if !bytes.Equal(buf[:n], want) {
		t.Fatalf("unexpected encoding: got=%v want=%v", buf[:n], want)
	}
}

func TestSovHistogram(t *testing.T) {
	cases := []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
	}

	for _, tc := range cases {
		if got := sovHistogram(tc.v); got != tc.want {
			t.Fatalf("sovHistogram(%d): got=%d want=%d", tc.v, got, tc.want)
		}
	}
}

func TestSozHistogram(t *testing.T) {
	vals := []uint64{0, 1, 2, 127, 128, math.MaxInt32}
	for _, v := range vals {
		if got := sozHistogram(v); got <= 0 {
			t.Fatalf("sozHistogram(%d): got=%d, want > 0", v, got)
		}
	}
}
