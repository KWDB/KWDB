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

package sql

import (
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

// Test that we don't attempt to create flows in an aborted transaction.
// Instead, a retryable error is created on the gateway. The point is to
// simulate a race where the heartbeat loop finds out that the txn is aborted
// just before a plan starts execution and check that we don't create flows in
// an aborted txn (which isn't allowed). Note that, once running, each flow can
// discover on its own that its txn is aborted - that's handled separately. But
// flows can't start in a txn that's already known to be aborted.
//
// We test this by manually aborting a txn and then attempting to execute a plan
// in it. We're careful to not use the transaction for anything but running the
// plan; planning will be performed outside of the transaction.
func TestDistSQLRunningInAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.ExecContext(
		ctx, "create database test; create table test.t(a int)"); err != nil {
		t.Fatal(err)
	}
	key := roachpb.Key("a")

	// Plan a statement.
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := internalPlanner.(*planner)
	query := "select * from test.t"
	stmt, err := parser.ParseOne(query)
	if err != nil {
		t.Fatal(err)
	}

	push := func(ctx context.Context, key roachpb.Key) error {
		// Conflicting transaction that pushes another transaction.
		conflictTxn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		// We need to explicitly set a high priority for the push to happen.
		if err := conflictTxn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		// Push through a Put, as opposed to a Get, so that the pushee gets aborted.
		if err := conflictTxn.Put(ctx, key, "pusher was here"); err != nil {
			return err
		}
		return conflictTxn.CommitOrCleanup(ctx)
	}

	// Make a db with a short heartbeat interval, so that the aborted txn finds
	// out quickly.
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			// Short heartbeat interval.
			HeartbeatInterval: time.Millisecond,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		s.DistSenderI().(*kvcoord.DistSender),
	)
	shortDB := kv.NewDB(ambient, tsf, s.Clock())

	iter := 0
	// We'll trace to make sure the test isn't fooling itself.
	runningCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
	defer cancel()
	err = shortDB.Txn(runningCtx, func(ctx context.Context, txn *kv.Txn) error {
		iter++
		if iter == 1 {
			// On the first iteration, abort the txn.

			if err := txn.Put(ctx, key, "val"); err != nil {
				t.Fatal(err)
			}

			if err := push(ctx, key); err != nil {
				t.Fatal(err)
			}

			// Now wait until the heartbeat loop notices that the transaction is aborted.
			testutils.SucceedsSoon(t, func() error {
				if txn.Sender().(*kvcoord.TxnCoordSender).IsTracking() {
					return fmt.Errorf("txn heartbeat loop running")
				}
				return nil
			})
		}

		// Create and run a DistSQL plan.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			stmt.AST.StatementType(),
			execCfg.RangeDescriptorCache,
			execCfg.LeaseHolderCache,
			txn,
			func(ts hlc.Timestamp) {
				execCfg.Clock.Update(ts)
			},
			p.ExtendedEvalContext().Tracing,
		)

		// We need to re-plan every time, since close() below makes
		// the plan unusable across retries.
		p.stmt = &Statement{Statement: stmt}
		if err := p.makeOptimizerPlan(ctx); err != nil {
			t.Fatal(err)
		}
		defer p.curPlan.close(ctx)

		evalCtx := p.ExtendedEvalContext()
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, nil /* txn */)
		// We need isLocal = false so that we executing the plan involves marshaling
		// the root txn meta to leaf txns. Local flows can start in aborted txns
		// because they just use the root txn.
		planCtx.isLocal = false
		planCtx.planner = p
		planCtx.stmtType = recv.stmtType

		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, txn, p.curPlan.plan, recv, p.GetStmt(), nil,
		)()
		return rw.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if iter != 2 {
		t.Fatalf("expected two iterations, but txn took %d to succeed", iter)
	}
	if tracing.FindMsgInRecording(getRec(), clientRejectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", clientRejectedMsg)
	}
}

// Test that the DistSQLReceiver overwrites previous errors as "better" errors
// come along.
func TestDistSQLReceiverErrorRanking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test goes through the trouble of creating a server because it wants to
	// create a txn. It creates the txn because it wants to test an interaction
	// between the DistSQLReceiver and the TxnCoordSender: the DistSQLReceiver
	// will feed retriable errors to the TxnCoordSender which will change those
	// errors to TransactionRetryWithProtoRefreshError.
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	txn := kv.NewTxn(ctx, db, s.NodeID())

	// We're going to use a rowResultWriter to which only errors will be passed.
	rw := newCallbackResultWriter(nil /* fn */)
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		tree.Rows, /* StatementType */
		nil,       /* rangeCache */
		nil,       /* leaseCache */
		txn,
		func(hlc.Timestamp) {}, /* updateClock */
		&SessionTracing{},
	)

	retryErr := roachpb.NewErrorWithTxn(
		roachpb.NewTransactionRetryError(
			roachpb.RETRY_SERIALIZABLE, "test err"),
		txn.TestingCloneTxn()).GoError()

	abortErr := roachpb.NewErrorWithTxn(
		roachpb.NewTransactionAbortedError(
			roachpb.ABORT_REASON_ABORTED_RECORD_FOUND),
		txn.TestingCloneTxn()).GoError()

	errs := []struct {
		err    error
		expErr string
	}{
		{
			// Initial error, retriable.
			err:    retryErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionRetryError",
		},
		{
			// A non-retriable error overwrites a retriable one.
			err:    fmt.Errorf("err1"),
			expErr: "err1",
		},
		{
			// Another non-retriable error doesn't overwrite the previous one.
			err:    fmt.Errorf("err2"),
			expErr: "err1",
		},
		{
			// A TransactionAbortedError overwrites anything.
			err:    abortErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionAbortedError",
		},
		{
			// A non-aborted retriable error does not overried the
			// TransactionAbortedError.
			err:    retryErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionAbortedError",
		},
	}

	for i, tc := range errs {
		recv.Push(nil, /* row */
			&execinfrapb.ProducerMetadata{
				Err: tc.err,
			})
		if !testutils.IsError(rw.Err(), tc.expErr) {
			t.Fatalf("%d: expected %s, got %s", i, tc.expErr, rw.Err())
		}
	}
}

type testResultWriter struct {
	rows            []tree.Datums
	rowsAffected    int
	err             error
	addRowErr       error
	addPGResultErr  error
	pgResults       [][]byte
	pgCompleteCmd   string
	pgCompleteTyp   tree.StatementType
	pgCompleteRows  int
	pgCompleteCalls int
}

func (w *testResultWriter) AddRow(_ context.Context, row tree.Datums) error {
	copied := append(tree.Datums(nil), row...)
	w.rows = append(w.rows, copied)
	return w.addRowErr
}

func (w *testResultWriter) AddPGResult(_ context.Context, res []byte) error {
	w.pgResults = append(w.pgResults, append([]byte(nil), res...))
	return w.addPGResultErr
}

func (w *testResultWriter) IsCommandResult() bool { return false }

func (w *testResultWriter) AddPGComplete(cmd string, typ tree.StatementType, rowsAffected int) {
	w.pgCompleteCmd = cmd
	w.pgCompleteTyp = typ
	w.pgCompleteRows = rowsAffected
	w.pgCompleteCalls++
}

func (w *testResultWriter) IncrementRowsAffected(n int) { w.rowsAffected += n }

func (w *testResultWriter) RowsAffected() int { return w.rowsAffected }

func (w *testResultWriter) SetError(err error) { w.err = err }

func (w *testResultWriter) Err() error { return w.err }

type testMetadataWriter struct {
	testResultWriter
	metas []*execinfrapb.ProducerMetadata
}

func (w *testMetadataWriter) AddMeta(_ context.Context, meta *execinfrapb.ProducerMetadata) {
	w.metas = append(w.metas, meta)
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic, got nil")
		}
	}()
	fn()
}

func makeIntRow(v int64) sqlbase.EncDatumRow {
	return sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(v))),
	}
}

func TestReverseCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src := []execinfrapb.ProcessorSpec{
		{ProcessorID: 1},
		{ProcessorID: 2},
		{ProcessorID: 3},
	}
	got := reverseCopy(src)

	if len(got) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(got))
	}
	if got[0].ProcessorID != 3 || got[1].ProcessorID != 2 || got[2].ProcessorID != 1 {
		t.Fatalf("unexpected reverseCopy result: %+v", got)
	}
	if src[0].ProcessorID != 1 || src[1].ProcessorID != 2 || src[2].ProcessorID != 3 {
		t.Fatalf("reverseCopy modified source slice: %+v", src)
	}
}

func TestNeedInputProcessors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name string
		spec execinfrapb.ProcessorSpec
		want bool
	}{
		{
			name: "noop without extra work",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Post: execinfrapb.PostProcessSpec{},
			},
			want: false,
		},
		{
			name: "noop with filter",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Post: execinfrapb.PostProcessSpec{
					Filter: execinfrapb.Expression{Expr: "@1 > 1"},
				},
			},
			want: true,
		},
		{
			name: "noop with limit",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Post: execinfrapb.PostProcessSpec{Limit: 1},
			},
			want: true,
		},
		{
			name: "noop with projection",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Post: execinfrapb.PostProcessSpec{Projection: true},
			},
			want: true,
		},
		{
			name: "noop with unknown output type",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Post: execinfrapb.PostProcessSpec{OutputTypes: []types.T{*types.Unknown}},
			},
			want: true,
		},
		{
			name: "sorter",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Sorter: &execinfrapb.SorterSpec{}},
			},
			want: true,
		},
		{
			name: "aggregator",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Aggregator: &execinfrapb.AggregatorSpec{}},
			},
			want: true,
		},
		{
			name: "distinct",
			spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{Distinct: &execinfrapb.DistinctSpec{}},
			},
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := needInputProcessors(tc.spec); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestFindTSProcessorsAndCheckTSEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	noTS := map[roachpb.NodeID]*execinfrapb.FlowSpec{
		1: {
			Processors: []execinfrapb.ProcessorSpec{
				{
					Core: execinfrapb.ProcessorCoreUnion{
						Noop: &execinfrapb.NoopCoreSpec{},
					},
				},
			},
		},
	}
	if findTSProcessors(noTS) {
		t.Fatal("expected findTSProcessors(noTS) to be false")
	}
	if CheckTSEngine(&noTS) {
		t.Fatal("expected CheckTSEngine(noTS) to be false")
	}

	withTS := map[roachpb.NodeID]*execinfrapb.FlowSpec{
		1: {
			Processors: []execinfrapb.ProcessorSpec{
				{
					Core: execinfrapb.ProcessorCoreUnion{
						Noop: &execinfrapb.NoopCoreSpec{},
					},
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
	}
	if !findTSProcessors(withTS) {
		t.Fatal("expected findTSProcessors(withTS) to be true")
	}
	if !CheckTSEngine(&withTS) {
		t.Fatal("expected CheckTSEngine(withTS) to be true")
	}
}

func TestMakeDistSQLReceiverCloneReleaseAndHelpers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	writer := &testResultWriter{}
	recv := MakeDistSQLReceiver(
		ctx,
		writer,
		tree.Rows,
		nil, nil, nil,
		func(hlc.Timestamp) {},
		&SessionTracing{},
	)

	recv.outputTypes = []types.T{*types.Int, *types.String}
	recv.resultToStreamColMap = []int{0, 1}

	if got := recv.Types(); !reflect.DeepEqual(got, []types.T{*types.Int, *types.String}) {
		t.Fatalf("unexpected Types(): %v", got)
	}
	if got := recv.GetCols(); got != 2 {
		t.Fatalf("expected GetCols()=2, got %d", got)
	}

	recv.AddStats(2*time.Second, true)
	recv.AddStats(3*time.Second, false)
	stats := recv.GetStats()
	if stats.NumRows != 1 {
		t.Fatalf("expected NumRows=1, got %d", stats.NumRows)
	}
	if stats.StallTime != 5*time.Second {
		t.Fatalf("expected StallTime=5s, got %s", stats.StallTime)
	}

	if err := recv.PushPGResult(ctx, []byte("pg")); err != nil {
		t.Fatalf("unexpected PushPGResult error: %v", err)
	}
	recv.AddPGComplete("SELECT", tree.Rows, 7)
	if writer.pgCompleteCalls != 1 || writer.pgCompleteCmd != "SELECT" || writer.pgCompleteRows != 7 {
		t.Fatalf("unexpected AddPGComplete capture: %+v", writer)
	}

	recv.SetError(stderrors.New("set-error"))
	if writer.Err() == nil || writer.Err().Error() != "set-error" {
		t.Fatalf("expected writer error to be set, got %v", writer.Err())
	}

	cloned := recv.clone()
	if cloned == recv {
		t.Fatal("expected clone to return a different receiver")
	}
	if cloned.ctx != recv.ctx {
		t.Fatal("expected clone to keep ctx")
	}
	if cloned.stmtType != tree.Rows {
		t.Fatalf("expected cloned stmtType Rows, got %v", cloned.stmtType)
	}
	if cloned.rangeCache != recv.rangeCache || cloned.leaseCache != recv.leaseCache || cloned.txn != recv.txn {
		t.Fatal("expected clone to copy receiver dependencies")
	}

	cleanupCalled := 0
	recv.cleanup = func() { cleanupCalled++ }
	recv.ProducerDone()
	if cleanupCalled != 1 {
		t.Fatalf("expected cleanup to run once, got %d", cleanupCalled)
	}
	assertPanics(t, func() { recv.ProducerDone() })

	cloned.Release()
	if cloned.ctx != nil || cloned.resultWriter != nil || cloned.cleanup != nil {
		t.Fatalf("expected Release() to reset receiver, got %+v", cloned)
	}
}

func TestMetadataCallbackWriterAddMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	meta := &execinfrapb.ProducerMetadata{}

	t.Run("success", func(t *testing.T) {
		w := &metadataCallbackWriter{
			rowResultWriter: &testResultWriter{},
			fn: func(_ context.Context, m *execinfrapb.ProducerMetadata) error {
				if m != meta {
					t.Fatalf("unexpected metadata pointer: %p", m)
				}
				return nil
			},
		}
		w.AddMeta(ctx, meta)
		if err := w.Err(); err != nil {
			t.Fatalf("unexpected writer error: %v", err)
		}
	})

	t.Run("error propagates via SetError", func(t *testing.T) {
		base := &testResultWriter{}
		w := &metadataCallbackWriter{
			rowResultWriter: base,
			fn: func(_ context.Context, _ *execinfrapb.ProducerMetadata) error {
				return stderrors.New("meta failed")
			},
		}
		w.AddMeta(ctx, meta)
		if base.Err() == nil || base.Err().Error() != "meta failed" {
			t.Fatalf("expected propagated meta error, got %v", base.Err())
		}
	})
}

func TestErrOnlyResultWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	w := &errOnlyResultWriter{}
	w.SetError(stderrors.New("x"))
	if w.Err() == nil || w.Err().Error() != "x" {
		t.Fatalf("unexpected Err(): %v", w.Err())
	}
	if w.IsCommandResult() {
		t.Fatal("expected IsCommandResult() to be false")
	}

	assertPanics(t, func() { _ = w.AddPGResult(context.Background(), []byte("x")) })
	assertPanics(t, func() { w.AddPGComplete("SELECT", tree.Rows, 1) })
	assertPanics(t, func() { _ = w.AddRow(context.Background(), tree.Datums{}) })
	assertPanics(t, func() { w.IncrementRowsAffected(1) })
	assertPanics(t, func() { _ = w.RowsAffected() })
}

func TestDistSQLReceiverPushBasicPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("non-row statement increments rows affected from metadata row", func(t *testing.T) {
		w := &testResultWriter{}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.DDL,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		status := recv.Push(makeIntRow(7), nil)
		if status != execinfra.NeedMoreRows {
			t.Fatalf("expected NeedMoreRows, got %v", status)
		}
		if w.RowsAffected() != 7 {
			t.Fatalf("expected RowsAffected=7, got %d", w.RowsAffected())
		}
	})

	t.Run("noColsRequired closes after first row", func(t *testing.T) {
		w := &testResultWriter{}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		recv.noColsRequired = true
		status := recv.Push(makeIntRow(1), nil)
		if status != execinfra.ConsumerClosed {
			t.Fatalf("expected ConsumerClosed, got %v", status)
		}
		if len(w.rows) != 1 || len(w.rows[0]) != 0 {
			t.Fatalf("expected one empty row, got %+v", w.rows)
		}
	})

	t.Run("discardRows ignores decoded output", func(t *testing.T) {
		w := &testResultWriter{}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		recv.discardRows = true
		status := recv.Push(makeIntRow(1), nil)
		if status != execinfra.NeedMoreRows {
			t.Fatalf("expected NeedMoreRows, got %v", status)
		}
		if len(w.rows) != 0 {
			t.Fatalf("expected no rows written, got %+v", w.rows)
		}
	})

	t.Run("limited result closed requests drain", func(t *testing.T) {
		w := &testResultWriter{addRowErr: ErrLimitedResultClosed}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		recv.outputTypes = []types.T{*types.Int}
		recv.resultToStreamColMap = []int{0}
		status := recv.Push(makeIntRow(5), nil)
		if status != execinfra.DrainRequested {
			t.Fatalf("expected DrainRequested, got %v", status)
		}
		if w.Err() != nil {
			t.Fatalf("expected no writer error, got %v", w.Err())
		}
		if recv.commErr != nil {
			t.Fatalf("expected no commErr, got %v", recv.commErr)
		}
	})

	t.Run("limited result not supported closes without commErr", func(t *testing.T) {
		w := &testResultWriter{addRowErr: ErrLimitedResultNotSupported}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		recv.outputTypes = []types.T{*types.Int}
		recv.resultToStreamColMap = []int{0}
		status := recv.Push(makeIntRow(5), nil)
		if status != execinfra.ConsumerClosed {
			t.Fatalf("expected ConsumerClosed, got %v", status)
		}
		if !stderrors.Is(w.Err(), ErrLimitedResultNotSupported) {
			t.Fatalf("expected writer error ErrLimitedResultNotSupported, got %v", w.Err())
		}
		if recv.commErr != nil {
			t.Fatalf("expected commErr to remain nil, got %v", recv.commErr)
		}
	})

	t.Run("canceled context closes receiver", func(t *testing.T) {
		w := &testResultWriter{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		recv := MakeDistSQLReceiver(
			ctx, w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)
		status := recv.Push(nil, nil)
		if status != execinfra.ConsumerClosed {
			t.Fatalf("expected ConsumerClosed, got %v", status)
		}
		if w.Err() == nil {
			t.Fatal("expected canceled context error to be recorded")
		}
	})
}

func setReflectBoolField(t *testing.T, f reflect.Value, v bool, name string) {
	t.Helper()
	if !f.IsValid() {
		t.Fatalf("field %s not found", name)
	}
	if f.Kind() != reflect.Bool {
		t.Fatalf("field %s is not bool, got %s", name, f.Kind())
	}
	f.SetBool(v)
}

func setReflectIntField(t *testing.T, f reflect.Value, v int64, name string) {
	t.Helper()
	if !f.IsValid() {
		t.Fatalf("field %s not found", name)
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		f.SetUint(uint64(v))
	default:
		t.Fatalf("field %s is not integer-like, got %s", name, f.Kind())
	}
}

func setReflectStringField(t *testing.T, f reflect.Value, v string, name string) {
	t.Helper()
	if !f.IsValid() {
		t.Fatalf("field %s not found", name)
	}
	if f.Kind() != reflect.String {
		t.Fatalf("field %s is not string, got %s", name, f.Kind())
	}
	f.SetString(v)
}

func makeTsInsertProducerMeta(
	t *testing.T,
	insertSuccess bool,
	dedupRows int64,
	dedupRule int64,
	insertRows int64,
	numRow int64,
	insertErr string,
) *execinfrapb.ProducerMetadata {
	t.Helper()

	meta := &execinfrapb.ProducerMetadata{}
	mv := reflect.ValueOf(meta).Elem()

	tsInsertField := mv.FieldByName("TsInsert")
	if !tsInsertField.IsValid() {
		t.Fatal("ProducerMetadata.TsInsert not found")
	}
	if tsInsertField.Kind() != reflect.Ptr {
		t.Fatalf("ProducerMetadata.TsInsert is not a pointer, got %s", tsInsertField.Kind())
	}
	if tsInsertField.IsNil() {
		tsInsertField.Set(reflect.New(tsInsertField.Type().Elem()))
	}

	tsInsert := tsInsertField.Elem()
	setReflectBoolField(t, tsInsert.FieldByName("InsertSuccess"), insertSuccess, "InsertSuccess")
	setReflectIntField(t, tsInsert.FieldByName("DedupRows"), dedupRows, "DedupRows")
	setReflectIntField(t, tsInsert.FieldByName("DedupRule"), dedupRule, "DedupRule")
	setReflectIntField(t, tsInsert.FieldByName("InsertRows"), insertRows, "InsertRows")
	setReflectIntField(t, tsInsert.FieldByName("NumRow"), numRow, "NumRow")
	setReflectStringField(t, tsInsert.FieldByName("InsertErr"), insertErr, "InsertErr")

	return meta
}

func TestDistSQLReceiverPushMetadataAndMetaWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("ts insert dedup path", func(t *testing.T) {
		w := &testMetadataWriter{}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)

		meta := makeTsInsertProducerMeta(
			t,
			true, /* insertSuccess */
			2,    /* dedupRows */
			int64(execinfrapb.DedupRule_TsReject),
			5,  /* insertRows */
			0,  /* numRow */
			"", /* insertErr */
		)
		status := recv.Push(nil, meta)
		if status != execinfra.ConsumerClosed {
			t.Fatalf("expected ConsumerClosed, got %v", status)
		}
		if !recv.useDeepRule || recv.dedupRows != 2 || recv.insertRows != 5 {
			t.Fatalf("unexpected dedup state: recv=%+v", recv)
		}
		if w.RowsAffected() != 5 {
			t.Fatalf("expected RowsAffected=5, got %d", w.RowsAffected())
		}
		if len(w.metas) != 1 || w.metas[0] != meta {
			t.Fatalf("expected metadata writer to receive meta, got %+v", w.metas)
		}
	})

	t.Run("ts insert failure path", func(t *testing.T) {
		w := &testResultWriter{}
		recv := MakeDistSQLReceiver(
			context.Background(), w, tree.Rows,
			nil, nil, nil,
			func(hlc.Timestamp) {},
			&SessionTracing{},
		)

		status := recv.Push(nil, makeTsInsertProducerMeta(
			t,
			false, /* insertSuccess */
			0,     /* dedupRows */
			0,     /* dedupRule */
			0,     /* insertRows */
			3,     /* numRow */
			"boom",
		))
		if status != execinfra.ConsumerClosed {
			t.Fatalf("expected ConsumerClosed, got %v", status)
		}
		if w.RowsAffected() != 3 {
			t.Fatalf("expected RowsAffected=3, got %d", w.RowsAffected())
		}
		if w.Err() == nil {
			t.Fatal("expected insert failure error")
		}
	})
}
