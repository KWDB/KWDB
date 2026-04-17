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

package rowexec

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func BenchmarkNoop(b *testing.B) {
	const numRows = 1 << 16

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &execinfra.RowDisposer{}
	for _, numCols := range []int{1, 1 << 1, 1 << 2, 1 << 4, 1 << 8} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			cols := make([]types.T, numCols)
			for i := range cols {
				cols[i] = *types.Int
			}
			input := execinfra.NewRepeatableRowSource(cols, sqlbase.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newNoopProcessor(flowCtx, 0, input, post, disposer, nil)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}

func TestNoopNextShortCircuitMetaErrorDoesNotDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	input := &execinfra.RowChannel{}
	input.InitWithNumSenders([]types.T{*types.Int}, 2)

	expectedErr := errors.New("short-circuit input failed")
	if status := input.Push(nil, &execinfrapb.ProducerMetadata{Err: expectedErr}); status != execinfra.NeedMoreRows {
		t.Fatalf("unexpected push status: %v", status)
	}

	noop, err := newNoopProcessor(
		flowCtx,
		0,
		input,
		&execinfrapb.PostProcessSpec{},
		&execinfra.RowDisposer{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	noop.Start(ctx)
	defer noop.ConsumerClosed()

	row, meta := noop.NextShortCircuitMeta()
	if row != nil {
		t.Fatalf("expected no row, got %v", row)
	}
	if meta == nil {
		t.Fatal("expected error metadata, got nil")
	}
	if !errors.Is(meta.Err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, meta.Err)
	}
	if noop.State != execinfra.StateRunning {
		t.Fatalf("expected noop to stay running after error metadata, got %v", noop.State)
	}
}

func TestNoopRunShortCircuitDoesNotBlockOnErrorFromUnclosedInput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	input := &execinfra.RowChannel{}
	input.InitWithNumSenders([]types.T{*types.Int}, 2)

	expectedErr := errors.New("remote short-circuit producer failed")
	if status := input.Push(nil, &execinfrapb.ProducerMetadata{Err: expectedErr}); status != execinfra.NeedMoreRows {
		t.Fatalf("unexpected push status: %v", status)
	}

	noop, err := newNoopProcessor(
		flowCtx,
		0,
		input,
		&execinfrapb.PostProcessSpec{},
		&execinfra.RowDisposer{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer noop.ConsumerClosed()

	errCh := make(chan error, 1)
	cleanupCalled := make(chan struct{})
	go func() {
		errCh <- noop.runShortCircuit(ctx, timeutil.Now(), func() {
			close(cleanupCalled)
		})
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		select {
		case <-cleanupCalled:
		default:
			t.Fatal("expected RunShortCircuit to run cleanup before returning")
		}
	case <-time.After(500 * time.Millisecond):
		// This is the pre-fix failure mode: the error metadata moved the noop
		// processor to draining, and DrainHelper waited for every producer to
		// close the RowChannel.
		input.ProducerDone()
		input.ProducerDone()
		t.Fatal("RunShortCircuit blocked waiting for an unclosed input after receiving error metadata")
	}
}
