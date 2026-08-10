// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// TestArrowProjectionNow verifies that the Arrow projection executor evaluates
// now()/current_timestamp (Kind "datetime", Func "now") by emitting the current
// statement timestamp as a timezone-aware TIMESTAMPTZ column whose length matches
// the input and whose value equals evalCtx.GetStmtTimestamp().
func TestArrowProjectionNow(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	const n = 5
	// A trivial input record (one int64 column) only to supply the row count.
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	defer ib.Release()
	for i := 0; i < n; i++ {
		ib.Append(int64(i))
	}
	in := array.NewRecord(arrow.NewSchema(
		[]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil),
		[]arrow.Array{ib.NewArray()}, int64(n))
	defer in.Release()

	p := &arrowProjection{alloc: memory.DefaultAllocator, evalCtx: &evalCtx}
	spec := ArrowProjectionSpec{Kind: "datetime", Func: "now", TZ: true}

	arr, err := p.evalArrowDatetimeFunc(context.Background(), in, spec)
	if err != nil {
		t.Fatalf("evalArrowDatetimeFunc(now): %v", err)
	}
	defer arr.Release()

	if arr.Len() != n {
		t.Fatalf("now() array len = %d, want %d", arr.Len(), n)
	}
	tsArr, ok := arr.(*array.Timestamp)
	if !ok {
		t.Fatalf("now() produced %T, want *array.Timestamp", arr)
	}
	want := arrow.Timestamp(evalCtx.GetStmtTimestamp().In(evalCtx.GetLocation()).UnixMicro())
	for i := 0; i < n; i++ {
		if tsArr.Value(i) != want {
			t.Fatalf("row %d: now() = %v, want %v", i, tsArr.Value(i), want)
		}
	}
}
