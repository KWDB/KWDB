// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// makeArrowTimestampRecord builds an arrow.Record with one or two TIMESTAMPTZ
// columns (microsecond, timezone "UTC") populated from the given Unix-micro
// values. colNames must match the ArrowArg.ColName strings used in the spec.
func makeArrowTimestampRecord(colNames []string, cols [][]int64) arrow.Record {
	fields := make([]arrow.Field, len(colNames))
	arrs := make([]arrow.Array, len(colNames))
	for i, name := range colNames {
		b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		for _, us := range cols[i] {
			b.Append(arrow.Timestamp(us))
		}
		fields[i] = arrow.Field{Name: name, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true}
		arrs[i] = b.NewArray()
		b.Release()
	}
	n := int64(len(cols[0]))
	rec := array.NewRecord(arrow.NewSchema(fields, nil), arrs, n)
	for _, a := range arrs {
		a.Release()
	}
	return rec
}

// TestArrowProjectionAgeTwoArg verifies age(end, begin) => begin - end over two
// TIMESTAMPTZ columns, producing a DInterval string column of matching length.
func TestArrowProjectionAgeTwoArg(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	const n = 3
	// begin = 2020-01-01, end = 2020-01-02  (1 day apart)
	beginUs := int64(1577836800000000)
	endUs := int64(1577923200000000)
	rec := makeArrowTimestampRecord([]string{"col0", "col1"}, [][]int64{
		{beginUs, beginUs, beginUs}, // begin (col0)
		{endUs, endUs, endUs},       // end (col1)
	})
	defer rec.Release()

	p := &arrowProjection{alloc: memory.DefaultAllocator, evalCtx: &evalCtx}
	spec := ArrowProjectionSpec{
		Kind: "datetime",
		Func: "age",
		TZ:   true,
		Args: []ArrowArg{
			{ColName: "col0"}, // begin
			{ColName: "col1"}, // end
		},
	}

	arr, err := p.evalArrowDatetimeFunc(context.Background(), rec, spec)
	if err != nil {
		t.Fatalf("evalArrowDatetimeFunc(age 2-arg): %v", err)
	}
	defer arr.Release()

	if arr.Len() != n {
		t.Fatalf("age() array len = %d, want %d", arr.Len(), n)
	}
	sArr, ok := arr.(*array.String)
	if !ok {
		t.Fatalf("age() produced %T, want *array.String", arr)
	}
	want, err := tree.TimestampDifference(&evalCtx,
		&tree.DTimestampTZ{Time: arrowMicroToTime(arrow.Timestamp(beginUs), true, p)},
		&tree.DTimestampTZ{Time: arrowMicroToTime(arrow.Timestamp(endUs), true, p)})
	if err != nil {
		t.Fatalf("reference TimestampDifference: %v", err)
	}
	wantStr := want.String()
	for i := 0; i < n; i++ {
		if sArr.Value(i) != wantStr {
			t.Fatalf("row %d: age = %q, want %q", i, sArr.Value(i), wantStr)
		}
	}
}

// TestArrowProjectionAgeOneArg verifies age(ts) => transaction_timestamp - ts,
// where the transaction timestamp is supplied as the "__TXN_TS__" sentinel that
// the executor resolves against evalCtx.GetTxnTimestamp.
func TestArrowProjectionAgeOneArg(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	const n = 2
	// ts column = 2020-06-01
	tsUs := int64(1590969600000000)
	rec := makeArrowTimestampRecord([]string{"col0"}, [][]int64{
		{tsUs, tsUs},
	})
	defer rec.Release()

	p := &arrowProjection{alloc: memory.DefaultAllocator, evalCtx: &evalCtx}
	// Single-arg form: Args[0] is the txn sentinel, Args[1] is the column.
	spec := ArrowProjectionSpec{
		Kind: "datetime",
		Func: "age",
		TZ:   true,
		Args: []ArrowArg{
			{ColName: "", Scalar: compute.NewDatum(ageTxnTsSentinelExecutor)},
			{ColName: "col0"},
		},
	}

	arr, err := p.evalArrowDatetimeFunc(context.Background(), rec, spec)
	if err != nil {
		t.Fatalf("evalArrowDatetimeFunc(age 1-arg): %v", err)
	}
	defer arr.Release()

	if arr.Len() != n {
		t.Fatalf("age() array len = %d, want %d", arr.Len(), n)
	}
	sArr, ok := arr.(*array.String)
	if !ok {
		t.Fatalf("age() produced %T, want *array.String", arr)
	}
	txn := evalCtx.GetTxnTimestamp(time.Microsecond)
	want, err := tree.TimestampDifference(&evalCtx,
		&tree.DTimestampTZ{Time: txn.Time},
		&tree.DTimestampTZ{Time: arrowMicroToTime(arrow.Timestamp(tsUs), true, p)})
	if err != nil {
		t.Fatalf("reference TimestampDifference: %v", err)
	}
	wantStr := want.String()
	for i := 0; i < n; i++ {
		if sArr.Value(i) != wantStr {
			t.Fatalf("row %d: age = %q, want %q", i, sArr.Value(i), wantStr)
		}
	}
}
