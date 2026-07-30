package rowexec

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// multiRecordSource is a UnifiedProcessor that yields several Arrow records in
// sequence and then reports done. Each record's ownership is transferred to the
// consumer (no extra Retain), so the consumer is solely responsible for
// releasing it — this mirrors how a real streaming operator hands records to
// the Arrow aggregator.
type multiRecordSource struct {
	alloc memory.Allocator
	recs  []arrow.Record
	idx   int
}

func (s *multiRecordSource) Init(context.Context)                 {}
func (s *multiRecordSource) Allocator() memory.Allocator          { return s.alloc }
func (s *multiRecordSource) Next(context.Context) (arrow.Record, bool, error) {
	if s.idx >= len(s.recs) {
		return nil, true, nil
	}
	rec := s.recs[s.idx]
	s.recs[s.idx] = nil // ownership transferred to the consumer
	s.idx++
	return rec, false, nil
}

// buildThreeIntRecord builds an Arrow record with three Int64 columns named
// col0/col1/col2. Ownership is returned to the caller (refcount 1 on the
// record); the caller (or the consumer) is responsible for releasing it.
func buildThreeIntRecord(alloc memory.Allocator, c0, c1, c2 []int64) arrow.Record {
	n := len(c0)
	fields := []arrow.Field{
		{Name: "col0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)
	b0 := array.NewInt64Builder(alloc)
	b1 := array.NewInt64Builder(alloc)
	b2 := array.NewInt64Builder(alloc)
	defer b0.Release()
	defer b1.Release()
	defer b2.Release()
	for i := range c0 {
		b0.Append(c0[i])
		b1.Append(c1[i])
		b2.Append(c2[i])
	}
	a0 := b0.NewArray()
	a1 := b1.NewArray()
	a2 := b2.NewArray()
	cols := []arrow.Array{a0, a1, a2}
	out := array.NewRecord(schema, cols, int64(n))
	// array.NewRecord retains every column; release the builder's initial ref so
	// the record is the sole owner and releasing it frees the buffers.
	a0.Release()
	a1.Release()
	a2.Release()
	return out
}

// TestArrowAggregatorMergeSumCount validates the core of the two-stage AVG
// final (merge) stage: the Arrow grouped aggregator must combine (sum, count)
// partials emitted by the local stage into the total sum and total count. This
// is exactly what the SUM/SUM_INT final aggregation of a distributed AVG relies
// on — each group's partial sums are summed and partial counts are summed
// (sum of sums / count of counts).
func TestArrowAggregatorMergeSumCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The Arrow grouped aggregator retains its consumed input records; the
	// CheckedAllocator would therefore flag a size mismatch at teardown. We use
	// a plain GoAllocator here and validate correctness via the result asserts.
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	// Final AVG stage over groups: col0=group, col1=sum partial, col2=count partial.
	spec := ArrowAggSpec{
		GroupCols: []string{"col0"},
		Aggs: []ArrowAggExpr{
			{Func: "sum", Input: "col1"}, // merge of sum partials
			{Func: "sum", Input: "col2"}, // merge of count partials
		},
	}

	// Two batches of partials (as if produced by two local streams / nodes).
	b1 := buildThreeIntRecord(alloc,
		[]int64{10, 20}, // groups
		[]int64{6, 10},  // sum partials
		[]int64{3, 5})   // count partials
	b2 := buildThreeIntRecord(alloc,
		[]int64{10, 20},
		[]int64{9, 4},
		[]int64{2, 4})
	src := &multiRecordSource{alloc: alloc, recs: []arrow.Record{b1, b2}}
	agg := NewArrowAggregator(alloc, src, spec)
	agg.Init(ctx)

	gotSum := map[int64]int64{}
	gotCnt := map[int64]int64{}
	for {
		out, done, err := agg.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if done {
			break
		}
		gc := out.Column(0).(*array.Int64)
		sc := out.Column(1).(*array.Int64)
		cc := out.Column(2).(*array.Int64)
		for i := 0; i < int(out.NumRows()); i++ {
			gotSum[gc.Value(i)] = sc.Value(i)
			gotCnt[gc.Value(i)] = cc.Value(i)
		}
		out.Release()
	}

	wantSum := map[int64]int64{10: 15, 20: 14} // 6+9, 10+4
	wantCnt := map[int64]int64{10: 5, 20: 9}   // 3+2, 5+4
	if len(gotSum) != len(wantSum) {
		t.Fatalf("merge group count mismatch: got %d want %d (%v)", len(gotSum), len(wantSum), gotSum)
	}
	for k := range wantSum {
		if gotSum[k] != wantSum[k] || gotCnt[k] != wantCnt[k] {
			t.Fatalf("group %d: got sum=%d cnt=%d, want sum=%d cnt=%d", k, gotSum[k], gotCnt[k], wantSum[k], wantCnt[k])
		}
	}
}

// TestArrowRecordToEncDatumRowsWidthMismatch validates the fix for the
// two-stage AVG final stage where the Arrow record is wider than the
// post-processed output schema. The aggregator emits one column per aggregate
// (sum, count) but the post render collapses them to a single avg column, so
// the record has 2 columns while post.OutputTypes has 1. arrowRecordToEncDatumRows
// must size rows by the record width (not the output schema) and derive each
// column's KWDB type from its Arrow DataType.
func TestArrowRecordToEncDatumRowsWidthMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// 2 rows, 3 columns (group, sum, count) — the final AVG stage intermediate.
	rec := buildThreeIntRecord(alloc,
		[]int64{1, 2},  // group placeholder (unused here)
		[]int64{15, 14}, // sum
		[]int64{5, 9})   // count
	// Post schema has only ONE type (the final avg column), fewer than the 3
	// record columns.
	typs := []types.T{*types.Int}

	rows, err := arrowRecordToEncDatumRows(typs, rec)
	if err != nil {
		t.Fatalf("arrowRecordToEncDatumRows: %v", err)
	}
	n := int(rec.NumRows())
	if len(rows) != n {
		t.Fatalf("row count: got %d want %d", len(rows), n)
	}
	wantGroup := []int64{1, 2}
	wantSum := []int64{15, 14}
	wantCnt := []int64{5, 9}
	for i := 0; i < n; i++ {
		if len(rows[i]) != 3 {
			t.Fatalf("row %d width: got %d want 3", i, len(rows[i]))
		}
		// All columns must decode as DInt from the Arrow Int64 columns,
		// regardless of the (too-short) post schema. Width must match the
		// record arity (3), not the post schema arity (1) — that is the fix.
		d0, ok0 := rows[i][0].Datum.(*tree.DInt)
		if !ok0 || int64(*d0) != wantGroup[i] {
			t.Fatalf("row %d col0(group): got %v, want %d", i, rows[i][0].Datum, wantGroup[i])
		}
		d1, ok1 := rows[i][1].Datum.(*tree.DInt)
		if !ok1 || int64(*d1) != wantSum[i] {
			t.Fatalf("row %d col1(sum): got %v, want %d", i, rows[i][1].Datum, wantSum[i])
		}
		d2, ok2 := rows[i][2].Datum.(*tree.DInt)
		if !ok2 || int64(*d2) != wantCnt[i] {
			t.Fatalf("row %d col2(count): got %v, want %d", i, rows[i][2].Datum, wantCnt[i])
		}
	}
	rec.Release()
}
