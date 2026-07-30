package rowexec_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
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

func (s *multiRecordSource) Init(context.Context)                  {}
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

// splitIntoRecords splits (groups,vals,nulls) into nBatches Arrow records.
func splitIntoRecords(alloc memory.Allocator, groups, vals []int64, nulls []bool, nBatches int) []arrow.Record {
	if nBatches < 1 {
		nBatches = 1
	}
	n := len(groups)
	size := (n + nBatches - 1) / nBatches
	var recs []arrow.Record
	for start := 0; start < n; start += size {
		end := start + size
		if end > n {
			end = n
		}
		recs = append(recs, buildIntRecord(alloc, groups[start:end], vals[start:end], nulls[start:end]))
	}
	return recs
}

// TestArrowGroupedStreamingMultiBatch verifies that the Arrow grouped
// aggregator produces correct results when its input arrives as several
// streaming batches. This exercises the streaming-accumulator path: group keys
// are captured per group at discovery time (not re-materialized from a single
// "current" batch), so a group first seen in an early batch keeps its key even
// after that batch has been released. The CheckedAllocator guards the
// refcount semantics — every input record and every intermediate key record
// must be released exactly once.
func TestArrowGroupedStreamingMultiBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		name     string
		n        int
		nGroups  int
		nullFrac float64
		nBatches int
	}{
		{"single-batch", 1000, 10, 0, 1},
		{"multi-batch", 10000, 7, 0.1, 4},
		{"many-groups", 8000, 500, 0, 7},
		{"sparse-nulls", 11000, 13, 0.2, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer alloc.AssertSize(t, 0)

			ctx := context.Background()
			rng := rand.New(rand.NewSource(42))
			groups, vals, nulls := genIntData(c.n, c.nGroups, c.nullFrac, rng)

			// Ground truth: per-group sum of non-null values.
			truth := map[int64]int64{}
			for i := range groups {
				if nulls[i] {
					continue
				}
				truth[groups[i]] += vals[i]
			}

			recs := splitIntoRecords(alloc, groups, vals, nulls, c.nBatches)
			src := &multiRecordSource{alloc: alloc, recs: recs}
			spec := rowexec.ArrowAggSpec{
				GroupCols: []string{"col0"},
				Aggs:      []rowexec.ArrowAggExpr{{Func: "sum", Input: "col1"}},
			}
			agg := rowexec.NewArrowAggregator(alloc, src, spec)
			agg.Init(ctx)

			got := map[int64]int64{}
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
				for i := 0; i < int(out.NumRows()); i++ {
					got[gc.Value(i)] = sc.Value(i)
				}
				out.Release()
			}

			if len(got) != len(truth) {
				t.Fatalf("group count mismatch: got %d want %d", len(got), len(truth))
			}
			for k, v := range truth {
				if got[k] != v {
					t.Fatalf("group %d: sum mismatch: got %d want %d", k, got[k], v)
				}
			}
		})
	}
}
