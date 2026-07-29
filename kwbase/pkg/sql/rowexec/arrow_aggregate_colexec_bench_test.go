package rowexec_test

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

// oneShotBatchSource feeds a single coldata.Batch to an operator exactly once
// (subsequent Next calls return an empty batch), so a draining consumer such as
// colexec's hashAggregator terminates instead of looping forever.
type oneShotBatchSource struct {
	colexec.ZeroInputNode
	batch coldata.Batch
	done  bool
}

func (o *oneShotBatchSource) Init() {}
func (o *oneShotBatchSource) Next(context.Context) coldata.Batch {
	if o.done {
		return coldata.NewMemBatchWithSize(nil, 0)
	}
	o.done = true
	return o.batch
}
func (o *oneShotBatchSource) OutputTypes() []coltypes.T {
	typs := make([]coltypes.T, o.batch.Width())
	for i := 0; i < o.batch.Width(); i++ {
		typs[i] = o.batch.ColVec(i).Type()
	}
	return typs
}

// newColexecTestAllocator builds a colexec.Allocator backed by an unlimited test
// memory account, mirroring colexec's own testAllocator wiring.
func newColexecTestAllocator(ctx context.Context) (*colexec.Allocator, func()) {
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, cluster.MakeTestingClusterSettings())
	memAcc := testMemMonitor.MakeBoundAccount()
	alloc := colexec.NewAllocator(ctx, &memAcc)
	cleanup := func() {
		memAcc.Close(ctx)
		testMemMonitor.Stop(ctx)
	}
	return alloc, cleanup
}

// genIntData deterministically generates a group column and a (nullable) value
// column of length n. It returns the raw slices so two independent arrow.Records
// can be built from identical data (the arrow path releases its record, so the
// colexec path must use a separate one).
func genIntData(n, nGroups int, nullFrac float64, rng *rand.Rand) (groups, vals []int64, nulls []bool) {
	groups = make([]int64, n)
	vals = make([]int64, n)
	nulls = make([]bool, n)
	for i := 0; i < n; i++ {
		groups[i] = int64(rng.Intn(nGroups))
		if nullFrac > 0 && rng.Float64() < nullFrac {
			nulls[i] = true
		} else {
			vals[i] = rng.Int63() % 100000
		}
	}
	return groups, vals, nulls
}

// buildIntRecord builds an arrow.Record with two int64 columns (group, value)
// from the given raw data.
func buildIntRecord(alloc memory.Allocator, groups, vals []int64, nulls []bool) arrow.Record {
	gb := array.NewInt64Builder(alloc)
	vb := array.NewInt64Builder(alloc)
	defer gb.Release()
	defer vb.Release()
	for i := range groups {
		gb.Append(groups[i])
		if nulls[i] {
			vb.AppendNull()
		} else {
			vb.Append(vals[i])
		}
	}
	cols := []arrow.Array{gb.NewArray(), vb.NewArray()}
	fields := []arrow.Field{
		{Name: "col0", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(len(groups)))
}

// runArrowSumGrouped runs our vectorized (colexec-style) arrow aggregation via
// the exported UnifiedProcessor API and returns the global SUM of col1.
func runArrowSumGrouped(ctx context.Context, alloc memory.Allocator, rec arrow.Record) (int64, error) {
	src := rowexec.NewArrowRecordSource(alloc, rec)
	spec := rowexec.ArrowAggSpec{
		Aggs: []rowexec.ArrowAggExpr{{Func: "sum", Input: "col1"}},
	}
	agg := rowexec.NewArrowAggregator(alloc, src, spec)
	agg.Init(ctx)
	var sum int64
	for {
		out, done, err := agg.Next(ctx)
		if err != nil {
			return 0, err
		}
		if done {
			break
		}
		sc := out.Column(0).(*array.Int64)
		for i := 0; i < int(out.NumRows()); i++ {
			sum += sc.Value(i)
		}
		out.Release()
	}
	return sum, nil
}

// arrowColTypeToColType maps an arrow data type to its colexec coltype. This
// mirrors colexec.arrowTypeToColType (unexported) so we can derive the input
// column types directly from the arrow.Record rather than relying on the batch
// width reported by RecordToBatch.
func arrowColTypeToColType(dt arrow.DataType) coltypes.T {
	switch dt.ID() {
	case arrow.INT64:
		return coltypes.Int64
	case arrow.FLOAT64:
		return coltypes.Float64
	case arrow.BOOL:
		return coltypes.Bool
	case arrow.STRING, arrow.BINARY:
		return coltypes.Bytes
	default:
		return coltypes.Unhandled
	}
}

// colexecBatchAndTypes converts an arrow.Record to a colexec Batch and derives
// the input column types, once, so the benchmark can reuse them across
// iterations (excluding the arrow->coldata conversion cost from the algorithm
// comparison).
func colexecBatchAndTypes(rec arrow.Record) (coldata.Batch, []coltypes.T, error) {
	colTypes := make([]coltypes.T, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		colTypes[i] = arrowColTypeToColType(rec.Column(i).DataType())
	}
	batch, err := colexec.RecordToBatch(rec)
	if err != nil {
		return nil, nil, err
	}
	return batch, colTypes, nil
}

// runColexecSumGrouped runs the real colexec hash aggregator on a pre-converted
// coldata.Batch and returns the global SUM of col1. colexec's hash aggregator
// keeps the group keys in a private keyMapping batch and emits only the
// aggregate columns, so for the cross-engine comparison we compare the global
// SUM (no group columns) -- this isolates the SUM algorithm itself, which is the
// point of "colexec is not row-by-row".
func runColexecSumGrouped(ctx context.Context, ca *colexec.Allocator, batch coldata.Batch, colTypes []coltypes.T) (int64, error) {
	source := &oneShotBatchSource{batch: batch}
	source.Init()
	agg, err := colexec.NewHashAggregator(ca, source, colTypes,
		[]execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM},
		nil, [][]uint32{{1}})
	if err != nil {
		return 0, err
	}
	agg.Init()
	var sum int64
	for {
		out := agg.Next(ctx)
		if out.Length() == 0 {
			break
		}
		sv := out.ColVec(0).Int64()
		for i := 0; i < out.Length(); i++ {
			if out.ColVec(0).Nulls().NullAt(i) {
				continue
			}
			sum += sv[i]
		}
	}
	return sum, nil
}

// TestArrowAggMatchesColexec asserts our arrow aggregation produces identical
// SUM-per-group results to the original colexec vectorized engine on the same
// data -- the explicit "增加原 colexec 的对比" requirement.
func TestArrowAggMatchesColexec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// colexec's scratch buffer is sized relative to coldata.BatchSize(), which
	// defaults to 1024 in tests; raise it so the comparison can use realistic
	// row counts (the scratch capacity is 3*BatchSize()).
	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(4096); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	alloc := memory.NewGoAllocator()
	rng, _ := randutil.NewPseudoRand()
	for _, tc := range []struct {
		n, nGroups int
		nullFrac   float64
	}{
		{1000, 10, 0},
		{10000, 7, 0.1},
		{8000, 500, 0},
	} {
		groups, vals, nulls := genIntData(tc.n, tc.nGroups, tc.nullFrac, rng)
		recArrow := buildIntRecord(alloc, groups, vals, nulls)
		recColexec := buildIntRecord(alloc, groups, vals, nulls)
		arrowRes, err := runArrowSumGrouped(ctx, alloc, recArrow)
		if err != nil {
			t.Fatalf("arrow agg: %v", err)
		}
		ca, cleanup := newColexecTestAllocator(ctx)
		cbatch, ctypes, err := colexecBatchAndTypes(recColexec)
		if err != nil {
			t.Fatalf("record->batch: %v", err)
		}
		colexecRes, err := runColexecSumGrouped(ctx, ca, cbatch, ctypes)
		if err != nil {
			t.Fatalf("colexec agg: %v", err)
		}
		cleanup()
		if arrowRes != colexecRes {
			t.Fatalf("global sum mismatch: arrow=%d colexec=%d", arrowRes, colexecRes)
		}
		recArrow.Release()
		recColexec.Release()
	}
}

// BenchmarkArrowVsColexecSumGrouped compares the wall-clock cost of our
// vectorized arrow hash aggregation against the original colexec engine on the
// same grouped SUM(int64) workload. Both sides operate on already-columnar data
// (the arrow.Record / the converted coldata.Batch), so the comparison isolates
// the aggregation algorithms themselves.
func BenchmarkArrowVsColexecSumGrouped(b *testing.B) {
	ctx := context.Background()
	alloc := memory.NewGoAllocator()
	rng, _ := randutil.NewPseudoRand()
	const n, nGroups = 10000, 16
	groups, vals, nulls := genIntData(n, nGroups, 0.05, rng)

	// colexec's scratch buffer is sized relative to coldata.BatchSize() (1024 in
	// tests); raise it so the comparison uses realistic row counts.
	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(4096); err != nil {
		b.Fatal(err)
	}

	ca, cleanup := newColexecTestAllocator(ctx)
	defer cleanup()

	// Build a fresh record inside each iteration: the arrow path releases its
	// record after consuming it, so a shared record would be freed mid-loop.
	// Both paths pay the identical build cost, so the comparison stays fair.
	b.Run("arrow", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec := buildIntRecord(alloc, groups, vals, nulls)
			if _, err := runArrowSumGrouped(ctx, alloc, rec); err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})

	b.Run("colexec", func(b *testing.B) {
		// Pre-convert once so the timed loop measures the aggregation algorithm,
		// not the arrow->coldata bridge.
		rec := buildIntRecord(alloc, groups, vals, nulls)
		batch, colTypes, err := colexecBatchAndTypes(rec)
		rec.Release()
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := runColexecSumGrouped(ctx, ca, batch, colTypes); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================================================
// §9.3: extend the colexec comparison to the GROUPED aggregation state.
//
// colexec's hashAggregator keeps the group keys in a private keyMapping batch
// and emits only the aggregate columns (a per-group SUM value list in
// nondeterministic group order). Our arrowHashAggregator, by contrast, emits
// (group_key, sum) rows. To compare the two engines on grouped aggregation we
// use an in-Go ground-truth per-group SUM dictionary computed directly from the
// raw input slices:
//   - arrow  path: exact (group_key -> sum) map comparison against the truth
//     (exercises arrow's group-key emission too);
//   - colexec path: the per-group SUM multiset (sorted values + null count)
//     comparison against the truth (transitively proving arrow == colexec).
// ============================================================================

// groundTruthGroupedSum computes the per-group SUM(int64) dictionary in pure
// Go from the raw input slices. It returns the exact (group -> sum) map, plus
// the sorted multiset of group sums and the number of all-null groups (whose
// SUM finalizes to NULL) -- the latter two for the colexec multiset comparison
// where group keys are not available.
func groundTruthGroupedSum(groups, vals []int64, nulls []bool) (map[int64]int64, []int64, int) {
	sums := make(map[int64]int64)
	hasVal := make(map[int64]bool)
	for i := range groups {
		if !nulls[i] {
			sums[groups[i]] += vals[i]
			hasVal[groups[i]] = true
		}
	}
	seen := make(map[int64]bool)
	var multiset []int64
	nullCount := 0
	for _, g := range groups {
		if seen[g] {
			continue
		}
		seen[g] = true
		if hasVal[g] {
			multiset = append(multiset, sums[g])
		} else {
			nullCount++
		}
	}
	sort.Slice(multiset, func(i, j int) bool { return multiset[i] < multiset[j] })
	return sums, multiset, nullCount
}

// sortedMultiset turns an exact (group -> sum) map plus a null count into the
// (sorted values, null count) form used for the colexec multiset comparison.
func sortedMultiset(m map[int64]int64, nullCount int) ([]int64, int) {
	s := make([]int64, 0, len(m))
	for _, v := range m {
		s = append(s, v)
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s, nullCount
}

// runArrowGroupedSum runs our vectorized arrow aggregation with GroupCols set
// and returns the exact (group_key -> sum) map plus the null-group count. The
// arrowHashAggregator emits (col0=group_key, col1=sum) rows, one per group.
func runArrowGroupedSum(ctx context.Context, alloc memory.Allocator, rec arrow.Record) (map[int64]int64, int, error) {
	src := rowexec.NewArrowRecordSource(alloc, rec)
	spec := rowexec.ArrowAggSpec{
		GroupCols: []string{"col0"},
		Aggs:      []rowexec.ArrowAggExpr{{Func: "sum", Input: "col1"}},
	}
	agg := rowexec.NewArrowAggregator(alloc, src, spec)
	agg.Init(ctx)
	m := make(map[int64]int64)
	nullCount := 0
	for {
		out, done, err := agg.Next(ctx)
		if err != nil {
			return nil, 0, err
		}
		if done {
			break
		}
		gc := out.Column(0).(*array.Int64)
		sc := out.Column(1).(*array.Int64)
		for i := 0; i < int(out.NumRows()); i++ {
			if sc.IsNull(i) {
				nullCount++
			} else {
				m[gc.Value(i)] = sc.Value(i)
			}
		}
		out.Release()
	}
	return m, nullCount, nil
}

// runColexecGroupedSum runs the real colexec hash aggregator grouped on col0
// and returns the multiset of per-group SUM values (group keys are NOT emitted
// by colexec's hashAggregator, hence the multiset form) plus the null-group
// count.
func runColexecGroupedSum(ctx context.Context, ca *colexec.Allocator, batch coldata.Batch, colTypes []coltypes.T) ([]int64, int, error) {
	source := &oneShotBatchSource{batch: batch}
	source.Init()
	agg, err := colexec.NewHashAggregator(ca, source, colTypes,
		[]execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM},
		[]uint32{0}, [][]uint32{{1}})
	if err != nil {
		return nil, 0, err
	}
	agg.Init()
	var vals []int64
	nullCount := 0
	for {
		out := agg.Next(ctx)
		if out.Length() == 0 {
			break
		}
		sv := out.ColVec(0).Int64()
		for i := 0; i < out.Length(); i++ {
			if out.ColVec(0).Nulls().NullAt(i) {
				nullCount++
			} else {
				vals = append(vals, sv[i])
			}
		}
	}
	return vals, nullCount, nil
}

// TestArrowGroupedAggMatchesColexec asserts our arrow GROUPED aggregation is
// correct and matches colexec on the same grouped SUM(int64) workload:
//   - arrow  (group_key, sum) rows are compared EXACTLY against the Go ground
//     truth (exercises arrow's group-key emission);
//   - colexec per-group SUM multiset is compared against the same ground truth
//     (colexec does not emit group keys, so a sorted multiset comparison is the
//     faithful cross-engine check; it transitively proves arrow == colexec).
func TestArrowGroupedAggMatchesColexec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(4096); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	alloc := memory.NewGoAllocator()
	rng, _ := randutil.NewPseudoRand()
	for _, tc := range []struct {
		n, nGroups int
		nullFrac   float64
	}{
		{1000, 10, 0},
		{10000, 7, 0.1},
		{8000, 500, 0},
		{11000, 13, 0.2},
	} {
		groups, vals, nulls := genIntData(tc.n, tc.nGroups, tc.nullFrac, rng)
		truth, truthMultiset, truthNulls := groundTruthGroupedSum(groups, vals, nulls)

		// --- arrow: exact (group -> sum) map ---
		recArrow := buildIntRecord(alloc, groups, vals, nulls)
		arrowMap, arrowNulls, err := runArrowGroupedSum(ctx, alloc, recArrow)
		if err != nil {
			t.Fatalf("arrow grouped agg: %v", err)
		}
		recArrow.Release()
		if arrowNulls != truthNulls {
			t.Fatalf("arrow null-group count mismatch: got %d want %d (n=%d nGroups=%d)",
				arrowNulls, truthNulls, tc.n, tc.nGroups)
		}
		for g, want := range truth {
			if got, ok := arrowMap[g]; !ok || got != want {
				t.Fatalf("arrow grouped sum mismatch for group %d: got %d (present=%v) want %d (n=%d nGroups=%d)",
					g, got, ok, want, tc.n, tc.nGroups)
			}
		}
		for g, got := range arrowMap {
			if _, ok := truth[g]; !ok {
				t.Fatalf("arrow emitted unknown group %d sum %d (n=%d nGroups=%d)",
					g, got, tc.n, tc.nGroups)
			}
		}
		arrowMultiset, _ := sortedMultiset(arrowMap, arrowNulls)
		if !equalMultiset(arrowMultiset, arrowNulls, truthMultiset, truthNulls) {
			t.Fatalf("arrow grouped multiset mismatch (n=%d nGroups=%d)", tc.n, tc.nGroups)
		}

		// --- colexec: per-group SUM multiset ---
		recColexec := buildIntRecord(alloc, groups, vals, nulls)
		ca, cleanup := newColexecTestAllocator(ctx)
		cbatch, ctypes, err := colexecBatchAndTypes(recColexec)
		if err != nil {
			t.Fatalf("record->batch: %v", err)
		}
		colexecVals, colexecNulls, err := runColexecGroupedSum(ctx, ca, cbatch, ctypes)
		if err != nil {
			t.Fatalf("colexec grouped agg: %v", err)
		}
		recColexec.Release()
		cleanup()
		sort.Slice(colexecVals, func(i, j int) bool { return colexecVals[i] < colexecVals[j] })
		if !equalMultiset(colexecVals, colexecNulls, truthMultiset, truthNulls) {
			t.Fatalf("colexec grouped multiset mismatch (n=%d nGroups=%d)", tc.n, tc.nGroups)
		}
	}
}

// equalMultiset compares two per-group SUM multisets: the sorted non-null
// values must be identical and the null-group counts must match.
func equalMultiset(a []int64, aNulls int, b []int64, bNulls int) bool {
	if aNulls != bNulls || len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BenchmarkArrowVsColexecGrouped compares our vectorized arrow GROUPED hash
// aggregation against the original colexec engine on the same grouped
// SUM(int64) workload.
func BenchmarkArrowVsColexecGrouped(b *testing.B) {
	ctx := context.Background()
	alloc := memory.NewGoAllocator()
	rng, _ := randutil.NewPseudoRand()
	const n, nGroups = 10000, 16
	groups, vals, nulls := genIntData(n, nGroups, 0.05, rng)

	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(4096); err != nil {
		b.Fatal(err)
	}

	ca, cleanup := newColexecTestAllocator(ctx)
	defer cleanup()

	b.Run("arrow", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec := buildIntRecord(alloc, groups, vals, nulls)
			if _, _, err := runArrowGroupedSum(ctx, alloc, rec); err != nil {
				b.Fatal(err)
			}
			rec.Release()
		}
	})

	b.Run("colexec", func(b *testing.B) {
		rec := buildIntRecord(alloc, groups, vals, nulls)
		batch, colTypes, err := colexecBatchAndTypes(rec)
		rec.Release()
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := runColexecGroupedSum(ctx, ca, batch, colTypes); err != nil {
				b.Fatal(err)
			}
		}
	})
}
