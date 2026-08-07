// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// pi is a small helper to build *int64 rows where nil means a SQL NULL.
func pi(v int64) *int64 { return &v }

// newColexecAlloc builds a colexec allocator backed by a test memory monitor,
// exactly like colexec's own unit tests do. The returned closure must be called
// to release the account.
func newColexecAlloc(ctx context.Context) (*colexec.Allocator, func()) {
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, cluster.MakeTestingClusterSettings())
	memAcc := testMemMonitor.MakeBoundAccount()
	ca := colexec.NewAllocator(ctx, &memAcc)
	return ca, func() { memAcc.Close(ctx) }
}

// intRecord builds an Arrow Record with the given column names; each cell is a
// *int64 (nil => NULL). All columns are INT64.
func intRecord(alloc memory.Allocator, cols []string, rows [][]*int64) arrow.Record {
	nCols := len(cols)
	nRows := len(rows)
	fields := make([]arrow.Field, nCols)
	arrs := make([]arrow.Array, nCols)
	for c := 0; c < nCols; c++ {
		b := array.NewInt64Builder(alloc)
		for r := 0; r < nRows; r++ {
			v := rows[r][c]
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(*v)
			}
		}
		fields[c] = arrow.Field{Name: cols[c], Type: arrow.PrimitiveTypes.Int64, Nullable: true}
		arrs[c] = b.NewArray()
		b.Release()
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), arrs, int64(nRows))
}

// recordToIntRows renders an Arrow Record into a lexicographically-sorted slice
// of "|"-joined cell strings (NULL cells render as "<NULL>"). The sort makes the
// comparison an order-independent multiset equality, which is what we want
// because the two engines emit join results in different (hash-driven) orders.
func recordToIntRows(rec arrow.Record) []string {
	n := int(rec.NumRows())
	m := int(rec.NumCols())
	out := make([]string, n)
	for i := 0; i < n; i++ {
		cells := make([]string, m)
		for c := 0; c < m; c++ {
			col := rec.Column(c)
			if col.IsNull(i) {
				cells[c] = "<NULL>"
			} else {
				cells[c] = fmt.Sprintf("%d", col.(*array.Int64).Value(i))
			}
		}
		out[i] = strings.Join(cells, "|")
	}
	sort.Strings(out)
	return out
}

// concatIntRecords stitches several Records (same schema) into one.
func concatIntRecords(alloc memory.Allocator, recs []arrow.Record) arrow.Record {
	if len(recs) == 0 {
		return intRecord(alloc, []string{}, [][]*int64{})
	}
	nCols := int(recs[0].NumCols())
	total := 0
	for _, r := range recs {
		total += int(r.NumRows())
	}
	cols := make([]string, nCols)
	for c := 0; c < nCols; c++ {
		cols[c] = recs[0].Schema().Field(c).Name
	}
	rows := make([][]*int64, 0, total)
	for _, r := range recs {
		for i := 0; i < int(r.NumRows()); i++ {
			row := make([]*int64, nCols)
			for c := 0; c < nCols; c++ {
				col := r.Column(c)
				if col.IsNull(i) {
					row[c] = nil
				} else {
					v := col.(*array.Int64).Value(i)
					row[c] = &v
				}
			}
			rows = append(rows, row)
		}
	}
	return intRecord(alloc, cols, rows)
}

func recordTypes(r arrow.Record) []coltypes.T {
	ts := make([]coltypes.T, r.NumCols())
	for i := range ts {
		ts[i] = coltypes.Int64
	}
	return ts
}

func arrowJoinTypeName(jt sqlbase.JoinType) string {
	switch jt {
	case sqlbase.JoinType_INNER:
		return "inner"
	case sqlbase.JoinType_LEFT_OUTER:
		return "left"
	case sqlbase.JoinType_RIGHT_OUTER:
		return "right"
	case sqlbase.JoinType_FULL_OUTER:
		return "full"
	}
	return "inner"
}

// runArrowJoin runs our vectorized arrow hash join over the two input Records
// and returns the output as a sorted multiset of rows. The arrow join emits the
// whole result in a single Next call, so we only call it once. The caller must
// keep left/right alive (and not rely on them afterwards) because the operator
// may take ownership of the underlying arrays.
func runArrowJoin(
	ctx context.Context,
	alloc memory.Allocator,
	left, right arrow.Record,
	jt sqlbase.JoinType,
	leftEq, rightEq []uint32,
) ([]string, error) {
	spec := rowexec.ArrowJoinSpec{
		LeftKeys:  []string{left.Schema().Field(int(leftEq[0])).Name},
		RightKeys: []string{right.Schema().Field(int(rightEq[0])).Name},
		Type:      arrowJoinTypeName(jt),
	}
	join := rowexec.NewArrowJoin(alloc, rowexec.NewArrowRecordSource(alloc, left), rowexec.NewArrowRecordSource(alloc, right), spec)
	join.Init(ctx)
	rec, _, err := join.Next(ctx)
	if err != nil {
		return nil, err
	}
	if rec == nil || rec.NumRows() == 0 {
		return []string{}, nil
	}
	return recordToIntRows(rec), nil
}

// runColexecJoin runs colexec's native vectorized hash joiner over the same
// inputs and returns the output as a sorted multiset of rows. The colexec
// operator may emit several batches, so we drain it until it returns an empty
// batch. The caller must keep left/right alive for the duration of the call.
func runColexecJoin(
	ctx context.Context,
	alloc memory.Allocator,
	left, right arrow.Record,
	jt sqlbase.JoinType,
	leftEq, rightEq []uint32,
) ([]string, error) {
	ca, caClose := newColexecAlloc(ctx)
	defer caClose()
	leftBatch, err := colexec.RecordToBatch(left, ca)
	if err != nil {
		return nil, err
	}
	rightBatch, err := colexec.RecordToBatch(right, ca)
	if err != nil {
		return nil, err
	}
	leftSrc := &oneShotBatchSource{batch: leftBatch}
	leftSrc.Init()
	rightSrc := &oneShotBatchSource{batch: rightBatch}
	rightSrc.Init()
	hj, err := colexec.NewHashJoiner(ca, jt, leftEq, rightEq, recordTypes(left), recordTypes(right), leftSrc, rightSrc)
	if err != nil {
		return nil, err
	}
	hj.Init()
	var recs []arrow.Record
	for {
		b := hj.Next(ctx)
		if b.Length() == 0 {
			break
		}
		rec, err := colexec.BatchToRecord(b, memory.NewGoAllocator())
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recordToIntRows(concatIntRecords(alloc, recs)), nil
}

// TestArrowJoinMatchesColexec checks that our colexec-style arrow join
// (§7.11 hash-bucket eval) produces exactly the same join multiset as colexec's
// native vectorized hash joiner, across all four join types and across datasets
// that exercise many-to-many fan-out and NULL-key exclusion.
func TestArrowJoinMatchesColexec(t *testing.T) {
	ctx := context.Background()
	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(1024); err != nil {
		t.Fatal(err)
	}
	alloc := memory.NewGoAllocator()

	joinTypes := []sqlbase.JoinType{
		sqlbase.JoinType_INNER,
		sqlbase.JoinType_LEFT_OUTER,
		sqlbase.JoinType_RIGHT_OUTER,
		sqlbase.JoinType_FULL_OUTER,
	}

	datasets := []struct {
		name   string
		leftC  []string
		rightC []string
		left   [][]*int64
		right  [][]*int64
	}{
		{
			"many-to-many",
			[]string{"k", "lv"}, []string{"k", "rv"},
			[][]*int64{
				{pi(1), pi(10)}, {pi(2), pi(20)}, {pi(2), pi(21)}, {pi(3), pi(30)},
			},
			[][]*int64{
				{pi(1), pi(100)}, {pi(2), pi(200)}, {pi(2), pi(201)}, {pi(4), pi(400)},
			},
		},
		{
			"null-keys",
			[]string{"k", "lv"}, []string{"k", "rv"},
			[][]*int64{
				{pi(1), pi(10)}, {nil, pi(20)}, {pi(2), pi(30)},
			},
			[][]*int64{
				{pi(1), pi(100)}, {pi(2), pi(200)}, {nil, pi(300)},
			},
		},
	}

	for _, ds := range datasets {
		for _, jt := range joinTypes {
			// Each engine gets its own fresh copy of the input records, since
			// the arrow join may take ownership of (and release) its inputs.
			leftArrow := intRecord(alloc, ds.leftC, ds.left)
			rightArrow := intRecord(alloc, ds.rightC, ds.right)
			arrowRows, err := runArrowJoin(ctx, alloc, leftArrow, rightArrow, jt, []uint32{0}, []uint32{0})
			if err != nil {
				t.Fatalf("arrow join %s/%s: %v", ds.name, arrowJoinTypeName(jt), err)
			}
			leftColexec := intRecord(alloc, ds.leftC, ds.left)
			rightColexec := intRecord(alloc, ds.rightC, ds.right)
			colecRows, err := runColexecJoin(ctx, alloc, leftColexec, rightColexec, jt, []uint32{0}, []uint32{0})
			if err != nil {
				t.Fatalf("colexec join %s/%s: %v", ds.name, arrowJoinTypeName(jt), err)
			}
			if !reflect.DeepEqual(arrowRows, colecRows) {
				t.Errorf("join mismatch for type=%s dataset=%s\narrow  = %v\ncolexec= %v",
					arrowJoinTypeName(jt), ds.name, arrowRows, colecRows)
			}
		}
	}
}

// BenchmarkArrowVsColexecJoin measures our arrow join against colexec's native
// hash joiner on a medium dataset with heavy many-to-many fan-out. The input
// records are Retained/Released per iteration so the operator's ownership
// transfers do not free the shared inputs prematurely.
func BenchmarkArrowVsColexecJoin(b *testing.B) {
	ctx := context.Background()
	defer coldata.ResetBatchSizeForTests()
	if err := coldata.SetBatchSizeForTests(1024); err != nil {
		b.Fatal(err)
	}
	alloc := memory.NewGoAllocator()

	const n = 1024
	leftRows := make([][]*int64, n)
	rightRows := make([][]*int64, n)
	for i := 0; i < n; i++ {
		k := int64(i % 64) // 64 distinct keys => ~16x fan-out
		leftRows[i] = []*int64{pi(k), pi(int64(i))}
		rightRows[i] = []*int64{pi(k), pi(int64(i * 10))}
	}
	jt := sqlbase.JoinType_INNER
	leftEq, rightEq := []uint32{0}, []uint32{0}

	b.Run("Arrow", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Fresh copies per iteration: the arrow join takes ownership of
			// (and releases) its inputs, so sharing would corrupt them.
			left := intRecord(alloc, []string{"k", "lv"}, leftRows)
			right := intRecord(alloc, []string{"k", "rv"}, rightRows)
			rows, err := runArrowJoin(ctx, alloc, left, right, jt, leftEq, rightEq)
			if err != nil {
				b.Fatal(err)
			}
			_ = rows
		}
	})
	b.Run("Colexec", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			left := intRecord(alloc, []string{"k", "lv"}, leftRows)
			right := intRecord(alloc, []string{"k", "rv"}, rightRows)
			rows, err := runColexecJoin(ctx, alloc, left, right, jt, leftEq, rightEq)
			if err != nil {
				b.Fatal(err)
			}
			_ = rows
		}
	})
}
