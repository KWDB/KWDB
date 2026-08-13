// Copyright 2024 The KWDB Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowexec

import (
	"bytes"
	"context"
	"fmt"
	"hash/maphash"
	"math"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ArrowJoinSpec is the unified (compute) description of a hash join. LeftKeys
// and RightKeys are the equality key column names on each side; Type is one of
// "inner", "left", "right" or "full". The output is the concatenation of the
// left columns followed by the right columns.
type ArrowJoinSpec struct {
	LeftKeys  []string
	RightKeys []string
	Type      string
}

type arrowJoinCore struct {
	spec  ArrowJoinSpec
	alloc memory.Allocator
}

func newArrowJoinCore(spec ArrowJoinSpec, alloc memory.Allocator) arrowJoinCore {
	return arrowJoinCore{spec: spec, alloc: alloc}
}

// arrowJoin is a UnifiedProcessor that performs an (inner/left/right/full) hash
// join over Arrow records. Both inputs are buffered, then a hash table over the
// right equality keys is probed with the left keys.
type arrowJoin struct {
	arrowJoinCore
	left  UnifiedProcessor
	right UnifiedProcessor
}

// NewArrowJoin builds a join operator over the left and right inputs.
func NewArrowJoin(alloc memory.Allocator, left UnifiedProcessor, right UnifiedProcessor, spec ArrowJoinSpec) UnifiedProcessor {
	return &arrowJoin{arrowJoinCore: newArrowJoinCore(spec, alloc), left: left, right: right}
}

// Allocator implements UnifiedProcessor.
func (j *arrowJoinCore) Allocator() memory.Allocator { return j.alloc }

// Init implements UnifiedProcessor.
func (j *arrowJoin) Init(ctx context.Context) {
	j.left.Init(ctx)
	j.right.Init(ctx)
}

// Next implements UnifiedProcessor. It buffers both inputs, computes the join,
// emits a single output Record, then reports done.
func (j *arrowJoin) Next(ctx context.Context) (arrow.Record, bool, error) {
	leftRec, err := readAll(j.alloc, j.left)
	if err != nil {
		return nil, false, err
	}
	rightRec, err := readAll(j.alloc, j.right)
	if err != nil {
		if leftRec != nil {
			leftRec.Release()
		}
		return nil, false, err
	}
	out, err := j.eval(ctx, leftRec, rightRec)
	if leftRec != nil {
		leftRec.Release()
	}
	if rightRec != nil {
		rightRec.Release()
	}
	if err != nil {
		return nil, false, err
	}
	return out, false, nil
}

// readAll drains a UnifiedProcessor into a single concatenated Record (or nil
// if it produced nothing).
func readAll(alloc memory.Allocator, src UnifiedProcessor) (arrow.Record, error) {
	var recs []arrow.Record
	for {
		rec, done, err := src.Next(context.Background())
		if err != nil {
			for _, r := range recs {
				r.Release()
			}
			return nil, err
		}
		if done {
			break
		}
		recs = append(recs, rec)
	}
	if len(recs) == 0 {
		return nil, nil
	}
	out := concatRecords(alloc, recs)
	for _, r := range recs {
		r.Release()
	}
	return out, nil
}

func concatRecords(alloc memory.Allocator, recs []arrow.Record) arrow.Record {
	var nonEmpty []arrow.Record
	for _, rec := range recs {
		if rec.NumRows() > 0 {
			nonEmpty = append(nonEmpty, rec)
		}
	}
	if len(nonEmpty) == 0 {
		return recs[0]
	}
	ncols := int(nonEmpty[0].NumCols())
	cols := make([]arrow.Array, ncols)
	for ci := 0; ci < ncols; ci++ {
		arrs := make([]arrow.Array, len(nonEmpty))
		for r := range nonEmpty {
			arrs[r] = nonEmpty[r].Column(ci)
		}
		c, err := array.Concatenate(arrs, alloc)
		if err != nil {
			for k := 0; k < ci; k++ {
				cols[k].Release()
			}
			panic(fmt.Sprintf("arrow join concat: %v", err))
		}
		cols[ci] = c
	}
	var total int64
	for _, rec := range nonEmpty {
		total += rec.NumRows()
	}
	return array.NewRecord(nonEmpty[0].Schema(), cols, total)
}

// eval computes the join between leftRec and rightRec. It supports inner, left
// outer, right outer and full outer equi-joins. Rows with a NULL equality key
// never match (SQL semantics), so they are treated as unmatched.
func (j *arrowJoinCore) eval(ctx context.Context, leftRec, rightRec arrow.Record) (arrow.Record, error) {
	nL := int(leftRec.NumRows())
	nR := int(rightRec.NumRows())

	// Build the right-side hash buckets. Colexec-style: bucket by a uint64 hash
	// of the key columns (no per-row string allocation, unlike the former
	// recordKey string builder); collisions are resolved by value equality in
	// the probe. NULL keys are excluded (NULL != NULL).
	//
	// The key arrays are only resolved for a side that actually has rows. Some
	// upstreams (e.g. an arrow filter that excludes every row) emit a degenerate
	// record with 0 rows AND 0 columns; resolving its key columns via
	// arrowOperandColumn would panic. When a side is empty there can be no
	// matched pair anyway, so skipping its key arrays is correct (outer-join
	// unmatched rows are emitted from the non-empty side's columns in Phases
	// 2/3 below).
	var leftArrs, rightArrs []arrow.Array
	if nL > 0 {
		leftArrs = joinKeyArrays(leftRec, j.spec.LeftKeys)
	}
	if nR > 0 {
		rightArrs = joinKeyArrays(rightRec, j.spec.RightKeys)
	}
	var seed maphash.Hash
	seed.SetSeed(maphash.MakeSeed())
	rightBuckets := make(map[uint64][]int32)
	for r := 0; r < nR; r++ {
		if joinHasNull(rightArrs, r) {
			continue
		}
		h := joinRowHash(rightArrs, r, &seed)
		rightBuckets[h] = append(rightBuckets[h], int32(r))
	}

	leftOutIdx := make([]int32, 0, nL)
	rightOutIdx := make([]int32, 0, nL)
	leftMatched := make([]bool, nL)
	rightMatched := make([]bool, nR)
	// Phase 1: matched pairs (left-major, ascending right index, deterministic).
	for l := 0; l < nL; l++ {
		if joinHasNull(leftArrs, l) {
			continue
		}
		h := joinRowHash(leftArrs, l, &seed)
		for _, m := range rightBuckets[h] {
			if joinRowsEqual(leftArrs, l, rightArrs, int(m)) {
				rightMatched[m] = true
				leftMatched[l] = true
				leftOutIdx = append(leftOutIdx, int32(l))
				rightOutIdx = append(rightOutIdx, m)
			}
		}
	}
	// Phase 2: unmatched left rows (NULL right side) for left/full outer.
	if j.spec.Type == "left" || j.spec.Type == "full" {
		for l := 0; l < nL; l++ {
			if !leftMatched[l] {
				leftOutIdx = append(leftOutIdx, int32(l))
				rightOutIdx = append(rightOutIdx, -1)
			}
		}
	}
	// Phase 3: unmatched right rows (NULL left side) for right/full outer.
	if j.spec.Type == "right" || j.spec.Type == "full" {
		for r := 0; r < nR; r++ {
			if !rightMatched[r] {
				leftOutIdx = append(leftOutIdx, -1)
				rightOutIdx = append(rightOutIdx, int32(r))
			}
		}
	}

	outCols := make([]arrow.Array, int(leftRec.NumCols())+int(rightRec.NumCols()))
	outIdx := 0
	for ci := 0; ci < int(leftRec.NumCols()); ci++ {
		outCols[outIdx] = gatherColumn(j.alloc, leftRec.Column(ci), leftOutIdx)
		outIdx++
	}
	for ci := 0; ci < int(rightRec.NumCols()); ci++ {
		outCols[outIdx] = gatherColumn(j.alloc, rightRec.Column(ci), rightOutIdx)
		outIdx++
	}

	fields := make([]arrow.Field, len(outCols))
	for i, c := range outCols {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: c.DataType(), Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, outCols, int64(len(leftOutIdx))), nil
}

// joinKeyArrays resolves the join key columns (by name) to their arrays.
func joinKeyArrays(rec arrow.Record, cols []string) []arrow.Array {
	out := make([]arrow.Array, len(cols))
	for i, c := range cols {
		out[i] = arrowOperandColumn(rec, c)
	}
	return out
}

// joinHasNull reports whether any key column is NULL at the given row.
func joinHasNull(arrs []arrow.Array, row int) bool {
	for _, a := range arrs {
		if a.IsNull(row) {
			return true
		}
	}
	return false
}

// joinRowHash folds the key-column values of a row into a uint64 hash. It is
// the colexec-style replacement for the former per-row recordKey string
// builder: it allocates nothing per row and only falls back to value
// comparison (joinRowsEqual) on hash collision.
func joinRowHash(arrs []arrow.Array, row int, seed *maphash.Hash) uint64 {
	seed.Reset()
	for _, a := range arrs {
		switch a.DataType().ID() {
		case arrow.INT16:
			seed.WriteString("i")
			v := uint64(uint16(a.(*array.Int16).Value(row)))
			seed.Write((*[8]byte)(unsafe.Pointer(&v))[:])
		case arrow.INT32:
			seed.WriteString("i")
			v := uint64(uint32(a.(*array.Int32).Value(row)))
			seed.Write((*[8]byte)(unsafe.Pointer(&v))[:])
		case arrow.INT64:
			seed.WriteString("i")
			v := uint64(a.(*array.Int64).Value(row))
			seed.Write((*[8]byte)(unsafe.Pointer(&v))[:])
		case arrow.FLOAT64:
			seed.WriteString("f")
			v := math.Float64bits(a.(*array.Float64).Value(row))
			seed.Write((*[8]byte)(unsafe.Pointer(&v))[:])
		case arrow.BOOL:
			seed.WriteString("b")
			if a.(*array.Boolean).Value(row) {
				seed.WriteByte(1)
			} else {
				seed.WriteByte(0)
			}
		case arrow.STRING:
			seed.WriteString("s")
			seed.WriteString(a.(*array.String).Value(row))
		case arrow.BINARY:
			seed.WriteString("y")
			seed.Write(a.(*array.Binary).Value(row))
		case arrow.TIMESTAMP:
			seed.WriteString("t")
			v := uint64(a.(*array.Timestamp).Value(row))
			seed.Write((*[8]byte)(unsafe.Pointer(&v))[:])
		case arrow.DECIMAL128:
			seed.WriteString("d")
			d := a.(*array.Decimal128).Value(row)
			hb := uint64(d.HighBits())
			lb := d.LowBits()
			seed.Write((*[8]byte)(unsafe.Pointer(&hb))[:])
			seed.Write((*[8]byte)(unsafe.Pointer(&lb))[:])
		default:
			seed.WriteString("x")
		}
	}
	return seed.Sum64()
}

// joinRowsEqual reports whether the key columns of left row l equal those of
// right row r. Used to resolve hash-bucket collisions; if either side is NULL
// the keys are not considered equal (NULL != NULL).
func joinRowsEqual(left []arrow.Array, l int, right []arrow.Array, r int) bool {
	for k := range left {
		if left[k].IsNull(l) || right[k].IsNull(r) {
			return false
		}
		if !arrValEqual(left[k], l, right[k], r) {
			return false
		}
	}
	return true
}

// arrValEqual compares the value at (a,i) with (b,j) for the supported join
// key types. The caller must ensure neither position is NULL.
func arrValEqual(a arrow.Array, i int, b arrow.Array, j int) bool {
	switch a.DataType().ID() {
	case arrow.INT16:
		return a.(*array.Int16).Value(i) == b.(*array.Int16).Value(j)
	case arrow.INT32:
		return a.(*array.Int32).Value(i) == b.(*array.Int32).Value(j)
	case arrow.INT64:
		return a.(*array.Int64).Value(i) == b.(*array.Int64).Value(j)
	case arrow.FLOAT64:
		return a.(*array.Float64).Value(i) == b.(*array.Float64).Value(j)
	case arrow.BOOL:
		return a.(*array.Boolean).Value(i) == b.(*array.Boolean).Value(j)
	case arrow.STRING:
		return a.(*array.String).Value(i) == b.(*array.String).Value(j)
	case arrow.BINARY:
		return bytes.Equal(a.(*array.Binary).Value(i), b.(*array.Binary).Value(j))
	case arrow.TIMESTAMP:
		return a.(*array.Timestamp).Value(i) == b.(*array.Timestamp).Value(j)
	case arrow.DECIMAL128:
		da := a.(*array.Decimal128).Value(i)
		db := b.(*array.Decimal128).Value(j)
		return da.HighBits() == db.HighBits() && da.LowBits() == db.LowBits()
	}
	return false
}

// gatherColumn builds an output column by gathering the rows listed in idxs
// from src. An idx of -1 emits a null (used for outer-join unmatched rows),
// matching the classic merge/hash joiner's NULL-fill for unmatched sides.
func gatherColumn(alloc memory.Allocator, src arrow.Array, idxs []int32) arrow.Array {
	b := array.NewBuilder(alloc, src.DataType())
	for _, idx := range idxs {
		if idx < 0 {
			b.AppendNull()
			continue
		}
		appendValueAt(b, src, int(idx))
	}
	arr := b.NewArray()
	b.Release()
	return arr
}

func appendValueAt(b array.Builder, src arrow.Array, idx int) {
	// The source row itself may be NULL (e.g. a NULL join key on an unmatched
	// outer-join row); in that case emit a null rather than the underlying
	// zero value, which would otherwise surface as a spurious 0/NULL mix.
	if src.IsNull(idx) {
		b.AppendNull()
		return
	}
	switch src.DataType().ID() {
	case arrow.INT16:
		b.(*array.Int16Builder).Append(src.(*array.Int16).Value(idx))
	case arrow.INT32:
		b.(*array.Int32Builder).Append(src.(*array.Int32).Value(idx))
	case arrow.INT64:
		b.(*array.Int64Builder).Append(src.(*array.Int64).Value(idx))
	case arrow.FLOAT64:
		b.(*array.Float64Builder).Append(src.(*array.Float64).Value(idx))
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(src.(*array.Boolean).Value(idx))
	case arrow.STRING:
		b.(*array.StringBuilder).Append(src.(*array.String).Value(idx))
	case arrow.BINARY:
		b.(*array.BinaryBuilder).Append(src.(*array.Binary).Value(idx))
	case arrow.TIMESTAMP:
		b.(*array.TimestampBuilder).Append(src.(*array.Timestamp).Value(idx))
	case arrow.DECIMAL128:
		b.(*array.Decimal128Builder).Append(src.(*array.Decimal128).Value(idx))
	default:
		b.AppendNull()
	}
}
