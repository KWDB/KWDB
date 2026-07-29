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
	"context"
	"fmt"
	"strings"

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

	// Build the right-side hash table.
	rightMap := make(map[string][]int32)
	for r := 0; r < nR; r++ {
		if recordHasNull(rightRec, j.spec.RightKeys, r) {
			continue
		}
		key := recordKey(rightRec, j.spec.RightKeys, r)
		rightMap[key] = append(rightMap[key], int32(r))
	}

	leftOutIdx := make([]int32, 0, nL)
	rightOutIdx := make([]int32, 0, nL)
	leftMatched := make([]bool, nL)
	rightMatched := make([]bool, nR)
	// Phase 1: matched pairs (left-major, ascending right index, deterministic).
	for l := 0; l < nL; l++ {
		if recordHasNull(leftRec, j.spec.LeftKeys, l) {
			continue
		}
		key := recordKey(leftRec, j.spec.LeftKeys, l)
		matches := rightMap[key]
		if len(matches) == 0 {
			continue
		}
		leftMatched[l] = true
		for _, m := range matches {
			rightMatched[m] = true
			leftOutIdx = append(leftOutIdx, int32(l))
			rightOutIdx = append(rightOutIdx, m)
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

// recordHasNull reports whether any of the named columns is NULL at the given
// row. A NULL equality key must not match anything.
func recordHasNull(rec arrow.Record, cols []string, row int) bool {
	for _, c := range cols {
		col := arrowOperandColumn(rec, c)
		if col.IsNull(row) {
			return true
		}
	}
	return false
}

// recordKey builds a canonical key for a row from the named columns.
func recordKey(rec arrow.Record, cols []string, row int) string {
	var sb strings.Builder
	for _, c := range cols {
		col := arrowOperandColumn(rec, c)
		if col.IsNull(row) {
			sb.WriteString("\x00∅\x00")
			continue
		}
		switch col.DataType().ID() {
		case arrow.INT64:
			fmt.Fprintf(&sb, "i%d\x1f", col.(*array.Int64).Value(row))
		case arrow.FLOAT64:
			fmt.Fprintf(&sb, "f%g\x1f", col.(*array.Float64).Value(row))
		case arrow.BOOL:
			fmt.Fprintf(&sb, "b%v\x1f", col.(*array.Boolean).Value(row))
		case arrow.STRING:
			sb.WriteString("s")
			sb.WriteString(col.(*array.String).Value(row))
			sb.WriteString("\x1f")
		default:
			fmt.Fprintf(&sb, "x%d\x1f", row)
		}
	}
	return sb.String()
}

// gatherColumn builds an output column by gathering the rows listed in idxs
// from src. An idx of -1 emits a null (used for left-outer unmatched rows).
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
	switch src.DataType().ID() {
	case arrow.INT64:
		b.(*array.Int64Builder).Append(src.(*array.Int64).Value(idx))
	case arrow.FLOAT64:
		b.(*array.Float64Builder).Append(src.(*array.Float64).Value(idx))
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(src.(*array.Boolean).Value(idx))
	case arrow.STRING:
		b.(*array.StringBuilder).Append(src.(*array.String).Value(idx))
	default:
		b.AppendNull()
	}
}
