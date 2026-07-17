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

package builtins

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/ring"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
)

// frameCursor tracks which values have already been added to the current
// window computation, preventing redundant work when the frame slides forward.
type frameCursor struct {
	prevStart int
	prevEnd   int
}

// markedDatum pairs a datum value with its row index for use in the priority
// deque maintained by the sliding window.
type markedDatum struct {
	value tree.Datum
	idx   int
}

// slidingWindow maintains a deque of values along with corresponding indices
// ordered by a custom comparison function:
//   - for Min behavior: cmp = -a.Compare(b)
//   - for Max behavior: cmp = a.Compare(b)
//
// It assumes that the frame bounds will never go back, i.e. non-decreasing
// sequences of frame start and frame end indices.
type slidingWindow struct {
	values  ring.Buffer
	evalCtx *tree.EvalContext
	cmp     func(*tree.EvalContext, tree.Datum, tree.Datum) int
}

func makeSlidingWindow(
	evalCtx *tree.EvalContext, cmp func(*tree.EvalContext, tree.Datum, tree.Datum) int,
) *slidingWindow {
	return &slidingWindow{
		evalCtx: evalCtx,
		cmp:     cmp,
	}
}

// add first removes all values that are "smaller or equal" (depending on cmp)
// from the end of the deque and then appends 'md' to the end. This way, the
// deque always contains unique values sorted in descending order of their
// "priority" (when we encounter duplicates, we always keep the one with the
// largest idx).
func (sw *slidingWindow) add(md *markedDatum) {
	sw.removeLowerPriorityEntries(md)
	sw.values.AddLast(md)
}

// removeLowerPriorityEntries discards entries from the end of the deque that
// have priority <= the incoming datum's priority (per the cmp function).
func (sw *slidingWindow) removeLowerPriorityEntries(md *markedDatum) {
	for i := sw.values.Len() - 1; i >= 0; i-- {
		existing := sw.values.Get(i).(*markedDatum)
		if sw.cmp(sw.evalCtx, existing.value, md.value) > 0 {
			break
		}
		sw.values.RemoveLast()
	}
}

// removeAllBefore removes all values from the beginning of the deque that have
// indices smaller than given 'idx'. This operation corresponds to shifting the
// start of the frame up to 'idx'.
func (sw *slidingWindow) removeAllBefore(idx int) {
	for i := 0; i < sw.values.Len() && i < idx; i++ {
		if sw.values.Get(i).(*markedDatum).idx >= idx {
			break
		}
		sw.values.RemoveFirst()
	}
}

func (sw *slidingWindow) string() string {
	var builder strings.Builder
	for i := 0; i < sw.values.Len(); i++ {
		md := sw.values.Get(i).(*markedDatum)
		builder.WriteString(fmt.Sprintf("(%v, %v)\t", md.value, md.idx))
	}
	return builder.String()
}

func (sw *slidingWindow) reset() {
	sw.values.Reset()
}

type slidingWindowFunc struct {
	sw     *slidingWindow
	cursor *frameCursor
}

func newSlidingWindowFunc() *slidingWindowFunc {
	return &slidingWindowFunc{
		cursor: &frameCursor{},
	}
}

// Compute implements WindowFunc interface.
func (w *slidingWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}

	if !wfr.Frame.DefaultFrameExclusion() {
		return w.computeWithExclusionFallback(ctx, evalCtx, wfr, frameStartIdx, frameEndIdx)
	}
	return w.computeWithSlidingWindow(ctx, wfr, frameStartIdx, frameEndIdx)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// computeWithExclusionFallback handles frames that have non-default exclusion,
// falling back to a naive quadratic scan over all rows in the frame.
func (w *slidingWindowFunc) computeWithExclusionFallback(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	wfr *tree.WindowFrameRun,
	frameStartIdx, frameEndIdx int,
) (tree.Datum, error) {
	var best tree.Datum
	for idx := frameStartIdx; idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return nil, err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return nil, err
		}
		best = updateBestDatum(best, args[0], evalCtx, w.sw.cmp)
	}
	if best == nil {
		return tree.DNull, nil // Spec: the frame is empty, so we return NULL.
	}
	return best, nil
}

// computeWithSlidingWindow adds newly entering rows to the deque and removes
// rows that have left the frame, then returns the highest-priority datum.
func (w *slidingWindowFunc) computeWithSlidingWindow(
	ctx context.Context, wfr *tree.WindowFrameRun, frameStartIdx, frameEndIdx int,
) (tree.Datum, error) {
	// Discard all values that are no longer in the frame.
	w.sw.removeAllBefore(frameStartIdx)

	// Add all values that just entered the frame and have not been added yet.
	if err := w.addNewRowsToWindow(ctx, wfr, frameStartIdx, frameEndIdx); err != nil {
		return nil, err
	}
	w.cursor.prevEnd = frameEndIdx

	if w.sw.values.Len() == 0 {
		return tree.DNull, nil // Spec: the frame is empty, so we return NULL.
	}

	// The datum with "highest priority" within the frame is at the very front
	// of the deque.
	return w.sw.values.GetFirst().(*markedDatum).value, nil
}

// addNewRowsToWindow iterates over rows that just entered the frame and adds
// their non-null values to the sliding window deque.
func (w *slidingWindowFunc) addNewRowsToWindow(
	ctx context.Context, wfr *tree.WindowFrameRun, frameStartIdx, frameEndIdx int,
) error {
	for idx := max(w.cursor.prevEnd, frameStartIdx); idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return err
		}
		value := args[0]
		if value == tree.DNull {
			// Nulls are neither min nor max over a frame with non-null values;
			// the case of an all-null frame is handled by the empty deque check.
			continue
		}
		w.sw.add(&markedDatum{value: value, idx: idx})
	}
	return nil
}

// Reset implements tree.WindowFunc interface.
func (w *slidingWindowFunc) Reset(context.Context) {
	w.cursor.prevEnd = 0
	w.sw.reset()
}

// Close implements WindowFunc interface.
func (w *slidingWindowFunc) Close(context.Context, *tree.EvalContext) {
	w.sw = nil
}

// slidingWindowSumFunc applies sliding window approach to summation over
// a frame. It assumes that the frame bounds will never go back, i.e.
// non-decreasing sequences of frame start and frame end indices.
type slidingWindowSumFunc struct {
	agg    tree.AggregateFunc // one of the four SumAggregates
	cursor *frameCursor

	// lastNonNullIdx is the index of the latest non-null value seen in the
	// sliding window so far. noNonNullSeen indicates non-null values are yet to
	// be seen.
	lastNonNullIdx int
}

const noNonNullSeen = -1

func newSlidingWindowSumFunc(agg tree.AggregateFunc) *slidingWindowSumFunc {
	return &slidingWindowSumFunc{
		agg:            agg,
		cursor:         &frameCursor{},
		lastNonNullIdx: noNonNullSeen,
	}
}

// subtractRowFromAggregate removes a row's contribution from the running sum
// by adding its negated value to the aggregate.
func (w *slidingWindowSumFunc) removeAllBefore(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) error {
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return err
	}
	for idx := w.cursor.prevStart; idx < frameStartIdx && idx < w.cursor.prevEnd; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return err
		}
		if err := w.subtractDatumFromAggregate(ctx, args[0]); err != nil {
			return err
		}
	}
	return nil
}

// Compute implements WindowFunc interface.
func (w *slidingWindowSumFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if !wfr.Frame.DefaultFrameExclusion() {
		return w.computeSumWithExclusionFallback(ctx, wfr, frameStartIdx, frameEndIdx)
	}

	// Discard all values that are no longer in the frame.
	if err = w.removeAllBefore(ctx, evalCtx, wfr); err != nil {
		return nil, err
	}

	// Sum all values that just entered the frame.
	if err = w.addRowsToAggregate(ctx, wfr, frameStartIdx, frameEndIdx); err != nil {
		return nil, err
	}

	w.cursor.prevStart = frameStartIdx
	w.cursor.prevEnd = frameEndIdx

	return w.evaluateFrameResult(frameStartIdx, frameEndIdx)
}

// computeSumWithExclusionFallback performs a naive full-frame summation when
// a frame exclusion clause prevents using the sliding window optimization.
func (w *slidingWindowSumFunc) computeSumWithExclusionFallback(
	ctx context.Context, wfr *tree.WindowFrameRun, frameStartIdx, frameEndIdx int,
) (tree.Datum, error) {
	w.agg.Reset(ctx)
	for idx := frameStartIdx; idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return nil, err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return nil, err
		}
		if err = w.agg.Add(ctx, args[0]); err != nil {
			return nil, err
		}
	}
	return w.agg.Result()
}

// addRowsToAggregate sums all non-null values that just entered the frame and
// have not yet been added.
func (w *slidingWindowSumFunc) addRowsToAggregate(
	ctx context.Context, wfr *tree.WindowFrameRun, frameStartIdx, frameEndIdx int,
) error {
	for idx := max(w.cursor.prevEnd, frameStartIdx); idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return err
		}
		if args[0] != tree.DNull {
			w.lastNonNullIdx = idx
			if err = w.agg.Add(ctx, args[0]); err != nil {
				return err
			}
		}
	}
	return nil
}

// evaluateFrameResult determines whether to return the aggregate result or NULL
// based on frame emptiness and whether all remaining values are nulls.
func (w *slidingWindowSumFunc) evaluateFrameResult(
	frameStartIdx, frameEndIdx int,
) (tree.Datum, error) {
	// If last non-null value has index smaller than the start of the window
	// frame, then only nulls can be in the frame.
	onlyNulls := w.lastNonNullIdx < frameStartIdx
	if frameStartIdx == frameEndIdx || onlyNulls {
		return tree.DNull, nil
	}
	return w.agg.Result()
}

// Reset implements tree.WindowFunc interface.
func (w *slidingWindowSumFunc) Reset(ctx context.Context) {
	w.cursor.prevStart = 0
	w.cursor.prevEnd = 0
	w.lastNonNullIdx = noNonNullSeen
	w.agg.Reset(ctx)
}

// Close implements WindowFunc interface.
func (w *slidingWindowSumFunc) Close(ctx context.Context, _ *tree.EvalContext) {
	w.agg.Close(ctx)
}

// avgWindowFunc uses slidingWindowSumFunc to compute average over a frame.
type avgWindowFunc struct {
	sum *slidingWindowSumFunc
}

// Compute implements WindowFunc interface.
func (w *avgWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	sum, err := w.sum.Compute(ctx, evalCtx, wfr)
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		// Spec: the frame is empty, so we return NULL.
		return tree.DNull, nil
	}

	frameSize := 0
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	for idx := frameStartIdx; idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return nil, err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, idx)
		if err != nil {
			return nil, err
		}
		if args[0] == tree.DNull {
			// Null values do not count towards the number of rows that contribute
			// to the sum, so we're omitting them from the frame.
			continue
		}
		frameSize++
	}

	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(frameSize)), nil
	case *tree.DDecimal:
		var avg tree.DDecimal
		count := apd.New(int64(frameSize), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &t.Decimal, count)
		return &avg, err
	case *tree.DInt:
		dd := tree.DDecimal{}
		dd.SetFinite(int64(*t), 0)
		var avg tree.DDecimal
		count := apd.New(int64(frameSize), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &dd.Decimal, count)
		return &avg, err
	case *tree.DInterval:
		return &tree.DInterval{Duration: t.Duration.Div(int64(frameSize))}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected SUM result type: %s", t)
	}
}

// Reset implements tree.WindowFunc interface.
func (w *avgWindowFunc) Reset(ctx context.Context) {
	w.sum.Reset(ctx)
}

// Close implements WindowFunc interface.
func (w *avgWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sum.Close(ctx, evalCtx)
}

// updateBestDatum compares a candidate datum against the current best using
// the provided comparison function and returns the better of the two.
func updateBestDatum(
	best, candidate tree.Datum,
	evalCtx *tree.EvalContext,
	cmp func(*tree.EvalContext, tree.Datum, tree.Datum) int,
) tree.Datum {
	if best == nil {
		return candidate
	}
	if cmp(evalCtx, candidate, best) > 0 {
		return candidate
	}
	return best
}

// subtractDatumFromAggregate removes a single datum's contribution from the
// running aggregate by adding the negated value.
func (w *slidingWindowSumFunc) subtractDatumFromAggregate(
	ctx context.Context, value tree.Datum,
) error {
	if value == tree.DNull {
		// Null values do not contribute to the running sum, so there is nothing
		// to subtract once they leave the window frame.
		return nil
	}
	switch v := value.(type) {
	case *tree.DInt:
		return w.agg.Add(ctx, tree.NewDInt(-*v))
	case *tree.DDecimal:
		d := tree.DDecimal{}
		d.Neg(&v.Decimal)
		return w.agg.Add(ctx, &d)
	case *tree.DFloat:
		return w.agg.Add(ctx, tree.NewDFloat(-*v))
	case *tree.DInterval:
		return w.agg.Add(ctx, &tree.DInterval{Duration: duration.Duration{}.Sub(v.Duration)})
	default:
		return errors.AssertionFailedf("unexpected value %v", v)
	}
}
