// Copyright 2016 The Cockroach Authors.
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
	"math"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func initWindowBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range windows {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if !v.props.Impure {
			panic(fmt.Sprintf("%s: window functions should all be impure, found %v", k, v))
		}
		if v.props.Class != tree.WindowClass {
			panic(fmt.Sprintf("%s: window functions should be marked with the tree.WindowClass "+
				"function class, found %v", k, v))
		}
		for _, w := range v.overloads {
			if w.WindowFunc == nil {
				panic(fmt.Sprintf("%s: window functions should have tree.WindowFunc constructors, "+
					"found %v", k, w))
			}
		}
		builtins[k] = v
	}
}

func winProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Impure: true,
		Class:  tree.WindowClass,
	}
}

// windows are a special class of builtin functions that can only be applied
// as window functions using an OVER clause.
// See `windowFuncHolder` in the sql package.
var windows = map[string]builtinDefinition{
	"row_number": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{}, types.Int, newRowNumberWindow,
			"Calculates the number of the current row within its partition, counting from 1."),
	),
	"rank": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{}, types.Int, newRankWindow,
			"Calculates the rank of the current row with gaps; same as row_number of its first peer."),
	),
	"dense_rank": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{}, types.Int, newDenseRankWindow,
			"Calculates the rank of the current row without gaps; this function counts peer groups."),
	),
	"percent_rank": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{}, types.Float, newPercentRankWindow,
			"Calculates the relative rank of the current row: (rank - 1) / (total rows - 1)."),
	),
	"cume_dist": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{}, types.Float, newCumulativeDistWindow,
			"Calculates the relative rank of the current row: "+
				"(number of rows preceding or peer with current row) / (total rows)."),
	),
	"ntile": makeBuiltin(winProps(),
		makeWindowOverload(tree.ArgTypes{{"n", types.Int}}, types.Int, newNtileWindow,
			"Calculates an integer ranging from 1 to `n`, dividing the partition as equally as possible."),
	),
	"lag": collectOverloads(
		winProps(),
		types.Scalar,
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}}, t,
				makeLeadLagWindowConstructor(false, false, false),
				"Returns `val` evaluated at the previous row within current row's partition; "+
					"if there is no such row, instead returns null.")
		},
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}, {"n", types.Int}}, t,
				makeLeadLagWindowConstructor(false, true, false),
				"Returns `val` evaluated at the row that is `n` rows before the current row within its partition; "+
					"if there is no such row, instead returns null. `n` is evaluated with respect to the current row.")
		},
		// TODO(nvanbenschoten): We still have no good way to represent two parameters that
		// can be any types but must be the same (eg. lag(T, Int, T)).
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{
				{"val", t}, {"n", types.Int}, {"default", t},
			}, t, makeLeadLagWindowConstructor(false, true, true),
				"Returns `val` evaluated at the row that is `n` rows before the current row within its partition; "+
					"if there is no such, row, instead returns `default` (which must be of the same type as `val`). "+
					"Both `n` and `default` are evaluated with respect to the current row.")
		},
	),
	"lead": collectOverloads(winProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}}, t,
				makeLeadLagWindowConstructor(true, false, false),
				"Returns `val` evaluated at the following row within current row's partition; "+""+
					"if there is no such row, instead returns null.")
		},
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}, {"n", types.Int}}, t,
				makeLeadLagWindowConstructor(true, true, false),
				"Returns `val` evaluated at the row that is `n` rows after the current row within its partition; "+
					"if there is no such row, instead returns null. `n` is evaluated with respect to the current row.")
		},
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{
				{"val", t}, {"n", types.Int}, {"default", t},
			}, t, makeLeadLagWindowConstructor(true, true, true),
				"Returns `val` evaluated at the row that is `n` rows after the current row within its partition; "+
					"if there is no such, row, instead returns `default` (which must be of the same type as `val`). "+
					"Both `n` and `default` are evaluated with respect to the current row.")
		},
	),
	"first_value": collectOverloads(winProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}}, t, newFirstValueWindow,
				"Returns `val` evaluated at the row that is the first row of the window frame.")
		}),
	"last_value": collectOverloads(winProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{{"val", t}}, t, newLastValueWindow,
				"Returns `val` evaluated at the row that is the last row of the window frame.")
		}),
	"nth_value": collectOverloads(winProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			return makeWindowOverload(tree.ArgTypes{
				{"val", t}, {"n", types.Int}}, t, newNthValueWindow,
				"Returns `val` evaluated at the row that is the `n`th row of the window frame (counting from 1); "+
					"null if no such row.")
		}),
	"diff": collectOverloads(winProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			if t.Equal(*types.Int) {
				return makeWindowOverload(tree.ArgTypes{{"val", types.Int}}, types.Int, newDiffWindowInt,
					"Returns the difference between the current row's `val` and the previous row's `val` (both of type Int); if there is no previous row, returns null.")
			}
			return makeWindowOverload(tree.ArgTypes{{"val", t}}, t, newDiffWindowFloat,
				"Returns the difference between the current row's `val` and the previous row's `val` (both of type Float or Double); if there is no previous row, returns null.")
		},
	),
}

func makeWindowOverload(
	in tree.ArgTypes,
	ret *types.T,
	f func([]*types.T, *tree.EvalContext) tree.WindowFunc,
	info string,
) tree.Overload {
	return tree.Overload{
		Types:      in,
		ReturnType: tree.FixedReturnType(ret),
		WindowFunc: f,
		Info:       info,
	}
}

var _ tree.WindowFunc = &aggregateWindowFunc{}
var _ tree.WindowFunc = &framableAggregateWindowFunc{}
var _ tree.WindowFunc = &rowNumberWindow{}
var _ tree.WindowFunc = &rankWindow{}
var _ tree.WindowFunc = &denseRankWindow{}
var _ tree.WindowFunc = &percentRankWindow{}
var _ tree.WindowFunc = &cumulativeDistWindow{}
var _ tree.WindowFunc = &ntileWindow{}
var _ tree.WindowFunc = &leadLagWindow{}
var _ tree.WindowFunc = &firstValueWindow{}
var _ tree.WindowFunc = &lastValueWindow{}
var _ tree.WindowFunc = &nthValueWindow{}

// aggregateWindowFunc aggregates over the the current row's window frame, using
// the internal tree.AggregateFunc to perform the aggregation.
type aggregateWindowFunc struct {
	agg     tree.AggregateFunc
	peerRes tree.Datum
	// peerFrameStartIdx and peerFrameEndIdx indicate the boundaries of the
	// window frame over which peerRes was computed.
	peerFrameStartIdx, peerFrameEndIdx int
}

// NewAggregateWindowFunc creates a constructor of aggregateWindowFunc
// with agg initialized by provided aggConstructor.
func NewAggregateWindowFunc(
	aggConstructor func(*tree.EvalContext, tree.Datums) tree.AggregateFunc,
) func(*tree.EvalContext) tree.WindowFunc {
	return func(evalCtx *tree.EvalContext) tree.WindowFunc {
		return &aggregateWindowFunc{agg: aggConstructor(evalCtx, nil /* arguments */)}
	}
}

func (w *aggregateWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if !wfr.FirstInPeerGroup() && wfr.Frame.DefaultFrameExclusion() {
		return w.peerRes, nil
	}

	// Accumulate all values in the peer group at the same time, as these
	// must return the same value.
	peerGroupRowCount := wfr.PeerHelper.GetRowCount(wfr.CurRowPeerGroupNum)
	for i := 0; i < peerGroupRowCount; i++ {
		if skipped, err := wfr.IsRowSkipped(ctx, wfr.RowIdx+i); err != nil {
			return nil, err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsWithRowOffset(ctx, i)
		if err != nil {
			return nil, err
		}
		var value tree.Datum
		var others tree.Datums
		// COUNT_ROWS takes no arguments.
		if len(args) > 0 {
			value = args[0]
			others = args[1:]
		}
		if err := w.agg.Add(ctx, value, others...); err != nil {
			return nil, err
		}
	}

	// Retrieve the value for the entire peer group, save it, and return it.
	peerRes, err := w.agg.Result()
	if err != nil {
		return nil, err
	}
	w.peerRes = peerRes
	w.peerFrameStartIdx = wfr.RowIdx
	w.peerFrameEndIdx = wfr.RowIdx + peerGroupRowCount
	return w.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *aggregateWindowFunc) Reset(ctx context.Context) {
	w.agg.Reset(ctx)
	w.peerRes = nil
	w.peerFrameStartIdx = 0
	w.peerFrameEndIdx = 0
}

func (w *aggregateWindowFunc) Close(ctx context.Context, _ *tree.EvalContext) {
	w.agg.Close(ctx)
}

// ShouldReset sets shouldReset to true if w is framableAggregateWindowFunc.
func ShouldReset(w tree.WindowFunc) {
	if f, ok := w.(*framableAggregateWindowFunc); ok {
		f.shouldReset = true
	}
}

// framableAggregateWindowFunc is a wrapper around aggregateWindowFunc that allows
// to reset the aggregate by creating a new instance via a provided constructor.
// shouldReset indicates whether the resetting behavior is desired.
type framableAggregateWindowFunc struct {
	agg            *aggregateWindowFunc
	aggConstructor func(*tree.EvalContext, tree.Datums) tree.AggregateFunc
	shouldReset    bool
}

func newFramableAggregateWindow(
	agg tree.AggregateFunc, aggConstructor func(*tree.EvalContext, tree.Datums) tree.AggregateFunc,
) tree.WindowFunc {
	return &framableAggregateWindowFunc{
		agg:            &aggregateWindowFunc{agg: agg, peerRes: tree.DNull},
		aggConstructor: aggConstructor,
	}
}

func (w *framableAggregateWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if wfr.FullPartitionIsInWindow() {
		// Full partition is always inside of the window, and aggregations will
		// return the same result for all of the rows, so we're actually performing
		// the aggregation once, on the first row, and reuse the result for all
		// other rows.
		if wfr.RowIdx > 0 {
			return w.agg.peerRes, nil
		}
	}
	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if !wfr.FirstInPeerGroup() && wfr.Frame.DefaultFrameExclusion() {
		// The concept of window framing takes precedence over the concept of
		// peers - although we calculated the result for one of the peers of the
		// current row, it is possible for that peer to have a different window
		// frame, so we check that.
		if frameStartIdx == w.agg.peerFrameStartIdx && frameEndIdx == w.agg.peerFrameEndIdx {
			// The window frame is the same, so we return already calculated result.
			return w.agg.peerRes, nil
		}
		// The window frame is different, so we need to recalculate the result.
	}
	if !w.shouldReset {
		// We should not reset, so we will use the same aggregateWindowFunc.
		return w.agg.Compute(ctx, evalCtx, wfr)
	}

	// We should reset the aggregate, so we dispose of the old aggregate function
	// and construct a new one for the computation.
	w.agg.Close(ctx, evalCtx)
	// No arguments are passed into the aggConstructor and they are instead passed
	// in during the call to add().
	*w.agg = aggregateWindowFunc{
		agg:     w.aggConstructor(evalCtx, nil /* arguments */),
		peerRes: tree.DNull,
	}

	// Accumulate all values in the window frame.
	for i := frameStartIdx; i < frameEndIdx; i++ {
		if skipped, err := wfr.IsRowSkipped(ctx, i); err != nil {
			return nil, err
		} else if skipped {
			continue
		}
		args, err := wfr.ArgsByRowIdx(ctx, i)
		if err != nil {
			return nil, err
		}
		var value tree.Datum
		var others tree.Datums
		// COUNT_ROWS takes no arguments.
		if len(args) > 0 {
			value = args[0]
			others = args[1:]
		}
		if err := w.agg.agg.Add(ctx, value, others...); err != nil {
			return nil, err
		}
	}

	// Retrieve the value for the entire peer group, save it, and return it.
	peerRes, err := w.agg.agg.Result()
	if err != nil {
		return nil, err
	}
	w.agg.peerRes = peerRes
	w.agg.peerFrameStartIdx = frameStartIdx
	w.agg.peerFrameEndIdx = frameEndIdx
	return w.agg.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *framableAggregateWindowFunc) Reset(ctx context.Context) {
	w.agg.Reset(ctx)
}

func (w *framableAggregateWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.agg.Close(ctx, evalCtx)
}

// rowNumberWindow computes the number of the current row within its partition,
// counting from 1.
type rowNumberWindow struct{}

func newRowNumberWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &rowNumberWindow{}
}

func (rowNumberWindow) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(wfr.RowIdx + 1 /* one-indexed */)), nil
}

// Reset implements tree.WindowFunc interface.
func (rowNumberWindow) Reset(context.Context) {}

func (rowNumberWindow) Close(context.Context, *tree.EvalContext) {}

// rankWindow computes the rank of the current row with gaps.
type rankWindow struct {
	peerRes *tree.DInt
}

func newRankWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &rankWindow{}
}

func (w *rankWindow) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if wfr.FirstInPeerGroup() {
		w.peerRes = tree.NewDInt(tree.DInt(wfr.Rank()))
	}
	return w.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *rankWindow) Reset(context.Context) {
	w.peerRes = nil
}

func (w *rankWindow) Close(context.Context, *tree.EvalContext) {}

// denseRankWindow computes the rank of the current row without gaps (it counts peer groups).
type denseRankWindow struct {
	denseRank int
	peerRes   *tree.DInt
}

func newDenseRankWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &denseRankWindow{}
}

func (w *denseRankWindow) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if wfr.FirstInPeerGroup() {
		w.denseRank++
		w.peerRes = tree.NewDInt(tree.DInt(w.denseRank))
	}
	return w.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *denseRankWindow) Reset(context.Context) {
	w.denseRank = 0
	w.peerRes = nil
}

func (w *denseRankWindow) Close(context.Context, *tree.EvalContext) {}

// percentRankWindow computes the relative rank of the current row using:
//
//	(rank - 1) / (total rows - 1)
type percentRankWindow struct {
	peerRes *tree.DFloat
}

func newPercentRankWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &percentRankWindow{}
}

var dfloatZero = tree.NewDFloat(0)

func (w *percentRankWindow) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	// Return zero if there's only one row, per spec.
	if wfr.PartitionSize() <= 1 {
		return dfloatZero, nil
	}

	if wfr.FirstInPeerGroup() {
		// (rank - 1) / (total rows - 1)
		w.peerRes = tree.NewDFloat(tree.DFloat(wfr.Rank()-1) / tree.DFloat(wfr.PartitionSize()-1))
	}
	return w.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *percentRankWindow) Reset(context.Context) {
	w.peerRes = nil
}

func (w *percentRankWindow) Close(context.Context, *tree.EvalContext) {}

// cumulativeDistWindow computes the relative rank of the current row using:
//
//	(number of rows preceding or peer with current row) / (total rows)
type cumulativeDistWindow struct {
	peerRes *tree.DFloat
}

func newCumulativeDistWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &cumulativeDistWindow{}
}

func (w *cumulativeDistWindow) Compute(
	_ context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if wfr.FirstInPeerGroup() {
		// (number of rows preceding or peer with current row) / (total rows)
		w.peerRes = tree.NewDFloat(tree.DFloat(wfr.DefaultFrameSize()) / tree.DFloat(wfr.PartitionSize()))
	}
	return w.peerRes, nil
}

// Reset implements tree.WindowFunc interface.
func (w *cumulativeDistWindow) Reset(context.Context) {
	w.peerRes = nil
}

func (w *cumulativeDistWindow) Close(context.Context, *tree.EvalContext) {}

// ntileWindow computes an integer ranging from 1 to the argument value, dividing
// the partition as equally as possible.
type ntileWindow struct {
	ntile          *tree.DInt // current result
	curBucketCount int        // row number of current bucket
	boundary       int        // how many rows should be in the bucket
	remainder      int        // (total rows) % (bucket num)
}

func newNtileWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &ntileWindow{}
}

var errInvalidArgumentForNtile = pgerror.Newf(
	pgcode.InvalidParameterValue, "argument of ntile() must be greater than zero")

func (w *ntileWindow) Compute(
	ctx context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	if w.ntile == nil {
		// If this is the first call to ntileWindow.Compute, set up the buckets.
		total := wfr.PartitionSize()

		args, err := wfr.Args(ctx)
		if err != nil {
			return nil, err
		}
		arg := args[0]
		if arg == tree.DNull {
			// per spec: If argument is the null value, then the result is the null value.
			return tree.DNull, nil
		}

		nbuckets := int(tree.MustBeDInt(arg))
		if nbuckets <= 0 {
			// per spec: If argument is less than or equal to 0, then an error is returned.
			return nil, errInvalidArgumentForNtile
		}

		w.ntile = tree.NewDInt(1)
		w.curBucketCount = 0
		w.boundary = total / nbuckets
		if w.boundary <= 0 {
			w.boundary = 1
		} else {
			// If the total number is not divisible, add 1 row to leading buckets.
			w.remainder = total % nbuckets
			if w.remainder != 0 {
				w.boundary++
			}
		}
	}

	w.curBucketCount++
	if w.boundary < w.curBucketCount {
		// Move to next ntile bucket.
		if w.remainder != 0 && int(*w.ntile) == w.remainder {
			w.remainder = 0
			w.boundary--
		}
		w.ntile = tree.NewDInt(*w.ntile + 1)
		w.curBucketCount = 1
	}
	return w.ntile, nil
}

// Reset implements tree.WindowFunc interface.
func (w *ntileWindow) Reset(context.Context) {
	w.boundary = 0
	w.curBucketCount = 0
	w.ntile = nil
	w.remainder = 0
}

func (w *ntileWindow) Close(context.Context, *tree.EvalContext) {}

type leadLagWindow struct {
	forward     bool
	withOffset  bool
	withDefault bool
}

func newLeadLagWindow(forward, withOffset, withDefault bool) tree.WindowFunc {
	return &leadLagWindow{
		forward:     forward,
		withOffset:  withOffset,
		withDefault: withDefault,
	}
}

func makeLeadLagWindowConstructor(
	forward, withOffset, withDefault bool,
) func([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return func([]*types.T, *tree.EvalContext) tree.WindowFunc {
		return newLeadLagWindow(forward, withOffset, withDefault)
	}
}

func (w *leadLagWindow) Compute(
	ctx context.Context, _ *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	offset := 1
	if w.withOffset {
		args, err := wfr.Args(ctx)
		if err != nil {
			return nil, err
		}
		offsetArg := args[1]
		if offsetArg == tree.DNull {
			return tree.DNull, nil
		}
		offset = int(tree.MustBeDInt(offsetArg))
	}
	if !w.forward {
		offset *= -1
	}

	if targetRow := wfr.RowIdx + offset; targetRow < 0 || targetRow >= wfr.PartitionSize() {
		// Target row is out of the partition; supply default value if provided,
		// otherwise return NULL.
		if w.withDefault {
			args, err := wfr.Args(ctx)
			if err != nil {
				return nil, err
			}
			return args[2], nil
		}
		return tree.DNull, nil
	}

	args, err := wfr.ArgsWithRowOffset(ctx, offset)
	if err != nil {
		return nil, err
	}
	return args[0], nil
}

// Reset implements tree.WindowFunc interface.
func (w *leadLagWindow) Reset(context.Context) {}

func (w *leadLagWindow) Close(context.Context, *tree.EvalContext) {}

// firstValueWindow returns value evaluated at the row that is the first row of the window frame.
type firstValueWindow struct{}

func newFirstValueWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &firstValueWindow{}
}

func (firstValueWindow) Compute(
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
	for idx := frameStartIdx; idx < frameEndIdx; idx++ {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return nil, err
		} else if !skipped {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return nil, err
			}
			return row.GetDatum(int(wfr.ArgsIdxs[0]))
		}
	}
	// All rows are skipped from the frame, so it is empty, and we return NULL.
	return tree.DNull, nil
}

// Reset implements tree.WindowFunc interface.
func (firstValueWindow) Reset(context.Context) {}

func (firstValueWindow) Close(context.Context, *tree.EvalContext) {}

// lastValueWindow returns value evaluated at the row that is the last row of the window frame.
type lastValueWindow struct{}

func newLastValueWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &lastValueWindow{}
}

func (lastValueWindow) Compute(
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
	for idx := frameEndIdx - 1; idx >= frameStartIdx; idx-- {
		if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
			return nil, err
		} else if !skipped {
			row, err := wfr.Rows.GetRow(ctx, idx)
			if err != nil {
				return nil, err
			}
			return row.GetDatum(int(wfr.ArgsIdxs[0]))
		}
	}
	// All rows are skipped from the frame, so it is empty, and we return NULL.
	return tree.DNull, nil
}

// Reset implements tree.WindowFunc interface.
func (lastValueWindow) Reset(context.Context) {}

func (lastValueWindow) Close(context.Context, *tree.EvalContext) {}

// nthValueWindow returns value evaluated at the row that is the nth row of the window frame
// (counting from 1). Returns null if no such row.
type nthValueWindow struct{}

func newNthValueWindow([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &nthValueWindow{}
}

var errInvalidArgumentForNthValue = pgerror.Newf(
	pgcode.InvalidParameterValue, "argument of nth_value() must be greater than zero")

func (nthValueWindow) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	args, err := wfr.Args(ctx)
	if err != nil {
		return nil, err
	}
	arg := args[1]
	if arg == tree.DNull {
		return tree.DNull, nil
	}

	nth := int(tree.MustBeDInt(arg))
	if nth <= 0 {
		return nil, errInvalidArgumentForNthValue
	}

	frameStartIdx, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	frameEndIdx, err := wfr.FrameEndIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if nth > frameEndIdx-frameStartIdx {
		// The requested index is definitely outside of the window frame, so we
		// return NULL.
		return tree.DNull, nil
	}
	var idx int
	// Note that we do not need to check whether a filter is present because
	// filters are only supported for aggregate functions.
	if wfr.Frame.DefaultFrameExclusion() {
		// We subtract 1 because nth is counting from 1.
		idx = frameStartIdx + nth - 1
	} else {
		ith := 0
		for idx = frameStartIdx; idx < frameEndIdx; idx++ {
			if skipped, err := wfr.IsRowSkipped(ctx, idx); err != nil {
				return nil, err
			} else if !skipped {
				ith++
				if ith == nth {
					// idx now points at the desired row.
					break
				}
			}
		}
		if idx == frameEndIdx {
			// The requested index is outside of the window frame, so we return NULL.
			return tree.DNull, nil
		}
	}
	row, err := wfr.Rows.GetRow(ctx, idx)
	if err != nil {
		return nil, err
	}
	return row.GetDatum(int(wfr.ArgsIdxs[0]))
}

// Reset implements tree.WindowFunc interface.
func (nthValueWindow) Reset(context.Context) {}

func (nthValueWindow) Close(context.Context, *tree.EvalContext) {}

// DiffWindowInt is use for DiffWindow for arg int
type DiffWindowInt struct {
	notNull tree.Datum
	IsFirst bool
}

func newDiffWindowInt([]*types.T, *tree.EvalContext) tree.WindowFunc {
	return &DiffWindowInt{}
}

// Compute is for DiffWindowInt compute
func (dw *DiffWindowInt) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	var err error
	columnIdx := int(wfr.ArgsIdxs[0])

	currentIndex := wfr.RowIdx

	var prevRow tree.IndexedRow
	var prevValue tree.Datum

	first, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if currentIndex == first || dw.IsFirst {
		currentRow, currentErr := wfr.Rows.GetRow(ctx, currentIndex)
		if currentErr != nil {
			return nil, currentErr
		}
		currentValue, currentErr := currentRow.GetDatum(columnIdx)
		if currentErr != nil {
			return nil, currentErr
		}
		if currentValue == tree.DNull {
			dw.IsFirst = true
			return tree.DNull, nil
		}
		dw.IsFirst = false
		return nil, nil
	}

	prevRow, err = wfr.Rows.GetRow(ctx, currentIndex-1)
	if err != nil {
		return nil, err
	}
	prevValue, err = prevRow.GetDatum(columnIdx)
	if err != nil {
		return nil, err
	}
	if prevValue == tree.DNull {
		prevValue = dw.notNull
	}

	var currentRow tree.IndexedRow
	currentRow, err = wfr.Rows.GetRow(ctx, currentIndex)
	if err != nil {
		return nil, err
	}
	var currentValue tree.Datum
	currentValue, err = currentRow.GetDatum(columnIdx)
	if err != nil {
		return nil, err
	}
	if currentValue == tree.DNull {
		if prevValue != tree.DNull {
			dw.notNull = prevValue
		}
		return tree.DNull, nil
	}

	if prevValue == nil {
		return tree.DNull, nil
	}

	diffValue, err := safeSub(tree.MustBeDInt(currentValue), tree.MustBeDInt(prevValue))
	if err != nil {
		return nil, err
	}

	return &diffValue, nil
}

// Reset is for DiffWindowInt reset
func (dw *DiffWindowInt) Reset(context.Context) {}

// Close is for DiffWindowInt close
func (dw *DiffWindowInt) Close(context.Context, *tree.EvalContext) {}

// DiffWindowFloat is use for DiffWindow for arg float
type DiffWindowFloat struct {
	notNull   tree.Datum
	ifdecimal bool
	IsFirst   bool
}

func newDiffWindowFloat(difftypes []*types.T, evalcontext *tree.EvalContext) tree.WindowFunc {
	result := &DiffWindowFloat{}
	for _, difftype := range difftypes {
		if difftype.InternalType.Family == types.DecimalFamily {
			result.ifdecimal = true
		}
	}
	return result
}

// Compute is for DiffWindowFloat compute
func (dw *DiffWindowFloat) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	var err error
	columnIdx := int(wfr.ArgsIdxs[0])

	currentIndex := wfr.RowIdx

	var prevRow tree.IndexedRow
	var prevValue tree.Datum

	first, err := wfr.FrameStartIdx(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	if currentIndex == first || dw.IsFirst {
		currentRow, currentErr := wfr.Rows.GetRow(ctx, currentIndex)
		if currentErr != nil {
			return nil, currentErr
		}
		currentValue, currentErr := currentRow.GetDatum(columnIdx)
		if currentErr != nil {
			return nil, currentErr
		}
		if currentValue == tree.DNull {
			dw.IsFirst = true
			return tree.DNull, nil
		}
		dw.IsFirst = false
		return nil, nil
	}

	prevRow, err = wfr.Rows.GetRow(ctx, currentIndex-1)
	if err != nil {
		return nil, err
	}
	prevValue, err = prevRow.GetDatum(columnIdx)
	if err != nil {
		return nil, err
	}
	if prevValue == tree.DNull {
		prevValue = dw.notNull
	}

	var currentRow tree.IndexedRow
	currentRow, err = wfr.Rows.GetRow(ctx, currentIndex)
	if err != nil {
		return nil, err
	}
	var currentValue tree.Datum
	currentValue, err = currentRow.GetDatum(columnIdx)
	if err != nil {
		return nil, err
	}
	if currentValue == tree.DNull {
		if prevValue != tree.DNull {
			dw.notNull = prevValue
		}
		return tree.DNull, nil
	}

	if dw.ifdecimal {
		currentDecimal, currentErr := strconv.ParseFloat(currentValue.String(), 64)
		if currentErr != nil {
			return nil, currentErr
		}
		if prevValue == nil {
			return tree.DNull, nil
		}
		prevDecimal, prevErr := strconv.ParseFloat(prevValue.String(), 64)
		if prevErr != nil {
			return nil, prevErr
		}
		var result tree.DDecimal
		diffValue, diffValueErr := result.SetFloat64(currentDecimal - prevDecimal)
		if diffValueErr != nil {
			return nil, diffValueErr
		}
		return &tree.DDecimal{*diffValue}, nil
	}

	if prevValue == nil {
		return tree.DNull, nil
	}

	diffValue := tree.MustBeDFloat(currentValue) - tree.MustBeDFloat(prevValue)
	if err != nil {
		return nil, err
	}

	return &diffValue, nil
}

// Reset is for DiffWindowFloat reset
func (dw *DiffWindowFloat) Reset(context.Context) {}

// Close is for DiffWindowFloat close
func (dw *DiffWindowFloat) Close(context.Context, *tree.EvalContext) {}

// safeSub saf sub
func safeSub(a, b tree.DInt) (tree.DInt, error) {
	// 检查减法是否会导致溢出
	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return 0, tree.ErrIntOutOfRange
	}
	return a - b, nil
}
