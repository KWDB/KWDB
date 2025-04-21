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

package rowexec

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stringarena"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type aggregateFuncs []tree.AggregateFunc

func (af aggregateFuncs) close(ctx context.Context) {
	for _, f := range af {
		f.Close(ctx)
	}
}

// aggregatorBase is the foundation of the processor core type that does
// "aggregation" in the SQL sense. It groups rows and computes an aggregate for
// each group. The group is configured using the group key and the aggregator
// can be configured with one or more aggregation functions, as defined in the
// AggregatorSpec_Func enum.
//
// aggregatorBase's output schema is comprised of what is specified by the
// accompanying SELECT expressions.
type aggregatorBase struct {
	execinfra.ProcessorBase

	// runningState represents the state of the aggregator. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState aggregatorState
	input        execinfra.RowSource
	inputDone    bool
	inputTypes   []types.T
	funcs        []*aggregateFuncHolder
	outputTypes  []types.T
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc
	gapfill      gapfilltype
	imputation   []imputationtype
	bucketsAcc   mon.BoundAccount
	aggFuncsAcc  mon.BoundAccount

	// isScalar can only be set if there are no groupCols, and it means that we
	// will generate a result row even if there are no input rows. Used for
	// queries like SELECT MAX(n) FROM t.
	isScalar         bool
	groupCols        []uint32
	orderedGroupCols []uint32
	aggregations     []execinfrapb.AggregatorSpec_Aggregation
	interpolated     bool

	lastOrdGroupCols  sqlbase.EncDatumRow
	eventOrdGroupCols sqlbase.EncDatumRow
	arena             stringarena.Arena
	row               sqlbase.EncDatumRow
	scratch           []byte

	cancelChecker *sqlbase.CancelChecker

	// keep track of all internal ts columns for last/last_row
	internalTsCols []uint32

	// hasTimeBucketGapFill is true if aggregations have function time_bucket_gapFill.
	hasTimeBucketGapFill   bool
	timeBucketGapFillColID int32
	// ScalarGroupBy with sum_Int agg in inside_out case must return 0 when the table is empty, because sum_int
	// is the twice agg of count.
	scalarGroupByWithSumInt bool

	groupWindow groupWindow
}

type groupWindow struct {
	rows               rowcontainer.MemRowContainer
	iter               rowcontainer.RowIterator
	memMonitor         *mon.BytesMonitor
	groupWindowValue   int
	groupWindowColID   int32
	groupWindowTsColID int32

	stateWindowHelper   StateWindowHelper
	countWindowHelper   CountWindowHelper
	timeWindowHelper    TimeWindowHelper
	sessionWindowHelper SessionWindowHelper
	startFlag           bool
}

// StateWindowHelper implement state_window.
type StateWindowHelper struct {
	lastDatum  sqlbase.EncDatum
	datumAlloc sqlbase.DatumAlloc
	IgnoreFlag bool
}

// SessionWindowHelper implement session_window.
type SessionWindowHelper struct {
	tsTimeStamp   tree.DTimestamp
	tsTimeStampTZ tree.DTimestampTZ
	dur           time.Duration
}

// CountWindowHelper implement count_window.
type CountWindowHelper struct {
	// countValue represents the number of current group.
	countValue        int
	windowNum         int
	slidingWindowSize int
}

// TimeWindowHelper implement time_window.
type TimeWindowHelper struct {
	tsTimeStampStart   tree.DTimestamp
	tsTimeStampEnd     tree.DTimestamp
	WindowEnd          tree.DTimestamp
	WindowBuckt        tree.DTimestamp
	tsTimeStampStartTZ tree.DTimestampTZ
	tsTimeStampEndTZ   tree.DTimestampTZ
	WindowEndTZ        tree.DTimestampTZ
	WindowBucktTZ      tree.DTimestampTZ
	noNeedReCompute    bool
	//read for read rows,delete later
	noMatch  bool
	reading  bool
	hasFirst bool
}

func (gw *groupWindow) Close(ctx context.Context) {
	gw.rows.Close(ctx)
	gw.memMonitor.Stop(ctx)
}

func (gw *groupWindow) CheckAndGetWindowDatum(
	typ []types.T, flowCtx *execinfra.FlowCtx, row *sqlbase.EncDatumRow,
) error {
	switch flowCtx.EvalCtx.GroupWindow.GroupWindowFunc {
	case tree.StateWindow:
		if *row == nil {
			return nil
		}
		return gw.handleStateWindow(typ, flowCtx.EvalCtx, *row)
	case tree.EventWindow:
		if *row == nil {
			return nil
		}
		return gw.handleEventWindow(typ, flowCtx.EvalCtx, *row)
	case tree.CountWindow:
		// optimize count_window when count_val == sliding_value.
		if !flowCtx.EvalCtx.GroupWindow.CountWindowHelper.IsSlide {
			return gw.handelSimpleCountWindow(typ, *row, flowCtx.EvalCtx.GroupWindow.CountWindowHelper.WindowNum)
		}
		if *row == nil {
			return gw.handleCountWindowForRemainingValue(typ, row)
		}
		return gw.handleCountWindow(typ, flowCtx, *row)
	case tree.TimeWindow:
		if flowCtx.EvalCtx.GroupWindow.TimeWindowHelper.IsSlide {
			if *row != nil {
				return gw.handleTimeWindowForRemainingValue(typ, flowCtx, *row)
			}
			gw.timeWindowHelper.reading = false
			if flowCtx.EvalCtx.GroupWindow.TimeWindowHelper.IfTZ {
				return gw.handleTimeTZWindowWithSlide(typ, flowCtx.EvalCtx, row)
			}
			return gw.handleTimeWindowWithSlide(typ, flowCtx.EvalCtx, row)
		}
		if *row == nil {
			return nil
		}
		return gw.handleTimeWindowWithNoSlide(typ, flowCtx.EvalCtx, *row)

	case tree.SessionWindow:
		if *row == nil {
			return nil
		}
		return gw.handleSessionWindow(typ, flowCtx.EvalCtx, *row)
	}
	return nil
}

func (gw *groupWindow) handleSessionWindow(
	typ []types.T, evalCtx *tree.EvalContext, row sqlbase.EncDatumRow,
) (err error) {
	switch v := row[gw.groupWindowColID].Datum.(type) {
	case *tree.DTimestampTZ:
		if gw.groupWindowValue == 0 {
			gw.sessionWindowHelper.tsTimeStampTZ = *v
			gw.sessionWindowHelper.dur = evalCtx.GroupWindow.SessionWindowHelper.Dur
			gw.groupWindowValue++
			row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
			return nil
		}

		boundary := &tree.DTimestampTZ{Time: gw.sessionWindowHelper.tsTimeStampTZ.Time.Add(gw.sessionWindowHelper.dur)}
		if v.Compare(evalCtx, boundary) > 0 {
			gw.groupWindowValue++
		}
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		gw.sessionWindowHelper.tsTimeStampTZ = *v
		return nil
	case *tree.DTimestamp:
		if gw.groupWindowValue == 0 {
			gw.sessionWindowHelper.tsTimeStamp = *v
			gw.sessionWindowHelper.dur = evalCtx.GroupWindow.SessionWindowHelper.Dur
			gw.groupWindowValue++
			row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
			return nil
		}
		boundary := &tree.DTimestamp{Time: gw.sessionWindowHelper.tsTimeStampTZ.Time.Add(gw.sessionWindowHelper.dur)}
		if v.Compare(evalCtx, boundary) > 0 {
			gw.groupWindowValue++
		}
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		gw.sessionWindowHelper.tsTimeStamp = *v
		return nil
	default:
		return errors.New("first arg should be timestamp or timestamptz")
	}
}

func (gw *groupWindow) handelSimpleCountWindow(
	typ []types.T, row sqlbase.EncDatumRow, target int,
) (err error) {
	if row == nil {
		return nil
	}
	if gw.countWindowHelper.countValue%target == 0 {
		gw.groupWindowValue++
	}
	gw.countWindowHelper.countValue++
	row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	return nil
}

// handleCountWindow handle count_window when row == nil.
func (gw *groupWindow) handleCountWindowForRemainingValue(
	typ []types.T, row *sqlbase.EncDatumRow,
) (err error) {
	if gw.rows.Len() > 0 {
		ok, err := gw.iter.Valid()
		if err != nil {
			return err
		}
		// when iter traverse completed or Meet the quantity requirements,
		// delete the first slidingWindowSize elements in gw.rows.
		if !ok || gw.countWindowHelper.countValue == gw.countWindowHelper.windowNum {
			// pop
			for i := 0; gw.rows.Len() > 0 && i < gw.countWindowHelper.slidingWindowSize; i++ {
				gw.rows.PopFirst()
			}
			if gw.rows.Len() == 0 {
				return nil
			}
			gw.iter.Rewind()
			gw.countWindowHelper.countValue = 0
			gw.groupWindowValue++
		}
		row1, err := gw.iter.Row()
		if err != nil {
			return err
		}
		gw.iter.Next()
		row1[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		*row = make(sqlbase.EncDatumRow, len(row1))
		copy(*row, row1)
		gw.countWindowHelper.countValue++
		return nil
	}
	return nil
}

// handleCountWindow handle count_window when row != nil
func (gw *groupWindow) handleCountWindow(
	typ []types.T, flowCtx *execinfra.FlowCtx, row sqlbase.EncDatumRow,
) (err error) {
	if gw.memMonitor == nil {
		gw.memMonitor = execinfra.NewLimitedMonitor(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, flowCtx.Cfg, "group-window-func")
		gw.rows.InitWithMon(
			nil /* ordering */, typ, flowCtx.EvalCtx, gw.memMonitor, 0, /* rowCapacity */
		)
		gw.countWindowHelper = CountWindowHelper{
			countValue:        0,
			windowNum:         flowCtx.EvalCtx.GroupWindow.CountWindowHelper.WindowNum,
			slidingWindowSize: flowCtx.EvalCtx.GroupWindow.CountWindowHelper.SlidingWindowSize,
		}
		gw.groupWindowValue++
		gw.iter = gw.rows.NewIterator(flowCtx.EvalCtx.Ctx())
		gw.iter.Rewind()
	}

	if err = gw.rows.AddRow(flowCtx.EvalCtx.Ctx(), row); err != nil {
		return err
	}

	if gw.countWindowHelper.countValue == gw.countWindowHelper.windowNum {
		gw.groupWindowValue++
		gw.countWindowHelper.countValue = 0
		// Handling useless rows
		for i := 0; i < gw.countWindowHelper.slidingWindowSize; i++ {
			gw.rows.PopFirst()
		}
		gw.iter.Rewind()
	}

	row1, err := gw.iter.Row()
	if err != nil {
		return err
	}
	gw.iter.Next()
	row1[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	copy(row, row1)
	gw.countWindowHelper.countValue++
	return nil
}

func (gw *groupWindow) handleTimeWindowWithNoSlide(
	typ []types.T, evalCtx *tree.EvalContext, row sqlbase.EncDatumRow,
) (err error) {
	switch v := row[gw.groupWindowColID].Datum.(type) {
	case *tree.DTimestampTZ:
		var newTimeDur *tree.DTimestampTZ
		if evalCtx.GroupWindow.TimeWindowHelper.Duration.Days != 0 {
			oldTimeUnix := v.Time.Unix() * 1000
			oldTimeInterval := ((evalCtx.GroupWindow.TimeWindowHelper.Duration.Days) * int64(time.Hour) * 24) / 1000000
			oldTimeTrun := oldTimeUnix / oldTimeInterval
			oldTimeMul := (evalCtx.GroupWindow.TimeWindowHelper.Duration.Days) * int64(time.Hour) * 24
			result := oldTimeTrun * oldTimeMul
			sts := timeutil.Unix(0, result)
			newTimeDur = tree.MakeDTimestampTZ(sts, 0)
		} else {
			newTimeDur = tree.MakeDTimestampTZ(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
		}

		if !newTimeDur.Time.Equal(gw.timeWindowHelper.tsTimeStampStartTZ.Time) {
			gw.timeWindowHelper.tsTimeStampStartTZ = *newTimeDur
			gw.groupWindowValue++
			if gw.groupWindowTsColID >= 0 {
				row[gw.groupWindowTsColID].Datum = newTimeDur
			}
		} else {
			durTime := duration.Add(newTimeDur.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration)
			newTime := &tree.DTimestampTZ{Time: durTime}
			if gw.groupWindowTsColID >= 0 {
				row[gw.groupWindowTsColID].Datum = newTime
			}
		}
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	case *tree.DTimestamp:
		var newTimeDur *tree.DTimestamp
		if evalCtx.GroupWindow.TimeWindowHelper.Duration.Days != 0 {
			oldTimeUnix := v.Time.Unix() * 1000
			oldTimeInterval := ((evalCtx.GroupWindow.TimeWindowHelper.Duration.Days) * int64(time.Hour) * 24) / 1000000
			oldTimeTrun := oldTimeUnix / oldTimeInterval
			oldTimeMul := (evalCtx.GroupWindow.TimeWindowHelper.Duration.Days) * int64(time.Hour) * 24
			result := oldTimeTrun * oldTimeMul
			sts := timeutil.Unix(0, result)
			newTimeDur = tree.MakeDTimestamp(sts, 0)
		} else {
			newTimeDur = tree.MakeDTimestamp(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
		}

		if !newTimeDur.Time.Equal(gw.timeWindowHelper.tsTimeStampStart.Time) {
			gw.timeWindowHelper.tsTimeStampStart = *newTimeDur
			gw.groupWindowValue++
			if gw.groupWindowTsColID >= 0 {
				row[gw.groupWindowTsColID].Datum = newTimeDur
			}
		} else {
			durTime := duration.Add(newTimeDur.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration)
			newTime := &tree.DTimestamp{Time: durTime}
			if gw.groupWindowTsColID >= 0 {
				row[gw.groupWindowTsColID].Datum = newTime
			}
		}
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	default:
		return errors.New("first arg should be timestamp or timestamptz")
	}
	return nil
}

func (gw *groupWindow) handleTimeTZWindowWithSlide(
	typ []types.T, evalCtx *tree.EvalContext, row *sqlbase.EncDatumRow,
) (err error) {
	var row1 sqlbase.EncDatumRow
	ok, err := gw.iter.Valid()
	if err != nil {
		return err
	}
	gw.timeWindowHelper.noMatch = false
	if !ok {
		newStart := duration.Add(gw.timeWindowHelper.tsTimeStampStartTZ.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Duration)
		if tree.MakeDTimestampTZ(newStart, 0).Compare(evalCtx, &gw.timeWindowHelper.WindowEndTZ) < 1 {
			gw.groupWindowValue++
			gw.timeWindowHelper.tsTimeStampStartTZ = *tree.MakeDTimestampTZ(newStart, 0)
			gw.timeWindowHelper.tsTimeStampEndTZ = *tree.MakeDTimestampTZ(duration.Add(gw.timeWindowHelper.tsTimeStampStartTZ.Time,
				evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
			gw.timeWindowHelper.noNeedReCompute = true
			gw.timeWindowHelper.hasFirst = true
		} else {
			gw.timeWindowHelper.noNeedReCompute = false
		}
		gw.iter.Rewind()
		ok, err = gw.iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	row1, err = gw.iter.Row()
	if err != nil {
		return err
	}

	v, _ := row1[gw.groupWindowColID].Datum.(*tree.DTimestampTZ)
	var startTimeDur, endTimeDur, newTimeDur *tree.DTimestampTZ

	newTimeDur = tree.MakeDTimestampTZ(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)

	for !gw.timeWindowHelper.noNeedReCompute {
		if !newTimeDur.Time.After(gw.timeWindowHelper.WindowBucktTZ.Time) {
			gw.rows.PopFirst()
			ok, err = gw.iter.Valid()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			gw.iter.Rewind()
			row1, err = gw.iter.Row()
			if err != nil {
				return err
			}
			v, _ = row1[gw.groupWindowColID].Datum.(*tree.DTimestampTZ)
			newTimeDur = tree.MakeDTimestampTZ(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
		} else {
			if evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Days != 0 || evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Nanos() != 0 {
				startTimeDur = tree.MakeDTimestampTZ(getNewTimeForWin(v.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
			} else {
				startTimeDur = tree.MakeDTimestampTZ(duration.Subtract(duration.Add(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime,
					time.UTC), evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Duration), evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
			}
			if startTimeDur.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampStartTZ) != 1 {
				gw.rows.PopFirst()
				ok, err = gw.iter.Valid()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				gw.iter.Rewind()
				row1, err = gw.iter.Row()
				if err != nil {
					return err
				}
				v, _ = row1[gw.groupWindowColID].Datum.(*tree.DTimestampTZ)
				newTimeDur = tree.MakeDTimestampTZ(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
				continue
			}
			gw.timeWindowHelper.WindowBucktTZ = *newTimeDur
			endTimeDur = tree.MakeDTimestampTZ(duration.Add(startTimeDur.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)

			gw.timeWindowHelper.noNeedReCompute = true
			gw.timeWindowHelper.tsTimeStampStartTZ = *startTimeDur
			gw.timeWindowHelper.tsTimeStampEndTZ = *endTimeDur
			gw.timeWindowHelper.WindowEndTZ = *endTimeDur
			gw.groupWindowValue++
			gw.timeWindowHelper.hasFirst = true
		}
	}

	gw.iter.Next()
	if v.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampStartTZ) > -1 && v.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampEndTZ) == -1 {
		row1[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	} else {
		gw.timeWindowHelper.noMatch = true
		return nil
	}

	endTimeDur = tree.MakeDTimestampTZ(duration.Add(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
	gw.timeWindowHelper.WindowEndTZ = *endTimeDur

	if !gw.timeWindowHelper.hasFirst {
		newTimeEnd := tree.MakeDTimestampTZ(gw.timeWindowHelper.tsTimeStampEndTZ.Time, 0)
		if gw.groupWindowTsColID >= 0 {
			row1[gw.groupWindowTsColID].Datum = newTimeEnd
		}
	} else {
		newTimeStart := tree.MakeDTimestampTZ(gw.timeWindowHelper.tsTimeStampStartTZ.Time, 0)
		if gw.groupWindowTsColID >= 0 {
			row1[gw.groupWindowTsColID].Datum = newTimeStart
		}
	}
	gw.timeWindowHelper.hasFirst = false

	for i := 0; i < len(row1); i++ {
		*row = append(*row, row1[i])
	}
	return nil
}

func (gw *groupWindow) handleTimeWindowWithSlide(
	typ []types.T, evalCtx *tree.EvalContext, row *sqlbase.EncDatumRow,
) (err error) {
	var row1 sqlbase.EncDatumRow
	ok, err := gw.iter.Valid()
	if err != nil {
		return err
	}
	gw.timeWindowHelper.noMatch = false
	if !ok {
		newStart := duration.Add(gw.timeWindowHelper.tsTimeStampStart.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Duration)
		if tree.MakeDTimestamp(newStart, 0).Compare(evalCtx, &gw.timeWindowHelper.WindowEnd) < 1 {
			gw.groupWindowValue++
			gw.timeWindowHelper.tsTimeStampStart = *tree.MakeDTimestamp(newStart, 0)
			gw.timeWindowHelper.tsTimeStampEnd = *tree.MakeDTimestamp(duration.Add(gw.timeWindowHelper.tsTimeStampStart.Time,
				evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
			gw.timeWindowHelper.noNeedReCompute = true
			gw.timeWindowHelper.hasFirst = true
		} else {
			gw.timeWindowHelper.noNeedReCompute = false
		}
		gw.iter.Rewind()
		ok, err = gw.iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	row1, err = gw.iter.Row()
	if err != nil {
		return err
	}

	v, _ := row1[gw.groupWindowColID].Datum.(*tree.DTimestamp)
	var startTimeDur, endTimeDur, newTimeDur *tree.DTimestamp

	newTimeDur = tree.MakeDTimestamp(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)

	for !gw.timeWindowHelper.noNeedReCompute {
		if !newTimeDur.Time.After(gw.timeWindowHelper.WindowBuckt.Time) {
			gw.rows.PopFirst()
			ok, err = gw.iter.Valid()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			gw.iter.Rewind()
			row1, err = gw.iter.Row()
			if err != nil {
				return err
			}
			v, _ = row1[gw.groupWindowColID].Datum.(*tree.DTimestamp)
			newTimeDur = tree.MakeDTimestamp(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
		} else {
			if evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Days != 0 || evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Nanos() != 0 {
				startTimeDur = tree.MakeDTimestamp(getNewTimeForWin(v.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
			} else {
				startTimeDur = tree.MakeDTimestamp(duration.Subtract(duration.Add(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.SlidingTime,
					time.UTC), evalCtx.GroupWindow.TimeWindowHelper.SlidingTime.Duration), evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
			}
			if startTimeDur.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampStart) != 1 {
				gw.rows.PopFirst()
				ok, err = gw.iter.Valid()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				gw.iter.Rewind()
				row1, err = gw.iter.Row()
				if err != nil {
					return err
				}
				v, _ = row1[gw.groupWindowColID].Datum.(*tree.DTimestamp)
				newTimeDur = tree.MakeDTimestamp(getNewTime(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration, time.UTC), 0)
				continue
			}
			gw.timeWindowHelper.WindowBuckt = *newTimeDur
			endTimeDur = tree.MakeDTimestamp(duration.Add(startTimeDur.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)

			gw.timeWindowHelper.noNeedReCompute = true
			gw.timeWindowHelper.tsTimeStampStart = *startTimeDur
			gw.timeWindowHelper.tsTimeStampEnd = *endTimeDur
			gw.timeWindowHelper.WindowEnd = *endTimeDur
			gw.groupWindowValue++
			gw.timeWindowHelper.hasFirst = true
		}
	}

	gw.iter.Next()
	if v.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampStart) > -1 && v.Compare(evalCtx, &gw.timeWindowHelper.tsTimeStampEnd) == -1 {
		row1[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	} else {
		gw.timeWindowHelper.noMatch = true
		return nil
	}

	endTimeDur = tree.MakeDTimestamp(duration.Add(v.Time, evalCtx.GroupWindow.TimeWindowHelper.Duration.Duration), 0)
	gw.timeWindowHelper.WindowEnd = *endTimeDur

	if !gw.timeWindowHelper.hasFirst {
		newTimeEnd := tree.MakeDTimestamp(gw.timeWindowHelper.tsTimeStampEnd.Time, 0)
		if gw.groupWindowTsColID >= 0 {
			row1[gw.groupWindowTsColID].Datum = newTimeEnd
		}
	} else {
		newTimeStart := tree.MakeDTimestamp(gw.timeWindowHelper.tsTimeStampStart.Time, 0)
		if gw.groupWindowTsColID >= 0 {
			row1[gw.groupWindowTsColID].Datum = newTimeStart
		}
	}
	gw.timeWindowHelper.hasFirst = false

	for i := 0; i < len(row1); i++ {
		*row = append(*row, row1[i])
	}
	return nil
}

func (gw *groupWindow) handleTimeWindowForRemainingValue(
	typ []types.T, flowCtx *execinfra.FlowCtx, row sqlbase.EncDatumRow,
) (err error) {
	if gw.memMonitor == nil {
		gw.memMonitor = execinfra.NewLimitedMonitor(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, flowCtx.Cfg, "group-window-func")
		gw.rows.InitWithMon(
			nil /* ordering */, typ, flowCtx.EvalCtx, gw.memMonitor, 0, /* rowCapacity */
		)
		gw.iter = gw.rows.NewIterator(context.Background())
		gw.iter.Rewind()
		gw.timeWindowHelper.reading = true
	}

	if err = gw.rows.AddRow(context.Background(), row); err != nil {
		return err
	}
	return nil
}

func (gw *groupWindow) handleStateWindow(
	typ []types.T, evalCtx *tree.EvalContext, row sqlbase.EncDatumRow,
) error {
	if _, ok := row[gw.groupWindowColID].Datum.(tree.DNullExtern); ok {
		gw.stateWindowHelper.IgnoreFlag = true
		return nil
	}
	if gw.groupWindowValue == 0 {
		gw.groupWindowValue = gw.groupWindowValue + 1
		gw.stateWindowHelper.lastDatum = row[gw.groupWindowColID]
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		return nil
	}
	res, err := gw.stateWindowHelper.lastDatum.Compare(&typ[gw.groupWindowColID], &gw.stateWindowHelper.datumAlloc, evalCtx, &row[gw.groupWindowColID])
	if err != nil {
		return err
	}
	if res != 0 {
		gw.groupWindowValue = gw.groupWindowValue + 1
		gw.stateWindowHelper.lastDatum = row[gw.groupWindowColID]
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		return nil
	}
	row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
	return nil
}

func (gw *groupWindow) handleEventWindow(
	typ []types.T, evalCtx *tree.EvalContext, row sqlbase.EncDatumRow,
) error {
	if gw.startFlag {
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		if evalCtx.GroupWindow.EventWindowHelper.EndFlag {
			gw.startFlag = false
		}
		return nil
	}

	if evalCtx.GroupWindow.EventWindowHelper.StartFlag {
		gw.startFlag = true
		gw.groupWindowValue = gw.groupWindowValue + 1
		row[gw.groupWindowColID] = encodeDatum(gw.groupWindowValue, typ[gw.groupWindowColID])
		if evalCtx.GroupWindow.EventWindowHelper.EndFlag {
			gw.startFlag = false
		}
		return nil
	}
	evalCtx.GroupWindow.EventWindowHelper.IgnoreFlag = true
	//row[gw.groupWindowColId] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(-1))}

	return nil
}

// aggGapFillState represents gapfill's state.
type aggGapFillState int

const (
	// gapFillInit represents that no row data that meets the requirements has appeared yet.
	gapFillInit aggGapFillState = iota
	// gapFillStart represents that there is already a row of data that meets the requirements.
	gapFillStart
	// gapFillEnd represents that the virtual rows between two actual rows have been filled,
	// now begin processing last row.
	gapFillEnd
)

// gapfilltype is used to store data for functions of time_bucket_gapfill and interpolate.
// It records previous data record and keeps track of gapfilling status along the way
// while we keep reading the next rows.
type gapfilltype struct {
	// prevtime is the timestamp of the previous row
	prevtime time.Time
	// prevgaptime is the last timestamp that we used for gapfilling.
	// we use this value to keep track of the gapfilling progress.
	prevgaptime time.Time
	// endgapfilling marks the end of gapfilling if true. We need to emit the gapfillingrow
	// at the end of gapfilling
	endgapfilling bool
	// gapfillingrow records the current row if gapfilling is needed. This row is saved temporarily
	// and emitted after gapfilling is finished.
	gapfillingrow sqlbase.EncDatumRow
	// gapfillingbucket records the current buckets if gapfilling is needed.
	gapfillingbucket aggregateFuncs
	// linearPos is used for linear methods. It keeps track of the position
	// of a linear interpolation.
	linearPos int
	// linearGap is used for linear methods.  It is the total distance from previous
	// timestamp to current timestamp.
	// linearGap = (int)((currTime-prevTime)/t.Timebucket + 1)
	// The linear interpolation equation is val := ((nextval-prevval)*pos/gap + prevval)
	linearGap int
	// firstInterval represents the first interval.
	// fix when the data timestamp is 0, the first interval has not been added row.
	firstInterval bool
	gapFillState  aggGapFillState
	// groupTimeIndex represents tb's index in Aggregations.
	// select time_bucket_gapfill(k_timestamp, '10W') as tb,t15 from test_select_timebucket_gapfill.tb group by tb order by tb;
	groupTimeIndex int
	// anyNotNUllNum represents the number of columns in group by.
	anyNotNUllNum int
	// gapFillGroupDatum represents the out datum in aggregations corresponding to the group by.
	// for example:
	// select time_bucket_gapfill(k_timestamp, '10W') as tb,t15,t16
	// from test_select_timebucket_gapfill.tb group by tb,t15,t16 order by tb,t15 limit 10;
	// gapFillGroupDatum is t15, t16 in group by.
	gapFillGroupDatum []tree.Datum
}

type imputationtype struct {
	// prevValue is used for imputation for prev and linear methods
	prevValue tree.Datum
	// prevValueTmp is used to facilitate the assignment of prevValue
	prevValueTmp tree.Datum
	// nextValue is used for imputation for next and linear methods
	nextValue tree.Datum
}

func (it *imputationtype) GetPrevValueFloat() (float64, error) {
	switch it.prevValue.(type) {
	case *tree.DInt:
		return float64(*it.prevValue.(*tree.DInt)), nil
	case *tree.DFloat:
		return float64(*it.prevValue.(*tree.DFloat)), nil
	case *tree.DDecimal:
		val, err := it.prevValue.(*tree.DDecimal).Float64()
		return val, err
	}
	return 0, errors.Newf("Unexpected type(%T) for value %v", it.prevValue, it.prevValue)
}

func (it *imputationtype) GetNextValueFloat() (float64, error) {
	switch it.nextValue.(type) {
	case *tree.DInt:
		return float64(*it.nextValue.(*tree.DInt)), nil
	case *tree.DFloat:
		return float64(*it.nextValue.(*tree.DFloat)), nil
	case *tree.DDecimal:
		val, err := it.nextValue.(*tree.DDecimal).Float64()
		return val, err
	}
	return 0, errors.Newf("Unexpected type(%T) for value %v", it.nextValue, it.nextValue)
}

// init initializes the aggregatorBase.
//
// trailingMetaCallback is passed as part of ProcStateOpts; the inputs to drain
// are in aggregatorBase.
func (ag *aggregatorBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	trailingMetaCallback func(context.Context) []execinfrapb.ProducerMetadata,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "aggregator-mem")
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = newInputStatCollector(input)
		ag.FinishTrace = ag.outputStatsToTrace
	}
	ag.input = input
	ag.isScalar = spec.IsScalar()
	ag.groupCols = spec.GroupCols
	ag.orderedGroupCols = spec.OrderedGroupCols
	ag.timeBucketGapFillColID = spec.TimeBucketGapFillColId
	ag.aggregations = spec.Aggregations
	groupTimeIndex := 0
	anyNotNUllNum := 0
	gapFillGroup := make([]int, 0)
	for i, v := range spec.Aggregations {
		if v.Func.String() == "ANY_NOT_NULL" {
			anyNotNUllNum++
			if v.ColIdx[0] == uint32(spec.TimeBucketGapFillColId) {
				groupTimeIndex = i
			} else {
				gapFillGroup = append(gapFillGroup, i)
			}
		}
	}
	ag.funcs = make([]*aggregateFuncHolder, len(spec.Aggregations))
	ag.outputTypes = make([]types.T, len(spec.Aggregations))
	ag.row = make(sqlbase.EncDatumRow, len(spec.Aggregations))
	ag.bucketsAcc = memMonitor.MakeBoundAccount()
	ag.arena = stringarena.Make(&ag.bucketsAcc)
	ag.aggFuncsAcc = memMonitor.MakeBoundAccount()
	ag.groupWindow = groupWindow{
		groupWindowValue:   0,
		groupWindowColID:   spec.GroupWindowId,
		groupWindowTsColID: spec.Group_WindowTscolid,
	}
	ag.gapfill = gapfilltype{
		groupTimeIndex:   groupTimeIndex,
		anyNotNUllNum:    anyNotNUllNum,
		firstInterval:    true,
		prevgaptime:      time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		endgapfilling:    false, // marks the end of gapfilling if true
		gapfillingrow:    nil,
		gapfillingbucket: nil,
		linearPos:        0,
		linearGap:        0,
		gapFillState:     gapFillInit,
	}
	ag.gapfill.gapFillGroupDatum = make([]tree.Datum, len(gapFillGroup))
	ag.hasTimeBucketGapFill = spec.HasTimeBucketGapFill
	ag.scalarGroupByWithSumInt = spec.ScalarGroupByWithSumInt
	ag.imputation = make([]imputationtype, len(spec.Aggregations))
	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	ag.inputTypes = input.OutputTypes()
	interpolateIndex := make([]int, 0)
	for i, aggInfo := range spec.Aggregations {
		if aggInfo.FilterColIdx != nil {
			col := *aggInfo.FilterColIdx
			if col >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("FilterColIdx out of range (%d)", col)
			}
			t := ag.inputTypes[col].Family()
			if t != types.BoolFamily && t != types.UnknownFamily {
				return errors.Errorf(
					"filter column %d must be of boolean type, not %s", *aggInfo.FilterColIdx, t,
				)
			}
		}
		argTypes := make([]types.T, len(aggInfo.ColIdx)+len(aggInfo.Arguments))
		for j, c := range aggInfo.ColIdx {
			if c >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			}
			argTypes[j] = ag.inputTypes[c]
		}

		arguments := make(tree.Datums, len(aggInfo.Arguments))
		for j, argument := range aggInfo.Arguments {
			h := execinfra.ExprHelper{}
			// Pass nil types and row - there are no variables in these expressions.
			if err := h.Init(argument, nil /* types */, flowCtx.EvalCtx); err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			d, err := h.Eval(nil /* row */)
			if err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			argTypes[len(aggInfo.ColIdx)+j] = *d.ResolvedType()
			if err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			arguments[j] = d
		}

		aggConstructor, retType, err := execinfrapb.GetAggregateInfo(aggInfo.Func, argTypes...)
		if err != nil {
			return err
		}
		if aggInfo.Func == execinfrapb.AggregatorSpec_INTERPOLATE {
			interpolateIndex = append(interpolateIndex, i)
		}

		ag.funcs[i] = ag.newAggregateFuncHolder(aggConstructor, arguments)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}

		ag.outputTypes[i] = *retType
		ag.imputation[i] = imputationtype{
			prevValue:    tree.DNull,
			prevValueTmp: tree.DNull,
			nextValue:    tree.DNull}
	}
	// interpolate func's return type should use return type of internal aggregation function.
	for _, ix := range interpolateIndex {
		ag.outputTypes[ix] = ag.outputTypes[ix+1]
	}

	return ag.ProcessorBase.Init(
		self, post, ag.outputTypes, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			InputsToDrain:        []execinfra.RowSource{ag.input},
			TrailingMetaCallback: trailingMetaCallback,
		},
	)
}

var _ execinfrapb.DistSQLSpanStats = &AggregatorStats{}

const aggregatorTagPrefix = "aggregator."

// Stats implements the SpanStats interface.
func (as *AggregatorStats) Stats() map[string]string {
	inputStatsMap := as.InputStats.Stats(aggregatorTagPrefix)
	inputStatsMap[aggregatorTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(as.MaxAllocatedMem)
	return inputStatsMap
}

// TsStats is stats of analyse in time series
func (as *AggregatorStats) TsStats() map[int32]map[string]string {
	return nil
}

// GetSpanStatsType check type of spanStats
func (as *AggregatorStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (as *AggregatorStats) StatsForQueryPlan() []string {
	stats := as.InputStats.StatsForQueryPlan("" /* prefix */)

	if as.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(as.MaxAllocatedMem)))
	}

	return stats
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (as *AggregatorStats) TsStatsForQueryPlan() map[int32][]string {
	stats := as.InputStats.StatsForQueryPlan("" /* prefix */)
	stats = append(stats,
		fmt.Sprintf("%s:%d", outputRowsQueryPlanSuffix, as.OutputRowNum))
	if as.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(as.MaxAllocatedMem)))
	}
	mapStats := make(map[int32][]string, 0)
	mapStats[0] = stats

	return mapStats
}

func (ag *aggregatorBase) outputStatsToTrace() {
	is, ok := getInputStats(ag.FlowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp,
			&AggregatorStats{
				InputStats:      is,
				MaxAllocatedMem: ag.MemMonitor.MaximumBytes(),
			},
		)
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) ChildCount(verbose bool) int {
	if _, ok := ag.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ag.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to aggregatorBase is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}

const (
	// hashAggregatorBucketsInitialLen is a guess on how many "items" the
	// 'buckets' map of hashAggregator has the capacity for initially.
	hashAggregatorBucketsInitialLen = 8
	// hashAggregatorSizeOfBucketsItem is a guess on how much space (in bytes)
	// each item added to 'buckets' map of hashAggregator takes up in the map
	// (i.e. it is memory internal to the map, orthogonal to "key-value" pair
	// that we're adding to the map).
	hashAggregatorSizeOfBucketsItem = 64
)

// hashAggregator is a specialization of aggregatorBase that must keep track of
// multiple grouping buckets at a time.
type hashAggregator struct {
	aggregatorBase

	// buckets is used during the accumulation phase to track the bucket keys
	// that have been seen. After accumulation, the keys are extracted into
	// bucketsIter for iteration.
	buckets     map[string]aggregateFuncs
	bucketsIter []string
	// bucketsLenGrowThreshold is the threshold which, when reached by the
	// number of items in 'buckets', will trigger the update to memory
	// accounting. It will start out at hashAggregatorBucketsInitialLen and
	// then will be doubling in size.
	bucketsLenGrowThreshold int
	// alreadyAccountedFor tracks the number of items in 'buckets' memory for
	// which we have already accounted for.
	alreadyAccountedFor int
}

// orderedAggregator is a specialization of aggregatorBase that only needs to
// keep track of a single grouping bucket at a time.
type orderedAggregator struct {
	aggregatorBase

	// bucket is used during the accumulation phase to aggregate results.
	bucket aggregateFuncs
}

var _ execinfra.Processor = &hashAggregator{}
var _ execinfra.RowSource = &hashAggregator{}
var _ execinfra.OpNode = &hashAggregator{}

const hashAggregatorProcName = "hash aggregator"

var _ execinfra.Processor = &orderedAggregator{}
var _ execinfra.RowSource = &orderedAggregator{}
var _ execinfra.OpNode = &orderedAggregator{}

const orderedAggregatorProcName = "ordered aggregator"

// aggregatorState represents the state of the processor.
type aggregatorState int

const (
	aggStateUnknown aggregatorState = iota
	// aggAccumulating means that rows are being read from the input and used to
	// compute intermediary aggregation results.
	aggAccumulating
	// aggEmittingRows means that accumulation has finished and rows are being
	// sent to the output.
	aggEmittingRows
)

func newAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	if spec.IsRowCount() {
		return newCountAggregator(flowCtx, processorID, input, post, output)
	}
	if len(spec.OrderedGroupCols) == len(spec.GroupCols) {
		return newOrderedAggregator(flowCtx, processorID, spec, input, post, output)
	}

	ag := &hashAggregator{
		buckets:                 make(map[string]aggregateFuncs),
		bucketsLenGrowThreshold: hashAggregatorBucketsInitialLen,
	}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func(context.Context) []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

func newOrderedAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*orderedAggregator, error) {
	ag := &orderedAggregator{}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func(context.Context) []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

// Start is part of the RowSource interface.
func (ag *hashAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, hashAggregatorProcName)
}

// Start is part of the RowSource interface.
func (ag *orderedAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, orderedAggregatorProcName)
}

func (ag *aggregatorBase) start(ctx context.Context, procName string) context.Context {
	ag.input.Start(ctx)
	ctx = ag.StartInternal(ctx, procName)
	ag.cancelChecker = sqlbase.NewCancelChecker(ctx)
	ag.runningState = aggAccumulating
	return ctx
}

func (ag *hashAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.PbCtx(), 2, "exiting aggregator")
		// If we have started emitting rows, bucketsIter will represent which
		// buckets are still open, since buckets are closed once their results are
		// emitted.
		if ag.bucketsIter == nil {
			for _, bucket := range ag.buckets {
				bucket.close(ag.PbCtx())
			}
		} else {
			for _, bucket := range ag.bucketsIter {
				ag.buckets[bucket].close(ag.PbCtx())
			}
		}
		// Make sure to release any remaining memory under 'buckets'.
		ag.buckets = nil
		// Note that we should be closing accounts only after closing all the
		// buckets since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the buckets.
		ag.bucketsAcc.Close(ag.PbCtx())
		ag.aggFuncsAcc.Close(ag.PbCtx())
		ag.MemMonitor.Stop(ag.PbCtx())
	}
}

func (ag *orderedAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.PbCtx(), 2, "exiting aggregator")
		if ag.bucket != nil {
			ag.bucket.close(ag.PbCtx())
		}
		// Note that we should be closing accounts only after closing the
		// bucket since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the bucket.
		ag.bucketsAcc.Close(ag.PbCtx())
		ag.aggFuncsAcc.Close(ag.PbCtx())
		ag.MemMonitor.Stop(ag.PbCtx())
		ag.gwClose()
	}
}

func (ag *orderedAggregator) gwClose() {
	if ag.EvalCtx == nil || ag.EvalCtx.GroupWindow == nil || ag.groupWindow.memMonitor == nil {
		return
	}
	if ag.EvalCtx.GroupWindow.GroupWindowFunc == tree.CountWindow && ag.EvalCtx.GroupWindow.CountWindowHelper.IsSlide ||
		(ag.EvalCtx.GroupWindow.GroupWindowFunc == tree.TimeWindow && ag.EvalCtx.GroupWindow.TimeWindowHelper.IsSlide) {
		ag.groupWindow.Close(ag.PbCtx())
	}
}

// matchLastOrdGroupCols takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (ag *aggregatorBase) matchLastOrdGroupCols(row sqlbase.EncDatumRow) (bool, error) {
	for _, colIdx := range ag.orderedGroupCols {
		res, err := ag.lastOrdGroupCols[colIdx].Compare(
			&ag.inputTypes[colIdx], &ag.datumAlloc, ag.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// matchLastOrdGroupColsForEvent takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (ag *aggregatorBase) matchLastOrdGroupColsForEvent(
	row sqlbase.EncDatumRow, groupWindowColID int32,
) (bool, error) {
	for _, colIdx := range ag.orderedGroupCols {
		if colIdx == uint32(groupWindowColID) {
			continue
		}
		res, err := ag.eventOrdGroupCols[colIdx].Compare(
			&ag.inputTypes[colIdx], &ag.datumAlloc, ag.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *hashAggregator) accumulateRows() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.PbCtx(), 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		bucket, err := ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		ag.buckets[""] = bucket
	}

	// Note that, for simplicity, we're ignoring the overhead of the slice of
	// strings.
	if err := ag.bucketsAcc.Grow(ag.PbCtx(), int64(len(ag.buckets))*sizeOfString); err != nil {
		ag.MoveToDraining(err)
		return aggStateUnknown, nil, nil
	}
	ag.bucketsIter = make([]string, 0, len(ag.buckets))
	for bucket := range ag.buckets {
		ag.bucketsIter = append(ag.bucketsIter, bucket)
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *orderedAggregator) accumulateRows() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}

		if haveGroupWindow(ag.EvalCtx) {
			if ag.FlowCtx.EvalCtx.GroupWindow.GroupWindowFunc == tree.EventWindow {
				if ag.eventOrdGroupCols == nil {
					ag.eventOrdGroupCols = ag.rowAlloc.CopyRow(row)
				} else {
					if row != nil {
						matched, err := ag.matchLastOrdGroupColsForEvent(row, ag.groupWindow.groupWindowColID)
						if err != nil {
							ag.MoveToDraining(err)
							return aggStateUnknown, nil, nil
						}
						if !matched {
							ag.groupWindow.startFlag = false
						}
					}
				}
			}
			if err := ag.groupWindow.CheckAndGetWindowDatum(ag.inputTypes, ag.FlowCtx, &row); err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if ag.groupWindow.stateWindowHelper.IgnoreFlag {
				ag.groupWindow.stateWindowHelper.IgnoreFlag = false
				continue
			}
			if ag.EvalCtx.GroupWindow.EventWindowHelper.IgnoreFlag {
				ag.EvalCtx.GroupWindow.EventWindowHelper.IgnoreFlag = false
				continue
			}
			if (ag.groupWindow.timeWindowHelper.reading || ag.groupWindow.timeWindowHelper.noMatch) &&
				ag.EvalCtx.GroupWindow.TimeWindowHelper.IsSlide {
				continue
			}
		}
		if row == nil {
			log.VEvent(ag.PbCtx(), 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if ag.bucket == nil && ag.isScalar {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

func haveGroupWindow(evalCtx *tree.EvalContext) bool {
	return evalCtx != nil && evalCtx.GroupWindow != nil && evalCtx.GroupWindow.GroupWindowFunc > 0
}

// getAggResults returns the new aggregatorState and the results from the
// bucket. The bucket is closed.
func (ag *aggregatorBase) getAggResults(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	defer bucket.close(ag.PbCtx())
	ag.Out.Gapfill = false
	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			result = tree.DNull
		}
		ag.outputTypes[i] = *result.ResolvedType()
		ag.row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], result)
	}
	if !ag.hasTimeBucketGapFill {
		if outRow := ag.ProcessRowHelper(ag.row); outRow != nil {
			return aggEmittingRows, outRow, nil
		}
		// We might have switched to draining, we might not have. In case we
		// haven't, aggEmittingRows is accurate. If we have, it will be ignored by
		// the caller.
		return aggEmittingRows, nil, nil
	}
	return ag.getAggResultsForTimeBucketGapFill(bucket)
}

// getAggResultsForTimeBucketGapFill perform gapfill and interpolation for agg result.
func (ag *aggregatorBase) getAggResultsForTimeBucketGapFill(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	outRow, err := ag.Out.ProcessRowWithOutLimit(ag.PbCtx(), ag.row)
	if err != nil {
		ag.MoveToDraining(err)
		return aggEmittingRows, nil, nil
	}
	if outRow == nil {
		return aggEmittingRows, nil, nil
	}
	switch ag.gapfill.gapFillState {
	case gapFillInit:
		return ag.aggGapFillInit(bucket)
	case gapFillStart:
		return ag.aggGapFillStart()
	case gapFillEnd:
		// If this is the end of gapfilling, we need to emit the last row, which was saved in imputation struct.
		// This row is original row/data
		return ag.aggGapFillEnd()
	default:
		ag.MoveToDraining(errors.Errorf("unknown gapFill State."))
		return aggEmittingRows, nil, nil
	}
}

// aggGapFillEnd handle the last row in the interval.
func (ag *aggregatorBase) aggGapFillEnd() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	ag.Out.Gapfill = true
	for _, b := range ag.gapfill.gapfillingbucket {
		switch b.(type) {
		case *(builtins.ImputationAggregate):
			ag.gapfill.linearGap = 0
			ag.gapfill.linearPos = 0
		}
	}
	if outRow := ag.ProcessRowHelper(ag.gapfill.gapfillingrow); outRow != nil {
		// The current interval has ended gapFill, switch the state to gapFillInit.
		ag.gapfill.gapFillState = gapFillInit
		ag.gapfill.gapfillingrow = nil
		ag.gapfill.gapfillingbucket = nil
		ag.Out.Gapfill = false
		return aggEmittingRows, outRow, nil
	}
	// The current interval has ended gapFill, switch the state to gapFillInit.
	ag.gapfill.gapFillState = gapFillInit
	return aggEmittingRows, nil, nil
}

// aggGapFillStart start filling new rows within the interval.
func (ag *aggregatorBase) aggGapFillStart() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// If this is in the middle of gapfilling, we use imputation structure to emit rows for the gaps.
	// row is used to store the gap filling values for aggregates in bucket. We emit the row during
	// gapfilling process. We emit ag.row for original data.
	row := make(sqlbase.EncDatumRow, len(ag.row))
	var curRowData tree.Datum
	var haveGapFilled bool
	haveGapFilled = false
	errorOut := false
	for i, b := range ag.gapfill.gapfillingbucket {
		switch t := b.(type) {
		case *(builtins.TimeBucketAggregate):
			// We check the gap between currTime and last filled timestamp value and update the
			// gapfilling status accordingly.
			newTime := ag.gapfill.prevgaptime.AddDate(0, int(t.Timebucket.Months), int(t.Timebucket.Days)).Add(time.Duration(t.Timebucket.Nanos())).UTC()
			newTimeUnix := newTime.UnixNano()
			currTime := t.Time.UnixNano()
			if !(newTimeUnix < currTime) {
				// gapFillStart ends, switch the state to gapFillEnd.
				ag.gapfill.gapFillState = gapFillEnd
				break
			}
			// increment prevgaptime by t.Timebucket
			ag.gapfill.prevgaptime = newTime
			timeTmp := newTime
			// record data in row
			ag.outputTypes[i] = *types.Timestamp
			row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestamp{Time: timeTmp})
			for j := 0; j < ag.gapfill.anyNotNUllNum; j++ {
				if ag.gapfill.groupTimeIndex == j {
					// change to the same time as the new line.
					row[j] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestamp{Time: timeTmp})
				} else {
					// when in a gapFillGroup, insert same group.
					row[j] = ag.row[j]
				}
			}
		case *(builtins.TimestamptzBucketAggregate):
			// We check the gap between currTime and last filled timestamp value and update the
			// gapfilling status accordingly.
			newTime := ag.gapfill.prevgaptime.AddDate(0, int(t.Timebucket.Months), int(t.Timebucket.Days)).Add(time.Duration(t.Timebucket.Nanos())).UTC()
			newTimeUnix := newTime.UnixNano()
			currTime := t.Time.UnixNano()
			if !(newTimeUnix < currTime) {
				// gapFillStart ends, switch the state to gapFillEnd.
				ag.gapfill.gapFillState = gapFillEnd
				break
			}
			ag.gapfill.prevgaptime = newTime
			timeTmp := newTime
			ag.outputTypes[i] = *types.TimestampTZ
			row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
			for j := 0; j < ag.gapfill.anyNotNUllNum; j++ {
				if ag.gapfill.groupTimeIndex == j {
					row[ag.gapfill.groupTimeIndex] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
				} else {
					row[j] = ag.row[j]
				}
			}
			// row[0] is a group by column
			//row[0] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
		case *(builtins.ImputationAggregate):
			imputationMethod := t.Exp
			switch imputationMethod {
			case builtins.ConstantIntMethod:
				curRowData = tree.NewDInt(tree.DInt(t.IntConstant))
			case builtins.ConstantFloatMethod:
				curRowData = tree.NewDFloat(tree.DFloat(t.FloatConstant))
			case builtins.ConstantDecimalMethod:
				curRowData = t.DecimalConstant
			case builtins.PrevMethod:
				curRowData = ag.imputation[i].prevValue
			case builtins.NextMethod:
				curRowData = ag.imputation[i].nextValue
			case builtins.LinearMethod:
				if ag.imputation[i].nextValue == tree.DNull || ag.imputation[i].prevValue == tree.DNull {
					curRowData = tree.DNull
				} else {
					nextval, _ := ag.imputation[i].GetNextValueFloat()
					prevval, _ := ag.imputation[i].GetPrevValueFloat()
					pos := float64(ag.gapfill.linearPos) //TODO: potential unnecessary mem alloc
					gap := float64(ag.gapfill.linearGap)
					val := (nextval-prevval)*pos/gap + prevval
					switch t.OriginalType {
					case types.Float:
						curRowData = tree.NewDFloat(tree.DFloat(val))
					case types.Int:
						curRowData = tree.NewDInt(tree.DInt(math.Round(val)))
					default:
						errorOut = true
					}
					if !errorOut && ag.outputTypes[i].InternalType.Family == types.DecimalFamily {
						curRowData, _ = tree.ParseDDecimal(curRowData.String())
					}
					// ag.gapfill.linearPos = ag.gapfill.linearPos + 1
					haveGapFilled = true
				}
			case builtins.NullMethod:
				curRowData = tree.DNull
			default:
				errorOut = true
			}
			if !errorOut {
				row[i] = sqlbase.DatumToEncDatum(curRowData.ResolvedType(), curRowData)
			} else {
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], tree.DNull)
			}
		// If we have a normal agg function, we fill it with null value.
		default:
			row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], tree.DNull)
		}
	}
	if haveGapFilled {
		ag.gapfill.linearPos = ag.gapfill.linearPos + 1
		haveGapFilled = false
	}
	// emit gapfilling row
	if ag.gapfill.gapFillState == gapFillStart {
		ag.Out.Gapfill = true
		if outRow := ag.ProcessRowHelper(row); outRow != nil {
			ag.Out.Gapfill = false
			return aggEmittingRows, outRow, nil
		}
	}
	return aggEmittingRows, nil, nil
}

// isGapFillGroup return true when the row and pre row is in a gapFill group.
func (ag *aggregatorBase) isGapFillGroup(bucket aggregateFuncs) bool {
	isGapFillGroup := true
	for i := 0; i < len(ag.gapfill.gapFillGroupDatum); i++ {
		result, _ := bucket[i].Result()
		if ag.gapfill.firstInterval {
			isGapFillGroup = false
		} else {
			if ag.gapfill.gapFillGroupDatum[i].Compare(ag.EvalCtx, result) != 0 {
				isGapFillGroup = false
			}
		}
		ag.gapfill.gapFillGroupDatum[i] = result
	}
	return isGapFillGroup
}

// aggGapFillInit init ag.gapfill by first real row.
func (ag *aggregatorBase) aggGapFillInit(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	row := make(sqlbase.EncDatumRow, len(ag.row))
	// traverse agg in bucket
	isGapFillGroup := ag.isGapFillGroup(bucket)
	for i, b := range bucket {
		result, err := b.Result()
		switch t := b.(type) {
		case *(builtins.TimeBucketAggregate):
			if ag.gapfill.firstInterval {
				ag.gapfill.prevtime = t.Time.AddDate(0, -int(t.Timebucket.Months), -int(t.Timebucket.Days)).Add(-time.Duration(t.Timebucket.Nanos()))
			}
			ag.gapfill.firstInterval = false
			prevTime := ag.gapfill.prevtime

			newTime := prevTime.AddDate(0, int(t.Timebucket.Months), int(t.Timebucket.Days)).Add(time.Duration(t.Timebucket.Nanos()))
			var newTimeUnix, currTime int64
			if newTime.After(time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC)) {
				newTimeUnix = newTime.Unix()
				currTime = t.Time.Unix()
			} else {
				newTimeUnix = newTime.UnixNano()
				currTime = t.Time.UnixNano()
			}
			ag.gapfill.prevtime = t.Time
			ag.gapfill.prevgaptime = newTime.AddDate(0, -int(t.Timebucket.Months), -int(t.Timebucket.Days)).Add(-time.Duration(t.Timebucket.Nanos())).UTC()
			// we check if gapfilling is needed by comparing currTime with Time in the last row
			if newTimeUnix < currTime && isGapFillGroup {
				if t.Timebucket.Months != 0 {
					ag.gapfill.linearGap = ((t.Time.Year()-newTime.Year())*12+int(t.Time.Month()-newTime.Month()))/int(t.Timebucket.Months) + 1
				} else {
					ag.gapfill.linearGap = int(t.Time.Sub(newTime)/(time.Duration(t.Timebucket.Days)*time.Hour*24+time.Duration(t.Timebucket.Nanos()))) + 1
				}
				ag.gapfill.linearPos = 1
				timeTmp := newTime
				ag.gapfill.gapfillingbucket = bucket
				ag.outputTypes[i] = *types.Timestamp
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestamp{Time: timeTmp})
				// gapFillInit ends, switch the state to gapFillStart.
				ag.gapfill.gapFillState = gapFillStart
			}
		case *(builtins.TimestamptzBucketAggregate):
			if ag.gapfill.firstInterval {
				ag.gapfill.prevtime = t.Time.AddDate(0, -int(t.Timebucket.Months), -int(t.Timebucket.Days)).Add(-time.Duration(t.Timebucket.Nanos()))
			}
			ag.gapfill.firstInterval = false
			prevTime := ag.gapfill.prevtime

			newTime := prevTime.AddDate(0, int(t.Timebucket.Months), int(t.Timebucket.Days)).Add(time.Duration(t.Timebucket.Nanos()))
			var newTimeUnix, currTime int64
			if newTime.After(time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC)) {
				newTimeUnix = newTime.Unix()
				currTime = t.Time.Unix()
			} else {
				newTimeUnix = newTime.UnixNano()
				currTime = t.Time.UnixNano()
			}

			ag.gapfill.prevtime = t.Time
			ag.gapfill.prevgaptime = newTime.AddDate(0, -int(t.Timebucket.Months), -int(t.Timebucket.Days)).Add(-time.Duration(t.Timebucket.Nanos())).UTC()
			// we check if gapfilling is needed by comparing currTime with Time in the last row
			if newTimeUnix < currTime && isGapFillGroup {
				if t.Timebucket.Months != 0 {
					ag.gapfill.linearGap = ((t.Time.Year()-newTime.Year())*12+int(t.Time.Month()-newTime.Month()))/int(t.Timebucket.Months) + 1
				} else {
					ag.gapfill.linearGap = int(t.Time.Sub(newTime)/(time.Duration(t.Timebucket.Days)*time.Hour*24+time.Duration(t.Timebucket.Nanos()))) + 1
				}
				ag.gapfill.linearPos = 1
				timeTmp := newTime
				ag.gapfill.gapfillingbucket = bucket
				ag.outputTypes[i] = *types.TimestampTZ
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
				// gapFillInit ends, switch the state to gapFillStart.
				ag.gapfill.gapFillState = gapFillStart
			}
		case *(builtins.ImputationAggregate):
			ag.imputation[i].nextValue = result
			ag.imputation[i].prevValue = ag.imputation[i].prevValueTmp
			ag.imputation[i].prevValueTmp = result
		// For normal agg, we use result
		default:
			ag.outputTypes[i] = *result.ResolvedType()
			row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], result)
		}
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			// We can't encode nil into an EncDatum, so we represent it with DNull.
			result = tree.DNull
		}
		if _, ok := b.(*builtins.ImputationAggregate); ok {
			ag.outputTypes[i] = *result.ResolvedType()
		}
	}
	// We record the original ag.row in imputation struct and emit row. Marks the start of gapfilling.
	if ag.gapfill.gapFillState == gapFillStart {
		ag.gapfill.gapfillingrow = ag.row
		return aggEmittingRows, nil, nil
	}
	if outRow := ag.ProcessRowHelper(ag.row); outRow != nil {
		return aggEmittingRows, outRow, nil
	}
	return aggEmittingRows, nil, nil
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (ag *hashAggregator) emitRow() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if len(ag.bucketsIter) == 0 {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.PbCtx()); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		// Before we create a new 'buckets' map below, we need to "release" the
		// already accounted for memory of the current map.
		ag.bucketsAcc.Shrink(ag.PbCtx(), int64(ag.alreadyAccountedFor)*hashAggregatorSizeOfBucketsItem)
		// Note that, for simplicity, we're ignoring the overhead of the slice of
		// strings.
		ag.bucketsAcc.Shrink(ag.PbCtx(), int64(len(ag.buckets))*sizeOfString)
		ag.bucketsIter = nil
		ag.buckets = make(map[string]aggregateFuncs)
		ag.bucketsLenGrowThreshold = hashAggregatorBucketsInitialLen
		ag.alreadyAccountedFor = 0
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucketsIter[0]
	ag.bucketsIter = ag.bucketsIter[1:]

	// Once we get the results from the bucket, we can delete it from the map.
	// This will allow us to return the memory to the system before the hash
	// aggregator is fully done (which matters when we have many buckets).
	// NOTE: accounting for the memory under aggregate builtins in the bucket
	// is updated in getAggResults (the bucket will be closed), however, we
	// choose to not reduce our estimate of the map's internal footprint
	// because it is error-prone to estimate the new footprint (we don't know
	// whether and when Go runtime will release some of the underlying memory).
	// This behavior is ok, though, since actual usage of buckets will be lower
	// than what we accounted for - in the worst case, the query might hit a
	// memory budget limit and error out when it might actually be within the
	// limit. However, we might be under accounting memory usage in other
	// places, so having some over accounting here might be actually beneficial
	// as a defensive mechanism against OOM crashes.
	state, row, meta := ag.getAggResults(ag.buckets[bucket])
	delete(ag.buckets, bucket)
	return state, row, meta
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered a the current row out.
func (ag *orderedAggregator) emitRow() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if ag.bucket == nil {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.PbCtx()); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucket
	ag.bucket = nil
	return ag.getAggResults(bucket)
}

// Next is part of the RowSource interface.
func (ag *hashAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.PbCtx(), "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// Next is part of the RowSource interface.
func (ag *orderedAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			if ag.gapfill.gapFillState != gapFillInit {
				// adding row after gapfilling
				ag.runningState, row, meta = ag.getAggResults(nil)
			} else {
				ag.runningState, row, meta = ag.emitRow()
			}
		default:
			log.Fatalf(ag.PbCtx(), "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *hashAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *orderedAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

func (ag *aggregatorBase) accumulateRowIntoBucket(
	row sqlbase.EncDatumRow, groupKey []byte, bucket aggregateFuncs,
) error {
	var err error
	// Feed the func holders for this bucket the non-grouping datums.
	for i, a := range ag.aggregations {
		if a.FilterColIdx != nil {
			col := *a.FilterColIdx
			if err = row[col].EnsureDecoded(&ag.inputTypes[col], &ag.datumAlloc); err != nil {
				return err
			}
			if row[*a.FilterColIdx].Datum != tree.DBoolTrue {
				// This row doesn't contribute to this aggregation.
				continue
			}
		}
		// Extract the corresponding arguments from the row to feed into the
		// aggregate function.
		// Most functions require at most one argument thus we separate
		// the first argument and allocation of (if applicable) a variadic
		// collection of arguments thereafter.
		var firstArg tree.Datum
		var otherArgs tree.Datums

		if len(a.ColIdx) > 1 {
			otherArgs = make(tree.Datums, len(a.ColIdx)-1)
		}
		isFirstArg := true
		for j, c := range a.ColIdx {
			if err = row[c].EnsureDecoded(&ag.inputTypes[c], &ag.datumAlloc); err != nil {
				return err
			}
			if isFirstArg {
				firstArg = row[c].Datum
				isFirstArg = false
				continue
			}
			otherArgs[j-1] = row[c].Datum
		}

		canAdd := true
		if a.Distinct {
			canAdd, err = ag.funcs[i].isDistinct(
				ag.PbCtx(),
				&ag.datumAlloc,
				groupKey,
				firstArg,
				otherArgs,
			)
			if err != nil {
				return err
			}
		}
		if !canAdd {
			continue
		}

		if err = bucket[i].Add(ag.PbCtx(), firstArg, otherArgs...); err != nil {
			return err
		}
	}
	return nil
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *hashAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	// The encoding computed here determines which bucket the non-grouping
	// datums are accumulated to.
	encoded, err := ag.encode(ag.scratch, row)
	if err != nil {
		return err
	}
	ag.scratch = encoded[:0]

	bucket, ok := ag.buckets[string(encoded)]
	if !ok {
		s, err := ag.arena.AllocBytes(ag.PbCtx(), encoded)
		if err != nil {
			return err
		}
		bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
		ag.buckets[s] = bucket
		if len(ag.buckets) == ag.bucketsLenGrowThreshold {
			toAccountFor := ag.bucketsLenGrowThreshold - ag.alreadyAccountedFor
			if err := ag.bucketsAcc.Grow(ag.PbCtx(), int64(toAccountFor)*hashAggregatorSizeOfBucketsItem); err != nil {
				return err
			}
			ag.alreadyAccountedFor = ag.bucketsLenGrowThreshold
			ag.bucketsLenGrowThreshold *= 2
		}
	}

	return ag.accumulateRowIntoBucket(row, encoded, bucket)
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *orderedAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	if ag.bucket == nil {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
	}

	return ag.accumulateRowIntoBucket(row, nil /* groupKey */, ag.bucket)
}

type aggregateFuncHolder struct {
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

	// arguments is the list of constant (non-aggregated) arguments to the
	// aggregate, for instance, the separator in string_agg.
	arguments tree.Datums

	group *aggregatorBase
	seen  map[string]struct{}
	arena *stringarena.Arena
}

const (
	sizeOfString         = int64(unsafe.Sizeof(""))
	sizeOfAggregateFuncs = int64(unsafe.Sizeof(aggregateFuncs{}))
	sizeOfAggregateFunc  = int64(unsafe.Sizeof(tree.AggregateFunc(nil)))
)

func (ag *aggregatorBase) newAggregateFuncHolder(
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc, arguments tree.Datums,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:    create,
		group:     ag,
		arena:     &ag.arena,
		arguments: arguments,
	}
}

// isDistinct returns whether this aggregateFuncHolder has not already seen the
// encoding of grouping columns and argument columns. It should be used *only*
// when we have DISTINCT aggregation so that we can aggregate only the "first"
// row in the group.
func (a *aggregateFuncHolder) isDistinct(
	ctx context.Context,
	alloc *sqlbase.DatumAlloc,
	prefix []byte,
	firstArg tree.Datum,
	otherArgs tree.Datums,
) (bool, error) {
	// Allocate one EncDatum that will be reused when encoding every argument.
	ed := sqlbase.EncDatum{Datum: firstArg}
	encoded, err := ed.Fingerprint(firstArg.ResolvedType(), alloc, prefix)
	if err != nil {
		return false, err
	}
	if otherArgs != nil {
		for _, arg := range otherArgs {
			ed.Datum = arg
			encoded, err = ed.Fingerprint(arg.ResolvedType(), alloc, encoded)
			if err != nil {
				return false, err
			}
		}
	}

	if _, ok := a.seen[string(encoded)]; ok {
		// We have already seen a row with such combination of grouping and
		// argument columns.
		return false, nil
	}
	s, err := a.arena.AllocBytes(ctx, encoded)
	if err != nil {
		return false, err
	}
	a.seen[s] = struct{}{}
	return true, nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregatorBase) encode(
	appendTo []byte, row sqlbase.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Fingerprint(
			&ag.inputTypes[colIdx], &ag.datumAlloc, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

func isLastOrFirst(specFunc execinfrapb.AggregatorSpec_Func) bool {
	if specFunc == execinfrapb.AggregatorSpec_FIRST || specFunc == execinfrapb.AggregatorSpec_LAST ||
		specFunc == execinfrapb.AggregatorSpec_FIRST_ROW || specFunc == execinfrapb.AggregatorSpec_LAST_ROW {
		return true
	}
	return false
}

func (ag *aggregatorBase) createAggregateFuncs() (aggregateFuncs, error) {
	if err := ag.bucketsAcc.Grow(ag.PbCtx(), sizeOfAggregateFuncs+sizeOfAggregateFunc*int64(len(ag.funcs))); err != nil {
		return nil, err
	}
	bucket := make(aggregateFuncs, len(ag.funcs))
	//var reserve int
	for i, f := range ag.funcs {
		agg := f.create(ag.EvalCtx, f.arguments)
		if ag.scalarGroupByWithSumInt {
			// AggHandling() will set seenNonNull to true of sum_int agg
			// when scalarGroupByWithSumInt is true, in order to return 0.
			agg.AggHandling()
		}
		if err := ag.bucketsAcc.Grow(ag.PbCtx(), agg.Size()); err != nil {
			return nil, err
		}
		bucket[i] = agg
	}
	for i := 0; i < len(bucket); i++ {
		if imputationbucket, ok := bucket[i].(*builtins.ImputationAggregate); ok {
			imputationbucket.Aggfunc = ag.funcs[i+1].create(ag.EvalCtx, ag.funcs[i+1].arguments)
			if isLastOrFirst(ag.aggregations[i+1].Func) {
				//The time column should be added to the last or first function.
				if len(ag.aggregations[i+1].ColIdx) > 0 {
					ag.aggregations[i].ColIdx = append(ag.aggregations[i].ColIdx, ag.aggregations[i+1].ColIdx[1])
				}
			}
			ag.interpolated = true
		}
		if twaBucket, ok := bucket[i].(*builtins.TwaAggregate); ok {
			colIdx := ag.aggregations[i].ColIdx
			if len(colIdx) > 0 {
				twaBucket.Precision = int64(ag.inputTypes[colIdx[0]].Precision())
			}
		}
		if elapsedBucket, ok := bucket[i].(*builtins.ElapsedAggregate); ok {
			colIdx := ag.aggregations[i].ColIdx
			if len(colIdx) > 0 {
				elapsedBucket.Precision = int64(ag.inputTypes[colIdx[0]].Precision())
			}
		}
	}
	return bucket, nil
}

// getNewTime returns the result of rounding oldTime down to a multiple of dInterval.
func getNewTime(oldTime time.Time, dInterval tree.DInterval, loc *time.Location) time.Time {
	newTime := time.Date(1, 1, 1, 0, 0, 0, 0, loc)
	if dInterval.Months != 0 {
		timeMonth := (oldTime.Year()-1)*12 + (int(oldTime.Month() - 1))
		newMonth := (int64(timeMonth) / dInterval.Months) * dInterval.Months
		newTime = newTime.AddDate(0, int(newMonth), 0)
		return newTime
	}
	var offSet int
	if loc != nil && loc != time.UTC {
		timeInLocation := oldTime.In(loc)
		_, offSet = timeInLocation.Zone()
		oldTime = oldTime.Add(time.Duration(offSet) * time.Second)
	}
	if dInterval.Days != 0 {
		newTime = oldTime.Truncate(time.Duration(dInterval.Days) * time.Hour * 24)
	}
	if dInterval.Nanos() != 0 {
		newTime = oldTime.Truncate(time.Duration(dInterval.Nanos()))
	}
	if loc != nil && loc != time.UTC {
		newTime = newTime.Add(-time.Duration(offSet) * time.Second)
	}
	return newTime
}

// getNewTimeForWin returns the result of rounding oldTime down to a multiple of dInterval.
func getNewTimeForWin(
	oldTime time.Time, dInterval tree.DInterval, duration1 tree.DInterval, loc *time.Location,
) time.Time {
	newTime := time.Date(1, 1, 1, 0, 0, 0, 0, loc)
	if dInterval.Months != 0 {
		timeMonth := (oldTime.Year()-1)*12 + (int(oldTime.Month() - 1))
		newMonth := (int64(timeMonth) / dInterval.Months) * dInterval.Months
		newTime = newTime.AddDate(0, int(newMonth), 0)
		return newTime
	}
	var offSet int
	if loc != nil && loc != time.UTC {
		timeInLocation := oldTime.In(loc)
		_, offSet = timeInLocation.Zone()
		oldTime = oldTime.Add(time.Duration(offSet) * time.Second)
	}
	if dInterval.Days != 0 {
		var firstTs time.Time
		oldTimeUnix := oldTime.Unix() * 1000
		oldTimeInterval := ((dInterval.Days) * int64(time.Hour) * 24) / 1000000
		oldTimeTrun := oldTimeUnix / oldTimeInterval
		oldTimeMul := (dInterval.Days) * int64(time.Hour) * 24
		result := oldTimeTrun * oldTimeMul
		sts := timeutil.Unix(0, result)
		ets := duration.Add(sts, duration1.Duration)
		if ets.Before(oldTime) {
			for ets.Before(oldTime) && sts.Before(oldTime) {
				sts = duration.Add(sts, dInterval.Duration)
				ets = duration.Add(sts, duration1.Duration)
			}
			firstTs = sts
		} else {
			for ets.After(oldTime) || ets.Equal(oldTime) {
				firstTs = sts
				sts = duration.Subtract(sts, dInterval.Duration)
				ets = duration.Add(sts, duration1.Duration)
			}
		}
		newTime = firstTs
	}
	if dInterval.Nanos() != 0 {
		var firstTs time.Time
		oldTimeUnix := oldTime.Unix() * 1000
		oldTimeInterval := dInterval.Nanos() / 1000000
		oldTimeTrun := oldTimeUnix / oldTimeInterval
		oldTimeMul := dInterval.Nanos()
		result := oldTimeTrun * oldTimeMul
		sts := timeutil.Unix(0, result)
		ets := duration.Add(sts, duration1.Duration)
		if ets.Before(oldTime) {
			for ets.Before(oldTime) && sts.Before(oldTime) {
				sts = duration.Add(sts, dInterval.Duration)
				ets = duration.Add(sts, duration1.Duration)
			}
			firstTs = sts
		} else {
			for ets.After(oldTime) || ets.Equal(oldTime) {
				firstTs = sts
				sts = duration.Subtract(sts, dInterval.Duration)
				ets = duration.Add(sts, duration1.Duration)
			}
		}
		newTime = firstTs
	}
	if loc != nil && loc != time.UTC {
		newTime = newTime.Add(-time.Duration(offSet) * time.Second)
	}
	return newTime
}

func encodeDatum(i int, t types.T) sqlbase.EncDatum {
	switch t.Family() {
	case types.IntFamily:
		return sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
	case types.BoolFamily:
		return sqlbase.EncDatum{Datum: tree.MakeDBool(i%2 == 0)}
	case types.StringFamily:
		return sqlbase.EncDatum{Datum: tree.NewDString(strconv.Itoa(i))}
	case types.BytesFamily:
		return sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(strconv.Itoa(i)))}
	case types.TimestampTZFamily:
		return sqlbase.EncDatum{Datum: tree.MakeDTimestampTZ(timeutil.Unix(int64(i), 0), 0)}
	case types.TimestampFamily:
		return sqlbase.EncDatum{Datum: tree.MakeDTimestamp(timeutil.Unix(int64(i), 0), 0)}
	default:
		// never hit.
		return sqlbase.EncDatum{}
	}
}
