// Copyright 2017 The Cockroach Authors.
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
	"encoding/binary"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/errors"
)

// SortedBucket is used to display sort bucket
type SortedBucket struct {
	// The estimated number of row count within the timestamp range.
	RowCount uint64

	// The estimated number of unordered row count within the timestamp range.
	UnorderedRowCount uint64

	// The estimated number of unordered row count within the timestamp range.
	UnorderedEntities uint64

	// The estimated number of unordered entities within the timestamp range.
	OrderedEntities uint64

	// UpperBound is the upper bound of the bucket.
	UpperBound tree.Datum

	NodeCount uint64
}

// SampledSortedInfo is used to represent sorted histogram for entities.
type SampledSortedInfo struct {
	minTimestamp int64
	maxTimestamp int64

	sampledSortedBucket map[int]SortedBucket
}

// sketchInfo contains the specification and run-time state for each sketch.
type sketchInfo struct {
	spec              execinfrapb.SketchSpec
	sketch            *hyperloglog.Sketch
	numNulls          int64
	numRows           int64
	SampledSortedData SampledSortedInfo
}

func (si *sketchInfo) isTagSketch() bool {
	return len(si.spec.ColumnTypes) > 0 && (si.spec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_PTAG) ||
		si.spec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_TAG))
}

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	execinfra.ProcessorBase

	flowCtx         *execinfra.FlowCtx
	input           execinfra.RowSource
	memAcc          mon.BoundAccount
	sr              stats.SampleReservoir
	sketches        []sketchInfo
	outTypes        []types.T
	maxFractionIdle float64

	// Output column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sketchCol    int
}

var _ execinfra.Processor = &samplerProcessor{}

const samplerProcName = "sampler"

// SamplerProgressInterval corresponds to the number of input rows after which
// the sampler will report progress by pushing a metadata record.  It is mutable
// for testing.
var SamplerProgressInterval = 10000

var supportedSketchTypes = map[execinfrapb.SketchType]struct{}{
	// The code currently hardcodes the use of this single type of sketch
	// (which avoids the extra complexity until we actually have multiple types).
	execinfrapb.SketchType_HLL_PLUS_PLUS_V1: {},
}

// maxIdleSleepTime is the maximum amount of time we sleep for throttling
// (we sleep once every SamplerProgressInterval rows).
const maxIdleSleepTime = 10 * time.Second

// At 25% average CPU usage we start throttling automatic stats.
const cpuUsageMinThrottle = 0.25

// At 75% average CPU usage we reach maximum throttling of automatic stats.
const cpuUsageMaxThrottle = 0.75

func newSamplerProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SamplerSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*samplerProcessor, error) {
	for _, s := range spec.Sketches {
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
		if len(s.Columns) != 1 {
			return nil, unimplemented.NewWithIssue(34422, "multi-column statistics are not supported yet.")
		}
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will disable histogram collection if this limit is not
	// enough.
	memMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "sampler-mem")
	s := &samplerProcessor{
		flowCtx:         flowCtx,
		input:           input,
		memAcc:          memMonitor.MakeBoundAccount(),
		sketches:        make([]sketchInfo, len(spec.Sketches)),
		maxFractionIdle: spec.MaxFractionIdle,
	}

	var sampleCols util.FastIntSet
	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:     spec.Sketches[i],
			sketch:   hyperloglog.New14(),
			numNulls: 0,
			numRows:  0,
		}
		if spec.Sketches[i].GenerateHistogram {
			sampleCols.Add(int(spec.Sketches[i].Columns[0]))
		}
	}

	s.sr.Init(int(spec.SampleSize), input.OutputTypes(), &s.memAcc, sampleCols)

	inTypes := input.OutputTypes()
	outTypes := make([]types.T, 0, len(inTypes)+5)

	// First columns are the same as the input.
	outTypes = append(outTypes, inTypes...)

	// An INT column for the rank of each row.
	s.rankCol = len(outTypes)
	outTypes = append(outTypes, *types.Int)

	// An INT column indicating the sketch index.
	s.sketchIdxCol = len(outTypes)
	outTypes = append(outTypes, *types.Int)

	// An INT column indicating the number of rows processed.
	s.numRowsCol = len(outTypes)
	outTypes = append(outTypes, *types.Int)

	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	s.numNullsCol = len(outTypes)
	outTypes = append(outTypes, *types.Int)

	// A BYTES column with the sketch data.
	s.sketchCol = len(outTypes)
	outTypes = append(outTypes, *types.Bytes)
	s.outTypes = outTypes

	if err := s.Init(
		nil, post, outTypes, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *samplerProcessor) pushTrailingMeta(ctx context.Context) {
	execinfra.SendTraceData(ctx, s.Out.Output())
}

func (s *samplerProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, nil
}

// Run is part of the Processor interface.
func (s *samplerProcessor) Run(ctx context.Context) execinfra.RowStats {
	s.input.Start(ctx)
	s.StartInternal(ctx, samplerProcName)

	earlyExit, err := s.mainLoop(s.PbCtx())
	if err != nil {
		execinfra.DrainAndClose(s.PbCtx(), s.Out.Output(), err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(s.PbCtx())
		s.input.ConsumerClosed()
		s.Out.Close()
	}
	s.MoveToDraining(nil /* err */)
	return execinfra.RowStats{}
}

// TestingSamplerSleep introduces a sleep inside the sampler, every
// <samplerProgressInterval>. Used to simulate a heavily throttled
// run for testing.
var TestingSamplerSleep time.Duration

func (s *samplerProcessor) mainLoop(ctx context.Context) (earlyExit bool, err error) {
	rng, _ := randutil.NewPseudoRand()
	var da sqlbase.DatumAlloc
	var buf []byte
	rowCount := 0
	lastWakeupTime := timeutil.Now()
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		rowCount++
		if rowCount%SamplerProgressInterval == 0 {
			// Send a metadata record to check that the consumer is still alive and
			// report number of rows processed since the last update.
			meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
				RowsProcessed: uint64(SamplerProgressInterval),
			}}
			if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				return true, nil
			}

			if s.maxFractionIdle > 0 {
				// Look at KWDB's average CPU usage in the last 10 seconds:
				//  - if it is lower than cpuUsageMinThrottle, we do not throttle;
				//  - if it is higher than cpuUsageMaxThrottle, we throttle all the way;
				//  - in-between, we scale the idle time proportionally.
				usage := s.flowCtx.Cfg.RuntimeStats.GetCPUCombinedPercentNorm()

				if usage > cpuUsageMinThrottle {
					fractionIdle := s.maxFractionIdle
					if usage < cpuUsageMaxThrottle {
						fractionIdle *= (usage - cpuUsageMinThrottle) /
							(cpuUsageMaxThrottle - cpuUsageMinThrottle)
					}
					if log.V(1) {
						log.Infof(
							ctx, "throttling to fraction idle %.2f (based on usage %.2f)", fractionIdle, usage,
						)
					}

					elapsed := timeutil.Now().Sub(lastWakeupTime)
					// Throttle the processor according to fractionIdle.
					// Wait time is calculated as follows:
					//
					//       fraction_idle = t_wait / (t_run + t_wait)
					//  ==>  t_wait = t_run * fraction_idle / (1 - fraction_idle)
					//
					wait := time.Duration(float64(elapsed) * fractionIdle / (1 - fractionIdle))
					if wait > maxIdleSleepTime {
						wait = maxIdleSleepTime
					}
					timer := time.NewTimer(wait)
					defer timer.Stop()
					select {
					case <-timer.C:
						break
					case <-s.flowCtx.Stopper().ShouldStop():
						break
					}
				}
				lastWakeupTime = timeutil.Now()
			}

			if TestingSamplerSleep != 0 {
				time.Sleep(TestingSamplerSleep)
			}
		}

		for i := range s.sketches {
			// TODO(radu): for multi-column sketches, we will need to do this for all
			// columns.
			col := s.sketches[i].spec.Columns[0]
			s.sketches[i].numRows++
			isNull := row[col].IsNull()
			if isNull {
				s.sketches[i].numNulls++
			}
			if s.outTypes[col].Family() == types.IntFamily && !isNull {
				// Fast path for integers.
				// TODO(radu): make this more general.
				val, err := row[col].GetInt()
				if err != nil {
					return false, err
				}

				if cap(buf) < 8 {
					buf = make([]byte, 8)
				} else {
					buf = buf[:8]
				}

				// Note: this encoding is not identical with the one in the general path
				// below, but it achieves the same thing (we want equal integers to
				// encode to equal []bytes). The only caveat is that all samplers must
				// use the same encodings, so changes will require a new SketchType to
				// avoid problems during upgrade.
				//
				// We could use a more efficient hash function and use InsertHash, but
				// it must be a very good hash function (HLL expects the hash values to
				// be uniformly distributed in the 2^64 range). Experiments (on tpcc
				// order_line) with simplistic functions yielded bad results.
				binary.LittleEndian.PutUint64(buf, uint64(val))
				s.sketches[i].sketch.Insert(buf)
			} else {
				buf, err = row[col].Fingerprint(&s.outTypes[col], &da, buf[:0])
				if err != nil {
					return false, err
				}
				s.sketches[i].sketch.Insert(buf)
			}
		}

		// Use Int63 so we don't have headaches converting to DInt.
		rank := uint64(rng.Int63())
		if err := s.sr.SampleRow(ctx, s.EvalCtx, row, rank); err != nil {
			if code := pgerror.GetPGCode(err); code != pgcode.OutOfMemory {
				return false, err
			}
			// We hit an out of memory error. Clear the sample reservoir and
			// disable histogram sample collection.
			s.sr.Disable()
			log.Info(ctx, "disabling histogram collection due to excessive memory utilization")

			// Send a metadata record so the sample aggregator will also disable
			// histogram collection.
			meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
				HistogramDisabled: true,
			}}
			if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				return true, nil
			}
		}
	}

	outRow := make(sqlbase.EncDatumRow, len(s.outTypes))
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(&s.outTypes[i], tree.DNull)
	}
	// Emit the sampled rows.
	for _, sample := range s.sr.Get() {
		copy(outRow, sample.Row)
		outRow[s.rankCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
		if !emitHelper(ctx, &s.Out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}
	// Release the memory for the sampled rows.
	s.sr = stats.SampleReservoir{}

	// Emit the sketch rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(&s.outTypes[i], tree.DNull)
	}

	for i, si := range s.sketches {
		outRow[s.sketchIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
		outRow[s.numRowsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numRows))}
		outRow[s.numNullsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numNulls))}
		data, err := si.sketch.MarshalBinary()
		if err != nil {
			return false, err
		}
		outRow[s.sketchCol] = sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(data))}
		if !emitHelper(ctx, &s.Out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}

	// Send one last progress update to the consumer.
	meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
		RowsProcessed: uint64(rowCount % SamplerProgressInterval),
	}}
	if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
		return true, nil
	}
	return false, nil
}

func (s *samplerProcessor) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.PbCtx())
		s.MemMonitor.Stop(s.PbCtx())
	}
}
