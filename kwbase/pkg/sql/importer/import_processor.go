// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package importer

import (
	// "bytes"
	"context"
	"math"
	// "os/exec"
	// "strconv"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var csvOutputTypes = []types.T{
	*types.Bytes,
	*types.Bytes,
}

var importBatchSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		16<<20,
	)
	return s
}()

var importPKAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.pk_buffer_size",
		"the initial size of the BulkAdder buffer handling primary index imports",
		32<<20,
	)
	return s
}()

var importPKAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_pk_buffer_size",
		"the maximum size of the BulkAdder buffer handling primary index imports",
		128<<20,
	)
	return s
}()

var importIndexAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.index_buffer_size",
		"the initial size of the BulkAdder buffer handling secondary index imports",
		32<<20,
	)
	return s
}()

var importIndexAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.max_index_buffer_size",
		"the maximum size of the BulkAdder buffer handling secondary index imports",
		512<<20,
	)
	return s
}()

var importBufferIncrementSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.buffer_increment",
		"the size by which the BulkAdder attempts to grow its buffer before flushing",
		32<<20,
	)
	return s
}()

// commandMetadataEstimate is an estimate of how much metadata Raft will addRecord to
// an AddSSTable command. It is intentionally a vast overestimate to avoid
// embedding intricate knowledge of the Raft encoding scheme here.
const commandMetadataEstimate = 1 << 20 // 1 MB

var defaultRejectedSize = 1 << 30 // 1G

// MaxImportBatchSize determines the maximum size of the payload in an
// AddSSTable request. It uses the ImportBatchSize setting directly unless the
// specified value would exceed the maximum Raft command size, in which case it
// returns the maximum recordBatch size that will fit within a Raft command.
func MaxImportBatchSize(st *cluster.Settings) int64 {
	desiredSize := importBatchSize.Get(&st.SV)
	maxCommandSize := kvserver.MaxCommandSize.Get(&st.SV)
	if desiredSize+commandMetadataEstimate > maxCommandSize {
		return maxCommandSize - commandMetadataEstimate
	}
	return desiredSize
}

// ImportBufferConfigSizes determines the minimum, maximum and step size for the
// BulkAdder buffer used in import.
func ImportBufferConfigSizes(st *cluster.Settings, isPKAdder bool) (int64, func() int64, int64) {
	if isPKAdder {
		return importPKAdderBufferSize.Get(&st.SV),
			func() int64 { return importPKAdderMaxBufferSize.Get(&st.SV) },
			importBufferIncrementSize.Get(&st.SV)
	}
	return importIndexAdderBufferSize.Get(&st.SV),
		func() int64 { return importIndexAdderMaxBufferSize.Get(&st.SV) },
		importBufferIncrementSize.Get(&st.SV)
}

type readImportDataProcessor struct {
	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ReadImportDataSpec
	output  execinfra.RowReceiver
}

var _ execinfra.Processor = &readImportDataProcessor{}

func (cp *readImportDataProcessor) Push(ctx context.Context, res []byte) error {
	return nil
}

func (cp *readImportDataProcessor) IsShortCircuitForPgEncode() bool {
	return false
}

func (cp *readImportDataProcessor) NextPgWire() (val []byte, code int, err error) {
	return nil, 0, nil
}

func (cp *readImportDataProcessor) SupportPgWire() bool {
	return false
}

func (cp *readImportDataProcessor) Start(ctx context.Context) context.Context {
	return nil
}

func (cp *readImportDataProcessor) RunTS(ctx context.Context) {
	panic("implement me")
}

func (cp *readImportDataProcessor) OutputTypes() []types.T {
	return csvOutputTypes
}

// Run is the main loop of readImportDataProcessor.It calls runImport and send
// summary to gateway node.
func (cp *readImportDataProcessor) Run(ctx context.Context) execinfra.RowStats {
	ctx, span := tracing.ChildSpan(ctx, "readImportDataProcessor", int32(cp.flowCtx.NodeID))
	defer tracing.FinishSpan(span)
	defer cp.output.ProducerDone()

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	var summary *roachpb.BulkOpSummary
	var err error
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(progCh)
		if cp.spec.Table.TableType == tree.RelationalTable {
			summary, err = runImport(ctx, cp.flowCtx, &cp.spec, progCh)
		} else {
			summary, err = runTimeSeriesImport(ctx, cp.flowCtx, &cp.spec, progCh)
		}
	}()
	for prog := range progCh {
		// Take a copy so that we can send the progress address to the output processor.
		p := prog
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}
	if err != nil {
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return execinfra.RowStats{}
	}
	if cp.flowCtx.Cfg.TestingKnobs.InjectErrorAfterUpdateImportJob != nil && !cp.flowCtx.Cfg.TestingKnobs.IsImportFinished {
		cp.flowCtx.Cfg.TestingKnobs.IsImportFinished = true
		err = cp.flowCtx.Cfg.TestingKnobs.InjectErrorAfterUpdateImportJob()
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return execinfra.RowStats{}
	}
	// Once the import is done, send back to the controller the serialized
	// summary of the import operation. For more info see roachpb.BulkOpSummary.\
	if summary == nil {
		summary = &roachpb.BulkOpSummary{}
	}
	countsBytes, err := protoutil.Marshal(summary)
	if err != nil {
		cp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return execinfra.RowStats{}
	}
	cp.output.Push(sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
	}, nil)
	return execinfra.RowStats{}
}

// runImport read csv file and ingest data to storage.
// 1. Read csv file and put [][]string to recordBatch.
// 2. Get data from recordBatch and Convert data into database internal types.(relational -> kvs timeSeries -> exprs)
// 3. Put database internal types to kvCh.
// 4. Read data from kvCh and ingest data to storage.
func runImport(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) (*roachpb.BulkOpSummary, error) {
	// Used to send ingested import rows to the KV layer.
	kvCh := make(chan row.KVBatch, 10)
	var err error
	// This group holds the go routines that are responsible for producing KV batches.
	// and ingesting produced KVs.
	group := ctxgroup.WithContext(ctx)
	// Read input files into kvs
	group.GoCtx(func(ctx context.Context) error {
		defer func() {
			close(kvCh)
		}()
		ctx, span := tracing.ChildSpan(ctx, "readImportFiles", int32(flowCtx.NodeID))
		defer tracing.FinishSpan(span)
		return readAndConvertFiles(ctx, flowCtx, spec, kvCh)
	})
	// Ingest the KVs that the producer group emitted to the chan and the row result
	// at the end is one row containing an encoded BulkOpSummary.
	var summary *roachpb.BulkOpSummary
	group.GoCtx(func(ctx context.Context) error {
		summary, err = ingestKvs(ctx, flowCtx, spec, kvCh, progCh)
		if err != nil {
			return err
		}
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.ResumePos = make(map[int32]int64)
		prog.CompletedFraction = make(map[int32]float32)
		for i := range spec.Uri {
			prog.CompletedFraction[i] = 1.0
			prog.ResumePos[i] = math.MaxInt64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case progCh <- prog:
			return nil
		}
	})

	if err = group.Wait(); err != nil {
		return nil, err
	}
	return summary, nil
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	kvCh <-chan row.KVBatch,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) (*roachpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "ingestKVs", int32(flowCtx.NodeID))
	defer tracing.FinishSpan(span)
	pkIndexAdder, indexAdder, err := initAdder(ctx, flowCtx, spec)
	if err != nil {
		return nil, err
	}
	defer pkIndexAdder.Close(ctx)
	defer indexAdder.Close(ctx)
	if err = writeKVs(ctx, flowCtx, spec, kvCh, progCh, pkIndexAdder, indexAdder); err != nil {
		return nil, err
	}
	addedSummary := pkIndexAdder.GetSummary()
	addedSummary.Add(indexAdder.GetSummary())
	return &addedSummary, nil
}

// initAdder init kv adder. We init a primary key adder and a index adder.
// Adders used to ingest kv to rocksdb.
func initAdder(
	ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.ReadImportDataSpec,
) (storagebase.BulkAdder, storagebase.BulkAdder, error) {
	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}

	sstSize := func() int64 { return MaxImportBatchSize(flowCtx.Cfg.Settings) }

	// We create two bulk adders so as to combat the excessive flushing of small
	// SSTs which was observed when using a single adder for both primary and
	// secondary index kvs. The number of secondary index kvs are small, and so we
	// expect the indexAdder to flushBatch much less frequently than the pkIndexAdder.
	//
	// It is highly recommended that the cluster setting controlling the max size
	// of the pkIndexAdder buffer be set below that of the indexAdder buffer.
	// Otherwise, as a consequence of filling up faster the pkIndexAdder buffer
	// will hog memory as it tries to grow more aggressively.
	minBufferSize, maxBufferSize, stepSize := ImportBufferConfigSizes(flowCtx.Cfg.Settings, true /* isPKAdder */)
	pkOptions := storagebase.BulkAdderOptions{
		Name:               "pkAdder",
		StepBufferSize:     stepSize,
		SSTSize:            sstSize,
		MinBufferSize:      minBufferSize,
		MaxBufferSize:      maxBufferSize,
		DisallowShadowing:  true,
		SkipDuplicates:     true,
		ShouldLogLogicalOp: true,
	}
	minBufferSize, maxBufferSize, stepSize = ImportBufferConfigSizes(flowCtx.Cfg.Settings, false /* isPKAdder */)
	idxOptions := storagebase.BulkAdderOptions{
		Name:               "indexAdder",
		StepBufferSize:     stepSize,
		SSTSize:            sstSize,
		MinBufferSize:      minBufferSize,
		MaxBufferSize:      maxBufferSize,
		DisallowShadowing:  true,
		SkipDuplicates:     true,
		ShouldLogLogicalOp: true,
	}
	pkIndexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, writeTS, pkOptions)
	if err != nil {
		return nil, nil, err
	}
	indexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, writeTS, idxOptions)
	if err != nil {
		return nil, nil, err
	}
	return pkIndexAdder, indexAdder, err
}

// writeKVs ingest kvs to relational storage.
// 1. Get kvs from kvCh.
// 2. Decode kv and get it indexID to distinguish primary key index  and the second indexes.
// 3. Add the kv to corresponding Adder, if Adder's kvs are larger than preset size or spanning the range, flush it to storage.
// 4. At the end, flush the remaining data in adders.
func writeKVs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	kvCh <-chan row.KVBatch,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	pkIndexAdder, indexAdder storagebase.BulkAdder,
) error {
	writtenRow := make(map[int32]int64, len(spec.Uri))

	pkFlushedRow, idxFlushedRow := sync.Map{}, sync.Map{}

	pkIndexAdder.SetOnFlush(func() {
		for i, emitted := range writtenRow {
			pkFlushedRow.Store(i, emitted)
		}
		if indexAdder.IsEmpty() {
			for i, emitted := range writtenRow {
				idxFlushedRow.Store(i, emitted)
			}
		}
	})
	indexAdder.SetOnFlush(func() {
		for i, emitted := range writtenRow {
			idxFlushedRow.Store(i, emitted)
		}
	})

	pushProgress := func() {
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.ResumePos = make(map[int32]int64)
		for file := range spec.Uri {
			var pk, idx int64
			if v, ok := pkFlushedRow.Load(file); ok {
				pk = v.(int64)
			}
			if v, ok := idxFlushedRow.Load(file); ok {
				idx = v.(int64)
			}
			if idx > pk {
				prog.ResumePos[file] = pk
			} else {
				prog.ResumePos[file] = idx
			}
		}
		progCh <- prog
	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(time.Second * 10)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case <-stopProgress:
				return nil
			case <-tick.C:
				pushProgress()
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)

		// We insert splits at every index span of the table above. Since the
		// BulkAdder is split aware when constructing SSTs, there is no risk of worst
		// case overlap behavior in the resulting AddSSTable calls.
		//
		// NB: We are getting rid of the pre-buffering stage which constructed
		// separate buckets for each table's primary data, and flushed to the
		// BulkAdder when the bucket was full. This is because, a tpcc 1k IMPORT would
		// OOM when maintaining this buffer. Two big wins we got from this
		// pre-buffering stage were:
		//
		// 1. We avoided worst case overlapping behavior in the AddSSTable calls as a
		// result of flushing keys with the same TableIDIndexID prefix, together.
		//
		// 2. Secondary index KVs which were few and filled the bucket infrequently
		// were flushed rarely, resulting in fewer L0 (and total) files.
		//
		// While we continue to achieve the first property as a result of the splits
		// mentioned above, the KVs sent to the BulkAdder are no longer grouped which
		// results in flushing a much larger number of small SSTs. This increases the
		// number of L0 (and total) files, but with a lower memory usage.
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				_, _, indexID, indexErr := sqlbase.DecodeTableIDIndexID(kv.Key)
				if indexErr != nil {
					return indexErr
				}

				// Decide which adder to send the KV to by extracting its index id.
				//
				// TODO(adityamaru): There is a potential optimization of plumbing the
				// different putters, and differentiating based on their type. It might be
				// more efficient than parsing every kv.
				if indexID == 1 {
					if err := pkIndexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if _, ok := err.(storagebase.DuplicateKeyError); ok {
							return errors.Wrap(err, "duplicate key in primary index")
						}
						return err
					}
				} else {
					if err := indexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if _, ok := err.(storagebase.DuplicateKeyError); ok {
							return errors.Wrap(err, "duplicate key in index")
						}
						return err
					}
				}
			}
			writtenRow[kvBatch.DataFileIdx] = kvBatch.LastRow
			if flowCtx.Cfg.TestingKnobs.BulkAdderFlushesEveryBatch {
				_ = pkIndexAdder.Flush(ctx)
				_ = indexAdder.Flush(ctx)
				pushProgress()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := pkIndexAdder.Flush(ctx); err != nil {
		if err, ok := err.(storagebase.DuplicateKeyError); ok {
			return errors.Wrap(err, "duplicate key in primary index")
		}
		return err
	}

	if err := indexAdder.Flush(ctx); err != nil {
		if err, ok := err.(storagebase.DuplicateKeyError); ok {
			return errors.Wrap(err, "duplicate key in index")
		}
		return err
	}
	return nil
}

// newReadImportDataProcessor init a readImportDataProcessor.
func newReadImportDataProcessor(
	flowCtx *execinfra.FlowCtx, spec execinfrapb.ReadImportDataSpec, output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
	}
	return cp, nil
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor
}
