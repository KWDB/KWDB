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

package kvserver

import (
	"context"
	"fmt"
	"io"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	kwdberrors "github.com/cockroachdb/errors"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/time/rate"
)

const (
	// Messages that provide detail about why a snapshot was rejected.
	snapshotStoreTooFullMsg = "store almost out of disk space"
	snapshotApplySemBusyMsg = "store busy applying snapshots"
	storeDrainingMsg        = "store is draining"

	// IntersectingSnapshotMsg is part of the error message returned from
	// canApplySnapshotLocked and is exposed here so testing can rely on it.
	IntersectingSnapshotMsg = "snapshot intersects existing range"
)

// incomingSnapshotStream is the minimal interface on a GRPC stream required
// to receive a snapshot over the network.
type incomingSnapshotStream interface {
	Send(*SnapshotResponse) error
	Recv() (*SnapshotRequest, error)
}

// outgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type outgoingSnapshotStream interface {
	Send(*SnapshotRequest) error
	Recv() (*SnapshotResponse, error)
}

// snapshotStrategy is an approach to sending and receiving Range snapshots.
// Each implementation corresponds to a SnapshotRequest_Strategy, and it is
// expected that the implementation that matches the Strategy specified in the
// snapshot header will always be used.
type snapshotStrategy interface {
	// Receive streams SnapshotRequests in from the provided stream and
	// constructs an IncomingSnapshot.
	Receive(context.Context, incomingSnapshotStream, SnapshotRequest_Header) (IncomingSnapshot, error)

	// ReceiveTS streams SnapshotRequests in from the provided stream and
	// constructs an IncomingSnapshot.
	ReceiveTS(context.Context, incomingSnapshotStream, SnapshotRequest_Header, *Store) (IncomingSnapshot, error)

	// Send streams SnapshotRequests created from the OutgoingSnapshot in to the
	// provided stream. On nil error, the number of bytes sent is returned.
	Send(context.Context, outgoingSnapshotStream, SnapshotRequest_Header, *OutgoingSnapshot) (int64, error)

	// SendTS streams SnapshotRequests created from the OutgoingSnapshot in to the
	// provided stream. On nil error, the number of bytes sent is returned.
	SendTS(context.Context, *tse.TsEngine, outgoingSnapshotStream, SnapshotRequest_Header, *OutgoingSnapshot) (int64, error)

	// Status provides a status report on the work performed during the
	// snapshot. Only valid if the strategy succeeded.
	Status() string

	// Close cleans up any resources associated with the snapshot strategy.
	Close(context.Context)
}

func assertStrategy(
	ctx context.Context, header SnapshotRequest_Header, expect SnapshotRequest_Strategy,
) {
	if header.Strategy != expect {
		log.Fatalf(ctx, "expected strategy %s, found strategy %s", expect, header.Strategy)
	}
}

// kvBatchSnapshotStrategy is an implementation of snapshotStrategy that streams
// batches of KV pairs in the BatchRepr format.
type kvBatchSnapshotStrategy struct {
	raftCfg *base.RaftConfig
	status  string

	// The size of the batches of PUT operations to send to the receiver of the
	// snapshot. Only used on the sender side.
	batchSize int64
	// Limiter for sending KV batches. Only used on the sender side.
	limiter *rate.Limiter
	// Only used on the sender side.
	newBatch func() storage.Batch
	// bytesSent is updated in sendBatch and returned from Send(). It does not
	// reflect the log entries sent (which are never sent in newer versions of
	// KWDB, as of VersionUnreplicatedTruncatedState).
	bytesSent int64

	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk. Only used on the receiver side.
	sstChunkSize int64
	// Only used on the receiver side.
	scratch *SSTSnapshotStorageScratch
}

// multiSSTWriter is a wrapper around RocksDBSstFileWriter and
// SSTSnapshotStorageScratch that handles chunking SSTs and persisting them to
// disk.
type multiSSTWriter struct {
	scratch   *SSTSnapshotStorageScratch
	currSST   storage.SSTWriter
	keyRanges []rditer.KeyRange
	currRange int
	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk.
	sstChunkSize int64
}

func newMultiSSTWriter(
	ctx context.Context,
	scratch *SSTSnapshotStorageScratch,
	keyRanges []rditer.KeyRange,
	sstChunkSize int64,
) (multiSSTWriter, error) {
	msstw := multiSSTWriter{
		scratch:      scratch,
		keyRanges:    keyRanges,
		sstChunkSize: sstChunkSize,
	}
	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

func (msstw *multiSSTWriter) initSST(ctx context.Context) error {
	newSSTFile, err := msstw.scratch.NewFile(ctx, msstw.sstChunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create new sst file")
	}
	newSST := storage.MakeIngestionSSTWriter(newSSTFile)
	msstw.currSST = newSST
	if err := msstw.currSST.ClearRange(msstw.keyRanges[msstw.currRange].Start, msstw.keyRanges[msstw.currRange].End); err != nil {
		msstw.currSST.Close()
		return errors.Wrap(err, "failed to clear range on sst file writer")
	}
	return nil
}

func (msstw *multiSSTWriter) finalizeSST(ctx context.Context) error {
	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	msstw.currRange++
	msstw.currSST.Close()
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key storage.MVCCKey, value []byte) error {
	for msstw.keyRanges[msstw.currRange].End.Key.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(ctx); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	if msstw.keyRanges[msstw.currRange].Start.Key.Compare(key.Key) > 0 {
		return kwdberrors.AssertionFailedf("client error: expected %s to fall in one of %s", key.Key, msstw.keyRanges)
	}
	if err := msstw.currSST.Put(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func (msstw *multiSSTWriter) Finish(ctx context.Context) error {
	if msstw.currRange < len(msstw.keyRanges) {
		for {
			if err := msstw.finalizeSST(ctx); err != nil {
				return err
			}
			if msstw.currRange >= len(msstw.keyRanges) {
				break
			}
			if err := msstw.initSST(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (msstw *multiSSTWriter) Close() {
	msstw.currSST.Close()
}

// ReceiveTS implements the snapshotStrategy interface.
//
// NOTE: This function assumes that the data are sent in sorted
// order. The data are sent in the following sorted order:
//
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. TS data
// 4. raft log
func (kvSS *kvBatchSnapshotStrategy) ReceiveTS(
	ctx context.Context, stream incomingSnapshotStream, header SnapshotRequest_Header, s *Store,
) (IncomingSnapshot, error) {
	assertStrategy(ctx, header, SnapshotRequest_TS_BATCH)
	keyRanges := rditer.MakeReplicatedKeyRanges(header.State.Desc)
	msstw, err := newMultiSSTWriter(ctx, kvSS.scratch, keyRanges, kvSS.sstChunkSize)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()

	rollbackFn := func(ctx context.Context, name string, sid uint64, tid uint64, rid roachpb.RangeID) {
		if rollbackErr := s.TsEngine.WriteSnapshotRollback(tid, sid); rollbackErr != nil {
			log.Errorf(ctx, "TsEngine.WriteSnapshotRollback failed r%v, %v, %v, %v", rid, tid, sid, rollbackErr)
		}
		log.VEventf(ctx, 3, "TsEngine.WriteSnapshotRollback success r%v, %v, %v", rid, tid, sid)
		if delErr := s.TsEngine.DeleteSnapshot(tid, sid); delErr != nil {
			log.Errorf(ctx, "TsEngine.DeleteSnapshot failed r%v, %v, %v, %v", rid, tid, sid, delErr)
		}
		log.VEventf(ctx, 3, "TsEngine.DeleteSnapshot success r%v, %v, %v", rid, tid, sid)
	}

	var tableID, writeSnapshotID uint64
	var logEntries [][]byte
	for {
		req, err := stream.Recv()
		if err != nil {
			return noSnap, err
		}

		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return noSnap, sendSnapshotError(stream, err)
		}

		if req.KVBatch != nil {
			batchReader, err := storage.NewRocksDBBatchReader(req.KVBatch)
			if err != nil {
				return noSnap, errors.Wrap(err, "failed to decode batch")
			}

			for batchReader.Next() {
				if batchReader.BatchType() != storage.BatchTypeValue {
					return noSnap, kwdberrors.AssertionFailedf("expected type %d, found type %d", storage.BatchTypeValue, batchReader.BatchType())
				}
				key, err := batchReader.MVCCKey()
				if err != nil {
					return noSnap, errors.Wrap(err, "failed to decode mvcc key")
				}
				if err := msstw.Put(ctx, key, batchReader.Value()); err != nil {
					return noSnap, err
				}
			}
		}

		tableID = req.TsTableId
		rangeID := header.State.Desc.RangeID
		if req.InitTsSnapshotForWrite {
			startKey := header.State.Desc.StartKey
			endKey := header.State.Desc.EndKey
			if header.State.Desc.HashNum == 0 {
				header.State.Desc.HashNum = api.HashParamV2
			}
			hashNum := header.State.Desc.HashNum
			_, startPoint, endPoint, startTs, endTs, err := sqlbase.DecodeTSRangeKey(startKey, endKey, hashNum)
			if err != nil {
				log.Errorf(ctx, "DecodeTSRangeKey failedD: %v", err)
				return IncomingSnapshot{}, errors.Wrap(err, "DecodeTSRangeKey failed")
			}
			var snapshotErr error
			writeSnapshotID, snapshotErr = s.TsEngine.CreateSnapshotForWrite(tableID, startPoint, endPoint, startTs, endTs)
			log.VEventf(ctx, 3, "TsEngine.CreateSnapshotForWrite r%v, %v, %v, %v, %v, %v, %v, %v, %v", rangeID, tableID, startPoint, endPoint, startTs, endTs, req.TsSnapshotId, writeSnapshotID, snapshotErr)
			if snapshotErr != nil {
				rollbackFn(ctx, "CreateSnapshotForWrite", writeSnapshotID, tableID, rangeID)
				return IncomingSnapshot{}, errors.Wrap(snapshotErr, "receiveTS CreateSnapshotForWrite failed")
			}
		}

		if req.TSBatch != nil {
			err := s.TsEngine.WriteSnapshotBatchData(tableID, writeSnapshotID, req.TSBatch)
			log.VEventf(ctx, 3, "TsEngine.WriteSnapshotBatchData r%v, %v, %v, %v, %v", rangeID, tableID, writeSnapshotID, len(req.TSBatch), err)
			if err != nil {
				rollbackFn(ctx, "WriteSnapshotBatchData", writeSnapshotID, tableID, rangeID)
				return IncomingSnapshot{}, errors.Wrap(err, "receiveTS WriteSnapshotBatchData failed")
			}
		}

		if req.LogEntries != nil {
			logEntries = append(logEntries, req.LogEntries...)
		}

		if req.Final {
			if err := msstw.Finish(ctx); err != nil {
				rollbackFn(ctx, "Finish", writeSnapshotID, tableID, rangeID)
				return noSnap, err
			}

			msstw.Close()
			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				err = errors.Wrap(err, "client error: invalid snapshot")
				rollbackFn(ctx, "FromBytes", writeSnapshotID, tableID, rangeID)
				return noSnap, sendSnapshotError(stream, err)
			}

			inSnap := IncomingSnapshot{
				UsesUnreplicatedTruncatedState: header.UnreplicatedTruncatedState,
				SnapUUID:                       snapUUID,
				SSTStorageScratch:              kvSS.scratch,
				LogEntries:                     logEntries,
				State:                          &header.State,
				snapType:                       header.Type,
				IsTSSnapshot:                   true,
				TableID:                        tableID,
				WriteSnapshotID:                writeSnapshotID,
			}

			expLen := inSnap.State.RaftAppliedIndex - inSnap.State.TruncatedState.Index
			if expLen != uint64(len(logEntries)) {
				log.Warningf(ctx,
					"missing log entries in snapshot (%s): got %d entries, expected %d",
					inSnap.String(), len(logEntries), expLen)
			}

			kvSS.status = fmt.Sprintf("log entries: %d", len(logEntries))
			return inSnap, nil
		}
	}
}

// Receive implements the snapshotStrategy interface.
//
// NOTE: This function assumes that the key-value pairs are sent in sorted
// order. The key-value pairs are sent in the following sorted order:
//
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. User key range
func (kvSS *kvBatchSnapshotStrategy) Receive(
	ctx context.Context, stream incomingSnapshotStream, header SnapshotRequest_Header,
) (IncomingSnapshot, error) {
	assertStrategy(ctx, header, SnapshotRequest_KV_BATCH)

	// At the moment we'll write at most three SSTs.
	// TODO(jeffreyxiao): Re-evaluate as the default range size grows.
	keyRanges := rditer.MakeReplicatedKeyRanges(header.State.Desc)
	msstw, err := newMultiSSTWriter(ctx, kvSS.scratch, keyRanges, kvSS.sstChunkSize)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()
	var logEntries [][]byte

	for {
		req, err := stream.Recv()
		if err != nil {
			return noSnap, err
		}
		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return noSnap, sendSnapshotError(stream, err)
		}

		if req.KVBatch != nil {
			batchReader, err := storage.NewRocksDBBatchReader(req.KVBatch)
			if err != nil {
				return noSnap, errors.Wrap(err, "failed to decode batch")
			}
			// All operations in the batch are guaranteed to be puts.
			for batchReader.Next() {
				if batchReader.BatchType() != storage.BatchTypeValue {
					return noSnap, kwdberrors.AssertionFailedf("expected type %d, found type %d", storage.BatchTypeValue, batchReader.BatchType())
				}
				key, err := batchReader.MVCCKey()
				if err != nil {
					return noSnap, errors.Wrap(err, "failed to decode mvcc key")
				}
				if err := msstw.Put(ctx, key, batchReader.Value()); err != nil {
					return noSnap, err
				}
			}
		}
		if req.LogEntries != nil {
			logEntries = append(logEntries, req.LogEntries...)
		}
		if req.Final {
			// We finished receiving all batches and log entries. It's possible that
			// we did not receive any key-value pairs for some of the key ranges, but
			// we must still construct SSTs with range deletion tombstones to remove
			// the data.
			if err := msstw.Finish(ctx); err != nil {
				return noSnap, err
			}

			msstw.Close()

			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				err = errors.Wrap(err, "client error: invalid snapshot")
				return noSnap, sendSnapshotError(stream, err)
			}

			inSnap := IncomingSnapshot{
				UsesUnreplicatedTruncatedState: header.UnreplicatedTruncatedState,
				SnapUUID:                       snapUUID,
				SSTStorageScratch:              kvSS.scratch,
				LogEntries:                     logEntries,
				State:                          &header.State,
				snapType:                       header.Type,
			}

			expLen := inSnap.State.RaftAppliedIndex - inSnap.State.TruncatedState.Index
			if expLen != uint64(len(logEntries)) {
				// We've received a botched snapshot. We could fatal right here but opt
				// to warn loudly instead, and fatal when applying the snapshot
				// (in Replica.applySnapshot) in order to capture replica hard state.
				log.Warningf(ctx,
					"missing log entries in snapshot (%s): got %d entries, expected %d",
					inSnap.String(), len(logEntries), expLen)
			}

			kvSS.status = fmt.Sprintf("log entries: %d, ssts: %d", len(logEntries), len(kvSS.scratch.SSTs()))
			return inSnap, nil
		}
	}
}

// errMalformedSnapshot indicates that the snapshot in question is malformed,
// for e.g. missing raft log entries.
var errMalformedSnapshot = errors.New("malformed snapshot generated")

// SendTS implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) SendTS(
	ctx context.Context,
	TsEngine *tse.TsEngine,
	stream outgoingSnapshotStream,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
) (int64, error) {
	assertStrategy(ctx, header, SnapshotRequest_TS_BATCH)

	for _, kvData := range snap.kvBatch {
		if len(kvData) == 0 {
			break
		}

		if err := kvSS.sendKVData(ctx, stream, kvData); err != nil {
			return 0, err
		}
	}

	// send TS data
	// The TSSnapshotID of an empty snapshot is 0
	totalSize := 0
	start := timeutil.Now()
	rangeID := header.State.Desc.RangeID
	if snap.TSSnapshotID != 0 {
		log.Infof(ctx, "(r%)TSEngine.GetSnapshotNextBatchData(ID:%d)", rangeID, snap.TSSnapshotID)
		var err error
		first := true
		for {
			var tsData []byte

			tsData, err = TsEngine.GetSnapshotNextBatchData(snap.tableID, snap.TSSnapshotID)
			log.VEventf(ctx, 3, "TsEngine.GetSnapshotNextBatchData r%v, %v, %v, %v, %v", rangeID, snap.tableID, snap.TSSnapshotID, len(tsData), err)
			if err != nil {
				return 0, errors.Wrapf(err, "SendTS GetSnapshotNextBatchData failed")
			}

			if len(tsData) == 0 {
				log.VEventf(ctx, 3, "TsEngine.sendTSByte end r%v, %v, %v, %v, %v, %v", rangeID, snap.tableID, snap.TSSnapshotID, totalSize, timeutil.Since(start).String(), err)
				durSent := timeutil.Since(start)
				log.Infof(
					ctx,
					"sendTSByte streamed %s to %s in %.2fs @ %s/s: %s",
					snap,
					header.RaftMessageRequest.ToReplica,
					durSent.Seconds(),
					humanizeutil.IBytes(int64(float64(totalSize)/durSent.Seconds())),
					kvSS.Status(),
				)
				// If it is an empty snapshot, skip sendTSByte
				break
			}
			if first {
				if err := stream.Send(&SnapshotRequest{
					InitTsSnapshotForWrite: true,
					TsTableId:              snap.tableID,
					TsSnapshotId:           snap.TSSnapshotID,
				}); err != nil {
					log.VEventf(ctx, 3, "TsEngine.stream.Send r%v, %v, %v, %v, %v", rangeID, snap.tableID, snap.TSSnapshotID, len(tsData), err)
					return 0, errors.Wrapf(err, "SendTS init snapshot for write err")
				}
				first = false
			}

			totalSize += len(tsData)
			if err := kvSS.sendTSByte(ctx, stream, tsData); err != nil {
				log.VEventf(ctx, 3, "TsEngine.sendTSByte r%v, %v, %v, %v, %v, %v, %v", rangeID, snap.tableID, snap.TSSnapshotID, len(tsData), totalSize, timeutil.Since(start).String(), err)
				if err == io.EOF {
					log.VEventf(ctx, 3, "TsEngine.sendTSByte end r%v, %v, %v, %v, %v, %v", rangeID, snap.tableID, snap.TSSnapshotID, totalSize, timeutil.Since(start).String(), err)
					err = nil
					break
				}
				// TODO(Yang Jie): rollback and delete remote snapshot
				return 0, err
			}
		}
	}

	// send raft log
	firstIndex := header.State.TruncatedState.Index + 1
	endIndex := snap.RaftSnap.Metadata.Index + 1
	preallocSize := endIndex - firstIndex
	const maxPreallocSize = 1000
	if preallocSize > maxPreallocSize {
		preallocSize = maxPreallocSize
	}
	logEntries := make([][]byte, 0, preallocSize)

	var raftLogBytes int64
	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		bytes, err := kv.Value.GetBytes()
		if err == nil {
			logEntries = append(logEntries, bytes)
			raftLogBytes += int64(len(bytes))
		}
		return false, err
	}

	if err := iterateEntries(ctx, snap.EngineSnap, rangeID, firstIndex, endIndex, scanFunc); err != nil {
		return 0, err
	}

	expLen := endIndex - firstIndex
	if expLen != uint64(len(logEntries)) {
		entriesRange, err := extractRangeFromEntries(logEntries)
		if err != nil {
			return 0, err
		}
		log.Warningf(ctx, "missing log entries in TS snapshot (%s): "+
			"got %d entries, expected %d (TruncatedState.Index=%d, LogEntries=%s)",
			snap.String(), len(logEntries), expLen, snap.State.TruncatedState.Index, entriesRange)
		return 0, errMalformedSnapshot
	}

	{
		var ent raftpb.Entry
		for i := range logEntries {
			if err := protoutil.Unmarshal(logEntries[i], &ent); err != nil {
				return 0, err
			}
			if !sniffSideloadedRaftCommand(ent.Data) {
				continue
			}
			if err := snap.WithSideloaded(func(ss SideloadStorage) error {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, ss, snap.RaftEntryCache,
				)
				if err != nil {
					return err
				}
				if newEnt != nil {
					ent = *newEnt
				}
				return nil
			}); err != nil {
				if errors.Cause(err) == errSideloadedFileNotFound {
					return 0, &errMustRetrySnapshotDueToTruncation{
						index: ent.Index,
						term:  ent.Term,
					}
				}
				return 0, err
			}

			var err error
			if logEntries[i], err = protoutil.Marshal(&ent); err != nil {
				return 0, err
			}
		}
	}
	kvSS.status = fmt.Sprintf("TS size: %d, log entries: %d", totalSize, len(logEntries))
	if err := stream.Send(&SnapshotRequest{LogEntries: logEntries}); err != nil {
		return 0, err
	}
	return kvSS.bytesSent, nil

}

// Send implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Send(
	ctx context.Context,
	stream outgoingSnapshotStream,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
) (int64, error) {
	assertStrategy(ctx, header, SnapshotRequest_KV_BATCH)

	// Iterate over all keys using the provided iterator and stream out batches
	// of key-values.
	n := 0
	var b storage.Batch
	for iter := snap.Iter; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		key := iter.Key()
		value := iter.Value()
		n++
		if b == nil {
			b = kvSS.newBatch()
		}
		if err := b.Put(key, value); err != nil {
			b.Close()
			return 0, err
		}

		if int64(b.Len()) >= kvSS.batchSize {
			if err := kvSS.sendBatch(ctx, stream, b); err != nil {
				return 0, err
			}
			b = nil
			// We no longer need the keys and values in the batch we just sent,
			// so reset ReplicaDataIterator's allocator and allow its data to
			// be garbage collected.
			iter.ResetAllocator()
		}
	}
	if b != nil {
		if err := kvSS.sendBatch(ctx, stream, b); err != nil {
			return 0, err
		}
	}

	// Iterate over the specified range of Raft entries and send them all out
	// together.
	firstIndex := header.State.TruncatedState.Index + 1
	endIndex := snap.RaftSnap.Metadata.Index + 1
	preallocSize := endIndex - firstIndex
	const maxPreallocSize = 1000
	if preallocSize > maxPreallocSize {
		// It's possible for the raft log to become enormous in certain
		// sustained failure conditions. We may bail out of the snapshot
		// process early in scanFunc, but in the worst case this
		// preallocation is enough to run the server out of memory. Limit
		// the size of the buffer we will preallocate.
		preallocSize = maxPreallocSize
	}
	logEntries := make([][]byte, 0, preallocSize)

	var raftLogBytes int64
	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		bytes, err := kv.Value.GetBytes()
		if err == nil {
			logEntries = append(logEntries, bytes)
			raftLogBytes += int64(len(bytes))
		}
		return false, err
	}

	rangeID := header.State.Desc.RangeID

	if err := iterateEntries(ctx, snap.EngineSnap, rangeID, firstIndex, endIndex, scanFunc); err != nil {
		return 0, err
	}

	// The difference between the snapshot index (applied index at the time of
	// snapshot) and the truncated index should equal the number of log entries
	// shipped over.
	expLen := endIndex - firstIndex
	if expLen != uint64(len(logEntries)) {
		// We've generated a botched snapshot. We could fatal right here but opt
		// to warn loudly instead, and fatal at the caller to capture a checkpoint
		// of the underlying storage engine.
		entriesRange, err := extractRangeFromEntries(logEntries)
		if err != nil {
			return 0, err
		}
		log.Warningf(ctx, "missing log entries in snapshot (%s): "+
			"got %d entries, expected %d (TruncatedState.Index=%d, LogEntries=%s)",
			snap.String(), len(logEntries), expLen, snap.State.TruncatedState.Index, entriesRange)
		return 0, errMalformedSnapshot
	}

	// Inline the payloads for all sideloaded proposals.
	//
	// TODO(tschottdorf): could also send slim proposals and attach sideloaded
	// SSTables directly to the snapshot. Probably the better long-term
	// solution, but let's see if it ever becomes relevant. Snapshots with
	// inlined proposals are hopefully the exception.
	{
		var ent raftpb.Entry
		for i := range logEntries {
			if err := protoutil.Unmarshal(logEntries[i], &ent); err != nil {
				return 0, err
			}
			if !sniffSideloadedRaftCommand(ent.Data) {
				continue
			}
			if err := snap.WithSideloaded(func(ss SideloadStorage) error {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, ss, snap.RaftEntryCache,
				)
				if err != nil {
					return err
				}
				if newEnt != nil {
					ent = *newEnt
				}
				return nil
			}); err != nil {
				if errors.Cause(err) == errSideloadedFileNotFound {
					// We're creating the Raft snapshot based on a snapshot of
					// the engine, but the Raft log may since have been
					// truncated and corresponding on-disk sideloaded payloads
					// unlinked. Luckily, we can just abort this snapshot; the
					// caller can retry.
					//
					// TODO(tschottdorf): check how callers handle this. They
					// should simply retry. In some scenarios, perhaps this can
					// happen repeatedly and prevent a snapshot; not sending the
					// log entries wouldn't help, though, and so we'd really
					// need to make sure the entries are always here, for
					// instance by pre-loading them into memory. Or we can make
					// log truncation less aggressive about removing sideloaded
					// files, by delaying trailing file deletion for a bit.
					return 0, &errMustRetrySnapshotDueToTruncation{
						index: ent.Index,
						term:  ent.Term,
					}
				}
				return 0, err
			}
			// TODO(tschottdorf): it should be possible to reuse `logEntries[i]` here.
			var err error
			if logEntries[i], err = protoutil.Marshal(&ent); err != nil {
				return 0, err
			}
		}
	}
	kvSS.status = fmt.Sprintf("kv pairs: %d, log entries: %d", n, len(logEntries))
	if err := stream.Send(&SnapshotRequest{LogEntries: logEntries}); err != nil {
		return 0, err
	}
	return kvSS.bytesSent, nil
}

func (kvSS *kvBatchSnapshotStrategy) sendBatch(
	ctx context.Context, stream outgoingSnapshotStream, batch storage.Batch,
) error {
	if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
		return err
	}
	repr := batch.Repr()
	kvSS.bytesSent += int64(len(repr))
	batch.Close()
	return stream.Send(&SnapshotRequest{KVBatch: repr})
}

// sendKVData send kv bytes
func (kvSS *kvBatchSnapshotStrategy) sendKVData(
	ctx context.Context, stream outgoingSnapshotStream, kv []byte,
) error {
	if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
		return err
	}
	kvSS.bytesSent += int64(len(kv))
	return stream.Send(&SnapshotRequest{KVBatch: kv})
}

// sendTSByte use stream to send data
func (kvSS *kvBatchSnapshotStrategy) sendTSByte(
	ctx context.Context, stream outgoingSnapshotStream, tsBytes []byte,
) error {
	if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
		return err
	}
	kvSS.bytesSent += int64(len(tsBytes))
	return stream.Send(&SnapshotRequest{
		TSBatch: tsBytes,
	})
}

// Status implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Status() string { return kvSS.status }

// Close implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Close(ctx context.Context) {
	if kvSS.scratch != nil {
		// A failure to clean up the storage is benign except that it will leak
		// disk space (which is reclaimed on node restart). It is unexpected
		// though, so log a warning.
		if err := kvSS.scratch.Clear(); err != nil {
			log.Warningf(ctx, "error closing kvBatchSnapshotStrategy: %v", err)
		}
	}
}

// reserveSnapshot throttles incoming snapshots. The returned closure is used
// to cleanup the reservation and release its resources. A nil cleanup function
// and a non-empty rejectionMessage indicates the reservation was declined.
func (s *Store) reserveSnapshot(
	ctx context.Context, header *SnapshotRequest_Header,
) (_cleanup func(), _rejectionMsg string, _err error) {
	tBegin := timeutil.Now()
	if header.RangeSize == 0 {
		// Empty snapshots are exempt from rate limits because they're so cheap to
		// apply. This vastly speeds up rebalancing any empty ranges created by a
		// RESTORE or manual SPLIT AT, since it prevents these empty snapshots from
		// getting stuck behind large snapshots managed by the replicate queue.
	} else if header.CanDecline {
		storeDesc, ok := s.cfg.StorePool.getStoreDescriptor(s.StoreID())
		if ok && (!maxCapacityCheck(storeDesc) || header.RangeSize > storeDesc.Capacity.Available) {
			return nil, snapshotStoreTooFullMsg, nil
		}
		select {
		case s.snapshotApplySem <- struct{}{}:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-s.stopper.ShouldStop():
			return nil, "", errors.Errorf("stopped")
		default:
			return nil, snapshotApplySemBusyMsg, nil
		}
	} else {
		select {
		case s.snapshotApplySem <- struct{}{}:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-s.stopper.ShouldStop():
			return nil, "", errors.Errorf("stopped")
		}
	}

	// The choice here is essentially arbitrary, but with a default range size of 64mb and the
	// Raft snapshot rate limiting of 8mb/s, we expect to spend less than 8s per snapshot.
	// Preemptive snapshots are limited to 2mb/s (by default), so they can take up to 4x longer,
	// but an average range is closer to 32mb, so we expect ~16s for larger preemptive snapshots,
	// which is what we want to log.
	const snapshotReservationWaitWarnThreshold = 13 * time.Second
	if elapsed := timeutil.Since(tBegin); elapsed > snapshotReservationWaitWarnThreshold {
		replDesc, _ := header.State.Desc.GetReplicaDescriptor(s.StoreID())
		log.Infof(
			ctx,
			"waited for %.1fs to acquire snapshot reservation to r%d/%d",
			elapsed.Seconds(),
			header.State.Desc.RangeID,
			replDesc.ReplicaID,
		)
	}

	s.metrics.ReservedReplicaCount.Inc(1)
	s.metrics.Reserved.Inc(header.RangeSize)
	return func() {
		s.metrics.ReservedReplicaCount.Dec(1)
		s.metrics.Reserved.Dec(header.RangeSize)
		if header.RangeSize != 0 {
			<-s.snapshotApplySem
		}
	}, "", nil
}

// canApplySnapshotLocked returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder can be added to the replicasByKey map (if
// necessary). If a placeholder is required, it is returned as the first value.
//
// Both the store mu (and the raft mu for an existing replica if there is one)
// must be held.
func (s *Store) canApplySnapshotLocked(
	ctx context.Context, snapHeader *SnapshotRequest_Header,
) (*ReplicaPlaceholder, error) {
	if snapHeader.IsPreemptive() {
		return nil, kwdberrors.AssertionFailedf(`expected a raft or learner snapshot`)
	}

	// TODO(tbg): see the comment on desc.Generation for what seems to be a much
	// saner way to handle overlap via generational semantics.
	desc := *snapHeader.State.Desc

	// First, check for an existing Replica.
	v, ok := s.mu.replicas.Load(
		int64(desc.RangeID),
	)
	if !ok {
		return nil, errors.Errorf("canApplySnapshotLocked requires a replica present")
	}
	existingRepl := (*Replica)(v)
	// The raftMu is held which allows us to use the existing replica as a
	// placeholder when we decide that the snapshot can be applied. As long
	// as the caller releases the raftMu only after feeding the snapshot
	// into the replica, this is safe.
	existingRepl.raftMu.AssertHeld()

	existingRepl.mu.RLock()
	existingDesc := existingRepl.mu.state.Desc
	existingIsInitialized := existingDesc.IsInitialized()
	existingDestroyStatus := existingRepl.mu.destroyStatus
	existingRepl.mu.RUnlock()

	if existingIsInitialized {
		// Regular Raft snapshots can't be refused at this point,
		// even if they widen the existing replica. See the comments
		// in Replica.maybeAcquireSnapshotMergeLock for how this is
		// made safe.
		//
		// NB: The snapshot must be intended for this replica as
		// withReplicaForRequest ensures that requests with a non-zero replica
		// id are passed to a replica with a matching id. Given this is not a
		// preemptive snapshot we know that its id must be non-zero.
		return nil, nil
	}

	// If we are not alive then we should not apply a snapshot as our removal
	// is imminent.
	if existingDestroyStatus.Removed() {
		return nil, existingDestroyStatus.err
	}

	// We have a key range [desc.StartKey,desc.EndKey) which we want to apply a
	// snapshot for. Is there a conflicting existing placeholder or an
	// overlapping range?
	if err := s.checkSnapshotOverlapLocked(ctx, snapHeader); err != nil {
		return nil, err
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: desc,
	}
	return placeholder, nil
}

// checkSnapshotOverlapLocked returns an error if the snapshot overlaps an
// existing replica or placeholder. Any replicas that do overlap have a good
// chance of being abandoned, so they're proactively handed to the GC queue .
func (s *Store) checkSnapshotOverlapLocked(
	ctx context.Context, snapHeader *SnapshotRequest_Header,
) error {
	desc := *snapHeader.State.Desc

	// NB: this check seems redundant since placeholders are also represented in
	// replicasByKey (and thus returned in getOverlappingKeyRangeLocked).
	if exRng, ok := s.mu.replicaPlaceholders[desc.RangeID]; ok {
		return errors.Errorf("%s: canApplySnapshotLocked: cannot add placeholder, have an existing placeholder %s %v", s, exRng, snapHeader.RaftMessageRequest.FromReplica)
	}

	// TODO(benesch): consider discovering and GC'ing *all* overlapping ranges,
	// not just the first one that getOverlappingKeyRangeLocked happens to return.
	if exRange := s.getOverlappingKeyRangeLocked(&desc); exRange != nil {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		exReplica, err := s.GetReplica(exRange.Desc().RangeID)
		msg := IntersectingSnapshotMsg
		if err != nil {
			log.Warning(ctx, errors.Wrapf(
				err, "unable to look up overlapping replica on %s", exReplica))
		} else {
			inactive := func(r *Replica) bool {
				if r.RaftStatus() == nil {
					return true
				}
				// TODO(benesch): this check does detect inactivity on replicas with
				// epoch-based leases. Since the validity of an epoch-based lease is
				// tied to the owning node's liveness, the lease can be valid well after
				// the leader of the range has cut off communication with this replica.
				// Expiration based leases, by contrast, will expire quickly if the
				// leader of the range stops sending this replica heartbeats.
				lease, pendingLease := r.GetLease()
				now := s.Clock().Now()
				return !r.IsLeaseValid(lease, now) &&
					(pendingLease == (roachpb.Lease{}) || !r.IsLeaseValid(pendingLease, now))
			}
			// We unconditionally send this replica through the GC queue. It's
			// reasonably likely that the GC queue will do nothing because the replica
			// needs to split instead, but better to err on the side of queueing too
			// frequently. Blocking Raft snapshots for too long can wedge a cluster,
			// and if the replica does need to be GC'd, this might be the only code
			// path that notices in a timely fashion.
			//
			// We're careful to avoid starving out other replicas in the GC queue by
			// queueing at a low priority unless we can prove that the range is
			// inactive and thus unlikely to be about to process a split.
			gcPriority := replicaGCPriorityDefault
			if inactive(exReplica) {
				gcPriority = replicaGCPrioritySuspect
			}

			msg += "; initiated GC:"
			s.replicaGCQueue.AddAsync(ctx, exReplica, gcPriority)
		}
		return errors.Errorf("%s %v (incoming %v)", msg, exReplica, snapHeader.State.Desc.RSpan()) // exReplica can be nil
	}
	return nil
}

// shouldAcceptSnapshotData is an optimization to check whether we should even
// bother to read the data for an incoming snapshot. If the snapshot overlaps an
// existing replica or placeholder, we'd error during application anyway, so do
// it before transferring all the data. This method is a guess and may have
// false positives. If the snapshot should be rejected, an error is returned
// with a description of why. Otherwise, nil means we should accept the
// snapshot.
func (s *Store) shouldAcceptSnapshotData(
	ctx context.Context, snapHeader *SnapshotRequest_Header,
) error {
	if snapHeader.IsPreemptive() {
		return kwdberrors.AssertionFailedf(`expected a raft or learner snapshot`)
	}
	pErr := s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest,
		func(ctx context.Context, r *Replica) *roachpb.Error {
			// If the current replica is not initialized then we should accept this
			// snapshot if it doesn't overlap existing ranges.
			if !r.IsInitialized() {
				s.mu.Lock()
				defer s.mu.Unlock()
				return roachpb.NewError(s.checkSnapshotOverlapLocked(ctx, snapHeader))
			}
			// If the current range is initialized then we need to accept this
			// snapshot.
			return nil
		})
	return pErr.GoError()
}

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	if fn := s.cfg.TestingKnobs.ReceiveSnapshot; fn != nil {
		if err := fn(header); err != nil {
			return sendSnapshotError(stream, err)
		}
	}

	if header.IsPreemptive() {
		return kwdberrors.AssertionFailedf(`expected a raft or learner snapshot`)
	}

	// Defensive check that any snapshot contains this store in the	descriptor.
	storeID := s.StoreID()
	if _, ok := header.State.Desc.GetReplicaDescriptor(storeID); !ok {
		return kwdberrors.AssertionFailedf(
			`snapshot of type %s was sent to s%d which did not contain it as a replica: %s`,
			header.Type, storeID, header.State.Desc.Replicas())
	}

	cleanup, rejectionMsg, err := s.reserveSnapshot(ctx, header)
	if err != nil {
		return err
	}
	if cleanup == nil {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_DECLINED,
			Message: rejectionMsg,
		})
	}
	defer cleanup()

	// Check to see if the snapshot can be applied but don't attempt to add
	// a placeholder here, because we're not holding the replica's raftMu.
	// We'll perform this check again later after receiving the rest of the
	// snapshot data - this is purely an optimization to prevent downloading
	// a snapshot that we know we won't be able to apply.
	if err := s.shouldAcceptSnapshotData(ctx, header); err != nil {
		return sendSnapshotError(stream,
			errors.Wrapf(err, "%s,r%d: cannot apply snapshot", s, header.State.Desc.RangeID),
		)
	}

	// Determine which snapshot strategy the sender is using to send this
	// snapshot. If we don't know how to handle the specified strategy, return
	// an error.
	var ss snapshotStrategy
	switch header.Strategy {
	case SnapshotRequest_KV_BATCH:
		snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
		if err != nil {
			err = errors.Wrap(err, "invalid snapshot")
			return sendSnapshotError(stream, err)
		}

		ss = &kvBatchSnapshotStrategy{
			raftCfg:      &s.cfg.RaftConfig,
			scratch:      s.sstSnapshotStorage.NewScratchSpace(header.State.Desc.RangeID, snapUUID),
			sstChunkSize: snapshotSSTWriteSyncRate.Get(&s.cfg.Settings.SV),
		}
		defer ss.Close(ctx)
	case SnapshotRequest_TS_BATCH:
		snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
		if err != nil {
			err = errors.Wrap(err, "invalid ts snapshot")
			return sendSnapshotError(stream, err)
		}

		ss = &kvBatchSnapshotStrategy{
			raftCfg:      &s.cfg.RaftConfig,
			scratch:      s.sstSnapshotStorage.NewScratchSpace(header.State.Desc.RangeID, snapUUID),
			sstChunkSize: snapshotSSTWriteSyncRate.Get(&s.cfg.Settings.SV),
		}
		defer ss.Close(ctx)
	default:
		return sendSnapshotError(stream,
			errors.Errorf("%s,r%d: unknown snapshot strategy: %s",
				s, header.State.Desc.RangeID, header.Strategy),
		)
	}

	if err := stream.Send(&SnapshotResponse{Status: SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	var inSnap IncomingSnapshot
	if header.Strategy == SnapshotRequest_KV_BATCH {
		inSnap, err = ss.Receive(ctx, stream, *header)
	} else if header.Strategy == SnapshotRequest_TS_BATCH {
		inSnap, err = ss.ReceiveTS(ctx, stream, *header, s)
	}
	if err != nil {
		return err
	}
	if err := s.processRaftSnapshotRequest(ctx, header, inSnap); err != nil {
		if inSnap.IsTSSnapshot && inSnap.WriteSnapshotID != 0 {
			rangeID := inSnap.State.Desc.RangeID
			if rollbackErr := s.TsEngine.WriteSnapshotRollback(inSnap.TableID, inSnap.WriteSnapshotID); rollbackErr != nil {
				log.Errorf(ctx, "TsEngine.WriteSnapshotRollback failed r%v, %v, %v, %v", rangeID, inSnap.TableID, inSnap.WriteSnapshotID, rollbackErr)
			}
			log.VEventf(ctx, 3, "TsEngine.WriteSnapshotRollback success r%v, %v, %v", rangeID, inSnap.TableID, inSnap.WriteSnapshotID)
			if delErr := s.TsEngine.DeleteSnapshot(inSnap.TableID, inSnap.WriteSnapshotID); delErr != nil {
				log.Errorf(ctx, "TsEngine.DeleteSnapshot failed r%v, %v, %v, %v", rangeID, inSnap.TableID, inSnap.WriteSnapshotID, delErr)
			}
			log.VEventf(ctx, 3, "TsEngine.DeleteSnapshot success r%v, %v, %v", rangeID, inSnap.TableID, inSnap.WriteSnapshotID)
		}
		return sendSnapshotError(stream, err.GoError())
	}

	return stream.Send(&SnapshotResponse{Status: SnapshotResponse_APPLIED})
}

func sendSnapshotError(stream incomingSnapshotStream, err error) error {
	return stream.Send(&SnapshotResponse{
		Status:  SnapshotResponse_ERROR,
		Message: err.Error(),
	})
}

// SnapshotStorePool narrows StorePool to make sendSnapshot easier to test.
type SnapshotStorePool interface {
	throttle(reason throttleReason, why string, toStoreID roachpb.StoreID)
}

// validatePositive is a function to validate that a settings value is positive.
func validatePositive(v int64) error {
	if v <= 0 {
		return errors.Errorf("%d is not positive", v)
	}
	return nil
}

// rebalanceSnapshotRate is the rate at which preemptive snapshots can be sent.
// This includes snapshots generated for upreplication or for rebalancing.
var rebalanceSnapshotRate = settings.RegisterPublicValidatedByteSizeSetting(
	"kv.snapshot_rebalance.max_rate",
	"the rate limit (bytes/sec) to use for rebalance and upreplication snapshots",
	envutil.EnvOrDefaultBytes("KWBASE_PREEMPTIVE_SNAPSHOT_RATE", 8<<20),
	validatePositive,
)

// recoverySnapshotRate is the rate at which Raft-initiated spanshots can be
// sent. Ideally, one would never see a Raft-initiated snapshot; we'd like all
// the snapshots to be preemptive. However, it has proved unfeasible to
// completely get rid of them.
// TODO(tbg): The existence of this rate, separate from rebalanceSnapshotRate,
// does not make a whole lot of sense.
var recoverySnapshotRate = settings.RegisterPublicValidatedByteSizeSetting(
	"kv.snapshot_recovery.max_rate",
	"the rate limit (bytes/sec) to use for recovery snapshots",
	envutil.EnvOrDefaultBytes("KWBASE_RAFT_SNAPSHOT_RATE", 8<<20),
	validatePositive,
)

// snapshotSSTWriteSyncRate is the size of chunks to write before fsync-ing.
// The default of 2 MiB was chosen to be in line with the behavior in bulk-io.
// See sstWriteSyncRate.
var snapshotSSTWriteSyncRate = settings.RegisterByteSizeSetting(
	"kv.snapshot_sst.sync_size",
	"threshold after which snapshot SST writes must fsync",
	2<<20, /* 2 MiB */
)

func snapshotRateLimit(
	st *cluster.Settings, priority SnapshotRequest_Priority,
) (rate.Limit, error) {
	switch priority {
	case SnapshotRequest_RECOVERY:
		return rate.Limit(recoverySnapshotRate.Get(&st.SV)), nil
	case SnapshotRequest_REBALANCE:
		return rate.Limit(rebalanceSnapshotRate.Get(&st.SV)), nil
	default:
		return 0, errors.Errorf("unknown snapshot priority: %s", priority)
	}
}

type errMustRetrySnapshotDueToTruncation struct {
	index, term uint64
}

func (e *errMustRetrySnapshotDueToTruncation) Error() string {
	return fmt.Sprintf(
		"log truncation during snapshot removed sideloaded SSTable at index %d, term %d",
		e.index, e.term,
	)
}

// sendTSSnapshot send TS snapshot
func sendTSSnapshot(
	ctx context.Context,
	raftCfg *base.RaftConfig,
	TsEngine *tse.TsEngine,
	st *cluster.Settings,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() storage.Batch,
	sent func(),
) error {
	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	}
	switch resp.Status {
	case SnapshotResponse_DECLINED:
		if header.CanDecline {
			declinedMsg := "reservation rejected"
			if len(resp.Message) > 0 {
				declinedMsg = resp.Message
			}
			err := &benignError{errors.Errorf("%s: remote declined %s: %s", to, snap, declinedMsg)}
			storePool.throttle(throttleDeclined, err.Error(), to.StoreID)
			return err
		}
		err := errors.Errorf("%s: programming error: remote declined required %s: %s",
			to, snap, resp.Message)
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	case SnapshotResponse_ERROR:
		storePool.throttle(throttleFailed, resp.Message, to.StoreID)
		return errors.Errorf("%s: remote couldn't accept %s with error: %s",
			to, snap, resp.Message)
	case SnapshotResponse_ACCEPTED:
	// This is the response we're expecting. Continue with snapshot sending.
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	}

	durQueued := timeutil.Since(start)
	start = timeutil.Now()

	const batchSize = 256 << 10 // 256 KB
	const tsBatchSize = 4 << 10 // 4 KB
	targetRate, err := snapshotRateLimit(st, header.Priority)
	if err != nil {
		return errors.Wrapf(err, "%s", to)
	}

	// Convert the bytes/sec rate limit to tsBatches/sec.
	limiter := rate.NewLimiter(targetRate/tsBatchSize, 1 /* burst size */)

	var ss snapshotStrategy
	switch header.Strategy {
	case SnapshotRequest_TS_BATCH:
		ss = &kvBatchSnapshotStrategy{
			raftCfg:   raftCfg,
			batchSize: batchSize,
			limiter:   limiter,
			newBatch:  newBatch,
		}
	default:
		log.Fatalf(ctx, "unknown snapshot strategy: %s", header.Strategy)
	}

	numBytesSent, err := ss.SendTS(ctx, TsEngine, stream, header, snap)
	if err != nil {
		return err
	}
	durSent := timeutil.Since(start)

	sent()
	if err := stream.Send(&SnapshotRequest{Final: true}); err != nil {
		return err
	}
	log.Infof(
		ctx,
		"streamed %s to %s in %.2fs @ %s/s: %s, rate-limit: %s/s, queued: %.2fs",
		snap,
		to,
		durSent.Seconds(),
		humanizeutil.IBytes(int64(float64(numBytesSent)/durSent.Seconds())),
		ss.Status(),
		humanizeutil.IBytes(int64(targetRate)),
		durQueued.Seconds(),
	)

	resp, err = stream.Recv()
	if err != nil {
		log.Infof(ctx, "remote failed to apply TS snapshot, error:%s", err)
		return errors.Wrapf(err, "%s: remote failed to apply TS snapshot", to)
	}

	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		return errors.Errorf("%s: expected EOF, got resp=%v err=%v", to, unexpectedResp, err)
	}
	switch resp.Status {
	case SnapshotResponse_ERROR:
		return errors.Errorf("%s: remote failed to apply TS snapshot for reason %s", to, resp.Message)
	case SnapshotResponse_APPLIED:
		log.Infof(ctx, "remote applied TS snapshot.")
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status)
	}
}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	raftCfg *base.RaftConfig,
	st *cluster.Settings,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() storage.Batch,
	sent func(),
) error {
	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server. The recipient may queue us
	// (only a limited number of snapshots are allowed concurrently) or flat-out
	// reject the snapshot. After the initial message exchange, we'll go and send
	// the actual snapshot (if not rejected).
	resp, err := stream.Recv()
	if err != nil {
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	}
	switch resp.Status {
	case SnapshotResponse_DECLINED:
		if header.CanDecline {
			declinedMsg := "reservation rejected"
			if len(resp.Message) > 0 {
				declinedMsg = resp.Message
			}
			err := &benignError{errors.Errorf("%s: remote declined %s: %s", to, snap, declinedMsg)}
			storePool.throttle(throttleDeclined, err.Error(), to.StoreID)
			return err
		}
		err := errors.Errorf("%s: programming error: remote declined required %s: %s",
			to, snap, resp.Message)
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	case SnapshotResponse_ERROR:
		storePool.throttle(throttleFailed, resp.Message, to.StoreID)
		return errors.Errorf("%s: remote couldn't accept %s with error: %s",
			to, snap, resp.Message)
	case SnapshotResponse_ACCEPTED:
	// This is the response we're expecting. Continue with snapshot sending.
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	}

	durQueued := timeutil.Since(start)
	start = timeutil.Now()

	// The size of batches to send. This is the granularity of rate limiting.
	const batchSize = 256 << 10 // 256 KB
	targetRate, err := snapshotRateLimit(st, header.Priority)
	if err != nil {
		return errors.Wrapf(err, "%s", to)
	}

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	limiter := rate.NewLimiter(targetRate/batchSize, 1 /* burst size */)

	// Create a snapshotStrategy based on the desired snapshot strategy.
	var ss snapshotStrategy
	switch header.Strategy {
	case SnapshotRequest_KV_BATCH:
		ss = &kvBatchSnapshotStrategy{
			raftCfg:   raftCfg,
			batchSize: batchSize,
			limiter:   limiter,
			newBatch:  newBatch,
		}
	default:
		log.Fatalf(ctx, "unknown snapshot strategy: %s", header.Strategy)
	}

	numBytesSent, err := ss.Send(ctx, stream, header, snap)
	if err != nil {
		return err
	}
	durSent := timeutil.Since(start)

	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(&SnapshotRequest{Final: true}); err != nil {
		return err
	}
	log.Infof(
		ctx,
		"streamed %s to %s in %.2fs @ %s/s: %s, rate-limit: %s/s, queued: %.2fs",
		snap,
		to,
		durSent.Seconds(),
		humanizeutil.IBytes(int64(float64(numBytesSent)/durSent.Seconds())),
		ss.Status(),
		humanizeutil.IBytes(int64(targetRate)),
		durQueued.Seconds(),
	)

	resp, err = stream.Recv()
	if err != nil {
		return errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		return errors.Errorf("%s: expected EOF, got resp=%v err=%v", to, unexpectedResp, err)
	}
	switch resp.Status {
	case SnapshotResponse_ERROR:
		return errors.Errorf("%s: remote failed to apply snapshot for reason %s", to, resp.Message)
	case SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status)
	}
}
