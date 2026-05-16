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

//go:build windows
// +build windows

package tse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// ErrExceedMaxBytes is a new error, exceeding max bytes
var ErrExceedMaxBytes = errors.New("exceed max bytes")

// TsRaftStoreSync indicates whether sync ts raft log in time.
var TsRaftStoreSync = settings.RegisterBoolSetting(
	"ts.raft_store.sync",
	"whether always sync ts raft store",
	true,
)

// TsRaftLogEngineConfig configuration of TsEngine
type TsRaftLogEngineConfig struct {
	Dir string
}

// RaftLogs is the raftlog data structure passed to the raftlog store
type RaftLogs struct {
	rangeID uint64
	start   uint64
	end     uint64
	value   [][]byte
	size    int
}

// TsRaftWriteBatch is the batch of raftlogs
type TsRaftWriteBatch struct {
	engine    *TsRaftLogEngine
	commitWg  sync.WaitGroup
	commitErr error
	logs      []RaftLogs
}

// NewTsRaftLogBatch is the new raftlog batch
func NewTsRaftLogBatch(eng *TsRaftLogEngine) *TsRaftWriteBatch {
	if eng == nil {
		return nil
	}
	return &TsRaftWriteBatch{engine: eng}
}

// Done decrements the WaitGroup counter by one.
func (b *TsRaftWriteBatch) Done() {
	b.commitWg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (b *TsRaftWriteBatch) Wait() {
	b.commitWg.Wait()
}

// Pending adds one to the WaitGroup counter.
func (b *TsRaftWriteBatch) Pending() {
	b.commitWg.Add(1)
}

// Put add raftLog to Batch.
func (b *TsRaftWriteBatch) Put(rangeID uint64, lIndex, hIndex uint64, values [][]byte, size int) {
	cnt := hIndex - lIndex
	if uint64(len(values)) != cnt {
		panic(fmt.Sprintf("len of values(%d) and indexes(%d) misatches", len(values), cnt))
	}
	b.logs = append(b.logs, RaftLogs{rangeID: rangeID, start: lIndex, end: hIndex, value: values, size: size})
}

// Truncate delete Raftlogs to the specified index.
func (b *TsRaftWriteBatch) Truncate(rangeID uint64, index uint64) {
	b.logs = append(b.logs, RaftLogs{rangeID: rangeID, start: index})
}

// ClearRange clear all Raftlogs of the specified range.
func (b *TsRaftWriteBatch) ClearRange(rangeID uint64) {
	b.logs = append(b.logs, RaftLogs{rangeID: rangeID})
}

// Commit write the raftlog to the disk.
func (b *TsRaftWriteBatch) Commit() error {
	return nil
}

// TsRaftLogEngine is ts database instance.
type TsRaftLogEngine struct {
	cfg TsRaftLogEngineConfig
	// engine     *C.RaftStore
	syncPeriod time.Duration
	asyncWrite bool

	committer struct {
		syncutil.Mutex
		cond       sync.Cond
		committing bool
		msgCnt     int
		pending    []*TsRaftWriteBatch
	}
	asyncer struct {
		syncutil.Mutex
		cond     sync.Cond
		closed   bool
		changing bool
		closeCh  chan interface{}
		waiting  bool
		pending  []*TsRaftWriteBatch
	}
}

// SetSyncPeriod sets syncPeriod
func (t *TsRaftLogEngine) SetSyncPeriod(period time.Duration) {
	if period > 100*time.Millisecond {
		t.asyncWrite = true
		t.syncPeriod = period
	} else {
		t.asyncWrite = false
	}
}

// IsAsyncWrite returns whether write raft log to disk async.
func (t *TsRaftLogEngine) IsAsyncWrite() bool {
	return t.asyncWrite
}

// GetSyncPeriod get sync period
func (t *TsRaftLogEngine) GetSyncPeriod() time.Duration {
	return t.syncPeriod
}

// NewTsRaftLogEngine create a new TsRaftlog store.
func NewTsRaftLogEngine(
	ctx context.Context, tsCfg TsRaftLogEngineConfig, stopper *stop.Stopper, sv *settings.Values,
) (*TsRaftLogEngine, error) {
	return nil, nil
}

// Close raft log engine
func (t *TsRaftLogEngine) Close() {
	close(t.asyncer.closeCh)
	t.asyncer.Lock()
	t.asyncer.closed = true
	t.asyncer.cond.Signal()
	t.asyncer.Unlock()
}

// asyncLoop asynchronous synchronous data
func (t *TsRaftLogEngine) asyncLoop() {
	// s := &t.asyncer
	// s.Lock()

	// var lastSync time.Time
	// timer := timeutil.NewTimer()
	// defer timer.Stop()

	// for {
	// 	if s.waiting && !s.closed {
	// 		s.cond.Wait()
	// 	}

	// 	if s.closed {
	// 		s.Unlock()
	// 		return
	// 	}

	// 	s.waiting = true
	// 	pending := s.pending
	// 	s.pending = nil

	// 	s.Unlock()

	// 	lastSync = timeutil.Now()
	// 	// status := C.TSSyncRaftLog(t.engine)
	// 	if err := statusToError(status); err != nil {
	// 		panic("failed sync raft log, err: " + err.Error())
	// 	}
	// 	for _, p := range pending {
	// 		p.commitWg.Done()
	// 	}
	// 	// Linux only guarantees we'll be notified of a writeback error once
	// 	// during a sync call. After sync fails once, we cannot rely on any
	// 	// future data written to WAL being crash-recoverable. That's because
	// 	// any future writes will be appended after a potential corruption in
	// 	// the WAL, and RocksDB's recovery terminates upon encountering any
	// 	// corruption. So, we must not call `DBSyncWAL` again after it has
	// 	// failed once.
	// 	for t.asyncWrite {
	// 		timer.Reset(t.syncPeriod - timeutil.Since(lastSync))
	// 		select {
	// 		case <-s.closeCh:
	// 			s.Lock()
	// 			if s.closed {
	// 				s.Unlock()
	// 				return
	// 			}
	// 			s.closeCh = make(chan interface{})
	// 			s.changing = false
	// 			s.Unlock()
	// 		case <-timer.C:
	// 			timer.Read = true
	// 		}
	// 		if timer.Read {
	// 			break
	// 		}
	// 		timer.Read = true
	// 	}

	// 	s.Lock()
	// }
}

// NotifySyncPeriodChange notify of the change in the synchronization cycle
func (t *TsRaftLogEngine) NotifySyncPeriodChange() {
	t.asyncer.Lock()
	defer t.asyncer.Unlock()
	if t.asyncer.changing {
		return
	}
	close(t.asyncer.closeCh)
	t.asyncer.changing = true
}

// GetRaftLog get the raftLog within the specified range.
func (t *TsRaftLogEngine) GetRaftLog(
	rangeID uint64, lIndex uint64, hIndex uint64, transfer func([]byte) error,
) error {
	// cnt := hIndex - lIndex
	// if cnt == 0 {
	// 	return nil
	// }
	// cTsSlice := make([]C.TSSlice, cnt)
	// // status := C.TSGetRaftLog(t.engine, C.uint64_t(rangeID), C.uint64_t(lIndex), C.uint64_t(hIndex), &cTsSlice[0])
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not get raft log")
	// }
	// for i := uint64(0); i < cnt; i++ {
	// 	// value := cSliceToUnsafeGoBytes(cTsSlice[i])
	// 	err := transfer(value)
	// 	// C.free(unsafe.Pointer(cTsSlice[i].data))
	// 	if err != nil {
	// 		if err == ErrExceedMaxBytes {
	// 			for j := i + 1; j < cnt; j++ {
	// 				// C.free(unsafe.Pointer(cTsSlice[j].data))
	// 			}
	// 			break
	// 		}
	// 		return err
	// 	}
	// }
	return nil
}

// GetLastIndex get the latest raft log index of the specified range.
func (t *TsRaftLogEngine) GetLastIndex(rangeID uint64) (uint64, error) {
	// var index C.uint64_t
	// status := C.TSGetLastIndex(t.engine, C.uint64_t(rangeID), &index)
	// if err := statusToError(status); err != nil {
	// 	return 0, errors.Wrap(err, "could not get raft log")
	// }
	// return uint64(index), nil
	return 0, nil
}

// GetTsFirstIndexTerm get the first index and term of raft log of the specified range.
// The index and term should be encoded by function transfer.
func (t *TsRaftLogEngine) GetTsFirstIndexTerm(rangeID uint64, transfer func([]byte) error) error {
	// var cTsSlice C.TSSlice
	// status := C.TSGetFirstRaftLog(t.engine, C.uint64_t(rangeID), &cTsSlice)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not get raft log")
	// }
	// value := cSliceToUnsafeGoBytes(cTsSlice)
	// err := transfer(value)
	// C.free(unsafe.Pointer(cTsSlice.data))
	// if err != nil {
	// 	return err
	// }
	return nil
}

// HasRange verify in the raftlog store whether the specified range already exists.
func (t *TsRaftLogEngine) HasRange(rangeID uint64) bool {
	// status := C.TSHasRange(t.engine, C.uint64_t(rangeID))
	// if err := statusToError(status); err != nil {
	// 	return false
	// }
	return true
}
