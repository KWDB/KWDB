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

package tse

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <string.h>
// #include <libkwdbts2.h>
import "C"
import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
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

// addRaftlog Add raftLog
func addRaftLog(raftLogs []C.TSRaftlog, off int, logs *RaftLogs) uint64 {
	const cLoneSize = unsafe.Sizeof(C.uint64_t(0))
	const cTsSliceSize = unsafe.Sizeof(C.TSSlice{})
	raftLogs[off].range_id = C.uint64_t(logs.rangeID)
	v := logs.value
	n := uint64(len(v))
	// to delete
	if n == 0 {
		if logs.start == logs.end {
			raftLogs[off].index_cnt = C.int(0)
			raftLogs[off].indexes = nil
		} else if logs.end == 0 {
			raftLogs[off].index_cnt = C.int(1)
			indexes := unsafe.Pointer(C.malloc(C.size_t(cLoneSize)))
			if indexes == nil {
				panic("failed malloc indexes")
			}
			*((*C.uint64_t)(indexes)) = C.uint64_t(logs.start)
			raftLogs[off].indexes = (*C.uint64_t)(indexes)
		} else {
			raftLogs[off].index_cnt = C.int(2)
			indexes := unsafe.Pointer(C.malloc(C.size_t(cLoneSize * 2)))
			if indexes == nil {
				panic("failed malloc indexes")
			}
			*((*C.uint64_t)(indexes)) = C.uint64_t(logs.start)
			*((*C.uint64_t)(unsafe.Pointer(uintptr(indexes) + cLoneSize))) = C.uint64_t(logs.end - 1)
			raftLogs[off].indexes = (*C.uint64_t)(indexes)
		}
		raftLogs[off].data = nil
		raftLogs[off].offs = nil
		return 0
	}
	// to put
	payload := unsafe.Pointer(C.malloc(C.size_t(logs.size)))
	if payload == nil {
		panic("failed malloc payload")
	}
	indexes := unsafe.Pointer(C.malloc(C.size_t(cLoneSize * uintptr(n))))
	if indexes == nil {
		panic("failed malloc indexes")
	}
	offs := unsafe.Pointer(C.malloc(C.size_t(cLoneSize * uintptr(n+1)))) // length n+1, the last is for the total size
	if offs == nil {
		panic("failed malloc indexes")
	}
	offPayload := uintptr(0)
	offUintArr := uintptr(0)
	size := uint64(0)
	for i := uint64(0); i < n; i++ {
		pPtr := unsafe.Pointer(uintptr(payload) + offPayload)
		iPtr := (*C.uint64_t)(unsafe.Pointer(uintptr(indexes) + offUintArr))
		oPtr := (*C.uint64_t)(unsafe.Pointer(uintptr(offs) + offUintArr))
		*oPtr = C.uint64_t(size)
		l := len(v[i])
		C.memcpy(pPtr, unsafe.Pointer(&v[i][0]), C.size_t(l))
		*iPtr = C.uint64_t(logs.start + i)
		size += uint64(l)

		offPayload += uintptr(l)
		offUintArr += cLoneSize
	}
	*(*C.uint64_t)(unsafe.Pointer(uintptr(offs) + offUintArr)) = C.uint64_t(size)
	raftLogs[off].index_cnt = C.int(n)
	raftLogs[off].indexes = (*C.uint64_t)(indexes)
	raftLogs[off].data = (*C.char)(payload)
	raftLogs[off].offs = (*C.uint64_t)(offs)
	return size
}

// Commit write the raftlog to the disk.
func (b *TsRaftWriteBatch) Commit() error {
	if len(b.logs) == 0 {
		return nil
	}
	c := &b.engine.committer
	b.commitWg.Add(1)
	c.Lock()
	leader := len(c.pending) == 0
	c.pending = append(c.pending, b)
	c.msgCnt += len(b.logs)
	if leader {
		for c.committing {
			c.cond.Wait()
		}
		pending := c.pending
		c.pending = nil
		c.committing = true
		cnt := c.msgCnt
		c.msgCnt = 0
		c.Unlock()
		raftLogs := make([]C.TSRaftlog, cnt)
		off := 0
		size := uint64(0)
		for _, p := range pending {
			for i := range p.logs {
				sz := addRaftLog(raftLogs, off, &p.logs[i])
				size += sz
				off++
			}
			p.logs = nil
		}
		if off != cnt {
			panic(fmt.Sprintf("off %d is not equal to cnt %d", off, cnt))
		}
		status := C.TSWriteRaftLog(b.engine.engine, C.int(cnt), (*C.TSRaftlog)(unsafe.Pointer(&raftLogs[0])), false)
		c.Lock()
		c.committing = false
		c.cond.Signal()
		c.Unlock()

		err := statusToError(status)
		err = errors.Wrap(err, "could not commit raft log")
		if err != nil {
			for _, p := range pending {
				p.commitErr = err
				p.commitWg.Done()
			}
			log.Errorf(context.Background(), "failed write raft log, err: %s", err)
			return err
		}
		if b.engine.asyncWrite {
			for _, p := range pending {
				p.commitErr = err
				p.commitWg.Done()
			}
			pending = nil
		}
		s := &b.engine.asyncer
		s.Lock()
		s.pending = append(s.pending, pending...)
		if s.waiting {
			s.waiting = false
			s.cond.Signal()
		}
		s.Unlock()

		for i := range raftLogs {
			if raftLogs[i].indexes != nil {
				C.free(unsafe.Pointer(raftLogs[i].indexes))
			}
			if raftLogs[i].data != nil {
				C.free(unsafe.Pointer(raftLogs[i].data))
			}
			if raftLogs[i].offs != nil {
				C.free(unsafe.Pointer(raftLogs[i].offs))
			}
		}
	} else {
		c.Unlock()
	}
	b.commitWg.Wait()
	return b.commitErr
}

// TsRaftLogEngine is ts database instance.
type TsRaftLogEngine struct {
	cfg        TsRaftLogEngineConfig
	engine     *C.RaftStore
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
	engine := &TsRaftLogEngine{
		cfg: tsCfg,
	}
	status := C.TSRaftOpen(&engine.engine, goToTSSlice([]byte(tsCfg.Dir)))
	if err := statusToError(status); err != nil {
		return nil, errors.Wrap(err, "could not open tsengine instance")
	}
	engine.committer.cond.L = &engine.committer.Mutex
	engine.asyncer.cond.L = &engine.asyncer.Mutex
	engine.asyncer.closeCh = make(chan interface{})
	engine.asyncer.waiting = true

	engine.SetSyncPeriod(TsRaftLogSyncPeriod.Get(sv))
	stopper.AddCloser(engine)
	go engine.asyncLoop()
	return engine, nil
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
	s := &t.asyncer
	s.Lock()

	var lastSync time.Time
	timer := timeutil.NewTimer()
	defer timer.Stop()

	for {
		if s.waiting && !s.closed {
			s.cond.Wait()
		}

		if s.closed {
			s.Unlock()
			return
		}

		s.waiting = true
		pending := s.pending
		s.pending = nil

		s.Unlock()

		lastSync = timeutil.Now()
		status := C.TSSyncRaftLog(t.engine)
		if err := statusToError(status); err != nil {
			panic("failed sync raft log, err: " + err.Error())
		}
		for _, p := range pending {
			p.commitWg.Done()
		}
		// Linux only guarantees we'll be notified of a writeback error once
		// during a sync call. After sync fails once, we cannot rely on any
		// future data written to WAL being crash-recoverable. That's because
		// any future writes will be appended after a potential corruption in
		// the WAL, and RocksDB's recovery terminates upon encountering any
		// corruption. So, we must not call `DBSyncWAL` again after it has
		// failed once.
		for t.asyncWrite {
			timer.Reset(t.syncPeriod - timeutil.Since(lastSync))
			select {
			case <-s.closeCh:
				s.Lock()
				if s.closed {
					s.Unlock()
					return
				}
				s.closeCh = make(chan interface{})
				s.changing = false
				s.Unlock()
			case <-timer.C:
				timer.Read = true
			}
			if timer.Read {
				break
			}
			timer.Read = true
		}

		s.Lock()
	}
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
	cnt := hIndex - lIndex
	if cnt == 0 {
		return nil
	}
	cTsSlice := make([]C.TSSlice, cnt)
	status := C.TSGetRaftLog(t.engine, C.uint64_t(rangeID), C.uint64_t(lIndex), C.uint64_t(hIndex), &cTsSlice[0])
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not get raft log")
	}
	for i := uint64(0); i < cnt; i++ {
		value := cSliceToUnsafeGoBytes(cTsSlice[i])
		err := transfer(value)
		C.free(unsafe.Pointer(cTsSlice[i].data))
		if err != nil {
			if err == ErrExceedMaxBytes {
				for j := i + 1; j < cnt; j++ {
					C.free(unsafe.Pointer(cTsSlice[j].data))
				}
				break
			}
			return err
		}
	}
	return nil
}

// GetLastIndex get the latest raft log index of the specified range.
func (t *TsRaftLogEngine) GetLastIndex(rangeID uint64) (uint64, error) {
	var index C.uint64_t
	status := C.TSGetLastIndex(t.engine, C.uint64_t(rangeID), &index)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "could not get raft log")
	}
	return uint64(index), nil
}

// GetTsFirstIndexTerm get the first index and term of raft log of the specified range.
// The index and term should be encoded by function transfer.
func (t *TsRaftLogEngine) GetTsFirstIndexTerm(rangeID uint64, transfer func([]byte) error) error {
	var cTsSlice C.TSSlice
	status := C.TSGetFirstRaftLog(t.engine, C.uint64_t(rangeID), &cTsSlice)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not get raft log")
	}
	value := cSliceToUnsafeGoBytes(cTsSlice)
	err := transfer(value)
	C.free(unsafe.Pointer(cTsSlice.data))
	if err != nil {
		return err
	}
	return nil
}

// HasRange verify in the raftlog store whether the specified range already exists.
func (t *TsRaftLogEngine) HasRange(rangeID uint64) bool {
	status := C.TSHasRange(t.engine, C.uint64_t(rangeID))
	if err := statusToError(status); err != nil {
		return false
	}
	return true
}
