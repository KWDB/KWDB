// Copyright 2016 The Cockroach Authors.
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

// +build !deadlock
// +build race

package syncutil

import (
	"sync"
	"sync/atomic"
)

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	mu      sync.Mutex
	wLocked int32 // updated atomically
}

// Lock locks m.
func (m *Mutex) Lock() {
	m.mu.Lock()
	atomic.StoreInt32(&m.wLocked, 1)
}

// Unlock unlocks m.
func (m *Mutex) Unlock() {
	atomic.StoreInt32(&m.wLocked, 0)
	m.mu.Unlock()
}

// AssertHeld may panic if the mutex is not locked (but it is not required to
// do so). Functions which require that their callers hold a particular lock
// may use this to enforce this requirement more directly than relying on the
// race detector.
//
// Note that we do not require the lock to be held by any particular thread,
// just that some thread holds the lock. This is both more efficient and allows
// for rare cases where a mutex is locked in one thread and used in another.
func (m *Mutex) AssertHeld() {
	if atomic.LoadInt32(&m.wLocked) == 0 {
		panic("mutex is not write locked")
	}
}

// An RWMutex is a reader/writer mutual exclusion lock.
type RWMutex struct {
	sync.RWMutex
	wLocked int32 // updated atomically
	rLocked int32 // updated atomically
}

// Lock locks rw for writing.
func (rw *RWMutex) Lock() {
	rw.RWMutex.Lock()
	atomic.StoreInt32(&rw.wLocked, 1)
}

// Unlock unlocks rw for writing.
func (rw *RWMutex) Unlock() {
	atomic.StoreInt32(&rw.wLocked, 0)
	rw.RWMutex.Unlock()
}

// RLock locks m for reading.
func (rw *RWMutex) RLock() {
	rw.RWMutex.RLock()
	atomic.AddInt32(&rw.rLocked, 1)
}

// RUnlock undoes a single RLock call.
func (rw *RWMutex) RUnlock() {
	atomic.AddInt32(&rw.rLocked, -1)
	rw.RWMutex.RUnlock()
}

// RLocker returns a Locker interface that implements
// the Lock and Unlock methods by calling rw.RLock and rw.RUnlock.
func (rw *RWMutex) RLocker() sync.Locker {
	return (*rlocker)(rw)
}

type rlocker RWMutex

func (r *rlocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*RWMutex)(r).RUnlock() }

// AssertHeld may panic if the mutex is not locked for writing (but it is not
// required to do so). Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the exclusive lock to be held by any particular
// thread, just that some thread holds the lock. This is both more efficient
// and allows for rare cases where a mutex is locked in one thread and used in
// another.
func (rw *RWMutex) AssertHeld() {
	if atomic.LoadInt32(&rw.wLocked) == 0 {
		panic("mutex is not write locked")
	}
}

// AssertRHeld may panic if the mutex is not locked for reading (but it is not
// required to do so). If the mutex is locked for writing, it is also considered
// to be locked for reading. Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the shared lock to be held by any particular
// thread, just that some thread holds the lock. This is both more efficient
// and allows for rare cases where a mutex is locked in one thread and used in
// another.
func (rw *RWMutex) AssertRHeld() {
	if atomic.LoadInt32(&rw.wLocked) == 0 && atomic.LoadInt32(&rw.rLocked) == 0 {
		panic("mutex is not read locked")
	}
}
