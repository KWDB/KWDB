// Copyright 2019 The Cockroach Authors.
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

package quotapool

import "sync"

// bufferSize is the size of the ringBuf buf served from a notifyQueueNodePool.
//
// Each node is 32+8*bufferSize bytes so at 28 a node is 256 bytes which
// feels like a nice number.
const bufferSize = 28

// notifyQueue provides an allocation efficient FIFO queue for chan struct{}.
//
// notifyQueue is not safe for concurrent use.
type notifyQueue struct {
	len  int64
	head *node

	pool *notifyQueueNodePool
}

// initializeNotifyQueue initializes a notifyQueue.
// notifyQueue values should not be used until they have been initialized.
// It is illegal to initialize a notifyQueue more than once and this function
// will panic if called with an already initialized notifyQueue.
func initializeNotifyQueue(q *notifyQueue) {
	if q.pool != nil {
		panic("cannot re-initialize a notifyQueue")
	}
	defaultNotifyQueueNodePool.initialize(q)
}

var defaultNotifyQueueNodePool = newNotifyQueueNodePool()

// enqueue adds c to the end of the queue and returns the address of the added
// notifyee.
func (q *notifyQueue) enqueue(c chan struct{}) (n *notifyee) {
	if q.head == nil {
		q.head = q.pool.pool.Get().(*node)
		q.head.prev = q.head
		q.head.next = q.head
	}
	tail := q.head.prev
	if n = tail.enqueue(c); n == nil {
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		if n = newTail.enqueue(c); n == nil {
			panic("failed to enqueue into a fresh buffer")
		}
	}
	q.len++
	return n
}

// dequeue removes the current head of the queue which can be accessed with
// peek().
func (q *notifyQueue) dequeue() {
	if q.head == nil {
		return
	}
	q.head.dequeue()
	if q.head.len == 0 {
		oldHead := q.head
		if oldHead.next == oldHead {
			q.head = nil
		} else {
			q.head = oldHead.next
			q.head.prev = oldHead.prev
			q.head.prev.next = q.head
		}
		*oldHead = node{}
		q.pool.pool.Put(oldHead)
	}
	q.len--
}

// peek returns the current head of the queue or nil if the queue is empty.
// It does not modify the queue. It is illegal to use the returned pointer after
// the next call to dequeue.
func (q *notifyQueue) peek() *notifyee {
	if q.head == nil {
		return nil
	}
	return &q.head.buf[q.head.head]
}

// notifyQueueNodePool constructs notifyQueue objects which internally pool
// their buffers.
type notifyQueueNodePool struct {
	pool sync.Pool
}

// newNotifyQueueNodePool returns a new notifyQueueNodePool which can be used
// to construct notifyQueues which internally pool their buffers.
func newNotifyQueueNodePool() *notifyQueueNodePool {
	return &notifyQueueNodePool{
		pool: sync.Pool{
			New: func() interface{} { return &node{} },
		},
	}
}

// initialize initializes a which will share a sync.Pool of nodes with the
// other notifyQueue instances initialized with this pool.
func (p *notifyQueueNodePool) initialize(q *notifyQueue) {
	*q = notifyQueue{pool: p}
}

type node struct {
	ringBuf
	prev, next *node
}

type notifyee struct {
	c chan struct{}
}

type ringBuf struct {
	buf  [bufferSize]notifyee
	head int64
	len  int64
}

func (rb *ringBuf) enqueue(c chan struct{}) *notifyee {
	if rb.len == bufferSize {
		return nil
	}
	i := (rb.head + rb.len) % bufferSize
	rb.buf[i] = notifyee{c: c}
	rb.len++
	return &rb.buf[i]
}

func (rb *ringBuf) dequeue() {
	// NB: the notifyQueue never contains an empty ringBuf.
	if rb.len == 0 {
		panic("cannot dequeue from an empty buffer")
	}
	rb.buf[rb.head] = notifyee{}
	rb.head++
	rb.head %= bufferSize
	rb.len--
}
