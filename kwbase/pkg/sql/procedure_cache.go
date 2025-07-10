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

package sql

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// ProcedureCache can be used by multiple threads in parallel.
type ProcedureCache struct {
	totalMem int64
	shards   []*ProcedureCacheShard
}

// ProcedureCacheShard is a procedure cache shard, keyed on procedure ID.
type ProcedureCacheShard struct {
	ctx context.Context

	totalShardMem int64

	mu struct {
		syncutil.Mutex

		availableShardMem int64

		// Sentinel list entries. All entries are part of either the used or the
		// free circular list. Any entry in the used list has a corresponding entry
		// in the map. The used list is in LRU order.
		used, free entry

		// Map with an entry for each used entry.
		m map[ProcedureInfo]*entry
	}
}

// DefaultProcedureCacheShardNum represents Procedure cache shard number.
const DefaultProcedureCacheShardNum = 32

// avgCachedSize is used to preallocate the number of "slots" in the cache.
// Specifically, the cache will be able to store at most
// (<size> / avgCachedSize) queries, even if their memory usage is small.
const avgCachedSize = 8 * 1024

// We disallow very large queries from being added to the cache.
const maxCachedSize = 128 * 1024

// CachedData is the data associated with a cache entry.
type CachedData struct {
	ProcedureInfo ProcedureInfo
	ProcedureFn   *memo.Memo
	// Only test
	ExtraStr string
}

// ProcedureInfo is the key of the cache entry.
type ProcedureInfo struct {
	ProcedureID uint32
}

func (cd *CachedData) memoryEstimate() int64 {
	var res int64
	res = int64(4) + cd.ProcedureFn.MemoryEstimate() + int64(len(cd.ExtraStr))
	return res
}

// entry in a circular linked list.
type entry struct {
	CachedData

	// Linked list pointers.
	prev, next *entry
}

// clear resets the CachedData in the entry.
func (e *entry) clear(ctx context.Context) {
	e.CachedData = CachedData{}
}

// remove removes the entry from the linked list it is part of.
func (e *entry) remove() {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
}

func (e *entry) insertAfter(a *entry) {
	b := a.next

	e.prev = a
	e.next = b

	a.next = e
	b.prev = e
}

// New creates a procedure cache of the given size.
func New(ctx context.Context, memorySize int64, shardCount int8) *ProcedureCache {
	c := &ProcedureCache{totalMem: memorySize}
	shardMem := memorySize / int64(shardCount)

	numShardEntries := shardMem / avgCachedSize
	c.shards = make([]*ProcedureCacheShard, shardCount)
	for j := 0; j < int(shardCount); j++ {
		cs := &ProcedureCacheShard{ctx: ctx, totalShardMem: shardMem}
		cs.mu.availableShardMem = shardMem
		cs.mu.m = make(map[ProcedureInfo]*entry, numShardEntries)
		entries := make([]entry, numShardEntries)
		// The used list is empty.
		cs.mu.used.next = &cs.mu.used
		cs.mu.used.prev = &cs.mu.used
		// Make a linked list of entries, starting with the sentinel.
		cs.mu.free.next = &entries[0]
		cs.mu.free.prev = &entries[numShardEntries-1]
		for i := range entries {
			if i > 0 {
				entries[i].prev = &entries[i-1]
			} else {
				entries[i].prev = &cs.mu.free
			}
			if i+1 < len(entries) {
				entries[i].next = &entries[i+1]
			} else {
				entries[i].next = &cs.mu.free
			}
		}
		c.shards[j] = cs
	}

	return c
}

// Find returns the entry for the given query, if it is in the cache.
//
// If any cached data needs to be updated, it must be done via Add.
func (c *ProcedureCache) Find(procedureInfo ProcedureInfo) *CachedData {
	shard := c.getShard(procedureInfo.ProcedureID)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	e := shard.mu.m[procedureInfo]
	if e == nil {
		return nil
	}

	// Move the entry to the front of the used list.
	e.remove()
	e.insertAfter(&shard.mu.used)
	return &e.CachedData
}

// Add adds an entry to the cache (possibly evicting some other entry). If the
// cache already has a corresponding entry for d.ProcedureID, it is updated.
func (c *ProcedureCache) Add(d *CachedData) {
	shard := c.getShard(d.ProcedureInfo.ProcedureID)
	mem := d.memoryEstimate()
	if d.ProcedureInfo.ProcedureID == 0 || mem > maxCachedSize || mem > shard.totalShardMem {
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	e, ok := shard.mu.m[d.ProcedureInfo]
	if ok {
		// The query already exists in the cache.
		e.remove()
		shard.mu.availableShardMem += e.memoryEstimate()
	} else {
		// Get an entry to use for this procedure.
		e = shard.getEntry()
		shard.mu.m[d.ProcedureInfo] = e
	}

	e.CachedData = *d

	// Evict more entries if necessary.
	shard.makeSpace(mem)
	shard.mu.availableShardMem -= mem

	// Insert the entry at the front of the used list.
	e.insertAfter(&shard.mu.used)
}

// makeSpace evicts entries from the used list until we have enough free space.
func (c *ProcedureCacheShard) makeSpace(needed int64) {
	for c.mu.availableShardMem < needed {
		// Evict entries as necessary, putting them in the free list.
		c.evict().insertAfter(&c.mu.free)
	}
}

// Evicts the last item in the used list.
func (c *ProcedureCacheShard) evict() *entry {
	e := c.mu.used.prev
	if e == &c.mu.used {
		panic("no more used entries")
	}
	e.remove()
	c.mu.availableShardMem += e.memoryEstimate()
	delete(c.mu.m, e.ProcedureInfo)
	e.clear(c.ctx)

	return e
}

// getEntry returns an entry that can be used for adding a new procedure to the
// cache. If there are free entries, one is returned; otherwise, a used entry is
// evicted.
func (c *ProcedureCacheShard) getEntry() *entry {
	if e := c.mu.free.next; e != &c.mu.free {
		e.remove()
		return e
	}
	// No free entries, we must evict an entry.
	return c.evict()
}

// Clear removes all the entries from the cache.
func (c *ProcedureCache) Clear() {
	for _, cs := range c.shards {
		cs.mu.Lock()

		// Clear the map.
		for sql, e := range cs.mu.m {
			cs.mu.availableShardMem += e.memoryEstimate()
			delete(cs.mu.m, sql)
			e.remove()
			e.clear(cs.ctx)
			e.insertAfter(&cs.mu.free)
		}
		cs.mu.Unlock()
	}
}

// Purge removes the entry for the given procedure id, if it exists.
func (c *ProcedureCache) Purge(procedureID uint32) {
	shard := c.getShard(procedureID)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	for k, v := range shard.mu.m {
		if k.ProcedureID == procedureID {
			shard.mu.availableShardMem += v.memoryEstimate()
			delete(shard.mu.m, k)
			v.clear(shard.ctx)
			v.remove()
			v.insertAfter(&shard.mu.free)
		}
	}
}

// check performs various assertions on the internal consistency of the cache
// structures. Used by testing code.
func (c *ProcedureCache) check() {
	for _, cs := range c.shards {
		cs.mu.Lock()
		// Verify that all entries in the used list have a corresponding entry in the
		// map, and that the memory accounting adds up.
		numUsed := 0
		memUsed := int64(0)
		for e := cs.mu.used.next; e != &cs.mu.used; e = e.next {
			numUsed++
			memUsed += e.memoryEstimate()
			if e.ProcedureInfo.ProcedureID == 0 {
				panic("used entry with empty procedure")
			}
			if me, ok := cs.mu.m[e.ProcedureInfo]; !ok {
				panic(fmt.Sprintf("used entry %d not in map", e.ProcedureInfo.ProcedureID))
			} else if e != me {
				panic(fmt.Sprintf("map entry for %d doesn't match used entry", e.ProcedureInfo.ProcedureID))
			}
		}

		if numUsed != len(cs.mu.m) {
			panic(fmt.Sprintf("map length %d doesn't match used list size %d", len(cs.mu.m), numUsed))
		}

		if memUsed+cs.mu.availableShardMem != cs.totalShardMem {
			panic(fmt.Sprintf(
				"memory usage doesn't add up: used=%d available=%d total=%d",
				memUsed, cs.mu.availableShardMem, cs.totalShardMem,
			))
		}

		cs.mu.Unlock()
	}
}

func (c *ProcedureCache) getShard(key uint32) *ProcedureCacheShard {
	return c.shards[key%uint32(len(c.shards))]
}
