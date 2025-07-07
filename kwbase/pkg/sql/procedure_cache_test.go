// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func toStr(c *ProcedureCache) string {
	c.check()

	var b strings.Builder
	for _, cs := range c.shards {
		cs.mu.Lock()
		for e := cs.mu.used.next; e != &cs.mu.used; e = e.next {
			if b.Len() != 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.Itoa(int(e.ProcedureInfo.ProcedureID)))
		}
		cs.mu.Unlock()
	}

	return b.String()
}

func expect(t *testing.T, c *ProcedureCache, exp string) {
	t.Helper()
	if s := toStr(c); s != exp {
		t.Errorf("expected %s, got %s", exp, s)
	}
}

func data(procedureID uint32, procedureName string, mem *memo.Memo, memEstimate int64) *CachedData {
	procedureInfo := ProcedureInfo{ProcedureID: procedureID}
	cd := &CachedData{ProcedureInfo: procedureInfo, ProcedureFn: mem}
	n := memEstimate - cd.memoryEstimate()
	if n < 0 {
		panic(fmt.Sprintf("size %d too small", memEstimate))
	}
	// Add characters to AnonymizedStr which should increase the estimate.
	s := make([]byte, n)
	for i := range s {
		s[i] = 'x'
	}
	cd.ExtraStr = string(s)
	if cd.memoryEstimate() != memEstimate {
		panic(fmt.Sprintf("failed to create CachedData of size %d", memEstimate))
	}
	return cd
}

// TestCache tests the main operations of the cache.
func TestCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sa := &memo.Memo{}
	sb := &memo.Memo{}
	sc := &memo.Memo{}
	sd := &memo.Memo{}

	// In this test, all entries have the same size: avgCachedSize.
	c := New(context.Background(), 6*avgCachedSize, 1)

	expect(t, c, "")
	c.Add(data(1, "1", sa, 2*avgCachedSize))
	expect(t, c, "1")
	c.Add(data(2, "2", sb, 2*avgCachedSize))
	expect(t, c, "2,1")
	c.Add(data(3, "3", sc, 2*avgCachedSize))
	expect(t, c, "3,2,1")
	c.Add(data(4, "4", sd, 2*avgCachedSize))
	expect(t, c, "4,3,2")
	if cacheData := c.Find(ProcedureInfo{ProcedureID: 1}); cacheData != nil {
		t.Errorf("1 shouldn't be in the cache")
	}
	if cacheData := c.Find(ProcedureInfo{ProcedureID: 3}); cacheData == nil {
		t.Errorf("c should be in the cache")
	} else if cacheData.ProcedureFn != sc {
		t.Errorf("invalid Memo for c")
	}
	expect(t, c, "3,4,2")

	if cacheData := c.Find(ProcedureInfo{ProcedureID: 2}); cacheData == nil {
		t.Errorf("b should be in the cache")
	} else if cacheData.ProcedureFn != sb {
		t.Errorf("invalid Memo for b")
	}
	expect(t, c, "2,3,4")

	c.Add(data(1, "1", sa, 2*avgCachedSize))
	expect(t, c, "1,2,3")

	c.Purge(2)
	expect(t, c, "1,3")
	if cacheData := c.Find(ProcedureInfo{ProcedureID: 2}); cacheData != nil {
		t.Errorf("b shouldn't be in the cache")
	}

	c.Purge(3)
	expect(t, c, "1")

	c.Add(data(2, "2", sb, 2*avgCachedSize))
	expect(t, c, "2,1")

	c.Clear()
	expect(t, c, "")
	if cacheData := c.Find(ProcedureInfo{ProcedureID: 2}); cacheData != nil {
		t.Errorf("2 shouldn't be in the cache")
	}
}

func TestCacheMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := &memo.Memo{}

	c := New(context.Background(), 20*avgCachedSize, 1)
	expect(t, c, "")
	for i := 0; i < 10; i++ {
		c.Add(data(uint32(i+1), fmt.Sprintf("%d", i), m, 2*avgCachedSize))
	}
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify handling when we have no more entries.
	c.Add(data(10, "10", m, 2*avgCachedSize))
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify handling when we have larger entries.
	c.Add(data(11, "large", m, avgCachedSize*10))
	expect(t, c, "11,10,9,8,7,6")
	c.Add(data(12, "verylarge", m, avgCachedSize*20))
	expect(t, c, "11,10,9,8,7,6")

	for i := 0; i < 10; i++ {
		c.Add(data(uint32(i+1), fmt.Sprintf("%d", i), m, 2*avgCachedSize))
	}
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify that we don't try to add an entry that's larger than the cache size.
	c.Add(data(11, "large", m, avgCachedSize*21))
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify handling when we update an existing entry with one that uses more
	// memory.
	c.Add(data(6, "5", m, avgCachedSize*10))
	expect(t, c, "6,10,9,8,7,5")

	c.Add(data(1, "0", m, 2*avgCachedSize))
	expect(t, c, "1,6,10,9,8,7")

	// Verify handling when we update an existing entry with one that uses less
	// memory.
	c.Add(data(6, "5", m, 2*avgCachedSize))
	expect(t, c, "6,1,10,9,8,7")
	c.Add(data(2, "1", m, 2*avgCachedSize))
	c.Add(data(3, "2", m, 2*avgCachedSize))
	c.Add(data(4, "3", m, 2*avgCachedSize))
	c.Add(data(5, "4", m, 2*avgCachedSize))
	expect(t, c, "5,4,3,2,6,1,10,9,8,7")

	// Verify Purge updates the available memory.
	c.Purge(4)
	expect(t, c, "5,3,2,6,1,10,9,8,7")
	c.Add(data(13, "x", m, 2*avgCachedSize))
	expect(t, c, "13,5,3,2,6,1,10,9,8,7")
	c.Add(data(14, "y", m, 2*avgCachedSize))
	expect(t, c, "14,13,5,3,2,6,1,10,9,8")
}

// TestSynchronization verifies that the cache doesn't crash (or cause a race
// detector error) when multiple goroutines are using it in parallel.
func TestSynchronization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const size = 1000
	c := New(context.Background(), size*avgCachedSize, 32)

	var wg sync.WaitGroup
	const goroutines = 200
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			rng, _ := randutil.NewPseudoRand()
			for j := 0; j < 5000; j++ {
				sql := rng.Intn(2 * size)
				switch r := rng.Intn(100); {
				case r == 0:
					// 1% of the time, clear the entire cache.
					c.Clear()
				case r <= 10:
					// 10% of the time, purge an entry.
					c.Purge(uint32(sql))
				case r <= 35:
					// 25% of the time, add an entry.
					c.Add(data(uint32(sql), strconv.Itoa(sql), &memo.Memo{}, int64(avgCachedSize+10+rng.Intn(10*avgCachedSize))))
				default:
					// The rest of the time, find an entry.
					_ = c.Find(ProcedureInfo{ProcedureID: uint32(sql)})
				}
				c.check()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
