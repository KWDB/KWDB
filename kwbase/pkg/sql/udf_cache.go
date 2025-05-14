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
//
// This file provides functionalities for managing user-defined functions (UDFs) within a SQL database system.
// This file specifically implements caching mechanisms and gossip-based synchronization to manage UDF lifecycle events such as creation, update, and deletion.
// It is designed to enhance the efficiency of UDF operations across distributed database instances by minimizing direct database accesses and
// leveraging a gossip protocol to propagate UDF changes. The primary use case is to support database environments where UDFs can be defined and
// modified by users, requiring real-time updates and high availability across clusters.
//
// Design:
// 1. UDFEntry: Represents a single UDF and its associated metadata within the cache. It includes mechanisms to handle concurrent access and to ensure data integrity during updates.
// 2. UDFCache: Manages a collection of UDFEntries, providing methods to add, update, fetch, and delete UDFs from the cache.
//              It integrates with the database's internal gossip system to receive updates about UDF changes.
//
// The design utilizes a locking mechanism via syncutil.Mutex to ensure thread safety and prevent data races in a concurrent environment.
//
// Additionally, the UDFCache subscribes to gossip updates to maintain cache consistency with changes distributed across the database cluster.
// This approach reduces the overhead of frequently accessing the database for UDF definitions by keeping a local, up-to-date copy in cache.
//
// Special handling is provided for scenarios where multiple goroutines may attempt to refresh a UDF concurrently,
// ensuring that only one refresh operation occurs at a time while others wait or are coalesced into a single subsequent refresh operation if needed during an ongoing refresh.

package sql

import (
	"context"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/cache"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

// The UDFEntry stores *cacheEntry objects. The caches are protected by the
// cache-wide mutex.
type UDFEntry struct {
	// If mustWait is true, we do not have any udf and we are in the process
	// of fetching the udf from the database. Other callers
	// can wait on the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	Fn *tree.FunctionDefinition
	// TODO(zh): support replace
	// If refreshing is true, the current udf-fn for this udfName is stale,
	// and we are in the process of fetching the updated udf from the database.
	//
	// If a goroutine tries to perform a refresh when a refresh is already
	// in progress, it will see that refreshing=true and will set the
	// mustRefreshAgain flag to true before returning. When the original
	// goroutine that was performing the refresh returns from the database and
	// sees that mustRefreshAgain=true, it will trigger another refresh.
	refreshing       bool
	mustRefreshAgain bool

	// err is populated if the internal query to retrieve udf hit an error.
	err error
}

// UDFCache is used for user defined cache
type UDFCache struct {
	mu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
		// Used for testing; keeps track of how many times we actually read udf
		// from the system table.
		numInternalQueries int64
	}
	Gossip      *gossip.Gossip
	ClientDB    *kv.DB
	SQLExecutor sqlutil.InternalExecutor
}

// NewUDFCache creates a new udf cache that can hold
// udf for <cacheSize> udf.
func NewUDFCache(
	cacheSize int, g *gossip.Gossip, db *kv.DB, sqlExecutor sqlutil.InternalExecutor,
) *UDFCache {
	udfCache := &UDFCache{
		Gossip:      g,
		ClientDB:    db,
		SQLExecutor: sqlExecutor,
	}
	udfCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})

	g.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyUDFUpdatedPrefix),
		udfCache.udfUpdatedGossipUpdate,
		gossip.Redundant,
	)
	g.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyUDFDeletedPrefix),
		udfCache.udfDeletedGossipUpdate,
		gossip.Redundant,
	)
	return udfCache
}

// udfUpdatedGossipUpdate is the gossip callback that fires when a new
// udf is available.
func (uc *UDFCache) udfUpdatedGossipUpdate(key string, value roachpb.Value) {
	udfName, err := gossip.UDFNameFromUDFUpdatedKey(key)
	if err != nil {
		log.Errorf(context.Background(), "udfUpdatedGossipUpdate(%s) error: %v", key, err)
		return
	}
	uc.RefreshUDF(context.Background(), udfName)
}

// udfUpdatedGossipUpdate is the gossip callback that fires when a
// udf is unavailable.
func (uc *UDFCache) udfDeletedGossipUpdate(key string, value roachpb.Value) {
	udfName, err := gossip.UDFNameFromUDFDeletedKey(key)
	if err != nil {
		log.Errorf(context.Background(), "udfDeletedGossipUpdate(%s) error: %v", key, err)
		return
	}
	uc.DeleteUDF(context.Background(), udfName)
}

// GetUDF is used to get udf cache
func (uc *UDFCache) GetUDF(ctx context.Context, udfName string) (*UDFEntry, error) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	// First try to get the UDF from cache
	if found, entry := uc.lookupUdfLocked(ctx, udfName); found {
		return entry, entry.err
	}

	return uc.addCacheEntryLocked(ctx, udfName)
}

// addCacheEntryLocked creates a new cache entry and retrieves udf
// from the database. It does this in a way so that the other goroutines that
// need the same udf can wait on us:
//   - an cache entry with wait=true is created;
//   - mutex is unlocked;
//   - udf are retrieved from database:
//   - mutex is locked again and the entry is updated.
func (uc *UDFCache) addCacheEntryLocked(ctx context.Context, udfName string) (*UDFEntry, error) {
	// Create a new cache entry, other coroutines will wait on this entry until we get the UDF
	entry := &UDFEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &uc.mu},
	}
	uc.mu.cache.Add(udfName, entry)

	func() {
		uc.mu.Unlock()
		defer uc.mu.Lock()

		log.VEventf(ctx, 0, "reading udf '%s'", udfName)
		var err error
		entry.Fn, err = uc.getUdfFromDB(ctx, udfName)
		if err != nil {
			entry.err = err
			log.Errorf(ctx, "error reading udf '%s': %v", udfName, err)
		}
		log.VEventf(ctx, 0, "finished reading udf '%s'", udfName)
	}()

	// The update is completed and other coroutines are allowed to continue.
	entry.mustWait = false
	entry.waitCond.Broadcast()

	if entry.err != nil {
		uc.mu.cache.Del(udfName)
		return nil, entry.err
	}

	return entry, nil
}

// RefreshUDF refreshes the cached UDF for the given UDF name
// by fetching the new definition from a central repository or database.
func (uc *UDFCache) RefreshUDF(ctx context.Context, udfName string) {
	log.VEventf(ctx, 1, "refreshing udf '%s'", udfName)
	ctx, span := tracing.ForkCtxSpan(ctx, "refresh-udf")
	// Perform a asynchronous refresh of the cache.
	go func() {
		defer tracing.FinishSpan(span)
		uc.refreshUDFCacheEntry(ctx, udfName)
	}()
}

// refreshUDFCacheEntry retrieves a UDF from the database and updates an existing cache entry or adds a new one if the UDF is not already cached.
// This function is designed to ensure that UDF entries within the cache are up-to-date with the latest versions stored in the database.
//
// Parameters:
// - ctx: Context which provides deadlines, cancellation signals, and other request-scoped values across API boundaries and between processes.
// - udfName: The name of the user-defined function to be refreshed.
//
// Workflow:
//  1. The function first attempts to locate the UDF in the cache.
//  2. If the UDF is not found in the cache, it creates a new cache entry and other goroutines will wait on this entry until the UDF data is fetched from the database.
//  3. If the UDF is found and no other refresh is in progress, it proceeds to refresh the UDF; if another refresh is already in progress,
//     it sets a flag to indicate that a subsequent refresh is needed.
//  4. It fetches the UDF definition from the database. If another refresh becomes necessary during this operation (indicated by the mustRefreshAgain flag), the refresh loop continues.
//  5. Once the UDF is successfully fetched and no further refreshes are required, the function updates the UDF entry in the cache and releases any waiting goroutines.
//  6. If an error occurs during the database fetch, the entry is removed from the cache to ensure that subsequent attempts will retry the fetch from the database.
//
// The function utilizes locking to ensure thread safety and prevent concurrent modification issues during the refresh operation.
// This approach helps maintain consistency and integrity of the UDF data within the cache.
func (uc *UDFCache) refreshUDFCacheEntry(ctx context.Context, udfName string) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	// If the udf does not exist in the cache, then we add a new udf cache;
	// TODO(ZH): if the udf already exists in the cache, then refresh it;
	found, e := uc.lookupUdfLocked(ctx, udfName)

	if !found {
		// Create a new cache entry, other coroutines will wait on this entry until we get the UDF
		e = &UDFEntry{
			mustWait: true,
			waitCond: sync.Cond{L: &uc.mu},
		}
		uc.mu.cache.Add(udfName, e)
	} else {
		// Don't perform a refresh if a refresh is already in progress, but let that
		// goroutine know it needs to refresh again.
		if e.refreshing {
			e.mustRefreshAgain = true
			return
		}
		e.refreshing = true
	}

	var fn *tree.FunctionDefinition
	var err error
	for {
		func() {
			uc.mu.numInternalQueries++
			uc.mu.Unlock()
			defer uc.mu.Lock()

			log.VEventf(ctx, 0, "refreshing udf '%s'\n", udfName)
			fn, err = uc.getUdfFromDB(ctx, udfName)
			log.VEventf(ctx, 0, "done refreshing udf '%s'\n", udfName)
		}()
		if !e.mustRefreshAgain {
			break
		}
		e.mustRefreshAgain = false
	}

	e.Fn, e.err = fn, err
	tree.ConcurrentFunDefs.RegisterFunc(udfName, fn)
	e.refreshing = false

	if !found {
		// The update is completed and other coroutines are allowed to continue.
		e.mustWait = false
		e.waitCond.Broadcast()
	}

	if err != nil {
		// Don't keep the cache entry around, so that we retry the query.
		uc.mu.cache.Del(udfName)
	}
}

// lookupUdfLocked retrieves any existing udf for the given table.
//
// If another goroutine is in the process of retrieving the same udf, this
// method waits until that completes.
//
// Assumes that the caller holds sc.mu. Note that the mutex can be unlocked and
// locked again if we need to wait (this can only happen when found=true).
func (uc *UDFCache) lookupUdfLocked(ctx context.Context, udfName string) (found bool, e *UDFEntry) {
	eUntyped, ok := uc.mu.cache.Get(udfName)
	if !ok {
		return false, nil
	}
	e = eUntyped.(*UDFEntry)

	if e.mustWait {
		// If another coroutine is operating this UDF, wait until the fetch is complete
		log.VEventf(ctx, 1, "waiting for udf '%s'", udfName)
		e.waitCond.Wait()
		log.VEventf(ctx, 1, "finished waiting for udf '%s'", udfName)
	} else {
		if log.V(2) {
			log.Infof(ctx, "udf '%s' found in cache", udfName)
		}
	}
	return true, e
}

// DeleteUDF is used to delete udf
func (uc *UDFCache) DeleteUDF(ctx context.Context, udfName string) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	if _, found := uc.mu.cache.Get(udfName); found {
		log.VEventf(ctx, 0, "deleting udf %s from cache", udfName)
		uc.mu.cache.Del(udfName)
	} else {
		log.VEventf(ctx, 0, "udf '%s' not found in cache, no need to delete", udfName)
	}
	tree.ConcurrentFunDefs.DeleteFunc(udfName)
	const deleteUdfQuery = `
	   Delete 
	   FROM system.user_defined_function
	   WHERE function_name = $1
	 `
	_, err := uc.SQLExecutor.Query(ctx, "Delete-udf", nil /* txn */, deleteUdfQuery, udfName)
	if err != nil {
		log.Errorf(context.Background(), "delete udf(%s) error: %v", udfName, err)
		return
	}
}

// getUdfFromDB is used to get user defined function
func (uc *UDFCache) getUdfFromDB(
	ctx context.Context, udfName string,
) (*tree.FunctionDefinition, error) {
	const getUdfQuery = `
	 SELECT 
   function_name, 
   argument_types,
   return_type,
   types_length,
   function_body
	 FROM system.user_defined_function
	 WHERE function_name = $1
	 LIMIT 1
	 `

	rows, err := uc.SQLExecutor.Query(ctx, "Get-udf", nil /* txn */, getUdfQuery, udfName)
	if err != nil {
		return nil, err
	}

	var udfFn *tree.FunctionDefinition
	for _, row := range rows {
		udf, err := builtins.RegisterLuaUDFs(row)
		if err != nil {
			return nil, err
		}
		udfFn = udf
	}

	return udfFn, nil
}

// GossipUdfAdded causes the UDF caches for this UDF to be validated.
func GossipUdfAdded(g *gossip.Gossip, udfName string) error {
	return g.AddInfo(
		gossip.MakeUdfAddedKey(udfName),
		nil, /* value */
		0,   /* ttl */
	)
}

// GossipUdfDeleted causes the UDF caches for this UDF to be invalidated.
func GossipUdfDeleted(g *gossip.Gossip, udfName string) error {
	return g.AddInfo(
		gossip.MakeUdfDeletedKey(udfName),
		nil, /* value */
		0,   /* ttl */
	)
}

// LoadUDF loads UDFs from the system table, registers them in the global cache,
// and adds them to the UDFCache.
func (uc *UDFCache) LoadUDF(ctx context.Context, stopper *stop.Stopper) error {
	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		const loadUdfQuery = `
    SELECT 
        function_name, 
        argument_types, 
        return_type, 
        types_length, 
        function_body
    FROM system.user_defined_function
    `

		rows, err := uc.SQLExecutor.Query(ctx, "Load-udf", nil /* txn */, loadUdfQuery)
		if err != nil {
			log.Errorf(ctx, "error loading udf(s): %v", err)
			return
		}

		for _, row := range rows {
			// Assuming row[0] contains the function_name.
			udfName := string(*row[0].(*tree.DString))

			udfFn, err := builtins.RegisterLuaUDFs(row)
			if err != nil {
				log.Errorf(ctx, "error registering udf %s: %v", udfName, err)
				continue // Skip this UDF and try to load the next one
			}

			// Add the loaded UDF to the global function definitions cache.
			tree.ConcurrentFunDefs.RegisterFunc(udfName, udfFn)

			// Create a new UDFEntry for the loaded UDF.
			entry := &UDFEntry{
				Fn: udfFn,
			}

			// Acquire the lock to safely update the cache.
			uc.mu.Lock()
			// Add the UDF entry to the UDFCache.
			uc.mu.cache.Add(udfName, entry)
			uc.mu.Unlock()
		}

		log.VEventf(ctx, 1, "loaded udf(s) and updated udf cache successfully")
	})
	return nil
}
