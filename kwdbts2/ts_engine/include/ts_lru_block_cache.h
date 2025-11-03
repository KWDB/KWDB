// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#pragma once

#include <math.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "ts_entity_segment.h"

namespace kwdbts {

/* Used to manage all the cached blocks in the whole system among queries
 * to reduce decompression of the blocks to speed up block access.
 * Currently it only support entity segment blocks, we need to expand it
 * later to support last segment blocks, and then we can implement last
 * segment compression to reduce the size of last segment files.
*/
class TsLRUBlockCache {
#ifdef WITH_TESTS
/* Following is only for unit test.
*/
 public:
  enum UNIT_TEST_PHASE {
    PHASE_NONE,
    PHASE_FIRST_INITIALIZING,
    PHASE_SECOND_ACCESS_DONE,
    PHASE_SECOND_GOING_TO_INITIALIZE,
    COLUMN_BLOCK_CRASH_PHASE_NONE,
    COLUMN_BLOCK_CRASH_PHASE_FIRST_INITIALIZING,
    COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE,
    VAR_COLUMN_BLOCK_CRASH_PHASE_NONE,
    VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_ONE_DONE,
    VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_GET_VAR_COL_ADDR_DONE,
    VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_TRY_GETTING_VAR_COLUMN_BLOCK,
    VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE,
    VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE
  };
  UNIT_TEST_PHASE unit_test_phase{PHASE_NONE};
  bool unit_test_enabled{false};

  // Check if current memmory size is accurate
  bool VerifyCacheMemorySize();
#endif

 public:
  static TsLRUBlockCache& GetInstance() {
    static TsLRUBlockCache block_cache(EngineOptions::block_cache_max_size);
    return block_cache;
  }
  explicit TsLRUBlockCache(uint64_t max_memory_size);
  ~TsLRUBlockCache();

  // Add a new TsEntityBlock to block cache.
  bool Add(std::shared_ptr<TsEntityBlock>& block);
  // Access a TsEntityBlock which will move this block to the head of the doubly linked list
  void Access(std::shared_ptr<TsEntityBlock>& block);
  // Get cache hit count
  uint32_t GetCacheHitCount();
  // Get cache miss count
  uint32_t GetCacheMissCount();
  // Get block cache hit and miss count;
  void GetRecentHitInfo(uint32_t* hit_count, uint32_t* miss_count, uint32_t* memory_size);
  // Get cache hit ratio
  float GetHitRatio();
  // Adjust max_memory_size_
  void SetMaxMemorySize(uint64_t max_memory_size);
  // Increase memory size after new column block is created, will kick off some blocks if total exceeds max size
  bool AddMemory(TsEntityBlock* block, uint32_t new_memory_size);
  // Evict all the ts blocks in the cache
  void EvictAll();

  // For unit tests
  uint64_t GetMemorySize();

 private:
  std::shared_ptr<TsEntityBlock> head_{nullptr};
  std::shared_ptr<TsEntityBlock> tail_{nullptr};
  uint64_t max_memory_size_;
  uint64_t cur_memory_size_{0};
  std::mutex lock_;
  // MAX_VISIT_CACHE_HISTORY_COUNT must be 2 ^ n
  static const uint32_t MAX_VISIT_CACHE_HISTORY_COUNT = 16384;
  // std::vector<bool> storage was optimized for boolean values by packing them into individual bits.
  std::vector<bool> cache_hit_miss_bits_;
  int32_t current_visit_cache_index_{-1};
  bool max_visit_cache_count_reached{false};

  void KickOffBlocks();
};

}  // namespace kwdbts
