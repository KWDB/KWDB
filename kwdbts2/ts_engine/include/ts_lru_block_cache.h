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

#include <memory>
#include <mutex>
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
    COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE
  };
  UNIT_TEST_PHASE unit_test_phase{PHASE_NONE};
  bool unit_test_enabled{false};
#endif

 public:
  static TsLRUBlockCache& GetInstance() {
    static TsLRUBlockCache block_cache(EngineOptions::block_cache_max_size);
    return block_cache;
  }
  explicit TsLRUBlockCache(uint32_t max_blocks);
  ~TsLRUBlockCache();

  // Add a new TsEntityBlock to block cache.
  bool Add(std::shared_ptr<TsEntityBlock>& block);
  // Access a TsEntityBlock which will move this block to the head of the doubly linked list
  void Access(std::shared_ptr<TsEntityBlock>& block);
  // Adjust max_blocks_
  void SetMaxBlocks(uint32_t max_blocks);

  // For unit tests
  uint32_t Count();

 private:
  std::shared_ptr<TsEntityBlock> head_{nullptr};
  std::shared_ptr<TsEntityBlock> tail_{nullptr};
  uint32_t max_blocks_;
  uint32_t cur_block_num_{0};
  std::mutex lock_;
};

}  // namespace kwdbts
