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

#include <mutex>
#include <vector>
#include "ts_lru_block_cache.h"

namespace kwdbts {

TsLRUBlockCache::TsLRUBlockCache(uint64_t max_memory_size) : max_memory_size_(max_memory_size),
                                                        cache_hit_miss_bits_(MAX_VISIT_CACHE_HISTORY_COUNT) {}

TsLRUBlockCache::~TsLRUBlockCache() {
  while (head_) {
    std::shared_ptr<TsEntityBlock> block = head_;
    head_ = head_->next_;
    block->pre_ = nullptr;
    block->next_ = nullptr;
    block->RemoveFromSegment();
  }
  tail_ = nullptr;
}

bool TsLRUBlockCache::Add(std::shared_ptr<TsEntityBlock>& block) {
  assert(block != nullptr);
  block->pre_ = nullptr;
  lock_.lock();
  ++current_visit_cache_index_;
  if (current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    max_visit_cache_count_reached = true;
    current_visit_cache_index_ &= MAX_VISIT_CACHE_HISTORY_COUNT - 1;
  }
  int cur_index = current_visit_cache_index_;
  block->next_ = head_;
  if (head_) {
    head_->pre_ = block;
  } else {
    tail_ = block;
  }
  head_ = block;
  lock_.unlock();
  cache_hit_miss_bits_[cur_index] = false;
  // We ignore the memory of a block without loading any data.
  return true;
}
inline void TsLRUBlockCache::KickOffBlocks() {
  while (cur_memory_size_ > max_memory_size_ && tail_) {
    std::shared_ptr<TsEntityBlock> tail_block = std::move(tail_);
    tail_ = tail_block->pre_;
    if (tail_) {
      tail_->next_ = nullptr;
    } else {
      head_ = nullptr;
    }
    tail_block->pre_ = nullptr;
    cur_memory_size_ -= tail_block->GetMemorySize();
    tail_block->RemoveFromSegment();
  }
}
bool TsLRUBlockCache::AddMemory(TsEntityBlock* block, uint32_t new_memory_size) {
  lock_.lock();
  block->AddMemory(new_memory_size);
  if (block->pre_ || (head_ && head_.get() == block)) {
    cur_memory_size_ += new_memory_size;
    KickOffBlocks();
  } else {
    // Don't need to do anything since block should be head_ or has been removed from doubly linked list.
  }
  lock_.unlock();
  return true;
}

void TsLRUBlockCache::Access(std::shared_ptr<TsEntityBlock>& block) {
  assert(block != nullptr);
  lock_.lock();
  ++current_visit_cache_index_;
  if (current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    max_visit_cache_count_reached = true;
    current_visit_cache_index_ &= MAX_VISIT_CACHE_HISTORY_COUNT - 1;
  }
  int cur_index = current_visit_cache_index_;
  if (block->pre_) {
    head_->pre_ = std::move(block->pre_->next_);
    block->pre_->next_ = std::move(block->next_);
    block->next_ = std::move(head_);
    if (block->pre_->next_) {
      head_ = std::move(block->pre_->next_->pre_);
      block->pre_->next_->pre_ = std::move(block->pre_);
    } else {
      head_ = std::move(tail_);
      tail_ = std::move(block->pre_);
    }
  } else {
    // Don't need to do anything since block should be head_ or has been removed from doubly linked list.
  }
  lock_.unlock();
  cache_hit_miss_bits_[cur_index] = true;
}

void TsLRUBlockCache::SetMaxMemorySize(uint64_t max_memory_size) {
  lock_.lock();
  max_memory_size_ = max_memory_size;
  KickOffBlocks();
  lock_.unlock();
}

uint64_t TsLRUBlockCache::GetMemorySize() {
  return cur_memory_size_;
}

void TsLRUBlockCache::EvictAll() {
  lock_.lock();
  while (head_) {
    std::shared_ptr<TsEntityBlock> tail_block = tail_;
    tail_ = tail_->pre_;
    tail_block->pre_ = nullptr;
    tail_block->next_ = nullptr;
    tail_block->RemoveFromSegment();
    if (tail_) {
      tail_->next_ = nullptr;
    } else {
      head_ = tail_;
    }
  }
  cur_memory_size_ = 0;
  lock_.unlock();
}

uint32_t TsLRUBlockCache::GetCacheHitCount() {
  int cur_index = current_visit_cache_index_;
  if (max_visit_cache_count_reached || current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    return std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.end(), true);
  } else if (cur_index >= 0) {
    return std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.begin() + cur_index + 1, true);
  } else {
    // None of entity block is touched so far
    return 0;
  }
}

uint32_t TsLRUBlockCache::GetCacheMissCount() {
  int cur_index = current_visit_cache_index_;
  if (max_visit_cache_count_reached || current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    return std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.end(), false);
  } else if (cur_index >= 0) {
    return std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.begin() + cur_index + 1, false);
  } else {
    // None of entity block is touched so far
    return 0;
  }
}

float TsLRUBlockCache::GetHitRatio() {
  int hit_count;
  int cur_index = current_visit_cache_index_;
  if (max_visit_cache_count_reached || current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    hit_count = std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.end(), true);
    return static_cast<float>(hit_count) / static_cast<float>(MAX_VISIT_CACHE_HISTORY_COUNT);
  } else if (cur_index >= 0) {
    hit_count = std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.begin() + cur_index + 1, true);
    return static_cast<float>(hit_count) / static_cast<float>(cur_index + 1);
  } else {
    // None of entity block is touched so far
    return 0;
  }
}

void TsLRUBlockCache::GetRecentHitInfo(uint32_t* hit_count, uint32_t* miss_count, uint64_t* memory_size) {
  lock_.lock();
  if (max_visit_cache_count_reached || current_visit_cache_index_ >= MAX_VISIT_CACHE_HISTORY_COUNT) {
    *hit_count = std::count(cache_hit_miss_bits_.begin(), cache_hit_miss_bits_.end(), true);
    *miss_count = MAX_VISIT_CACHE_HISTORY_COUNT - *hit_count;
  } else if (current_visit_cache_index_ >= 0) {
    *hit_count = std::count(cache_hit_miss_bits_.begin(),
                            cache_hit_miss_bits_.begin() + current_visit_cache_index_ + 1, true);
    *miss_count = current_visit_cache_index_ + 1 - *hit_count;
  } else {
    *hit_count = 0;
    *miss_count = 0;
  }
  *memory_size = cur_memory_size_;
  lock_.unlock();
  return;
}

#ifdef WITH_TESTS
bool TsLRUBlockCache::VerifyCacheMemorySize() {
  bool passed;
  lock_.lock();
  uint64_t memory_size = 0;
  std::shared_ptr<TsEntityBlock> block = head_;
  while (block) {
    memory_size += block->GetMemorySize();
    block = block->next_;
  }
  passed = (memory_size == cur_memory_size_);
  lock_.unlock();
  return passed;
}
#endif

}  // namespace kwdbts
