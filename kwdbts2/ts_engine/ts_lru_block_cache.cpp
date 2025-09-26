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

TsLRUBlockCache::TsLRUBlockCache(uint64_t max_memory_size) : max_memory_size_(max_memory_size) {}

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
  block->next_ = head_;
  if (head_) {
    head_->pre_ = block;
  } else {
    tail_ = block;
  }
  head_ = block;
  lock_.unlock();
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

}  // namespace kwdbts
