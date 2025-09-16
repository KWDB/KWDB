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

#include <vector>
#include "ts_lru_block_cache.h"

namespace kwdbts {

TsLRUBlockCache::TsLRUBlockCache(uint32_t max_blocks) : max_blocks_(max_blocks) {}

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
  if (cur_block_num_ >= max_blocks_) {
    if (tail_ == nullptr) {
      lock_.unlock();
      // Something is wrong with LRU block cache, head_ should not be tail_.
      return false;
    }
    std::shared_ptr<TsEntityBlock> tail_block = tail_;
    tail_ = tail_->pre_;
    if (tail_) {
      tail_->next_ = nullptr;
    } else {
      // In this case, max_blocks_ is set to 0.
      head_ = nullptr;
    }
    tail_block->pre_ = nullptr;
    lock_.unlock();
    tail_block->RemoveFromSegment();
  } else {
    cur_block_num_++;
    lock_.unlock();
  }
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

void TsLRUBlockCache::SetMaxBlocks(uint32_t max_blocks) {
  lock_.lock();
  if (cur_block_num_ > max_blocks) {
    for (int i = 0; i < cur_block_num_ - max_blocks; ++i) {
      std::shared_ptr<TsEntityBlock> tail_block = tail_;
      tail_ = tail_->pre_;
      tail_block->pre_ = nullptr;
      tail_block->next_ = nullptr;
      tail_block->RemoveFromSegment();
    }
    if (tail_) {
      tail_->next_ = nullptr;
    } else {
      head_ = tail_;
    }
    cur_block_num_ = max_blocks;
  }
  max_blocks_ = max_blocks;
  lock_.unlock();
}

uint32_t TsLRUBlockCache::Count() {
  return cur_block_num_;
}

}  // namespace kwdbts
