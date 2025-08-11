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

#include "ts_mem_seg_index.h"

namespace kwdbts {

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

TsMemSegIndex::TsMemSegIndex(BaseAllocator* allocator, int32_t max_height, int32_t branching_factor)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      kBranching_(static_cast<uint16_t>(branching_factor)),
      kScaledInverseBranching_((Random::MAX_NEXT + 1) / kBranching_),
      allocator_(allocator),
      compare_(),
      head_(AllocateNode(0, max_height)),
      skiplist_max_height_(1),
      seq_splice_(AllocateSkiplistSplice()) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);

  for (int i = 0; i < kMaxHeight_; ++i) {
    head_->SetNext(i, nullptr);
  }
}

char* TsMemSegIndex::AllocateKeyValue(size_t key_size) {
  return const_cast<char*>(AllocateNode(key_size, RandomHeight())->Key());
}

SkipListNode* TsMemSegIndex::AllocateNode(size_t key_size, int height) {
  auto prefix = sizeof(std::atomic<SkipListNode*>) * (height - 1);
  char* raw = allocator_->AllocateAligned(prefix + sizeof(SkipListNode) + key_size);
  SkipListNode* x = reinterpret_cast<SkipListNode*>(raw + prefix);
  x->StashHeight(height);
  return x;
}

SkipListSplice* TsMemSegIndex::AllocateSkiplistSplice() {
  // size of prev_ and next_
  size_t array_size = sizeof(SkipListNode*) * (kMaxHeight_ + 1);
  char* raw = allocator_->AllocateAligned(sizeof(SkipListSplice) + array_size * 2);
  SkipListSplice* splice = reinterpret_cast<SkipListSplice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<SkipListNode**>(raw + sizeof(SkipListSplice));
  splice->next_ = reinterpret_cast<SkipListNode**>(raw + sizeof(SkipListSplice) + array_size);
  return splice;
}

bool TsMemSegIndex::InsertRowData(const TSMemSegRowData& row, uint32_t row_idx) {
  size_t malloc_size = sizeof(TSMemSegRowData) + row.row_data.len + TSMemSegRowData::GetKeyLen();
  char* buf = AllocateKeyValue(malloc_size);
  if (buf != nullptr) {
    TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(buf + +TSMemSegRowData::GetKeyLen());
    memcpy(cur_row, &row, sizeof(TSMemSegRowData));
    cur_row->row_data.data = buf + sizeof(TSMemSegRowData) + TSMemSegRowData::GetKeyLen();
    cur_row->row_data.len = row.row_data.len;
    cur_row->row_idx_in_mem_seg = row_idx;
    memcpy(cur_row->row_data.data, row.row_data.data, row.row_data.len);
    cur_row->GenKey(buf);
    return InsertWithCAS(buf);
  }
  return false;
}

bool TsMemSegIndex::InsertWithCAS(const char* key) {
  SkipListNode* prev[kMaxPossibleHeight];
  SkipListNode* next[kMaxPossibleHeight];
  SkipListSplice cur_splice;
  cur_splice.prev_ = prev;
  cur_splice.next_ = next;
  SkipListNode* x = reinterpret_cast<SkipListNode*>(const_cast<char*>(key)) - 1;
  const TSMemSegRowData* key_decoded = compare_.DecodeKeyValue(key);
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  int max_height = skiplist_max_height_.load(std::memory_order_relaxed);
  while (height > max_height) {
    if (skiplist_max_height_.compare_exchange_weak(max_height, height)) {
      // successfully updated it
      max_height = height;
      break;
    }
  }
  assert(max_height <= kMaxPossibleHeight);

  int recompute_height = 0;
  if (cur_splice.height_ < max_height) {
    cur_splice.prev_[max_height] = head_;
    cur_splice.next_[max_height] = nullptr;
    cur_splice.height_ = max_height;
    recompute_height = max_height;
  } else {
    while (recompute_height < max_height) {
      if (cur_splice.prev_[recompute_height]->Next(recompute_height) !=
          cur_splice.next_[recompute_height]) {
        ++recompute_height;
      } else if (cur_splice.prev_[recompute_height] != head_ &&
                 !KeyIsAfterNode(key_decoded,
                                 cur_splice.prev_[recompute_height])) {
        recompute_height = max_height;
      } else if (KeyIsAfterNode(key_decoded,
                                cur_splice.next_[recompute_height])) {
        recompute_height = max_height;
      } else {
        break;
      }
    }
  }
  assert(recompute_height <= max_height);
  if (recompute_height > 0) {
    RecomputeSpliceLevels(key_decoded, &cur_splice, recompute_height);
  }

  bool splice_is_valid = true;
  {
    for (int i = 0; i < height; ++i) {
      while (true) {
        // Checking for duplicate keys on the level 0 is sufficient
        if (UNLIKELY(i == 0 && cur_splice.next_[i] != nullptr &&
                     compare_(x->Key(), cur_splice.next_[i]->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        if (UNLIKELY(i == 0 && cur_splice.prev_[i] != head_ &&
                     compare_(cur_splice.prev_[i]->Key(), x->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        assert(cur_splice.next_[i] == nullptr ||
               compare_(x->Key(), cur_splice.next_[i]->Key()) < 0);
        assert(cur_splice.prev_[i] == head_ ||
               compare_(cur_splice.prev_[i]->Key(), x->Key()) < 0);
        x->NoBarrier_SetNext(i, cur_splice.next_[i]);
        if (cur_splice.prev_[i]->CASNext(i, cur_splice.next_[i], x)) {
          // success
          break;
        }
        // CAS failed, we need to recompute prev and next. It is unlikely
        // to be helpful to try to use a different level as we redo the
        // search, because it should be unlikely that lots of nodes have
        // been inserted between prev[i] and next[i]. No point in using
        // next[i] as the after hint, because we know it is stale.
        FindSpliceForLevel<false>(key_decoded, cur_splice.prev_[i], nullptr, i,
                                  &cur_splice.prev_[i], &cur_splice.next_[i]);

        // Since we've narrowed the bracket for level i, we might have
        // violated the SkipListSplice constraint between i and i-1.  Make sure
        // we recompute the whole thing next time.
        if (i > 0) {
          splice_is_valid = false;
        }
      }
    }
  }
  if (splice_is_valid) {
    for (int i = 0; i < height; ++i) {
      cur_splice.prev_[i] = x;
    }
    assert(cur_splice.prev_[cur_splice.height_] == head_);
    assert(cur_splice.next_[cur_splice.height_] == nullptr);
    for (int i = 0; i < cur_splice.height_; ++i) {
      assert(cur_splice.next_[i] == nullptr ||
             compare_(key, cur_splice.next_[i]->Key()) < 0);
      assert(cur_splice.prev_[i] == head_ ||
             compare_(cur_splice.prev_[i]->Key(), key) <= 0);
      assert(cur_splice.prev_[i + 1] == cur_splice.prev_[i] ||
             cur_splice.prev_[i + 1] == head_ ||
             compare_(cur_splice.prev_[i + 1]->Key(), cur_splice.prev_[i]->Key()) <
                 0);
      assert(cur_splice.next_[i + 1] == cur_splice.next_[i] ||
             cur_splice.next_[i + 1] == nullptr ||
             compare_(cur_splice.next_[i]->Key(), cur_splice.next_[i + 1]->Key()) <
                 0);
    }
  } else {
    cur_splice.height_ = 0;
  }
  return true;
}

void TsMemSegIndex::RecomputeSpliceLevels(const TSMemSegRowData*& key,
                                                       SkipListSplice* splice,
                                                       int recompute_level) {
  assert(recompute_level > 0);
  assert(recompute_level <= splice->height_);
  for (int i = recompute_level - 1; i >= 0; --i) {
    FindSpliceForLevel<true>(key, splice->prev_[i + 1], splice->next_[i + 1], i,
                       &splice->prev_[i], &splice->next_[i]);
  }
}

bool TsMemSegIndex::Contains(const char* key) const {
  SkipListNode* x = FindGreaterOrEqual(key);
  if (x != nullptr && Equal(key, x->Key())) {
    return true;
  } else {
    return false;
  }
}

int TsMemSegIndex::RandomHeight() {
  auto rnd = Random::GetInstance();
  int height = 1;
  while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
         rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight_);
  assert(height <= kMaxPossibleHeight);
  return height;
}

bool TsMemSegIndex::KeyIsAfterNode(const char* key, SkipListNode* n) const {
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

bool TsMemSegIndex::KeyIsAfterNode(const TSMemSegRowData*& key, SkipListNode* n) const {
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

SkipListNode* TsMemSegIndex::FindGreaterOrEqual(const char* key) const {
  SkipListNode* x = head_;
  int level = GetMaxHeight() - 1;
  SkipListNode* last_bigger = nullptr;
  const TSMemSegRowData* key_decoded = compare_.DecodeKeyValue(key);
  while (true) {
    SkipListNode* next = x->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    // Make sure the lists are sorted
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    // Make sure we haven't overshot during our search
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    int cmp = (next == nullptr || next == last_bigger)
                  ? 1
                  : compare_(next->Key(), key_decoded);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      return next;
    } else if (cmp < 0) {
      // Keep searching in this list
      x = next;
    } else {
      // Switch to next list, reuse compare_() result
      last_bigger = next;
      level--;
    }
  }
}


}  //  namespace kwdbts
