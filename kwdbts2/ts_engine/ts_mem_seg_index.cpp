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

TsMemSegIndex::TsMemSegIndex(int32_t max_height, int32_t branching_factor)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      kBranching_(static_cast<uint16_t>(branching_factor)),
      kScaledInverseBranching_((FakeRandom::MAX_NEXT + 1) / kBranching_),
      allocator_(),
      compare_(),
      head_node_(AllocateNode(0, max_height)),
      skiplist_max_height_(1),
      seq_splice_(AllocateSkiplistSplice()) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);

  for (int i = 0; i < kMaxHeight_; ++i) {
    head_node_->SetNext(i, nullptr);
  }
}

char* TsMemSegIndex::AllocateKeyValue(size_t key_size) {
  return const_cast<char*>(AllocateNode(key_size, RandomHeight())->Key());
}

SkipListNode* TsMemSegIndex::AllocateNode(size_t key_size, int sl_height) {
  auto prefix = sizeof(std::atomic<SkipListNode*>) * (sl_height - 1);
  char* raw = allocator_.AllocateAligned(prefix + sizeof(SkipListNode) + key_size);
  SkipListNode* x = reinterpret_cast<SkipListNode*>(raw + prefix);
  x->StashHeight(sl_height);
  return x;
}

SkipListSplice* TsMemSegIndex::AllocateSkiplistSplice() {
  // size of prev_ and next_
  size_t array_size = sizeof(SkipListNode*) * (kMaxHeight_ + 1);
  char* raw = allocator_.AllocateAligned(sizeof(SkipListSplice) + array_size * 2);
  SkipListSplice* splice = reinterpret_cast<SkipListSplice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<SkipListNode**>(raw + sizeof(SkipListSplice));
  splice->next_ = reinterpret_cast<SkipListNode**>(raw + sizeof(SkipListSplice) + array_size);
  return splice;
}

bool TsMemSegIndex::InsertRowData(const TSMemSegRowData& row) {
  size_t malloc_size = sizeof(TSMemSegRowData) + row.row_data.len + TSMemSegRowData::GetKeyLen();
  char* buf = AllocateKeyValue(malloc_size);
  if (buf == nullptr) {
    return false;
  }
  TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(buf + TSMemSegRowData::GetKeyLen());
  memcpy(cur_row, &row, sizeof(TSMemSegRowData));
  cur_row->row_data.data = buf + sizeof(TSMemSegRowData) + TSMemSegRowData::GetKeyLen();
  cur_row->row_data.len = row.row_data.len;
  memcpy(cur_row->row_data.data, row.row_data.data, row.row_data.len);
  cur_row->GenKey(buf);
  InsertWithCAS(buf);
  return true;
}

void TsMemSegIndex::InsertWithCAS(const char* key) {
  SkipListNode* prev[kMaxPossibleHeight];
  SkipListNode* next_node[kMaxPossibleHeight];
  SkipListSplice cur_splice;
  cur_splice.prev_ = prev;
  cur_splice.next_ = next_node;
  SkipListNode* x = reinterpret_cast<SkipListNode*>(const_cast<char*>(key)) - 1;
  const TSMemSegRowData* key_decoded = compare_.DecodeKeyValue(key);
  int sl_height = x->UnstashHeight();
  assert(sl_height >= 1 && sl_height <= kMaxHeight_);

  int max_height = skiplist_max_height_.load(std::memory_order_relaxed);
  while (sl_height > max_height) {
    if (skiplist_max_height_.compare_exchange_weak(max_height, sl_height)) {
      // success
      max_height = sl_height;
      break;
    }
  }
  assert(max_height <= kMaxPossibleHeight);

  cur_splice.prev_[max_height] = head_node_;
  cur_splice.next_[max_height] = nullptr;
  cur_splice.height_ = max_height;

  RecomputeSpliceLevels(key_decoded, &cur_splice, max_height);

  bool splice_valid = true;
  {
    for (int i = 0; i < sl_height; ++i) {
      while (true) {
        x->NoBarrier_SetNext(i, cur_splice.next_[i]);
        if (cur_splice.prev_[i]->CASNext(i, cur_splice.next_[i], x)) {
          // insert success
          break;
        }
        FindSpliceForLevel<false>(key_decoded, cur_splice.prev_[i], nullptr, i, &cur_splice.prev_[i],
                                  &cur_splice.next_[i]);
      }
    }
  }
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

int TsMemSegIndex::RandomHeight() {
  auto rnd = FakeRandom::GetInstance();
  int sl_height = 1;
  while (sl_height < kMaxHeight_ && sl_height < kMaxPossibleHeight && rnd->Next() < kScaledInverseBranching_) {
    sl_height++;
  }
  assert(sl_height > 0);
  assert(sl_height <= kMaxHeight_);
  assert(sl_height <= kMaxPossibleHeight);
  return sl_height;
}

bool TsMemSegIndex::IsKeyAfterNode(const char* key, SkipListNode* n) const {
  assert(n != head_node_);
  return (n != nullptr) && (compare_(n->Key(), key) <= 0);
}

bool TsMemSegIndex::IsKeyAfterNode(const TSMemSegRowData*& key, SkipListNode* n) const {
  assert(n != head_node_);
  return (n != nullptr) && (compare_(n->Key(), key) <= 0);
}

SkipListNode* TsMemSegIndex::FindGreaterOrEqual(const char* key) const {
  SkipListNode* x = head_node_;
  const TSMemSegRowData* key_decoded = compare_.DecodeKeyValue(key);
  int level = GetMaxHeight() - 1;
  SkipListNode* last_bigger_node = nullptr;
  while (true) {
    SkipListNode* next_node = x->Next(level);
    if (next_node != nullptr) {
      PREFETCH(next_node->Next(level), 0, 1);
    }
    assert(x == head_node_ || IsKeyAfterNode(key_decoded, x));
    assert(x == head_node_ || next_node == nullptr || IsKeyAfterNode(next_node->Key(), x));
    int cmp = (next_node == nullptr || next_node == last_bigger_node) ? 1 : compare_(next_node->Key(), key_decoded);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      return next_node;
    } else if (cmp < 0) {
      x = next_node;
    } else {
      last_bigger_node = next_node;
      level--;
    }
  }
}


}  //  namespace kwdbts
