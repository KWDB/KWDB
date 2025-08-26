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

#include <assert.h>
#include <stdlib.h>
#include <algorithm>
#include <atomic>
#include <type_traits>

#include "ts_arena.h"

namespace kwdbts {

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

// store row-based data struct for certain entity.
#pragma pack(1)
struct TSMemSegRowData {
  TSTableID table_id;
  uint32_t table_version;
  TSEntityID entity_id;
  timestamp64 ts;
  TS_LSN lsn;
  uint32_t database_id;
  TSSlice row_data;

 private:
  char* skip_list_key_;

 public:
  TSMemSegRowData(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id) {
    database_id = db_id;
    table_id = tbl_id;
    table_version = tbl_version;
    entity_id = en_id;
  }
  inline bool operator<(const TSMemSegRowData& b) const {
    return Compare(b) < 0;
  }
  void SetData(timestamp64 cts, TS_LSN clsn, const TSSlice& crow_data) {
    ts = cts;
    lsn = clsn;
    row_data = crow_data;
  }
  static constexpr size_t GetKeyLen() {
    return 8 + 4 + 8 + 8 + 8 + 4;
  }

#define HTOBEFUNC(buf, value, size) { \
  auto tmp = value; \
  memcpy(buf, &tmp, size); \
  buf += size; \
}

  void GenKey(char* buf) {
    skip_list_key_ = buf;
    HTOBEFUNC(buf, htobe64(table_id), sizeof(table_id));
    HTOBEFUNC(buf, htobe32(table_version), sizeof(table_version));
    HTOBEFUNC(buf, htobe64(entity_id), sizeof(entity_id));
    uint64_t cts = ts - INT64_MIN;
    HTOBEFUNC(buf, htobe64(cts), sizeof(cts));
    HTOBEFUNC(buf, htobe64(lsn), sizeof(lsn));
    HTOBEFUNC(buf, htobe32(database_id), sizeof(database_id));
  }

  inline bool SameEntityAndTableVersion(TSMemSegRowData* b) {
    return memcmp(this, b, 20) == 0;
  }
  inline bool SameEntityAndTs(TSMemSegRowData* b) {
    return entity_id == b->entity_id && ts == b->ts;
  }
  inline bool SameTableId(TSMemSegRowData* b) {
    return this->table_id == b->table_id;
  }

  inline int Compare(const TSMemSegRowData& b) const { return memcmp(skip_list_key_, b.skip_list_key_, GetKeyLen()); }
};
#pragma pack()

struct TSRowDataComparator {
  inline TSMemSegRowData* DecodeKeyValue(const char* b) const {
    return reinterpret_cast<TSMemSegRowData*>(const_cast<char*>(b + TSMemSegRowData::GetKeyLen()));
  }

  int operator()(const char* a, const char* b) const { return memcmp(a, b, TSMemSegRowData::GetKeyLen()); }

  int operator()(const char* a, const TSMemSegRowData* b) const {
    return memcmp(a, reinterpret_cast<const char*>(b) - TSMemSegRowData::GetKeyLen(), TSMemSegRowData::GetKeyLen());
  }
};

// skiplist node, one node is one row data.
struct SkipListNode {
 private:
  std::atomic<SkipListNode*> next_[1];

 public:
  void StashHeight(const int height) {
    assert(sizeof(int) <= sizeof(next_[0]));
    memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
  }

  int UnstashHeight() const {
    int rv;
    memcpy(&rv, &next_[0], sizeof(int));
    return rv;
  }

  const char* Key() const { return reinterpret_cast<const char*>(&next_[1]); }

  SkipListNode* Next(int n) {
    assert(n >= 0);
    return ((&next_[0] - n)->load(std::memory_order_acquire));
  }

  void SetNext(int n, SkipListNode* x) {
    assert(n >= 0);
    (&next_[0] - n)->store(x, std::memory_order_release);
  }

  bool CASNext(int n, SkipListNode* expected, SkipListNode* x) {
    assert(n >= 0);
    return (&next_[0] - n)->compare_exchange_strong(expected, x, std::memory_order_release);
  }

  SkipListNode* NoBarrier_Next(int n) {
    assert(n >= 0);
    return (&next_[0] - n)->load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, SkipListNode* x) {
    assert(n >= 0);
    (&next_[0] - n)->store(x, std::memory_order_relaxed);
  }

  void InsertAfter(SkipListNode* prev, int level) {
    NoBarrier_SetNext(level, prev->NoBarrier_Next(level));
    prev->SetNext(level, this);
  }
};

// skiplist insert postition for every level.
struct SkipListSplice {
  int height_ = 0;
  SkipListNode** prev_;
  SkipListNode** next_;
};

class SkiplistIterator;

// using skiplist to index data in memory segment.
class TsMemSegIndex {
 private:
  const uint16_t kMaxHeight_;
  const uint16_t kBranching_;
  const uint32_t kScaledInverseBranching_;

  ConcurrentAllocator allocator_;
  TSRowDataComparator const compare_;
  SkipListNode* const head_node_;
  std::atomic<int> skiplist_max_height_;
  SkipListSplice* seq_splice_;

 public:
  static const uint16_t kMaxPossibleHeight = 32;

  explicit TsMemSegIndex(int32_t max_height = 12, int32_t branching_factor = 4);

  auto & GetAllocator() {
    return allocator_;
  }

  inline char* AllocateKeyValue(size_t key_size);

  inline SkipListSplice* AllocateSkiplistSplice();

  void InsertWithCAS(const char* key);

  bool InsertRowData(const TSMemSegRowData& row);

  inline TSMemSegRowData* ParseKey(const char* key) {
    return compare_.DecodeKeyValue(key);
  }

  inline int GetMaxHeight() const {
    return skiplist_max_height_.load(std::memory_order_relaxed);
  }

  inline int RandomHeight();

  inline SkipListNode* AllocateNode(size_t key_size, int height);

  inline bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  inline bool LessThan(const char* a, const char* b) const {
    return (compare_(a, b) < 0);
  }

  // Return true if key is greater than the data stored in "n".  Null n
  // is considered infinite.  n should not be head_node_.
  inline bool IsKeyAfterNode(const char* key, SkipListNode* n) const;
  inline bool IsKeyAfterNode(const TSMemSegRowData*& key, SkipListNode* n) const;

  SkipListNode* FindGreaterOrEqual(const char* key) const;

  template <bool prefetch_before>
  inline void FindSpliceForLevel(const TSMemSegRowData*& key, SkipListNode* before, SkipListNode* after, int level,
                          SkipListNode** out_prev, SkipListNode** out_next);

  inline void RecomputeSpliceLevels(const TSMemSegRowData*& key, SkipListSplice* splice,
                             int recompute_level);

  friend SkiplistIterator;
};

// Iteration over the contents of a skip list
class SkiplistIterator {
 private:
  const TsMemSegIndex* list_;
  SkipListNode* node_;

 public:
  explicit SkiplistIterator(const TsMemSegIndex* list) {
    list_ = list;
    node_ = nullptr;
  }

  inline bool Valid() const { return node_ != nullptr; }

  inline const char* key() const {
    assert(Valid());
    return node_->Key();
  }

  inline void Next() {
    assert(Valid());
    node_ = node_->Next(0);
  }

  inline void Seek(const char* target) { node_ = list_->FindGreaterOrEqual(target); }

  inline void SeekToFirst() { node_ = list_->head_node_->Next(0); }
};

template <bool prefetch_before>
void TsMemSegIndex::FindSpliceForLevel(const TSMemSegRowData*& key,
                                    SkipListNode* before, SkipListNode* after,
                                    int level, SkipListNode** out_prev,
                                    SkipListNode** out_next) {
  while (true) {
    SkipListNode* next = before->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    if constexpr (prefetch_before == true) {
      if (next != nullptr && level > 0) {
        PREFETCH(next->Next(level - 1), 0, 1);
      }
    }
    if (next == after || !IsKeyAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

}  // namespace kwdbts
