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
#include <endian.h>
#include <stdlib.h>
#include <sys/types.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "data_type.h"
#include "libkwdbts2.h"
#include "ts_arena.h"

namespace kwdbts {

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

// store row-based data struct for certain entity.
struct TSMemSegRowData {
  // the flowing fields WILL BE used to sort and in big-endian order.
 private:
  uint64_t entity_id = 0;
  uint64_t ts = 0;
  uint64_t lsn = 0;

  // the following fields WILL NOT BE used to sort and in little-endian order.
 private:
  TSTableID table_id;
  uint32_t table_version;
  uint32_t database_id;
  TSSlice row_data;

 private:
  TS_LSN little_endian_lsn = 0;

 public:
  TSMemSegRowData(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id)
      : entity_id(htobe64(en_id)), table_id(tbl_id), table_version(tbl_version), database_id(db_id) {}

  void SetRowData(const TSSlice& crow_data) { row_data = crow_data; }
  void SetData(timestamp64 cts, TS_LSN clsn) {
    // the following line has undefined behavior, see: https://godbolt.org/z/1e567j4G5
    // ts = htobe64(cts - INT64_MIN);

    ts = htobe64(static_cast<uint64_t>(cts) ^ (1ULL << 63));
    lsn = htobe64(clsn);
    little_endian_lsn = clsn;
  }
  static constexpr size_t GetKeyLen() { return offsetof(TSMemSegRowData, table_id); }

  TSEntityID GetEntityId() const { return be64toh(entity_id); }
  timestamp64 GetTS() const { return static_cast<timestamp64>(be64toh(ts) ^ (1ULL << 63)); }
  TS_LSN GetLSN() const { return little_endian_lsn; }
  const TS_LSN* GetLSNAddr() const { return &little_endian_lsn; }

  TSTableID GetTableId() const { return table_id; }
  uint32_t GetTableVersion() const { return table_version; }
  uint32_t GetDatabaseId() const { return database_id; }
  TSSlice GetRowData() const { return row_data; }

  inline bool SameEntityAndTableVersion(const TSMemSegRowData* b) const {
    return this->entity_id == b->entity_id && this->table_version == b->table_version;
  }
  inline bool SameEntityAndTs(const TSMemSegRowData* b) const {
    return std::memcmp(this, b, offsetof(TSMemSegRowData, lsn)) == 0;
  }
  inline bool SameTableId(const TSMemSegRowData* b) const { return this->table_id == b->table_id; }
};

static_assert(sizeof(TSMemSegRowData) == 64, "TSMemSegRowData size is not 64");
static_assert(std::has_unique_object_representations_v<TSMemSegRowData>,
              "TSMemSegRowData has some uninitialized padding");

struct TSRowDataComparator {
  inline const TSMemSegRowData* DecodeKeyValue(const char* b) const {
    return reinterpret_cast<const TSMemSegRowData*>(b);
  }

  int operator()(const char* a, const char* b) const { return memcmp(a, b, TSMemSegRowData::GetKeyLen()); }
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

  // use replacement new outside of this class.
  TSMemSegRowData* AllocateMemSegRowData(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id,
                                         TSSlice row_data);

  void InsertRowData(const TSMemSegRowData* row);

  inline const TSMemSegRowData* ParseKey(const char* key) {
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

  SkipListNode* FindGreaterOrEqual(const char* key) const;

  template <bool prefetch_before>
  inline void FindSpliceForLevel(const char* key, SkipListNode* before, SkipListNode* after, int level,
                                 SkipListNode** out_prev, SkipListNode** out_next);

  inline void RecomputeSpliceLevels(const char* key, SkipListSplice* splice, int recompute_level);

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
  inline void Seek(const TSMemSegRowData* target) { this->Seek(reinterpret_cast<const char*>(target)); }

  inline void SeekToFirst() { node_ = list_->head_node_->Next(0); }

  inline bool operator!=(const SkiplistIterator& other) const { return node_ != other.node_; }
};

template <bool prefetch_before>
void TsMemSegIndex::FindSpliceForLevel(const char* key, SkipListNode* before, SkipListNode* after, int level,
                                       SkipListNode** out_prev, SkipListNode** out_next) {
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
    // we will break only if next > key
    if (next == after || compare_(next->Key(), key) > 0) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

}  // namespace kwdbts
