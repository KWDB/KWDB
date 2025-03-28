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

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <algorithm>
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_commonv2.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "lt_rw_latch.h"
#include "utils/compress_utils.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "lg_api.h"
#include "mmap/mmap_string_column.h"
#include "bitmap_utils.h"

class MMapSegmentTable;

extern uint32_t k_per_null_bitmap_size;

template <class T>
class Defer {
 public:
  explicit Defer(T& closure) : _closure(closure) {}
  explicit Defer(T&& closure) : _closure(std::move(closure)) {}
  ~Defer() { _closure(); }
 private:
  T _closure;
};

namespace kwdbts {
// ***In order to compile some temporary type definitions, each module needs to be redefined ****
typedef std::string TS_TABLE_ID;

typedef uint64_t TS_LSN;  // LSN number used for WAL logs

struct DelRowSpan {
  timestamp64 partition_ts;   // Partition timestamp
  uint16_t blockitem_id;      // partition block item id
  char delete_flags[128] = {
      0};  // Which rows in the data block were deleted when DeleteData() was recorded, with a bit of 1 for the deleted rows
}__attribute__((packed));

struct DelRowSpans {
  string primary_tag;
  std::vector<DelRowSpan> spans;
};

struct UnorderedDataStats {
  k_uint32 total_data_rows = 0;
  k_uint32 unordered_data_rows = 0;
  k_uint32 ordered_entity_cnt = 0;
  k_uint32 unordered_entity_cnt = 0;

  UnorderedDataStats operator+=(const UnorderedDataStats& stats) {
    total_data_rows += stats.total_data_rows;
    unordered_data_rows += stats.unordered_data_rows;
    ordered_entity_cnt += stats.ordered_entity_cnt;
    unordered_entity_cnt += stats.unordered_entity_cnt;
    return *this;
  }
};

enum class VacuumStatus {
  NOT_BEGIN = 0,
  CANCEL,
  FINISH,
  FAILED
};

enum SortOrder {
  ASC = 0,
  DESC,
};

struct Batch {
  Batch() = delete;

  Batch(void* m, k_uint32 c, const std::shared_ptr<MMapSegmentTable>& t)
      : mem(m), count(c), segment_table(t) {}

  Batch(void* m, k_uint32 c, void* b, const std::shared_ptr<MMapSegmentTable>& t)
      : mem(m), bitmap(b), count(c), segment_table(t) {}

  Batch(void* m, k_uint32 c, void* b, k_uint32 o, const std::shared_ptr<MMapSegmentTable>& t)
      : mem(m), bitmap(b), count(c), offset(o), segment_table(t) {}

  Batch(k_uint32 c, void* b, k_uint32 o, const std::shared_ptr<MMapSegmentTable>& t)
      : bitmap(b), count(c), offset(o), segment_table(t) {}

  // Record whether mem_ is the memory space requested on the heap
  bool is_new = false;
  bool is_overflow = false;
  bool need_free_bitmap = false;
  void* mem = nullptr;
  void* bitmap = nullptr;
  k_uint32 count = 0;
  k_uint32 offset = 0;
  // Holding smart pointers to avoid switching between segments in use
  std::shared_ptr<MMapSegmentTable> segment_table = nullptr;

  virtual ~Batch() {
    if (is_new && mem) {
      free(mem);
      mem = nullptr;
    }
    if (need_free_bitmap) {
      free(bitmap);
      bitmap = nullptr;
    }
  }

  // row_idx  start from 0
  virtual void* getVarColData(k_uint32 row_idx) const { return nullptr; }

  virtual uint16_t getVarColDataLen(k_uint32 row_idx) const { return 0; }

  virtual void push_back(const std::shared_ptr<void>& data) { return; }

  // row_idx  start from 0
  virtual KStatus isNull(k_uint32 row_idx, bool* is_null) const {
    if (bitmap == nullptr) {
      *is_null = true;
      return KStatus::SUCCESS;
    }
    if (row_idx >= count) {
      return KStatus::FAIL;
    }
    int byte = (offset + row_idx - 1) >> 3;
    int bit = 1 << ((offset + row_idx - 1) & 7);
    *is_null = static_cast<char*>(bitmap)[byte] & bit;
    return KStatus::SUCCESS;
  }

  virtual KStatus setNull(uint32_t row_idx) {
    if (!bitmap || row_idx >= count) {
      return KStatus::FAIL;
    }
    size_t byte = (offset + row_idx - 1) >> 3;
    size_t bit = (offset + row_idx - 1) & 7;
    static_cast<char*>(bitmap)[byte] |= (1 << bit);
    return KStatus::SUCCESS;
  }

  bool hasNull() const {
    if (bitmap == nullptr) {
      return true;
    }
    return hasNonZeroBit(static_cast<char* >(bitmap), offset, count);
  }
};

struct TagBatch : public Batch {
  uint32_t data_length_;

  TagBatch(uint32_t data_len, void* m, k_uint32 c) : Batch(m, c, nullptr, nullptr) {
    data_length_ = data_len;
  }

  ~TagBatch() override {
    if (mem) {
      free(mem);
      mem = nullptr;
    }
    if (bitmap) {
      free(bitmap);
      bitmap = nullptr;
    }
  }

  void writeData(uint32_t row_idx, void* data, uint32_t data_len) {
    memcpy(reinterpret_cast<void*>((intptr_t)mem + row_idx * data_length_),
          data, data_len);
  }

  KStatus isNull(k_uint32 row_idx, bool* is_null) const override {
    if (mem == nullptr || row_idx >= count) {
      *is_null = true;
      return KStatus::FAIL;
    }
    *is_null = *reinterpret_cast<char*>((intptr_t)mem + row_idx * data_length_) == 0;
    return KStatus::SUCCESS;
  }

  KStatus setNull(uint32_t row_idx) override {
    if (row_idx >= count) {
      return KStatus::FAIL;
    }
    *reinterpret_cast<char*>((intptr_t)mem + row_idx * data_length_) = 0;
    return KStatus::SUCCESS;
  }
  KStatus setNotNull(uint32_t row_idx) {
    if (row_idx >= count) {
      return KStatus::FAIL;
    }
    *reinterpret_cast<char*>((intptr_t)mem + row_idx * data_length_) = 1;
    return KStatus::SUCCESS;
  }

  char* getRowAddr(uint32_t row_idx) {
    return reinterpret_cast<char*>((intptr_t)mem + row_idx * data_length_ + k_per_null_bitmap_size);
  }
};

struct VarTagBatch : public Batch {
  char* var_data_;
  char* var_end_;
  char* next_record_ptr_;
  uint32_t total_size_{0};
  std::vector<void*> var_data_ptrs_;
  std::vector<void*> m_blocks_;
  VarTagBatch(uint32_t total_sz, char* var_data, uint32_t cnt) : Batch(nullptr, cnt, nullptr),
              var_data_(var_data), total_size_(total_sz) {
    var_end_ = var_data_ + total_sz;
    next_record_ptr_ = var_data_;
    var_data_ptrs_.resize(count);
    m_blocks_.push_back(var_data_);
  }
  ~VarTagBatch() override {
    if (mem) {
      free(mem);
      mem = nullptr;
    }
    if (bitmap) {
      free(bitmap);
      bitmap = nullptr;
    }
    for (auto& it : m_blocks_) {
      if (it) {
        free(it);
      }
      it = nullptr;
    }
    m_blocks_.clear();
  }
  int writeDataIncludeLen(uint32_t row_idx, void* data, uint16_t var_len) {
    if (next_record_ptr_ + var_len > var_end_) {
      size_t total_realloc_size = std::max(2 * total_size_, (uint32_t)var_len);
      var_data_ = reinterpret_cast<char*>(std::malloc(total_realloc_size));
      if (nullptr == var_data_) {
        LOG_ERROR("VarTagBatch out of memory. total size: %u extend size: %lu", total_size_, total_realloc_size);
        return -1;
      }
      next_record_ptr_ = var_data_;
      var_end_ = var_data_ + total_realloc_size;
      total_size_ = total_realloc_size;
      m_blocks_.push_back(var_data_);
    }
    memcpy(next_record_ptr_, data, var_len);
    var_data_ptrs_[row_idx] = next_record_ptr_;
    next_record_ptr_ += var_len;
    return 0;
  }

  int writeDataExcludeLen(uint32_t row_idx, void* data, uint16_t var_len) {
    if (next_record_ptr_ + var_len + MMapStringColumn::kStringLenLen + MMapStringColumn::kEndCharacterLen > var_end_) {
      size_t total_realloc_size = std::max(2 * total_size_, (uint32_t)var_len +
                                                            MMapStringColumn::kStringLenLen +
                                                            MMapStringColumn::kEndCharacterLen);
      var_data_ = reinterpret_cast<char*>(std::malloc(total_realloc_size));
      if (nullptr == var_data_) {
        LOG_ERROR("VarTagBatch out of memory. total size: %u extend size: %lu", total_size_, total_realloc_size);
        return -1;
      }
      next_record_ptr_ = var_data_;
      var_end_ = var_data_ + total_realloc_size;
      total_size_ = total_realloc_size;
      m_blocks_.push_back(var_data_);
    }
    var_data_ptrs_[row_idx] = next_record_ptr_;

    *reinterpret_cast<uint16_t*>(next_record_ptr_) = var_len + MMapStringColumn::kEndCharacterLen;
    next_record_ptr_ += MMapStringColumn::kStringLenLen;

    memcpy(next_record_ptr_, data, var_len);
    next_record_ptr_ += var_len;

    *next_record_ptr_ = 0x00;
    next_record_ptr_ += MMapStringColumn::kEndCharacterLen;
    return 0;
  }

  void* getVarColData(k_uint32 row_idx) const override {
    if (var_data_ptrs_[row_idx] == nullptr) {
      return nullptr;
    }
    return reinterpret_cast<void*>((intptr_t)var_data_ptrs_[row_idx] +
                                   sizeof(uint16_t));
  }

  uint16_t getVarColDataLen(k_uint32 row_idx) const override {
    if (var_data_ptrs_[row_idx] == nullptr) {
      return 0;
    }
    return *reinterpret_cast<uint16_t*>((intptr_t)var_data_ptrs_[row_idx]);
  }

  KStatus isNull(k_uint32 row_idx, bool* is_null) const override {
    if (row_idx >= count) {
      *is_null = true;
      return KStatus::FAIL;
    }
    *is_null = var_data_ptrs_[row_idx] == nullptr;
    return KStatus::SUCCESS;
  }

  KStatus setNull(uint32_t row_idx) override {
    if (row_idx >= count) {
      return KStatus::FAIL;
    }
    var_data_ptrs_[row_idx] = nullptr;
    return KStatus::SUCCESS;
  }
};

const uint32_t k_default_block_size = 4 * 1024;  // 4K

struct AggBatch : public Batch {
  AggBatch(void* m, k_uint32 c, const std::shared_ptr<MMapSegmentTable>& t) : Batch(m, c, t) {}

  AggBatch(const std::shared_ptr<void>& m, k_uint32 c, const std::shared_ptr<MMapSegmentTable>& t)
           : Batch(m.get(), c, t), var_mem_(m) {}

  // row_idx  start from 0
  KStatus isNull(k_uint32 row_idx, bool* is_null) const override {
    *is_null = (count == 0) || (mem == nullptr);
    return KStatus::SUCCESS;
  }

  void* getVarColData(uint32_t row_idx) const override {
    if (!var_mem_) return nullptr;
    return reinterpret_cast<void*>((intptr_t)var_mem_.get() + sizeof(uint16_t));
  }

  uint16_t getVarColDataLen(k_uint32 row_idx) const override {
    if (!var_mem_) return 0;
    return *reinterpret_cast<uint16_t*>(var_mem_.get());
  }

  std::shared_ptr<void> var_mem_ = nullptr;
};

struct VarColumnBatch : public Batch {
  VarColumnBatch(k_uint32 c, void* b, k_uint32 o, const std::shared_ptr<MMapSegmentTable>& t) : Batch(c, b, o, t) {}

  ~VarColumnBatch() override {
    var_data_mem_.clear();
  }

  void* getVarColData(k_uint32 row_idx) const override {
    if (var_data_mem_[row_idx] == nullptr) {
      return nullptr;
    }
    return reinterpret_cast<void*>((intptr_t)var_data_mem_[row_idx].get() +
                                   sizeof(uint16_t));
  }

  uint16_t getVarColDataLen(k_uint32 row_idx) const override {
    if (var_data_mem_[row_idx] == nullptr) {
      return 0;
    }
    return *reinterpret_cast<uint16_t*>((intptr_t)var_data_mem_[row_idx].get());
  }

  void push_back(const std::shared_ptr<void>& data) override { var_data_mem_.emplace_back(data); }

  std::vector<std::shared_ptr<void>> var_data_mem_;
};

// EntityResultIndex
struct EntityResultIndex {
  EntityResultIndex() {}
  EntityResultIndex(uint64_t entityGroupId, uint32_t entityId, uint32_t subGroupId):
                      entityGroupId(entityGroupId), entityId(entityId), subGroupId(subGroupId) {}
  EntityResultIndex(uint64_t entityGroupId, uint32_t entityId, uint32_t subGroupId, void* mem) :
                     entityGroupId(entityGroupId), entityId(entityId), subGroupId(subGroupId), mem(mem) {}
  EntityResultIndex(uint64_t entityGroupId, uint32_t entityId, uint32_t subGroupId, uint32_t hash_point, void* mem) :
                     entityGroupId(entityGroupId), entityId(entityId), subGroupId(subGroupId),
                     hash_point(hash_point), mem(mem) {}
  uint64_t entityGroupId{0};
  uint32_t entityId{0};
  uint32_t subGroupId{0};
  uint32_t hash_point{0};
  uint32_t index{0};
  uint32_t ts_version{0};
  void* mem{nullptr};  // primaryTags address

  bool equalsWithoutMem(const EntityResultIndex& entity_index) {
    if (entityId != entity_index.entityId ||
        subGroupId != entity_index.subGroupId ||
        entityGroupId != entity_index.entityGroupId) {
      return false;
    }
    return true;
  }
};

struct ResultSet {
  k_uint32 col_num_{0};
  EntityResultIndex entity_index{};
  std::vector<std::vector<const Batch*>> data;

  ResultSet() = default;

  explicit ResultSet(k_uint32 col_num) : col_num_(col_num), data(col_num) {
  }

  void setColumnNum(k_uint32 col_num) {
    col_num_ = col_num;
    data.resize(col_num_);
  }

  void push_back(k_uint32 col_location, const Batch* batch) {
    data[col_location].push_back(batch);
  }

  void clear() {
    entity_index = {};
    for (const auto& it : data) {
      for (auto batch : it) {
        delete batch;
      }
    }
    data.clear();
    data.resize(col_num_);
  }

  bool empty() {
    for (const auto& it : data) {
      if (!it.empty()) {
        return false;
      }
    }
    return true;
  }

  ~ResultSet() {
    for (const auto& it : data) {
      for (auto batch : it) {
        delete batch;
      }
    }
  }
};

static inline bool(likely)(bool x) { return __builtin_expect((x), true); }

static inline bool(unlikely)(bool x) { return __builtin_expect((x), false); }

const uint32_t ONE_FETCH_COUNT = 1000;

enum EntityGroupType {
  UNINITIALIZED = -1,
  LEADER = 0,
  FOLLOWER = 1,
};

// calculate the length of intersection between two intervals
inline timestamp64 intersectLength(timestamp64 start1, timestamp64 end1, timestamp64 start2, timestamp64 end2) {
  // Calculate the maximum start point and minimum end point
  timestamp64 max_start = std::max(start1, start2);
  timestamp64 min_end = std::min(end1, end2);
  // If there is no overlap, return 0
  if (max_start >= min_end)
    return 0;
  // Otherwise, the intersection length is the difference between minEnd and maxStart
  return min_end - max_start;
}

// [start, end] cross with spans
inline bool isTimestampInSpans(const std::vector<KwTsSpan>& spans,
                               timestamp64 start, timestamp64 end) {
  for (auto& span : spans) {
    if (start <= span.end && end >= span.begin) {
      return true;
    }
  }
  return false;
}

// [start, end] include in spans
inline bool isTimestampWithinSpans(const std::vector<KwTsSpan>& spans,
                                   timestamp64 start, timestamp64 end) {
  for (auto& span : spans) {
    if (start >= span.begin && end <= span.end) {
      return true;
    }
  }
  return false;
}

inline bool CheckIfTsInSpan(timestamp64 ts, std::vector<KwTsSpan>& ts_spans) {
  for (auto& ts_span : ts_spans) {
    if (ts >= ts_span.begin && ts <= ts_span.end) {
      return true;
    }
  }
  return false;
}

inline void getMaxAndMinTs(std::vector<KwTsSpan>& spans, timestamp64* min_ts,
                           timestamp64* max_ts) {
  for (int i = 0; i < spans.size(); ++i) {
    if (i == 0 || spans[i].begin < *min_ts) {
      *min_ts = spans[i].begin;
    }
    if (i == 0 || spans[i].end > *max_ts) {
      *max_ts = spans[i].end;
    }
  }
}

inline bool isTsType(DATATYPE type) {
  if (type == TIMESTAMP || type == TIMESTAMP64 || type == TIMESTAMP64_MICRO || type == TIMESTAMP64_NANO
      || type == TIMESTAMP64_LSN || type == TIMESTAMP64_LSN_MICRO || type == TIMESTAMP64_LSN_NANO) {
    return true;
  }
  return false;
}

inline bool isTsWithLSNType(DATATYPE type) {
  if (type == TIMESTAMP64_LSN || type == TIMESTAMP64_LSN_MICRO || type == TIMESTAMP64_LSN_NANO) {
    return true;
  }
  return false;
}


inline bool isSumType(DATATYPE type) {
  if (type == INT8 || type == INT16 || type == INT32 || type == INT64 || type == FLOAT || type == DOUBLE) {
    return true;
  }
  return false;
}

inline DATATYPE getSumType(DATATYPE type) {
  DATATYPE sum_type{INVALID};
  switch (type) {
    case INT8:
    case INT16:
    case INT32:
    case INT64:
      sum_type = INT64;
      break;
    case FLOAT:
    case DOUBLE:
      sum_type = DOUBLE;
      break;
    default:
      break;
  }
  return sum_type;
}

inline k_uint32 getSumSize(DATATYPE type) {
  k_uint32 sum_size{0};
  switch (type) {
    case FLOAT:
    case INT8:
    case INT16:
    case INT32:
    case INT64:
    case DOUBLE:
      sum_size = 8;
      break;
    default:
      break;
  }
  return sum_size;
}

enum Sumfunctype {
  ANY_NOT_NULL = 0,
  AVG = 1,
  BOOL_AND = 2,
  BOOL_OR = 3,
  CONCAT_AGG = 4,
  COUNT = 5,
  MAX = 7,
  MIN = 8,
  STDDEV = 9,
  SUM = 10,
  SUM_INT = 11,
  VARIANCE = 12,
  XOR_AGG = 13,
  COUNT_ROWS = 14,
  SQRDIFF = 15,
  FINAL_VARIANCE = 16,
  FINAL_STDDEV = 17,
  ARRAY_AGG = 18,
  JSON_AGG = 19,
  // JSONB_AGG is an alias for JSON_AGG, they do the same thing.
  JSONB_AGG = 20,
  STRING_AGG = 21,
  BIT_AND = 22,
  BIT_OR = 23,
  CORR = 24,
  FIRST = 25,
  LAST = 26,
  MATCHING = 27,
  TIME_BUCKET_GAPFILL_INTERNAL = 28,
  INTERPOLATE = 29,
  LAST_ROW = 30,
  LASTTS = 31,
  LASTROWTS = 32,
  FIRSTTS = 33,
  FIRST_ROW = 34,
  FIRSTROWTS = 35,
  ELAPSED = 36,
  TWA = 37
};

enum WindowFunc {
    // These mirror window functions from window_builtins.go.
    ROW_NUMBER = 0,
    RANK = 1,
    DENSE_RANK = 2,
    PERCENT_RANK = 3,
    CUME_DIST = 4,
    NTILE = 5,
    LAG = 6,
    LEAD = 7,
    FIRST_VALUE = 8,
    LAST_VALUE = 9,
    NTH_VALUE = 10,
    DIFF = 11
  };

/**
 * @brief A shared LRU cache based on std::unordered_map and std::list, with thread safety and automatic cleaning mechanism.
 *
 * @tparam key_t The type of key.
 * @tparam value_t The type of value.
 */
template<typename key_t, typename value_t>
class SharedLruUnorderedMap {
 public:
  typedef typename std::pair<key_t, std::shared_ptr<value_t>> key_value_pair_t;
  typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

  /**
   * @brief Constructor.
   *
   * @param[in] capacity The capacity of the cache.
   * @param[in] async_clean Whether to enable asynchronous cleaning thread, default to true.
   * @note Ensure that capacity is greater than 0.
   */
  explicit SharedLruUnorderedMap(size_t capacity, bool async_clean = true) : capacity_(capacity), async_clean_(async_clean) {
    assert(capacity > 0);
    rw_latch_ = new KRWLatch(RWLATCH_ID_TSTABLE_LRU_CACHE_RWLOCK);
    is_running_ = true;
  }

  /**
   * @brief Destructor, clean up resources.
   */
  ~SharedLruUnorderedMap() {
    is_running_ = false;
    if (async_clean_) {
      closeAsyncThread();
    }
    delete rw_latch_;
  }


  /**
   * @brief Initialize function, if asynchronous cleaning is enabled, will initialize the asynchronous cleaning thread.
   */
  void Init() {
    if (async_clean_) {
      initAsyncThread();
    }
  }

  /**
   * @brief Insert key value pairs into the cache.
   *
   * @param[in] key key_t value.
   * @param[in] value A smart pointer to a value
   */
  void Put(const key_t& key, const std::shared_ptr<value_t>& value) {
    wrLock();
    Defer defer{[&]() { unLock(); }};
    try {
      // If the key already exists, clean and insert a new kv.
      auto it = cache_items_map_.find(key);
      if (it != cache_items_map_.end()) {
        cache_items_list_.erase(it->second);
        cache_items_map_.erase(it);
      }
      cache_items_list_.push_front(key_value_pair_t(key, value));
      cache_items_map_.insert(std::make_pair(key, cache_items_list_.begin()));

      if (cache_items_map_.size() > capacity_) {
        Clear(cache_items_map_.size() - capacity_, false);
      }
    } catch (...) {
      return;
    }
  }

  /**
   * @brief Get the value based on the key.
   *
   * @param[in] key key_t value.
   * @return A smart pointer to a value that returns nullptr if the key does not exist.
   */
  std::shared_ptr<value_t> Get(const key_t& key) {
    wrLock();
    Defer defer{[&]() { unLock(); }};
    try {
      auto it = cache_items_map_.find(key);
      if (it == cache_items_map_.end()) {
        return nullptr;
      } else {
        // Move the second element pointed to by the iterator it in the linked list cache_item_list_,
        // which is it ->second, to the beginning of the linked list.
        cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
        return it->second->second;
      }
    } catch (...) {
      return nullptr;
    }
  }

  /**
   * @brief Check if the key exists.
   *
   * @param[in] key key_t value.
   * @return If the key exists, return true; otherwise, return false.
   */
  k_bool Exists(const key_t& key) {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    return cache_items_map_.find(key) != cache_items_map_.end();
  }

  /**
   * @brief Delete specified key value pairs from cache
   *
   * @param[in] key key_t value.
   */
  void Erase(const key_t& key) {
    wrLock();
    Defer defer{[&]() { unLock(); }};

    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
  }

  /**
   * @brief Check the reference count and delete cache entries that are no longer in use.
   *
   * @param[in] key key_t value.
   */
  void EraseAndCheckRef(const key_t& key) {
    wrLock();
    Defer defer{[&]() { unLock(); }};

    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end() && it->second->second.use_count() <= 1) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    } else if (it != cache_items_map_.end()) {
      erase_items_.insert(it->first);
    }
  }

  /**
   * @brief Clear cache.
   */
  void Clear() {
    wrLock();
    Defer defer{[&]() { unLock(); }};

    erase_items_.clear();
    cache_items_list_.clear();
    cache_items_map_.clear();
  }

  /**
   * @brief Clear by quantity, clear cache items without references.
   *
   * @param[in] num The number of cleanups, if 0, clears all unreferenced cache entries.
   * @param[in] lock Whether to lock before operation, default to true.
   * @return The number of cache entries cleared.
   */
  int Clear(int num, bool lock = true) {
    if (lock) {
      wrLock();
    }

    if (num == 0) {
      num = INT32_MAX;
    }

    int clear_num = 0;
    auto last_iter = cache_items_list_.rbegin();
    while (clear_num < num && last_iter != cache_items_list_.rend()) {
      if (last_iter->second.use_count() <= 1) {
        cache_items_map_.erase(last_iter->first);
        cache_items_list_.erase(std::next(last_iter).base());
        clear_num++;
        continue;
      }
      ++last_iter;
    }

    if (lock) {
      unLock();
    }
    return clear_num;
  }

  /**
   * @brief Get the number of items in the cache.
   *
   * @return The number of items in the cache.
   */
  size_t Size() {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    return cache_items_map_.size();
  }

  /**
   * @brief Get the capacity of the cache.
   *
   * @return The capacity of the cache.
   */
  size_t GetCapacity() {
    return capacity_;
  }

  /**
   * @brief Set the maximum cache capacity.
   *
   * @param[in] new_capacity New capacity.
   */
  void SetCapacity(size_t new_capacity) {
    wrLock();
    Defer defer{[&]() { unLock(); }};
    assert(new_capacity > 0);
    int cur_size = cache_items_map_.size();
    if (new_capacity < cur_size) {
      Clear(cur_size - new_capacity, false);
    }
    capacity_ = new_capacity;
  }

  /**
   * @brief Get all values in the cache.
   * @return std::vector<std::shared_ptr<value_t>>
   */
  std::list<key_value_pair_t> GetAllValues() {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    return cache_items_list_;
  }

  /**
   * @brief Traverse cache items and apply the given function.
   *
   * @param func If the function returns false for each cache item, the traversal terminates.
   * @return bool True indicates completion of traversal, False indicates that the traversal is terminated midway.
   */
  bool Traversal(std::function<bool(key_t, std::shared_ptr<value_t>)> func) {
    rdLock();
    Defer defer{[&]() { unLock(); }};

    auto iter = cache_items_list_.begin();
    while (iter != cache_items_list_.end()) {
      if (!func(iter->first, iter->second)) {
        return false;
      }
      iter++;
    }
    return true;
  }

 protected:
  /**
   * @brief Thread scheduling executes cleaning tasks to clean up items that require erasing.
   */
  void routine(void* args) {
    while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && is_running_) {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      // Check every 5 minutes if cleaning is necessary
      cv_.wait_for(lock, std::chrono::seconds(300), [this] { return !is_running_; });
      lock.unlock();
      // If the thread pool stops or the system is no longer running, exit the loop
      if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !is_running_) {
        break;
      }
      // Execute cleaning tasks
      this->clearEraseItems();
    }
  }

  /**
   * @brief Initialize asynchronous cleanup thread.
   */
  void initAsyncThread() {
    KWDBOperatorInfo kwdb_operator_info;
    // Set the name and owner of the operation
    kwdb_operator_info.SetOperatorName("SharedLruUnorderedMap");
    kwdb_operator_info.SetOperatorOwner("SharedLruUnorderedMap");
    time_t now;
    // Record the start time of the operation
    kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
    // Start asynchronous thread
    clean_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
        std::bind(&SharedLruUnorderedMap::routine, this, std::placeholders::_1), this,
        &kwdb_operator_info);
    if (clean_thread_id_ < 1) {
      // If thread creation fails, record error message
      LOG_ERROR("SharedLruUnorderedMap clean_thread create failed");
    }
  }

  /**
   * @brief Close the asynchronous cleanup thread.
   */
  void closeAsyncThread() {
    if (clean_thread_id_ > 0) {
      // Wake up potentially dormant cleanup threads
      cv_.notify_all();
      // Waiting for the cleaning thread to complete
      KWDBDynamicThreadPool::GetThreadPool().JoinThread(clean_thread_id_, 0);
    }
  }

  /**
   * @brief Clean up cache entries that require erase.
   */
  void clearEraseItems() {
    wrLock();
    Defer defer{[&]() { unLock(); }};

    for (auto it = erase_items_.begin(); it != erase_items_.end();) {
      auto map_it = cache_items_map_.find(*it);
      if (map_it != cache_items_map_.end() && map_it->second->second.use_count() <= 1) {
        // If the item is found and its usage count is not greater than 1, delete it
        cache_items_list_.erase(map_it->second);
        cache_items_map_.erase(map_it);
        it = erase_items_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  // Cache item list, maintained in order of usage frequency
  std::list<key_value_pair_t> cache_items_list_;
  // Mapping of key to cache item list iterator
  std::unordered_map<key_t, list_iterator_t> cache_items_map_;
  // Record items that were not truly erased due to being in use
  std::set<key_t> erase_items_;
  // Read-write lock
  KRWLatch* rw_latch_;

  // Cache capacity
  size_t capacity_{0};
  // Flag indicating whether it is running or not
  bool is_running_{false};

  // Flag for starting asynchronous cleanup thread
  bool async_clean_{false};
  // Id of the clearing thread
  KThreadID clean_thread_id_{0};
  // Conditional variable, used for asynchronous cleaning of thread synchronization
  std::condition_variable cv_;
  // Mutexes for condition variables
  std::mutex cv_mutex_;

 public:
  // The following three functions encapsulate read-write locks to simplify lock operations.
  inline int rdLock() { return RW_LATCH_S_LOCK(rw_latch_); }
  inline int wrLock() { return RW_LATCH_X_LOCK(rw_latch_); }
  inline int unLock() { return RW_LATCH_UNLOCK(rw_latch_); }
};

inline int64_t getEnvInt(std::string env, int64_t default_value) {
  char* env_str = getenv(env.c_str());
  if (env_str) {
    return atoi(env_str);
  } else {
    return default_value;
  }
}

inline int64_t convertToTimestamp(std::string& ts_str) {
  if (ts_str[0] == 'm') {
    ts_str[0] = '-';
  }
  try {
    return std::stoll(ts_str);
  } catch (...) {
    LOG_ERROR("Convert string to timestamp failed.");
    abort();
  }
}

inline std::string convertTsToDirectoryName(timestamp64 ts) {
  std::string ret;
  if (ts < 0) {
    ret = "m";
    ts = 0 - ts;
  }
  ret += std::to_string(ts);
  return ret;
}

inline string booleanToString(bool v) {
  return (v) ? s_true : s_false;
}
inline string tableTypeToString(int type) { return s_row; }

inline int numDigit(double v) {
  double x = std::abs(v);
  int d = 0;
  while (x > 1.0) {
    x = x / 10;
    d++;
  }
  return d + (v < 0);
}

inline timestamp64 convertTsToPTime(timestamp64 ts, DATATYPE ts_type) {
  if (ts == INT64_MAX || ts == INT64_MIN) {
    return ts;
  }
  int64_t precision = 0;
  switch (ts_type) {
    case TIMESTAMP64_LSN:
    case TIMESTAMP64:
      precision = 1000;
      break;
    case TIMESTAMP64_LSN_MICRO:
    case TIMESTAMP64_MICRO:
      precision = 1000000;
      break;
    case TIMESTAMP64_LSN_NANO:
    case TIMESTAMP64_NANO:
      precision = 1000000000;
      break;
    default:
      assert(false);
      break;
  }
  timestamp64 ret;
  if (ts < 0 && ts != INT64_MIN) {
    ret = (ts - precision + 1) / precision;
  } else {
    ret = ts / precision;
  }
  return ret;
}

enum TSTagOpType {
    opAnd = 0,
    opOr = 1,
    opUnKnow = 2,
};

inline timestamp64 convertSecondToPrecisionTS(timestamp64 ts, DATATYPE ts_type) {
  if (ts == INT64_MAX || ts == INT64_MIN) {
    return ts;
  }
  int64_t precision = 0;
  switch (ts_type) {
    case TIMESTAMP64_LSN:
    case TIMESTAMP64:
      precision = 1000;
      break;
    case TIMESTAMP64_LSN_MICRO:
    case TIMESTAMP64_MICRO:
      precision = 1000000;
      break;
    case TIMESTAMP64_LSN_NANO:
    case TIMESTAMP64_NANO:
      precision = 1000000000;
      break;
    default:
      assert(false);
      break;
  }
  return ts * precision;
}

inline timestamp64 convertMSToPrecisionTS(timestamp64 ts, DATATYPE ts_type) {
  if (ts == INT64_MAX || ts == INT64_MIN) {
    return ts;
  }
  int64_t precision = 0;
  switch (ts_type) {
    case TIMESTAMP64_LSN:
    case TIMESTAMP64:
      precision = 1;
      break;
    case TIMESTAMP64_LSN_MICRO:
    case TIMESTAMP64_MICRO:
      precision = 1000;
      break;
    case TIMESTAMP64_LSN_NANO:
    case TIMESTAMP64_NANO:
      precision = 1000000;
      break;
    default:
      assert(false);
      break;
  }
  return ts * precision;
}

}  //  namespace kwdbts
