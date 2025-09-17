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

#include <deque>
#include <map>
#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "lt_rw_latch.h"
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_common.h"
#include "st_config.h"

namespace kwdbts {
class TsAggIterator;

class ColBlockBitmaps {
 public:
  ~ColBlockBitmaps() {
    if (bitmap_) {
      free(bitmap_);
    }
  }

  bool Init(size_t col, bool all_agg_cols_not_null) {
    all_agg_cols_not_null_ = all_agg_cols_not_null;
    if (all_agg_cols_not_null_) {
      bitmap_ = reinterpret_cast<char*>(malloc(BLOCK_ITEM_BITMAP_SIZE));
      if (bitmap_ == nullptr) {
        return false;
      }
      memset(bitmap_, 0, BLOCK_ITEM_BITMAP_SIZE);
      return true;
    }
    col_num_ = col;
    bitmap_ = reinterpret_cast<char*>(malloc(col_num_ * BLOCK_ITEM_BITMAP_SIZE));
    if (bitmap_ == nullptr) {
      return false;
    }
    memset(bitmap_, 0, col_num_ * BLOCK_ITEM_BITMAP_SIZE);
    col_bitmap_addr_.resize(col_num_);
    return true;
  }

  inline char* GetColBitMapAddr(size_t col_idx) {
    if (all_agg_cols_not_null_) {
      return bitmap_;
    }
    char* ret_addr = bitmap_ + col_idx * BLOCK_ITEM_BITMAP_SIZE;
    if (col_bitmap_addr_[col_idx] != nullptr) {
      memcpy(ret_addr, col_bitmap_addr_[col_idx], BLOCK_ITEM_BITMAP_SIZE);
      col_bitmap_addr_[col_idx] = nullptr;
    }
    return ret_addr;
  }

  inline void SetColBitMap(size_t col_idx, char* bitmap, bool force_cpy = false) {
    if (bitmap != nullptr) {
      if (force_cpy) {
        memcpy(GetColBitMapAddr(col_idx), bitmap, BLOCK_ITEM_BITMAP_SIZE);
        col_bitmap_addr_[col_idx] = nullptr;
      } else {
        col_bitmap_addr_[col_idx] = bitmap;
      }
    } else {
      memset(GetColBitMapAddr(col_idx), 0xFF, BLOCK_ITEM_BITMAP_SIZE);
      col_bitmap_addr_[col_idx] = nullptr;
    }
  }

  inline void SetColBitMapVaild(size_t col_idx) {
    memset(GetColBitMapAddr(col_idx), 0, BLOCK_ITEM_BITMAP_SIZE);
    col_bitmap_addr_[col_idx] = nullptr;
  }

  inline bool IsColNull(size_t col_idx, size_t row_num) {
    if (all_agg_cols_not_null_) {
      return false;
    }
    return isRowDeleted(GetColBitMapAddr(col_idx), row_num);
  }

  inline bool IsColAllNull(size_t col_idx, size_t count) {
    if (all_agg_cols_not_null_) {
      return false;
    }
    return isAllDeleted(GetColBitMapAddr(col_idx), 1, count);
  }

  inline bool IsColSpanNull(size_t col_idx, size_t start_row, size_t count) {
    if (all_agg_cols_not_null_) {
      return false;
    }
    return isAllDeleted(GetColBitMapAddr(col_idx), start_row, count);
  }

 private:
  char* bitmap_{nullptr};
  bool all_agg_cols_not_null_{false};
  size_t col_num_;
  std::vector<char*> col_bitmap_addr_;
};

/**
 * @brief This is the iterator base class implemented internally in the storage layer, and its two derived classes are:
 *        (1) TsRawDataIterator, used for raw data queries (2) TsAggIterator, used for aggregate queries
 */
class TsStorageIterator {
 public:
  TsStorageIterator();
  TsStorageIterator(uint64_t entity_group_id, uint32_t subgroup_id,
                    const vector<uint32_t>& entity_ids, const std::vector<KwTsSpan>& ts_spans,
                    const std::vector<BlockFilter>& block_filter, DATATYPE ts_col_type,
                    const std::vector<uint32_t>& kw_scan_cols, const std::vector<uint32_t>& ts_scan_cols,
                    uint32_t table_version);

  virtual ~TsStorageIterator();

  virtual KStatus Init(bool is_reversed) = 0;

  static bool IsFirstAggType(const Sumfunctype& agg_type) {
    return agg_type == FIRST || agg_type == FIRSTTS || agg_type == FIRST_ROW || agg_type == FIRSTROWTS;
  }

  static bool IsLastAggType(const Sumfunctype& agg_type) {
    return agg_type == LAST || agg_type == LASTTS || agg_type == LAST_ROW || agg_type == LASTROWTS;
  }

  static bool IsLastTsAggType(const Sumfunctype& agg_type) {
    return agg_type == LAST || agg_type == LASTTS;
  }

  /**
   * @brief An internally implemented iterator query interface that provides a subgroup data query result to the TsTableIterator class
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   * @param ts             used for block and partition table level data filtering during orderbylimit queries
   */
  virtual KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) = 0;

  virtual bool IsDisordered() = 0;

 protected:
  inline bool checkIfTsInSpan(timestamp64 ts) {
    for (auto& ts_span : ts_spans_) {
      if (ts >= ts_span.begin && ts <= ts_span.end) {
        return true;
      }
    }
    return false;
  }

  bool getCurBlockSpan(BlockItem* cur_block, std::shared_ptr<MMapSegmentTable>& segment_tbl, uint32_t* first_row,
                       uint32_t* count);

  bool matchesFilterRange(const BlockFilter& filter, SpanValue min, SpanValue max, DATATYPE datatype);

  void nextEntity() {
    cur_block_item_ = nullptr;
    cur_block_ts_check_res_ = TimestampCheckResult::NonOverlapping;
    cur_blockdata_offset_ = 1;
    block_item_queue_.clear();
    ++cur_entity_idx_;
  }

 protected:
  uint64_t entity_group_id_{0};
  uint32_t subgroup_id_{0};
  vector<uint32_t> entity_ids_{};
  // the data time range queried by the iterator
  std::vector<KwTsSpan> ts_spans_;
  std::vector<BlockFilter> block_filter_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  // column attributes
  vector<AttributeInfo> attrs_;
  DATATYPE ts_col_type_;
    // table version
  uint32_t table_version_;
  // save all BlockItem objects in the partition table being queried
  std::deque<BlockItem*> block_item_queue_;
  // save the data offset within the BlockItem object being queried, used for traversal
  k_uint32 cur_blockdata_offset_ = 1;
  BlockItem* cur_block_item_ = nullptr;
  TimestampCheckResult cur_block_ts_check_res_ = TimestampCheckResult::NonOverlapping;
  k_uint32 cur_entity_idx_ = 0;
  MMapSegmentTableIterator* segment_iter_{nullptr};
  // Identifies whether the iterator returns blocks in reverse order
  bool is_reversed_ = false;
  // need sorting
  bool sort_flag_ = false;
  // todo(liangbo) set lsn parameter.
  TS_LSN scan_lsn_{UINT64_MAX};
};

class TsIterator {
 public:
  virtual ~TsIterator() {}
  virtual bool IsDisordered() = 0;
  virtual uint32_t GetFilterCount() = 0;
  virtual KStatus Next(ResultSet* res, k_uint32* count, timestamp64 ts = INVALID_TS) = 0;
};

/**
 * @brief The iterator class provided to the execution layer.
 */
class TsTableIterator : public TsIterator {
 public:
  TsTableIterator() : latch_(LATCH_ID_TSTABLE_ITERATOR_MUTEX) {}
  ~TsTableIterator() override {
    for (auto iter : iterators_) {
      delete iter;
    }
  }

  void AddEntityIterator(TsStorageIterator* iter) {
    iterators_.push_back(iter);
  }

  inline size_t GetIterNumber() {
    return iterators_.size();
  }

  /**
   * @brief Check whether the partition table of entity being queried is disordered.
   */
  bool IsDisordered() override {
    return iterators_[current_iter_]->IsDisordered();
  }

  uint32_t GetFilterCount() override {
    return 0;
  }

  /**
   * @brief The iterator query interface provided to the execution layer, When count is 0, it indicates the end of the query.
   *
   * @param res     the set of returned query results
   * @param count   number of rows of data
   */
  KStatus Next(ResultSet* res, k_uint32* count, timestamp64 ts = INVALID_TS) override;

 private:
  KLatch latch_;
  size_t current_iter_ = 0;
  // an array of TsStorageIterator objects, where one TsStorageIterator corresponds to the data of a subgroup
  std::vector<TsStorageIterator*> iterators_;
};

struct TimestampComparator {
  bool is_reversed = false;
  TimestampComparator() {}
  explicit TimestampComparator(bool reversed) : is_reversed(reversed) {}

  bool operator()(const timestamp64& a, const timestamp64& b) const {
    return is_reversed ? a > b : a < b;
  }
};
}  //  namespace kwdbts
