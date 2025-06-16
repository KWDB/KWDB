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
#include "ts_time_partition.h"
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_common.h"
#include "st_subgroup.h"
#include "st_config.h"

namespace kwdbts {
class TsEntityGroup;
class SubEntityGroupManager;
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

// Used for first/last query optimization:
// If only the first/last type is included in a single aggregation query process,
// as most temporal data is written in sequence, optimization can be carried out for this special scenario.
// 1. For first related queries, start traversing from the head of the partition table array, which is the smallest
// partition table.
// 2. For last related queries, start traversing from the end of the partition table array, which is the largest
// partition table.
class TsFirstLastRow {
 public:
  TsFirstLastRow() {}

  ~TsFirstLastRow() {
    delObjects();
  }

  void Init(const std::vector<uint32_t>& ts_scan_cols, const std::vector<Sumfunctype>& scan_agg_types) {
    ts_scan_cols_ = ts_scan_cols;
    scan_agg_types_ = scan_agg_types;
    // If the query aggregation type contains first/last correlation, the corresponding member variables need to be
    // initialized to record the results during the query process.
    Reset();
  }

  void delObjects() {
    for (int i = 0; i < first_pairs_.size(); ++i) {
      if (first_pairs_[i].second.partion_tbl != nullptr) {
        ReleaseTable(first_pairs_[i].second.partion_tbl);
        first_pairs_[i].second.partion_tbl = nullptr;
      }
    }
    for (int i = 0; i < last_pairs_.size(); ++i) {
      if (last_pairs_[i].second.partion_tbl != nullptr) {
        ReleaseTable(last_pairs_[i].second.partion_tbl);
        last_pairs_[i].second.partion_tbl = nullptr;
      }
    }
    if (first_row_pair_.partion_tbl != nullptr) {
      ReleaseTable(first_row_pair_.partion_tbl);
      first_row_pair_.partion_tbl = nullptr;
    }
    if (last_row_pair_.partion_tbl != nullptr) {
      ReleaseTable(last_row_pair_.partion_tbl);
      last_row_pair_.partion_tbl = nullptr;
    }
  }

  void Reset();

  void SetLastTsPoints(std::vector<timestamp64>& last_ts_points) {
    last_ts_points_ = std::move(last_ts_points);
  }

  inline bool HaslastTsPoint() {
    return last_ts_points_.size() > 0;
  }

  bool NeedFirstLastAgg() {
    return !no_first_last_type_;
  }

  inline bool FirstAggRowValid() {
    return first_agg_valid_ == first_pairs_.size() + 1;
  }

  inline bool LastAggRowValid() {
    return last_agg_valid_ == last_pairs_.size() + 1;
  }

  inline timestamp64 GetFirstMaxTs() {
    return first_max_ts_;
  }

  inline timestamp64 GetLastMinTs() {
    return last_min_ts_;
  }

  KStatus UpdateFirstRow(timestamp64 ts, MetricRowID row_id, TsTimePartition* partition_table,
                         std::shared_ptr<MMapSegmentTable>& segment_tbl, ColBlockBitmaps& col_bitmap);

  KStatus UpdateLastRow(timestamp64 ts, MetricRowID row_id, TsTimePartition* partition_table,
                        std::shared_ptr<MMapSegmentTable>& segment_tbl, ColBlockBitmaps& col_bitmap);

  KStatus GetAggBatch(TsAggIterator* iter, u_int32_t col_idx, size_t col_id,
                      const AttributeInfo& col_attr, Batch** agg_batch);

  int getActualColAggBatch(TsTimePartition* p_bt, shared_ptr<MMapSegmentTable>& segment_tbl,
                           MetricRowID real_row, uint32_t ts_col,
                           const AttributeInfo& col_attr, Batch** b);

  struct TsRowTableInfo {
    TsTimePartition* partion_tbl = nullptr;
    std::shared_ptr<MMapSegmentTable> segment_tbl = nullptr;
    timestamp64 row_ts;
    MetricRowID row_id;
    vector<bool> is_null;
  };

 private:
  std::vector<uint32_t> ts_scan_cols_;
  std::vector<Sumfunctype> scan_agg_types_;
  // Used to record first/last related results during a traversal process.
  // std::map<index of column, std::pair<index of partition table, std::pair<timestamp64, row id>>>
  std::vector<std::pair<k_uint32, TsRowTableInfo>> first_pairs_;
  std::vector<std::pair<k_uint32, TsRowTableInfo>> last_pairs_;
  std::vector<timestamp64> last_ts_points_;
  // std::pair<index of column, std::pair<timestamp64, row id>>
  TsRowTableInfo first_row_pair_;
  TsRowTableInfo last_row_pair_;
  bool no_first_last_type_{true};
  size_t first_agg_valid_{0};
  size_t last_agg_valid_{0};
  timestamp64 first_max_ts_{INVALID_TS};
  timestamp64 last_min_ts_{INVALID_TS};
  bool all_agg_cols_not_null_{false};
};

/**
 * @brief This is the iterator base class implemented internally in the storage layer, and its two derived classes are:
 *        (1) TsRawDataIterator, used for raw data queries (2) TsAggIterator, used for aggregate queries
 */
class TsStorageIterator {
 public:
  TsStorageIterator();
  TsStorageIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
             vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
             std::vector<uint32_t>& kw_scan_cols, std::vector<uint32_t>& ts_scan_cols,
             uint32_t table_version);

  virtual ~TsStorageIterator();

  virtual KStatus Init(bool is_reversed);

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

  bool IsDisordered() {
    TsSubGroupPTIterator cur_iter(partition_table_iter_.get());
    cur_iter.Reset();
    while (true) {
      TsTimePartition* cur_pt = nullptr;
      // TODO(zqh): Next() return Status
      cur_iter.Next(&cur_pt);
      if (cur_pt == nullptr) {
        break;
      }
      EntityItem* entity_item = cur_pt->getEntityItem(entity_ids_[cur_entity_idx_]);
      if (entity_item->is_disordered) {
        return true;
      }
    }
    return false;
  }

 protected:
  inline bool checkIfTsInSpan(timestamp64 ts) {
    for (auto& ts_span : ts_spans_) {
      if (ts >= ts_span.begin && ts <= ts_span.end) {
        return true;
      }
    }
    return false;
  }
  // ts is used for block filter for orderbylimit queries
  int nextBlockItem(k_uint32 entity_id, timestamp64 ts = INVALID_TS);

  bool getCurBlockSpan(BlockItem* cur_block, std::shared_ptr<MMapSegmentTable>& segment_tbl, uint32_t* first_row,
                       uint32_t* count);

  void fetchBlockItems(k_uint32 entity_id);

  void nextEntity() {
    cur_block_item_ = nullptr;
    cur_blockdata_offset_ = 1;
    block_item_queue_.clear();
    ++cur_entity_idx_;
    partition_table_iter_->Reset(is_reversed_);
  }

 protected:
  uint64_t entity_group_id_{0};
  uint32_t subgroup_id_{0};
  vector<uint32_t> entity_ids_{};
  // the data time range queried by the iterator
  std::vector<KwTsSpan> ts_spans_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  // column attributes
  vector<AttributeInfo> attrs_;
  DATATYPE ts_col_type_;
    // table version
  uint32_t table_version_;
  TsTimePartition* cur_partition_table_ = nullptr;
  std::shared_ptr<TsSubGroupPTIterator> partition_table_iter_;
  // save all BlockItem objects in the partition table being queried
  std::deque<BlockItem*> block_item_queue_;
  // save the data offset within the BlockItem object being queried, used for traversal
  k_uint32 cur_blockdata_offset_ = 1;
  BlockItem* cur_block_item_ = nullptr;
  k_uint32 cur_entity_idx_ = 0;
  MMapSegmentTableIterator* segment_iter_{nullptr};
  std::shared_ptr<TsEntityGroup> entity_group_;
  // Identifies whether the iterator returns blocks in reverse order
  bool is_reversed_ = false;
  // need sorting
  bool sort_flag_ = false;
  // todo(liangbo) set lsn parameter.
  TS_LSN scan_lsn_{UINT64_MAX};
};

// used for raw data queries
class TsRawDataIterator : public TsStorageIterator {
 public:
  TsRawDataIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                    vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                    std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols, uint32_t table_version) :
                    TsStorageIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans, ts_col_type,
                               kw_scan_cols, ts_scan_cols, table_version) {}

  /**
   * @brief The internal implementation of the row data query interface returns the maximum number of consecutive data
   *        in a BlockItem that meets the query criteria when called once.
   *        When is_finished is true, it indicates the end of the query.
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   * @param ts             used for block and partition table level data filtering during orderbylimit queries
   */
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;
};

class TsSortedRowDataIterator : public TsStorageIterator {
 public:
  TsSortedRowDataIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                          vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          uint32_t table_version, SortOrder order_type = ASC) :
      TsStorageIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans, ts_col_type,
                 kw_scan_cols, ts_scan_cols, table_version), order_type_(order_type) {}

  KStatus Init(bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 private:
  void nextEntity() {
    cur_block_span_ = BlockSpan{};
    cur_blockdata_offset_ = 1;
    block_spans_.clear();
    ++cur_entity_idx_;
    partition_table_iter_->Reset(is_reversed_);
  }

  int nextBlockSpan(k_uint32 entity_id);
  void fetchBlockSpans(k_uint32 entity_id);

  KStatus GetBatch(std::shared_ptr<MMapSegmentTable>& segment_tbl, BlockItem* cur_block_item, size_t block_start_idx,
                   ResultSet* res, k_uint32 count);

  std::deque<BlockSpan> block_spans_;
  BlockSpan cur_block_span_;
  SortOrder order_type_ = SortOrder::ASC;
};

// used for aggregate queries
class TsAggIterator : public TsStorageIterator {
 public:
  TsAggIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                vector<uint32_t>& entity_ids, vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                std::vector<Sumfunctype>& scan_agg_types, std::vector<timestamp64>& ts_points, uint32_t table_version) :
                TsStorageIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans, ts_col_type,
                                  kw_scan_cols, ts_scan_cols, table_version), scan_agg_types_(scan_agg_types) {
    // When creating an aggregate query iterator, the elements of the ts_scan_cols_ and scan_agg_types_ arrays
    // correspond one-to-one, and their lengths must be consistent.
    assert(scan_agg_types_.empty() || ts_scan_cols_.size() == scan_agg_types_.size());
    if (!ts_points.empty()) {
      last_ts_points_.assign(scan_agg_types_.size(), INVALID_TS);
      for (size_t i = 0; i < ts_scan_cols_.size(); ++i) {
        if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
          last_ts_points_[i] = ts_points[i];
        }
      }
    }
    first_last_row_.SetLastTsPoints(last_ts_points_);
  }

  ~TsAggIterator() {}

  KStatus Init(bool is_reversed) override;
  /**
   * @brief The internally implemented aggregate data query interface returns the aggregate query result of an entity
   *        when called once. When is_finished is true, it indicates that the query has ended.
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   */
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 private:
  /**
   * @brief    When the Next function is completed once, it indicates that the aggregated data of a entity has been queried.
   *           It is necessary to switch the entity ID and restore the variables that record the query status
   *           in order to query the next entity normally in the future.
   */
  void reset() {
    cur_block_item_ = nullptr;
    cur_blockdata_offset_ = 1;
    ++cur_entity_idx_;
    first_last_row_.Reset();
    partition_table_iter_->Reset(is_reversed_);
  }

  inline bool onlyHasFirstAggType() {
    bool flag = true;
    for (auto& agg_type : scan_agg_types_) {
      if (!IsFirstAggType(agg_type)) {
        flag = false;
      }
      if (agg_type == FIRST_ROW || agg_type == FIRSTROWTS) {
        no_first_row_type_ = false;
      }
    }
    return flag;
  }

  inline bool onlyHasLastAggType() {
    bool flag = true;
    for (auto& agg_type : scan_agg_types_) {
      if (!IsLastAggType(agg_type)) {
        flag = false;
      }
      if (agg_type == LAST_ROW || agg_type == LASTROWTS) {
        no_last_row_type_ = false;
      }
    }
    return flag;
  }

  inline bool onlyLastRowAggType() {
    for (auto& agg_type : scan_agg_types_) {
      if (agg_type != LAST_ROW && agg_type != LASTROWTS) {
        return false;
      }
    }
    return true;
  }

  inline bool onlyHasFirstLastAggType() {
    for (auto& agg_type : scan_agg_types_) {
      if (!(IsFirstAggType(agg_type) || IsLastAggType(agg_type))) {
        return false;
      }
    }
    return true;
  }

  inline bool hasFoundFirstAggData() {
    return first_last_row_.FirstAggRowValid();
  }

  inline bool hasFoundLastAggData() {
    return first_last_row_.LastAggRowValid();
  }

  // Determine whether certain column types have query results of certain aggregation types,
  // such as non-numeric types do not have SUM aggregation type.
  inline bool colTypeHasAggResult(DATATYPE col_type, Sumfunctype agg_type) {
    if (agg_type == COUNT) {
      return true;
    }
    if (agg_type == FIRST || agg_type == FIRSTTS ||agg_type == FIRST_ROW ||agg_type == FIRSTROWTS ||
        agg_type == LAST || agg_type == LASTTS || agg_type == LAST_ROW || agg_type == LASTROWTS) {
      return false;
    }
    if (agg_type == SUM && !isSumType(col_type)) {
      return false;
    }
    return true;
  }

  inline bool isAllAggResNull(ResultSet* res) {
    for (auto& it : res->data) {
      if (it.size() > 0 && it[0]->count > 0) {
        return false;
      }
    }
    return true;
  }

  inline bool onlyCountTs() {
    if (CLUSTER_SETTING_COUNT_USE_STATISTICS && scan_agg_types_.size() == 1
        && scan_agg_types_[0] == Sumfunctype::COUNT && ts_scan_cols_.size() == 1 && ts_scan_cols_[0] == 0) {
      return true;
    }
    return false;
  }

  /**
   * @brief using iterator find first data info, store into first_last_row_
  */
  KStatus findFirstDataByIter(timestamp64 ts);

  KStatus findFirstData(ResultSet* res, k_uint32* count, timestamp64 ts);

  KStatus countDataUseStatistics(ResultSet* res, k_uint32* count, timestamp64 ts);

  KStatus countDataAllBlocks(ResultSet* res, k_uint32* count, timestamp64 ts);

  /**
   * @brief using iterator find last data info, store into first_last_row_
  */
  KStatus findLastDataByIter(timestamp64 ts);

  KStatus findLastData(ResultSet* res, k_uint32* count, timestamp64 ts);

  /**
   * @brief generate batch info by first_last_row_.
  */
  KStatus genBatchData(ResultSet* res, k_uint32* count);

  KStatus findFirstLastData(ResultSet* res, k_uint32* count, timestamp64 ts);

  KStatus getBlockBitmap(std::shared_ptr<MMapSegmentTable>& segment_tbl, BlockItem* block_item, int type);

  /**
   * @brief Used internally in the Next function, which returns the aggregated result of the most consecutive data in a BlockItem
   *        that meets the query criteria, is an intermediate result. When count is 0, it indicates that the query is completed.
   *
   *        The Next function obtains the intermediate aggregation result of all data within an entity that meets the query criteria
   *        by continuously calling traverseAllBlocks, and then integrates and calculates the result set to obtain the final
   *        aggregation result of the entity.
   */
  KStatus traverseAllBlocks(ResultSet* res, k_uint32* count, timestamp64 ts);

  /**
   * @brief Convert a continuous piece of data into a query column type, and then aggregate and calculate the converted data.
   *
   * @param segment_tbl     segment table pointer, where stores the continuous piece of data
   * @param start_row       the starting row id of this continuous piece of data
   * @param col_idx         queried col index
   * @param count           the number of rows in this continuous piece of data
   * @param mem             memory address for storing converted fixed length data
   * @param var_mem         memory address for storing variable length data after conversion
   * @param bitmap          memory address of bitmap for the data
   * @param need_free_bitmap need free bitmap if the bitmap address is generated using malloc
   */
  KStatus getActualColMemAndBitmap(std::shared_ptr<MMapSegmentTable>& segment_tbl, BLOCK_ID block_id, size_t start_row,
                                   uint32_t col_idx, k_uint32 count, std::shared_ptr<void>* mem,
                                   std::vector<std::shared_ptr<void>>& var_mem, void** bitmap, bool& need_free_bitmap);
  /**
   * @brief Used to obtain the actual Batch object for a specific row of data, if a column type conversion occurs,
   *        the corresponding conversion needs to be performed on the row of data first.
   *        Otherwise, the address can be directly obtained.
   *
   * @param p_bt             partition table pointer, where stores the row of data
   * @param real_row         row id
   * @param col_idx          queried col index
   * @param b                Batch objects that encapsulate data
   */
  int getActualColAggBatch(TsTimePartition* p_bt, MetricRowID real_row, uint32_t col_idx, Batch** b);

 private:
  // The aggregation type corresponding to each column.
  // It can be empty, but if it is not empty, the size must be consistent with the size of scan.cols_
  std::vector<Sumfunctype> scan_agg_types_;
  std::vector<timestamp64> last_ts_points_;
  bool only_first_type_ = false;
  bool no_first_row_type_ = true;
  bool only_last_type_ = false;
  bool only_last_row_type_ = false;
  bool no_last_row_type_ = true;
  bool only_first_last_type_ = false;
  TsFirstLastRow first_last_row_;
  // store all bitmap of columns copyed from mmap file.
  ColBlockBitmaps col_blk_bitmaps_;
  bool all_agg_cols_not_null_{false};
  bool only_count_ts_ = false;
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

class TsOffsetIterator : public TsIterator {
 public:
  TsOffsetIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id,
                   std::map<SubGroupID, std::vector<EntityID>>& entity_ids, std::vector<KwTsSpan>& ts_spans,
                   DATATYPE ts_col_type, std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                   uint32_t table_version, uint32_t offset, uint32_t limit);

  ~TsOffsetIterator() override;

  KStatus Init(bool is_reversed);

  // not available!!!
  bool IsDisordered() override {
    return false;
  }

  uint32_t GetFilterCount() override {
    return filter_cnt_;
  }

  KStatus Next(ResultSet* res, k_uint32* count, timestamp64 ts = INVALID_TS) override;

 private:
  KStatus divideBlockSpans(timestamp64 begin_ts, timestamp64 end_ts,
                           uint32_t* lower_cnt, deque<BlockSpan>& lower_block_span_);
  KStatus filterLower(uint32_t* cnt);
  KStatus filterUpper(uint32_t filter_num, uint32_t* cnt);

  int nextBlockSpan();
  KStatus fetchBlockItems(uint32_t subgroup_id, uint32_t* cnt);

  inline void GetTerminationTime() {
    switch (ts_col_type_) {
      case TIMESTAMP64_LSN:
      case TIMESTAMP64:
        t_time_ = 10;
        break;
      case TIMESTAMP64_LSN_MICRO:
      case TIMESTAMP64_MICRO:
        t_time_ = 10000;
        break;
      case TIMESTAMP64_LSN_NANO:
      case TIMESTAMP64_NANO:
        t_time_ = 10000000;
        break;
      default:
        assert(false);
        break;
    }
  }

 private:
  KLatch latch_;
  uint64_t entity_group_id_{0};
  std::map<SubGroupID, std::vector<EntityID>> entity_ids_;
  // map<timestamp, {subgroup_id}>
  map<timestamp64, vector<uint32_t>> p_times_;
  map<timestamp64, vector<uint32_t>>::iterator p_time_it_;
  // unordered_map<subgroup_id, TsSubGroupPTIterator>
  unordered_map<uint32_t, std::shared_ptr<TsSubGroupPTIterator>> partition_table_iter_;
  unordered_map<uint32_t, TsTimePartition*> cur_partition_table_;
  // the data time range queried by the iterator
  std::vector<KwTsSpan> ts_spans_;

  // column attributes
  vector<AttributeInfo> attrs_;
  DATATYPE ts_col_type_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  std::shared_ptr<TsEntityGroup> entity_group_;
  std::pair<uint32_t, MMapSegmentTableIterator*> segment_iter_ = {0, nullptr};

  uint32_t table_version_;
  bool is_reversed_ = false;
  BlockSpan cur_block_span_;
  std::deque<BlockSpan> filter_block_spans_;
  std::deque<BlockSpan> block_spans_;

  int32_t offset_;
  int32_t limit_;
  int32_t filter_cnt_ = 0;
  bool filter_end_ = false;
  int32_t deviation_ = 1000;

  int32_t queried_cnt = 0;
  timestamp t_time_ = 0;
};

}  //  namespace kwdbts
