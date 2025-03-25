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

#include <map>
#include <string>
#include <vector>
#include <cstdio>
#include <list>
#include "ts_common.h"
#include "ts_const.h"

namespace kwdbts {

enum TsIterResultSetType {
  RESULT_SET_COL_BASED,
  BLOCK_ITEM_REFER,
};

/**
 * TsIteratorParams used for store parameters of iterator for storage data.
 */
struct TsIteratorParams {
  TSTableID table_id = 0;
  std::vector<TSSlice> primary_keys;
  std::vector<KwTsSpan> ts_spans;
  std::vector<uint32_t> kw_scan_cols;
  uint32_t table_version = 0;
  bool is_reversed = false;
  bool need_sort = false;
  TsIterResultSetType resulst_type = BLOCK_ITEM_REFER;
};

struct TsMetricBatchRefer {
  TsBlockItem blk_item;
  bool all_block_data;
  std::list<uint32_t> metric_idx;
};

class IterResultSet {
 private:
  TsIterResultSetType type_;
  union {
    ResultSet* result_set_;
    TsMetricBatchRefer* batch_refer_;
  };

 public:
  explicit IterResultSet(ResultSet* res) : type_(TsIterResultSetType::RESULT_SET_COL_BASED), result_set_(res) {}
  explicit IterResultSet(TsMetricBatchRefer* ref) : type_(TsIterResultSetType::BLOCK_ITEM_REFER), batch_refer_(ref) {}
  ~IterResultSet() {
    if (TsIterResultSetType::RESULT_SET_COL_BASED == type_) {
      if (result_set_ != nullptr) {
        delete result_set_;
        result_set_ = nullptr;
      }
    } else {
      if (batch_refer_ != nullptr) {
        delete batch_refer_;
        batch_refer_ = nullptr;
      }
    }
  }
  ResultSet* GetResultSet() {
    if (TsIterResultSetType::RESULT_SET_COL_BASED == type_) {
      return result_set_;
    }
    return nullptr;
  }
  TsMetricBatchRefer* GetMetricBatchRefer() {
    if (TsIterResultSetType::BLOCK_ITEM_REFER == type_) {
      return batch_refer_;
    }
    return nullptr;
  }
};

/**
 * @brief This is the iterator base class implemented internally in the storage layer, and its two derived classes are:
 *        (1) TsRawDataIterator, used for raw data queries (2) TsAggIterator, used for aggregate queries
 */
class TsIteratorv2 {
 private:
  const TsIteratorParams& params_;
  std::vector<k_uint32> ts_scan_cols_;
  vector<AttributeInfo> attrs_;

 public:
  explicit TsIteratorv2(const TsIteratorParams& param) : params_(param) {}

  virtual ~TsIteratorv2();

  virtual KStatus Init();

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
   * @param is_finished    identify whether the iterator has completed querying
   * @param ts             used for block and partition table level data filtering during orderbylimit queries
   */
  virtual KStatus Next(IterResultSet* res, bool* is_finished, timestamp64 ts = INVALID_TS) = 0;


 protected:
  inline bool checkIfTsInSpan(timestamp64 ts) {
    for (auto& ts_span : params_.ts_spans) {
      if (ts >= ts_span.begin && ts <= ts_span.end) {
        return true;
      }
    }
    return false;
  }
  // ts is used for block filter for orderbylimit queries
  // int nextBlockItem(k_uint32 entity_id, timestamp64 ts = INVALID_TS);

  // TsTimePartition* cur_partition_table_ = nullptr;
  // std::shared_ptr<TsSubGroupPTIterator> partition_table_iter_;
  // // save all BlockItem objects in the partition table being queried
  // std::deque<BlockItem*> block_item_queue_;
  // // save the data offset within the BlockItem object being queried, used for traversal
  // k_uint32 cur_blockdata_offset_ = 1;
  // BlockItem* cur_block_item_ = nullptr;
  // k_uint32 cur_entity_idx_ = 0;
  // MMapSegmentTableIterator* segment_iter_{nullptr};
  // std::shared_ptr<TsEntityGroup> entity_group_;
  // Identifies whether the iterator returns blocks in reverse order
};




}  // namespace kwdbts
