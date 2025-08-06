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
#include <memory>
#include <vector>
#include <list>
#include <map>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include "ts_common.h"
#include "iterator.h"
#include "ts_lastsegment.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"
#include "ts_block_span_sorted_iterator.h"

namespace kwdbts {

#define KW_BITMAP_SIZE(n)  (n + 7) >> 1

typedef enum {
  SCAN_STATUS_UNKNOWN,
  SCAN_MEM_SEGMENT,
  SCAN_LAST_SEGMENT,
  SCAN_ENTITY_SEGMENT,
  SCAN_STATUS_DONE
} STORAGE_SCAN_STATUS;

class TsVGroup;
class TsMemSegmentIterator;
class TsLastSegmentIterator;
class TsEntitySegmentIterator;

class TsStorageIteratorV2Impl : public TsStorageIterator {
 public:
  TsStorageIteratorV2Impl();
  TsStorageIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                          std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsStorageIteratorV2Impl();

  KStatus Init(bool is_reversed) override;

 protected:
  bool IsFilteredOut(timestamp64 begin_ts, timestamp64 end_ts, timestamp64 ts);
  /*
   * Update ts_spans_ to reduce the data scanning from storage based on timestamp(ts)
   * provided by execution engine.
   */
  void UpdateTsSpans(timestamp64 ts);
  /*
   * Convert block span data to result set which will be returned to execution engine
   * for further process.
   */
  KStatus ScanEntityBlockSpans(timestamp64 ts);

  k_int32 cur_entity_index_{-1};
  k_int32 cur_partition_index_{-1};
  TSTableID table_id_;
  uint32_t db_id_;

  std::shared_ptr<TsVGroup> vgroup_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<std::shared_ptr<const TsPartitionVersion>> ts_partitions_;

  std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans_;

  std::shared_ptr<TsScanFilterParams> filter_;
};

class TsSortedRawDataIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsSortedRawDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                                std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                                SortOrder order_type = ASC);
  ~TsSortedRawDataIteratorV2Impl();

  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;
  bool IsDisordered() override;

 protected:
  KStatus ScanAndSortEntityData(timestamp64 ts);
  KStatus MoveToNextEntity(timestamp64 ts);

  std::shared_ptr<TsBlockSpanSortedIterator> block_span_sorted_iterator_{nullptr};
};

class TsAggIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsAggIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                      std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                      std::vector<k_int32>& agg_extend_cols,
                      std::vector<Sumfunctype>& scan_agg_types, std::vector<timestamp64>& ts_points,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsAggIteratorV2Impl();

  KStatus Init(bool is_reversed) override;
  // need call Next function times: entity_ids.size(), no matter Next return what.
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;
  bool IsDisordered() override;

 protected:
  KStatus Aggregate();
  KStatus UpdateAggregation(bool can_remove_last_candidate);
  KStatus UpdateAggregation(std::shared_ptr<TsBlockSpan>& block_span,
                            bool aggregate_first_last_cols,
                            bool can_remove_last_candidate);
  void InitAggData(TSSlice& agg_data);
  void InitSumValue(void* data, int32_t type);
  void UpdateTsSpans();
  void ConvertToDoubleIfOverflow(uint32_t blk_col_idx, TSSlice& agg_data);
  KStatus AddSumNotOverflowYet(uint32_t blk_col_idx,
                                int32_t type,
                                void* current,
                                TSSlice& agg_data);
  KStatus AddSumOverflow(int32_t type,
                          void* current,
                          TSSlice& agg_data);

  std::vector<Sumfunctype> scan_agg_types_;
  std::vector<timestamp64> last_ts_points_;
  std::vector<k_int32> agg_extend_cols_;

  std::vector<TSSlice> final_agg_data_;
  std::vector<AggCandidate> candidates_;
  std::vector<bool> is_overflow_;
  std::vector<k_uint32> first_col_idxs_;
  std::vector<int64_t> first_col_ts_;
  std::vector<k_uint32> last_col_idxs_;
  std::vector<int64_t> last_col_ts_;

  std::vector<k_uint32> cur_first_col_idxs_;
  std::vector<k_uint32> cur_last_col_idxs_;

  std::map<k_uint32, k_uint32> max_map_;
  std::map<k_uint32, k_uint32> min_map_;

  std::map<k_uint32, k_uint32> first_map_;
  std::map<k_uint32, k_uint32> last_map_;
  std::vector<uint32_t> count_col_idxs_;
  std::vector<uint32_t> sum_col_idxs_;
  std::vector<uint32_t> max_col_idxs_;
  std::vector<uint32_t> min_col_idxs_;

  bool first_last_only_agg_;

  bool has_first_row_col_;
  bool has_last_row_col_;
  bool only_count_ts_{false};
  bool only_last_row_{true};
  AggCandidate first_row_candidate_{INT64_MAX, 0, nullptr};
  AggCandidate last_row_candidate_{INT64_MIN, 0, nullptr};
};

class TsOffsetIteratorV2Impl : public TsIterator {
 public:
  TsOffsetIteratorV2Impl(std::map<uint32_t, std::shared_ptr<TsVGroup>>& vgroups,
                         std::map<uint32_t, std::vector<EntityID>>& vgroup_ids, std::vector<KwTsSpan>& ts_spans,
                         DATATYPE ts_col_type, std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                         std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                         uint32_t offset, uint32_t limit)
      : table_version_(table_version),
        table_schema_mgr_(table_schema_mgr),
        ts_col_type_(ts_col_type),
        ts_spans_(ts_spans),
        kw_scan_cols_(kw_scan_cols),
        ts_scan_cols_(ts_scan_cols),
        vgroup_ids_(vgroup_ids),
        vgroups_(vgroups),
        offset_(offset),
        limit_(limit) {}

  ~TsOffsetIteratorV2Impl() override {}

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
  KStatus ScanPartitionBlockSpans(uint32_t* cnt);

  KStatus divideBlockSpans(timestamp64 begin_ts, timestamp64 end_ts, uint32_t* lower_cnt,
                           deque<std::shared_ptr<TsBlockSpan>>& lower_block_span);
  KStatus filterLower(uint32_t* cnt);
  KStatus filterUpper(uint32_t filter_num, uint32_t* cnt);
  KStatus filterBlockSpan();

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
  uint32_t db_id_;
  TSTableID table_id_;
  uint32_t table_version_;
  // column attributes
  vector<AttributeInfo> attrs_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;

  DATATYPE ts_col_type_;
  bool is_reversed_ = false;
  // the data time range queried by the iterator
  std::vector<KwTsSpan> ts_spans_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<uint32_t> ts_scan_cols_;
  std::unordered_map<uint32_t, std::vector<uint32_t>> blk_scan_cols_;

  std::map<uint32_t, std::vector<EntityID>> vgroup_ids_;
  std::map<uint32_t, std::shared_ptr<TsVGroup>> vgroups_;
  // map<timestamp, {vgroup_id, TsPartition}>
  TimestampComparator comparator_;
  map<timestamp64, std::vector<pair<uint32_t, std::shared_ptr<const TsPartitionVersion>>>, TimestampComparator> p_times_;
  map<timestamp64, std::vector<pair<uint32_t, std::shared_ptr<const TsPartitionVersion>>>>::iterator p_time_it_;

  std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans_;
  std::deque<std::shared_ptr<TsBlockSpan>> block_spans_;
  std::deque<std::shared_ptr<TsBlockSpan>> filter_block_spans_;

  int32_t offset_;
  int32_t limit_;
  int32_t filter_cnt_ = 0;
  int32_t queried_cnt = 0;
  bool filter_end_ = false;

  timestamp t_time_ = 0;
  int32_t deviation_ = 1000;
  // todo(liangbo) set lsn parameter.
  TS_LSN scan_lsn_{UINT64_MAX};
};

}  //  namespace kwdbts
