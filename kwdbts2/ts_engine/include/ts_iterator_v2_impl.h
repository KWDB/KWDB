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

#include <memory>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "ts_common.h"
#include "iterator.h"
#include "ts_lastsegment.h"
#include "ts_table_schema_manager.h"
#include "ts_vgroup_partition.h"
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

struct TsPartition {
  std::shared_ptr<TsVGroupPartition> ts_vgroup_partition;
  KwTsSpan ts_partition_range;
};

class TsStorageIteratorV2Impl : public TsStorageIterator {
 public:
  TsStorageIteratorV2Impl();
  TsStorageIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                          std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsStorageIteratorV2Impl();

  KStatus Init(bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 protected:
  KStatus AddMemSegmentBlockSpans();
  KStatus AddLastSegmentBlockSpans();
  KStatus AddEntitySegmentBlockSpans();
  KStatus ConvertBlockSpanToResultSet(shared_ptr<TsBlockSpan> ts_blk_span, ResultSet* res, k_uint32* count);
  KStatus ScanEntityBlockSpans();
  KStatus ScanPartitionBlockSpans();
  KStatus GetBlkScanColsInfo(uint32_t version, std::vector<uint32_t>& scan_cols,
                              vector<AttributeInfo>& valid_schema);
  void UpdateTsSpans(timestamp64 ts);

  k_int32 cur_entity_index_{-1};
  k_int32 cur_partition_index_{-1};
  TSTableID table_id_;
  uint32_t db_id_;

  std::shared_ptr<TsVGroup> vgroup_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<TsPartition> ts_partitions_;

  std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans_;
  std::unordered_map<uint32_t, std::vector<uint32_t>> blk_scan_cols_;
};

class TsRawDataIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsRawDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                          std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsRawDataIteratorV2Impl();

  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 protected:
  KStatus NextBlockSpan(ResultSet* res, k_uint32* count, timestamp64 ts);
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

 protected:
  KStatus ScanAndSortEntityData();
  KStatus MoveToNextEntity();

  std::shared_ptr<TsBlockSpanSortedIterator> block_span_sorted_iterator_{nullptr};
};

class TsAggIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsAggIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                      std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                      std::vector<Sumfunctype>& scan_agg_types, std::vector<timestamp64>& ts_points,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsAggIteratorV2Impl();

  KStatus Init(bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 protected:
  KStatus Aggregate();
  KStatus UpdateAggregation();
  KStatus UpdateAggregation(std::shared_ptr<TsBlockSpan>& block_span,
                            const std::vector<AttributeInfo>& schema);
  void InitAggData(TSSlice& agg_data);
  void InitSumValue(void* data, int32_t type);
  int valcmp(void* l, void* r, int32_t type, int32_t size);
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

  std::vector<TSSlice> final_agg_data_;
  std::vector<AggCandidate> candidates_;
  std::vector<bool> is_overflow_;
  std::vector<k_uint32> first_col_idxs_;
  std::vector<int64_t> first_col_ts_;
  std::vector<k_uint32> last_col_idxs_;
  std::vector<int64_t> last_col_ts_;
  int64_t max_first_ts_;
  int64_t min_last_ts_;

  std::map<k_uint32, k_uint32> first_map_;
  std::map<k_uint32, k_uint32> last_map_;
  std::vector<uint32_t> count_col_idxs_;
  std::vector<uint32_t> sum_col_idxs_;
  std::vector<uint32_t> max_col_idxs_;
  std::vector<uint32_t> min_col_idxs_;

  bool first_last_only_agg_;

  bool has_first_row_col_;
  bool has_last_row_col_;
  bool first_row_need_candidate_;
  bool last_row_need_candidate_;
  AggCandidate first_row_candidate_{INT64_MAX, 0, nullptr};
  AggCandidate last_row_candidate_{INT64_MIN, 0, nullptr};
};

}  //  namespace kwdbts
