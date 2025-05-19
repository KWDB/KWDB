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
  KStatus ConvertBlockSpanToResultSet(TsBlockSpan& ts_blk_span, ResultSet* res, k_uint32* count);
  KStatus ScanEntityBlockSpans();
  KStatus ScanPartitionBlockSpans();
  KStatus GetBlkScanColsInfo(uint32_t version, std::vector<uint32_t>& scan_cols,
                              vector<AttributeInfo>& valid_schema);

  k_int32 cur_entity_index_{-1};
  k_int32 cur_partition_index_{-1};
  TSTableID table_id_;
  uint32_t db_id_;

  std::shared_ptr<TsVGroup> vgroup_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<std::shared_ptr<TsVGroupPartition>> ts_partitions_;

  std::list<TsBlockSpan> ts_block_spans_;
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
  KStatus NextBlockSpan(ResultSet* res, k_uint32* count);
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

struct FirstOrLastCandidate {
  int64_t ts;
  int row_idx = -1;;
  TsBlockSpan blk_span;
  bool valid = false;;
};

class TsAggIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsAggIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                      std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                      std::vector<Sumfunctype>& scan_agg_types, std::vector<timestamp64>& ts_points,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsAggIteratorV2Impl();

  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 protected:
  KStatus AggregateBlockSpans(ResultSet* res, k_uint32* count);
  KStatus AggregateFirstOrLastColumns(const std::vector<k_uint32>& cols, std::vector<TSSlice>& final_agg_data);
  KStatus UpdateFirstAndLastCandidates(const std::vector<k_uint32>& cols, std::vector<FirstOrLastCandidate>& candidates);

  std::vector<Sumfunctype> scan_agg_types_;
};

}  //  namespace kwdbts
