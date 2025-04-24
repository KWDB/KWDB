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
#include "ts_common.h"
#include "ts_lastsegment.h"
#include "ts_table_schema_manager.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {

#define KW_BITMAP_SIZE(n)  (n + 7) >> 1

typedef enum {
  SCAN_STATUS_UNKNOWN,
  SCAN_MEM_TABLE,
  SCAN_LAST_SEGMENT,
  SCAN_BLOCK_SEGMENT,
  SCAN_STATUS_DONE
} STORAGE_SCAN_STATUS;

class TsVGroup;
class TsMemSegmentIterator;
class TsLastSegmentIterator;
class TsBlockSegmentIterator;
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
  KStatus InitializeMemSegmentIterator();
  KStatus InitializeLastSegmentIterator();
  KStatus InitializeBlockSegmentIterator();

  k_uint32 cur_entity_index_;
  k_uint32 cur_partition_index_;
  std::unique_ptr<TsMemSegmentIterator> mem_segment_iterator_{nullptr};
  std::unique_ptr<TsLastSegmentIterator> last_segment_iterator_{nullptr};
  std::unique_ptr<TsBlockSegmentIterator> block_segment_iterator_{nullptr};

  std::shared_ptr<TsVGroup> vgroup_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<std::shared_ptr<TsVGroupPartition>> ts_partitions_;
};

class TsRawDataIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsRawDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                          std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsRawDataIteratorV2Impl();

  KStatus Init(bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 protected:
  STORAGE_SCAN_STATUS status_{SCAN_STATUS_UNKNOWN};
};

class TsSortedRowDataIteratorV2Impl : public TsStorageIteratorV2Impl {
 public:
  TsSortedRowDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                                std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                                SortOrder order_type = ASC);
  ~TsSortedRowDataIteratorV2Impl();

  KStatus Init(bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;
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
  std::vector<Sumfunctype> scan_agg_types_;
};

class TsSegmentIterator {
 public:
  TsSegmentIterator(std::shared_ptr<TsVGroup>& vgroup, std::shared_ptr<TsVGroupPartition> ts_partition,
                    uint32_t entity_id, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                    std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                    std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  virtual ~TsSegmentIterator();
  virtual KStatus Init();
  virtual KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished);
  virtual KStatus ScanCount(k_uint32* count);

 protected:
  KStatus AddBlockData(std::shared_ptr<TsBlock> ts_block, ResultSet* res, k_uint32* count);

  KStatus AddBlockSpanData(const TsBlockSpan& ts_block, ResultSet* res, k_uint32* count);

  std::list<TsBlockSpan> ts_block_spans_;
  uint32_t entity_id_;
  std::shared_ptr<TsVGroupPartition> ts_partition_{nullptr};

  std::shared_ptr<TsVGroup> vgroup_;
  STORAGE_SCAN_STATUS status_{SCAN_STATUS_UNKNOWN};
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<std::shared_ptr<TsVGroupPartition>> ts_partitions_;

  std::vector<KwTsSpan> ts_spans_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  DATATYPE ts_col_type_;
    // table version
  uint32_t table_version_;
  // column attributes
  vector<AttributeInfo> attrs_;
};

class TsMemSegmentIterator : public TsSegmentIterator {
 public:
  TsMemSegmentIterator(std::shared_ptr<TsVGroup>& vgroup, uint32_t entity_id,
                        std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                        std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                        std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsMemSegmentIterator();

  KStatus Init() override;
};

class TsLastSegmentIterator : public TsSegmentIterator {
 public:
  TsLastSegmentIterator(std::shared_ptr<TsVGroup>& vgroup, std::shared_ptr<TsVGroupPartition> ts_partition,
                        uint32_t entity_id, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                        std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                        std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsLastSegmentIterator();

  KStatus Init() override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished) override;
  KStatus ScanAgg(k_uint32* count);
 private:
  std::vector<std::shared_ptr<TsLastSegmentEntityBlockIteratorBase>> last_segment_block_iterators_;
  uint32_t last_segment_block_iterator_index_;
};

class TsBlockSegmentIterator : public TsSegmentIterator {
 public:
  TsBlockSegmentIterator(std::shared_ptr<TsVGroup>& vgroup, std::shared_ptr<TsVGroupPartition> ts_partition,
                         uint32_t entity_id, std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                         std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                         std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version);
  ~TsBlockSegmentIterator();

  KStatus Init() override;
};

}  //  namespace kwdbts
