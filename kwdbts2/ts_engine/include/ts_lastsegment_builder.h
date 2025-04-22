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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "data_type.h"
#include "libkwdbts2.h"
#include "ts_common.h"
#include "ts_engine_schema_manager.h"
#include "ts_lastsegment.h"
#include "ts_payload.h"

namespace kwdbts {
class TsLastSegmentBuilder {
  std::unique_ptr<TsFile> last_segment_;

  struct BlockInfo;
  class MetricBlockBuilder;  // Helper for build DataBlock
  class InfoHandle;
  class IndexHandle;

  // TODO(zzr) Meta Blocks Handle...

  std::unique_ptr<MetricBlockBuilder> data_block_builder_;
  std::unique_ptr<InfoHandle> info_handle_;
  std::unique_ptr<IndexHandle> index_handle_;

  TSTableID table_id_ = -1;  // INVALID ID
  uint32_t version_ = -1;    // INVALID ID
  uint32_t file_number_;

  size_t nblock_ = 0;
  struct EntityPayload {
    TS_LSN seq_no;
    TSEntityID entity_id;
    TSSlice metric;
  };
  struct EntityColData {
    TS_LSN seq_no;
    TSEntityID entity_id;
    std::vector<TSSlice> col_data;
  };

  struct PayloadBuffer {
    std::vector<EntityPayload> buffer;
    bool disordered = false;
    std::unique_ptr<TsRawPayloadRowParser> parser;

    void Reset(const std::vector<AttributeInfo>& schema) {
      parser = std::make_unique<TsRawPayloadRowParser>(schema);
    }

    bool Compare(const EntityPayload& lhs, const EntityPayload& rhs) const {
      auto ts_lhs = parser->GetTimestamp(lhs.metric);
      auto ts_rhs = parser->GetTimestamp(rhs.metric);
      return lhs.entity_id < rhs.entity_id || (lhs.entity_id == rhs.entity_id && ts_lhs < ts_rhs);
    }
    void push_back(const EntityPayload& v) {
      assert(buffer.empty() || buffer.back().entity_id <= v.entity_id);
      disordered = disordered || (!buffer.empty() && Compare(v, buffer.back()));
      buffer.push_back(v);
    }
    void sort() {
      if (!disordered) return;
      std::sort(
          buffer.begin(), buffer.end(),
          [this](const EntityPayload& l, const EntityPayload& r) { return this->Compare(l, r); });
    }
    void clear() {
      buffer.clear();
      disordered = false;
    }
  };
  PayloadBuffer payload_buffer_;

  struct ColDataBuffer {
    std::vector<EntityColData> buffer;
    bool disordered = false;

    bool Compare(const EntityColData& lhs, const EntityColData& rhs) const {
      auto ts_lhs = KTimestamp(lhs.col_data[0].data);
      auto ts_rhs = KTimestamp(lhs.col_data[0].data);
      return lhs.entity_id < rhs.entity_id || (lhs.entity_id == rhs.entity_id && ts_lhs < ts_rhs);
    }
    void push_back(const EntityColData& v) {
      assert(buffer.empty() || buffer.back().entity_id <= v.entity_id);
      disordered = disordered || (!buffer.empty() && Compare(v, buffer.back()));
      buffer.push_back(v);
    }
    void sort() {
      if (!disordered) return;
      std::sort(
        buffer.begin(), buffer.end(),
        [this](const EntityColData& l, const EntityColData& r) { return this->Compare(l, r); });
    }
    void clear() {
      buffer.clear();
      disordered = false;
    }
  };
  ColDataBuffer cols_data_buffer_;

  const TsEngineSchemaManager* schema_mgr_;

  KStatus WriteMetricBlock(MetricBlockBuilder* builder);
  KStatus FlushPayloadBuffer();
  KStatus FlushColDataBuffer();

 public:
  TsLastSegmentBuilder(TsEngineSchemaManager* schema_mgr, std::unique_ptr<TsFile>&& last_segment,
                       uint32_t file_number)
      : last_segment_(std::move(last_segment)),
        data_block_builder_(std::make_unique<MetricBlockBuilder>(schema_mgr)),
        info_handle_(std::make_unique<InfoHandle>()),
        index_handle_(std::make_unique<IndexHandle>()),
        file_number_(file_number),
        schema_mgr_(schema_mgr) {}

  KStatus FlushBuffer();

  KStatus Finalize();
  KStatus PutRowData(TSTableID table_id, uint32_t version, TSEntityID entity_id, TS_LSN seq_no,
                     TSSlice row_data);

  KStatus PutColData(TSTableID table_id, uint32_t version, TSEntityID entity_id, TS_LSN seq_no,
                     std::vector<TSSlice> col_data);

  uint32_t GetFileNumber() const {return file_number_;}

  bool ConsistentWith(TSTableID table_id, uint32_t version) const {
    return table_id == table_id_ && version_ == version;
  }
};

struct TsLastSegmentBuilder::BlockInfo {
  struct ColInfo {
    uint32_t col_offset;
    uint16_t bitmap_len;
    uint32_t data_len;
  };
  TSTableID table_id;
  uint32_t version;
  uint32_t nrow;
  uint32_t ndevice;
  uint32_t var_offset;
  uint32_t var_len;
  int64_t min_ts, max_ts;
  uint64_t min_entity_id, max_entity_id;
  std::vector<ColInfo> col_infos;
  BlockInfo() { Reset(-1, -1); }
  void Reset(TSTableID table_id, uint32_t version) {
    this->table_id = table_id;
    this->version = version;

    nrow = ndevice = var_offset = var_len = 0;
    max_ts = INT64_MIN;
    min_ts = INT64_MAX;
    min_entity_id = UINT64_MAX;
    max_entity_id = 0;
    col_infos.clear();
  }
};

class TsLastSegmentBuilder::InfoHandle {
 private:
  bool finished_;
  uint64_t cursor_ = 0;
  std::vector<BlockInfo> infos_;
  std::vector<uint64_t> offset_;

  size_t length_ = 0;  // for debug;

 public:
  size_t RecordBlock(size_t block_length, const BlockInfo& info);
  KStatus WriteInfo(TsFile*);
};

class TsLastSegmentBuilder::IndexHandle {
 private:
  bool finished_;
  uint64_t cursor_ = 0;
  std::vector<TsLastSegmentBlockIndex> indices_;

 public:
  void RecordBlockInfo(size_t info_length, const BlockInfo& info);
  void ApplyInfoBlockOffset(size_t offset);
  KStatus WriteIndex(TsFile*);
};
class TsLastSegmentBuilder::MetricBlockBuilder {
 private:
  class ColumnBlockBuilder;
  std::vector<std::unique_ptr<ColumnBlockBuilder>> colblocks_;

  TsEngineSchemaManager* schema_mgr_;
  std::shared_ptr<MMapMetricsTable> table_schema_;

  std::vector<AttributeInfo> metric_schema_;
  std::unique_ptr<TsRawPayloadRowParser> parser_;

  std::string varchar_buffer_;
  bool finished_ = true;

  BlockInfo info_;
  TSEntityID last_entity_id_ = -1;

 public:
  // do not copy;
  MetricBlockBuilder(const MetricBlockBuilder&) = delete;
  void operator=(const MetricBlockBuilder&) = delete;

  explicit MetricBlockBuilder(TsEngineSchemaManager* schema_mgr);

  KStatus Reset(TSTableID table_id, uint32_t table_version);

  void Add(TSEntityID entity_id, TS_LSN seq_no, TSSlice metric_data);
  void Add(TSEntityID entity_id, TS_LSN seq_no, const std::vector<TSSlice>& col_data);
  void Finish();
  bool IsFinished() const { return finished_; }
  BlockInfo GetBlockInfo() const;
  int GetNRows() const { return info_.nrow; }
  int GetNColumns() const { return colblocks_.size(); }
  bool Empty() const { return info_.nrow == 0; }
  void Reserve(size_t nrow);

  TSSlice GetColumnData(size_t i);
  std::vector<TSSlice> GetColumnDatas();
  TSSlice GetColumnBitmap(size_t i);

  TSSlice GetVarcharBuffer() { return {varchar_buffer_.data(), varchar_buffer_.size()}; }
};

class TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder {
 private:
  bool has_bitmap_;
  TsBitmap bitmap_;
  std::string bitmap_buffer_;
  std::string data_buffer_;
  DATATYPE dtype_;

  uint32_t row_cnt_ = 0;
  int dsize_ = -1;

 public:
  // do not copy
  ColumnBlockBuilder(const ColumnBlockBuilder&) = delete;
  void operator=(const ColumnBlockBuilder&) = delete;

  explicit ColumnBlockBuilder(DATATYPE dtype, bool has_bitmap)
      : has_bitmap_(has_bitmap), dtype_(dtype) {
    if (dtype_ == TIMESTAMP64_LSN_MICRO || dtype_ == TIMESTAMP64_LSN ||
        dtype_ == TIMESTAMP64_LSN_NANO) {
      // discard LSN
      dsize_ = 8;
    } else {
      dsize_ = getDataTypeSize(dtype);
    }
  }
  void Add(const TSSlice& col_data) noexcept;
  DATATYPE GetDatatype() const { return dtype_; }
  void Compress();
  void Reserve(size_t nrow) {
    data_buffer_.reserve(nrow * getDataTypeSize(dtype_));
    bitmap_.Reset(has_bitmap_ ? nrow : 0);
  }
  TSSlice GetData() { return TSSlice{data_buffer_.data(), data_buffer_.size()}; }

  TSSlice GetBitmap() { return TSSlice{bitmap_buffer_.data(), bitmap_buffer_.size()}; }
};
}  // namespace kwdbts
