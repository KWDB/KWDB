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

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_type.h"
#include "libkwdbts2.h"
#include "ts_block.h"
#include "ts_bufferbuilder.h"
#include "ts_column_block.h"
#include "ts_common.h"
#include "ts_engine_schema_manager.h"
#include "ts_sliceguard.h"
#include "ts_table_schema_manager.h"
namespace kwdbts {

struct TsMetricCompressInfo {
  int row_count;
  int osn_len;
  std::vector<TsColumnCompressInfo> column_compress_infos;

  struct OffsetLength {
    size_t offset;
    size_t length;
  };
  std::vector<OffsetLength> column_data_segments;
};
class TsMetricBlock {
  friend class TsMetricBlockBuilder;

 private:
  int count_;
  std::vector<uint64_t> osn_buffer_;
  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks_;

  TsMetricBlock(int count, std::vector<uint64_t>&& osn_buffer,
                std::vector<std::unique_ptr<TsColumnBlock>>&& column_blocks)
      : count_(count), osn_buffer_(std::move(osn_buffer)), column_blocks_(std::move(column_blocks)) {}

 public:
  static KStatus ParseCompressedMetricData(const std::vector<AttributeInfo>& schema, TsSliceGuard&& compressed_data,
                                           const TsMetricCompressInfo& compress_info,
                                           std::unique_ptr<TsMetricBlock>* metric_block);
  const uint64_t* GetOSNAddr() const { return reinterpret_cast<const uint64_t*>(osn_buffer_.data()); }
  const timestamp64* GetTSAddr() const { return reinterpret_cast<const timestamp64*>(column_blocks_[0]->GetColAddr()); }
  int GetColNum() const { return column_blocks_.size(); }
  int GetRowNum() const { return count_; }

  bool GetCompressedData(TsBufferBuilder* output, TsMetricCompressInfo* compress_info, bool compress_ts_and_osn,
                         bool compress_columns);
};

class TsMetricBlockBuilder {
 private:
  // TODO(zzr): avoid copy schema;
  // Note: DO NOT USE const reference. If so, col_schemas_ may point to an object created on
  // stack and will be destroyed after the function returns.
  std::shared_ptr<TsTableSchemaManager> table_schema_manager_;
  std::shared_ptr<MMapMetricsTable> schema_table_;
  const std::vector<AttributeInfo>* col_schemas_{nullptr};
  std::vector<std::unique_ptr<TsColumnBlockBuilder>> column_block_builders_;

  int count_ = 0;
  std::vector<uint64_t> osn_buffer_;

  struct PrivateConstructTag {};

 public:
  explicit TsMetricBlockBuilder(std::shared_ptr<TsTableSchemaManager> table_schema_manager,
                                std::shared_ptr<MMapMetricsTable> schema_table,
                                const std::vector<AttributeInfo>* col_schemas, PrivateConstructTag private_)
      : table_schema_manager_(std::move(table_schema_manager)),
        schema_table_(std::move(schema_table)),
        col_schemas_(col_schemas),
        column_block_builders_(col_schemas_->size()) {
    for (size_t i = 0; i < col_schemas->size(); i++) {
      column_block_builders_[i] = std::make_unique<TsColumnBlockBuilder>((*col_schemas_)[i]);
    }
  }

  static std::pair<std::unique_ptr<TsMetricBlockBuilder>, KStatus> Create(TsEngineSchemaManager* engine_schema_manager,
                                                                          TSTableID table_id, uint32_t table_version) {
    std::shared_ptr<TsTableSchemaManager> table_schema_manager;

    auto s = engine_schema_manager->GetTableSchemaMgr(table_id, table_schema_manager);
    if (s == FAIL) {
      LOG_ERROR("Failed to get table schema manager for table %lu", table_id);
      return {std::unique_ptr<TsMetricBlockBuilder>(), FAIL};
    }

    std::shared_ptr<MMapMetricsTable> schema_table;
    s = table_schema_manager->GetMetricSchema(table_version, &schema_table);
    if (s == FAIL) {
      LOG_ERROR("Failed to get metric schema for version %u", table_version);
      return {std::unique_ptr<TsMetricBlockBuilder>(), FAIL};
    }

    auto col_schemas = schema_table->getSchemaInfoExcludeDroppedPtr();
    return {std::make_unique<TsMetricBlockBuilder>(std::move(table_schema_manager), std::move(schema_table),
                                                   col_schemas, PrivateConstructTag{}),
            SUCCESS};
  }

  KStatus PutBlockSpan(const std::shared_ptr<TsBlockSpan> &span);
  std::unique_ptr<TsMetricBlock> GetMetricBlock();
  int GetRowNum() const { return count_; }
  void Reset() {
    count_ = 0;
    osn_buffer_.clear();
    for (auto& builder : column_block_builders_) {
      builder->Reset();
    }
  }
};
}  // namespace kwdbts
