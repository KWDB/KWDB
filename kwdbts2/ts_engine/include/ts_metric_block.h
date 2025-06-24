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
#include "ts_block.h"
#include "ts_column_block.h"
#include "ts_common.h"
namespace kwdbts {

struct TsMetricCompressInfo {
  int row_count;
  int lsn_len;
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
  std::string lsn_buffer_;
  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks_;

  TsMetricBlock(int count, std::string&& lsn_buffer,
                std::vector<std::unique_ptr<TsColumnBlock>>&& column_blocks)
      : count_(count),
        lsn_buffer_(std::move(lsn_buffer)),
        column_blocks_(std::move(column_blocks)) {}

 public:
  static KStatus ParseCompressedMetricData(const std::vector<AttributeInfo>& schema,
                                           TSSlice compressed_data,
                                           const TsMetricCompressInfo& compress_info,
                                           std::unique_ptr<TsMetricBlock>* metric_block);
  const TS_LSN* GetLSNAddr() const { return reinterpret_cast<const TS_LSN*>(lsn_buffer_.data()); }

  bool GetCompressedData(std::string* output, TsMetricCompressInfo* compress_info);
};

class TsMetricBlockBuilder {
 private:
  // TODO(zzr): avoid copy schema;
  // Note: DO NOT USE const reference. If so, col_schemas_ may point to an object created on
  // stack and will be destroyed after the function returns.
  const std::vector<AttributeInfo> col_schemas_;
  std::vector<std::unique_ptr<TsColumnBlockBuilder>> column_block_builders_;

  int count_ = 0;
  std::string lsn_buffer_;

 public:
  explicit TsMetricBlockBuilder(const std::vector<AttributeInfo>& col_schemas)
      : col_schemas_(col_schemas), column_block_builders_(col_schemas.size()) {
    for (size_t i = 0; i < col_schemas.size(); i++) {
      column_block_builders_[i] = std::make_unique<TsColumnBlockBuilder>(col_schemas[i]);
    }
  }

  KStatus PutBlockSpan(std::shared_ptr<TsBlockSpan> span);
  std::unique_ptr<TsMetricBlock> GetMetricBlock();
};
}  // namespace kwdbts
