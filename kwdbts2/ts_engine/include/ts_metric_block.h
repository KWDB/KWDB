#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "data_type.h"
#include "ts_block.h"
#include "ts_column_block.h"
#include "ts_common.h"
namespace kwdbts {

struct TsMetricCompressInfo {
  int lsn_len;
  std::vector<TsColumnCompressInfo> column_compress_infos;
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
  static KStatus ParseCompressedMetricData();
  const TS_LSN* GetLsnAddr() const { return reinterpret_cast<const TS_LSN*>(lsn_buffer_.data()); }

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
  TsMetricBlockBuilder(const std::vector<AttributeInfo>& col_schemas)
      : col_schemas_(col_schemas), column_block_builders_(col_schemas.size()) {
    for (size_t i = 0; i < col_schemas.size(); i++) {
      column_block_builders_[i] = std::make_unique<TsColumnBlockBuilder>(col_schemas[i]);
    }
  }

  KStatus PutBlockSpan(std::shared_ptr<TsBlockSpan> span);
  std::unique_ptr<TsMetricBlock> GetMetricBlock();
};
}  // namespace kwdbts
