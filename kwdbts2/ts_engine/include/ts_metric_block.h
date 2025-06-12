#pragma once

#include <memory>

#include "data_type.h"
#include "ts_block.h"
#include "ts_column_block.h"
#include "ts_common.h"
namespace kwdbts {
class TsMetricBlock {
 private:
  std::unique_ptr<TS_LSN[]> lsn_arr_;
  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks_;

  TsMetricBlock() = default;

 public:
};

class TsMetricBlockBuilder {
 private:
  const std::vector<AttributeInfo> col_schemas_;
  std::vector<std::unique_ptr<TsColumnBlockBuilder>> column_block_builders_;

 public:
  TsMetricBlockBuilder(const std::vector<AttributeInfo>& col_schemas)
      : col_schemas_(col_schemas), column_block_builders_(col_schemas.size()) {
    for (size_t i = 0; i < col_schemas.size(); i++) {
      column_block_builders_[i] = std::make_unique<TsColumnBlockBuilder>(col_schemas[i]);
    }
  }

  KStatus PutBlockSpan(std::shared_ptr<TsBlockSpan> span);
};
}  // namespace kwdbts
