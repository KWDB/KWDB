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
#include <cstdint>
#include <memory>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
namespace kwdbts {
class TsBlock {
 public:
  virtual ~TsBlock() {}
  virtual TSTableID GetTableId() = 0;
  virtual uint32_t GetTableVersion() = 0;
  virtual size_t GetRowNum() = 0;
  // if has three rows, this return three value for certain column using col-based storege struct.
  virtual KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                             char** value) = 0;
  virtual KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                               TsBitmap& bitmap) = 0;
  virtual KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                                TSSlice& value) = 0;
  virtual bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) = 0;
  // if just get timestamp , this function return fast.
  virtual timestamp64 GetTS(int row_num) = 0;
};

struct TsBlockSpan {
  const TSEntityID entity_id;

  std::shared_ptr<TsBlock> block;
  int start_row, nrow;

  TsBlockSpan(TSTableID table_id, uint32_t table_version, TSEntityID entitiy_id,
              std::shared_ptr<TsBlock> block, int start, int nrow)
      : entity_id(entitiy_id), block(block), start_row(start), nrow(nrow) {
    assert(nrow >= 1);
  }

  TSTableID GetTableID() const { return block->GetTableId(); }
  TSTableID GetTableVersion() const { return block->GetTableVersion(); }

  // if just get timestamp, these function return fast.
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
    *min_ts = block->GetTS(start_row);
    *max_ts = block->GetTS(start_row + nrow - 1);
  }
};
}  // namespace kwdbts