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
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <list>
#include <vector>
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_bitmap.h"

namespace kwdbts {

class TsBlockSpan;
class TsBlock;

// notice: make sure block_ no free, while TSBlkSpanDataTypeConvert exists.
class TSBlkDataTypeConvert {
 private:
  TsBlock* block_ = nullptr;
  uint32_t start_row_idx_ = 0;
  uint32_t row_num_ = 0;
  std::list<char*> alloc_mems_;

 public:
  TSBlkDataTypeConvert() = default;

  explicit TSBlkDataTypeConvert(TsBlockSpan& blk_span);

  TSBlkDataTypeConvert(TsBlock* block, uint32_t row_idx, uint32_t row_num) :
      block_(block), start_row_idx_(row_idx), row_num_(row_num) {}

  ~TSBlkDataTypeConvert() {
    for (auto mem : alloc_mems_) {
      free(mem);
    }
    alloc_mems_.clear();
  }

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema, const AttributeInfo& dest_type,
                             char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& dest_type, DataFlags& flag, TSSlice& data);

  KStatus GetColBitmap(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap);
};


}  // namespace kwdbts
