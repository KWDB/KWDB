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
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_bitmap.h"

namespace kwdbts {

class TsBlockSpan;

class TSBlkSpanDataTypeConvert {
 private:
  const TsBlockSpan& blk_span_;
  std::list<char*> alloc_mems_;

 public:
  TSBlkSpanDataTypeConvert(const TsBlockSpan& blk_span) : blk_span_(blk_span) {}

  ~TSBlkSpanDataTypeConvert() {
    for (auto mem : alloc_mems_) {
      free(mem);
    }
    alloc_mems_.clear();
  }

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, const AttributeInfo& desc_type,
                             char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& desc_type, DataFlags& flag, TSSlice& data);

  KStatus GetAggResult(uint32_t col_id, const std::vector<AttributeInfo>& schema, const AttributeInfo& desc_type,
    std::vector<Sumfunctype> agg_types, std::vector<TSSlice>& agg_data);
};

class TSBlkSpanDataAgg {
 private:
  const TsBlockSpan& blk_span_;
  std::list<char*> alloc_mems_;

 public:
  TSBlkSpanDataAgg(const TsBlockSpan& blk_span) : blk_span_(blk_span) {}

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, const AttributeInfo& desc_type,
                             char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t col_idx, const std::vector<AttributeInfo>& schema,
    const AttributeInfo& desc_type, DataFlags& flag, TSSlice& data);
};



}  // namespace kwdbts
