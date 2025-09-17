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
#include <map>
#include <memory>
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_bitmap.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

class TsBlockSpan;
class TsBlock;

// notice: make sure block_ no free, while TSBlkSpanDataTypeConvert exists.
class TSBlkDataTypeConvert {
 private:
  std::list<char*> alloc_mems_;

 public:
  uint32_t block_version_ = 0;
  uint32_t scan_version_ = 0;
  std::shared_ptr<SchemaVersionConv> version_conv_;
  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr_;
  uint64_t key_;
  const std::vector<AttributeInfo>* scan_attrs_ = nullptr;

 public:
  TSBlkDataTypeConvert() = default;

  explicit TSBlkDataTypeConvert(uint32_t block_version, uint32_t scan_version,
                                const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr);

  ~TSBlkDataTypeConvert() {
    for (auto mem : alloc_mems_) {
      free(mem);
    }
    alloc_mems_.clear();
  }

  TSBlkDataTypeConvert(const TSBlkDataTypeConvert& src, TsBlockSpan& desc_blk_span);

  KStatus Init();

  std::unique_ptr<TSBlkDataTypeConvert> NewConvertOnlyChangeBlockSpan();

  int ConvertDataTypeToMem(uint32_t scan_col, int32_t new_type_size, void* old_mem, uint16_t old_var_len,
                           std::shared_ptr<void>* new_mem);

  bool IsColExist(uint32_t scan_idx) { return version_conv_->blk_cols_extended_[scan_idx] != UINT32_MAX; }
  bool IsSameType(uint32_t scan_idx) {
    auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
    return isSameType((*version_conv_->blk_attrs_)[blk_col_idx], (*version_conv_->scan_attrs_)[scan_idx]);
  }
  bool IsColNotNull(uint32_t scan_idx) {
    auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
    return (*version_conv_->blk_attrs_)[blk_col_idx].isFlag(AINFO_NOT_NULL);
  }
  bool IsVarLenType(uint32_t scan_idx) { return isVarLenType((*version_conv_->scan_attrs_)[scan_idx].type); }
  int32_t GetColSize(uint32_t scan_idx) { return (*version_conv_->scan_attrs_)[scan_idx].size; }
  int32_t GetColType(uint32_t scan_idx) { return (*version_conv_->scan_attrs_)[scan_idx].type; }

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(TsBlockSpan* blk_span, uint32_t scan_idx, char** value,
                           std::unique_ptr<TsBitmapBase>* bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(TsBlockSpan* blk_span, uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data);
  KStatus GetColBitmap(TsBlockSpan* blk_span, uint32_t scan_idx, std::unique_ptr<TsBitmapBase>* bitmap);
  KStatus getColBitmapConverted(TsBlockSpan* blk_span, uint32_t scan_idx, std::unique_ptr<TsBitmapBase>* bitmap);
  KStatus GetPreCount(TsBlockSpan* blk_span, uint32_t scan_idx, uint16_t& count);
  KStatus GetPreSum(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size, void* &pre_sum, bool& is_overflow);
  KStatus GetPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, void* &pre_max);
  KStatus GetPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size, void* &pre_min);
  KStatus GetVarPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, TSSlice& pre_max);
  KStatus GetVarPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, TSSlice& pre_min);
};


}  // namespace kwdbts
