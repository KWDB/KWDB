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
  TsBlock* block_ = nullptr;
  uint32_t start_row_idx_ = 0;
  uint32_t row_num_ = 0;
  std::list<char*> alloc_mems_;

 public:
  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr_;
  uint32_t scan_version_;
  std::vector<uint32_t> ts_scan_cols_;
  std::vector<uint32_t> blk_scan_cols_;
  std::vector<AttributeInfo> table_schema_all_;
  std::vector<AttributeInfo> blk_schema_valid_;
  std::map<uint32_t, TsBitmap> col_bitmaps;

 public:
  TSBlkDataTypeConvert() = default;

  explicit TSBlkDataTypeConvert(TsBlockSpan& blk_span, std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                uint32_t scan_version, const std::vector<uint32_t>& ts_scan_cols);

  TSBlkDataTypeConvert(TsBlock* block, uint32_t row_idx, uint32_t row_num,
                       std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                       uint32_t scan_version, const std::vector<uint32_t>& ts_scan_cols) :
      block_(block), start_row_idx_(row_idx), tbl_schema_mgr_(tbl_schema_mgr),
      row_num_(row_num), scan_version_(scan_version), ts_scan_cols_(ts_scan_cols) {}

  ~TSBlkDataTypeConvert() {
    for (auto mem : alloc_mems_) {
      free(mem);
    }
    alloc_mems_.clear();
  }

  KStatus SetConvertVersion(std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version,
                            std::vector<uint32_t> ts_scan_cols);

  KStatus GetBlkScanColsInfo(uint32_t version, std::vector<uint32_t>& scan_cols,
                              vector<AttributeInfo>& blk_schema);

  bool IsColExist(uint32_t scan_idx);
  bool IsSameType(uint32_t scan_idx);
  bool IsColNotNull(uint32_t scan_idx);

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data);
  KStatus GetColBitmap(uint32_t scan_idx, TsBitmap& bitmap);
  KStatus GetPreCount(uint32_t scan_idx, uint16_t& count);
  KStatus GetPreSum(uint32_t scan_idx, int32_t size, void* &pre_sum, bool& is_overflow);
  KStatus GetPreMax(uint32_t scan_idx, void* &pre_max);
  KStatus GetPreMin(uint32_t scan_idx, int32_t size, void* &pre_min);
  KStatus GetVarPreMax(uint32_t scan_idx, TSSlice& pre_max);
  KStatus GetVarPreMin(uint32_t scan_idx, TSSlice& pre_min);
};


}  // namespace kwdbts
