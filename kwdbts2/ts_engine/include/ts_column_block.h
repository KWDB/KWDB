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
#include <memory>
#include <string>
#include <utility>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"

namespace kwdbts {

struct TsColumnCompressInfo {
  //   bool has_bitmap;   // optimize later
  int bitmap_len;
  int fixdata_len;
  int vardata_len;
  int row_count;
};

class TsColumnBlock {
  friend class TsColumnBlockBuilder;

 private:
  const AttributeInfo col_schema_;
  int count_ = 0;
  TsBitmap bitmap_;
  std::string fixlen_data_;
  std::string varchar_data_;

  TsColumnBlock(const AttributeInfo& col_schema, int count, const TsBitmap& bitmap, const std::string& fixlen_data,
                const std::string& varchar_data)
      : col_schema_(col_schema),
        count_(count),
        bitmap_(bitmap),
        fixlen_data_(std::move(fixlen_data)),
        varchar_data_(std::move(varchar_data)) {}

 public:
  static KStatus ParseCompressedColumnData(const AttributeInfo& col_schema, TSSlice compressed_data,
                                           const TsColumnCompressInfo& info, std::unique_ptr<TsColumnBlock>* colblock);

  bool GetCompressedData(std::string*, TsColumnCompressInfo*, bool compress);

  size_t GetRowNum() const { return count_; }
  const AttributeInfo& GetColSchama() const { return col_schema_; }
  char* GetColAddr() { return fixlen_data_.data(); }
  KStatus GetColBitmap(TsBitmap& bitmap);
  KStatus GetValueSlice(int row_num, TSSlice& value);
};

class TsColumnBlockBuilder {
 private:
  const AttributeInfo& col_schema_;
  int count_ = 0;
  TsBitmap bitmap_;
  std::string fixlen_data_;
  std::string varchar_data_;

 public:
  explicit TsColumnBlockBuilder(const AttributeInfo& col_schema) : col_schema_(col_schema) {}
  void AppendFixLenData(TSSlice data, int count, const TsBitmap& bmap);
  void AppendVarLenData(TSSlice data, DataFlags flag);

  // TODO(zzr) make it const ref
  void AppendColumnBlock(TsColumnBlock& col);

  std::unique_ptr<TsColumnBlock> GetColumnBlock() const {
    return std::unique_ptr<TsColumnBlock>{new TsColumnBlock(col_schema_, count_, bitmap_, fixlen_data_, varchar_data_)};
  }

  void Reset() {
    count_ = 0;
    bitmap_ = TsBitmap();
    fixlen_data_.clear();
    varchar_data_.clear();
  }
};

// TODO(zzr): remove this function later
inline bool need_convert_ts(int dtype) {
  return (dtype == TIMESTAMP64_LSN_MICRO || dtype == TIMESTAMP64_LSN || dtype == TIMESTAMP64_LSN_NANO);
}

}  // namespace kwdbts
