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
#include "ts_compressor.h"

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
  TsSliceGuard data_;
  std::unique_ptr<TsBitmapBase> bitmap_;
  TsSliceGuard fixlen_guard_, varchar_guard_;

  TsColumnBlock(const AttributeInfo& col_schema, int count, TsSliceGuard&& data, std::unique_ptr<TsBitmapBase>&& bitmap,
                TsSliceGuard&& fixlen_data, TsSliceGuard&& varchar_data)
      : col_schema_(col_schema),
        count_(count),
        data_(std::move(data)),
        bitmap_(std::move(bitmap)),
        fixlen_guard_(std::move(fixlen_data)),
        varchar_guard_(std::move(varchar_data)) {}

  TsColumnBlock(const AttributeInfo& col_schema, int count, std::unique_ptr<TsBitmapBase>&& bitmap,
                TsSliceGuard&& fixlen_data, TsSliceGuard&& varchar_data)
      : col_schema_(col_schema),
        count_(count),
        bitmap_(std::move(bitmap)),
        fixlen_guard_(std::move(fixlen_data)),
        varchar_guard_(std::move(varchar_data)) {}

 public:
  static KStatus ParseColumnData(const AttributeInfo& col_schema, TsSliceGuard& compressed_guard,
                                 const TsColumnCompressInfo& info, std::unique_ptr<TsColumnBlock>* colblock);

  bool GetCompressedData(std::string*, TsColumnCompressInfo*, bool compress);

  size_t GetRowNum() const { return count_; }
  const AttributeInfo& GetColSchama() const { return col_schema_; }
  char* GetColAddr() { return fixlen_guard_.data(); }
  KStatus GetColBitmap(std::unique_ptr<TsBitmapBase>* bitmap) const;
  KStatus GetValueSlice(int row_num, TSSlice& value);
};

class TsColumnBlockBuilder {
 private:
  const AttributeInfo& col_schema_;
  int count_ = 0;
  std::unique_ptr<TsBitmap> bitmap_;
  std::string fixlen_data_;
  std::string varchar_data_;

 public:
  explicit TsColumnBlockBuilder(const AttributeInfo& col_schema)
      : col_schema_(col_schema), bitmap_(std::make_unique<TsBitmap>()) {}
  void AppendFixLenData(TSSlice data, int count, const TsBitmapBase* bitmap);
  void AppendVarLenData(TSSlice data, DataFlags flag);

  // TODO(zzr) make it const ref
  void AppendColumnBlock(TsColumnBlock& col);

  std::unique_ptr<TsColumnBlock> GetColumnBlock() {
    std::string fixlen_data, varchar_data;
    fixlen_data.swap(fixlen_data_);
    varchar_data.swap(varchar_data_);
    TsSliceGuard fixlen_guard{std::move(fixlen_data)};
    TsSliceGuard varchar_guard{std::move(varchar_data)};
    return std::unique_ptr<TsColumnBlock>{new TsColumnBlock(col_schema_, count_, std::move(bitmap_),
                                                            std::move(fixlen_guard), std::move(varchar_guard))};
  }

  void Reset() {
    count_ = 0;
    bitmap_ = std::make_unique<TsBitmap>();
    fixlen_data_.clear();
    varchar_data_.clear();
  }
};

}  // namespace kwdbts
