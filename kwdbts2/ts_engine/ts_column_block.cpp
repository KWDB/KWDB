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

#include "ts_column_block.h"

#include <cstddef>
#include <string>
#include <memory>
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_coding.h"
#include "ts_compressor.h"
namespace kwdbts {
void TsColumnBlockBuilder::AppendFixLenData(TSSlice data, int count, const TsBitmap& bitmap) {
  assert(count == bitmap.GetCount());
  bitmap_ += bitmap;
  assert(!isVarLenType(col_schema_.type));
  fixlen_data_.append(data.data, data.len);
  count_ += count;
}

void TsColumnBlockBuilder::AppendVarLenData(TSSlice data, DataFlags flag) {
  assert(isVarLenType(col_schema_.type));
  PutFixed32(&fixlen_data_, varchar_data_.size());
  if (flag == kValid) {
    varchar_data_.append(data.data, data.len);
  }
  bitmap_.push_back(flag);
  count_ += 1;
}

void TsColumnBlockBuilder::AppendColumnBlock(TsColumnBlock& col) {
  size_t count = col.GetRowNum();
  // TODO(zzr) deal with schama mismatch?
  if (isVarLenType(col_schema_.type)) {
    uint32_t current_offset = varchar_data_.size();
    this->varchar_data_.append(col.varchar_guard_.AsStringView());
    const uint32_t* rhs_offset = reinterpret_cast<const uint32_t*>(col.fixlen_guard_.data());
    for (int i = 0; i < count; ++i) {
      PutFixed32(&fixlen_data_, rhs_offset[i] + current_offset);
    }
  } else {
    this->fixlen_data_.append(col.fixlen_guard_.AsStringView());
  }
  bitmap_ += TsBitmap{col.bitmap_guard_.AsSlice(), col.count_};
  count_ += col.GetRowNum();
}

KStatus TsColumnBlock::GetColBitmap(TsBitmap& bitmap) {
  bitmap = TsBitmap{bitmap_guard_.AsSlice(), count_};
  return SUCCESS;
}

KStatus TsColumnBlock::GetValueSlice(int row_num, TSSlice& value) {
  assert(row_num < count_);
  if (!isVarLenType(col_schema_.type)) {
    size_t offset = col_schema_.size * row_num;
    assert(offset + col_schema_.size <= fixlen_guard_.size());
    value.data = fixlen_guard_.data() + offset;
    value.len = col_schema_.size;
    return SUCCESS;
  }

  uint32_t* varchar_offsets = reinterpret_cast<uint32_t*>(fixlen_guard_.data());
  uint32_t start = varchar_offsets[row_num];
  uint32_t end = row_num + 1 == count_ ? varchar_guard_.size() : varchar_offsets[row_num + 1];
  assert(end >= start && end <= varchar_guard_.size());
  value.data = varchar_guard_.data() + start;
  value.len = end - start;
  return SUCCESS;
}

bool TsColumnBlock::GetCompressedData(std::string* out, TsColumnCompressInfo* info, bool compress) {
  std::string compressed_data;
  info->row_count = count_;

  TsBitmap bitmap = TsBitmap{bitmap_guard_.AsSlice(), count_};

  // 1. compress bitmap;
  // TODO(zzr) bitmap compression algorithms;
  assert(count_ == bitmap.GetCount());
  compressed_data.append(bitmap.GetStr());
  info->bitmap_len = compressed_data.size();

  // 2. compress fixlen data
  std::string tmp;
  TSSlice input = fixlen_guard_.AsSlice();
  tmp.append(input.data, input.len);

  info->fixdata_len = tmp.size();
  compressed_data.append(tmp);

  // 3. compress varchar data
  if (!varchar_guard_.empty()) {
    tmp.clear();
    tmp.append(varchar_guard_.AsSlice().data, varchar_guard_.AsSlice().len);
    info->vardata_len = tmp.size();
    compressed_data.append(tmp);
  } else {
    info->vardata_len = 0;
  }
  out->swap(compressed_data);
  return true;
}

KStatus TsColumnBlock::ParseColumnData(const AttributeInfo& col_schema, TSSlice compressed_data,
                                                 const TsColumnCompressInfo& info,
                                                 std::unique_ptr<TsColumnBlock>* colblock) {
  assert(compressed_data.len == info.bitmap_len + info.fixdata_len + info.vardata_len);
  // 1. Decompress Bitmap
  // std::unique_ptr<TsBitmap> p_bitmap = nullptr;
  TsSliceGuard bitmap_guard;
  if (info.bitmap_len != 0) {
    TSSlice bitmap_data;
    bitmap_data.data = compressed_data.data;
    bitmap_data.len = info.bitmap_len;
    bitmap_guard = TsSliceGuard{bitmap_data};
    RemovePrefix(&compressed_data, bitmap_data.len);
  }

  // 2. Decompress Metric
  TSSlice fixlen_slice;
  fixlen_slice.data = compressed_data.data;
  fixlen_slice.len = info.fixdata_len;
  TsSliceGuard fixlen_guard(fixlen_slice);
  RemovePrefix(&compressed_data, info.fixdata_len);

  // 3. Decompress Varchar
  TsSliceGuard varchar_guard;
  if (info.vardata_len != 0) {
    TSSlice varlen_slice = compressed_data;
    assert(varlen_slice.len == info.vardata_len);
    varchar_guard = TsSliceGuard{varlen_slice};
  }
  colblock->reset(new TsColumnBlock(col_schema, info.row_count, std::move(bitmap_guard),
                                    std::move(fixlen_guard), std::move(varchar_guard)));
  return SUCCESS;
}
}  // namespace kwdbts
