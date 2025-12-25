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
#include <cstdint>
#include <memory>
#include <string>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compressor.h"
namespace kwdbts {
void TsColumnBlockBuilder::AppendFixLenData(TSSlice data, int count, const TsBitmapBase* bitmap) {
  assert(bitmap != nullptr);
  assert(count == bitmap->GetCount());
  bitmap_->Append(bitmap);
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
  bitmap_->push_back(flag);
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

  bitmap_->Append(col.bitmap_.get());
  count_ += col.GetRowNum();
}

KStatus TsColumnBlock::GetColBitmap(std::unique_ptr<TsBitmapBase>* bitmap) const {
  *bitmap = bitmap_->AsView();
  return SUCCESS;
}

KStatus TsColumnBlock::GetValueSlice(int row_num, TSSlice& value) {
  assert(row_num < count_);
  if (!isVarLenType(col_schema_.type)) {
    size_t offset = col_schema_.size * row_num;
    assert(offset + col_schema_.size <= fixlen_guard_.size());
    value = fixlen_guard_.SubSlice(offset, col_schema_.size);
    return SUCCESS;
  }

  const uint32_t* varchar_offsets = reinterpret_cast<const uint32_t*>(fixlen_guard_.data());
  uint32_t start = varchar_offsets[row_num];
  uint32_t end = row_num + 1 == count_ ? varchar_guard_.size() : varchar_offsets[row_num + 1];
  assert(end >= start && end <= varchar_guard_.size());
  value = varchar_guard_.SubSlice(start, end - start);
  return SUCCESS;
}

bool TsColumnBlock::GetCompressedData(TsBufferBuilder* out, TsColumnCompressInfo* info, bool compress) {
  const auto& mgr = CompressorManager::GetInstance();
  TsBufferBuilder compressed_data;
  info->row_count = count_;

  // 1. compress bitmap;
  // TODO(zzr) bitmap compression algorithms;
  assert(count_ == bitmap_->GetCount());
  mgr.CompressBitmap(bitmap_.get(), &compressed_data);
  info->bitmap_len = compressed_data.size();

  // 2. compress fixlen data
  TsBufferBuilder tmp;
  TsBitmapBase* p_bitmap = bitmap_.get();
  auto [first, second] = mgr.GetDefaultAlgorithm(static_cast<DATATYPE>(col_schema_.type));
  if (isVarLenType(col_schema_.type)) {
    // varchar use simple8b algorithm
    first = compress ? TsCompAlg::kSimple8B_V2_u32 : TsCompAlg::kPlain;

    /* do not use bitmap to compress offset for varchar, otherwise, the query on varchar column will be wrong
       for example:

       valid | offset | varchar
       1     | 0      | "abc"
       0     | 3      | ""
       1     | 3      | "ghi"
       0     | 6      | ""

       if we compress the offset with bitmap, offset column will be recorded as [0, 3], and after decompress,
       the offset will be [0, 0, 3, 0], which is wrong during query.
     */
    p_bitmap = nullptr;
  }

  TSSlice input = fixlen_guard_.AsSlice();
  if (!compress) {
    first = TsCompAlg::kPlain;
    second = GenCompAlg::kPlain;
  }

  bool ok = mgr.CompressData(input, p_bitmap, count_, &tmp, first, second);
  if (!ok) {
    return false;
  }
  info->fixdata_len = tmp.size();
  compressed_data.append(tmp);

  // 3. compress varchar data
  if (!varchar_guard_.empty()) {
    tmp.clear();
    auto comp_alg = compress ? GenCompAlg::kSnappy : GenCompAlg::kPlain;
    ok = mgr.CompressVarchar(varchar_guard_.AsSlice(), &tmp, comp_alg);
    if (!ok) {
      return false;
    }
    info->vardata_len = tmp.size();
    compressed_data.append(tmp);
  } else {
    info->vardata_len = 0;
  }
  *out = std::move(compressed_data);
  return true;
}

KStatus TsColumnBlock::ParseColumnData(const AttributeInfo& col_schema, TsSliceGuard&& compressed_data,
                                       const TsColumnCompressInfo& info, std::unique_ptr<TsColumnBlock>* colblock) {
  const auto& mgr = CompressorManager::GetInstance();
  assert(compressed_data.size() == info.bitmap_len + info.fixdata_len + info.vardata_len);
  // 1. Decompress Bitmap
  std::unique_ptr<TsBitmapBase> bitmap;
  if (info.bitmap_len != 0) {
    TSSlice bitmap_data = compressed_data.SubSlice(0, info.bitmap_len);
    uint64_t bytes_consumed = 0;
    bool ok = mgr.DecompressBitmap(bitmap_data, &bitmap, info.row_count, &bytes_consumed);
    if (!ok) {
      return KStatus::FAIL;
    }
    assert(bytes_consumed == info.bitmap_len);
    compressed_data.RemovePrefix(info.bitmap_len);
  }

  TsSliceGuard fixlen_slice = compressed_data.SubSliceGuard(0, info.fixdata_len);
  TsBitmapBase* p_bitmap = isVarLenType(col_schema.type) ? nullptr : bitmap.get();
  TsSliceGuard fixlen_guard;
  bool ok = mgr.DecompressData(std::move(fixlen_slice), p_bitmap, info.row_count, &fixlen_guard);
  if (!ok) {
    return KStatus::FAIL;
  }
  compressed_data.RemovePrefix(info.fixdata_len);

  // 3. Decompress Varchar
  TsSliceGuard varchar_guard;
  if (info.vardata_len != 0) {
    assert(compressed_data.size() == info.vardata_len);
    ok = mgr.DecompressVarchar(std::move(compressed_data), &varchar_guard);
    if (!ok) {
      return KStatus::FAIL;
    }
  }
  colblock->reset(new TsColumnBlock(col_schema, info.row_count, std::move(bitmap), std::move(fixlen_guard),
                                    std::move(varchar_guard)));
  return SUCCESS;
}
}  // namespace kwdbts
