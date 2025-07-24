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

#include "data_type.h"
#include "kwdb_type.h"
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
    this->varchar_data_.append(col.varchar_data_);
    uint32_t* rhs_offset = reinterpret_cast<uint32_t*>(col.fixlen_data_.data());
    for (int i = 0; i < count; ++i) {
      PutFixed32(&fixlen_data_, rhs_offset[i] + current_offset);
    }
  } else {
    this->fixlen_data_.append(col.fixlen_data_);
  }
  bitmap_ += col.bitmap_;
  count_ += col.GetRowNum();
}

KStatus TsColumnBlock::GetColBitmap(TsBitmap& bitmap) {
  bitmap = bitmap_;
  return SUCCESS;
}

KStatus TsColumnBlock::GetValueSlice(int row_num, TSSlice& value) {
  assert(row_num < count_);
  if (!isVarLenType(col_schema_.type)) {
    size_t offset = col_schema_.size * row_num;
    assert(offset + col_schema_.size < fixlen_data_.size());
    value.data = fixlen_data_.data() + offset;
    value.len = col_schema_.size;
    return SUCCESS;
  }

  uint32_t* varchar_offsets = reinterpret_cast<uint32_t*>(fixlen_data_.data());
  uint32_t start = varchar_offsets[row_num];
  uint32_t end = row_num + 1 == count_ ? varchar_data_.size() : varchar_offsets[row_num + 1];
  assert(end >= start && end <= varchar_data_.size());
  value.data = varchar_data_.data() + start;
  value.len = end - start;
  return SUCCESS;
}

bool TsColumnBlock::GetCompressedData(std::string* out, TsColumnCompressInfo* info, bool compress) {
  std::string compressed_data;
  info->row_count = count_;

  // 1. compress bitmap;
  // TODO(zzr) bitmap compression algorithms;
  assert(count_ == bitmap_.GetCount());
  compressed_data.push_back(static_cast<char>(BitmapCompAlg::kPlain));
  compressed_data.append(bitmap_.GetStr());
  info->bitmap_len = compressed_data.size();

  // 2. compress fixlen data
  const auto& mgr = CompressorManager::GetInstance();
  std::string tmp;
  TsBitmap* bitmap = &bitmap_;
  auto [first, second] = mgr.GetDefaultAlgorithm(static_cast<DATATYPE>(col_schema_.type));
  if (isVarLenType(col_schema_.type)) {
    // varchar use Gorilla algorithm
    first = compress ? TsCompAlg::kChimp_32 : TsCompAlg::kPlain;
    bitmap = nullptr;
  }

  TSSlice input{fixlen_data_.data(), fixlen_data_.size()};
  std::vector<timestamp64> tmp_ts;
  // TODO(zzr) remove it when payload has 8 bytes timestamp
  if (need_convert_ts(col_schema_.type)) {
    tmp_ts.resize(count_);
    for (int i = 0; i < count_; ++i) {
      tmp_ts[i] = *reinterpret_cast<timestamp64*>(fixlen_data_.data() + i * 16);
    }
    input.data = reinterpret_cast<char*>(tmp_ts.data());
    input.len = tmp_ts.size() * sizeof(timestamp64);
  }

  if (!compress) {
    first = TsCompAlg::kPlain;
    second = GenCompAlg::kPlain;
  }

  bool ok = mgr.CompressData(input, bitmap, count_, &tmp, first, second);
  if (!ok) {
    return false;
  }
  info->fixdata_len = tmp.size();
  compressed_data.append(tmp);

  // 3. compress varchar data
  tmp.clear();
  ok = mgr.CompressVarchar({varchar_data_.data(), varchar_data_.size()}, &tmp, GenCompAlg::kSnappy);
  if (!ok) {
    return false;
  }
  info->vardata_len = tmp.size();
  compressed_data.append(tmp);
  out->swap(compressed_data);
  return true;
}

KStatus TsColumnBlock::ParseCompressedColumnData(const AttributeInfo& col_schema,
                                                 TSSlice compressed_data,
                                                 const TsColumnCompressInfo& info,
                                                 std::unique_ptr<TsColumnBlock>* colblock) {
  // 1. Decompress Bitmap
  assert(compressed_data.len == info.bitmap_len + info.fixdata_len + info.vardata_len);
  BitmapCompAlg bitmap_alg = static_cast<BitmapCompAlg>(compressed_data.data[0]);
  assert(bitmap_alg == BitmapCompAlg::kPlain);
  RemovePrefix(&compressed_data, 1);

  TSSlice bitmap_data;
  bitmap_data.data = compressed_data.data;
  bitmap_data.len = info.bitmap_len - 1;
  TsBitmap bitmap(bitmap_data, info.row_count);
  RemovePrefix(&compressed_data, bitmap_data.len);

  // 2. Decompress Metric
  TSSlice fixlen_slice;
  fixlen_slice.data = compressed_data.data;
  fixlen_slice.len = info.fixdata_len;
  const auto& mgr = CompressorManager::GetInstance();
  std::string fixlen_data;
  TsBitmap* pbitmap = isVarLenType(col_schema.type) ? nullptr : &bitmap;
  bool ok = mgr.DecompressData(fixlen_slice, pbitmap, info.row_count, &fixlen_data);
  if (!ok) {
    return KStatus::FAIL;
  }
  RemovePrefix(&compressed_data, info.fixdata_len);

  // 3. Decompress Varchar
  TSSlice varlen_slice = compressed_data;
  assert(varlen_slice.len == info.vardata_len);
  std::string varlen_data;
  ok = mgr.DecompressVarchar(varlen_slice, &varlen_data);
  if (!ok) {
    return KStatus::FAIL;
  }
  colblock->reset(new TsColumnBlock(col_schema, info.row_count, bitmap, fixlen_data, varlen_data));
  return SUCCESS;
}
}  // namespace kwdbts
