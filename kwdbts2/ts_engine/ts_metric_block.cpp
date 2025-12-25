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

#include "ts_metric_block.h"

#include <cstddef>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_column_block.h"
#include "ts_compressor.h"

namespace kwdbts {

bool TsMetricBlock::GetCompressedData(TsBufferBuilder* output, TsMetricCompressInfo* compress_info,
                                      bool compress_ts_and_osn, bool compress_columns) {
  TsBufferBuilder compressed_data;
  const auto& mgr = CompressorManager::GetInstance();
  // 1. Compress OSN
  TSSlice osn_slice{reinterpret_cast<char*>(osn_buffer_.data()), osn_buffer_.size() * sizeof(uint64_t)};
  TsCompAlg osn_alg = compress_ts_and_osn ? TsCompAlg::kSimple8B_u64 : TsCompAlg::kPlain;
  auto ok = mgr.CompressData(osn_slice, nullptr, count_, &compressed_data, osn_alg, GenCompAlg::kPlain);
  if (!ok) {
    LOG_ERROR("compress osn error");
    return FAIL;
  }
  compress_info->osn_len = compressed_data.size();
  size_t offset = compress_info->osn_len;

  // 2. Compress column data
  compress_info->column_compress_infos.resize(column_blocks_.size());
  compress_info->column_data_segments.resize(column_blocks_.size());
  TsBufferBuilder tmp;
  TsColumnCompressInfo col_compress_info;
  for (int i = 0; i < column_blocks_.size(); i++) {
    tmp.clear();
    ok = column_blocks_[i]->GetCompressedData(&tmp, &col_compress_info, compress_columns);
    if (!ok) {
      LOG_ERROR("compress column data error");
      return FAIL;
    }
    compressed_data.append(tmp);
    compress_info->column_compress_infos[i] = col_compress_info;
    compress_info->column_data_segments[i] = {offset, tmp.size()};
    offset += tmp.size();
  }
  compress_info->row_count = count_;
  *output = std::move(compressed_data);
  return SUCCESS;
}

KStatus TsMetricBlockBuilder::PutBlockSpan(std::shared_ptr<TsBlockSpan> span) {
  auto row_count = span->GetRowNum();

  osn_buffer_.reserve(osn_buffer_.size() + span->GetRowNum());
  for (int i = 0; i < span->GetRowNum(); ++i) {
    auto osn_addr = span->GetOSNAddr(i);
    osn_buffer_.push_back(*osn_addr);
  }
  for (int icol = 0; icol < col_schemas_->size(); icol++) {
    if (isVarLenType((*col_schemas_)[icol].type)) {
      // looping row by row to copy data
      for (int irow = 0; irow < row_count; irow++) {
        DataFlags flag;
        TSSlice data;
        auto s = span->GetVarLenTypeColAddr(irow, icol, flag, data);
        if (s == FAIL) {
          return s;
        }
        column_block_builders_[icol]->AppendVarLenData(data, flag);
      }
    } else {
      char* data = nullptr;
      std::unique_ptr<TsBitmapBase> bitmap;
      auto s = span->GetFixLenColAddr(icol, &data, &bitmap);
      if (s == FAIL) {
        return s;
      }
      TSSlice s_data;
      s_data.data = const_cast<char*>(data);
      s_data.len = (*col_schemas_)[icol].size * row_count;
      column_block_builders_[icol]->AppendFixLenData(s_data, row_count, bitmap.get());
    }
  }
  count_ += row_count;
  return SUCCESS;
}

std::unique_ptr<TsMetricBlock> TsMetricBlockBuilder::GetMetricBlock() {
  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks;
  for (int i = 0; i < column_block_builders_.size(); ++i) {
    column_blocks.push_back(column_block_builders_[i]->GetColumnBlock());
  }

  std::vector<uint64_t> osn_buffer;
  osn_buffer.swap(osn_buffer_);
  return std::unique_ptr<TsMetricBlock>(new TsMetricBlock{count_, std::move(osn_buffer), std::move(column_blocks)});
}

KStatus TsMetricBlock::ParseCompressedMetricData(const std::vector<AttributeInfo>& schema,
                                                 TsSliceGuard&& compressed_data,
                                                 const TsMetricCompressInfo& compress_info,
                                                 std::unique_ptr<TsMetricBlock>* metric_block) {
  // 0. Check schema
  if (schema.size() != compress_info.column_compress_infos.size()) {
    LOG_ERROR("schema size not match compress info");
    return FAIL;
  }

  const auto& mgr = CompressorManager::GetInstance();
  // 1. Decompress OSN
  TsSliceGuard osn_slice = compressed_data.SubSliceGuard(0, compress_info.osn_len);
  TsSliceGuard out_osn_guard;
  bool ok = mgr.DecompressData(std::move(osn_slice), nullptr, compress_info.row_count, &out_osn_guard);
  if (!ok) {
    LOG_ERROR("decompress osn error");
    return FAIL;
  }

  if (out_osn_guard.size() != compress_info.row_count * sizeof(uint64_t)) {
    LOG_ERROR("decompress osn size not match");
    return FAIL;
  }
  std::vector<uint64_t> osn_vec(compress_info.row_count);
  std::memcpy(osn_vec.data(), out_osn_guard.data(), out_osn_guard.size());

  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks;
  for (int i = 0; i < schema.size(); i++) {
    TsSliceGuard data_slice = compressed_data.SubSliceGuard(compress_info.column_data_segments[i].offset,
                                                            compress_info.column_data_segments[i].length);
    std::unique_ptr<TsColumnBlock> colblock;
    auto s = TsColumnBlock::ParseColumnData(
        schema[i], std::move(data_slice), compress_info.column_compress_infos[i], &colblock);
    if (s == FAIL) {
      LOG_ERROR("parse column data error");
      return s;
    }
    column_blocks.push_back(std::move(colblock));
  }
  metric_block->reset(new TsMetricBlock{compress_info.row_count, std::move(osn_vec), std::move(column_blocks)});
  return SUCCESS;
}

}  // namespace kwdbts
