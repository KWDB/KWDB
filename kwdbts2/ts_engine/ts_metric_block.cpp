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

bool TsMetricBlock::GetCompressedData(std::string* output, TsMetricCompressInfo* compress_info,
                                      bool compress_ts_and_lsn, bool compress_columns) {
  std::string compressed_data;
  const auto& mgr = CompressorManager::GetInstance();
  // 1. Compress LSN
  TSSlice lsn_slice{reinterpret_cast<char*>(lsn_buffer_.data()), lsn_buffer_.size() * sizeof(TS_LSN)};
  compressed_data.append(lsn_slice.data, lsn_slice.len);
  compress_info->lsn_len = compressed_data.size();
  size_t offset = compress_info->lsn_len;

  // 2. Compress column data
  compress_info->column_compress_infos.resize(column_blocks_.size());
  compress_info->column_data_segments.resize(column_blocks_.size());
  std::string tmp;
  TsColumnCompressInfo col_compress_info;
  for (int i = 0; i < column_blocks_.size(); i++) {
    tmp.clear();
    bool ok = column_blocks_[i]->GetCompressedData(&tmp, &col_compress_info, compress_columns);
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
  output->swap(compressed_data);
  return SUCCESS;
}

KStatus TsMetricBlockBuilder::PutBlockSpan(std::shared_ptr<TsBlockSpan> span) {
  auto row_count = span->GetRowNum();

  lsn_buffer_.reserve(lsn_buffer_.size() + span->GetRowNum());
  for (int i = 0; i < span->GetRowNum(); ++i) {
    auto lsn_addr = span->GetLSNAddr(i);
    lsn_buffer_.push_back(*lsn_addr);
  }
  for (int icol = 0; icol < col_schemas_->size(); icol++) {
    if (isVarLenType((*col_schemas_)[icol].type)) {
      // looping row by row to copy data
      for (int irow = 0; irow < row_count; irow++) {
        DataFlags flag;
        TSSlice data;
        auto s =
            span->GetVarLenTypeColAddr(irow, icol, flag, data);
        if (s == FAIL) {
          return s;
        }
        column_block_builders_[icol]->AppendVarLenData(data, flag);
      }
    } else {
      char* data = nullptr;
      TsBitmap bitmap;
      auto s = span->GetFixLenColAddr(icol, &data, bitmap);
      if (s == FAIL) {
        return s;
      }
      TSSlice s_data;
      s_data.data = data;
      s_data.len = (*col_schemas_)[icol].size * row_count;
      column_block_builders_[icol]->AppendFixLenData(s_data, row_count, bitmap);
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

  std::vector<TS_LSN> lsn_buffer;
  lsn_buffer.swap(lsn_buffer_);
  return std::unique_ptr<TsMetricBlock>(new TsMetricBlock{count_, std::move(lsn_buffer), std::move(column_blocks)});
}

KStatus TsMetricBlock::ParseCompressedMetricData(const std::vector<AttributeInfo>& schema,
                                                 TSSlice compressed_data,
                                                 const TsMetricCompressInfo& compress_info,
                                                 std::unique_ptr<TsMetricBlock>* metric_block) {
  // 0. Check schema
  if (schema.size() != compress_info.column_compress_infos.size()) {
    LOG_ERROR("schema size not match compress info");
    return FAIL;
  }

  const auto& mgr = CompressorManager::GetInstance();
  // 1. Decompress LSN
  TSSlice lsn_slice;
  lsn_slice.data = compressed_data.data;
  lsn_slice.len = compress_info.lsn_len;
  TsSliceGuard out_lsn_guard(lsn_slice);

  if (out_lsn_guard.size() != compress_info.row_count * sizeof(TS_LSN)) {
    LOG_ERROR("decompress lsn size not match");
    return FAIL;
  }
  std::vector<TS_LSN> lsn_vec(compress_info.row_count);
  std::memcpy(lsn_vec.data(), out_lsn_guard.data(), out_lsn_guard.size());

  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks;
  for (int i = 0; i < schema.size(); i++) {
    TSSlice data_slice;
    data_slice.data = compressed_data.data + compress_info.column_data_segments[i].offset;
    data_slice.len = compress_info.column_data_segments[i].length;
    std::unique_ptr<TsColumnBlock> colblock;
    auto s = TsColumnBlock::ParseColumnData(
        schema[i], data_slice, compress_info.column_compress_infos[i], &colblock);
    if (s == FAIL) {
      LOG_ERROR("parse column data error");
      return s;
    }
    column_blocks.push_back(std::move(colblock));
  }
  metric_block->reset(new TsMetricBlock{compress_info.row_count, std::move(lsn_vec), std::move(column_blocks)});
  return SUCCESS;
}

}  // namespace kwdbts
