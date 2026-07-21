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

#include "compression/ts_compressor_manager.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_column_block.h"

namespace kwdbts {

bool TsMetricBlock::GetCompressedData(TsBufferBuilder& output, TsMetricCompressInfo& compress_info,
                                      bool compress_ts_and_osn, bool compress_columns) {
  const auto& mgr = CompressorManager::GetInstance();
  TsCompressionConfig cfg;
  // 1. Compress OSN
  TSSlice osn_slice{reinterpret_cast<char*>(osn_buffer_.data()), osn_buffer_.size() * sizeof(uint64_t)};
  cfg.encoder = compress_ts_and_osn ? EncodeAlgo::kSimple8B_u64 : EncodeAlgo::kPlain;
  size_t origin_size = output.size();
  auto ok = mgr.CompressData(osn_slice, nullptr, count_, &output, cfg);
  if (!ok) {
    LOG_ERROR("compress osn error");
    return FAIL;
  }
  compress_info.osn_len = output.size() - origin_size;
  size_t offset = compress_info.osn_len;

  // 2. Compress column data
  compress_info.column_compress_infos.resize(column_blocks_.size());
  compress_info.column_data_segments.resize(column_blocks_.size());
  TsColumnCompressInfo col_compress_info;
  for (int i = 0; i < column_blocks_.size(); i++) {
    origin_size = output.size();
    ok = column_blocks_[i]->GetCompressedData(&output, &col_compress_info, compress_columns);
    if (!ok) {
      LOG_ERROR("compress column data error");
      return FAIL;
    }
    compress_info.column_compress_infos[i] = col_compress_info;
    compress_info.column_data_segments[i] = {offset, output.size() - origin_size};
    offset += output.size() - origin_size;
  }
  compress_info.row_count = count_;
  return SUCCESS;
}

KStatus TsMetricBlockBuilder::PutBlockSpan(const std::shared_ptr<TsBlockSpan>& span) {
  auto row_count = span->GetRowNum();
  std::copy_n(span->GetOSNAddr(0), row_count, std::back_inserter(osn_buffer_));
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
      DirectColumnDataCopy direct_copy;
      direct_copy.dest_buffer_builder = column_block_builders_[icol]->GetFixLenBufferBuilder();
      direct_copy.copy_rows = row_count;
      auto s = span->GetFixLenColAddr(icol, &data, &bitmap, nullptr, &direct_copy);
      if (s == FAIL) {
        return s;
      }
      TSSlice s_data;
      s_data.data = const_cast<char*>(data);
      s_data.len = (*col_schemas_)[icol].size * row_count;
      if (direct_copy.copied_to_dest) {
        // The data is already copied into column_block_builders_[icol] buffer, so only need to append bitmap
        column_block_builders_[icol]->AppendFixLenBitmap(row_count, bitmap.get());
      } else {
        column_block_builders_[icol]->AppendFixLenData(s_data, row_count, bitmap.get());
      }
    }
  }
  count_ += row_count;
  return SUCCESS;
}

std::unique_ptr<TsMetricBlock> TsMetricBlockBuilder::GetMetricBlock() {
  std::vector<std::unique_ptr<TsColumnBlock>> column_blocks;
  column_blocks.reserve(column_block_builders_.size());
  for (const auto & column_block_builder : column_block_builders_) {
    column_blocks.push_back(column_block_builder->GetColumnBlock());
  }

  std::vector<uint64_t> osn_buffer;
  osn_buffer.swap(osn_buffer_);
  return std::unique_ptr<TsMetricBlock>(new TsMetricBlock{count_, std::move(osn_buffer), std::move(column_blocks),
                                                         table_id_});
}

}  // namespace kwdbts
