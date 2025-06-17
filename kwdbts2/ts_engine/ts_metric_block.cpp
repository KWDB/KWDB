#include "ts_metric_block.h"

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_compressor.h"

namespace kwdbts {

bool TsMetricBlock::GetCompressedData(std::string* output, TsMetricCompressInfo* compress_info) {
  std::string compressed_data;
  const auto& mgr = CompressorManager::GetInstance();
  // 1. Compress LSN
  TSSlice lsn_slice{lsn_buffer_.data(), lsn_buffer_.size()};
  auto ok = mgr.CompressData(lsn_slice, nullptr, count_, &compressed_data, TsCompAlg::kGorilla_64,
                             GenCompAlg::kPlain);
  if (!ok) {
    LOG_ERROR("compress lsn error");
    return FAIL;
  }
  compress_info->lsn_len = compressed_data.size();

  // 2. Compress column data
  compress_info->column_compress_infos.resize(column_blocks_.size());
  std::string tmp;
  TsColumnCompressInfo col_compress_info;
  for (int i = 0; i < column_blocks_.size(); i++) {
    tmp.clear();
    ok = column_blocks_[i]->GetCompressedData(&tmp, &col_compress_info);
    if (!ok) {
      LOG_ERROR("compress column data error");
      return FAIL;
    }
    compressed_data.append(tmp);
    compress_info->column_compress_infos[i] = col_compress_info;
  }
  output->swap(compressed_data);
  return SUCCESS;
}

KStatus TsMetricBlockBuilder::PutBlockSpan(std::shared_ptr<TsBlockSpan> span) {
  auto row_count = span->GetRowNum();
  auto lsn_addr = span->GetLSNAddr(0);
  lsn_buffer_.append(reinterpret_cast<char*>(lsn_addr), sizeof(uint64_t) * row_count);
  for (int icol = 0; icol < col_schemas_.size(); icol++) {
    if (isVarLenType(col_schemas_[icol].type)) {
      // looping row by row to copy data
      for (int irow = 0; irow < row_count; irow++) {
        DataFlags flag;
        TSSlice data;
        auto s =
            span->GetVarLenTypeColAddr(irow, icol, col_schemas_, col_schemas_[icol], flag, data);
        if (s == FAIL) {
          return s;
        }
        column_block_builders_[icol]->AppendVarLenData(data, flag);
      }
    } else {
      char* data = nullptr;
      TsBitmap bitmap;
      auto s = span->GetFixLenColAddr(icol, col_schemas_, col_schemas_[icol], &data, bitmap);
      if (s == FAIL) {
        return s;
      }
      TSSlice s_data;
      s_data.data = data;
      s_data.len = col_schemas_[icol].size * row_count;
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
  return std::unique_ptr<TsMetricBlock>(
      new TsMetricBlock{count_, std::move(lsn_buffer_), std::move(column_blocks)});
}
}  // namespace kwdbts