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
#include "ts_env.h"
#include "ts_common.h"

namespace kwdbts {

TSSlice TsMetricBlockBuilder::Build(const std::vector<TsBlockColData>& col_datas) {
  assert(schema_.size() == col_datas.size());
  std::vector<TSSlice> compressed_cols;
  bool all_ok = true;
  size_t col_data_len = 0;
  auto compressor_mgr = TsEnvInstance::GetInstance().Compressor();
  if (compressor_mgr == nullptr) {
    LOG_ERROR("cannot initialize compressor manager.");
    TSSlice{nullptr, 0};
  }
  for (size_t i = 0; i < schema_.size(); i++) {
    TSSlice col_compressed = compressor_mgr->Encode(col_datas[i].data, col_datas[i].bitmap, row_num_,
                                                    (DATATYPE)(schema_[i].type));
    if (col_compressed.len == 0) {
      LOG_ERROR("Compressor Encode failed. %lu", i);
      all_ok = false;
      break;
    }
    compressed_cols.push_back(col_compressed);
    col_data_len += col_compressed.len;
  }
  if (!all_ok) {
    for (auto& col : compressed_cols) {
      free(col.data);
    }
    LOG_ERROR("Compressor Encode failed.");
    return TSSlice{nullptr, 0};
  }
  size_t col_len_len = sizeof(uint32_t) * schema_.size();
  char* mem = reinterpret_cast<char*>(malloc(col_data_len  + col_len_len));
  if (mem == nullptr) {
    LOG_ERROR("malloc memory failed.[%lu].", (col_data_len  + col_len_len));
    return TSSlice{nullptr, 0};
  }
  char* mem_offset = mem;
  for (size_t i = 0; i < compressed_cols.size(); i++) {
    KUint32(mem_offset) = compressed_cols[i].len;
    mem_offset += sizeof(uint32_t);
  }
  for (size_t i = 0; i < compressed_cols.size(); i++) {
    memcpy(mem_offset, compressed_cols[i].data, compressed_cols[i].len);
    mem_offset += compressed_cols[i].len;
    free(compressed_cols[i].data);
  }
  assert(mem_offset == (mem + col_data_len  + col_len_len));
  return TSSlice{mem, col_data_len  + col_len_len};
}

bool TsMetricBlockParser::Parse(TSSlice block_addr) {
  assert(block_addr.len > sizeof(uint32_t) * schema_.size());
  size_t col_len_len = sizeof(uint32_t) * schema_.size();
  char* col_data_idx = block_addr.data + col_len_len;
  col_compressed_data_.resize(schema_.size());
  for (size_t i = 0; i < schema_.size(); i++) {
    uint32_t col_len = KUint32(block_addr.data + i * sizeof(uint32_t));
    col_compressed_data_[i].len = col_len;
    col_compressed_data_[i].data = col_data_idx;
    col_data_idx += col_len;
  }
  return true;
}

bool TsMetricBlockParser::GetColData(uint32_t col_idx, TsBlockColData* data) {
  if (col_idx >= col_compressed_data_.size()) {
    LOG_ERROR("col_idx[%u] is larger than size[%lu].", col_idx, col_compressed_data_.size());
    return false;
  }
  auto decompressor_mgr = TsEnvInstance::GetInstance().Compressor();
  if (decompressor_mgr == nullptr) {
    LOG_ERROR("cannot initialize compressor manager.");
    return false;
  }
  data->need_free = true;
  bool ok = decompressor_mgr->Decode(col_compressed_data_[col_idx], row_num_, &(data->data), &(data->bitmap));
  return ok;
}

}  //  namespace kwdbts
