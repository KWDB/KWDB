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

#include "compression/ts_codec_utils.h"
#include "compression/ts_compressor_defs.h"
#include "zstd.h"

namespace kwdbts {
static constexpr int ZSTD_CLEVEL_LOW = 1;
static constexpr int ZSTD_CLEVEL_MEDIUM = 3;
static constexpr int ZSTD_CLEVEL_HIGH = 9;
static constexpr int level_map[] = {ZSTD_CLEVEL_MEDIUM, ZSTD_CLEVEL_LOW, ZSTD_CLEVEL_MEDIUM, ZSTD_CLEVEL_HIGH};
bool ZSTDString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  // zstd:

  int level_idx = GetLevelIdx(cfg.level);
  int level = level_map[level_idx];

  if (data.len == 0) {
    out->append(data);
    return true;
  }
  const size_t dst_capacity = ZSTD_compressBound(data.len);
  if (dst_capacity == 0) {
    LOG_ERROR("ZSTD Compress Failed! Input size is incorrect (too large or negative).");
    return false;
  }
  out->reserve(out->size() + kGeneralCompressionHeaderSize + dst_capacity);
  PutFixed64(out, data.len);
  const size_t compressed_offset = out->size();
  out->resize(compressed_offset + dst_capacity);
  size_t compressed_size = ZSTD_compress(out->data() + compressed_offset, dst_capacity, data.data, data.len, level);
  if (ZSTD_isError(compressed_size)) {
    LOG_ERROR("ZSTD Compress Failed!");
    out->clear();
    return false;
  }
  out->resize(compressed_offset + compressed_size);
  return true;
}

bool ZSTDString::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (data.len == 0) {
    *out = TsSliceGuard(data);
    return true;
  }
  if (!HasGeneralCompressionHeader(data, "ZSTD decompress")) {
    return false;
  }
  uint64_t org_size = DecodeFixed64(data.data);
  TsBufferBuilder builder(org_size);
  size_t ret_size = ZSTD_decompress(builder.data(), org_size, data.data + kGeneralCompressionHeaderSize,
                                    data.len - kGeneralCompressionHeaderSize);
  if (ZSTD_isError(ret_size) || ret_size != org_size) {
    LOG_ERROR("ZSTD Decompress Failed!");
    return false;
  }
  *out = builder.GetBuffer();
  return true;
}

size_t ZSTDString::GetUncompressedSize(TSSlice data, uint64_t count) const {
  if (data.len == 0) {
    return 0;
  }
  if (!HasGeneralCompressionHeader(data, "ZSTD get uncompressed size")) {
    return static_cast<size_t>(-1);
  }
  uint64_t org_size = DecodeFixed64(data.data);
  return org_size == 0 ? -1 : org_size;
}

}  // namespace kwdbts
