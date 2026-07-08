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

#include "zlib.h"
#include "compression/ts_codec_utils.h"
#include "compression/ts_compressor_defs.h"

namespace kwdbts {
bool ZLIBString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  static constexpr int Z_CLEVEL_MEDIUM = 6;
  static constexpr int level_map[] = {Z_CLEVEL_MEDIUM, Z_BEST_SPEED, Z_CLEVEL_MEDIUM, Z_BEST_COMPRESSION};
  int level_idx = GetLevelIdx(cfg.level);
  int level = level_map[level_idx];
  if (data.len == 0) {
    out->append(data);
    return true;
  }
  if (data.len > static_cast<size_t>(std::numeric_limits<uInt>::max())) {
    LOG_ERROR("Zlib Compress Failed! Input size %lu exceeds supported range.", data.len);
    return false;
  }
  z_stream zs = {};
  if (deflateInit(&zs, level) != Z_OK) {
    LOG_ERROR("Zlib deflateInit failed!");
    return false;
  }

  const uInt input_size = static_cast<uInt>(data.len);
  const uLong dst_capacity = deflateBound(&zs, input_size);
  if (dst_capacity == 0 || dst_capacity > static_cast<uLong>(std::numeric_limits<uInt>::max())) {
    LOG_ERROR("Zlib deflateBound failed for input size %u.", input_size);
    deflateEnd(&zs);
    return false;
  }

  out->reserve(out->size() + kGeneralCompressionHeaderSize + static_cast<size_t>(dst_capacity));
  PutFixed64(out, data.len);
  const size_t compressed_offset = out->size();
  out->resize(compressed_offset + static_cast<size_t>(dst_capacity));

  zs.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(data.data));
  zs.avail_in = input_size;
  zs.next_out = reinterpret_cast<Bytef *>(out->data() + compressed_offset);
  zs.avail_out = static_cast<uInt>(dst_capacity);

  int ret = deflate(&zs, Z_FINISH);
  if (ret != Z_STREAM_END) {
    LOG_ERROR("Zlib deflate failed during compression! Error code:%d", ret);
    deflateEnd(&zs);
    out->clear();
    return false;
  }

  size_t compressed_size = zs.total_out;
  deflateEnd(&zs);
  out->resize(compressed_offset + compressed_size);
  return true;
}

bool ZLIBString::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (data.len == 0) {
    *out = TsSliceGuard(data);
    return true;
  }
  if (!HasGeneralCompressionHeader(data, "Zlib decompress")) {
    return false;
  }
  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  if (inflateInit(&zs) != Z_OK) {
    LOG_ERROR("Zlib inflateInit failed!");
    return false;
  }

  uint64_t org_size = DecodeFixed64(data.data);
  TsBufferBuilder builder(org_size);
  zs.next_out = reinterpret_cast<Bytef *>(builder.data());
  zs.avail_out = org_size;

  zs.next_in = reinterpret_cast<Bytef *>(data.data + kGeneralCompressionHeaderSize);
  zs.avail_in = data.len - kGeneralCompressionHeaderSize;

  int ret = inflate(&zs, Z_FINISH);
  if (ret != Z_STREAM_END) {
    LOG_ERROR("Zlib inflate failed during decompression! Error code: %d", ret);
    inflateEnd(&zs);
    return false;
  }
  inflateEnd(&zs);
  *out = builder.GetBuffer();
  return true;
}

size_t ZLIBString::GetUncompressedSize(TSSlice data, uint64_t count) const {
  if (data.len == 0) {
    return 0;
  }
  if (!HasGeneralCompressionHeader(data, "Zlib get uncompressed size")) {
    return static_cast<size_t>(-1);
  }
  uint64_t org_size = DecodeFixed64(data.data);
  return org_size == 0 ? -1 : org_size;
}

}  // namespace kwdbts
