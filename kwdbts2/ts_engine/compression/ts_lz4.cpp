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
#include "lz4.h"

namespace kwdbts {
bool LZ4String::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  if (data.len == 0) {
    return true;
  }
  if (data.len > static_cast<size_t>(std::numeric_limits<int>::max())) {
    LOG_ERROR("LZ4 Compress Failed! Input size %lu exceeds supported range.", data.len);
    return false;
  }
  const int input_size = static_cast<int>(data.len);
  const int dst_capacity = LZ4_compressBound(input_size);
  if (dst_capacity <= 0) {
    LOG_ERROR("LZ4 Compress Failed! Invalid destination capacity for input size %d.", input_size);
    return false;
  }
  out->reserve(out->size() + kGeneralCompressionHeaderSize + static_cast<size_t>(dst_capacity));
  PutFixed64(out, data.len);
  const size_t compressed_offset = out->size();
  out->resize(compressed_offset + static_cast<size_t>(dst_capacity));
  int compressed_size = LZ4_compress_default(data.data, out->data() + compressed_offset, input_size, dst_capacity);
  if (compressed_size <= 0) {
    LOG_ERROR("LZ4 Compress Failed!");
    out->clear();
    return false;
  }
  out->resize(compressed_offset + static_cast<size_t>(compressed_size));
  return true;
  // maybe lz4frame if too large?
}

bool LZ4String::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (data.len == 0) {
    *out = TsSliceGuard(data);
    return true;
  }
  if (!HasGeneralCompressionHeader(data, "LZ4 decompress")) {
    return false;
  }
  uint64_t org_size = DecodeFixed64(data.data);
  TsBufferBuilder builder(org_size);
  int ret_size = LZ4_decompress_safe(data.data + kGeneralCompressionHeaderSize, builder.data(),
                                     data.len - kGeneralCompressionHeaderSize, org_size);
  if (ret_size != org_size) {
    LOG_ERROR("LZ4 Decompress Failed!");
    return false;
  }
  *out = builder.GetBuffer();
  return true;
}

size_t LZ4String::GetUncompressedSize(TSSlice data, uint64_t count) const {
  if (data.len == 0) {
    return 0;
  }
  if (!HasGeneralCompressionHeader(data, "LZ4 get uncompressed size")) {
    return static_cast<size_t>(-1);
  }
  uint64_t org_size = DecodeFixed64(data.data);
  return org_size == 0 ? -1 : org_size;
}

}  // namespace kwdbts
