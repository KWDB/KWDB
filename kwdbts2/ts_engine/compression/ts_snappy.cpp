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

#include "compression/ts_compressor_defs.h"
namespace kwdbts {
bool SnappyString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  snappy::ByteArraySource src(data.data, data.len);
  BufferSink sink(out);
  snappy::Compress(&src, &sink);
  return true;
}

bool SnappyString::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  TsBufferBuilder builder;
  BufferSink sink(&builder);
  snappy::ByteArraySource src(data.data, data.len);
  bool ok = snappy::Uncompress(&src, &sink);
  if (!ok) {
    return false;
  }
  *out = builder.GetBuffer();
  return true;
}

size_t SnappyString::GetUncompressedSize(TSSlice data, uint64_t count) const {
  size_t result;
  bool ok = snappy::GetUncompressedLength(data.data, data.len, &result);
  if (ok) {
    return result;
  }
  return -1;
}
}  // namespace kwdbts
