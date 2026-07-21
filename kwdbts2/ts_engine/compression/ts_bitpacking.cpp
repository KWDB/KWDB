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

#include "compression/ts_encoder_defs.h"

#include "ts_bufferbuilder.h"
#include "ts_sliceguard.h"

namespace kwdbts {

bool BitPacking::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  if (count == 0) return true;
  assert(data.len == count);
  uint8_t c = 0;
  for (int i = 0; i < count; ++i) {
    c += (data.data[i] != 0) << (i % 8);
    if (i % 8 == 7) {
      out->push_back(c);
      c = 0;
    }
  }
  if (count % 8 != 0) {
    out->push_back(c);
  }
  return true;
}
bool BitPacking::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (count == 0) return true;
  if (data.len < (count + 7) / 8) return false;
  TsBufferBuilder builder(count);
  char *ptr = builder.data();
  for (int i = 0; i < count; ++i) {
    uint8_t c = data.data[i / 8];
    ptr[i] = ((c >> (i % 8)) & 1);
  }
  *out = builder.GetBuffer();
  return out->size() == count;
}

}  // namespace kwdbts
