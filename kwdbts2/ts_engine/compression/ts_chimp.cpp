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

#include <array>

#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"

namespace kwdbts {
alignas(64) static constexpr std::array<int, 8> leading_mapping{0, 8, 12, 16, 18, 20, 22, 24};
template <class T>
bool Chimp<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  assert(data.len == sizeof(T) * count);
  auto sz = sizeof(T) * 8;

  if (count == 0) {
    return true;  // no data, no need to compress
  }

  using utype = std::conditional_t<std::is_same_v<T, double>, uint64_t, uint32_t>;
  const utype *ptr = reinterpret_cast<utype *>(data.data);
  TsBitWriter writer(out);

  writer.WriteBits(sz, ptr[0]);
  utype prev = ptr[0];
  uint64_t buffer = 0;
  int prev_lead_idx = 0;
  for (int i = 1; i < count; ++i) {
    utype xored = ptr[i] ^ prev;
    prev = ptr[i];
    if (xored == 0) {
      writer.WriteBits(2, 0);
      continue;
    }
    int trail, lead;
    if constexpr (sizeof(xored) == 8) {
      trail = __builtin_ctzl(xored);
      lead = __builtin_clzl(xored);
    } else {
      trail = __builtin_ctz(xored);
      lead = __builtin_clz(xored);
    }
    int lead_idx = 0;
    for (int i_lead_idx = leading_mapping.size() - 1; i_lead_idx >= 0; --i_lead_idx) {
      if (leading_mapping[i_lead_idx] <= lead) {
        lead_idx = i_lead_idx;
        break;
      }
    }
    int lead_bits = leading_mapping[lead_idx];
    if (trail > 6) {
      int center_bits = sz - lead_bits - trail;
      buffer = (((0b01 << 3) + lead_idx) << 6) + center_bits;
      writer.WriteBits(11, buffer);
      writer.WriteBits(center_bits, xored >> trail);
    } else {
      if (lead_idx == prev_lead_idx) {
        buffer = 0b10;
        writer.WriteBits(2, buffer);
      } else {
        buffer = (0b11 << 3) + lead_idx;
        writer.WriteBits(5, buffer);
      }
      writer.WriteBits(sz - lead_bits, xored);
    }
    prev_lead_idx = lead_idx;
  }
  return true;
}

template <class T>
bool Chimp<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (count == 0) {
    return true;
  }
  auto sz = sizeof(T) * 8;
  TsBufferBuilder builder;
  builder.reserve(count * sizeof(T));
  TsBitReader reader(std::string_view{data.data, data.len});
  uint64_t v;
  bool ok = reader.ReadBits(sz, &v);
  if (!ok) {
    return false;
  }
  if constexpr (std::is_same_v<T, double>) {
    PutFixed64(&builder, v);
  } else {
    PutFixed32(&builder, v);
  }
  using utype = std::conditional_t<std::is_same_v<T, double>, uint64_t, uint32_t>;
  utype prev = v, prev_lead_idx = 0;
  for (int i = 1; i < count; ++i) {
    ok = reader.ReadBits(2, &v);
    if (!ok) {
      return false;
    }
    utype xored = 0;
    switch (v) {
      case 0b00:
        break;
      case 0b01: {
        ok = reader.ReadBits(9, &v);
        if (!ok) return false;
        int idx = v >> 6;
        int center_bits = v & 0x3F;
        ok = reader.ReadBits(center_bits, &v);
        if (!ok) return false;
        int trail = sz - leading_mapping[idx] - center_bits;
        xored = v << trail;
        prev_lead_idx = idx;
        break;
      }
      case 0b10: {
        ok = reader.ReadBits(sz - leading_mapping[prev_lead_idx], &v);
        xored = v;
        if (!ok) return false;
        break;
      }
      case 0b11: {
        uint64_t idx;
        ok = reader.ReadBits(3, &idx);
        if (!ok) return false;
        prev_lead_idx = idx;
        ok = reader.ReadBits(sz - leading_mapping[idx], &v);
        xored = v;
        if (!ok) return false;
        break;
      }
      default:
        assert(false);
    }
    utype current = prev ^ xored;
    if constexpr (std::is_same_v<T, double>) {
      PutFixed64(&builder, current);
    } else {
      PutFixed32(&builder, current);
    }
    prev = current;
  }
  *out = builder.GetBuffer();
  return true;
}
// export
template class Chimp<double>;
template class Chimp<float>;
}  // namespace kwdbts
