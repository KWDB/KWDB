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

#include <endian.h>

#include <cassert>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "libkwdbts2.h"
#include "ts_coding.h"
#include "ts_compressor.h"

namespace kwdbts {

// Gorilla compression; delta of delta
// Ref: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// NOTE: We should do some extra optimization for this algorithm, because the
//       timestamp is recorded as nanosecond.
namespace Gorilla {

static constexpr int dsize = sizeof(int64_t);
bool Compress(TSSlice data, uint64_t count, std::string *out) {
  assert(data.len == count * dsize);
  out->reserve(dsize * count);
  TsBitWriter writer(out);
  int64_t *ts_data = reinterpret_cast<int64_t *>(data.data);

  // First, record the first timestamp;
  writer.WriteBits(64, ts_data[0]);

  // TODO(zzr): check overflow;
  int64_t delta;
  if (__builtin_ssubl_overflow(ts_data[1], ts_data[0], &delta)) {
    // overflow, return
    return false;
  }
  if (delta < std::numeric_limits<int32_t>::min() || delta > std::numeric_limits<int32_t>::max()) {
    return false;
  }
  writer.WriteBits(32, static_cast<uint32_t>(delta));

  for (int i = 2; i < count; ++i) {
    int64_t current_delta = 0, dod = 0;
    if (__builtin_ssubl_overflow(ts_data[i], ts_data[i - 1], &current_delta)) {
      return false;
    }
    if (__builtin_ssubl_overflow(current_delta, delta, &dod)) {
      return false;
    }
    if (dod == 0) {
      writer.WriteBit(0);
    } else if (dod >= -63 && dod <= 64) {
      writer.WriteBits(2, 0b10);
      writer.WriteBits(7, dod + 63);
    } else if (dod >= -255 && dod <= 256) {
      writer.WriteBits(3, 0b110);
      writer.WriteBits(9, dod + 255);
    } else if (dod >= -255 && dod <= 256) {
      writer.WriteBits(4, 0b1110);
      writer.WriteBits(12, dod + 2047);
    } else {
      writer.WriteBits(4, 0b1111);
      writer.WriteBits(32, static_cast<uint32_t>(dod));
    }
    delta = current_delta;
  }
  return true;
}

bool Decompress(TSSlice data, uint64_t count, std::string *out) {
  out->reserve(8 * count);
  TsBitReader reader(std::string_view{data.data, data.len});

  uint64_t v;
  bool ok = reader.ReadBits(64, &v);
  if (!ok) {
    return false;
  }
  int64_t current_ts = v;
  out->append(reinterpret_cast<char *>(&current_ts), dsize);
  ok = reader.ReadBits(32, &v);
  if (!ok) {
    return false;
  }
  int32_t delta = v;
  current_ts += delta;
  out->append(reinterpret_cast<char *>(&current_ts), dsize);
  for (int i = 2; i < count; ++i) {
    bool bit;
    int64_t dod;

    // Read up to 4 bits
    int ibit = 0;
    for (; ibit < 4; ++ibit) {
      ok = reader.ReadBit(&bit);
      if (!ok) return false;
      if (bit == 0) break;
    }
    switch (ibit) {
      case 0:
        dod = 0;
        break;
      case 1:
        ok = reader.ReadBits(7, &v);
        dod = v - 63;
        break;
      case 2:
        ok = reader.ReadBits(9, &v);
        dod = v - 255;
        break;
      case 3:
        ok = reader.ReadBits(12, &v);
        dod = v - 2047;
        break;
      case 4:
        ok = reader.ReadBits(32, &v);
        dod = v;
        break;
      default:
        assert(false);
    }
    if (!ok) {
      return false;
    }
    delta += dod;
    current_ts += delta;
    out->append(reinterpret_cast<char *>(&current_ts), dsize);
  }
  return true;
}
}  // namespace Gorilla

}  // namespace kwdbts
