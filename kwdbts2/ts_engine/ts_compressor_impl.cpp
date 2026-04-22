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

#include "ts_compressor_impl.h"

#include <endian.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ios>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "data_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compressor.h"
#include "ts_sliceguard.h"

namespace kwdbts {

namespace {

constexpr size_t kGeneralCompressionHeaderSize = sizeof(uint64_t);
constexpr int kDefaultCompressionLevelIndex = 0;
constexpr int kLowCompressionLevelIndex = 1;
constexpr int kMediumCompressionLevelIndex = 2;
constexpr int kHighCompressionLevelIndex = 3;

int GetLevelIdx(int level) {
  switch (level) {
    case roachpb::COMPRESS_LEVEL_UNSPECIFIED:
      switch (EngineOptions::compress_level) {
        case CompressLevel::LOW:
          return kLowCompressionLevelIndex;
        case CompressLevel::MEDIUM:
          return kMediumCompressionLevelIndex;
        case CompressLevel::HIGH:
          return kHighCompressionLevelIndex;
        default:
          LOG_ERROR("Invalid cluster compress level: %d, fallback to default level.",
                    static_cast<int>(EngineOptions::compress_level));
          return kDefaultCompressionLevelIndex;
      }
    case roachpb::COMPRESS_LEVEL_LOW:
    case roachpb::COMPRESS_LEVEL_MEDIUM:
    case roachpb::COMPRESS_LEVEL_HIGH:
      return level;
    default: {
      if (level < 0 || level >= 4) {
        LOG_ERROR("Invalid compress level index: %d, fallback to default level.", level);
        return kDefaultCompressionLevelIndex;
      }
    }
  }
  return kDefaultCompressionLevelIndex;
}

bool HasGeneralCompressionHeader(TSSlice data, const char* algorithm_name) {
  if (data.len < kGeneralCompressionHeaderSize) {
    LOG_ERROR("%s input too short: %lu", algorithm_name, data.len);
    return false;
  }
  return true;
}

CompressAlgo GetDefaultCompressAlgo(DATATYPE dtype) {
  switch (dtype) {
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      return CompressAlgo::kPlain;
    default:
      break;
  }
  return EngineOptions::compress_stage == 2 || EngineOptions::compress_stage == 3 ?
         EngineOptions::compression_algorithm : CompressAlgo::kPlain;
}

}  // namespace

// Gorilla compression; a.k.a delta of delta
// Ref: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// NOTE: We should do some extra optimization for this algorithm, because the
//       timestamp is recorded as nanosecond.
bool GorillaInt::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
  static constexpr int dsize = sizeof(int64_t);
  if (count == 0) {
    return true;
  }
  assert(data.len == count * dsize);
  out->reserve(out->size() + dsize * count);
  TsBitWriter writer(out);
  int64_t *ts_data = reinterpret_cast<int64_t *>(data.data);

  // First, record the first timestamp;
  writer.WriteBits(64, ts_data[0]);
  if (count == 1) {
    return true;
  }

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
    } else if (dod >= -2047 && dod <= 2048) {
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

bool GorillaInt::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  static constexpr int dsize = sizeof(int64_t);
  if (count == 0) {
    *out = TsSliceGuard();
    return true;
  }
  TsBufferBuilder builder;
  builder.reserve(8 * count);
  TsBitReader reader(std::string_view{data.data, data.len});

  uint64_t v;
  bool ok = reader.ReadBits(64, &v);
  if (!ok) {
    return false;
  }
  int64_t current_ts = v;
  builder.append(reinterpret_cast<char *>(&current_ts), dsize);
  if (count == 1) {
    assert(builder.size() == dsize);
    *out = builder.GetBuffer();
    return true;
  }
  ok = reader.ReadBits(32, &v);
  if (!ok) {
    return false;
  }
  int32_t delta = v;
  current_ts += delta;
  builder.append(reinterpret_cast<char *>(&current_ts), dsize);
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
    builder.append(reinterpret_cast<char *>(&current_ts), dsize);
  }
  *out = builder.GetBuffer();
  return true;
}

template <class T>
static inline bool CheckedSub(const T a, const T b, T *out) {
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>);
  if constexpr (std::is_same_v<T, int64_t>) {
    return __builtin_ssubl_overflow(a, b, out);
  } else {
    return __builtin_ssub_overflow(a, b, out);
  }
  return false;
}

template <class T>
static inline void TypedPutVarint(TsBufferBuilder *dst, T v) {
  static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>);
  if constexpr (std::is_same_v<T, uint64_t>) {
    return PutVarint64(dst, v);
  } else {
    return PutVarint32(dst, v);
  }
}

template <class T>
static inline void TypedPutFixed(TsBufferBuilder *dst, T v) {
  if constexpr (sizeof(T) == 8) {
    return PutFixed64(dst, v);
  } else {
    return PutFixed32(dst, v);
  }
}

template <class T>
static inline const char *TypedDecodeVarint(const char *ptr, const char *limit, T *v) {
  static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>);
  if constexpr (std::is_same_v<T, uint64_t>) {
    return DecodeVarint64(ptr, limit, v);
  } else {
    return DecodeVarint32(ptr, limit, v);
  }
}

template <class T>
bool GorillaIntV2<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
  if (count == 0) {
    return true;
  }
  assert(data.len == count * stride);
  out->reserve(out->size() + stride * count);
  T *ts_data = reinterpret_cast<T *>(data.data);

  // 1. record the first timestamp;
  TypedPutVarint(out, EncodeZigZag(ts_data[0]));
  if (count == 1) {
    return true;
  }
  // 2. record delta
  T delta;
  if (CheckedSub(ts_data[1], ts_data[0], &delta)) {
    // overflow, return
    return false;
  }
  TypedPutVarint(out, EncodeZigZag(delta));

  for (int i = 2; i < count; ++i) {
    T current_delta = 0, dod = 0;
    if (CheckedSub(ts_data[i], ts_data[i - 1], &current_delta)) {
      return false;
    }
    if (CheckedSub(current_delta, delta, &dod)) {
      return false;
    }
    TypedPutVarint(out, EncodeZigZag(dod));
    delta = current_delta;
  }
  return true;
}

template <class T>
bool GorillaIntV2<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (count == 0) {
    return true;
  }
  TsBufferBuilder builder(stride * count);
  T *outdata = reinterpret_cast<T *>(builder.data());

  using utype = std::make_unsigned_t<T>;
  utype v;
  const char *limit = data.data + data.len;
  const char *ptr = TypedDecodeVarint(data.data, limit, &v);
  if (ptr == nullptr) {
    return false;
  }
  T ts = DecodeZigZag(v);
  outdata[0] = ts;
  if (count == 1) {
    *out = builder.GetBuffer();
    return true;
  }

  ptr = TypedDecodeVarint(ptr, limit, &v);
  if (ptr == nullptr) {
    return false;
  }
  T delta = DecodeZigZag(v);
  ts += delta;
  outdata[1] = ts;
  for (int i = 2; i < count; ++i) {
    ptr = TypedDecodeVarint(ptr, limit, &v);
    if (ptr == nullptr) {
      return false;
    }
    T dod = DecodeZigZag(v);
    delta += dod;
    ts += delta;
    outdata[i] = ts;
  }

  *out = builder.GetBuffer();
  return true;
}

static int leading_mapping[] = {0, 8, 12, 16, 18, 20, 22, 24};
template <class T>
bool Chimp<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
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
    int *p = std::upper_bound(leading_mapping, leading_mapping + 8, lead);
    p--;
    int lead_idx = p - leading_mapping;
    if (trail > 6) {
      int center_bits = sz - *p - trail;
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
      writer.WriteBits(sz - *p, xored);
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

namespace _simple8b_detail {
alignas(64) static constexpr uint32_t ITEMWIDTH[16] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
/* The following array is generate by python code:
>>> width = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60]
>>> print(list(map(lambda x : bisect.bisect_left(width, x), range(64))))
*/

alignas(64) static constexpr uint8_t NBITS2SELECTOR[64] = {
    0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 16, 16, 16};

alignas(64) static constexpr int32_t GROUPSIZE[16] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

template <typename T>
static inline int GetValidBits(T v) {
  static_assert(std::is_unsigned_v<T>);
  if (v == 0) return 1;
  return 64 - __builtin_clzll(v);
}

template <class T>
static inline std::make_unsigned_t<T> EncodeZigZagIfNeeded(T v) {
  if constexpr (std::is_signed_v<T>) {
    return EncodeZigZag(v);
  }
  return v;
}

template <class T>
static inline T DecodeZigZagIfNeeded(std::make_unsigned_t<T> v) {
  if constexpr (std::is_signed_v<T>) {
    return DecodeZigZag(v);
  }
  return v;
}

template <typename T>
static bool CompressImplGreedy(const T *data, uint64_t count, TsBufferBuilder *out) {
  static_assert(std::is_integral_v<T>);
  for (int i = 0; i < count;) {
    auto data_i = EncodeZigZagIfNeeded(data[i]);
    int valid_nbits = GetValidBits(data_i);
    if (valid_nbits > 60) return false;
    uint8_t selector = NBITS2SELECTOR[valid_nbits];

    auto header = data_i;
    bool all_same = true;
    int j = i + 1;
    for (; j < count; ++j) {
      auto data_j = EncodeZigZagIfNeeded(data[j]);
      all_same &= (data_j == header);
      if (all_same && j - i < GROUPSIZE[0]) continue;
      if (j - i >= GROUPSIZE[selector]) {
        // current group cannot take this number, break and process in next iteration
        break;
      }
      valid_nbits = GetValidBits(data_j);
      if (valid_nbits > 60) return false;  // data >= 1ULL << 60, can not compress
      uint8_t current_selector = NBITS2SELECTOR[valid_nbits];

      // check whether current itemwidth can hold this number;
      if (GROUPSIZE[current_selector] < GROUPSIZE[selector]) {
        // no, check wether we can enlarge the itemwidth;
        if (j - i + 1 <= GROUPSIZE[current_selector]) {
          // yes, record the current selector and continue to next number;
          selector = current_selector;
          continue;
        }
        break;
      }
    }
    // encode;
    // 1. Encode special selector first (selector = 0 or 1)
    int n_number = j - i;
    bool can_be_zero_data = n_number >= 120;
    assert(n_number <= 240);
    if (can_be_zero_data) {
      uint64_t special_selector = n_number == 240 ? 0 : 1;
      uint64_t batch = ((special_selector) << 60) + header;
      PutFixed64(out, batch);
      i += GROUPSIZE[special_selector];
      n_number -= GROUPSIZE[special_selector];
      if (n_number == 0) continue;
    }

    assert(selector != -1);
    if (!can_be_zero_data) {
      while (n_number < GROUPSIZE[selector]) {
        ++selector;
      }
    }
    while (n_number >= GROUPSIZE[selector]) {
      uint64_t batch = selector;
      uint64_t item_width = ITEMWIDTH[selector];
      uint64_t mask = (1ULL << item_width) - 1;
      for (int k = 0; k < GROUPSIZE[selector]; ++k) {
        batch <<= item_width;
        batch += static_cast<uint64_t>(EncodeZigZagIfNeeded(data[i + k])) & mask;
      }
      batch <<= 60 % GROUPSIZE[selector];
      assert(batch >> 60 == selector);
      PutFixed64(out, batch);
      i += GROUPSIZE[selector];
      n_number -= GROUPSIZE[selector];
    }
  }
  return true;
}

// Can get higher compression ratio in some case using dynamic programming algorithm,
// but slow, O(n) complicity with a large constant
// TODO(zzr): implement this algorithm
template <typename T>
static bool CompressImplDP(const T *data, uint64_t count, std::string *out) {
  assert(false);
  return false;
}

template <class T>
static inline T Restore(uint64_t n, int width) {
  uint64_t mask = (1ULL << width) - 1;
  n &= mask;
  return DecodeZigZagIfNeeded<T>(n);
}

template <typename T>
bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) {
  if (data.len % 8 != 0) {
    return false;
  }
  TsBufferBuilder builder(sizeof(T) * count);
  T *outdata = reinterpret_cast<T *>(builder.data());
  uint64_t idx = 0;
  const char *cursor = data.data;
  while (cursor < data.data + data.len && idx < count) {
    uint64_t batch = *reinterpret_cast<const uint64_t *>(cursor);
    int selector = (batch) >> 60;
    batch &= (1ULL << 60) - 1;
    if (selector <= 1) {
      T val = Restore<T>(batch, 60);
      for (int i = 0; i < GROUPSIZE[selector]; ++i) {
        outdata[idx++] = val;
      }
    } else {
      batch >>= 60 % GROUPSIZE[selector];
      int shift = (GROUPSIZE[selector] - 1) * ITEMWIDTH[selector];
      for (int i = 0; i < GROUPSIZE[selector]; ++i) {
        assert(shift >= 0);
        T val = Restore<T>(batch >> shift, ITEMWIDTH[selector]);
        outdata[idx++] = val;
        shift -= ITEMWIDTH[selector];
      }
      assert(shift + ITEMWIDTH[selector] == 0);
    }
    cursor += 8;
  }
  *out = builder.GetBuffer();
  return out->size() / sizeof(T) == count;
}

template <class T>
inline auto CheckedSubForS8B(const T a, const T b) -> std::pair<int64_t, bool> {
  if constexpr (std::is_unsigned_v<T>) {
    int64_t diff = static_cast<int64_t>(static_cast<std::make_signed_t<T>>(a - b));
    bool overflow = (a > b && diff < 0) || (a < b && diff > 0);
    return {diff, overflow};
  }

  int64_t aa = static_cast<int64_t>(a);
  int64_t bb = static_cast<int64_t>(b);
  int64_t diff = 0;
  bool overflow = __builtin_ssubl_overflow(aa, bb, &diff);
  return {diff, overflow};
}

//  delta-of-delta + simple8b
template <typename T>
bool V2CompressImplGreedy(const T *data, uint64_t count, TsBufferBuilder *out) {
  static_assert(std::is_integral_v<T>);
  if (count == 0) {
    return true;
  }

  if constexpr (sizeof(T) >= 4) {
    auto v1 = EncodeZigZagIfNeeded(data[0]);
    TypedPutVarint(out, v1);
  } else if constexpr (sizeof(T) == 2) {
    PutFixed16(out, data[0]);
  } else {
    out->append(reinterpret_cast<const char *>(&data[0]), sizeof(T));
  }

  if (count == 1) {
    return true;
  }

  auto [delta, overflow] = CheckedSubForS8B(data[1], data[0]);
  if (overflow) {
    return false;
  }

  auto v2 = EncodeZigZagIfNeeded(delta);
  TypedPutVarint(out, v2);

  int run_length_limit = 0xFFFF;
  for (int i = 2; i < count;) {
    auto [i_delta, i_overflow] = CheckedSubForS8B(data[i], data[i - 1]);
    const auto [prev_delta, prev_delta_overflow] = CheckedSubForS8B(data[i - 1], data[i - 2]);
    assert(!prev_delta_overflow);

    auto [dod, dod_overflow] = CheckedSubForS8B(i_delta, prev_delta);
    if (i_overflow || dod_overflow) {
      return false;
    }

    uint64_t dod_zigzag = EncodeZigZagIfNeeded(dod);
    int valid_nbits_i = GetValidBits(dod_zigzag);
    if (valid_nbits_i > 60) {
      return false;
    }
    uint8_t selector = NBITS2SELECTOR[valid_nbits_i];

    int run_length = 1;
    int j = i + 1;
    bool can_use_rle = valid_nbits_i < 44;

    for (; j < count && run_length < run_length_limit; ++j) {
      auto [j_delta, j_overflow] = CheckedSubForS8B(data[j], data[j - 1]);
      const auto [prev_delta_j, prev_delta_j_overflow] = CheckedSubForS8B(data[j - 1], data[j - 2]);
      assert(!prev_delta_j_overflow);
      auto [j_dod, j_dod_overflow] = CheckedSubForS8B(j_delta, prev_delta_j);
      if (j_overflow || j_dod_overflow) {
        return false;
      }

      if (can_use_rle && dod == j_dod) {
        run_length++;
        continue;
      }

      // check current groupsize
      if (run_length >= GROUPSIZE[selector] && run_length != 1) {
        assert(can_use_rle);
        break;
      }
      can_use_rle = false;

      if (j - i >= GROUPSIZE[selector]) {
        break;
      }

      uint64_t dod_zigzag_j = EncodeZigZagIfNeeded(j_dod);

      // current group size is bigger than run length, check following datas;
      int valid_nbits_j = GetValidBits(dod_zigzag_j);
      if (valid_nbits_j > 60) {
        return false;
      }
      uint8_t selector_j = NBITS2SELECTOR[valid_nbits_j];
      if (GROUPSIZE[selector_j] < GROUPSIZE[selector]) {
        if (j - i + 1 <= GROUPSIZE[selector_j]) {
          // yes, record the current selector and continue to next number;
          selector = selector_j;
        } else {
          // no, break and process in next iteration
          break;
        }
      }
    }

    can_use_rle = can_use_rle && run_length > 1;

    if (!can_use_rle) {
      while (j - i < GROUPSIZE[selector] && selector < 15) {
        ++selector;
      }
      assert(selector != 16);
      can_use_rle = GROUPSIZE[selector] <= run_length && run_length != 1 && valid_nbits_i < 44;
    }

    // encode;
    if (can_use_rle) {
      assert(run_length < 65536);
      assert(dod_zigzag < (1ULL << 44));
      uint64_t special_selector = 0;
      uint64_t batch = ((special_selector) << 60) + run_length;
      batch <<= 44;
      batch += dod_zigzag;
      PutFixed64(out, batch);
      j = i + run_length;
    } else {
      uint64_t batch = selector;
      for (int k = i; k < i + GROUPSIZE[selector]; ++k) {
        batch <<= ITEMWIDTH[selector];
        auto [d1, d1_overflow] = CheckedSubForS8B(data[k], data[k - 1]);
        auto [d2, d2_overflow] = CheckedSubForS8B(data[k - 1], data[k - 2]);
        assert(!d1_overflow && !d2_overflow);
        int64_t current_dod = d1 - d2;
        uint64_t current_dod_zigzag = EncodeZigZagIfNeeded(current_dod);
        assert(current_dod_zigzag >> ITEMWIDTH[selector] == 0);
        batch += current_dod_zigzag;
      }
      j = i + GROUPSIZE[selector];
      batch <<= 60 % ITEMWIDTH[selector];
      assert(batch >> 60 == selector);
      PutFixed64(out, batch);
    }
    i = j;
  }
  return true;
}

template <typename T>
bool V2Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) {
  if (count == 0) {
    return true;
  }
  TsBufferBuilder builder(sizeof(T) * count);
  T *outdata = reinterpret_cast<T *>(builder.data());
  uint64_t idx = 0;

  const char *cursor = data.data;
  const char *end = data.data + data.len;

  using utype_t = std::make_unsigned_t<T>;

  T prev_value = 0, curr_value = 0;
  if constexpr (sizeof(T) >= 4) {
    utype_t v1 = 0;
    cursor = TypedDecodeVarint(cursor, end, &v1);
    prev_value = DecodeZigZagIfNeeded<T>(v1);
  } else if constexpr (sizeof(T) == 2) {
    prev_value = DecodeFixed16(cursor);
    cursor += sizeof(T);
  } else {
    prev_value = *reinterpret_cast<const T *>(cursor);
    cursor += sizeof(utype_t);
  }

  outdata[idx++] = prev_value;
  if (count == 1) {
    *out = builder.GetBuffer();
    return idx == count && cursor == end;
  }

  uint64_t v2 = 0;
  cursor = TypedDecodeVarint(cursor, end, &v2);
  int64_t delta = DecodeZigZagIfNeeded<int64_t>(v2);
  curr_value = prev_value + delta;


  outdata[idx++] = curr_value;

  while (cursor + 8 <= end && idx < count) {
    uint64_t batch = *reinterpret_cast<const uint64_t *>(cursor);
    int selector = (batch) >> 60;
    uint64_t pack_data = batch & ((1ULL << 60) - 1);
    cursor += 8;

    if (selector == 0) {
      int run_length = pack_data >> 44;
      uint64_t dod_zigzag = pack_data & ((1ULL << 44) - 1);
      int64_t dod = DecodeZigZagIfNeeded<int64_t>(dod_zigzag);
      for (int i = 0; i < run_length && idx < count; ++i) {
        delta += dod;
        curr_value += delta;
        outdata[idx++] = curr_value;
      }
      continue;
    }

    pack_data >>= 60 % GROUPSIZE[selector];
    int shift = (GROUPSIZE[selector] - 1) * ITEMWIDTH[selector];
    for (int i = 0; i < GROUPSIZE[selector] && idx < count; ++i) {
      assert(shift >= 0);
      int64_t dod = Restore<int64_t>(pack_data >> shift, ITEMWIDTH[selector]);
      delta += dod;
      curr_value += delta;
      outdata[idx++] = curr_value;
      shift -= ITEMWIDTH[selector];
    }
    // assert(shift + ITEMWIDTH[selector] == 0);
  }
  *out = builder.GetBuffer();
  return idx == count && cursor == end;
}

};  // namespace _simple8b_detail

template <class T>
bool Simple8BInt<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
  assert(data.len == sizeof(T) * count);
  const T *p_data = reinterpret_cast<const T *>(data.data);
  return _simple8b_detail::CompressImplGreedy<T>(p_data, count, out);
}

template <class T>
bool Simple8BInt<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  return _simple8b_detail::Decompress<T>(data, count, out);
}

template <class T>
bool Simple8BIntV2<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
  assert(data.len == sizeof(T) * count);
  const T *p_data = reinterpret_cast<const T *>(data.data);
  return _simple8b_detail::V2CompressImplGreedy<T>(p_data, count, out);
}

template <class T>
bool Simple8BIntV2<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  return _simple8b_detail::V2Decompress<T>(data, count, out);
}

bool BitPacking::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
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
  TsBufferBuilder builder(count);
  char *ptr = builder.data();
  for (int i = 0; i < count; ++i) {
    uint8_t c = data.data[i / 8];
    ptr[i] = ((c >> (i % 8)) & 1);
  }
  *out = builder.GetBuffer();
  return out->size() == count;
}

bool SnappyString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
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

bool LZ4String::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
  if (data.len == 0) {
    return true;
  }
  if (data.len > static_cast<size_t>(std::numeric_limits<int>::max())) {
    LOG_ERROR("LZ4 Compress Failed! Input size %lu exceeds supported range.", data.len);
    return false;
  }
  (void)level;
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
  int compressed_size = LZ4_compress_default(data.data, out->data() + compressed_offset, input_size,
                                             dst_capacity);
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

bool ZSTDString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
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

bool ZLIBString::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, int level) const {
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

  zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data.data));
  zs.avail_in = input_size;
  zs.next_out = reinterpret_cast<Bytef*>(out->data() + compressed_offset);
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
  zs.next_out = reinterpret_cast<Bytef*>(builder.data());
  zs.avail_out = org_size;

  zs.next_in = reinterpret_cast<Bytef*>(data.data + kGeneralCompressionHeaderSize);
  zs.avail_in = data.len - kGeneralCompressionHeaderSize;

  int  ret = inflate(&zs, Z_FINISH);
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

// Reuse the thread local buffer to avoid memory allocation
thread_local TsBufferBuilder tl_first_out;
thread_local TsBufferBuilder tl_second_out;

bool CompressorManager::TwoLevelCompressor::Compress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count,
                                                     TsBufferBuilder *out, int level) const {
  auto first = first_algo_;
  auto second = second_algo_;
  if (IsPlain()) {
    EncodeAlgorithm(out, first, second);
    out->append(raw);
    return true;
  }
  TSSlice data;
  bool ok = true;
  if (first_ == nullptr) {
    data = raw;
  } else {
    tl_first_out.clear();
    ok = first_->Compress(raw, bitmap, count, &tl_first_out);
    data = tl_first_out.AsSlice();
  }
  if (!ok || data.len > raw.len) {
    first = EncodeAlgo::kPlain;
    data = raw;
  }
  if (second_ == nullptr) {
    EncodeAlgorithm(out, first, second);
    out->append(data);
    return true;
  }
  tl_second_out.clear();
  ok = second_->Compress(data, &tl_second_out, level);
  if (!ok || tl_second_out.size() > data.len) {
    second = CompressAlgo::kPlain;
  } else {
    data = tl_second_out.AsSlice();
  }
  EncodeAlgorithm(out, first, second);
  out->append(data);
  return true;
}
bool CompressorManager::TwoLevelCompressor::Decompress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count,
                                                       TsSliceGuard *out) const {
  if (IsPlain()) return false;  // control should not reach here.
  uint16_t first_algo, second_algo;
  GetFixed16(&raw, &first_algo);
  GetFixed16(&raw, &second_algo);

  TsSliceGuard buf;
  TSSlice data;
  bool ok = true;
  if (second_ == nullptr) {
    data = raw;
  } else {
    ok = second_->Decompress(raw, &buf);
    data = buf.AsSlice();
  }
  if (!ok) {
    return false;
  }
  if (first_ == nullptr) {
    std::swap(*out, buf);
    return true;
  }
  return first_->Decompress(data, bitmap, count, out);
}

std::tuple<EncodeAlgo, CompressAlgo> CompressorManager::TwoLevelCompressor::GetAlgorithms() const {
  return {first_algo_, second_algo_};
}

CompressorManager::CompressorManager() {
  // 1. construct default algorithms.
  const std::vector<DATATYPE> timestamp_type{
      DATATYPE::TIMESTAMP64,     DATATYPE::TIMESTAMP64_MICRO,     DATATYPE::TIMESTAMP64_NANO,
      DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64_MICRO, DATATYPE::TIMESTAMP64_NANO};
  for (auto i : timestamp_type) {
    default_encode_algs_[i] = EncodeAlgo::kSimple8B_V2_s64;
  }

  default_encode_algs_[DATATYPE::INT16] = EncodeAlgo::kSimple8B_V2_s16;
  default_encode_algs_[DATATYPE::INT32] = EncodeAlgo::kSimple8B_V2_s32;
  default_encode_algs_[DATATYPE::INT64] = EncodeAlgo::kSimple8B_V2_s64;
  default_encode_algs_[DATATYPE::FLOAT] = EncodeAlgo::kChimp_32;
  default_encode_algs_[DATATYPE::DOUBLE] = EncodeAlgo::kChimp_64;
  // char string
  default_encode_algs_[DATATYPE::BYTE] = EncodeAlgo::kPlain;
  default_encode_algs_[DATATYPE::CHAR] = EncodeAlgo::kPlain;
  default_encode_algs_[DATATYPE::BINARY] = EncodeAlgo::kPlain;

  default_encode_algs_[DATATYPE::BOOL] = EncodeAlgo::kBitPacking;

  /* customized pre-defined levels */
  // lz4: level is ignored when using LZ4_compress_default.
  // snappy: no need
  // zstd:
  static constexpr int ZSTD_CLEVEL_LOW = 1;
  static constexpr int ZSTD_CLEVEL_MEDIUM = 3;
  static constexpr int ZSTD_CLEVEL_HIGH = 9;
  // zlib:
  static constexpr int Z_CLEVEL_MEDIUM = 6;

  compress_levels_[CompressAlgo::kPlain] = {1, 1, 1, 1};
  // algs_level_[GenCompAlg::kLz4] = {LZ4HC_CLEVEL_DEFAULT, LZ4HC_CLEVEL_MIN, LZ4HC_CLEVEL_DEFAULT, LZ4HC_CLEVEL_MAX};
  compress_levels_[CompressAlgo::kLz4] = {1, 1, 1, 1};
  compress_levels_[CompressAlgo::kSnappy] = {1, 1, 1, 1};
  compress_levels_[CompressAlgo::kZstd] = {ZSTD_CLEVEL_MEDIUM, ZSTD_CLEVEL_LOW, ZSTD_CLEVEL_MEDIUM, ZSTD_CLEVEL_HIGH};
  compress_levels_[CompressAlgo::kZlib] = {Z_CLEVEL_MEDIUM, Z_BEST_SPEED, Z_CLEVEL_MEDIUM, Z_BEST_COMPRESSION};

  // 2. construct encoding algorithms.
  ts_encoders_[EncodeAlgo::kGorilla_32] = &ConcreateTsCompressor<GorillaIntV2<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kGorilla_64] = &ConcreateTsCompressor<GorillaIntV2<int64_t>>::GetInstance();

  ts_encoders_[EncodeAlgo::kSimple8B_s8] = &ConcreateTsCompressor<Simple8BInt<int8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s16] = &ConcreateTsCompressor<Simple8BInt<int16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s32] = &ConcreateTsCompressor<Simple8BInt<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s64] = &ConcreateTsCompressor<Simple8BInt<int64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u8] = &ConcreateTsCompressor<Simple8BInt<uint8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u16] = &ConcreateTsCompressor<Simple8BInt<uint16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u32] = &ConcreateTsCompressor<Simple8BInt<uint32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u64] = &ConcreateTsCompressor<Simple8BInt<uint64_t>>::GetInstance();

  ts_encoders_[EncodeAlgo::kSimple8B_V2_s8] = &ConcreateTsCompressor<Simple8BIntV2<int8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s16] = &ConcreateTsCompressor<Simple8BIntV2<int16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s32] = &ConcreateTsCompressor<Simple8BIntV2<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s64] = &ConcreateTsCompressor<Simple8BIntV2<int64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u8] = &ConcreateTsCompressor<Simple8BIntV2<uint8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u16] = &ConcreateTsCompressor<Simple8BIntV2<uint16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u32] = &ConcreateTsCompressor<Simple8BIntV2<uint32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u64] = &ConcreateTsCompressor<Simple8BIntV2<uint64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kBitPacking] = &ConcreateTsCompressor<BitPacking>::GetInstance();
  // Float
  ts_encoders_[EncodeAlgo::kChimp_32] = &ConcreateTsCompressor<Chimp<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kChimp_64] = &ConcreateTsCompressor<Chimp<double>>::GetInstance();

  // construct general compression algorithms
  ts_compressors_[CompressAlgo::kSnappy] = &ConcreateGenCompressor<SnappyString>::GetInstance();
  ts_compressors_[CompressAlgo::kLz4] = &ConcreateGenCompressor<LZ4String>::GetInstance();
  ts_compressors_[CompressAlgo::kZstd] = &ConcreateGenCompressor<ZSTDString>::GetInstance();
  ts_compressors_[CompressAlgo::kZlib] = &ConcreateGenCompressor<ZLIBString>::GetInstance();
}
auto CompressorManager::GetCompressor(EncodeAlgo first, CompressAlgo second) const -> TwoLevelCompressor {
  const TsCompressorBase *first_comp = nullptr;
  const GenCompressorBase *second_comp = nullptr;
  {
    auto it = ts_encoders_.find(first);
    if (it != ts_encoders_.end()) first_comp = it->second;
  }
  {
    auto it = ts_compressors_.find(second);
    if (it != ts_compressors_.end()) second_comp = it->second;
  }
  return TwoLevelCompressor{first_comp, second_comp, first, second};
}

auto CompressorManager::GetAlgorithm(DATATYPE dtype,
                                     const AttributeInfo &attr_info) const
    -> std::tuple<EncodeAlgo, CompressAlgo> {
  EncodeAlgo encode_algo = EncodeAlgo::kPlain;
  CompressAlgo compress_algo = CompressAlgo::kPlain;
  switch (attr_info.encode_algo) {
    case roachpb::ENCODE_ALGO_UNSPECIFIED: {
      auto it = default_encode_algs_.find(dtype);
      if (it != default_encode_algs_.end()) {
        encode_algo = it->second;
      } else {
        encode_algo = EncodeAlgo::kPlain;
      }
      break;
    }
    case roachpb::ENCODE_ALGO_SIMPLE8B: {
      switch (dtype) {
        case DATATYPE::INT16:
          encode_algo = EncodeAlgo::kSimple8B_V2_s16;
          break;
        case DATATYPE::INT32:
          encode_algo = EncodeAlgo::kSimple8B_V2_s32;
          break;
        case DATATYPE::INT64:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_MICRO:
        case DATATYPE::TIMESTAMP64_NANO:
          encode_algo = EncodeAlgo::kSimple8B_V2_s64;
          break;
        default:
          LOG_ERROR("The data type %d does not match simple8b algorithm.", dtype);
          break;
        }
        break;
    }
    case roachpb::ENCODE_ALGO_BIT_PACKING:
      encode_algo = EncodeAlgo::kBitPacking;
      break;
    case roachpb::ENCODE_ALGO_CHIMP:
      switch (dtype) {
        case DATATYPE::FLOAT:
          encode_algo = EncodeAlgo::kChimp_32;
          break;
        case DATATYPE::DOUBLE:
          encode_algo = EncodeAlgo::kChimp_64;
          break;
        default:
          LOG_ERROR("The data type %d does not match chimp algorithm.", dtype);
          break;
      }
      break;
    default:
      encode_algo = EncodeAlgo::kPlain;
      break;
  }

  switch (attr_info.compress_algo) {
    case roachpb::COMPRESS_ALGO_UNSPECIFIED: {
      compress_algo = GetDefaultCompressAlgo(dtype);
      break;
    }
    case roachpb::COMPRESS_ALGO_SNAPPY:
      compress_algo = CompressAlgo::kSnappy;
      break;
    case roachpb::COMPRESS_ALGO_LZ4:
      compress_algo = CompressAlgo::kLz4;
      break;
    case roachpb::COMPRESS_ALGO_ZLIB:
      compress_algo = CompressAlgo::kZlib;
      break;
    case roachpb::COMPRESS_ALGO_ZSTD:
      compress_algo = CompressAlgo::kZstd;
      break;
    default:
      compress_algo = CompressAlgo::kPlain;
      break;
  }
  return {encode_algo, compress_algo};
}

auto CompressorManager::GetDefaultAlgorithm(DATATYPE dtype) const -> std::tuple<EncodeAlgo, CompressAlgo> {
  auto it = default_encode_algs_.find(dtype);
  if (it == default_encode_algs_.end()) {
    return {EncodeAlgo::kPlain, CompressAlgo::kPlain};
  }
  return {it->second, GetDefaultCompressAlgo(dtype)};
}

auto CompressorManager::GetDefaultCompressor(DATATYPE dtype) const -> TwoLevelCompressor {
  auto [encode_algo, compress_algo] = GetDefaultAlgorithm(dtype);
  return GetCompressor(encode_algo, compress_algo);
}

bool CompressorManager::CompressData(TSSlice input, const TsBitmapBase *bitmap, uint64_t count, TsBufferBuilder *output,
                                     EncodeAlgo encode_algo, CompressAlgo compress_algo, int level) const {
  switch (EngineOptions::compress_stage) {
    case 0:
      encode_algo = EncodeAlgo::kPlain;
      compress_algo = CompressAlgo::kPlain;
      break;
    case 1:
      compress_algo = CompressAlgo::kPlain;
      break;
    case 2:
      encode_algo = EncodeAlgo::kPlain;
      break;
    default:
      break;
  }

  static_assert(sizeof(encode_algo) == sizeof(uint16_t));
  static_assert(sizeof(compress_algo) == sizeof(uint16_t));
  auto level_it = compress_levels_.find(compress_algo);
  if (level_it == compress_levels_.end()) {
    LOG_ERROR("Invalid general compression algorithm: %d", static_cast<int>(compress_algo));
    return false;
  }
  auto compressor = GetCompressor(encode_algo, compress_algo);
  return compressor.Compress(input, bitmap, count, output, level_it->second[GetLevelIdx(level)]);
}

bool CompressorManager::CompressVarchar(TSSlice input, TsBufferBuilder *output, CompressAlgo alg, int level) const {
  switch (EngineOptions::compress_stage) {
    case 0:
    case 1:
      alg = CompressAlgo::kPlain;
      break;
    default:
      break;
  }
  static_assert(sizeof(alg) == sizeof(uint16_t));
  if (alg == CompressAlgo::kPlain) {
    PutFixed16(output, static_cast<uint16_t>(alg));
    output->append(input.data, input.len);
    return true;
  }
  TsBufferBuilder tmp;
  PutFixed16(&tmp, static_cast<uint16_t>(alg));
  auto it = ts_compressors_.find(alg);
  if (it == ts_compressors_.end()) {
    LOG_ERROR("Invalid general compression algorithm: %d", static_cast<int>(alg));
    return false;
  }
  bool ok = it->second->Compress(input, &tmp, compress_levels_.at(alg)[GetLevelIdx(level)]);
  if (!ok) {
    return false;
  }
  if (tmp.size() >= input.len) {
    PutFixed16(output, static_cast<uint16_t>(CompressAlgo::kPlain));
    output->append(input.data, input.len);
    return true;
  }
  output->append(tmp.AsSlice());
  return true;
}

bool CompressorManager::DoDecompressData(TsSliceGuard &&input, const TsBitmapBase *bitmap, uint64_t count,
                                         TsSliceGuard *out) const {
  auto algo = input.SubSlice(0, sizeof(EncodeAlgo) + sizeof(CompressAlgo));
  uint16_t v;
  GetFixed16(&algo, &v);
  EncodeAlgo first = static_cast<EncodeAlgo>(v);
  GetFixed16(&algo, &v);
  CompressAlgo second = static_cast<CompressAlgo>(v);
  if (first >= EncodeAlgo::TS_COMP_ALG_LAST || second >= CompressAlgo::GEN_COMP_ALG_LAST) {
    LOG_ERROR("Invalid algorithm id: first: %d, second: %d", static_cast<int>(first), static_cast<int>(second));
    return false;
  }
  auto compressor = GetCompressor(first, second);
  return compressor.Decompress(input.AsSlice(), bitmap, count, out);
}

bool CompressorManager::DoDecompressVarchar(CompressAlgo alg, TsSliceGuard &&input, TsSliceGuard *out) const {
  if (alg >= CompressAlgo::GEN_COMP_ALG_LAST) {
    return false;
  }
  auto it = ts_compressors_.find(alg);
  if (it == ts_compressors_.end()) {
    return false;
  }
  return it->second->Decompress(input.AsSlice(), out);
}

bool CompressorManager::CompressBitmap(TsBitmapBase *bitmap, TsBufferBuilder *output) const {
  if (bitmap->IsAllValid()) {
    output->push_back(static_cast<char>(BitmapType::kAllValid));
    return true;
  }

  if (bitmap->IsAllNull()) {
    output->push_back(static_cast<char>(BitmapType::kAllNull));
    return true;
  }

  if (bitmap->IsAllNone()) {
    output->push_back(static_cast<char>(BitmapType::kAllNone));
    return true;
  }

  output->push_back(static_cast<char>(BitmapType::kRaw));
  output->append(bitmap->GetStr());
  return true;
}

bool CompressorManager::DecompressBitmap(TSSlice input, std::unique_ptr<TsBitmapBase> *bitmap, uint64_t count,
                                         uint64_t *bytes_consumed) const {
  if (input.len < 1) {
    LOG_ERROR("Invalid input length = 0, too short");
    return false;
  }
  BitmapType alg = static_cast<BitmapType>(input.data[0]);
  RemovePrefix(&input, 1);
  *bytes_consumed = 1;
  switch (alg) {
    case BitmapType::kRaw: {
      auto n_bytes = TsBitmap::GetBitmapLen(count);
      if (input.len < n_bytes) {
        LOG_ERROR("Invalid bitmap length, too short. expected: %lu, actual: %lu", n_bytes, input.len);
        return false;
      }
      *bitmap = std::make_unique<TsBitmap>(TSSlice{input.data, n_bytes}, count);
      *bytes_consumed += n_bytes;
      break;
    }

    case BitmapType::kAllValid: {
      *bitmap = std::make_unique<TsUniformBitmap<kValid>>(count);
      break;
    }

    case BitmapType::kAllNull: {
      *bitmap = std::make_unique<TsUniformBitmap<kNull>>(count);
      break;
    }

    case BitmapType::kAllNone: {
      *bitmap = std::make_unique<TsUniformBitmap<kNone>>(count);
      break;
    }

    default: {
      LOG_ERROR("Invalid bitmap type: %d", static_cast<int>(alg));
      return false;
    }
  }
  return true;
}

}  // namespace kwdbts
