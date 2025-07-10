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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "data_type.h"
#include "ee_field_common.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_coding.h"
#include "ts_compressor.h"

namespace kwdbts {

// Gorilla compression; a.k.a delta of delta
// Ref: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// NOTE: We should do some extra optimization for this algorithm, because the
//       timestamp is recorded as nanosecond.
bool GorillaInt::Compress(const TSSlice &data, uint64_t count, std::string *out) const {
  static constexpr int dsize = sizeof(int64_t);
  if (count < 2) {
    return false;
  }
  assert(data.len == count * dsize);
  out->reserve(dsize * count);
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

bool GorillaInt::Decompress(const TSSlice &data, uint64_t count, std::string *out) const {
  static constexpr int dsize = sizeof(int64_t);
  if (count == 0) {
    out->clear();
    return true;
  }
  out->reserve(8 * count);
  TsBitReader reader(std::string_view{data.data, data.len});

  uint64_t v;
  bool ok = reader.ReadBits(64, &v);
  if (!ok) {
    return false;
  }
  int64_t current_ts = v;
  out->append(reinterpret_cast<char *>(&current_ts), dsize);
  if (count == 1) {
    return true;
  }
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

template <class T>
static inline bool CheckedSub(const T a, const T b, T *out) {
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>);
  if constexpr (std::is_same_v<T, int64_t>) {
    return __builtin_ssubl_overflow(a, b, out);
  } else {
    return __builtin_ssub_overflow(a, b, out);
  }
  assert(false);
  return false;
}

template <class T>
static inline void TypedPutVarint(std::string *dst, T v) {
  static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>);
  if constexpr (std::is_same_v<T, uint64_t>) {
    return PutVarint64(dst, v);
  } else {
    return PutVarint32(dst, v);
  }
}

template <class T>
static inline void TypedPutFixed(std::string *dst, T v) {
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
bool GorillaIntV2<T>::Compress(const TSSlice &data, uint64_t count, std::string *out) const {
  if (count <= 2) {
    return false;
  }
  assert(data.len == count * stride);
  out->reserve(stride * count);
  T *ts_data = reinterpret_cast<T *>(data.data);

  // 1. record the first timestamp;
  TypedPutVarint(out, EncodeZigZag(ts_data[0]));
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
bool GorillaIntV2<T>::Decompress(const TSSlice &data, uint64_t count, std::string *out) const {
  if (count <= 2) {
    return false;
  }
  out->resize(stride * count);
  T *outdata = reinterpret_cast<T *>(out->data());
  using utype = std::make_unsigned_t<T>;
  utype v;
  const char *limit = data.data + data.len;
  const char *ptr = TypedDecodeVarint(data.data, limit, &v);
  if (ptr == nullptr) {
    return false;
  }
  T ts = DecodeZigZag(v);
  outdata[0] = ts;

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
  return true;
}

static int leading_mapping[] = {0, 8, 12, 16, 18, 20, 22, 24};
template <class T>
bool Chimp<T>::Compress(const TSSlice &data, uint64_t count, std::string *out) const {
  assert(data.len == sizeof(T) * count);
  auto sz = sizeof(T) * 8;
  out->clear();
  if (count <= 1) {
    return false;
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
bool Chimp<T>::Decompress(const TSSlice &data, uint64_t count, std::string *out) const {
  auto sz = sizeof(T) * 8;
  out->clear();
  if (count == 0) {
    return true;
  }
  TsBitReader reader(std::string_view{data.data, data.len});
  uint64_t v;
  bool ok = reader.ReadBits(sz, &v);
  if (!ok) {
    return false;
  }
  if constexpr (std::is_same_v<T, double>) {
    PutFixed64(out, v);
  } else {
    PutFixed32(out, v);
  }
  using utype = std::conditional_t<std::is_same_v<T, double>, uint64_t, uint32_t>;
  utype prev = v, prev_lead_idx = 0;
  for (int i = 1; i < count; ++i) {
    bool ok = reader.ReadBits(2, &v);
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
      PutFixed64(out, current);
    } else {
      PutFixed32(out, current);
    }
    prev = current;
  }
  return true;
}
// export
template class Chimp<double>;
template class Chimp<float>;

namespace __simple8b_detail {
alignas(64) static constexpr uint32_t ITEMWIDTH[16] = {0, 0, 1,  2,  3,  4,  5,  6,
                                                       7, 8, 10, 12, 15, 20, 30, 60};
/* The following array is generate by python code:
>>> width = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60]
>>> print(list(map(lambda x : bisect.bisect_left(width, x), range(64))))
*/

alignas(64) static constexpr uint8_t NBITS2SELECTOR[64] = {
    0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 16, 16, 16};

alignas(64) static constexpr int32_t GROUPSIZE[16] = {240, 120, 60, 30, 20, 15, 12, 10,
                                                      8,   7,   6,  5,  4,  3,  2,  1};

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
static bool CompressImplGreedy(const T *data, uint64_t count, std::string *out) {
  static_assert(std::is_integral_v<T>);
  out->clear();
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
      out->append(reinterpret_cast<char *>(&batch), 8);
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
      out->append(reinterpret_cast<char *>(&batch), 8);
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
bool Decompress(const TSSlice &data, uint64_t count, std::string *out) {
  if (data.len % 8 != 0) {
    return false;
  }
  out->resize(sizeof(T) * count);
  T *outdata = reinterpret_cast<T *>(out->data());
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
  return out->size() / sizeof(T) == count;
}

};  // namespace __simple8b_detail

template <class T>
bool Simple8BInt<T>::Compress(const TSSlice &data, uint64_t count, std::string *out) const {
  assert(data.len == sizeof(T) * count);
  const T *p_data = reinterpret_cast<const T *>(data.data);
  return __simple8b_detail::CompressImplGreedy<T>(p_data, count, out);
}

template <class T>
bool Simple8BInt<T>::Decompress(const TSSlice &data, uint64_t count, std::string *out) const {
  return __simple8b_detail::Decompress<T>(data, count, out);
}

bool CompressorManager::TwoLevelCompressor::Compress(const TSSlice &raw, const TsBitmap *bitmap,
                                                     uint32_t count, std::string *out) const {
  if (IsPlain()) return false;
  std::string buf;
  TSSlice data;
  bool ok = true;
  if (first_ == nullptr) {
    data = raw;
  } else {
    ok = first_->Compress(raw, bitmap, count, &buf);
    data = {buf.data(), buf.size()};
  }
  if (!ok) {
    return false;
  }
  if (second_ == nullptr) {
    out->swap(buf);
    return true;
  }
  return second_->Compress(data, out);
}
bool CompressorManager::TwoLevelCompressor::Decompress(const TSSlice &raw, const TsBitmap *bitmap,
                                                       uint32_t count, std::string *out) const {
  if (IsPlain()) return false;
  out->clear();
  std::string buf;
  TSSlice data;
  bool ok = true;
  if (second_ == nullptr) {
    data = raw;
  } else {
    ok = second_->Decompress(raw, &buf);
    data = {buf.data(), buf.size()};
  }
  if (!ok) {
    return false;
  }
  if (first_ == nullptr) {
    out->swap(buf);
    return true;
  }
  return first_->Decompress(data, bitmap, count, out);
}

std::tuple<TsCompAlg, GenCompAlg> CompressorManager::TwoLevelCompressor::GetAlgorithms() const {
  return {first_algo_, second_algo_};
}

CompressorManager::CompressorManager() {
  const char *twolevelenv = getenv("KW_TWOLEVEL_COMPRESS");
  bool twolevel = false;
  if (twolevelenv) {
    std::string config;
    std::transform(twolevelenv, twolevelenv + std::strlen(twolevelenv), std::back_inserter(config),
                   [](char c) { return std::tolower(c); });
    if (config == "on" || config == "true") {
      twolevel = true;
    }
  }
  GenCompAlg second = twolevel ? GenCompAlg::kSnappy : GenCompAlg::kPlain;
  const std::vector<DATATYPE> timestamp_type{
      DATATYPE::TIMESTAMP64,     DATATYPE::TIMESTAMP64_MICRO,     DATATYPE::TIMESTAMP64_NANO,
      DATATYPE::TIMESTAMP64_LSN, DATATYPE::TIMESTAMP64_LSN_MICRO, DATATYPE::TIMESTAMP64_LSN_NANO};
  for (auto i : timestamp_type) {
    default_algs_[i] = {TsCompAlg::kGorilla_64, GenCompAlg::kPlain};
  }

  ts_comp_[TsCompAlg::kGorilla_32] = &ConcreateTsCompressor<GorillaIntV2<int32_t>>::GetInstance();
  ts_comp_[TsCompAlg::kGorilla_64] = &ConcreateTsCompressor<GorillaIntV2<int64_t>>::GetInstance();

  ts_comp_[TsCompAlg::kSimple8B_s8] = &ConcreateTsCompressor<Simple8BInt<int8_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_s16] = &ConcreateTsCompressor<Simple8BInt<int16_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_s32] = &ConcreateTsCompressor<Simple8BInt<int32_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_s64] = &ConcreateTsCompressor<Simple8BInt<int64_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_u8] = &ConcreateTsCompressor<Simple8BInt<uint8_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_u16] = &ConcreateTsCompressor<Simple8BInt<uint16_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_u32] = &ConcreateTsCompressor<Simple8BInt<uint32_t>>::GetInstance();
  ts_comp_[TsCompAlg::kSimple8B_u64] = &ConcreateTsCompressor<Simple8BInt<uint64_t>>::GetInstance();

  default_algs_[DATATYPE::INT16] = {TsCompAlg::kSimple8B_s16, second};
  default_algs_[DATATYPE::INT32] = {TsCompAlg::kSimple8B_s32, second};
  default_algs_[DATATYPE::INT64] = {TsCompAlg::kSimple8B_s64, second};

  // Float
  ts_comp_[TsCompAlg::kChimp_32] = &ConcreateTsCompressor<Chimp<float>>::GetInstance();
  ts_comp_[TsCompAlg::kChimp_64] = &ConcreateTsCompressor<Chimp<double>>::GetInstance();
  default_algs_[DATATYPE::FLOAT] = {TsCompAlg::kChimp_32, second};
  default_algs_[DATATYPE::DOUBLE] = {TsCompAlg::kChimp_64, second};

  general_compressor_[GenCompAlg::kSnappy] = &ConcreateGenCompressor<SnappyString>::GetInstance();

  // char string
  default_algs_[DATATYPE::BYTE] = {TsCompAlg::kPlain, GenCompAlg::kSnappy};
  default_algs_[DATATYPE::CHAR] = {TsCompAlg::kPlain, GenCompAlg::kSnappy};
  default_algs_[DATATYPE::BINARY] = {TsCompAlg::kPlain, GenCompAlg::kSnappy};

  // varchar varstring
  // default_algs_[DATATYPE::VARSTRING] = {TsCompAlg::kGorilla_32, GenCompAlg::kPlain};
  // default_algs_[DATATYPE::VARBINARY] = {TsCompAlg::kGorilla_32, GenCompAlg::kPlain};
}
auto CompressorManager::GetCompressor(TsCompAlg first, GenCompAlg second) const
    -> TwoLevelCompressor {
  const TsCompressorBase *first_comp = nullptr;
  const GenCompressorBase *second_comp = nullptr;
  {
    auto it = ts_comp_.find(first);
    if (it != ts_comp_.end()) first_comp = it->second;
  }
  {
    auto it = general_compressor_.find(second);
    if (it != general_compressor_.end()) second_comp = it->second;
  }
  return TwoLevelCompressor{first_comp, second_comp, first, second};
}

auto CompressorManager::GetDefaultAlgorithm(DATATYPE dtype) const
    -> std::tuple<TsCompAlg, GenCompAlg> {
  auto it = default_algs_.find(dtype);
  if (it == default_algs_.end()) {
    return {TsCompAlg::kPlain, GenCompAlg::kPlain};
  }
  return it->second;
}

auto CompressorManager::GetDefaultCompressor(DATATYPE dtype) const -> TwoLevelCompressor {
  auto [first, second] = GetDefaultAlgorithm(dtype);
  return GetCompressor(first, second);
}

bool CompressorManager::CompressData(TSSlice input, const TsBitmap *bitmap, uint64_t count,
                                     std::string *output, TsCompAlg first,
                                     GenCompAlg second) const {
  static_assert(sizeof(first) == sizeof(uint16_t));
  static_assert(sizeof(second) == sizeof(uint16_t));
  auto compressor = GetCompressor(first, second);
  std::string tmp;
  bool ok = compressor.Compress(input, bitmap, count, &tmp);
  if (!ok) {
    first = TsCompAlg::kPlain;
    second = GenCompAlg::kPlain;
  }
  PutFixed16(output, static_cast<uint16_t>(first));
  PutFixed16(output, static_cast<uint16_t>(second));
  if (ok) {
    output->append(tmp);
  } else {
    output->append(input.data, input.len);
  }
  return true;
}

bool CompressorManager::CompressVarchar(TSSlice input, std::string *output,
                                        GenCompAlg alg) const {
  assert(sizeof(alg) == sizeof(uint16_t));
  output->clear();
  PutFixed16(output, static_cast<uint16_t>(alg));
  auto it = general_compressor_.find(alg);
  if (it == general_compressor_.end()) {
    // no compression
    output->append(input.data, input.len);
    return true;
  }
  std::string tmp;
  bool ok = it->second->Compress(input, &tmp);
  if (!ok) {
    return false;
  }
  output->append(tmp);
  return true;
}

bool CompressorManager::DecompressData(TSSlice input, const TsBitmap *bitmap, uint64_t count,
                                       std::string *output) const {
  if (input.len < 4) {
    LOG_ERROR("Invalid input length, too short");
    return false;
  }
  uint16_t v;
  GetFixed16(&input, &v);
  TsCompAlg first = static_cast<TsCompAlg>(v);
  GetFixed16(&input, &v);
  GenCompAlg second = static_cast<GenCompAlg>(v);
  if (first >= TsCompAlg::TS_COMP_ALG_LAST || second >= GenCompAlg::GEN_COMP_ALG_LAST) {
    LOG_ERROR("Invalid algorithm id");
    return false;
  }
  if (first == TsCompAlg::kPlain && second == GenCompAlg::kPlain) {
    output->assign(input.data, input.len);
    return true;
  }
  auto compressor = GetCompressor(first, second);
  return compressor.Decompress(input, bitmap, count, output);
}

bool CompressorManager::DecompressVarchar(TSSlice input, std::string *output) const {
  if (input.len < 2) {
    return false;
  }
  uint16_t v;
  GetFixed16(&input, &v);
  GenCompAlg alg = static_cast<GenCompAlg>(v);
  if (alg >= GenCompAlg::GEN_COMP_ALG_LAST) {
    return false;
  }

  if (alg == GenCompAlg::kPlain) {
    output->assign(input.data, input.len);
    return true;
  }

  auto it = general_compressor_.find(alg);
  if (it == general_compressor_.end()) {
    return false;
  }
  std::string tmp;
  output->clear();
  return it->second->Decompress(input, output);
}

}  // namespace kwdbts
