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

#include <cstdint>
#include <type_traits>

#include "compression/ts_codec_utils.h"
#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"

namespace kwdbts {

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
  if (count == 0) return true;
  for (uint64_t i = 0; i < count;) {
    auto data_i = EncodeZigZagIfNeeded(data[i]);
    int valid_nbits = GetValidBits(data_i);
    if (valid_nbits > 60) return false;
    uint8_t selector = NBITS2SELECTOR[valid_nbits];

    auto header = data_i;
    bool all_same = true;
    uint64_t j = i + 1;
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
  if (count == 0) return true;
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
  for (uint64_t i = 2; i < count;) {
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

    uint64_t run_length = 1;
    uint64_t j = i + 1;
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
bool Simple8BInt<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out,
                              const TsCompressionConfig &cfg) const {
  assert(data.len == sizeof(T) * count);
  const T *p_data = reinterpret_cast<const T *>(data.data);
  return _simple8b_detail::CompressImplGreedy<T>(p_data, count, out);
}

template <class T>
bool Simple8BInt<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  return _simple8b_detail::Decompress<T>(data, count, out);
}

template <class T>
bool Simple8BIntV2<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out,
                                const TsCompressionConfig &cfg) const {
  assert(data.len == sizeof(T) * count);
  const T *p_data = reinterpret_cast<const T *>(data.data);
  return _simple8b_detail::V2CompressImplGreedy<T>(p_data, count, out);
}

template <class T>
bool Simple8BIntV2<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  return _simple8b_detail::V2Decompress<T>(data, count, out);
}

// export
template class Simple8BInt<uint8_t>;
template class Simple8BInt<uint16_t>;
template class Simple8BInt<uint32_t>;
template class Simple8BInt<uint64_t>;
template class Simple8BIntV2<uint8_t>;
template class Simple8BIntV2<uint16_t>;
template class Simple8BIntV2<uint32_t>;
template class Simple8BIntV2<uint64_t>;

template class Simple8BInt<int8_t>;
template class Simple8BInt<int16_t>;
template class Simple8BInt<int32_t>;
template class Simple8BInt<int64_t>;
template class Simple8BIntV2<int8_t>;
template class Simple8BIntV2<int16_t>;
template class Simple8BIntV2<int32_t>;
template class Simple8BIntV2<int64_t>;
}  // namespace kwdbts
