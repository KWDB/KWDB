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

#include <cassert>
#include <type_traits>

#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"

namespace kwdbts {

// BSS — Byte Stream Split for floating-point compression.
//
// Core idea: transpose a batch of float/double values at byte granularity.
// IEEE 754 values that are numerically close share the same high bytes
// (sign + exponent + high mantissa). After transposition, those bytes are
// grouped together, making downstream general-purpose compressors (LZ4,
// Zstd, etc.) much more effective.
//
// Wire format (byte stream):
//   [byte_stream_0]     N bytes: byte 0 (LSB) of each value
//   [byte_stream_1]     N bytes: byte 1 of each value
//   ...
//   [byte_stream_{K-1}] N bytes: byte K-1 (MSB) of each value
//                        K = sizeof(T): float=4, double=8
//
// Total encoded size = N × sizeof(T), identical to the raw input size.
// BSS is a pure lossless transform — zero header overhead, no compression
// on its own.

namespace BSSCodec {

// Encode — transpose data[0..size) at byte granularity and append to out.
//
//   for each byte position b (0 .. K-1):
//       for each value i (0 .. size-1):
//           emit byte[b] of data[i]
template <class T>
void Encode(const T *data, size_t size, TsBufferBuilder *out) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>, "BSS codec only supports float and double");
  if (size == 0) return;

  constexpr size_t kBytes = sizeof(T);
  const char *src = reinterpret_cast<const char *>(data);

  // Reserve output space upfront — one pass, no realloc.
  size_t base = out->size();
  out->resize(base + size * kBytes);
  char *dst = out->data() + base;

  for (size_t i = 0; i < size; ++i) {
    for (size_t b = 0; b < kBytes; ++b) {
      dst[b * size + i] = src[i * kBytes + b];
    }
  }
}

// Decode — inverse transpose, read from in and append decoded values to out.
//
// Returns false if the input slice is too short (truncated data).
template <class T>
bool Decode(const TsSliceGuard &in, size_t size, TsBufferBuilder *out) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>, "BSS codec only supports float and double");
  if (size == 0) return true;

  constexpr size_t kBytes = sizeof(T);
  if (in.size() < size * kBytes) return false;

  const char *src = in.data();

  size_t base = out->size();
  out->resize(base + size * kBytes);
  char *dst = out->data() + base;

  for (size_t i = 0; i < size; ++i) {
    for (size_t b = 0; b < kBytes; ++b) {
      dst[i * kBytes + b] = src[b * size + i];
    }
  }
  return true;
}

}  // namespace BSSCodec

template <class T>
bool BSS<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  if (count == 0) return true;
  const T *float_data = reinterpret_cast<const T *>(data.data);
  BSSCodec::Encode(float_data, count, out);
  return true;
}

template <class T>
bool BSS<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  TsSliceGuard input{data};
  TsBufferBuilder builder;
  bool ok = BSSCodec::Decode<T>(input, count, &builder);
  *out = builder.GetBuffer();
  return ok;
}

template class BSS<float>;
template class BSS<double>;

}  // namespace kwdbts
