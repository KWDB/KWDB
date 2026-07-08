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
#include <cassert>
#include <cmath>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "compression/ts_codec_utils.h"
#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"

namespace kwdbts {

// ALP — Adaptive Lossless floating-Point compression.
//
// Core idea: for a batch of float/double values, find (e,f) such that
//   round(v * 10^e / 10^f) * 10^f / 10^e == v
// for as many values as possible. Survivors are stored as bit-packed
// integers (frame-of-reference); the rest are stored raw as exceptions.
//
// Wire format (byte stream):
//   [ef : 1 byte]    encoded (e,f)
//   [bw : 1 byte]    bit-width for packed integers
//   [min_v : varint] ZigZag-encoded frame-of-reference base
//   [packed data]    bit-packed inliers, 1024-value chunks
//   [exc_count : varint]
//   [exc_pos[]]      exception positions, exc_bytes each, little-endian
//   [exc_raw[]]      raw exception values, sizeof(T) each

namespace ALPCodec {
namespace details {

// "Sweet" constant for FastRound: pushes fractional part out of the mantissa.
// double: 2^51 + 2^52, float: 2^22 + 2^23.
template <class T>
constexpr T GetSweet() {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>, "GetMaxE only supports float and double");
  if constexpr (std::is_same_v<T, double>) {
    return (1ULL << 51) + (1ULL << 52);
  } else {
    return (1ULL << 22) + (1ULL << 23);
  }
}

// Fast round-to-nearest via "sweet" add/sub. volatile prevents the compiler
// from folding the round-trip into a direct truncation.
//
// Inputs outside IType range trigger UB per [conv.fpint] but produce sentinel
// values (INT_MIN / saturated) on x86/ARM; these are discarded by the
// caller's round-trip exception check. Kept branch-free for vectorization.
template <class T>
FloatSRep_t<T> FastRound(T n) {
  constexpr T sweet = GetSweet<T>();
  volatile T x = n + sweet;
  return static_cast<FloatSRep_t<T>>(x - sweet);
}

// Forward transform: v → round(v * 10^ef.e / 10^ef.f).  Writes into out[].
template <class T>
void EncodeALP(const Span<T> &data, EF ef, std::vector<FloatSRep_t<T>> &out) {
  const auto &Exp10 = ALPConst<T>::Exp10;
  const auto &iExp10 = ALPConst<T>::iExp10;
  for (int i = 0; i < data.size(); ++i) {
    T n = data[i] * Exp10[ef.e] * iExp10[ef.f];
    out[i] = FastRound(n);
  }
}

// Inverse transform: int → v * 10^ef.f / 10^ef.e.  Writes into out[].
template <class T>
void DecodeALP(const Span<FloatSRep_t<T>> &data, EF ef, std::vector<T> &out) {
  const auto &Exp10 = ALPConst<T>::Exp10;
  const auto &iExp10 = ALPConst<T>::iExp10;
  for (int i = 0; i < data.size(); ++i) {
    out[i] = data[i] * Exp10[ef.f] * iExp10[ef.e];
  }
}

// Pack (e,f) ∈ [0,21]×[0,e] → [0,252]. Split at e=10 to interleave both
// families without gaps.
inline uint8_t EncodeEF(EF v) {
  if (v.e > 10) {
    v.e = 21 - v.e;
    v.f = v.e + 1 + v.f;
  }
  return static_cast<uint8_t>(v.e * 23 + v.f);
}

inline EF DecodeEF(uint8_t v) {
  uint8_t e = v / 23;
  uint8_t f = v % 23;
  if (f > e) {
    auto ee = 21 - e;
    f = f - e - 1;
    e = ee;
  }
  assert(v <= 252);  // 253-255 decode to duplicate pairs
  return {static_cast<uint8_t>(e), static_cast<uint8_t>(f)};
}

}  // namespace details

std::array<std::vector<details::EF>, 80> ALPState::Stats(const std::array<BlockMeta, kWindowSize> &buffer) const {
  std::array<std::vector<details::EF>, 80> bpv_hist;
  for (size_t i = 0; i < kWindowSize; ++i) {
    for (size_t j = 0; j < buffer[i].bpv.size(); ++j) {
      if (buffer[i].bpv[j] >= 80) continue;
      bpv_hist[buffer[i].bpv[j]].push_back(buffer[i].efs[j]);
    }
  }
  return bpv_hist;
}

std::vector<details::EF> ALPState::GetCandidates(uint32_t max_e, int ncandidates) const {
  if (ncandidates < 0) {
    ncandidates = 100;
  }
  std::vector<details::EF> candidates;
  // Cold start: exhaustive enumeration
  if (window_idx_.load(std::memory_order_relaxed) == 0) {
    for (uint32_t e = 0; e <= max_e; ++e) {
      for (uint32_t f = 0; f <= e; ++f) {
        candidates.push_back({e, f});
      }
    }
    return candidates;
  }
  // Warm path: reuse historically successful pairs
  std::array<BlockMeta, kWindowSize> buffer;
  {
    std::shared_lock lk(mu_);
    buffer = circular_buffer_;
  }

  auto bpv_hist = Stats(buffer);

  struct EFHasher {
    uint64_t operator()(const details::EF &x) const { return (static_cast<uint64_t>(x.e) << 32) + x.f; }
  };
  std::unordered_set<details::EF, EFHasher> ef_set;
  bool stop = false;
  for (size_t i = 0; i < bpv_hist.size(); ++i) {
    for (size_t j = 0; j < bpv_hist[i].size(); ++j) {
      ef_set.insert(bpv_hist[i][j]);
      if (ef_set.size() >= ncandidates) {
        stop = true;
        break;
      }
    }
    if (stop) break;
  }

  for (auto &ef : ef_set) {
    candidates.push_back(ef);
  }
  return candidates;
}

template <class T>
int ALPState::Estimate(const details::Span<T> &raw, const details::Span<FloatSRep_t<T>> &encoded,
                       const details::Span<T> &decoded) {
  assert(raw.size() == decoded.size());
  assert(encoded.size() == decoded.size());
  assert(!raw.empty());
  uint64_t cnt = 0;

  FloatSRep_t<T> min_v = std::numeric_limits<FloatSRep_t<T>>::max();
  FloatSRep_t<T> max_v = std::numeric_limits<FloatSRep_t<T>>::min();
  for (size_t i = 0; i < raw.size(); ++i) {
    bool exc = raw[i] != decoded[i];
    cnt += exc;
    if (exc) continue;
    min_v = std::min(min_v, encoded[i]);
    max_v = std::max(max_v, encoded[i]);
  }

  using UDiff = FloatURep_t<T>;
  UDiff diff = 0;
  if (cnt != raw.size()) {
    diff = static_cast<UDiff>(max_v) - static_cast<UDiff>(min_v);
  }

  double total_bits = cnt * (sizeof(T) * 8 + 16) + (diff == 0 ? 0 : GetValidBits(diff)) * (raw.size() - cnt);
  return std::ceil(total_bits / raw.size());
}

template <class T>
details::EF ALPState::FindBestEF(const T *data, size_t size) {
  details::Span<T> probe(data, size);
  std::vector<T> probe_data;
  // Stratified sampling: pick 256 evenly-spaced values for large batches
  if (size > 512) {
    probe_data.resize(256);
    size_t n = size;
    for (int i = 0; i < 256; ++i) {
      size_t idx = (i * (n - 1)) / 255;
      probe_data[i] = data[idx];
    }
    probe = details::Span(probe_data);
  }

  auto max_e = details::ALPConst<T>::MAX_E;
  auto efs = GetCandidates(max_e);

  int score = std::numeric_limits<int>::max();  // bits per value, lower is better
  details::EF best_ef = {0, 0};                 // fallback if all pairs score >= 80
  BlockMeta meta;
  // Pre-allocate buffers once to avoid per-candidate heap allocations.
  std::vector<FloatSRep_t<T>> tmp(probe.size());
  std::vector<T> dec(probe.size());
  for (auto ef : efs) {
    details::EncodeALP(probe, ef, tmp);
    details::DecodeALP<T>(details::Span(tmp), ef, dec);
    auto v = Estimate<T>(probe, details::Span(tmp), details::Span(dec));
    if (v >= 80) continue;  // skip pairs where most values are exceptions
    if (v < score) {
      score = v;
      best_ef = ef;
    }
    meta.bpv.push_back(v);
    meta.efs.push_back(ef);
  }

  // Record this block's results for adaptive candidate selection
  {
    constexpr auto kWindowMask = kWindowSize - 1;
    std::unique_lock lk(mu_);
    auto idx = window_idx_.fetch_add(1, std::memory_order_relaxed);
    circular_buffer_[idx & kWindowMask] = std::move(meta);
  }
  return best_ef;
}

// Encode — compress data[0..size) into out.
//
//   1. Find best (e,f) via adaptive search.
//   2. Forward/round-trip to classify inliers vs exceptions.
//   3. Frame-of-reference: bit_width(max - min) → bit-width.
//   4. Bit-pack inliers in 1024-value chunks.
//   5. Append exception positions and raw values.
template <class T>
void Encode(const T *data, size_t size, ALPState *state, TsBufferBuilder *out) {
  using Utype = std::make_unsigned_t<FloatSRep_t<T>>;
  assert(state != nullptr && size > 0);

  details::Span span(data, size);
  auto ef = state->FindBestEF(data, size);
  std::vector<std::make_unsigned_t<FloatSRep_t<T>>> uenc_data;
  std::vector<size_t> exc_pos;
  FloatSRep_t<T> min_v = std::numeric_limits<FloatSRep_t<T>>::max();
  FloatSRep_t<T> max_v = std::numeric_limits<FloatSRep_t<T>>::min();
  {
    auto enc_data = std::vector<FloatSRep_t<T>>(size);
    details::EncodeALP(span, ef, enc_data);
    auto v = std::vector<T>(size);
    details::DecodeALP<T>(details::Span(enc_data), ef, v);

    // Inlier ↔ round-trip exact
    for (size_t i = 0; i < size; ++i) {
      if (data[i] != v[i]) {
        exc_pos.push_back(i);
        continue;
      }
      min_v = std::min(min_v, enc_data[i]);
      max_v = std::max(max_v, enc_data[i]);
    }
    // All-exception guard: diff=0 → bitwidth=0, everything stored raw.
    if (exc_pos.size() == size) {
      min_v = 0;
      max_v = 0;
    }

    // Build unsigned deltas: delta[i] = enc[i] - min_v (0 for exceptions).
    uenc_data.resize(enc_data.size());
    for (size_t i = 0; i < size; ++i) {
      if (data[i] != v[i]) {
        uenc_data[i] = 0;
      } else {
        uenc_data[i] = static_cast<Utype>(enc_data[i]) - static_cast<Utype>(min_v);
      }
    }
  }

  Utype diff = static_cast<Utype>(max_v) - static_cast<Utype>(min_v);
  int bitwidth = diff == 0 ? 0 : GetValidBits(diff);
  int bits_per_pos = static_cast<int>(std::ceil(std::log2(size)));
  int exc_bytes = std::max(1, (bits_per_pos + 7) / 8);

  // ---- Wire format ----
  // 1. (e,f) + bit-width
  {
    auto v = details::EncodeEF(ef);
    out->push_back(v);
    out->push_back(bitwidth);
  }

  // 2. Frame-of-reference base (ZigZag varint)
  PutVarint64(out, EncodeZigZag(min_v));

  // 3. Bit-packed inliers — two layers per 1024-value chunk, auto-vectorized.
  //   Full bytes: emit n_full_bytes low bytes, then shift.
  //   Remainder: transposed bit-columns (bit i of value j → byte j/8, bit j%8).

  auto n_full_bytes = bitwidth / 8;
  auto n_remainder = bitwidth % 8;

  constexpr size_t chunk_size = 1024;

  auto encode_batch = [&](size_t idx_start, int count) {
    // Full bytes: emit low byte, shift right
    for (int j = 0; j < count; ++j) {
      for (int i = 0; i < n_full_bytes; ++i) {
        auto b = uenc_data[idx_start + j] & 0xFF;
        out->push_back(b);
        uenc_data[idx_start + j] >>= 8;
      }
    }
    // Remainder: transposed bit-columns
    for (int i = 0; i < n_remainder; ++i) {
      constexpr size_t buffer_size = chunk_size / 8;
      std::array<uint8_t, buffer_size> buffer{};
      for (int j = 0; j < count; ++j) {
        auto buf_idx = j / 8;
        auto vshift = j % 8;
        auto v = uenc_data[idx_start + j] & 0x1;
        v <<= vshift;
        buffer[buf_idx] += v;
        uenc_data[idx_start + j] >>= 1;
      }
      out->append(reinterpret_cast<char *>(buffer.data()), (count + 7) / 8);
    }
  };

  auto n_chunk = size / chunk_size;
  auto n_left = size % chunk_size;

  for (size_t ichunk = 0; ichunk < n_chunk; ++ichunk) {
    encode_batch(ichunk * chunk_size, chunk_size);
  }
  if (n_left > 0) {
    encode_batch(n_chunk * chunk_size, n_left);
  }

  // 4. Exception positions (little-endian, exc_bytes each) + raw values
  {
    PutVarint64(out, static_cast<uint64_t>(exc_pos.size()));
    if (!exc_pos.empty()) {
      for (size_t pos : exc_pos) {
        uint64_t v = pos;
        for (int b = 0; b < exc_bytes; ++b) {
          out->push_back(static_cast<char>(v & 0xFF));
          v >>= 8;
        }
      }
      for (size_t pos : exc_pos) {
        out->append(reinterpret_cast<const char *>(&data[pos]), sizeof(T));
      }
    }
  }
}

// Decode — decompress in → out.  Mirror of Encode layout.
//   1. Header: (e,f) + bit-width.
//   2. Frame-of-reference base (ZigZag varint).
//   3. Bit-unpack inliers (1024-value chunks).
//   4. Add min_v → signed integers.
//   5. DecodeALP: integers → floating-point.
//   6. Patch exception positions with raw values.
template <class T>
bool Decode(const TsSliceGuard &in, size_t size, TsBufferBuilder *out) {
  using Utype = std::make_unsigned_t<FloatSRep_t<T>>;
  const char *ptr = in.data();
  const char *limit = in.data() + in.size();

  if (size == 0) return true;
  if (ptr + 2 > limit) return false;  // need ≥2 header bytes

  // 1. (e,f) + bit-width
  details::EF ef = details::DecodeEF(*ptr++);
  int bitwidth = *ptr++;
  int bits_per_pos = static_cast<int>(std::ceil(std::log2(size)));
  int exc_bytes = std::max(1, (bits_per_pos + 7) / 8);

  // 2. Frame-of-reference base (ZigZag varint)
  int64_t min_v;
  {
    uint64_t v;
    ptr = DecodeVarint64(ptr, limit, &v);
    if (ptr == nullptr) return false;
    min_v = DecodeZigZag(v);
  }
  auto n_full_bytes = bitwidth / 8;
  auto n_remainder = bitwidth % 8;

  constexpr size_t chunk_size = 1024;

  const uint8_t *uptr = reinterpret_cast<const uint8_t *>(ptr);

  // 3. Bit-unpack inliers (reverse of encode transposed layout)

  std::vector<FloatSRep_t<T>> decoded;
  decoded.reserve(size);
  {
    std::vector<Utype> udecoded(size);
    auto decode_batch = [&](size_t idx_start, int count) {
      // Full bytes: shift each into position
      for (int i = 0; i < count; ++i) {
        for (int j = 0; j < n_full_bytes; ++j) {
          udecoded[idx_start + i] += static_cast<Utype>(uptr[j]) << (j * 8);
        }
        uptr += n_full_bytes;
      }

      // Remainder: transposed bit-columns
      for (int j = 0; j < n_remainder; ++j) {
        for (int i = 0; i < count; ++i) {
          auto buf_idx = i / 8;
          auto vshift = i % 8;
          udecoded[idx_start + i] += static_cast<Utype>((uptr[buf_idx] >> vshift) & 0b1) << (j + n_full_bytes * 8);
        }
        uptr += (count + 7) / 8;
      }
    };

    auto n_chunk = size / chunk_size;
    auto n_left = size % chunk_size;

    for (size_t ichunk = 0; ichunk < n_chunk; ++ichunk) {
      decode_batch(ichunk * chunk_size, chunk_size);
    }
    if (n_left > 0) {
      decode_batch(n_chunk * chunk_size, n_left);
    }

    // 4. unsigned_delta + min_v → signed
    for (int i = 0; i < udecoded.size(); ++i) {
      decoded.push_back(udecoded[i] + min_v);
    }
  }

  // 5. Integer → float, then patch exceptions
  const char *cptr = reinterpret_cast<const char *>(uptr);
  {
    uint64_t exc_count;
    cptr = DecodeVarint64(cptr, limit, &exc_count);
    if (cptr == nullptr) return false;

    auto raw_float = std::vector<T>(decoded.size());
    details::DecodeALP<T>(details::Span{decoded}, ef, raw_float);

    if (exc_count > 0) {
      // Exception positions (little-endian, exc_bytes each)
      std::vector<size_t> exc_pos(exc_count);
      for (uint64_t i = 0; i < exc_count; ++i) {
        if (cptr + exc_bytes > limit) return false;
        uint64_t pos = 0;
        for (int b = 0; b < exc_bytes; ++b) {
          pos += static_cast<uint64_t>(static_cast<uint8_t>(cptr[b])) << (b * 8);
        }
        cptr += exc_bytes;
        if (pos >= size) return false;
        exc_pos[i] = static_cast<size_t>(pos);
      }
      // Patch raw exception values
      for (uint64_t i = 0; i < exc_count; ++i) {
        if (cptr + sizeof(T) > limit) return false;
        T val;
        std::memcpy(&val, cptr, sizeof(T));
        cptr += sizeof(T);
        raw_float[exc_pos[i]] = val;
      }
    }

    out->append(reinterpret_cast<char *>(raw_float.data()), raw_float.size() * sizeof(T));
  }

  assert(cptr == limit);
  return true;
}

}  // namespace ALPCodec

template <class T>
bool ALP<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  if (count == 0) return true;
  ALPCodec::ALPState *p_state = nullptr;
  if (cfg.extra_cfg.has_value() && cfg.extra_cfg->alp_state != nullptr) {
    p_state = cfg.extra_cfg->alp_state.get();
  }
  const T *float_data = reinterpret_cast<const T *>(data.data);
  if (p_state == nullptr) {
    ALPCodec::ALPState local;
    ALPCodec::Encode(float_data, count, &local, out);
  } else {
    ALPCodec::Encode(float_data, count, p_state, out);
  }
  return true;
}

template <class T>
bool ALP<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (count == 0) return true;
  TsBufferBuilder out_builder;
  bool ok = ALPCodec::Decode<T>(TsSliceGuard{data}, count, &out_builder);
  *out = out_builder.GetBuffer();
  return ok;
}

template class ALP<float>;
template class ALP<double>;

}  // namespace kwdbts
