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
#include <cstdint>
#include <type_traits>

#include "compression/ts_encoder_defs.h"
#include "compression/ts_floatrep_helper.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"

namespace kwdbts {

inline static std::array<double, 21> map10iP_d{1.0,    1.0e1,  1.0e2,  1.0e3,  1.0e4,  1.0e5,  1.0e6,
                                               1.0e7,  1.0e8,  1.0e9,  1.0e10, 1.0e11, 1.0e12, 1.0e13,
                                               1.0e14, 1.0e15, 1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20};
inline static std::array<double, 21> map10iN_d{1.0,     1.0e-1,  1.0e-2,  1.0e-3,  1.0e-4,  1.0e-5,  1.0e-6,
                                               1.0e-7,  1.0e-8,  1.0e-9,  1.0e-10, 1.0e-11, 1.0e-12, 1.0e-13,
                                               1.0e-14, 1.0e-15, 1.0e-16, 1.0e-17, 1.0e-18, 1.0e-19, 1.0e-20};

inline static std::array<float, 21> map10iP_f{1.0f,    1.0e1f,  1.0e2f,  1.0e3f,  1.0e4f,  1.0e5f,  1.0e6f,
                                              1.0e7f,  1.0e8f,  1.0e9f,  1.0e10f, 1.0e11f, 1.0e12f, 1.0e13f,
                                              1.0e14f, 1.0e15f, 1.0e16f, 1.0e17f, 1.0e18f, 1.0e19f, 1.0e20f};
inline static std::array<float, 21> map10iN_f{1.0f,     1.0e-1f,  1.0e-2f,  1.0e-3f,  1.0e-4f,  1.0e-5f,  1.0e-6f,
                                              1.0e-7f,  1.0e-8f,  1.0e-9f,  1.0e-10f, 1.0e-11f, 1.0e-12f, 1.0e-13f,
                                              1.0e-14f, 1.0e-15f, 1.0e-16f, 1.0e-17f, 1.0e-18f, 1.0e-19f, 1.0e-20f};

template <class T>
T GetExp10(int i) {
  assert(i >= 0);
  if constexpr (std::is_same_v<T, float>) {
    if (i >= static_cast<int>(map10iP_f.size())) return std::pow(10.0f, i);
    return map10iP_f[i];
  } else {
    if (i >= static_cast<int>(map10iP_d.size())) return std::pow(10.0, i);
    return map10iP_d[i];
  }
}

template <class T>
T GetIExp10(int i) {
  assert(i >= 0);
  if constexpr (std::is_same_v<T, float>) {
    if (i >= static_cast<int>(map10iN_f.size())) return std::pow(10.0f, -i);
    return map10iN_f[i];
  } else {
    if (i >= static_cast<int>(map10iN_d.size())) return std::pow(10.0, -i);
    return map10iN_d[i];
  }
}

template <class T>
class ElfIEEEFloat : public IEEEFloat<T> {
  using IEEEFloat<T>::IEEEFloat;

 public:
  void RoundUp(int alpha) {
    auto s = this->GetSignBit();
    T scale = GetExp10<T>(alpha);
    T val = this->ToNative();
    val = std::ceil(val * scale) / scale;
    this->rep_ = IEEEFloat<T>(val).GetUValue();
    this->SetSignBit(s);
  }
};

namespace ELFCodec {

namespace detail {

// kMaxSignificantDigits: values with >= 16 significant decimal digits
// cannot have any mantissa bits erased without risk of precision loss.
// 17 is used as a sentinel in GetBeta to signal overflow/underflow.
constexpr int kMaxSignificantDigits = 16;
constexpr int kBetaSentinel = kMaxSignificantDigits + 1;  // 17

inline static double LOG_2_10 = std::log2(10);
inline int getFAlpha(int alpha) {
  assert(alpha >= 0);
  return static_cast<int>(std::ceil(alpha * LOG_2_10));
}

template <class T>
std::pair<int, bool> GetSP(T v) {
  double log10v = std::log10(v);
  int r = std::floor(log10v);
  return {r, r == log10v};
}

// ---- bit helpers ----

template <class T>
inline int clz(T v) {
  static_assert(std::is_unsigned_v<T>, "clz requires an unsigned type");
  static_assert(sizeof(T) <= 8, "clz supports up to 64-bit types");
  constexpr int nbits = sizeof(T) * 8;
  if (v == 0) return nbits;  // early return avoids UB on __builtin_clzll(0)
  auto lz = __builtin_clzll(v);
  return lz - (64 - nbits);
}
template <class T>
inline int ctz(T v) {
  static_assert(std::is_unsigned_v<T>, "ctz requires an unsigned type");
  static_assert(sizeof(T) <= 8, "ctz supports up to 64-bit types");
  constexpr int nbits = sizeof(T) * 8;
  if (v == 0) return nbits;  // early return avoids UB on __builtin_ctzll(0)
  return __builtin_ctzll(v);
}

// ---- leading-zero quantisation (shared by encoder & decoder) ----
inline int roundLz(int lz) {
  static constexpr int map[65] = {0,  0,  0,  0,  0,  0,  0,  0,  8,  8,  8,  8,  12, 12, 12, 12,
                                  16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24,
                                  24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
                                  24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24};
  return map[lz];
}
inline int encodeLz(int lz) {
  static constexpr int map[65] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5,
                                  6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                                  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};
  return map[lz];
}
inline int decodeLz(int code) {
  static constexpr int map[8] = {0, 8, 12, 16, 18, 20, 22, 24};
  return map[code];
}

template <class U>
class XorCmpEnc {
 private:
  bool first = true;
  int last_lz = -1;
  int last_tz = -1;
  U last_v = 0;

  TsBitWriter* bw_;

 public:
  void Put(U v) {
    constexpr int nbits = sizeof(U) * 8;
    if (first) {
      last_tz = ctz(v);
      bw_->WriteBits(7, last_tz);
      if (last_tz < nbits) {
        bw_->WriteBits(nbits - last_tz, v >> last_tz);
      }
      first = false;
      last_v = v;
      return;
    }
    auto xor_v = static_cast<U>(v ^ last_v);
    if (xor_v == 0) {
      bw_->WriteBits(2, 0b01);
      last_v = v;
      return;
    }
    auto raw_lz = clz(xor_v);
    auto lz = roundLz(raw_lz);
    auto tz = ctz(xor_v);

    if (lz == last_lz && tz >= last_tz) {
      // Scene A: meaningful bits within previous window
      int center = nbits - lz - last_tz;
      bw_->WriteBits(2, 0b00);
      bw_->WriteBits(center, xor_v >> last_tz);
    } else {
      int center = nbits - lz - tz;
      if (center <= 16) {
        // Scene B: small center (1..16), 16 → 0 in 4-bit field
        bw_->WriteBits(2, 0b10);
        bw_->WriteBits(3, encodeLz(raw_lz));
        bw_->WriteBits(4, center & 0xf);
      } else {
        // Scene C: large center (17..64), 64 → 0 in 6-bit field
        bw_->WriteBits(2, 0b11);
        bw_->WriteBits(3, encodeLz(raw_lz));
        bw_->WriteBits(6, center & 0x3f);
      }
      bw_->WriteBits(center, xor_v >> tz);
    }

    last_lz = lz;
    last_tz = tz;
    last_v = v;
  }

  explicit XorCmpEnc(TsBitWriter* bw) : bw_(bw) {}

  void Encode(const U* data, int size) {
    for (int i = 0; i < size; ++i) {
      Put(data[i]);
    }
  }
};

template <class U>
class XorCmpDec {
 private:
  bool first = true;
  int last_lz = -1;
  int last_tz = -1;
  U last_v = 0;

  TsBitReader* br_;

 public:
  explicit XorCmpDec(TsBitReader* br) : br_(br) {}

  bool Get(U* v) {
    constexpr int nbits = sizeof(U) * 8;
    if (first) {
      // ---- first value ----
      uint64_t trail;
      if (!br_->ReadBits(7, &trail)) return false;
      last_tz = static_cast<int>(trail);
      if (last_tz < nbits) {
        uint64_t data;
        if (!br_->ReadBits(nbits - last_tz, &data)) return false;
        *v = static_cast<U>(data << last_tz);
      } else {
        *v = 0;
      }
      last_v = *v;
      first = false;
      return true;
    }

    uint64_t ctrl;
    if (!br_->ReadBits(2, &ctrl)) return false;

    if (ctrl == 0b01) {  // zero delta
      *v = last_v;
      return true;
    }

    U xor_v;
    if (ctrl == 0b00) {
      // Scene A: within previous window
      int center = nbits - last_lz - last_tz;
      uint64_t data;
      if (!br_->ReadBits(center, &data)) return false;
      xor_v = static_cast<U>(data << last_tz);
      last_tz = ctz(xor_v);
      // last_lz unchanged
    } else {
      // Scene B (0b10) or C (0b11)
      uint64_t lz_code;
      if (!br_->ReadBits(3, &lz_code)) return false;
      last_lz = decodeLz(static_cast<int>(lz_code));

      int center_bits_width = (ctrl == 0b10) ? 4 : 6;
      uint64_t center;
      if (!br_->ReadBits(center_bits_width, &center)) return false;
      int ci = static_cast<int>(center);
      // 0 in the field means max value: 16 for 4-bit, 64 for 6-bit
      if (ci == 0) ci = (ctrl == 0b10) ? 16 : 64;

      last_tz = nbits - last_lz - ci;

      uint64_t data;
      if (!br_->ReadBits(ci, &data)) return false;
      xor_v = static_cast<U>(data << last_tz);
    }

    *v = last_v ^ xor_v;
    last_v = *v;
    return true;
  }

  bool Decode(U* out, int size) {
    for (int i = 0; i < size; ++i) {
      if (!Get(&out[i])) return false;
    }
    return true;
  }
};

template <class T>
class Encoder {
  using Rep = RepType_t<GetRepSize<sizeof(T) * 8>()>;

 private:
  int last_beta = -1, last_beta_star = -1;

  TsBitWriter bw_;
  XorCmpEnc<Rep> xorCmpEnc_;

  int GetBeta(T v, int sp) {
    int i = 1;
    if (last_beta != -1) {
      i = std::max(last_beta - sp - 1, 1);
    }
    last_beta = kBetaSentinel;
    T temp = v * GetExp10<T>(i);
    int64_t temp_int = static_cast<int64_t>(temp);
    while (temp_int != temp) {
      if (std::isinf(temp) || temp == 0) {
        return last_beta;  // overflow/underflow → sentinel
      }
      ++i;
      temp = v * GetExp10<T>(i);
      temp_int = static_cast<int64_t>(temp);
    }
    if (temp / GetExp10<T>(i) != v) {
      return last_beta;
    }
    while (i > 0 && temp_int % 10 == 0) {
      --i;
      temp_int /= 10;
    }
    last_beta = sp + i + 1;
    return last_beta;
  }

  std::pair<int, int> GetAlphaAndBetaStar(T v) {
    v = std::abs(v);
    auto [sp, is_exp10] = GetSP(v);
    int beta = GetBeta(v, sp);
    // sentinel: value too large / underflowed → degrade to no-erase
    if (beta >= kBetaSentinel) {
      return {0, beta};  // alpha=0 safe for getFAlpha; betaStar >= kMaxSignificantDigits → no-erase
    }
    auto alpha = beta - sp - 1;
    auto betaStar = is_exp10 ? 0 : beta;
    last_beta = beta;
    return {alpha, betaStar};
  }

 public:
  explicit Encoder(TsBufferBuilder* out) : bw_(out), xorCmpEnc_(&bw_) {}

  // ===================================================================
  // PutValue — ElfEraser + ElfXORcmp pipeline
  // ===================================================================
  void PutValue(T v) {
    ElfIEEEFloat<T> fv{v};
    auto raw_rep = fv.GetUValue();
    if (fv.IsNan()) {
      raw_rep = ElfIEEEFloat<T>::Nan().GetUValue();
    }

    // ---- Branch 1: special values (0 / Inf / NaN) → raw encode, skip XOR ----
    //   Writes: [flag 1b=0] [raw bits sizeof(Rep)*8]
    if (v == 0 || fv.IsInf() || fv.IsNan()) {
      bw_.WriteBit(true);  // special flag = 1
      xorCmpEnc_.Put(raw_rep);
      return;
    }
    bw_.WriteBit(false);  // special flag = 0

    auto [alpha, betaStar] = GetAlphaAndBetaStar(v);

    Rep u;
    bool erased = false;
    if (betaStar < kMaxSignificantDigits) {
      auto falpha = getFAlpha(alpha);
      auto galpha = falpha + fv.GetExponentVal();
      int eraseBits = fv.GetNMantissaBits() - galpha;
      assert(eraseBits < 64);
      uint64_t delta = fv.GetUValue() & ((1ULL << eraseBits) - 1);
      if (delta != 0 && eraseBits > 4) {
        // ---- Branch 2: erase mantissa, then XOR-encode ----
        //   Writes: [erase flag 1b=1] [beta-same 1b] [betaStar 4b if diff] [XOR(u)]
        bw_.WriteBit(true);
        fv.EraseNBits(eraseBits);
        bool same_beta = betaStar == last_beta_star;
        bw_.WriteBit(same_beta);
        if (!same_beta) {
          bw_.WriteBits(4, static_cast<uint64_t>(betaStar));
          last_beta_star = betaStar;  // update last_beta_star
        }
        u = fv.GetUValue();
        erased = true;
      }
    }
    if (!erased) {
      bw_.WriteBit(false);  // erase flag = 0
      u = fv.GetUValue();
    }
    xorCmpEnc_.Put(u);
  }
};

template <class T>
class Decoder {
  using Rep = RepType_t<GetRepSize<sizeof(T) * 8>()>;
  static constexpr int kRepBits = sizeof(Rep) * 8;
  static_assert(sizeof(T) == sizeof(Rep));

 private:
  TsBitReader br_;
  XorCmpDec<Rep> xorCmpDec_;
  int last_beta_star = -1;

  // ---- reconstruct original float from u and betaStar ----
  T Reconstruct(Rep u, int betaStar) {
    static_assert(sizeof(T) == sizeof(Rep));
    ElfIEEEFloat<T> fv{u};
    bool negative = fv.GetSignBit();
    fv.Abs();
    auto [sp, _] = GetSP(fv.ToNative());

    if (betaStar == 0) {
      // exact power of 10: reconstruct magnitude from sp
      T v = GetIExp10<T>(-sp - 1);
      if (negative) v = -v;
      return v;
    }

    // round up to original precision
    int alpha = betaStar - sp - 1;
    fv.RoundUp(alpha);
    fv.SetSignBit(negative);
    return fv.ToNative();
  }

  bool Get(T* v) {
    // ---- read discriminator bit ----
    bool is_special;
    if (!br_.ReadBit(&is_special)) return false;

    Rep u;

    if (is_special) {
      // ---- Branch 1: special value (0 / Inf / NaN) ----
      uint64_t raw;
      if (!xorCmpDec_.Get(&u)) return false;
      *v = ElfIEEEFloat<T>{static_cast<Rep>(u)}.ToNative();
      return true;
    }

    // ---- normal value: read erase flag ----
    bool erased;
    if (!br_.ReadBit(&erased)) return false;

    if (!erased) {
      if (!xorCmpDec_.Get(&u)) return false;
      // ---- Branch 3: no-erase — u is the full float binary ----
      *v = ElfIEEEFloat<T>{u}.ToNative();
      return true;
    }

    // ---- Branch 2: erased — read betaStar, then reconstruct ----
    int betaStar;
    bool same_beta;
    if (!br_.ReadBit(&same_beta)) return false;
    if (!same_beta) {
      uint64_t bs;
      if (!br_.ReadBits(4, &bs)) return false;
      betaStar = static_cast<int>(bs);
      last_beta_star = betaStar;
    } else {
      betaStar = last_beta_star;
    }
    // then read u and reconstruct original float
    if (!xorCmpDec_.Get(&u)) return false;

    *v = Reconstruct(u, betaStar);
    return true;
  }

 public:
  explicit Decoder(std::string_view data) : br_(data), xorCmpDec_(&br_) {}

  bool Decode(T* out, int size) {
    for (int i = 0; i < size; ++i) {
      if (!Get(&out[i])) return false;
    }
    return true;
  }
};

}  // namespace detail

// ===================================================================
// Public API (aligned with ALP)
// ===================================================================

template <class T>
void Encode(const T* data, size_t size, TsBufferBuilder* out) {
  detail::Encoder<T> enc(out);
  for (size_t i = 0; i < size; ++i) {
    enc.PutValue(data[i]);
  }
}

template <class T>
bool Decode(const TsSliceGuard& in, size_t size, TsBufferBuilder* out) {
  if (size == 0) {
    out->clear();
    return true;
  }
  detail::Decoder<T> dec(std::string_view(in.data(), in.size()));
  out->resize(size * sizeof(T));
  T* buf = reinterpret_cast<T*>(out->data());
  return dec.Decode(buf, static_cast<int>(size));
}

}  // namespace ELFCodec

template <class T>

bool ELF<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder* out, const TsCompressionConfig& cfg) const {
  if (count == 0) return true;
  const T* float_data = reinterpret_cast<const T*>(data.data);
  ELFCodec::Encode(float_data, count, out);
  return true;
}

template <class T>
bool ELF<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard* out) const {
  TsSliceGuard input{data};
  TsBufferBuilder builder;
  bool ok = ELFCodec::Decode<T>(input, count, &builder);
  *out = builder.GetBuffer();
  return ok;
}

template class ELF<float>;
template class ELF<double>;

}  // namespace kwdbts
