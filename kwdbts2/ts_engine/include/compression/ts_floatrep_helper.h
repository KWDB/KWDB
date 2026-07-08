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

// ============================================================================
// floatnum_helper.h — Lightweight float bit manipulation for custom-width floats
// ============================================================================
//
// This header provides:
//   1. GetRepSize<N> / RepType_t<N>  — map a bit-width to the smallest uint type
//   2. FloatBase<EBits, MBits>       — bitfield access, classification,
//                                      FromNative/ToDouble/Cast conversion
//   3. IEEEFloat<T>                  — type-punning wrapper for native float/double
//
// Bit layout (from MSB to LSB):  [sign] [exponent] [mantissa]
//   SignBit   = EBits + MBits
//   Exponent  = bits [EBits+MBits-1 : MBits]
//   Mantissa  = bits [MBits-1 : 0]
// ============================================================================

#pragma once
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

// ============================================================================
// GetRepSize<N> — round up a bit count to the nearest power-of-2 uint size (min 8)
//
// Examples:  3→8,  8→8,  9→16,  16→16,  17→32
// Used to select the storage type that can hold the custom float's total bits.
// ============================================================================
template <size_t Size>
constexpr int GetRepSize() {
  static_assert(Size > 0 && Size <= 64, "Invalid representation size");
  // Already a power-of-2 and ≥8: return as-is
  if constexpr ((Size & (Size - 1)) == 0 && Size >= 8) {
    return Size;
  }
  // Round up to next power-of-2 via bit-smearing
  // e.g. Size=9 (1001): sz=15 (1111) → sz+1=16
  auto sz = Size | 0x7;
  sz |= sz >> 1;
  sz |= sz >> 2;
  sz |= sz >> 4;
  sz |= sz >> 8;
  sz |= sz >> 16;
  sz |= sz >> 32;
  return sz + 1;
}

// ============================================================================
// RepType_t<N> — pick the smallest unsigned integer type that holds N bits
// ============================================================================
template <size_t Size>
constexpr auto RepTypeHelper() {
  if constexpr (Size == 8)
    return uint8_t{};
  else if constexpr (Size == 16)
    return uint16_t{};
  else if constexpr (Size == 32)
    return uint32_t{};
  else if constexpr (Size == 64)
    return uint64_t{};
}

template <class T>
struct FloatRepHelper {
  static_assert(std::is_floating_point_v<T>);
  using type = decltype(RepTypeHelper<sizeof(T) * 8>());
};

template <class T>
using FloatURep_t = typename FloatRepHelper<T>::type;

template <class T>
using FloatSRep_t = std::make_signed_t<FloatURep_t<T>>;

template <size_t Size>
using RepType_t = decltype(RepTypeHelper<Size>());


// ============================================================================
// FloatBase<EBits, MBits, has_sign, Bias>
// ============================================================================
//
// Stores a floating-point value in a packed integer `Rep rep_` with a custom
// IEEE-754-like bit layout:
//
//   [sign:1] [exponent:EBits] [mantissa:MBits]    (if has_sign == true)
//            [exponent:EBits] [mantissa:MBits]    (if has_sign == false)
//
// Provides raw bitfield access, classification helpers, static factories
// (Nan/Inf), conversion to/from native floating-point types (FromNative,
// ToDouble), cross-format conversion (Cast), and in-place Abs().
//
// Template parameters:
//   EBits     — number of exponent bits
//   MBits     — number of mantissa (fraction) bits
//   has_sign  — whether the format has a sign bit (default true)
//   Bias      — exponent bias, defaults to 2^(EBits-1)-1 (IEEE convention)
// ============================================================================
template <size_t EBits, size_t MBits, bool has_sign = true, int32_t Bias = (1L << (EBits - 1)) - 1>
class FloatBase {
  static_assert(EBits > 0 && MBits > 0, "Invalid exponent bits or mantissa bits, must be > 0");

 protected:
  // Bit-field geometry (shared across all derived classes)
  static constexpr uint64_t ExponentBits = EBits;
  static constexpr uint64_t MantissaBits = MBits;
  static constexpr uint64_t SignBit = EBits + MBits;  // position of sign bit (from LSB)
  static constexpr uint64_t ExponentMask = (1ULL << ExponentBits) - 1;
  static constexpr uint64_t MantissaMask = (1ULL << MantissaBits) - 1;
  static constexpr uint64_t SignMask = 1ULL << SignBit;

  static_assert(has_sign + EBits + MBits <= 64, "Invalid float type");
  using Rep = RepType_t<GetRepSize<EBits + MBits + has_sign>()>;
  Rep rep_ = 0;  // the packed integer representation

 public:
  FloatBase() = default;
  explicit FloatBase(Rep r) : rep_{r} {}

  // ---- introspection ----
  constexpr auto GetNExponentBits() const { return ExponentBits; }
  constexpr auto GetNMantissaBits() const { return MantissaBits; }
  constexpr auto GetBias() const { return Bias; }

  // ---- sign bit (no-op when has_sign == false) ----
  bool GetSignBit() const { return rep_ & this->SignMask; }
  void SetSignBit(bool s) {
    if constexpr (!has_sign) return;
    auto sb = static_cast<Rep>(s) << this->SignBit;
    rep_ &= ~this->SignMask;
    rep_ |= sb;
  }

  // ---- absolute value in-place ----
  FloatBase& Abs() {
    if constexpr (has_sign) {
      rep_ &= ~SignMask;
    }
    return *this;
  }

  // ---- raw integer representation ----
  Rep GetUValue() const { return rep_; }

  // ---- mantissa (raw fraction bits, no implicit leading-1) ----
  uint64_t GetMantissa() const { return rep_ & this->MantissaMask; }
  void SetMantissa(uint64_t m) {
    rep_ &= ~(this->MantissaMask);
    rep_ |= m;
  }

  // ---- erase low N bits of the mantissa, return the erased bits ----
  // The returned uint64_t holds the bits that were cleared. Caller can
  // inspect its MSB (bit n-1) to perform round-to-nearest.
  uint64_t EraseNBits(int n) {
    uint64_t erased = static_cast<uint64_t>(rep_) & ((1ULL << n) - 1);
    rep_ &= ~static_cast<Rep>((1ULL << n) - 1);
    return erased;
  }

  // ---- exponent (raw biased value, not the mathematical exponent) ----
  uint32_t GetRawExponent() const { return (rep_ >> this->MantissaBits) & this->ExponentMask; }
  void SetRawExponent(uint32_t e) {
    rep_ &= ~(this->ExponentMask << this->MantissaBits);
    rep_ |= ((e & this->ExponentMask) << this->MantissaBits);
  }

  // ---- classification (IEEE-754 rules) ----
  bool IsDenormal() const { return GetRawExponent() == 0 && GetMantissa() != 0; }
  bool IsNan() const { return GetRawExponent() == this->ExponentMask && GetMantissa() != 0; }
  bool IsInf() const { return GetRawExponent() == this->ExponentMask && GetMantissa() == 0; }

  // ---- mathematical exponent value (unbiased) ----
  // Normal:   raw_exp - Bias
  // Denormal: 1 - Bias  (same exponent as the smallest normal, but without implicit-1)
  int32_t GetExponentVal() const {
    if (IsDenormal()) return 1 - Bias;
    return GetRawExponent() - Bias;
  }

  // ---- factory for quiet NaN ----
  static FloatBase Nan() {
    FloatBase nan;
    nan.SetRawExponent(ExponentMask);
    nan.SetMantissa(1ULL << (MantissaBits - 1));
    return nan;
  }
  // ---- factory for positive infinity ----
  static FloatBase Inf() {
    FloatBase inf;
    inf.SetRawExponent(ExponentMask);
    inf.SetMantissa(0);
    return inf;
  }

  // ---- construct from native float/double (quantized) ----
  static_assert(EBits <= 11, "EBits must be <= 11 for full IEEE compliance");
  static_assert(MBits <= 52, "MBits must be <= 52 for full IEEE compliance");
  static FloatBase FromNative(double v);

  // ---- decode to double ----
  double ToDouble() const;

  // ---- cross-format conversion ----
  template <size_t TargetEBits, size_t TargetMBits, bool TargetHasSign = true,
            int64_t TargetBias = (1L << (TargetEBits - 1)) - 1>
  FloatBase<TargetEBits, TargetMBits, TargetHasSign, TargetBias> Cast() const;
};

// ============================================================================
// IEEEFloatBase<T> — partial specialization to map C++ type → bit geometry
//
//   float  → FloatBase<8, 23>   (1 sign,  8 exponent, 23 mantissa)
//   double → FloatBase<11, 52>  (1 sign, 11 exponent, 52 mantissa)
// ============================================================================
template <class T>
struct IEEEFloatBase;

template <>
struct IEEEFloatBase<float> : public FloatBase<8, 23> {
  using FloatBase::FloatBase;
};

template <>
struct IEEEFloatBase<double> : public FloatBase<11, 52> {
  using FloatBase::FloatBase;
};

// ============================================================================
// IEEEFloat<T> — type-punning wrapper for native IEEE-754 float / double
//
// Uses a union { T f; Rep u; } to safely convert between floating-point values
// and their bit representation.  Provides:
//   - Construction from T (store bit pattern) or Rep (raw bits)
//   - GetFValue() → recover the native float (Abs() is inherited from FloatBase)
// ============================================================================
template <class T>
class IEEEFloat : public IEEEFloatBase<T> {
 protected:
  using typename IEEEFloatBase<T>::Rep;
  // Union for type-punning between T and its integer representation
  union Conv {
    static_assert(std::is_floating_point_v<T>, "T must be a floating point type");
    T f;
    Rep u;
  };

 public:
  IEEEFloat() = default;
  // Construct from float/double: type-pun via union
  explicit IEEEFloat(T v) : IEEEFloat{Conv{v}.u} {}
  // Construct from raw integer bit pattern
  explicit IEEEFloat(Rep u) : IEEEFloatBase<T>{u} {}

  // Recover the native float/double from the stored bit pattern
  T ToNative() const {
    Conv c;
    c.u = this->rep_;
    return c.f;
  }
};

// ============================================================================
// Out-of-class definitions for FloatBase conversion members
// (must appear after IEEEFloat is fully defined)
// ============================================================================

template <size_t EBits, size_t MBits, bool has_sign, int32_t Bias>
FloatBase<EBits, MBits, has_sign, Bias> FloatBase<EBits, MBits, has_sign, Bias>::FromNative(double v) {
  FloatBase result;
  IEEEFloat<double> ieee{v};
  if constexpr (has_sign) {
    result.SetSignBit(ieee.GetSignBit());
  }

  // NaN short-circuit
  if (std::isnan(v)) {
    auto nan = Nan();
    nan.SetSignBit(result.GetSignBit());
    result.rep_ = nan.GetUValue();
    return result;
  }

  // INF short-circuit
  if (std::isinf(v)) {
    auto inf = Inf();
    inf.SetSignBit(result.GetSignBit());
    result.rep_ = inf.GetUValue();
    return result;
  }

  // 1. Mantissa width adaptation
  uint64_t m = ieee.GetMantissa();
  constexpr auto src_mbits = ieee.GetNMantissaBits();
  if constexpr (src_mbits > MBits) {
    m >>= (src_mbits - MBits);
  } else {
    m <<= (MBits - src_mbits);
  }

  // Threshold computation (compile-time evaluable)
  double denormal_th = 0.0, underflow_th = 0.0;
  {
    IEEEFloat<double> th;
    th.SetMantissa(0);
    constexpr auto e = 1 - Bias + th.GetBias();
    static_assert(e > 0, "denormal_th must be representable as a normal double");
    th.SetRawExponent(e);
    denormal_th = th.ToNative();
  }
  {
    IEEEFloat<double> th;
    constexpr auto e = 1 - Bias - static_cast<int64_t>(MBits);
    if constexpr (e < 1 - th.GetBias()) {
      th.SetRawExponent(0);
      constexpr auto v_e = 1 - th.GetBias() - e;
      uint64_t v_m = 1ULL << th.GetNMantissaBits();
      v_m >>= v_e;
      th.SetMantissa(v_m);
    } else {
      th.SetMantissa(0);
      th.SetRawExponent(static_cast<uint32_t>(e + th.GetBias()));
    }
    underflow_th = th.ToNative();
  }

  // 2. Exponent mapping
  auto exp_val = ieee.GetExponentVal();
  auto abs_v = std::abs(v);
  uint32_t raw_exp;
  if (abs_v < underflow_th) {
    m = 0;
    raw_exp = 0;
  } else if (abs_v < denormal_th) {
    assert(exp_val <= 1 - Bias);
    raw_exp = 0;
    if (!ieee.IsDenormal()) {
      m |= 1ULL << MBits;
    }
    m >>= (1 - Bias - exp_val);
  } else if (exp_val > Bias) {
    if (!std::isnan(v)) m = 0;
    raw_exp = static_cast<uint32_t>(ExponentMask);
  } else {
    raw_exp = static_cast<uint32_t>(exp_val + Bias);
  }
  result.SetMantissa(m);
  result.SetRawExponent(raw_exp);
  return result;
}

template <size_t EBits, size_t MBits, bool has_sign, int32_t Bias>
double FloatBase<EBits, MBits, has_sign, Bias>::ToDouble() const {
  if (GetRawExponent() == 0 && GetMantissa() == 0) {
    return has_sign && GetSignBit() ? -0.0 : 0.0;
  }
  if (IsNan()) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (IsInf()) {
    auto inf = std::numeric_limits<double>::infinity();
    return has_sign && GetSignBit() ? -inf : inf;
  }

  IEEEFloat<double> ieee;
  if constexpr (has_sign) {
    ieee.SetSignBit(GetSignBit());
  }

  auto exp = GetExponentVal();
  auto m = GetMantissa();
  m <<= ieee.GetNMantissaBits() - MBits;
  m |= IsDenormal() ? 0 : 1ULL << ieee.GetNMantissaBits();
  constexpr auto exp_th = 1 - ieee.GetBias();
  while (exp > exp_th && m >> ieee.GetNMantissaBits() == 0) {
    exp--;
    m <<= 1;
  }

  auto raw_exp = exp + ieee.GetBias();
  if (exp == exp_th) {
    if (m >> ieee.GetNMantissaBits() == 0) {
      raw_exp = 0;
    }
  }

  ieee.SetRawExponent(raw_exp);
  ieee.SetMantissa(m & ((1ULL << ieee.GetNMantissaBits()) - 1));
  return ieee.ToNative();
}

template <size_t EBits, size_t MBits, bool has_sign, int32_t Bias>
template <size_t TargetEBits, size_t TargetMBits, bool TargetHasSign, int64_t TargetBias>
FloatBase<TargetEBits, TargetMBits, TargetHasSign, TargetBias> FloatBase<EBits, MBits, has_sign, Bias>::Cast() const {
  using Dst = FloatBase<TargetEBits, TargetMBits, TargetHasSign, TargetBias>;
  return Dst::FromNative(this->ToDouble());
}

using FP16_REL = FloatBase<8, 8, false>;
using FP16_ABS = FloatBase<11, 5, false>;
static_assert(sizeof(FP16_ABS) == 2, "FP16_ABS must be 2 bytes");
static_assert(sizeof(FP16_REL) == 2, "FP16_REL must be 2 bytes");
