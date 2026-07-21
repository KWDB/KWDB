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
#include <cmath>
#include <cstdint>
#include <type_traits>

#include "compression/ts_encoder_defs.h"
#include "compression/ts_floatrep_helper.h"
#include "ts_bufferbuilder.h"

// FpTrunc — lossy floating-point mantissa truncation preprocessor.
//
// Erases low-order mantissa bits that fall within a user-specified error
// tolerance, with round-to-nearest. The truncated output is fed to a
// downstream compressor (e.g. Chimp) for further compression.
//
// Error model (strict conjunction):
//   max_err = min(abs_err, rel_err * |v|)
//
// To use only one constraint, pass INF for the other.

namespace kwdbts {
namespace FpTruncCodec {

template <class T>
class Encoder {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>, "FpTrunc only supports float and double");

  double rel_err_;
  double abs_err_;

 public:
  // rel_err: maximum relative error  (INF if unused)
  // abs_err: maximum absolute error  (INF if unused)
  //
  // NOTE: rel_err_ and abs_err_ are read from AttributeInfo as FP16_REL / FP16_ABS
  // (uint16_t with 8/5 mantissa bits).  The round-trip double→FP16→double incurs up
  // to ~3-6% relative quantization error.  Callers should validate the error bounds
  // at the SQL layer (minRelErr = 1e-9 in table.go) before they reach this encoder.
  Encoder(double rel_err, double abs_err) : rel_err_(rel_err), abs_err_(abs_err) {
    assert(!std::isnan(rel_err) && !std::isnan(abs_err));
    // If both bounds are +INF (neither constraint specified), default to
    // a reasonable relative-error bound to avoid erasing the entire mantissa.
    if (std::isinf(rel_err_) && std::isinf(abs_err_)) {
      rel_err_ = 0.001;
    }
    // Go-side validation (table.go) enforces REL ∈ (1e-9, 1) and ABS > 0
    // at the SQL layer.  We only sanity-check positivity here — the lower
    // bound must NOT be re-checked because FP16_REL quantization (8-bit
    // mantissa truncation) can reduce a value that just barely exceeds
    // 1e-9 to slightly below it, causing a false-positive assert.
    //
    // When only one constraint is specified, the other is +INF — this is
    // valid (Encode() handles INF correctly: INF*|v| never wins min()).
    assert((rel_err_ > 0.0 && rel_err_ < 1.0) || std::isinf(rel_err_));
    assert(abs_err_ > 0.0 || std::isinf(abs_err_));
  }

  // Process data[0..size) — append sizeof(T) raw bytes per truncated value to out.
  void Encode(const T *data, size_t size, TsBufferBuilder *out) {
    for (size_t i = 0; i < size; ++i) {
      T v = data[i];

      // ---- classification bypass: NaN, Inf, ±0 are passed through ----
      if (!std::isfinite(v) || v == static_cast<T>(0.0)) {
        out->append(reinterpret_cast<const char *>(&v), sizeof(T));
        continue;
      }

      IEEEFloat<T> fv(v);

      // ---- compute max allowed error ----
      double abs_v = std::abs(static_cast<double>(v));
      double max_err = abs_err_;
      double dyn_err = rel_err_ * abs_v;
      if (dyn_err < max_err) max_err = dyn_err;

      // ---- compute ULP ----
      int exp_val = fv.GetExponentVal();
      int mantissa_bits = static_cast<int>(fv.GetNMantissaBits());
      double ulp = std::ldexp(1.0, exp_val - mantissa_bits);

      // ---- compute erasable_bits ----
      int erasable_bits = 0;
      double erasable_ulps = std::floor(max_err / ulp);
      if (!std::isfinite(max_err) || !std::isfinite(erasable_ulps)) {
        // max_err is INF or max_err/ulp overflowed — erase all mantissa bits
        erasable_bits = mantissa_bits;
      } else if (erasable_ulps >= 2.0) {
        erasable_bits = static_cast<int>(std::floor(std::log2(erasable_ulps)));
        if (erasable_bits > mantissa_bits) erasable_bits = mantissa_bits;
      }

      if (erasable_bits == 0) {
        out->append(reinterpret_cast<const char *>(&v), sizeof(T));
        continue;
      }

      // ---- erase low mantissa bits, inspect MSB for round-to-nearest ----
      uint64_t erased = fv.EraseNBits(erasable_bits);
      uint64_t msb_of_erased = (erased >> (erasable_bits - 1)) & 1;

      if (msb_of_erased == 1) {
        // Carry into mantissa at the erased boundary.
        // Uses FloatBase public API: GetMantissa/SetMantissa/SetRawExponent.
        // Integer addition on the mantissa naturally propagates carry into exponent.
        uint64_t m = fv.GetMantissa();
        uint64_t carry = 1ULL << erasable_bits;
        m += carry;

        if (m >> mantissa_bits) {  // mantissa overflow → carry into exponent
          fv.SetRawExponent(fv.GetRawExponent() + 1);
          m &= (1ULL << mantissa_bits) - 1;
        }
        fv.SetMantissa(m);

        // Saturation guard: rounding overflow to infinity → fall back to original
        if (fv.IsInf()) {
          out->append(reinterpret_cast<const char *>(&v), sizeof(T));
          continue;
        }
      }

      T result = fv.ToNative();
      out->append(reinterpret_cast<const char *>(&result), sizeof(T));
    }
  }
};

}  // namespace FpTruncCodec

template <class T>
bool FpTrunc<T>::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &config) const {
  if (count == 0) return true;
  const T *float_data = reinterpret_cast<const T *>(data.data);
  assert(config.extra_cfg.has_value());
  FpTruncCodec::Encoder<T> encoder(config.extra_cfg.value().rel_err, config.extra_cfg.value().abs_err);
  TsBufferBuilder tmp_builder;
  encoder.Encode(float_data, count, &tmp_builder);

  const auto &chimp = Chimp<T>::GetInstance();
  return chimp.Compress(tmp_builder.AsSlice(), count, out, config);
}

template <class T>
bool FpTrunc<T>::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  const auto &chimp = Chimp<T>::GetInstance();
  return chimp.Decompress(data, count, out);
}

template class FpTrunc<float>;
template class FpTrunc<double>;

}  // namespace kwdbts
