#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>

#include "compression/ts_floatrep_helper.h"

// ============================================================
// GetRepSize / RepType_t
// ============================================================

TEST(GetRepSizeTest, All) {
    // powers of 2 are returned directly
    EXPECT_EQ((GetRepSize<8>()), 8);
    EXPECT_EQ((GetRepSize<16>()), 16);
    EXPECT_EQ((GetRepSize<32>()), 32);
    EXPECT_EQ((GetRepSize<64>()), 64);
    // non-powers round up (min 8)
    EXPECT_EQ((GetRepSize<3>()), 8);
    EXPECT_EQ((GetRepSize<9>()), 16);
    EXPECT_EQ((GetRepSize<17>()), 32);
    EXPECT_EQ((GetRepSize<33>()), 64);
}

TEST(RepTypeTest, CorrectTypes) {
    EXPECT_TRUE((std::is_same_v<RepType_t<8>, uint8_t>));
    EXPECT_TRUE((std::is_same_v<RepType_t<16>, uint16_t>));
    EXPECT_TRUE((std::is_same_v<RepType_t<32>, uint32_t>));
    EXPECT_TRUE((std::is_same_v<RepType_t<64>, uint64_t>));
}

// ============================================================
// FloatBase — constants & bit manipulation
// ============================================================

using Float32Base = FloatBase<8, 23>;
using Float64Base = FloatBase<11, 52>;

template <class FB>
struct OpenFloatBase : public FB {
    using typename FB::Rep;
    using FB::ExponentBits;
    using FB::MantissaBits;
    using FB::SignBit;
    using FB::ExponentMask;
    using FB::MantissaMask;
    using FB::SignMask;
};

TEST(FloatBaseTest, Constants) {
    // float-like
    using H32 = OpenFloatBase<Float32Base>;
    EXPECT_EQ(H32::ExponentBits, 8u);
    EXPECT_EQ(H32::MantissaBits, 23u);
    EXPECT_EQ(H32::SignBit, 31u);
    EXPECT_EQ(H32::ExponentMask, 0xFFu);
    EXPECT_EQ(H32::MantissaMask, 0x7FFFFFu);
    EXPECT_EQ(H32::SignMask, 0x80000000u);
    // double-like
    using H64 = OpenFloatBase<Float64Base>;
    EXPECT_EQ(H64::ExponentBits, 11u);
    EXPECT_EQ(H64::MantissaBits, 52u);
    EXPECT_EQ(H64::SignBit, 63u);
    EXPECT_EQ(H64::ExponentMask, 0x7FFu);
    EXPECT_EQ(H64::SignMask, 0x8000000000000000uLL);
}

TEST(FloatBaseTest, SignBit) {
    Float32Base pos(0x3f800000u);   // 1.0f
    EXPECT_FALSE(pos.GetSignBit());
    pos.SetSignBit(true);
    EXPECT_TRUE(pos.GetSignBit());
    EXPECT_EQ(pos.GetUValue(), 0xbf800000u);  // -1.0f
    pos.SetSignBit(false);
    EXPECT_EQ(pos.GetUValue(), 0x3f800000u);
}

TEST(FloatBaseTest, Mantissa) {
    Float32Base fb(0x3fc00000u);  // 1.5f: mantissa bit 22 = 1
    EXPECT_EQ(fb.GetMantissa(), 0x400000u);
    fb.SetMantissa(0x7FFFFFu);
    EXPECT_EQ(fb.GetMantissa(), 0x7FFFFFu);
}

TEST(FloatBaseTest, Exponent) {
    Float32Base fb(0x3f800000u);  // 1.0f
    EXPECT_EQ(fb.GetRawExponent(), 127u);
    EXPECT_EQ(fb.GetExponentVal(), 0);

    fb.SetRawExponent(128u);       // val=1 → 2.0f
    EXPECT_EQ(fb.GetExponentVal(), 1);
    fb.SetRawExponent(126u);       // val=-1 → 0.5f
    EXPECT_EQ(fb.GetExponentVal(), -1);
}

TEST(FloatBaseTest, Construction) {
    Float32Base def;
    EXPECT_EQ(def.GetUValue(), 0u);

    Float32Base from_rep(0x3f800000u);
    EXPECT_EQ(from_rep.GetUValue(), 0x3f800000u);

    EXPECT_EQ(from_rep.GetNExponentBits(), 8);
    EXPECT_EQ(from_rep.GetNMantissaBits(), 23);
}

// ============================================================
// FloatBase — without sign bit
// ============================================================

using FloatBaseNoSign = FloatBase<8, 23, false, 127>;

TEST(FloatBaseNoSignTest, SetSignBitNoOp) {
    FloatBaseNoSign fb(0x7FFFFFFFu);
    fb.SetSignBit(true);
    EXPECT_EQ(fb.GetUValue(), 0x7FFFFFFFu);  // unchanged
}

// ============================================================
// FloatBase — no-sign formats (has_sign=false)
// ============================================================

// 8-bit exponent, 8-bit mantissa, no sign — e.g. non-negative bfloat16 variant
using NoSign8x8 = FloatBase<8, 8, false>;
// 11-bit exponent, 5-bit mantissa, no sign — e.g. non-negative float16 variant
using NoSign11x5 = FloatBase<11, 5, false>;

TEST(FloatBaseNoSignTest, FromNative8x8) {
    // ---- normal values ----
    {
        auto f = NoSign8x8::FromNative(1.0);
        EXPECT_EQ(f.GetUValue(), 0x7F00u) << "1.0: raw_exp=127, mant=0";
        EXPECT_DOUBLE_EQ(f.ToDouble(), 1.0);
    }
    {
        auto f = NoSign8x8::FromNative(2.0);
        EXPECT_EQ(f.GetUValue(), 0x8000u) << "2.0: raw_exp=128, mant=0";
    }
    {
        auto f = NoSign8x8::FromNative(0.5);
        EXPECT_EQ(f.GetUValue(), 0x7E00u) << "0.5: raw_exp=126, mant=0";
    }
    // ---- negative input → same as positive (no sign bit) ----
    {
        auto f_pos = NoSign8x8::FromNative(3.0);
        auto f_neg = NoSign8x8::FromNative(-3.0);
        EXPECT_EQ(f_neg.GetUValue(), f_pos.GetUValue())
            << "negative input should produce same bits as positive";
        EXPECT_FALSE(f_neg.GetSignBit());
    }
    // ---- special: zero ----
    {
        auto f = NoSign8x8::FromNative(0.0);
        EXPECT_EQ(f.GetUValue(), 0u);
        EXPECT_DOUBLE_EQ(f.ToDouble(), 0.0);
    }
    {
        auto f = NoSign8x8::FromNative(-0.0);
        EXPECT_EQ(f.GetUValue(), 0u) << "-0.0 same as 0.0 (no sign)";
    }
    // ---- special: INF ----
    {
        auto f = NoSign8x8::FromNative(
            std::numeric_limits<double>::infinity());
        EXPECT_TRUE(f.IsInf());
        EXPECT_EQ(f.GetRawExponent(), 0xFFu);
        EXPECT_EQ(f.GetMantissa(), 0u);
        EXPECT_DOUBLE_EQ(f.ToDouble(),
                         std::numeric_limits<double>::infinity());
    }
    // ---- special: NaN ----
    {
        auto f = NoSign8x8::FromNative(
            std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(f.IsNan());
        EXPECT_EQ(f.GetRawExponent(), 0xFFu);
        EXPECT_NE(f.GetMantissa(), 0u);
        EXPECT_TRUE(std::isnan(f.ToDouble()));
    }
}

TEST(FloatBaseNoSignTest, FromNative11x5) {
    // ---- normal values (Bias=1023) ----
    {
        auto f = NoSign11x5::FromNative(1.0);
        EXPECT_EQ(f.GetUValue(), 0x7FE0u) << "1.0: raw_exp=1023=0x3FF, mant=0";
        EXPECT_DOUBLE_EQ(f.ToDouble(), 1.0);
    }
    {
        auto f = NoSign11x5::FromNative(2.0);
        EXPECT_EQ(f.GetUValue(), 0x8000u) << "2.0: raw_exp=1024=0x400, shift 5";
        EXPECT_DOUBLE_EQ(f.ToDouble(), 2.0);
    }
    // ---- negative → same as positive ----
    {
        auto f_pos = NoSign11x5::FromNative(42.0);
        auto f_neg = NoSign11x5::FromNative(-42.0);
        EXPECT_EQ(f_neg.GetUValue(), f_pos.GetUValue());
        EXPECT_FALSE(f_neg.GetSignBit());
    }
    // ---- zero ----
    {
        auto f = NoSign11x5::FromNative(0.0);
        EXPECT_EQ(f.GetUValue(), 0u);
    }
    // ---- INF ----
    {
        auto f = NoSign11x5::FromNative(
            std::numeric_limits<double>::infinity());
        EXPECT_TRUE(f.IsInf());
        EXPECT_EQ(f.GetRawExponent(), 0x7FFu);  // 11-bit all-ones
        EXPECT_EQ(f.GetMantissa(), 0u);
    }
    // ---- NaN ----
    {
        auto f = NoSign11x5::FromNative(
            std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(f.IsNan());
        EXPECT_EQ(f.GetRawExponent(), 0x7FFu);
        EXPECT_NE(f.GetMantissa(), 0u);
    }
    // ---- small value (normal, not denormal) ----
    {
        double v = std::ldexp(1.0, -500);  // well above 2^-1022
        auto f = NoSign11x5::FromNative(v);
        EXPECT_NEAR(f.ToDouble(), v, v * 0.1);
    }
}

TEST(FloatBaseNoSignTest, CastBetweenNoSignFormats) {
    auto f = NoSign8x8::FromNative(3.14);
    auto g = f.Cast<11, 5, false>();
    EXPECT_NEAR(g.ToDouble(), 3.14, 0.1);
    // roundtrip
    auto back = g.Cast<8, 8, false>();
    EXPECT_NEAR(back.ToDouble(), f.ToDouble(), 0.1);
}

// (Covered by FromNative8x8 SetSignBitNoOp.)

// ============================================================
// IEEEFloat (IEEE type-punning)
// ============================================================

TEST(IEEEFloatTest, Construction) {
    IEEEFloat<float> f(1.0f);
    EXPECT_EQ(f.GetUValue(), 0x3f800000u);

    IEEEFloat<float> fneg(-1.0f);
    EXPECT_EQ(fneg.GetUValue(), 0xbf800000u);

    IEEEFloat<double> d(1.0);
    EXPECT_EQ(d.GetUValue(), 0x3FF0000000000000uLL);

    IEEEFloat<float> from_rep(0x3f800000u);
    EXPECT_FLOAT_EQ(from_rep.ToNative(), 1.0f);
}

TEST(IEEEFloatTest, Roundtrip) {
    struct { float v; uint32_t bits; } fcases[] = {
        {1.0f, 0x3f800000u}, {2.0f, 0x40000000u}, {0.5f, 0x3f000000u},
        {-1.0f, 0xbf800000u}, {3.0f, 0x40400000u}, {0.15625f, 0x3e200000u},
    };
    for (auto [v, bits] : fcases) {
        IEEEFloat<float> f(v);
        EXPECT_EQ(f.GetUValue(), bits) << v;
        EXPECT_FLOAT_EQ(f.ToNative(), v);
    }

    struct { double v; uint64_t bits; } dcases[] = {
        {1.0, 0x3FF0000000000000uLL}, {2.0, 0x4000000000000000uLL},
        {0.5, 0x3FE0000000000000uLL}, {-1.0, 0xBFF0000000000000uLL},
    };
    for (auto [v, bits] : dcases) {
        IEEEFloat<double> d(v);
        EXPECT_EQ(d.GetUValue(), bits) << v;
        EXPECT_DOUBLE_EQ(d.ToNative(), v);
    }
}

TEST(IEEEFloatTest, Abs) {
    IEEEFloat<float> f(-3.14f);
    f.Abs();
    EXPECT_FALSE(f.GetSignBit());
    EXPECT_FLOAT_EQ(f.ToNative(), 3.14f);

    IEEEFloat<float> pos(5.0f);
    auto before = pos.GetUValue();
    pos.Abs();
    EXPECT_EQ(pos.GetUValue(), before);
}

TEST(IEEEFloatTest, SpecialValues) {
    // zero
    IEEEFloat<float> z(0.0f);
    EXPECT_EQ(z.GetUValue(), 0u);
    EXPECT_EQ(z.GetExponentVal(), -127);

    // negative zero
    IEEEFloat<float> nz(-0.0f);
    EXPECT_EQ(nz.GetUValue(), 0x80000000u);
    EXPECT_TRUE(std::signbit(nz.ToNative()));

    // denormal
    IEEEFloat<float> dm(std::numeric_limits<float>::denorm_min());
    EXPECT_EQ(dm.GetRawExponent(), 0u);
    EXPECT_EQ(dm.GetMantissa(), 1u);
    EXPECT_FLOAT_EQ(dm.ToNative(), std::numeric_limits<float>::denorm_min());

    // infinity
    IEEEFloat<float> inf(std::numeric_limits<float>::infinity());
    EXPECT_EQ(inf.GetRawExponent(), 0xFFu);
    EXPECT_EQ(inf.GetMantissa(), 0u);
    EXPECT_TRUE(std::isinf(inf.ToNative()));

    // NaN
    IEEEFloat<float> nan(std::numeric_limits<float>::quiet_NaN());
    EXPECT_NE(nan.GetMantissa(), 0u);
    EXPECT_TRUE(std::isnan(nan.ToNative()));
}

// (Covered by IEEEFloatTest::SpecialValues.)

// ============================================================
// FloatBase — FromNative construction
// ============================================================

using CF16 = FloatBase<5, 10>;  // bias=15, denormal_th=2^-14, underflow_th=2^-24

TEST(FloatBaseTest, ConstructNormal) {
    auto f = CF16::FromNative(1.0f);
    EXPECT_EQ(f.GetUValue(), 0x3C00u);   // sign=0, exp=15, mant=0
    auto f2 = CF16::FromNative(-1.0f);
    EXPECT_EQ(f2.GetUValue(), 0xBC00u);  // sign=1, exp=15, mant=0
    auto f3 = CF16::FromNative(2.0f);
    EXPECT_EQ(f3.GetUValue(), 0x4000u);  // sign=0, exp=16, mant=0
    // double source also works
    auto fd = CF16::FromNative(-2.0);
    EXPECT_EQ(fd.GetUValue(), 0xC000u);
}

TEST(FloatBaseTest, ConstructSpecial) {
    // zero
    auto z = CF16::FromNative(0.0f);
    EXPECT_EQ(z.GetUValue(), 0u);
    // negative zero
    auto nz = CF16::FromNative(-0.0f);
    EXPECT_EQ(nz.GetUValue(), 0x8000u);
    // overflow → INF
    auto ov = CF16::FromNative(1e10f);
    EXPECT_EQ(ov.GetRawExponent(), 31u);
    EXPECT_EQ(ov.GetMantissa(), 0u);
    // NaN
    auto nan = CF16::FromNative(std::numeric_limits<float>::quiet_NaN());
    EXPECT_EQ(nan.GetRawExponent(), 31u);
    EXPECT_NE(nan.GetMantissa(), 0u);
    // no-sign variant
    using CFNoSign = FloatBase<5, 10, false, 15>;
    auto ns = CFNoSign::FromNative(-1.0f);
    EXPECT_FALSE(ns.GetSignBit());
    EXPECT_EQ(ns.GetUValue(), 0x3C00u);
    // from Rep
    CF16 from_rep(0x3C00u);
    EXPECT_EQ(from_rep.GetUValue(), 0x3C00u);
}

// Regression: NaN must be correctly classified regardless of Bias magnitude.
// float NaN has exp_val=128; when Bias ≥ 128 (EBits ≥ 9), the `exp_val > Bias`
// overflow guard fails and NaN falls through to the normal branch.
TEST(FloatBaseTest, ConstructNaNWithLargeBias) {
    // ---- EBits=5 (Bias=15) — baseline, should always work ----
    {
        using CF5 = FloatBase<5, 10>;
        auto f = CF5::FromNative(std::numeric_limits<float>::quiet_NaN());
        EXPECT_TRUE(f.IsNan());
        EXPECT_EQ(f.GetRawExponent(), 31u);  // all-ones for 5-bit exponent
        EXPECT_NE(f.GetMantissa(), 0u);

        auto d = CF5::FromNative(std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(d.IsNan());
        EXPECT_EQ(d.GetRawExponent(), 31u);
        EXPECT_NE(d.GetMantissa(), 0u);
    }

    // ---- EBits=8 (Bias=127) — boundary: 128 > 127 still holds ----
    {
        using CF8 = FloatBase<8, 10>;
        auto f = CF8::FromNative(std::numeric_limits<float>::quiet_NaN());
        EXPECT_TRUE(f.IsNan());
        EXPECT_EQ(f.GetRawExponent(), 255u);
        EXPECT_NE(f.GetMantissa(), 0u);

        auto d = CF8::FromNative(std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(d.IsNan());
    }

    // ---- EBits=9 (Bias=255) — BUG: 128 ≤ 255, NaN skips overflow guard ----
    {
        using CF9 = FloatBase<9, 10>;
        auto f_nan = CF9::FromNative(std::numeric_limits<float>::quiet_NaN());
        EXPECT_TRUE(f_nan.IsNan())
            << "float NaN with EBits=9 (Bias=255): exp_val=128 ≤ 255, "
            << "should still be classified as NaN";
        EXPECT_EQ(f_nan.GetRawExponent(), 511u);  // ExponentMask for 9-bit

        // Double NaN is unaffected (exp_val=1024 > 255)
        auto d_nan = CF9::FromNative(std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(d_nan.IsNan());
        EXPECT_EQ(d_nan.GetRawExponent(), 511u);
    }

    // ---- EBits=11 (Bias=1023) — worst case for float NaN ----
    {
        using CF11 = FloatBase<11, 20>;
        auto f_nan = CF11::FromNative(std::numeric_limits<float>::quiet_NaN());
        EXPECT_TRUE(f_nan.IsNan())
            << "float NaN with EBits=11 (Bias=1023): exp_val=128 ≪ 1023, "
            << "should still be classified as NaN";
        EXPECT_EQ(f_nan.GetRawExponent(), 2047u);  // ExponentMask for 11-bit

        // Double NaN still fine (exp_val=1024 > 1023)
        auto d_nan = CF11::FromNative(std::numeric_limits<double>::quiet_NaN());
        EXPECT_TRUE(d_nan.IsNan());
    }

    // ---- sign preserved for negative NaN ----
    {
        using CF9 = FloatBase<9, 10>;
        float neg_nan = -std::numeric_limits<float>::quiet_NaN();
        auto f = CF9::FromNative(neg_nan);
        EXPECT_TRUE(f.IsNan());
        EXPECT_TRUE(f.GetSignBit()) << "negative NaN should preserve sign bit";
    }
}

// Same root cause as NaN: float INF has exp_val=128; when Bias ≥ 128
// the `exp_val > Bias` guard fails and INF is encoded as a normal number.
TEST(FloatBaseTest, ConstructINFWithLargeBias) {
    // ---- EBits=5 (Bias=15) — baseline, should always work ----
    {
        using CF5 = FloatBase<5, 10>;
        auto f = CF5::FromNative(std::numeric_limits<float>::infinity());
        EXPECT_TRUE(f.IsInf());
        EXPECT_EQ(f.GetRawExponent(), 31u);
        EXPECT_EQ(f.GetMantissa(), 0u);

        auto fneg = CF5::FromNative(-std::numeric_limits<float>::infinity());
        EXPECT_TRUE(fneg.IsInf());
        EXPECT_TRUE(fneg.GetSignBit());

        auto d = CF5::FromNative(std::numeric_limits<double>::infinity());
        EXPECT_TRUE(d.IsInf());
    }

    // ---- EBits=8 (Bias=127) — boundary: 128 > 127 still holds ----
    {
        using CF8 = FloatBase<8, 10>;
        auto f = CF8::FromNative(std::numeric_limits<float>::infinity());
        EXPECT_TRUE(f.IsInf());
        EXPECT_EQ(f.GetRawExponent(), 255u);
    }

    // ---- EBits=9 (Bias=255) — BUG: 128 ≤ 255, INF skips overflow guard ----
    {
        using CF9 = FloatBase<9, 10>;
        auto f_inf = CF9::FromNative(std::numeric_limits<float>::infinity());
        EXPECT_TRUE(f_inf.IsInf())
            << "float INF with EBits=9 (Bias=255): exp_val=128 ≤ 255, "
            << "should be classified as INF";
        EXPECT_EQ(f_inf.GetRawExponent(), 511u);
        EXPECT_EQ(f_inf.GetMantissa(), 0u);

        // negative INF
        auto f_neginf = CF9::FromNative(-std::numeric_limits<float>::infinity());
        EXPECT_TRUE(f_neginf.IsInf());
        EXPECT_TRUE(f_neginf.GetSignBit());

        // double INF is unaffected (exp_val=1024 > 255)
        auto d_inf = CF9::FromNative(std::numeric_limits<double>::infinity());
        EXPECT_TRUE(d_inf.IsInf());
    }

    // ---- EBits=11 (Bias=1023) — worst case for float INF ----
    {
        using CF11 = FloatBase<11, 20>;
        auto f_inf = CF11::FromNative(std::numeric_limits<float>::infinity());
        EXPECT_TRUE(f_inf.IsInf())
            << "float INF with EBits=11 (Bias=1023): exp_val=128 ≪ 1023, "
            << "should be classified as INF";
        EXPECT_EQ(f_inf.GetRawExponent(), 2047u);
        EXPECT_EQ(f_inf.GetMantissa(), 0u);
    }
}

TEST(FloatBaseTest, Cast) {
    using CF16 = FloatBase<5, 10>;

    // Cast to same format → identity
    {
        auto f = CF16::FromNative(3.14f);
        auto g = f.Cast<5, 10>();  // same EBits, MBits
        EXPECT_EQ(g.GetUValue(), f.GetUValue());
        EXPECT_DOUBLE_EQ(g.ToDouble(), f.ToDouble());
    }

    // Cast to wider format and back → consistent
    {
        auto f = CF16::FromNative(1.5f);
        // Cast to IEEE-float-like and back
        auto wide = f.Cast<8, 23>();
        auto back = wide.Cast<5, 10>();
        EXPECT_DOUBLE_EQ(back.ToDouble(), f.ToDouble());
    }

    // Cast is equivalent to FromNative(ToDouble())
    {
        float v = -2.75f;
        auto f = CF16::FromNative(v);
        auto via_cast = f.Cast<8, 10>();
        auto via_manual = FloatBase<8, 10>::FromNative(f.ToDouble());
        EXPECT_EQ(via_cast.GetUValue(), via_manual.GetUValue());
    }

    // Cast sign is preserved
    {
        auto f = CF16::FromNative(-1.0f);
        auto g = f.Cast<5, 10>();
        EXPECT_TRUE(g.GetSignBit());
        EXPECT_DOUBLE_EQ(g.ToDouble(), -1.0);
    }

    // Cast zero
    {
        auto f = CF16::FromNative(0.0f);
        auto g = f.Cast<11, 52>();
        EXPECT_EQ(g.GetRawExponent(), 0u);
        EXPECT_EQ(g.GetMantissa(), 0u);
    }
}

TEST(FloatBaseTest, RepRoundtrip) {
    // Construct CF16 from float, dump to uint16_t, reconstruct from that uint16_t
    float cases[] = {
        1.0f, -1.0f, 2.0f, 3.14f, 0.5f, -0.0f, 0.0f,
        std::ldexp(1.0f, -20),  // denormal
        std::ldexp(1.0f, -24),  // at underflow threshold
        65504.0f,               // max representable
    };
    for (float v : cases) {
        auto original = CF16::FromNative(v);
        uint16_t bits = original.GetUValue();
        CF16 restored(bits);
        EXPECT_EQ(restored.GetUValue(), bits) << "v=" << v;
        EXPECT_DOUBLE_EQ(restored.ToDouble(), original.ToDouble()) << "v=" << v;
    }
}

TEST(FloatBaseTest, ConstructDenormal) {
    // 2^-20 in [underflow_th, denormal_th) → denormal
    float v = std::ldexp(1.0f, -20);
    auto f = CF16::FromNative(v);
    EXPECT_TRUE(f.IsDenormal());
    EXPECT_EQ(f.GetRawExponent(), 0u);
    EXPECT_GT(f.GetMantissa(), 0u);

    // at denormal threshold 2^-14 → normal
    auto f2 = CF16::FromNative(std::ldexp(1.0f, -14));
    EXPECT_FALSE(f2.IsDenormal());
    EXPECT_EQ(f2.GetRawExponent(), 1u);

    // at underflow threshold 2^-24 → denormal (not flushed)
    auto f3 = CF16::FromNative(std::ldexp(1.0f, -24));
    EXPECT_TRUE(f.IsDenormal());
    EXPECT_EQ(f3.GetMantissa(), 1u);

    // below underflow → zero
    auto f4 = CF16::FromNative(std::ldexp(1.0f, -25));
    EXPECT_EQ(f4.GetMantissa(), 0u);
    EXPECT_FALSE(f4.IsDenormal());

    // negative denormal
    auto f5 = CF16::FromNative(-std::ldexp(1.0f, -20));
    EXPECT_TRUE(f5.GetSignBit());
    EXPECT_TRUE(f5.IsDenormal());
}

// ============================================================
// FloatBase — ToDouble
// ============================================================

TEST(FloatBaseTest, GetFValueNormal) {
    // constructed from float
    auto f = CF16::FromNative(1.0f);
    EXPECT_DOUBLE_EQ(f.ToDouble(), 1.0);
    auto f2 = CF16::FromNative(-1.0f);
    EXPECT_DOUBLE_EQ(f2.ToDouble(), -1.0);

    // from known bit patterns
    CF16 f3(0x4200u);  // raw_exp=16(val=1), mant=0x200=0.5 → 1.5×2=3.0
    EXPECT_DOUBLE_EQ(f3.ToDouble(), 3.0);

    // roundtrip with mantissa truncation (~0.1% tolerance for CF16)
    auto f4 = CF16::FromNative(3.14f);
    EXPECT_NEAR(f4.ToDouble(), 3.14, 0.02);
}

TEST(FloatBaseTest, GetFValueDenormal) {
    // smallest denormal: mant=1 → 2^-24
    CF16 f(0x0001u);
    EXPECT_TRUE(f.IsDenormal());
    EXPECT_DOUBLE_EQ(f.ToDouble(), std::ldexp(1.0, -24));

    // mid denormal: mant=512 → 2^-15
    CF16 f2(0x0200u);
    EXPECT_DOUBLE_EQ(f2.ToDouble(), std::ldexp(1.0, -15));

    // largest denormal: mant=1023 → 2^-14 * 1023/1024
    CF16 f3(0x03FFu);
    EXPECT_NEAR(f3.ToDouble(), std::ldexp(1.0, -14) * 1023.0 / 1024.0, 1e-15);

    // sign preserved
    CF16 f4(0x8200u);  // sign=1, raw_exp=0, mant=512
    EXPECT_DOUBLE_EQ(f4.ToDouble(), -std::ldexp(1.0, -15));

    // roundtrip float → CF denormal → double
    float v = std::ldexp(1.0f, -20);
    auto f5 = CF16::FromNative(v);
    EXPECT_NEAR(f5.ToDouble(), static_cast<double>(v), static_cast<double>(v) * 0.002);
}

// ============================================================
// FloatBase — same-bias float (EBits=8, MBits=23)
// ============================================================

using CFFloat = FloatBase<8, 23>;

TEST(FloatBaseTest, SameBiasDenormal) {
    // (float denorm_min coverage is provided by IEEEFloatTest::SpecialValues.)

    // mid-range float denormal
    float v = std::ldexp(1.0f, -140);
    auto f2 = CFFloat::FromNative(v);
    EXPECT_TRUE(f2.IsDenormal());
    EXPECT_DOUBLE_EQ(f2.ToDouble(), static_cast<double>(v));

    // CF denormal → double exact roundtrip
    CFFloat f3(0x000001u);  // raw_exp=0, mant=1
    EXPECT_DOUBLE_EQ(f3.ToDouble(), static_cast<double>(std::numeric_limits<float>::denorm_min()));
}

// ============================================================
// Edge / invalid inputs
// ============================================================

// (Covered by GetRepSizeTest::All.)

TEST(FloatBaseTest, EraseNBitsBeyondMantissa) {
    // Erasing more bits than the mantissa width eats into exponent and sign bits.
    // 1.5f = 0x3FC00000: sign=0, exp=0x7F, mantissa=0x400000
    IEEEFloat<float> fv(1.5f);
    uint32_t before = fv.GetUValue();
    EXPECT_EQ(before, 0x3FC00000u);

    fv.EraseNBits(24);  // mantissa has 23 bits; bit 24 clears LSB of exponent
    uint32_t after = fv.GetUValue();
    EXPECT_NE(after, before);  // value must have changed
    // Exponent dropped from 0x7F to 0x7E or mantissa and exponent both affected
    EXPECT_NE(fv.ToNative(), 1.5f);
}

// ============================================================
// ToDouble NaN sign bit preservation
// ============================================================

// (Covered by IEEEFloatTest::SpecialValues and FromNative8x8.)

// ============================================================
// IEEEFloat<double> denormal roundtrip
// ============================================================

// (Covered by IEEEFloatTest::SpecialValues.)

// ============================================================
// Cast cross has_sign
// ============================================================

using CF16_Sign = FloatBase<5, 10, true, 15>;
using CF16_NoSign = FloatBase<5, 10, false, 15>;

TEST(FloatBaseTest, CastCrossSign) {
    // Signed → unsigned: sign bit is stripped but magnitude preserved.
    auto f_signed = CF16_Sign::FromNative(-3.0);
    EXPECT_TRUE(f_signed.GetSignBit());
    auto f_nosign = f_signed.Cast<5, 10, false>();
    EXPECT_FALSE(f_nosign.GetSignBit());
    EXPECT_NEAR(f_nosign.ToDouble(), 3.0, 0.1);

    // Unsigned → signed: always positive.
    auto f_nosign2 = CF16_NoSign::FromNative(2.5);
    auto f_signed2 = f_nosign2.Cast<5, 10, true>();
    EXPECT_FALSE(f_signed2.GetSignBit());
    EXPECT_NEAR(f_signed2.ToDouble(), 2.5, 0.1);
}

// ============================================================
// Custom Bias
// ============================================================

// Non-standard bias: EBits=4, MBits=4, Bias=7 (normal IEEE would be 7 for 4 bits anyway,
// but using a deliberately different value: Bias=3).
using CustomBias = FloatBase<4, 4, true, 3>;

TEST(FloatBaseTest, CustomBiasRoundtrip) {
    // Small format: 4-bit exponent, 4-bit mantissa, Bias=3.
    // 1.0: exp_val=0, raw_exp=3
    auto f = CustomBias::FromNative(1.0);
    EXPECT_EQ(f.GetRawExponent(), 3u);
    EXPECT_NEAR(f.ToDouble(), 1.0, 0.1);

    // 2.0: exp_val=1, raw_exp=4
    auto f2 = CustomBias::FromNative(2.0);
    EXPECT_EQ(f2.GetRawExponent(), 4u);
    EXPECT_NEAR(f2.ToDouble(), 2.0, 0.2);

    // -1.0: sign preserved
    auto f3 = CustomBias::FromNative(-1.0);
    EXPECT_TRUE(f3.GetSignBit());
    EXPECT_NEAR(f3.ToDouble(), -1.0, 0.1);
}
