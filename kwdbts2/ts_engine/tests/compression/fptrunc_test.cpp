#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <limits>

#include "compression/ts_encoder_defs.h"
#include "test_common.h"
#include "ts_bufferbuilder.h"

using namespace kwdbts;

// ============================================================
// Helpers
// ============================================================

static TsCompressionConfig MakeFpTruncCfg(double rel_err, double abs_err) {
    TsCompressionConfig cfg;
    cfg.extra_cfg = TsExtraCompConfig{};
    cfg.extra_cfg->rel_err = rel_err;
    cfg.extra_cfg->abs_err = abs_err;
    return cfg;
}

/// Compress → decompress → verify.  Returns encoded byte size.
/// FpTrunc applies precision truncation first, then Chimp-compresses the result.
/// The encoded output is smaller than raw; decompress recovers the truncated values.
template <typename T, typename VerifyFn>
static void FpTruncRoundTrip(const T *data, size_t n,
                             const TsCompressionConfig &cfg, VerifyFn &&verify) {
    TsBufferBuilder encoded;
    ASSERT_TRUE(FpTrunc<T>::GetInstance().Compress(MakeSlice(data, n), n, &encoded, cfg));

    TsSliceGuard decoded;
    ASSERT_TRUE(FpTrunc<T>::GetInstance().Decompress(encoded.AsSlice(), n, &decoded));
    ASSERT_EQ(decoded.size(), n * sizeof(T));

    auto *result = reinterpret_cast<const T *>(decoded.data());
    verify(result, n);
}

// ============================================================
// Error bound invariant
// ============================================================
// (NaN/Inf/±0 passthrough is covered by FpTruncEdgeCaseTest.)

TEST(FpTruncTest, ErrorBoundInvariantFloat) {
    double rel = 0.001;
    double abs = 0.01;
    auto cfg = MakeFpTruncCfg(rel, abs);
    float inputs[] = {1.0f, 3.14159f, 100.0f, 0.005f, -2.718f};

    FpTruncRoundTrip<float>(inputs, 5, cfg, [&](const float *result, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            double max_err = std::min(abs, rel * std::abs(static_cast<double>(inputs[i])));
            double actual_err = std::abs(static_cast<double>(result[i]) -
                                         static_cast<double>(inputs[i]));
            EXPECT_LE(actual_err, max_err + 1e-15)
                << "i=" << i << " input=" << inputs[i] << " result=" << result[i]
                << " err=" << actual_err << " max=" << max_err;
        }
    });
}

TEST(FpTruncTest, ErrorBoundInvariantDouble) {
    double rel = 1e-6;
    double abs = 1e-6;
    auto cfg = MakeFpTruncCfg(rel, abs);
    double inputs[] = {1.0, 3.141592653589793, 1e10, 1e-10, -2.718281828};

    FpTruncRoundTrip<double>(inputs, 5, cfg, [&](const double *result, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            double max_err = std::min(abs, rel * std::abs(inputs[i]));
            double actual_err = std::abs(result[i] - inputs[i]);
            EXPECT_LE(actual_err, max_err + 1e-30)
                << "i=" << i << " input=" << inputs[i] << " result=" << result[i];
        }
    });
}

// ============================================================
// Round-to-nearest: erased MSB decides direction
// ============================================================

TEST(FpTruncTest, RoundDown) {
    float eps = std::numeric_limits<float>::epsilon();
    float input = 1.0f + 1.0f * eps;
    auto cfg = MakeFpTruncCfg(5e-7, INFINITY);

    FpTruncRoundTrip<float>(&input, 1, cfg, [&](const float *result, size_t) {
        EXPECT_LE(result[0], input);
        EXPECT_NE(result[0], input);
    });
}

TEST(FpTruncTest, RoundUp) {
    float eps = std::numeric_limits<float>::epsilon();
    float input = 1.0f + 2.0f * eps;
    auto cfg = MakeFpTruncCfg(5e-7, INFINITY);

    FpTruncRoundTrip<float>(&input, 1, cfg, [&](const float *result, size_t) {
        EXPECT_GE(result[0], input);
        EXPECT_NE(result[0], input);
    });
}

// ============================================================
// Saturation guard: near-overflow values don't go to Inf
// ============================================================

TEST(FpTruncTest, SaturationGuardFloat) {
    float v = std::nextafter(std::numeric_limits<float>::max(), 0.0f);
    // rel_err unused (0.5 satisfies >0 && <1), abs_err=1e32 determines erasable_bits
    auto cfg = MakeFpTruncCfg(0.5, 1e32);

    FpTruncRoundTrip<float>(&v, 1, cfg, [&](const float *result, size_t) {
        EXPECT_TRUE(std::isfinite(result[0]));
        EXPECT_EQ(result[0], v);
    });
}

TEST(FpTruncTest, SaturationGuardDouble) {
    double v = std::nextafter(std::numeric_limits<double>::max(), 0.0);
    auto cfg = MakeFpTruncCfg(0.5, 1e300);

    FpTruncRoundTrip<double>(&v, 1, cfg, [&](const double *result, size_t) {
        EXPECT_TRUE(std::isfinite(result[0]));
    });
}

// ============================================================
// Subnormal values
// ============================================================

TEST(FpTruncTest, SubnormalFloat) {
    float v = std::numeric_limits<float>::denorm_min();
    auto cfg = MakeFpTruncCfg(0.1, 1e-40);

    FpTruncRoundTrip<float>(&v, 1, cfg, [&](const float *result, size_t) {
        double max_err = std::min(1e-40, 0.1 * static_cast<double>(v));
        double actual_err = std::abs(static_cast<double>(result[0]) - static_cast<double>(v));
        EXPECT_LE(actual_err, max_err + 1e-45);
    });
}

// ============================================================
// Idempotence: Truncate(Truncate(v)) == Truncate(v)
// ============================================================

TEST(FpTruncTest, Idempotence) {
    double rel = 0.001;
    double abs = 1e-6;
    auto cfg = MakeFpTruncCfg(rel, abs);

    float inputs[] = {1.234567f, -9.876543f, 1000.0f, 0.00012345f, 3.14159f};

    // Pass 1: truncate → Chimp-encode
    TsBufferBuilder enc1;
    ASSERT_TRUE(FpTrunc<float>::GetInstance().Compress(
        MakeSlice(inputs, 5), 5, &enc1, cfg));

    // Decode pass 1 → intermediate
    TsSliceGuard dec1;
    ASSERT_TRUE(FpTrunc<float>::GetInstance().Decompress(enc1.AsSlice(), 5, &dec1));
    auto *intermediate = reinterpret_cast<const float *>(dec1.data());

    // Pass 2: re-truncate intermediate
    TsBufferBuilder enc2;
    ASSERT_TRUE(FpTrunc<float>::GetInstance().Compress(
        MakeSlice(intermediate, 5), 5, &enc2, cfg));

    // Decode pass 2
    TsSliceGuard dec2;
    ASSERT_TRUE(FpTrunc<float>::GetInstance().Decompress(enc2.AsSlice(), 5, &dec2));
    auto *pass2 = reinterpret_cast<const float *>(dec2.data());

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(intermediate[i], pass2[i])
            << "i=" << i << " pass1=" << intermediate[i] << " pass2=" << pass2[i];
    }
}

// ============================================================
// Zero error tolerance: nothing changes
// ============================================================

TEST(FpTruncTest, ZeroToleranceIsPassthrough) {
    auto cfg = MakeFpTruncCfg(1e-15, 1e-15);
    float inputs[] = {1.2345f, 100.0f, -3.14f};

    FpTruncRoundTrip<float>(inputs, 3, cfg, [&](const float *result, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            EXPECT_FLOAT_EQ(result[i], inputs[i]) << "i=" << i;
        }
    });
}

// ============================================================
// Edge / invalid inputs
// ============================================================

TEST(FpTruncTest, BothErrorsInfiniteDefaultsToRelErr) {
    auto cfg = MakeFpTruncCfg(INFINITY, INFINITY);
    float inputs[] = {3.14159f, -2.718f, 100.5f, 0.125f};

    FpTruncRoundTrip<float>(inputs, 4, cfg, [&](const float *result, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            EXPECT_TRUE(std::isfinite(result[i])) << "i=" << i;
            double max_err = 0.001 * std::abs(static_cast<double>(inputs[i]));
            double actual_err = std::abs(static_cast<double>(result[i]) -
                                         static_cast<double>(inputs[i]));
            EXPECT_LE(actual_err, max_err + 1e-15) << "i=" << i;
        }
    });
}

TEST(FpTruncTest, DoubleSubnormalPassthrough) {
    double v = std::numeric_limits<double>::denorm_min();
    auto cfg = MakeFpTruncCfg(0.1, 1e-300);

    FpTruncRoundTrip<double>(&v, 1, cfg, [&](const double *result, size_t) {
        double max_err = std::min(1e-300, 0.1 * std::abs(v));
        double actual_err = std::abs(result[0] - v);
        EXPECT_LE(actual_err, max_err + 1e-310);
    });
}

// ============================================================
// Double-specific scenarios
// ============================================================

TEST(FpTruncTest, DoubleIdempotence) {
    double rel = 0.001;
    double abs = 1e-9;
    auto cfg = MakeFpTruncCfg(rel, abs);

    double inputs[] = {1.23456789, -9.87654321, 1000.0, 0.00012345, 3.141592653589793};

    TsBufferBuilder enc1;
    ASSERT_TRUE(FpTrunc<double>::GetInstance().Compress(
        MakeSlice(inputs, 5), 5, &enc1, cfg));

    TsSliceGuard dec1;
    ASSERT_TRUE(FpTrunc<double>::GetInstance().Decompress(enc1.AsSlice(), 5, &dec1));
    auto *intermediate = reinterpret_cast<const double *>(dec1.data());

    TsBufferBuilder enc2;
    ASSERT_TRUE(FpTrunc<double>::GetInstance().Compress(
        MakeSlice(intermediate, 5), 5, &enc2, cfg));

    TsSliceGuard dec2;
    ASSERT_TRUE(FpTrunc<double>::GetInstance().Decompress(enc2.AsSlice(), 5, &dec2));
    auto *pass2 = reinterpret_cast<const double *>(dec2.data());

    for (int i = 0; i < 5; ++i) {
        EXPECT_DOUBLE_EQ(intermediate[i], pass2[i]) << "i=" << i;
    }
}

TEST(FpTruncTest, DoubleZeroToleranceIsPassthrough) {
    auto cfg = MakeFpTruncCfg(1e-30, 1e-30);
    double inputs[] = {1.23456789, 100.0, -3.141592653589793};

    FpTruncRoundTrip<double>(inputs, 3, cfg, [&](const double *result, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            EXPECT_DOUBLE_EQ(result[i], inputs[i]) << "i=" << i;
        }
    });
}

// ============================================================
// Sample dataset error-bound verification (shared CSV list)
// ============================================================

class FpTruncSampleTest : public ::testing::TestWithParam<std::string> {};

TEST_P(FpTruncSampleTest, ErrorBoundOnSamples) {
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) {
            auto cfg = MakeFpTruncCfg(0.001, 1e-6);
            FpTruncRoundTrip<double>(input.data(), input.size(), cfg,
                [&](const double *result, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        double max_err = std::min(1e-6, 0.001 * std::abs(input[i]));
                        double actual_err = std::abs(result[i] - input[i]);
                        EXPECT_LE(actual_err, max_err + 1e-15) << "i=" << i;
                    }
                });
        });
}

INSTANTIATE_TEST_CASE_P(
    Samples,
    FpTruncSampleTest,
    ::testing::ValuesIn(kSampleCSVs));

class FpTruncGeneratedTest : public ::testing::TestWithParam<NamedDataset<double>> {};
TEST_P(FpTruncGeneratedTest, ErrorBoundDouble) {
    RunDatasetRoundtrip<double>(GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) {
            auto cfg = MakeFpTruncCfg(0.001, 1e-6);
            FpTruncRoundTrip<double>(input.data(), input.size(), cfg,
                [&](const double *result, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        double max_err = std::min(1e-6, 0.001 * std::abs(input[i]));
                        EXPECT_LE(std::abs(result[i] - input[i]), max_err + 1e-15)
                            << "i=" << i;
                    }
                });
        });
}
INSTANTIATE_TEST_CASE_P(Generated, FpTruncGeneratedTest, ::testing::ValuesIn(kGeneratedDatasets));

class FpTruncEdgeCaseTest : public ::testing::TestWithParam<std::string> {};
TEST_P(FpTruncEdgeCaseTest, ErrorBoundDouble) {
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) {
            auto cfg = MakeFpTruncCfg(0.001, 1e-6);
            FpTruncRoundTrip<double>(input.data(), input.size(), cfg,
                [&](const double *result, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        if (!std::isfinite(input[i])) {
                            if (std::isnan(input[i])) {
                                EXPECT_TRUE(std::isnan(result[i])) << "i=" << i;
                            } else {
                                EXPECT_TRUE(std::isinf(result[i])) << "i=" << i;
                            }
                            continue;
                        }
                        double max_err = std::min(1e-6, 0.001 * std::abs(input[i]));
                        EXPECT_LE(std::abs(result[i] - input[i]), max_err + 1e-15)
                            << "i=" << i;
                    }
                });
        });
}
INSTANTIATE_TEST_CASE_P(EdgeCase, FpTruncEdgeCaseTest, ::testing::ValuesIn(kEdgeCaseDoubleCSVs));

class FpTruncFloatTest : public ::testing::TestWithParam<NamedDataset<float>> {};
TEST_P(FpTruncFloatTest, ErrorBoundFloat) {
    RunDatasetRoundtrip<float>(GetParam(), 1024,
        [](const std::vector<float> &input, const std::string &) {
            auto cfg = MakeFpTruncCfg(0.001f, 1e-6f);
            FpTruncRoundTrip<float>(input.data(), input.size(), cfg,
                [&](const float *result, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        double max_err = std::min(1e-6, 0.001 * std::abs(
                            static_cast<double>(input[i])));
                        EXPECT_LE(std::abs(static_cast<double>(result[i]) -
                                          static_cast<double>(input[i])),
                                  max_err + 1e-15) << "i=" << i;
                    }
                });
        });
}
INSTANTIATE_TEST_CASE_P(Float, FpTruncFloatTest, ::testing::ValuesIn(kFloatInlineDatasets));
