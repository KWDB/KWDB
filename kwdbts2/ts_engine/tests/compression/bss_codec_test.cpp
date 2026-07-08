#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"
#include "test_common.h"

using namespace kwdbts;

// ============================================================
// Helpers
// ============================================================

/// Encode, decode, verify byte-exact roundtrip.
/// BSS is a pure lossless transform — encoded size always equals raw size.
template <typename T>
static void RoundtripAndVerify(const std::vector<T> &input) {
  if(input.empty()) return;
  const size_t N = input.size();
  ASSERT_GT(N, 0u);

  TsCompressionConfig cfg;
  TsBufferBuilder encoded;
  bool compress_ok = BSS<T>::GetInstance().Compress(MakeSlice(input.data(), N), N, &encoded, cfg);
  ASSERT_TRUE(compress_ok);

  // BSS has zero overhead: encoded size == N * sizeof(T)
  EXPECT_EQ(encoded.size(), N * sizeof(T));

  TsSliceGuard decoded;
  bool ok = BSS<T>::GetInstance().Decompress(encoded.AsSlice(), N, &decoded);
  ASSERT_TRUE(ok);

  auto *out = reinterpret_cast<const T *>(decoded.data());
  ASSERT_EQ(decoded.size(), N * sizeof(T));

  for (size_t i = 0; i < N; ++i) {
    if (std::isnan(input[i])) {
      // NaN roundtrip: bit-exact (not just "is NaN")
      // Compare NaN bit patterns byte-exact.
      // Use unsigned integer of exactly sizeof(T) to avoid reading
      // uninitialized bytes (UB when T=float uses uint64_t).
      if constexpr (std::is_same_v<T, float>) {
        uint32_t a, b;
        std::memcpy(&a, &input[i], sizeof(T));
        std::memcpy(&b, &out[i], sizeof(T));
        EXPECT_EQ(a, b) << "NaN bit pattern mismatch at i=" << i;
      } else {
        uint64_t a, b;
        std::memcpy(&a, &input[i], sizeof(T));
        std::memcpy(&b, &out[i], sizeof(T));
        EXPECT_EQ(a, b) << "NaN bit pattern mismatch at i=" << i;
      }
    } else if constexpr (std::is_same_v<T, float>) {
      EXPECT_FLOAT_EQ(out[i], input[i]) << "i=" << i;
    } else {
      EXPECT_DOUBLE_EQ(out[i], input[i]) << "i=" << i;
    }
  }
}

// ============================================================
// Basic roundtrip tests
// ============================================================

TEST(BSSCodecTest, RoundtripDoubleBasic) {
  std::vector<double> input = {1.0, 10.0, 100.0, 1000.0, 0.1, 0.01, 0.001,
                                -3.14, -2.718, 1.414, 6.022e23, 1.6e-19};
  RoundtripAndVerify(input);
}

TEST(BSSCodecTest, RoundtripFloatBasic) {
  std::vector<float> input = {1.0f, 10.0f, 100.0f, 0.1f, 0.01f, 3.14f, -2.5f,
                               -0.0f, 1e10f, float(M_PI)};
  RoundtripAndVerify(input);
}

// ---- Constant-value / single-element cases are covered by the
//      parameterized Generated/Float suites and BSSCodecSizeTest ----

// ---- NaN / Inf / special values ----

TEST(BSSCodecTest, RoundtripDoubleNaN) {
  // NaN/Inf bit patterns must survive byte-exact.
  // 0.0 and -0.0 are already covered by generated bw0 and RoundtripDoubleDenormal.
  std::vector<double> input = {
      std::numeric_limits<double>::quiet_NaN(),
      -std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
  };
  RoundtripAndVerify(input);
}
TEST(BSSCodecTest, RoundtripFloatSpecial) {
  std::vector<float> input = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(),
      -std::numeric_limits<float>::infinity(),
      std::numeric_limits<float>::denorm_min(),
      -std::numeric_limits<float>::denorm_min(),
  };
  RoundtripAndVerify(input);
}

// ---- Small arrays (exercises b < kBytes inner loop with few iterations) ----

TEST(BSSCodecTest, RoundtripDoubleSmall) {
  for (size_t n = 1; n <= 8; ++n) {
    std::vector<double> input(n);
    for (size_t i = 0; i < n; ++i) input[i] = static_cast<double>(i + 1);
    RoundtripAndVerify(input);
  }
}
TEST(BSSCodecTest, RoundtripFloatSmall) {
  for (size_t n = 1; n <= 8; ++n) {
    std::vector<float> input(n);
    for (size_t i = 0; i < n; ++i) input[i] = static_cast<float>(i + 1);
    RoundtripAndVerify(input);
  }
}

// ---- Large random input ----

TEST(BSSCodecTest, RoundtripFloatLarge) {
  const size_t N = 10000;
  std::vector<float> input(N);
  std::mt19937 rng(42);
  std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);
  for (size_t i = 0; i < N; ++i) input[i] = dist(rng);
  RoundtripAndVerify(input);
}

// ---- Integer-valued floats (exact bit representation) ----

TEST(BSSCodecTest, RoundtripDoubleIntegerValues) {
  // Powers of 2 have exact IEEE 754 representation
  std::vector<double> input;
  for (int i = -20; i <= 20; ++i) input.push_back(std::pow(2.0, i));
  // Exact integers within the 53-bit mantissa
  for (int64_t i = -1000; i <= 1000; ++i) input.push_back(static_cast<double>(i));
  RoundtripAndVerify(input);
}
TEST(BSSCodecTest, RoundtripFloatIntegerValues) {
  std::vector<float> input;
  for (int i = -10; i <= 10; ++i) input.push_back(std::pow(2.0f, i));
  for (int32_t i = -1000; i <= 1000; ++i) input.push_back(static_cast<float>(i));
  RoundtripAndVerify(input);
}

// ============================================================
// Decode error handling
// ============================================================

TEST(BSSCodecTest, DecodeTruncatedFails) {
  // Provide fewer bytes than needed — must return false
  std::vector<float> input = {1.0f, 2.0f, 3.0f};  // 3 × 4 = 12 bytes
  TsBufferBuilder encoded;
  BSS<float>::GetInstance().Compress(MakeSlice(input.data(), 3), 3, &encoded, TsCompressionConfig{});

  // Truncate to various short lengths
  for (size_t trunc = 0; trunc < 12; ++trunc) {
    TsSliceGuard decoded;
    bool ok = BSS<float>::GetInstance().Decompress(
        TSSlice{encoded.data(), trunc}, 3, &decoded);
    EXPECT_FALSE(ok) << "Should fail with " << trunc << " bytes for 3 floats";
  }
}

// ============================================================
// Roundtrip across various sizes (exercises different alignments)
// ============================================================

class BSSCodecSizeTest : public ::testing::TestWithParam<size_t> {};

TEST_P(BSSCodecSizeTest, RoundtripDoubleVariousSizes) {
  size_t N = GetParam();
  std::vector<double> input(N);
  std::mt19937 rng(N);
  std::uniform_real_distribution<double> dist(-100.0, 100.0);
  for (size_t i = 0; i < N; ++i) input[i] = dist(rng);
  RoundtripAndVerify(input);
}

TEST_P(BSSCodecSizeTest, RoundtripFloatVariousSizes) {
  size_t N = GetParam();
  std::vector<float> input(N);
  std::mt19937 rng(N);
  std::uniform_real_distribution<float> dist(-100.0f, 100.0f);
  for (size_t i = 0; i < N; ++i) input[i] = dist(rng);
  RoundtripAndVerify(input);
}

INSTANTIATE_TEST_CASE_P(
    VariousSizes,
    BSSCodecSizeTest,
    ::testing::Values(1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33,
                      63, 64, 65, 127, 128, 129, 255, 256, 257, 511,
                      512, 513, 1023, 1024, 1025));

// ============================================================
// Byte-level verification: confirm BSS layout is correct
// ============================================================

TEST(BSSCodecTest, VerifyByteLayout) {
  // Manually verify that encoded bytes follow the documented wire format:
  //   [byte_stream_0][byte_stream_1]...[byte_stream_{K-1}]
  // where byte_stream_b contains byte b of each value.
  constexpr size_t N = 8;
  double input[N] = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0};

  TsBufferBuilder encoded;
  BSS<double>::GetInstance().Compress(MakeSlice(input, N), N, &encoded, TsCompressionConfig{});
  ASSERT_EQ(encoded.size(), N * sizeof(double));

  const char *raw = encoded.data();
  const char *src = reinterpret_cast<const char *>(input);

  // Byte b of value i should be at encoded position b * N + i
  for (size_t b = 0; b < sizeof(double); ++b) {
    for (size_t i = 0; i < N; ++i) {
      EXPECT_EQ(static_cast<uint8_t>(raw[b * N + i]),
                static_cast<uint8_t>(src[i * sizeof(double) + b]))
          << "byte " << b << " of value " << i;
    }
  }
}

// ============================================================
// Invalid / excess input — Decode error paths
// ============================================================

TEST(BSSCodecTest, DecodeWithExcessBytesSucceeds) {
  // When in.size() > size * kBytes, the extra bytes are ignored.
  // Verify roundtrip still works correctly.
  std::vector<float> input = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  TsBufferBuilder encoded;
  BSS<float>::GetInstance().Compress(MakeSlice(input.data(), 5), 5, &encoded, TsCompressionConfig{});
  ASSERT_EQ(encoded.size(), 5u * sizeof(float));

  // Append trailing garbage
  encoded.push_back('\xFF');
  encoded.push_back('\x00');
  encoded.push_back('\xAB');

  TsSliceGuard decoded;
  bool ok = BSS<float>::GetInstance().Decompress(encoded.AsSlice(), 5, &decoded);
  EXPECT_TRUE(ok);
  auto *out = reinterpret_cast<const float *>(decoded.data());
  for (size_t i = 0; i < 5; ++i) EXPECT_FLOAT_EQ(out[i], input[i]) << "i=" << i;
}

// ============================================================
// Double denormal values
// ============================================================

TEST(BSSCodecTest, RoundtripDoubleDenormal) {
  // Denormal doubles must survive the byte-transpose roundtrip bit-exact.
  std::vector<double> input = {
      std::numeric_limits<double>::denorm_min(),
      -std::numeric_limits<double>::denorm_min(),
      std::ldexp(1.0, -1022),   // smallest normal
      std::ldexp(1.0, -1023),   // max denormal
      std::ldexp(1.0, -1050),   // mid denormal
      0.0,
      -0.0,
  };
  RoundtripAndVerify(input);
}

// ============================================================
// Large size stress
// ============================================================

TEST(BSSCodecTest, RoundtripLarge50K) {
  const size_t N = 50000;
  std::vector<double> input(N);
  std::mt19937 rng(123);
  std::uniform_real_distribution<double> dist(-1e6, 1e6);
  for (size_t i = 0; i < N; ++i) input[i] = dist(rng);
  RoundtripAndVerify(input);
}

// ============================================================
// Sample dataset roundtrip (shared CSV list)
// ============================================================

class BSSCodecSampleTest : public ::testing::TestWithParam<std::string> {};

TEST_P(BSSCodecSampleTest, RoundtripDoubleSample) {
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) {
            RoundtripAndVerify(input);
        });
}

INSTANTIATE_TEST_CASE_P(
    Samples,
    BSSCodecSampleTest,
    ::testing::ValuesIn(kSampleCSVs));

class BSSCodecGeneratedTest : public ::testing::TestWithParam<NamedDataset<double>> {};
TEST_P(BSSCodecGeneratedTest, RoundtripDouble) {
    RunDatasetRoundtrip<double>(GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) { RoundtripAndVerify(input); });
}
INSTANTIATE_TEST_CASE_P(Generated, BSSCodecGeneratedTest, ::testing::ValuesIn(kGeneratedDatasets));

class BSSCodecEdgeCaseTest : public ::testing::TestWithParam<std::string> {};
TEST_P(BSSCodecEdgeCaseTest, RoundtripDouble) {
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) { RoundtripAndVerify(input); });
}
INSTANTIATE_TEST_CASE_P(EdgeCase, BSSCodecEdgeCaseTest, ::testing::ValuesIn(kEdgeCaseDoubleCSVs));

class BSSCodecFloatTest : public ::testing::TestWithParam<NamedDataset<float>> {};
TEST_P(BSSCodecFloatTest, RoundtripFloat) {
    RunDatasetRoundtrip<float>(GetParam(), 1024,
        [](const std::vector<float> &input, const std::string &) { RoundtripAndVerify(input); });
}
INSTANTIATE_TEST_CASE_P(Float, BSSCodecFloatTest, ::testing::ValuesIn(kFloatInlineDatasets));
