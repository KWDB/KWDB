#include <gtest/gtest.h>

#include <iomanip>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "compression/ts_encoder_defs.h"
#include "test_common.h"

using namespace kwdbts;

// ============================================================
// Helpers
// ============================================================

/// Build a TsCompressionConfig that carries a shared ALPState.
/// When no state is provided, the compressor uses a local state (no adaptive history).
static TsCompressionConfig MakeALPCfg(
    std::shared_ptr<ALPCodec::ALPState> state = nullptr) {
  TsCompressionConfig cfg;
  if (state != nullptr) {
    cfg.extra_cfg = TsExtraCompConfig{};
    cfg.extra_cfg->alp_state = std::move(state);
  }
  return cfg;
}

/// Encode, decode, verify roundtrip.
/// When check_compression is true, asserts encoded ≤ raw size (the common case).
template <typename T>
static void RoundtripAndVerify(const std::vector<T> &input, bool check_compression = true) {
    const size_t N = input.size();
    if (N == 0) return;

    TsBufferBuilder encoded;
    ALP<T>::GetInstance().Compress(MakeSlice(input.data(), N), N, &encoded,
                                   TsCompressionConfig{});

    TsSliceGuard decoded;
    bool ok = ALP<T>::GetInstance().Decompress(encoded.AsSlice(), N, &decoded);
    ASSERT_TRUE(ok);

    auto *out = reinterpret_cast<const T *>(decoded.data());
    ASSERT_EQ(decoded.size(), N * sizeof(T));

    for (size_t i = 0; i < N; ++i) {
        ExpectValueEq(input[i], out[i], i);
    }

    if (check_compression) {
        size_t raw_size = N * sizeof(T);
        EXPECT_LE(encoded.size(), raw_size)
            << "Encoded " << encoded.size() << " bytes, raw " << raw_size << " bytes"
            << " (ratio " << std::fixed << std::setprecision(1)
            << (100.0 * encoded.size() / raw_size) << "%)";
    }
}

// ============================================================
// Original unit tests
// ============================================================
// (Constant-value and single-element cases are covered by the parameterized
// Generated/Float suites.)

TEST(ALPCodecTest, ConcurrentFindBestEF) {
    // Multiple threads sharing one ALPState. Exercises shared_lock/unique_lock
    // on the circular buffer and the cold→warm transition (kWindowSize=16).
    constexpr int kThreads = 8;
    constexpr int kBatchesPerThread = 32;
    constexpr int kBatchSize = 256;

    auto state = std::make_shared<ALPCodec::ALPState>();
    auto cfg = MakeALPCfg(state);
    std::mt19937 rng(42);

    auto worker = [&](int seed) {
        std::mt19937 local_rng(seed);
        std::uniform_real_distribution<double> dist(-1000.0, 1000.0);
        for (int b = 0; b < kBatchesPerThread; ++b) {
            std::vector<double> input(kBatchSize);
            for (int i = 0; i < kBatchSize; ++i)
                input[i] = dist(local_rng);

            TsBufferBuilder encoded;
            ALP<double>::GetInstance().Compress(
                MakeSlice(input.data(), kBatchSize), kBatchSize, &encoded, cfg);

            TsSliceGuard decoded;
            ASSERT_TRUE(ALP<double>::GetInstance().Decompress(
                encoded.AsSlice(), kBatchSize, &decoded));
            auto *out = reinterpret_cast<const double *>(decoded.data());
            for (int i = 0; i < kBatchSize; ++i)
                EXPECT_DOUBLE_EQ(out[i], input[i]) << "thread=" << seed << " i=" << i;
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back(worker, 100 + t);
    for (auto &t : threads)
        t.join();
}

// --- Edge-case datasets ---

class ALPCodecEdgeCaseTest : public ::testing::TestWithParam<std::string> {};

TEST_P(ALPCodecEdgeCaseTest, RoundtripDouble) {
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<double> &input, const std::string &) { RoundtripAndVerify(input); });
}
INSTANTIATE_TEST_CASE_P(EdgeCase, ALPCodecEdgeCaseTest, ::testing::ValuesIn(kEdgeCaseDoubleCSVs));

class ALPCodecFloatEdgeCaseTest : public ::testing::TestWithParam<std::string> {};

TEST_P(ALPCodecFloatEdgeCaseTest, RoundtripFloat) {
    RunDatasetRoundtrip<float>(TEST_DATA_DIR, GetParam(), 1024,
        [](const std::vector<float> &input, const std::string &) { RoundtripAndVerify(input); });
}
INSTANTIATE_TEST_CASE_P(EdgeCase, ALPCodecFloatEdgeCaseTest, ::testing::ValuesIn(kEdgeCaseFloatCSVs));

// --- Generated columns (different bit-widths) ---
// Test a representative subset of the 65 possible bit-widths

class ALPCodecGeneratedTest : public ::testing::TestWithParam<int> {};

TEST_P(ALPCodecGeneratedTest, RoundtripDoubleGenerated) {
    int bw = GetParam();
    auto input = MakeGeneratedDoubles(bw);
    ASSERT_FALSE(input.empty());
    // bw=64 values overflow int64, all stored as raw exceptions → no compression expected
    if (bw == 64) {
        RoundtripAndVerify(input, false);
    } else {
        RoundtripAndVerify(input);
    }
}

INSTANTIATE_TEST_CASE_P(
    GeneratedColumns,
    ALPCodecGeneratedTest,
    ::testing::Values(0, 8, 16, 24, 32, 40, 48, 56, 64));

// --- Sample datasets (real-world time series from the ALP benchmark) ---
// Each CSV contains 1024 double values.

class ALPCodecSampleTest : public ::testing::TestWithParam<std::string> {};

TEST_P(ALPCodecSampleTest, RoundtripDoubleSample) {
    auto name = GetParam();
    RunDatasetRoundtrip<double>(TEST_DATA_DIR, name, 1024,
        [&](const std::vector<double> &input, const std::string &fname) {
            bool check_compression = fname.find("poi_lat") == std::string::npos
                                  && fname.find("poi_lon") == std::string::npos;
            RoundtripAndVerify(input, check_compression);
        });
}

INSTANTIATE_TEST_CASE_P(
    Samples,
    ALPCodecSampleTest,
    ::testing::ValuesIn(kSampleCSVs));

// --- Float sample datasets ---

class ALPCodecFloatSampleTest : public ::testing::TestWithParam<NamedDataset<float>> {};

TEST_P(ALPCodecFloatSampleTest, RoundtripFloatSample) {
    RunDatasetRoundtrip<float>(GetParam(), 1024,
        [&](const std::vector<float> &input, const std::string &) {
            RoundtripAndVerify(input);
        });
}

INSTANTIATE_TEST_CASE_P(
    FloatSamples,
    ALPCodecFloatSampleTest,
    ::testing::ValuesIn(kFloatInlineDatasets));

// ============================================================
// Invalid / corrupt input — Decode error paths
// ============================================================

TEST(ALPCodecTest, DecodeTruncatedHeader) {
    // Header needs ≥2 bytes (ef + bitwidth). 0 or 1 byte must fail.
    double input[] = {1.0, 2.0, 3.0};
    TsBufferBuilder encoded;
    ALP<double>::GetInstance().Compress(MakeSlice(input, 3), 3, &encoded,
                                        TsCompressionConfig{});
    ASSERT_GT(encoded.size(), 2u);

    TsSliceGuard decoded;
    EXPECT_FALSE(ALP<double>::GetInstance().Decompress(
        TSSlice{encoded.data(), 0}, 3, &decoded));
    EXPECT_FALSE(ALP<double>::GetInstance().Decompress(
        TSSlice{encoded.data(), 1}, 3, &decoded));
}

TEST(ALPCodecTest, DecodeSizeMismatch) {
    // Encoded 3 values, ask for 10 — must fail (can't read enough exceptions).
    double input[] = {1.0, 2.0, 3.0};
    TsBufferBuilder encoded;
    ALP<double>::GetInstance().Compress(MakeSlice(input, 3), 3, &encoded,
                                        TsCompressionConfig{});

    TsSliceGuard decoded;
    EXPECT_FALSE(ALP<double>::GetInstance().Decompress(
        encoded.AsSlice(), 10, &decoded));
}

TEST(ALPCodecTest, DecodeCorruptMinVVarint) {
    // Corrupt the min_v varint: set continuation bytes without termination.
    double input[] = {1.0, 2.0, 3.0, 4.0, 5.0};
    TsBufferBuilder encoded;
    ALP<double>::GetInstance().Compress(MakeSlice(input, 5), 5, &encoded,
                                        TsCompressionConfig{});
    ASSERT_GT(encoded.size(), 3u);  // need header + at least 1 varint byte

    // Byte 0 = ef, byte 1 = bitwidth, byte 2+ = varint for min_v.
    // Overwrite the varint bytes with only continuation markers.
    for (size_t i = 2; i < encoded.size(); ++i) {
        encoded.data()[i] = static_cast<char>(0x80);
    }

    TsSliceGuard decoded;
    EXPECT_FALSE(ALP<double>::GetInstance().Decompress(
        encoded.AsSlice(), 5, &decoded));
}

TEST(ALPCodecTest, DecodeExceptionPosOutOfRange) {
    // Truncate encoded data before exception positions → must fail.
    // Use values that produce exceptions so the encoded stream is large enough.
    double nan = std::numeric_limits<double>::quiet_NaN();
    double inf = std::numeric_limits<double>::infinity();
    double input[] = {inf, -inf, nan, 1e100, -1e-100};
    TsBufferBuilder encoded;
    ALP<double>::GetInstance().Compress(MakeSlice(input, 5), 5, &encoded,
                                        TsCompressionConfig{});
    ASSERT_GT(encoded.size(), sizeof(double) + 3u);  // must have exception data

    // Truncate the last few bytes (raw exception values or positions).
    TsSliceGuard decoded;
    EXPECT_FALSE(ALP<double>::GetInstance().Decompress(
        TSSlice{encoded.data(), encoded.size() - sizeof(double) - 1}, 5, &decoded));
}

TEST(ALPCodecTest, RoundtripChunkBoundarySizes) {
    // Exercise chunk_size=1024 boundaries: exactly 1 chunk, 2 chunks, n_left only.
    for (size_t n : {1u, 1023u, 1024u, 2048u, 2049u}) {
        std::vector<double> input(n);
        for (size_t i = 0; i < n; ++i) input[i] = static_cast<double>(i + 1);
        RoundtripAndVerify(input, /*check_compression=*/false);
    }
}

TEST(ALPCodecTest, AllExceptionBatch) {
    // Mix of INF, NaN, and extreme values that resist ALP compression.
    // Every value should become an exception since no single (e,f) captures all.
    double nan = std::numeric_limits<double>::quiet_NaN();
    double inf = std::numeric_limits<double>::infinity();
    std::vector<double> input = {
        inf, -inf, nan, 1e100, -1e-100, 1.234567890123456e-50,
        9.876543210987654e50, 0.0, -0.0, 1.0, 100.0, -0.001
    };
    // check_compression=false because all-exception may exceed raw size
    RoundtripAndVerify(input, /*check_compression=*/false);
}
