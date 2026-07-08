#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <random>

#include "compression/ts_encoder_defs.h"
#include "test_common.h"

using namespace kwdbts;

// ============================================================
// Helpers
// ============================================================

/// Compress → decompress bool array, verify roundtrip.
static void BoolRoundTrip(const bool *input, size_t n) {
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  bool ok = TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input, n), n, &encoded, cfg);
  ASSERT_TRUE(ok);
  EXPECT_GT(encoded.size(), 0u) << "encoded output should not be empty for n=" << n;

  TsSliceGuard decoded;
  ok = TsRangeCodecBool::GetInstance().Decompress(encoded.AsSlice(), n, &decoded);
  ASSERT_TRUE(ok);
  ASSERT_EQ(decoded.size(), n * sizeof(bool));

  auto *out = reinterpret_cast<const bool *>(decoded.data());
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(out[i], input[i]) << "i=" << i;
  }
}

// ============================================================
// Basic roundtrip
// ============================================================

TEST(TsRangeCodecBoolTest, RoundtripBasic) {
  bool input[] = {true, false, true, true, false, true, false, false, true, true};
  constexpr size_t n = sizeof(input) / sizeof(input[0]);
  BoolRoundTrip(input, n);
}

TEST(TsRangeCodecBoolTest, RoundtripSingleTrue) {
  bool input[] = {true};
  BoolRoundTrip(input, 1);
}

TEST(TsRangeCodecBoolTest, RoundtripSingleFalse) {
  bool input[] = {false};
  BoolRoundTrip(input, 1);
}

// ============================================================
// Homogeneous arrays
// ============================================================

TEST(TsRangeCodecBoolTest, RoundtripAllTrue) {
  constexpr size_t kN = 1000;
  auto input = std::make_unique<bool[]>(kN);
  for (size_t i = 0; i < kN; ++i) input[i] = true;
  BoolRoundTrip(input.get(), kN);
}

TEST(TsRangeCodecBoolTest, RoundtripAllFalse) {
  constexpr size_t kN = 1000;
  auto input = std::make_unique<bool[]>(kN);
  for (size_t i = 0; i < kN; ++i) input[i] = false;
  BoolRoundTrip(input.get(), kN);
}

// ============================================================
// Alternating patterns
// ============================================================

TEST(TsRangeCodecBoolTest, RoundtripAlternating) {
  constexpr size_t kN = 1024;
  auto input = std::make_unique<bool[]>(kN);
  for (size_t i = 0; i < kN; ++i) input[i] = (i & 1);
  BoolRoundTrip(input.get(), kN);
}

TEST(TsRangeCodecBoolTest, RoundtripLongRuns) {
  constexpr size_t kN = 2048;
  auto input = std::make_unique<bool[]>(kN);
  // 500 true, 1 false, 1000 true, 1 false, rest true
  size_t pos = 0;
  for (size_t i = 0; i < 500 && pos < kN; ++i) input[pos++] = true;
  if (pos < kN) input[pos++] = false;
  for (size_t i = 0; i < 1000 && pos < kN; ++i) input[pos++] = true;
  if (pos < kN) input[pos++] = false;
  while (pos < kN) input[pos++] = true;
  BoolRoundTrip(input.get(), kN);
}

// ============================================================
// Random data — stress test
// ============================================================

TEST(TsRangeCodecBoolTest, RoundtripRandom50K) {
  constexpr size_t kN = 50000;
  auto input = std::make_unique<bool[]>(kN);
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, 1);
  for (size_t i = 0; i < kN; ++i) input[i] = dist(rng);
  BoolRoundTrip(input.get(), kN);
}

// ============================================================
// Compression ratio: skewed distributions should compress well
// ============================================================

TEST(TsRangeCodecBoolTest, CompressionRatioSkewed) {
  // 99% true → entropy ≈ 0.081 bits/symbol → should compress heavily
  constexpr size_t kN = 4096;
  auto input = std::make_unique<bool[]>(kN);
  std::mt19937 rng(777);
  std::bernoulli_distribution dist(0.99);
  size_t count_true = 0;
  for (size_t i = 0; i < kN; ++i) {
    input[i] = dist(rng);
    if (input[i]) ++count_true;
  }

  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input.get(), kN), kN, &encoded, cfg));

  // Roundtrip verification
  TsSliceGuard decoded;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), kN, &decoded));
  auto *out = reinterpret_cast<const bool *>(decoded.data());
  for (size_t i = 0; i < kN; ++i) {
    ASSERT_EQ(out[i], input[i]) << "i=" << i;
  }

  // With extreme skew, range coding should beat a bitpacked baseline (kN/8 bytes)
  size_t bitpacked_bytes = kN / 8;
  EXPECT_LT(encoded.size(), bitpacked_bytes)
      << "encoded=" << encoded.size() << " B, bitpacked=" << bitpacked_bytes << " B"
      << " (true=" << count_true << "/" << kN << ")";
}

TEST(TsRangeCodecBoolTest, CompressionRatioUniform) {
  // 50% true → entropy ≈ 1 bit/symbol → should still compress reasonably
  constexpr size_t kN = 4096;
  auto input = std::make_unique<bool[]>(kN);
  std::mt19937 rng(123);
  std::bernoulli_distribution dist(0.5);
  for (size_t i = 0; i < kN; ++i) input[i] = dist(rng);

  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input.get(), kN), kN, &encoded, cfg));

  // Roundtrip
  TsSliceGuard decoded;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), kN, &decoded));
  auto *out = reinterpret_cast<const bool *>(decoded.data());
  for (size_t i = 0; i < kN; ++i) {
    ASSERT_EQ(out[i], input[i]) << "i=" << i;
  }

  // Even uniform bool should be at or below bitpacked baseline (512 B for 4096)
  size_t bitpacked_bytes = kN / 8;
  EXPECT_LE(encoded.size(), bitpacked_bytes + 32)  // allow small overhead for table header
      << "encoded=" << encoded.size() << " B, bitpacked=" << bitpacked_bytes << " B";
}

// ============================================================
// Idempotence: encode→decode→re-encode produces identical stream
// ============================================================

TEST(TsRangeCodecBoolTest, MultipleRoundtripsDeterministic) {
  constexpr size_t kN = 1000;
  bool original[kN];
  std::mt19937 rng(555);
  std::uniform_int_distribution<int> dist(0, 1);
  for (size_t i = 0; i < kN; ++i) original[i] = dist(rng);

  TsBufferBuilder enc1;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(original, kN), kN, &enc1, cfg));

  TsSliceGuard dec1;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      enc1.AsSlice(), kN, &dec1));

  auto *round1 = reinterpret_cast<const bool *>(dec1.data());

  // Re-encode the decoded output
  TsBufferBuilder enc2;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(round1, kN), kN, &enc2, cfg));

  // Both encodes should produce the same output (deterministic)
  EXPECT_EQ(enc1.AsStringView(), enc2.AsStringView());
}

// ============================================================
// Empty input
// ============================================================

TEST(TsRangeCodecBoolTest, EmptyInput) {
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  bool ok = TsRangeCodecBool::GetInstance().Compress(
      TSSlice{nullptr, 0}, 0, &encoded, cfg);
  EXPECT_TRUE(ok);

  TsSliceGuard decoded;
  ok = TsRangeCodecBool::GetInstance().Decompress(encoded.AsSlice(), 0, &decoded);
  EXPECT_TRUE(ok);
  EXPECT_EQ(decoded.size(), 0u);
}

// ============================================================
// Decode error: count mismatch
// ============================================================

TEST(TsRangeCodecBoolTest, DecodeWrongCountProducesGarbage) {
  // Range coder doesn't encode count — asking for more symbols "succeeds" but
  // produces garbage after the true content is exhausted.
  bool input[] = {true, false, true};
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input, 3), 3, &encoded, cfg));

  TsSliceGuard decoded;
  // Decode nominally succeeds (range coder can always generate more symbols)
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), 100, &decoded));
  ASSERT_EQ(decoded.size(), 100u * sizeof(bool));

  auto *out = reinterpret_cast<const bool *>(decoded.data());
  // First 3 should match (they come from the actual encoded state)
  for (int i = 0; i < 3; ++i) EXPECT_EQ(out[i], input[i]) << "i=" << i;
  // The entire 100-value output cannot match because 3 ≠ 100
  EXPECT_NE(decoded.size(), 3u * sizeof(bool));
}

// ============================================================
// Decode error: corrupted stream
// ============================================================

TEST(TsRangeCodecBoolTest, DecodeCorruptStream) {
  bool input[] = {true, false, true, true, false};
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input, 5), 5, &encoded, cfg));
  ASSERT_GT(encoded.size(), 1u);

  // Corrupt the shift byte (first byte)
  encoded.data()[0] = static_cast<char>(0);  // shift=0 is illegal

  TsSliceGuard decoded;
  EXPECT_FALSE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), 5, &decoded));
}

// ============================================================
// Decode error: truncated stream
// ============================================================

TEST(TsRangeCodecBoolTest, DecodeTruncatedStream) {
  bool input[] = {true, false, true, true, false, true};
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input, 6), 6, &encoded, cfg));
  ASSERT_GT(encoded.size(), 1u);

  // Truncated streams: if FreqTable header is intact, the decoder pads with
  // zeros and silently produces garbage.  Only verify that output is wrong
  // (not identical to original) and that it doesn't crash.
  bool any_failed = false;
  bool any_garbage = false;
  for (size_t trunc = 0; trunc < encoded.size(); ++trunc) {
    TsSliceGuard decoded;
    bool ok = TsRangeCodecBool::GetInstance().Decompress(
        TSSlice{encoded.data(), trunc}, 6, &decoded);
    if (!ok) {
      any_failed = true;
      continue;
    }
    ASSERT_EQ(decoded.size(), 6u * sizeof(bool));
    auto *out = reinterpret_cast<const bool *>(decoded.data());
    if (std::memcmp(out, input, 6 * sizeof(bool)) != 0) {
      any_garbage = true;
    }
  }
  EXPECT_TRUE(any_failed) << "At least trunc=0 should fail (empty header)";
  EXPECT_TRUE(any_garbage)
      << "Truncation past the header should produce wrong output";
}

TEST(TsRangeCodecBoolTest, DecodeEmptyEncodedNonZeroCount) {
  TsSliceGuard decoded;
  EXPECT_FALSE(TsRangeCodecBool::GetInstance().Decompress(
      TSSlice{nullptr, 0}, 5, &decoded));
}

// ============================================================
// Degenerate distributions: single flip in long run
// ============================================================

TEST(TsRangeCodecBoolTest, SingleTrueInLongFalseRun) {
  constexpr size_t kN = 1024;
  auto input = std::make_unique<bool[]>(kN);
  for (size_t i = 0; i < kN; ++i) input[i] = false;
  input[kN / 2] = true;  // single true at midpoint
  BoolRoundTrip(input.get(), kN);
}

TEST(TsRangeCodecBoolTest, SingleFalseInLongTrueRun) {
  constexpr size_t kN = 1024;
  auto input = std::make_unique<bool[]>(kN);
  for (size_t i = 0; i < kN; ++i) input[i] = true;
  input[0] = false;  // single false at start
  BoolRoundTrip(input.get(), kN);
}

// ============================================================
// Small batch sizes — exercise carry / byte alignment boundaries
// ============================================================

TEST(TsRangeCodecBoolTest, RoundtripSmallSizes) {
  for (size_t n = 2; n <= 16; ++n) {
    auto input = std::make_unique<bool[]>(n);
    for (size_t i = 0; i < n; ++i) input[i] = (i % 3 == 0);
    BoolRoundTrip(input.get(), n);
  }
}

TEST(TsRangeCodecBoolTest, RoundtripPowerOfTwoSizes) {
  // Range coder carry state machine is byte-oriented; test near 2^k boundaries
  for (size_t n : {17u, 31u, 32u, 33u, 63u, 64u, 65u, 255u, 256u, 257u}) {
    auto input = std::make_unique<bool[]>(n);
    std::mt19937 rng(static_cast<unsigned>(n));
    std::uniform_int_distribution<int> dist(0, 1);
    for (size_t i = 0; i < n; ++i) input[i] = dist(rng);
    BoolRoundTrip(input.get(), n);
  }
}

// ============================================================
// Consecutive encodes — encoder state isolation
// ============================================================

TEST(TsRangeCodecBoolTest, ConsecutiveEncodes) {
  bool batch1[] = {true, false, true, true, false};
  bool batch2[] = {false, false, true, false, true};

  TsCompressionConfig cfg;

  TsBufferBuilder enc1, enc2;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(batch1, 5), 5, &enc1, cfg));
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(batch2, 5), 5, &enc2, cfg));

  // Decode both independently — second encode must not corrupt first's output
  TsSliceGuard dec1, dec2;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      enc1.AsSlice(), 5, &dec1));
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      enc2.AsSlice(), 5, &dec2));

  auto *out1 = reinterpret_cast<const bool *>(dec1.data());
  auto *out2 = reinterpret_cast<const bool *>(dec2.data());
  for (int i = 0; i < 5; ++i) EXPECT_EQ(out1[i], batch1[i]) << "batch1 i=" << i;
  for (int i = 0; i < 5; ++i) EXPECT_EQ(out2[i], batch2[i]) << "batch2 i=" << i;
}

// ============================================================
// Decode with excess bytes — trailing garbage should be ignored
// ============================================================

TEST(TsRangeCodecBoolTest, DecodeWithExcessBytes) {
  bool input[] = {true, false, true, true};
  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input, 4), 4, &encoded, cfg));

  // Append trailing garbage
  encoded.push_back('\xFF');
  encoded.push_back('\x00');
  encoded.push_back('\xAB');

  // Decode should succeed — extra bytes after valid stream are ignored
  TsSliceGuard decoded;
  EXPECT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), 4, &decoded));
  auto *out = reinterpret_cast<const bool *>(decoded.data());
  for (int i = 0; i < 4; ++i) EXPECT_EQ(out[i], input[i]) << "i=" << i;
}

// ============================================================
// Large random input — stress test
// ============================================================

TEST(TsRangeCodecBoolTest, LargeInput100K) {
  constexpr size_t kN = 100000;
  auto input = std::make_unique<bool[]>(kN);
  std::mt19937 rng(999);
  std::bernoulli_distribution dist(0.3);
  for (size_t i = 0; i < kN; ++i) input[i] = dist(rng);

  TsBufferBuilder encoded;
  TsCompressionConfig cfg;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Compress(
      MakeSlice(input.get(), kN), kN, &encoded, cfg));

  TsSliceGuard decoded;
  ASSERT_TRUE(TsRangeCodecBool::GetInstance().Decompress(
      encoded.AsSlice(), kN, &decoded));

  auto *out = reinterpret_cast<const bool *>(decoded.data());
  ASSERT_EQ(decoded.size(), kN * sizeof(bool));
  for (size_t i = 0; i < kN; ++i) {
    ASSERT_EQ(out[i], input[i]) << "i=" << i;
  }

  // With P(true)=0.3, entropy ≈ 0.88 bits/symbol → ≈ 11 kB
  // Should be well under bitpacked (12.5 kB)
  size_t bitpacked_bytes = kN / 8;
  EXPECT_LT(encoded.size(), bitpacked_bytes)
      << "encoded=" << encoded.size() << " B, bitpacked=" << bitpacked_bytes << " B";
}
