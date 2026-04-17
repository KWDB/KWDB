#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "data_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_compressor.h"
#include "ts_sliceguard.h"

template <class T>
struct Generator {
  T operator()(size_t i) const { return i; }
};

template <>
struct Generator<float> {
  float operator()(size_t i) const { return std::sqrt(i); }
};

template <>
struct Generator<double> {
  float operator()(size_t i) const { return std::sqrt(i); }
};

template <class>
struct GetCompressorAlg {};

template <>
struct GetCompressorAlg<int32_t> {
  static const kwdbts::EncodeAlgo Alg = kwdbts::EncodeAlgo::kSimple8B_s32;
};

template <>
struct GetCompressorAlg<int64_t> {
  static const kwdbts::EncodeAlgo Alg = kwdbts::EncodeAlgo::kSimple8B_s64;
};

template <>
struct GetCompressorAlg<float> {
  static const kwdbts::EncodeAlgo Alg = kwdbts::EncodeAlgo::kChimp_32;
};

template <>
struct GetCompressorAlg<double> {
  static const kwdbts::EncodeAlgo Alg = kwdbts::EncodeAlgo::kChimp_64;
};

template <class T>
class CompressorManagerTester : public ::testing::Test {};
// using AllDataTypes = ::testing::Types<int32_t, int64_t, float, double>;
using AllDataTypes = ::testing::Types<int64_t, int32_t, double, float>;
TYPED_TEST_CASE(CompressorManagerTester, AllDataTypes);
TYPED_TEST(CompressorManagerTester, TwoLevelCompress) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  auto sz = sizeof(TypeParam);

  Generator<TypeParam> gen;
  int count = 10333;
  std::vector<TypeParam> vec(count);
  for (int i = 0; i < vec.size(); ++i) {
    vec[i] = gen(i);
  }
  kwdbts::TsBufferBuilder out;
  kwdbts::TsSliceGuard raw;
  ASSERT_TRUE(inst.CompressData({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, nullptr, vec.size(), &out,
                                GetCompressorAlg<TypeParam>::Alg, kwdbts::CompressAlgo::kSnappy, 1));
  ASSERT_TRUE(inst.DecompressData(out.GetBuffer(), nullptr, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sz);
  EXPECT_EQ(std::memcmp(vec.data(), raw.data(), raw.size()), 0);

  kwdbts::TsBitmap bitmap(count);
  for (int i = 0; i < count; ++i) {
    bitmap[i] = i % 7 ? kwdbts::kValid : kwdbts::kNull;
  }
  out.clear();
  ASSERT_TRUE(inst.CompressData({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, &bitmap, vec.size(), &out,
                    GetCompressorAlg<TypeParam>::Alg, kwdbts::CompressAlgo::kSnappy, 1));
  ASSERT_TRUE(inst.DecompressData(out.GetBuffer(), &bitmap, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sz);
  const TypeParam* pdata = reinterpret_cast<TypeParam*>(raw.data());
  for (int i = 0; i < count; ++i) {
    if (bitmap[i] != kwdbts::kValid) continue;
    EXPECT_EQ(vec[i], pdata[i]);
  }
}

TEST(Bitmap, CompressDecompress) {
  const auto& mgr = kwdbts::CompressorManager::GetInstance();
  kwdbts::TsBitmap bm(997);
  for (kwdbts::DataFlags f : {kwdbts::kValid, kwdbts::kNull, kwdbts::kNone}) {
    bm.SetAll(f);

    kwdbts::TsBufferBuilder output;
    ASSERT_EQ(mgr.CompressBitmap(&bm, &output), true);
    ASSERT_EQ(output.size(), 1);

    TSSlice input{output.data(), output.size()};
    std::unique_ptr<kwdbts::TsBitmapBase> bitmap;
    uint64_t bytes_consumed;
    ASSERT_EQ(mgr.DecompressBitmap(input, &bitmap, bm.GetCount(), &bytes_consumed), true);

    ASSERT_EQ(bytes_consumed, output.size());
    ASSERT_EQ(bitmap->GetCount(), bm.GetCount());
    ASSERT_EQ(bitmap->Count(f), 997);

    switch (f) {
      case kwdbts::kValid:
        ASSERT_NE(dynamic_cast<kwdbts::TsUniformBitmap<kwdbts::kValid>*>(bitmap.get()), nullptr);
        ASSERT_EQ(output[0], static_cast<char>(kwdbts::BitmapType::kAllValid));
        break;
      case kwdbts::kNull:
        ASSERT_NE(dynamic_cast<kwdbts::TsUniformBitmap<kwdbts::kNull>*>(bitmap.get()), nullptr);
        ASSERT_EQ(output[0], static_cast<char>(kwdbts::BitmapType::kAllNull));
        break;
      case kwdbts::kNone:
        ASSERT_NE(dynamic_cast<kwdbts::TsUniformBitmap<kwdbts::kNone>*>(bitmap.get()), nullptr);
        ASSERT_EQ(output[0], static_cast<char>(kwdbts::BitmapType::kAllNone));
        break;
    }
  }

  kwdbts::TsBitmap bm2(997);
  std::default_random_engine drng{0};
  std::vector<kwdbts::DataFlags> flags(bm2.GetCount());
  std::uniform_int_distribution<int> udist(0, 2);

  static kwdbts::DataFlags all_flags[] = {kwdbts::kValid, kwdbts::kNull, kwdbts::kNone};
  for (int i = 0; i < bm2.GetCount(); ++i) {
    flags[i] = all_flags[udist(drng)];
    bm2[i] = flags[i];
  }

  kwdbts::TsBufferBuilder output;
  ASSERT_TRUE(mgr.CompressBitmap(&bm2, &output));
  ASSERT_EQ(output.size(), 1 + kwdbts::TsBitmap::GetBitmapLen(997));

  TSSlice input{output.data(), output.size()};
  std::unique_ptr<kwdbts::TsBitmapBase> bitmap;
  uint64_t bytes_consumed;
  ASSERT_TRUE(mgr.DecompressBitmap(input, &bitmap, bm2.GetCount(), &bytes_consumed));

  ASSERT_EQ(bytes_consumed, output.size());
  ASSERT_EQ(bitmap->GetCount(), bm2.GetCount());
  for (int i = 0; i < bm2.GetCount(); ++i) {
    ASSERT_EQ(bitmap->At(i), flags[i]);
  }

  ASSERT_NE(dynamic_cast<kwdbts::TsBitmap*>(bitmap.get()), nullptr);
}



template <class T>
std::vector<T> GenerateMockData(size_t count) {
  std::vector<T> data(count);
  std::iota(data.begin(), data.end(), 0);
  return data;
}

std::unique_ptr<kwdbts::TsBitmapBase> GenerateMockBitmap(size_t count, double p_valid) {
  if (p_valid == 0) {
    return std::make_unique<kwdbts::TsUniformBitmap<kwdbts::kNone>>(count);
  }

  if (p_valid == 1) {
    return std::make_unique<kwdbts::TsUniformBitmap<kwdbts::kValid>>(count);
  }

  std::default_random_engine drng{0};
  std::bernoulli_distribution bdist(p_valid);
  auto bitmap = std::make_unique<kwdbts::TsBitmap>(count);
  for (int i = 0; i < count; ++i) {
    (*bitmap)[i] = bdist(drng) ? kwdbts::kValid : kwdbts::kNull;
  }
  return bitmap;
}

TYPED_TEST(CompressorManagerTester, Null_Compressions) {
  int count = 1000;
  const auto& inst = kwdbts::CompressorManager::GetInstance();

  auto raw_data = GenerateMockData<TypeParam>(count);
  TSSlice raw_slice{reinterpret_cast<char*>(raw_data.data()), raw_data.size() * sizeof(TypeParam)};
  static double probabilities[] = {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

  for (auto p : probabilities) {
    auto bitmap = GenerateMockBitmap(raw_data.size(), p);

    kwdbts::TsBufferBuilder compressed;
    bool ok = inst.CompressData(raw_slice, bitmap.get(), count, &compressed, GetCompressorAlg<TypeParam>::Alg,
                                kwdbts::CompressAlgo::kPlain, 0);
    ASSERT_TRUE(ok);
    ASSERT_GE(compressed.size(), sizeof(uint32_t));
    const size_t payload_size = compressed.size() - sizeof(uint32_t);
    const bool first_stage_enabled = kwdbts::EngineOptions::compress_stage != 0 &&
                                     kwdbts::EngineOptions::compress_stage != 2;
    const size_t max_payload_size = first_stage_enabled ? sizeof(TypeParam) * bitmap->GetValidCount() : raw_slice.len;
    ASSERT_LE(payload_size, max_payload_size);

    kwdbts::TsSliceGuard out;
    inst.DecompressData(compressed.GetBuffer(), bitmap.get(), count, &out);

    ASSERT_EQ(out.size(), raw_slice.len);
    const TypeParam* pdata = reinterpret_cast<TypeParam*>(out.data());
    for (int i = 0; i < count; ++i) {
      if (bitmap->At(i) == kwdbts::kValid) {
        EXPECT_EQ(raw_data[i], pdata[i]);
      }
    }
  }
}

TEST(CompressorManager, ChimpAlgorithmMatchesFloatingType) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  AttributeInfo attr_info{};
  attr_info.encode_algo = roachpb::ENCODE_ALGO_CHIMP;

  auto [float_alg, float_general_alg] = inst.GetAlgorithm(DATATYPE::FLOAT, attr_info);
  auto [double_alg, double_general_alg] = inst.GetAlgorithm(DATATYPE::DOUBLE, attr_info);
  (void)float_general_alg;
  (void)double_general_alg;

  EXPECT_EQ(float_alg, kwdbts::EncodeAlgo::kChimp_32);
  EXPECT_EQ(double_alg, kwdbts::EncodeAlgo::kChimp_64);
}

TEST(CompressorManager, InvalidCompressionLevelFallsBackToDefault) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();

  std::vector<int32_t> vec(4096);
  std::iota(vec.begin(), vec.end(), 0);
  kwdbts::TsBufferBuilder compressed;
  ASSERT_TRUE(inst.CompressData({reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(int32_t)}, nullptr,
                                vec.size(), &compressed, kwdbts::EncodeAlgo::kSimple8B_V2_s32,
                                kwdbts::CompressAlgo::kZstd, 99));

  kwdbts::TsSliceGuard raw;
  ASSERT_TRUE(inst.DecompressData(compressed.GetBuffer(), nullptr, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sizeof(int32_t));
  EXPECT_EQ(std::memcmp(vec.data(), raw.data(), raw.size()), 0);

  std::string varchar(2048, 'x');
  kwdbts::TsBufferBuilder varchar_compressed;
  ASSERT_TRUE(inst.CompressVarchar({varchar.data(), varchar.size()}, &varchar_compressed, kwdbts::CompressAlgo::kZstd,
                                   -3));
  kwdbts::TsSliceGuard varchar_raw;
  ASSERT_TRUE(inst.DecompressVarchar(varchar_compressed.GetBuffer(), &varchar_raw));
  EXPECT_EQ(varchar_raw.AsStringView(), varchar);
}

TEST(CompressorManager, UnspecifiedCompressionLevelUsesClusterSetting) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  const auto old_level = kwdbts::EngineOptions::compress_level;

  std::vector<int32_t> vec(4096);
  std::iota(vec.begin(), vec.end(), 0);
  std::string varchar(4096, 'x');

  struct TestCase {
    kwdbts::CompressLevel cluster_level;
    int explicit_level;
  };

  const std::vector<TestCase> cases = {
      {kwdbts::CompressLevel::LOW, roachpb::COMPRESS_LEVEL_LOW},
      {kwdbts::CompressLevel::MEDIUM, roachpb::COMPRESS_LEVEL_MEDIUM},
      {kwdbts::CompressLevel::HIGH, roachpb::COMPRESS_LEVEL_HIGH},
  };

  for (const auto& tc : cases) {
    kwdbts::EngineOptions::compress_level = tc.cluster_level;

    kwdbts::TsBufferBuilder expected_numeric;
    kwdbts::TsBufferBuilder actual_numeric;
    ASSERT_TRUE(inst.CompressData({reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(int32_t)}, nullptr,
                                  vec.size(), &expected_numeric, kwdbts::EncodeAlgo::kSimple8B_V2_s32,
                                  kwdbts::CompressAlgo::kZstd, tc.explicit_level));
    ASSERT_TRUE(inst.CompressData({reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(int32_t)}, nullptr,
                                  vec.size(), &actual_numeric, kwdbts::EncodeAlgo::kSimple8B_V2_s32,
                                  kwdbts::CompressAlgo::kZstd, roachpb::COMPRESS_LEVEL_UNSPECIFIED));
    ASSERT_EQ(expected_numeric.size(), actual_numeric.size());
    EXPECT_EQ(std::memcmp(expected_numeric.data(), actual_numeric.data(), expected_numeric.size()), 0);

    kwdbts::TsBufferBuilder expected_varchar;
    kwdbts::TsBufferBuilder actual_varchar;
    ASSERT_TRUE(inst.CompressVarchar({varchar.data(), varchar.size()}, &expected_varchar, kwdbts::CompressAlgo::kZstd,
                                     tc.explicit_level));
    ASSERT_TRUE(inst.CompressVarchar({varchar.data(), varchar.size()}, &actual_varchar, kwdbts::CompressAlgo::kZstd,
                                     roachpb::COMPRESS_LEVEL_UNSPECIFIED));
    ASSERT_EQ(expected_varchar.size(), actual_varchar.size());
    EXPECT_EQ(std::memcmp(expected_varchar.data(), actual_varchar.data(), expected_varchar.size()), 0);
  }

  kwdbts::EngineOptions::compress_level = old_level;
}

TEST(CompressorManager, UnspecifiedCompressionAlgorithmUsesCurrentClusterSetting) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  const auto old_stage = kwdbts::EngineOptions::compress_stage;
  const auto old_algorithm = kwdbts::EngineOptions::compression_algorithm;

  AttributeInfo attr_info{};
  attr_info.encode_algo = roachpb::ENCODE_ALGO_UNSPECIFIED;
  attr_info.compress_algo = roachpb::COMPRESS_ALGO_UNSPECIFIED;

  kwdbts::EngineOptions::compress_stage = 0;
  kwdbts::EngineOptions::compression_algorithm = kwdbts::CompressAlgo::kLz4;
  auto [first_stage0, second_stage0] = inst.GetAlgorithm(DATATYPE::INT32, attr_info);
  auto [default_first_stage0, default_second_stage0] = inst.GetDefaultAlgorithm(DATATYPE::INT32);
  EXPECT_EQ(first_stage0, kwdbts::EncodeAlgo::kSimple8B_V2_s32);
  EXPECT_EQ(second_stage0, kwdbts::CompressAlgo::kPlain);
  EXPECT_EQ(default_first_stage0, kwdbts::EncodeAlgo::kSimple8B_V2_s32);
  EXPECT_EQ(default_second_stage0, kwdbts::CompressAlgo::kPlain);

  kwdbts::EngineOptions::compress_stage = 3;
  kwdbts::EngineOptions::compression_algorithm = kwdbts::CompressAlgo::kZstd;
  auto [first_stage3, second_stage3] = inst.GetAlgorithm(DATATYPE::INT32, attr_info);
  auto [default_first_stage3, default_second_stage3] = inst.GetDefaultAlgorithm(DATATYPE::INT32);
  EXPECT_EQ(first_stage3, kwdbts::EncodeAlgo::kSimple8B_V2_s32);
  EXPECT_EQ(second_stage3, kwdbts::CompressAlgo::kZstd);
  EXPECT_EQ(default_first_stage3, kwdbts::EncodeAlgo::kSimple8B_V2_s32);
  EXPECT_EQ(default_second_stage3, kwdbts::CompressAlgo::kZstd);

  auto [ts_first, ts_second] = inst.GetAlgorithm(DATATYPE::TIMESTAMP64, attr_info);
  auto [ts_default_first, ts_default_second] = inst.GetDefaultAlgorithm(DATATYPE::TIMESTAMP64);
  EXPECT_EQ(ts_first, kwdbts::EncodeAlgo::kSimple8B_V2_s64);
  EXPECT_EQ(ts_second, kwdbts::CompressAlgo::kPlain);
  EXPECT_EQ(ts_default_first, kwdbts::EncodeAlgo::kSimple8B_V2_s64);
  EXPECT_EQ(ts_default_second, kwdbts::CompressAlgo::kPlain);

  kwdbts::EngineOptions::compress_stage = old_stage;
  kwdbts::EngineOptions::compression_algorithm = old_algorithm;
}

TEST(CompressorManager, VarcharCompressionFallsBackToPlainWhenPayloadGrows) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();

  std::string input = "abc";
  kwdbts::TsBufferBuilder compressed;
  ASSERT_TRUE(inst.CompressVarchar({input.data(), input.size()}, &compressed, kwdbts::CompressAlgo::kLz4, 1));

  TSSlice header{compressed.data(), compressed.size()};
  uint16_t alg = 0;
  kwdbts::GetFixed16(&header, &alg);
  EXPECT_EQ(alg, static_cast<uint16_t>(kwdbts::CompressAlgo::kPlain));

  kwdbts::TsSliceGuard raw;
  ASSERT_TRUE(inst.DecompressVarchar(compressed.GetBuffer(), &raw));
  EXPECT_EQ(raw.AsStringView(), input);
}

TEST(CompressorManager, VarcharCompressionHonorsCompressStage) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  const auto old_stage = kwdbts::EngineOptions::compress_stage;

  std::string input(4096, 'x');
  kwdbts::TsBufferBuilder plain;
  ASSERT_TRUE(inst.CompressVarchar({input.data(), input.size()}, &plain, kwdbts::CompressAlgo::kPlain,
                                   roachpb::COMPRESS_LEVEL_UNSPECIFIED));

  for (uint8_t stage : {static_cast<uint8_t>(0), static_cast<uint8_t>(1)}) {
    kwdbts::EngineOptions::compress_stage = stage;
    kwdbts::TsBufferBuilder compressed;
    ASSERT_TRUE(inst.CompressVarchar({input.data(), input.size()}, &compressed, kwdbts::CompressAlgo::kZstd,
                                     roachpb::COMPRESS_LEVEL_UNSPECIFIED));
    ASSERT_EQ(compressed.size(), plain.size());
    EXPECT_EQ(std::memcmp(compressed.data(), plain.data(), plain.size()), 0);

    kwdbts::TsSliceGuard raw;
    ASSERT_TRUE(inst.DecompressVarchar(compressed.GetBuffer(), &raw));
    EXPECT_EQ(raw.AsStringView(), input);
  }

  for (uint8_t stage : {static_cast<uint8_t>(2), static_cast<uint8_t>(3)}) {
    kwdbts::EngineOptions::compress_stage = stage;
    kwdbts::TsBufferBuilder compressed;
    ASSERT_TRUE(inst.CompressVarchar({input.data(), input.size()}, &compressed, kwdbts::CompressAlgo::kZstd,
                                     roachpb::COMPRESS_LEVEL_UNSPECIFIED));

    TSSlice header{compressed.data(), compressed.size()};
    uint16_t alg = 0;
    kwdbts::GetFixed16(&header, &alg);
    EXPECT_EQ(alg, static_cast<uint16_t>(kwdbts::CompressAlgo::kZstd));

    kwdbts::TsSliceGuard raw;
    ASSERT_TRUE(inst.DecompressVarchar(compressed.GetBuffer(), &raw));
    EXPECT_EQ(raw.AsStringView(), input);
  }

  kwdbts::EngineOptions::compress_stage = old_stage;
}

