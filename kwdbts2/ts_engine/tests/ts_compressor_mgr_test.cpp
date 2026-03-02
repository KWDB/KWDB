#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

#include "data_type.h"
#include "libkwdbts2.h"
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
  static const kwdbts::TsCompAlg Alg = kwdbts::TsCompAlg::kSimple8B_s32;
};

template <>
struct GetCompressorAlg<int64_t> {
  static const kwdbts::TsCompAlg Alg = kwdbts::TsCompAlg::kSimple8B_s64;
};

template <>
struct GetCompressorAlg<float> {
  static const kwdbts::TsCompAlg Alg = kwdbts::TsCompAlg::kChimp_32;
};

template <>
struct GetCompressorAlg<double> {
  static const kwdbts::TsCompAlg Alg = kwdbts::TsCompAlg::kChimp_64;
};

template <class T>
class CompressorManagerTester : public ::testing::Test {};
// using AllDataTypes = ::testing::Types<int32_t, int64_t, float, double>;
using AllDataTypes = ::testing::Types<int64_t, int32_t, double, float>;
TYPED_TEST_CASE(CompressorManagerTester, AllDataTypes);
TYPED_TEST(CompressorManagerTester, TwoLevelCompress) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  auto comp = inst.GetCompressor(GetCompressorAlg<TypeParam>::Alg, kwdbts::GenCompAlg::kSnappy);
  auto sz = sizeof(TypeParam);

  Generator<TypeParam> gen;
  int count = 10333;
  std::vector<TypeParam> vec(count);
  for (int i = 0; i < vec.size(); ++i) {
    vec[i] = gen(i);
  }
  kwdbts::TsBufferBuilder out;
  kwdbts::TsSliceGuard raw;
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, nullptr, vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, nullptr, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sz);
  EXPECT_EQ(std::memcmp(vec.data(), raw.data(), raw.size()), 0);

  kwdbts::TsBitmap bitmap(count);
  for (int i = 0; i < count; ++i) {
    bitmap[i] = i % 7 ? kwdbts::kValid : kwdbts::kNull;
  }
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, &bitmap, vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, &bitmap, vec.size(), &raw));
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
                                kwdbts::GenCompAlg::kPlain);
    ASSERT_TRUE(ok);
    ASSERT_LE(compressed.size() - 4 /* header */, sizeof(TypeParam) * bitmap->GetValidCount());

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