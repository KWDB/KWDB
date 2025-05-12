#include <gtest/gtest.h>

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
#include "ts_compressor.h"

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
class CompressorManagerTester : public ::testing::Test {
};
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
  std::string out, raw;
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, nullptr,
                            vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, nullptr, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sz);
  EXPECT_EQ(std::memcmp(vec.data(), raw.data(), raw.size()), 0);

  kwdbts::TsBitmap bitmap(count);
  for (int i = 0; i < count; ++i) {
    bitmap[i] = i % 7 ? kwdbts::kValid : kwdbts::kNull;
  }
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * sz}, &bitmap,
                            vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, &bitmap, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * sz);
  const TypeParam* pdata = reinterpret_cast<TypeParam*>(raw.data());
  for (int i = 0; i < count; ++i) {
    if (bitmap[i] != kwdbts::kValid) continue;
    EXPECT_EQ(vec[i], pdata[i]);
  }
}