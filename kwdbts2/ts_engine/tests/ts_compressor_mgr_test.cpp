#include <gtest/gtest.h>

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

TEST(CompressorManager, TwoLevelCompress) {
  const auto& inst = kwdbts::CompressorManager::GetInstance();
  auto comp =
      inst.GetCompressor(DATATYPE::INT64, kwdbts::TsCmpAlg::kSimple8B, kwdbts::GenCmpAlg::kSnappy);

  int count = 10333;
  std::vector<int64_t> vec(count);
  for (int i = 0; i < vec.size(); ++i) {
    vec[i] = i;
  }
  std::string out, raw;
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * 8}, nullptr,
                            vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, nullptr, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * 8);
  EXPECT_EQ(std::memcmp(vec.data(), raw.data(), raw.size()), 0);

  kwdbts::TsBitmap bitmap(count);
  for (int i = 0; i < count; ++i) {
    bitmap[i] = i % 7 ? kwdbts::kValid : kwdbts::kNull;
  }
  ASSERT_TRUE(comp.Compress({reinterpret_cast<char*>(vec.data()), vec.size() * 8}, &bitmap,
                            vec.size(), &out));
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, &bitmap, vec.size(), &raw));
  ASSERT_EQ(raw.size(), vec.size() * 8);
  const int64_t* pdata = reinterpret_cast<int64_t*>(raw.data());
  for (int i = 0; i < count; ++i) {
    if (bitmap[i] != kwdbts::kValid) continue;
    EXPECT_EQ(vec[i], pdata[i]);
  }
}
