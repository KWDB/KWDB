#include "ts_compressor.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <numeric>
#include <cstring>
#include <random>
#include "libkwdbts2.h"

TEST(TimeStamp, Gorilla) {
  int count = 8000;
  int start = 0x12345678;
  {
    std::vector<int64_t> ts(count);
    std::iota(ts.begin(), ts.end(), 1741851161);
    TSSlice data{reinterpret_cast<char *>(ts.data()), ts.size() * 8};

    std::string out;
    ASSERT_TRUE(kwdbts::Gorilla::Compress(data, count, &out));
    EXPECT_LT(out.size(), data.len);

    TSSlice compressed{out.data(), out.size()};
    std::string buf;
    ASSERT_TRUE(kwdbts::Gorilla::Decompress(compressed, count, &buf));

    ASSERT_EQ(buf.size(), count * 8);
    int64_t *p_ts = reinterpret_cast<int64_t *>(buf.data());
    for (int i = 0; i < count; ++i) {
      ASSERT_EQ(ts[i], p_ts[i]) << "IDX: " << i;
    }
  }
  {
    count = 8000;
    std::vector<int64_t> ts(count);
    ts[0] = start;
    std::default_random_engine rng{0};
    std::normal_distribution<double> d(0, 2000);
    for (int i = 1; i < count; ++i) {
      ts[i] = ts[i - 1] + d(rng);
    }

    TSSlice data{reinterpret_cast<char *>(ts.data()), ts.size() * 8};

    std::string out;
    ASSERT_TRUE(kwdbts::Gorilla::Compress(data, count, &out));
    EXPECT_LT(out.size(), data.len);

    TSSlice compressed{out.data(), out.size()};
    std::string buf;
    ASSERT_TRUE(kwdbts::Gorilla::Decompress(compressed, count, &buf));

    ASSERT_EQ(buf.size(), count * 8);
    int64_t *p_ts = reinterpret_cast<int64_t *>(buf.data());
    for (int i = 0; i < count; ++i) {
      ASSERT_EQ(ts[i], p_ts[i]) << "IDX: " << i;
    }
  }
}