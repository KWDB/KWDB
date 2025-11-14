#include "ts_compressor.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

#include "libkwdbts2.h"
#include "ts_coding.h"
#include "ts_bitmap.h"
#include "ts_compressor_impl.h"

template <class Compressor>
class TimestampCompressorTester : public ::testing::Test {};
using AllTsTypes = ::testing::Types<kwdbts::GorillaInt, kwdbts::GorillaIntV2<int64_t>, kwdbts::GorillaIntV2<int32_t>,
                                    kwdbts::Simple8BIntV2<int64_t>, kwdbts::Simple8BIntV2<int32_t>>;
template <class T>
struct TargetType {};

template <template <class> class T, class U>
struct TargetType<T<U>> {
  using Type = U;
};

template <>
struct TargetType<kwdbts::GorillaInt> {
  using Type = int64_t;
};

TYPED_TEST_CASE(TimestampCompressorTester, AllTsTypes);
TYPED_TEST(TimestampCompressorTester, CompressDecompress) {
  int count = 8000;
  int start = 0x12345678;
  using dtype = typename TargetType<TypeParam>::Type;
  const kwdbts::CompressorImpl &comp = TypeParam::GetInstance();
  {
    std::vector<dtype> ts(count);
    std::iota(ts.begin(), ts.end(), 1741851161);
    TSSlice data{reinterpret_cast<char *>(ts.data()), ts.size() * sizeof(dtype)};

    std::string out;
    ASSERT_TRUE(comp.Compress(data, count, &out));
    EXPECT_LT(out.size(), data.len);

    TSSlice compressed{out.data(), out.size()};
    std::string buf;
    ASSERT_TRUE(comp.Decompress(compressed, count, &buf));

    ASSERT_EQ(buf.size(), count * sizeof(dtype));
    dtype *p_ts = reinterpret_cast<dtype *>(buf.data());
    for (int i = 0; i < count; ++i) {
      ASSERT_EQ(ts[i], p_ts[i]) << "IDX: " << i;
    }
  }
  {
    count = 8000;
    std::vector<dtype> ts(count);
    ts[0] = start;
    std::default_random_engine rng{0};
    std::normal_distribution<double> d(0, 2000);
    for (int i = 1; i < count; ++i) {
      ts[i] = ts[i - 1] + d(rng);
    }

    TSSlice data{reinterpret_cast<char *>(ts.data()), ts.size() * sizeof(dtype)};

    std::string out;
    ASSERT_TRUE(comp.Compress(data, count, &out));
    EXPECT_LT(out.size(), data.len);

    TSSlice compressed{out.data(), out.size()};
    std::string buf;
    ASSERT_TRUE(comp.Decompress(compressed, count, &buf));

    ASSERT_EQ(buf.size(), count * sizeof(dtype));
    dtype *p_ts = reinterpret_cast<dtype *>(buf.data());
    for (int i = 0; i < count; ++i) {
      ASSERT_EQ(ts[i], p_ts[i]) << "IDX: " << i;
    }
  }
}

TYPED_TEST(TimestampCompressorTester, CompressDecompressOneRow) {
  using dtype = typename TargetType<TypeParam>::Type;
  int start = 0x12345678;
  const kwdbts::CompressorImpl &comp = TypeParam::GetInstance();
  std::vector<dtype> ts(1);
  ts[0] = start;
  TSSlice data{reinterpret_cast<char *>(ts.data()), ts.size() * sizeof(dtype)};

  std::string out;
  ASSERT_FALSE(comp.Compress(data, 1, &out));
}

template <class Compressor>
class IntegerCompressorTester : public ::testing::Test {};
using AllIntTypes = ::testing::Types<kwdbts::Simple8BInt<uint64_t>>;
TYPED_TEST_CASE(IntegerCompressorTester, AllIntTypes);
TYPED_TEST(IntegerCompressorTester, CompressDecompress) {

}


// The following are testing for simple8b only

template <class T>
static bool Simple8BEncode(const std::vector<T> &input, std::string *out) {
  kwdbts::ConcreateTsCompressor<kwdbts::Simple8BInt<T>>::GetInstance();
  const TSSlice plain{(char *)(input.data()), sizeof(T) * input.size()};
  return kwdbts::Simple8BInt<T>::GetInstance().Compress(plain, input.size(), out);
}

template <class T>
static bool Simple8BDecode(const std::string &data, int count, std::string *out) {
  kwdbts::ConcreateTsCompressor<kwdbts::Simple8BInt<T>>::GetInstance();
  const TSSlice s_data{(char *)data.data(), data.size()};
  return kwdbts::Simple8BInt<T>::GetInstance().Decompress(s_data, count, out);
}

template <class T>
static bool Simple8BV2Encode(const std::vector<T> &input, std::string *out) {
  kwdbts::ConcreateTsCompressor<kwdbts::Simple8BInt<T>>::GetInstance();
  const TSSlice plain{(char *)(input.data()), sizeof(T) * input.size()};
  return kwdbts::Simple8BIntV2<T>::GetInstance().Compress(plain, input.size(), out);
}

template <class T>
static bool Simple8BV2Decode(const std::string &data, int count, std::string *out) {
  kwdbts::ConcreateTsCompressor<kwdbts::Simple8BInt<T>>::GetInstance();
  const TSSlice s_data{(char *)data.data(), data.size()};
  return kwdbts::Simple8BIntV2<T>::GetInstance().Decompress(s_data, count, out);
}

alignas(64) static constexpr uint32_t ITEMWIDTH[16] = {0, 0, 1,  2,  3,  4,  5,  6,
                                                       7, 8, 10, 12, 15, 20, 30, 60};

alignas(64) static constexpr int32_t GROUPSIZE[16] = {240, 120, 60, 30, 20, 15, 12, 10,
                                                      8,   7,   6,  5,  4,  3,  2,  1};

static std::default_random_engine drng{0};
std::vector<uint64_t> GenBatch(int selector) {
  std::vector<uint64_t> res(GROUPSIZE[selector]);
  if (selector <= 1) {
    std::uniform_int_distribution<uint64_t> d(0, (1ULL << 60) - 1);
    uint64_t num = d(drng);
    for (int i = 0; i < GROUPSIZE[selector]; ++i) {
      res[i] = num;
    }
  } else {
    int width = ITEMWIDTH[selector];
    uint64_t min = selector == 2 ? 0 : 1ULL << (width - 1);
    std::uniform_int_distribution<uint64_t> d(min, (1ULL << width) - 1);
    for (int i = 0; i < GROUPSIZE[selector]; ++i) {
      res[i] = d(drng);
    }
  }
  return res;
}

TEST(Simple8B, EncodeOneNumberUint64) {
  std::vector<uint64_t> success{0x1,  0x3,   0x7,   0xF,    0x1F,    0x3F,       0x7F,
                                0xFF, 0x3FF, 0xFFF, 0x7FFF, 0xFFFFF, 0x3FFFFFFF, (1ULL << 60) - 1};
  std::vector<uint64_t> failed{1ULL << 60, 1ULL << 61, 1ULL << 62, 1ULL << 63};
  std::vector<std::vector<uint64_t>> datasets(success.size() + failed.size());
  for (int i = 0; i < success.size(); ++i) {
    datasets[i].push_back(success[i]);
  }
  for (int i = 0; i < failed.size(); ++i) {
    datasets[success.size() + i].push_back(failed[i]);
  }
  ASSERT_EQ(datasets.size(), success.size() + failed.size());
  for (int i = 0; i < datasets.size(); ++i) {
    std::string compressed;
    if (i < success.size()) {
      EXPECT_TRUE(Simple8BEncode(datasets[i], &compressed));
      ASSERT_EQ(compressed.size(), 8) << i;
      EXPECT_EQ(*reinterpret_cast<const uint64_t *>(compressed.data()) >> 60, 15);

      std::string data;
      EXPECT_TRUE(Simple8BDecode<uint64_t>(compressed, 1, &data));
      ASSERT_EQ(datasets[i].size() * 8, data.size());
      EXPECT_EQ(std::memcmp(datasets[i].data(), data.data(), data.size()), 0);
    } else {
      EXPECT_FALSE(Simple8BEncode(datasets[i], &compressed)) << i;
    }
  }
}

// max: 1<<60-1; min: -(1LL << 60) + 1
TEST(Simple8B, EncodeOneNumberInt64) {
  // TEST selector from 2 to 15
  struct MinMax {
    int64_t min, max;
  };
  std::vector<MinMax> minmax(16);
  for (int i = 2; i <= 15; ++i) {
    if (ITEMWIDTH[i] == 1) {
      minmax[i].min = 0;
      minmax[i].max = 0;
      continue;
    }
    minmax[i].max = (1LL << (ITEMWIDTH[i] - 1)) - 1;
    minmax[i].min = -minmax[i].max - 1;
  }

  std::map<int, std::vector<int64_t>> datas;
  for (int i = 2; i <= 15; ++i) {
    if (ITEMWIDTH[i] == 1) {
      datas[i].push_back(0);
      continue;
    }
    std::uniform_int_distribution<int64_t> dmax{minmax[i - 1].max + 1, minmax[i].max};
    std::uniform_int_distribution<int64_t> dmin{minmax[i].min, minmax[i - 1].min - 1};
    for (int j = 0; j < 10; ++j) {
      datas[i].push_back(dmax(drng));
      datas[i].push_back(dmin(drng));
    }
    // add corner case
    datas[i].insert(datas[i].end(),
                    {minmax[i - 1].max + 1, minmax[i].max, minmax[i].min, minmax[i - 1].min - 1});
  }

  for (int selector = 2; selector <= 15; ++selector) {
    for (auto i : datas[selector]) {
      std::string compressed;
      std::vector<int64_t> d(1);
      d[0] = i;
      EXPECT_TRUE(Simple8BEncode(d, &compressed)) << i;
      ASSERT_EQ(compressed.size(), 8) << i;
      EXPECT_EQ(*reinterpret_cast<const uint64_t *>(compressed.data()) >> 60, 15);

      std::string raw;
      EXPECT_TRUE(Simple8BDecode<int64_t>(compressed, 1, &raw));
      ASSERT_EQ(8, raw.size());
      EXPECT_EQ(std::memcmp(d.data(), raw.data(), raw.size()), 0) << i;
    }
  }

  // numbers cannot be encoded
  std::vector<int64_t> failed;
  int64_t lower_bound = minmax.back().max + 1;
  int64_t upper_bound = minmax.back().min - 1;
  std::uniform_int_distribution<int64_t> d_pos{lower_bound, std::numeric_limits<int64_t>::max()};
  std::uniform_int_distribution<int64_t> d_neg{std::numeric_limits<int64_t>::min(), upper_bound};
  for (int i = 0; i < 100; ++i) {
    failed.push_back(d_neg(drng));
    failed.push_back(d_pos(drng));
  }
  // corner cases;
  failed.push_back(lower_bound);
  failed.push_back(upper_bound);
  failed.push_back(std::numeric_limits<int64_t>::min());
  failed.push_back(std::numeric_limits<int64_t>::max());
  for (auto i : failed) {
    std::string compressed;
    std::vector<int64_t> d(1);
    d[0] = i;
    EXPECT_FALSE(Simple8BEncode(d, &compressed));
  }
}

TEST(Simple8B, EncodeSpecial) {
  size_t count = 119;
  std::vector<uint64_t> number(count, 1024);
  std::string out;
  ASSERT_TRUE(Simple8BEncode(number, &out));
  //   ASSERT_EQ(out.size(), 8);
  //   EXPECT_EQ(*reinterpret_cast<const uint64_t*>(out.data()) >> 60, 1);

  std::string v;
  EXPECT_TRUE(Simple8BDecode<uint64_t>(out, count, &v));
  ASSERT_EQ(number.size() * 8, v.size());
  EXPECT_EQ(std::memcmp(number.data(), v.data(), v.size()), 0);
}

TEST(Simple8BV2, EncodeSpecial) {
  size_t count = 119;
  std::vector<uint64_t> number(count, 1024);
  std::string out;
  ASSERT_TRUE(Simple8BV2Encode(number, &out));
  ASSERT_EQ(out.size(), 2 + 1 + 8);

  std::string v;
  EXPECT_TRUE(Simple8BV2Decode<uint64_t>(out, count, &v));
  ASSERT_EQ(number.size() * 8, v.size());
  EXPECT_EQ(std::memcmp(number.data(), v.data(), v.size()), 0);
}

TEST(Simple8B, OneBatch) {
  for (int selector = 0; selector < 16; ++selector) {
    auto v = GenBatch(selector);

    std::string out;
    ASSERT_TRUE(Simple8BEncode<uint64_t>(v, &out));
    ASSERT_EQ(out.size(), 8);
    std::string raw;
    ASSERT_TRUE(Simple8BDecode<uint64_t>(out, v.size(), &raw));
    ASSERT_EQ(raw.size(), v.size() * sizeof(uint64_t));
    ASSERT_EQ(std::memcmp(raw.data(), v.data(), raw.size()), 0);
  }
}

TEST(Simple8B, MultiBatch) {
  int nbatch = 100;
  std::uniform_int_distribution<int> d(0, 15);
  std::vector<uint64_t> data;
  for (int i = 0; i < nbatch; ++i) {
    auto v = GenBatch(d(drng));
    data.insert(data.end(), v.begin(), v.end());
  }
  std::string out;
  ASSERT_TRUE(Simple8BEncode<uint64_t>(data, &out));
  ASSERT_EQ(out.size(), 8 * nbatch);
  std::string raw;
  ASSERT_TRUE(Simple8BDecode<uint64_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(uint64_t));
  ASSERT_EQ(std::memcmp(raw.data(), data.data(), raw.size()), 0);
}

TEST(Simple8B, UnfullBatch) {
  for (int selector = 0; selector < 16; ++selector) {
    auto v = GenBatch(selector);
    if (v.size() > 1) {
      v.pop_back();
    }

    std::string out;
    ASSERT_TRUE(Simple8BEncode<uint64_t>(v, &out));
    std::string raw;
    ASSERT_TRUE(Simple8BDecode<uint64_t>(out, v.size(), &raw));
    ASSERT_EQ(raw.size(), v.size() * sizeof(uint64_t));
    ASSERT_EQ(std::memcmp(raw.data(), v.data(), raw.size()), 0);
  }
}

TEST(Simple8B, MultiUnfullBatch) {
  int nbatch = 50;
  std::uniform_int_distribution<int> d(0, 15);
  std::vector<uint64_t> data;
  for (int i = 0; i < nbatch; ++i) {
    int selector = d(drng);
    auto v = GenBatch(selector);
    if (v.size() > 1) {
      v.pop_back();
    }
    data.insert(data.end(), v.begin(), v.end());
  }
  std::string out;
  ASSERT_TRUE(Simple8BEncode<uint64_t>(data, &out));
  std::string raw;
  ASSERT_TRUE(Simple8BDecode<uint64_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(uint64_t));
  auto p = reinterpret_cast<uint64_t *>(raw.data());
  for (int i = 0; i < data.size(); ++i) {
    EXPECT_EQ(p[i], data[i]) << i;
  }
}

TEST(Simple8B, Bug1) {
  std::vector<uint64_t> data{1 << 4, 1 << 14, 1 << 14, 1 << 14, 1 << 11};
  std::string out;
  ASSERT_TRUE(Simple8BEncode<uint64_t>(data, &out));
  std::string raw;
  ASSERT_TRUE(Simple8BDecode<uint64_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(uint64_t));
  auto p = reinterpret_cast<uint64_t *>(raw.data());
  for (int i = 0; i < data.size(); ++i) {
    EXPECT_EQ(p[i], data[i]) << i;
  }
}

TEST(Simple8BV2, Bug1) {
  std::vector<uint64_t> data{1 << 4, 1 << 14, 1 << 14, 1 << 14, 1 << 11};
  std::string out;
  ASSERT_TRUE(Simple8BV2Encode<uint64_t>(data, &out));
  std::string raw;
  ASSERT_TRUE(Simple8BV2Decode<uint64_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(uint64_t));
  auto p = reinterpret_cast<uint64_t *>(raw.data());
  for (int i = 0; i < data.size(); ++i) {
    EXPECT_EQ(p[i], data[i]) << i;
  }
}

TEST(Simple8B, Bug2) {
  std::vector<int16_t> data{5079, 8477, 1760, 3220, 4244, 4374, 4749, 6412, 5194,
                            1631, 5734, 4339, 7815, 5237, 8829, 1245, 7099, 9217,
                            9274, 8227, 8881, 1461, 5528, 2946, 8872, 9103, 5161};

  std::string out;
  ASSERT_TRUE(Simple8BEncode<int16_t>(data, &out));
  std::string raw;
  ASSERT_TRUE(Simple8BDecode<int16_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(int16_t));
  auto p = reinterpret_cast<int16_t *>(raw.data());
  for (int i = 0; i < data.size(); ++i) {
    EXPECT_EQ(p[i], data[i]) << i;
  }
}

TEST(Simple8BV2, Bug2) {
  std::vector<int16_t> data{5079, 8477, 1760, 3220, 4244, 4374, 4749, 6412, 5194, 1631, 5734, 4339, 7815, 5237,
                            8829, 1245, 7099, 9217, 9274, 8227, 8881, 1461, 5528, 2946, 8872, 9103, 5161};

  std::string out;
  ASSERT_TRUE(Simple8BV2Encode<int16_t>(data, &out));
  std::string raw;
  ASSERT_TRUE(Simple8BV2Decode<int16_t>(out, data.size(), &raw));
  ASSERT_EQ(raw.size(), data.size() * sizeof(int16_t));
  auto p = reinterpret_cast<int16_t *>(raw.data());
  for (int i = 0; i < data.size(); ++i) {
    EXPECT_EQ(p[i], data[i]) << i;
  }
}

[[maybe_unused]]
static std::vector<int64_t> GenBatchV2(const std::vector<uint64_t> &dod_zigzag) {
  int delta = 0;
  std::vector<int64_t> data{0, 0};
  for (auto i : dod_zigzag) {
    int64_t dod = kwdbts::DecodeZigZag(i);
    delta += dod;
    data.push_back(data.back() + delta);
  }
  return data;
}

TEST(Simple8BV2, AllBranch) {

  struct TestCases {
    std::vector<int64_t> data;
    int expected_size;
  };
  std::vector<TestCases> cases;

  {
    std::vector<int64_t> data;
    data = {1, 2};
    cases.push_back({std::move(data), 2});

    data = {0, 0, 0};
    cases.push_back({std::move(data), 10});

    data = std::vector<int64_t>(10000, 1);
    cases.push_back({std::move(data), 2 + 8});

    data = std::vector<int64_t>(70000, 2);
    cases.push_back({std::move(data), 2 + 8 + 8});

    data = std::vector<int64_t>(100, 1);
    data.push_back(100);
    cases.push_back({std::move(data), 2 + 8 + 8});

    {
      data = {0, 0};
      int delta = 0;
      int dods[] = {-2, 1};
      for (int i = 0; i < 35; ++i) {
        int dod = dods[i % 2];
        data.push_back(data.back() + delta);
        delta += dod;
      }
      cases.push_back({std::move(data), -1});
    }
    {
      data = {0, 0};
      int delta = 0;
      for (int i = 0; i < 5; ++i) {
        int dod = 3;
        data.push_back(data.back() + delta);
        delta += dod;
      }
      data.push_back(data.back());
      cases.push_back({std::move(data), -1});
    }

    {
      data = GenBatchV2({8, 8, 8, 8, 8, 8, 9});
      cases.push_back({std::move(data), -1});
    }

    {
      std::vector<uint64_t> dod(15, 8);
      dod.push_back(9);
      data = GenBatchV2(std::move(dod));
      cases.push_back({std::move(data), -1});
    }
    {
      std::vector<int64_t> data;
      data.reserve(10000);
      for (int i = 0; i < 10000; ++i) {
        data.push_back(std::sin(i * 3.14 * 2 / 10000) * 10000);
      }
      for(int i = 2; i < data.size(); ++i) {
        int64_t d1 = data[i] - data[i - 1];
        int64_t d2 = data[i - 1] - data[i - 2];
        int64_t dod = d1 - d2;
      }
      cases.push_back({std::move(data), -1});
    }
    {
      for (int i = 1; i < 64; ++i) {
        std::vector<uint64_t> dod(i, 7);
        dod.push_back(9);
        std::vector<int64_t> data = GenBatchV2(std::move(dod));
        cases.push_back({std::move(data), -1});
      }
    }
  }
  for (const auto &c : cases) {
    std::string out;
    ASSERT_TRUE(Simple8BV2Encode(c.data, &out));
    if (c.expected_size != -1) {
      ASSERT_EQ(out.size(), c.expected_size);
    }
    std::string raw;
    ASSERT_TRUE(Simple8BV2Decode<int64_t>(out, c.data.size(), &raw));
    ASSERT_EQ(raw.size(), c.data.size() * sizeof(int64_t));
    auto p = reinterpret_cast<int64_t *>(raw.data());
    for (int i = 0; i < c.data.size(); ++i) {
      ASSERT_EQ(p[i], c.data[i]) << i;
    }
  }
}

template<class Type>
void Simple8BUintTester() {
  struct TestCases {
    std::vector<Type> data;
    int expected_size;
  };

  std::vector<TestCases> cases;

  {
    std::vector<Type> data;
    {
      std::vector<Type> data;
      data.reserve(10000);
      for (int i = 0; i < 35; ++i) {
        data.push_back(std::cos(i * 3.14 * 2 / 10000) * 10000 + 20000);
      }
      cases.push_back({std::move(data), -1});
    }
  }

  for (const auto &c : cases) {
    std::string out;
    ASSERT_TRUE(Simple8BV2Encode(c.data, &out));
    if (c.expected_size != -1) {
      ASSERT_EQ(out.size(), c.expected_size);
    }
    std::string raw;
    ASSERT_TRUE(Simple8BV2Decode<Type>(out, c.data.size(), &raw));
    ASSERT_EQ(raw.size(), c.data.size() * sizeof(Type));
    auto p = reinterpret_cast<Type *>(raw.data());
    for (int i = 0; i < c.data.size(); ++i) {
      ASSERT_EQ(p[i], c.data[i]) << i;
    }
  }
}

TEST(Simple8BV2, Uint) {
  Simple8BUintTester<uint32_t>();
  Simple8BUintTester<uint64_t>();
}

// Snappy

TEST(Snappy, CompressDecompress) {
  const kwdbts::CompressorImpl &comp = kwdbts::SnappyString::GetInstance();
  std::string s;
  s.resize(8192);
  char str[] = "WhAtEvEr!!!";
  for (int i = 0; i < s.size(); ++i) {
    s[i] = str[i % sizeof(str)];
  }
  std::string out;
  ASSERT_TRUE(comp.Compress({s.data(), s.size()}, 0, &out));

  EXPECT_LT(out.size(), s.size());

  std::string origin;
  ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, 0, &origin));
  size_t origin_size = comp.GetUncompressedSize({out.data(), out.size()}, 0);
  ASSERT_EQ(origin_size, s.size());

  EXPECT_EQ(origin, s);
}

// Float & Double
template<class T>
class FloatingPointCompressorTester : public ::testing::Test {};
using AllFloatingTypes = ::testing::Types<float, double>;
TYPED_TEST_CASE(FloatingPointCompressorTester, AllFloatingTypes);
TYPED_TEST(FloatingPointCompressorTester, CompressDecompress) {
  const kwdbts::CompressorImpl &comp = kwdbts::Chimp<TypeParam>::GetInstance();
  using utype = std::conditional_t<std::is_same_v<TypeParam, double>, uint64_t, uint32_t>;
  std::vector<std::vector<TypeParam>> c;
  {
    std::vector<TypeParam> data(8000);
    for (int i = 0; i < data.size(); ++i) {
      data[i] = 0.1 * i;
    }
    c.push_back(std::move(data));
  }
  {
    std::vector<TypeParam> data(1234);
    for (int i = 0; i < data.size(); ++i) {
      data[i] = 0.112345676545;
    }
    c.push_back(std::move(data));
  }
  {
    // just two number
    c.push_back({1.345, 1.234345995});
  }
  {
    // tail > 6;
    std::vector<TypeParam> data(3456);
    utype *p_data = reinterpret_cast<utype *>(data.data());
    for (int i = 0; i < data.size(); ++i) {
      p_data[i] = i << 10;
    }
    c.push_back(std::move(data));
  }
  {
    // tail < 6;
    std::vector<TypeParam> data(3456);
    utype *p_data = reinterpret_cast<utype *>(data.data());
    for (int i = 0; i < data.size(); ++i) {
      p_data[i] = i << 3;
    }
    c.push_back(std::move(data));
  }

  for (int i = 0; i < c.size(); ++i) {
    std::string out, plain;
    ASSERT_TRUE(comp.Compress(
        TSSlice{reinterpret_cast<char *>(c[i].data()), c[i].size() * sizeof(TypeParam)},
        c[i].size(), &out))
        << i;
    ASSERT_TRUE(comp.Decompress({out.data(), out.size()}, c[i].size(), &plain));
    EXPECT_EQ(plain.size(), c[i].size() * sizeof(TypeParam));
    TypeParam *raw = reinterpret_cast<TypeParam *>(plain.data());
    for (int j = 0; j < c[i].size(); ++j) {
      EXPECT_EQ(c[i][j], raw[j]) << i;
    }
  }
}


// bool

static bool BitPackingEnc(const std::vector<uint8_t> &data, std::string *out) {
  return kwdbts::BitPacking::GetInstance().Compress(TSSlice{(char *)data.data(), data.size()}, data.size(), out);
}

static bool BitPackingDec(std::string &data, size_t size, std::vector<uint8_t> *out) {
  std::string plain;
  out->resize(size);
  bool ok = kwdbts::BitPacking::GetInstance().Decompress(TSSlice{data.data(), data.size()}, size, &plain);
  std::copy(plain.begin(), plain.end(), out->begin());
  if (!ok) {
    return false;
  }
  return true;
}

TEST(Bool, CompressDecompress) {
  for (int i = 1; i < 1025; ++i) {
    std::default_random_engine drng(i);
    std::bernoulli_distribution d(0.25);
    std::vector<uint8_t> data(i);

    for (int j = 0; j < i; ++j) {
      data[j] = d(drng);
    }

    std::string out;
    ASSERT_TRUE(BitPackingEnc(data, &out));

    std::vector<uint8_t> x2;
    ASSERT_TRUE(BitPackingDec(out, data.size(), &x2));
    ASSERT_EQ(data.size(), x2.size());

    EXPECT_EQ(data, x2);
  }
}