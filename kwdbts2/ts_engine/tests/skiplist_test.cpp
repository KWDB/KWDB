#include <gtest/gtest.h>
#include <set>
#include <random>
#include <thread>
#include <mutex>
#include <vector>
#include "sl_set.h"

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

static constexpr auto COUNT = 100000ull;

class SkipListTest : public testing::Test {
public:
  static uint64_t Random() {
    return distribution()(mt19937());
  }

private:
  static std::mt19937 & mt19937() {
    thread_local std::mt19937 mt19937(std::random_device{}());
    return mt19937;
  }
  static std::uniform_int_distribution<uint64_t> & distribution() {
    thread_local std::uniform_int_distribution<uint64_t> distribution(0, 1000000000);
    return distribution;
  }
};

TEST_F(SkipListTest, SimpleInsert) {
  sl_set<uint64_t> skiplist;
  std::set<uint64_t> stl_set;
  for (size_t i = 0; i < COUNT; ++i) {
    auto rnd = Random();
    skiplist.insert(rnd);
    stl_set.insert(rnd);
  }
  ASSERT_EQ(skiplist.size(), stl_set.size());
  auto it1 = skiplist.begin();
  for (auto it2 = stl_set.begin(); it1 != skiplist.end() && it2 != stl_set.end(); ++it1, ++it2) {
    EXPECT_EQ(*it1, *it2);
  }
}

TEST_F(SkipListTest, SimpleQuery) {
  sl_set<uint64_t> skiplist;
  std::set<uint64_t> stl_set;
  for (size_t i = 0; i < COUNT; ++i) {
    auto rnd = Random();
    skiplist.insert(rnd);
    stl_set.insert(rnd);
  }
  ASSERT_EQ(skiplist.size(), stl_set.size());
  for (int i = 0; i < COUNT; ++i) {
    auto rnd = Random();
    EXPECT_EQ(skiplist.find(rnd) != skiplist.end(), stl_set.find(rnd) != stl_set.end());
  }
}

TEST_F(SkipListTest, SimpleRangeQuery) {
  sl_set<uint64_t> skiplist;
  std::set<uint64_t> stl_set;
  for (size_t i = 0; i < COUNT; ++i) {
    auto rnd = Random();
    skiplist.insert(rnd);
    stl_set.insert(rnd);
  }
  ASSERT_EQ(skiplist.size(), stl_set.size());
  for (size_t i = 0; i < COUNT / 10; ++i) {
    auto rnd = Random();
    if (auto it1 = skiplist.seek(rnd); it1 == skiplist.end()) {
      auto last = *skiplist.rbegin();
      EXPECT_EQ(last, *stl_set.rbegin());
      EXPECT_GT(rnd, last);
    } else if (*it1 == rnd) {
      EXPECT_TRUE(stl_set.find(rnd) != stl_set.end());
    } else {
      EXPECT_TRUE(stl_set.find(rnd) == stl_set.end());
      auto it2 = stl_set.find(*it1);
      EXPECT_TRUE(it2 != stl_set.end());
      if (it2 != stl_set.begin()) {
        EXPECT_EQ(*--it1, *--it2);
      }
    }
  }
}

template <size_t N> struct ReturnType;
template <> struct ReturnType<1> { using type = uint8_t; };
template <> struct ReturnType<2> { using type = uint16_t; };
template <> struct ReturnType<4> { using type = uint32_t; };
template <> struct ReturnType<8> { using type = uint64_t; };

template <size_t N>
struct BigEndianUInt {
  const char *key;
  static constexpr uint64_t zero = 0;
  static_assert(N > 0 && N <= 8 && (N & N - 1) == 0);

  BigEndianUInt() : key(reinterpret_cast<const char *>(&zero)) {}
  explicit BigEndianUInt(const char *k) : key(k) {}
  bool operator<(const BigEndianUInt rhs) const {
    return memcmp(key, rhs.key, N) < 0;
  }
  bool operator==(const BigEndianUInt rhs) const {
    return memcmp(key, rhs.key, N) == 0;
  }
  bool operator>(BigEndianUInt rhs) const {
    return !(*this == rhs) && !(*this < rhs);
  }
  bool operator!=(BigEndianUInt rhs) const {
    return !(*this == rhs);
  }

  typename ReturnType<N>::type BigEndianValue() const {
    return *reinterpret_cast<const typename ReturnType<N>::type *>(key);
  }

  typename ReturnType<N>::type LittleEndianValue() const {
    return be64toh(*reinterpret_cast<const typename ReturnType<N>::type *>(key));
  }
};

using BigEndianUInt8 = BigEndianUInt<1>;
using BigEndianUInt16 = BigEndianUInt<2>;
using BigEndianUInt32 = BigEndianUInt<4>;
using BigEndianUInt64 = BigEndianUInt<8>;

TEST_F(SkipListTest, CustomComparatorInsert) {
  sl_set<BigEndianUInt64> skiplist;
  std::set<uint64_t> nums;
  for (size_t i = 0; i < COUNT; ++i) {
    nums.insert(Random());
  }
  std::vector<uint64_t> storage;
  storage.reserve(nums.size());
  for (auto num : nums) {
    storage.emplace_back(htobe64(num));
    skiplist.insert(BigEndianUInt64{reinterpret_cast<const char *>(&storage.back())});
  }
  ASSERT_EQ(skiplist.size(), nums.size());
  auto it1 = skiplist.begin();
  for (auto it2 = nums.begin(); it1 != skiplist.end() && it2 != nums.end(); ++it1, ++it2) {
    auto decoded_num = (*it1).LittleEndianValue();
    EXPECT_EQ(decoded_num, *it2);
  }
}

TEST_F(SkipListTest, CustomComparatorQuery) {
  sl_set<BigEndianUInt64> skiplist;
  std::set<uint64_t> nums;
  for (size_t i = 0; i < COUNT; ++i) {
    nums.insert(Random());
  }
  std::vector<uint64_t> storage;
  storage.reserve(nums.size());
  for (auto num : nums) {
    storage.emplace_back(htobe64(num));
    skiplist.insert(BigEndianUInt64{reinterpret_cast<const char *>(&storage.back())});
  }
  ASSERT_EQ(skiplist.size(), nums.size());
  for (int i = 0; i < COUNT; ++i) {
    uint64_t rnd = Random();
    uint64_t rnd_be = htobe64(rnd);
    EXPECT_EQ(skiplist.find(BigEndianUInt64{reinterpret_cast<const char *>(&rnd_be)}) != skiplist.end(), nums.find(rnd) != nums.end());
  }
}

TEST_F(SkipListTest, CustomComparatorRangeQuery) {
  sl_set<BigEndianUInt64> skiplist;
  std::set<uint64_t> nums;
  for (size_t i = 0; i < COUNT; ++i) {
    nums.insert(Random());
  }
  std::vector<uint64_t> storage;
  storage.reserve(nums.size());
  for (auto num : nums) {
    storage.emplace_back(htobe64(num));
    skiplist.insert(BigEndianUInt64{reinterpret_cast<const char *>(&storage.back())});
  }
  ASSERT_EQ(skiplist.size(), nums.size());
  for (size_t i = 0; i < COUNT / 10; ++i) {
    uint64_t rnd = Random();
    uint64_t rnd_be = htobe64(rnd);
    auto it1 = skiplist.seek(BigEndianUInt64{reinterpret_cast<const char *>(&rnd_be)});
    if (it1 == skiplist.end()) {
      auto last = *skiplist.rbegin();
      EXPECT_EQ(last.LittleEndianValue(), *nums.rbegin());
      EXPECT_GT(rnd, last.LittleEndianValue());
    } else {
      auto num_le = (*it1).LittleEndianValue();
      if (num_le == rnd) {
        EXPECT_TRUE(nums.find(rnd) != nums.end());
      } else {
        EXPECT_TRUE(nums.find(rnd) == nums.end());
        auto it2 = nums.find(num_le);
        EXPECT_TRUE(it2 != nums.end());
        if (it2 != nums.begin()) {
          auto num2_le = (*--it1).LittleEndianValue();
          EXPECT_EQ(num2_le, *--it2);
        }
      }
    }
  }
}

TEST_F(SkipListTest, ConcurrentInsert) {
  sl_set<uint64_t> skiplist;
  std::set<uint64_t> stl_set;
  std::mutex mtx;
  std::vector<std::thread> workers;
  auto thread_count = std::thread::hardware_concurrency();
  workers.reserve(thread_count);
  for (size_t i = 0; i < thread_count; ++i) {
    workers.emplace_back([&skiplist, &stl_set, &mtx] {
      for (size_t j = 0; j < COUNT; ++j) {
        auto rnd = Random();
        skiplist.insert(rnd);
        std::lock_guard lock(mtx);
        stl_set.insert(rnd);
      }
    });
  }
  std::for_each(workers.begin(), workers.end(), [] (std::thread & w) { w.join(); });
  ASSERT_EQ(skiplist.size(), stl_set.size());
  auto it1 = skiplist.begin();
  for (auto it2 = stl_set.begin(); it1 != skiplist.end() && it2 != stl_set.end(); ++it1, ++it2) {
    EXPECT_EQ(*it1, *it2);
  }
}

TEST_F(SkipListTest, ConcurrentInsertLockFree) {
  sl_set<size_t> skiplist;
  std::vector<std::thread> workers;
  auto thread_count = std::thread::hardware_concurrency();
  workers.reserve(thread_count);
  for (size_t i = 0; i < thread_count; ++i) {
    workers.emplace_back([&skiplist] (size_t thread_idx) {
      for (size_t j = 0; j < COUNT; ++j) {
        skiplist.insert(thread_idx * COUNT + j);
      }
    }, i);
  }
  std::for_each(workers.begin(), workers.end(), [] (std::thread &t) { t.join(); });
  ASSERT_EQ(skiplist.size(), thread_count * COUNT);
  size_t num = 0;
  for (auto it = skiplist.begin(); it != skiplist.end(); ++it, ++num) {
    EXPECT_EQ(*it, num);
  }
}

void ConcurrentReadWriteTest(size_t producer, size_t consumer) {
  assert(producer != 0 && consumer != 0);
  sl_set<size_t> skiplist;
  std::vector<std::thread> readers, writers;
  readers.reserve(consumer);
  writers.reserve(producer);
  std::atomic<size_t> write_done{0};
  // reader
  for (size_t i = 0; i < consumer; ++i) {
    readers.emplace_back([&skiplist, &write_done, producer] {
      while (write_done.load(std::memory_order_acquire) != producer) {
        size_t total = 0, last = 0;
        for (auto it = skiplist.begin(); it != skiplist.end(); ++it) {
          EXPECT_LT(*it, COUNT * producer);
          if (likely(last != 0)) {
            EXPECT_LT(last, *it);
          }
          last = *it;
          ++total;
        }
        EXPECT_LE(total, COUNT * producer);
      }
      // write done
      size_t start = 0;
      std::for_each(skiplist.begin(), skiplist.end(), [&start] (size_t v) {
        EXPECT_EQ(v, start++);
      });
    });
  }
  // writer
  for (size_t i = 0; i < producer; ++i) {
    writers.emplace_back([&skiplist, &write_done] (size_t thread_idx) {
      for (size_t j = 0; j < COUNT; ++j) {
        skiplist.insert(thread_idx * COUNT + j);
      }
      write_done.fetch_add(1, std::memory_order_release);
    }, i);
  }
  // wait
  std::for_each(writers.begin(), writers.end(), [] (std::thread &t) { t.join(); });
  std::for_each(readers.begin(), readers.end(), [] (std::thread &t) { t.join(); });
}

TEST_F(SkipListTest, SPSC) {
  ConcurrentReadWriteTest(1, 1);
}

TEST_F(SkipListTest, SPMC) {
  auto thread_count = std::thread::hardware_concurrency();
  ConcurrentReadWriteTest(1, thread_count / 4 + 2);
}

TEST_F(SkipListTest, MPSC) {
  auto thread_count = std::thread::hardware_concurrency();
  ConcurrentReadWriteTest(thread_count / 2 + 2, 1);
}

TEST_F(SkipListTest, MPMC) {
  auto thread_count = std::thread::hardware_concurrency();
  ConcurrentReadWriteTest(thread_count / 2 + 2, thread_count / 4 + 2);
}

