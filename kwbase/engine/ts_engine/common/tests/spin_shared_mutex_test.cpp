#include "spin_shared_mutex.h"

#include <gtest/gtest.h>

#include <atomic>
#include <shared_mutex>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace kwdbts;

// ==================== 基础功能测试 ====================

TEST(BasicTest, LockUnlock) {
  spin_shared_mutex mtx;
  mtx.lock();
  mtx.unlock();
  SUCCEED();
}

TEST(BasicTest, SharedLockUnlock) {
  spin_shared_mutex mtx;
  mtx.lock_shared();
  mtx.unlock_shared();
  SUCCEED();
}

TEST(BasicTest, MultipleSharedLocks) {
  spin_shared_mutex mtx;
  mtx.lock_shared();
  mtx.lock_shared();
  mtx.lock_shared();
  mtx.unlock_shared();
  mtx.unlock_shared();
  mtx.unlock_shared();
  SUCCEED();
}

// ==================== try_lock 测试 ====================

TEST(TryLockTest, TryLockSuccessWhenFree) {
  spin_shared_mutex mtx;
  EXPECT_TRUE(mtx.try_lock());
  mtx.unlock();
}

TEST(TryLockTest, TryLockFailWhenWriteLocked) {
  spin_shared_mutex mtx;
  mtx.lock();
  EXPECT_FALSE(mtx.try_lock());
  mtx.unlock();
}

TEST(TryLockTest, TryLockFailWhenSharedLocked) {
  spin_shared_mutex mtx;
  mtx.lock_shared();
  EXPECT_FALSE(mtx.try_lock());
  mtx.unlock_shared();
}

TEST(TryLockTest, TryLockSharedSuccessWhenFree) {
  spin_shared_mutex mtx;
  EXPECT_TRUE(mtx.try_lock_shared());
  mtx.unlock_shared();
}

TEST(TryLockTest, TryLockSharedFailWhenWriteLocked) {
  spin_shared_mutex mtx;
  mtx.lock();
  EXPECT_FALSE(mtx.try_lock_shared());
  mtx.unlock();
}

TEST(TryLockTest, TryLockSharedSuccessWithExistingShared) {
  spin_shared_mutex mtx;
  mtx.lock_shared();
  EXPECT_TRUE(mtx.try_lock_shared());
  mtx.unlock_shared();
  mtx.unlock_shared();
}

// ==================== 互斥性测试 ====================

TEST(MutualExclusionTest, WriteBlocksWrite) {
  spin_shared_mutex mtx;
  std::atomic<bool> write_acquired{false};
  std::atomic<bool> second_write_started{false};

  mtx.lock();

  std::thread t([&]() {
    second_write_started = true;
    mtx.lock();
    write_acquired = true;
    mtx.unlock();
  });

  std::this_thread::sleep_for(50ms);
  // 第二个写锁应该还在等待
  EXPECT_TRUE(second_write_started);
  EXPECT_FALSE(write_acquired);

  mtx.unlock();
  t.join();

  EXPECT_TRUE(write_acquired);
}

TEST(MutualExclusionTest, WriteBlocksRead) {
  spin_shared_mutex mtx;
  std::atomic<bool> read_acquired{false};
  std::atomic<bool> read_started{false};

  mtx.lock();

  std::thread t([&]() {
    read_started = true;
    mtx.lock_shared();
    read_acquired = true;
    mtx.unlock_shared();
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_TRUE(read_started);
  EXPECT_FALSE(read_acquired);

  mtx.unlock();
  t.join();

  EXPECT_TRUE(read_acquired);
}

TEST(MutualExclusionTest, ReadBlocksWrite) {
  spin_shared_mutex mtx;
  std::atomic<bool> write_acquired{false};
  std::atomic<bool> write_started{false};

  mtx.lock_shared();

  std::thread t([&]() {
    write_started = true;
    mtx.lock();
    write_acquired = true;
    mtx.unlock();
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_TRUE(write_started);
  EXPECT_FALSE(write_acquired);

  mtx.unlock_shared();
  t.join();

  EXPECT_TRUE(write_acquired);
}

TEST(MutualExclusionTest, MultipleReadsDontBlock) {
  spin_shared_mutex mtx;
  std::atomic<int> active_reads{0};
  std::atomic<int> max_concurrent_reads{0};
  const int num_threads = 10;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&]() {
      mtx.lock_shared();
      int current = ++active_reads;
      int expected_max = max_concurrent_reads.load();
      while (current > expected_max && !max_concurrent_reads.compare_exchange_weak(expected_max, current)) {
      }
      std::this_thread::sleep_for(10ms);
      --active_reads;
      mtx.unlock_shared();
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(max_concurrent_reads, num_threads);
}

// ==================== 多线程并发测试 ====================

TEST(ConcurrencyTest, ReaderWriterConcurrency) {
  spin_shared_mutex mtx;
  std::atomic<int> shared_data{0};
  std::atomic<int> write_count{0};
  std::atomic<int> read_count{0};
  std::atomic<bool> stop{false};

  const int num_writers = 4;
  const int num_readers = 8;

  std::vector<std::thread> writers;
  writers.reserve(num_writers);
  for (int i = 0; i < num_writers; i++) {
    writers.emplace_back([&]() {
      while (!stop) {
        std::lock_guard<spin_shared_mutex> lock(mtx);
        shared_data.fetch_add(1);
        write_count++;
        std::this_thread::sleep_for(1ms);
      }
    });
  }

  std::vector<std::thread> readers;
  readers.reserve(num_readers);
  for (int i = 0; i < num_readers; i++) {
    readers.emplace_back([&]() {
      while (!stop) {
        std::shared_lock<spin_shared_mutex> lock(mtx);
        [[maybe_unused]] volatile int value = shared_data.load();
        read_count++;
        std::this_thread::sleep_for(1ms);
      }
    });
  }

  std::this_thread::sleep_for(100ms);
  stop = true;

  for (auto& t : writers) t.join();
  for (auto& t : readers) t.join();

  std::cout << "\n  Writes: " << write_count.load() << ", Reads: " << read_count.load() << std::endl;

  EXPECT_GT(write_count, 0);
  EXPECT_GT(read_count, 0);
}

TEST(ConcurrencyTest, WriteExclusivity) {
  spin_shared_mutex mtx;
  std::atomic<int> active_writers{0};
  std::atomic<bool> error{false};
  const int num_threads = 8;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&]() {
      for (int j = 0; j < 100; j++) {
        mtx.lock();
        int current = ++active_writers;
        if (current != 1) {
          error = true;
        }
        std::this_thread::yield();
        --active_writers;
        mtx.unlock();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_FALSE(error);
  EXPECT_EQ(active_writers, 0);
}

TEST(ConcurrencyTest, LockUpgrade) {
  spin_shared_mutex mtx;
  int data = 0;

  mtx.lock_shared();
  int read_value = data;
  mtx.unlock_shared();

  mtx.lock();
  data = read_value + 1;
  mtx.unlock();

  EXPECT_EQ(data, 1);
}

TEST(ConcurrencyTest, LockDowngrade) {
  spin_shared_mutex mtx;
  int data = 42;

  mtx.lock();
  data = 100;
  mtx.unlock();

  mtx.lock_shared();
  int value = data;
  mtx.unlock_shared();

  EXPECT_EQ(value, 100);
}

TEST(ConcurrencyTest, HighContention) {
  spin_shared_mutex mtx;
  std::atomic<int> counter{0};
  const int num_threads = 16;
  const int iterations = 1000;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&]() {
      for (int j = 0; j < iterations; j++) {
        if (j % 10 == 0) {
          // 10% 写操作
          std::lock_guard<spin_shared_mutex> lock(mtx);
          counter.fetch_add(1);
        } else {
          // 90% 读操作
          std::shared_lock<spin_shared_mutex> lock(mtx);
          [[maybe_unused]] volatile int value = counter.load();
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  int expected = num_threads * (iterations / 10);
  EXPECT_EQ(counter, expected);
}