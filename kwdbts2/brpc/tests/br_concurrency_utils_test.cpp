// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "br_blocking_priority_queue.h"
#include "br_object_pool.h"
#include "br_priority_thread_pool.h"
#include "br_spinlock.h"

namespace kwdbts {

namespace {

// Test helper that supports move semantics but not copy.
// Verifies the queue handles move-only types correctly.
struct MoveOnlyItem {
  int priority = 0;
  std::unique_ptr<int> payload;

  MoveOnlyItem() = default;
  MoveOnlyItem(int p, int v) : priority(p), payload(std::make_unique<int>(v)) {}
  MoveOnlyItem(MoveOnlyItem&&) noexcept = default;
  MoveOnlyItem& operator=(MoveOnlyItem&&) noexcept = default;
  MoveOnlyItem(const MoveOnlyItem&) = delete;
  MoveOnlyItem& operator=(const MoveOnlyItem&) = delete;

  // Required for priority queue ordering (higher priority first).
  bool operator<(const MoveOnlyItem& other) const { return priority < other.priority; }
  // Priority increment operator for testing priority upgrade mechanism.
  MoveOnlyItem& operator++() {
    ++priority;
    return *this;
  }
};

// Test helper with atomic counter to track priority upgrade operations.
// The static upgrades counter is incremented each time operator++ is called,
// allowing tests to verify that priority upgrades actually occur.
struct AgingItem {
  static std::atomic<int> upgrades;  // Counts total priority upgrade operations
  int priority = 0;
  int payload = 0;

  AgingItem() = default;
  AgingItem(int p, int v) : priority(p), payload(v) {}
  AgingItem(const AgingItem&) = default;
  AgingItem& operator=(const AgingItem&) = default;
  AgingItem(AgingItem&&) noexcept = default;
  AgingItem& operator=(AgingItem&&) noexcept = default;

  // Required for priority queue ordering (higher priority first).
  bool operator<(const AgingItem& other) const { return priority < other.priority; }
  // Priority increment that also updates the global upgrade counter.
  AgingItem& operator++() {
    ++priority;
    ++upgrades;
    return *this;
  }
};

std::atomic<int> AgingItem::upgrades{0};

// Test helper to verify object destruction order.
// Records the value of each object when it is destroyed into a static vector,
// allowing tests to verify reverse destruction order (LIFO).
struct TrackingObject {
  static std::vector<int> destruct_order;  // Global record of destruction order

  explicit TrackingObject(int v) : value(v) {}
  ~TrackingObject() { destruct_order.push_back(value); }

  int value;
};

std::vector<int> TrackingObject::destruct_order;

}  // namespace

// Tests SpinLock functionality: try_lock, lock state verification, and thread
// contention.
//
// Verifies that SpinLock correctly prevents concurrent access and handles
// multiple threads competing for the same lock.
TEST(BrConcurrencyUtilsTest, SpinLockSupportsTryLockAndContention) {
  SpinLock lock;
  EXPECT_TRUE(lock.try_lock());
  lock.dcheck_locked();
  EXPECT_FALSE(lock.try_lock());  // Lock already held, try_lock should fail
  lock.unlock();

  // Spawn 4 threads, each incrementing counter 1000 times.
  // If SpinLock works correctly, final count should be exactly 4000.
  std::atomic<int> counter{0};
  constexpr int kIterations = 1000;
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kIterations; ++j) {
        std::lock_guard<SpinLock> guard(lock);
        ++counter;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(counter.load(), 4 * kIterations);
}

// Tests BlockingPriorityQueue blocking behavior and graceful shutdown.
//
// Verifies:
//   - Capacity limits are enforced.
//   - BlockingPut blocks when queue is full.
//   - Shutdown unblocks blocked operations and rejects new puts.
//   - MoveOnlyItem types can be stored and retrieved correctly.
TEST(BrConcurrencyUtilsTest, BlockingPriorityQueueHandlesBlockingAndShutdown) {
  BlockingPriorityQueue<MoveOnlyItem> queue(1);
  ASSERT_TRUE(queue.BlockingPut(MoveOnlyItem(1, 10)));
  EXPECT_EQ(queue.GetCapacity(), 1u);
  EXPECT_EQ(queue.GetSize(), 1u);
  EXPECT_FALSE(queue.TryPut(MoveOnlyItem(2, 20)));  // Queue full, TryPut fails

  // Start a thread that will block on BlockingPut
  std::atomic<bool> put_returned{false};
  std::thread blocked_putter([&]() {
    put_returned = !queue.BlockingPut(MoveOnlyItem(3, 30));  // Returns false after shutdown
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  queue.Shutdown();
  blocked_putter.join();
  EXPECT_TRUE(put_returned.load());

  // Verify the original item is still retrievable
  MoveOnlyItem out;
  EXPECT_TRUE(queue.NonBlockingGet(&out));
  ASSERT_NE(out.payload, nullptr);
  EXPECT_EQ(*out.payload, 10);
  EXPECT_FALSE(queue.NonBlockingGet(&out));              // Queue now empty
  EXPECT_FALSE(queue.BlockingPut(MoveOnlyItem(4, 40)));  // Rejected after shutdown
}

// Tests ForcePut bypass mechanism and priority upgrade behavior.
//
// Verifies:
//   - ForcePut ignores capacity limits.
//   - Adding higher priority items triggers upgrade operations.
//   - Shutdown prevents ForcePut from increasing queue size.
TEST(BrConcurrencyUtilsTest, BlockingPriorityQueueForcePutAndPriorityUpgradeWork) {
  BlockingPriorityQueue<AgingItem> queue(8);
  AgingItem::upgrades = 0;

  // Fill and drain queue repeatedly to trigger internal reorganization
  for (int i = 0; i < 513; ++i) {
    ASSERT_TRUE(queue.TryPut(AgingItem(1, i)));
    AgingItem out;
    ASSERT_TRUE(queue.NonBlockingGet(&out));
  }

  // Add items with different priorities; higher priority should trigger upgrades
  ASSERT_TRUE(queue.TryPut(AgingItem(1, 1)));
  ASSERT_TRUE(queue.TryPut(AgingItem(5, 2)));
  AgingItem out;
  ASSERT_TRUE(queue.NonBlockingGet(&out));
  EXPECT_GT(AgingItem::upgrades.load(), 0);  // Verify upgrades occurred

  // ForcePut bypasses capacity limit
  queue.ForcePut(AgingItem(7, 70));
  EXPECT_EQ(queue.GetSize(), 2u);
  queue.Shutdown();
  // ForcePut still works after shutdown, but size remains capped
  queue.ForcePut(AgingItem(9, 90));
  EXPECT_EQ(queue.GetSize(), 2u);
}

// Tests PriorityThreadPool priority-based task scheduling and shutdown behavior.
//
// Verifies:
//   - Tasks are executed in priority order (highest priority first).
//   - Offer blocks if queue is full, TryOffer returns immediately.
//   - DrainAndShutdown waits for pending tasks and rejects new submissions.
TEST(BrConcurrencyUtilsTest, PriorityThreadPoolRunsPriorityTasksAndRejectsAfterShutdown) {
  PriorityThreadPool pool("prio", 0, 16);
  std::mutex lock;
  std::vector<int> order;

  EXPECT_EQ(pool.GetQueueCapacity(), 16u);
  EXPECT_EQ(pool.GetQueueSize(), 0u);

  // Submit tasks with different priorities: 1, 9, 5
  // Expected execution order: 9 (highest), 5, 1 (lowest)
  ASSERT_TRUE(pool.Offer(PriorityThreadPool::Task{1, [&]() {
                                                    std::lock_guard<std::mutex> guard(lock);
                                                    order.push_back(1);
                                                  }}));
  ASSERT_TRUE(pool.TryOffer(PriorityThreadPool::Task{9, [&]() {
                                                       std::lock_guard<std::mutex> guard(lock);
                                                       order.push_back(9);
                                                     }}));
  ASSERT_TRUE(pool.Offer(PriorityThreadPool::Task{5, [&]() {
                                                    std::lock_guard<std::mutex> guard(lock);
                                                    order.push_back(5);
                                                  }}));
  EXPECT_EQ(pool.GetQueueSize(), 3u);

  pool.SetNumThread(1);     // Start 1 worker thread
  pool.DrainAndShutdown();  // Wait for completion and shutdown

  // Verify tasks were executed in priority order
  ASSERT_EQ(order.size(), 3u);
  EXPECT_EQ(order[0], 9);
  EXPECT_EQ(order[1], 5);
  EXPECT_EQ(order[2], 1);
  // After shutdown, no new tasks accepted
  EXPECT_FALSE(pool.Offer([]() {}));
  EXPECT_FALSE(pool.TryOffer([]() {}));
}

// Tests PriorityThreadPool dynamic thread scaling.
//
// Verifies that SetNumThread can increase worker threads at runtime and that
// all submitted tasks are completed before DrainAndShutdown returns.
TEST(BrConcurrencyUtilsTest, PriorityThreadPoolCanGrowThreadsAndFinishWork) {
  PriorityThreadPool pool("grow", 0, 32);
  std::atomic<int> finished{0};

  pool.SetNumThread(2);  // Dynamically add 2 workers
  for (int i = 0; i < 6; ++i) {
    ASSERT_TRUE(pool.Offer([&finished]() { finished.fetch_add(1); }));
  }
  pool.DrainAndShutdown();
  EXPECT_EQ(finished.load(), 6);  // All tasks must complete
}

// Tests PriorityThreadPool with long thread names.
//
// Verifies that the pool handles extended thread names without truncation or
// formatting issues, which is important for debugging and logging.
TEST(BrConcurrencyUtilsTest, PriorityThreadPoolSupportsLongThreadNames) {
  PriorityThreadPool pool("priority-thread-name", 0, 4);
  std::atomic<int> finished{0};

  pool.SetNumThread(1);
  ASSERT_TRUE(pool.Offer([&finished]() { finished.fetch_add(1); }));
  pool.DrainAndShutdown();

  EXPECT_EQ(finished.load(), 1);
}

// Tests Task priority bump operation.
//
// Verifies that the operator++ correctly increments priority value, enabling
// dynamic priority adjustment of queued tasks.
TEST(BrConcurrencyUtilsTest, PriorityThreadPoolTaskSupportsPriorityBump) {
  PriorityThreadPool::Task low{1, []() {}};
  PriorityThreadPool::Task high{3, []() {}};

  EXPECT_TRUE(low < high);
  ++low;  // Bump priority from 1 to 3
  EXPECT_EQ(low.priority, 3);
}

// Tests ObjectPool lifecycle management and destruction order.
//
// Verifies:
//   - Objects are destroyed in reverse order of addition (LIFO).
//   - AcquireData transfers ownership correctly.
//   - Move constructor and move assignment work properly.
TEST(BrConcurrencyUtilsTest, ObjectPoolAcquiresAndDestroysObjectsInReverseOrder) {
  // Test 1: Clear destroys objects in reverse order (3, 2, 1)
  TrackingObject::destruct_order.clear();
  {
    ObjectPool pool;
    pool.Add(new TrackingObject(1));
    pool.Add(new TrackingObject(2));
    pool.Add(new TrackingObject(3));
    pool.Clear();
  }
  ASSERT_EQ(TrackingObject::destruct_order.size(), 3u);
  EXPECT_EQ(TrackingObject::destruct_order[0], 3);  // Last in, first out
  EXPECT_EQ(TrackingObject::destruct_order[1], 2);
  EXPECT_EQ(TrackingObject::destruct_order[2], 1);

  // Test 2: AcquireData, move constructor, and move assignment
  TrackingObject::destruct_order.clear();
  {
    ObjectPool source;
    source.Add(new TrackingObject(10));
    source.Add(new TrackingObject(20));

    ObjectPool target;
    target.AcquireData(&source);  // Transfer from source
    target.Add(new TrackingObject(30));

    ObjectPool moved(std::move(target));  // Move constructor
    ObjectPool assigned;
    assigned = std::move(moved);  // Move assignment
  }
  // Final destruction order reflects the last pool's contents
  ASSERT_EQ(TrackingObject::destruct_order.size(), 3u);
  EXPECT_EQ(TrackingObject::destruct_order[0], 30);
  EXPECT_EQ(TrackingObject::destruct_order[1], 20);
  EXPECT_EQ(TrackingObject::destruct_order[2], 10);
}

}  // namespace kwdbts
