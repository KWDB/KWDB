// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
//
#include <pthread.h>
#include <gtest/gtest.h>
#include <iostream>
#include <chrono>
#include <future>
#include <queue>
#include <vector>
#include "th_kwdb_dynamic_thread_pool.h"
#include "th_kwdb_operator_info.h"
#include "kwdb_type.h"
#include "lg_impl.h"

namespace kwdbts {

constexpr int MAX_THREAD_NUMBER = 63000;
KWDBDynamicThreadPool &g_thread_pool = KWDBDynamicThreadPool::GetThreadPool(MAX_THREAD_NUMBER);
kwdbContext_p g_pmain_ctxt = ContextManager::GetThreadContext();

struct UserArg {
  explicit UserArg(int index) : _index(index) {}
  int _index;
};

constexpr int A_LARGE_SIZE = {2000};

std::vector<uint> g_count_calls = std::vector<uint>(A_LARGE_SIZE, 0);
std::vector<UserArg*> g_args(A_LARGE_SIZE, nullptr);

void SubUserRoutineLarge(void *arg) {
  UserArg *uArg = static_cast<UserArg *>(arg);
  auto idx = uArg->_index;

  auto start_time = std::chrono::high_resolution_clock::now();
  std::chrono::seconds time_limit(5);

  while (time_limit.count() > 0) {
    char *pRes = new char[1024];
    auto end_time = std::chrono::high_resolution_clock::now();
    delete [] pRes;
    time_limit -= std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  ++g_count_calls[idx];
}

class TestKWDBDynamicThreadPoolStress : public ::testing::Test {
 public:
  TestKWDBDynamicThreadPoolStress() {}

  static void SetUpTestCase() {
    LOGGER.Init();
  }

  static void TearDownTestCase() {
    g_thread_pool.Stop();
  }
};

TEST_F(TestKWDBDynamicThreadPoolStress, Apply_a_large_size_of_threads) {
  KWDBOperatorInfo operator_info;
  operator_info.SetOperatorName("apply_a_large_size_of_threads_operator");
  operator_info.SetOperatorOwner("apply_a_large_size_of_threads_thread");
  time_t now;
  operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));

  ASSERT_FALSE(g_thread_pool.IsStop());

  std::vector<k_uint64> ids(A_LARGE_SIZE, 0);

  for (int i = 0; i < A_LARGE_SIZE; ++i) {
    g_args[i] = new UserArg(i);
  }

  for (int i = 0; i < A_LARGE_SIZE; ++i) {
    ids[i] = g_thread_pool.ApplyThread(SubUserRoutineLarge, g_args[i], &operator_info);
    ASSERT_TRUE(ids[i] >= KWDBDynamicThreadPool::thread_id_offset_);
  }

  for (int i = 0; i < A_LARGE_SIZE; ++i) {
    EXPECT_EQ(g_thread_pool.JoinThread(ids[i], 0), KStatus::SUCCESS);
  }

  std::deque<int> running_tasks;
  uint total_calls = 0;

  for (int i = 0; i < g_count_calls.size(); ++i) {
    total_calls += g_count_calls[i];

    if (g_count_calls[i] > 0) {
      continue;
    }
    running_tasks.push_back(i);
  }

  std::cout << "total calls: " << total_calls << std::endl;
  constexpr int TIMEOUT = 30000;
  constexpr int SLEEPTIME = 200;
  int last_idx = 0;
  int wait_time = 0;

  while (!running_tasks.empty() && wait_time < TIMEOUT) {
    wait_time += SLEEPTIME;

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEPTIME));
    auto idx = running_tasks.front();
    if ( g_count_calls[idx] ) {
      running_tasks.pop_front();
    } else if (idx != last_idx) {
      last_idx = idx;
    }
  }

  std::cout << "timeout: " << TIMEOUT / 1000 << " seconds" << std::endl;

  int count = {0};

  while (!running_tasks.empty()) {
    auto idx = running_tasks.front();
    count += g_count_calls[idx] ? 0 : 1;
    running_tasks.pop_front();
  }

  std::cout << "the total number of created system threads: " << g_thread_pool.GetThreadsNum() << std::endl;
  std::cout << "the total number of active threads: " << g_thread_pool.GetActiveThreadsNum() << std::endl;
  std::cout << count << " threads are not completed..." << std::endl;

#ifdef WITH_TESTS
  uint startThreadCnt = g_thread_pool.DebugGetEnterStartThreadCount();
  uint routineCnt = g_thread_pool.DebugGetEnterRoutineCount();
  uint userRoutineCnt = g_thread_pool.DebugGetUserRoutineCount();
  std::cout << "Enter start thread count is "<< startThreadCnt << std::endl;
  std::cout << "Enter routine count is "<< routineCnt << std::endl;
  std::cout << "Hookup user routine count is "<< userRoutineCnt << std::endl;
#endif

  EXPECT_EQ(count, 0);

  for (int i = 0; i < A_LARGE_SIZE; ++i) {
    delete g_args[i];
    g_args[i] = nullptr;
  }
}

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
}  // namespace kwdbts
