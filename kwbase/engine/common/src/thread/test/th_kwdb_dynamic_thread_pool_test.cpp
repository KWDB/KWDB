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

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include "th_kwdb_dynamic_thread_pool.h"
#include "th_kwdb_operator_info.h"
#include "kwdb_type.h"
#include "lg_impl.h"


namespace kwdbts {

k_int32 max_threads_num = 2;
k_int32 min_threads_num = 1;
KWDBDynamicThreadPool &kwdb_thread_pool_test =
    KWDBDynamicThreadPool::GetThreadPool(max_threads_num, min_threads_num);
KThreadID threads_id[6];
k_int32 failnum = 0;
kwdbContext_p thread_pool_ctx_ = ContextManager::GetThreadContext();
std::atomic<int> task_count(0);

void UserRoutineSleep(size_t number) {
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

void UserRoutine(void *) {
  size_t thread_id_hash =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  UserRoutineSleep(thread_id_hash);
  task_count++;
}

void UserRoutineLoop(void *) {
  size_t thread_id_hash =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  do {
    UserRoutineSleep(thread_id_hash);
  } while (!kwdb_thread_pool_test.IsCancel());
}

void UserRoutineInfo(void *) {
  size_t thread_id_hash =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_thread_pool_test.GetOperatorInfo(&kwdb_operator_info);
  if (kwdb_operator_info.GetOperatorName().empty()) {
    failnum++;
  }
  kwdb_operator_info.SetOperatorName("Sleep4");
  kwdb_operator_info.SetOperatorOwner("Test4");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  kwdb_thread_pool_test.SetOperatorInfo(&kwdb_operator_info);
  KWDBOperatorInfo kwdb_operator_info2;
  kwdb_thread_pool_test.GetOperatorInfo(&kwdb_operator_info2);
  if (kwdb_operator_info2.GetOperatorName().empty()) {
    failnum++;
  }
  do {
    UserRoutineSleep(thread_id_hash);
  } while (!kwdb_thread_pool_test.IsCancel());
}

class TestKWDBDynamicThreadPool : public ::testing::Test {
 public:
  TestKWDBDynamicThreadPool() {}

  static void SetUpTestCase() {
    LOGGER.Init();
  }

  static void TearDownTestCase() {
    LOGGER.Destroy();
  }
};

TEST_F(TestKWDBDynamicThreadPool, IsStop) {
  EXPECT_TRUE(kwdb_thread_pool_test.IsAvailable());
  EXPECT_FALSE(kwdb_thread_pool_test.IsStop());
}

TEST_F(TestKWDBDynamicThreadPool, GetThreadsNum) {
  EXPECT_EQ(kwdb_thread_pool_test.GetThreadsNum(), 1);
}

TEST_F(TestKWDBDynamicThreadPool, ApplyThread1) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep0");
  kwdb_operator_info.SetOperatorOwner("Test0");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  threads_id[0] = kwdb_thread_pool_test.ApplyThread(
      UserRoutine, &kwdb_thread_pool_test, &kwdb_operator_info);
  EXPECT_NE(threads_id[0], 0);
}

TEST_F(TestKWDBDynamicThreadPool, ApplyThread2) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep1");
  kwdb_operator_info.SetOperatorOwner("Test1");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  threads_id[1] = kwdb_thread_pool_test.ApplyThread(
      UserRoutineLoop, &kwdb_thread_pool_test, &kwdb_operator_info);
  EXPECT_NE(threads_id[1], 0);
  EXPECT_EQ(kwdb_thread_pool_test.GetThreadsNum(), 2);
}

TEST_F(TestKWDBDynamicThreadPool, GetThreadsNum2) {
  EXPECT_EQ(kwdb_thread_pool_test.GetThreadsNum(), 2);
}

TEST_F(TestKWDBDynamicThreadPool, JoinThreadLeaseTime) {
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  auto index = threads_id[0];
  EXPECT_EQ(kwdb_thread_pool_test.JoinThread(index, 1000), KStatus::SUCCESS);
  EXPECT_EQ(kwdb_thread_pool_test.JoinThread(index - 1, 1000), KStatus::FAIL);
}

TEST_F(TestKWDBDynamicThreadPool, JoinThreadTimeout) {
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  auto index = threads_id[1];
  EXPECT_EQ(kwdb_thread_pool_test.JoinThread(index, 1000), KStatus::SUCCESS);
}

TEST_F(TestKWDBDynamicThreadPool, ApplyThread3) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep2");
  kwdb_operator_info.SetOperatorOwner("Test2");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  threads_id[0] = kwdb_thread_pool_test.ApplyThread(
      UserRoutineLoop, &kwdb_thread_pool_test, &kwdb_operator_info);
  EXPECT_NE(threads_id[0], 0);
}

TEST_F(TestKWDBDynamicThreadPool, ApplyThread4) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep");
  kwdb_operator_info.SetOperatorOwner("Test");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  threads_id[2] = kwdb_thread_pool_test.ApplyThread(
      UserRoutine, &kwdb_thread_pool_test, &kwdb_operator_info);
}

TEST_F(TestKWDBDynamicThreadPool, CancelThread) {
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  auto index = threads_id[0];
  kwdb_thread_pool_test.CancelThread(index - 1);
  EXPECT_NE(kwdb_thread_pool_test.CancelThread(index), KStatus::FAIL);
}

TEST_F(TestKWDBDynamicThreadPool, ApplyThread5) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep3");
  kwdb_operator_info.SetOperatorOwner("Test3");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  sleep(1);
  threads_id[3] = kwdb_thread_pool_test.ApplyThread(
      UserRoutineLoop, &kwdb_thread_pool_test, &kwdb_operator_info);
  EXPECT_NE(threads_id[3], 0);
}

TEST_F(TestKWDBDynamicThreadPool, CancelThread2) {
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());
  auto index = threads_id[1];
  EXPECT_NE(kwdb_thread_pool_test.CancelThread(index), KStatus::FAIL);
  // EXPECT_EQ(kwdb_thread_pool_test.GetThreadsNum(), 2);
  sleep(1);
}

TEST_F(TestKWDBDynamicThreadPool, SetOperatorInfo) {
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("Sleep3");
  kwdb_operator_info.SetOperatorOwner("Test3");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  threads_id[4] = kwdb_thread_pool_test.ApplyThread(
      UserRoutineInfo, &kwdb_thread_pool_test, &kwdb_operator_info);
  EXPECT_NE(threads_id[4], 0);
  sleep(1);
}

TEST_F(TestKWDBDynamicThreadPool, SetOperatorInfo3) {
  KWDBOperatorInfo kwdb_operator_info;
  ASSERT_FALSE(kwdb_thread_pool_test.IsStop());

  threads_id[0] = kwdb_thread_pool_test.ApplyThread(
      UserRoutine, &kwdb_thread_pool_test, &kwdb_operator_info);

  kwdb_operator_info.SetOperatorName("Sleep1");
  kwdb_operator_info.SetOperatorOwner("Test1");
  time_t now;
  kwdb_operator_info.SetOperatorStartTime((kwdbts::k_uint64)time(&now));
  kwdb_operator_info.SetOperatorStatus("Status1");
  threads_id[1] = kwdb_thread_pool_test.ApplyThread(
      UserRoutine, &kwdb_thread_pool_test, &kwdb_operator_info);

  EXPECT_EQ(threads_id[0], 0);
  EXPECT_EQ(threads_id[1], 0);
  EXPECT_EQ(kwdb_thread_pool_test.GetThreadsNum(), 2);
  kwdb_thread_pool_test.CancelThread(100);
  kwdb_thread_pool_test.CancelThread(101);
}

void UserRoutineLoopCtx(void *) {
  size_t thread_id_hash =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  do {
    UserRoutineSleep(thread_id_hash);
  } while (!kwdb_thread_pool_test.IsCancel());
}

void UserRoutineCtx(void *) {
  size_t thread_id_hash =
      std::hash<std::thread::id>{}(std::this_thread::get_id());
  UserRoutineSleep(thread_id_hash);
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_thread_pool_test.ApplyThread(UserRoutineLoopCtx, &kwdb_thread_pool_test, &kwdb_operator_info);
}

TEST_F(TestKWDBDynamicThreadPool, CheckContext) {
  sleep(1);
  KWDBOperatorInfo kwdb_operator_info;
  threads_id[5]= kwdb_thread_pool_test.ApplyThread(UserRoutineCtx, &kwdb_thread_pool_test, &kwdb_operator_info);
}

TEST_F(TestKWDBDynamicThreadPool, JoinThread) {
  sleep(1);
  KWDBOperatorInfo kwdb_operator_info;
  threads_id[5]= kwdb_thread_pool_test.ApplyThread(UserRoutine, &kwdb_thread_pool_test, &kwdb_operator_info);
  kwdb_thread_pool_test.JoinThread(threads_id[5], 0);
  EXPECT_EQ(task_count.load(), 2);
}

TEST_F(TestKWDBDynamicThreadPool, Stop) {
  EXPECT_EQ(kwdb_thread_pool_test.Stop(), KStatus::SUCCESS);
  EXPECT_FALSE(kwdb_thread_pool_test.IsAvailable());
  sleep(1);
  EXPECT_EQ(kwdb_thread_pool_test.PoolStatus::STOPPED, kwdb_thread_pool_test.GetStatus());
  EXPECT_EQ(kwdb_thread_pool_test.GetOperatorContext(), nullptr);
}

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace kwdbts
