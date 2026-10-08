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

#include "ee_exec_pool.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestExecPool : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

class SimulatedNormalTask : public ExecTask {
 public:
  explicit SimulatedNormalTask() {};
  ~SimulatedNormalTask() {};

  bool is_can_schedule() override {
    return true;
  }

  void Run(kwdbContext_p ctx) override {
    // run for 6 seconds and then exit.
    sleep(6);
  }
};

class SimulatedExtraTask : public ExecTask {
 public:
  explicit SimulatedExtraTask() {};
  ~SimulatedExtraTask() {};

  bool is_can_schedule() override {
    return true;
  }

  void Run(kwdbContext_p ctx) override {
    // Extra task will complete immediately without doing anything
    return;
  }
};

TEST_F(TestExecPool, TestThreadExpansion) {
  // Initialize exec thread pool with 2 minimum threads
  ExecPool& exec_pool = ExecPool::GetInstance(10, 2);
  /**
   * Update temporary thread wait times to 3, which means the temporary thread
   * in exec thread pool will exit if there is no further task in 6 seconds
   */
  exec_pool.SetTemporaryThreadWaitTimes(3);
  kwdbContext_t context;
  exec_pool.Init(&context);
  std::shared_ptr<ExecTask> normal_task1 = std::make_shared<SimulatedNormalTask>();
  std::shared_ptr<ExecTask> normal_task2 = std::make_shared<SimulatedNormalTask>();
  // wait thread number should be 2 at the beginning
  ASSERT_EQ(exec_pool.GetWaitThreadNum(), 2);
  // total thread number should be 2 at the beginning
  ASSERT_EQ(exec_pool.GetTotalThreadNum(), 2);
  exec_pool.PushTask(normal_task1);
  exec_pool.PushTask(normal_task2);
  sleep(2);
  // wait thread number should be 0 since two tasks are running
  ASSERT_EQ(exec_pool.GetWaitThreadNum(), 0);
  // total thread number still should be 2
  ASSERT_EQ(exec_pool.GetTotalThreadNum(), 2);
  std::shared_ptr<ExecTask> extra_task1 = std::make_shared<SimulatedExtraTask>();
  std::shared_ptr<ExecTask> extra_task2 = std::make_shared<SimulatedExtraTask>();
  // two extra tasks are comming in
  exec_pool.PushTask(extra_task1);
  exec_pool.PushTask(extra_task2);
  // there should be 4 threads for now
  ASSERT_EQ(exec_pool.GetTotalThreadNum(), 4);
  sleep(2);
  // two extra tasks should be complete immediately, so wait thread number should be 2
  ASSERT_EQ(exec_pool.GetWaitThreadNum(), 2);
  // total thread number should be 4
  ASSERT_EQ(exec_pool.GetTotalThreadNum(), 4);
  sleep(6);
  // total thread numbe should shrink back to 2
  ASSERT_EQ(exec_pool.GetTotalThreadNum(), 2);
  // two normal tasks should be complete
  ASSERT_EQ(exec_pool.GetWaitThreadNum(), 2);
}

}  // namespace kwdbts
