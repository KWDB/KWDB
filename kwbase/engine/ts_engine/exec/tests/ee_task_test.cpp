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

#include "ee_task.h"
#include "gtest/gtest.h"
namespace kwdbts {
class ExecTaskTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
// Test the state settings and fetch of Exektask
TEST(ExecTaskTest, StateTest) {
  kwdbts::ExecTask task;
  EXPECT_EQ(task.GetState(), kwdbts::EXEC_TASK_STATE_IDLE);
  task.SetState(kwdbts::EXEC_TASK_STATE_WAITING);
  EXPECT_EQ(task.GetState(), kwdbts::EXEC_TASK_STATE_WAITING);
  task.SetState(kwdbts::EXEC_TASK_STATE_RUNNING);
  EXPECT_EQ(task.GetState(), kwdbts::EXEC_TASK_STATE_RUNNING);
}

}  // namespace kwdbts
