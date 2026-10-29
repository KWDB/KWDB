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

#include "ee_executor.h"

#include <iostream>
#include <memory>
#include <string>

#include "../../roachpb/ee_pb_plan.pb.h"
#include "gtest/gtest.h"
#include "libkwdbts2.h"

namespace kwdbts {

extern kwdbContext_p g_kwdb_context;
kwdbContext_p ctx = g_kwdb_context;

class TestExecutor : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    InitServerKWDBContext(ctx);
  }

  static void TearDownTestCase() {
    DestroyKWDBContext(ctx);
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  TestExecutor() {}
};

// Verify Executor initialization and destruction
TEST_F(TestExecutor, ExecutorInitAndDestroy) {
  kwdbts::EngineOptions options;
  options.db_path = "tsdb";
  options.thread_pool_size = 1;
  options.task_queue_size = 1;
  options.buffer_pool_size = 1;
  EXPECT_EQ(kwdbts::InitExecutor(ctx, options), SUCCESS);
  std::cout << "Executor Init ok. " << std::endl;
  EXPECT_EQ(kwdbts::DestoryExecutor(), SUCCESS);
  std::cout << "Executor Destory ok. " << std::endl;
  EXPECT_TRUE(true);
}

// Test GetWaitThreadNum function
TEST_F(TestExecutor, TestGetWaitThreadNum) {
  // Initialize ThreadInfo structure
  ThreadInfo thread_info;
  thread_info.wait_threads = 0;
  
  // Call GetWaitThreadNum function
  KStatus status = GetWaitThreadNum(ctx, &thread_info);
  
  // Verify return status is SUCCESS
  EXPECT_EQ(status, KStatus::SUCCESS);
  
  // Verify wait_threads is set (should be >= 0)
  EXPECT_GE(thread_info.wait_threads, 0);
  
  std::cout << "GetWaitThreadNum test passed. Wait threads: " << thread_info.wait_threads << std::endl;
}

}  // namespace kwdbts
