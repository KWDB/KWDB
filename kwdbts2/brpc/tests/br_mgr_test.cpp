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

#include "br_mgr.h"

#include <brpc/server.h>
#include <brpc/socket.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "br_data_stream_mgr.h"
#include "br_internal_service.h"
#include "br_priority_thread_pool.h"
#include "br_stub_cache.h"
#include "cm_func.h"
#include "settings.h"

namespace kwdbts {

class BrMgrTest : public ::testing::Test {
 protected:
  BrMgrTest() { InitServerKWDBContext(ctx_); }
  void SetUp() override { options_.brpc_addr = "127.0.0.1:27257"; }
  void TearDown() override { BrMgr::GetInstance().Destroy(); }

  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;
  EngineOptions options_;
};

TEST_F(BrMgrTest, TestSingleton) {
  BrMgr& instance1 = BrMgr::GetInstance();
  BrMgr& instance2 = BrMgr::GetInstance();
  EXPECT_EQ(&instance1, &instance2);
}

// Test successful initialization
TEST_F(BrMgrTest, TestInitSuccess) {
  // Test initialization
  KStatus status = BrMgr::GetInstance().Init(ctx_, options_);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Verify that the components have been initialized correctly
  EXPECT_NE(BrMgr::GetInstance().GetBrpcStubCache(), nullptr);
  EXPECT_NE(BrMgr::GetInstance().GetDataStreamMgr(), nullptr);
  EXPECT_NE(BrMgr::GetInstance().GetPriorityThreadPool(), nullptr);
}

}  // namespace kwdbts
