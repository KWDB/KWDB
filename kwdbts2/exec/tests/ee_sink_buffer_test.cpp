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

#include "ee_sink_buffer.h"

#include "ee_op_test_base.h"
#include "gtest/gtest.h"

namespace kwdbts {

class OutboundBufferTest : public OperatorTestBase {
 public:
  OutboundBufferTest() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    // Create fragment destinations
    FragmentDestination dest1;
    dest1.target_node_id = 1;
    dest1.brpc_addr.hostname_ = "localhost";
    dest1.brpc_addr.port_ = 8080;
    dest1.query_id = 123;

    FragmentDestination dest2;
    dest2.target_node_id = 2;
    dest2.brpc_addr.hostname_ = "localhost";
    dest2.brpc_addr.port_ = 8081;
    dest2.query_id = 123;

    // Create a destination with target_node_id = -1 (should be skipped)
    FragmentDestination dest3;
    dest3.target_node_id = -1;
    dest3.brpc_addr.hostname_ = "localhost";
    dest3.brpc_addr.port_ = 8082;
    dest3.query_id = 123;

    destinations_ = {dest1, dest2, dest3};
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }

  std::vector<FragmentDestination> destinations_;
};

// Test constructor and initialization
TEST_F(OutboundBufferTest, TestConstructor) {
  // Create OutboundBuffer with is_dest_merge = false
  OutboundBuffer buffer(destinations_, false);

  // Test IncrSinker
  buffer.IncrSinker();

  // Test IsFull - should be false initially
  EXPECT_FALSE(buffer.IsFull());

  // Test IsFinished - should be false initially
  EXPECT_FALSE(buffer.IsFinished());

  // Test IsFinishedExtra - should be true if no RPCs are in flight
  EXPECT_TRUE(buffer.IsFinishedExtra());

  // Test CancelOneSinker
  buffer.CancelOneSinker();

  // Test IsFinished - should be true after canceling sinker
  EXPECT_TRUE(buffer.IsFinished());
}

}  // namespace kwdbts