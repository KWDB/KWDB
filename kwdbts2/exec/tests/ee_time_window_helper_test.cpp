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

#include "ee_time_window_helper.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_iterator_create_test.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_scan_op.h"
#include "ee_storage_handler.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Test class for TimeWindowHelper
class TimeWindowHelperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
  }

  void TearDown() override {
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
};

// Test TimeWindowHelper constructor and basic functionality
TEST_F(TimeWindowHelperTest, TestConstructor) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create TimeWindowHelper with mock operator
  TimeWindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));

  // Verify initial values
  EXPECT_EQ(helper.first_span_, true);
  EXPECT_EQ(helper.row_size_, 0);
  EXPECT_EQ(helper.current_line_, 0);

  reader_reader.TearDown();
}

// Test NextSlidingWindow and NextEntity methods
TEST_F(TimeWindowHelperTest, TestNextWindowMethods) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create TimeWindowHelper with mock operator
  TimeWindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));

  // Test NextSlidingWindow
  helper.NextSlidingWindow();

  // Test NextEntity
  helper.NextEntity();

  reader_reader.TearDown();
}

// Test MaterializeWindowField method
TEST_F(TimeWindowHelperTest, TestMaterializeWindowField) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create TimeWindowHelper with mock operator
  TimeWindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));

  // Test MaterializeWindowField (we'll just call it to ensure it doesn't crash)
  KWThdContext* thd = current_thd;
  if (thd) {
    // Create a simple DataChunk for testing
    DataChunk chunk;
    helper.MaterializeWindowField(thd, nullptr, &chunk, 0, 0);
  }

  reader_reader.TearDown();
}

}  // namespace kwdbts