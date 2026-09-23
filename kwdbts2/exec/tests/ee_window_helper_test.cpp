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

#include "ee_window_helper.h"

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

// Test class for WindowHelper
class WindowHelperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
  }

  void TearDown() override {
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
};

// Test PushBatch and PopBatch functionality
TEST_F(WindowHelperTest, TestPushAndPopBatch) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create WindowHelper with mock operator
  WindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));
  helper.row_size_ = 100;  // 100 bytes per row

  // Create a fake ScanRowBatch
  auto rowbatch = new ScanRowBatch(reader_reader.table_);
  rowbatch->SetCount(10);

  // Push the batch
  KStatus status = helper.PushBatch(rowbatch);
  EXPECT_EQ(status, SUCCESS);
  EXPECT_EQ(helper.rowbatchs_.size(), 1U);

  // Pop the batch
  helper.PopBatch();
  EXPECT_EQ(helper.rowbatchs_.size(), 0U);
  reader_reader.TearDown();
}

// Test PushBatch with memory limit exceeded
TEST_F(WindowHelperTest, TestPushBatchMemoryLimitExceeded) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create WindowHelper with mock operator
  WindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));
  helper.row_size_ = 100;  // 100 bytes per row

  // Create a fake ScanRowBatch with large count
  auto rowbatch = new ScanRowBatch(reader_reader.table_);
  rowbatch->SetCount(100000);  // Large number of rows

  // Push the batch (should exceed memory limit)
  KStatus status = helper.PushBatch(rowbatch);
  EXPECT_EQ(status, SUCCESS);
  EXPECT_EQ(helper.rowbatchs_.size(), 1U);
  reader_reader.TearDown();
}

// Test CountWindowHelper methods
TEST_F(WindowHelperTest, TestCountWindowHelperMethods) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);
  // Create CountWindowHelper with mock operator
  CountWindowHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));

  // Test ResetCurrentLine
  k_int32 result = helper.ResetCurrentLine(20, 10);
  EXPECT_EQ(result, 10);  // 20 - 10

  // Test NextEntity
  helper.current_line_ = 10;
  helper.NextEntity();
  EXPECT_EQ(helper.current_line_, 0);

  // Test NextSlidingWindow
  helper.current_line_ = 10;
  helper.NextSlidingWindow();
  EXPECT_EQ(helper.current_line_, 10);  // sliding_ defaults to 0

  // Test ProcessWindow
  helper.first_span_ = true;

  // Create a fake TABLE and ScanRowBatch
  TABLE table(1, 1);
  ScanRowBatch rowbatch(&table);
  bool next_window = false;

  // Test basic functionality
  next_window = false;
  bool process_result = helper.ProcessWindow(&rowbatch, next_window);
  EXPECT_FALSE(process_result);
  EXPECT_TRUE(next_window);
  EXPECT_FALSE(helper.first_span_);
  reader_reader.TearDown();
}

}  // namespace kwdbts
