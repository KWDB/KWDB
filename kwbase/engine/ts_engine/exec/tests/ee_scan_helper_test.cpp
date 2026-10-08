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

#include "ee_scan_helper.h"

#include <memory>

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

// Test class for ScanHelper
class ScanHelperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
  }

  void TearDown() override {
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
};

// Test ScanHelper constructor
TEST_F(ScanHelperTest, TestConstructor) {
  // Create a mock TableScanOperator
  CreateTableReader reader_reader;
  reader_reader.SetUp(ctx_, 1);

  // Create ScanHelper with mock operator
  ScanHelper helper(dynamic_cast<TableScanOperator*>(reader_reader.iter_));
  EXPECT_NE(&helper, nullptr);
  reader_reader.TearDown();
}

}  // namespace kwdbts
