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

#include "ee_scan_row_batch.h"

#include "ee_op_test_base.h"
#include "gtest/gtest.h"

namespace kwdbts {

class ScanRowBatchTest : public OperatorTestBase {
 public:
  ScanRowBatchTest() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    // Create TABLE object
    table_ = KNEW TABLE(1, 1);
  }

  void TearDown() override {
    SafeDeletePointer(table_);
    OperatorTestBase::TearDown();
  }

  TABLE* table_;
};

// Test ScanRowBatch basic functionality
TEST_F(ScanRowBatchTest, TestScanRowBatchBasic) {
  // Create ScanRowBatch
  ScanRowBatch batch(table_);

  // Test Reset
  batch.Reset();

  // Test Count - should be 0 initially
  EXPECT_EQ(batch.Count(), 0);

  // Test NextLine - should return -1 initially
  EXPECT_EQ(batch.NextLine(), -1);

  // Test ResetLine - should not crash
  batch.ResetLine();

  // Test SetCurrentLine - should return false for invalid line
  EXPECT_FALSE(batch.SetCurrentLine(0));
}

// Test ReverseScanRowBatch basic functionality
TEST_F(ScanRowBatchTest, TestReverseScanRowBatchBasic) {
  // Create ReverseScanRowBatch
  ReverseScanRowBatch batch(table_);

  // Test Reset
  batch.Reset();

  // Test Count - should be 0 initially
  EXPECT_EQ(batch.Count(), 0);

  // Test NextLine - should return -1 initially
  EXPECT_EQ(batch.NextLine(), -1);

  // Test ResetLine - should not crash
  batch.ResetLine();

  // Test SetCurrentLine - should return false for invalid line
  EXPECT_FALSE(batch.SetCurrentLine(0));
}

// Create a test class to access protected members
class TestableScanRowBatch : public ScanRowBatch {
 public:
  explicit TestableScanRowBatch(TABLE* table) : ScanRowBatch(table) {
  }

  void SetEffectCount(k_uint32 count) {
    effect_count_ = count;
  }
  void SetIsFilter(bool filter) {
    is_filter_ = filter;
  }
  void AddSelectionItem(const SelectionItem& item) {
    selection_.push_back(item);
  }
};

class TestableReverseScanRowBatch : public ReverseScanRowBatch {
 public:
  explicit TestableReverseScanRowBatch(TABLE* table) : ReverseScanRowBatch(table) {
  }

  void SetEffectCount(k_uint32 count) {
    effect_count_ = count;
  }
  void SetIsFilter(bool filter) {
    is_filter_ = filter;
  }
  void AddSelectionItem(const SelectionItem& item) {
    selection_.push_back(item);
  }
};

// Test ScanRowBatch with filter
TEST_F(ScanRowBatchTest, TestScanRowBatchWithFilter) {
  // Create TestableScanRowBatch
  TestableScanRowBatch batch(table_);

  // Set is_filter_ to true and effect_count_ to 2
  batch.SetIsFilter(true);
  batch.SetEffectCount(2);

  // Add selection items
  SelectionItem selection1;
  selection1.batch_ = 0;
  selection1.line_ = 0;
  batch.AddSelectionItem(selection1);

  SelectionItem selection2;
  selection2.batch_ = 0;
  selection2.line_ = 1;
  batch.AddSelectionItem(selection2);

  // Test ResetLine
  batch.ResetLine();

  // Test Count
  EXPECT_EQ(batch.Count(), 2);

  // Test NextLine
  EXPECT_EQ(batch.NextLine(), 1);
  EXPECT_EQ(batch.NextLine(), -1);

  // Test SetCurrentLine
  EXPECT_TRUE(batch.SetCurrentLine(0));
  EXPECT_FALSE(batch.SetCurrentLine(2));
}

// Test ReverseScanRowBatch with filter
TEST_F(ScanRowBatchTest, TestReverseScanRowBatchWithFilter) {
  // Create TestableReverseScanRowBatch
  TestableReverseScanRowBatch batch(table_);

  // Set is_filter_ to true and effect_count_ to 2
  batch.SetIsFilter(true);
  batch.SetEffectCount(2);

  // Add selection items
  SelectionItem selection1;
  selection1.batch_ = 0;
  selection1.line_ = 0;
  batch.AddSelectionItem(selection1);

  SelectionItem selection2;
  selection2.batch_ = 0;
  selection2.line_ = 1;
  batch.AddSelectionItem(selection2);

  // Test ResetLine
  batch.ResetLine();

  // Test Count
  EXPECT_EQ(batch.Count(), 2);

  // Test NextLine
  EXPECT_EQ(batch.NextLine(), 1);
  EXPECT_EQ(batch.NextLine(), -1);

  // Test SetCurrentLine
  EXPECT_TRUE(batch.SetCurrentLine(0));
  EXPECT_FALSE(batch.SetCurrentLine(2));
}

}  // namespace kwdbts