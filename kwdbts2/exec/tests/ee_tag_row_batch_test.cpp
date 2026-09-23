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

#include "ee_tag_row_batch.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Test class for TagRowBatch
class TagRowBatchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    // Create a fake TABLE
    table_ = std::make_unique<TABLE>(1, 1);
    table_->min_tag_id_ = 100;
    table_->field_num_ = 110;
    table_->tag_num_ = 10;

    // Allocate fields_ array
    table_->fields_ = new Field*[table_->field_num_];
    memset(table_->fields_, 0, table_->field_num_ * sizeof(Field*));

    // Add tag fields
    for (k_uint32 i = 100; i < 110; i++) {
      // Normal tag
      FieldVarchar* field = new FieldVarchar(i, roachpb::DataType::VARCHAR, 30);
      field->set_num(i);
      table_->fields_[i] = field;
    }

    // Add scan tags
    for (k_uint32 i = 0; i < 10; i++) {
      table_->scan_tags_.push_back(i);  // 0-9 correspond to 100-109
    }

    // Create TagRowBatch
    tag_row_batch_ = std::make_unique<TagRowBatch>();
  }

  void TearDown() override {
    // Clean up fields
    if (table_->fields_) {
      for (k_uint32 i = 0; i < table_->field_num_; i++) {
        if (table_->fields_[i]) {
          delete table_->fields_[i];
        }
      }
      delete[] table_->fields_;
      table_->fields_ = nullptr;
    }
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  std::unique_ptr<TABLE> table_;
  std::unique_ptr<TagRowBatch> tag_row_batch_;
};

// Test Init method
TEST_F(TagRowBatchTest, TestInit) {
  // Test initialization
  tag_row_batch_->Init(table_.get());

  // Verify that the table is set correctly
  // We can't directly access the table_ member, but we can test other functionalities
  // that depend on it

  // Test Reset method
  tag_row_batch_->Reset();
  // Reset should clear all data structures
}

// Test Reset method
TEST_F(TagRowBatchTest, TestReset) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Reset
  tag_row_batch_->Reset();

  // We can't directly access private members, but we can test the behavior
  // by calling other methods that depend on these values
  int result = tag_row_batch_->NextLine();
  EXPECT_EQ(result, -1);  // Should return -1 since count is 0
}

// Test ResetLine method
TEST_F(TagRowBatchTest, TestResetLine) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Reset line
  tag_row_batch_->ResetLine();

  // We can't directly access private members, but we can test the behavior
  // by calling other methods that depend on these values
  int result = tag_row_batch_->NextLine();
  EXPECT_EQ(result, -1);  // Should return -1 since count is 0
}

// Test NextLine method
TEST_F(TagRowBatchTest, TestNextLine) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Test NextLine (should return -1 since count is 0)
  int result = tag_row_batch_->NextLine();
  EXPECT_EQ(result, -1);
}

// Test NextLine with line parameter
TEST_F(TagRowBatchTest, TestNextLineWithParameter) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Test NextLine with line parameter
  k_uint32 line = 0;
  KStatus status = tag_row_batch_->NextLine(&line);
  EXPECT_EQ(status, KStatus::FAIL);  // Should fail since pipe_entity_num_ is empty
  EXPECT_EQ(line, 0);
}

// Test SetPipeEntityNum method
TEST_F(TagRowBatchTest, TestSetPipeEntityNum) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Test SetPipeEntityNum
  tag_row_batch_->SetPipeEntityNum(ctx_, 3);

  // We can't directly access private members, but we can test the behavior
  // by calling other methods that depend on these values
  k_uint32 line = 0;
  KStatus status = tag_row_batch_->NextLine(&line);
  // Since no entities are set, this should fail
  EXPECT_EQ(status, KStatus::FAIL);
  EXPECT_EQ(line, 0);
}

// Test isAllDistributed method
TEST_F(TagRowBatchTest, TestIsAllDistributed) {
  // Initialize first
  tag_row_batch_->Init(table_.get());

  // Test isAllDistributed
  // By default, current_pipe_no_ should be 0 and valid_pipe_no_ should be 0
  // so isAllDistributed should return true
  EXPECT_TRUE(tag_row_batch_->isAllDistributed());
}

}  // namespace kwdbts
