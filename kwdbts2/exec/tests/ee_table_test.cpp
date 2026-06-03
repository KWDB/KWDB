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

#include "ee_table.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Test class for TABLE
class TableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    // Create a TABLE
    table_ = std::make_unique<TABLE>(1, 1);
  }

  void TearDown() override {
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  std::unique_ptr<TABLE> table_;
};

// Test constructor and destructor
TEST_F(TableTest, TestConstructor) {
  // Test default constructor
  EXPECT_EQ(table_->field_num_, 0);
  EXPECT_EQ(table_->tag_num_, 0);
  EXPECT_EQ(table_->min_tag_id_, 0);
  EXPECT_EQ(table_->fields_, nullptr);
}

// Test GetFieldWithColNum method
TEST_F(TableTest, TestGetFieldWithColNum) {
  // Test with field_num_ = 0
  Field* field = table_->GetFieldWithColNum(0);
  EXPECT_EQ(field, nullptr);

  // Test with invalid column number
  field = table_->GetFieldWithColNum(100);
  EXPECT_EQ(field, nullptr);
}

// Test GetRelFields method
TEST_F(TableTest, TestGetRelFields) {
  // Test GetRelFields
  std::vector<Field*>& rel_fields = table_->GetRelFields();
  EXPECT_TRUE(rel_fields.empty());
}

// Test GetRelTagJoinColumnIndexes method
TEST_F(TableTest, TestGetRelTagJoinColumnIndexes) {
  // Test GetRelTagJoinColumnIndexes
  std::vector<std::pair<k_uint32, k_uint32>>& join_indexes = table_->GetRelTagJoinColumnIndexes();
  EXPECT_TRUE(join_indexes.empty());
}

// Test GetScanTags method
TEST_F(TableTest, TestGetScanTags) {
  // Test GetScanTags
  std::vector<k_uint32>& scan_tags = table_->GetScanTags();
  EXPECT_TRUE(scan_tags.empty());
}

// Test PTagCount method
TEST_F(TableTest, TestPTagCount) {
  // Test PTagCount with no fields
  k_uint32 ptag_count = table_->PTagCount();
  EXPECT_EQ(ptag_count, 0);
}

// Test Init method with a simple spec
TEST_F(TableTest, TestInit) {
  // Create a simple TSTagReaderSpec
  TSTagReaderSpec spec;

  // Add a timestamp column
  TSCol* col1 = spec.add_colmetas();
  col1->set_storage_type(roachpb::DataType::TIMESTAMP);
  col1->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  col1->set_nullable(false);

  // Add an integer column
  TSCol* col2 = spec.add_colmetas();
  col2->set_storage_type(roachpb::DataType::INT);
  col2->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  col2->set_nullable(false);

  // Add a tag column
  TSCol* col3 = spec.add_colmetas();
  col3->set_storage_type(roachpb::DataType::VARCHAR);
  col3->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
  col3->set_nullable(false);
  col3->set_storage_len(30);

  // Test Init
  KStatus status = table_->Init(ctx_, &spec);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Verify field_num_
  EXPECT_EQ(table_->field_num_, 3U);

  // Verify fields are created
  EXPECT_NE(table_->fields_, nullptr);

  // Verify fields are initialized correctly
  EXPECT_NE(table_->GetFieldWithColNum(0), nullptr);
  EXPECT_NE(table_->GetFieldWithColNum(1), nullptr);
  EXPECT_NE(table_->GetFieldWithColNum(2), nullptr);

  // Verify tag_num_
  EXPECT_EQ(table_->tag_num_, 1U);

  // Verify min_tag_id_
  EXPECT_EQ(table_->min_tag_id_, 2U);
}

// Test Init method with relational columns
TEST_F(TableTest, TestInitWithRelationalColumns) {
  // Create a TSTagReaderSpec with relational columns
  TSTagReaderSpec spec;

  // Add a timestamp column
  TSCol* col1 = spec.add_colmetas();
  col1->set_storage_type(roachpb::DataType::TIMESTAMP);
  col1->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  col1->set_nullable(false);

  // Add a relational column
  TSCol* rel_col1 = spec.add_relationalcols();
  rel_col1->set_storage_type(roachpb::DataType::INT);
  rel_col1->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  rel_col1->set_nullable(false);

  // Add probe and hash column IDs
  spec.add_probecolids(0);
  spec.add_hashcolids(1);

  // Test Init
  KStatus status = table_->Init(ctx_, &spec);
  EXPECT_EQ(status, KStatus::SUCCESS);

  // Verify field_num_
  EXPECT_EQ(table_->field_num_, 1U);

  // Verify relational fields
  std::vector<Field*>& rel_fields = table_->GetRelFields();
  EXPECT_EQ(rel_fields.size(), 1U);

  // Verify join column indexes
  std::vector<std::pair<k_uint32, k_uint32>>& join_indexes = table_->GetRelTagJoinColumnIndexes();
  EXPECT_EQ(join_indexes.size(), 1U);
  EXPECT_EQ(join_indexes[0].first, 0U);
  EXPECT_EQ(join_indexes[0].second, 1U);
}

}  // namespace kwdbts