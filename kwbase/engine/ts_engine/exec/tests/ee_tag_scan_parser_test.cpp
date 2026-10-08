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

#include "ee_tag_scan_parser.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Test class for TsTagScanParser
class TsTagScanParserTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    // Create a fake TABLE
    table_ = std::make_unique<TABLE>(1, 1);
    table_->min_tag_id_ = 100;
    table_->field_num_ = 200;  // Set field_num_ to 200 to separate tag and relational columns

    // Allocate fields_ array
    table_->fields_ = new Field*[table_->field_num_];
    memset(table_->fields_, 0, table_->field_num_ * sizeof(Field*));

    // Add tag fields
    for (k_uint32 i = 100; i < 105; i++) {
      // Use FieldVarchar as a concrete Field subclass for tags
      FieldVarchar* field = new FieldVarchar(i, roachpb::DataType::VARCHAR, 255);
      field->set_num(i);
      table_->fields_[i] = field;
    }
    // Add relational fields to rel_fields_ for multiple model processing
    for (k_uint32 i = 0; i < 5; i++) {
      // Use FieldLonglong as a concrete Field subclass for relational fields
      FieldLonglong* field = new FieldLonglong(200 + i, roachpb::DataType::BIGINT, 8);
      field->set_num(200 + i);
      rel_fields_.push_back(field);
    }
    // Set rel_fields_ in table_
    // We need to access the protected member rel_fields_
    // For testing purposes, we'll use a hack to set it
    // First, get the address of rel_fields_
    auto rel_fields_ptr = &(table_->GetRelFields());
    // Clear existing fields
    rel_fields_ptr->clear();
    // Add our relational fields
    for (auto field : rel_fields_) {
      rel_fields_ptr->push_back(field);
    }

    // Create TSTagReaderSpec
    spec_ = std::make_unique<TSTagReaderSpec>();
    spec_->set_tableid(1);
    spec_->set_tableversion(1);
    spec_->set_only_tag(false);

    // Add primary tags
    auto primary_tag = spec_->add_primarytags();
    primary_tag->add_tagvalues("1");
    primary_tag->add_tagvalues("2");
    primary_tag->add_tagvalues("3");

    // Create PostProcessSpec
    post_ = std::make_unique<PostProcessSpec>();
    post_->add_output_columns(101);  // Tag column
    post_->add_output_columns(102);  // Tag column
    post_->add_output_columns(201);  // Relational column
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
    // Do not clean up rel_fields_ here, as TABLE destructor will handle it
    rel_fields_.clear();
  }

  std::vector<Field*> rel_fields_;

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  std::unique_ptr<TABLE> table_;
  std::unique_ptr<TSTagReaderSpec> spec_;
  std::unique_ptr<PostProcessSpec> post_;
};

// Test constructor
TEST_F(TsTagScanParserTest, TestConstructor) {
  TsTagScanParser parser(spec_.get(), post_.get(), table_.get());
  // Constructor should not throw
}

// Test ParserTagSpec method
TEST_F(TsTagScanParserTest, TestParserTagSpec) {
  TsTagScanParser parser(spec_.get(), post_.get(), table_.get());

  KStatus status = parser.ParserTagSpec(ctx_);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_EQ(table_->ptag_size_, 1);  // (3 tag values + 7) / 8 = 1
  EXPECT_EQ(table_->table_version_, 1);
  EXPECT_FALSE(table_->only_tag_);
}

// Test class to access protected members
class TestableTsTagScanParser : public TsTagScanParser {
 public:
  TestableTsTagScanParser(TSTagReaderSpec* spec, PostProcessSpec* post, TABLE* table)
      : TsTagScanParser(spec, post, table) {
  }

  void SetInputColsCount(k_uint32 count) {
    inputcols_count_ = count;
  }
};

// Test ParserScanTags method
TEST_F(TsTagScanParserTest, TestParserScanTags) {
  TestableTsTagScanParser parser(spec_.get(), post_.get(), table_.get());

  // Set inputcols_count_
  parser.SetInputColsCount(post_->output_columns_size());

  EEIteratorErrCode code = parser.ParserScanTags(ctx_);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_EQ(table_->scan_tags_.size(), 2U);  // Only tag columns (101, 102)
  EXPECT_EQ(table_->scan_tags_[0], 1);       // 101 - 100
  EXPECT_EQ(table_->scan_tags_[1], 2);       // 102 - 100
}

// Test ParserScanTagsRelCols method
TEST_F(TsTagScanParserTest, TestParserScanTagsRelCols) {
  TestableTsTagScanParser parser(spec_.get(), post_.get(), table_.get());

  // Set inputcols_count_
  parser.SetInputColsCount(post_->output_columns_size());

  EEIteratorErrCode code = parser.ParserScanTagsRelCols(ctx_);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_EQ(table_->scan_rel_cols_.size(), 1U);  // Only relational column (201)
  EXPECT_EQ(table_->scan_rel_cols_[0], 1);       // 201 - 200
}

}  // namespace kwdbts