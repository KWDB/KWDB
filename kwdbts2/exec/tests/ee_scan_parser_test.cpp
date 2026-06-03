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

#include "ee_scan_parser.h"

#include "ee_op_test_base.h"
#include "gtest/gtest.h"

namespace kwdbts {

// Create a test class to access protected members
class TestableTsTableScanParser : public TsTableScanParser {
 public:
  TestableTsTableScanParser(TSReaderSpec* spec, PostProcessSpec* post, TABLE* table)
      : TsTableScanParser(spec, post, table) {
  }

  void SetInputCols(Field** cols, k_uint32 count) {
    input_cols_ = cols;
    inputcols_count_ = count;
  }
};

class TsScanParserTest : public OperatorTestBase {
 public:
  TsScanParserTest() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    // Create TABLE object
    table_ = KNEW TABLE(1, 1);

    // Create TSReaderSpec
    spec_ = KNEW TSReaderSpec();

    // Create PostProcessSpec
    post_ = KNEW PostProcessSpec();

    // Create TestableTsTableScanParser
    parser_ = KNEW TestableTsTableScanParser(spec_, post_, table_);
  }

  void TearDown() override {
    SafeDeletePointer(parser_);
    SafeDeletePointer(post_);
    SafeDeletePointer(spec_);

    SafeDeletePointer(table_);
    OperatorTestBase::TearDown();
  }

  TABLE* table_;
  TSReaderSpec* spec_;
  PostProcessSpec* post_;
  TestableTsTableScanParser* parser_;
};

// Test TsTableScanParser constructor
TEST_F(TsScanParserTest, TestConstructor) {
  EXPECT_NE(parser_, nullptr);
}

// Test ParserScanCols with no input columns
TEST_F(TsScanParserTest, TestParserScanColsWithNoInputCols) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  // Create test field
  Field** fields = static_cast<Field**>(malloc(1 * sizeof(Field*)));
  fields[0] = KNEW FieldInt(0, roachpb::DataType::INT, 4);
  table_->fields_ = fields;
  table_->field_num_ = 1;

  // Set inputcols_count_ to 0
  parser_->SetInputCols(nullptr, 0);

  // Call ParserScanCols
  EEIteratorErrCode code = parser_->ParserScanCols(ctx);

  // Check result
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_EQ(table_->scan_cols_.size(), 1);
}

}  // namespace kwdbts