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

#include "ee_sort_parser.h"

#include "ee_op_spec_utils.h"
#include "ee_op_test_base.h"
#include "gtest/gtest.h"

namespace kwdbts {

class TsSortParserTest : public OperatorTestBase {
 public:
  TsSortParserTest() : OperatorTestBase() {
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    // Create TSSorterSpec
    spec_ = KNEW TSSorterSpec();

    // Add order column
    auto* order = spec_->mutable_output_ordering();
    auto* column = order->add_columns();
    column->set_col_idx(0);
    column->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);

    // Create PostProcessSpec
    post_ = KNEW PostProcessSpec();
    post_->set_limit(10);
    post_->set_offset(0);

    // Add output columns
    post_->add_output_columns(0);
    post_->add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    // Create TABLE object
    table_ = KNEW TABLE(1, table_id_);

    // Create TsSortParser
    parser_ = std::make_unique<TsSortParser>(spec_, post_, table_);
  }

  void TearDown() override {
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
    SafeDeletePointer(table_);
    OperatorTestBase::TearDown();
  }

  TSSorterSpec* spec_{nullptr};
  PostProcessSpec* post_{nullptr};
  TABLE* table_{nullptr};
  std::unique_ptr<TsSortParser> parser_;
};

// Test RenderSize method
TEST_F(TsSortParserTest, TestRenderSize) {
  k_uint32 num = 0;
  parser_->RenderSize(ctx_, &num);
  EXPECT_EQ(num, 1);  // We already added one output column in SetUp
}

}  // namespace kwdbts