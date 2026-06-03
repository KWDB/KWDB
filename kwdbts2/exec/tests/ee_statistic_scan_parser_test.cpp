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

#include "ee_statistic_scan_parser.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Test class for TsStatisticScanParser
class TsStatisticScanParserTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);

    // Create a TABLE
    table_ = std::make_unique<TABLE>(1, 1);

    // Add fields to the table
    // Create a data field
    FieldLonglong* data_field = new FieldLonglong(0, roachpb::DataType::BIGINT, sizeof(k_int64));
    data_field->set_num(0);
    data_field->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    // Create a tag field
    FieldVarchar* tag_field = new FieldVarchar(1, roachpb::DataType::VARCHAR, 255);
    tag_field->set_num(1);
    tag_field->set_column_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);

    // Allocate fields_ array and set the fields
    table_->fields_ = new Field*[2];
    table_->fields_[0] = data_field;
    table_->fields_[1] = tag_field;
    table_->field_num_ = 2;
    table_->min_tag_id_ = 1;

    // Create TSStatisticReaderSpec
    spec_ = std::make_unique<TSStatisticReaderSpec>();

    // Add paramidx and aggtypes
    auto param = spec_->add_paramidx();
    auto param_info = param->add_param();
    param_info->set_typ(TSStatisticReaderSpec_ParamInfo_type_colID);
    param_info->set_value(0);  // data field
    param_info->set_func_value("");

    spec_->add_aggtypes(Sumfunctype::SUM);

    // Create PostProcessSpec
    post_ = std::make_unique<PostProcessSpec>();

    // Create TsStatisticScanParser
    parser_ = std::make_unique<TsStatisticScanParser>(spec_.get(), post_.get(), table_.get());
  }

  void TearDown() override {
    // Clean up fields
    if (table_->fields_) {
      for (k_uint32 i = 0; i < table_->field_num_; i++) {
        delete table_->fields_[i];
      }
      delete[] table_->fields_;
      table_->fields_ = nullptr;
    }
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  std::unique_ptr<TABLE> table_;
  std::unique_ptr<TSStatisticReaderSpec> spec_;
  std::unique_ptr<PostProcessSpec> post_;
  std::unique_ptr<TsStatisticScanParser> parser_;
};

// Test constructor and destructor
TEST_F(TsStatisticScanParserTest, TestConstructor) {
  // Test constructor
  EXPECT_NE(parser_, nullptr);
  EXPECT_EQ(parser_->spec_, spec_.get());
  EXPECT_EQ(parser_->insert_ts_index_, 0);
  EXPECT_EQ(parser_->statistic_last_tag_index_, 0);
  EXPECT_EQ(parser_->is_insert_ts_index_, 0);
  EXPECT_EQ(parser_->insert_last_tag_ts_num_, 0);
  EXPECT_EQ(parser_->tag_count_index_, -1);
}

// Test RenderSize method
TEST_F(TsStatisticScanParserTest, TestRenderSize) {
  k_uint32 num = 0;
  parser_->RenderSize(ctx_, &num);
  EXPECT_EQ(num, spec_->tscols_size());
}

// Test ResolveChecktTagCount method
TEST_F(TsStatisticScanParserTest, TestResolveChecktTagCount) {
  k_int32 tag_count_index = parser_->ResolveChecktTagCount();
  EXPECT_EQ(tag_count_index, -1);
}

// Test ResolveScanCols method
TEST_F(TsStatisticScanParserTest, TestResolveScanCols) {
  EEIteratorErrCode code = parser_->ResolveScanCols(ctx_);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_GT(table_->scan_cols_.size(), 0U);
  EXPECT_GT(table_->scan_agg_types_.size(), 0U);
}

// Test ParserRender method
TEST_F(TsStatisticScanParserTest, TestParserRender) {
  Field** render = nullptr;
  k_uint32 num = spec_->paramidx_size();  // Use paramidx_size instead of tscols_size

  EEIteratorErrCode code = parser_->ParserRender(ctx_, &render, num);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);

  // Clean up render array, but not the fields (they're managed by TsBaseParser)
  if (render) {
    free(render);
  }
}

}  // namespace kwdbts
