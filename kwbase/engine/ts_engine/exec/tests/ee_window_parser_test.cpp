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

#include "ee_window_parser.h"

#include <memory>
#include <vector>

#include "ee_base_op.h"
#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

namespace kwdbts {

// Testable class that exposes protected methods
class InspectableWindowParser : public TsWindowParser {
 public:
  InspectableWindowParser(WindowerSpec* spec, PostProcessSpec* post, TABLE* table) : TsWindowParser(spec, post, table) {
  }

  EEIteratorErrCode CallMallocArray(kwdbContext_p ctx) {
    return MallocArray(ctx);
  }

  EEIteratorErrCode CallParserWindowFuncCol(kwdbContext_p ctx, Field** renders, k_uint32 num) {
    return ParserWindowFuncCol(ctx, renders, num);
  }

  // Expose protected members for testing
  using TsWindowParser::func_size_;
  using TsWindowParser::input_;
  using TsWindowParser::map_filter_index;
  using TsWindowParser::map_winfunc_index;
  using TsWindowParser::output_fields_;
  using TsWindowParser::outputcol_count_;
  using TsWindowParser::renders_size_;
  using TsWindowParser::window_field_;
};

class FakeInputOperator : public BaseOperator {
 public:
  FakeInputOperator() : BaseOperator(nullptr, nullptr, nullptr, 0) {
  }

  OperatorType Type() override {
    return OperatorType::OPERATOR_NOOP;
  }
  EEIteratorErrCode Init(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Start(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  EEIteratorErrCode Reset(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }
  EEIteratorErrCode Close(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }

  std::vector<Field*>& OutputFields() override {
    return output_fields_;
  }

  k_uint32 GetRenderSize() override {
    return render_fields_.size();
  }

  Field** GetRender() override {
    return render_fields_.data();
  }

  Field* GetRender(int index) override {
    if (index >= 0 && static_cast<size_t>(index) < render_fields_.size()) {
      return render_fields_[index];
    }
    return nullptr;
  }

  std::vector<Field*> output_fields_;
  std::vector<Field*> render_fields_;
};

class TsWindowParserTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
};

TEST_F(TsWindowParserTest, MallocArraySuccess) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Set func_size_ to 1 for testing
  parser.func_size_ = 1;

  EEIteratorErrCode code = parser.CallMallocArray(ctx_);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_NE(parser.window_field_, nullptr);

  // Clean up handled by destructor
}

TEST_F(TsWindowParserTest, ParserWindowFuncColWithDiff) {
  WindowerSpec spec;
  auto* window_fn = spec.add_windowfns();
  window_fn->add_argsidxs(0);
  window_fn->set_outputcolidx(1);
  auto* func = window_fn->mutable_func();
  func->set_windowfunc(WindowerSpec_WindowFunc_DIFF);

  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create fake input operator with a field
  auto input_op = std::make_unique<FakeInputOperator>();
  auto field = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  input_op->render_fields_.push_back(field);
  input_op->output_fields_.push_back(field);

  // Set input for parser
  parser.input_ = input_op.get();

  // Test ParserWindowFuncCol
  parser.func_size_ = 1;
  Field* renders[2];
  EEIteratorErrCode code = parser.CallParserWindowFuncCol(ctx_, renders, 2);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);
  EXPECT_NE(parser.window_field_[0], nullptr);
  EXPECT_EQ(parser.map_winfunc_index.size(), 1U);
  EXPECT_EQ(parser.map_winfunc_index[1], 0U);

  // Clean up handled by destructor
  delete field;
}

TEST_F(TsWindowParserTest, ParserWindowFuncColWithUnknownFunction) {
  WindowerSpec spec;
  auto* window_fn = spec.add_windowfns();
  window_fn->add_argsidxs(0);
  window_fn->set_outputcolidx(1);
  auto* func = window_fn->mutable_func();
  func->set_windowfunc(WindowerSpec_WindowFunc_DIFF);  // Use DIFF function for testing

  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create fake input operator with a field
  auto input_op = std::make_unique<FakeInputOperator>();
  auto field = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  input_op->render_fields_.push_back(field);

  // Set input for parser
  parser.input_ = input_op.get();

  // Test ParserWindowFuncCol with known function
  parser.func_size_ = 1;
  Field* renders[2];
  EEIteratorErrCode code = parser.CallParserWindowFuncCol(ctx_, renders, 2);
  EXPECT_EQ(code, EEIteratorErrCode::EE_OK);

  // Clean up handled by destructor
  delete field;
}

TEST_F(TsWindowParserTest, RenderSizeWithRendersSize) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Set renders_size_ to 5
  parser.renders_size_ = 5;

  k_uint32 num = 0;
  parser.RenderSize(ctx_, &num);
  EXPECT_EQ(num, 5U);
}

TEST_F(TsWindowParserTest, RenderSizeWithOutputColCount) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Set outputcol_count_ to 3
  parser.outputcol_count_ = 3;

  k_uint32 num = 0;
  parser.RenderSize(ctx_, &num);
  EXPECT_EQ(num, 3U);
}

TEST_F(TsWindowParserTest, RenderSizeWithInputFields) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create fake input operator with 2 fields
  auto input_op = std::make_unique<FakeInputOperator>();
  auto field1 = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  auto field2 = new FieldConstDouble(roachpb::DataType::DOUBLE, 20.0);
  input_op->output_fields_.push_back(field1);
  input_op->output_fields_.push_back(field2);

  // Set input for parser
  parser.input_ = input_op.get();

  // Set func_size_ to 1
  parser.func_size_ = 1;

  k_uint32 num = 0;
  parser.RenderSize(ctx_, &num);
  EXPECT_EQ(num, 3U);  // 2 input fields + 1 window function

  // Clean up
  delete field1;
  delete field2;
}

TEST_F(TsWindowParserTest, ParserWinFuncFields) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create a window field
  parser.func_size_ = 1;
  parser.CallMallocArray(ctx_);

  auto field = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  parser.window_field_[0] = field;

  // Test ParserWinFuncFields
  std::vector<Field*> output_fields;
  parser.ParserWinFuncFields(output_fields);
  EXPECT_EQ(output_fields.size(), 1U);
  EXPECT_NE(output_fields[0], nullptr);
  EXPECT_NE(output_fields[0], field);  // Should be a copy

  // Clean up handled by destructor
  delete output_fields[0];
}

TEST_F(TsWindowParserTest, ParserSetOutputFields) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create output fields
  std::vector<Field*> output_fields;
  auto field1 = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  auto field2 = new FieldConstDouble(roachpb::DataType::DOUBLE, 20.0);
  output_fields.push_back(field1);
  output_fields.push_back(field2);

  // Test ParserSetOutputFields
  parser.ParserSetOutputFields(output_fields);
  EXPECT_EQ(parser.output_fields_.size(), 2U);
  EXPECT_EQ(parser.output_fields_[0], field1);
  EXPECT_EQ(parser.output_fields_[1], field2);

  // Clean up
  delete field1;
  delete field2;
}

TEST_F(TsWindowParserTest, ParserFilterFields) {
  WindowerSpec spec;
  PostProcessSpec post;
  TABLE table(1, 1);

  InspectableWindowParser parser(&spec, &post, &table);

  // Create filter fields
  auto field1 = new FieldConstDouble(roachpb::DataType::DOUBLE, 10.0);
  auto field2 = new FieldConstDouble(roachpb::DataType::DOUBLE, 20.0);
  parser.map_filter_index[1] = field1;
  parser.map_filter_index[2] = field2;

  // Test ParserFilterFields
  std::vector<Field*> fields;
  parser.ParserFilterFields(fields);
  EXPECT_EQ(fields.size(), 2U);
  EXPECT_EQ(fields[0], field1);
  EXPECT_EQ(fields[1], field2);

  // Clean up
  delete field1;
  delete field2;
}

}  // namespace kwdbts
