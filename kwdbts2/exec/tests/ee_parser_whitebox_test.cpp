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

#include "ee_aggregate_parser.h"

#include <list>
#include <memory>
#include <unordered_set>
#include <vector>

#include "ee_base_parser.h"
#include "ee_field.h"
#include "ee_field_agg.h"
#include "ee_field_const.h"
#include "ee_field_typecast.h"
#include "ee_global.h"
#include "ee_internal_type.h"
#include "ee_kwthd_context.h"
#include "ee_table.h"
#include "gtest/gtest.h"

extern "C" {
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}

namespace kwdbts {
namespace {

void DeleteUniqueFields(std::vector<Field*>* fields) {
  std::unordered_set<Field*> seen;
  for (Field* field : *fields) {
    if (field != nullptr && seen.insert(field).second) {
      SafeDeletePointer(field);
    }
  }
  fields->clear();
}

class DummyOperator : public BaseOperator {
 public:
  explicit DummyOperator(std::vector<Field*> fields = {})
      : BaseOperator(nullptr, nullptr, nullptr, 0) {
    output_fields_ = std::move(fields);
  }

  ~DummyOperator() override = default;

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  void BuildRenderFromOutputFields() {
    num_ = output_fields_.size();
    if (num_ == 0) {
      renders_ = nullptr;
      return;
    }
    renders_ = static_cast<Field**>(malloc(num_ * sizeof(Field*)));
    memset(renders_, 0, num_ * sizeof(Field*));
    for (k_uint32 i = 0; i < num_; ++i) {
      renders_[i] = output_fields_[i];
    }
  }
};

class InspectableBaseParser : public TsBaseParser {
 public:
  InspectableBaseParser(PostProcessSpec* post, TABLE* table)
      : TsBaseParser(post, table) {}

  EEIteratorErrCode ParserReference(
      kwdbContext_p,
      const std::shared_ptr<VirtualField>& virtualField,
      Field** field) override {
    for (auto i : virtualField->args_) {
      if (i == 0 || i > references_.size()) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                      "Invalid parameter value");
        return EEIteratorErrCode::EE_ERROR;
      }
      if (*field == nullptr) {
        *field = references_[i - 1];
      } else {
        (*field)->next_ = references_[i - 1];
      }
    }
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode ParserRender(kwdbContext_p, Field***, k_uint32) override {
    return EEIteratorErrCode::EE_OK;
  }

  void RenderSize(kwdbContext_p, k_uint32* num) override { *num = 0; }

  EEIteratorErrCode CallBuildBinaryTree(kwdbContext_p ctx, const KString& expr,
                                        ExprPtr* tree) {
    return BuildBinaryTree(ctx, expr, tree);
  }

  Field* CallParserBinaryTree(kwdbContext_p ctx, ExprPtr expr) {
    return ParserBinaryTree(ctx, expr);
  }

  Field* CallParserCast(kwdbContext_p ctx, Field* left,
                        const KString& output_type) {
    return ParserCast(ctx, left, output_type);
  }

  Field* CallParserOperator(kwdbContext_p ctx, AstEleType operator_type,
                            Field* left, Field* right,
                            std::list<Field*> args, k_bool is_negative,
                            const KString& func_name) {
    return ParserOperator(ctx, operator_type, left, right, std::move(args),
                          is_negative, func_name);
  }

  EEIteratorErrCode CallParserConst(kwdbContext_p ctx,
                                    std::shared_ptr<Element> const_ptr,
                                    Field** field) {
    return ParserConst(ctx, const_ptr, field);
  }

  EEIteratorErrCode CallBuildOutputFields(
      kwdbContext_p ctx, Field** renders, k_uint32 num,
      const std::vector<k_uint32>& outputcol_indexs,
      const std::vector<k_uint32>& outputcol_types,
      std::vector<Field*>& output_fields, bool ignore_outputtype) {
    return BuildOutputFields(ctx, renders, num, outputcol_indexs,
                             outputcol_types, output_fields, ignore_outputtype);
  }

  Field* CallParserFuncOperator(kwdbContext_p ctx, const KString& func_name,
                                std::list<Field*> args, k_bool is_negative) {
    return ParserFuncOperator(ctx, func_name, std::move(args), is_negative);
  }

  Field* CallParserInOperator(kwdbContext_p ctx, Field* left, Field* right,
                              k_bool is_negative) {
    return ParserInOperator(ctx, left, right, is_negative);
  }

  EEIteratorErrCode CallParserInValueString(kwdbContext_p ctx,
                                            std::string substr,
                                            Field** malloc_field,
                                            k_bool* has_null) {
    return ParserInValueString(ctx, std::move(substr), malloc_field, has_null);
  }

  Field* CallParserInField(kwdbContext_p ctx, const std::string& str) {
    return ParserInField(ctx, str);
  }

  std::vector<Field*> references_;
};

class InspectableAggregateParser : public TsAggregateParser {
 public:
  using TsAggregateParser::TsAggregateParser;

  EEIteratorErrCode CallResolveAggCol(kwdbContext_p ctx, Field** render,
                                      k_bool is_render) {
    return ResolveAggCol(ctx, render, is_render);
  }

  EEIteratorErrCode CallMallocArray(kwdbContext_p ctx) {
    return MallocArray(ctx);
  }
};

class PassthroughOperatorParser : public TsOperatorParser {
 public:
  PassthroughOperatorParser(PostProcessSpec* post, TABLE* table)
      : TsOperatorParser(post, table) {}

  EEIteratorErrCode ParserReference(kwdbContext_p,
                                    const std::shared_ptr<VirtualField>&,
                                    Field**) override {
    return EEIteratorErrCode::EE_OK;
  }

  void RenderSize(kwdbContext_p, k_uint32* num) override { *num = 1; }

  EEIteratorErrCode HandleRender(kwdbContext_p, Field** render,
                                 k_uint32 num) override {
    for (k_uint32 i = 0; i < num; ++i) {
      render[i] = input_->GetRender(i);
    }
    return EEIteratorErrCode::EE_OK;
  }
};

class ParserWhiteBoxTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(ctx_);
    EEPgErrorInfo::ResetPgErrorInfo();
  }

  void TearDown() override {
    DeleteUniqueFields(&owned_fields_);
    EEPgErrorInfo::ResetPgErrorInfo();
    SafeDeletePointer(table_);
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
  }

  void CreateSimpleTable() {
    spec_ = new TSTagReaderSpec();
    spec_->set_tableversion(1);
    spec_->set_tableid(1);
    spec_->set_accessmode(TSTableReadMode::metaTable);
    spec_->set_only_tag(false);
    spec_->set_uniontype(0);

    TSCol* col = spec_->add_colmetas();
    col->set_storage_type(roachpb::DataType::TIMESTAMP);
    col->set_storage_len(sizeof(k_int64));
    col->set_column_type(roachpb::KWDBKTSColumn::TYPE_DATA);

    col = spec_->add_colmetas();
    col->set_storage_type(roachpb::DataType::INT);
    col->set_storage_len(sizeof(k_int32));
    col->set_column_type(roachpb::KWDBKTSColumn::TYPE_DATA);

    col = spec_->add_colmetas();
    col->set_storage_type(roachpb::DataType::VARCHAR);
    col->set_storage_len(16);
    col->set_column_type(roachpb::KWDBKTSColumn::TYPE_DATA);

    post_ = KNEW PostProcessSpec();
    table_ = new TABLE(1, 1);
    table_->Init(ctx_, spec_);
  }

  std::shared_ptr<Element> MakeStringElement(AstEleType type,
                                             const KString& value) {
    auto element = std::make_shared<Element>(type, KFALSE);
    element->value.string_type = value;
    return element;
  }

  std::shared_ptr<Element> MakeIntElement(AstEleType type, k_int64 value) {
    auto element = std::make_shared<Element>(type, KFALSE);
    element->value.number.int_type = value;
    return element;
  }

  template <typename T>
  T* TakeField(T* field) {
    if (field != nullptr) {
      owned_fields_.push_back(field);
    }
    return field;
  }

  void TakeFields(const std::vector<Field*>& fields) {
    for (Field* field : fields) {
      if (field != nullptr) {
        owned_fields_.push_back(field);
      }
    }
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  TABLE* table_{nullptr};
  TSTagReaderSpec* spec_{nullptr};
  PostProcessSpec* post_{nullptr};
  std::vector<Field*> owned_fields_;
};

TEST_F(ParserWhiteBoxTest, ParserConstCreatesCommonFieldTypes) {
  PostProcessSpec post;
  InspectableBaseParser parser(&post, nullptr);
  Field* field = nullptr;

  EXPECT_EQ(parser.CallParserConst(ctx_, MakeIntElement(AstEleType::INT_TYPE, 7),
                                   &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::BIGINT);
  field = nullptr;

  auto float_element = std::make_shared<Element>(AstEleType::FLOAT_TYPE, KFALSE);
  float_element->value.number.float_type = 2.5;
  EXPECT_EQ(parser.CallParserConst(ctx_, float_element, &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::DOUBLE);
  field = nullptr;

  auto decimal_element =
      std::make_shared<Element>(AstEleType::DECIMAL, KFALSE);
  decimal_element->value.number.decimal = 9.75;
  EXPECT_EQ(parser.CallParserConst(ctx_, decimal_element, &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::DOUBLE);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeStringElement(AstEleType::STRING_TYPE, "value"),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::CHAR);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeStringElement(AstEleType::INTERVAL_TYPE, "1s"),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_field_type(), Field::FIELD_INTERVAL);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeStringElement(AstEleType::BYTES_TYPE, "aa55"), &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::BINARY);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMP_TYPE, 12345), &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMP);
  field = nullptr;

  EXPECT_EQ(
      parser.CallParserConst(ctx_,
                             MakeStringElement(AstEleType::DATE_TYPE,
                                               "2024-03-01"),
                             &field),
      EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::DATE);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, std::make_shared<Element>(AstEleType::NULL_TYPE, KFALSE),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::NULLVAL);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, std::make_shared<Element>(AstEleType::Function, KFALSE),
                &field),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(field, nullptr);

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMPTZ_TYPE, 12345),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMPTZ);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMP_MICRO_TYPE, 12345),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMP_MICRO);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMPTZ_MICRO_TYPE, 12345),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMPTZ_MICRO);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMP_NANO_TYPE, 12345),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMP_NANO);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, MakeIntElement(AstEleType::TIMESTAMPTZ_NANO_TYPE, 12345),
                &field),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->get_storage_type(), roachpb::DataType::TIMESTAMPTZ_NANO);
  field = nullptr;

  EXPECT_EQ(parser.CallParserConst(
                ctx_, std::make_shared<Element>(AstEleType::PLUS, KFALSE),
                &field),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(field, nullptr);
}

TEST_F(ParserWhiteBoxTest, ParserCastAndBuildOutputFieldsHandleSupportedBranches) {
  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::FloatFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::BoolFamily, 1));
  post.add_output_types(
      MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 16));
  post.add_output_types(
      MarshalToOutputType(KWDBTypeFamily::UnknownFamily, 0));
  post.add_output_types(
      MarshalToOutputType(KWDBTypeFamily::DecimalFamily, 8));
  InspectableBaseParser parser(&post, nullptr);

  FieldInt int_field(0, roachpb::DataType::INT, sizeof(k_int32));
  FieldLonglong ts_field(1, roachpb::DataType::TIMESTAMP, sizeof(k_int64));
  FieldLonglong tstz_field(2, roachpb::DataType::TIMESTAMPTZ,
                           sizeof(k_int64));

  EXPECT_TRUE((dynamic_cast<FieldTypeCastSigned<k_int64>*>(
                   parser.CallParserCast(ctx_, &int_field, "INT8")) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastSigned<k_int32>*>(
                   parser.CallParserCast(ctx_, &int_field, "INT4")) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastSigned<k_int16>*>(
                   parser.CallParserCast(ctx_, &int_field, "INT2")) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastReal<k_float32>*>(
                   parser.CallParserCast(ctx_, &int_field, "FLOAT4")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastReal<k_float64>*>(
                   parser.CallParserCast(ctx_, &int_field, "FLOAT8")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastTimestamptz2String*>(
                   parser.CallParserCast(ctx_, &ts_field, "VARCHAR(16)")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastTimestamptz2String*>(
                   parser.CallParserCast(ctx_, &tstz_field, "STRING(16)")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastTimestampTz*>(
                   parser.CallParserCast(ctx_, &int_field, "TIMESTAMPTZ(6)")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastTimestampTz*>(
                   parser.CallParserCast(ctx_, &int_field, "DATE")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastBool*>(
                   parser.CallParserCast(ctx_, &int_field, "BOOL")) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastBytes*>(
                   parser.CallParserCast(ctx_, &int_field, "VARBYTES")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastBytes*>(
                   parser.CallParserCast(ctx_, &int_field, "BYTES(4)")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastBytes*>(
                   parser.CallParserCast(ctx_, &int_field, "BYTES")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastDecimal*>(
                   parser.CallParserCast(ctx_, &int_field, "DECIMAL")) !=
               nullptr));
  EXPECT_TRUE((dynamic_cast<FieldTypeCastString*>(
                   parser.CallParserCast(ctx_, &int_field, "CHAR(8)")) !=
               nullptr));

  EEPgErrorInfo::ResetPgErrorInfo();
  EXPECT_EQ(parser.CallParserCast(ctx_, &int_field, "UNSUPPORTED"), nullptr);
  EXPECT_TRUE(EEPgErrorInfo::IsError());
  EEPgErrorInfo::ResetPgErrorInfo();

  FieldDouble render0(0, roachpb::DataType::DOUBLE, sizeof(k_double64));
  FieldBool render1(1, roachpb::DataType::BOOL, sizeof(k_bool));
  FieldLonglong render2(2, roachpb::DataType::TIMESTAMP, sizeof(k_int64));
  FieldVarchar render3(3, roachpb::DataType::VARCHAR, 16);
  FieldConstNull render4(roachpb::DataType::NULLVAL, 0);
  FieldDecimal render5(5, roachpb::DataType::DECIMAL, sizeof(k_double64));
  Field* renders[] = {&render0, &render1, &render2, &render3, &render4,
                      &render5};
  std::vector<k_uint32> output_indexes{0, 1, 2, 3, 4, 5};
  std::vector<Field*> output_fields;
  EXPECT_EQ(parser.CallBuildOutputFields(ctx_, renders, 6, output_indexes,
                                         output_indexes, output_fields, false),
            EEIteratorErrCode::EE_OK);
  ASSERT_EQ(output_fields.size(), 6U);
  TakeFields(output_fields);
  EXPECT_TRUE(output_fields[0]->is_chunk_);
  EXPECT_EQ(output_fields[3]->get_storage_type(), roachpb::DataType::VARCHAR);
  EXPECT_EQ(output_fields[4]->get_storage_type(), roachpb::DataType::NULLVAL);

  std::vector<Field*> bad_output_fields;
  std::vector<k_uint32> bad_types{99};
  EXPECT_EQ(parser.CallBuildOutputFields(ctx_, renders, 1, {0}, bad_types,
                                         bad_output_fields, false),
            EEIteratorErrCode::EE_ERROR);
  TakeFields(bad_output_fields);
}

TEST_F(ParserWhiteBoxTest, ParserOperatorInHelpersAndFunctionsCoverExtraBranches) {
  PostProcessSpec post;
  InspectableBaseParser parser(&post, nullptr);

  FieldConstInt int_left(roachpb::DataType::BIGINT, 1, sizeof(k_int64));
  FieldConstInt int_right(roachpb::DataType::BIGINT, 2, sizeof(k_int64));
  FieldConstString string_left(roachpb::DataType::CHAR, "AbC");
  FieldConstString string_right(roachpb::DataType::CHAR, "(1:::INT8, 2:::INT8)");
  FieldConstNull null_field(roachpb::DataType::NULLVAL, 0);

  std::list<Field*> args{&int_left, &int_right};
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::DIVIDEZ, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::REMAINDER, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ANDCAL, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ORCAL, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::TILDE, &int_left,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::REGEX, &string_left,
                                      &string_left, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::IREGEX, &string_left,
                                      &string_left, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::LEFTSHIFT, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::RIGHTSHIFT, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ANY, nullptr, nullptr,
                                      args, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ALL, nullptr, nullptr,
                                      args, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::NOT, &int_left, nullptr,
                                      {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ILike, &string_left,
                                      &string_left, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::IS_NULL, &null_field,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::IS_UNKNOWN, &null_field,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::IS_NAN, &int_left,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::WHEN, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::THEN, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::CASE, nullptr, nullptr,
                                      args, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::CASE, &int_left,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::CASE, &int_left,
                                      &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::ELSE, &int_left,
                                      nullptr, {}, KFALSE, ""),
            nullptr);
  EXPECT_NE(parser.CallParserOperator(ctx_, AstEleType::COALESCE, &null_field,
                                      &int_right, {}, KFALSE, ""),
            nullptr);

  Field* in_field =
      TakeField(parser.CallParserInOperator(ctx_, &int_left, &string_right, KFALSE));
  EXPECT_NE(in_field, nullptr);

  Field* parsed_value = parser.CallParserInField(ctx_, "1:::INT8");
  ASSERT_NE(parsed_value, nullptr);
  EXPECT_EQ(parsed_value->get_storage_type(), roachpb::DataType::BIGINT);

  Field* tuple_fields[2] = {nullptr, nullptr};
  k_bool has_null = KFALSE;
  EXPECT_EQ(parser.CallParserInValueString(ctx_, "(1:::INT8,NULL)",
                                           tuple_fields, &has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_TRUE(has_null);
  ASSERT_NE(tuple_fields[0], nullptr);
  ASSERT_NE(tuple_fields[1], nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "now", {}, KFALSE)), nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "current_date", {}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "timeofday", {}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "age", {&int_left}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "age", {&int_left, &int_right},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "length", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "bit_length", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "octet_length", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "random", {}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "width_bucket",
                                          {&int_left, &int_right, &int_right,
                                           &int_right},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "crc32c", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "crc32ieee", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "fnv32", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "fnv32a", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "fnv64", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "fnv64a", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "chr", {&int_left}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "get_bit",
                                          {&string_left, &int_right}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "initcap", {&string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "encode",
                                          {&string_left, &string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "decode",
                                          {&string_left, &string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "concat",
                                          {&string_left, &string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "cast_check_ts",
                                          {&string_left, &string_left},
                                          KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "abs", {&int_left}, KFALSE)),
            nullptr);
  EXPECT_NE(TakeField(parser.CallParserFuncOperator(ctx_, "pow",
                                          {&int_left, &int_right}, KFALSE)),
            nullptr);

  EEPgErrorInfo::ResetPgErrorInfo();
  EXPECT_EQ(parser.CallParserOperator(ctx_, static_cast<AstEleType>(9999),
                                      &int_left, &int_right, {}, KFALSE, ""),
            nullptr);
  EXPECT_TRUE(EEPgErrorInfo::IsError());
}

TEST_F(ParserWhiteBoxTest, ParserFilterAndOperatorParserCoverExpressionBranches) {
  PostProcessSpec post;
  auto* filter = post.mutable_filter();
  filter->set_expr(" @1 > 1:::INT8 ");
  InspectableBaseParser parser(&post, nullptr);
  FieldInt ref_field(0, roachpb::DataType::INT, sizeof(k_int32));
  parser.references_.push_back(&ref_field);

  Field* field = nullptr;
  EXPECT_EQ(parser.ParserFilter(ctx_, &field), EEIteratorErrCode::EE_OK);
  EXPECT_NE(field, nullptr);
  field = nullptr;

  post.mutable_filter()->set_expr(" none ");
  EXPECT_EQ(parser.ParserFilter(ctx_, &field), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(field, nullptr);

  post.mutable_filter()->set_expr("   ");
  EXPECT_EQ(parser.ParserFilter(ctx_, &field), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(field, nullptr);

  post.mutable_filter()->set_expr("(");
  EXPECT_EQ(parser.ParserFilter(ctx_, &field), EEIteratorErrCode::EE_ERROR);

  DummyOperator input(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))});
  input.BuildRenderFromOutputFields();

  PostProcessSpec passthrough_post;
  PassthroughOperatorParser passthrough(&passthrough_post, nullptr);
  passthrough.AddInput(&input);
  EXPECT_EQ(passthrough.ParserInputRenderSize(), 1);

  Field** render = nullptr;
  EXPECT_EQ(passthrough.ParserRender(ctx_, &render, 0), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(render, input.GetRender());
}

TEST_F(ParserWhiteBoxTest,
       AggregateParserResolveAggColAndHandleRenderCoverCoreBranches) {
  DummyOperator input({
      new FieldLonglong(0, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldDouble(2, roachpb::DataType::DOUBLE, sizeof(k_double64)),
      new FieldVarchar(3, roachpb::DataType::VARCHAR, 16),
  });

  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);

  auto* agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
  agg->add_col_idx(2);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED);
  agg->add_col_idx(0);

  PostProcessSpec post;
  InspectableAggregateParser parser(&spec, &post, nullptr, nullptr);
  parser.AddInput(&input);

  std::vector<Field*> renders(spec.aggregations_size(), nullptr);
  EXPECT_EQ(parser.CallResolveAggCol(ctx_, renders.data(), true),
            EEIteratorErrCode::EE_OK);
  ASSERT_EQ(parser.aggs_size_, static_cast<k_uint32>(spec.aggregations_size()));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[0]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[1]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggInt*>(renders[2]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggInt*>(renders[3]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[4]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[5]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggDouble*>(renders[6]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggDecimal*>(renders[7]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[8]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[9]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[10]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[11]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[12]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[13]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[14]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[15]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggLonglong*>(renders[16]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggAvg*>(renders[17]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggString*>(renders[18]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggDouble*>(renders[19]) != nullptr));
  EXPECT_TRUE((dynamic_cast<FieldAggDouble*>(renders[20]) != nullptr));

  PostProcessSpec output_post;
  output_post.add_output_columns(1);
  output_post.add_output_columns(0);
  InspectableAggregateParser output_parser(&spec, &output_post, nullptr, nullptr);
  output_parser.AddInput(&input);
  k_uint32 render_size = 0;
  output_parser.RenderSize(ctx_, &render_size);
  EXPECT_EQ(render_size, 2U);

  Field* copied_outputs[2] = {nullptr, nullptr};
  EXPECT_EQ(output_parser.HandleRender(ctx_, copied_outputs, 2),
            EEIteratorErrCode::EE_OK);
  EXPECT_EQ(copied_outputs[0], output_parser.aggs_[1]);
  EXPECT_EQ(copied_outputs[1], output_parser.aggs_[0]);

  auto output_ref =
      std::make_shared<VirtualField>(std::list<k_uint32>{1, 2});
  Field* output_field = nullptr;
  EXPECT_EQ(output_parser.ParserReference(ctx_, output_ref, &output_field),
            EEIteratorErrCode::EE_OK);
  ASSERT_EQ(output_field, output_parser.outputs_[0]);
  ASSERT_EQ(output_field->next_, output_parser.outputs_[1]);

  PostProcessSpec render_post;
  render_post.add_output_columns(1);
  render_post.add_output_columns(0);
  render_post.add_render_exprs()->set_expr("@2");
  InspectableAggregateParser render_parser(&spec, &render_post, nullptr, nullptr);
  render_parser.AddInput(&input);
  render_size = 0;
  render_parser.RenderSize(ctx_, &render_size);
  EXPECT_EQ(render_size, 1U);

  Field* render_expr_field[1] = {nullptr};
  EXPECT_EQ(render_parser.HandleRender(ctx_, render_expr_field, 1),
            EEIteratorErrCode::EE_OK);
  EXPECT_EQ(render_expr_field[0], render_parser.outputs_[1]);

  auto invalid_output_ref =
      std::make_shared<VirtualField>(std::list<k_uint32>{3});
  output_field = nullptr;
  EXPECT_EQ(render_parser.ParserReference(ctx_, invalid_output_ref, &output_field),
            EEIteratorErrCode::EE_ERROR);

  PostProcessSpec plain_post;
  InspectableAggregateParser plain_parser(&spec, &plain_post, nullptr, nullptr);
  plain_parser.AddInput(&input);
  render_size = 0;
  plain_parser.RenderSize(ctx_, &render_size);
  EXPECT_EQ(render_size, static_cast<k_uint32>(spec.aggregations_size()));
  auto invalid_agg_ref =
      std::make_shared<VirtualField>(std::list<k_uint32>{100});
  output_field = nullptr;
  EXPECT_EQ(plain_parser.ParserReference(ctx_, invalid_agg_ref, &output_field),
            EEIteratorErrCode::EE_ERROR);

}

TEST_F(ParserWhiteBoxTest, ScanParserUsesRenderExpressionsWithRealTableFields) {
  CreateSimpleTable();
  post_->add_output_columns(0);
  post_->add_output_columns(1);
  post_->add_output_columns(2);
  post_->add_output_types(
      MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  post_->add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
  post_->add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 16));
  post_->add_render_exprs()->set_expr("@2 + 1:::INT8");
  post_->add_render_exprs()->set_expr("@3");
  post_->mutable_filter()->set_expr("@2 > 0:::INT8");

  TsScanParser parser(post_, table_);
  EXPECT_EQ(parser.ParserInputField(ctx_), EEIteratorErrCode::EE_OK);

  Field* filter = nullptr;
  EXPECT_EQ(parser.ParserFilter(ctx_, &filter), EEIteratorErrCode::EE_OK);
  EXPECT_NE(filter, nullptr);
  filter = nullptr;

  k_uint32 render_size = 0;
  parser.RenderSize(ctx_, &render_size);
  EXPECT_EQ(render_size, 2U);

  Field** renders = nullptr;
  EXPECT_EQ(parser.ParserRender(ctx_, &renders, render_size),
            EEIteratorErrCode::EE_OK);
  ASSERT_NE(renders, nullptr);
  EXPECT_NE(renders[0], nullptr);
  EXPECT_NE(renders[1], nullptr);
  free(renders);
}

}  // namespace
}  // namespace kwdbts
