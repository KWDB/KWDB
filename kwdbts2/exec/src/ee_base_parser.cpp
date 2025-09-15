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

#include "ee_base_parser.h"
#include "cm_lexer.h"
#include "ee_parse_query.h"
#include "ee_field_compare.h"
#include "ee_field_func.h"
#include "ee_field_func_math.h"
#include "ee_field_func_string.h"
#include "ee_field_typecast.h"
#include "ee_field_window.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

#define InValueFree(in_value) \
  for (auto it : in_value) {  \
    SafeFreePointer(it);      \
    Return(nullptr);          \
  }

static void ee_trim(std::string *s) {
  if (s->empty()) {
    return;
  }
  s->erase(0, s->find_first_not_of(" "));
  s->erase(s->find_last_not_of(" ") + 1);
}

static void trim_brackets(std::string *str) {
  std::string::size_type pos = str->find_first_of('(');
  if (pos != std::string::npos) {
    str->erase(0, pos + 1);
  }

  pos = str->find_last_of(')');
  if (pos != std::string::npos) {
    str->erase(pos);
  }
}

static k_uint32 extractNumInBrackets(const std::string &input) {
  size_t leftPos = input.find('(');
  size_t rightPos = input.find(')', leftPos);
  std::stringstream ss;
  if (leftPos != std::string::npos && rightPos != std::string::npos) {
    ss.str(input.substr(leftPos + 1, rightPos - leftPos - 1));
    k_uint32 v;
    ss >> v;
    return v;
  } else {
    return 0;
  }
}


TsBaseParser::TsBaseParser(TSPostProcessSpec *post, TABLE *table)
  : post_(post),
    table_(table),
    renders_size_(post->renders_size()) {}

TsBaseParser::~TsBaseParser() {
  for (auto it : new_fields_) {
    SafeDeletePointer(it);
  }
}

EEIteratorErrCode TsBaseParser::ParserFilter(kwdbContext_p ctx, Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  do {
    if (!post_->has_filter()) {
      break;
    }

    std::string filter = post_->filter();
    ee_trim(&filter);
    if (filter.empty() || 0 == filter.compare("none")) {
      break;
    }

    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, filter, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve binary tree
    *field = ParserBinaryTree(ctx, expr);
    if (nullptr == *field) {
      LOG_ERROR("Parser filter failed, filter is %s.", filter.c_str());
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode TsBaseParser::ParserOutputFields(kwdbContext_p ctx, Field **renders, k_uint32 num,
                          std::vector<Field*> &output_fields, bool ignore_outputtype) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  std::vector<k_uint32> outputcol_indexs;
  std::vector<k_uint32> outputcol_types;
  for (k_uint32 i = 0; i < num; ++i) {
    outputcol_indexs.push_back(i);
    outputcol_types.push_back(i);
  }

  code = BuildOutputFields(ctx, renders, num, outputcol_indexs, outputcol_types, output_fields, ignore_outputtype);

  Return(code);
}


EEIteratorErrCode TsBaseParser::BuildBinaryTree(kwdbContext_p ctx, const KString &str, ExprPtr *expr) const {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<kwdb::Tokens>(str.data(), str.data() + str.size(), max_query_size);
  kwdb::IParser::Pos pos(tokens_ptr, max_parser_depth);
  kwdbts::ParseQuery parser(str, pos);
  if (!parser.ParseImpl(reinterpret_cast<void*>(expr), nullptr) || *expr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid expr");
    LOG_ERROR("Parse expr failed, expr is: %s", str.c_str());
    code = EEIteratorErrCode::EE_ERROR;
  }

  Return(code);
}

Field *TsBaseParser::ParserBinaryTree(kwdbContext_p ctx, ExprPtr expr) {
  Field *field = nullptr;

  do {
    if (!expr->is_leaf) {
      Field *right = nullptr;
      if (expr->left != nullptr) {
        Field *left = ParserBinaryTree(ctx, expr->left);
        if (nullptr == left) {
          break;
        }

        if (expr->right) {
          right = ParserBinaryTree(ctx, expr->right);
          if (nullptr == right) {
            break;
          }
          field = ParserOperator(ctx, expr->operator_type, left, right, {},
                                  expr->is_negative, "");
        } else if (expr->operator_type == AstEleType::THEN ||
                   expr->operator_type == AstEleType::TILDE || expr->operator_type == AstEleType::NOT) {
          field = ParserOperator(ctx, expr->operator_type, left, nullptr, {},
                                  expr->is_negative, "");
        } else if (expr->operator_type == AstEleType::CAST) {
          field = ParserCast(ctx, left, expr->const_ptr->value.string_type);
        } else {
          field = left;
        }
      } else if (expr->operator_type == AstEleType::ANY ||
        expr->operator_type == AstEleType::ALL) {
        std::list<Field *> args;
        for (auto arg : expr->args) {
          auto a = ParserBinaryTree(ctx, arg);
          if (a == nullptr) {
            return nullptr;
          }
          args.push_back(a);
        }

        field = ParserOperator(ctx, expr->operator_type, nullptr, nullptr,
                                args, expr->is_negative, "");
      }
    } else {
      if (expr->reference_ptr != nullptr) {
        ParserReference(ctx, expr->reference_ptr, &field);
      } else if (expr->operator_type == AstEleType::Function ||
                 expr->operator_type == AstEleType::CASE) {
        Field *left{nullptr}, *right{nullptr};
        std::list<Field *> args;
        if (expr->left != nullptr) {
          left = ParserBinaryTree(ctx, expr->left);
          if (left == nullptr) {
            return nullptr;
          }
        }
        if (expr->right != nullptr) {
          right = ParserBinaryTree(ctx, expr->right);
          if (right == nullptr) {
            return nullptr;
          }
        }
        if (!expr->args.empty()) {
          for (auto arg : expr->args) {
            auto a = ParserBinaryTree(ctx, arg);
            if (a == nullptr) {
              return nullptr;
            }
            args.push_back(a);
          }
        }
        std::string func_type = expr->operator_type == AstEleType::Function
                                    ? expr->const_ptr->value.string_type
                                    : "";
        field = ParserOperator(ctx, expr->operator_type, left, right, args,
                                expr->is_negative, func_type);
      } else {
        ParserConst(ctx, expr->const_ptr, &field);
      }
    }
  } while (0);

  return field;
}

Field *TsBaseParser::ParserOperator(kwdbContext_p ctx, AstEleType operator_type,
                                    Field *left, Field *right,
                                    std::list<Field *> args, k_bool is_negative,
                                    KString func_name) {
  EnterFunc();
  Field *field = nullptr;

  switch (operator_type) {
    case AstEleType::PLUS: {
      field = KNEW FieldFuncPlus(left, right);
      break;
    }
    case AstEleType::MINUS: {
      field = KNEW FieldFuncMinus(left, right);
      break;
    }
    case AstEleType::MULTIPLE: {
      field = KNEW FieldFuncMult(left, right);
      break;
    }
    case AstEleType::DIVIDE: {
      field = KNEW FieldFuncDivide(left, right);
      break;
    }
    case AstEleType::DIVIDEZ: {
      field = KNEW FieldFuncDividez(left, right);
      break;
    }
    case AstEleType::REMAINDER: {
      field = KNEW FieldFuncRemainder(left, right);
      break;
    }
    case AstEleType::PERCENT: {
      field = KNEW FieldFuncPercent(left, right);
      break;
    }
    case AstEleType::POWER: {
      for (k_int32 i = 0; i < mathFuncBuiltinsNum2; i++) {
        if (mathFuncBuiltins2[i].name == "pow") {
          field = KNEW FieldFuncMath(left, right, mathFuncBuiltins2[i]);
          break;
        }
      }
      break;
    }
    case AstEleType::ANDCAL: {
      field = KNEW FieldFuncAndCal(left, right);
      break;
    }
    case AstEleType::ORCAL: {
      field = KNEW FieldFuncOrCal(left, right);
      break;
    }
    case AstEleType::TILDE: {
      field = KNEW FieldFuncNotCal(left);
      break;
    }
    case AstEleType::REGEX: {
      field = KNEW FieldFuncRegex(left, right, is_negative, false);
      break;
    }
    case AstEleType::IREGEX: {
      field = KNEW FieldFuncRegex(left, right, is_negative, true);
      break;
    }
    case AstEleType::EQUALS: {
      field = KNEW FieldFuncEq(left, right);
      break;
    }
    case AstEleType::LESS: {
      field = KNEW FieldFuncLess(left, right);
      break;
    }
    case AstEleType::GREATER: {
      field = KNEW FieldFuncGt(left, right);
      break;
    }
    case AstEleType::LESS_OR_EQUALS: {
      field = KNEW FieldFuncLessEq(left, right);
      break;
    }
    case AstEleType::GREATER_OR_EQUALS: {
      field = KNEW FieldFuncGtEq(left, right);
      break;
    }
    case AstEleType::NOT_EQUALS: {
      field = KNEW FieldFuncNotEq(left, right);
      break;
    }
    case AstEleType::LEFTSHIFT: {
      field = KNEW FieldFuncLeftShift(left, right);
      break;
    }
    case AstEleType::RIGHTSHIFT: {
      field = KNEW FieldFuncRightShift(left, right);
      break;
    }
    case AstEleType::AND: {
      field = KNEW FieldCondAnd(left, right);
      break;
    }
    case AstEleType::OR: {
      field = KNEW FieldCondOr(left, right);
      break;
    }
    case AstEleType::In: {
      field = ParserInOperator(ctx, left, right, is_negative);
      break;
    }
    case AstEleType::ANY: {
      field = KNEW FieldFuncAny(args);
      break;
    }
    case AstEleType::ALL: {
      field = KNEW FieldFuncAll(args);
      break;
    }
    case AstEleType::NOT: {
      field = KNEW FieldFuncNot(left);
      break;
    }
    case AstEleType::Like: {
      field = KNEW FieldFuncLike(left, right, is_negative, false);
      break;
    }
    case AstEleType::ILike: {
      field = KNEW FieldFuncLike(left, right, is_negative, true);
      break;
    }
    case AstEleType::Function: {
      field = ParserFuncOperator(ctx, func_name, args, is_negative);
      break;
    }
    case AstEleType::IS_NULL: {
      field = KNEW FieldCondIsNull(left, is_negative);
      break;
    }
    case AstEleType::IS_UNKNOWN: {
      field = KNEW FieldCondIsUnknown(left, is_negative);
      break;
    }
    case AstEleType::IS_NAN: {
      field = KNEW FieldCondIsNan(left, is_negative);
      break;
    }
    case AstEleType::WHEN: {
      field = KNEW FieldFuncEq(left, right);
      break;
    }
    case AstEleType::THEN: {
      field = KNEW FieldFuncThen(left, right);
      break;
    }
    case AstEleType::CASE: {
      if (args.size() > 0) {
        field = KNEW FieldFuncCase(args);
      } else if (right == nullptr) {
        field = KNEW FieldFuncCase(left);
      } else {
        field = KNEW FieldFuncCase(left, right);
      }
      break;
    }
    case AstEleType::ELSE: {
      field = KNEW FieldFuncElse(left);
      break;
    }
    case AstEleType::COALESCE: {
      field = KNEW FieldFuncCoalesce(left, right);
      break;
    }
    default: {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Undecided datatype");
      LOG_ERROR("unknow operator type, type is: %d.", operator_type);
      break;
    }
  }

  if (nullptr != field) {
    new_fields_.insert(new_fields_.end(), field);
    field->table_ = table_;
  } else {
    if (!EEPgErrorInfo::IsError()) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
    }
  }

  Return(field);
}

Field *TsBaseParser::ParserCast(kwdbContext_p ctx, Field *left, const KString &output_type) {
  EnterFunc();
  Field *field = nullptr;
  if (output_type == "INT8") {
    field = KNEW FieldTypeCastSigned<k_int64>(left);
  } else if (output_type == "INT4") {
    field = KNEW FieldTypeCastSigned<k_int32>(left);
  } else if (output_type == "INT2") {
    field = KNEW FieldTypeCastSigned<k_int16>(left);
  } else if (output_type == "FLOAT4") {
    field = KNEW FieldTypeCastReal<k_float32>(left);
    field->set_storage_type(roachpb::DataType::FLOAT);
    field->set_storage_length(sizeof(k_float32));
  } else if (output_type == "FLOAT8") {
    field = KNEW FieldTypeCastReal<k_float64>(left);
  } else if (output_type.find("CHAR") != std::string::npos ||
             output_type.find("STRING") != std::string::npos) {
    k_uint32 len = extractNumInBrackets(output_type);
    roachpb::DataType left_type = left->get_storage_type();
    if (left_type == roachpb::DataType::TIMESTAMPTZ ||
        left_type == roachpb::DataType::TIMESTAMPTZ_MICRO ||
        left_type == roachpb::DataType::TIMESTAMPTZ_NANO) {
      field = KNEW FieldTypeCastTimestamptz2String(left, len, output_type,
                                                   ctx->timezone);
    } else if (left_type == roachpb::DataType::TIMESTAMP ||
        left_type == roachpb::DataType::TIMESTAMP_MICRO ||
        left_type == roachpb::DataType::TIMESTAMP_NANO) {
      field = KNEW FieldTypeCastTimestamptz2String(left, len, output_type,
                                                   0);
    } else {
      field = KNEW FieldTypeCastString(left, len, output_type);
    }
  } else if (output_type.find("TIMESTAMPTZ") != std::string::npos) {
    k_uint32 type_num = extractNumInBrackets(output_type);
    field = KNEW FieldTypeCastTimestampTz(left, type_num, 0);
  } else if (output_type.find("TIMESTAMP") != std::string::npos || output_type == "DATE") {
    k_uint32 type_num = extractNumInBrackets(output_type);
    field = KNEW FieldTypeCastTimestampTz(left, type_num, ctx->timezone);
  } else if (output_type == "BOOL") {
    field = KNEW FieldTypeCastBool(left);
  } else if (output_type.find("BYTES") != std::string::npos) {
    k_uint32 len = extractNumInBrackets(output_type);
    if (len == 0) {
      if (output_type.find("VARBYTES") != std::string::npos) {
        field = KNEW FieldTypeCastBytes(left, 0, 0);
      } else {
        field = KNEW FieldTypeCastBytes(left, 1, 1);
      }
    } else {
      field = KNEW FieldTypeCastBytes(left, len, len);
    }
  } else if (output_type == "DECIMAL") {
    field = new FieldTypeCastDecimal(left);
  }
  if (nullptr != field) {
    field->table_ = table_;
    new_fields_.insert(new_fields_.end(), field);
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
  }

  Return(field);
}

EEIteratorErrCode TsBaseParser::ParserConst(kwdbContext_p ctx, std::shared_ptr<Element> const_ptr, Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  switch (const_ptr->operators) {
    case AstEleType::INT_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::BIGINT,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::FLOAT_TYPE: {
      *field = new FieldConstDouble(roachpb::DataType::DOUBLE,
                                    const_ptr->value.number.float_type);
      break;
    }
    case AstEleType::DECIMAL: {
      *field = new FieldConstDouble(roachpb::DataType::DOUBLE,
                                    const_ptr->value.number.decimal);
      break;
    }
    case AstEleType::STRING_TYPE: {
      *field = new FieldConstString(roachpb::DataType::CHAR,
                                    const_ptr->value.string_type);
      break;
    }
    case AstEleType::INTERVAL_TYPE: {
      *field = new FieldConstInterval(roachpb::DataType::TIMESTAMP,
                                      const_ptr->value.string_type);
      break;
    }
    case AstEleType::BYTES_TYPE: {
      *field = new FieldConstString(roachpb::DataType::BINARY,
                                    const_ptr->value.string_type);
      break;
    }
    case AstEleType::Function: {
      //       *field = KNEW FieldConst<KString>(roachpb::DataType::FUNCTION,
      //                                             const_ptr->value.string_type);
      break;
    }
    case AstEleType::TIMESTAMP_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMP,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::TIMESTAMPTZ_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMPTZ,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::TIMESTAMP_MICRO_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMP_MICRO,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::TIMESTAMPTZ_MICRO_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMPTZ_MICRO,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::TIMESTAMP_NANO_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMP_NANO,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::TIMESTAMPTZ_NANO_TYPE: {
      *field =
          new FieldConstInt(roachpb::DataType::TIMESTAMPTZ_NANO,
                            const_ptr->value.number.int_type, sizeof(k_int64));
      break;
    }
    case AstEleType::DATE_TYPE: {
      *field = KNEW FieldConstString(roachpb::DataType::DATE,
                                     const_ptr->value.string_type);
      break;
    }
    case AstEleType::NULL_TYPE: {
      *field = KNEW FieldConstNull(roachpb::DataType::NULLVAL, 0);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Undecided datatype");
      break;
  }

  if (nullptr == *field) {
    LOG_ERROR("malloc failed.");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    code = EEIteratorErrCode::EE_ERROR;
  } else {
    new_fields_.insert(new_fields_.end(), *field);
  }

  Return(code);
}

Field *TsBaseParser::ParserFuncOperator(kwdbContext_p ctx, const KString &func_name, std::list<Field *> args,
                                                              k_bool is_negative) {
  EnterFunc();
  Field *field = nullptr;
  Field *right{nullptr};
  if (args.size() == 2) {
    auto it = std::next(args.begin(), 1);
    right = (*it);
  }

  if (func_name == "time_bucket") {
    FieldFuncTimeBucket *time_field = KNEW FieldFuncTimeBucket(args, ctx->timezone);
    if (time_field && EEPgErrorInfo::IsError()) {
      SafeDeletePointer(time_field);
    } else {
      field = time_field;
    }
  } else if (func_name == "now") {
    field = KNEW FieldFuncNow();
  } else if (func_name == "current_date") {
    field = KNEW FieldFuncCurrentDate();
  } else if (func_name == "current_timestamp" ||
             func_name == "localtimestamp") {
    field = KNEW FieldFuncCurrentTimeStamp(args.front(), ctx->timezone);
  } else if (func_name == "date_trunc") {
    field = KNEW FieldFuncDateTrunc(args.front(), right, ctx->timezone);
  } else if (func_name == "extract") {
    field = KNEW FieldFuncExtract(args.front(), right, ctx->timezone);
  } else if (func_name == "experimental_strftime") {
    field = KNEW FieldFuncExpStrftime(args.front(), right);
  } else if (func_name == "timeofday") {
    field = KNEW FieldFuncTimeOfDay(ctx->timezone);
  } else if (func_name == "age") {
    if (right == nullptr) {
      field = KNEW FieldFuncAge(args.front());
    } else {
      field = KNEW FieldFuncAge(args.front(), right);
    }
  } else if (func_name == "length" || func_name == "char_length" ||
             func_name == "character_length") {
    field = KNEW FieldFuncLength(args.front());
  } else if (func_name == "bit_length") {
    field = KNEW FieldFuncBitLength(args.front());
  } else if (func_name == "octet_length") {
    field = KNEW FieldFuncOctetLength(args.front());
  } else if (func_name == "random") {
    field = KNEW FieldFuncRandom();
  } else if (func_name == "width_bucket") {
    field = KNEW FieldFuncWidthBucket(args);
  } else if (func_name == "crc32c") {
    field = KNEW FieldFuncCrc32C(args);
  } else if (func_name == "crc32ieee") {
    field = KNEW FieldFuncCrc32I(args);
  } else if (func_name == "fnv32") {
    field = KNEW FieldFuncFnv32(args);
  } else if (func_name == "fnv32a") {
    field = KNEW FieldFuncFnv32a(args);
  } else if (func_name == "fnv64") {
    field = KNEW FieldFuncFnv64(args);
  } else if (func_name == "fnv64a") {
    field = KNEW FieldFuncFnv64a(args);
  } else if (func_name == "chr") {
    field = KNEW FieldFuncChr(args.front());
  } else if (func_name == "get_bit") {
    field = KNEW FieldFuncGetBit(args.front(), right);
  } else if (func_name == "initcap") {
    field = KNEW FieldFuncInitCap(args.front());
  } else if (func_name == "encode") {
    field = KNEW FieldFuncEncode(args.front(), right);
  } else if (func_name == "decode") {
    field = KNEW FieldFuncDecode(args.front(), right);
  } else if (func_name == "concat" || func_name == "substr" ||
             func_name == "substring" || func_name == "lpad" ||
             func_name == "rpad" || func_name == "ltrim" ||
             func_name == "rtrim" || func_name == "left" ||
             func_name == "right" || func_name == "upper" ||
             func_name == "lower") {
    field = KNEW FieldFuncString(func_name, args);
  } else if (func_name == "cast_check_ts") {
    field = KNEW FieldFuncCastCheckTs(args.front(), right);
  } else if (func_name == "state_window") {
    field = KNEW FieldFuncStateWindow(args.front());
    current_thd->wtyp_ = WindowGroupType::EE_WGT_STATE;
    current_thd->window_field_ = field;
  } else if (func_name == "event_window") {
    field = KNEW FieldFuncEventWindow(args.front(), right);
    current_thd->wtyp_ = WindowGroupType::EE_WGT_EVENT;
    current_thd->window_field_ = field;
  } else if (func_name == "session_window") {
    field = KNEW FieldFuncSessionWindow(args.front(), right);
    current_thd->wtyp_ = WindowGroupType::EE_WGT_SESSION;
    current_thd->window_field_ = field;
  } else if (func_name == "count_window") {
    field = KNEW FieldFuncCountWindow(args);
    current_thd->wtyp_ = WindowGroupType::EE_WGT_COUNT;
    if ((static_cast<FieldFuncCountWindow *>(field))->IsSilding()) {
      current_thd->wtyp_ = WindowGroupType::EE_WGT_COUNT_SLIDING;
    }
    current_thd->window_field_ = field;
  } else if (func_name == "time_window") {
    field = KNEW FieldFuncTimeWindow(args, ctx->timezone);
    current_thd->wtyp_ = WindowGroupType::EE_WGT_TIME;
    current_thd->window_field_ = field;
  } else if (func_name == "time_window_start") {
    field = KNEW FieldFuncTimeWindowStart(args.front());
    current_thd->window_start_field_ = field;
  } else if (func_name == "time_window_end") {
    field = KNEW FieldFuncTimeWindowEnd(args.front());
    current_thd->window_end_field_ = field;
  } else {  // MathFields
    if (args.size() == 1) {
      for (k_int32 i = 0; i < mathFuncBuiltinsNum1; i++) {
        if (mathFuncBuiltins1[i].name == func_name) {
          field = KNEW FieldFuncMath(args, mathFuncBuiltins1[i]);
          break;
        }
      }
    } else if (args.size() == 2) {
      for (k_int32 i = 0; i < mathFuncBuiltinsNum2; i++) {
        if (mathFuncBuiltins2[i].name == func_name) {
          field = KNEW FieldFuncMath(args, mathFuncBuiltins2[i]);
          break;
        }
      }
    }
  }
  if (nullptr != field) {
    field->table_ = table_;
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
  }

  Return(field);
}

Field *TsBaseParser::ParserInOperator(kwdbContext_p ctx, Field *left, Field *right, k_bool is_negative) {
  EnterFunc();
  Field *field = nullptr;
  std::list<Field *> fields;
  Field *cur = left;
  // resolve field
  while (nullptr != cur) {
    fields.push_back(cur);
    cur = cur->next_;
  }
  size_t num = fields.size();
  // resolve in
  std::list<Field **> in_values;
  String s1 = right->ValStr();
  std::string str = std::string(s1.getptr(), s1.length_);
  trim_brackets(&str);

  Field **malloc_field = nullptr;
  std::string::size_type pos = std::string::npos;
  size_t i = 1;
  std::string::size_type begin = 0;
  k_bool have_null = KFALSE;
  while ((pos = str.find_first_of(',', begin)) != std::string::npos) {
    if (i < num) {
      ++i;
      begin = pos + 1;
      continue;
    }
    i = 1;
    begin = 0;
    malloc_field = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == malloc_field) {
      LOG_ERROR("malloc failed.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      InValueFree(in_values);
    }
    in_values.push_back(malloc_field);
    std::string substr = str.substr(0, pos);
    //  Delete processed parts in str. Include space and comma.
    str.erase(0, pos + 2);
    EEIteratorErrCode code = ParserInValueString(ctx, substr, malloc_field, &have_null);
    if (EEIteratorErrCode::EE_ERROR == code) {
      InValueFree(in_values);
    }
  }

  if (!str.empty()) {
    malloc_field = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == malloc_field) {
      LOG_ERROR("malloc failed.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      InValueFree(in_values);
    }
    in_values.push_back(malloc_field);
    EEIteratorErrCode code = ParserInValueString(ctx, str, malloc_field, &have_null);
    if (EEIteratorErrCode::EE_ERROR == code) {
      InValueFree(in_values);
    }
  }

  field = new FieldFuncIn(fields, in_values, fields.size(), is_negative, have_null);

  Return(field);
}

EEIteratorErrCode TsBaseParser::ParserInValueString(kwdbContext_p ctx, std::string substr, Field **malloc_field,
                                                        k_bool *has_null) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  if (('(' == substr[0]) && (')' == substr[substr.length() - 1])) {
    trim_brackets(&substr);
  }

  do {
    std::string::size_type pos = std::string::npos;
    int i = 0;
    while ((pos = substr.find_first_of(',')) != std::string::npos) {
      std::string str = substr.substr(0, pos);
      // str = str.substr(0, str.find_first_of(':'));
      substr.erase(0, pos + 1);
      Field *cst = ParserInField(ctx, str);
      if (nullptr == cst) {
        code = EEIteratorErrCode::EE_ERROR;
        break;
      }
      if (cst->get_storage_type() == roachpb::DataType::NULLVAL) {
        *has_null = KTRUE;
      }

      malloc_field[i] = cst;
      ++i;
    }

    if (EEIteratorErrCode::EE_ERROR == code) {
      break;
    }

    Field *cst = ParserInField(ctx, substr);
    if (nullptr == cst) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
    if (cst->get_storage_type() == roachpb::DataType::NULLVAL) {
      *has_null = KTRUE;
    }
    malloc_field[i] = cst;
  } while (0);

  Return(code);
}

Field *TsBaseParser::ParserInField(kwdbContext_p ctx, const std::string &str) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  Field *in_field = nullptr;
  do {
    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, str, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve binary tree
    in_field = ParserBinaryTree(ctx, expr);
    if (nullptr == in_field) {
      LOG_ERROR("Parser in clause failed, expr is %s.", str.c_str());
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
  } while (0);

  Return(in_field);
}

EEIteratorErrCode TsBaseParser::BuildOutputFields(kwdbContext_p ctx, Field **renders, k_uint32 num,
          const std::vector<k_uint32> &outputcol_indexs, const std::vector<k_uint32> &outputcol_types,
          std::vector<Field*> &output_fields, bool ignore_outputtype) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_uint32 outputcols_count = outputcol_indexs.size();
  for (int i = 0; i < outputcols_count; ++i) {
    Field *field = renders[outputcol_indexs[i]];
    // Output Field object
    Field *new_field = nullptr;
    switch (field->get_storage_type()) {
        case roachpb::DataType::DOUBLE:
          new_field = KNEW FieldDouble(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::FLOAT:
          new_field = KNEW FieldFloat(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::BOOL:
          new_field = KNEW FieldBool(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::SMALLINT:
          new_field = KNEW FieldShort(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::INT:
          new_field = KNEW FieldInt(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT:
          new_field = KNEW FieldLonglong(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::CHAR:
          new_field = KNEW FieldChar(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::VARCHAR:
          new_field = KNEW FieldVarchar(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::NCHAR:
          new_field = KNEW FieldNchar(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::NVARCHAR:
          new_field = KNEW FieldNvarchar(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::BINARY:
          new_field = KNEW FieldBlob(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::VARBINARY:
          new_field = KNEW FieldVarBlob(i, field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::NULLVAL:
          new_field = KNEW FieldConstNull(field->get_storage_type(), field->get_storage_length());
          break;
        case roachpb::DataType::DECIMAL:
          new_field = KNEW FieldDecimal(i, field->get_storage_type(), field->get_storage_length());
          break;
        default:
          LOG_WARN("Unknown Output Field Type: %d.", field->get_storage_type());
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Unknown Output Field Type");
          break;
    }
    if (new_field) {
      new_field->is_chunk_ = true;
      new_field->table_ = field->table_;
      if (!ignore_outputtype) {
        k_uint32 idx = outputcol_types[i];
        if (idx >= post_->outputtypes_size()) {
          LOG_ERROR("outputcol_types[%u] is invalid, outputtypes_size is %d", idx, post_->outputtypes_size());
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Unknown Output Field Return Type");
          code = EEIteratorErrCode::EE_ERROR;
          SafeDeletePointer(new_field);
          break;
        }
        new_field->set_return_type(post_->outputtypes(outputcol_types[i]));
        field->set_return_type(post_->outputtypes(outputcol_types[i]));
      }
      // DataChunk columns are treated as TYPEDATA
      new_field->set_column_type(::roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
      new_field->set_allow_null(field->is_allow_null());
      output_fields.push_back(new_field);
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      code = EEIteratorErrCode::EE_ERROR;
    }
  }

  Return(code);
}


TsScanParser::TsScanParser(TSPostProcessSpec *post, TABLE *table)
  : TsBaseParser(post, table),
    inputcols_count_(post->outputcols_size()) {}

TsScanParser::~TsScanParser() {
  if (inputcols_count_ > 0 && input_cols_!= nullptr) {
    free(input_cols_);
  }
  inputcols_count_ = 0;
  input_cols_ = nullptr;
}

EEIteratorErrCode TsScanParser::ParserFilter(kwdbContext_p ctx, Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  is_filter_ = 1;
  code = TsBaseParser::ParserFilter(ctx, field);
  is_filter_ = 0;

  Return(code);
}

EEIteratorErrCode TsScanParser::ParserInputField(kwdbContext_p ctx) {
  EnterFunc();

  if (inputcols_count_ > 0) {
    input_cols_ = static_cast<Field **>(malloc(inputcols_count_ * sizeof(Field *)));
    if (nullptr == input_cols_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(input_cols_, 0, inputcols_count_ * sizeof(Field *));
  }

  for (k_uint32 i = 0; i < inputcols_count_; i++) {
    k_uint32 col_index = post_->outputcols(i);
    Field* field = table_->GetFieldWithColNum(col_index);
    input_cols_[i] = field;
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TsScanParser::ParserReference(kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
                                                                                       Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    if (is_filter_) {
      if (nullptr == *field) {
        *field = table_->GetFieldWithColNum(i - 1);
      } else {
        (*field)->next_ = table_->GetFieldWithColNum(i - 1);
      }
    } else {
      if (nullptr == *field) {
        *field = input_cols_[i - 1];
      } else {
        (*field)->next_ = input_cols_[i - 1];
      }
    }
  }

  Return(code);
}

void TsScanParser::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_> 0) {
    *num = renders_size_;
  } else if (inputcols_count_ > 0) {
    *num = inputcols_count_;
  } else {
    *num = 0;
  }
}

EEIteratorErrCode TsScanParser::ParserRender(kwdbContext_p ctx, Field ***render, k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_bool has_timestamp = 0;

  if (num > 0) {
    *render = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == *render) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(*render, 0, num * sizeof(Field *));
  }

  if (renders_size_ == 0) {
    for (k_uint32 i = 0; i < inputcols_count_; i++) {
      (*render)[i] = input_cols_[i];
    }

    Return(EEIteratorErrCode::EE_OK);
  }

  // resolve render
  for (k_int32 i = 0; i < renders_size_; ++i) {
    std::string str = post_->renders(i);
    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, str, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve binary tree
    Field *field = ParserBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      (*render)[i] = field;
    }
  }

  Return(code);
}

TsOperatorParser::TsOperatorParser(TSPostProcessSpec *post, TABLE *table)
  : TsBaseParser(post, table),
    outputcol_count_(post->outputcols_size()) {}


EEIteratorErrCode TsOperatorParser::ParserRender(kwdbContext_p ctx, Field ***render, k_uint32 num) {
  EnterFunc();

  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  if (num == 0) {
    *render = input_->GetRender();
    Return(code);
  }

  // resolve renders
  if (num > 0) {
    *render = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == *render) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(*render, 0, num * sizeof(Field *));
  }

  // handle renders
  code = HandleRender(ctx, *render, num);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  Return(code);
}

}  // namespace kwdbts
