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
// Created by liguoliang on 2022/07/18.

#include "ee_flow_param.h"

#include <utility>

#include "ee_base_op.h"
#include "ee_field.h"
#include "ee_field_compare.h"
#include "ee_field_const.h"
#include "ee_field_func.h"
#include "ee_field_func_math.h"
#include "ee_field_func_string.h"
#include "ee_field_typecast.h"
#include "ee_field_window.h"
#include "ee_lexer.h"
#include "ee_kwthd_context.h"
#include "ee_parse_query.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "ee_token_iterator.h"
#include "kwdb_type.h"
#include "lg_api.h"
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

PostResolve::PostResolve(TSPostProcessSpec *post, TABLE *table)
    : post_(post),
      table_(table),
      renders_size_(post->renders_size()),
      outputcols_size_(post->outputcols_size()) {}

PostResolve::~PostResolve() {
  for (auto it : new_fields_) {
    SafeDeletePointer(it);
  }

  new_fields_.clear();

  if (outputcols_size_ > 0 && outputcols_ != nullptr) {
    free(outputcols_);
  }
  outputcols_size_ = 0;
  outputcols_ = nullptr;
}

EEIteratorErrCode PostResolve::ResolveOutputFields(kwdbContext_p ctx,
                                                 Field **renders,
                                                 k_uint32 num,
                                                 std::vector<Field*> &output_fields) {
  EnterFunc();
  for (int i = 0; i < num; ++i) {
    Field *field = renders[i];
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
      new_field->set_return_type(field->get_return_type());
      // DataChunk columns are treated as TYPEDATA
      new_field->set_column_type(::roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
      new_field->set_allow_null(field->is_allow_null());
      output_fields.push_back(new_field);
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    }
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode PostResolve::ResolveFilter(kwdbContext_p ctx, Field **field, bool is_tagFilter) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  do {
    if (!post_->has_filter()) {
      break;
    }

    std::string filter = post_->filter();
    ee_trim(&filter);
    if (filter.empty()) {
      break;
    }

    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, filter, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve binary tree
    *field = ResolveBinaryTree(ctx, expr);
    if (nullptr == *field) {
      LOG_ERROR("Resolve filter failed, filter is %s.", filter.c_str());
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode PostResolve::ResolveOutputType(kwdbContext_p ctx,
                                                 Field **render,
                                                 k_uint32 num) const {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  k_uint32 count = post_->outputtypes_size();
  if (count != num) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid outputtype");
    LOG_ERROR("outputtype num %u don't equal renders num %u", count, num);
    Return(EEIteratorErrCode::EE_ERROR);
  }

  for (k_int32 i = 0; i < count; ++i) {
    KWDBTypeFamily idx = post_->outputtypes(i);
    Field *field = render[i];
    field->set_return_type(idx);
  }

  Return(code);
}

EEIteratorErrCode PostResolve::BuildBinaryTree(kwdbContext_p ctx,
                                               const KString &str,
                                               ExprPtr *expr) const {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(
      str.data(), str.data() + str.size(), max_query_size);
  kwdbts::IParser::Pos pos(tokens_ptr, max_parser_depth);
  kwdbts::ParseQuery parser(str, pos);
  *expr = parser.ParseImpl();  // binary tree
  if (nullptr == *expr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid expr");
    LOG_ERROR("Parse expr failed, expr is: %s", str.c_str());
    code = EEIteratorErrCode::EE_ERROR;
  }

  Return(code);
}

Field *PostResolve::ResolveBinaryTree(kwdbContext_p ctx, ExprPtr expr) {
  Field *field = nullptr;

  do {
    if (!expr->is_leaf) {
      Field *right = nullptr;
      if (expr->left != nullptr) {
        Field *left = ResolveBinaryTree(ctx, expr->left);
        if (nullptr == left) {
          break;
        }

        if (expr->right) {
          right = ResolveBinaryTree(ctx, expr->right);
          if (nullptr == right) {
            break;
          }
          field = ResolveOperator(ctx, expr->operator_type, left, right, {},
                                  expr->is_negative, "");
        } else if (expr->operator_type == AstEleType::THEN ||
                   expr->operator_type == AstEleType::TILDE || expr->operator_type == AstEleType::NOT) {
          field = ResolveOperator(ctx, expr->operator_type, left, nullptr, {},
                                  expr->is_negative, "");
        } else if (expr->operator_type == AstEleType::CAST) {
          field = ResolveCast(ctx, left, expr->const_ptr->value.string_type);
        } else {
          field = left;
        }
      } else if (expr->operator_type == AstEleType::ANY ||
        expr->operator_type == AstEleType::ALL) {
        std::list<Field *> args;
        for (auto arg : expr->args) {
          auto a = ResolveBinaryTree(ctx, arg);
          if (a == nullptr) {
            return nullptr;
          }
          args.push_back(a);
        }

        field = ResolveOperator(ctx, expr->operator_type, nullptr, nullptr,
                                args, expr->is_negative, "");
      }
    } else {
      if (expr->reference_ptr != nullptr) {
        ResolveReference(ctx, expr->reference_ptr, &field);
      } else if (expr->operator_type == AstEleType::Function ||
                 expr->operator_type == AstEleType::CASE) {
        Field *left{nullptr}, *right{nullptr};
        std::list<Field *> args;
        if (expr->left != nullptr) {
          left = ResolveBinaryTree(ctx, expr->left);
          if (left == nullptr) {
            return nullptr;
          }
        }
        if (expr->right != nullptr) {
          right = ResolveBinaryTree(ctx, expr->right);
          if (right == nullptr) {
            return nullptr;
          }
        }
        if (!expr->args.empty()) {
          for (auto arg : expr->args) {
            auto a = ResolveBinaryTree(ctx, arg);
            if (a == nullptr) {
              return nullptr;
            }
            args.push_back(a);
          }
        }
        std::string func_type = expr->operator_type == AstEleType::Function
                                    ? expr->const_ptr->value.string_type
                                    : "";
        field = ResolveOperator(ctx, expr->operator_type, left, right, args,
                                expr->is_negative, func_type);
      } else {
        ResolveConst(ctx, expr->const_ptr, &field);
      }
    }
  } while (0);

  return field;
}

Field *PostResolve::ResolveOperator(kwdbContext_p ctx, AstEleType operator_type,
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
      field = ResolveInOperator(ctx, left, right, is_negative);
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
      field = ResolveFuncOperator(ctx, func_name, args, is_negative);
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

EEIteratorErrCode PostResolve::ResolveConst(kwdbContext_p ctx,
                                            std::shared_ptr<Element> const_ptr,
                                            Field **field) {
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

Field *PostResolve::ResolveInOperator(kwdbContext_p ctx, Field *left,
                                      Field *right, k_bool is_negative) {
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
    EEIteratorErrCode code =
        ResolveInValueString(ctx, substr, malloc_field, &have_null);
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
    EEIteratorErrCode code =
        ResolveInValueString(ctx, str, malloc_field, &have_null);
    if (EEIteratorErrCode::EE_ERROR == code) {
      InValueFree(in_values);
    }
  }

  field =
      new FieldFuncIn(fields, in_values, fields.size(), is_negative, have_null);

  Return(field);
}

EEIteratorErrCode PostResolve::ResolveInValueString(kwdbContext_p ctx,
                                                    std::string substr,
                                                    Field **malloc_field,
                                                    k_bool *have_null) {
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
      Field *cst = ResolveInField(ctx, str);
      if (nullptr == cst) {
        code = EEIteratorErrCode::EE_ERROR;
        break;
      }
      if (cst->get_storage_type() == roachpb::DataType::NULLVAL) {
        *have_null = KTRUE;
      }

      malloc_field[i] = cst;
      ++i;
    }

    if (EEIteratorErrCode::EE_ERROR == code) {
      break;
    }

    Field *cst = ResolveInField(ctx, substr);
    if (nullptr == cst) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
    if (cst->get_storage_type() == roachpb::DataType::NULLVAL) {
      *have_null = KTRUE;
    }
    malloc_field[i] = cst;
  } while (0);

  Return(code);
}

Field *PostResolve::ResolveInField(kwdbContext_p ctx, const std::string &str) {
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
    in_field = ResolveBinaryTree(ctx, expr);
    if (nullptr == in_field) {
      LOG_ERROR("Resolve in clause failed, expr is %s.", str.c_str());
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
  } while (0);

  Return(in_field);
}

BasePostResolve::BasePostResolve(BaseOperator *input, TSPostProcessSpec *post,
                                 TABLE *table)
    : PostResolve(post, table), input_{input} {}

BasePostResolve::~BasePostResolve() {}

EEIteratorErrCode BasePostResolve::ResolveRender(kwdbContext_p ctx,
                                                 Field ***render,
                                                 k_uint32 num) {
  EnterFunc();

  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // resolve outputcol
  code = ResolveOutputCol(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
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

  code = ResolveOutputType(ctx, *render, num);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  Return(code);
}

EEIteratorErrCode BasePostResolve::ResolveOutputCol(kwdbContext_p ctx) {
  EnterFunc();
  // dispose outputcols
  if (nullptr != outputcols_) {
    return EEIteratorErrCode::EE_OK;
  }
  if (outputcols_size_ > 0) {
    outputcols_ =
        static_cast<Field **>(malloc(outputcols_size_ * sizeof(Field *)));
    if (nullptr == outputcols_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }

    memset(outputcols_, 0, outputcols_size_ * sizeof(Field *));
  }

  for (k_int32 i = 0; i < outputcols_size_; ++i) {
    k_uint32 tab = post_->outputcols(i);
    Field *field = table_->GetFieldWithColNum(tab);
    outputcols_[i] = field;
  }

  if (outputcols_size_ == 0) {
    outputcols_ = input_->GetRender();
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode ReaderPostResolve::ResolveFilter(kwdbContext_p ctx,
                                                   Field **field,
                                                   bool is_tagFilter) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  is_filter_ = 1;
  code = super::ResolveFilter(ctx, field, is_tagFilter);
  is_filter_ = 0;

  Return(code);
}

EEIteratorErrCode ReaderPostResolve::ResolveRender(kwdbContext_p ctx,
                                                   Field ***render,
                                                   k_uint32 num) {
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

  if (outputcols_size_ > 0) {
    outputcols_ =
        static_cast<Field **>(malloc(outputcols_size_ * sizeof(Field *)));
    if (nullptr == outputcols_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(outputcols_, 0, outputcols_size_ * sizeof(Field *));
  }

  for (k_int32 i = 0; i < outputcols_size_; ++i) {
    k_uint32 tab = post_->outputcols(i);
    if (0 == tab) {
      has_timestamp = 1;
    }
    Field *field = table_->GetFieldWithColNum(tab);
    if (nullptr == field) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "field is null");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    outputcols_[i] = field;
    if (renders_size_ == 0) {
      (*render)[i] = field;
    }
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
    Field *field = ResolveBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      (*render)[i] = field;
    }
  }

  Return(code);
}

void ReaderPostResolve::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcols_size_ > 0) {
    *num = outputcols_size_;
  } else {
    *num = 0;
  }
}

EEIteratorErrCode ReaderPostResolve::ResolveReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
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
        *field = outputcols_[i - 1];
      } else {
        (*field)->next_ = outputcols_[i - 1];
      }
    }
  }

  Return(code);
}

EEIteratorErrCode PostResolve::ResolveScanTags(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_uint32 size = post_->outputcols_size();
  for (k_uint32 i = 0; i < size; i++) {
    k_uint32 col_id = post_->outputcols(i);
    // skip the col_id if it exceeds table_->field_num_ which should be relational col for multiple model processing
    if (col_id < table_->field_num_) {
      table_->scan_tags_.push_back(col_id - table_->min_tag_id_);
      Field* field = table_->GetFieldWithColNum(col_id);
      field->setColIdxInRs(i);
    }
  }

  Return(code);
}

EEIteratorErrCode PostResolve::ResolveScanCols(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // resolve col
  table_->scan_cols_.reserve(table_->field_num_);
  if (outputcols_size_ == 0) {
    table_->scan_cols_.push_back(table_->fields_[0]->get_num());
  } else {
    for (k_uint32 i = 0; i < outputcols_size_; ++i) {
      if (outputcols_[i]->get_num() < table_->min_tag_id_)
        table_->scan_cols_.push_back(outputcols_[i]->get_num());
    }
  }

  for (int i = 0; i < table_->scan_cols_.size(); i++) {
    k_uint32 col_id = table_->scan_cols_[i];
    Field* field = table_->GetFieldWithColNum(col_id);
    field->setColIdxInRs(i);
  }

  Return(code);
}

EEIteratorErrCode PostResolve::ResolveScanRelCols(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // resolve relational cols
  table_->scan_rel_cols_.reserve(table_->GetRelFields().size());
  k_uint32 size = post_->outputcols_size();
  for (k_uint32 i = 0; i < size; ++i) {
    k_uint32 col_id = post_->outputcols(i);
    if (col_id >= table_->field_num_) {
      table_->scan_rel_cols_.push_back(col_id - table_->field_num_);
      Field* field = table_->GetFieldWithColNum(col_id);
      field->setColIdxInRs(i);
      field->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
  }
  Return(code);
}

Field *PostResolve::ResolveFuncOperator(kwdbContext_p ctx, KString &func_name,
                                        std::list<Field *> args,
                                        k_bool is_negative) {
  EnterFunc();
  Field *field = nullptr;
  // std::list<Field *> fields;
  // Field *cur = left;
  //
  // while (nullptr != cur) {
  //   fields.push_back(cur);
  //   cur = cur->next_;
  // }
  // size_t num = fields.size();
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

inline k_uint32 extractNumInBrackets(const std::string &input) {
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

Field *PostResolve::ResolveCast(kwdbContext_p ctx, Field *left,
                                KString &output_type) {
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
  } else if (output_type.find("TIMESTAMP") != std::string::npos ||
             output_type == "DATE") {
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
}  // namespace kwdbts
