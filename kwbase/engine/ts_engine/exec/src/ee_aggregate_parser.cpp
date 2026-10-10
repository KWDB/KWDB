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
#include "ee_common.h"
#include "ee_field.h"
#include "ee_field_agg.h"
#include "ee_table.h"
#include "cm_func.h"

namespace kwdbts {

// err dispose
#define IsAggColNull(field)                               \
  if (nullptr == field) {                                 \
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,  \
                                  "Insufficient memory"); \
    LOG_ERROR("new FieldSum failed\n");                   \
    Return(EEIteratorErrCode::EE_ERROR);                  \
  }

TsAggregateParser::TsAggregateParser(TSAggregatorSpec *spec, PostProcessSpec *post,
                                                          TABLE *table, BaseOperator *agg_op)
  : TsOperatorParser(post, table), spec_(spec), agg_op_(agg_op) { }

TsAggregateParser::~TsAggregateParser() {
  if (aggs_) {
    for (k_uint32 i = 0; i < aggs_size_; ++i) {
      SafeDeletePointer(aggs_[i]);
    }

    SafeFreePointer(aggs_);
  }
  SafeFreePointer(outputs_);

  aggs_size_ = 0;
}

void TsAggregateParser::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcol_count_ > 0) {
    *num = outputcol_count_;
  } else {
    *num = spec_->aggregations_size();
  }
}

EEIteratorErrCode TsAggregateParser::HandleRender(kwdbContext_p ctx, Field **render, k_uint32 num) {
  EnterFunc();

  // resolve agg func
  bool is_renders = (0 == renders_size_ && 0 == outputcol_count_) ? true : false;
  EEIteratorErrCode code = ResolveAggCol(ctx, render, is_renders);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }
  if (outputcol_count_ > 0) {
    outputs_ = static_cast<Field**>(malloc(outputcol_count_ * sizeof(Field*)));
    if (nullptr == outputs_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("aggs_ malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(outputs_, 0, outputcol_count_ * sizeof(Field*));
    for (k_uint32 i = 0; i < outputcol_count_; ++i) {
      k_uint32 index = post_->output_columns(i);
      outputs_[i] = aggs_[index];
    }
  }
  if (renders_size_ < 1) {
    for (k_uint32 i = 0; i < outputcol_count_; ++i) {
      render[i] = outputs_[i];
    }
    Return(code);
  }
  for (k_uint32 i = 0; i < renders_size_; ++i) {
    Expression render_expr = post_->render_exprs(i);
    // produce Binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, render_expr.expr(), &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    // resolve Binary tree
    Field *field = ParserBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      render[i] = field;
    }
  }

  Return(code);
}

EEIteratorErrCode TsAggregateParser::ParserReference(kwdbContext_p ctx,
                        const std::shared_ptr<VirtualField> &virtualField, Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    if (outputcol_count_ > 0) {
      if (i > outputcol_count_) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid parameter value");
        LOG_ERROR("Invalid parameter value");
        Return(EEIteratorErrCode::EE_ERROR);
      }
      if (nullptr == *field) {
        *field = outputs_[i - 1];
      } else {
        (*field)->next_ = outputs_[i - 1];
      }
    } else {
      if (i > aggs_size_) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid parameter value");
        LOG_ERROR("Invalid parameter value");
        Return(EEIteratorErrCode::EE_ERROR);
      }
      if (nullptr == *field) {
        *field = aggs_[i - 1];
      } else {
        (*field)->next_ = aggs_[i - 1];
      }
    }
  }

  Return(code);
}

EEIteratorErrCode TsAggregateParser::ResolveAggCol(kwdbContext_p ctx, Field **render, k_bool is_render) {
  EnterFunc();
  FieldAggNum *func_field = nullptr;
  // alloc memory
  EEIteratorErrCode code = MallocArray(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }
  std::vector<Field *> &input_fields = input_->OutputFields();
  for (k_int32 i = 0; i < aggs_size_; ++i) {
    func_field = nullptr;
    const auto &agg = spec_->aggregations(i);
    k_int32 num = agg.col_idx_size();
    k_int32 func_type = agg.func();
    bool is_distinct = agg.distinct();
    switch (func_type) {
      case Sumfunctype::COUNT: {
        func_field = new FieldAggLonglong(i, roachpb::DataType::BIGINT, sizeof(k_int64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::COUNT_ROWS: {
        func_field = new FieldAggLonglong(i, roachpb::DataType::BIGINT, sizeof(k_int64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::MAX:
      case Sumfunctype::MIN: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::MAX_EXTEND:
      case Sumfunctype::MIN_EXTEND: {
        k_uint32 col = agg.col_idx(1);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::SUM: {
        k_uint32 col = agg.col_idx(0);
        if (input_fields[col]->get_storage_type() == roachpb::DataType::FLOAT ||
            input_fields[col]->get_storage_type() == roachpb::DataType::DOUBLE) {
          func_field = new FieldAggDouble(i, roachpb::DataType::DOUBLE, sizeof(k_double64), agg_op_);
        } else {
          func_field = new FieldAggDecimal(i, roachpb::DataType::DECIMAL, sizeof(k_double64), agg_op_);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::SUM_INT: {
        func_field = new FieldAggLonglong(i, roachpb::DataType::BIGINT, sizeof(k_int64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 col = agg.col_idx(1);
        func_field = new FieldAggLonglong(i, input_fields[col]->get_storage_type(), sizeof(KTimestamp), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 col = agg.col_idx(1);
        func_field = new FieldAggLonglong(i, input_fields[col]->get_storage_type(), sizeof(KTimestamp), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 col = agg.col_idx(1);
        func_field = new FieldAggLonglong(i, input_fields[col]->get_storage_type(), sizeof(KTimestamp), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 col = agg.col_idx(1);
        func_field = new FieldAggLonglong(i, input_fields[col]->get_storage_type(), sizeof(KTimestamp), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::AVG: {
        // avg  returns the double type，sum return the double type ,and the
        // count return int64
        func_field = new FieldAggAvg(i, roachpb::DataType::DOUBLE,
                                     sizeof(k_double64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        k_uint32 col = agg.col_idx(0);
        KStatus ret = CreateAggField(i, input_fields[col], agg_op_, &func_field);
        if (ret != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::TWA: {
        k_uint32 col = agg.col_idx(0);
        func_field = new FieldAggDouble(i, roachpb::DataType::DOUBLE,
                                        sizeof(k_double64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      case Sumfunctype::ELAPSED: {
        k_uint32 col = agg.col_idx(0);
        func_field = new FieldAggDouble(i, roachpb::DataType::DOUBLE,
                                        sizeof(k_double64), agg_op_);
        IsAggColNull(func_field);
        break;
      }
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Undecided datatype");
        LOG_ERROR("unknow agg function num : %d\n", i);
        Return(EEIteratorErrCode::EE_ERROR);
    }

    if (nullptr != func_field) {
      aggs_[i] = func_field;
      if (is_render) {
        render[i] = func_field;
      }
    }
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TsAggregateParser::MallocArray(kwdbContext_p ctx) {
  EnterFunc();
  aggs_size_ = spec_->aggregations_size();

  aggs_ = static_cast<Field **>(malloc(aggs_size_ * sizeof(Field *)));
  if (nullptr == aggs_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("aggs_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(aggs_, 0, aggs_size_ * sizeof(Field *));
  Return(EEIteratorErrCode::EE_OK);
}

}  // namespace kwdbts
