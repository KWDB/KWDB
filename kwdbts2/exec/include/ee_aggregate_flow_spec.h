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
#pragma once
#include <memory>
#include <string>
#include <vector>
#include "cm_assert.h"
#include "ee_flow_param.h"
#include "ee_common.h"
#include "ee_table.h"
#include "lg_api.h"

namespace kwdbts {
// err dispose
#define IsAggColNull(field)                               \
  if (nullptr == field) {                                 \
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,  \
                                  "Insufficient memory"); \
    LOG_ERROR("new FieldSum failed\n");                   \
    Return(EEIteratorErrCode::EE_ERROR);                  \
  }
// agg spec
template <class T>
class AggregatorSpecParam : public BasePostResolve {
 public:
  AggregatorSpecParam(BaseOperator *input, T *spec, TSPostProcessSpec *post,
                      TABLE *table, BaseOperator *agg_op = nullptr)
      : BasePostResolve(input, post, table), spec_(spec), agg_op_(agg_op) {
    group_size_ = spec_->group_cols_size();
  }

  ~AggregatorSpecParam();
  // resolve render size
  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;
  EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;
  EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                 k_uint32 num) override;
  EEIteratorErrCode ResolveGroupByCol(kwdbContext_p ctx, Field **renders,
                                      k_uint32 num);
  EEIteratorErrCode MakeGroupCache(kwdbContext_p ctx);
  EEIteratorErrCode SetRenderTemplateNum(kwdbContext_p ctx, Field **render,
                                         k_uint32 num);

 protected:
  EEIteratorErrCode ResolveAggCol(kwdbContext_p ctx, Field **render,
                                  k_bool is_render);
  EEIteratorErrCode MallocArray(kwdbContext_p ctx);

 public:
  T *spec_{nullptr};
  Field **group_{nullptr};
  Field **template_group_{nullptr};
  Field **aggs_{nullptr};
  CacheField **group_cache_{nullptr};
  k_uint32 group_size_{0};
  k_uint32 aggs_size_{0};  // it contains the func count of Sumfunctype::ANY_NOT_NULL
  BaseOperator *agg_op_{nullptr};   // point agg operator
};

template <class T>
AggregatorSpecParam<T>::~AggregatorSpecParam() {
  if (aggs_) {
    for (k_uint32 i = 0; i < aggs_size_; ++i) {
      SafeDeletePointer(aggs_[i]);
    }

    SafeFreePointer(aggs_);
  }


  if (group_cache_) {
    for (k_uint32 i = 0; i < group_size_; ++i) {
      SafeDeletePointer(group_cache_[i]);
    }

    SafeFreePointer(group_cache_);
  }

  SafeFreePointer(template_group_);
  SafeFreePointer(group_);

  aggs_ = nullptr;
  group_ = nullptr;
  group_cache_ = nullptr;
  aggs_size_ = 0;
  group_size_ = 0;
}

template <class T>
void AggregatorSpecParam<T>::RenderSize(kwdbContext_p ctx,
                                                     k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else {
    *num = spec_->aggregations_size();
  }
}

template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::HandleRender(kwdbContext_p ctx,
                                                       Field **render,
                                                       k_uint32 num) {
  EnterFunc();

  // resolve agg func
  EEIteratorErrCode code =
      ResolveAggCol(ctx, render, renders_size_ == 0 ? 1 : 0);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }

  for (k_uint32 i = 0; i < renders_size_; ++i) {
    std::string str = post_->renders(i);
    // produce Binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, str, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve Binary tree
    Field *field = ResolveBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      render[i] = field;
    }
  }

  Return(code);
}
// resolve Reference field
template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::ResolveReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
    Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    if (nullptr == *field) {
      *field = aggs_[i - 1];
    } else {
      (*field)->next_ = aggs_[i - 1];
    }
  }

  Return(code);
}

// resolve GroupBy field
template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::ResolveGroupByCol(kwdbContext_p ctx,
                                                            Field **renders,
                                                            k_uint32 num) {
  EnterFunc();
  if (0 == group_size_) {
    Return(EEIteratorErrCode::EE_OK);
  }
  group_ = static_cast<Field **>(malloc(group_size_ * sizeof(Field *)));
  if (nullptr == group_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("group by malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(group_, 0, group_size_ * sizeof(Field *));

  template_group_ = static_cast<Field **>(malloc(group_size_ * sizeof(Field *)));
  if (nullptr == template_group_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("template group by malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(template_group_, 0, group_size_ * sizeof(Field *));

  std::vector<Field *> &output_fields = input_->OutputFields();
  for (k_int32 i = 0; i < group_size_; ++i) {
    k_uint32 groupcol = spec_->group_cols(i);
    group_[i] = output_fields[groupcol];
    template_group_[i] = aggs_[group_[i]->group_by_copy_in_aggs_index_];
    group_[i]->group_by_copy_in_aggs_index_ = -1;
  }

  Return(EEIteratorErrCode::EE_OK);
}

// group field cache
template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::MakeGroupCache(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  CacheField *cache = nullptr;
  if (0 == group_size_) {
    Return(EEIteratorErrCode::EE_OK);
  }
  group_cache_ =
      static_cast<CacheField **>(malloc(group_size_ * sizeof(CacheField *)));
  if (nullptr == group_cache_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("aggregator malloc group by cache failed");
    Return(code);
  }
  memset(group_cache_, 0, group_size_ * sizeof(CacheField *));

  for (k_uint32 i = 0; i < group_size_; ++i) {
    Field *field = group_[i];
    switch (field->get_sql_type()) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BOOL:
      case roachpb::DataType::BIGINT: {
        cache = KNEW CacheFieldInt(field);
        break;
      }
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE: {
        cache = KNEW CacheFieldReal(field);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARBINARY: {
        cache = KNEW CacheFieldStr(field);
        break;
      }
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Undecided datatype");
        LOG_ERROR(
            "AggregatorSpecParam::MakeGroupCache() -- unknow datatype "
            "%d\n",
            field->get_sql_type());
        break;
    }

    if (nullptr != cache) {
      cache->SetTemplateField(template_group_[i]);
      group_cache_[i] = cache;
      code = EEIteratorErrCode::EE_OK;
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR(
          "AggregatorSpecParam::MakeGroupCache() -- new CacheField() "
          "failed\n");
      break;
    }
  }

  Return(code);
}

template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::SetRenderTemplateNum(
    kwdbContext_p ctx, Field **render, k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  for (k_uint32 i = 0; i < num; ++i) {
    render[i]->set_num(i);
  }

  Return(code);
}

// make agg field
template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::ResolveAggCol(kwdbContext_p ctx,
                                                        Field **render,
                                                        k_bool is_render) {
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

// malloc agg field
template <class T>
EEIteratorErrCode AggregatorSpecParam<T>::MallocArray(kwdbContext_p ctx) {
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
