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

#include <variant>
#include <chrono>
#include "ee_aggregate_op.h"
#include "ee_kwthd_context.h"
#include "cm_func.h"
#include "ee_pb_plan.pb.h"
#include "lg_api.h"
#include "ee_common.h"

namespace kwdbts {

BaseAggregator::BaseAggregator(TsFetcherCollection* collection, TSAggregatorSpec* spec,
                                  PostProcessSpec* post, TABLE* table, int32_t processor_id)
    : BaseOperator(collection, table, post, processor_id),
      spec_{spec},
      param_(spec, post, table, this),
      group_type_(spec->type()),
      offset_(post->offset()),
      limit_(post->limit()) {
  for (k_int32 i = 0; i < spec_->aggregations_size(); ++i) {
    aggregations_.push_back(spec_->aggregations(i));
  }
}

BaseAggregator::BaseAggregator(const BaseAggregator& other, int32_t processor_id)
    : BaseOperator(other),
      spec_(other.spec_),
      param_(other.spec_, other.post_, other.table_, this),
      group_type_(other.spec_->type()),
      offset_(other.offset_),
      limit_(other.limit_) {
  for (k_int32 i = 0; i < other.spec_->aggregations_size(); ++i) {
    aggregations_.push_back(spec_->aggregations(i));
  }
  is_clone_ = true;
}

BaseAggregator::~BaseAggregator() {
  for (auto func : funcs_) {
    SafeDeletePointer(func);
  }
}

KStatus BaseAggregator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;
  std::vector<Field*>& input_fields = childrens_[0]->OutputFields();

  k_uint32 max_or_min_index = 0;
  // all agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    AggregateFunc *agg_func = nullptr;

    k_int32 func_type = agg.func();
    switch (func_type) {
      case Sumfunctype::MAX: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = input_fields[argIdx]->get_storage_length();
        max_or_min_index = i;
        agg_func = AggregateFuncFactory::CreateMax(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::MIN: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = input_fields[argIdx]->get_storage_length();
        max_or_min_index = i;
        agg_func = AggregateFuncFactory::CreateMin(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = input_fields[argIdx]->get_storage_length();
        agg_func = AggregateFuncFactory::CreateAnyNotNull(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::SUM: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = param_.aggs_[i]->get_storage_length();
        agg_func = AggregateFuncFactory::CreateSum(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::SUM_INT: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateSumInt(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::COUNT: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateCount(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::COUNT_ROWS: {
        k_uint32 len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateCountRow(i, len);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);
        k_int64 time = agg.timestampconstant(0);
        k_uint32 len = fixLength(input_fields[argIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateLast(param_.aggs_[i]->get_storage_type(), i, argIdx, len, tsIdx, time);
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }
        k_int64 time = agg.timestampconstant(0);
        k_uint32 len = fixLength(input_fields[tsIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateLastTS(i, argIdx, len, tsIdx, time);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);
        k_uint32 len = fixLength(input_fields[argIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateLastRow(param_.aggs_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }
        k_uint32 len = fixLength(input_fields[tsIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateLastRowTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);
        k_uint32 len = fixLength(input_fields[argIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateFirst(param_.aggs_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }
        k_uint32 len = fixLength(input_fields[tsIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateFirstTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);
        k_uint32 len = fixLength(input_fields[argIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateFirstRow(param_.aggs_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }
        k_uint32 len = fixLength(input_fields[tsIdx]->get_storage_length());
        agg_func = AggregateFuncFactory::CreateFirstRowTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::STDDEV: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateSTDDEVRow(param_.aggs_[i]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::AVG: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = param_.aggs_[i]->get_storage_length();
        agg_func = AggregateFuncFactory::CreateAVGRow(input_fields[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::TWA: {
        k_int32 argIdx = INT32_MAX;
        double const_val = 0.0f;
        k_uint32 tsIdx = agg.col_idx(0);
        k_uint32 len = param_.aggs_[i]->get_storage_length();
        roachpb::DataType storage_type = roachpb::DataType::BIGINT;
        if (agg.col_idx_size() > 1) {
          argIdx = agg.col_idx(1);
          storage_type = input_fields[argIdx]->get_storage_type();
        } else if (agg.col_idx_size() > 0) {
          Expression a = agg.arguments(0);
          string s = *a.mutable_expr();
          if (s.find_first_of("(") == string::npos) {
            const_val = atof(s.substr(0, s.find_first_of(":::")).c_str());
          } else {
            size_t begin = s.find_first_of("(");
            size_t end = s.find_first_of(")");
            const_val = atof(s.substr(begin + 1, end - 1).c_str());
          }
          storage_type = roachpb::DataType::DOUBLE;
        }
        agg_func = AggregateFuncFactory::CreateTwa(storage_type, i, argIdx, len, tsIdx, const_val);
        break;
      }
      case Sumfunctype::ELAPSED: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 len = param_.aggs_[i]->get_storage_length();
        string time = "'00:00:00.001':::INTERVAL";
        if (agg.arguments_size() > 0) {
          Expression a = agg.arguments(0);
          time = *a.mutable_expr();
        }
        agg_func = AggregateFuncFactory::CreateElapsed(input_fields[argIdx]->get_storage_type(), i, argIdx, len, time);
        break;
      }
      case Sumfunctype::MAX_EXTEND: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 argIdx2 = agg.col_idx(1);
        k_uint32 len2 = input_fields[argIdx2]->get_storage_length();
        auto storage_type2 = input_fields[argIdx2]->get_storage_type();
        bool is_string = false;
        if (IsStringType(storage_type2)) {
          len2 += STRING_WIDE;
          is_string = true;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        agg_func = AggregateFuncFactory::CreateMaxExtend(input_fields[argIdx]->get_storage_type(), i, argIdx, len2,
                                                         argIdx2, is_string);
        if (agg_func) {
          agg_func->SetRefOffset(func_offsets_[max_or_min_index]);
        }
        break;
      }
      case Sumfunctype::MIN_EXTEND: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 argIdx2 = agg.col_idx(1);
        k_uint32 len2 = input_fields[argIdx2]->get_storage_length();
        auto storage_type2 = input_fields[argIdx2]->get_storage_type();
        bool is_string = false;
        if (IsStringType(storage_type2)) {
          len2 += STRING_WIDE;
          is_string = true;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        agg_func = AggregateFuncFactory::CreateMinExtend(input_fields[argIdx]->get_storage_type(), i, argIdx, len2,
                                                         argIdx2, is_string);
        if (agg_func) {
          agg_func->SetRefOffset(func_offsets_[max_or_min_index]);
        }
        break;
      }
      default:
        LOG_ERROR("unknown aggregation function type %d\n", func_type);
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_FUNCTION_DEFINITION, "unknown aggregation function type");
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      agg_func->SetOffset(func_offsets_[i]);
      param_.aggs_[i]->set_column_offset(func_offsets_[i]);
      funcs_.push_back(agg_func);
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    }
  }

  Return(status);
}

EEIteratorErrCode BaseAggregator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  do {
    param_.AddInput(childrens_[0]);
    // init subquery iterator
    code = childrens_[0]->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    // resolve renders num
    param_.RenderSize(ctx, &num_);

    // resolve render
    code = param_.ParserRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveRender() error\n");
      break;
    }

    // resolve having
    code = param_.ParserFilter(ctx, &having_filter_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("Resolve having clause error\n");
      break;
    }

    // dispose Output Fields
    code = param_.ParserOutputFields(ctx, renders_, num_, output_fields_, false);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // calculate the offset of the aggregation result in the bucket
    CalculateAggOffsets();

    // dispose Agg func
    KStatus ret = ResolveAggFuncs(ctx);
    if (ret != KStatus::SUCCESS) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }

    // dispose Group By
    ResolveGroupByCols(ctx);

    // calculate Agg wideth
    for (int i = 0; i < param_.aggs_size_; i++) {
      agg_row_size_ += param_.aggs_[i]->get_storage_length();
      if (IsStringType(param_.aggs_[i]->get_storage_type())) {
        agg_row_size_ += STRING_WIDE;
      } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
        agg_row_size_ += BOOL_WIDE;
      } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
        agg_row_size_ += sizeof(k_int64);
      } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED) {
        agg_row_size_ += sizeof(ElapsedInfo);
      } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA) {
        agg_row_size_ += sizeof(TwaInfo);
      }

      if (IsFirstLastAggFunc(aggregations_[i].func())) {
        agg_row_size_ += sizeof(KTimestamp);
      }
    }
    agg_null_offset_ = agg_row_size_;
    agg_row_size_ += (param_.aggs_size_ + 7) / 8;
    std::vector<Field*>& input_fields = childrens_[0]->OutputFields();
    for (auto field : input_fields) {
      col_types_.push_back(field->get_storage_type());
      col_lens_.push_back(field->get_storage_length());
      col_allow_null_.push_back(field->is_allow_null());
    }

    // init column info used by data chunk.
    code = InitOutputColInfo(output_fields_);
    if (code != EEIteratorErrCode::EE_OK) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
    if (funcs_.empty()) {
      SafeDeleteArray(output_col_info_);
      std::vector<Field*> empty_output_fields;
      for (k_int32 i = 0; i < param_.ParserInputRenderSize(); i++) {
        empty_output_fields.push_back(renders_[i]);
      }
      code = InitOutputColInfo(empty_output_fields);
      if (code != EEIteratorErrCode::EE_OK) {
        Return(EEIteratorErrCode::EE_ERROR);
      }
    }
    constructDataChunk();
    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR)
    }
  } while (0);

  Return(code);
}

void BaseAggregator::ResolveGroupByCols(kwdbContext_p ctx) {
  k_uint32 group_size_ = spec_->group_cols_size();
  for (k_int32 i = 0; i < group_size_; ++i) {
    k_uint32 groupcol = spec_->group_cols(i);
    group_cols_.push_back(groupcol);
  }
}

void BaseAggregator::CalculateAggOffsets() {
  if (param_.aggs_size_ < 1) {
    return;
  }

  func_offsets_.resize(param_.aggs_size_);
  k_uint32 offset = 0;
  for (int i = 0; i < param_.aggs_size_; i++) {
    func_offsets_[i] = offset;
    if (IsStringType(param_.aggs_[i]->get_storage_type())) {
      offset += STRING_WIDE;
    } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
      offset += BOOL_WIDE;
    } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
      offset += sizeof(k_int64);
    } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED) {
      offset += sizeof(ElapsedInfo);
    } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA) {
      offset += sizeof(TwaInfo);
    }

    if (IsFirstLastAggFunc(aggregations_[i].func())) {
      offset += sizeof(KTimestamp);
    }
    offset += param_.aggs_[i]->get_storage_length();
  }
}

void BaseAggregator::InitFirstLastTimeStamp(DatumRowPtr ptr) {
  for (int i = 0; i < aggregations_.size(); i++) {
    auto func_type = aggregations_[i].func();
    k_uint32 offset = funcs_[i]->GetOffset();
    k_uint32 len = funcs_[i]->GetLen();

    if (func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW ||
        func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS) {
      KTimestamp max_ts = INT64_MAX;
      std::memcpy(ptr + offset + len - sizeof(KTimestamp), &max_ts, sizeof(KTimestamp));
    } else if (func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW ||
               func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS) {
      KTimestamp min_ts = INT64_MIN;
      std::memcpy(ptr + offset + len - sizeof(KTimestamp), &min_ts, sizeof(KTimestamp));
    }
  }
}

EEIteratorErrCode BaseAggregator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  EEIteratorErrCode code = childrens_[0]->Close(ctx);

  Return(code);
}

EEIteratorErrCode BaseAggregator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  childrens_[0]->Reset(ctx);
  Return(EEIteratorErrCode::EE_OK);
}

KStatus BaseAggregator::accumulateRowIntoBucket(kwdbContext_p ctx, DatumRowPtr bucket, k_uint32 agg_null_offset,
                                                IChunk* chunk, k_uint32 line) {
  EnterFunc();
  for (int i = 0; i < funcs_.size(); i++) {
    // distinct
    const auto& agg = aggregations_[i];

    // Distinct Agg
    if (agg.distinct()) {
      // group cols + agg cols
      k_bool is_distinct;
      if (funcs_[i]->isDistinct(chunk, line, col_types_, col_lens_, group_cols_, &is_distinct, col_allow_null_) < 0) {
        Return(KStatus::FAIL);
      }
      if (is_distinct == false) {
        continue;
      }
    }

    // execute agg
    funcs_[i]->addOrUpdate(bucket, bucket + agg_null_offset, chunk, line);
  }

  Return(KStatus::SUCCESS);
}

HashAggregateOperator::~HashAggregateOperator() {
  //  delete input
  if (is_clone_) {
    delete childrens_[0];
  }

  SafeDeletePointer(ht_);
}

EEIteratorErrCode HashAggregateOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = BaseAggregator::Init(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  std::vector<roachpb::DataType> group_types;
  std::vector<k_uint32> group_lens;
  std::vector<bool> group_allow_null;
  std::vector<Field*>& input_fields = childrens_[0]->OutputFields();
  for (auto& col : group_cols_) {
    group_lens.push_back(input_fields[col]->get_storage_length());
    group_types.push_back(input_fields[col]->get_storage_type());
    group_allow_null.push_back(input_fields[col]->is_allow_null());
  }
  ht_ = KNEW LinearProbingHashTable(group_types, group_lens, agg_row_size_, group_allow_null);
  if (ht_ == nullptr || ht_->Resize() < 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}


EEIteratorErrCode HashAggregateOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  // set current offset
  cur_offset_ = offset_;

  code = childrens_[0]->Start(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // aggregation calculation for all data from sub operators
  KStatus ret = accumulateRows(ctx);
  if (ret != KStatus::SUCCESS) {
    code = EEIteratorErrCode::EE_ERROR;
  }

  iter_ = ht_->begin();

  Return(code);
}

EEIteratorErrCode HashAggregateOperator::Next(kwdbContext_p ctx,
                                              DataChunkPtr& chunk) {
  EnterFunc();
  if (is_done_) {
    fetcher_.Update(0, 0, 0, ht_->Capacity() * ht_->tupleSize(), 0, 0);
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  KWThdContext *thd = current_thd;
  auto start = std::chrono::high_resolution_clock::now();
  if (nullptr == chunk) {
    // init data chunk
    chunk = std::make_unique<DataChunk>(output_col_info_, output_col_num_);
    if (chunk->Initialize() != true) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      chunk = nullptr;
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  // write aggdata to result_set
  if (getAggResults(ctx, chunk) != KStatus::SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  OPERATOR_DIRECT_ENCODING(ctx,
                           output_encoding_,
                           use_query_short_circuit_,
                           use_query_compress_type_,
                           output_type_oid_,
                           thd,
                           chunk);
  auto end = std::chrono::high_resolution_clock::now();
  fetcher_.Update(0, (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, chunk->Count());

  if (0 == chunk->Count()) {
    chunk = nullptr;
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }

  Return(EEIteratorErrCode::EE_OK);
}

KStatus HashAggregateOperator::accumulateBatch(kwdbContext_p ctx,
                                               IChunk* chunk) {
  EnterFunc()
  if (chunk->Count() <= 0) {
    Return(KStatus::SUCCESS);
  }
  for (k_uint32 line = 0; line < chunk->Count(); ++line) {
    k_uint64 loc;
    int ret = ht_->FindOrCreateGroups(chunk, line, group_cols_, &loc);
    if (ret < 0) {
      Return(KStatus::FAIL);
    }
    auto agg_ptr = ht_->GetAggResult(loc);

    if (ret == 1) {
      ht_->SetUsed(loc);
      // copy group keys from data chunk to hash table
      ht_->CopyGroups(chunk, line, group_cols_, loc);

      InitFirstLastTimeStamp(agg_ptr);
    }
    for (int i = 0; i < funcs_.size(); i++) {
      if (aggregations_[i].distinct()) {
        k_bool is_distinct;
        if (funcs_[i]->isDistinct(chunk, line, col_types_, col_lens_, group_cols_, &is_distinct, col_allow_null_) <
            0) {
          Return(KStatus::FAIL);
        }
        if (is_distinct == false) {
          continue;
        }
      }
      funcs_[i]->addOrUpdate(agg_ptr, agg_ptr + agg_null_offset_, chunk, line);
    }
  }
  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::accumulateRow(kwdbContext_p ctx, DataChunkPtr& chunk, k_uint32 line) {
  EnterFunc();

  k_uint64 loc;
  if (ht_->FindOrCreateGroups(chunk.get(), line, group_cols_, &loc) < 0) {
    Return(KStatus::FAIL);
  }
  auto agg_ptr = ht_->GetAggResult(loc);
  if (!ht_->IsUsed(loc)) {
    ht_->SetUsed(loc);
    // copy group keys from data chunk to hash table
    ht_->CopyGroups(chunk.get(), line, group_cols_, loc);

    InitFirstLastTimeStamp(agg_ptr);
  }
  accumulateRowIntoBucket(ctx, agg_ptr, agg_null_offset_, chunk.get(), line);

  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::accumulateRows(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  int64_t duration = 0;
  for (;;) {
    DataChunkPtr chunk = nullptr;
    // read a batch of data
    code = childrens_[0]->Next(ctx, chunk);
    auto start = std::chrono::high_resolution_clock::now();
    if (code != EEIteratorErrCode::EE_OK) {
      if (code == EEIteratorErrCode::EE_END_OF_RECORD ||
          code == EEIteratorErrCode::EE_TIMESLICE_OUT) {
        code = EEIteratorErrCode::EE_OK;
        break;
      }
      // LOG_ERROR("Failed to fetch data from child operator, return code = %d.\n", code);
      Return(KStatus::FAIL);
    }
    // LOG_ERROR("begin to print HashAggregateOperator child chunk data :");
    // chunk->DebugPrintData();
    accumulateBatch(ctx, chunk.get());
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(chunk->Count(), (end - start).count(), 0, 0, 0, 0);
  }

  // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
  auto start = std::chrono::high_resolution_clock::now();
  if (ht_->Empty() && group_cols_.empty()) {
    // retrun NULL
    k_uint64 loc = ht_->CreateNullGroups();
    auto agg_ptr = ht_->GetAggResult(loc);
    if (!ht_->IsUsed(loc)) {
      ht_->SetUsed(loc);
      InitFirstLastTimeStamp(agg_ptr);
    }

    // return 0
    for (int i = 0; i < aggregations_.size(); i++) {
      if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT) {
        // set no null, default 0
        AggregateFunc::SetNotNull(agg_ptr + agg_null_offset_, i);
      }
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);

  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::getAggResults(kwdbContext_p ctx,
                                             DataChunkPtr& results) {
  EnterFunc();
  k_uint32 BATCH_SIZE = results->Capacity();

  // row indicates indicates the row position inserted into the current
  // DataChunk
  k_uint32 row = 0;
  while (total_read_row_ < ht_->Size()) {
    // filter
    if (nullptr != having_filter_) {
      k_int64 ret = having_filter_->ValInt();
      if (0 == ret) {
        ++iter_;
        ++total_read_row_;
        continue;
      }
    }

    // limit
    if (limit_ && examined_rows_ >= limit_) {
      is_done_ = true;
      break;
    }

    // offset
    if (cur_offset_ > 0) {
      --cur_offset_;
      ++iter_;
      ++total_read_row_;
      continue;
    }

    FieldsToChunk(GetRender(), GetRenderSize(), row, results);

    ++iter_;
    ++examined_rows_;
    ++total_read_row_;
    results->AddCount();
    ++row;

    if (examined_rows_ % BATCH_SIZE == 0) {
      // BATCH_SIZE
      row = 0;
      break;
    }
  }

  if (total_read_row_ == ht_->Size()) {
    is_done_ = true;
  }
  Return(KStatus::SUCCESS);
}

BaseOperator* HashAggregateOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator* iter = NewIterator<HashAggregateOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }
  return iter;
}


///////////////// OrderedAggregateOperator //////////////////////

OrderedAggregateOperator::OrderedAggregateOperator(TsFetcherCollection* collection,
                                                   TSAggregatorSpec* spec,
                                                   PostProcessSpec* post,
                                                   TABLE* table,
                                                   int32_t processor_id)
    : BaseAggregator(collection, spec, post, table, processor_id) {
  append_additional_timestamp_ = false;
}

OrderedAggregateOperator::OrderedAggregateOperator(const OrderedAggregateOperator& other, int32_t processor_id)
    : BaseAggregator(other, processor_id) {
  append_additional_timestamp_ = false;
}

OrderedAggregateOperator::~OrderedAggregateOperator() {
  SafeDeleteArray(agg_output_col_info_);
  //  delete input
  if (is_clone_) {
    delete childrens_[0];
  }
}

EEIteratorErrCode OrderedAggregateOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = BaseAggregator::Init(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // construct the output column information for agg functions.
  agg_output_col_num_ = param_.aggs_size_;
  agg_output_col_info_ = KNEW ColumnInfo[agg_output_col_num_];
  if (agg_output_col_info_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  for (int i = 0; i < agg_output_col_num_; i++) {
    if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
      agg_output_col_info_[i] =
          ColumnInfo(param_.aggs_[i]->get_storage_length() + sizeof(k_int64),
                     param_.aggs_[i]->get_storage_type(),
                     param_.aggs_[i]->get_return_type());
    } else {
      agg_output_col_info_[i] =
          ColumnInfo(param_.aggs_[i]->get_storage_length(),
                     param_.aggs_[i]->get_storage_type(),
                     param_.aggs_[i]->get_return_type());
    }
  }

  constructAggResults();
  if (agg_data_chunk_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (group_by_metadata_.initialize() != true) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode OrderedAggregateOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = childrens_[0]->Start(ctx);
  // set current offset
  cur_offset_ = offset_;
  Return(code);
}

EEIteratorErrCode OrderedAggregateOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc()
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  int64_t duration = 0;
  int64_t read_row_num = 0;
  std::chrono::_V2::system_clock::time_point start;
  do {
    DataChunkPtr input_chunk = nullptr;
    // read a batch of data from sub operator
    code = childrens_[0]->Next(ctx, input_chunk);
    start = std::chrono::high_resolution_clock::now();
    if (code != EEIteratorErrCode::EE_OK) {
      if (code == EEIteratorErrCode::EE_END_OF_RECORD ||
          code == EEIteratorErrCode::EE_TIMESLICE_OUT) {
        is_done_ = true;

        if (agg_data_chunk_ != nullptr) {
          KStatus status = FinishProcess(ctx);
          if (status != KStatus::SUCCESS) {
            Return(EEIteratorErrCode::EE_ERROR)
          }
          temporary_data_chunk_ = std::move(agg_data_chunk_);
          temporary_data_chunk_->ResetLine();
          temporary_data_chunk_->NextLine();
          if (current_data_chunk_ == nullptr) {
            constructDataChunk();
            if (current_data_chunk_ == nullptr) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
              Return(EEIteratorErrCode::EE_ERROR);
            }
          }
          for (int idx = 0; idx < temporary_data_chunk_->Count(); idx++) {
            if (current_data_chunk_->isFull()) {
              output_queue_.push(std::move(current_data_chunk_));
              // initialize a new agg result buffer.
              constructDataChunk();
              if (current_data_chunk_ == nullptr) {
                EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
                Return(EEIteratorErrCode::EE_ERROR);
              }
            }
            getAggResult(ctx, current_data_chunk_);
            temporary_data_chunk_->NextLine();
          }
          temporary_data_chunk_.reset();
        }
        if (current_data_chunk_ != nullptr && current_data_chunk_->Count() == 0) {
          handleEmptyResults(ctx);
        }

        if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
          output_queue_.push(std::move(current_data_chunk_));
        }

        code = EEIteratorErrCode::EE_OK;
        break;
      }
      LOG_ERROR("Failed to fetch data from child operator, return code = %d.\n", code);
      Return(EEIteratorErrCode::EE_ERROR);
    }
    read_row_num += input_chunk->Count();
    // no data,continue
    if (input_chunk->Count() == 0) {
      input_chunk = nullptr;
      continue;
    }

    if (!is_done_) {
      input_chunk->ResetLine();
      input_chunk->NextLine();
      if (input_chunk->Count() > 0) {
        KStatus status = ProcessData(ctx, input_chunk);

        if (status != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR)
        }
      }
    } else {
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        output_queue_.push(std::move(current_data_chunk_));
      }
    }
  } while (!is_done_ && output_queue_.empty());

  if (!output_queue_.empty()) {
    chunk = std::move(output_queue_.front());
    OPERATOR_DIRECT_ENCODING(ctx,
                             output_encoding_,
                             use_query_short_circuit_,
                             use_query_compress_type_,
                             output_type_oid_,
                             thd,
                             chunk);
    output_queue_.pop();
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(read_row_num, (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, chunk->Count());
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK)
    } else {
      Return(code)
    }
  } else {
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
    if (is_done_) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD)
    } else {
      Return(code)
    }
  }
}

KStatus OrderedAggregateOperator::FinishProcess(kwdbContext_p ctx) {
  k_int32 count_of_current_chunk = (k_int32)agg_data_chunk_->Count();
  std::vector<DataChunk*> chunks;
  chunks.push_back(agg_data_chunk_.get());
  k_int32 start_line_in_begin_chunk = group_cols_.empty() ? 0 : count_of_current_chunk - 1;
  // need reset to the first line of row_batch before processing agg columns.
  for (int i = 0; i < funcs_.size(); i++) {
    DistinctOpt opt{aggregations_[i].distinct(), col_types_, col_lens_, group_cols_, col_allow_null_};
    if (funcs_[i]->addOrUpdate(chunks, start_line_in_begin_chunk, nullptr, group_by_metadata_, opt) < 0) {
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus OrderedAggregateOperator::ProcessData(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc()
  if (chunk == nullptr) {
    Return(KStatus::FAIL)
  }
  DataChunk *chunk_ptr = chunk.get();
  std::queue<DataChunkPtr> agg_output_queue;
  k_int32 count_of_current_chunk = 0;

  if (agg_data_chunk_ != nullptr) {
    count_of_current_chunk = (k_int32)agg_data_chunk_->Count();
  } else {
    Return(KStatus::FAIL);
  }

  k_int32 target_row = count_of_current_chunk - 1;
  k_uint32 row_batch_count = chunk_ptr->Count();
  bool has_new_group = false;
  if (group_by_metadata_.reset(row_batch_count) != true) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  std::vector<DataChunk*> chunks;
  chunks.push_back(agg_data_chunk_.get());

  if (!group_cols_.empty()) {
    if (!last_group_key_.is_init_) {
      bool ret = last_group_key_.Init(chunk_ptr->GetColumnInfo(), group_cols_);
      if (!ret) {
        Return(KStatus::FAIL);
      }
    }
    for (k_uint32 row = 0; row < row_batch_count; ++row) {
      bool is_new_group = last_group_key_.IsNewGroup(chunk_ptr, row, group_cols_);

      // new group or end of rowbatch
      if (is_new_group) {
        has_agg_result = true;
        has_new_group = true;
        group_by_metadata_.setNewGroup(row);

        if (agg_data_chunk_->isFull()) {
          agg_output_queue.push(std::move(agg_data_chunk_));
          // initialize a new agg result buffer.
          constructAggResults();
          if (agg_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(KStatus::FAIL);
          }
          target_row = -1;
          chunks.push_back(agg_data_chunk_.get());
        }

        ++target_row;
        agg_data_chunk_->AddCount();

        AggregateFunc::ConstructGroupKeys(chunk_ptr, group_cols_, row, last_group_key_);
      }

      chunk_ptr->NextLine();
    }
  } else {
    // if the group by column(s) is empty, it needs at least one row to hold the response.
    if (agg_data_chunk_->Count() <= 0) {
      agg_data_chunk_->AddCount();
    }
  }

  k_int32 start_line_in_begin_chunk = group_cols_.empty() ? 0 : count_of_current_chunk - 1;
  // need reset to the first line of row_batch before processing agg columns.
  for (int i = 0; i < funcs_.size(); i++) {
    chunk_ptr->ResetLine();
    chunk_ptr->NextLine();
    DistinctOpt opt{aggregations_[i].distinct(), col_types_, col_lens_, group_cols_, col_allow_null_};
    if (funcs_[i]->addOrUpdate(chunks, start_line_in_begin_chunk, chunk_ptr, group_by_metadata_, opt) < 0) {
      Return(KStatus::FAIL);
    }
  }
  if (has_new_group) {
    last_group_key_.chunk_ = std::move(chunk);
  }
  while (!agg_output_queue.empty()) {
    temporary_data_chunk_ = std::move(agg_output_queue.front());
    agg_output_queue.pop();

    temporary_data_chunk_->ResetLine();
    temporary_data_chunk_->NextLine();
    for (int idx = 0; idx < temporary_data_chunk_->Count(); idx++) {
      if (current_data_chunk_->isFull()) {
        output_queue_.push(std::move(current_data_chunk_));
        // initialize a new agg result buffer.
        constructDataChunk();
        if (current_data_chunk_ == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          Return(KStatus::FAIL);
        }
      }
      getAggResult(ctx, current_data_chunk_);
      temporary_data_chunk_->NextLine();
    }
  }
  temporary_data_chunk_.reset();
  Return(KStatus::SUCCESS)
}

KStatus OrderedAggregateOperator::getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk) {
  // Having filter
  k_int64 keep = 1;
  if (nullptr != having_filter_) {
    keep = having_filter_->ValInt();
  }

  while (keep) {
    // limit
    if (limit_ && examined_rows_ >= limit_) {
      is_done_ = true;
      break;
    }

    if (cur_offset_ > 0) {
      --cur_offset_;
      break;
    }

    chunk->AddCount();
    k_int32 row = chunk->NextLine();
    if (row < 0) {
      return KStatus::FAIL;
    }
    // insert one row into data chunk
    FieldsToChunk(GetRender(), GetRenderSize(), row, chunk);
    ++examined_rows_;

    keep = 0;
  }

  return KStatus::SUCCESS;
}

BaseOperator* OrderedAggregateOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  if (input == nullptr) {
    return nullptr;
  }

  BaseOperator* iter = NewIterator<OrderedAggregateOperator>(*this, processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }
  return iter;
}

}  // namespace kwdbts
