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

#include "ee_post_agg_scan_op.h"
#include <variant>

#include "ee_pb_plan.pb.h"
#include "ee_common.h"
#include "ee_disk_data_container.h"
#include "ee_agg_scan_op.h"

namespace kwdbts {

PostAggScanOperator::PostAggScanOperator(TsFetcherCollection* collection,
                                         TSAggregatorSpec* spec,
                                         PostProcessSpec* post,
                                         TABLE* table, int32_t processor_id)
    : HashAggregateOperator(collection, spec, post, table, processor_id) {}

PostAggScanOperator::PostAggScanOperator(const PostAggScanOperator& other, int32_t processor_id)
    : HashAggregateOperator(other, processor_id) {}

EEIteratorErrCode PostAggScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = BaseAggregator::Init(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // construct the output column information for agg output.
  agg_output_col_num_ = output_fields_.size();
  agg_output_col_info_ = KNEW ColumnInfo[agg_output_col_num_];
  if (agg_output_col_info_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  for (k_uint32 i = 0; i < output_fields_.size(); ++i) {
    agg_output_col_info_[i] =
        ColumnInfo(output_fields_[i]->get_storage_length(),
                   output_fields_[i]->get_storage_type(),
                   output_fields_[i]->get_return_type());
    agg_output_col_info_[i].sql_type = output_fields_[i]->get_sql_type();
  }

  col_types_.clear();
  col_lens_.clear();
  col_types_.reserve(output_fields_.size());
  col_lens_.reserve(output_fields_.size());
  for (auto field : output_fields_) {
    col_types_.push_back(field->get_storage_type());
    col_lens_.push_back(field->get_storage_length());
  }

  std::vector<roachpb::DataType> group_types;
  std::vector<k_uint32> group_lens;
  std::vector<bool> group_allow_null;
  for (auto& col : group_cols_) {
    group_lens.push_back(output_fields_[col]->get_storage_length());
    group_types.push_back(output_fields_[col]->get_storage_type());
    group_allow_null.push_back(output_fields_[col]->is_allow_null());
  }
  ht_ = KNEW LinearProbingHashTable(group_types, group_lens, agg_row_size_, group_allow_null);
  if (ht_ == nullptr || ht_->Resize() < 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

KStatus PostAggScanOperator::accumulateRows(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

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

    // no data
    if (chunk->Count() == 0) {
      continue;
    }
    fetcher_.Update(chunk->Count(), 0, 0, 0, 0, 0);
    // the chunk->isScanAgg() is always true.
    pass_agg_ &= !chunk->isDisorder();
    agg_result_counter_ += chunk->Count();
    if (!pass_agg_) {
      while (!processed_chunks_.empty()) {
        auto& buf = processed_chunks_.front();
        accumulateBatch(ctx, buf.get());
        processed_chunks_.pop();
      }
      accumulateBatch(ctx, chunk.get());
    } else {
      processed_chunks_.push(std::move(chunk));
    }
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
  }


  // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
  // aggregated.

  /**
   * scalar group
   * select max(c1) from t1 => handler_->NewTagIterator
   */
  auto start = std::chrono::high_resolution_clock::now();
  if (agg_result_counter_ == 0) {
    if (ht_->Empty() && group_cols_.empty() &&
        group_type_ == TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR) {
      // return null
      k_uint64 loc = ht_->CreateNullGroups();
      auto agg_ptr = ht_->GetAggResult(loc);

      if (!ht_->IsUsed(loc)) {
        ht_->SetUsed(loc);
        InitFirstLastTimeStamp(agg_ptr);
      }

      // COUNT_ROW or COUNT，return 0
      for (int i = 0; i < aggregations_.size(); i++) {
        if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
            aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT) {
          // set not null
          AggregateFunc::SetNotNull(agg_ptr + agg_null_offset_, i);
        }
      }
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);

  Return(KStatus::SUCCESS);
}

KStatus PostAggScanOperator::getAggResults(kwdbContext_p ctx, DataChunkPtr& results) {
  EnterFunc();
  if (pass_agg_) {
    if (processed_chunks_.empty()) {
      is_done_ = true;
    } else {
      if (nullptr == having_filter_ && 0 == cur_offset_ && 0 == limit_) {
        results = std::move(processed_chunks_.front());
        processed_chunks_.pop();
        examined_rows_ += results->Count();
        Return(KStatus::SUCCESS);
      }

      results = std::move(processed_chunks_.front());
      processed_chunks_.pop();

      // handle offset
      while (cur_offset_ != 0) {
        k_uint64 count = results->Count();
        if (cur_offset_ - (total_read_row_ + count) >= 0) {
          // skip current data chunk
          total_read_row_ += count;
          cur_offset_ -= count;
          results = std::move(processed_chunks_.front());
          processed_chunks_.pop();
        } else {
          // copy data to a new data chunk, and then return it.
          auto data = constructAggResults(count - cur_offset_);
          if (data == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(KStatus::FAIL);
          }
          data->Append(results.get(), cur_offset_ - total_read_row_, count);
          results = std::move(data);
        }
      }

      // handle limit
      if (limit_ && examined_rows_ >= limit_) {
        is_done_ = true;
        Return(KStatus::SUCCESS);
      }

      k_uint64 count = results->Count();
      while (examined_rows_ >= limit_) {
        if (limit_ - (examined_rows_ + count) > 0) {
          // return current data chunk
          examined_rows_ += count;
          break;
        } else {
          // copy data to a new data chunk, and then return it.
          k_uint32 copy_row_number = limit_ - examined_rows_;

          auto data = constructAggResults(copy_row_number);
          if (data == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(KStatus::FAIL);
          }
          data->Append(results.get(), 0, copy_row_number - 1);
          results = std::move(data);
          examined_rows_ += copy_row_number;
          break;
        }
      }
      Return(KStatus::SUCCESS);
    }
  } else {
    k_uint32 BATCH_SIZE = results->Capacity();

    // assembling rows
    k_uint32 index = 0;
    while (total_read_row_ < ht_->Size()) {
      DatumRowPtr data = *iter_;
      for (int col = 0; col < output_fields_.size(); col++) {
        // dispose null
        char* bitmap = data + agg_row_size_ - (output_fields_.size() + 7) / 8;
        if (AggregateFunc::IsNull(bitmap, col)) {
          results->SetNull(index, col);
          continue;
        }

        k_uint32 offset = funcs_[col]->GetOffset();
        if (IsStringType(output_fields_[col]->get_storage_type())) {
          k_uint32 len = 0;
          std::memcpy(&len, data + offset, STRING_WIDE);
          results->InsertData(index, col, data + offset + STRING_WIDE, len);
        } else if (output_fields_[col]->get_storage_type() == roachpb::DataType::DECIMAL) {
          k_uint32 len = output_fields_[col]->get_storage_length();
          results->InsertData(index, col, data + offset, len + BOOL_WIDE);
        } else {
          k_uint32 len = output_fields_[col]->get_storage_length();
          results->InsertData(index, col, data + offset, len);
        }
      }
      results->AddCount();
      ++index;
      ++iter_;
      ++examined_rows_;
      ++total_read_row_;

      if (examined_rows_ % BATCH_SIZE == 0) {
        break;
      }
    }

    if (examined_rows_ == ht_->Size()) {
      is_done_ = true;
    }
  }
  Return(KStatus::SUCCESS);
}

BaseOperator* PostAggScanOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  // input_:TagScanOperator
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator* iter = NewIterator<PostAggScanOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }
  return iter;
}

KStatus PostAggScanOperator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;
  k_uint32 max_or_min_index = 0;
  // all agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    k_int32 func_type = agg.func();

    // POST AGG SCAN :input column is same with output column
    k_uint32 argIdx = i;
    k_uint32 len = output_fields_[argIdx]->get_storage_length();

    AggregateFunc *agg_func = nullptr;
    switch (func_type) {
      case Sumfunctype::MAX: {
        max_or_min_index = i;
        agg_func = AggregateFuncFactory::CreateMax(output_fields_[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::MIN: {
        max_or_min_index = i;
        agg_func = AggregateFuncFactory::CreateMin(output_fields_[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        // create the Assembling rows of columns list (POST AGG SCAN）
        agg_source_target_col_map_[agg.col_idx(0)] = i;
        agg_func = AggregateFuncFactory::CreateAnyNotNull(output_fields_[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::SUM: {
        agg_func = AggregateFuncFactory::CreateSum(output_fields_[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::COUNT:
      case Sumfunctype::COUNT_ROWS: {
        agg_func = AggregateFuncFactory::CreateSumInt(output_fields_[argIdx]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 tsIdx = argIdx + 1;
        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);
        k_int64 time = agg.timestampconstant(0);
        agg_func = AggregateFuncFactory::CreateLast(output_fields_[i]->get_storage_type(), i, argIdx, len, tsIdx, time);
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 tsIdx = argIdx;
        k_int64 time = agg.timestampconstant(0);
        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = AggregateFuncFactory::CreateLastTS(i, argIdx, len, tsIdx, time);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 tsIdx = argIdx + 1;
        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);
        agg_func = AggregateFuncFactory::CreateLastRow(output_fields_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 tsIdx = argIdx;
        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = AggregateFuncFactory::CreateLastRowTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 tsIdx = argIdx + 1;
        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);
        agg_func = AggregateFuncFactory::CreateFirst(output_fields_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 tsIdx = argIdx;
        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = AggregateFuncFactory::CreateFirstTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 tsIdx = argIdx + 1;
        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);
        agg_func = AggregateFuncFactory::CreateFirstRow(output_fields_[i]->get_storage_type(), i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 tsIdx = argIdx;
        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = AggregateFuncFactory::CreateFirstRowTS(i, argIdx, len, tsIdx);
        break;
      }
      case Sumfunctype::STDDEV: {
        len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateSTDDEVRow(output_fields_[i]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::AVG: {
        len = sizeof(k_int64);
        agg_func = AggregateFuncFactory::CreateAVGRow(output_fields_[i]->get_storage_type(), i, argIdx, len);
        break;
      }
      case Sumfunctype::MAX_EXTEND: {
        k_uint32 argIdx2 = argIdx;
        argIdx = max_or_min_index;
        k_uint32 len2 = output_fields_[argIdx2]->get_storage_length();
        auto storage_type2 = output_fields_[argIdx2]->get_storage_type();
        bool is_string = false;
        if (IsStringType(storage_type2)) {
          is_string = true;
          len2 += STRING_WIDE;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        agg_func = AggregateFuncFactory::CreateMaxExtend(output_fields_[argIdx]->get_storage_type(), i, argIdx, len2,
                                                         argIdx2, is_string);
        if (agg_func) {
          agg_func->SetRefOffset(func_offsets_[max_or_min_index]);
          argIdx = argIdx2;
        }
        break;
      }
      case Sumfunctype::MIN_EXTEND: {
        k_uint32 argIdx2 = argIdx;
        argIdx = max_or_min_index;
        k_uint32 len2 = output_fields_[argIdx2]->get_storage_length();
        auto storage_type2 = output_fields_[argIdx2]->get_storage_type();
        bool is_string = false;
        if (IsStringType(storage_type2)) {
          is_string = true;
          len2 += STRING_WIDE;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        agg_func = AggregateFuncFactory::CreateMinExtend(output_fields_[argIdx]->get_storage_type(), i, argIdx, len2,
                                                         argIdx2, is_string);
        if (agg_func) {
          agg_func->SetRefOffset(func_offsets_[max_or_min_index]);
          argIdx = argIdx2;
        }
        break;
      }
      default:
        LOG_ERROR("unknown aggregation function type %d\n", func_type);
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      agg_func->SetOffset(func_offsets_[argIdx]);
      funcs_.push_back(agg_func);
    }
  }

  Return(status);
}

void PostAggScanOperator::CalculateAggOffsets() {
  if (output_fields_.empty()) {
    return;
  }
  func_offsets_.resize(output_fields_.size());
  k_uint32 offset = 0;
  for (int i = 0; i < output_fields_.size(); i++) {
    func_offsets_[i] = offset;
    if (IsStringType(output_fields_[i]->get_storage_type())) {
      offset += STRING_WIDE;
    } else if (output_fields_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
      offset += BOOL_WIDE;
    }

    if (IsFirstLastAggFunc(aggregations_[i].func())) {
      offset += sizeof(KTimestamp);
    }
    offset += output_fields_[i]->get_storage_length();
  }
}

void PostAggScanOperator::AddDependency(BaseOperator *children) {
  HashAggregateOperator::AddDependency(children);
  AggTableScanOperator* input = dynamic_cast<AggTableScanOperator*>(children);
  if (input) {
    input->SetHasPostAgg(true);
  }
}

void PostAggScanOperator::ResolveGroupByCols(kwdbContext_p ctx) {
  k_uint32 group_size_ = spec_->group_cols_size();
  for (k_int32 i = 0; i < group_size_; ++i) {
    k_uint32 groupcol = spec_->group_cols(i);
    group_cols_.push_back(agg_source_target_col_map_[groupcol]);
  }
}

}  // namespace kwdbts
