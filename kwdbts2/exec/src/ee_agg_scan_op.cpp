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

#include "ee_agg_scan_op.h"

#include <memory>
#include <string>
#include <vector>

#include "ee_row_batch.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_common.h"

namespace kwdbts {

EEIteratorErrCode AggTableScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret;
  do {
    if (!is_clone_) {
      if (parent_operators_.size() > 0 && OperatorType::OPERATOR_POST_AGG_SCAN == parent_operators_[0]->Type()) {
        has_post_agg_ = true;
      }
    }

    ignore_outputtypes_ = true;
    ret = TableScanOperator::Init(ctx);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("scan op Init() failed\n")
      break;
    }

    for (k_int32 i = 0; i < spec_->aggregator().aggregations_size(); ++i) {
      aggregations_.push_back(spec_->aggregator().aggregations(i));
    }

    group_cols_size_ = spec_->aggregator().group_cols_size();
    group_cols_ = static_cast<k_uint32 *>(malloc(group_cols_size_ * sizeof(k_uint32)));
    if (!group_cols_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }
    for (k_int32 i = 0; i < group_cols_size_; ++i) {
      k_uint32 group_col = spec_->aggregator().group_cols(i);
      group_cols_[i] = group_col;
    }
    agg_source_target_col_map_ = static_cast<k_uint32 *>(malloc(num_ * sizeof(k_uint32)));
    if (!agg_source_target_col_map_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }
    // extract from agg spec
    agg_param_ = new TsAggregateParser(const_cast<TSAggregatorSpec*>(&aggregation_spec_),
                                                            const_cast<PostProcessSpec*>(&aggregation_post_),
                                                            table_, this);
    if (nullptr == agg_param_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    agg_param_->AddInput(this);

    if (aggregation_spec_.group_cols_size() <= 1) {
      disorder_ = true;
    }
    // get the size of renders
    agg_param_->RenderSize(ctx, &agg_num_);

    // resolve renders
    ret = agg_param_->ParserRender(ctx, &agg_renders_, agg_num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveRender() failed\n")
      break;
    }

    // resolve output Field
    ret = agg_param_->ParserOutputFields(ctx, agg_renders_, agg_num_, agg_output_fields_, false);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputFields() failed\n")
      break;
    }

    extractTimeBucket(renders_, num_);
    if (interval_seconds_ == 0) {
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    ResolveAggFuncs(ctx);

    // construct the output column information for agg functions.
    agg_output_col_num_ = agg_param_->aggs_size_;
    agg_output_col_info_ = KNEW ColumnInfo[agg_output_col_num_];
    if (agg_output_col_info_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    for (k_uint32 i = 0; i < agg_param_->aggs_size_; ++i) {
      if (aggregations_[i].func() ==
          TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
        agg_output_col_info_[i] = ColumnInfo(
            agg_param_->aggs_[i]->get_storage_length() + sizeof(k_int64),
            agg_param_->aggs_[i]->get_storage_type(),
            agg_param_->aggs_[i]->get_return_type());
        agg_output_col_info_[i].sql_type = agg_param_->aggs_[i]->get_sql_type();
        is_resolve_datachunk_ = true;
      } else {
        agg_output_col_info_[i] =
            ColumnInfo(agg_param_->aggs_[i]->get_storage_length(),
                       agg_param_->aggs_[i]->get_storage_type(),
                       agg_param_->aggs_[i]->get_return_type());
        agg_output_col_info_[i].sql_type = agg_param_->aggs_[i]->get_sql_type();
      }
    }

    constructAggResults();
    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }

    if (group_by_metadata_.initialize() != true) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }

    for (int i = 0; i < num_; ++i) {
      data_types_.push_back(renders_[i]->get_storage_type());
    }
    if (!has_post_agg_) {
      std::swap(output_fields_, agg_output_fields_);
    }
    if (aggregation_post_.has_limit()) {
      agg_limit_ = aggregation_post_.limit();
    }
    SafeDeleteArray(output_col_info_);
    ret = InitOutputColInfo(output_fields_);
  } while (false);
  Return(ret)
}

EEIteratorErrCode AggTableScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;
  if (table_->hash_points_.empty() && thd->IsRemote()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  StorageHandler* handler = handler_;
  auto start = std::chrono::high_resolution_clock::now();
  TsScanStats ts_scan_stats;
  do {
    if (limit_ && examined_rows_ >= limit_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      is_done_ = true;
      break;
    }

    code = InitScanRowBatch(ctx, &row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // read data
    while (!is_done_) {
      row_batch_->ts_ = max_ts_;
      code = handler->TsNext(ctx, &ts_scan_stats);
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        break;
      }
      if (row_batch_->count_ < 1) continue;
      total_read_row_ += row_batch_->count_;

      if (nullptr == filter_) {
        examined_rows_ += row_batch_->count_;
        break;
      }

      // filter
      for (int i = 0; i < row_batch_->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            row_batch_->NextLine();
            continue;
          }
        }

        row_batch_->AddSelection();
        row_batch_->NextLine();
        ++examined_rows_;
      }

      if (!row_batch_->GetSelection()->empty()) {
        break;
      }
    }

    if (!is_done_) {
      // If the result set is unordered, it is necessary to perform secondary
      // HASH aggregation on the basis of AGG SCAN.
      if (disorder_ || (!table_->ordered_scan_ && handler->isDisorderedMetrics())) {
        disorder_ = true;
        if (nullptr != current_data_chunk_) {
          current_data_chunk_->setDisorder(true);
        }
      }
      // reset
      row_batch_->ResetLine();
      if (row_batch_->Count() > 0) {
        KStatus status = AddRowBatchData(ctx, row_batch_);

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
    if (is_resolve_datachunk_) {
      temporary_data_chunk_ = std::move(output_queue_.front());
      KStatus ret = getAggResult(ctx, chunk);
      if (KStatus::FAIL == ret) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        Return(EEIteratorErrCode::EE_ERROR);
      }
    } else {
      chunk = std::move(output_queue_.front());
    }
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, output_type_oid_, thd, chunk);
    output_queue_.pop();
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0, 0, &ts_scan_stats);
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK)
    } else {
      Return(code)
    }
  } else {
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0, 0, &ts_scan_stats);
    if (is_done_) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD)
    } else {
      Return(code)
    }
  }
}

KStatus AggTableScanOperator::AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch) {
  EnterFunc()
  if (row_batch == nullptr) {
    Return(KStatus::FAIL)
  }

  k_int32 count_of_current_chunk = 0;
  if (current_data_chunk_ != nullptr) {
    count_of_current_chunk = (k_int32) current_data_chunk_->Count();
  } else {
    Return(KStatus::FAIL);
  }
  k_int32 target_row = count_of_current_chunk - 1;
  k_uint32 row_batch_count = row_batch->Count();
  auto current_chunk = current_data_chunk_.get();
  std::vector<DataChunk*> chunks;
  chunks.push_back(current_chunk);

  GroupByColumnInfo group_by_cols[group_cols_size_] = {};

  if (group_by_metadata_.reset(row_batch_count) != true) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  k_uint32 new_group_count = 0;
  for (k_uint32 row = 0; row < row_batch_count; ++row) {
    // check all the group by column, and increase the target_row number if it finds a different group.
    KTimestampTz time_bucket = 0;
    bool is_new_group = ProcessGroupCols(target_row, row_batch, group_by_cols, time_bucket, current_chunk);

    // new group or end of rowbatch
    if (is_new_group) {
      if (agg_limit_ > 0) {
        if ((++new_group_count) > agg_limit_) {
          max_ts_ = time_bucket;
          row_batch->SetCount(row);
          break;
        }
      }
      group_by_metadata_.setNewGroup(row);

      if (current_chunk->isFull()) {
        output_queue_.push(std::move(current_data_chunk_));
        // initialize a new agg result buffer.
        constructAggResults();
        if (current_data_chunk_ == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          Return(KStatus::FAIL);
        }
        target_row = -1;
        current_chunk = current_data_chunk_.get();
        chunks.push_back(current_chunk);
      }

      ++target_row;
      current_chunk->AddCount();
      for (int j = 0; j < group_cols_size_; j++) {
        auto& col = group_by_cols[j];
        current_chunk->InsertData(target_row, col.col_index, col.data_ptr, col.len);
      }
    }

    row_batch->NextLine();
  }

  k_int32 start_line_in_begin_chunk = count_of_current_chunk - 1;
  // need reset to the first line of row_batch before processing agg column.
  for (auto& func : funcs_) {
    row_batch->ResetLine();
    func->addOrUpdate(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata_, renders_);
  }

  Return(KStatus::SUCCESS)
}

k_bool AggTableScanOperator::ProcessGroupCols(k_int32& target_row, RowBatch* row_batch,
                                              GroupByColumnInfo* group_by_cols,
                                              KTimestampTz& time_bucket, IChunk *chunk) {
  bool is_new_group = false;
  k_uint32 current_line = target_row <= 0 ? 0 : target_row;
  k_int32 col_index = -1;
  k_uint32 col = 0;
  for (k_int32 i = 0; i < group_cols_size_; i++) {
    col_index++;
    col =  group_cols_[i];
    k_uint32 target_col = agg_source_target_col_map_[col];

    // maybe first row in group
    bool is_dest_null = chunk->IsNull(current_line, target_col);
    auto target_ptr = chunk->GetData(current_line, target_col);

    auto* field = GetRender(col);
    auto* source_ptr = field->get_ptr(row_batch);

    // handle the null value in input data.
    if (field->CheckNull()) {
      continue;
    }

    switch (field->get_storage_type()) {
      case roachpb::DataType::BOOL: {
        processGroupByColumn<bool>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                    col_index);
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        if (col == col_idx_) {
          time_bucket = field->ValInt();
          source_ptr = reinterpret_cast<char*>(&time_bucket);
        }
        processGroupByColumn<k_int64>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                      col_index);
        break;
      }
      case roachpb::DataType::INT: {
        processGroupByColumn<k_int32>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                      col_index);
        break;
      }
      case roachpb::DataType::SMALLINT: {
        processGroupByColumn<k_int16>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                      col_index);
        break;
      }
      case roachpb::DataType::FLOAT: {
        processGroupByColumn<k_float32>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                        col_index);
        break;
      }
      case roachpb::DataType::DOUBLE: {
        processGroupByColumn<k_float64>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group,
                                        col_index);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::VARBINARY: {
        processGroupByColumn<std::string>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols,
                                          is_new_group, col_index);
        break;
      }
      default: {
        break;
      }
    }
  }

  return is_new_group;
}

KStatus AggTableScanOperator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc()
  KStatus status = KStatus::SUCCESS;

  // dispose agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    k_int32 func_type = agg.func();
    k_uint32 argIdx;
    if (agg.col_idx_size() > 0) {
      argIdx = agg.col_idx(0);
    }

    AggregateFunc* agg_func = nullptr;
    k_bool aggfunc_new_flag = true;

    switch (func_type) {
      case Sumfunctype::MAX: {
        k_uint32 len = output_fields_[argIdx]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = KNEW MaxAggregate<k_bool>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW MaxAggregate<k_int16>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW MaxAggregate<k_int32>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = KNEW MaxAggregate<k_int64>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW MaxAggregate<k_float32>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = KNEW MaxAggregate<k_double64>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = KNEW MaxAggregate<std::string>(i, argIdx, len + STRING_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for max aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::MIN: {
        k_uint32 len = output_fields_[argIdx]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = KNEW MinAggregate<k_bool>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW MinAggregate<k_int16>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW MinAggregate<k_int32>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = KNEW MinAggregate<k_int64>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW MinAggregate<k_float32>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func =
                KNEW MinAggregate<k_double64>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = KNEW MinAggregate<std::string>(
                i, argIdx, len + STRING_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for min aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for min aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        agg_source_target_col_map_[argIdx] = i;
        k_uint32 len = output_fields_[argIdx]->get_storage_length();

        // skip to construct the ANY_NOT_NULL func for time_bucket column and group by columns.
        if (col_idx_ == argIdx) {
          aggfunc_new_flag = false;
          break;
        }

        bool ignore = 0;
        for (k_uint32 i = 0; i < group_cols_size_; ++i) {
          if (group_cols_[i] == argIdx) {
            ignore = 1;
            break;
          }
        }

        if (ignore) {
          aggfunc_new_flag = false;
          break;
        }

        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = KNEW AnyNotNullAggregate<k_bool>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW AnyNotNullAggregate<k_int16>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW AnyNotNullAggregate<k_int32>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = KNEW AnyNotNullAggregate<k_int64>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW AnyNotNullAggregate<k_float32>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = KNEW AnyNotNullAggregate<k_double64>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = KNEW AnyNotNullAggregate<std::string>(i, argIdx, len + STRING_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for any_not_null aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for not_null aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::SUM: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW SumAggregate<k_int16, k_decimal>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW SumAggregate<k_int32, k_decimal>(i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = KNEW SumAggregate<k_int64, k_decimal>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW SumAggregate<k_float32, k_double64>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = KNEW SumAggregate<k_double64, k_double64>(i, argIdx, len);
            break;
          default:
            LOG_ERROR("unsupported data type for sum aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for sum aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::COUNT: {
        k_uint32 len = sizeof(k_int64);
        agg_func = KNEW CountAggregate(i, argIdx, len);
        break;
      }
      case Sumfunctype::COUNT_ROWS: {
        k_uint32 len = sizeof(k_int64);
        agg_func = KNEW CountRowAggregate(i, len);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);
        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        k_int64 time = agg.timestampconstant(0);
        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          auto last_func = KNEW LastAggregate<true>(i, argIdx, tsIdx, time, len + STRING_WIDE);
          agg_func = std::move(last_func);
        } else {
          agg_func = KNEW LastAggregate<false>(i, argIdx, tsIdx, time, len);
        }
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);
        k_int64 time = agg.timestampconstant(0);
        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);
        agg_func = KNEW LastTSAggregate(i, argIdx, tsIdx, time, len);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = KNEW LastRowAggregate<true>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = KNEW LastRowAggregate<false>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);

        agg_func = KNEW LastRowTSAggregate(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = KNEW FirstAggregate<true>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = KNEW FirstAggregate<false>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);
        agg_func = KNEW FirstTSAggregate(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = KNEW FirstRowAggregate<true>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = KNEW FirstRowAggregate<false>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 len = sizeof(k_uint64);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);

        agg_func = KNEW FirstRowTSAggregate(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::STDDEV: {
        status = KStatus::FAIL;
        break;
      }
      case Sumfunctype::AVG: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();;
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW AVGRowAggregate<k_int16>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW AVGRowAggregate<k_int32>(i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = KNEW AVGRowAggregate<k_int64>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW AVGRowAggregate<k_float32>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = KNEW AVGRowAggregate<k_double64>(i, argIdx, len);
            break;
          default:
            LOG_ERROR("unsupported data type for sum aggregation\n");
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for sum aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::MAX_EXTEND: {
        auto argIdx2 = agg.col_idx(1);
        k_uint32 len2 = output_fields_[argIdx2]->get_storage_length();
        auto storage_type2 = output_fields_[argIdx2]->get_storage_type();
        if (storage_type2 == roachpb::DataType::CHAR ||
            storage_type2 == roachpb::DataType::VARCHAR ||
            storage_type2 == roachpb::DataType::NCHAR ||
            storage_type2 == roachpb::DataType::NVARCHAR ||
            storage_type2 == roachpb::DataType::BINARY ||
            storage_type2 == roachpb::DataType::VARBINARY) {
          len2 += STRING_WIDE;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = KNEW MaxExtendAggregate<k_bool>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW MaxExtendAggregate<k_int16>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW MaxExtendAggregate<k_int32>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = KNEW MaxExtendAggregate<k_int64>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW MaxExtendAggregate<k_float32>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = KNEW MaxExtendAggregate<k_double64>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = KNEW MaxExtendAggregate<std::string>(i, argIdx, len2, argIdx2);
            break;
          default:
            LOG_ERROR("unsupported data type for max aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::MIN_EXTEND: {
        auto argIdx2 = agg.col_idx(1);
        k_uint32 len2 = output_fields_[argIdx2]->get_storage_length();
        auto storage_type2 = output_fields_[argIdx2]->get_storage_type();
        if (storage_type2 == roachpb::DataType::CHAR ||
            storage_type2 == roachpb::DataType::VARCHAR ||
            storage_type2 == roachpb::DataType::NCHAR ||
            storage_type2 == roachpb::DataType::NVARCHAR ||
            storage_type2 == roachpb::DataType::BINARY ||
            storage_type2 == roachpb::DataType::VARBINARY) {
          len2 += STRING_WIDE;
        } else if (storage_type2 == roachpb::DataType::DECIMAL) {
          len2 += BOOL_WIDE;
        }
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = KNEW MinExtendAggregate<k_bool>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = KNEW MinExtendAggregate<k_int16>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::INT:
            agg_func = KNEW MinExtendAggregate<k_int32>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = KNEW MinExtendAggregate<k_int64>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = KNEW MinExtendAggregate<k_float32>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func =
                KNEW MinExtendAggregate<k_double64>(i, argIdx, len2, argIdx2);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = KNEW MinExtendAggregate<std::string>(
                i, argIdx, len2, argIdx2);
            break;
          default:
            LOG_ERROR("unsupported data type for min aggregation\n")
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for min aggregation");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      default:
        LOG_ERROR("unknown aggregation function type %d\n", func_type)
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_FUNCTION_DEFINITION, "unknown aggregation function type");
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      if (agg.distinct()) {
        // the distinct operator is not supported by Agg Scan OP, report an error here.
        // should use the original HASH Agg instead.
        Return(FAIL)
      }
      funcs_.push_back(agg_func);
    } else if ((status != KStatus::FAIL) && (aggfunc_new_flag != false)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    }
  }
  Return(status)
}

KStatus AggTableScanOperator::getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk) {
  if (chunk == nullptr) {
    chunk = std::make_unique<DataChunk>(output_col_info_, output_col_num_);
    if (chunk->Initialize() != true) {
      chunk = nullptr;
      return KStatus::FAIL;
    }
    chunk->SetAllNull();
  }

  k_int32 count = temporary_data_chunk_->Count();
  temporary_data_chunk_->ResetLine();
  temporary_data_chunk_->NextLine();
  for (k_int32 i = 0; i < count; ++i) {
    FieldsToChunk(agg_renders_, agg_num_, i, chunk);
    temporary_data_chunk_->NextLine();
    chunk->AddCount();
  }
  return KStatus::SUCCESS;
}

BaseOperator* AggTableScanOperator::Clone() {
  BaseOperator* iter = NewIterator<AggTableScanOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(childrens_[0]);
  }
  return iter;
}

}  // namespace kwdbts
