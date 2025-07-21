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
#include "ee_window_op.h"

#include <cxxabi.h>

#include <vector>

#include "ee_base_op.h"
#include "ee_cancel_checker.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

WindowOperator::WindowOperator(TsFetcherCollection* collection,
                               BaseOperator* input, WindowerSpec* spec,
                               TSPostProcessSpec* post, TABLE* table,
                               int32_t processor_id)
    : BaseOperator(collection, table, processor_id),
      spec_{spec},
      post_{post},
      limit_{post->limit()},
      param_(input, spec, post, table),
      input_(input),
      input_fields_{input->OutputFields()} {
  k_uint32 size_ = spec_->partitionby_size();
  for (k_int32 i = 0; i < size_; ++i) {
    k_uint32 part_col = spec_->partitionby(i);
    partition_cols_.push_back(part_col);
  }
  // input_fields_.clear();
  // size_ = input->GetRenderSize();
  // for (k_int32 i = 0; i < size_; ++i) {
  //   input_fields_.push_back(input_->GetRender(i));
  // }
}

WindowOperator::WindowOperator(const WindowOperator& other, BaseOperator* input,
                               int32_t processor_id)
    : BaseOperator(other),
      spec_{other.spec_},
      post_{other.post_},
      limit_{other.limit_},
      param_(input, other.spec_, other.post_, other.table_),
      input_(input),
      input_fields_{input->OutputFields()} {
  k_uint32 size_ = spec_->partitionby_size();
  for (k_int32 i = 0; i < size_; ++i) {
    k_uint32 part_col = spec_->partitionby(i);
    partition_cols_.push_back(part_col);
  }
  is_clone_ = true;
}

WindowOperator::~WindowOperator() {
  if (is_clone_) {
    delete input_;
  }
  for (auto field : win_func_output_fields_) {
    SafeDeletePointer(field);
  }
  win_func_output_fields_.clear();

  for (auto field : win_func_fields_) {
    SafeDeletePointer(field);
  }
  win_func_fields_.clear();

  for (auto col : group_by_cols_) {
    if (nullptr != col.data_ptr) {
      free(col.data_ptr);
      col.data_ptr = nullptr;
    }
  }
  group_by_cols_.clear();
  SafeDeleteArray(output_filter_col_info_);
}

EEIteratorErrCode WindowOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  do {
    // init subquery iterator
    code = input_->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    // resolve renders num
    param_.RenderSize(ctx, &num_);
    // resolve render
    code = param_.ResolveRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveRender() error\n");
      break;
    }

    // output Field
    code = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // win func material
    param_.ResolveWinFuncFields(win_func_fields_);

    // temp output field
    code = ResolveWinTempFields(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveWinTempFields() failed\n");
      break;
    }
    // set trunk win filter field;
    param_.ResolveSetOutputFields(win_func_output_fields_);

    code = param_.ResolveFilter(ctx, &filter_, false);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("Resolve having clause error\n");
      break;
    }

    param_.ResolveFilterFields(filter_fields_);

    for (auto it : filter_fields_) {
      filter_fields_num_.push_back(it->get_num());
    }

    // reset num
    k_uint32 i = 0;
    k_uint32 size = output_fields_.size() + filter_fields_.size();
    for (auto it : win_func_output_fields_) {
      it->set_num(i + size);
      i++;
    }

    // init column info used by data chunk.
    code = InitOutputColInfo(output_fields_);
    if (code != EEIteratorErrCode::EE_OK) {
      Return(EEIteratorErrCode::EE_ERROR);
    }

    if (nullptr != filter_) {
      output_filter_col_info_ = output_col_info_;
      output_filter_col_num_ = output_col_num_;
      output_col_num_ = output_fields_.size() +
                               filter_fields_.size() +
                               win_func_fields_.size();
      output_col_info_ = KNEW ColumnInfo[output_col_num_];
      if (output_col_info_ == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                      "Insufficient memory");
        Return(EEIteratorErrCode::EE_ERROR);
      }
      k_int32 index = 0;
      for (index = 0; index < output_filter_col_num_; index++) {
        output_col_info_[index] = output_filter_col_info_[index];
      }
      for (auto field : filter_fields_) {
        output_col_info_[index] =
            ColumnInfo(field->get_storage_length(), field->get_storage_type(),
                       field->get_return_type());
        index++;
      }

      for (auto field : win_func_fields_) {
        output_col_info_[index] =
            ColumnInfo(field->get_storage_length(), field->get_storage_type(),
                       field->get_return_type());
        index++;
      }
    }
    constructDataChunk();

    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR)
    }

    col_size_ = output_fields_.size();
  } while (0);
  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode WindowOperator::ResolveWinTempFields(kwdbContext_p ctx) {
  EnterFunc();
  int num = win_func_fields_.size();
  for (int i = 0; i < num; ++i) {
    Field* field = win_func_fields_[i];
    // Output Field object
    Field* new_field = nullptr;
    switch (field->get_storage_type()) {
      case roachpb::DataType::DOUBLE:
        new_field = KNEW FieldDouble(i, field->get_storage_type(),
                                     field->get_storage_length());
        break;
      case roachpb::DataType::FLOAT:
        new_field = KNEW FieldFloat(i, field->get_storage_type(),
                                    field->get_storage_length());
        break;
      case roachpb::DataType::SMALLINT:
        new_field = KNEW FieldShort(i, field->get_storage_type(),
                                    field->get_storage_length());
        break;
      case roachpb::DataType::INT:
        new_field = KNEW FieldInt(i, field->get_storage_type(),
                                  field->get_storage_length());
        break;
      case roachpb::DataType::BIGINT:
        new_field = KNEW FieldLonglong(i, field->get_storage_type(),
                                       field->get_storage_length());
        break;
      default:
        LOG_WARN("Unknown Output temp Field Type!\n");
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE,
                                      "Unknown Output temp Field Type");
        break;
    }
    if (new_field) {
      new_field->is_chunk_ = true;
      new_field->table_ = field->table_;
      new_field->set_return_type(field->get_return_type());
      // DataChunk columns are treated as TYPEDATA
      new_field->set_column_type(::roachpb::KWDBKTSColumn::ColumnType::
                                     KWDBKTSColumn_ColumnType_TYPE_DATA);
      win_func_output_fields_.push_back(new_field);
    } else {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
    }
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode WindowOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  do {
    if (input_) {
      code = input_->Start(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        LOG_ERROR("window table Scan Init() failed\n");
        break;
      }
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode WindowOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  Return(EEIteratorErrCode::EE_OK)
}

k_bool WindowOperator::CheckNewPartition(RowBatch* row_batch) {
  if (0 == partition_cols_.size() && !first_check_) {
    first_check_ = true;
    return true;
  }
  k_bool is_empty = group_by_cols_.empty();
  int i = 0;
  k_bool is_same = true;
  for (k_uint32 col : partition_cols_) {  // ptag
    Field* field = input_->GetRender()[col];
    k_uint32 colId = field->getColIdxInRs();
    char* data = row_batch->GetData(
        colId, 0, roachpb::KWDBKTSColumn::TYPE_TAG, field->get_storage_type());
    if (is_empty) {
      GroupByColumnInfo info;
      info.col_index = colId;
      k_uint32 size = field->get_storage_length();
      info.data_ptr = static_cast<char*>(malloc(size));
      if (info.data_ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                      "Insufficient memory");
        return true;
      }
      memset(info.data_ptr, 0, size);
      memcpy(info.data_ptr, data, size);
      info.len = size;
      group_by_cols_.push_back(info);
      is_same = false;
    } else {
      if (0 !=
          memcmp(data, group_by_cols_[i].data_ptr, group_by_cols_[i].len)) {
        memcpy(group_by_cols_[i].data_ptr, data, group_by_cols_[i].len);
        is_same = false;
      }
    }
    i++;
  }
  return !is_same;
}

void WindowOperator::ResolvePartionByCols(kwdbContext_p ctx) {
  k_uint32 size = spec_->partitionby_size();
  for (k_uint32 i = 0; i < size; i++) {
    partition_cols_.push_back(i);
  }
}

void WindowOperator::ResolveWindowsFuncs(kwdbContext_p ctx) {}

EEIteratorErrCode WindowOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  do {
    RowBatch* row_batch_ = nullptr;
    while (!is_done_) {
      code = input_->Next(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        break;
      }
      row_batch_ = input_->GetRowBatch(ctx);
      if (limit_ == 0) {
        examined_rows_ += row_batch_->Count();
        break;
      }
      for (k_int32 i = 0; i < row_batch_->Count(); i++) {
        if (limit_ && examined_rows_ >= (limit_ + 1)) {
          break;
        }
        row_batch_->AddSelection();
        row_batch_->NextLine();
        ++examined_rows_;
      }
      if (0 != static_cast<ScanRowBatch *>(row_batch_)->GetEffectCount()) {
        break;
      }
    }

    if (!is_done_) {
      row_batch_->ResetLine();
      if (row_batch_->Count() > 0) {
        if (current_data_chunk_->Capacity() - current_data_chunk_->Count() <
            row_batch_->Count()) {
          // the current chunk have no enough space to save the whole data
          // results in row_batch_, push it into the output queue, create a new
          // one and ensure it has enough space.
          if (current_data_chunk_->Count() > 0) {
            if (NULL != filter_) {
              DoFilter(ctx);
              output_queue_.push(std::move(current_filter_data_chunk_));
              current_data_chunk_.reset();
            } else {
              output_queue_.push(std::move(current_data_chunk_));
            }
          }
          k_uint32 capacity = DataChunk::EstimateCapacity(output_col_info_, output_col_num_);
          if (capacity >= row_batch_->Count()) {
            constructDataChunk();
          } else {
            constructDataChunk(row_batch_->Count());
          }
          count_index_ = 0;
          if (current_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                          "Insufficient memory");
            Return(EEIteratorErrCode::EE_ERROR)
          }
        }

        if (CheckNewPartition(row_batch_)) {
          examined_rows_ = 0;
          is_new_partition_ = true;
        }

        AddRowBatchData(ctx, row_batch_);
      }
    } else {
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        if (NULL != filter_) {
          DoFilter(ctx);
          output_queue_.push(std::move(current_filter_data_chunk_));
          current_data_chunk_.reset();
        } else {
          output_queue_.push(std::move(current_data_chunk_));
        }
      }
    }
  } while (!is_done_ && output_queue_.empty());

  if (!output_queue_.empty()) {
    chunk = std::move(output_queue_.front());
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
    output_queue_.pop();
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK)
    } else {
      Return(code)
    }
  } else {
    if (is_done_) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD);
    } else {
      Return(code)
    }
  }
  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode WindowOperator::DoFilter(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  if (nullptr == filter_) {
    Return(EEIteratorErrCode::EE_OK);
  }
  ProcessFilterCols(true);

  constructFilterDataChunk(output_filter_col_info_, output_filter_col_num_,
                           current_data_chunk_->Count());
  if (current_filter_data_chunk_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR)
  }
  current_thd->SetDataChunk(current_data_chunk_.get());
  for (k_uint32 i = 0; i < current_data_chunk_->Count(); i++) {
    current_data_chunk_->NextLine();
    k_int64 ret = filter_->ValInt();
    if (0 == ret) {
      // current_data_chunk_->NextLine();
      continue;
    }
    current_filter_data_chunk_->InsertData(ctx, current_data_chunk_.get(),
                                           output_fields_);
  }
  ProcessFilterCols(false);
  Return(EEIteratorErrCode::EE_OK)
}

void WindowOperator::ProcessFilterCols(k_bool ischunk) {
  k_uint32 size = output_fields_.size();
  k_uint32 size_f = filter_fields_.size();
  for (k_uint32 i = 0; i < size_f; i++) {
    if (ischunk) {
      filter_fields_[i]->set_num(i + size);
    } else {
      filter_fields_[i]->set_num(filter_fields_num_[i]);
    }
    filter_fields_[i]->is_chunk_ = ischunk;
  }
}

k_int32 WindowOperator::PreComputeRowIndex(RowBatch* row_batch) {
  Field* field = win_func_fields_[win_func_fields_.size() - 1];
  row_batch->ResetLine();
  for (int row = 0; row < row_batch->Count(); ++row) {
    if (field->CheckNull()) {
      row_batch->NextLine();
      continue;
    }
    return row;
  }
  return ROW_NEED_SKIP;
}

KStatus WindowOperator::AddRowBatchData(kwdbContext_p ctx,
                                        RowBatch* row_batch) {
  EnterFunc() KStatus status = KStatus::SUCCESS;
  if (row_batch == nullptr) {
    is_new_partition_ = false;
    Return(KStatus::FAIL)
  }

  k_uint32 filter_size = 0;
  k_uint32 func_size = 0;
  if (NULL != filter_) {
    filter_size = filter_fields_.size();  // not include diff filter
    func_size = win_func_fields_.size();
  }
  if (is_new_partition_) {
    row_start_index_ = PreComputeRowIndex(row_batch);
  } else {
    row_start_index_ = ROW_NO_NEED_SKIP;
  }

  for (k_uint32 col = 0; col < col_size_ + filter_size + func_size; ++col) {
    row_batch->ResetLine();
    Field* field = nullptr;
    if (col < col_size_) {
      field = renders_[col];
    } else if (col < col_size_ + filter_size) {
      field = filter_fields_[col - col_size_];
    } else {
      field = win_func_fields_[col - col_size_ - filter_size];
    }
    k_bool is_diff = false;
    if (field->get_field_type() == Field::Type::FIELD_FUNC &&
        (dynamic_cast<FieldFunc*>(field))->functype() ==
            FieldFunc::Functype::DIFF_FUNC) {
      field->set_clearup_diff(is_new_partition_);
      is_diff = true;
    }
    ProcessDataValue(ctx, row_batch, col, field, is_diff, false);
  }

  if (row_start_index_ != ROW_NEED_SKIP) {
    is_new_partition_ = false;
  }

  count_index_ += row_batch->Count() - (row_start_index_ >= 0 ? 1 : 0);
  current_data_chunk_->SetCount(count_index_);
  Return(status)
}

k_int32 WindowOperator::SkipRowCount(k_int32 row) {
  if (row_start_index_ < 0 || row == 0) {
    return 0;
  }
  return row >= row_start_index_ ? 1 : 0;
}

void WindowOperator::ProcessDataValue(kwdbContext_p ctx, RowBatch* row_batch,
                                      k_uint32 col, Field* field,
                                      k_bool is_diff_func, k_bool diff_first) {
  k_uint32 len = field->get_storage_length();
  switch (field->get_storage_type()) {
    case roachpb::DataType::BOOL: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!(row == row_start_index_)) {
            current_data_chunk_->SetNull(count_index_ + row - SkipRowCount(row),
                                         col);
          }
          row_batch->NextLine();
          continue;
        }

        bool val = field->ValInt() > 0 ? 1 : 0;
        if (!(row == row_start_index_)) {
          current_data_chunk_->InsertData(
              count_index_ + row - SkipRowCount(row), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
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
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row -
                    (diff_first ? 1 : (is_diff_func ? 0 : SkipRowCount(row))),
                col);
          }
          row_batch->NextLine();
          continue;
        }

        k_int64 val = field->ValInt();
        if (is_diff_func && is_new_partition_ && !diff_first) {
          diff_first = true;
          row_batch->NextLine();
          continue;
        }
        if (!((row == row_start_index_) && !is_diff_func)) {
          current_data_chunk_->InsertData(
              count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::INT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row -
                    (diff_first ? 1 : (is_diff_func ? 0 : SkipRowCount(row))),
                col);
          }
          row_batch->NextLine();
          continue;
        }
        k_int32 val = field->ValInt();
        if (is_diff_func && is_new_partition_ && !diff_first) {
          diff_first = true;
          row_batch->NextLine();
          continue;
        }
        if (!((row == row_start_index_) && !is_diff_func)) {
          current_data_chunk_->InsertData(
              count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::SMALLINT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row -
                    (diff_first ? 1 : (is_diff_func ? 0 : SkipRowCount(row))),
                col);
          }
          row_batch->NextLine();
          continue;
        }
        k_int16 val = field->ValInt();
        if (is_diff_func && is_new_partition_ && !diff_first) {
          diff_first = true;
          row_batch->NextLine();
          continue;
        }
        if (!((row == row_start_index_) && !is_diff_func)) {
          current_data_chunk_->InsertData(
              count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::FLOAT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row -
                    (diff_first ? 1 : (is_diff_func ? 0 : SkipRowCount(row))),
                col);
          }
          row_batch->NextLine();
          continue;
        }
        k_float32 val = field->ValReal();
        if (is_diff_func && is_new_partition_ && !diff_first) {
          diff_first = true;
          row_batch->NextLine();
          continue;
        }
        if (!((row == row_start_index_) && !is_diff_func)) {
          current_data_chunk_->InsertData(
              count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::DOUBLE: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row -
                    (diff_first ? 1 : (is_diff_func ? 0 : SkipRowCount(row))),
                col);
          }
          row_batch->NextLine();
          continue;
        }
        k_double64 val = field->ValReal();
        if (is_diff_func && is_new_partition_ && !diff_first) {
          diff_first = true;
          row_batch->NextLine();
          continue;
        }
        if (!((row == row_start_index_) && !is_diff_func)) {
          current_data_chunk_->InsertData(
              count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col,
              reinterpret_cast<char*>(&val), len);
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!(row == row_start_index_)) {
            current_data_chunk_->SetNull(count_index_ + row - SkipRowCount(row),
                                         col);
          }
          row_batch->NextLine();
          continue;
        }

        kwdbts::String val = field->ValStr();
        if (val.isNull()) {
          current_data_chunk_->SetNull(count_index_ + row - SkipRowCount(row),
                                       col);
          row_batch->NextLine();
          continue;
        }
        char* mem = const_cast<char*>(val.c_str());
        if (!(row == row_start_index_)) {
          current_data_chunk_->InsertData(
              count_index_ + row - SkipRowCount(row), col, mem, val.length());
        }
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::DECIMAL: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!(row == row_start_index_)) {
            current_data_chunk_->SetNull(count_index_ + row - SkipRowCount(row),
                                         col);
          }
          row_batch->NextLine();
          continue;
        }
        k_bool overflow = field->is_over_flow();
        if (!(row == row_start_index_)) {
          if (field->get_sql_type() == roachpb::DataType::DOUBLE ||
              field->get_sql_type() == roachpb::DataType::FLOAT || overflow) {
            k_double64 val = field->ValReal();
            current_data_chunk_->InsertDecimal(
                count_index_ + row - SkipRowCount(row), col,
                reinterpret_cast<char*>(&val), true);
          } else {
            k_int64 val = field->ValInt();
            current_data_chunk_->InsertDecimal(
                count_index_ + row - SkipRowCount(row), col,
                reinterpret_cast<char*>(&val), false);
          }
        }
        row_batch->NextLine();
      }
      break;
    }
    default: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          if (!((row == row_start_index_) && !is_diff_func)) {
            current_data_chunk_->SetNull(
                count_index_ + row - (diff_first ? 1 : SkipRowCount(row)), col);
          }
          row_batch->NextLine();
        }
      }
      break;
    }
  }
}

EEIteratorErrCode WindowOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  Return(EEIteratorErrCode::EE_OK)
}

KStatus WindowOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Return(KStatus::SUCCESS);
}

BaseOperator* WindowOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter =
      NewIterator<WindowOperator>(*this, input, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
