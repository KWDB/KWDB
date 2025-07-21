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

#include "ee_sort_scan_op.h"

#include <queue>

#include "cm_func.h"
#include "ee_cancel_checker.h"
#include "ee_common.h"
#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "lg_api.h"

namespace kwdbts {

SortScanOperator::SortScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, TSPostProcessSpec* post,
                                   TABLE* table, BaseOperator* input,
                                   int32_t processor_id)
    : TableScanOperator(collection, spec, post, table, input, processor_id) {}

SortScanOperator::SortScanOperator(const SortScanOperator& other,
                                   BaseOperator* input, int32_t processor_id)
    : TableScanOperator(other, input, processor_id) {}

SortScanOperator::~SortScanOperator() {
  if (is_offset_opt_) {
    SafeFreePointer(tmp_renders_);
    SafeDeletePointer(new_field_);
    SafeDeleteArray(tmp_output_col_info_)
  }
}

EEIteratorErrCode SortScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  do {
    k_int32 idx = 0;
    code = TableScanOperator::Init(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("TableScanOperator::PreInit() failed\n");
      break;
    }
    is_offset_opt_ = spec_->offsetopt();
    if (!is_offset_opt_) {
      const TSOrdering& order = spec_->sorter();
      k_uint32 order_size_ = order.columns_size();
      if (order_size_ != 1) {
        LOG_ERROR("SortScanOperator must have one order column");
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "must have one order column");
        break;
      }

      idx = order.columns(0).col_idx();
      TSOrdering_Column_Direction direction = order.columns(0).direction();
      if (TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC == direction) {
        table_->is_reverse_ = 1;
      }
    } else {
      idx = 0;
      table_->is_reverse_ = spec_->reverse();
    }

    table_->optimize_offset_ = is_offset_opt_;
    table_->offset_ = offset_;
    table_->limit_ = limit_;

    order_field_ = param_.GetOutputField(ctx, idx);
    code = InitOutputColInfo(output_fields_);
    if (code == EEIteratorErrCode::EE_ERROR) {
      break;
    }
    if (is_offset_opt_) {
      code = mallocTempField(ctx);
    } else {
      tmp_renders_ = renders_;
      tmp_output_fields_ = output_fields_;
      tmp_output_col_info_ = output_col_info_;
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode SortScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = TableScanOperator::Start(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    return code;
  }

  auto start = std::chrono::high_resolution_clock::now();
  KWThdContext* thd = current_thd;
  StorageHandler* handler = handler_;

  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // read data
  while (true) {
    code = InitScanRowBatch(ctx, &row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    row_batch_->ts_ = ts_;
    if (!is_offset_opt_) {
      code = handler_->TsNext(ctx);
    } else {
      code = handler_->TsOffsetNext(ctx);
    }

    if (EEIteratorErrCode::EE_OK != code) {
      if (EEIteratorErrCode::EE_END_OF_RECORD == code ||
          EEIteratorErrCode::EE_TIMESLICE_OUT == code) {
        code = EEIteratorErrCode::EE_OK;
      }
      break;
    }

    k_uint32 storage_offset = handler->GetStorageOffset();
    cur_offset_ = offset_ - storage_offset;

    if (nullptr == data_chunk_) {
      code = initContainer(ctx, data_chunk_, tmp_output_col_info_,
                            is_offset_opt_ ? output_col_num_ + 1 : output_col_num_, limit_ + cur_offset_);
      if (code != EEIteratorErrCode::EE_OK) {
        return code;
      }
    }

    // resolve filter
    ResolveFilter(ctx, row_batch_);
    if (0 == row_batch_->Count()) {
      continue;
    }
    // sort
    PrioritySort(ctx, row_batch_, limit_ + cur_offset_);
  }
  if (nullptr == data_chunk_ && code != EEIteratorErrCode::EE_ERROR) {
    code = initContainer(ctx, data_chunk_, tmp_output_col_info_,
                            is_offset_opt_ ? output_col_num_ + 1 : output_col_num_, 1);
    if (code != EEIteratorErrCode::EE_OK) {
      return code;
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  if (data_chunk_) {
    fetcher_.Update(data_chunk_->Count(), (end - start).count(),
                                    data_chunk_->Count() * data_chunk_->RowSize(), 0, 0, 0);
  }

  Return(code);
}

EEIteratorErrCode SortScanOperator::Next(kwdbContext_p ctx,
                                         DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  if (is_done_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }

  if (!is_offset_opt_) {
    chunk = std::move(data_chunk_);
  } else {
    code = initContainer(ctx, chunk, output_col_info_, output_col_num_, limit_);
    if (code != EEIteratorErrCode::EE_OK) {
      return code;
    }
    if (data_chunk_->Count() > cur_offset_) {
      chunk->CopyFrom(data_chunk_, cur_offset_, data_chunk_->Count() - 1, table_->is_reverse_);
    }
  }

  OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
  is_done_ = true;

  Return(EEIteratorErrCode::EE_OK);
}

KStatus SortScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = input_->Close(ctx);
  Reset(ctx);

  Return(ret);
}

EEIteratorErrCode SortScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  TableScanOperator::Reset(ctx);
  Data* del = nullptr;
  while (!data_asc_.empty()) {
    del = data_asc_.top();
    data_asc_.pop();
    SafeDeletePointer(del);
  }
  while (!data_desc_.empty()) {
    del = data_desc_.top();
    data_desc_.pop();
    SafeDeletePointer(del);
  }

  ts_ = 0;
  order_field_ = nullptr;
  spec_ = nullptr;
  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator* SortScanOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter =
      NewIterator<SortScanOperator>(*this, input, this->processor_id_);
  return iter;
}

EEIteratorErrCode SortScanOperator::mallocTempField(kwdbContext_p ctx) {
  EnterFunc();
  tmp_renders_ = static_cast<Field **>(malloc((num_ + 1) * sizeof(Field *)));
  if (nullptr == tmp_renders_) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  for (k_uint32 i = 0; i < num_; ++i) {
    tmp_renders_[i] = renders_[i];
  }
  tmp_renders_[num_] = table_->GetFieldWithColNum(0);

  new_field_ = table_->GetFieldWithColNum(0)->field_to_copy();
  if (nullptr == new_field_) {
    SafeFreePointer(tmp_renders_);
    Return(EEIteratorErrCode::EE_ERROR);
  }
  new_field_->set_num(output_fields_.size());
  new_field_->is_chunk_ = true;
  tmp_output_fields_.insert(tmp_output_fields_.end(), output_fields_.begin(), output_fields_.end());
  tmp_output_fields_.push_back(new_field_);

  EEIteratorErrCode code = InitTmpOutputColInfo(ctx);
  if (EEIteratorErrCode::EE_ERROR == code) {
    SafeFreePointer(tmp_renders_);
    SafeDeletePointer(new_field_);
  }

  Return(code);
}

EEIteratorErrCode SortScanOperator::InitTmpOutputColInfo(kwdbContext_p ctx) {
  tmp_output_col_info_ = KNEW ColumnInfo[tmp_output_fields_.size()];
  if (tmp_output_col_info_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    return EEIteratorErrCode::EE_ERROR;
  }
  for (k_int32 i = 0; i < tmp_output_fields_.size(); i++) {
    tmp_output_col_info_[i] = ColumnInfo(tmp_output_fields_[i]->get_storage_length(),
                                      tmp_output_fields_[i]->get_storage_type(),
                                      tmp_output_fields_[i]->get_return_type());
    tmp_output_col_info_[i].allow_null = tmp_output_fields_[i]->is_allow_null();
  }
  return EEIteratorErrCode::EE_OK;
}

EEIteratorErrCode SortScanOperator::initContainer(kwdbContext_p ctx, DataChunkPtr&chunk,
                                    ColumnInfo* output_col_info, k_int32 output_col_num, k_uint32 line) {
  // init col
  k_uint32 row_size = DataChunk::ComputeRowSize(output_col_info, output_col_num);
  if (line * row_size < BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE) {
    chunk = std::make_unique<DataChunk>(output_col_info, output_col_num, line);
  }
  if (!chunk || chunk->Initialize() != true) {
    chunk = nullptr;
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    return EEIteratorErrCode::EE_ERROR;
  }
  return EEIteratorErrCode::EE_OK;
}

EEIteratorErrCode SortScanOperator::ResolveFilter(kwdbContext_p ctx,
                                                  ScanRowBatch *row_batch) {
  if (nullptr == filter_) {
    return EEIteratorErrCode::EE_OK;
  }

  for (int i = 0; i < row_batch->count_; ++i) {
    k_int64 ret = filter_->ValInt();
    if (0 == ret) {
      row_batch->NextLine();
      continue;
    }

    row_batch->AddSelection();
    row_batch->NextLine();
  }
  row_batch->ResetLine();
  row_batch->is_filter_ = true;
  return EEIteratorErrCode::EE_OK;
}

EEIteratorErrCode SortScanOperator::PrioritySort(kwdbContext_p ctx,
                                                 ScanRowBatch *row_batch,
                                                 k_uint32 limit) {
  k_uint32 renders_num = is_offset_opt_ ? num_ + 1 : num_;
  k_uint32 count = row_batch->Count();
  k_uint32 free = limit - data_chunk_->Count();
  k_uint32 num = free > count ? count : free;
  if (0 == table_->is_reverse_) {  // asc
    for (k_uint32 i = 0; i < num; ++i) {
      Data* data = new Data;
      data->ts_ = order_field_->ValInt();
      data->rowno_ = data_chunk_->Count();
      data_asc_.push(data);
      FieldsToChunk(tmp_renders_, renders_num, data->rowno_, data_chunk_);
      data_chunk_->AddCount();
      row_batch->NextLine();
    }

    for (k_uint32 i = num; i < count; ++i) {
      k_int64 ts = order_field_->ValInt();
      Data *top = data_asc_.top();
      if (ts < top->ts_) {
        data_asc_.pop();
        Data* data = new Data;
        data->ts_ = ts;
        data->rowno_ = top->rowno_;
        FieldsToChunk(tmp_renders_, renders_num, top->rowno_, data_chunk_);
        data_asc_.push(data);
        SafeDeletePointer(top);
      }
      row_batch->NextLine();
    }
    if (data_asc_.size() == limit) {
      ts_ = data_asc_.top()->ts_;
    }
  } else {
    for (k_uint32 i = 0; i < num; ++i) {
      Data* data = new Data;
      data->ts_ = order_field_->ValInt();
      data->rowno_ = data_chunk_->Count();
      data_desc_.push(data);
      FieldsToChunk(tmp_renders_, renders_num, data->rowno_, data_chunk_);
      data_chunk_->AddCount();
      row_batch->NextLine();
    }

    for (k_uint32 i = num; i < count; ++i) {
      k_int64 ts = order_field_->ValInt();
      Data *top = data_desc_.top();
      if (ts > top->ts_) {
        data_desc_.pop();
        Data* data = new Data;
        data->ts_ = ts;
        data->rowno_ = top->rowno_;
        FieldsToChunk(tmp_renders_, renders_num, top->rowno_, data_chunk_);
        data_desc_.push(data);
        SafeDeletePointer(top);
      }
      row_batch->NextLine();
    }
    if (data_desc_.size() == limit) {
      ts_ = data_desc_.top()->ts_;
    }
  }

  return EEIteratorErrCode::EE_OK;
}

}  // namespace kwdbts
