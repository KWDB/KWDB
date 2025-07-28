// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_noop_op.h"

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "lg_api.h"

namespace kwdbts {

NoopOperator::NoopOperator(TsFetcherCollection* collection, BaseOperator *input, TSNoopSpec *spec,
                           TSPostProcessSpec *post, TABLE *table,
                           int32_t processor_id)
    : BaseOperator(collection, table, processor_id),
      limit_(post->limit()),
      offset_(post->offset()),
      param_(input, post, table),
      post_(post),
      input_{input} {}

NoopOperator::NoopOperator(const NoopOperator &other, BaseOperator *input,
                           int32_t processor_id)
    : BaseOperator(other),
      limit_(other.limit_),
      offset_(other.offset_),
      param_(input, other.post_, other.table_),
      post_(other.post_),
      input_{input} {
  is_clone_ = true;
}

Field *NoopOperator::GetRender(int i) {
  if (i < GetRenderSize()) {
    return renders_[i];
  }

  return nullptr;
}

EEIteratorErrCode NoopOperator::Init(kwdbContext_p ctx) {
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
    // resolve having
    code = param_.ResolveFilter(ctx, &filter_, false);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("Resolve having clause error\n");
      break;
    }
    // resolve output fields
    code = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (0 == output_fields_.size()) {
      size_t sz = input_->OutputFields().size();
      // output_fields_.reserve(sz);
      for (size_t i = 0; i < sz; ++i) {
        Field *field_copy = input_->OutputFields()[i]->field_to_copy();
        output_fields_.push_back(field_copy);
        field_copy->is_chunk_ = true;
      }
    }
    code = InitOutputColInfo(output_fields_);
  } while (0);

  Return(code);
}

EEIteratorErrCode NoopOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = input_->Start(ctx);

  Return(code);
}

EEIteratorErrCode NoopOperator::Next(kwdbContext_p ctx, DataChunkPtr &chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  if (limit_ && examined_rows_ >= limit_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  KWThdContext *thd = current_thd;
  std::chrono::_V2::system_clock::time_point start;
  do {
    DataChunkPtr data_batch = nullptr;
    code = input_->Next(ctx, data_batch);
    start = std::chrono::high_resolution_clock::now();
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    DataChunk *input_chunk = data_batch.get();
    thd->SetDataChunk(input_chunk);
    input_chunk->ResetLine();
    k_uint32 count = input_chunk->Count();

    make_noop_data_chunk(ctx, &chunk, count);
    if (chunk == nullptr) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }

    for (k_uint32 i = 0; i < count; ++i) {
      k_uint32 row = input_chunk->NextLine();
      if (nullptr != filter_ && 0 == filter_->ValInt()) {
        continue;
      }

      if (offset_ > 0) {
        --offset_;
        continue;
      }

      chunk->InsertData(ctx, input_chunk, num_ != 0 ? renders_ : nullptr);
      ++examined_rows_;
      if (limit_ > 0 && examined_rows_ >= limit_) {
        break;
      }
    }
  } while (0);

  auto end = std::chrono::high_resolution_clock::now();
  if (chunk != nullptr) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0);
  } else {
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
  }

  Return(code);
}

KStatus NoopOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  input_->Close(ctx);

  Return(KStatus::SUCCESS);
}

EEIteratorErrCode NoopOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  input_->Reset(ctx);
  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator *NoopOperator::Clone() {
  BaseOperator *input = input_->Clone();
  // input_ is TagScanOperator
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator *iter = NewIterator<NoopOperator>(*this, input, processor_id_);
  return iter;
}

EEIteratorErrCode NoopOperator::ResolveFilter(kwdbContext_p ctx,
                                              const RowBatchPtr &row_batch) {
  EnterFunc();

  if (nullptr == filter_) {
    Return(EEIteratorErrCode::EE_OK);
  }

  k_uint32 count = row_batch->Count();
  for (k_uint32 i = 0; i < count; ++i) {
    if (filter_->ValInt()) {
      row_batch->AddSelection();
    }
    row_batch->NextLine();
  }

  row_batch->EndSelection();

  Return(EEIteratorErrCode::EE_OK);
}

void NoopOperator::ResolveLimitOffset(kwdbContext_p ctx,
                                      const RowBatchPtr &row_batch) {
  k_uint32 count = row_batch->Count();
  if (0 == count && 0 == offset_ && 0 == limit_) {
    return;
  }

  row_batch->SetLimitOffset(limit_, offset_);
}

void NoopOperator::make_noop_data_chunk(kwdbContext_p ctx, DataChunkPtr *chunk,
                                        k_uint32 capacity) {
  EnterFunc();

  if (nullptr == *chunk) {
    // init resultset
    *chunk = std::make_unique<DataChunk>(output_col_info_, output_col_num_, capacity);
    if ((*chunk)->Initialize() != true) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      (*chunk) = nullptr;
    }
  }

  ReturnVoid();
}

}  // namespace kwdbts
