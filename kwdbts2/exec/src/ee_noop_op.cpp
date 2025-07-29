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

NoopOperator::NoopOperator(TsFetcherCollection* collection, TSNoopSpec *spec,
                           TSPostProcessSpec *post, TABLE *table,
                           int32_t processor_id)
    : BaseOperator(collection, table, post, processor_id),
      limit_(post->limit()),
      offset_(post->offset()),
      param_(post, table) {}

NoopOperator::NoopOperator(const NoopOperator &other, int32_t processor_id)
    : BaseOperator(other),
      limit_(other.limit_),
      offset_(other.offset_),
      param_(other.post_, other.table_) {
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
    code = param_.ParserFilter(ctx, &filter_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("Resolve having clause error\n");
      break;
    }
    // resolve output fields
    code = param_.ParserOutputFields(ctx, renders_, num_, output_fields_, false);
    if (0 == output_fields_.size()) {
      size_t sz = childrens_[0]->OutputFields().size();
      // output_fields_.reserve(sz);
      for (size_t i = 0; i < sz; ++i) {
        Field *field_copy = childrens_[0]->OutputFields()[i]->field_to_copy();
        output_fields_.push_back(field_copy);
        field_copy->is_chunk_ = true;
      }
    }
    code = InitOutputColInfo(output_fields_);

    if (0 == num_ && nullptr == filter_ && 0 == offset_ && 0 == limit_) {
      is_pass_through_ = true;
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode NoopOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = childrens_[0]->Start(ctx);

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
    code = childrens_[0]->Next(ctx, data_batch);
    start = std::chrono::high_resolution_clock::now();
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    if (is_pass_through_) {
      chunk = std::move(data_batch);
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
    if (chunk->Count() > 0) {
      break;
    }
  } while (1);

  auto end = std::chrono::high_resolution_clock::now();
  if (chunk != nullptr) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, thd, chunk);
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0);
  } else {
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
  }

  Return(code);
}

EEIteratorErrCode NoopOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Close(ctx));
}

EEIteratorErrCode NoopOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  input_->Reset(ctx);
  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator *NoopOperator::Clone() {
  BaseOperator *input = childrens_[0]->Clone();
  // input_ is TagScanOperator
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator *iter = NewIterator<NoopOperator>(*this, processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }
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
  // init resultset
  *chunk =
      std::make_unique<DataChunk>(output_col_info_, output_col_num_, capacity);
  if ((*chunk)->Initialize() != true) {
    LOG_ERROR("DataChunk::Initialize() error\n");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    (*chunk) = nullptr;
  }

  ReturnVoid();
}

PassThroughNoopOperaotr::PassThroughNoopOperaotr(TsFetcherCollection* collection, TSNoopSpec *spec,
                              TSPostProcessSpec *post, TABLE *table, int32_t processor_id)
  : BaseOperator(collection, table, post, processor_id),
    limit_(post->limit()),
    offset_(post->offset()) { }

EEIteratorErrCode PassThroughNoopOperaotr::Init(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Init(ctx));
}

EEIteratorErrCode PassThroughNoopOperaotr::Start(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Start(ctx));
}

EEIteratorErrCode PassThroughNoopOperaotr::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  if (limit_ && examined_rows_ >= limit_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  KWThdContext *thd = current_thd;
  std::chrono::_V2::system_clock::time_point start;
  do {
    DataChunkPtr data_batch = nullptr;
    code = childrens_[0]->Next(ctx, chunk);
    start = std::chrono::high_resolution_clock::now();
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // limit/offset
    break;
  } while (0);

  auto end = std::chrono::high_resolution_clock::now();
  if (chunk != nullptr) {
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0);
  } else {
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
  }

  Return(code);
}

EEIteratorErrCode PassThroughNoopOperaotr::Reset(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Reset(ctx));
}

EEIteratorErrCode PassThroughNoopOperaotr::Close(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Close(ctx));
}

}  // namespace kwdbts
