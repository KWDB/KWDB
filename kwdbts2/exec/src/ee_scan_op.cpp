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

#include "ee_scan_op.h"

#include <cmath>
#include <memory>
#include <string>
#include <vector>
#include <chrono>

#include "cm_func.h"
#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_window_helper.h"
#include "ee_time_window_helper.h"
#include "ee_kwthd_context.h"
#include "ee_tag_scan_op.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "lg_api.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

TableScanOperator::TableScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, TSPostProcessSpec* post,
                                     TABLE* table, BaseOperator* input, int32_t processor_id)
    : BaseOperator(collection, table, processor_id),
      spec_{spec},
      post_(post),
      schema_id_(0),
      object_id_(spec->tableid()),
      limit_(post->limit()),
      offset_(post->offset()),
      param_(post, table),
      input_{input} {
  int count = spec->ts_spans_size();
  for (int i = 0; i < count; ++i) {
    TsSpan* span = spec->mutable_ts_spans(i);
    KwTsSpan ts_span;
    if (span->has_fromtimestamp()) {
      ts_span.begin = span->fromtimestamp();
    }
    if (span->has_totimestamp()) {
      ts_span.end = span->totimestamp();
    }
    ts_kwspans_.push_back(ts_span);
  }
  if (0 == count) {
    KwTsSpan ts_span;
    ts_span.begin = kInt64Min;
    ts_span.end = kInt64Max;
    ts_kwspans_.push_back(ts_span);
  }
  if (spec->orderedscan()) {
    table->ordered_scan_ = true;
  }
  if (spec->reverse()) {
    table->is_reverse_ = true;
  }
}

TableScanOperator::TableScanOperator(const TableScanOperator& other, BaseOperator* input, int32_t processor_id)
    : BaseOperator(other),
      spec_{other.spec_},
      post_(other.post_),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      limit_(other.limit_),
      offset_(other.offset_),
      param_(other.post_, other.table_),
      input_{input} {
  is_clone_ = true;
}

TableScanOperator::~TableScanOperator() {}

EEIteratorErrCode TableScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();

  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    if (input_) {
      ret = input_->Init(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Init tagscan failed.");
        break;
      }
    }

    // post->filter;
    ret = param_.ResolveFilter(ctx, &filter_, false);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("Rosolve filter failed.");
      break;
    }

    // render num
    param_.RenderSize(ctx, &num_);

    // resolve renders
    ret = param_.ResolveRender(ctx, &renders_, num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Resolve render failed.");
      break;
    }

    if (!ignore_outputtypes_) {
      // resolve output type(return type)
      ret = param_.ResolveOutputType(ctx, renders_, num_);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Resolve output type failed.");
        break;
      }
    }
    // output Field
    ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Resolve output fields failed.");
      break;
    }

    // init column info used by data chunk.
    ret = InitOutputColInfo(output_fields_);
    if (ret != EEIteratorErrCode::EE_OK) {
      Return(EEIteratorErrCode::EE_ERROR);
    }

    constructDataChunk();
    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR)
    }

    // batch_copy_ indicates that batch materialization optimization can be used.
    // If the data type of the field is not FIELD_ITEM, it means that the field is not a simple field,
    // and the data cannot be directly copied to the data chunk.
    batch_copy_ = true;

    for (int i = 0; i < num_; i++) {
      auto field = renders_[i];
      if (field->get_field_type() != Field::Type::FIELD_ITEM) {
        batch_copy_ = false;
      }
    }

    if (!is_clone_) {
      ret = param_.ResolveScanCols(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        Return(ret);
      }
      ret = param_.ResolveBlockFilter(spec_);
    }
  } while (0);
  Return(ret);
}

EEIteratorErrCode TableScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  cur_offset_ = offset_;

  code = InitHandler(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  code = InitHelper(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  if (input_) {
    code = input_->Start(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Start tagscan failed.");
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  if (limit_ && examined_rows_ >= limit_) {
    code = EEIteratorErrCode::EE_END_OF_RECORD;
    Return(code);
  }
  code = InitScanRowBatch(ctx, &row_batch_);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  code =
      handler_->TsNextAndFilter(ctx, filter_, &cur_offset_, limit_, row_batch_,
                                &total_read_row_, &examined_rows_);

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;

  auto start = std::chrono::high_resolution_clock::now();
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  chunk = nullptr;
  code = helper_->NextChunk(ctx, chunk);
  if (chunk) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0);
    Return(code);
  } else {
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
    Return(code);
  }
}

EEIteratorErrCode TableScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  SafeDeletePointer(handler_);
  SafeDeletePointer(helper_);
  SafeDeletePointer(row_batch_);
  Return(EEIteratorErrCode::EE_OK);
}

KStatus TableScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  KStatus ret;
  if (input_) {
    ret = input_->Close(ctx);
  }
  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitHandler(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  handler_ = KNEW StorageHandler(table_);
  if (!handler_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  handler_->Init(ctx);
  handler_->SetTagScan(static_cast<TagScanBaseOperator*>(input_));
  handler_->SetSpans(&ts_kwspans_);

  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitHelper(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  if (thd->wtyp_ == WindowGroupType::EE_WGT_COUNT_SLIDING) {
    helper_ = KNEW CountWindowHelper(this);
  } else if (thd->wtyp_ == WindowGroupType::EE_WGT_TIME) {
    helper_ = KNEW TimeWindowHelper(this);
  } else {
    helper_ = KNEW ScanHelper(this);
  }
  if (!helper_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  ret = helper_->Init(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitScanRowBatch(kwdbContext_p ctx, ScanRowBatch** row_batch) {
  if (nullptr != *row_batch) {
    (*row_batch)->Reset();
  } else {
    if (table_->is_reverse_) {
      *row_batch = KNEW ReverseScanRowBatch(table_);
    } else {
      *row_batch = KNEW ScanRowBatch(table_);
    }
    KWThdContext* thd = current_thd;
    thd->SetRowBatch(*row_batch);
  }

  if (nullptr == (*row_batch)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("Create rowbatch failed.");
    return EEIteratorErrCode::EE_ERROR;
  }
  return EEIteratorErrCode::EE_OK;
}

RowBatch* TableScanOperator::GetRowBatch(kwdbContext_p ctx) {
  EnterFunc();
  KWThdContext* thd = current_thd;
  Return(thd->GetRowBatch());
}

k_bool TableScanOperator::ResolveOffset() {
  k_bool ret = KFALSE;

  do {
    if (cur_offset_ == 0) {
      break;
    }
    --cur_offset_;
    ret = KTRUE;
  } while (0);

  return ret;
}

BaseOperator* TableScanOperator::Clone() {
  // input_ is TagScanIterator object，shared by TableScanIterator
  BaseOperator* iter = NewIterator<TableScanOperator>(*this, input_, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
