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

TableScanOperator::TableScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, PostProcessSpec* post,
                                     TABLE* table, int32_t processor_id)
    : BaseOperator(collection, table, post, processor_id),
      spec_(spec),
      schema_id_(0),
      object_id_(spec->tableid()),
      limit_(post->limit()),
      offset_(post->offset()),
      param_(spec, post, table) {
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

TableScanOperator::TableScanOperator(const TableScanOperator& other, int32_t processor_id)
    : BaseOperator(other),
      spec_(other.spec_),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      limit_(other.limit_),
      offset_(other.offset_),
      param_(other.spec_, other.post_, other.table_) {
  is_clone_ = true;
}

TableScanOperator::~TableScanOperator() {
  auto ctx = ContextManager::GetThreadContext();
  Reset(ctx);
}

EEIteratorErrCode TableScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    if (childrens_[0]) {
      ret = childrens_[0]->Init(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Init tagscan failed.");
        break;
      }
    }

    // post->filter;
    ret = param_.ParserFilter(ctx, &filter_);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("Rosolve filter failed.");
      break;
    }

    // parser input fields
    ret = param_.ParserInputField(ctx);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ParserInputFields failed.");
      break;
    }
    // render num
    param_.RenderSize(ctx, &num_);

    // resolve renders
    ret = param_.ParserRender(ctx, &renders_, num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Resolve render failed.");
      break;
    }

    // output Field
    ret = param_.ParserOutputFields(ctx, renders_, num_, output_fields_, ignore_outputtypes_);
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
      ret = param_.ParserScanCols(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        Return(ret);
      }
      ret = param_.ResolveBlockFilter();
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
  if (childrens_[0]) {
    code = childrens_[0]->Start(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("table_->hash_points_ size : %lu, is remote : %d", table_->hash_points_.size(), current_thd->IsRemote());
      if (table_->hash_points_.empty() && current_thd->IsRemote()) {
        EEPgErrorInfo::ResetPgErrorInfo();
        Return(EEIteratorErrCode::EE_OK);
      }
      LOG_ERROR("Start tagscan failed.");
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  if (table_->hash_points_.empty() && current_thd->IsRemote()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
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
                                &total_read_row_, &examined_rows_, NULL);

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;
  if (table_->hash_points_.empty() && thd->IsRemote()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  auto start = std::chrono::high_resolution_clock::now();
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  TsScanStats& ts_scan_stats = helper_->GetTsScanStats();
  ts_scan_stats.reset();
  chunk = nullptr;
  code = helper_->NextChunk(ctx, chunk);
  if (chunk) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, thd, chunk);
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0, 0, &ts_scan_stats);
    Return(code);
  } else {
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0, 0, &ts_scan_stats);
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

EEIteratorErrCode TableScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = Reset(ctx);
  if (childrens_[0] && !is_clone_) {
    code = childrens_[0]->Close(ctx);
  }
  Return(code);
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
  handler_->SetTagScan(static_cast<TagScanBaseOperator*>(childrens_[0]));
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
  BaseOperator* iter = NewIterator<TableScanOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(childrens_[0]);
  }

  return iter;
}

}  // namespace kwdbts
