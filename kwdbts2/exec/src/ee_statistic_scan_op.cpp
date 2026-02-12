
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

#include "ee_statistic_scan_op.h"

#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_tag_scan_op.h"
#include "lg_api.h"
#include "ee_scan_op.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

TableStatisticScanOperator::TableStatisticScanOperator(TsFetcherCollection* collection,
    TSStatisticReaderSpec* spec, PostProcessSpec* post, TABLE* table, int32_t processor_id)
    : BaseOperator(collection, table, post, processor_id),
      schema_id_(0),
      object_id_(spec->tableid()),
      param_(spec, post, table) {
  for (int i = 0; i < spec->tsspans_size(); i++) {
    TsSpan* span = spec->mutable_tsspans(i);
    KwTsSpan ts_span;
    if (span->has_fromtimestamp()) {
      ts_span.begin = span->fromtimestamp();
    }

    if (span->has_totimestamp()) {
      ts_span.end = span->totimestamp();
    }
    ts_kwspans_.push_back(ts_span);
  }
  if (0 == spec->tsspans_size()) {
    KwTsSpan ts_span;
    ts_span.begin = kInt64Min;
    ts_span.end = kInt64Max;
    ts_kwspans_.push_back(ts_span);
  }
}

TableStatisticScanOperator::TableStatisticScanOperator(
    const TableStatisticScanOperator& other, int32_t processor_id)
    : BaseOperator(other),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      param_(other.param_.spec_, other.post_, other.table_) {
  is_clone_ = true;
  param_.insert_ts_index_ = other.param_.insert_ts_index_;
  param_.insert_last_tag_ts_num_ = other.param_.insert_last_tag_ts_num_;
  param_.tag_count_index_ = other.param_.tag_count_index_;
}

TableStatisticScanOperator::~TableStatisticScanOperator() {
  SafeDeletePointer(handler_);
  SafeDeletePointer(row_batch_);
}

EEIteratorErrCode TableStatisticScanOperator::InitHandler(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  handler_ = KNEW StorageHandler(table_);
  if (!handler_) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  handler_->Init(ctx);
  handler_->SetTagScan(static_cast<TagScanBaseOperator *>(childrens_[0]));
  handler_->SetSpans(&ts_kwspans_);
  Return(ret);
}

EEIteratorErrCode TableStatisticScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  code = InitHandler(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  if (childrens_[0]) {
    code = childrens_[0]->Start(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Tag Scan Init() failed\n");
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode TableStatisticScanOperator::InitScanRowBatch(kwdbContext_p ctx, ScanRowBatch **row_batch) {
  EnterFunc();
  KWThdContext *thd = current_thd;
  *row_batch = static_cast<ScanRowBatch *>(thd->GetRowBatch());
  if (nullptr != *row_batch) {
    (*row_batch)->Reset();
  } else {
    *row_batch = KNEW ScanRowBatch(table_);
    thd->SetRowBatch(*row_batch);
  }

  if (nullptr == (*row_batch)) {
    LOG_ERROR("make scan data handle failed");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;

  do {
    if (childrens_[0]) {
      ret = childrens_[0]->Init(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Tag Scan Init() failed\n");
        break;
      }
    }

    param_.RenderSize(ctx, &num_);

    if (!is_clone_) {
      ret = param_.ResolveScanCols(ctx);
    }

    ret = param_.ParserRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve ResolveRender failed!\n");
      break;
    }

    // resolve scalar
    is_scalar_ = param_.spec_->scalar();

    is_lastrow_optimize_ = param_.spec_->lastrowopt();

    // dispose Output Fields
    ret = param_.ParserOutputFields(ctx, renders_, num_, output_fields_, false);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // dispose tag span filter
    tag_count_read_index_ = param_.ResolveChecktTagCount();

    // init column info used by data chunk.
    ret = InitOutputColInfo(output_fields_);
    if (ret != EEIteratorErrCode::EE_OK) {
      break;
    }

    constructDataChunk();
    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR)
    }
  } while (0);

  Return(ret);
}

EEIteratorErrCode TableStatisticScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  if (childrens_[0] && !is_clone_) {
    code = childrens_[0]->Close(ctx);
  }
  Return(code);
}

EEIteratorErrCode TableStatisticScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  if (table_->hash_spans_.empty() && current_thd->IsRemote()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  TsScanStats ts_scan_stats;
  do {
    code = InitScanRowBatch(ctx, &row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      Return(code);
    }

    while (!is_done_) {
      if (!is_lastrow_optimize_) {
        code = handler_->TsNext(ctx, &ts_scan_stats);
      } else {
        code = handler_->TsStatisticCacheNext(ctx, &ts_scan_stats);
      }
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        code = EEIteratorErrCode::EE_END_OF_RECORD;
        if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
          output_queue_.push(std::move(current_data_chunk_));
        }
        break;
      }
      if (row_batch_->count_ < 1) {
        continue;
      } else {
        is_has_data_for_scalar_ = true;
        break;
      }
    }

    // reset line
    row_batch_->ResetLine();

    if (is_scalar_ && !is_has_data_for_scalar_ &&
        code == EEIteratorErrCode::EE_END_OF_RECORD) {
      ProcessScalar();
    }

    if (!is_done_) {
      if (row_batch_->Count() > 0) {
        if (0 == ProcessPTagSpanFilter(row_batch_)) {
          continue;
        }
        if (current_data_chunk_->Capacity() - current_data_chunk_->Count() < row_batch_->Count()) {
          if (current_data_chunk_->Count() > 0) {
            output_queue_.push(std::move(current_data_chunk_));
          }
          k_uint32 capacity = DataChunk::EstimateCapacity(output_col_info_, output_col_num_);
          if (capacity >= row_batch_->Count()) {
            constructDataChunk();
          } else {
            constructDataChunk(row_batch_->Count());
          }
          if (current_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(EEIteratorErrCode::EE_ERROR)
          }
        }

        KStatus status = current_data_chunk_->AddRowBatchData(ctx, row_batch_, renders_);
        if (status != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
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
      Return(EEIteratorErrCode::EE_END_OF_RECORD);
    } else {
      Return(code)
    }
  }
}

k_int64 TableStatisticScanOperator::ProcessPTagSpanFilter(RowBatch* row_batch) {
  if (tag_count_read_index_ < 0) {
    return 1;
  }

  row_batch->ResetLine();
  return row_batch->IsNull(tag_count_read_index_,
                           roachpb::KWDBKTSColumn::TYPE_DATA)
             ? 0
             : 1;
}

void TableStatisticScanOperator::ProcessScalar() {
  k_uint32 count_ = current_data_chunk_->Count();
  k_uint32 col_num_ = output_col_num_;
  for (k_uint32 col = 0; col < col_num_; ++col) {
    if (renders_[col]->get_field_statistic()) {
      k_int64 val = 0;
      current_data_chunk_->InsertData(
          count_, col, reinterpret_cast<char*>(&val), sizeof(k_int64));
    } else {
      current_data_chunk_->SetNull(count_, col);
    }
  }
  current_data_chunk_->AddCount();
}

BaseOperator* TableStatisticScanOperator::Clone() {
  // input_ is the object of TagScanIterator，shared by the object of TableScanIterator
  BaseOperator* iter = NewIterator<TableStatisticScanOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(childrens_[0]);
  }
  return iter;
}

}  // namespace kwdbts
