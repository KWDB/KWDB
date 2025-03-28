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

#include "ee_tag_scan_op.h"

#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "cm_func.h"
#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "lg_api.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

TagScanOperator::TagScanOperator(TsFetcherCollection *collection, TSTagReaderSpec* spec, TSPostProcessSpec* post,
                                 TABLE* table, int32_t processor_id)
    : TagScanBaseOperator(collection, table, processor_id),
      spec_(spec),
      post_(post),
      schema_id_(0),
      param_(post, table) {
  if (spec) {
    table->SetAccessMode(spec->accessmode());
    table->ptag_size_ = spec->primarytags_size();
    object_id_ = spec->tableid();
    table->SetTableVersion(spec->tableversion());
    table->SetOnlyTag(spec->only_tag());
  }
}

TagScanOperator::~TagScanOperator() = default;

EEIteratorErrCode TagScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (is_init_) {
    Return(init_code_);
  }
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    // resolve tag
    param_.ResolveScanTags(ctx);
    // post->filter;
    ret = param_.ResolveFilter(ctx, &filter_, true);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve::ResolveFilter() failed");
      break;
    }
    if (object_id_ > 0) {
      // renders num
      param_.RenderSize(ctx, &num_);
      ret = param_.ResolveRender(ctx, &renders_, num_);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("ResolveRender() failed");
        break;
      }
      // resolve render output type
      if (table_->only_tag_) {
        param_.ResolveOutputType(ctx, renders_, num_);
      }
      // Output Fields
      ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
      if (EEIteratorErrCode::EE_OK != ret) {
        LOG_ERROR("ResolveOutputFields() failed");
        break;
      }
      ret = InitOutputColInfo(output_fields_);
      if (ret != EEIteratorErrCode::EE_OK) {
        break;
      }
    }
  } while (0);
  is_init_ = true;
  init_code_ = ret;
  Return(ret);
}

EEIteratorErrCode TagScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  // Involving parallelism, ensuring that it is only called once
  if (started_) {
    Return(start_code_);
  }
  started_ = true;
  start_code_ = EEIteratorErrCode::EE_ERROR;
  handler_ = new StorageHandler(table_);
  start_code_ = handler_->Init(ctx);
  if (start_code_ == EEIteratorErrCode::EE_ERROR) {
    Return(start_code_);
  }

  k_uint32 access_mode = table_->GetAccessMode();
  switch (access_mode) {
    case TSTableReadMode::tagIndex:
    case TSTableReadMode::tagIndexTable:
    case TSTableReadMode::tagHashIndex: {
      break;
    }
    case TSTableReadMode::tableTableMeta:
    case TSTableReadMode::metaTable: {
      handler_->SetReadMode((TSTableReadMode) access_mode);
      start_code_ = handler_->NewTagIterator(ctx);
      if (start_code_ != EE_OK) {
        Return(start_code_);
      }
      break;
    }
    default: {
      LOG_ERROR("access mode unknow, %d", access_mode);
      break;
    }
  }
  Return(start_code_);
}

EEIteratorErrCode TagScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  EEIteratorErrCode code = EEIteratorErrCode::EE_END_OF_RECORD;
  k_uint32 access_mode = table_->GetAccessMode();

  do {
    tag_rowbatch_ = std::make_shared<TagRowBatch>();
    tag_rowbatch_->Init(table_);
    handler_->SetTagRowBatch(tag_rowbatch_);
    if ((access_mode < TSTableReadMode::tableTableMeta) ||
        (access_mode == TSTableReadMode::tagHashIndex)) {
      if (!tag_index_once_) {
        break;
      }
      tag_index_once_ = false;
      code = handler_->GetEntityIdList(ctx, spec_, filter_);
      if (code != EE_OK && code != EE_END_OF_RECORD) {
        break;
      }
    } else {
      code = handler_->TagNext(ctx, filter_);
      if (code != EE_OK && code != EE_END_OF_RECORD) {
        break;
      }
    }
    total_read_row_ += tag_rowbatch_->count_;
  } while (0);

  auto end = std::chrono::high_resolution_clock::now();
  fetcher_.Update(tag_rowbatch_->count_, (end - start).count(), 0, 0, 0, 0);

  Return(code);
}

EEIteratorErrCode TagScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_END_OF_RECORD;
  KWThdContext *thd = current_thd;
  k_uint32 access_mode = table_->GetAccessMode();
  auto start = std::chrono::high_resolution_clock::now();
  do {
    tag_rowbatch_ = std::make_shared<TagRowBatch>();
    tag_rowbatch_->Init(table_);
    handler_->SetTagRowBatch(tag_rowbatch_);
    if ((access_mode < TSTableReadMode::tableTableMeta) ||
        (access_mode == TSTableReadMode::tagHashIndex)) {
      if (!tag_index_once_) {
        break;
      }
      tag_index_once_ = false;
      code = handler_->GetEntityIdList(ctx, spec_, filter_);
      if (code != EE_OK && code != EE_END_OF_RECORD) {
        break;
      }
    } else {
      code = handler_->TagNext(ctx, filter_);
      if (code != EE_OK && code != EE_END_OF_RECORD) {
        break;
      }
    }
    total_read_row_ += tag_rowbatch_->count_;
    thd->SetRowBatch(tag_rowbatch_.get());

    // reset
    tag_rowbatch_->ResetLine();
    if (tag_rowbatch_->Count() > 0) {
      // init DataChunk
      if (nullptr == chunk) {
        chunk = std::make_unique<DataChunk>(output_col_info_, output_col_num_, tag_rowbatch_->Count());
        if (chunk->Initialize() != true) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          chunk = nullptr;
          Return(EEIteratorErrCode::EE_ERROR);
        }
      }

      KStatus status = chunk->AddRowBatchData(ctx, tag_rowbatch_.get(), renders_);
      if (status != KStatus::SUCCESS) {
        Return(EEIteratorErrCode::EE_ERROR);
      }
    }
  } while (0);
  auto end = std::chrono::high_resolution_clock::now();
  if (chunk != nullptr) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
    fetcher_.Update(chunk->Count(), (end - start).count(), 0, 0, 0, 0);
  }

  Return(code);
}

RowBatch* TagScanOperator::GetRowBatch(kwdbContext_p ctx) {
  EnterFunc();

  Return(tag_rowbatch_.get());
}

EEIteratorErrCode TagScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();

  if (handler_) {
    SafeDeletePointer(handler_);
  }
  examined_rows_ = 0;
  total_read_row_ = 0;
  data_ = nullptr;
  count_ = 0;
  tag_index_once_ = false;
  started_ = false;
  tag_index_once_ = true;

  Return(EEIteratorErrCode::EE_OK)
}

KStatus TagScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  Return(KStatus::SUCCESS);
}

KStatus TagScanOperator::GetEntities(kwdbContext_p ctx,
                                     std::vector<EntityResultIndex> *entities,
                                     TagRowBatchPtr *row_batch_ptr) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (*row_batch_ptr == nullptr) {
    *row_batch_ptr = tag_rowbatch_;
  }
  if (is_first_entity_ || (*row_batch_ptr != nullptr &&
                           (row_batch_ptr->get()->isAllDistributed()))) {
    if (is_first_entity_ || *row_batch_ptr == tag_rowbatch_ || tag_rowbatch_->isAllDistributed()) {
      is_first_entity_ = false;
      EEIteratorErrCode code = Next(ctx);
      if (code != EE_OK) {
        Return(FAIL);
      }
    } else if (tag_rowbatch_.get()->Count() == 0) {
      Return(FAIL);
    }

    // construct ts_iterator
    *row_batch_ptr = tag_rowbatch_;
  }
  KStatus ret = row_batch_ptr->get()->GetEntities(entities);
  Return(ret);
}

}  // namespace kwdbts
