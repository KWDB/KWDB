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

#include "ee_event_window_helper.h"

#include "ee_common.h"
#include "ee_kwthd_context.h"
#include "ee_scan_op.h"
#include "ee_storage_handler.h"

namespace kwdbts {

bool EventWindowHelper::MaterializeWindowField(KWThdContext* thd, Field* field, DataChunk* chunk, k_int32 row,
                                               k_int32 col) {
  if (field == thd->window_field_) {
    chunk->InsertData(row, col, reinterpret_cast<char*>(&group_id_), field->get_storage_length(), false);
    return true;
  }
  return false;
}

EEIteratorErrCode EventWindowHelper::ReadRowBatch(kwdbContext_p ctx, DataChunk* chunk, bool continue_last_rowbatch) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_int32 count = chunk->Count();
  k_int32 i = 0;
  bool has_begin = false;
  k_int32 start_row = 0;
  while (rowbatchs_.size() > 0) {
    auto rowbatch = rowbatchs_[0];
    has_begin = true;
    start_row = 0;
    if (i == 0) {
      start_row = start_line_;
    }
    thd_->SetRowBatch(rowbatch);
    rowbatch->SetCurrentLine(start_row);
    for (k_int32 j = start_row; j < rowbatch->Count(); j++) {
      MaterializeRow(thd_, chunk, count);
      count++;
      rowbatch->NextLine();
      if (chunk->Count() == chunk->Capacity()) {
        sliding_status_ = SWS_RT_CHUNK;
        start_line_ = j + 1;
        return code;
      }
    }
    PopBatch();
    i++;
  }
  thd_->SetRowBatch(rowbatch_);
  start_row = 0;
  if (!has_begin) {
    start_row = start_line_;
  }
  rowbatch_->SetCurrentLine(start_row);
  for (k_int32 j = start_row; j < end_line_ + 1; j++) {
    MaterializeRow(thd_, chunk, count);
    count++;
    rowbatch_->NextLine();
    if (chunk->Count() == chunk->Capacity()) {
      sliding_status_ = SWS_RT_CHUNK;
      start_line_ = j + 1;
      return code;
    }
  }
  sliding_status_ = SWS_NEXT_WINDOW;
  return code;
}

EEIteratorErrCode EventWindowHelper::NextChunk(kwdbContext_p ctx, DataChunkPtr& chunk) {
  if (no_more_data_) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  thd_ = current_thd;
  op_->constructDataChunk();
  chunk = std::move(op_->current_data_chunk_);
  DataChunk* pchunk = chunk.get();
  while (true) {
    switch (sliding_status_) {
      case SWS_RT_CHUNK: {
        sliding_status_ = SWS_READ_BATCH;
        continue_last_rowbatch_ = true;
        return EEIteratorErrCode::EE_OK;
      } break;
      case SWS_NEXT_WINDOW: {
        if (!rowbatch_) {
          return code;
        }
        rowbatch_->ResetLine();
        rowbatch_->SetCurrentLine(start_row_);
        thd_->SetRowBatch(rowbatch_);
        for (k_int32 i = start_row_; i < rowbatch_->Count(); i++) {
          if (!is_begin_ && field_->IsBegin()) {
            start_line_ = i;
            is_begin_ = true;
            group_id_++;
            end_line_ = -1;
          }
          if (is_begin_ && field_->IsEnd()) {
            end_line_ = i;
            start_row_ = i + 1;
            break;
          }
          rowbatch_->NextLine();
        }
        if (!is_begin_) {
          sliding_status_ = SWS_NEXT_BATCH;
          start_row_ = 0;
          break;
        }
        if (end_line_ < 0 && is_begin_) {
          if (PushBatch(rowbatch_) == FAIL) {
            return EEIteratorErrCode::EE_ERROR;
          }
          rowbatch_ = nullptr;
          sliding_status_ = SWS_NEXT_BATCH;
          start_row_ = 0;
          break;
        }
        is_begin_ = false;
        sliding_status_ = SWS_READ_BATCH;
        break;
      }
      case SWS_READ_BATCH: {
        ReadRowBatch(ctx, pchunk, continue_last_rowbatch_);
        continue_last_rowbatch_ = false;
      } break;
      case SWS_NEXT_BATCH: {
        code = NextBatch(ctx, rowbatch_);
        if (code == EEIteratorErrCode::EE_ERROR) {
          return code;
        } else if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
          no_more_data_ = true;
          if (chunk->Count() > 0) {
            return EEIteratorErrCode::EE_OK;
          }
          return EEIteratorErrCode::EE_END_OF_RECORD;
        }
        if (!is_first_batch_ && !current_entity_index_.equalsWithoutMem(rowbatch_->res_.entity_index)) {
          sliding_status_ = SWS_NEXT_ENTITY;
        } else {
          sliding_status_ = SWS_NEXT_WINDOW;
        }
        is_first_batch_ = false;
        current_entity_index_ = rowbatch_->res_.entity_index;
      } break;
      case SWS_NEXT_ENTITY: {
        is_begin_ = false;
        start_line_ = -1;
        end_line_ = -1;
        while (rowbatchs_.size() > 0) {
          PopBatch();
        }
        sliding_status_ = SWS_NEXT_WINDOW;
      } break;
    }
  }
  return code;
}
}  // namespace kwdbts
