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

#include "ee_window_helper.h"

#include "ee_common.h"
#include "ee_kwthd_context.h"
#include "ee_scan_op.h"
#include "ee_storage_handler.h"

namespace kwdbts {

void WindowHelper::MaterializeRow(KWThdContext *thd, DataChunk *chunk,
                                  k_int32 count) {
  ColumnInfo *col_info = chunk->GetColumnInfo();
  for (k_uint32 col = 0; col < op_->num_; ++col) {
    Field *field = op_->renders_[col];
    if (MaterializeWindowField(thd, field, chunk, count, col)) {
      continue;
    }
    // dispose null
    if (field->CheckNull()) {
      chunk->SetNull(count, col);
      continue;
    }
    if (col_info[col].is_string) {
      kwdbts::String val = field->ValStr();
      if (val.isNull()) {
        chunk->SetNull(count, col);
        break;
      }
      char *mem = const_cast<char *>(val.c_str());
      chunk->InsertData(count, col, mem, val.length(), false);
    } else {
      k_uint32 len = col_info[col].storage_len;
      char *ptr = field->get_ptr();
      if (ptr) {
        chunk->InsertData(count, col, ptr, len, false);
      } else {
        switch (col_info[col].storage_type) {
          case roachpb::DataType::BOOL: {
            bool val = field->ValInt() > 0 ? 1 : 0;
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT: {
            k_int64 val = field->ValInt();
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
          case roachpb::DataType::INT: {
            k_int32 val = field->ValInt();
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
          case roachpb::DataType::SMALLINT: {
            k_int16 val = field->ValInt();
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
          case roachpb::DataType::FLOAT: {
            k_float32 val = field->ValReal();
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
          case roachpb::DataType::DOUBLE: {
            k_double64 val = field->ValReal();
            chunk->InsertData(count, col, reinterpret_cast<char *>(&val), len, false);
            break;
          }
        }
      }
    }
  }
  chunk->AddCount();
}

EEIteratorErrCode WindowHelper::NextBatch(kwdbContext_p ctx,
                                          ScanRowBatch *&rowbatch) {
  EEIteratorErrCode code = op_->InitScanRowBatch(ctx, &rowbatch);
  if (EEIteratorErrCode::EE_OK != code) {
    return code;
  }
  if (rowbatchs_.size() > 0) {
    rowbatch->Copy(rowbatchs_[rowbatchs_.size() - 1]);
  }
  code = op_->handler_->TsNextAndFilter(
      ctx, op_->filter_, &op_->cur_offset_, op_->limit_, rowbatch,
      &op_->total_read_row_, &op_->examined_rows_, NULL);
  return code;
}

EEIteratorErrCode WindowHelper::ReadRowBatch(kwdbContext_p ctx,
                                             ScanRowBatch *rowbatch,
                                             DataChunk *chunk,
                                             bool continue_last_rowbatch) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  k_int32 count = chunk->Count();
  if (first_span_) {
    current_entity_index_ = rowbatch->res_.entity_index;
  } else if (!current_entity_index_.equalsWithoutMem(
                 rowbatch->res_.entity_index)) {
    if (rowbatchs_.size() == 1) {
      sliding_status_ = SWS_NEXT_ENTITY;
      return code;
    }
    sliding_status_ = SWS_NEXT_WINDOW;
    return code;
  }

  thd->SetRowBatch(rowbatch);
  rowbatch->ResetLine();
  k_int32 start_row = 0;
  if (continue_last_rowbatch) {
    start_row = last_rowbatch_line_;
    rowbatch->SetCurrentLine(start_row);
  } else if (current_rowbatch_index_ == 0) {
    start_row = current_line_;
    rowbatch->SetCurrentLine(start_row);
  }
  for (k_int32 i = start_row; i < rowbatch->Count(); i++) {
    bool next_window = false;
    if (ProcessWindow(rowbatch, next_window)) {
      current_line_ = i + 1;
      rowbatch->NextLine();
      continue;
    }
    if (next_window) {
      sliding_status_ = SWS_NEXT_WINDOW;
      return code;
    }
    MaterializeRow(thd, chunk, count);
    count++;
    rowbatch->NextLine();
    if (chunk->Count() == chunk->Capacity()) {
      sliding_status_ = SWS_RT_CHUNK;
      last_rowbatch_line_ = i + 1;
      return code;
    }
  }
  if (current_rowbatch_index_ == 0 && current_line_ >= rowbatch->Count()) {
    current_line_ = ResetCurrentLine(current_line_, rowbatch->Count());
    PopBatch();
    current_rowbatch_index_ = -1;
  }
  sliding_status_ = SWS_NEXT_BATCH;
  return code;
}

EEIteratorErrCode WindowHelper::SlidingCase(kwdbContext_p ctx,
                                            DataChunkPtr &chunk) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  op_->constructDataChunk();
  chunk = std::move(op_->current_data_chunk_);
  DataChunk *pchunk = chunk.get();
  while (true) {
    switch (sliding_status_) {
      case SWS_RT_CHUNK: {
        sliding_status_ = SWS_READ_BATCH;
        continue_last_rowbatch_ = true;
        return EEIteratorErrCode::EE_OK;
      } break;
      case SWS_NEXT_WINDOW: {
        if (rowbatchs_.size() < 1) {
          if (no_more_data_) {
            if (pchunk->Count() > 0) {
              return EEIteratorErrCode::EE_OK;
            }
            return EEIteratorErrCode::EE_END_OF_RECORD;
          }
          ScanRowBatch *rowbatch = nullptr;
          code = NextBatch(ctx, rowbatch);
          if (code != EEIteratorErrCode::EE_OK) {
            return code;
          } else {
            PushBatch(rowbatch);
          }
        }
        if (!first_span_) {
          NextSlidingWindow();
        }
        current_rowbatch_index_ = 0;
        rowbatch_ = rowbatchs_[current_rowbatch_index_];
        sliding_status_ = SWS_READ_BATCH;
      } break;
      case SWS_READ_BATCH: {
        ReadRowBatch(ctx, rowbatch_, pchunk, continue_last_rowbatch_);
        continue_last_rowbatch_ = false;
      } break;
      case SWS_NEXT_BATCH: {
        if ((current_rowbatch_index_ + 1) >= rowbatchs_.size()) {
          if (no_more_data_) {
            sliding_status_ = SWS_NEXT_WINDOW;
            break;
          }
          ScanRowBatch *rowbatch = nullptr;
          code = NextBatch(ctx, rowbatch);
          if (code == EEIteratorErrCode::EE_ERROR) {
            return code;
          } else if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
            sliding_status_ = SWS_NEXT_WINDOW;
            no_more_data_ = true;
            if (current_rowbatch_index_ == -1) {
              return code;
            }
            break;
          } else {
            if (PushBatch(rowbatch) == FAIL) {
              return EEIteratorErrCode::EE_ERROR;
            }
          }
        }
        current_rowbatch_index_++;
        rowbatch_ = rowbatchs_[current_rowbatch_index_];
        sliding_status_ = SWS_READ_BATCH;
      } break;
      case SWS_NEXT_ENTITY: {
        first_span_ = true;
        NextEntity();
        sliding_status_ = SWS_NEXT_WINDOW;
      } break;
    }
  }
  return code;
}

}  // namespace kwdbts
