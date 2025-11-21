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

#include "ee_scan_helper.h"

#include "ee_scan_op.h"
#include "ee_storage_handler.h"

namespace kwdbts {

EEIteratorErrCode ScanHelper::NextChunk(kwdbContext_p ctx,
                                        DataChunkPtr &chunk) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_END_OF_RECORD;
  do {
    if (op_->limit_ && op_->examined_rows_ >= op_->limit_) {
      op_->is_done_ = true;
    }
    if (op_->is_done_) {
      if (op_->current_data_chunk_ != nullptr &&
          op_->current_data_chunk_->Count() > 0) {
        chunk = std::move(op_->current_data_chunk_);
        code = EEIteratorErrCode::EE_OK;
      }
      break;
    }
    code = op_->InitScanRowBatch(ctx, &op_->row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    code = op_->handler_->TsNextAndFilter(
        ctx, op_->filter_, &op_->cur_offset_, op_->limit_, op_->row_batch_,
        &op_->total_read_row_, &op_->examined_rows_, &ts_scan_stats);
    if (code == EEIteratorErrCode::EE_ERROR) {
      break;
    }
    if (EEIteratorErrCode::EE_END_OF_RECORD == code) {
      if (op_->current_data_chunk_ != nullptr &&
          op_->current_data_chunk_->Count() > 0) {
        chunk = std::move(op_->current_data_chunk_);
        code = EEIteratorErrCode::EE_OK;
      }
      op_->is_done_ = true;
    } else {
      BeginMaterialize(ctx, op_->row_batch_);
      if (op_->row_batch_->Count() > 0) {
        if (op_->current_data_chunk_->Capacity() -
                op_->current_data_chunk_->Count() <
            op_->row_batch_->Count()) {
          // the current chunk have no enough space to save the whole data
          // results in row_batch_, push it into the output queue, create a new
          // one and ensure it has enough space.
          if (op_->current_data_chunk_->Count() > 0) {
            chunk = std::move(op_->current_data_chunk_);
          }
          k_uint32 capacity =
              DataChunk::EstimateCapacity(op_->output_col_info_, op_->output_col_num_);
          if (capacity >= op_->row_batch_->Count()) {
            op_->constructDataChunk();
          } else {
            op_->constructDataChunk(op_->row_batch_->Count());
          }
          if (op_->current_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                          "Insufficient memory");
            return EEIteratorErrCode::EE_ERROR;
          }
        }
        Materialize(ctx, op_->row_batch_, op_->current_data_chunk_);
      }
    }
  } while (!op_->is_done_ && !chunk);
  return code;
}

EEIteratorErrCode ScanHelper::Materialize(kwdbContext_p ctx,
                                          ScanRowBatch *rowbatch,
                                          DataChunkPtr &chunk) {
  chunk->AddRowBatchData(ctx, rowbatch, op_->renders_, op_->batch_copy_);
  return EEIteratorErrCode::EE_OK;
}

}  // namespace kwdbts
