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

#include "ee_remote_merge_sort_inbound_op.h"

#include "br_data_stream_recvr.h"
#include "br_mgr.h"
#include "br_pass_through_chunk_buffer.h"
#include "ee_cancel_checker.h"
#include "ee_dml_exec.h"
#include "ee_exec_pool.h"
#include "ee_pipeline_task.h"

namespace kwdbts {

EEIteratorErrCode RemoteMergeSortInboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  if (EEIteratorErrCode::EE_OK != RemoteInboundOperator::Init(ctx)) {
    LOG_ERROR("RemoteMergeSortInboundOperator Init faild");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (KStatus::SUCCESS !=
      stream_recvr_->CreateMergerForPipeline(ctx, &order_column_ids_,
                                             &asc_order_, &null_first_)) {
    LOG_ERROR("CreateMergerForPipeline faild");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode RemoteMergeSortInboundOperator::Next(kwdbContext_p ctx,
                                                       DataChunkPtr& chunk) {
  EEIteratorErrCode code = TryPullChunk(ctx, chunk);

  // LOG_ERROR("RemoteMergeSortInboundOperator Success to Get DataChunk %p,
  // chunk row count : %d", chunk.get(), chunk->Count());
  // chunk->DebugPrintData();
  return code;
}
EEIteratorErrCode RemoteMergeSortInboundOperator::TryPullChunk(
    kwdbContext_p ctx, DataChunkPtr& chunk) {
  while (true) {
    {
      std::unique_lock l(lock_);
      if (is_stop_ || (CheckCancel(ctx) != SUCCESS)) {
        LOG_ERROR(
            "RemoteMergeSortInboundOperator pull be cancel or stop (queryid "
            "%ld) .",
            query_id_);
        return EEIteratorErrCode::EE_ERROR;
      }

      if (status_.status_code() > 0) {
        EEPgErrorInfo::SetPgErrorInfo(status_.status_code(),
                                      status_.error_msgs(0).c_str());
        LOG_ERROR(
            "RemoteMergeSortInboundOperator TryPullChunk status_code %d, msg "
            "%s",
            status_.status_code(), status_.error_msgs(0).c_str());
        return EEIteratorErrCode::EE_ERROR;
      }

      if (IsFinished()) {
        return EEIteratorErrCode::EE_END_OF_RECORD;
      }

      if (HasOutput()) {
      } else {
        not_fill_cv_.wait_for(l, std::chrono::seconds(2));
        continue;
      }
    }
    if (KStatus::SUCCESS != PullChunk(ctx, chunk)) {
      LOG_ERROR("RemoteMergeSortInboundOperator PullChunk  queryid %ld",
                query_id_);
      return EEIteratorErrCode::EE_ERROR;
    }

    if (chunk != nullptr) {
      total_rows_ += chunk->Count();
      return EEIteratorErrCode::EE_OK;
    }
    continue;
  }
  return EEIteratorErrCode::EE_OK;
}

KStatus RemoteMergeSortInboundOperator::PullChunk(kwdbContext_p ctx,
                                                  DataChunkPtr& chunk) {
  if (KStatus::SUCCESS != GetNextMerging(ctx, chunk)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus RemoteMergeSortInboundOperator::GetNextMerging(kwdbContext_p ctx,
                                                       DataChunkPtr& chunk) {
  // Todo : need to check finish
  if (IsFinished()) {
    return KStatus::SUCCESS;
  }
  k_bool should_exit = false;
  if (num_rows_input_ < offset_) {
    DataChunkPtr tmp_chunk = nullptr;
    while (is_finished_ && !should_exit && num_rows_input_ < offset_) {
      DataChunkPtr empty_chunk = nullptr;
      if (stream_recvr_->GetNextForPipeline(&empty_chunk, &is_finished_,
                                            &should_exit)) {
        LOG_ERROR("RemoteMergeSortInboundOperator to Get DataChunk, faild");
        return KStatus::FAIL;
      }

      if (empty_chunk) {
        num_rows_input_ += empty_chunk->Count();
        std::swap(empty_chunk, tmp_chunk);
      } else {
        break;
      }
    }

    if (num_rows_input_ > offset_) {
      int64_t rewind_size = num_rows_input_ - offset_;
      int64_t offset_in_chunk = tmp_chunk->Count() - rewind_size;
      if (limit_ > 0 && rewind_size > limit_) {
        rewind_size = limit_;
      }
      chunk = std::make_unique<DataChunk>(tmp_chunk->GetColumnInfo(),
                                          tmp_chunk->ColumnNum(), rewind_size);
      chunk->Append(tmp_chunk.get(), offset_in_chunk, rewind_size);
      num_rows_input_ = offset_;
      num_rows_returned_ += rewind_size;
      return KStatus::SUCCESS;
    }

    if (!tmp_chunk) {
      return KStatus::SUCCESS;
    }
  }

  if (!should_exit) {
    if (KStatus::SUCCESS != stream_recvr_->GetNextForPipeline(
                                &chunk, &is_finished_, &should_exit)) {
      LOG_ERROR("RemoteMergeSortInboundOperator to Get DataChunk, faild");
      return KStatus::FAIL;
    }
  }

  if (chunk != nullptr) {
    size_t size_in_chunk = chunk->Count();
    if (limit_ > 0 && size_in_chunk + num_rows_returned_ > limit_) {
      size_in_chunk -= (size_in_chunk + num_rows_returned_ - limit_);
      chunk->SetCount(size_in_chunk);
    }
    num_rows_returned_ += size_in_chunk;
  }
  return KStatus::SUCCESS;
}

EEIteratorErrCode RemoteMergeSortInboundOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  code = RemoteInboundOperator::Close(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }
  Return(EEIteratorErrCode::EE_OK);
}

k_bool RemoteMergeSortInboundOperator::HasOutput() {
  return stream_recvr_->IsDataReady();
}

k_bool RemoteMergeSortInboundOperator::NeedInput() {
  return stream_recvr_ && (stream_recvr_->GetTotalChunks(stream_id_) < 100);
}
k_bool RemoteMergeSortInboundOperator::IsFinished() { return is_finished_; }

}  // namespace kwdbts
