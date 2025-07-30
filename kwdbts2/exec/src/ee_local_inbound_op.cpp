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

#include "ee_local_inbound_op.h"
#include "lg_api.h"

namespace kwdbts {

#define MAX_QUEUE_SIZE 128

EEIteratorErrCode LocalInboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_uint32 sz = childrens_.size();
  for (k_uint32 i = 0; i < sz; i++) {
    code = childrens_[i]->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("childrens[%d] init failed.", i);
      Return(code);
    }
  }

  code = InboundOperator::Init(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode LocalInboundOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  PullChunk(chunk);
  if (pg_info_.code > 0) {
    LOG_ERROR("LocalInboundOperator next error code is %d, error msg is %s", pg_info_.code, pg_info_.msg);
    EEPgErrorInfo::SetPgErrorInfo(pg_info_.code, pg_info_.msg);
    Return(EEIteratorErrCode::EE_ERROR);
  }
  if (!chunk) {
    code = EEIteratorErrCode::EE_END_OF_RECORD;
    if (local_inbound_code_ != EEIteratorErrCode::EE_OK) {
      code = local_inbound_code_;
    }
    Return(code);
  }

  Return(code);
}

void LocalInboundOperator::PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                                      const EEPgErrorInfo& pgInfo) {
  std::lock_guard<std::mutex> l(chunk_lock_);
  is_finished_ = 1;
  if (code != EEIteratorErrCode::EE_OK &&
      code != EEIteratorErrCode::EE_END_OF_RECORD &&
      code != EEIteratorErrCode::EE_TIMESLICE_OUT) {
    local_inbound_code_ = code;
  }
  if (pgInfo.code > 0) {
    pg_info_ = pgInfo;
  }

  // PushTaskToQueue();
  // notify idle thread
  wait_cond_.notify_one();
}

KStatus LocalInboundOperator::PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                                        EEIteratorErrCode code) {
  // lock
  std::lock_guard<std::mutex> l(chunk_lock_);
  if (chunks_.size() >= MAX_QUEUE_SIZE) {
    return KStatus::FAIL;
  }
  // chunk push to queue。
  chunks_.push_back(std::move(chunk));
  // notify idle thread
  wait_cond_.notify_one();
  return KStatus::SUCCESS;
}

KStatus LocalInboundOperator::PullChunk(DataChunkPtr& chunk) {
  // lock
  std::unique_lock l(chunk_lock_);
  while (true) {
    // data_queue_ is empty，wait
    if (chunks_.empty()) {
      // all children finish break
      if (is_finished_) {
        break;
      }
      // wait for data
      wait_cond_.wait_for(l, std::chrono::seconds(2));
      if (local_inbound_code_ != EEIteratorErrCode::EE_OK) {
        chunk = nullptr;
        break;
      }
      continue;
    }
    // get task
    chunk = std::move(chunks_.front());
    total_rows_ += chunk->Count();
    chunks_.pop_front();
    break;
  }

  return KStatus::SUCCESS;
}

k_bool LocalInboundOperator::HasOutput() {
  std::unique_lock l(chunk_lock_);

  if (chunks_.empty()) {
    if (is_finished_) {
      return true;
    } else {
      return false;
    }
  }

  return true;
}

}  // namespace kwdbts
