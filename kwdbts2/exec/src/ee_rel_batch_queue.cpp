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
#include "ee_rel_batch_queue.h"
#include "ee_common.h"

namespace kwdbts {

RelBatchQueue::RelBatchQueue(std::vector<Field*> &output_fields) {
  output_col_info_.reserve(output_fields.size());

  for (auto field : output_fields) {
    output_col_info_.emplace_back(field->get_storage_length(), field->get_storage_type(), field->get_return_type());
  }
}

RelBatchQueue::~RelBatchQueue() {}

KStatus RelBatchQueue::Add(kwdbContext_p ctx, char *batchData, k_uint32 count) {
  EnterFunc();
  if (count == 0) {
    Done(ctx);
    Return(KStatus::SUCCESS);
  }
  if (batchData == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NULL_VALUE_NOT_ALLOWED, "Relational batch data pointer should not be null");
    Return(KStatus::FAIL);
  }
  DataChunkPtr data_chunk = std::make_unique<kwdbts::DataChunk>(output_col_info_, count);
  if (data_chunk->Initialize() < 0) {
      data_chunk = nullptr;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(KStatus::FAIL);
  }
  if (data_chunk->PutData(ctx, batchData, count) != KStatus::SUCCESS) {
    Return(KStatus::FAIL);
  }
  data_queue_.push_back(std::move(data_chunk));
  Return(KStatus::SUCCESS);
}

EEIteratorErrCode RelBatchQueue::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  if (no_more_data_chunk && data_queue_.empty()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  std::unique_lock<std::mutex> lk(mutex_);
  if (cv.wait_for(lk, std::chrono::milliseconds(10), [this]{ return data_queue_.size() > 0; })) {
    chunk = std::move(data_queue_.front());
    data_queue_.pop_front();
  } else {
    code = EEIteratorErrCode::EE_TIMESLICE_OUT;
  }
  Return(code);
}

EEIteratorErrCode RelBatchQueue::Done(kwdbContext_p ctx) {
  EnterFunc();
  no_more_data_chunk = true;
  Return(EEIteratorErrCode::EE_OK);
}

}  // namespace kwdbts