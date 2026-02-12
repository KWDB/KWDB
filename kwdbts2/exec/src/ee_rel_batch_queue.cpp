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
#include "ee_cancel_checker.h"

namespace kwdbts {

RelBatchQueue::RelBatchQueue() {}

RelBatchQueue::~RelBatchQueue() { SafeDeleteArray(output_col_info_); }


KStatus RelBatchQueue::Init(std::vector<Field*> &output_fields) {
  output_col_num_ = output_fields.size();
  output_col_info_ = KNEW ColumnInfo[output_col_num_];
  if (output_col_info_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    return KStatus::FAIL;
  }
  for (k_int32 i = 0; i < output_fields.size(); i++) {
    output_col_info_[i] = ColumnInfo(output_fields[i]->get_storage_length(),
                                       output_fields[i]->get_storage_type(),
                                       output_fields[i]->get_return_type());
  }
  is_init_ = true;
  cv.notify_one();
  return KStatus::SUCCESS;
}

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
  while (!is_init_) {
    // wait
    std::unique_lock<std::mutex> lk(mutex_);
    cv.wait_for(lk, std::chrono::milliseconds(10));
    if (is_error_) {
      Return(KStatus::FAIL);
    }
  }
  DataChunkPtr data_chunk = std::make_unique<kwdbts::DataChunk>(output_col_info_, output_col_num_, count);
  if (!data_chunk->Initialize()) {
    data_chunk = nullptr;
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  k_uint32 bitmap_size = (count + 7) / 8;;
  k_uint32 capacity = count;
  k_uint32 offset = 0;
  std::vector<k_uint32> bitmap_offset;
  std::vector<k_uint32> col_offset;
  for (k_int32 i = 0; i < output_col_num_; i++) {
    bitmap_offset.push_back(offset);
    col_offset.push_back(offset + bitmap_size);
    k_uint32 col_len = output_col_info_[i].fixed_storage_len;
    if (output_col_info_[i].is_string == KWStringType::VAR_LENGTH) {
      col_len = output_col_info_[i].storage_len + STRING_WIDE;
    }
    offset += col_len * capacity + bitmap_size;
  }
  char* dst_ptr = data_chunk->GetData();
  k_uint32* dst_bitmap_offset = data_chunk->GetBitmapOffset();
  k_uint32* dst_col_offset = data_chunk->GetColumnOffset();
  for (k_uint16 i = 0; i < output_col_num_; i++) {
    memcpy(dst_ptr + dst_bitmap_offset[i], batchData + bitmap_offset[i], bitmap_size);
    k_uint32 col_len = output_col_info_[i].fixed_storage_len;
    if (output_col_info_[i].is_string == KWStringType::VAR_LENGTH) {
      col_len = output_col_info_[i].storage_len + STRING_WIDE;
      for (k_uint32 j = 0; j < capacity; j++) {
        if (data_chunk->IsNull(j, i)) {
          continue;
        }
        char* ptr = batchData + col_offset[i] + j * col_len;
        k_uint32 len = *reinterpret_cast<k_uint16*>(ptr);
        data_chunk->InsertData(j, i, ptr + STRING_WIDE, len);
      }
    } else {
      memcpy(dst_ptr + dst_col_offset[i], batchData + col_offset[i], col_len * capacity);
    }
  }
  data_chunk->SetCount(count);
  std::unique_lock<std::mutex> lk(mutex_);
  data_queue_.push_back(std::move(data_chunk));
  cv.notify_one();
  Return(KStatus::SUCCESS);
}

EEIteratorErrCode RelBatchQueue::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  if (no_more_data_chunk && data_queue_.empty()) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  std::unique_lock<std::mutex> lk(mutex_);
  while (data_queue_.size() < 1) {
    cv.wait_for(lk, std::chrono::milliseconds(10),
                [this] { return data_queue_.size() > 0; });
    if (no_more_data_chunk && data_queue_.empty()) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD);
    }
    if (is_error_ || CheckCancel(ctx) != SUCCESS) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }
  chunk = std::move(data_queue_.front());
  data_queue_.pop_front();
  Return(code);
}

EEIteratorErrCode RelBatchQueue::Done(kwdbContext_p ctx) {
  EnterFunc();
  no_more_data_chunk = true;
  cv.notify_one();
  Return(EEIteratorErrCode::EE_OK);
}

}  // namespace kwdbts
