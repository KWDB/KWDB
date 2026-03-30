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
#include "ee_hash_table_tuple_data.h"

#include "ee_mempool.h"

namespace kwdbts {

// =================== MemoryTupleData Implementation ===================

MemoryTupleData::MemoryTupleData(k_uint32 tuple_size, k_uint32 capacity, k_bool allow_abandoned)
    : tuple_size_(tuple_size), count_(0), capacity_(capacity), allow_abandoned_(allow_abandoned) {
  tuple_data_ = EE_MemPoolMalloc(g_pstBufferPoolInfo, tuple_size_ * capacity_);
  memset(tuple_data_, 0, tuple_size_ * capacity_);
  abandoned_tuple_ = EE_MemPoolMalloc(g_pstBufferPoolInfo, tuple_size_);
  memset(abandoned_tuple_, 0, tuple_size_);
}

MemoryTupleData::~MemoryTupleData() {
  EE_MemPoolFree(g_pstBufferPoolInfo, tuple_data_);
  EE_MemPoolFree(g_pstBufferPoolInfo, abandoned_tuple_);
}

PTDFeedBack MemoryTupleData::GetNextTuplePtr(size_t hash_val, DatumPtr& tuple_data) {
  if (count_ < capacity_) {
    tuple_data = tuple_data_ + count_ * tuple_size_;
    count_++;
    return PTDFeedBack::DO_NOTHING;
  }

  if (allow_abandoned_ && capacity_ * tuple_size_ > MAX_MEMORY_SIZE) {
    tuple_data = abandoned_tuple_;
    memset(abandoned_tuple_, 0, tuple_size_);
    return PTDFeedBack::ABANDONED;
  }

  if (Resize(capacity_ * 2) == KStatus::SUCCESS) {
    return PTDFeedBack::REHASH;
  }

  return PTDFeedBack::FAIL;
}

DatumPtr MemoryTupleData::GetTupleData(k_uint64 loc) const {
  if (loc >= count_) {
    return nullptr;
  }
  return tuple_data_ + loc * tuple_size_;
}

KStatus MemoryTupleData::Resize(k_uint64 new_capacity) {
  if (new_capacity <= capacity_) {
    return KStatus::SUCCESS;
  }

  DatumPtr new_tuple_data =
      EE_MemPoolMalloc(g_pstBufferPoolInfo, tuple_size_ * new_capacity);

  if (new_tuple_data == nullptr) {
    return KStatus::FAIL;
  }

  // Copy existing data and initialize new space
  memcpy(new_tuple_data, tuple_data_, tuple_size_ * count_);
  memset(new_tuple_data + tuple_size_ * count_, 0,
         tuple_size_ * (new_capacity - count_));

  // Free old memory and update pointer
  EE_MemPoolFree(g_pstBufferPoolInfo, tuple_data_);
  tuple_data_ = new_tuple_data;
  capacity_ = new_capacity;

  return KStatus::SUCCESS;
}

KStatus MemoryTupleData::Reset() {
  count_ = 0;
  current_line_ = -1;
  memset(tuple_data_, 0, tuple_size_ * capacity_);
  memset(abandoned_tuple_, 0, tuple_size_);
  return KStatus::SUCCESS;
}

void MemoryTupleData::FinishRepartition() {
  // In MemoryTupleData, we just free the current memory
  // The caller should ensure that this object is not used after this call
  EE_MemPoolFree(g_pstBufferPoolInfo, tuple_data_);
  tuple_data_ = nullptr;
  count_ = 0;
  capacity_ = 0;
}

EEIteratorErrCode MemoryTupleData::NextTuple(DatumPtr& tuple_data) {
  if (current_line_ == count_ - 1) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  current_line_++;
  tuple_data = GetTupleData(current_line_);
  return EEIteratorErrCode::EE_OK;
}

DatumPtr MemoryTupleData::CurrentTuple() const {
  return GetTupleData(current_line_);
}

}  // namespace kwdbts
