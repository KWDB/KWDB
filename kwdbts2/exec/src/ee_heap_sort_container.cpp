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

#include "ee_heap_sort_container.h"

#include <parallel/algorithm>
#include <algorithm>
#include <utility>
#include <memory>

#include "ee_common.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

constexpr k_int64 OptimalDiskDataSize = 131072;  // 128k

#define INTPUT_CHUNK_SELECTION UINT32_MAX

KStatus HeapSortContainer::Init() {
  mem_chunk_ptr_ = std::make_unique<DataChunk>(col_info_, col_num_, capacity_);
  if (mem_chunk_ptr_ == nullptr) {
    return FAIL;
  }
  if (!mem_chunk_ptr_->Initialize()) {
    return FAIL;
  }
  return SUCCESS;
}

HeapSortContainer::~HeapSortContainer() { mem_chunk_ptr_ = nullptr; }
void HeapSortContainer::Reset() { mem_chunk_ptr_ = nullptr; }

/**
 * @brief Appends rows from a DataChunk to the HeapSortContainer.
 * 
 * This function attempts to append rows from the provided DataChunk to the internal memory chunk.
 * If the internal memory chunk has enough capacity, all rows from the DataChunk will be appended.
 * Otherwise, it fills the remaining capacity first and then selectively replaces rows in the memory chunk
 * using a heap-based selection algorithm based on the comparison function.
 * 
 * @param chunk A reference to a smart pointer pointing to the DataChunk from which rows will be appended.
 * @return KStatus Returns SUCCESS if the operation is successful, otherwise returns an appropriate error status.
 */
KStatus HeapSortContainer::Append(DataChunkPtr& chunk) {
  KStatus ret = SUCCESS;
  if (chunk->Count() == 0) {
    return SUCCESS;
  }
  disorder_ = true;
  k_uint32 resudual_count =
      mem_chunk_ptr_->Capacity() - mem_chunk_ptr_->Count();
  if (resudual_count >= chunk->Count()) {
    ret = mem_chunk_ptr_->Append(chunk.get());
    if (ret != SUCCESS) {
      return ret;
    }
    for (k_uint32 i = count_; i < mem_chunk_ptr_->Count(); i++) {
      selection_heap_.push(i);
    }
    count_ = mem_chunk_ptr_->Count();
    return ret;
  }
  chunk->ResetLine();
  k_int32 input_chunk_line = 0;
  if (resudual_count > 0) {
    ret = mem_chunk_ptr_->Append(chunk.get(), 0, resudual_count);
    for (k_uint32 i = 0; i <= resudual_count; i++) {
      input_chunk_line = chunk->NextLine();
    }
    for (k_uint32 i = count_; i < mem_chunk_ptr_->Count(); i++) {
      selection_heap_.push(i);
    }
    count_ = mem_chunk_ptr_->Count();
  } else {
    input_chunk_line = chunk->NextLine();
  }
  input_chunk_ptr_ = chunk.get();
  while (input_chunk_line != -1) {
    k_uint32 top_selection = selection_heap_.top();
    if (compare_(INTPUT_CHUNK_SELECTION, top_selection)) {
      mem_chunk_ptr_->ReplaceRow(chunk, top_selection, input_chunk_line);
      selection_heap_.pop();
      selection_heap_.push(top_selection);
    }
    input_chunk_line = chunk->NextLine();
  }
  return SUCCESS;
}

KStatus HeapSortContainer::Append(std::queue<DataChunkPtr>& buffer) {
  KStatus ret = SUCCESS;
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    ret = Append(buf);
    if (ret != SUCCESS) {
      return ret;
    }
    buffer.pop();
  }
  return ret;
}

k_int32 HeapSortContainer::NextLine() {
  if (current_line_ + 1 >= count_) {
    return -1;
  }

  ++current_line_;
  if (!selection_.empty()) {
    return selection_[current_line_];  // for sort
  } else {
    return current_line_;
  }
}

bool HeapSortContainer::IsNull(k_uint32 row, k_uint32 col) {
  if (row == INTPUT_CHUNK_SELECTION) {
    return input_chunk_ptr_->IsNull(col);
  }
  if (disorder_) {
    return mem_chunk_ptr_->IsNull(row, col);
  }
  return mem_chunk_ptr_->IsNull(selection_[row], col);
}

bool HeapSortContainer::IsNull(k_uint32 col) {
  k_int32 reqRow =
      selection_.empty() ? current_line_ : selection_[current_line_];
  return mem_chunk_ptr_->IsNull(reqRow, col);
}

DatumPtr HeapSortContainer::GetData(k_uint32 row, k_uint32 col) {
  if (row == INTPUT_CHUNK_SELECTION) {
    return input_chunk_ptr_->GetData(col);
  }
  if (disorder_) {
    return mem_chunk_ptr_->GetData(row, col);
  }
  return mem_chunk_ptr_->GetData(selection_[row], col);
}

DatumPtr HeapSortContainer::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  if (row == INTPUT_CHUNK_SELECTION) {
    return input_chunk_ptr_->GetVarData(col, len);
  }
  if (disorder_) {
    return mem_chunk_ptr_->GetData(row, col, len);
  }
  return mem_chunk_ptr_->GetData(selection_[row], col, len);
}

DatumPtr HeapSortContainer::GetData(k_uint32 col) {
  k_int32 reqRow =
      selection_.empty() ? current_line_ : selection_[current_line_];
  return mem_chunk_ptr_->GetData(reqRow, col);
}

k_uint32 HeapSortContainer::ComputeCapacity() {
  int capacity = (OptimalDiskDataSize * 8 - 7 * col_num_) /
                 (col_num_ + 8 * static_cast<int>(row_size_));
  if (capacity < 2) {
    return 1;
  } else {
    return static_cast<k_uint32>(capacity);
  }
}

EEIteratorErrCode HeapSortContainer::NextChunk(DataChunkPtr& data_chunk) {
  return EEIteratorErrCode::EE_ERROR;
}

KStatus HeapSortContainer::Sort() {
  selection_.resize(count_);
  for (k_int32 i = count_ - 1; i >= 0; i--) {
    selection_[i] = selection_heap_.top();
    selection_heap_.pop();
  }

  disorder_ = false;
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
