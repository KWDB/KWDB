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

#include "ee_memory_data_container.h"

#include <parallel/algorithm>
#include <algorithm>
#include <utility>

#include "ee_common.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

constexpr k_int64 OptimalDiskDataSize = 131072;  // 128k

KStatus MemRowContainer::Init() {
  ErrorInfo err_info;
  AttributeInfo attr;

  // create tmp table
  k_int32 encoding = ROW_TABLE | NULLBITMAP_TABLE;

  if (err_info.errcode < 0) {
    LOG_ERROR("create temp table error : %d, %s", err_info.errcode,
              err_info.errmsg.c_str());
    return FAIL;
  }
  return SUCCESS;
}

MemRowContainer::~MemRowContainer() {
  mem_chunk_ptrs_.clear();
}
void MemRowContainer::Reset() {
  mem_chunk_ptrs_.clear();
}

KStatus MemRowContainer::Append(DataChunkPtr &chunk) {
    if (chunk->Count() == 0) {
      return SUCCESS;
    }
    count_ += chunk->Count();
    mem_chunk_ptrs_.push_back(std::move(chunk));
    return SUCCESS;
}

KStatus MemRowContainer::Append(std::queue<DataChunkPtr>& buffer) {
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

k_int32 MemRowContainer::NextLine() {
  if (current_sel_idx_ + 1 >= count_) {
    return -1;
  }

  ++current_sel_idx_;
  if (!selection_.empty()) {
    return selection_[current_sel_idx_];  // for sort
  } else {
    return current_sel_idx_;
  }
}

bool MemRowContainer::IsNull(k_uint32 row, k_uint32 col) {
    return sorted_chunk_ptr_->IsNull(row, col);
}

bool MemRowContainer::IsNull(k_uint32 col) {
    return sorted_chunk_ptr_->IsNull(selection_[current_sel_idx_], col);
}

DatumPtr MemRowContainer::GetData(k_uint32 row, k_uint32 col) {
    return sorted_chunk_ptr_->GetData(row, col);
}

DatumPtr MemRowContainer::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
    return sorted_chunk_ptr_->GetData(row, col, len);
}

DatumPtr MemRowContainer::GetData(k_uint32 col) {
    return sorted_chunk_ptr_->GetData(selection_[current_sel_idx_], col);
}

k_uint32 MemRowContainer::ComputeCapacity() {
  int capacity = (OptimalDiskDataSize * 8 - 7 * col_num_) /
                 (col_num_ + 8 * static_cast<int>(row_size_));
  if (capacity < 2) {
    return 1;
  } else {
    return static_cast<k_uint32>(capacity);
  }
}

EEIteratorErrCode MemRowContainer::NextChunk(DataChunkPtr& data_chunk) {
  if (disorder_) {
    if (count_ == 0) {
      return EEIteratorErrCode::EE_END_OF_RECORD;
    }
    data_chunk = std::move(mem_chunk_ptrs_.back());
    mem_chunk_ptrs_.pop_back();
    count_ -= data_chunk->Count();
    return EEIteratorErrCode::EE_OK;
  }
  return EEIteratorErrCode::EE_ERROR;
}

KStatus MemRowContainer::Sort() {
  // selection_ init
  sorted_chunk_ptr_ = std::make_unique<DataChunk>(col_info_, col_num_, count_);
  if (!sorted_chunk_ptr_->Initialize()) {
    return KStatus::FAIL;
  }
  for (auto& chunk : mem_chunk_ptrs_) {
    sorted_chunk_ptr_->Append(chunk.get());
  }
  selection_.resize(count_);
  for (int i = 0; i < count_; i++) {
    selection_[i] = i;
  }

  // iterator
  auto it_begin = selection_.begin();
  auto it_end = selection_.end();

  disorder_ = false;
  // sort
  OrderColumnCompare cmp(this, order_info_);
  std::sort(it_begin, it_end, cmp);
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
