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
  line_chunk_indexer_.clear();
  cumulative_count_.clear();
  cumulative_count_.push_back(0);
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
    k_uint16 chunk_index = mem_chunk_ptrs_.size();
    cumulative_count_.push_back(cumulative_count_.back() + chunk->Count());
    for (k_uint32 i = 0; i < chunk->Count(); i++) {
      line_chunk_indexer_.emplace_back(chunk_index);
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

bool MemRowContainer::IsNull(k_uint32 row, k_uint32 col) {
  k_uint32 batch_index = 0;
  k_uint32 row_in_chunk = 0;
  GetRowInCacheChunks(row, &batch_index, &row_in_chunk);
  return mem_chunk_ptrs_[batch_index]->IsNull(row_in_chunk, col);
}

bool MemRowContainer::IsNull(k_uint32 col) {
    k_uint32 batch_index = 0;
  k_uint32 row_in_chunk = 0;
  k_int32 reqRow =
      selection_.empty() ? current_line_ : selection_[current_line_];
  GetRowInCacheChunks(reqRow, &batch_index, &row_in_chunk);
  return mem_chunk_ptrs_[batch_index]->IsNull(row_in_chunk, col);
}

DatumPtr MemRowContainer::GetData(k_uint32 row, k_uint32 col) {
    k_uint32 batch_index = 0;
  k_uint32 row_in_chunk = 0;
  GetRowInCacheChunks(row, &batch_index, &row_in_chunk);
  return mem_chunk_ptrs_[batch_index]->GetData(row_in_chunk, col);
}

DatumPtr MemRowContainer::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
    k_uint32 batch_index = 0;
  k_uint32 row_in_chunk = 0;
  GetRowInCacheChunks(row, &batch_index, &row_in_chunk);
  return mem_chunk_ptrs_[batch_index]->GetData(row_in_chunk, col, len);
}

DatumPtr MemRowContainer::GetData(k_uint32 col) {
  k_uint32 batch_index = 0;
  k_uint32 row_in_chunk = 0;
  k_int32 reqRow =
      selection_.empty() ? current_line_ : selection_[current_line_];
  GetRowInCacheChunks(reqRow, &batch_index, &row_in_chunk);
  return mem_chunk_ptrs_[batch_index]->GetData(row_in_chunk, col);
}

k_uint32 MemRowContainer::ComputeCapacity() {
  // Cautiousness: OptimalDiskDataSize * 8 < 7 * col_num
  // (capacity_ + 7)/8 * col_num + capacity_ * row_size_ <= OptimalDiskDataSize
  int capacity = (OptimalDiskDataSize * 8 - 7 * col_num_) /
                 (col_num_ + 8 * static_cast<int>(row_size_));
  if (capacity < 2) {
    return 1;
  } else {
    return static_cast<k_uint32>(capacity);
  }
}

void MemRowContainer::GetRowInCacheChunks(k_uint32 row, k_uint32* chunk_index,
                               k_uint32* row_in_chunk) {
  *chunk_index = line_chunk_indexer_[row];
  *row_in_chunk = row - cumulative_count_[*chunk_index];
  return;
}

EEIteratorErrCode MemRowContainer::NextChunk(DataChunkPtr& data_chunk) {
  if (disorder_) {
    if (count_ == 0) {
      return EEIteratorErrCode::EE_END_OF_RECORD;
    }
    data_chunk = std::move(mem_chunk_ptrs_.back());
    mem_chunk_ptrs_.pop_back();
    cumulative_count_.pop_back();
    count_ -= data_chunk->Count();
    return EEIteratorErrCode::EE_OK;
  }
  return EEIteratorErrCode::EE_ERROR;
}

void MemRowContainer::Sort() {
  // selection_ init
  selection_.resize(count_);
  for (int i = 0; i < count_; i++) {
    selection_[i] = i;
  }

  // iterator
  auto it_begin = selection_.begin();
  auto it_end = selection_.end();

  // sort
  OrderColumnCompare cmp(this, order_info_);
  if (count_ > 30000) {
    __gnu_parallel::sort(it_begin, it_end, cmp);

  } else {
    std::stable_sort(it_begin, it_end, cmp);
  }

  disorder_ = false;
}

}  // namespace kwdbts
