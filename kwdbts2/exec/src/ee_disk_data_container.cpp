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

#include "ee_disk_data_container.h"

#include <algorithm>
#include <vector>
#include <utility>
#include <memory>

#include "ee_common.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

constexpr k_uint64 OptimalDiskDataSize = 268435456;  // 256m

void LoserTree::Init(k_int32 size, std::vector<io_file_reader_t>& reader_ptrs,
                     ColumnCompare* compare) {
  size_ = size;
  compare_ = compare;
  tree_.resize(size_);
  data_.resize(size_);
  for (k_int32 i = 0; i < size_; ++i) {
    tree_[i] = -1;
    if (reader_ptrs[i].chunk_ptr_->NextLine() != -1) {
      data_[i] = reader_ptrs[i].chunk_ptr_->GetRowData();
    } else {
      data_[i] = nullptr;
    }
  }
  for (k_int32 i = size_ - 1; i >= 0; --i) {
    Adjust(i);
  }
}

void LoserTree::Adjust(k_int32 s) {
  k_int32 t = (tree_.size() + s) / 2;
  while (t > 0) {
    if (s == -1) {
      break;
    }
    if (data_[s] == nullptr ||
    tree_[t] == -1 ||
     (data_[tree_[t]] != nullptr && !(*compare_)(data_[s], data_[tree_[t]]))) {
      std::swap(s, tree_[t]);
    }
    t /= 2;
  }
  tree_[0] = s;
}

EEIteratorErrCode LoserTree::GetWinner(DatumPtr& data_ptr,
                                       k_int32& winner_index) {
  winner_index = tree_[0];
  if (winner_index == size_) {
    return EEIteratorErrCode::EE_ERROR;
  }
  data_ptr = data_[winner_index];
  if (data_ptr == nullptr) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  return EEIteratorErrCode::EE_OK;
}

void LoserTree::Input(DatumPtr data_ptr) {
  data_[tree_[0]] = data_ptr;
  Adjust(tree_[0]);
}

KStatus DiskDataContainer::Init() {
  if (write_cache_chunk_ptr_ == nullptr) {
    write_cache_chunk_ptr_ = make_unique<SortRowChunk>(
        col_info_, order_info_, col_num_, OptimalDiskDataSize, max_output_rows_,
        count_, false);
    if (!write_cache_chunk_ptr_->Initialize()) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return KStatus::FAIL;
    }
    all_constant_ = write_cache_chunk_ptr_->IsAllConstant();
    all_constant_in_order_col_ =
        write_cache_chunk_ptr_->IsAllConstantInOrderCol();
    col_offset_ = write_cache_chunk_ptr_->GetColOffset();
    if (all_constant_in_order_col_) {
      compare_ = KNEW AllConstantColumnCompare(this);
    } else {
      compare_ = KNEW HasNonConstantColumnCompare(this);
    }
  }
  return SUCCESS;
}

void DiskDataContainer::Reset() {
  for (k_int32 i = 0; i < cache_chunk_readers_.size(); ++i) {
    if (cache_chunk_readers_[i].chunk_ptr_ != nullptr) {
      cache_chunk_readers_[i].chunk_ptr_.reset();
    }
  }

  if (write_cache_chunk_ptr_ != nullptr) {
    write_cache_chunk_ptr_.reset();
  }
  write_cache_chunk_ptr_ = nullptr;
  col_offset_ = nullptr;
  SafeDeletePointer(compare_);
}

KStatus DiskDataContainer::Append(DataChunkPtr &chunk) {
  k_uint32 begin_row = 0;
  k_uint32 end_row = chunk->Count();
  while (begin_row < end_row) {
    KStatus ret = write_cache_chunk_ptr_->Append(chunk, begin_row,
                                                 begin_row + 1);
    if (ret != SUCCESS) {
      if (UpdateWriteCacheChunk() != SUCCESS) {
        return FAIL;
      }
    }
    count_ += 1;
  }

  return SUCCESS;
}

k_int32 DiskDataContainer::NextLine() {
  if (current_line_ + 1 >= count_) {
    current_line_ = -1;
    return -1;
  }
  if (output_chunk_ptr_ == nullptr || output_chunk_ptr_->NextLine() == -1) {
    auto code = NextChunk(output_chunk_ptr_);
    if (code != EEIteratorErrCode::EE_OK || output_chunk_ptr_->NextLine() == -1) {
      return -1;
    }
  }
  current_line_++;
  return current_line_;
}

EEIteratorErrCode DiskDataContainer::NextChunk(DataChunkPtr& chunk) {
  if (current_chunk_ + 1 >= read_merge_infos_->chunk_infos_.size()) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  if (read_merge_infos_->chunk_infos_.size() == 1) {
    cache_chunk_readers_[0].chunk_ptr_->DecodeData();
    chunk = std::move(cache_chunk_readers_[0].chunk_ptr_);
    current_chunk_++;
    return EEIteratorErrCode::EE_OK;
  }
  KStatus ret = UpdateTempCacheChunk();
  if (ret != SUCCESS) {
    return EEIteratorErrCode::EE_ERROR;
  }
  EEIteratorErrCode code = mergeMultipleLists(0u, &sorted_count_, true);
  if (code != EEIteratorErrCode::EE_OK) return code;
  write_cache_chunk_ptr_->DecodeData();
  output_chunk_start_row_index_ = write_merge_infos_->count_;
  write_merge_infos_->chunk_infos_.push_back(
      {write_cache_chunk_ptr_->Count(), write_merge_infos_->count_, 0,
       0});  //  useless offset and non constant size after last merge sort
  write_merge_infos_->batch_chunk_indexs_.back().push(write_merge_infos_->chunk_infos_.size() - 1);
  write_merge_infos_->count_ += write_cache_chunk_ptr_->Count();
  chunk = std::move(write_cache_chunk_ptr_);
  current_chunk_++;
  return EEIteratorErrCode::EE_OK;
}

k_uint32 DiskDataContainer::ComputeCapacity() {
  int capacity = OptimalDiskDataSize / static_cast<int>(row_size_);
  if (capacity < 2) {
    return 1;
  } else {
    return static_cast<k_uint32>(capacity);
  }
}

bool DiskDataContainer::IsNull(k_uint32 row, k_uint32 col) {
  if (row < output_chunk_start_row_index_ || row >= write_merge_infos_->count_) {
    return true;
  }
  return output_chunk_ptr_->IsNull(
      row - output_chunk_start_row_index_,
      col);
}

bool DiskDataContainer::IsNull(k_uint32 col) {
  return output_chunk_ptr_->IsNull(col);
}

DatumPtr DiskDataContainer::GetData(k_uint32 row, k_uint32 col) {
  if (row < output_chunk_start_row_index_ || row >= write_merge_infos_->count_) {
    return nullptr;
  }
  return output_chunk_ptr_->GetData(
      row - output_chunk_start_row_index_,
      col);
}

DatumPtr DiskDataContainer::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  if (row < output_chunk_start_row_index_ || row >= write_merge_infos_->count_) {
    return nullptr;
  }
  return output_chunk_ptr_->GetData(
      row - output_chunk_start_row_index_,
      col, len);
}

DatumPtr DiskDataContainer::GetData(k_uint32 col) {
  return output_chunk_ptr_->GetData(col);
}

KStatus DiskDataContainer::UpdateReadCacheChunk() {
  auto* reader = &cache_chunk_readers_[0];
  if (reader->chunk_ptr_->NextLine() != -1) {
    return SUCCESS;
  }

  reader->chunk_ptr_->Reset();

  reader->chunk_index_++;

  chunk_info_t *chunk_info = &(read_merge_infos_->chunk_infos_[reader->chunk_index_]);

  reader->chunk_ptr_->SetCount(chunk_info->count_);

  read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetData(), chunk_info->offset_,
                             reader->chunk_ptr_->Size());
  if (!all_constant_) {
    read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetNonConstantData(),
                               chunk_info->offset_ + reader->chunk_ptr_->Size(),
                               chunk_info->non_constant_data_size_);
    reader->chunk_ptr_->non_constant_save_mode_ = OFFSET_MODE;
        if (!all_constant_in_order_col_) {
        reader->chunk_ptr_->ChangeOffsetToPointer();
      }
  }

  return UpdateReadCacheChunk();
}

KStatus DiskDataContainer::UpdateWriteCacheChunk() {
  KStatus ret = KStatus::SUCCESS;

  if (write_cache_chunk_ptr_->isFull()) {
    auto ret = SortAndFlushLastChunk(false);
    if (ret != SUCCESS) {
      LOG_ERROR("SortAndFlushLastChunk failed");
      return ret;
    }
    write_cache_chunk_ptr_->Reset();
  }
  return SUCCESS;
}

KStatus DiskDataContainer::UpdateTempCacheChunk() {
  if (write_cache_chunk_ptr_ != nullptr) {
    write_cache_chunk_ptr_->Reset(write_force_constant_);
    write_cache_chunk_ptr_->SetMaxOutputCount(max_output_rows_);
  } else {
    write_cache_chunk_ptr_ = make_unique<SortRowChunk>(
        col_info_, order_info_, col_num_, OptimalDiskDataSize,
        max_output_rows_, 0, write_force_constant_);
    if (!write_cache_chunk_ptr_->Initialize()) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return KStatus::FAIL;
    }
  }

  return SUCCESS;
}

void DiskDataContainer::Sort() {
  KStatus ret = KStatus::SUCCESS;

  ret = SortAndFlushLastChunk(true);
  if (ret != SUCCESS) {
    LOG_ERROR("SortAndFlushLastChunk Failed : %d", ret);
    return;
  }
  write_force_constant_ = true;
  if (read_merge_infos_->batch_chunk_indexs_.size() > 1) {
    ret = divideAndConquerMerge();
    if (ret != SUCCESS) {
      LOG_ERROR("ConquerMerge Failed : %d", ret);
      return;
    }

    ReloadReadPtr(MAX_CHUNK_BATCH_NUM, 0);
    loser_tree_.Init(read_merge_infos_->batch_chunk_indexs_.size(), cache_chunk_readers_, compare_);
  }

  sorted_count_ = 0;
}

void DiskDataContainer::ReloadReadPtr(
    k_uint32 ptr_pool_size, k_uint32 start_batch_index) {
  current_read_pool_size_ = ptr_pool_size;
  for (k_uint32 i = 0; i < cache_chunk_readers_.size(); ++i) {
    if (cache_chunk_readers_[i].chunk_ptr_ != nullptr) {
      cache_chunk_readers_[i].chunk_ptr_->Reset(read_force_constant_);
    }
  }
  for (k_uint32 i = cache_chunk_readers_.size(); i < ptr_pool_size; ++i) {
    cache_chunk_readers_.push_back(io_file_reader_t());
    cache_chunk_readers_.back().chunk_ptr_ = std::make_unique<SortRowChunk>(
        col_info_, order_info_, col_num_, OptimalDiskDataSize,
        max_output_rows_, 0, read_force_constant_);
    if (!cache_chunk_readers_.back().chunk_ptr_->Initialize()) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
    }
  }

  for (k_uint32 i = start_batch_index;
       (i - start_batch_index) < ptr_pool_size &&
       i < read_merge_infos_->batch_chunk_indexs_.size();
       ++i) {
    auto chunk_index = read_merge_infos_->batch_chunk_indexs_[i].front();
    read_merge_infos_->batch_chunk_indexs_[i].pop();
    auto* reader = &cache_chunk_readers_[i % ptr_pool_size];


    reader->chunk_index_ = chunk_index;
    chunk_info_t *chunk_info = &(read_merge_infos_->chunk_infos_[chunk_index]);
    reader->chunk_ptr_->Reset(read_force_constant_);
    reader->chunk_ptr_->SetCount(chunk_info->count_);
    read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetData(), chunk_info->offset_,
                               reader->chunk_ptr_->Size());
    if (!all_constant_) {
      read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetNonConstantData(),
                                 chunk_info->offset_ + reader->chunk_ptr_->Size(),
                                 chunk_info->non_constant_data_size_);
      reader->chunk_ptr_->non_constant_save_mode_ = OFFSET_MODE;
      if (!all_constant_in_order_col_) {
        reader->chunk_ptr_->ChangeOffsetToPointer();
      }
    }
  }
  if (read_merge_infos_->batch_chunk_indexs_.size() > 0) {
    all_constant_ = cache_chunk_readers_[0].chunk_ptr_->IsAllConstant();
    all_constant_in_order_col_ =
        cache_chunk_readers_[0].chunk_ptr_->IsAllConstantInOrderCol();
        col_offset_ = cache_chunk_readers_[0].chunk_ptr_->GetColOffset();
    if (all_constant_in_order_col_) {
      compare_ = new AllConstantColumnCompare(this);
    } else {
      compare_ = new HasNonConstantColumnCompare(this);
    }
  }
}

KStatus DiskDataContainer::SortAndFlushLastChunk(k_bool force_merge) {
  KStatus ret = KStatus::SUCCESS;
  if (write_cache_chunk_ptr_ == nullptr ||
      write_cache_chunk_ptr_->Count() == 0) {
    return KStatus::SUCCESS;
  }

  auto chunk_index = read_merge_infos_->chunk_infos_.size();
  read_merge_infos_->chunk_infos_.push_back(
      {write_cache_chunk_ptr_->Count(), read_merge_infos_->count_, 0, 0});
  if (cache_chunk_readers_.size() <= (chunk_index % MAX_CHUNK_BATCH_NUM)) {
    cache_chunk_readers_.push_back(io_file_reader_t());
    auto& chunk_reader = cache_chunk_readers_.back();
    chunk_reader.chunk_ptr_ = make_unique<SortRowChunk>(
        col_info_, order_info_, col_num_, OptimalDiskDataSize, max_output_rows_,
        0, false);
    if (!chunk_reader.chunk_ptr_->Initialize()) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return KStatus::FAIL;
    }
    chunk_reader.chunk_ptr_->CopyWithSortFrom(write_cache_chunk_ptr_);
    chunk_reader.chunk_index_ = chunk_index;
  } else {
    auto chunk_reader =
        &cache_chunk_readers_[ chunk_index % MAX_CHUNK_BATCH_NUM];
    chunk_reader->chunk_ptr_->Reset(false);
    chunk_reader->chunk_ptr_->CopyWithSortFrom(write_cache_chunk_ptr_);
    chunk_reader->chunk_index_ = chunk_index;
  }
  read_merge_infos_->count_ += write_cache_chunk_ptr_->Count();
  read_merge_infos_->batch_chunk_indexs_.push_back({});

  if ((force_merge && chunk_index > 0)|| (chunk_index+1) % MAX_CHUNK_BATCH_NUM == 0) {
    k_int64 sorted_count = 0;
    auto batch_start_chunk_index = (chunk_index/MAX_CHUNK_BATCH_NUM)*MAX_CHUNK_BATCH_NUM;
    auto batch_chunk_num = chunk_index % MAX_CHUNK_BATCH_NUM + 1;
    write_merge_infos_->batch_chunk_indexs_.push_back({});
    KStatus ret = UpdateTempCacheChunk();
    if (ret != SUCCESS) {
      return ret;
    }
    loser_tree_.Init(batch_chunk_num, cache_chunk_readers_, compare_);
    for (;;) {
      EEIteratorErrCode code =
          mergeMultipleLists(batch_start_chunk_index, &sorted_count);
      if (code == EE_ERROR)
        return KStatus::FAIL;
      else if (code == EE_END_OF_RECORD)
        break;
      Write(write_cache_chunk_ptr_);
      // Compute max length for each variable length column
      if (!all_constant_) {
        for (k_int32 i = 0; i < col_num_; i++) {
          if (col_info_[i].is_string) {
            col_info_[i].max_string_len = std::max(
                col_info_[i].max_string_len,
                write_cache_chunk_ptr_->GetColumnInfo()[i].max_string_len);
          }
        }
      }
      ret = UpdateTempCacheChunk();
      if (ret != SUCCESS) {
        return ret;
      }
    }
  }

  return KStatus::SUCCESS;
}

EEIteratorErrCode DiskDataContainer::mergeMultipleLists(
    k_uint32 start_batch_index, k_int64* sorted_count, k_bool with_limit_offset) {
  DatumPtr data_ptr = nullptr;
  k_int32 data_index = -1;
  while (loser_tree_.GetWinner(data_ptr, data_index) ==
             EEIteratorErrCode::EE_OK &&
         *sorted_count < max_output_rows_) {
    k_uint32 batch_index = start_batch_index + data_index;
    if (!with_limit_offset || *sorted_count >= offset_) {
      KStatus ret = write_cache_chunk_ptr_->Append(
          cache_chunk_readers_[data_index].chunk_ptr_, data_ptr);
      if (ret != SUCCESS) {
        return EEIteratorErrCode::EE_OK;
      }
    }
    (*sorted_count)++;


    if (cache_chunk_readers_[data_index].chunk_ptr_->NextLine() == -1) {
      auto res = ReadNextChunk(batch_index);
      if (res != EE_OK) {
        if (res == EE_END_OF_RECORD) {
          loser_tree_.Input(nullptr);
          continue;
        } else {
          return EEIteratorErrCode::EE_ERROR;
        }
      }
      if (cache_chunk_readers_[data_index].chunk_ptr_->NextLine() == -1) {
        return EEIteratorErrCode::EE_ERROR;
      }
    }
    loser_tree_.Input(
        cache_chunk_readers_[data_index].chunk_ptr_->GetRowData());
  }
  if (write_cache_chunk_ptr_->Count() > 0) {
    return EEIteratorErrCode::EE_OK;
  }
  return EEIteratorErrCode::EE_END_OF_RECORD;
}

KStatus DiskDataContainer::divideAndConquerMerge() {
  ReverseFile();
  if (read_merge_infos_->batch_chunk_indexs_.size() <= (MAX_CHUNK_BATCH_NUM + 1)) {
    return KStatus::SUCCESS;
  }
  k_uint32 batch_num = MAX_CHUNK_BATCH_NUM;
  for (size_t i = 0; i < read_merge_infos_->batch_chunk_indexs_.size(); i += batch_num) {
    k_int64 sorted_count = 0;
    ReloadReadPtr(MAX_CHUNK_BATCH_NUM, i);
    write_merge_infos_->batch_chunk_indexs_.push_back({});

    KStatus ret = UpdateTempCacheChunk();
    if (ret != SUCCESS) {
      return ret;
    }
    loser_tree_.Init(batch_num, cache_chunk_readers_, compare_);
    for (;;) {
      EEIteratorErrCode code =
          mergeMultipleLists(i, &sorted_count);
      if (code == EEIteratorErrCode::EE_ERROR)
        return KStatus::FAIL;
      else if (code == EE_END_OF_RECORD)
        break;
      Write(write_cache_chunk_ptr_);
      ret = UpdateTempCacheChunk();
      if (ret != SUCCESS) {
        return ret;
      }
    }
  }

  read_force_constant_ = true;
  return divideAndConquerMerge();
}

void DiskDataContainer::ReverseFile() {
  if (write_merge_infos_ != &merge_info_1_) {
    write_merge_infos_ = &merge_info_1_;
    read_merge_infos_ = &merge_info_2_;
  } else {
    write_merge_infos_ = &merge_info_2_;
    read_merge_infos_ = &merge_info_1_;
  }
  write_merge_infos_->io_cache_handler_.Reset();
  write_merge_infos_->batch_chunk_indexs_.clear();
  write_merge_infos_->chunk_infos_.clear();
  write_merge_infos_->count_ = 0;
}

KStatus DiskDataContainer::Write(SortRowChunkPtr& chunk_ptr) {
  if (chunk_ptr->Count() == 0) {
    return KStatus::SUCCESS;
  }
  chunk_ptr->ChangePointerToOffset();
  k_uint64 offset = write_merge_infos_->io_cache_handler_.Size();
  KStatus ret =
      write_merge_infos_->io_cache_handler_.Write(chunk_ptr->GetData(), chunk_ptr->Size());

  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Write io cache failed");
    return KStatus::FAIL;
  }
  if (!all_constant_) {
    ret = write_merge_infos_->io_cache_handler_.Write(chunk_ptr->GetNonConstantData(),
                                       chunk_ptr->GetNonConstantDataSize());

    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Write io cache failed");
      return KStatus::FAIL;
    }
  }
  write_merge_infos_->chunk_infos_.push_back(
    {chunk_ptr->Count(), write_merge_infos_->count_, offset,
      chunk_ptr->GetNonConstantDataSize()});
  write_merge_infos_->batch_chunk_indexs_.back().push(write_merge_infos_->chunk_infos_.size() - 1);
  write_merge_infos_->count_ += chunk_ptr->Count();
  return KStatus::SUCCESS;
}
EEIteratorErrCode DiskDataContainer::ReadNextChunk(k_uint32 batch_index) {
  if (batch_index >= read_merge_infos_->batch_chunk_indexs_.size()) {
    return EEIteratorErrCode::EE_ERROR;
  }
  if (read_merge_infos_->batch_chunk_indexs_[batch_index].empty()) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  auto chunk_index = read_merge_infos_->batch_chunk_indexs_[batch_index].front();
  read_merge_infos_->batch_chunk_indexs_[batch_index].pop();
  auto* reader = &cache_chunk_readers_[batch_index % current_read_pool_size_];

  reader->chunk_index_ = chunk_index;
  chunk_info_t *chunk_info = &(read_merge_infos_->chunk_infos_[chunk_index]);
  reader->chunk_ptr_->Reset(read_force_constant_);
  reader->chunk_ptr_->SetCount(chunk_info->count_);
  read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetData(), chunk_info->offset_,
                              reader->chunk_ptr_->Size());
  if (!all_constant_) {
    read_merge_infos_->io_cache_handler_.Read(reader->chunk_ptr_->GetNonConstantData(),
                                chunk_info->offset_ + reader->chunk_ptr_->Size(),
                                chunk_info->non_constant_data_size_);
    reader->chunk_ptr_->non_constant_save_mode_ = OFFSET_MODE;
      if (!all_constant_in_order_col_) {
        reader->chunk_ptr_->ChangeOffsetToPointer();
      }
  }
  return EEIteratorErrCode::EE_OK;
}
}  // namespace kwdbts
