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

#include <math.h>
#include "utils/big_table_utils.h"
#include "ee_data_chunk_container.h"

namespace kwdbts {

DataChunkContainer::DataChunkContainer(k_int64 data_size_threshold_in_memory)
                                      : data_size_threshold_in_memory_(data_size_threshold_in_memory) {
}

DataChunkContainer::~DataChunkContainer() {
  if (!data_in_memory_) {
    // clean up mmap file
    mmap_file_->remove();
    delete mmap_file_;
    mmap_file_ = nullptr;
  }
}

bool DataChunkContainer::IsMaterialized() {
  return (!data_in_memory_ && mmap_file_);
}

KStatus DataChunkContainer::AddDataChunk(DataChunkPtr &data_chunk) {
  KStatus ret = KStatus::SUCCESS;
  if (data_in_memory_) {
    if (current_data_size_ + data_chunk->Capacity() < data_size_threshold_in_memory_) {
      // continue to keep all the data chunks in memmory
      current_data_size_ += data_chunk->Size();
      data_chunks_.push_back(std::move(data_chunk));
      return ret;
    } else {
      // create a temporary mmap file to flush data chunks
      ret = InitMmapFile();
      if (ret == KStatus::FAIL) {
        return ret;
      }
    }
  }
  // reserve enough space for new data chunk
  if (Reserve(current_data_size_ + data_chunk->Size()) == KStatus::FAIL) {
    return KStatus::FAIL;
  }
  // copy data into mmap file
  memcpy(reinterpret_cast<char*>(mmap_file_->memAddr()) + current_data_size_, data_chunk->GetData(),
         data_chunk->Size());
  // reset the data chunk buffer to mmap file
  data_chunk->ResetDataPtr(reinterpret_cast<char*>(mmap_file_->memAddr()) + current_data_size_);

  current_data_size_ += data_chunk->Size();
  data_chunks_.push_back(std::move(data_chunk));
  return ret;
}

KStatus DataChunkContainer::InitMmapFile() {
  int error_code = 0;
  mmap_file_ = new MMapFile();
  if (nullptr == mmap_file_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("mmap_file_ new failed\n");
    return KStatus::FAIL;
  }
  error_code = mmap_file_->openTemp();
  if (error_code < 0) {
    LOG_ERROR("failed to create temporary mmap file, error: %d", error_code);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus DataChunkContainer::Reserve(k_int64 capacity) {
  if (capacity > mmap_capacity_) {
    k_int64 new_capacity = mmap_capacity_ == 0 ? 2 * capacity : 2 * mmap_capacity_;
    int error_code = mmap_file_->mremap(new_capacity);
    if (error_code != 0) {
      LOG_ERROR("Failed to remap for %s, error: %d", mmap_file_->filePath().c_str(), error_code);
      return KStatus::FAIL;
    } else {
      LOG_INFO("Succeeded to remap for %s", mmap_file_->filePath().c_str());
    }
    // update data ptr with new mem_ in mmap_file_
    char* data_ptr = reinterpret_cast<char*>(mmap_file_->memAddr());
    for (int i = 0; i < data_chunks_.size(); ++i) {
      if (mmap_capacity_ == 0) {
        // copy data into mmap file
        memcpy(data_ptr, data_chunks_[i]->GetData(), data_chunks_[i]->Size());
      }
      // reset the data chunk buffer to new ptr
      data_chunks_[i]->ResetDataPtr(data_ptr);
      data_ptr += data_chunks_[i]->Size();
    }
    mmap_capacity_ = new_capacity;
    data_in_memory_ = false;
  }
  return KStatus::SUCCESS;
}

DataChunkPtr& DataChunkContainer::GetDataChunk(k_uint16 batch_no) {
  return data_chunks_[batch_no];
}

k_uint16 DataChunkContainer::GetBatchCount() {
  return data_chunks_.size();
}

}  // namespace kwdbts
