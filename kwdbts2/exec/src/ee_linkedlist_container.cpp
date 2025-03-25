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
#include "ee_linkedlist_container.h"
#include "ee_batch_data_container.h"

namespace kwdbts {

LinkedListContainer::LinkedListContainer(int64_t data_size_threshold_in_memory)
                                      : data_size_threshold_in_memory_(data_size_threshold_in_memory) {
}

LinkedListContainer::~LinkedListContainer() {
  if (!data_in_memory_) {
    // clean up mmap file
    mmap_file_->remove();
    delete mmap_file_;
    mmap_file_ = nullptr;
  }
}

bool LinkedListContainer::IsMaterialized() {
  return (!data_in_memory_ && mmap_file_);
}

KStatus LinkedListContainer::AddLinkedList(LinkedListPtr linked_list) {
  KStatus ret = KStatus::SUCCESS;
  if (data_in_memory_) {
    if (current_data_size_ + linked_list->row_count_*LINKED_LIST_ENTRY_SIZE < data_size_threshold_in_memory_) {
      // continue to keep all the data chunks in memmory
      current_data_size_ += linked_list->row_count_*LINKED_LIST_ENTRY_SIZE;
      linked_lists_.push_back(std::move(linked_list));
      return ret;
    } else {
      // create a temporary mmap file to flush data chunks
      ret = InitMmapFile();
      if (ret == KStatus::FAIL) {
        return ret;
      }
      data_in_memory_ = false;
    }
  }
  // reserve enough space for new linked list
  if (Reserve(current_data_size_ + linked_list->row_count_*LINKED_LIST_ENTRY_SIZE) == KStatus::FAIL) {
    return KStatus::FAIL;
  }
  // copy data into mmap file
  memcpy(reinterpret_cast<char*>(mmap_file_->memAddr()) + current_data_size_,
         linked_list->row_indice_list_, linked_list->row_count_ * LINKED_LIST_ENTRY_SIZE);
  // reset the data chunk buffer to mmap file
  linked_list->ResetDataPtr(reinterpret_cast<RowIndice*>(mmap_file_->memAddr()) + current_data_size_);

  current_data_size_ += linked_list->row_count_*LINKED_LIST_ENTRY_SIZE;
  linked_lists_.push_back(std::move(linked_list));
  return ret;
}

KStatus LinkedListContainer::InitMmapFile() {
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

KStatus LinkedListContainer::Reserve(int64_t capacity) {
  if (capacity > mmap_capacity_) {
    int64_t new_capacity = mmap_capacity_ == 0 ? 2 * capacity : 2 * mmap_capacity_;
    if (mmap_file_->mremap(new_capacity) != 0) {
      return KStatus::FAIL;
    }
    // update data ptr with new mem_ in mmap_file_
    RowIndice* data_ptr = reinterpret_cast<RowIndice*>(mmap_file_->memAddr());
    for (int i = 0; i < linked_lists_.size(); ++i) {
      if (mmap_capacity_ == 0) {
        // copy data into mmap file
        memcpy(data_ptr, linked_lists_[i]->row_indice_list_, linked_lists_[i]->row_count_*LINKED_LIST_ENTRY_SIZE);
      }
      // reset the data chunk buffer to new ptr
      linked_lists_[i]->ResetDataPtr(data_ptr);
      data_ptr += linked_lists_[i]->row_count_*LINKED_LIST_ENTRY_SIZE;
    }
    mmap_capacity_ = new_capacity;
    data_in_memory_ = false;
  }
  return KStatus::SUCCESS;
}

LinkedListPtr& LinkedListContainer::GetLinkedList(uint16_t batch_no) {
  return linked_lists_[batch_no];
}

}  // namespace kwdbts
