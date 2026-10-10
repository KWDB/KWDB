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
#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_data_chunk.h"

namespace kwdbts {

class BatchDataLinkedList;
// data size threshold to be kept in memory, otherwise the data will be flush to disk
#define DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY     256 * 1024 * 1024       // 256M
#define LINKED_LIST_ENTRY_SIZE sizeof(RowIndice)  // length for each linked list entry
typedef std::unique_ptr<BatchDataLinkedList> LinkedListPtr;
// linkedlist container to keep linkedlist in memory or flush them into disk if necessary
class LinkedListContainer {
 public:
  explicit LinkedListContainer(int64_t data_size_threshold_in_memory = DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY);
  ~LinkedListContainer();
  // add a linkedlist into the container
  KStatus AddLinkedList(LinkedListPtr rel_linked_list);
  // get the linkedlist with batch number
  LinkedListPtr& GetLinkedList(uint16_t batch_no);
  // get the number of data chunks
  // uint16_t GetBatchCount();
  // check if the linkedlist are materiliazed into mmap file for unit test
  bool IsMaterialized();

 private:
  // initialize mmap file for flushing data chunks
  KStatus InitMmapFile();
  // reserve enough space to flush data chuanks
  KStatus Reserve(int64_t capacity);
  // all the data chuangs kept in the container
  std::vector<LinkedListPtr> linked_lists_;
  // data is in memory if true
  bool data_in_memory_{true};
  // data chunk will be kept in memory if total data size is less than this threshold
  int64_t data_size_threshold_in_memory_;
  // current total data size in data chunks
  int64_t current_data_size_{0};
  // mmap file length
  int64_t mmap_capacity_{0};
  // mmap file to store data chunk data
  MMapFile* mmap_file_{nullptr};
};

};  // namespace kwdbts
