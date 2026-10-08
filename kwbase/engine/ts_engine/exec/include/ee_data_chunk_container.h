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

// data size threshold to be kept in memory, otherwise the data will be flush to disk
#define DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY     256 * 1024 * 1024       // 256M

// data chunk container to keep relational data in memory or flush them into disk if necessary
class DataChunkContainer {
 public:
  explicit DataChunkContainer(k_int64 data_size_threshold_in_memory = DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY);
  ~DataChunkContainer();
  // add a data chunk into the container
  KStatus AddDataChunk(DataChunkPtr &rel_data_chunk);
  // get the data chunk with batch number
  DataChunkPtr& GetDataChunk(k_uint16 batch_no);
  // get the number of data chunks
  k_uint16 GetBatchCount();
  // check if the data chunks are materiliazed into mmap file for unit test
  bool IsMaterialized();

 private:
  // initialize mmap file for flushing data chunks
  KStatus InitMmapFile();
  // reserve enough space to flush data chuanks
  KStatus Reserve(k_int64 capacity);
  // all the data chuangs kept in the container
  std::vector<DataChunkPtr> data_chunks_;
  // data is in memory if true
  bool data_in_memory_{true};
  // data chunk will be kept in memory if total data size is less than this threshold
  k_int64 data_size_threshold_in_memory_;
  // current total data size in data chunks
  k_int64 current_data_size_{0};
  // mmap file length
  k_int64 mmap_capacity_{0};
  // mmap file to store data chunk data
  MMapFile* mmap_file_{nullptr};
};

};  // namespace kwdbts
