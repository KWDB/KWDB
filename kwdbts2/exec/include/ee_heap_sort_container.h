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
#include <list>
#include <vector>
#include <queue>
#include <algorithm>
#include <sstream>
#include <utility>

#include "kwdb_type.h"
#include "ee_data_chunk.h"
// #include "rocksdb/db.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

/**
 * @brief HeapSortContainer
 * @details A memory-heap-sorting container based on DataChunk
 */
class HeapSortContainer : public DataContainer, public std::enable_shared_from_this<HeapSortContainer> {
 public:
  HeapSortContainer(std::vector<ColumnOrderInfo>& order_info,
                    ColumnInfo* col_info, k_int32 col_num)
      : order_info_(order_info),
        col_info_(col_info),
        col_num_(col_num),
        compare_(this, order_info_),
        selection_heap_(compare_) {
    row_size_ = DataChunk::ComputeRowSize(col_info, col_num);
    capacity_ = ComputeCapacity();
  }

  HeapSortContainer(std::vector<ColumnOrderInfo>& order_info,
                    ColumnInfo* col_info, k_int32 col_num, k_uint32 capacity)
      : order_info_(order_info),
        col_info_(col_info),
        col_num_(col_num),
        compare_(this, order_info_),
        selection_heap_(compare_),
        capacity_(capacity) {
    row_size_ = DataChunk::ComputeRowSize(col_info, col_num);
  }

  ~HeapSortContainer() override;

  std::shared_ptr<HeapSortContainer> Ptr() {
    return shared_from_this();
  }
  KStatus Append(DataChunkPtr& chunk) override;

  KStatus Append(std::queue<DataChunkPtr>& buffer) override;

  bool IsNull(k_uint32 row, k_uint32 col) override;

  bool IsNull(k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len) override;

  DatumPtr GetData(k_uint32 col) override;

  ColumnInfo* GetColumnInfo() override { return col_info_; }

  k_uint32 Count() override { return count_; }

  k_int32 NextLine() override;

  KStatus Sort() override;

  KStatus Init() override;

  EEIteratorErrCode NextChunk(DataChunkPtr& chunk) override;

 private:
  k_uint32 ComputeCapacity();

  void Reset();

  KStatus UpdateWriteCacheChunk(bool& isUpdated);

  ColumnInfo* col_info_{nullptr};  // column info
  k_int32 col_num_{0};
  k_uint32 row_size_{0};
  k_uint32 capacity_{0};

  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;
  OrderColumnCompare compare_;
  std::priority_queue<k_uint32, std::vector<k_uint32>, OrderColumnCompare> selection_heap_;
  k_uint32 count_{0};  // total row number
  k_int32 current_line_{-1};  // current row
  DataChunk* input_chunk_ptr_;
  DataChunkPtr mem_chunk_ptr_;
  k_bool disorder_{true};
};

}   // namespace kwdbts
