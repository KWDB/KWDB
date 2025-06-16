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
#include "ee_pb_plan.pb.h"

namespace kwdbts {

/**
 * @brief MemRowContainer
 * @details A memory-sorting container based on DataChunk
 */
class MemRowContainer : public DataContainer, public std::enable_shared_from_this<MemRowContainer> {
 public:
  explicit MemRowContainer(ColumnInfo* col_info, k_int32 col_num)
      : col_info_(col_info), col_num_(col_num) {
    row_size_ = DataChunk::ComputeRowSize(col_info, col_num);
    capacity_ = ComputeCapacity();
  }

  MemRowContainer(std::vector<ColumnOrderInfo>& order_info,
                         ColumnInfo* col_info, k_int32 col_num)
      : order_info_(order_info),
        col_info_(col_info),
        col_num_(col_num) {
    row_size_ = DataChunk::ComputeRowSize(col_info, col_num);
    capacity_ = ComputeCapacity();
  }


  ~MemRowContainer() override;

  std::shared_ptr<MemRowContainer> Ptr() {
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

  void Sort() override;

  KStatus Init() override;

  EEIteratorErrCode NextChunk(DataChunkPtr& chunk) override;

 private:
  k_uint32 ComputeCapacity();

  void Reset();

  void GetRowInCacheChunks(k_uint32 row, k_uint32* chunk_index,
                               k_uint32* row_in_chunk);

  KStatus UpdateWriteCacheChunk(bool& isUpdated);

  ColumnInfo* col_info_{nullptr};  // column info
  k_int32 col_num_{0};
  k_uint32 row_size_{0};
  k_uint32 capacity_{0};

  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;
  k_int32 current_sel_idx_{-1};
  k_uint32 count_{0};  // total row number
  std::vector<k_uint16> line_chunk_indexer_;
  std::vector<k_uint32> cumulative_count_;
  k_int32 current_line_{-1};  // current row

  std::vector<DataChunkPtr> mem_chunk_ptrs_;
  k_bool disorder_{true};
};

}   // namespace kwdbts
