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
  explicit MemRowContainer(std::vector<ColumnOrderInfo> order_info,
                           ColumnInfo* col_info, k_int32 col_num,
                           k_uint32 capacity)
      : data_chunk_(col_info, col_num, capacity) {
    order_info_ = std::move(order_info);
  }

  ~MemRowContainer() override = default;

  std::shared_ptr<MemRowContainer> Ptr() {
    return shared_from_this();
  }

  bool IsNull(k_uint32 row, k_uint32 col) override {
    return data_chunk_.IsNull(row, col);
  };

  bool IsNull(k_uint32 col) override {
    return data_chunk_.IsNull(selection_[current_sel_idx_], col);
  };

  DatumPtr GetData(k_uint32 row, k_uint32 col) override {
    return data_chunk_.GetData(row, col);
  }

  DatumPtr GetData(k_uint32 col) override {
    return data_chunk_.GetData(selection_[current_sel_idx_], col);
  }

  DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len) override {
    return data_chunk_.GetData(row, col, len);
  }

  KStatus Append(DataChunk* chunk) override {
    return data_chunk_.Append(chunk);
  }

  KStatus Append(std::queue<DataChunkPtr>& buffer) override {
    return data_chunk_.Append(buffer);
  }

  k_uint32 Count() override {
    return selection_.size();
  }

  k_int32 NextLine() override;

  ColumnInfo* GetColumnInfo() override { return data_chunk_.GetColumnInfo(); }

  void Sort() override;

  KStatus Init() override;

 private:
  DataChunk data_chunk_;
  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;
  k_int32 current_sel_idx_{-1};
};

}   // namespace kwdbts
