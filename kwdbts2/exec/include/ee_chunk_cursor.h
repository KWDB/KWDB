// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.
#pragma once

#include <functional>
#include <utility>
#include <vector>

#include "ee_data_container.h"
#include "kwdb_type.h"

namespace kwdbts {

using DataChunkProvider = std::function<k_bool(DataChunkPtr*, k_bool*)>;
class SimpleChunkSortCursor {
 public:
  SimpleChunkSortCursor() = delete;
  SimpleChunkSortCursor(const SimpleChunkSortCursor& rhs) = delete;
  SimpleChunkSortCursor(DataChunkProvider chunk_provider,
                        const std::vector<k_uint32>* order_column);
  ~SimpleChunkSortCursor() = default;

  k_bool IsDataReady();
  std::pair<DataChunkPtr, std::vector<k_uint32>> TryGetNextChunk();
  k_bool IsEos();

  const std::vector<k_uint32>* GetSortColumns() const { return order_column_; }

 private:
  k_bool data_ready_ = false;
  k_bool eos_ = false;

  DataChunkProvider chunk_provider_;
  const std::vector<k_uint32>* order_column_;
};

}  // namespace kwdbts
