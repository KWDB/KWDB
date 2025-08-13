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

#include "ee_chunk_cursor.h"

#include <utility>

#include "ee_data_chunk.h"
#include "ee_sort_chunk_merger.h"

namespace kwdbts {

SortedChunkCursor::SortedChunkCursor(DataChunkProvider chunk_provider,
                                             const std::vector<k_uint32>* sorted_column)
    : chunk_provider_(std::move(chunk_provider)), sorted_column_(sorted_column) {}

std::pair<DataChunkPtr, std::vector<k_uint32>> SortedChunkCursor::FetchNextSortedChunk() {
  if (is_end_) {
    return {nullptr, std::vector<k_uint32>{}};
  }
  DataChunkPtr next_chunk = nullptr;
  const bool fetch_success = chunk_provider_(&next_chunk, &is_end_);
  if (!fetch_success || !next_chunk || (0 == next_chunk->Count())) {
    return {nullptr, std::vector<k_uint32>{}};
  }
  return {std::move(next_chunk), *sorted_column_};
}

k_bool SortedChunkCursor::IsAtEnd() {
  return is_end_;
}

k_bool SortedChunkCursor::IsDataReady() {
  if (!sorted_data_ready_ && !chunk_provider_(nullptr, nullptr)) {
    return false;
  }
  sorted_data_ready_ = true;
  return true;
}

}  // namespace kwdbts
