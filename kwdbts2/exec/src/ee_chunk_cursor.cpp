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



#include <utility>

#include "ee_data_chunk.h"
#include "ee_chunk_cursor.h"
#include "ee_sort_chunk_merger.h"

namespace kwdbts {

SimpleChunkSortCursor::SimpleChunkSortCursor(DataChunkProvider chunk_provider, const std::vector<k_uint32>* order_column)
        : chunk_provider_(std::move(chunk_provider)), order_column_(order_column) {}

k_bool SimpleChunkSortCursor::IsDataReady() {
    if (!data_ready_ && !chunk_provider_(nullptr, nullptr)) {
        return false;
    }
    data_ready_ = true;
    return true;
}

std::pair<DataChunkPtr, std::vector<k_uint32>> SimpleChunkSortCursor::TryGetNextChunk() {
    if (eos_) {
        return {nullptr, {}};
    }
    DataChunkPtr chunk = nullptr;
    if (!chunk_provider_(&chunk, &eos_) || !chunk) {
        return {nullptr, {}};
    }
    if (!chunk || (0 == chunk->Count())) {
        return {nullptr, {}};
    }
    return {std::move(chunk), *order_column_};
}

k_bool SimpleChunkSortCursor::IsEos() {
    return eos_;
}

}  // namespace kwdbts
