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
#include <atomic>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "ee_chunk_cursor.h"
#include "ee_data_chunk.h"
#include "ee_data_container.h"
#include "ee_sort_desc.h"
#include "kwdb_type.h"

namespace kwdbts {
// SortedRun represents part of sorted chunk, specified by the range
// The chunk is sorted based on `orderby` columns
struct PermutationItem {
  k_uint32 chunk_index;
  k_uint32 index_in_chunk;

  PermutationItem() = default;
  PermutationItem(k_uint32 ci, k_uint32 ii)
      : chunk_index(ci), index_in_chunk(ii) {}
};
using Permutation = std::vector<PermutationItem>;

struct SortedRun {
  DataChunkSPtr chunk;
  std::vector<k_uint32> orderby;
  std::pair<k_uint32, k_uint32> range;

  SortedRun() = default;
  ~SortedRun() = default;
  SortedRun(const SortedRun& rhs) = default;
  SortedRun(SortedRun&& rhs) = default;
  SortedRun& operator=(const SortedRun& rhs) = default;

  SortedRun(const DataChunkSPtr& chunk, const std::vector<k_uint32>* columns)
      : chunk(chunk), range(0, chunk->Count()) {
    for (auto column : *columns) {
      orderby.push_back(column);
    }
  }

  SortedRun(const SortedRun& rhs, size_t start, size_t end)
      : chunk(rhs.chunk), orderby(rhs.orderby), range(start, end) {
  }

  void SetRange(size_t start, size_t end) {
    range.first = start;
    range.second = end;
  }
  void ResetRange() {
    range.first = 0;
    range.second = chunk->Count();
  }
  size_t ColsNum() const { return orderby.size(); }
  size_t StartIndex() const { return range.first; }
  size_t EndIndex() const { return range.second; }
  size_t Count() const {
    return range.second - range.first;
  }
  k_uint32 GetColumnIdx(int index) const { return orderby[index]; }
  k_bool Empty() const {
    return range.second == range.first || chunk == nullptr;
  }
  void Reset();
  int Intersect(const SortDescs& sort_desc, const SortedRun& right_run) const;

  DataChunkPtr CloneSlice() const;

  int CompareRow(const SortDescs& desc, const SortedRun& rhs, size_t lhs_row,
                 size_t rhs_row) const;

  // k_bool IsSorted(const SortDescs& desc) const;
};

struct SortedRuns {
  std::deque<SortedRun> chunks;

  SortedRuns() = default;
  ~SortedRuns() = default;
  SortedRuns(const SortedRuns& run) = default;
  SortedRuns(SortedRuns&& run) = default;
  explicit SortedRuns(const SortedRun& run) : chunks{run} {}
  DataChunkSPtr get_chunk(size_t i) const { return chunks[i].chunk; }
  size_t Chunksize() const { return chunks.size(); }
  size_t Count() const {
    size_t res = 0;
    for (auto& run : chunks) {
      res += run.Count();
    }
    return res;
  }
  void Clear();
};

// Merge two sorted cusor
class SimpleChunkSortCursor;
class MergeTwoCursor {
 public:
  MergeTwoCursor(const SortDescs& sort_desc,
                 std::unique_ptr<SimpleChunkSortCursor>&& left_cursor,
                 std::unique_ptr<SimpleChunkSortCursor>&& right_cursor);

  k_bool IsDataReady();
  k_bool IsEos();
  KStatus Next(DataChunkPtr& chunk);
  std::unique_ptr<SimpleChunkSortCursor> AsChunkCursor();

 private:
  DataChunkProvider& AsProvider() { return chunk_provider_; }
  KStatus MergeSortedCursorTwoWay(DataChunkPtr& chunk);
  KStatus MergeSortedForIntersectedCursor(DataChunkPtr& chunk, SortedRun& run1,
                                          SortedRun& run2);
  k_bool MoveCursor();

  SortDescs sort_desc_;
  SortedRun left_;
  SortedRun right_;
  std::unique_ptr<SimpleChunkSortCursor> left_cursor_;
  std::unique_ptr<SimpleChunkSortCursor> right_cursor_;
  DataChunkProvider chunk_provider_;

#ifndef NDEBUG
  k_bool left_is_empty_ = false;
  k_bool right_is_empty_ = false;
#endif
};

// Merge multiple cursors in cascade way
class MergeCursorsCascade {
 public:
  MergeCursorsCascade() = default;
  ~MergeCursorsCascade() = default;

  KStatus Init(const SortDescs& sort_desc,
               std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors);
  k_bool IsDataReady();
  k_bool IsEos();
  DataChunkPtr TryGetNextChunk();

 private:
  std::vector<std::unique_ptr<MergeTwoCursor>> mergers_;
  std::unique_ptr<SimpleChunkSortCursor> root_cursor_;
};

KStatus MergeSortedTwoWay(const SortDescs& sort_desc, const SortedRun& left,
                          const SortedRun& right, Permutation* output);

}  // namespace kwdbts
