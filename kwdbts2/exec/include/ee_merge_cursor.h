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
  SortedRun(SortedRun&& rhs) = default;
  SortedRun(const SortedRun& rhs) = default;
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
  size_t ColsNum() const {
    return orderby.size();
  }
  k_uint32 GetRangeLength() const {
    return range.second - range.first;
  }
  void SetRange(size_t start, size_t end) {
    range.first = start;
    range.second = end;
  }
  bool HasOrderByColumn(k_uint32 columnIndex) const {
    return std::find(orderby.begin(), orderby.end(), columnIndex) != orderby.end();
  }
  void ResetRange() {
    range.first = 0;
    range.second = chunk->Count();
  }
  k_uint32 GetColumnIdx(int index) const {
    return orderby[index];
  }
  size_t StartIndex() const {
    return range.first;
  }

  const std::pair<k_uint32, k_uint32>& GetRange() const {
    return range;
  }

  size_t Count() const {
    return range.second - range.first;
  }

  k_bool Empty() const {
    return range.second == range.first || chunk == nullptr;
  }

  size_t EndIndex() const {
    return range.second;
  }

  void Reset() {
    chunk.reset();
    orderby.clear();
    range = {};
  }
  const std::vector<k_uint32>& GetOrderByColumns() const {
    return orderby;
  }

  size_t GetOrderByColumnCount() const {
    return orderby.size();
  }

  int Intersect(const SortingRules& sort_desc, const SortedRun& right_run) const;

  DataChunkPtr CloneDataSlice() const;

  int CompareRow(const SortingRules& desc, const SortedRun& rhs, size_t lhs_row,
                 size_t rhs_row) const;

  // k_bool IsSorted(const SortingRules& desc) const;
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

struct CursorOperationAlgorithm {
  static k_int32 CompareTailRow(const SortingRules& sort_rule, const SortedRun& first_run,
                                const SortedRun& second_run) {
    size_t first_tail_index = first_run.range.second - 1;
    size_t second_tail_index = second_run.range.second - 1;
    return first_run.CompareRow(sort_rule, second_run, first_tail_index, second_tail_index);
  }
};
// Merge two sorted cusor
class SortedChunkCursor;
class TwoCursorMerger {
 public:
  TwoCursorMerger(const SortingRules& sort_desc,
                 std::unique_ptr<SortedChunkCursor>&& left_sort_cursor,
                 std::unique_ptr<SortedChunkCursor>&& right_sort_cursor);

  k_bool IsAtEnd();
  std::unique_ptr<SortedChunkCursor> AsChunkCursor();
  KStatus GetNextMergedChunk(DataChunkPtr& chunk);
  k_bool IsDataReady();

 private:
  KStatus ExecuteDualCursorMerge(DataChunkPtr& chunk);
  KStatus ExecuteIntersectionMerge(DataChunkPtr& chunk, SortedRun& run1, SortedRun& run2);
  k_bool AdvanceCursors() {
    k_bool eos = left_.Empty() || right_.Empty();
    if (left_.Empty() && !left_cursor_->IsAtEnd()) {
      auto temp_chunk = left_cursor_->FetchNextSortedChunk();
      if (temp_chunk.first) {
        left_ = SortedRun(DataChunkSPtr(temp_chunk.first.release()), &temp_chunk.second);
      }
    }
    if (right_.Empty() && !right_cursor_->IsAtEnd()) {
      auto temp_chunk = right_cursor_->FetchNextSortedChunk();
      if (temp_chunk.first) {
        right_ = SortedRun(DataChunkSPtr(temp_chunk.first.release()), &temp_chunk.second);
      }
    }

    if (!left_.Empty() && !right_.Empty()) {
      eos = false;
    }

    if ((left_cursor_->IsAtEnd() && !right_.Empty()) || (right_cursor_->IsAtEnd() && !left_.Empty())) {
      eos = false;
    }

    return eos;
  }
  DataChunkProvider& GetChunkProvider() {
    return chunk_provider_;
  }

  SortingRules sort_rules_;
  SortedRun left_;
  SortedRun right_;
  std::unique_ptr<SortedChunkCursor> left_cursor_;
  std::unique_ptr<SortedChunkCursor> right_cursor_;
  DataChunkProvider chunk_provider_;

#ifndef NDEBUG
  k_bool left_is_empty_ = false;
  k_bool right_is_empty_ = false;
#endif
};

// Merge multiple cursors in cascade way
class CascadingCursorMerger {
 public:
  CascadingCursorMerger() = default;
  ~CascadingCursorMerger() = default;

  KStatus Init(const SortingRules& sort_desc, std::vector<std::unique_ptr<SortedChunkCursor>>&& cursors) {
    std::vector<std::unique_ptr<SortedChunkCursor>> cur_level = std::move(cursors);

    while (cur_level.size() > 1) {
      std::vector<std::unique_ptr<SortedChunkCursor>> next_level;
      next_level.reserve(cur_level.size() / 2);

      int level_size = cur_level.size() & ~1;
      for (int i = 0; i < level_size; i += 2) {
        auto& left = cur_level[i];
        auto& right = cur_level[i + 1];
        cascade_mergers_.push_back(std::make_unique<TwoCursorMerger>(sort_desc, std::move(left), std::move(right)));
        next_level.push_back(cascade_mergers_.back()->AsChunkCursor());
      }
      if (cur_level.size() % 2 == 1) {
        next_level.push_back(std::move(cur_level.back()));
      }

      std::swap(next_level, cur_level);
    }
    root_cursor_ = std::move(cur_level.front());

    return KStatus::SUCCESS;
  }
  k_bool IsDataReady();
  k_bool IsAtEnd();
  DataChunkPtr FetchNextSortedChunk();

 private:
  std::vector<std::unique_ptr<TwoCursorMerger>> cascade_mergers_;
  std::unique_ptr<SortedChunkCursor> root_cursor_;
};

KStatus MergeTwoSortedRuns(const SortingRules& sort_desc, const SortedRun& left,
                          const SortedRun& right, Permutation* output);

}  // namespace kwdbts
