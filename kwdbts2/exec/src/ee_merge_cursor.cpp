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

#include "ee_merge_cursor.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "lg_api.h"

namespace kwdbts {

void MaterializeDst(DataChunk* dst, const std::vector<DataChunkSPtr>& chunks,
                    PermutationItem* perm, size_t merge_rows) {
  if (chunks.empty() || (merge_rows < 1)) {
    return;
  }

  for (size_t row = 0; row < merge_rows; row++) {
    dst->Append_Selective(chunks[perm[row].chunk_index].get(),
                          perm[row].index_in_chunk);
  }
  return;
}

struct CursorOpAlgo {
  static k_int32 CompareTailRow(const SortDescs& desc, const SortedRun& left,
                                const SortedRun& right) {
    size_t lhs_tail = left.range.second - 1;
    size_t rhs_tail = right.range.second - 1;
    return left.CompareRow(desc, right, lhs_tail, rhs_tail);
  }
};

MergeTwoCursor::MergeTwoCursor(
    const SortDescs& sort_desc,
    std::unique_ptr<SimpleChunkSortCursor>&& left_cursor,
    std::unique_ptr<SimpleChunkSortCursor>&& right_cursor)
    : sort_desc_(sort_desc),
      left_cursor_(std::move(left_cursor)),
      right_cursor_(std::move(right_cursor)) {
  chunk_provider_ = [&](DataChunkPtr* output, k_bool* eos) -> k_bool {
    if (output == nullptr || eos == nullptr) {
      return IsDataReady();
    }
    *eos = IsEos();
    if (KStatus::SUCCESS != Next(*output)) {
      return false;
    }
    if (nullptr == (*output)) {
      return false;
    } else {
      return true;
    }
  };
}

// use this as cursor
std::unique_ptr<SimpleChunkSortCursor> MergeTwoCursor::AsChunkCursor() {
  return std::make_unique<SimpleChunkSortCursor>(
      AsProvider(), left_cursor_->GetSortColumns());
}
k_bool MergeTwoCursor::IsDataReady() {
  return left_cursor_->IsDataReady() && right_cursor_->IsDataReady();
}

k_bool MergeTwoCursor::IsEos() {
  return left_.Empty() && left_cursor_->IsEos() && right_.Empty() &&
         right_cursor_->IsEos();
}

KStatus MergeTwoCursor::Next(DataChunkPtr& chunk) {
  if (!IsDataReady() || IsEos()) {
    return KStatus::SUCCESS;
  }
  if (MoveCursor()) {
    return KStatus::SUCCESS;
  }
  return MergeSortedCursorTwoWay(chunk);
}

k_bool MergeTwoCursor::MoveCursor() {
  k_bool eos = left_.Empty() || right_.Empty();
  if (left_.Empty() && !left_cursor_->IsEos()) {
    auto chunk = left_cursor_->TryGetNextChunk();
    if (chunk.first) {
      left_ = SortedRun(DataChunkSPtr(chunk.first.release()), &chunk.second);
    }
  }
  if (right_.Empty() && !right_cursor_->IsEos()) {
    auto chunk = right_cursor_->TryGetNextChunk();
    if (chunk.first) {
      right_ = SortedRun(DataChunkSPtr(chunk.first.release()), &chunk.second);
    }
  }

  if (!left_.Empty() && !right_.Empty()) {
    eos = false;
  }

  if ((left_cursor_->IsEos() && !right_.Empty()) ||
      (right_cursor_->IsEos() && !left_.Empty())) {
    eos = false;
  }

  return eos;
}

KStatus MergeTwoCursor::MergeSortedCursorTwoWay(DataChunkPtr& chunk) {
  const SortDescs& sort_desc = sort_desc_;
  // debug scope
#ifndef NDEBUG
  left_is_empty_ |= left_.Empty();
  right_is_empty_ |= right_.Empty();
#endif

  k_int32 intersect = left_.Intersect(sort_desc, right_);
  if (intersect < 0) {
    chunk = left_.CloneSlice();
    left_.Reset();
  } else if (intersect > 0) {
    chunk = right_.CloneSlice();
    right_.Reset();
  } else {
    return MergeSortedForIntersectedCursor(chunk, left_, right_);
  }

  return KStatus::SUCCESS;
}

KStatus MergeTwoCursor::MergeSortedForIntersectedCursor(DataChunkPtr& chunk,
                                                        SortedRun& run1,
                                                        SortedRun& run2) {
  const auto& sort_desc = sort_desc_;

  k_int32 tail_cmp = CursorOpAlgo::CompareTailRow(sort_desc, run1, run2);

  Permutation permutation;
  if (KStatus::SUCCESS !=
      MergeSortedTwoWay(sort_desc, run1, run2, &permutation)) {
    return KStatus::FAIL;
  }

  size_t merged_rows = std::min(run1.Count(), run2.Count());

  chunk = run2.chunk->CloneEmpty(merged_rows);
  MaterializeDst(chunk.get(), {run1.chunk, run2.chunk}, permutation.data(),
                 merged_rows);

  auto left_rows = permutation.size() - merged_rows;
  DataChunkPtr left_merged = run2.chunk->CloneEmpty(left_rows);
  MaterializeDst(left_merged.get(), {run1.chunk, run2.chunk},
                 permutation.data() + merged_rows, left_rows);

  if (tail_cmp <= 0) {
    run1.Reset();
    run2 = SortedRun(std::move(left_merged), left_cursor_->GetSortColumns());
  } else {
    run1 = SortedRun(std::move(left_merged), left_cursor_->GetSortColumns());
    run2.Reset();
  }
  return KStatus::SUCCESS;
}

KStatus MergeCursorsCascade::Init(
    const SortDescs& sort_desc,
    std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors) {
  std::vector<std::unique_ptr<SimpleChunkSortCursor>> cur_level =
      std::move(cursors);

  while (cur_level.size() > 1) {
    std::vector<std::unique_ptr<SimpleChunkSortCursor>> next_level;
    next_level.reserve(cur_level.size() / 2);

    int level_size = cur_level.size() & ~1;
    for (int i = 0; i < level_size; i += 2) {
      auto& left = cur_level[i];
      auto& right = cur_level[i + 1];
      mergers_.push_back(std::make_unique<MergeTwoCursor>(
          sort_desc, std::move(left), std::move(right)));
      next_level.push_back(mergers_.back()->AsChunkCursor());
    }
    if (cur_level.size() % 2 == 1) {
      next_level.push_back(std::move(cur_level.back()));
    }

    std::swap(next_level, cur_level);
  }
  root_cursor_ = std::move(cur_level.front());

  return KStatus::SUCCESS;
}

k_bool MergeCursorsCascade::IsDataReady() {
  return root_cursor_->IsDataReady();
}

k_bool MergeCursorsCascade::IsEos() { return root_cursor_->IsEos(); }

DataChunkPtr MergeCursorsCascade::TryGetNextChunk() {
  return root_cursor_->TryGetNextChunk().first;
}

}  // namespace kwdbts
