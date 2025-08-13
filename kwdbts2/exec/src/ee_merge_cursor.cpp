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

TwoCursorMerger::TwoCursorMerger(
    const SortingRules& sort_desc,
    std::unique_ptr<SortedChunkCursor>&& left_sort_cursor,
    std::unique_ptr<SortedChunkCursor>&& right_sort_cursor)
    : sort_rules_(sort_desc),
      left_cursor_(std::move(left_sort_cursor)),
      right_cursor_(std::move(right_sort_cursor)) {
  chunk_provider_ = [&](DataChunkPtr* chunk, k_bool* eos) -> k_bool {
    if (chunk == nullptr || eos == nullptr) {
      return IsDataReady();
    }
    *eos = IsAtEnd();
    if (KStatus::SUCCESS != GetNextMergedChunk(*chunk)) {
      return false;
    }
    if (nullptr == (*chunk)) {
      return false;
    } else {
      return true;
    }
  };
}

k_bool TwoCursorMerger::IsAtEnd() {
  return left_.Empty() && left_cursor_->IsAtEnd() && right_.Empty() &&
         right_cursor_->IsAtEnd();
}

KStatus TwoCursorMerger::GetNextMergedChunk(DataChunkPtr& chunk) {
  if (!IsDataReady() || IsAtEnd()) {
    return KStatus::SUCCESS;
  }
  if (AdvanceCursors()) {
    return KStatus::SUCCESS;
  }
  return ExecuteDualCursorMerge(chunk);
}

// k_bool TwoCursorMerger::AdvanceCursors() {
//   k_bool eos = left_.Empty() || right_.Empty();
//   if (left_.Empty() && !left_cursor_->IsAtEnd()) {
//     auto temp_chunk = left_cursor_->FetchNextSortedChunk();
//     if (temp_chunk.first) {
//       left_ = SortedRun(DataChunkSPtr(temp_chunk.first.release()), &temp_chunk.second);
//     }
//   }
//   if (right_.Empty() && !right_cursor_->IsAtEnd()) {
//     auto temp_chunk = right_cursor_->FetchNextSortedChunk();
//     if (temp_chunk.first) {
//       right_ = SortedRun(DataChunkSPtr(temp_chunk.first.release()), &temp_chunk.second);
//     }
//   }

//   if (!left_.Empty() && !right_.Empty()) {
//     eos = false;
//   }

//   if ((left_cursor_->IsAtEnd() && !right_.Empty()) ||
//       (right_cursor_->IsAtEnd() && !left_.Empty())) {
//     eos = false;
//   }

//   return eos;
// }

// use this as cursor
std::unique_ptr<SortedChunkCursor> TwoCursorMerger::AsChunkCursor() {
  return std::make_unique<SortedChunkCursor>(
      GetChunkProvider(), left_cursor_->GetSortColumns());
}
k_bool TwoCursorMerger::IsDataReady() {
  return left_cursor_->IsDataReady() && right_cursor_->IsDataReady();
}

KStatus TwoCursorMerger::ExecuteDualCursorMerge(DataChunkPtr& chunk) {
  const SortingRules& sort_desc = sort_rules_;
  // debug scope
#ifndef NDEBUG
  left_is_empty_ |= left_.Empty();
  right_is_empty_ |= right_.Empty();
#endif

  k_int32 intersect = left_.Intersect(sort_desc, right_);
  if (intersect < 0) {
    chunk = left_.CloneDataSlice();
    if (chunk == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "sort data error");
      return KStatus::FAIL;
    }
    left_.Reset();
  } else if (intersect > 0) {
    chunk = right_.CloneDataSlice();
    if (chunk == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "sort data error");
      return KStatus::FAIL;
    }
    right_.Reset();
  } else {
    return ExecuteIntersectionMerge(chunk, left_, right_);
  }

  return KStatus::SUCCESS;
}

DataChunkPtr CascadingCursorMerger::FetchNextSortedChunk() {
  return root_cursor_->FetchNextSortedChunk().first;
}

KStatus TwoCursorMerger::ExecuteIntersectionMerge(DataChunkPtr& chunk,
                                                        SortedRun& run1,
                                                        SortedRun& run2) {
  const auto& sort_rule = sort_rules_;

  k_int32 tailComparison = CursorOperationAlgorithm::CompareTailRow(sort_rule, run1, run2);

  Permutation permutation;
  if (KStatus::SUCCESS !=
      MergeTwoSortedRuns(sort_rule, run1, run2, &permutation)) {
    return KStatus::FAIL;
  }

  size_t merged_rows = std::min(run1.Count(), run2.Count());

  chunk = run2.chunk->CloneEmpty(merged_rows);
  if (chunk == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "sort data error");
    return KStatus::FAIL;
  }
  MaterializeDst(chunk.get(), {run1.chunk, run2.chunk}, permutation.data(),
                 merged_rows);

  auto left_rows = permutation.size() - merged_rows;
  DataChunkPtr left_merged = run2.chunk->CloneEmpty(left_rows);
  if (left_merged == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "sort data error");
    return KStatus::FAIL;
  }
  MaterializeDst(left_merged.get(), {run1.chunk, run2.chunk},
                 permutation.data() + merged_rows, left_rows);

  if (tailComparison <= 0) {
    run1.Reset();
    run2 = SortedRun(std::move(left_merged), left_cursor_->GetSortColumns());
  } else {
    run1 = SortedRun(std::move(left_merged), left_cursor_->GetSortColumns());
    run2.Reset();
  }
  return KStatus::SUCCESS;
}

// KStatus CascadingCursorMerger::Init(
//     const SortingRules& sort_desc,
//     std::vector<std::unique_ptr<SortedChunkCursor>>&& cursors) {
//   std::vector<std::unique_ptr<SortedChunkCursor>> cur_level =
//       std::move(cursors);

//   while (cur_level.size() > 1) {
//     std::vector<std::unique_ptr<SortedChunkCursor>> next_level;
//     next_level.reserve(cur_level.size() / 2);

//     int level_size = cur_level.size() & ~1;
//     for (int i = 0; i < level_size; i += 2) {
//       auto& left = cur_level[i];
//       auto& right = cur_level[i + 1];
//       cascade_mergers_.push_back(std::make_unique<TwoCursorMerger>(
//           sort_desc, std::move(left), std::move(right)));
//       next_level.push_back(cascade_mergers_.back()->AsChunkCursor());
//     }
//     if (cur_level.size() % 2 == 1) {
//       next_level.push_back(std::move(cur_level.back()));
//     }

//     std::swap(next_level, cur_level);
//   }
//   root_cursor_ = std::move(cur_level.front());

//   return KStatus::SUCCESS;
// }

k_bool CascadingCursorMerger::IsDataReady() {
  return root_cursor_->IsDataReady();
}

k_bool CascadingCursorMerger::IsAtEnd() { return root_cursor_->IsAtEnd(); }

}  // namespace kwdbts
