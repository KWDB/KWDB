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

#include <utility>

#include "ee_data_chunk.h"
#include "ee_merge_cursor.h"
#include "ee_sort_desc.h"
#include "kwdb_type.h"

namespace kwdbts {
struct EqualRange {
  using Range = std::pair<k_uint64, k_uint64>;

  Range left_range;
  Range right_range;

  EqualRange(Range left, Range right) : left_range(std::move(left)), right_range(std::move(right)) {
  }
  k_uint64 LeftStart() const {
    return left_range.first;
  }

  k_uint64 LeftEnd() const {
    return left_range.second;
  }

  k_uint64 RightStart() const {
    return right_range.first;
  }

  k_uint64 RightEnd() const {
    return right_range.second;
  }

  k_uint64 LeftCount() const {
    return LeftEnd() - LeftStart();
  }
  k_uint64 RightCount() const {
    return RightEnd() - RightStart();
  }
};

// TwoColumnMerger
class TwoColumnMerger {
 public:
  TwoColumnMerger(SortingRule rule, DataChunk* left_col, DataChunk* right_col,
                 k_uint32 order_col, std::vector<EqualRange>* equal_range,
                 Permutation* perm)
      : sortDirection_(rule.order_direction),
        null_first_(rule.null_first),
        left_col_(left_col),
        right_col_(right_col),
        order_col_(order_col),
        equal_ranges_(equal_range),
        perm_(perm) {}

  template <class Equal>
  EqualRange::Range findEqualRange(size_t startIdx, size_t endIdx, Equal equal) {
    k_uint64 first = startIdx;
    k_uint64 last = startIdx + 1;
    while (last < endIdx && equal(last - 1, last)) {
      last++;
    }
    return {first, last};
  }
  template <class Cmp>
  k_int32 CompareCurrentElements(size_t lhs, size_t lhs_end, size_t rhs, size_t rhs_end, const EqualRange::Range& left_range,
                                 const EqualRange::Range& right_range, Cmp cmpFunc) {
    if (lhs < lhs_end && rhs < rhs_end) {
      return cmpFunc(left_range.first, right_range.first);
    } else if (lhs < lhs_end) {
      return -1;
    } else if (rhs < rhs_end) {
      return 1;
    }
    return 0;
  }
  template <class Cmp, class LeftEqual, class RightEqual>
  KStatus DoMergeColumn(Cmp cmpFunc, LeftEqual equal_left, RightEqual equal_right) {
    std::vector<EqualRange> nextEqualRanges;
    nextEqualRanges.reserve(equal_ranges_->size());

    // Iterate each equal-range
    for (auto currentRange : *equal_ranges_) {
      size_t lhs = currentRange.left_range.first;
      size_t rhs = currentRange.right_range.first;
      size_t lhs_end = currentRange.left_range.second;
      size_t rhs_end = currentRange.right_range.second;
      size_t output_index = lhs + rhs;

      // Merge rows in the equal-range
      auto left_range = findEqualRange(lhs, lhs_end, equal_left);
      auto right_range = findEqualRange(rhs, rhs_end, equal_right);
      while (lhs < lhs_end || rhs < rhs_end) {
        k_int32 result = 0;
        result = CompareCurrentElements(lhs, lhs_end, rhs, rhs_end, left_range, right_range, cmpFunc);
        if (result == 0) {
          nextEqualRanges.emplace_back(left_range, right_range);
        }
        if (result <= 0) {
          for (size_t i = left_range.first; i < left_range.second; i++) {
            (*perm_)[output_index++] = PermutationItem(kLeftIndex, i);
          }
          lhs = left_range.second;
          left_range = findEqualRange(lhs, lhs_end, equal_left);
        }
        if (result >= 0) {
          for (size_t i = right_range.first; i < right_range.second; i++) {
            (*perm_)[output_index++] = PermutationItem(kRightIndex, i);
          }
          rhs = right_range.second;
          right_range = findEqualRange(rhs, rhs_end, equal_right);
        }
      }
    }

    equal_ranges_->swap(nextEqualRanges);
    return KStatus::SUCCESS;
  }

  KStatus MergeOrdinaryColumn() {
    auto cmpFunc = [&](size_t leftIndex, size_t rightIndex) {
      k_int32 result = left_col_->Compare(leftIndex, rightIndex, order_col_, right_col_);
      return (sortDirection_ == -1) ? (result * -1) : result;
    };
    auto leftEqualFunc = [&](size_t leftIndex, size_t rightIndex) {
      return left_col_->Compare(leftIndex, rightIndex, order_col_, left_col_) ==
             0;
    };
    auto rightEqualFunc = [&](size_t leftIndex, size_t rightIndex) {
      return right_col_->Compare(leftIndex, rightIndex, order_col_,
                                 right_col_) == 0;
    };
    return DoMergeColumn(cmpFunc, leftEqualFunc, rightEqualFunc);
  }

 private:
  constexpr static k_uint64 kLeftIndex = 0;
  constexpr static k_uint64 kRightIndex = 1;

  const k_int32 sortDirection_;
  const k_int32 null_first_;
  DataChunk* left_col_;
  DataChunk* right_col_;
  const k_uint32 order_col_;
  std::vector<EqualRange>* equal_ranges_;
  Permutation* perm_;
};

static void FillPermutationWithRun(const SortedRun& run, k_uint32 chunkIndex, Permutation* output) {
  const size_t rowCount = run.Count();
  output->resize(rowCount);
  for (size_t i = 0; i < rowCount; ++i) {
    (*output)[i] = {chunkIndex, static_cast<k_uint32>(i + run.range.first)};
  }
}

class MergeTwoChunk {
 public:
  static constexpr k_int32 LeftChunkIndex = 0;
  static constexpr k_int32 RightChunkIndex = 1;

  static void AddRangeToOutput(Permutation* output, k_int32 chunkIndex, const SortedRun& run, size_t rowCount) {
    for (size_t i = 0; i < rowCount; ++i) {
      output->emplace_back(chunkIndex, i + run.range.first);
    }
  }
  static KStatus MergeSortedChunksInPairs(const SortingRules& rules,
                                         const SortedRun& left_run,
                                         const SortedRun& right_run,
                                         Permutation* output) {
    if (left_run.Empty()) {
      FillPermutationWithRun(right_run, RightChunkIndex, output);
    } else if (right_run.Empty()) {
      FillPermutationWithRun(left_run, LeftChunkIndex, output);
    } else {
      k_int32 inter_select = left_run.Intersect(rules, right_run);
      if (inter_select != 0) {
        output->resize(0);
        size_t left_row_count = left_run.Count();
        size_t right_row_count = right_run.Count();
        output->reserve(left_row_count + right_row_count);

        if (inter_select < 0) {
          AddRangeToOutput(output, LeftChunkIndex, left_run, left_row_count);
          AddRangeToOutput(output, RightChunkIndex, right_run, right_row_count);
        } else {
          AddRangeToOutput(output, RightChunkIndex, right_run, right_row_count);
          AddRangeToOutput(output, LeftChunkIndex, left_run, left_row_count);
        }
      } else {
        std::vector<EqualRange> equal_ranges;
        size_t count = left_run.range.second + right_run.range.second;
        output->resize(count);
        equal_ranges.emplace_back(left_run.range, right_run.range);
        equal_ranges.reserve(std::max(static_cast<size_t>(1), count / 4));
        for (int col = 0; col < rules.ColumnsNum(); col++) {
          k_uint32 col_idx = left_run.GetColumnIdx(col);
          TwoColumnMerger merge(rules.GetColumnRule(col),
                               left_run.chunk.get(), right_run.chunk.get(),
                               col_idx, &equal_ranges, output);

          KStatus ret = merge.MergeOrdinaryColumn();
          if (KStatus::SUCCESS != ret) {
            return ret;
          }
          if (equal_ranges.size() == 0) {
            break;
          }
        }
      }
    }

    return KStatus::SUCCESS;
  }
};
k_int32 SortedRun::Intersect(const SortingRules& sort_desc,
                             const SortedRun& right_run) const {
  if (Empty()) {
    return 1;
  }
  if (right_run.Empty()) {
    return -1;
  }

  k_int32 lefttailComparison =
      CompareRow(sort_desc, right_run, EndIndex() - 1, right_run.StartIndex());
  if (lefttailComparison < 0) {
    return -1;
  }

  k_int32 leftheadComparison =
      CompareRow(sort_desc, right_run, StartIndex(), right_run.EndIndex() - 1);
  if (leftheadComparison > 0) {
    return 1;
  }
  return 0;
}

DataChunkPtr SortedRun::CloneDataSlice() const {
  k_uint32 count = chunk->Count();
  if (range.first == 0 && range.second == count) {
    DataChunkPtr temp = chunk->CloneEmpty(count);
    if (temp == nullptr) {
      return nullptr;
    }
    temp->Append(chunk.get(), range.first, count);
    return temp;
  } else {
    size_t slice_rows = Count();
    DataChunkPtr temp = chunk->CloneEmpty(slice_rows);
    if (temp == nullptr) {
      return nullptr;
    }
    temp->Append(chunk.get(), range.first, slice_rows);
    return temp;
  }
}

k_int32 SortedRun::CompareRow(const SortingRules& desc, const SortedRun& rhs,
                              size_t lhs_row, size_t rhs_row) const {
  for (int i = 0; i < desc.ColumnsNum(); i++) {
    int x = chunk->Compare(lhs_row, rhs_row, orderby[i], rhs.chunk.get());
    if (x != 0) {
      return x * desc.GetColumnRule(i).order_direction;
    }
  }
  return 0;
}
void SortedRuns::Clear() { chunks.clear(); }

KStatus MergeTwoSortedRuns(const SortingRules& sort_desc, const SortedRun& left,
                          const SortedRun& right, Permutation* output) {
  return MergeTwoChunk::MergeSortedChunksInPairs(sort_desc, left, right, output);
}

}  // namespace kwdbts
