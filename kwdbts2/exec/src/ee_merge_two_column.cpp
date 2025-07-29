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

  EqualRange(Range left, Range right)
      : left_range(std::move(left)), right_range(std::move(right)) {}
};

// MergeTwoColumn incremental merge two columns
class MergeTwoColumn {
 public:
  MergeTwoColumn(SortDesc desc, DataChunk* left_col, DataChunk* right_col,
                 k_uint32 order_col, std::vector<EqualRange>* equal_range,
                 Permutation* perm)
      : sort_order_(desc.sort_order),
        null_first_(desc.null_first),
        left_col_(left_col),
        right_col_(right_col),
        order_col_(order_col),
        equal_ranges_(equal_range),
        perm_(perm) {}

  template <class Cmp, class LeftEqual, class RightEqual>
  KStatus DoMergeColumn(Cmp cmp, LeftEqual equal_left, RightEqual equal_right) {
    std::vector<EqualRange> next_ranges;
    next_ranges.reserve(equal_ranges_->size());

    // Iterate each equal-range
    for (auto equal_range : *equal_ranges_) {
      size_t lhs = equal_range.left_range.first;
      size_t rhs = equal_range.right_range.first;
      size_t lhs_end = equal_range.left_range.second;
      size_t rhs_end = equal_range.right_range.second;
      size_t output_index = lhs + rhs;

      // Merge rows in the equal-range
      auto left_range = fetch_equal(lhs, lhs_end, equal_left);
      auto right_range = fetch_equal(rhs, rhs_end, equal_right);
      while (lhs < lhs_end || rhs < rhs_end) {
        int x = 0;
        if (lhs < lhs_end && rhs < rhs_end) {
          x = cmp(left_range.first, right_range.first);
        } else if (lhs < lhs_end) {
          x = -1;
        } else if (rhs < rhs_end) {
          x = 1;
        }

        if (x == 0) {
          next_ranges.emplace_back(left_range, right_range);
        }
        if (x <= 0) {
          for (size_t i = left_range.first; i < left_range.second; i++) {
            (*perm_)[output_index++] = PermutationItem(kLeftIndex, i);
          }
          lhs = left_range.second;
          left_range = fetch_equal(lhs, lhs_end, equal_left);
        }
        if (x >= 0) {
          for (size_t i = right_range.first; i < right_range.second; i++) {
            (*perm_)[output_index++] = PermutationItem(kRightIndex, i);
          }
          rhs = right_range.second;
          right_range = fetch_equal(rhs, rhs_end, equal_right);
        }
      }
    }

    equal_ranges_->swap(next_ranges);
    return KStatus::SUCCESS;
  }

  template <class Equal>
  EqualRange::Range fetch_equal(size_t lhs, size_t lhs_end, Equal equal) {
    k_uint64 first = lhs;
    k_uint64 last = lhs + 1;
    while (last < lhs_end && equal(last - 1, last)) {
      last++;
    }
    return {first, last};
  }
  KStatus MergeOrdinaryColumn() {
    auto cmp = [&](size_t lhs_index, size_t rhs_index) {
      int x = left_col_->Compare(lhs_index, rhs_index, order_col_, right_col_);
      if (sort_order_ == -1) {
        x *= -1;
      }
      return x;
    };
    auto equal_left = [&](size_t lhs_index, size_t rhs_index) {
      return left_col_->Compare(lhs_index, rhs_index, order_col_, left_col_) ==
             0;
    };
    auto equal_right = [&](size_t lhs_index, size_t rhs_index) {
      return right_col_->Compare(lhs_index, rhs_index, order_col_,
                                 right_col_) == 0;
    };
    return DoMergeColumn(cmp, equal_left, equal_right);
  }

 private:
  constexpr static k_uint64 kLeftIndex = 0;
  constexpr static k_uint64 kRightIndex = 1;

  const k_int32 sort_order_;
  const k_int32 null_first_;
  DataChunk* left_col_;
  DataChunk* right_col_;
  const k_uint32 order_col_;
  std::vector<EqualRange>* equal_ranges_;
  Permutation* perm_;
};

class MergeTwoChunk {
 public:
  static constexpr k_int32 kLeftChunkIndex = 0;
  static constexpr k_int32 kRightChunkIndex = 1;

  static KStatus MergeSortedChunksTwoWay(const SortDescs& sort_desc,
                                         const SortedRun& left_run,
                                         const SortedRun& right_run,
                                         Permutation* output) {
    if (left_run.Empty()) {
      size_t count = right_run.Count();
      output->resize(count);
      for (size_t i = 0; i < count; i++) {
        (*output)[i].chunk_index = kRightChunkIndex;
        (*output)[i].index_in_chunk = i + right_run.range.first;
      }
    } else if (right_run.Empty()) {
      size_t count = left_run.Count();
      output->resize(count);
      for (size_t i = 0; i < count; i++) {
        (*output)[i].chunk_index = kLeftChunkIndex;
        (*output)[i].index_in_chunk = i + left_run.range.first;
      }
    } else {
      k_int32 intersect = left_run.Intersect(sort_desc, right_run);
      if (intersect != 0) {
        size_t left_rows = left_run.Count();
        size_t right_rows = right_run.Count();
        output->resize(0);
        output->reserve(left_rows + right_rows);

        if (intersect < 0) {
          for (size_t i = 0; i < left_rows; i++) {
            output->emplace_back(kLeftChunkIndex, i + left_run.range.first);
          }
          for (size_t i = 0; i < right_rows; i++) {
            output->emplace_back(kRightChunkIndex, i + right_run.range.first);
          }
        } else {
          for (size_t i = 0; i < right_rows; i++) {
            output->emplace_back(kRightChunkIndex, i + right_run.range.first);
          }
          for (size_t i = 0; i < left_rows; i++) {
            output->emplace_back(kLeftChunkIndex, i + left_run.range.first);
          }
        }
      } else {
        std::vector<EqualRange> equal_ranges;
        equal_ranges.emplace_back(left_run.range, right_run.range);
        size_t count = left_run.range.second + right_run.range.second;
        output->resize(count);
        equal_ranges.reserve(std::max(static_cast<size_t>(1), count / 4));
        for (int col = 0; col < sort_desc.ColumnsNum(); col++) {
          k_uint32 col_idx = left_run.GetColumnIdx(col);
          MergeTwoColumn merge(sort_desc.GetColumnDesc(col),
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
k_int32 SortedRun::Intersect(const SortDescs& sort_desc,
                             const SortedRun& right_run) const {
  if (Empty()) {
    return 1;
  }
  if (right_run.Empty()) {
    return -1;
  }

  k_int32 left_tail_cmp =
      CompareRow(sort_desc, right_run, EndIndex() - 1, right_run.StartIndex());
  if (left_tail_cmp < 0) {
    return -1;
  }

  k_int32 left_head_cmp =
      CompareRow(sort_desc, right_run, StartIndex(), right_run.EndIndex() - 1);
  if (left_head_cmp > 0) {
    return 1;
  }
  return 0;
}

void SortedRun::Reset() {
  chunk.reset();
  orderby.clear();
  range = {};
}

DataChunkPtr SortedRun::CloneSlice() const {
  if (range.first == 0 && range.second == chunk->Count()) {
    DataChunkPtr cloned = chunk->CloneEmpty(chunk->Count());
    cloned->Append(chunk.get(), range.first, chunk->Count());
    return cloned;
  } else {
    size_t slice_rows = Count();
    DataChunkPtr cloned = chunk->CloneEmpty(slice_rows);
    cloned->Append(chunk.get(), range.first, slice_rows);
    return cloned;
  }
}

k_int32 SortedRun::CompareRow(const SortDescs& desc, const SortedRun& rhs,
                              size_t lhs_row, size_t rhs_row) const {
  for (int i = 0; i < desc.ColumnsNum(); i++) {
    int x = chunk->Compare(lhs_row, rhs_row, orderby[i], rhs.chunk.get());
    if (x != 0) {
      return x * desc.GetColumnDesc(i).sort_order;
    }
  }
  return 0;
}
void SortedRuns::Clear() { chunks.clear(); }

KStatus MergeSortedTwoWay(const SortDescs& sort_desc, const SortedRun& left,
                          const SortedRun& right, Permutation* output) {
  return MergeTwoChunk::MergeSortedChunksTwoWay(sort_desc, left, right, output);
}

}  // namespace kwdbts
