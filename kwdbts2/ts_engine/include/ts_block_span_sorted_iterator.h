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

#include <list>
#include <memory>
#include <vector>
#include "kwdb_type.h"
#include "ts_block.h"
#include "ts_iterator_v2_impl.h"
#include "ts_mem_segment_mgr.h"

namespace kwdbts {

class TsBlockSpanSortedIterator {
 private:
  struct TsBlockSpanRowInfo {
    uint64_t entity_id = 0;
    timestamp64 ts = 0;
    uint64_t seq_no = 0;
    TsBlockSpan* block_span = nullptr;
    int row_idx = 0;

    inline bool operator<(const TsBlockSpanRowInfo& other) const {
      return entity_id != other.entity_id ? entity_id < other.entity_id
                                          : ts != other.ts ? ts < other.ts : seq_no < other.seq_no;
    }
    inline bool operator==(const TsBlockSpanRowInfo& other) const {
      return entity_id == other.entity_id && ts == other.ts && seq_no == other.seq_no;
    }
    inline bool operator<=(const TsBlockSpanRowInfo& other) const {
      return *this < other || *this == other;
    }
    inline bool operator>(const TsBlockSpanRowInfo& other) const {
      return entity_id != other.entity_id ? entity_id > other.entity_id
                                          : ts != other.ts ? ts > other.ts : seq_no > other.seq_no;
    }
    inline bool operator>=(const TsBlockSpanRowInfo& other) const {
      return *this > other || *this == other;
    }
  };
  std::list<TsBlockSpan> block_spans_;
  bool is_reverse_ = false;
  std::list<TsBlockSpanRowInfo> span_row_infos_;

  void insertRowInfo(TsBlockSpanRowInfo& row_info) {
    auto it = span_row_infos_.begin();
    while (it != span_row_infos_.end()) {
      if ((!is_reverse_ && *it >= row_info) || (is_reverse_ && *it <= row_info)) {
        break;
      }
      ++it;
    }
    span_row_infos_.insert(it, row_info);
  }

  inline void binarySearch(TsBlockSpanRowInfo& target_row_info, TsBlockSpan* block_span, int& row_idx) {
    int left = 0;
    int right = block_span->GetRowNum() - 1;
    int result;
    if (!is_reverse_) {
      result = block_span->GetRowNum();
    } else {
      result = -1;
    }

    while (left <= right) {
      int mid = left + (right - left) / 2;
      TsBlockSpanRowInfo cur_row_info = {block_span->GetEntityID(), block_span->GetTS(mid),
                                         *(block_span->GetSeqNoAddr(mid))};
      if (!is_reverse_) {
        if (cur_row_info > target_row_info) {
          result = mid;
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      } else {
        if (cur_row_info < target_row_info) {
          result = mid;
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
    row_idx = result;
  }

 public:
  explicit TsBlockSpanSortedIterator(std::list<TsBlockSpan>& block_spans, bool is_reverse = false) :
                          block_spans_(block_spans), is_reverse_(is_reverse) {}
  explicit TsBlockSpanSortedIterator(std::vector<std::list<TsBlockSpan>>& block_spans, bool is_reverse = false) :
    is_reverse_(is_reverse) {
    for (auto& block_span_list : block_spans) {
      block_spans_.merge(block_span_list);
    }
  }
  ~TsBlockSpanSortedIterator() = default;
  TsBlockSpanSortedIterator(const TsBlockSpanSortedIterator& other) {
    block_spans_ = other.block_spans_;
  }

  KStatus Init() {
    for (TsBlockSpan& block_span : block_spans_) {
      int start_row_idx = 0;
      if (is_reverse_) {
        start_row_idx = block_span.GetRowNum() - 1;
      }
      timestamp64 ts = block_span.GetTS(start_row_idx);
      uint64_t seq_no = *(block_span.GetSeqNoAddr(start_row_idx));
      span_row_infos_.push_back({block_span.GetEntityID(), ts, seq_no, &block_span, start_row_idx});
    }
    if (!is_reverse_) {
      span_row_infos_.sort();
    } else {
      span_row_infos_.sort(std::greater<TsBlockSpanRowInfo>());
    }
    return KStatus::SUCCESS;
  }

  KStatus Next(TsBlockSpan* block_span, bool* is_finished) {
    if (span_row_infos_.empty()) {
      *is_finished = true;
      return KStatus::SUCCESS;
    }
    TsBlockSpan* cur_block_span = span_row_infos_.front().block_span;
    TsBlockSpanRowInfo next_span_row_info;
    if (!is_reverse_) {
      next_span_row_info = {UINT64_MAX, INT64_MAX, UINT64_MAX};
    } else {
      next_span_row_info = {0, INT64_MIN, 0};
    }
    if (span_row_infos_.size() > 1) {
      auto iter = span_row_infos_.begin();
      iter++;
      next_span_row_info = *iter;
    }

    int end_row_idx, row_idx;
    if (!is_reverse_) {
      end_row_idx = cur_block_span->GetRowNum() - 1;
    } else {
      end_row_idx = 0;
    }
    TsBlockSpanRowInfo cur_span_end_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(end_row_idx),
                                                *cur_block_span->GetSeqNoAddr(end_row_idx)};
    row_idx = span_row_infos_.begin()->row_idx;
    if (!is_reverse_ && cur_span_end_row_info <= next_span_row_info) {
      row_idx = cur_block_span->GetRowNum();
    } else if (is_reverse_ && cur_span_end_row_info >= next_span_row_info) {
      row_idx = -1;
    } else {
      binarySearch(next_span_row_info, cur_block_span, row_idx);
    }

    if (!is_reverse_) {
      cur_block_span->SplitFront(row_idx, block_span);
    } else {
      cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - row_idx, block_span);
    }
    *is_finished = false;

    if (!is_reverse_) {
      end_row_idx = cur_block_span->GetRowNum();
    } else {
      end_row_idx = -1;
    }
    span_row_infos_.pop_front();
    if (row_idx != end_row_idx) {
      int start_row_idx = 0;
      if (is_reverse_) {
        start_row_idx = cur_block_span->GetRowNum() - 1;
      }
      TsBlockSpanRowInfo next_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(start_row_idx),
                                           *(cur_block_span->GetSeqNoAddr(start_row_idx)),
                                           cur_block_span, start_row_idx};
      insertRowInfo(next_row_info);
    }
    return KStatus::SUCCESS;
  }
};

}  //  namespace kwdbts
