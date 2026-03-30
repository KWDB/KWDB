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

#include <vector>
#include <memory>

#include "ts_common.h"

namespace kwdbts {
class TsBlockSpanSplitter {
 private:
  int64_t begin_ts_ = INVALID_TS;
  int64_t interval_ = 0;
  std::vector<std::shared_ptr<TsBlockSpan>> ts_block_spans_;

 private:
  int32_t binarySearch(std::shared_ptr<TsBlockSpan>& block_span, timestamp64 split_ts) {
    int32_t split_row_idx = -1;
    uint32_t start = 0, end = block_span->GetRowNum() - 1;
    while (start <= end) {
      uint32_t mid = start + (end - start) / 2;
      timestamp64 mid_ts = block_span->GetTS(mid);
      if (mid_ts <= split_ts) {
        start = mid + 1;
        split_row_idx = mid;
      } else {
        end = mid - 1;
      }
    }
    return split_row_idx;
  }

 public:
  TsBlockSpanSplitter(int64_t begin_ts, int64_t interval, std::vector<std::shared_ptr<TsBlockSpan>>& ts_block_spans) :
    begin_ts_(begin_ts), interval_(interval), ts_block_spans_(ts_block_spans) {}

  KStatus SplitBlockSpans(std::vector<std::vector<std::shared_ptr<TsBlockSpan>>>& split_block_spans) {
    auto it = ts_block_spans_.begin();
    timestamp64 split_ts_point = begin_ts_ + interval_ - 1;
    std::vector<std::shared_ptr<TsBlockSpan>> cur_block_spans;
    while (it != ts_block_spans_.end()) {
      if (split_ts_point >= (*it)->GetLastTS()) {
        cur_block_spans.emplace_back(*it);
        ++it;
      } else if (split_ts_point < (*it)->GetFirstTS()) {
        split_block_spans.emplace_back(cur_block_spans);
        cur_block_spans.clear();
        split_ts_point += interval_;
      } else {
        int32_t split_row_index = binarySearch(*it, split_ts_point);
        assert(split_row_index >= 0 && split_row_index < ((*it)->GetRowNum() - 1));
        std::shared_ptr<TsBlockSpan> front_block_span;
        (*it)->SplitFront(split_row_index + 1, front_block_span);
        cur_block_spans.emplace_back(front_block_span);
      }
    }
    if (!cur_block_spans.empty()) {
      split_block_spans.emplace_back(cur_block_spans);
    }
    return KStatus::SUCCESS;
  }
};
}  //  namespace kwdbts
