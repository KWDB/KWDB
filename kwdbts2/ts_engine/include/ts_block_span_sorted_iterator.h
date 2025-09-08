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

#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_block.h"
#include "ts_common.h"

namespace kwdbts {

class TsBlockSpanSortedIterator {
 private:
  struct TsBlockSpanRowInfo {
    uint64_t entity_id = 0;
    timestamp64 ts = 0;
    shared_ptr<TsBlockSpan> block_span = nullptr;
    int row_idx = 0;

    using CompareHelper = std::pair<TSEntityID, timestamp64>;
    CompareHelper GetCompareHelper() const { return std::make_pair(entity_id, ts); }
    inline bool IsSameEntityAndTs(const TsBlockSpanRowInfo& other) const {
      return entity_id == other.entity_id && ts == other.ts;
    }
    // lsn only load if  ts is equal.
    inline TS_LSN GetLSN() const {
      return  block_span != nullptr ? *(block_span->GetLSNAddr(row_idx)) : 0;
    }

    inline bool operator<(const TsBlockSpanRowInfo& other) const {
      if (entity_id == other.entity_id) {
        if (ts == other.ts) {
          return GetLSN() < other.GetLSN();
        }
        return ts < other.ts;
      }
      return entity_id < other.entity_id;
    }
    inline bool operator==(const TsBlockSpanRowInfo& other) const {
      auto ret = memcmp(this, &other, 16);
      if (ret == 0) {
        ret = GetLSN() - other.GetLSN();
      }
      return ret == 0;
    }
    inline bool operator<=(const TsBlockSpanRowInfo& other) const { return *this < other || *this == other; }
    inline bool operator>(const TsBlockSpanRowInfo& other) const { return !(*this <= other); }
    inline bool operator>=(const TsBlockSpanRowInfo& other) const { return !(*this < other); }
  };
  std::list<shared_ptr<TsBlockSpan>> block_spans_;
  DedupRule dedup_rule_ = DedupRule::OVERRIDE;
  bool is_reverse_ = false;
  std::list<TsBlockSpanRowInfo> span_row_infos_;

  void insertRowInfo(TsBlockSpanRowInfo& row_info, bool check_insert = false) {
    assert(row_info.block_span->GetRowNum() > 0);
    auto it = span_row_infos_.begin();
    while (it != span_row_infos_.end()) {
      if ((!is_reverse_ && *it > row_info) || (is_reverse_ && *it < row_info)) {
        break;
      }
      ++it;
    }
    if (check_insert) {
      if (it == span_row_infos_.begin() && it != span_row_infos_.end()) {
        LOG_ERROR("insert row info error, row_info{entity_id=%lu, ts=%ld}, it{entity_id=%lu, ts=%ld}",
                  row_info.entity_id, row_info.ts, it->entity_id, it->ts);
      }
      assert(it != span_row_infos_.begin());
    }
    span_row_infos_.insert(it, row_info);
  }

  inline TsBlockSpanRowInfo defaultBlockSpanRowInfo() {
    if (!is_reverse_) {
      return {UINT64_MAX, INT64_MAX};
    } else {
      return {0, INT64_MIN};
    }
  }

  inline void binarySearch(TsBlockSpanRowInfo& target_row_info, shared_ptr<TsBlockSpan>& block_span, int& row_idx) {
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
      TsBlockSpanRowInfo cur_row_info = {block_span->GetEntityID(), block_span->GetTS(mid), block_span, mid};
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

  TsBlockSpanRowInfo getFirstRowInfo(std::shared_ptr<TsBlockSpan>& block_span) {
    if (!is_reverse_) {
      return {block_span->GetEntityID(), block_span->GetFirstTS(), block_span, 0};
    } else {
      int row_idx = block_span->GetRowNum() - 1;
      return {block_span->GetEntityID(), block_span->GetLastTS(), block_span, row_idx};
    }
  }

  inline void getTsAndLSN(std::shared_ptr<TsBlockSpan>& block_span, int row_idx, timestamp64& row_ts, TS_LSN& row_lsn) {
    if (row_idx == 0) {
      row_ts = block_span->GetFirstTS();
      row_lsn = block_span->GetFirstLSN();
    } else if (row_idx == block_span->GetRowNum() - 1) {
      row_ts = block_span->GetLastTS();
      row_lsn = block_span->GetLastLSN();
    } else {
      row_ts = block_span->GetTS(row_idx);
      row_lsn = *block_span->GetLSNAddr(row_idx);
    }
  }

 public:
  explicit TsBlockSpanSortedIterator(std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     DedupRule dedup_rule = DedupRule::OVERRIDE,
                                     bool is_reverse = false) :
                                     block_spans_(std::move(block_spans)),
                                     dedup_rule_(dedup_rule), is_reverse_(is_reverse) {}
  explicit TsBlockSpanSortedIterator(std::vector<std::list<shared_ptr<TsBlockSpan>>>& block_spans,
                                     DedupRule dedup_rule = DedupRule::OVERRIDE,
                                     bool is_reverse = false) : dedup_rule_(dedup_rule), is_reverse_(is_reverse) {
    for (auto& block_span_list : block_spans) {
      block_spans_.merge(block_span_list);
    }
  }
  ~TsBlockSpanSortedIterator() = default;
  TsBlockSpanSortedIterator(const TsBlockSpanSortedIterator& other) {
    block_spans_ = other.block_spans_;
  }

  KStatus Init() {
    for (auto& block_span : block_spans_) {
      span_row_infos_.push_back(getFirstRowInfo(block_span));
    }
    if (!is_reverse_) {
      span_row_infos_.sort();
    } else {
      span_row_infos_.sort(std::greater<TsBlockSpanRowInfo>());
    }
    return KStatus::SUCCESS;
  }

  KStatus Next(shared_ptr<TsBlockSpan>& block_span, bool* is_finished) {
    if (span_row_infos_.empty()) {
      *is_finished = true;
      return KStatus::SUCCESS;
    } else {
      *is_finished = false;
    }
    shared_ptr<TsBlockSpan>& cur_block_span = span_row_infos_.front().block_span;
    TsBlockSpanRowInfo next_span_row_info = defaultBlockSpanRowInfo();
    if (span_row_infos_.size() > 1) {
      next_span_row_info = *(++span_row_infos_.begin());
    }

    // find the intersection of two BlockSpan.
    // find the data location in the first BlockSpan that is larger than
    // the timestamp of the first row of data in the second BlockSpan.
    timestamp64 cur_span_end_ts;
    int end_row_idx, row_idx;
    if (!is_reverse_) {
      cur_span_end_ts = cur_block_span->GetLastTS();
      end_row_idx = cur_block_span->GetRowNum() - 1;
    } else {
      cur_span_end_ts = cur_block_span->GetFirstTS();
      end_row_idx = 0;
    }
    TsBlockSpanRowInfo cur_span_end_row_info = {cur_block_span->GetEntityID(), cur_span_end_ts, cur_block_span, end_row_idx};
    if (!is_reverse_ && cur_span_end_row_info <= next_span_row_info) {
      row_idx = cur_block_span->GetRowNum();
    } else if (is_reverse_ && cur_span_end_row_info >= next_span_row_info) {
      row_idx = -1;
    } else {
      binarySearch(next_span_row_info, cur_block_span, row_idx);
    }

    timestamp64 cur_span_row_ts;
    TS_LSN cur_span_row_lsn;
    if (dedup_rule_ == DedupRule::OVERRIDE) {
      auto iter = span_row_infos_.begin();
      TsBlockSpanRowInfo dedup_row_info = defaultBlockSpanRowInfo();
      if (!is_reverse_) {
        int prev_row_idx = row_idx - 1;
        assert(prev_row_idx >= 0);
        getTsAndLSN(cur_block_span, prev_row_idx, cur_span_row_ts, cur_span_row_lsn);
        TsBlockSpanRowInfo prev_row_info = {cur_block_span->GetEntityID(), cur_span_row_ts};
        if (prev_row_info.IsSameEntityAndTs(next_span_row_info)) {
          if (prev_row_idx != 0) {
            cur_block_span->SplitFront(prev_row_idx, block_span);
            iter = span_row_infos_.end();
          } else {
            // need dedup
            dedup_row_info = next_span_row_info;
            iter++;
          }
          cur_block_span->TrimFront(1);
        } else {
          if (cur_block_span->GetRowNum() == row_idx) {
            block_span = std::move(cur_block_span);
          } else {
            cur_block_span->SplitFront(row_idx, block_span);
          }
          iter = span_row_infos_.end();
        }
      } else {
        int next_row_idx = row_idx + 1;
        assert(next_row_idx <= cur_block_span->GetRowNum() - 1);
        getTsAndLSN(cur_block_span, next_row_idx, cur_span_row_ts, cur_span_row_lsn);
        TsBlockSpanRowInfo next_row_info = {cur_block_span->GetEntityID(), cur_span_row_ts};
        cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - row_idx, block_span);
        if (next_row_info.IsSameEntityAndTs(next_span_row_info)) {
          // need dedup
          dedup_row_info = next_row_info;
          iter++;
        } else {
          iter = span_row_infos_.end();
        }
      }

      // check whether the current TsBlockSpan is empty.
      // If it is not empty, it needs to be readded to the linked list.
      if (cur_block_span) {
        if (cur_block_span->GetRowNum() != 0) {
          TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
          span_row_infos_.pop_front();
          insertRowInfo(next_row_info);
        } else {
          cur_block_span->Clear();
          span_row_infos_.pop_front();
        }
      } else {
        span_row_infos_.pop_front();
      }

      // dealing with duplicate data in other TsBlockSpan
      while (iter != span_row_infos_.end()) {
        if (iter->IsSameEntityAndTs(dedup_row_info)) {
          if (!is_reverse_) {
            iter->block_span->SplitFront(1, block_span);
          } else {
            iter->block_span->TrimBack(1);
          }
          if (iter->block_span->GetRowNum() != 0) {
            TsBlockSpanRowInfo next_row_info = getFirstRowInfo(iter->block_span);
            insertRowInfo(next_row_info, true);
          } else {
            iter->block_span->Clear();
          }
          iter = span_row_infos_.erase(iter);
        } else {
          break;
        }
      }
    } else if (dedup_rule_ == DedupRule::DISCARD) {
      auto iter = span_row_infos_.begin();
      TsBlockSpanRowInfo dedup_row_info = defaultBlockSpanRowInfo();
      if (!is_reverse_) {
        int prev_row_idx = row_idx - 1;
        assert(prev_row_idx >= 0);
        getTsAndLSN(cur_block_span, prev_row_idx, cur_span_row_ts, cur_span_row_lsn);
        TsBlockSpanRowInfo prev_row_info = {cur_block_span->GetEntityID(), cur_span_row_ts};
        if (cur_block_span->GetRowNum() == row_idx) {
          block_span = std::move(cur_block_span);
        } else {
          cur_block_span->SplitFront(row_idx, block_span);
        }
        if (prev_row_info.IsSameEntityAndTs(next_span_row_info)) {
          // need dedup
          dedup_row_info = next_span_row_info;
          iter++;
        } else {
          iter = span_row_infos_.end();
        }
      } else {
        int next_row_idx = row_idx + 1;
        assert(next_row_idx <= cur_block_span->GetRowNum() - 1);
        getTsAndLSN(cur_block_span, next_row_idx, cur_span_row_ts, cur_span_row_lsn);
        TsBlockSpanRowInfo next_row_info = {cur_block_span->GetEntityID(), cur_span_row_ts};
        if (next_row_info.IsSameEntityAndTs(next_span_row_info)) {
          if (next_row_idx != span_row_infos_.begin()->row_idx) {
            cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - next_row_idx, block_span);
            iter = span_row_infos_.end();
          } else {
            // need dedup
            dedup_row_info = next_span_row_info;
            iter++;
          }
          cur_block_span->TrimBack(1);
        } else {
          cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - row_idx, block_span);
          iter = span_row_infos_.end();
        }
      }

      // check whether the current TsBlockSpan is empty.
      // If it is not empty, it needs to be readded to the linked list.
      if (cur_block_span) {
        if (cur_block_span->GetRowNum() != 0) {
          TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
          span_row_infos_.pop_front();
          insertRowInfo(next_row_info);
        } else {
          cur_block_span->Clear();
          span_row_infos_.pop_front();
        }
      } else {
        span_row_infos_.pop_front();
      }

      // dealing with duplicate data in other TsBlockSpan
      while (iter != span_row_infos_.end()) {
        if (iter->IsSameEntityAndTs(dedup_row_info)) {
          if (!is_reverse_) {
            iter->block_span->TrimFront(1);
          } else {
            iter->block_span->SplitBack(1, block_span);
          }
          if (iter->block_span->GetRowNum() != 0) {
            TsBlockSpanRowInfo next_row_info = getFirstRowInfo(iter->block_span);
            insertRowInfo(next_row_info, true);
          } else {
            iter->block_span->Clear();
          }
          iter = span_row_infos_.erase(iter);
        } else {
          break;
        }
      }
    } else {
      if (!is_reverse_) {
        if (cur_block_span->GetRowNum() == row_idx) {
          block_span = std::move(cur_block_span);
        } else {
          cur_block_span->SplitFront(row_idx, block_span);
        }
      } else {
        cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - row_idx, block_span);
      }

      // check whether the current TsBlockSpan is empty.
      // If it is not empty, it needs to be readded to the linked list.
      if (cur_block_span) {
        if (cur_block_span->GetRowNum() != 0) {
          TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
          span_row_infos_.pop_front();
          insertRowInfo(next_row_info);
        } else {
          cur_block_span->Clear();
          span_row_infos_.pop_front();
        }
      } else {
        span_row_infos_.pop_front();
      }
    }
    return KStatus::SUCCESS;
  }
};

}  //  namespace kwdbts
