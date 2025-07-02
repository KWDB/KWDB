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

#include <functional>
#include <list>
#include <memory>
#include <vector>
#include "kwdb_type.h"
#include "ts_block.h"
#include "ts_mem_segment_mgr.h"

namespace kwdbts {

class TsBlockSpanSortedIterator {
 private:
  struct TsBlockSpanRowInfo {
    uint64_t entity_id = 0;
    timestamp64 ts = 0;
    TS_LSN lsn = 0;
    shared_ptr<TsBlockSpan> block_span = nullptr;
    int row_idx = 0;

    inline bool IsSameEntityAndTs(const TsBlockSpanRowInfo& other) const {
      return entity_id == other.entity_id && ts == other.ts;
    }

    inline bool operator<(const TsBlockSpanRowInfo& other) const {
      return entity_id != other.entity_id ? entity_id < other.entity_id
                                          : ts != other.ts ? ts < other.ts : lsn < other.lsn;
    }
    inline bool operator==(const TsBlockSpanRowInfo& other) const {
      return entity_id == other.entity_id && ts == other.ts && lsn == other.lsn;
    }
    inline bool operator<=(const TsBlockSpanRowInfo& other) const {
      return *this < other || *this == other;
    }
    inline bool operator>(const TsBlockSpanRowInfo& other) const {
      return entity_id != other.entity_id ? entity_id > other.entity_id
                                          : ts != other.ts ? ts > other.ts : lsn > other.lsn;
    }
    inline bool operator>=(const TsBlockSpanRowInfo& other) const {
      return *this > other || *this == other;
    }
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
        LOG_ERROR("insert row info error, row_info{entity_id=%lu, ts=%ld, lsn=%lu}, it{entity_id=%lu, ts=%ld, lsn=%lu}",
                  row_info.entity_id, row_info.ts, row_info.lsn, it->entity_id, it->ts, it->lsn);
      }
      assert(it != span_row_infos_.begin());
    }
    span_row_infos_.insert(it, row_info);
  }

  inline TsBlockSpanRowInfo defaultBlockSpanRowInfo() {
    if (!is_reverse_) {
      return {UINT64_MAX, INT64_MAX, UINT64_MAX};
    } else {
      return {0, INT64_MIN, 0};
    }
  }

  inline void binarySearch(TsBlockSpanRowInfo& target_row_info, shared_ptr<TsBlockSpan> block_span, int& row_idx) {
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
                                         *(block_span->GetLSNAddr(mid))};
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

  TsBlockSpanRowInfo getFirstRowInfo(std::shared_ptr<TsBlockSpan> block_span) {
    if (!is_reverse_) {
      return {block_span->GetEntityID(), block_span->GetTS(0), *block_span->GetLSNAddr(0), block_span, 0};
    } else {
      int row_idx = block_span->GetRowNum() - 1;
      return {block_span->GetEntityID(), block_span->GetTS(row_idx), *block_span->GetLSNAddr(row_idx),
              block_span, row_idx};
    }
  }

 public:
  explicit TsBlockSpanSortedIterator(std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     DedupRule dedup_rule = DedupRule::OVERRIDE,
                                     bool is_reverse = false) :
                                     block_spans_(block_spans), dedup_rule_(dedup_rule), is_reverse_(is_reverse) {}
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
    shared_ptr<TsBlockSpan> cur_block_span = span_row_infos_.front().block_span;
    TsBlockSpanRowInfo next_span_row_info = defaultBlockSpanRowInfo();
    if (span_row_infos_.size() > 1) {
      next_span_row_info = *(++span_row_infos_.begin());
    }

    // find the intersection of two BlockSpan.
    // find the data location in the first BlockSpan that is larger than
    // the timestamp of the first row of data in the second BlockSpan.
    int end_row_idx, row_idx;
    if (!is_reverse_) {
      end_row_idx = cur_block_span->GetRowNum() - 1;
    } else {
      end_row_idx = 0;
    }
    TsBlockSpanRowInfo cur_span_end_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(end_row_idx),
                                                *cur_block_span->GetLSNAddr(end_row_idx)};
    if (!is_reverse_ && cur_span_end_row_info <= next_span_row_info) {
      row_idx = cur_block_span->GetRowNum();
    } else if (is_reverse_ && cur_span_end_row_info >= next_span_row_info) {
      row_idx = -1;
    } else {
      binarySearch(next_span_row_info, cur_block_span, row_idx);
    }

    if (dedup_rule_ == DedupRule::OVERRIDE) {
      auto iter = span_row_infos_.begin();
      TsBlockSpanRowInfo dedup_row_info = defaultBlockSpanRowInfo();
      if (!is_reverse_) {
        int prev_row_idx = row_idx - 1;
        assert(prev_row_idx >= 0);
        TsBlockSpanRowInfo prev_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(prev_row_idx),
                                            *cur_block_span->GetLSNAddr(prev_row_idx)};
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
          cur_block_span->SplitFront(row_idx, block_span);
          iter = span_row_infos_.end();
        }
      } else {
        int next_row_idx = row_idx + 1;
        assert(next_row_idx <= cur_block_span->GetRowNum() - 1);
        TsBlockSpanRowInfo next_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(next_row_idx),
                                            *cur_block_span->GetLSNAddr(next_row_idx)};
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
      span_row_infos_.pop_front();
      if (cur_block_span->GetRowNum() != 0) {
        TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
        insertRowInfo(next_row_info);
      } else {
        cur_block_span->Clear();
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
        TsBlockSpanRowInfo prev_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(prev_row_idx),
                                            *cur_block_span->GetLSNAddr(prev_row_idx)};
        cur_block_span->SplitFront(row_idx, block_span);
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
        TsBlockSpanRowInfo next_row_info = {cur_block_span->GetEntityID(), cur_block_span->GetTS(next_row_idx),
                                            *cur_block_span->GetLSNAddr(next_row_idx)};
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
      span_row_infos_.pop_front();
      if (cur_block_span->GetRowNum() != 0) {
        TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
        insertRowInfo(next_row_info);
      } else {
        cur_block_span->Clear();
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
        cur_block_span->SplitFront(row_idx, block_span);
      } else {
        cur_block_span->SplitBack(span_row_infos_.begin()->row_idx - row_idx, block_span);
      }

      // check whether the current TsBlockSpan is empty.
      // If it is not empty, it needs to be readded to the linked list.
      span_row_infos_.pop_front();
      if (cur_block_span->GetRowNum() != 0) {
        TsBlockSpanRowInfo next_row_info = getFirstRowInfo(cur_block_span);
        insertRowInfo(next_row_info);
      } else {
        cur_block_span->Clear();
      }
    }
    return KStatus::SUCCESS;
  }
};

}  //  namespace kwdbts
