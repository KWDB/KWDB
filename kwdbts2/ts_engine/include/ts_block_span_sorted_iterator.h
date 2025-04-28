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

struct TsIteratorRowInfo {
  uint64_t entity_id;
  timestamp64 ts;
  uint64_t seq_no;

  uint64_t iterator_idx;
  int row_idx;

  inline bool operator<(const TsIteratorRowInfo& other) const {
    return entity_id != other.entity_id ? entity_id < other.entity_id
                                        : ts != other.ts ? ts < other.ts : seq_no > other.seq_no;
  }
  inline bool operator==(const TsIteratorRowInfo& other) const {
    return entity_id == other.entity_id && ts == other.ts && seq_no == other.seq_no;
  }
  inline bool operator<=(const TsIteratorRowInfo& other) const {
    return *this < other || *this == other;
  }
};

class TsBlockSpanSortIterator {
 protected:
  std::list<TsBlockSpan> block_spans_;

 public:
  explicit TsBlockSpanSortIterator(std::list<TsBlockSpan>& block_spans) :
                          block_spans_(block_spans) {}
  TsBlockSpanSortIterator() {}
  TsBlockSpanSortIterator(const TsBlockSpanSortIterator& other) {
    block_spans_ = other.block_spans_;
  }

  virtual KStatus Init() = 0;

  virtual KStatus Next(TsBlockSpan* block_span, bool* is_finished) {
    if (block_spans_.empty()) {
      *is_finished = true;
      return KStatus::SUCCESS;
    }
    TsBlockSpan& cur_block_span = block_spans_.front();
    TsIteratorRowInfo next_span_row_info{UINT64_MAX, INT64_MAX, UINT64_MAX};
    if (block_spans_.size() > 1) {
      auto iter = block_spans_.begin();
      iter++;
      next_span_row_info = {iter->entity_id, iter->block->GetTS(cur_block_span.start_row),
                            *iter->block->GetSeqNoAddr(cur_block_span.start_row)};
    }

    int end_row_idx = cur_block_span.start_row + cur_block_span.nrow - 1;
    TsIteratorRowInfo cur_span_row_info = {cur_block_span.entity_id, cur_block_span.block->GetTS(end_row_idx),
                                           *cur_block_span.block->GetSeqNoAddr(end_row_idx)};
    int row_idx = cur_block_span.start_row;
    if (cur_span_row_info <= next_span_row_info) {
      row_idx = cur_block_span.start_row + cur_block_span.nrow;
    } else {
      for (; row_idx < cur_block_span.start_row + cur_block_span.nrow; ++row_idx) {
        cur_span_row_info = {cur_block_span.entity_id, cur_block_span.block->GetTS(row_idx),
                             *(cur_block_span.block->GetSeqNoAddr(row_idx))};
        if (next_span_row_info < cur_span_row_info) {
          break;
        }
      }
    }

    block_span->entity_id = cur_block_span.entity_id;
    block_span->start_row = cur_block_span.start_row;
    block_span->nrow = row_idx - cur_block_span.start_row;
    block_span->block = cur_block_span.block;
    *is_finished = false;

    if (row_idx == cur_block_span.start_row + cur_block_span.nrow) {
      block_spans_.pop_front();
    } else {
      cur_block_span.start_row = row_idx;
      cur_block_span.nrow = cur_block_span.start_row + cur_block_span.nrow - row_idx;
      block_spans_.sort();
    }
    return KStatus::SUCCESS;
  }
};

class TsMemSegmentBlockSpanSortIterator : public TsBlockSpanSortIterator {
 public:
  explicit TsMemSegmentBlockSpanSortIterator(std::list<TsBlockSpan>& block_spans) :
    TsBlockSpanSortIterator(block_spans) {}
  ~TsMemSegmentBlockSpanSortIterator() {}

  KStatus Init() override {
    // 1.Sort all data of blocks in all block spans
    for (auto& block_span : block_spans_) {
      TsMemSegBlock* block = dynamic_cast<TsMemSegBlock *>(block_span.block.get());
      block->Sort();
    }
    // 2.Sort between block spans
    block_spans_.sort();
    return KStatus::SUCCESS;
  }
};

class TsLastSegmentBlockSpanSortIterator : public TsBlockSpanSortIterator {
 public:
  explicit TsLastSegmentBlockSpanSortIterator(std::list<TsBlockSpan>& block_spans) :
    TsBlockSpanSortIterator(block_spans) {}
  ~TsLastSegmentBlockSpanSortIterator() {}

  KStatus Init() override {
    // The block span of the last segment is already in order.
    return KStatus::SUCCESS;
  }

  KStatus Next(TsBlockSpan* block_span, bool* is_finished) override {
    if (block_spans_.empty()) {
      block_span = nullptr;
      *is_finished = true;
      return KStatus::SUCCESS;
    }
    *block_span = block_spans_.front();
    block_spans_.pop_front();
    return KStatus::SUCCESS;
  }
 private:
};

class TsEntitySegmentBlockSpanSortIterator : public TsBlockSpanSortIterator {
 public:
  explicit TsEntitySegmentBlockSpanSortIterator(std::list<TsBlockSpan>& block_spans) :
    TsBlockSpanSortIterator(block_spans) {}
  ~TsEntitySegmentBlockSpanSortIterator() {}

  KStatus Init() override {
    block_spans_.sort();
    return KStatus::SUCCESS;
  }
};

class TsSegmentsBlockSpanSortIterator {
 private:
  std::vector<std::shared_ptr<TsBlockSpanSortIterator>>& block_span_iterators_;
  std::vector<TsBlockSpan> cur_block_spans_;
  std::list<TsIteratorRowInfo> row_infos_;

  void insertRowInfo(TsIteratorRowInfo& row_info) {
    auto it = row_infos_.begin();
    while (it != row_infos_.end() && *it < row_info) {
      ++it;
    }
    row_infos_.insert(it, row_info);
  }

 public:
  explicit TsSegmentsBlockSpanSortIterator(std::vector<std::shared_ptr<TsBlockSpanSortIterator>>& block_span_iterators) :
    block_span_iterators_(block_span_iterators) {}
  ~TsSegmentsBlockSpanSortIterator() {}

  KStatus Init() {
    cur_block_spans_.resize(block_span_iterators_.size());
    for (size_t i = 0; i < block_span_iterators_.size(); ++i) {
      bool is_finished = false;
      KStatus s = block_span_iterators_[i]->Next(&cur_block_spans_[i], &is_finished);
      if (s == KStatus::FAIL) {
        return s;
      }
      if (!is_finished) {
        uint64_t entity_id = cur_block_spans_[i].GetEntityID();
        timestamp64 ts = cur_block_spans_[i]->GetTS(0);
        uint64_t seq_no = *(cur_block_spans_[i]->GetSeqNoAddr(0));
        TsIteratorRowInfo row_info{entity_id, ts, seq_no, i, cur_block_spans_[i].start_row};
        insertRowInfo(row_info);
      }
    }
    row_infos_.sort();
    return KStatus::SUCCESS;
  }

  KStatus Next(TsBlockSpan* block_span, bool* is_finished) {
    if (row_infos_.empty()) {
      block_span = nullptr;
      *is_finished = true;
      return KStatus::SUCCESS;
    }

    TsIteratorRowInfo cur_span_row_info = row_infos_.front();
    row_infos_.pop_front();
    TsIteratorRowInfo next_span_row_info{UINT64_MAX, INT64_MAX, UINT64_MAX};
    if (!row_infos_.empty()) {
      next_span_row_info = row_infos_.front();
    }

    TsBlockSpan& cur_block_span = cur_block_spans_[cur_span_row_info.iterator_idx];
    uint32_t n_rows = cur_block_span.nrow;
    uint32_t row_idx = cur_span_row_info.row_idx;
    std::shared_ptr<TsBlock>& block = cur_block_span.block;
    TsIteratorRowInfo new_row_info = {cur_block_spans_[cur_span_row_info.iterator_idx].GetEntityID(),
                                      block->GetTS(cur_block_span.start_row + n_rows - 1),
                                      *(block->GetSeqNoAddr(cur_block_span.start_row + n_rows - 1))};
    if (new_row_info <= next_span_row_info) {
      row_idx = cur_block_span.start_row + n_rows;
    } else {
      for (; row_idx < cur_block_span.start_row + n_rows; ++row_idx) {
        new_row_info = {cur_block_spans_[cur_span_row_info.iterator_idx].GetEntityID(),
                        block->GetTS(row_idx), *(block->GetSeqNoAddr(row_idx))};
        if (next_span_row_info < new_row_info) {
          new_row_info.iterator_idx = cur_span_row_info.iterator_idx;
          new_row_info.row_idx = row_idx;
          insertRowInfo(new_row_info);
          break;
        }
      }
    }

    block_span->entity_id = cur_block_span.GetEntityID();
    block_span->start_row = cur_span_row_info.row_idx;
    block_span->nrow = row_idx - cur_span_row_info.row_idx;
    block_span->block = cur_block_span.block;
    *is_finished = false;

    if (row_idx == cur_block_span.start_row + n_rows) {
      auto iter_idx = cur_span_row_info.iterator_idx;
      bool iter_is_finished = false;
      KStatus s = block_span_iterators_[iter_idx]->Next(&cur_block_spans_[iter_idx], &iter_is_finished);
      if (s == KStatus::FAIL) {
        return s;
      }
      if (!iter_is_finished) {
        new_row_info = {cur_block_spans_[iter_idx].GetEntityID(), block->GetTS(cur_block_span.start_row),
                        *(block->GetSeqNoAddr(cur_block_span.start_row)), iter_idx, cur_block_span.start_row};
        insertRowInfo(new_row_info);
      }
    }
    return KStatus::SUCCESS;
  }
};

}  //  namespace kwdbts
