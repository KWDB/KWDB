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

#include "ts_block_span_sorted_iterator.h"

#include <unistd.h>

#include <cstdint>

#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "test_util.h"
#include "ts_block.h"
#include "ts_entity_segment.h"
#include "ts_vgroup.h"

using namespace kwdbts;  // NOLINT

TsBlockSpan::TsBlockSpan(TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
                         const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, uint32_t scan_version)
    : block_(block), entity_id_(entity_id), start_row_(start), nrow_(nrow) {}

void TsBlockSpan::SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span) {
  EXPECT_TRUE(row_num <= nrow_);
  front_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_, row_num, nullptr, 1);
  // change current span info
  start_row_ += row_num;
  nrow_ -= row_num;
}

void TsBlockSpan::SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span) {
  EXPECT_TRUE(row_num <= nrow_);
  back_span = make_shared<TsBlockSpan>(entity_id_, block_, start_row_ + nrow_ - row_num, row_num, nullptr, 1);
  // change current span info
  nrow_ -= row_num;
}

class TsBlockSpanSortedIteratorTest : public ::testing::Test {
 public:
  std::vector<AttributeInfo> schema_;
  std::list<TSMemSegRowData*> datas_;
  std::list<char*> row_datas_;

 public:
  TsBlockSpanSortedIteratorTest() {
    schema_.clear();
    AttributeInfo ts;
    ts.id = 1;
    ts.length = 16;
    ts.size = 16;
    ts.offset = 0;
    ts.type = DATATYPE::TIMESTAMP64;
    schema_.push_back(ts);
  }
  ~TsBlockSpanSortedIteratorTest() {
    for (auto& data : datas_) {
      delete data;
    }
    datas_.clear();
    for (auto& row : row_datas_) {
      free(row);
    }
    row_datas_.clear();
  }

  std::shared_ptr<TsBlock> AddBlockWithTs(std::vector<timestamp64>& tss, TS_LSN lsn) {
    std::sort(tss.begin(), tss.end());
    auto cur_block = std::make_shared<TsMemSegBlock>(nullptr);

    for (size_t i = 0; i < tss.size(); i++) {
      TSMemSegRowData* data = new TSMemSegRowData(1, 1, 1, 1);
      datas_.push_back(data);
      char* row = reinterpret_cast<char*>(malloc(16));
      row_datas_.push_back(row);
      KTimestamp(row) = tss[i];
      KUint64(row + 8) = lsn;
      data->SetData(tss[i], lsn, {row, 16});
      bool ok = cur_block->InsertRow(data);
      EXPECT_TRUE(ok);
    }
    return cur_block;
  }

  shared_ptr<TsBlockSpan> GenBlockWithSpan(std::vector<timestamp64> tss, TS_LSN lsn) {
    auto block = AddBlockWithTs(tss, lsn);
    return std::make_shared<TsBlockSpan>(1, block, 0, tss.size(), nullptr, 1);
  }
  shared_ptr<TsBlockSpan> GenBlockWithSpan1(timestamp64 start, int interval, int num, TS_LSN lsn) {
    if (num == 0) {
      return std::make_shared<TsBlockSpan>(1, nullptr, 0, 0, nullptr, 1);
    }
    std::vector<timestamp64> tss;
    for (size_t i = 0; i < num; i++) {
      tss.push_back(start + i * interval);
    }
    auto block = AddBlockWithTs(tss, lsn);
    return std::make_shared<TsBlockSpan>(1, block, 0, tss.size(), nullptr, 1);
  }
};

TEST_F(TsBlockSpanSortedIteratorTest, empty) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  TsBlockSpanSortedIterator iter(spans);
  auto s = iter.Init();
  EXPECT_TRUE(s == KStatus::SUCCESS);
  std::shared_ptr<kwdbts::TsBlockSpan> block_span;
  bool is_finished;
  do {
    auto s = iter.Next(block_span, &is_finished);
    EXPECT_TRUE(s == KStatus::SUCCESS);
    EXPECT_TRUE(is_finished);
  } while (!is_finished);
}

TEST_F(TsBlockSpanSortedIteratorTest, multiTS) {
  int ts_num = 10;
  std::vector<DedupRule> dedup_rules = {DedupRule::KEEP, DedupRule::OVERRIDE, DedupRule::DISCARD};
  for (auto rule : dedup_rules) {
    uint32_t total_rows = 0;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
      TsBlockSpanSortedIterator iter(spans, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      EXPECT_TRUE(total_rows == ts_num);
    }
    total_rows = 0;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
      TsBlockSpanSortedIterator iter(spans, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      uint32_t total_rows = 0;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      EXPECT_TRUE(total_rows == ts_num);
    }
  }
}

TEST_F(TsBlockSpanSortedIteratorTest, multi_SameBlockSpan) {
  std::vector<DedupRule> dedup_rules = {DedupRule::KEEP, DedupRule::OVERRIDE, DedupRule::DISCARD};
  for (auto rule : dedup_rules) {
    uint32_t total_rows = 0;
    int ts_num = 10;
    int span_num = 5;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      for (size_t i = 0; i < span_num; i++) {
        spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
      }
      TsBlockSpanSortedIterator iter(spans, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      switch (rule) {
        case DedupRule::KEEP:
          EXPECT_TRUE(total_rows == span_num * ts_num);
          break;
        case DedupRule::OVERRIDE:
          EXPECT_TRUE(total_rows == ts_num);
          break;
        case DedupRule::DISCARD:
          EXPECT_TRUE(total_rows == ts_num);
          break;
        case DedupRule::MERGE:
        // not support yet.
        case DedupRule::REJECT:
          ASSERT_TRUE(false);
          break;
      }
    }
    total_rows = 0;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      for (size_t i = 0; i < span_num; i++) {
        spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
      }
      TsBlockSpanSortedIterator iter(spans, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      uint32_t total_rows = 0;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      switch (rule) {
        case DedupRule::KEEP:
          EXPECT_TRUE(total_rows == span_num * ts_num);
          break;
        case DedupRule::OVERRIDE:
          EXPECT_TRUE(total_rows == ts_num);
          break;
        case DedupRule::DISCARD:
          EXPECT_TRUE(total_rows == ts_num);
          break;
        case DedupRule::MERGE:
        // not support yet.
        case DedupRule::REJECT:
          ASSERT_TRUE(false);
          break;
      }
    }
  }
}

TEST_F(TsBlockSpanSortedIteratorTest, multiBlockSpanWithCrossData) {
  std::vector<DedupRule> dedup_rules = {DedupRule::KEEP, DedupRule::OVERRIDE, DedupRule::DISCARD};
  for (auto rule : dedup_rules) {
    int ts_num = 10;
    int span_num = 5;
    uint32_t total_rows = 0;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      for (size_t i = 0; i < span_num; i++) {
        spans.push_back(GenBlockWithSpan1(1000 + i, 1, ts_num, 1));
      }
      TsBlockSpanSortedIterator iter(spans, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      switch (rule) {
        case DedupRule::KEEP:
          EXPECT_EQ(total_rows, ts_num * span_num);
          break;
        case DedupRule::OVERRIDE:
          EXPECT_TRUE(total_rows == ts_num + span_num - 1);
          break;
        case DedupRule::DISCARD:
          EXPECT_TRUE(total_rows == ts_num + span_num - 1);
          break;
        case DedupRule::MERGE:
        // not support yet.
        case DedupRule::REJECT:
          ASSERT_TRUE(false);
          break;
      }
    }
    total_rows = 0;
    {
      std::list<shared_ptr<TsBlockSpan>> spans;
      for (size_t i = 0; i < span_num; i++) {
        spans.push_back(GenBlockWithSpan1(1000 + i, 1, ts_num, 1));
      }
      TsBlockSpanSortedIterator iter(spans, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      uint32_t total_rows = 0;
      do {
        auto s = iter.Next(block_span, &is_finished);
        EXPECT_TRUE(s == KStatus::SUCCESS);
        if (!is_finished) {
          total_rows += block_span->GetRowNum();
        }
      } while (!is_finished);
      switch (rule) {
        case DedupRule::KEEP:
          EXPECT_EQ(total_rows, ts_num * span_num);
          break;
        case DedupRule::OVERRIDE:
          EXPECT_TRUE(total_rows == ts_num + span_num - 1);
          break;
        case DedupRule::DISCARD:
          EXPECT_TRUE(total_rows == ts_num + span_num - 1);
          break;
        case DedupRule::MERGE:
        // not support yet.
        case DedupRule::REJECT:
          ASSERT_TRUE(false);
          break;
      }
    }
  }
}
