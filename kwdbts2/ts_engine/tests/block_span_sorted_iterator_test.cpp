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

#include <gtest/gtest.h>
#include "test_util.h"
#include "ts_mem_segment_mgr.h"
#include "ts_block_span_sorted_iterator.h"

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

class TsSortedBlockSpanTest : public ::testing::Test {
 public:
  std::vector<AttributeInfo> schema_;
  std::list<TSMemSegRowData*> datas_;
  std::list<char*> row_datas_;
 public:
  TsSortedBlockSpanTest() {
    schema_.clear();
    AttributeInfo ts;
    ts.id = 1;
    ts.length = 16;
    ts.size = 16;
    ts.offset = 0;
    ts.type = DATATYPE::TIMESTAMP64_LSN;
    schema_.push_back(ts);
  }
  ~TsSortedBlockSpanTest() {
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

  shared_ptr<TsBlockSpan> AddBlockWithTs(std::vector<std::pair<timestamp64, TS_LSN>> tss) {
    auto cur_block = std::make_shared<TsMemSegBlock>(nullptr);
    for (size_t i = 0; i < tss.size(); i++) {
      TSMemSegRowData* data = new TSMemSegRowData(1, 1, 1, 1);
      datas_.push_back(data);
      char* row = reinterpret_cast<char*>(malloc(16));
      row_datas_.push_back(row);
      KTimestamp(row) = tss[i].first;
      KUint64(row + 8) = tss[i].second;
      data->SetData(tss[i].first, tss[i].second, {row, 16});
      bool ok = cur_block->InsertRow(data);
      EXPECT_TRUE(ok);
    }
    return std::make_shared<TsBlockSpan>(1, cur_block, 0, tss.size(), nullptr, 1);
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

TEST_F(TsSortedBlockSpanTest, empty) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  TsBlockSpanSortedIterator iter(spans);
  std::shared_ptr<kwdbts::TsBlockSpan> block_span;
  bool is_finished;
  do {
    auto s = iter.Next(block_span, &is_finished);
    EXPECT_TRUE(s == KStatus::SUCCESS);
    EXPECT_TRUE(is_finished);
  } while (!is_finished);
}

TEST_F(TsSortedBlockSpanTest, multiTS) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  int ts_num = 10;

  spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
  TsBlockSpanSortedIterator iter(spans);
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

TEST_F(TsSortedBlockSpanTest, multi_SameBlockSpan) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  int ts_num = 10;
  int span_num = 5;
  for (size_t i = 0; i < span_num; i++) {
    spans.push_back(GenBlockWithSpan1(1000, 1, ts_num, 1));
  }
  TsBlockSpanSortedIterator iter(spans);
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
  EXPECT_EQ(total_rows, ts_num);
}

TEST_F(TsSortedBlockSpanTest, multiBlockSpanWithCrossData) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  int ts_num = 10;
  int span_num = 5;
  for (size_t i = 0; i < span_num; i++) {
    spans.push_back(GenBlockWithSpan1(1000 + i, 1, ts_num, 1));
  }
  TsBlockSpanSortedIterator iter(spans);
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
  EXPECT_EQ(total_rows, ts_num + span_num - 1);
}

// TEST_F(TsSortedBlockSpanTest, BlockSpanNull) {
//   std::list<shared_ptr<TsBlockSpan>> spans;
//   spans.push_back(AddBlockWithTs({{1000, 100}}));
//   spans.push_back(AddBlockWithTs({{1000, 100}, {1000, 90}}));
//   TsBlockSpanSortedIterator iter(spans);
//   auto s = iter.Init();
//   EXPECT_TRUE(s == KStatus::SUCCESS);
//   bool is_finished;
//   std::shared_ptr<kwdbts::TsBlockSpan> block_span;
//   uint32_t total_rows = 0;
//   do {
//     auto s = iter.Next(block_span, &is_finished);
//     EXPECT_TRUE(s == KStatus::SUCCESS);
//     if (!is_finished) {
//       total_rows += block_span->GetRowNum();
//     }
//   } while (!is_finished);
//   EXPECT_EQ(total_rows, 2);
// }
