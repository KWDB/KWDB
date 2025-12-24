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

std::shared_ptr<TSBlkDataTypeConvert> empty_convert = nullptr;

void TsBlockSpan::SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span) {
  EXPECT_TRUE(row_num <= nrow_);
  SplitFrontImpl(row_num, front_span);
}

void TsBlockSpan::SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span) {
  EXPECT_TRUE(row_num <= nrow_);
  SplitBackImpl(row_num, back_span);
}

class SimulatedTsEntityBlock : public TsBlock {
 private:
  timestamp64 first_ts_;
  int rows_;
  uint64_t osn_;
 public:
  SimulatedTsEntityBlock() = delete;
  SimulatedTsEntityBlock(timestamp64 first_ts, int rows, uint64_t osn) {
    first_ts_ = first_ts;
    rows_ = rows;
    osn_ = osn;
  }
  bool HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) override {
    return 0 == begin_row_idx && row_num == rows_;
  }
  uint32_t GetBlockVersion() const override { return 1; }
  TSTableID GetTableId() override { return 1; }
  uint32_t GetTableVersion() override { return 1; }
  size_t GetRowNum() override { return rows_; }
  timestamp64 GetTS(int row_num, TsScanStats* ts_scan_stats = nullptr) override {
    return first_ts_ + row_num;
  }
  timestamp64 GetFirstTS() override { return first_ts_; }
  timestamp64 GetLastTS() override { return first_ts_ + rows_ - 1; }
  uint64_t GetBlockID() override { return 0; }
  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                             char** value, TsScanStats* ts_scan_stats = nullptr) override {
    return KStatus::FAIL;
  }
  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                               std::unique_ptr<TsBitmapBase>* bitmap, TsScanStats* ts_scan_stats = nullptr) override {
    return KStatus::FAIL;
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                                TSSlice& value, TsScanStats* ts_scan_stats = nullptr) override {
    return KStatus::FAIL;
  }
  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                          TsScanStats* ts_scan_stats = nullptr) override {
    return KStatus::FAIL;
  }

  void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) override {
    min_osn = osn_;
    max_osn = osn_;
    return;
  }

  uint64_t GetFirstOSN() override { return KStatus::FAIL; }

  uint64_t GetLastOSN() override { return KStatus::FAIL; }

  const uint64_t* GetOSNAddr(int row_num, TsScanStats* ts_scan_stats = nullptr) override { return &osn_; }

  KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, std::string& data) override {
    return KStatus::FAIL;
  }

  KStatus GetPreCount(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, uint16_t& count) override {
    return KStatus::FAIL;
  }
  KStatus GetPreSum(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats,
                            void* &pre_sum, bool& is_overflow) override {
    return KStatus::FAIL;
  }
  KStatus GetPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, void* &pre_max) override {
    return KStatus::FAIL;
  }
  KStatus GetPreMin(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats, void* &pre_min) override {
    return KStatus::FAIL;
  }
  KStatus GetVarPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_max) override {
    return KStatus::FAIL;
  }
  KStatus GetVarPreMin(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_min) override {
    return KStatus::FAIL;
  }
};
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

  std::shared_ptr<TsBlock> AddBlockWithTs(std::vector<timestamp64>& tss, uint64_t osn) {
    std::sort(tss.begin(), tss.end());
    auto cur_block = std::make_shared<TsMemSegBlock>(nullptr);

    for (size_t i = 0; i < tss.size(); i++) {
      TSMemSegRowData* data = new TSMemSegRowData(1, 1, 1, 1);
      datas_.push_back(data);
      char* row = reinterpret_cast<char*>(malloc(16));
      row_datas_.push_back(row);
      KTimestamp(row) = tss[i];
      KUint64(row + 8) = osn;
      data->SetData(tss[i], osn);
      data->SetRowData(TSSlice{row, 16});
      bool ok = cur_block->InsertRow(data);
      EXPECT_TRUE(ok);
    }
    return cur_block;
  }

  shared_ptr<TsBlockSpan> GenBlockWithSpan(std::vector<timestamp64> tss, uint64_t osn) {
    auto block = AddBlockWithTs(tss, osn);
    return std::make_shared<TsBlockSpan>(0, 1, block, 0, tss.size(), empty_convert, 1, nullptr);
  }
  shared_ptr<TsBlockSpan> GenBlockWithSpan1(timestamp64 start, int interval, int num, uint64_t osn) {
    if (num == 0) {
      return std::make_shared<TsBlockSpan>(0, 1, nullptr, 0, 0, empty_convert, 1, nullptr);
    }
    std::vector<timestamp64> tss;
    for (size_t i = 0; i < num; i++) {
      tss.push_back(start + i * interval);
    }
    auto block = AddBlockWithTs(tss, osn);
    return std::make_shared<TsBlockSpan>(0, 1, block, 0, tss.size(), empty_convert, 1, nullptr);
  }

  shared_ptr<TsBlockSpan> GenSimulatedEntityBlockSpan(timestamp64 start, int num, uint64_t osn) {
    if (num == 0) {
      return std::make_shared<TsBlockSpan>(0, 1, nullptr, 0, 0, empty_convert, 1, nullptr);
    }
    auto block = std::make_shared<SimulatedTsEntityBlock>(start, num, osn);
    return std::make_shared<TsBlockSpan>(0, 1, block, 0, num, empty_convert, 1, nullptr);
  }
};

TEST_F(TsBlockSpanSortedIteratorTest, empty) {
  std::list<shared_ptr<TsBlockSpan>> spans;
  TsBlockSpanSortedIterator iter(spans, nullptr);
  auto s = iter.Init();
  EXPECT_TRUE(s == KStatus::SUCCESS);
  std::shared_ptr<kwdbts::TsBlockSpan> block_span;
  bool is_finished;
  do {
    s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      total_rows = 0;
      do {
        s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      total_rows = 0;
      do {
        s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      do {
        s = iter.Next(block_span, &is_finished);
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
      TsBlockSpanSortedIterator iter(spans, nullptr, rule, true);
      auto s = iter.Init();
      EXPECT_TRUE(s == KStatus::SUCCESS);
      bool is_finished;
      std::shared_ptr<kwdbts::TsBlockSpan> block_span;
      total_rows = 0;
      do {
        s = iter.Next(block_span, &is_finished);
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

TEST_F(TsBlockSpanSortedIteratorTest, preAgg) {
  std::vector<DedupRule> dedup_rules = {DedupRule::KEEP, DedupRule::OVERRIDE, DedupRule::DISCARD};
  std::vector<int> total_row_count = {220, 120, 120};
  for (int i = 0; i < 3; ++i) {
    DedupRule rule = dedup_rules[i];
    uint32_t total_rows = 0;
    std::list<shared_ptr<TsBlockSpan>> spans;
    spans.push_back(GenSimulatedEntityBlockSpan(1000, 100, 1));
    spans.push_back(GenSimulatedEntityBlockSpan(1000, 120, 1));
    TsBlockSpanSortedIterator iter(spans, nullptr, rule);
    auto s = iter.Init();
    EXPECT_TRUE(s == KStatus::SUCCESS);
    bool is_finished;
    std::shared_ptr<kwdbts::TsBlockSpan> block_span;
    do {
      s = iter.Next(block_span, &is_finished);
      EXPECT_TRUE(s == KStatus::SUCCESS);
      if (!is_finished) {
        total_rows += block_span->GetRowNum();
        EXPECT_FALSE(block_span->HasPreAgg());
      }
    } while (!is_finished);
    EXPECT_TRUE(total_rows == total_row_count[i]);
  }
}
