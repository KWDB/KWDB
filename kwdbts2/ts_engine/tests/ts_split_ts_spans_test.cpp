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

#include "test_util.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_entity_segment.h"
#include "ts_split_block_spans.h"

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

class TsSplitBlockSpansTest : public ::testing::Test {
 public:
  std::vector<AttributeInfo> schema_;
  std::list<TSMemSegRowData*> datas_;
  std::list<char*> row_datas_;

 public:
  TsSplitBlockSpansTest() {
    schema_.clear();
    AttributeInfo ts;
    ts.id = 1;
    ts.length = 16;
    ts.size = 16;
    ts.offset = 0;
    ts.type = DATATYPE::TIMESTAMP64;
    schema_.push_back(ts);
  }
  ~TsSplitBlockSpansTest() {
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

  shared_ptr<TsBlockSpan> GenBlockWithSpan(timestamp64 start, int interval, int num, uint64_t osn) {
    if (num == 0) {
      return std::make_shared<TsBlockSpan>(0, 1, nullptr, 0, 0, empty_convert, nullptr);
    }
    std::vector<timestamp64> tss;
    for (size_t i = 0; i < num; i++) {
      tss.push_back(start + i * interval);
    }
    auto block = AddBlockWithTs(tss, osn);
    return std::make_shared<TsBlockSpan>(0, 1, block, 0, tss.size(), empty_convert, nullptr);
  }

  void GetFirstLastTsAndRowNum(std::vector<shared_ptr<TsBlockSpan>> block_spans, timestamp64& first_ts, timestamp64& last_ts, uint32_t& row_num) {
    first_ts = INT64_MAX;
    last_ts = INT64_MIN;
    row_num = 0;
    for (auto it : block_spans) {
      row_num += it->GetRowNum();
      if (it->GetFirstTS() < first_ts) {
        first_ts = it->GetFirstTS();
      }
      if (it->GetLastTS() > last_ts) {
        last_ts = it->GetLastTS();
      }
    }
  }
};

TEST_F(TsSplitBlockSpansTest, basic) {
  int ts_num = 100;
  timestamp64 start_time = 1010;
  timestamp64 begin_ts = 1000, interval = 100;

  std::vector<shared_ptr<TsBlockSpan>> spans1;
  spans1.push_back(GenBlockWithSpan(start_time, 1, 5, 1));
  spans1.push_back(GenBlockWithSpan(start_time + 10, 1, 20, 1));
  spans1.push_back(GenBlockWithSpan(start_time + 50, 1, 12, 1));
  TsBlockSpanSplitter span_splitter1(begin_ts, interval, spans1);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans1;
  std::vector<uint64_t> block_spans_index;
  span_splitter1.SplitBlockSpans(split_block_spans1, block_spans_index);
  EXPECT_EQ(split_block_spans1.size(), 1);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans1 = split_block_spans1.front();
  EXPECT_EQ(blockspans1.size(), 3);
  uint32_t row_num1;
  timestamp64 begin_ts1, end_ts1;
  GetFirstLastTsAndRowNum(blockspans1, begin_ts1, end_ts1, row_num1);
  EXPECT_EQ(row_num1, 5 + 20 + 12);
  EXPECT_LE(begin_ts1, start_time);
  EXPECT_GE(end_ts1, start_time + 50 + 12 - 1);

  std::vector<shared_ptr<TsBlockSpan>> spans2;
  spans2.push_back(GenBlockWithSpan(start_time, 1, ts_num, 1));
  TsBlockSpanSplitter span_splitter2(begin_ts, interval, spans2);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans2;
  span_splitter2.SplitBlockSpans(split_block_spans2, block_spans_index);
  EXPECT_EQ(split_block_spans2.size(), 2);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans2 = split_block_spans2.front();
  uint32_t row_num2;
  timestamp64 begin_ts2, end_ts2;
  GetFirstLastTsAndRowNum(blockspans2, begin_ts2, end_ts2, row_num2);
  EXPECT_EQ(begin_ts + interval - start_time, row_num2);
  EXPECT_LE(begin_ts, begin_ts2);
  EXPECT_GE(begin_ts + interval - 1, end_ts2);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans3 = split_block_spans2.back();
  uint32_t row_num3;
  timestamp64 begin_ts3, end_ts3;
  GetFirstLastTsAndRowNum(blockspans3, begin_ts3, end_ts3, row_num3);
  EXPECT_EQ((start_time + ts_num) - (begin_ts + interval), row_num3);
  EXPECT_LE(begin_ts + interval, begin_ts3);
  EXPECT_GE(begin_ts + 2 * interval - 1, end_ts3);
}

TEST_F(TsSplitBlockSpansTest, multi_spans_1) {
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 100;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  // timebucket 1
  spans.push_back(GenBlockWithSpan(start_time, 1, 10, 1));
  spans.push_back(GenBlockWithSpan(start_time + 10, 1, 50, 1));
  spans.push_back(GenBlockWithSpan(start_time + 70, 1, 20, 1));
  // timebucket 2
  spans.push_back(GenBlockWithSpan(start_time + interval, 1, 20, 1));
  spans.push_back(GenBlockWithSpan(start_time + interval + 50, 1, 30, 1));
  // timebucket 3
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 1, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 5, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 10, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 14, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 27, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 38, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 39, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 50, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 80, 1, 1, 1));

  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), 3);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans1 = split_block_spans[0];
  EXPECT_EQ(blockspans1.size(), 3);
  uint32_t row_num1;
  timestamp64 begin_ts1, end_ts1;
  GetFirstLastTsAndRowNum(blockspans1, begin_ts1, end_ts1, row_num1);
  EXPECT_EQ(begin_ts1, start_time);
  EXPECT_EQ(end_ts1, start_time + 70 + 20 - 1);
  EXPECT_EQ(row_num1, 10 + 50 + 20);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans2 = split_block_spans[1];
  EXPECT_EQ(blockspans2.size(), 2);
  uint32_t row_num2;
  timestamp64 begin_ts2, end_ts2;
  GetFirstLastTsAndRowNum(blockspans2, begin_ts2, end_ts2, row_num2);
  EXPECT_EQ(begin_ts2, start_time + interval);
  EXPECT_EQ(end_ts2, start_time + interval + 50 + 30 - 1);
  EXPECT_EQ(row_num2, 20 + 30);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans3 = split_block_spans[2];
  EXPECT_EQ(blockspans3.size(), 10);
  uint32_t row_num3;
  timestamp64 begin_ts3, end_ts3;
  GetFirstLastTsAndRowNum(blockspans3, begin_ts3, end_ts3, row_num3);
  EXPECT_EQ(begin_ts3, start_time + 2 * interval);
  EXPECT_EQ(end_ts3, start_time + 2 * interval + 80);
  EXPECT_EQ(row_num3, 10);
}

TEST_F(TsSplitBlockSpansTest, multi_spans_2) {
  timestamp64 begin_ts = 1000000, interval = 10000;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  const uint32_t bucket_num = 5;
  for (int i = 0; i < bucket_num; ++i) {
    for (int j = 0; j < interval; ++j) {
      spans.push_back(GenBlockWithSpan(begin_ts + i * interval + j, 1, 1, 1));
    }
  }

  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), bucket_num);

  for (int i = 0; i < bucket_num; ++i) {
    EXPECT_EQ(split_block_spans[i].size(), interval);
  }
}

TEST_F(TsSplitBlockSpansTest, multi_spans_3) {
  timestamp64 begin_ts = 1000000, interval = 10000;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  const uint32_t bucket_num = 5;
  for (int i = 0; i < bucket_num; ++i) {
    for (int j = 0; j < interval / (i + 1); ++j) {
      spans.push_back(GenBlockWithSpan(begin_ts + i * interval + j, 1, 1, 1));
    }
  }

  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), bucket_num);

  for (int i = 0; i < bucket_num; ++i) {
    EXPECT_EQ(split_block_spans[i].size(), interval / (i + 1));
    uint32_t row_num;
    timestamp64 start_ts, end_ts;
    GetFirstLastTsAndRowNum(split_block_spans[i], start_ts, end_ts, row_num);
    EXPECT_EQ(begin_ts + i * interval, start_ts);
    EXPECT_EQ(begin_ts + i * interval + interval / (i + 1) - 1, end_ts);
    EXPECT_EQ(interval / (i + 1), row_num);
  }
}

TEST_F(TsSplitBlockSpansTest, has_empty_time_bucket) {
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 100;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  // timebucket 1
  spans.push_back(GenBlockWithSpan(start_time, 1, 10, 1));
  spans.push_back(GenBlockWithSpan(start_time + 10, 1, 50, 1));
  spans.push_back(GenBlockWithSpan(start_time + 70, 1, 20, 1));
  // timebucket 2 empty
  // timebucket 3
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 1, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 5, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 10, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 14, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 27, 1, 1, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 38, 1, 1, 1));

  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), 2);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans1 = split_block_spans[0];
  EXPECT_EQ(blockspans1.size(), 3);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans2 = split_block_spans[1];
  EXPECT_EQ(blockspans2.size(), 7);
}

TEST_F(TsSplitBlockSpansTest, boundary_value1) {
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 100;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  spans.push_back(GenBlockWithSpan(start_time, 1, 101, 1));
  spans.push_back(GenBlockWithSpan(start_time + interval + 1, 1, 100, 1));
  spans.push_back(GenBlockWithSpan(start_time + 2 * interval + 1, 1, 100, 1));

  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), 4);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans1 = split_block_spans[0];
  EXPECT_EQ(blockspans1.size(), 1);
  uint32_t row_num1;
  timestamp64 begin_ts1, end_ts1;
  GetFirstLastTsAndRowNum(blockspans1, begin_ts1, end_ts1, row_num1);
  EXPECT_EQ(row_num1, 100);
  EXPECT_LE(begin_ts1, start_time);
  EXPECT_GE(end_ts1, start_time + interval - 1);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans2 = split_block_spans[1];
  EXPECT_EQ(blockspans2.size(), 2);
  uint32_t row_num2;
  timestamp64 begin_ts2, end_ts2;
  GetFirstLastTsAndRowNum(blockspans2, begin_ts2, end_ts2, row_num2);
  EXPECT_EQ(row_num2, 100);
  EXPECT_LE(begin_ts2, start_time + interval);
  EXPECT_GE(end_ts2, start_time + 2 * interval - 1);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans3 = split_block_spans[2];
  EXPECT_EQ(blockspans3.size(), 2);
  uint32_t row_num3;
  timestamp64 begin_ts3, end_ts3;
  GetFirstLastTsAndRowNum(blockspans3, begin_ts3, end_ts3, row_num3);
  EXPECT_EQ(row_num3, 100);
  EXPECT_LE(begin_ts3, start_time + 2 * interval);
  EXPECT_GE(end_ts3, start_time + 3 * interval - 1);

  std::vector<std::shared_ptr<TsBlockSpan>> blockspans4 = split_block_spans[3];
  EXPECT_EQ(blockspans4.size(), 1);
  uint32_t row_num4;
  timestamp64 begin_ts4, end_ts4;
  GetFirstLastTsAndRowNum(blockspans4, begin_ts4, end_ts4, row_num4);
  EXPECT_EQ(row_num4, 1);
  EXPECT_LE(begin_ts4, start_time + 3 * interval);
  EXPECT_GE(end_ts4, start_time + 3 * interval);
}

TEST_F(TsSplitBlockSpansTest, boundary_value2) {
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 100;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  uint32_t bucket_num = 5;
  for (int i = 0; i < bucket_num; ++i) {
    spans.push_back(GenBlockWithSpan(start_time + i * interval + 0.5 * interval, 1, 100, 1));
  }
  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), bucket_num + 1);

  for (int i = 0; i < bucket_num + 1; ++i) {
    uint32_t row_num;
    timestamp64 start_ts, end_ts;
    GetFirstLastTsAndRowNum(split_block_spans[i], start_ts, end_ts, row_num);
    if (i == 0) {
      EXPECT_EQ(split_block_spans[i].size(), 1);
      EXPECT_EQ(0.5 * interval, row_num);
      EXPECT_EQ(begin_ts + i * interval + 0.5 * interval, start_ts);
      EXPECT_EQ(begin_ts + (i + 1) * interval - 1, end_ts);
    } else if (i == bucket_num) {
      EXPECT_EQ(split_block_spans[i].size(), 1);
      EXPECT_EQ(0.5 * interval, row_num);
      EXPECT_EQ(begin_ts + i * interval, start_ts);
      EXPECT_EQ(begin_ts + i * interval + 0.5 * interval - 1, end_ts);
    } else {
      EXPECT_EQ(split_block_spans[i].size(), 2);
      EXPECT_EQ(interval, row_num);
      EXPECT_EQ(begin_ts + i * interval, start_ts);
      EXPECT_EQ(begin_ts + (i + 1) * interval - 1, end_ts);
    }
  }
}

TEST_F(TsSplitBlockSpansTest, boundary_value3) {
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 100;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  uint32_t bucket_num = 1000;
  for (int i = 0; i < bucket_num; ++i) {
    spans.push_back(GenBlockWithSpan(start_time + i * interval, 1, 1, 1));
    spans.push_back(GenBlockWithSpan(start_time + (i + 1) * interval - 1, 1, 1, 1));
  }
  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), bucket_num);

  for (int i = 0; i < bucket_num; ++i) {
    uint32_t row_num;
    timestamp64 start_ts, end_ts;
    GetFirstLastTsAndRowNum(split_block_spans[i], start_ts, end_ts, row_num);
    EXPECT_EQ(split_block_spans[i].size(), 2);
    EXPECT_EQ(row_num, 2);
    EXPECT_EQ(begin_ts + i * interval, start_ts);
    EXPECT_EQ(begin_ts + (i + 1) * interval - 1, end_ts);
  }
}

TEST_F(TsSplitBlockSpansTest, small_interval_value) {
  int ts_num = 100;
  timestamp64 start_time = 1000;
  timestamp64 begin_ts = 1000, interval = 1;
  std::vector<shared_ptr<TsBlockSpan>> spans;
  spans.push_back(GenBlockWithSpan(start_time, 1, ts_num, 1));
  TsBlockSpanSplitter span_splitter(begin_ts, interval, spans);
  std::vector<std::vector<std::shared_ptr<TsBlockSpan>>> split_block_spans;
  std::vector<uint64_t> block_spans_index;
  span_splitter.SplitBlockSpans(split_block_spans, block_spans_index);
  EXPECT_EQ(split_block_spans.size(), 100);

  for (int i = 0; i < ts_num; ++i) {
    std::vector<std::shared_ptr<TsBlockSpan>> blockspans = split_block_spans[i];
    uint32_t row_num1;
    timestamp64 begin_ts1, end_ts1;
    GetFirstLastTsAndRowNum(blockspans, begin_ts1, end_ts1, row_num1);
    EXPECT_EQ(row_num1, 1);
    EXPECT_LE(begin_ts1, start_time + i);
    EXPECT_GE(end_ts1, start_time + i);
  }
}