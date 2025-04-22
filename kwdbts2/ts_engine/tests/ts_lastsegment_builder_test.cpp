#include "ts_lastsegment_builder.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <unordered_map>
#include <utility>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "test_util.h"
#include "ts_arena.h"
#include "ts_block.h"
#include "ts_coding.h"
#include "ts_engine_schema_manager.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_lastsegment_manager.h"
#include "ts_payload.h"

using namespace roachpb;
std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::INT,   DataType::BIGINT,
                                      DataType::VARCHAR,   DataType::FLOAT, DataType::DOUBLE,
                                      DataType::VARCHAR};
class LastSegmentReadWriteTest : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("schema");
    std::filesystem::remove("last.ver-0000");
  }
  void TearDown() override {
    std::filesystem::remove_all("schema");
    std::filesystem::remove("last.ver-0000");
  }
};

void BuilderWithBasicCheck(TSTableID table_id, int nrow, const std::string &filename) {
  {
    System("rm -rf schema");
    CreateTsTable meta;
    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
    auto s = mgr->CreateTable(nullptr, table_id, &meta);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    s = mgr->GetTableSchemaMgr(table_id, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);

    TsLastSegmentManager last_segment_mgr("./");
    std::unique_ptr<TsFile> last_segment;
    uint32_t file_number;
    last_segment_mgr.NewLastSegmentFile(&last_segment, &file_number);
    TsLastSegmentBuilder builder(mgr.get(), std::move(last_segment), file_number);
    auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1, nrow, 123);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{payload, metric_schema};

    for (int i = 0; i < p.GetRowCount(); ++i) {
      s = builder.PutRowData(table_id, 1, 1, i, p.GetRowData(i));
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
    builder.Finalize();
    free(payload.data);
  }

  auto file = std::make_unique<TsMMapFile>(filename, true);
  TsLastSegmentFooter footer;
  auto sz = file->GetFileSize();
  ASSERT_TRUE(sz >= sizeof(footer));
  TSSlice slice;
  file->Read(sz - sizeof(footer), sizeof(footer), &slice, reinterpret_cast<char *>(&footer));
  ASSERT_EQ(footer.magic_number, FOOTER_MAGIC);

  auto nblock = footer.n_data_block;
  EXPECT_EQ(nblock, (nrow + TsLastSegment::kNRowPerBlock - 1) / TsLastSegment::kNRowPerBlock);
  int expected_nrows = nrow;
  nrow = 0;
  for (int i = 0; i < nblock; ++i) {
    TsLastSegmentBlockIndex idx_block;
    file->Read(footer.block_info_idx_offset + i * sizeof(idx_block), sizeof(idx_block), &slice,
               reinterpret_cast<char *>(&idx_block));
    EXPECT_EQ(idx_block.table_id, table_id);

    char buf[10240];
    TSSlice result;
    file->Read(idx_block.offset, idx_block.length, &result, buf);
    TsLastSegmentBlockInfo info;
    GetFixed64(&result, &info.block_offset);
    GetFixed32(&result, &info.nrow);
    GetFixed32(&result, &info.ncol);
    GetFixed32(&result, &info.var_offset);
    GetFixed32(&result, &info.var_len);

    ASSERT_EQ(info.ncol, dtypes.size() + 2);
    info.col_infos.resize(info.ncol);
    for (int j = 0; j < info.ncol; ++j) {
      GetFixed32(&result, &info.col_infos[j].offset);
      GetFixed16(&result, &info.col_infos[j].bitmap_len);
      GetFixed32(&result, &info.col_infos[j].data_len);
      EXPECT_NE(info.col_infos[j].bitmap_len, 1);
    }
    ASSERT_EQ(result.len, 0);
    for (int j = 0; j < info.ncol; ++j) {
      if (j < 2) {
        ASSERT_EQ(info.col_infos[j].bitmap_len, 0) << "At Column " << j;
      }
      if (info.col_infos[j].bitmap_len != 0) {
        file->Read(info.block_offset + info.col_infos[j].offset, info.col_infos[j].bitmap_len,
                   &result, buf);
        ASSERT_EQ(buf[0], 0) << "At block: " << i << ", Column: " << j;
      }
    }
  }
}

void IteratorCheck(const std::string &filename, TSTableID table_id) {
  auto file = TsLastSegment::Create(0, filename);
  ASSERT_TRUE(file->Open() == kwdbts::SUCCESS);

  std::vector<TsBlockSpan> spans;
  ASSERT_EQ(file->GetBlockSpans(&spans), SUCCESS);
  for (const auto &span : spans) {
    EXPECT_EQ(span.GetEntityID(), 1);
    EXPECT_EQ(span.GetTableID(), table_id);
  }
}

TEST_F(LastSegmentReadWriteTest, WriteAndRead1) {
  BuilderWithBasicCheck(13, 1, "last.ver-0000");
  IteratorCheck("last.ver-0000", 13);
}

TEST_F(LastSegmentReadWriteTest, WriteAndRead2) {
  BuilderWithBasicCheck(14, 12345, "last.ver-0000");
  IteratorCheck("last.ver-0000", 14);
}

TEST_F(LastSegmentReadWriteTest, WriteAndRead3) {
  BuilderWithBasicCheck(15, TsLastSegment::kNRowPerBlock, "last.ver-0000");
  IteratorCheck("last.ver-0000", 15);
}

struct R {
  std::unique_ptr<TsLastSegmentBuilder> builder;
  std::unique_ptr<TsEngineSchemaManager> schema_mgr;
  std::vector<AttributeInfo> metric_schema;
  std::vector<TagInfo> tag_schema;
};

R GenBuilders(TSTableID table_id) {
  CreateTsTable meta;
  ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
  auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
  auto s = mgr->CreateTable(nullptr, table_id, &meta);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  s = mgr->GetTableSchemaMgr(table_id, schema_mgr);

  std::vector<AttributeInfo> metric_schema;
  s = schema_mgr->GetMetricMeta(1, metric_schema);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);

  TsLastSegmentManager last_segment_mgr("./");
  std::unique_ptr<TsFile> last_segment;
  uint32_t file_number;
  last_segment_mgr.NewLastSegmentFile(&last_segment, &file_number);
  R res;
  res.builder =
      std::make_unique<TsLastSegmentBuilder>(mgr.get(), std::move(last_segment), file_number);
  res.metric_schema = std::move(metric_schema);
  res.tag_schema = std::move(tag_schema);
  res.schema_mgr = std::move(mgr);
  return res;
}

decltype(auto) GenRowPayloadWrapper(const std::vector<AttributeInfo> &metric,
                                    const std::vector<TagInfo> &tag, TSTableID table_id,
                                    uint32_t version, TSEntityID dev_id, int num, KTimestamp ts,
                                    KTimestamp interval = 1000) {
  auto deleter = [](TSSlice *p) {
    free(p->data);
    delete p;
  };
  auto slice = GenRowPayload(metric, tag, table_id, version, dev_id, num, ts, interval);
  auto p = new TSSlice(slice);
  return std::unique_ptr<TSSlice, decltype(deleter)>(p, deleter);
}

template <class T>
struct FOO {};

template <class T, class... Args>
struct FOO<T(Args...)> {
  using type = T;
};

void PushPayloadToBuilder(R *builder, TSSlice *payload, TSTableID table_id, uint32_t version,
                          TSEntityID entity_id) {
  TsRawPayloadRowParser parser{builder->metric_schema};
  TsRawPayload p{*payload, builder->metric_schema};

  std::vector<int> idx(p.GetRowCount());
  std::iota(idx.begin(), idx.end(), 0);
  std::mt19937_64 gen(0);
  std::shuffle(idx.begin(), idx.end(), gen);
  for (int i = 0; i < idx.size(); ++i) {
    auto s = builder->builder->PutRowData(table_id, version, entity_id, 0, p.GetRowData(idx[i]));
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

static void Int32Checker(TSSlice r) {
  ASSERT_EQ(r.len, sizeof(int));
  int val = *reinterpret_cast<int *>(r.data);
  EXPECT_LE(val, 1024);
  EXPECT_GE(val, 0);
}

static void Int64Checker(TSSlice r) {
  ASSERT_EQ(r.len, sizeof(int64_t));
  int64_t val = *reinterpret_cast<int64_t *>(r.data);
  EXPECT_LE(val, 10240);
  EXPECT_GE(val, 0);
}

static void FloatChecker(TSSlice r) {
  ASSERT_EQ(r.len, 4);
  float val = *reinterpret_cast<float *>(r.data);
  EXPECT_LE(val, 1024 * 1024);
  EXPECT_GE(val, 0);
}

static void DoubleChecker(TSSlice r) {
  ASSERT_EQ(r.len, 8);
  double val = *reinterpret_cast<double *>(r.data);
  EXPECT_LE(val, 1024 * 1024);
  EXPECT_GE(val, 0);
}

static void VarcharChecker(TSSlice r) { ASSERT_EQ(std::memcmp(r.data, "varstring_", 10), 0); }

class TimestampChecker {
 private:
  bool first = true;
  timestamp64 cur_ts;
  int int_;

 public:
  TimestampChecker(timestamp64 initial, int interval) : cur_ts(initial), int_(interval) {}
  void operator()(TSSlice r) {
    ASSERT_EQ(r.len, 8);
    timestamp64 val = *reinterpret_cast<timestamp64 *>(r.data);
    if (first) {
      EXPECT_EQ(val, cur_ts);
      first = false;
      return;
    }
    EXPECT_EQ(val - cur_ts, int_) << cur_ts;
    cur_ts = val;
  }
};

std::unordered_map<roachpb::DataType, std::function<void(TSSlice)>> checker_funcs{
    {DataType::INT, Int32Checker},       {DataType::BIGINT, Int64Checker},
    {DataType::FLOAT, FloatChecker},     {DataType::DOUBLE, DoubleChecker},
    {DataType::VARCHAR, VarcharChecker},
};

TEST_F(LastSegmentReadWriteTest, IteratorTest1) {
  TSTableID table_id = 123;
  uint32_t table_version = 1;
  int interval = 997;
  timestamp64 start_ts = 123;

  auto res = GenBuilders(table_id);
  int nrow_per_block = TsLastSegment::kNRowPerBlock;
  ASSERT_EQ(nrow_per_block % 2, 0);

  std::vector<TSEntityID> dev_ids{1, 3, 5, 19, 1239};
  std::vector<FOO<decltype(GenRowPayloadWrapper)>::type> payloads;
  for (auto dev_id : dev_ids) {
    auto payload = GenRowPayloadWrapper(res.metric_schema, res.tag_schema, table_id, table_version,
                                        dev_id, nrow_per_block / 2, start_ts, interval);
    PushPayloadToBuilder(&res, payload.get(), table_id, 1, dev_id);
    payloads.push_back(std::move(payload));
  }
  res.builder->Finalize();
  res.builder.reset();

  auto last_segment = TsLastSegment::Create(0, "last.ver-0000");
  TsLastSegmentFooter footer;
  ASSERT_EQ(last_segment->GetFooter(&footer), SUCCESS);
  ASSERT_EQ(footer.n_data_block, 3);

  std::vector<TsBlockSpan> spans;
  last_segment->GetBlockSpans(&spans);
  ASSERT_EQ(spans.size(), dev_ids.size());
  auto dev_iter = dev_ids.begin();
  for (const auto &span : spans) {
    checker_funcs[DataType::TIMESTAMP] = TimestampChecker(start_ts, interval);
    EXPECT_EQ(span.GetTableID(), table_id);
    ASSERT_NE(dev_iter, dev_ids.end());
    EXPECT_EQ(span.GetEntityID(), *dev_iter);
    EXPECT_EQ(span.nrow, nrow_per_block / 2);
    EXPECT_EQ(span.GetTableVersion(), table_version);
    dev_iter++;
  }

  // scan for specific table & entity;

  last_segment->GetBlockSpans({1, table_id, 3, {{INT64_MIN, INT64_MAX}}}, &spans);
  ASSERT_EQ(spans.size(), 1);
  EXPECT_EQ(spans[0].GetEntityID(), 3);
  EXPECT_EQ(spans[0].nrow, 2048);

  struct TestCases {
    timestamp64 min_ts, max_ts;
    int expect_row;
    timestamp64 expect_min, expect_max;
  };
  std::vector<TestCases> cases{
      {start_ts, start_ts + 10 * interval, 11, start_ts, start_ts + 10 * interval},
      {start_ts + 1, start_ts + 10 * interval - 1, 9, start_ts + interval,
       start_ts + 9 * interval}};

  for (auto c : cases) {
    auto s = last_segment->GetBlockSpans({0, table_id, 3, {{c.min_ts, c.max_ts}}}, &spans);
    ASSERT_EQ(s, SUCCESS);
    ASSERT_EQ(spans.size(), 1);
    EXPECT_EQ(spans[0].nrow, c.expect_row);
    timestamp64 l, r;
    spans[0].GetTSRange(&l, &r);
    EXPECT_EQ(l, c.expect_min);
    EXPECT_EQ(r, c.expect_max);
  }

  last_segment->GetBlockSpans({0, table_id, 3, {{1000, 0}}}, &spans);
  ASSERT_EQ(spans.size(), 0);

  last_segment->GetBlockSpans({0, table_id, 3, {{-100, 0}}}, &spans);
  ASSERT_EQ(spans.size(), 0);

  last_segment->GetBlockSpans({0, table_id, 3, {{123, 2000}, {3000, 6000}}}, &spans);
  ASSERT_EQ(spans.size(), 2);
  EXPECT_EQ(spans[0].nrow, 2);
  EXPECT_EQ(spans[1].nrow, 3);
}

TEST_F(LastSegmentReadWriteTest, IteratorTest2) {
  TSTableID table_id = 312;
  uint32_t table_version = 1;
  int interval = 993;
  int start_ts = 12345;

  auto res = GenBuilders(table_id);
  int nrow_per_block = TsLastSegment::kNRowPerBlock = 4096;
  ASSERT_EQ(nrow_per_block % 2, 0);

  std::vector<TSEntityID> dev_ids{1, 2, 3, 4, 5, 19, 1239, 9913, 10311};
  std::vector<int> nrows{nrow_per_block, nrow_per_block, 3000, 6000, 300, 4000, 1000, 12335, 54321};
  ASSERT_EQ(dev_ids.size(), nrows.size());
  std::vector<FOO<decltype(GenRowPayloadWrapper)>::type> payloads;
  for (int i = 0; i < dev_ids.size(); ++i) {
    auto dev_id = dev_ids[i];
    auto nrow = nrows[i];
    auto payload = GenRowPayloadWrapper(res.metric_schema, res.tag_schema, table_id, table_version,
                                        dev_id, nrow, start_ts, interval);
    PushPayloadToBuilder(&res, payload.get(), table_id, 1, dev_id);
    payloads.push_back(std::move(payload));
  }
  res.builder->Finalize();
  res.builder.reset();

  auto last_segment = TsLastSegment::Create(0, "last.ver-0000");
  TsLastSegmentFooter footer;
  ASSERT_EQ(last_segment->GetFooter(&footer), SUCCESS);
  int total = std::accumulate(nrows.begin(), nrows.end(), 0);
  ASSERT_EQ(footer.n_data_block, (total + nrow_per_block - 1) / nrow_per_block);

  std::vector<TsBlockSpan> result_spans;
  last_segment->GetBlockSpans(&result_spans);

  int idx = 0;
  int sum = 0;
  checker_funcs[roachpb::DataType::TIMESTAMP] = TimestampChecker(start_ts, interval);
  for (auto &s : result_spans) {
    EXPECT_EQ(s.GetTableVersion(), table_version);
    EXPECT_EQ(s.GetTableID(), table_id);
    if (s.GetEntityID() != dev_ids[idx]) {
      EXPECT_EQ(sum, nrows[idx]);
      ++idx;
      sum = 0;
      checker_funcs[roachpb::DataType::TIMESTAMP] = TimestampChecker(start_ts, interval);
    }

    ASSERT_LT(idx, dev_ids.size());
    EXPECT_EQ(s.GetEntityID(), dev_ids[idx]);
    sum += s.nrow;

    TSSlice val;
    for (int icol = 0; icol < dtypes.size(); ++icol) {
      for (int i = 0; i < s.nrow; ++i) {
        s.block->GetValueSlice(i + s.start_row, icol, res.metric_schema, val);
        checker_funcs[dtypes[icol]](val);
      }
    }
  }
  EXPECT_EQ(idx + 1, dev_ids.size());

  std::vector<int> expected_rows;
  std::vector<int> expected_dev;
  {
    int i = 0;
    int space = nrow_per_block;
    int dev_left = 0;
    while (dev_left > 0 || i < nrows.size()) {
      if (dev_left == 0) {
        dev_left = nrows[i];
        ++i;
        continue;
      }
      if (dev_left < space) {
        expected_rows.push_back(dev_left);
        space -= dev_left;
        dev_left = 0;
      } else {
        expected_rows.push_back(space);
        dev_left -= space;
        space = nrow_per_block;
      }
      expected_dev.push_back(dev_ids[i - 1]);
    }
  }

  last_segment->GetBlockSpans(&result_spans);
  ASSERT_EQ(result_spans.size(), expected_rows.size());
  for (int i = 0; i < expected_rows.size(); ++i) {
    EXPECT_EQ(result_spans[i].nrow, expected_rows[i]);
    EXPECT_EQ(result_spans[i].GetEntityID(), expected_dev[i]);
  }

  last_segment->GetBlockSpans({0, table_id, 9913, {{INT64_MIN, INT64_MAX}}}, &result_spans);
  ASSERT_EQ(result_spans.size(), 4);
  for (int i = 0; i < result_spans.size(); ++i) {
    EXPECT_EQ(result_spans[i].GetEntityID(), 9913);
    auto expn = std::vector<int>{2084, 4096, 4096, 2059}[i];
    EXPECT_EQ(result_spans[i].nrow, expn);
  }

  std::vector<KwTsSpan> spans{
      {start_ts, start_ts + interval * 2000},
      {start_ts + interval * 2080, start_ts + interval * 4096},
      {start_ts + interval * 5000, start_ts + interval * (2084 + 4096)},
  };
  last_segment->GetBlockSpans({0, table_id, 9913, spans}, &result_spans);
  ASSERT_EQ(result_spans.size(), 5);
  std::vector<std::pair<int, int>> expected_minmax = {
      {start_ts, start_ts + interval * 2000},
      {start_ts + interval * 2080, start_ts + interval * 2083},
      {start_ts + interval * 2084, start_ts + interval * 4096},
      {start_ts + interval * 5000, start_ts + interval * (2084 + 4096 - 1)},
      {start_ts + interval * (2084 + 4096), start_ts + interval * (2084 + 4096)},
  };
  for (int i = 0; i < result_spans.size(); ++i) {
    EXPECT_EQ(result_spans[i].GetEntityID(), 9913);
    auto expn = std::vector<int>{2001, 4, 2013, 1180, 1}[i];
    EXPECT_EQ(result_spans[i].nrow, expn);
    timestamp64 min_ts, max_ts;
    result_spans[i].GetTSRange(&min_ts, &max_ts);
    EXPECT_EQ(min_ts, expected_minmax[i].first);
    EXPECT_EQ(max_ts, expected_minmax[i].second);
  }
}