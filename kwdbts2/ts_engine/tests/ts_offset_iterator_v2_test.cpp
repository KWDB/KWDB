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
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TestOffsetIteratorV2 : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineImpl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

 public:
  TestOffsetIteratorV2() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineImpl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestOffsetIteratorV2() {
    if (engine_) {
      delete engine_;
    }
  }
};

// 单分区 多分区 单设备 多设备 顺序乱序 正序逆序 极端值
TEST_F(TestOffsetIteratorV2, basic) {
  roachpb::CreateTsTable meta;

  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600 * 1000;
  auto payload = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10000, start_ts, 10);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 5000,
      .limit = 10,
  };
  // ASSERT_EQ(ts_table->GetEntityIdList(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_GE(total_cnt, 5010);
  ASSERT_LE(total_cnt, 10000);
  ASSERT_GE(ts.size(), 10);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts + iter1->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts + total_cnt * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  // desc
  TsIterator* iter2;
  params.reverse = true;
  params.offset = 3000;
  params.limit = 50;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_GE(total_cnt,3010);
  ASSERT_LE(total_cnt, 10000);
  ASSERT_GE(ts.size(), 50);

  timestamp64 end_ts = start_ts + (10000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_LE(it, end_ts - iter2->GetFilterCount() * 10);
    ASSERT_GE(it, end_ts - total_cnt * 10);
  }

  delete iter2;
}

TEST_F(TestOffsetIteratorV2, multi_partition) {
  roachpb::CreateTsTable meta;

  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts1 = 864000 * 1000;
  auto payload1 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10000, start_ts1, 10);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload1.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts2 = 2.0 * 864000 * 1000;
  auto payload2 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10000, start_ts2, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload2.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts3 = 3.0 * 864000 * 1000;
  auto payload3 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10000, start_ts3, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload3, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload3.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 15000,
      .limit = 10,
  };
  // ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_GE(total_cnt, 15010);
  ASSERT_LE(total_cnt, 20000);
  ASSERT_GE(ts.size(), 10);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts2 + (iter1->GetFilterCount() - 10000) * 10);
    ASSERT_LE(it, start_ts2 + (total_cnt - 10000) * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  // desc
  TsIterator* iter2;
  params.reverse = true;
  params.offset = 23000;
  params.limit = 50;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_GE(total_cnt,23050);
  ASSERT_LE(total_cnt, 30000);
  ASSERT_GE(ts.size(), 50);

  timestamp64 end_ts = start_ts1 + (10000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_LE(it, end_ts - (iter2->GetFilterCount() - 20000) * 10);
    ASSERT_GE(it, end_ts - (total_cnt - 20000) * 10);
  }

  delete iter2;
}

TEST_F(TestOffsetIteratorV2, extreme) {
  roachpb::CreateTsTable meta;

  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts1 = 3600 * 1000;
  auto payload1 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 2, start_ts1, 10);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload1.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 1,
      .limit = 1,
  };
  // ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_EQ(total_cnt, 2);
  ASSERT_GE(ts.size(), 1);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts1 + iter1->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts1 + total_cnt * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  timestamp64 start_ts2 = 3600 * 1000 + 20;
  auto payload2 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10000, start_ts2, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload2.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TsIterator* iter2;
  params.offset = 10000;
  params.limit = 2;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 8 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_EQ(total_cnt,10002);
  ASSERT_GE(ts.size(), 2);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts1 + iter2->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts1 + total_cnt * 10);
  }

  delete iter2;
}

//TEST_F(TestV2OffsetIterator, disorder) {
//  roachpb::CreateTsTable meta;
//
//  TSTableID table_id = 999;
//  roachpb::CreateTsTable pb_meta;
//  ConstructRoachpbTable(&pb_meta, table_id);
//  std::shared_ptr<TsTable> ts_table;
//  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
//  ASSERT_EQ(s, KStatus::SUCCESS);
//  s = engine_->GetTsTable(ctx_, table_id, ts_table);
//  ASSERT_EQ(s, KStatus::SUCCESS);
//
//  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
//  s = engine_->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
//  ASSERT_EQ(s , KStatus::SUCCESS);
//
//  std::vector<AttributeInfo> metric_schema;
//  s = table_schema_mgr->GetMetricMeta(1, metric_schema);
//  ASSERT_EQ(s , KStatus::SUCCESS);
//
//  std::vector<TagInfo> tag_schema;
//  s = table_schema_mgr->GetTagMeta(1, tag_schema);
//  ASSERT_EQ(s , KStatus::SUCCESS);
//
//  KTimestamp start_ts = 10000 * 1000, disorder_ts = 7200 * 1000;
//
//  int write_count = 20;
//  uint16_t inc_entity_cnt;
//  uint32_t inc_unordered_cnt;
//  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
//  // Cross write unordered data
//  for (int i = 0; i < write_count; ++i) {
//    TSSlice payload;
//    if (i % 2 == 0) {
//      payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1, 100, start_ts + (i / 2) * 100 * 10, 10);
//    } else {
//      // Write unordered data
//      payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1, 100, disorder_ts + (i / 2) * 100 * 10, 10);
//    }
//    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
//    free(payload.data);
//    ASSERT_EQ(s, KStatus::SUCCESS);
//  }
//
//  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
//  std::vector<k_uint32> scan_cols = {0, 1, 2};
//  std::vector<Sumfunctype> scan_agg_types;
//
//  // asc
//  TsIterator* iter1;
//  std::vector<EntityResultIndex> entity_results;
//  k_uint32 count;
//  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
//  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, {}, scan_agg_types, 1, &iter1, {}, false, false, 500, 10),
//            KStatus::SUCCESS);
//
//  ResultSet res1{(k_uint32) scan_cols.size()};
//  uint32_t total_cnt = 0;
//  vector<timestamp64> ts;
//  do {
//    ASSERT_EQ(iter1->Next(&res1, &count), KStatus::SUCCESS);
//    for (int i = 0; i < count; ++i) {
//      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res1.data[0][0]->mem + 8 * i)));
//    }
//    res1.clear();
//    total_cnt += count;
//  } while (count != 0);
//
//  total_cnt += iter1->GetFilterCount();
//
//  ASSERT_GE(total_cnt, 510);
//  ASSERT_LE(total_cnt, 2000);
//  ASSERT_GE(ts.size(), 10);
//
//  for (auto it : ts) {
//    ASSERT_GE(it, disorder_ts + iter1->GetFilterCount() * 10);
//    ASSERT_LE(it, disorder_ts + total_cnt * 10);
//  }
//
//  ts.clear();
//  total_cnt = 0;
//  delete iter1;
//
//  TsIterator* iter2;
//  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, {}, scan_agg_types, 1, &iter2, {}, false, false, 1500, 20),
//            KStatus::SUCCESS);
//
//  ResultSet res2{(k_uint32) scan_cols.size()};
//  do {
//    ASSERT_EQ(iter2->Next(&res2, &count), KStatus::SUCCESS);
//    for (int i = 0; i < count; ++i) {
//      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res2.data[0][0]->mem + 8 * i)));
//    }
//    res2.clear();
//    total_cnt += count;
//  } while (count != 0);
//
//  total_cnt += iter2->GetFilterCount();
//
//  ASSERT_GE(total_cnt, 1520);
//  ASSERT_LE(total_cnt, 2000);
//  ASSERT_GE(ts.size(), 20);
//
//  for (auto it : ts) {
//    ASSERT_GE(it, start_ts + (iter2->GetFilterCount() - 1000) * 10);
//    ASSERT_LE(it, start_ts + (total_cnt - 1000) * 10);
//  }
//
//  ts.clear();
//  total_cnt = 0;
//  delete iter2;
//
//  // desc
//  TsIterator* iter3;
//  ResultSet res3{(k_uint32) scan_cols.size()};
//
//  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, {}, scan_agg_types, 1, &iter3, {}, true, false, 1300, 50),
//            KStatus::SUCCESS);
//
//  do {
//    ASSERT_EQ(iter3->Next(&res3, &count), KStatus::SUCCESS);
//    for (int i = 0; i < count; ++i) {
//      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res3.data[0][0]->mem + 8 * i)));
//    }
//    res3.clear();
//    total_cnt += count;
//  } while (count != 0);
//
//  total_cnt += iter3->GetFilterCount();
//
//  ASSERT_GE(total_cnt,1350);
//  ASSERT_LE(total_cnt, 2000);
//  ASSERT_GE(ts.size(), 50);
//
//  timestamp64 end_ts = disorder_ts + (1000 - 1) * 10;
//  for (auto it : ts) {
//    ASSERT_LE(it, end_ts - (iter3->GetFilterCount() - 1000) * 10);
//    ASSERT_GE(it, end_ts - (total_cnt - 1000) * 10);
//  }
//
//  delete iter3;
//}
