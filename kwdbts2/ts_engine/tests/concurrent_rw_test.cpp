#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "test_util.h"
#include "ts_common.h"
#include "ts_engine.h"
#include "ts_payload.h"
#include "ts_vgroup.h"

using namespace roachpb;
std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::DOUBLE};
class ConcurrentRWTest : public testing::Test {
 protected:
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_ = &g_ctx_;

  void SetUp() override {
    std::filesystem::remove_all("./tsdb");
    std::filesystem::remove_all("./schema");
  }
  void TearDown() override {}

  ~ConcurrentRWTest() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }
};

struct QueryResult {
  std::vector<uint64_t> count, expect;
};


TEST_F(ConcurrentRWTest, FlushOnly) {
  InitKWDBContext(ctx_);
  EngineOptions opts;
  opts.db_path = "./tsdb";
  TSTableID table_id = 12315;
  opts.vgroup_max_num = 1;
  opts.g_dedup_rule = DedupRule::KEEP;
  opts.mem_segment_max_size = 128 << 10;
  opts.max_last_segment_num = UINT32_MAX;
  
  int npayload = 100;
  int nrow = 50;

  auto engine = std::make_unique<TSEngineV2Impl>(opts);
  engine->Init(ctx_);

  roachpb::CreateTsTable meta;
  ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
  std::shared_ptr<TsTable> ts_table;
  engine->CreateTsTable(ctx_, table_id, &meta, ts_table);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  auto s = engine->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<AttributeInfo> metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::atomic_bool stop = false;

  auto PutWork = [&]() {
    for (int i = 0; i < npayload; ++i) {
      auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1, nrow, 1000 * i, 1);

      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
      TsRawPayload p{payload};
      engine->PutData(nullptr, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt,
                      &dedup_result);

      free(payload.data);
    }
    stop.store(true);
  };

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    do {
      std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine->GetTsVGroups();
      const auto& vgroup = (*ts_vgroups)[0];
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      s = engine->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
      ASSERT_EQ(s, KStatus::SUCCESS);
      s = vgroup->GetIterator(ctx_, {1}, {ts_span}, ts_col_type, scan_cols, scan_cols, {}, scan_agg_types,
                              table_schema_mgr, 1, &ts_iter, vgroup, {}, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32)scan_cols.size()};
      k_uint32 count = 0;
      uint32_t sum = 0;
      bool is_finished = false;
      while (!is_finished) {
        ts_iter->Next(&res, &count, &is_finished);
      }

      std::unordered_set<timestamp64> ts_set;
      for (auto x : res.data[0]) {
        sum += x->count;
        timestamp64* ts = (timestamp64*)x->mem;
        for (int i = 0; i < x->count; i++) {
          ts_set.insert(ts[i * 2]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(ts_set.size());
    } while (stop.load() != true);
    promise.set_value(std::move(result));
  };

  std::promise<QueryResult> q_promise;
  std::thread t_query(QueryWork, std::ref(q_promise));
  std::thread t_put(PutWork);
  t_query.join();
  t_put.join();

  auto q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), q_result.expect.size());
  for (int i = 0; i < q_result.count.size(); i++) {
    EXPECT_EQ(q_result.count[i], q_result.expect[i]);
  }

  q_promise = std::promise<QueryResult>();
  QueryWork(q_promise);
  q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), 1);
  EXPECT_EQ(q_result.count.back(), npayload * nrow);
  EXPECT_EQ(q_result.expect.back(), npayload * nrow);
}

TEST_F(ConcurrentRWTest, CompactOnly) {
  CreateTsTable meta;
  TSTableID table_id = 123;
  ConstructRoachpbTableWithTypes(
      &meta, table_id, {DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE, DataType::BIGINT, DataType::VARCHAR});
  auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
  auto s = mgr->CreateTable(nullptr, 1, table_id, &meta);
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

  kwdbContext_t ctx;
  EngineOptions opts;
  EngineOptions::max_last_segment_num = 0;
  EngineOptions::max_compact_num = 2;
  EngineOptions::mem_segment_max_size = INT32_MAX;  // we will flush memsegment manually
  opts.db_path = "./tsdb";
  auto vgroup = std::make_shared<TsVGroup>(opts, 0, mgr.get(), false);
  EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  for (int i = 0; i < 10; ++i) {
    auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1, 103 + (i + 1) * 1000, 100000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{payload, metric_schema};
    auto ptag = p.GetPrimaryTag();

    vgroup->PutData(&ctx, table_id, 0, &ptag, 1, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  std::atomic_bool stop = false;

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    do {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      s = vgroup->GetIterator(ctx_, {1}, {ts_span}, ts_col_type, scan_cols, scan_cols, {}, scan_agg_types, schema_mgr,
                              1, &ts_iter, vgroup, {}, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32)scan_cols.size()};
      k_uint32 count = 0;
      uint32_t sum = 0;
      bool is_finished = false;
      while (!is_finished) {
        ts_iter->Next(&res, &count, &is_finished);
      }

      std::unordered_set<timestamp64> ts_set;
      for (auto x : res.data[0]) {
        sum += x->count;
        timestamp64* ts = (timestamp64*)x->mem;
        for (int i = 0; i < x->count; i++) {
          ts_set.insert(ts[i * 2]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(ts_set.size());
    } while (stop.load() != true);
    promise.set_value(std::move(result));
  };

  auto CompactWork = [&]() {
    for (int i = 0; i < 10; ++i) {
      vgroup->Compact();
    }
    stop.store(true);
  };

  {
    // check correctness statically
    std::promise<QueryResult> q_promise;
    stop = true;
    QueryWork(q_promise);
    auto q_result = q_promise.get_future().get();
    EXPECT_EQ(q_result.count.size(), 1);
    EXPECT_EQ(q_result.count[0], 56030);
    EXPECT_EQ(q_result.expect.size(), 1);
    EXPECT_EQ(q_result.expect[0], 56030);
  }

  {
    stop = false;
    std::promise<QueryResult> q_promise;
    std::thread t_query(QueryWork, std::ref(q_promise));
    std::thread t_compact(CompactWork);
    t_query.join();
    t_compact.join();
    auto q_result = q_promise.get_future().get();
    EXPECT_EQ(q_result.count.size(), q_result.expect.size());
    auto size = q_result.count.size();
    for (int i = 0; i < size; i++) {
      EXPECT_EQ(q_result.count[i], q_result.expect[i]);
    }
  }

  // vgroup->Compact();
}