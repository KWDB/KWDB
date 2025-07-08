#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
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
  EngineOptions opts_;

  TSTableID table_id = 12315;

  std::unique_ptr<TSEngineV2Impl> engine_;

  ConcurrentRWTest() {
    EngineOptions::vgroup_max_num = 1;
    EngineOptions::g_dedup_rule = DedupRule::KEEP;
    EngineOptions::mem_segment_max_size = INT32_MAX;
    EngineOptions::max_last_segment_num = 0;
    EngineOptions::max_compact_num = 2;
    opts_.db_path = "./tsdb";
  }

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  std::vector<AttributeInfo> metric_schema_;
  std::vector<TagInfo> tag_schema_;

  void SetUp() override {
    std::filesystem::remove_all("./tsdb");
    std::filesystem::remove_all("./schema");

    InitKWDBContext(ctx_);
    engine_ = std::make_unique<TSEngineV2Impl>(opts_);
    ASSERT_EQ(engine_->Init(ctx_), KStatus::SUCCESS);

    roachpb::CreateTsTable meta;
    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    std::shared_ptr<TsTable> ts_table;
    ASSERT_EQ(engine_->CreateTsTable(ctx_, table_id, &meta, ts_table), KStatus::SUCCESS);
    ASSERT_EQ(engine_->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetMetricMeta(1, metric_schema_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetTagMeta(1, tag_schema_), KStatus::SUCCESS);
  }
  void TearDown() override { engine_.reset(); }

  ~ConcurrentRWTest() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }
};

struct QueryResult {
  std::vector<uint64_t> count, expect;
};

TEST_F(ConcurrentRWTest, FlushOnly) {
  int npayload = 100;
  int nrow = 50;
  auto vgroups = engine_->GetTsVGroups();
  ASSERT_EQ(vgroups->size(), 1);
  auto vgroup = (*vgroups)[0];

  for (int i = 0; i < npayload; ++i) {
    auto payload = GenRowPayload(metric_schema_, tag_schema_, table_id, 1, 1, nrow, 1000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema_};
    TsRawPayload p{payload, metric_schema_};
    auto ptag = p.GetPrimaryTag();
    vgroup->PutData(ctx_, table_id, 0, &ptag, 1, &payload, false);
    free(payload.data);
  }

  bool stop = false;

  auto FlushWork = [&]() {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    stop = true;
  };

  DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 1};
    std::vector<Sumfunctype> scan_agg_types;
    do {
      ASSERT_EQ(vgroup->GetIterator(ctx_, {1}, {ts_span}, ts_col_type, scan_cols, scan_cols, {}, scan_agg_types,
                                    table_schema_mgr_, 1, &ts_iter, vgroup, {}, false, false),
                KStatus::SUCCESS);
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
    } while (stop != true);
    promise.set_value(std::move(result));
  };

  std::promise<QueryResult> q_promise;
  std::thread t_query(QueryWork, std::ref(q_promise));
  std::thread t_put(FlushWork);
  t_query.join();
  t_put.join();

  auto q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), q_result.expect.size());
  for (int i = 0; i < q_result.count.size(); i++) {
    EXPECT_EQ(q_result.count[i], q_result.expect[i]);
    EXPECT_EQ(q_result.count[i], npayload * nrow);
  }

  q_promise = std::promise<QueryResult>();
  QueryWork(q_promise);
  q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), 1);
  EXPECT_EQ(q_result.count.back(), npayload * nrow);
  EXPECT_EQ(q_result.expect.back(), npayload * nrow);
}

TEST_F(ConcurrentRWTest, CompactOnly) {
  int nrow_per_last_segment = 4000;
  int nlast_segment = 10;
  int total_row = nrow_per_last_segment * nlast_segment;
  auto vgroups = engine_->GetTsVGroups();
  ASSERT_EQ(vgroups->size(), 1);
  auto vgroup = (*vgroups)[0];

  for (int i = 0; i < nlast_segment; ++i) {
    auto payload = GenRowPayload(metric_schema_, tag_schema_, table_id, 1, 1, nrow_per_last_segment, 100000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema_};
    TsRawPayload p{payload, metric_schema_};
    auto ptag = p.GetPrimaryTag();

    vgroup->PutData(ctx_, table_id, 0, &ptag, 1, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  std::atomic_bool stop = false;

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    do {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      ASSERT_EQ(vgroup->GetIterator(ctx_, {1}, {ts_span}, ts_col_type, scan_cols, scan_cols, {}, scan_agg_types,
                                    table_schema_mgr_, 1, &ts_iter, vgroup, {}, false, false),
                KStatus::SUCCESS);
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
    std::cout << "query done" << std::endl;
  };

  auto CompactWork = [&]() {
    for (int i = 0; i < 5; ++i) {
      vgroup->Compact();
    }
    stop.store(true);
    std::cout << "compact done" << std::endl;
  };

  {
    // check correctness statically
    std::promise<QueryResult> q_promise;
    stop = true;
    QueryWork(q_promise);
    auto q_result = q_promise.get_future().get();
    EXPECT_EQ(q_result.count.size(), 1);
    EXPECT_EQ(q_result.count[0], total_row);
    EXPECT_EQ(q_result.expect.size(), 1);
    EXPECT_EQ(q_result.expect[0], total_row);
  }

  {
    stop = false;
    std::promise<QueryResult> q_promise;
    std::thread t_query(QueryWork, std::ref(q_promise));
    std::thread t_compact(CompactWork);
    auto q_result = q_promise.get_future().get();
    t_query.join();
    t_compact.join();
    std::cout << "joined" << std::endl;
    EXPECT_EQ(q_result.count.size(), q_result.expect.size());
    auto size = q_result.count.size();
    for (int i = 0; i < size; i++) {
      EXPECT_EQ(q_result.count[i], q_result.expect[i]);
    }
    std::cout << "checked" << std::endl;
  }

  // vgroup->Compact();
}