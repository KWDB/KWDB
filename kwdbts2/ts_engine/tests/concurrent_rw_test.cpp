#ifdef NDEBUG
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
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

kwdbContext_t g_ctx_;
class Stopper {
 public:
  Stopper() { KWDBDynamicThreadPool::GetThreadPool().Init(8, &g_ctx_); }
  ~Stopper() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }
};

static Stopper s;

using namespace roachpb;
std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::DOUBLE};
class ConcurrentRWTest : public testing::Test {
 protected:
  kwdbContext_p ctx_ = &g_ctx_;
  EngineOptions opts_;

  TSTableID table_id = 12315;
  roachpb::CreateTsTable meta;

  ConcurrentRWTest() {
    EngineOptions::vgroup_max_num = 1;
    EngineOptions::g_dedup_rule = DedupRule::KEEP;
    EngineOptions::mem_segment_max_size = INT32_MAX;
    EngineOptions::max_last_segment_num = 0;
    EngineOptions::max_compact_num = 2;
    EngineOptions::min_rows_per_block = 1;
    opts_.db_path = "./tsdb";
  }

  std::unique_ptr<TsEngineSchemaManager> schema_mgr_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  const std::vector<AttributeInfo>* metric_schema_{nullptr};
  std::vector<TagInfo> tag_schema_;

  std::shared_ptr<TsVGroup> vgroup_;

  void SetUp() override {
    fs::remove_all("./tsdb");
    fs::remove_all("./schema");

    InitKWDBContext(ctx_);
    schema_mgr_ = std::make_unique<TsEngineSchemaManager>(opts_.db_path + "/schema");

    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    std::shared_ptr<TsTable> ts_table;

    ASSERT_EQ(schema_mgr_->CreateTable(ctx_, 1, table_id, &meta), SUCCESS);
    ASSERT_EQ(schema_mgr_->GetTableSchemaMgr(table_id, table_schema_mgr_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetMetricMeta(1, &metric_schema_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetTagMeta(1, tag_schema_), KStatus::SUCCESS);

    std::shared_mutex wal_level_mutex;
    vgroup_ = std::make_shared<TsVGroup>(&opts_, 1, schema_mgr_.get(), &wal_level_mutex, false);
    ASSERT_EQ(vgroup_->Init(ctx_), KStatus::SUCCESS);

    InitKWDBContext(ctx_);
  }
  void TearDown() override { vgroup_.reset(); }

  ~ConcurrentRWTest() {}
};

struct QueryResult {
  std::vector<uint64_t> count, expect;
};

TEST_F(ConcurrentRWTest, FlushOnly) {
  int npayload = 100;
  int nrow = 20;
  int total_row = npayload * nrow;

  for (int i = 0; i < npayload; ++i) {
    auto payload = GenRowPayload(*metric_schema_, tag_schema_, table_id, 1, 1, nrow, 1000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema_};
    TsRawPayload p{payload, metric_schema_};
    auto ptag = p.GetPrimaryTag();
    vgroup_->PutData(ctx_, table_id, 0, &ptag, 1, &payload, false);
    free(payload.data);
  }

  bool stop = false;

  auto FlushWork = [&]() {
    ASSERT_EQ(vgroup_->Flush(), KStatus::SUCCESS);
    stop = true;
  };

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 1};
    std::vector<Sumfunctype> scan_agg_types;
    do {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr_->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {1};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      ASSERT_EQ(
          vgroup_->GetIterator(ctx_, entity_ids, ts_spans, block_filter, scan_cols, scan_cols, agg_extend_cols,
                               scan_agg_types, table_schema_mgr_, schema, &ts_iter, vgroup_, ts_points, false, false),
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
          ts_set.insert(ts[i]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(ts_set.size());
      std::this_thread::yield();
    } while (stop != true);
    promise.set_value(std::move(result));
  };

  {
    // check correctness statically
    std::promise<QueryResult> q_promise;
    stop = true;
    QueryWork(q_promise);
    auto q_result = q_promise.get_future().get();
    ASSERT_EQ(q_result.count.size(), 1);
    ASSERT_EQ(q_result.count[0], total_row);
    ASSERT_EQ(q_result.expect.size(), 1);
    ASSERT_EQ(q_result.expect[0], total_row);
  }

  std::promise<QueryResult> q_promise;
  std::thread t_query(QueryWork, std::ref(q_promise));
  std::thread t_put(FlushWork);
  t_query.join();
  t_put.join();

  auto q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), q_result.expect.size());
  for (int i = 0; i < q_result.count.size(); i++) {
    EXPECT_EQ(q_result.count[i], q_result.expect[i]);
    EXPECT_EQ(q_result.count[i], total_row) << i;
  }

  q_promise = std::promise<QueryResult>();
  QueryWork(q_promise);
  q_result = q_promise.get_future().get();
  ASSERT_EQ(q_result.count.size(), 1);
  EXPECT_EQ(q_result.count.back(), npayload * nrow);
  EXPECT_EQ(q_result.expect.back(), npayload * nrow);
}

TEST_F(ConcurrentRWTest, CompactOnly) {
  int nrow_per_last_segment = 400;
  int nlast_segment = 5;
  int total_row = nrow_per_last_segment * nlast_segment;
  auto vgroup = vgroup_;

  for (int i = 0; i < nlast_segment; ++i) {
    auto payload = GenRowPayload(*metric_schema_, tag_schema_, table_id, 1, 1, nrow_per_last_segment, 10000000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema_};
    TsRawPayload p{payload, metric_schema_};
    auto ptag = p.GetPrimaryTag();

    vgroup->PutData(ctx_, table_id, 0, &ptag, 1, &payload, false);
    free(payload.data);
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  bool stop = false;

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    do {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};

      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr_->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {1};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};

      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      ASSERT_EQ(
          vgroup->GetIterator(ctx_, entity_ids, ts_spans, block_filter, scan_cols, scan_cols, agg_extend_cols,
                              scan_agg_types, table_schema_mgr_, schema, &ts_iter, vgroup, ts_points, false, false),
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
          ts_set.insert(ts[i]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(ts_set.size());
      std::this_thread::yield();
    } while (stop != true);
    promise.set_value(std::move(result));
  };

  auto CompactWork = [&]() {
    vgroup->Compact();
    stop = true;
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
    std::cout << "joined: query " << q_result.count.size() << " times" << std::endl;
    EXPECT_EQ(q_result.count.size(), q_result.expect.size());
    auto size = q_result.count.size();
    for (int i = 0; i < size; i++) {
      EXPECT_EQ(q_result.count[i], q_result.expect[i]);
    }
    std::cout << "checked" << std::endl;
  }

  // vgroup->Compact();
}

TEST_F(ConcurrentRWTest, SwitchMem) {
  EngineOptions::mem_segment_max_size = 0;
  EngineOptions::max_last_segment_num = 1 << 30;
  auto engine = std::make_unique<TSEngineV2Impl>(opts_);
  engine->Init(ctx_);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(engine->CreateTsTable(ctx_, table_id, &meta, ts_table), SUCCESS);

  int npayload = 2000;
  int nrow = 1;
  int total_row = npayload * nrow;

  std::vector<TSSlice> payloads(npayload);
  volatile bool stop = false;

  std::atomic_int atomic_count{0};

  for (int i = 0; i < npayload; ++i) {
    auto payload = GenRowPayload(*metric_schema_, tag_schema_, table_id, 1, 1, nrow, 1000 * i, 1);
    payloads[i] = payload;
  }

  auto WriteWork = [&]() {
    for (int i = 0; i < npayload; ++i) {
      auto payload = payloads[i];
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      DedupResult dedup_result;
      engine->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      atomic_count++;
    }
  };

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    auto vgroup = engine->GetTsVGroups()->at(0);
    int i = 0;
    do {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      std::vector<uint32_t> entity_ids = {1};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr_->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      auto write_count = atomic_count.load();
      ASSERT_EQ(
          vgroup->GetIterator(ctx_, entity_ids, ts_spans, block_filter, scan_cols, scan_cols, agg_extend_cols,
                              scan_agg_types, table_schema_mgr_, schema, &ts_iter, vgroup, ts_points, false, false),
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
          ts_set.insert(ts[i]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(write_count);
      std::this_thread::yield();
    } while (stop != true);
    promise.set_value(std::move(result));
  };

  std::promise<QueryResult> q_promise;
  std::thread t_query(QueryWork, std::ref(q_promise));
  std::thread t_put(WriteWork);
  t_put.join();
  stop = true;
  auto q_result = q_promise.get_future().get();
  t_query.join();
  EXPECT_EQ(q_result.count.size(), q_result.expect.size());
  auto size = q_result.count.size();
  for (int i = 0; i < size; i++) {
    ASSERT_GE(q_result.count[i], q_result.expect[i]);
  }

  for (auto i : payloads) {
    free(i.data);
  }
}

TEST_F(ConcurrentRWTest, RandomFlush) {
  EngineOptions::mem_segment_max_size = 0;
  EngineOptions::max_last_segment_num = 1 << 30;
  auto engine = std::make_unique<TSEngineV2Impl>(opts_);
  engine->Init(ctx_);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(engine->CreateTsTable(ctx_, table_id, &meta, ts_table), SUCCESS);

  int npayload = 2000;
  int nrow = 1;
  int total_row = npayload * nrow;

  std::vector<TSSlice> payloads(npayload);
  volatile bool stop = false;

  std::atomic_int atomic_count{0};

  for (int i = 0; i < npayload; ++i) {
    auto payload = GenRowPayload(*metric_schema_, tag_schema_, table_id, 1, 1, nrow, 1000 * i, 1);
    payloads[i] = payload;
  }

  auto WriteWork = [&]() {
    for (int i = 0; i < npayload; ++i) {
      auto payload = payloads[i];
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      DedupResult dedup_result;
      engine->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      atomic_count++;
    }
  };

  auto QueryWork = [&](std::promise<QueryResult>& promise) {
    QueryResult result;
    auto vgroup = engine->GetTsVGroups()->at(0);
    int i = 0;
    do {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      std::vector<uint32_t> entity_ids = {1};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr_->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      auto write_count = atomic_count.load();
      ASSERT_EQ(
          vgroup->GetIterator(ctx_, entity_ids, ts_spans, block_filter, scan_cols, scan_cols, agg_extend_cols,
                              scan_agg_types, table_schema_mgr_, schema, &ts_iter, vgroup, ts_points, false, false),
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
          ts_set.insert(ts[i]);
        }
      }
      delete ts_iter;
      result.count.push_back(sum);
      result.expect.push_back(write_count);
      std::this_thread::yield();
    } while (stop != true);
    promise.set_value(std::move(result));
  };

  auto FlushWork = [&]() {
    auto vgroup = engine->GetTsVGroups()->at(0);
    while (stop != true) {
      vgroup->Flush();
      std::this_thread::yield();
    }
  };

  std::promise<QueryResult> q_promise;
  std::thread t_query(QueryWork, std::ref(q_promise));
  std::thread t_put(WriteWork);
  std::thread t_flush(FlushWork);
  t_put.join();
  stop = true;
  auto q_result = q_promise.get_future().get();
  t_query.join();
  t_flush.join();
  EXPECT_EQ(q_result.count.size(), q_result.expect.size());
  auto size = q_result.count.size();
  for (int i = 0; i < size; i++) {
    ASSERT_GE(q_result.count[i], q_result.expect[i]);
  }

  for (auto i : payloads) {
    free(i.data);
  }
}
#endif