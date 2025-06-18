#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_payload.h"
#include "ts_vgroup.h"

using namespace roachpb;
std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::DOUBLE};
class ConcurrentRWTest : public testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all("./tsdb"); }
  void TearDown() override {}

  ~ConcurrentRWTest() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }
};

TEST_F(ConcurrentRWTest, DISABLED_FlushOnly) {
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_ = &g_ctx_;
  InitKWDBContext(ctx_);
  EngineOptions opts;
  opts.db_path = "./tsdb";
  TSTableID table_id = 12315;
  opts.vgroup_max_num = 1;
  opts.mem_segment_max_size = 512 << 10;  // flush every 512 KB
  opts.max_last_segment_num = UINT32_MAX;
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
    int npayload = 10000;
    int nrow = 50;
    for (int i = 0; i < npayload; ++i) {
      auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1, nrow, 123);

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

  std::vector<uint32_t> result;
  auto QueryWork = [&]() {
    while (stop.load() != true) {
      std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine->GetTsVGroups();
      const auto& vgroup = (*ts_vgroups)[0];
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1};
      std::vector<Sumfunctype> scan_agg_types;
      s = engine->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
      s = vgroup->GetIterator(ctx_, {1}, {ts_span}, ts_col_type, scan_cols, scan_cols,
                              scan_agg_types, table_schema_mgr, 1, &ts_iter, vgroup, {}, false,
                              false);
      ResultSet res{(k_uint32)scan_cols.size()};
      k_uint32 count = 0;
      uint32_t sum = 0;
      bool is_finished = false;
      while (!is_finished) {
        ts_iter->Next(&res, &count, &is_finished);
        sum += count;
      }
      delete ts_iter;
      result.push_back(sum);
    }
  };

  std::thread t_query(QueryWork);
  std::thread t_put(PutWork);
  t_query.join();
  t_put.join();

  for(auto i : result){
    std::cout << i << std::endl;
  }
  EXPECT_TRUE(std::is_sorted(result.begin(), result.end()));
}
