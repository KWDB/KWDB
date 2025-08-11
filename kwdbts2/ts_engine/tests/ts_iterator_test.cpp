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

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestV2Iterator : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineV2Impl *engine_;
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
  TestV2Iterator() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestV2Iterator() {
    if (engine_) {
      delete engine_;
    }
  }
};

TEST_F(TestV2Iterator, basic) {
    TSTableID table_id = 999;
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id);
    std::shared_ptr<TsTable> ts_table;
    auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = engine_->GetTsTable(ctx_, table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = table_schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<TagInfo> tag_schema;
    s = table_schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    timestamp64 start_ts = 3600;
    auto pay_load = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
        if (!vgroup || vgroup->GetMaxEntityID() < 1) {
            continue;
        }
        TsStorageIterator* ts_iter;
        k_uint32 entity_id = 1;
        KwTsSpan ts_span = {start_ts, start_ts + 20};
        DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
        ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
        std::vector<k_uint32> scan_cols = {0, 1, 2};
        std::vector<Sumfunctype> scan_agg_types;

        s = vgroup->GetIterator(ctx_, {entity_id}, {ts_span}, ts_col_type,
                            scan_cols, scan_cols, {}, scan_agg_types, table_schema_mgr,
                            1, &ts_iter, vgroup, {}, false, false);
        ASSERT_EQ(s, KStatus::SUCCESS);

        ResultSet res{(k_uint32) scan_cols.size()};
        k_uint32 count;
        bool is_finished = false;
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));

        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 0);

        delete ts_iter;
    }
}

TEST_F(TestV2Iterator, mulitEntity) {
    TSTableID table_id = 999;
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id);
    std::shared_ptr<TsTable> ts_table;
    auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = engine_->GetTsTable(ctx_, table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = table_schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<TagInfo> tag_schema;
    s = table_schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    timestamp64 start_ts = 3600;
    KTimestamp interval = 100L;
    int entity_num = 30;
    int entity_row_num = 3;
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        s = vgroup->GetIterator(ctx_, {entity_id}, {ts_span}, ts_col_type,
                          scan_cols, scan_cols, {}, scan_agg_types, table_schema_mgr,
                          1, &ts_iter, vgroup, {}, false, false);
        ASSERT_EQ(s, KStatus::SUCCESS);
        ResultSet res{(k_uint32) scan_cols.size()};
        k_uint32 count;
        bool is_finished = false;
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, entity_row_num);
        ASSERT_EQ(KTimestamp(reinterpret_cast<char*>(res.data[0][0]->mem) + (entity_row_num -1) * 8) - KTimestamp(res.data[0][0]->mem), interval * (entity_row_num - 1));
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 0);
        delete ts_iter;
      }
    }
}

TEST_F(TestV2Iterator, multiDBAndEntity) {
    TSTableID table_id = 999;
    int db_num = 3;
    std::shared_ptr<TsTable> ts_table;
    for (size_t i = 1; i <= db_num; i++) {
      roachpb::CreateTsTable pb_meta;
      ConstructRoachpbTable(&pb_meta, table_id, i);
      auto s = engine_->CreateTsTable(ctx_, table_id + i - 1, &pb_meta, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    auto s = engine_->GetTableSchemaMgr(ctx_, table_id, table_schema_mgr);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = table_schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<TagInfo> tag_schema;
    s = table_schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    timestamp64 start_ts = 3600;
    KTimestamp interval = 100L;
    int entity_num = db_num * 10;
    int entity_row_num = 3;
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(metric_schema, tag_schema ,table_id + i % db_num, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    int entity_result_num = 0;
    for (const auto& vgroup : *ts_vgroups) {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        bool found = false;
        for (size_t db_id = 1; db_id <= db_num; db_id++) {
          s = engine_->GetTableSchemaMgr(ctx_, table_id + db_id - 1, table_schema_mgr);
          ASSERT_EQ(s , KStatus::SUCCESS);
          s = vgroup->GetIterator(ctx_, {entity_id}, {ts_span}, ts_col_type,
                          scan_cols, scan_cols, {}, scan_agg_types, table_schema_mgr,
                          1, &ts_iter, vgroup, {}, false, false);
          ASSERT_EQ(s, KStatus::SUCCESS);
          ResultSet res{(k_uint32) scan_cols.size()};
          k_uint32 count;
          bool is_finished = false;
          ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
          if (count > 0) {
            ASSERT_EQ(count, entity_row_num);
            ASSERT_EQ(KTimestamp(reinterpret_cast<char*>(res.data[0][0]->mem) + (entity_row_num -1) * 8) - KTimestamp(res.data[0][0]->mem), interval * (entity_row_num - 1));
            ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
            ASSERT_EQ(count, 0);
            entity_result_num++;
            found = true;
          }
          delete ts_iter;
          if (found) {
            break;
          }
        }
        ASSERT_TRUE(found);
      }
    }
    ASSERT_EQ(entity_result_num, entity_num);
}
