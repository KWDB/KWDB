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
class TestTsTableMaxTSV2 : public ::testing::Test {
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
  TestTsTableMaxTSV2() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestTsTableMaxTSV2() {
    if (engine_) {
      delete engine_;
    }
  }
};

TEST_F(TestTsTableMaxTSV2, InsertOneRecord) {
  roachpb::CreateTsTable meta;

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

  timestamp64 start_ts = 8640000;
  auto payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  EntityResultIndex entity_id;
  s = ts_table->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_GE(entity_id.subGroupId, 1);
  EXPECT_LE(entity_id.subGroupId, opts_.vgroup_max_num);
  EXPECT_EQ(entity_id.entityId, 1);
}

TEST_F(TestTsTableMaxTSV2, InsertManyTags) {
  roachpb::CreateTsTable meta;

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

  uint32_t entity_num = 100;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 1; i <= entity_num; ++i) {
    auto payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, i, 1, start_ts - i);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  EntityResultIndex entity_id;
  s = ts_table->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_GE(entity_id.subGroupId, 1);
  EXPECT_LE(entity_id.subGroupId, opts_.vgroup_max_num);
  EXPECT_EQ(entity_id.entityId, 1);
}

TEST_F(TestTsTableMaxTSV2, InsertManyTags1) {
  roachpb::CreateTsTable meta;

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

  uint32_t entity_num = 100;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 1; i <= entity_num; ++i) {
    auto payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, i, 1, start_ts + i);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  EntityResultIndex entity_id;
  s = ts_table->GetLastRowEntity(entity_id);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id.entityGroupId, 1);
  EXPECT_GE(entity_id.subGroupId, 1);
  EXPECT_LE(entity_id.subGroupId, opts_.vgroup_max_num);
  auto vgroup = engine_->GetTsVGroup(entity_id.subGroupId);
  EXPECT_EQ(entity_id.entityId, vgroup->GetMaxEntityID());
}

TEST_F(TestTsTableMaxTSV2, restart) {
  roachpb::CreateTsTable meta;

  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table1;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetTsTable(ctx_, table_id, ts_table1);
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

  uint32_t entity_num = 100;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 1; i <= entity_num; ++i) {
    auto payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, i, 1, start_ts + i);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  EntityResultIndex entity_id1;
  s = ts_table1->GetLastRowEntity(entity_id1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id1.entityGroupId, 1);
  EXPECT_GE(entity_id1.subGroupId, 1);
  EXPECT_LE(entity_id1.subGroupId, opts_.vgroup_max_num);
  auto vgroup = engine_->GetTsVGroup(entity_id1.subGroupId);
  EXPECT_EQ(entity_id1.entityId, vgroup->GetMaxEntityID());

  ts_table1.reset();
  std::shared_ptr<TsTable> ts_table2;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetTsTable(ctx_, table_id, ts_table2);
  ASSERT_EQ(s, KStatus::SUCCESS);

  EntityResultIndex entity_id2;
  s = ts_table2->GetLastRowEntity(entity_id2);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id2.entityGroupId, entity_id1.entityGroupId);
  EXPECT_EQ(entity_id2.subGroupId, entity_id1.subGroupId);
  EXPECT_EQ(entity_id2.entityId, entity_id1.entityId);
}

TEST_F(TestTsTableMaxTSV2, deleteSomeData) {
  roachpb::CreateTsTable meta;

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

  uint32_t entity_num = 100;
  timestamp64 start_ts = 8640000;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 1; i <= entity_num; ++i) {
    auto payload = GenRowPayload(metric_schema, tag_schema ,table_id, 1, i, 1, start_ts + i);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  EntityResultIndex entity_id1;
  s = ts_table->GetLastRowEntity(entity_id1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id1.entityGroupId, 1);
  EXPECT_GE(entity_id1.subGroupId, 1);
  EXPECT_LE(entity_id1.subGroupId, opts_.vgroup_max_num);
  auto vgroup1 = engine_->GetTsVGroup(entity_id1.subGroupId);
  EXPECT_EQ(entity_id1.entityId, vgroup1->GetMaxEntityID());

  uint64_t tmp_count;
  uint64_t p_tag_entity_id = entity_num;
  std::string p_key = string((char*)(&p_tag_entity_id), sizeof(p_tag_entity_id));
  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts + entity_num, start_ts + entity_num}}, &tmp_count, 0, false);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(tmp_count, 1);

  EntityResultIndex entity_id2;
  s = ts_table->GetLastRowEntity(entity_id2);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(entity_id2.entityGroupId, 1);
  EXPECT_GE(entity_id2.subGroupId, 1);
  EXPECT_LE(entity_id2.subGroupId, opts_.vgroup_max_num);
  auto vgroup2 = engine_->GetTsVGroup(entity_id2.subGroupId);
  if (entity_id1.subGroupId == entity_id2.subGroupId) {
    EXPECT_EQ(entity_id2.entityId, vgroup2->GetMaxEntityID() - 1);
  } else {
    EXPECT_EQ(entity_id2.entityId, vgroup2->GetMaxEntityID());
  }

  ASSERT_TRUE(entity_id1.subGroupId != entity_id2.subGroupId || entity_id1.entityId != entity_id2.entityId);
}
