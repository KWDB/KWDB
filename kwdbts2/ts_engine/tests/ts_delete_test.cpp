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
class TestV2DeleteTest : public ::testing::Test {
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
  TestV2DeleteTest() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineImpl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EngineOptions::g_dedup_rule = DedupRule::KEEP;
  }

  ~TestV2DeleteTest() {
    if (engine_) {
      delete engine_;
    }
  }

  std::string GetPrimaryKey(TSTableID table_id, TSEntityID dev_id) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    uint64_t pkey_len = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        pkey_len += tag_schema[i].m_size;
      }
    }
    char* mem = reinterpret_cast<char*>(malloc(pkey_len));
    memset(mem, 0, pkey_len);
    std::string dev_str = intToString(dev_id);
    size_t offset = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        if (tag_schema[i].m_data_type == DATATYPE::VARSTRING) {
          memcpy(mem + offset, dev_str.data(), dev_str.length());
        } else {
          memcpy(mem + offset, (char*)(&dev_id), tag_schema[i].m_size);
        }
        offset += tag_schema[i].m_size;
      }
    }
    auto ret = std::string{mem, pkey_len};
    free(mem);
    return ret;
  }

  void CheckRowCount(std::shared_ptr<TsTableSchemaManager> table_schema_mgr, std::shared_ptr<TsVGroup>& entity_v_group, uint32_t entity_id, KwTsSpan& ts_span, uint64_t expect) {
    TsStorageIterator* ts_iter;
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    auto s = entity_v_group->GetIterator(ctx_, table_schema_mgr->GetCurrentVersion(), entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, entity_v_group, ts_points, false, false);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    uint64_t total_num = 0;
    bool is_finished = false;
    do {
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      total_num += count;
    } while (!is_finished);
    ASSERT_EQ(total_num, expect);
    delete ts_iter;
  }

};

TEST_F(TestV2DeleteTest, basicDelete) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 100;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, row_num, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  std::shared_ptr<TsVGroup> entity_v_group;
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    entity_v_group = vgroup;
    break;
  }
  KwTsSpan ts_span = {start_ts, start_ts + row_num * 1000};
  CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, row_num);

  uint64_t tmp_count;
  uint64_t p_tag_entity_id = 1;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);
  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts, start_ts + row_num / 2 * 1000 - 1}}, &tmp_count, 0, 1, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, row_num / 2);
}

TEST_F(TestV2DeleteTest, MultiInsertAndDelete) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 100;
  int insert_times = 3;
  for (size_t i = 0; i < insert_times; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, row_num, start_ts + row_num * 1000 * i);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  std::shared_ptr<TsVGroup> entity_v_group;
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    entity_v_group = vgroup;
    break;
  }
  KwTsSpan ts_span = {start_ts, INT64_MAX};
  CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, row_num * insert_times);

  uint64_t tmp_count;
  uint64_t p_tag_entity_id = 1;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);
  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts, start_ts + row_num * 1000 - 1}}, &tmp_count, 0, 1, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, row_num * (insert_times - 1));

  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts + row_num * 1000 * (insert_times - 1), INT64_MAX}}, &tmp_count, 0, 1, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, row_num * (insert_times - 2));
}

TEST_F(TestV2DeleteTest, InsertAndDeleteAndInsert) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 3;
  int del_num = 1;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, row_num, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  std::shared_ptr<TsVGroup> entity_v_group;
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    entity_v_group = vgroup;
    break;
  }

  uint64_t tmp_count;
  uint64_t p_tag_entity_id = 1;
  uint64_t cur_osn = 1;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);
  for (size_t i = 0; i < 5; i++) {
    s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts, start_ts + del_num * 1000 - 1}}, &tmp_count, 0, cur_osn++, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span = {start_ts, INT64_MAX};
    CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, (i + 1) * (row_num - del_num));

    TsRawPayload::SetOSN(pay_load, cur_osn);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    ASSERT_EQ(s, KStatus::SUCCESS);
    CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, (i + 1) * (row_num - del_num) + row_num);
  }
  free(pay_load.data);
}

TEST_F(TestV2DeleteTest, undoDelete) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 3;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, row_num, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  std::shared_ptr<TsVGroup> entity_v_group;
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    entity_v_group = vgroup;
    break;
  }

  uint64_t p_tag_entity_id = 1;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);
  TSSlice p_tag{p_key.data(), p_key.length()};
  TSEntityID cur_entity_id;
  s = entity_v_group->getEntityIdByPTag(ctx_, table_id, p_tag, &cur_entity_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (size_t i = 0; i < 5; i++) {
    s = entity_v_group->DeleteData(ctx_, table_id, cur_entity_id, 10086000 + i * 5000, {{start_ts, INT64_MAX}});
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span = {start_ts, INT64_MAX};
    CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, 0);
    s = entity_v_group->undoDeleteData(ctx_, table_id, p_key, 10086000 + i * 5000, {{start_ts, INT64_MAX}});
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    ASSERT_EQ(s, KStatus::SUCCESS);
    CheckRowCount(table_schema_mgr, entity_v_group, 1, ts_span, (i + 2) * row_num);
  }
  free(pay_load.data);
}

TEST_F(TestV2DeleteTest, undoPutAndRedoPut) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 3;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, row_num, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  std::shared_ptr<TsVGroup> entity_v_group;
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    entity_v_group = vgroup;
    break;
  }
  s = entity_v_group->undoPut(ctx_, 1, pay_load);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = entity_v_group->redoPut(ctx_, 1, pay_load);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(pay_load.data);
}
