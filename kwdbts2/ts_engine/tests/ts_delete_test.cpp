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
#include "ts_test_base.h"
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestV2DeleteTest : public TsEngineTestBase {
 public:
  TestV2DeleteTest() {
    EngineOptions::g_dedup_rule = DedupRule::KEEP_EXPERIMENTAL;
    InitContext();
    InitEngine(engine_root_path);
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
    FillParams fill_params;
    auto s = entity_v_group->GetIterator(ctx_, table_schema_mgr->GetCurrentVersion(), entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, entity_v_group, ts_points, false, false, UINT64_MAX, fill_params);
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

// Test 1: delete entity with data in current mem segment, verify count stats invalidated
TEST_F(TestV2DeleteTest, CountStatsInvalidOnDeleteWithData) {
  TSTableID table_id = 4001;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Insert entity 1 (5 rows) and entity 2 (5 rows)
  timestamp64 start_ts = 3600;
  int row_num = 5;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, row_num, start_ts);
  TsRawPayload::SetOSN(pay_load, 10);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 2, row_num, start_ts);
  TsRawPayload::SetOSN(pay_load, 11);
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Find entity 1's vgroup via tag table (entity 2 may be in a different vgroup)
  std::shared_ptr<TsVGroup> entity_vg;
  {
    std::string p_key1 = GetPrimaryKey(table_id, 1);
    auto tag_table = table_schema_mgr->GetTagTable();
    uint32_t vgroup_id;
    uint32_t check_entity_id;
    ASSERT_TRUE(tag_table->hasPrimaryKey(p_key1.data(), p_key1.size(), check_entity_id, vgroup_id));
    ASSERT_EQ(check_entity_id, 1);
    entity_vg = engine_->GetTsVGroup(vgroup_id);
  }
  ASSERT_NE(entity_vg, nullptr);

  // Delete entity 1
  uint64_t tmp_count;
  std::string p_key = GetPrimaryKey(table_id, 1);
  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts, start_ts + row_num * 1000 - 1}},
                          &tmp_count, 0, 12, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tmp_count, row_num);

  // Flush
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify entity 1 count stats is invalid in the vgroup where it resides
  auto db_id = table_schema_mgr->GetDbID();
  auto ts_type = table_schema_mgr->GetTsColDataType();
  std::vector<KwTsSpan> ts_spans = {{start_ts, start_ts + row_num * 1000}};
  auto partitions = entity_vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
  ASSERT_FALSE(partitions.empty());
  TsEntityCountStats stats1;
  stats1.entity_id = 1;
  auto* count_mgr = partitions[0]->GetCountManager().get();
  ASSERT_NE(count_mgr, nullptr);
  ASSERT_EQ(count_mgr->GetEntityCountStats(stats1), KStatus::SUCCESS);
  ASSERT_FALSE(stats1.is_count_valid);

  // Entity 2 count stats should still be valid with correct row count;
  // use tag table to find entity 2's vgroup and local entity ID
  {
    std::string p_key2 = GetPrimaryKey(table_id, 2);
    auto tag_table = table_schema_mgr->GetTagTable();
    uint32_t vgroup_id_2;
    uint32_t entity2_local_id;
    ASSERT_TRUE(tag_table->hasPrimaryKey(p_key2.data(), p_key2.size(), entity2_local_id, vgroup_id_2));
    auto entity2_vg = engine_->GetTsVGroup(vgroup_id_2);
    ASSERT_NE(entity2_vg, nullptr);
    auto vg_partitions = entity2_vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
    ASSERT_FALSE(vg_partitions.empty());
    TsEntityCountStats stats2;
    stats2.entity_id = entity2_local_id;
    ASSERT_EQ(vg_partitions[0]->GetCountManager()->GetEntityCountStats(stats2), KStatus::SUCCESS);
    ASSERT_TRUE(stats2.is_count_valid);
    ASSERT_EQ(stats2.valid_count, row_num);
  }

  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Test 2: delete entity after previous flush (no data rows in current mem segment)
TEST_F(TestV2DeleteTest, CountStatsInvalidOnDeleteAfterFlush) {
  TSTableID table_id = 4002;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  int row_num = 5;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, row_num, start_ts);
  TsRawPayload::SetOSN(pay_load, 10);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsVGroup> entity_vg;
  auto* vgroups = engine_->GetTsVGroups();
  for (auto& vg : *vgroups) {
    if (vg && vg->GetMaxEntityID() >= 1) { entity_vg = vg; break; }
  }
  ASSERT_NE(entity_vg, nullptr);
  auto db_id = table_schema_mgr->GetDbID();
  auto ts_type = table_schema_mgr->GetTsColDataType();

  // First flush
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // After first flush, count stats should be valid with correct row count
  std::vector<KwTsSpan> ts_spans = {{start_ts, start_ts + row_num * 1000}};
  auto partitions = entity_vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
  ASSERT_FALSE(partitions.empty());
  TsEntityCountStats stats1;
  stats1.entity_id = 1;
  ASSERT_EQ(partitions[0]->GetCountManager()->GetEntityCountStats(stats1), KStatus::SUCCESS);
  ASSERT_TRUE(stats1.is_count_valid);
  ASSERT_EQ(stats1.valid_count, row_num);

  // Delete entity 1 (only del_range in current mem segment, no data rows)
  uint64_t tmp_count;
  std::string p_key = GetPrimaryKey(table_id, 1);
  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts, start_ts + row_num * 1000 - 1}},
                          &tmp_count, 0, 11, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Second flush
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // After second flush, entity 1 count stats should be invalid
  partitions = entity_vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
  ASSERT_FALSE(partitions.empty());
  TsEntityCountStats ts;
  ts.entity_id = 1;
  ASSERT_EQ(partitions[0]->GetCountManager()->GetEntityCountStats(ts), KStatus::SUCCESS);
  ASSERT_FALSE(ts.is_count_valid);

  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Test 3: cross-partition delete, verify count stats invalid in both partitions
TEST_F(TestV2DeleteTest, CountStatsInvalidOnCrossPartitionDelete) {
  TSTableID table_id = 4003;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id, 2);  // use db_id=2 to avoid partition interval cache
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Insert entity 1 in two batches 10000*86400 apart to span multiple partitions
  timestamp64 start_ts = 3600;
  int row_num = 5;
  KTimestamp interval = 100L;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};

  // First batch
  auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, row_num, start_ts, interval);
  TsRawPayload::SetOSN(pay_load, 10);
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Second batch: 10000*86400 later → different partition
  timestamp64 start_ts2 = start_ts + 10000 * 86400;
  pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, row_num, start_ts2, interval);
  TsRawPayload::SetOSN(pay_load, 11);
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsVGroup> entity_vg;
  {
    std::string p_key1 = GetPrimaryKey(table_id, 1);
    auto tag_table = table_schema_mgr->GetTagTable();
    uint32_t vgroup_id;
    uint32_t check_entity_id;
    ASSERT_TRUE(tag_table->hasPrimaryKey(p_key1.data(), p_key1.size(), check_entity_id, vgroup_id));
    ASSERT_EQ(check_entity_id, 1);
    entity_vg = engine_->GetTsVGroup(vgroup_id);
  }
  ASSERT_NE(entity_vg, nullptr);
  auto db_id = table_schema_mgr->GetDbID();
  auto ts_type = table_schema_mgr->GetTsColDataType();

  // Delete entity 1 across full timestamp range (spans both partitions)
  uint64_t tmp_count;
  std::string p_key = GetPrimaryKey(table_id, 1);
  s = engine_->DeleteData(ctx_, table_id, 0, p_key,
                          {{start_ts, start_ts2 + row_num * interval - 1}},
                          &tmp_count, 0, 12, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tmp_count, 2 * row_num);

  // Flush
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify count stats invalid in all affected partitions
  std::vector<KwTsSpan> ts_spans = {{start_ts, start_ts2 + row_num * interval}};
  auto partitions = entity_vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
  ASSERT_GE(partitions.size(), 2);
  for (const auto& partition : partitions) {
    TsEntityCountStats stats;
    stats.entity_id = 1;
    ASSERT_EQ(partition->GetCountManager()->GetEntityCountStats(stats), KStatus::SUCCESS);
    ASSERT_FALSE(stats.is_count_valid);
  }

  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Test 4: multi-partition write, partial delete across partitions, verify count stats
TEST_F(TestV2DeleteTest, CountStatsInvalidOnMultiEntityDelete) {
  TSTableID table_id = 4004;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  KTimestamp interval = 100L;
  int row_num = 10;
  int total_entities = 10;
  int delete_count = 5;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  auto db_id = table_schema_mgr->GetDbID();
  auto ts_type = table_schema_mgr->GetTsColDataType();

  // Step 1: write partition 1 data (10 entities × row_num rows each)
  for (int i = 1; i <= total_entities; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, i, row_num, start_ts, interval);
    TsRawPayload::SetOSN(pay_load, 10 + i - 1);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  // Flush partition 1
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify all entities have valid count stats with row_num rows
  {
    auto tag_table = table_schema_mgr->GetTagTable();
    for (int i = 1; i <= total_entities; i++) {
      std::string p_key = GetPrimaryKey(table_id, i);
      uint32_t vgroup_id, local_eid;
      ASSERT_TRUE(tag_table->hasPrimaryKey(p_key.data(), p_key.size(), local_eid, vgroup_id));
      auto vg = engine_->GetTsVGroup(vgroup_id);
      ASSERT_NE(vg, nullptr);
      std::vector<KwTsSpan> ts_spans = {{start_ts, start_ts + row_num * interval}};
      auto partitions = vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
      ASSERT_FALSE(partitions.empty());
      TsEntityCountStats ts;
      ts.entity_id = local_eid;
      ASSERT_EQ(partitions[0]->GetCountManager()->GetEntityCountStats(ts), KStatus::SUCCESS);
      ASSERT_TRUE(ts.is_count_valid);
      ASSERT_EQ(ts.valid_count, row_num);
    }
  }

  // Step 2: write partition 2 data (same 10 entities, 10000*86400 later)
  timestamp64 start_ts2 = start_ts + 10000 * 86400;
  for (int i = 1; i <= total_entities; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, i, row_num, start_ts2, interval);
    TsRawPayload::SetOSN(pay_load, 20 + i - 1);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  // Delete half of partition 1 data + half of partition 2 data for entities 1~5
  for (int i = 1; i <= delete_count; i++) {
    uint64_t tmp_count;
    std::string p_key = GetPrimaryKey(table_id, i);
    s = engine_->DeleteData(ctx_, table_id, 0, p_key,
                            {{start_ts, start_ts + (row_num / 2) * interval - 1},
                             {start_ts2, start_ts2 + (row_num / 2) * interval - 1}},
                            &tmp_count, 0, 30 + i - 1, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(tmp_count, row_num);
  }

  // Flush
  s = engine_->FlushVGroups(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify: deleted entities have invalid count in both partitions,
  // non-deleted entities have valid count = row_num per partition.
  {
    auto tag_table = table_schema_mgr->GetTagTable();
    std::vector<KwTsSpan> ts_spans = {{start_ts, start_ts2 + row_num * interval}};
    for (int i = 1; i <= total_entities; i++) {
      std::string p_key = GetPrimaryKey(table_id, i);
      uint32_t vgroup_id, local_eid;
      ASSERT_TRUE(tag_table->hasPrimaryKey(p_key.data(), p_key.size(), local_eid, vgroup_id));
      auto vg = engine_->GetTsVGroup(vgroup_id);
      ASSERT_NE(vg, nullptr);
      auto partitions = vg->CurrentVersion()->GetPartitions(db_id, ts_spans, ts_type);
      ASSERT_EQ(partitions.size(), 2);
      for (auto& partition : partitions) {
        TsEntityCountStats ts;
        ts.entity_id = local_eid;
        ASSERT_EQ(partition->GetCountManager()->GetEntityCountStats(ts), KStatus::SUCCESS);
        if (i <= delete_count) {
          ASSERT_FALSE(ts.is_count_valid);
        } else {
          ASSERT_TRUE(ts.is_count_valid);
          ASSERT_EQ(ts.valid_count, row_num);
        }
      }
    }
  }

  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
