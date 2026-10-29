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

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "test_util.h"
#include "ts_test_base.h"
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string mei_engine_root_path = "./tsdb_vg_mei_migration";

// Covers the one-shot migration of the legacy vg.mei file (per-vgroup global
// max entity ids written by the old allocator on DropTsTable) into the
// per-db max_entity_id files:
//   1. legacy vg.mei present at startup -> counters get the per-vgroup floor;
//   2. per-db files are persisted and vg.mei is removed;
//   3. migration is idempotent across restarts and never lowers counters;
//   4. a db whose tables were all dropped (invisible to the tag scan, but
//      still holding partitions) also gets the floor.
class TestVgMeiMigration : public TsEngineTestBase {
 public:
  TestVgMeiMigration() {
    InitContext();
  }

  fs::path VgMeiPath() { return fs::path(mei_engine_root_path) / "vg.mei"; }

  fs::path PerDbCounterFile(uint32_t db_id) {
    // The per-db dir lives beside schema/ and is named with a zero-padded id.
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", db_id);
    return fs::path(mei_engine_root_path) / "db" / buf / "max_entity_id";
  }

  // Write a legacy-format vg.mei: one little-endian uint32 per vgroup.
  void WriteLegacyVgMei(uint32_t max_eid) {
    std::ofstream f(VgMeiPath(), std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(f.is_open());
    for (int i = 0; i < EngineOptions::vgroup_max_num; i++) {
      f.write(reinterpret_cast<const char*>(&max_eid), sizeof(uint32_t));
    }
    f.close();
    ASSERT_TRUE(fs::exists(VgMeiPath()));
  }

  void CreateTableAndPutRows(TSTableID table_id, uint32_t dev_id) {
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id);
    std::shared_ptr<TsTable> ts_table;
    ASSERT_EQ(engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table), KStatus::SUCCESS);

    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    bool is_dropped = false;
    ASSERT_EQ(engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr), KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema;
    ASSERT_EQ(table_schema_mgr->GetMetricMeta(1, &metric_schema), KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    ASSERT_EQ(table_schema_mgr->GetTagMeta(1, tag_schema), KStatus::SUCCESS);

    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 10, 3600);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    KStatus s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt,
                                 &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  std::shared_ptr<TsDBSchema> GetDbSchema(uint32_t db_id) {
    return engine_->GetEngineSchemaManager()->GetDbSchemaMgr()->GetDatabase(db_id);
  }

  TSEntityID MaxEntityIdOverVGroups(uint32_t db_id) {
    auto db_schema = GetDbSchema(db_id);
    if (db_schema == nullptr) {
      return 0;
    }
    TSEntityID max_eid = 0;
    for (int vg = 1; vg <= EngineOptions::vgroup_max_num; vg++) {
      max_eid = std::max(max_eid, db_schema->GetMaxEntityID(vg));
    }
    return max_eid;
  }
};

// Scenario 1 + 2: legacy vg.mei is applied as a per-vgroup floor for the old
// db, the per-db counter file is persisted, vg.mei is removed, and new
// allocations continue above the migrated floor.
TEST_F(TestVgMeiMigration, MigrationAppliesLegacyFloor) {
  const TSTableID table_id = 3001;
  const uint32_t db_id = 1;
  const uint32_t kLegacyMax = 1000;

  InitEngine(mei_engine_root_path);
  CreateTableAndPutRows(table_id, 1);
  ASSERT_GE(MaxEntityIdOverVGroups(db_id), 1);
  ASSERT_LT(MaxEntityIdOverVGroups(db_id), kLegacyMax);
  DestroyEngine();

  WriteLegacyVgMei(kLegacyMax);
  InitEngine(mei_engine_root_path, false);

  EXPECT_FALSE(fs::exists(VgMeiPath()));
  EXPECT_TRUE(fs::exists(PerDbCounterFile(db_id)));
  auto db_schema = GetDbSchema(db_id);
  ASSERT_NE(db_schema, nullptr);
  for (int vg = 1; vg <= EngineOptions::vgroup_max_num; vg++) {
    EXPECT_GE(db_schema->GetMaxEntityID(vg), kLegacyMax);
  }

  // A new tag must be allocated above the migrated floor.
  CreateTableAndPutRows(table_id, 2);
  EXPECT_EQ(MaxEntityIdOverVGroups(db_id), kLegacyMax + 1);
}

// Scenario 3: a second restart (vg.mei already gone) keeps the counters, and
// re-planting a stale vg.mei with a smaller value never lowers them.
TEST_F(TestVgMeiMigration, MigrationIsIdempotentAcrossRestarts) {
  const TSTableID table_id = 3101;
  const uint32_t db_id = 1;
  const uint32_t kLegacyMax = 500;

  InitEngine(mei_engine_root_path);
  CreateTableAndPutRows(table_id, 1);
  DestroyEngine();

  WriteLegacyVgMei(kLegacyMax);
  InitEngine(mei_engine_root_path, false);
  ASSERT_FALSE(fs::exists(VgMeiPath()));
  DestroyEngine();

  // Second restart without vg.mei: counters restored from the per-db file.
  InitEngine(mei_engine_root_path, false);
  auto db_schema = GetDbSchema(db_id);
  ASSERT_NE(db_schema, nullptr);
  for (int vg = 1; vg <= EngineOptions::vgroup_max_num; vg++) {
    EXPECT_GE(db_schema->GetMaxEntityID(vg), kLegacyMax);
  }
  DestroyEngine();

  // A stale vg.mei with a smaller value must not lower the counters
  // (migration merges with max).
  WriteLegacyVgMei(kLegacyMax / 2);
  InitEngine(mei_engine_root_path, false);
  EXPECT_FALSE(fs::exists(VgMeiPath()));
  db_schema = GetDbSchema(db_id);
  ASSERT_NE(db_schema, nullptr);
  for (int vg = 1; vg <= EngineOptions::vgroup_max_num; vg++) {
    EXPECT_GE(db_schema->GetMaxEntityID(vg), kLegacyMax);
  }
}

// Scenario 4: a db whose only table was dropped is invisible to the tag scan
// after restart, but its partitions remain; the migration must still apply
// the legacy floor to it via the partition enumeration.
TEST_F(TestVgMeiMigration, DroppedTableDbStillGetsFloor) {
  const TSTableID table_id = 3201;
  const uint32_t db_id = 1;
  const uint32_t kLegacyMax = 800;

  InitEngine(mei_engine_root_path);
  CreateTableAndPutRows(table_id, 1);
  // Flush so the written data materializes partitions that outlive the drop.
  for (const auto& vgroup : *engine_->GetTsVGroups()) {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }
  ASSERT_EQ(engine_->DropTsTable(ctx_, table_id), KStatus::SUCCESS);
  DestroyEngine();

  WriteLegacyVgMei(kLegacyMax);
  InitEngine(mei_engine_root_path, false);

  // Scenario preconditions: the table is gone from the schema view, while at
  // least one partition of the db is still on disk.
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  bool is_dropped = false;
  EXPECT_NE(engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr), KStatus::SUCCESS);
  bool db_has_partition = false;
  for (const auto& vgroup : *engine_->GetTsVGroups()) {
    for (const auto& partition : vgroup->CurrentVersion()->GetAllPartitions()) {
      if (std::get<0>(partition.first) == db_id) {
        db_has_partition = true;
      }
    }
  }
  ASSERT_TRUE(db_has_partition);

  EXPECT_FALSE(fs::exists(VgMeiPath()));
  EXPECT_TRUE(fs::exists(PerDbCounterFile(db_id)));
  auto db_schema = GetDbSchema(db_id);
  ASSERT_NE(db_schema, nullptr);
  for (int vg = 1; vg <= EngineOptions::vgroup_max_num; vg++) {
    EXPECT_GE(db_schema->GetMaxEntityID(vg), kLegacyMax);
  }
}
