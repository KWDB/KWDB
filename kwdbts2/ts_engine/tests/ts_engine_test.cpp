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

#include <fcntl.h>
#include <unistd.h>
#include "cm_kwdb_context.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TsEngineV2Test : public TsEngineTestBase {
 public:
  TsEngineV2Test() {
    InitContext();
    InitEngine(engine_root_path);
  }
};

TEST_F(TsEngineV2Test, empty) {
}

TEST_F(TsEngineV2Test, simpleInsert) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  timestamp64 start_ts = 10086000;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TsEngineV2Test, InsertMulitMemSeg) {
  using namespace roachpb;
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 ts = 10086000;
  for (int i = 0; i < 100000; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts += 1000;
  }
}

TEST_F(TsEngineV2Test, InsertMulitMemSeg2) {
  using namespace roachpb;
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 ts = 10086000;
  for (size_t j = 0; j < 5; j++) {
    for (int i = 0; i < 10000; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
  }
}

TEST_F(TsEngineV2Test, CreateCheckpoint) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 ts = 10087000;
  
  // Insert some data before checkpoint
  for (size_t j = 0; j < 3; j++) {
    for (int i = 0; i < 100; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
  }
  
  // Test 1: TSMtrBegin in single node mode
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 1;
    uint64_t mtr_id = 0;
    const char* tsx_id = "test_tsx_001";
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }
  
  // Create checkpoint after TSMtrBegin
  s = engine_->CreateCheckpoint(&ctx);
  ASSERT_EQ(s , KStatus::SUCCESS);

  
  // Insert more data after checkpoint
  for (size_t j = 0; j < 2; j++) {
    for (int i = 0; i < 50; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
  }
  // Final checkpoint
  s = engine_->CreateCheckpoint(&ctx);
  ASSERT_EQ(s , KStatus::SUCCESS);

}

TEST_F(TsEngineV2Test, Recover){
  using namespace roachpb;
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  kwdbContext_t ctx;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 ts = 10086000;
  for (size_t j = 0; j < 5; j++) {
    for (int i = 0; i < 10000; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
  }

  s = engine_->CreateCheckpoint(&ctx);
  ASSERT_EQ(s , KStatus::SUCCESS);

  for (size_t j = 0; j < 5; j++) {
    for (int i = 0; i < 10000; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema , table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
  }

  s = engine_->Recover(&ctx);
  ASSERT_EQ(s , KStatus::SUCCESS);
}

TEST_F(TsEngineV2Test, TableCache){
  using namespace roachpb;
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  kwdbContext_t ctx;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  SharedFixedUnorderedMap<KTableKey, TsTable> table_cache(50, true);
  for (int i = 0; i < 30; ++i) {
    table_cache.Put(i, nullptr);
  }
  ASSERT_EQ(table_cache.Size(), 30);
  for (int i = 30; i < 60; ++i) {
    table_cache.Put(i, nullptr);
  }
  ASSERT_EQ(table_cache.Size(), 50);
  table_cache.SetCapacity(40);
  ASSERT_EQ(table_cache.Size(), 40);
  table_cache.SetCapacity(55);
  std::vector<std::shared_ptr<TsTable>> tables;
  const std::vector<std::shared_ptr<TsVGroup>> vgroups = {};
  for (int i = 0; i < 60; ++i) {
    std::shared_ptr<TsTable> table = std::make_shared<TsTableV2Impl>(schema_mgr, vgroups);
    tables.push_back(table);
    table_cache.Put(i, table);
  }
  ASSERT_EQ(table_cache.Size(), 60);
  tables.clear();
  table_cache.SetCapacity(55);
  ASSERT_EQ(table_cache.Size(), 55);
  auto kv = table_cache.GetAllValues();
  for (auto it = kv.begin(); it != kv.end(); ++it) {
    ASSERT_LT(it->first, 60);
  }
}

TEST_F(TsEngineV2Test, LoadVGroupCfgTest) {
  KStatus s;

  // Test 1: Configuration file does not exist, should return SUCCESS
  {
    std::map<int, std::string> empty_cfg;
    // Directly use engine_root_path since ts-vgroup.cfg file does not exist
    s = loadVGroupCfg(engine_root_path, empty_cfg);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(empty_cfg.size(), 0);
  }

  // Test 2: Valid vgroup configuration
  {
    std::map<int, std::string> valid_cfg;
    fs::path valid_cfg_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(valid_cfg_path);
    ASSERT_TRUE(ofs.is_open());
    // Write complete vgroup configuration
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string vg_dir = engine_root_path + "/valid_vg" + std::to_string(i);
      MakeDirectory(vg_dir.c_str());
      ofs << i << ", " << vg_dir << '\n';
    }
    ofs.close();
    s = loadVGroupCfg(engine_root_path, valid_cfg);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(valid_cfg.size(), EngineOptions::vgroup_max_num);
    // Verify each vgroup configuration (Note: paths are normalized, removing ./ prefix)
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string expected_path = "tsdb/valid_vg" + std::to_string(i);
      EXPECT_EQ(valid_cfg[i], expected_path);
    }
    // Cleanup
    Remove(valid_cfg_path);
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      Remove(engine_root_path + "/valid_vg" + std::to_string(i));
    }
  }

  // Test 3: Invalid vgroup ID (ID is 0)
  {
    std::map<int, std::string> invalid_id_cfg;
    fs::path invalid_id_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(invalid_id_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << "0, " << engine_root_path << "/vg0" << '\n';
    ofs.close();
    MakeDirectory((engine_root_path + "/vg0").c_str());
    s = loadVGroupCfg(engine_root_path, invalid_id_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(invalid_id_path);
    Remove(engine_root_path + "/vg0");
  }

  // Test 4: Invalid vgroup ID (exceeds maximum)
  {
    std::map<int, std::string> exceed_id_cfg;
    fs::path exceed_id_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(exceed_id_path);
    ASSERT_TRUE(ofs.is_open());
    int invalid_id = EngineOptions::vgroup_max_num + 1;
    ofs << invalid_id << ", " << engine_root_path << "/vg_invalid" << '\n';
    ofs.close();
    MakeDirectory((engine_root_path + "/vg_invalid").c_str());
    s = loadVGroupCfg(engine_root_path, exceed_id_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(exceed_id_path);
    Remove(engine_root_path + "/vg_invalid");
  }

  // Test 5: Duplicate vgroup ID
  {
    std::map<int, std::string> duplicate_cfg;
    fs::path duplicate_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(duplicate_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << "1, " << engine_root_path << "/vg1_dup1" << std::endl;
    ofs << "1, " << engine_root_path << "/vg1_dup2" << '\n';
    ofs.close();
    MakeDirectory((engine_root_path + "/vg1_dup1").c_str());
    MakeDirectory((engine_root_path + "/vg1_dup2").c_str());
    s = loadVGroupCfg(engine_root_path, duplicate_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(duplicate_path);
    Remove(engine_root_path + "/vg1_dup1");
    Remove(engine_root_path + "/vg1_dup2");
  }

  // Test 6: Directory does not exist
  {
    std::map<int, std::string> no_dir_cfg;
    fs::path no_dir_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(no_dir_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << "1, " << engine_root_path << "/non_existent_dir" << '\n';
    ofs.close();
    s = loadVGroupCfg(engine_root_path, no_dir_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(no_dir_path);
  }

  // Test 7: Incomplete vgroup configuration count
  {
    std::map<int, std::string> incomplete_cfg;
    fs::path incomplete_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(incomplete_path);
    ASSERT_TRUE(ofs.is_open());
    // Only configure partial vgroups (should configure EngineOptions::vgroup_max_num vgroups)
    for (int i = 1; i < EngineOptions::vgroup_max_num; i++) {
      std::string vg_dir = engine_root_path + "/incomplete_vg" + std::to_string(i);
      MakeDirectory(vg_dir.c_str());
      ofs << i << ", " << vg_dir << '\n';
    }
    ofs.close();
    s = loadVGroupCfg(engine_root_path, incomplete_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(incomplete_path);
    for (int i = 1; i < EngineOptions::vgroup_max_num; i++) {
      Remove(engine_root_path + "/incomplete_vg" + std::to_string(i));
    }
  }

  // Test 8: Configuration with comments and empty lines
  {
    std::map<int, std::string> comment_cfg;
    fs::path comment_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(comment_path);
    ASSERT_TRUE(ofs.is_open());
    // Add comments and empty lines
    ofs << "# This is a comment" << '\n';
    ofs << '\n';
    ofs << "  # Another comment with leading spaces" << '\n';
    ofs << "\t" << '\n';  // Tab only line
    // Write valid configuration
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string vg_dir = engine_root_path + "/comment_vg" + std::to_string(i);
      MakeDirectory(vg_dir.c_str());
      // Add some configuration lines with spaces
      if (i % 2 == 0) {
        ofs << "  " << i << "  ,  " << vg_dir << "  " << '\n';
      } else {
        ofs << i << "," << vg_dir << '\n';
      }
    }
    ofs << "# Final comment" << '\n';
    ofs.close();
    s = loadVGroupCfg(engine_root_path, comment_cfg);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(comment_cfg.size(), EngineOptions::vgroup_max_num);
    // Verify paths are correctly trimmed (Note: paths are normalized, removing ./ prefix)
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string expected_path = "tsdb/comment_vg" + std::to_string(i);
      EXPECT_EQ(comment_cfg[i], expected_path);
    }
    // Cleanup
    Remove(comment_path);
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      Remove(engine_root_path + "/comment_vg" + std::to_string(i));
    }
  }

  // Test 9: Path with trailing slashes
  {
    std::map<int, std::string> slash_cfg;
    fs::path slash_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(slash_path);
    ASSERT_TRUE(ofs.is_open());
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string vg_dir = engine_root_path + "/slash_vg" + std::to_string(i);
      MakeDirectory(vg_dir.c_str());
      // Add multiple trailing slashes
      ofs << i << ", " << vg_dir << "///" << '\n';
    }
    ofs.close();
    s = loadVGroupCfg(engine_root_path, slash_cfg);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(slash_cfg.size(), EngineOptions::vgroup_max_num);
    // Verify paths are normalized (removing trailing slashes and ./ prefix)
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      std::string expected_path = "tsdb/slash_vg" + std::to_string(i);
      EXPECT_EQ(slash_cfg[i], expected_path);
    }
    // Cleanup
    Remove(slash_path);
    for (int i = 1; i <= EngineOptions::vgroup_max_num; i++) {
      Remove((engine_root_path + "/slash_vg" + std::to_string(i)).c_str());
    }
  }

  // Test 10: Non-numeric vgroup ID
  {
    std::map<int, std::string> non_numeric_cfg;
    fs::path non_numeric_path = fs::path(engine_root_path) / "ts-vgroup.cfg";
    std::ofstream ofs(non_numeric_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << "abc, " << engine_root_path << "/vg_abc" << '\n';
    ofs.close();
    MakeDirectory((engine_root_path + "/vg_abc").c_str());
    s = loadVGroupCfg(engine_root_path, non_numeric_cfg);
    EXPECT_EQ(s, KStatus::FAIL);
    Remove(non_numeric_path);
    Remove(engine_root_path + "/vg_abc");
  }
}

// Comprehensive test for SortWALFile function
TEST_F(TsEngineV2Test, SortWALFileTest) {
  using namespace roachpb;
  KStatus s;
  
  // Test 1: No checkpoint file - should return SUCCESS immediately
  {
    s = engine_->SortWALFile(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 2: Checkpoint file exists with END_CHECKPOINT - test through CreateCheckpoint
  {
    // Create table and insert data to generate WAL logs
    TSTableID table_id = 99991;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    timestamp64 ts = 1000000;
    // Insert some data to generate WAL logs
    for (int i = 0; i < 10; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(payload.data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
    // Create checkpoint - this will write END_CHECKPOINT to WAL
    s = engine_->CreateCheckpoint(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Call SortWALFile - should process the checkpoint correctly
    s = engine_->SortWALFile(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 3: Checkpoint file without END_CHECKPOINT - test with incomplete checkpoint
  {
    // Create table but don't create checkpoint, so no END_CHECKPOINT will be present
    TSTableID table_id = 99993;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Insert some data to generate WAL logs but no checkpoint
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    timestamp64 ts = 1000000;
    for (int i = 0; i < 5; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(payload.data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
    // Call SortWALFile - should handle incomplete checkpoint (no END_CHECKPOINT)
    s = engine_->SortWALFile(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 4: Vgroups with mixed checkpoint states - test with real checkpoint
  {
    // Create table and data, then create checkpoint
    TSTableID table_id = 99992;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Create checkpoint which will create engine and vgroup checkpoint files
    s = engine_->CreateCheckpoint(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = engine_->SortWALFile(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 5: Empty checkpoint file
  {
    // Create empty checkpoint file
    std::string chk_path = engine_root_path + "/wal/engine/kwdb_wal.chk";
    std::string dir_path = engine_root_path + "/wal/engine";
    MakeDirectory(dir_path);
    std::ofstream ofs(chk_path, std::ios::binary | std::ios::out);
    ofs.close();
    s = engine_->SortWALFile(ctx_);
    // Should handle empty checkpoint gracefully
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Cleanup
  Remove(engine_root_path + "/wal");
}

// Comprehensive test for Create/Drop/Alter NormalTagIndex functions
TEST_F(TsEngineV2Test, NormalTagIndexTest) {
  using namespace roachpb;
  KStatus s;
  // Complete lifecycle - Create -> Alter -> Drop
  TSTableID table_id = 77807;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  const uint64_t index_id = 3;
  std::string txn_id = "5041921481932003";
  // Create index
  s = engine_->CreateNormalTagIndex(ctx_, table_id, index_id, txn_id.c_str(),
                                    is_dropped, 1, 2, std::vector<uint32_t>{3});
  EXPECT_TRUE(s == KStatus::SUCCESS);
  // Alter index (stub)
  s = engine_->AlterNormalTagIndex(ctx_, table_id, index_id, txn_id.c_str(),
                                   is_dropped, 1, 2, std::vector<uint32_t>{3});
  EXPECT_EQ(s, KStatus::SUCCESS);
  // Drop index
  s = engine_->DropNormalTagIndex(ctx_, table_id, index_id, txn_id.c_str(),
                                  is_dropped, 2, 3);
  EXPECT_TRUE(s == KStatus::SUCCESS);
}

// Test GetClusterSetting and UpdateSetting functions
TEST_F(TsEngineV2Test, ClusterSettings) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  InitKWDBContext(&ctx);

  std::string value;
  // Test 1: Get non-existent setting - should return FAIL
  s = engine_->GetClusterSetting(&ctx, "nonexistent.key", &value);
  EXPECT_EQ(s, KStatus::FAIL);
  // Test 2: Set wal_level to 0 (disable WAL) and verify GetClusterSetting works
  TSSlice key{const_cast<char*>("ts.wal.wal_level"), strlen("ts.wal.wal_level")};
  TSSlice val{const_cast<char*>("0"), 1};
  // set cluster setting
  TSSetClusterSetting(key, val);
  // Now get the setting we just set - should return SUCCESS
  s = engine_->GetClusterSetting(&ctx, "ts.wal.wal_level", &value);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(value, "0");
  // Test 3: Set wal_level to 1 (enable WAL)
  TSSlice val2{const_cast<char*>("1"), 1};
  TSSetClusterSetting(key, val2);
  s = engine_->GetClusterSetting(&ctx, "ts.wal.wal_level", &value);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(value, "1");
  // Test 4: Change wal_level from 1 to 2
  TSSlice val3{const_cast<char*>("2"), 1};
  TSSetClusterSetting(key, val3);
  s = engine_->UpdateSetting(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = engine_->GetClusterSetting(&ctx, "ts.wal.wal_level", &value);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(value, "2");
}

TEST_F(TsEngineV2Test, BlocksDistributionTest) {
  KStatus s;
  
  // Test 1: GetTableBlocksDistribution with empty table (no data)
  {
    TSTableID table_id = 88001;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    TSSlice blocks_info;
    s = engine_->GetTableBlocksDistribution(table_id, &blocks_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
    roachpb::BlocksDistribution distribution;
    EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
    EXPECT_GE(distribution.block_info_size(), 1);
    if (distribution.block_info_size() > 0) {
      auto total_info = distribution.block_info(0);
      EXPECT_EQ(total_info.blocks_num(), 0);
      EXPECT_EQ(total_info.blocks_size(), 0);
    }
    free(blocks_info.data);
  }
  
  // Test 2: GetTableBlocksDistribution with data inserted
  {
    TSTableID table_id = 88002;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    timestamp64 ts = 10086000;
    for (int i = 0; i < 100; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
    
    // Flush VGroups to generate blocks
    s = engine_->FlushVGroups(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    TSSlice blocks_info;
    s = engine_->GetTableBlocksDistribution(table_id, &blocks_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
    roachpb::BlocksDistribution distribution;
    EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
    EXPECT_GE(distribution.block_info_size(), 1);
    bool found_total = false;
    for (int i = 0; i < distribution.block_info_size(); ++i) {
      auto info = distribution.block_info(i);
      if (info.level() == "total") {
        found_total = true;
        EXPECT_GT(info.blocks_num(), 0);
        EXPECT_GT(info.blocks_size(), 0);
        if (info.blocks_num() > 0) {
          EXPECT_GT(info.avg_size(), 0);
        }
        break;
      }
    }
    EXPECT_TRUE(found_total);
    free(blocks_info.data);
  }
  
  // Test 3: GetDBBlocksDistribution with single table
  {
    TSTableID table_id = 88003;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint32_t db_id = schema_mgr->GetDbID();
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    timestamp64 ts = 10086000;
    for (int i = 0; i < 50; ++i) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ts += 1000;
    }
    
    // Flush VGroups to generate blocks
    s = engine_->FlushVGroups(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    TSSlice blocks_info;
    s = engine_->GetDBBlocksDistribution(db_id, &blocks_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
    roachpb::BlocksDistribution distribution;
    EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
    EXPECT_GE(distribution.block_info_size(), 1);
    if (distribution.block_info_size() > 0) {
      auto total_info = distribution.block_info(0);
      EXPECT_EQ(total_info.level(), "total");
      EXPECT_GT(total_info.blocks_num(), 0);
      EXPECT_GT(total_info.blocks_size(), 0);
      if (total_info.blocks_num() > 0) {
        EXPECT_GT(total_info.avg_size(), 0);
      }
    }
    free(blocks_info.data);
  }
  
  // Test 4: GetDBBlocksDistribution with multiple tables in same DB
  {
    uint32_t db_id = 0;
    std::vector<TSTableID> table_ids;
    for (int t = 0; t < 3; ++t) {
      TSTableID table_id = 88010 + t;
      table_ids.push_back(table_id);
      roachpb::CreateTsTable pb_meta;
      std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
      ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
      std::shared_ptr<TsTable> ts_table;
      s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (t == 0) {
        std::shared_ptr<TsTableSchemaManager> schema_mgr;
        bool is_dropped = false;
        s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
        ASSERT_EQ(s, KStatus::SUCCESS);
        db_id = schema_mgr->GetDbID();
      }
      std::shared_ptr<TsTableSchemaManager> schema_mgr;
      bool is_dropped = false;
      s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
      ASSERT_EQ(s, KStatus::SUCCESS);
      const std::vector<AttributeInfo>* metric_schema{nullptr};
      s = schema_mgr->GetMetricMeta(1, &metric_schema);
      ASSERT_EQ(s, KStatus::SUCCESS);
      std::vector<TagInfo> tag_schema;
      s = schema_mgr->GetTagMeta(1, tag_schema);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt = 0;
      DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
      timestamp64 ts = 10086000;
      for (int i = 0; i < 30; ++i) {
        auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
        s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
        free(pay_load.data);
        ASSERT_EQ(s, KStatus::SUCCESS);
        ts += 1000;
      }
    }
    
    // Flush VGroups to generate blocks
    s = engine_->FlushVGroups(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    TSSlice blocks_info;
    s = engine_->GetDBBlocksDistribution(db_id, &blocks_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
    roachpb::BlocksDistribution distribution;
    EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
    EXPECT_GE(distribution.block_info_size(), 1);
    if (distribution.block_info_size() > 0) {
      auto total_info = distribution.block_info(0);
      EXPECT_EQ(total_info.level(), "total");
      EXPECT_GT(total_info.blocks_num(), 0);
      EXPECT_GT(total_info.blocks_size(), 0);
    }
    free(blocks_info.data);
  }
  
  // Test 5: GetTableBlocksDistribution with invalid table ID
  {
    TSTableID invalid_table_id = 999999;
    TSSlice blocks_info;
    s = engine_->GetTableBlocksDistribution(invalid_table_id, &blocks_info);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 6: GetDBBlocksDistribution with non-existent DB ID
  {
    uint32_t invalid_db_id = 999999;
    TSSlice blocks_info;
    s = engine_->GetDBBlocksDistribution(invalid_db_id, &blocks_info);
    EXPECT_EQ(s, KStatus::SUCCESS);
    roachpb::BlocksDistribution distribution;
    EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
    if (distribution.block_info_size() > 0) {
      auto total_info = distribution.block_info(0);
      EXPECT_EQ(total_info.blocks_num(), 0);
      EXPECT_EQ(total_info.blocks_size(), 0);
    }
    free(blocks_info.data);
  }
}

// Comprehensive test for DropColumn and AlterColumnType functions
TEST_F(TsEngineV2Test, DropAndAlterAndAddColumnTest) {
  using namespace roachpb;
  KStatus s;
  // Test 1: DropColumn - Successful column drop
  {
    TSTableID table_id = 66001;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Prepare column metadata to drop (column 3 - DOUBLE)
    KWDBKTSColumn column_to_drop;
    column_to_drop.set_column_id(3);
    column_to_drop.set_name("column_3");
    column_to_drop.set_storage_type(roachpb::DOUBLE);
    column_to_drop.set_nullable(true);
    column_to_drop.set_dropped(true);
    std::string column_data;
    column_to_drop.SerializeToString(&column_data);
    TSSlice column_slice{const_cast<char*>(column_data.c_str()), column_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932004";
    // Try to drop column
    s = engine_->DropColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                           is_dropped, column_slice, 1, 2, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
  }
  
  // Test 2: DropColumn - Invalid protobuf data (parse failure)
  {
    TSTableID table_id = 66002;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Invalid protobuf data
    TSSlice invalid_slice{const_cast<char*>("invalid_protobuf_data"), 21};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932005";
    s = engine_->DropColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                           is_dropped, invalid_slice, 1, 2, err_msg);
    // Should fail due to protobuf parsing error
    EXPECT_EQ(s, KStatus::FAIL);
    EXPECT_FALSE(err_msg.empty());
  }
  // Test 3: DropColumn - Non-existent table
  {
    TSTableID non_existent_table = 999999;
    KWDBKTSColumn column_to_drop;
    column_to_drop.set_column_id(2);
    column_to_drop.set_name("column_2");
    column_to_drop.set_storage_type(roachpb::INT);
    column_to_drop.set_nullable(true);
    column_to_drop.set_dropped(true);
    std::string column_data;
    column_to_drop.SerializeToString(&column_data);
    TSSlice column_slice{const_cast<char*>(column_data.c_str()), column_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932006";
    s = engine_->DropColumn(ctx_, non_existent_table, const_cast<char*>(txn_id.c_str()), 
                           is_dropped, column_slice, 1, 2, err_msg);
    // Should fail because table doesn't exist
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 4: AlterColumnType - Successful column type alteration
  {
    TSTableID table_id = 66005;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Prepare new column metadata (change INT to BIGINT)
    KWDBKTSColumn new_col_meta;
    new_col_meta.set_column_id(2);
    new_col_meta.set_name("column_2");
    new_col_meta.set_storage_type(roachpb::BIGINT);  // Change from INT to BIGINT
    new_col_meta.set_nullable(true);
    std::string new_col_data;
    new_col_meta.SerializeToString(&new_col_data);
    TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
    // Origin column reference
    KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(2);
    origin_col_meta.set_name("column_2");
    origin_col_meta.set_storage_type(roachpb::INT);
    std::string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932007";
    s = engine_->AlterColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), is_dropped,
      new_col_slice, origin_col_slice, 1, 2, AlterType::ALTER_COLUMN_TYPE, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
  }
  
  // Test 5: AlterColumnType - Invalid new column protobuf data
  {
    TSTableID table_id = 66006;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Invalid new column protobuf
    TSSlice invalid_new_col{const_cast<char*>("invalid_new_col"), 17};
    KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(2);
    std::string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932008";
    s = engine_->AlterColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), is_dropped,
      invalid_new_col, origin_col_slice, 1, 2, AlterType::ALTER_COLUMN_TYPE, err_msg);
    // Should fail due to protobuf parsing error
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 6: AlterColumnType - Non-existent table
  {
    TSTableID non_existent_table = 999999;
    KWDBKTSColumn new_col_meta;
    new_col_meta.set_column_id(2);
    new_col_meta.set_storage_type(roachpb::BIGINT);
    std::string new_col_data;
    new_col_meta.SerializeToString(&new_col_data);
    TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
    KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(2);
    std::string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932010";
    s = engine_->AlterColumn(ctx_, non_existent_table, const_cast<char*>(txn_id.c_str()), is_dropped,
      new_col_slice, origin_col_slice, 1, 2, AlterType::ALTER_COLUMN_TYPE, err_msg);
    // Should fail because table doesn't exist
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 7: AddColumn - Successful column addition
  {
    TSTableID table_id = 66011;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Prepare new column metadata (add a FLOAT column)
    KWDBKTSColumn new_col;
    new_col.set_column_id(4);
    new_col.set_name("column_4");
    new_col.set_storage_type(roachpb::FLOAT);
    new_col.set_nullable(true);
    new_col.set_dropped(false);
    std::string col_data;
    new_col.SerializeToString(&col_data);
    TSSlice col_slice{const_cast<char*>(col_data.c_str()), col_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932011";
    // Add new column
    s = engine_->AddColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                          is_dropped, col_slice, 1, 2, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
  }
  
  // Test 8: AddColumn - Invalid protobuf data (parse failure)
  {
    TSTableID table_id = 66012;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Invalid protobuf data
    TSSlice invalid_slice{const_cast<char*>("invalid_protobuf_data"), 21};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932012";
    s = engine_->AddColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                          is_dropped, invalid_slice, 1, 2, err_msg);
    // Should fail due to protobuf parsing error
    EXPECT_EQ(s, KStatus::FAIL);
    EXPECT_FALSE(err_msg.empty());
  }
  
  // Test 9: AddColumn - Non-existent table
  {
    TSTableID non_existent_table = 999999;
    KWDBKTSColumn new_col;
    new_col.set_column_id(4);
    new_col.set_name("column_4");
    new_col.set_storage_type(roachpb::FLOAT);
    new_col.set_nullable(true);
    std::string col_data;
    new_col.SerializeToString(&col_data);
    TSSlice col_slice{const_cast<char*>(col_data.c_str()), col_data.size()};
    bool is_dropped = false;
    std::string err_msg;
    std::string txn_id = "5041921481932013";
    s = engine_->AddColumn(ctx_, non_existent_table, const_cast<char*>(txn_id.c_str()), 
                          is_dropped, col_slice, 1, 2, err_msg);
    // Should fail because table doesn't exist
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 10: Complete lifecycle - Create table -> Add column -> Alter type -> Drop column
  {
    TSTableID table_id = 66015;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::string txn_id = "5041921481932014";
    bool is_dropped = false;
    std::string err_msg;
    // Step 1: Add a new column (FLOAT)
    KWDBKTSColumn new_col;
    new_col.set_column_id(4);
    new_col.set_name("column_4");
    new_col.set_storage_type(roachpb::FLOAT);
    new_col.set_nullable(true);
    new_col.set_dropped(false);
    std::string col_data;
    new_col.SerializeToString(&col_data);
    TSSlice col_slice{const_cast<char*>(col_data.c_str()), col_data.size()};
    s = engine_->AddColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                          is_dropped, col_slice, 1, 2, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
    // Step 2: Alter the added column type (FLOAT -> BIGINT)
    KWDBKTSColumn alter_col_meta;
    alter_col_meta.set_column_id(4);
    alter_col_meta.set_name("column_4");
    alter_col_meta.set_storage_type(roachpb::BIGINT);
    alter_col_meta.set_nullable(true);
    std::string alter_col_data;
    alter_col_meta.SerializeToString(&alter_col_data);
    TSSlice alter_col_slice{const_cast<char*>(alter_col_data.c_str()), alter_col_data.size()};
    KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(4);
    origin_col_meta.set_name("column_4");
    origin_col_meta.set_storage_type(roachpb::FLOAT);
    std::string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    s = engine_->AlterColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), is_dropped,
      alter_col_slice, origin_col_slice, 2, 3, AlterType::ALTER_COLUMN_TYPE, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
    // Step 3: Drop another column (INT column)
    KWDBKTSColumn column_to_drop;
    column_to_drop.set_column_id(2);
    column_to_drop.set_name("column_2");
    column_to_drop.set_storage_type(roachpb::INT);
    column_to_drop.set_nullable(true);
    column_to_drop.set_dropped(true);
    std::string drop_col_data;
    column_to_drop.SerializeToString(&drop_col_data);
    TSSlice drop_col_slice{const_cast<char*>(drop_col_data.c_str()), drop_col_data.size()};
    s = engine_->DropColumn(ctx_, table_id, const_cast<char*>(txn_id.c_str()), 
                           is_dropped, drop_col_slice, 3, 4, err_msg);
    EXPECT_TRUE(s == KStatus::SUCCESS);
  }
}

// Test for AlterLifetime function covering success and failure
TEST_F(TsEngineV2Test, AlterLifetimeTest) {
  using namespace roachpb;
  KStatus s;
  
  // Test 1: Success - Alter lifetime on a valid table
  {
    TSTableID table_id = 88001;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    // Get initial lifetime
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    // Alter lifetime to 3600 seconds (1 hour)
    uint64_t new_lifetime = 3600;
    s = engine_->AlterLifetime(ctx_, table_id, new_lifetime, is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
    
    // Verify the lifetime was changed
    std::shared_ptr<TsTableSchemaManager> schema_mgr_after;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr_after);
    ASSERT_EQ(s, KStatus::SUCCESS);
    LifeTime updated_lifetime = schema_mgr_after->GetLifeTime();
    EXPECT_EQ(updated_lifetime.ts, static_cast<int64_t>(new_lifetime));  // Lifetime is stored in seconds
    EXPECT_EQ(updated_lifetime.precision, 1000);
  }
  
  // Test 2: Failure - Alter lifetime on non-existent table
  {
    TSTableID non_existent_table = 999999;
    uint64_t lifetime = 7200;
    bool is_dropped = false;
    
    // Try to alter lifetime on table that doesn't exist
    s = engine_->AlterLifetime(ctx_, non_existent_table, lifetime, is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }
}

// Test for TSMtrBegin, TSMtrCommit, and TSMtrRollback functions
TEST_F(TsEngineV2Test, MtrOperationsTest) {
  using namespace roachpb;
  KStatus s;

  // Create a table for MTR operations
  TSTableID table_id = 99001;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Test 1: Success - Complete MTR lifecycle (Begin -> Commit)
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 1;
    uint64_t mtr_id = 0;
    const char* tsx_id = "mtr_001";
    // Begin MTR
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Commit MTR
    s = engine_->TSMtrCommit(ctx_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 2: Success - MTR with Rollback
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 2;
    uint64_t mtr_id = 0;
    const char* tsx_id = "mtr_002";
    // Begin MTR
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Rollback MTR (skip_log = false)
    s = engine_->TSMtrRollback(ctx_, table_id, range_group_id, mtr_id, false, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 3: Success - MTR with skip_log = true
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 3;
    uint64_t mtr_id = 0;
    const char* tsx_id = "txn_mtr_003";

    // Begin MTR
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);

    // Rollback MTR with skip_log = true
    s = engine_->TSMtrRollback(ctx_, table_id, range_group_id, mtr_id, true, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 4: Success - MTR when WAL is disabled (wal_level = 0)
  {
    kwdbContext_t ctx;
    InitKWDBContext(&ctx);
    // First disable WAL by setting wal_level to 0
    TSSlice key{const_cast<char*>("ts.wal.wal_level"), strlen("ts.wal.wal_level")};
    TSSlice val{const_cast<char*>("0"), 1};
    TSSetClusterSetting(key, val);
    s = engine_->UpdateSetting(&ctx);
    EXPECT_EQ(s, KStatus::SUCCESS);

    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 4;
    uint64_t mtr_id = 0;
    const char* tsx_id = "mtr_004";

    // Begin MTR should return SUCCESS immediately when WAL is disabled
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Commit MTR should return SUCCESS immediately when WAL is disabled
    s = engine_->TSMtrCommit(ctx_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Rollback MTR should return SUCCESS immediately when WAL is disabled
    s = engine_->TSMtrRollback(ctx_, table_id, range_group_id, mtr_id, false, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Restore WAL level to 2
    TSSlice val2{const_cast<char*>("2"), 1};
    TSSetClusterSetting(key, val2);
    s = engine_->UpdateSetting(&ctx);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 5: Success - MTR with null tsx_id
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 5;
    uint64_t mtr_id = 0;
    const char* tsx_id = nullptr;
    // Begin MTR with null tsx_id
    s = engine_->TSMtrBegin(ctx_, table_id, range_group_id, range_id, index, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Commit MTR with null tsx_id
    s = engine_->TSMtrCommit(ctx_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

// Test for RemoveChkFile and ParallelRemoveChkFiles functions
TEST_F(TsEngineV2Test, RemoveCheckpointFilesTest) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  InitKWDBContext(&ctx);
  
  // Create a table and some data to ensure checkpoint files exist
  TSTableID table_id = 99002;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Insert some data
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  timestamp64 ts = 70000000;
  for (int i = 0; i < 100; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts += 1000;
  }
  
  // Create checkpoint to ensure checkpoint files exist
  s = engine_->CreateCheckpoint(&ctx);
  ASSERT_EQ(s, KStatus::SUCCESS);

  
  // Test 1: Success - RemoveChkFile for a single vgroup
  {
    uint32_t vgroup_id = 1;
    s = engine_->RemoveChkFile(ctx_, vgroup_id);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }
  
  // Recreate checkpoint files for ParallelRemoveChkFiles test
  // Insert more data
  ts = 80000000;
  for (int i = 0; i < 50; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts += 1000;
  }
  
  // Create checkpoint again
  s = engine_->CreateCheckpoint(&ctx);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Test 2: Success - ParallelRemoveChkFiles (remove all vgroups' checkpoints)
  {
    s = engine_->ParallelRemoveChkFiles(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }
}

// Test for GetTableVersion, GetTableIDList, and GetMetaData functions
TEST_F(TsEngineV2Test, TableMetadataOperationsTest) {
  using namespace roachpb;
  KStatus s;
  
  // Create a table for testing
  TSTableID table_id = 99003;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Test 1: Success - GetTableVersion on existing table
  {
    uint32_t version = 0;
    bool is_dropped = false;
    s = engine_->GetTableVersion(ctx_, table_id, &version, is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_GT(version, 0);  // Version should be greater than 0
  }
  
  // Test 2: Failure - GetTableVersion on non-existent table
  {
    TSTableID non_existent_table = 999999;
    uint32_t version = 0;
    bool is_dropped = false;
    s = engine_->GetTableVersion(ctx_, non_existent_table, &version, is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 3: Success - GetTableIDList returns all table IDs
  {
    std::vector<KTableKey> table_id_list;
    engine_->GetTableIDList(ctx_, table_id_list);
    EXPECT_FALSE(table_id_list.empty());  // Should contain at least our created table
    bool found = false;
    for (const auto& id : table_id_list) {
      if (id == table_id) {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);  // Our table should be in the list
  }
  
  // Test 4: Success - GetMetaData on existing table
  {
    CreateTsTable meta;
    RangeGroup range;
    bool is_dropped = false;
    s = engine_->GetMetaData(ctx_, table_id, range, &meta, is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_EQ(meta.ts_table().ts_table_id(), table_id);
    EXPECT_GT(meta.ts_table().ts_version(), 0);
  }
  
  // Test 5: Failure - GetMetaData on non-existent table
  {
    TSTableID non_existent_table = 999999;
    CreateTsTable meta;
    RangeGroup range;
    bool is_dropped = false;
    s = engine_->GetMetaData(ctx_, non_existent_table, range, &meta, is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }
}

// Test for DeleteRangeData, DeleteEntityByTag, DeleteMetricByTag, and CountRangeData functions
TEST_F(TsEngineV2Test, DataOperationsTest) {
  using namespace roachpb;
  KStatus s;
  
  // Create a table for testing
  TSTableID table_id = 99004;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Insert some data first
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  timestamp64 ts = 90000000;
  for (int i = 0; i < 10; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 10, ts);
    s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(payload.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts += 1000;
  }
  
  // Test 1: Success - CountRangeData on existing table
  {
    uint64_t range_group_id = 1;
    HashIdSpan hash_span = {0, UINT64_MAX};
    std::vector<KwTsSpan> ts_spans;
    KwTsSpan span{90000000, 90050000};
    ts_spans.push_back(span);
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    uint64_t osn = 0;
    s = engine_->CountRangeData(ctx_, table_id, range_group_id, hash_span, ts_spans, &count, mtr_id, osn);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 2: Failure - CountRangeData on non-existent table
  {
    TSTableID non_existent_table = 999999;
    uint64_t range_group_id = 1;
    HashIdSpan hash_span = {0, UINT64_MAX};
    std::vector<KwTsSpan> ts_spans;
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    uint64_t osn = 0;
    s = engine_->CountRangeData(ctx_, non_existent_table, range_group_id, hash_span, ts_spans, &count, mtr_id, osn);
    EXPECT_EQ(s, KStatus::FAIL);
  }

  // Test 3: Success - DeleteRangeData on existing table
  {
    uint64_t range_group_id = 1;
    HashIdSpan hash_span = {0, UINT64_MAX};
    std::vector<KwTsSpan> ts_spans;
    KwTsSpan span{90000000, 90002000};
    ts_spans.push_back(span);
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    uint64_t osn = 0;
    s = engine_->DeleteRangeData(ctx_, table_id, range_group_id, hash_span, ts_spans, &count, mtr_id, osn, is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 4: Failure - DeleteRangeData on non-existent table
  {
    TSTableID non_existent_table = 999999;
    uint64_t range_group_id = 1;
    HashIdSpan hash_span = {0, UINT64_MAX};
    std::vector<KwTsSpan> ts_spans;
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    uint64_t osn = 0;
    s = engine_->DeleteRangeData(ctx_, non_existent_table, range_group_id, hash_span, ts_spans, &count, mtr_id, osn, is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }

  std::vector<uint32_t> tags_index_id = {3};
  uint64_t dev_id = 1;
  std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
  std::vector<std::string> tags = {tag_value};
  // Test 5: Success - DeleteMetricByTag on existing table
  {
    std::vector<KwTsSpan> ts_spans;
    KwTsSpan span{90030000, 90050000};
    ts_spans.push_back(span);
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t osn = 0;
    s = engine_->DeleteMetricByTag(ctx_, table_id, is_dropped, tags_index_id, tags, ts_spans, &count, mtr_id, hash_span, osn);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  // Test 6: Success - DeleteEntityByTag on existing table
  {
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t osn = 0;
    s = engine_->DeleteEntityByTag(ctx_, table_id, is_dropped, tags_index_id, tags, &count, mtr_id, hash_span, osn);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 7: Failure - DeleteEntityByTag on non-existent table
  {
    TSTableID non_existent_table = 999999;
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t osn = 0;
    s = engine_->DeleteEntityByTag(ctx_, non_existent_table, is_dropped, tags_index_id, tags, &count, mtr_id, hash_span, osn);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 8: Failure - DeleteMetricByTag on non-existent table
  {
    TSTableID non_existent_table = 999999;
    std::vector<KwTsSpan> ts_spans;
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t osn = 0;
    s = engine_->DeleteMetricByTag(ctx_, non_existent_table, is_dropped, tags_index_id, tags, ts_spans, &count, mtr_id, hash_span, osn);
    EXPECT_EQ(s, KStatus::FAIL);
  }
}

// Test for DropResidualTsTable function
TEST_F(TsEngineV2Test, DropResidualTsTableTest) {
  using namespace roachpb;
  KStatus s;
  
  // Test 1: Success - DropResidualTsTable with no residual tables
  {
    // Create a table first
    TSTableID table_id = 99005;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    // DropResidualTsTable should succeed (no residual tables in WITH_TESTS mode)
    s = engine_->DropResidualTsTable(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 2: Success - DropResidualTsTable handles dropped tables
  {
    // Create another table
    TSTableID table_id = 99006;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = engine_->DropResidualTsTable(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

// Test for FlushBuffer, GetWalLevel, SetUseRaftLogAsWAL, and GetTsWaitThreadNum functions
TEST_F(TsEngineV2Test, WalOperationsTest) {
  KStatus s;
  
  // Test 1: Success - FlushBuffer flushes all WAL buffers
  {
    s = engine_->FlushBuffer(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 2: Success - GetWalLevel returns current WAL level
  {
    uint8_t wal_level = 0;
    s = engine_->GetWalLevel(ctx_, &wal_level);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_GT(wal_level, 0);  // Should be > 0 (default is 2)
  }
  
  // Test 3: Failure - GetWalLevel with nullptr parameter
  {
    s = engine_->GetWalLevel(ctx_, nullptr);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 4: Success - SetUseRaftLogAsWAL sets the flag
  {
    // Set to true
    s = engine_->SetUseRaftLogAsWAL(ctx_, true);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Verify by getting wal level (when use_raft_log_as_wal is true, FlushBuffer should skip)
    uint8_t wal_level = 0;
    s = engine_->GetWalLevel(ctx_, &wal_level);
    EXPECT_EQ(s, KStatus::SUCCESS);
    // Set back to false
    s = engine_->SetUseRaftLogAsWAL(ctx_, false);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 5: Success - GetTsWaitThreadNum returns thread count
  {
    ThreadInfo info;
    s = engine_->GetTsWaitThreadNum(ctx_, &info);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }
}

// Test for Vacuum function
TEST_F(TsEngineV2Test, VacuumTest) {
  KStatus s;

  // Test 1: Success - Vacuum with force=false
  {
    s = engine_->Vacuum(ctx_, false);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }

  // Test 2: Success - Vacuum with force=true
  {
    s = engine_->Vacuum(ctx_, true);
    EXPECT_EQ(s, KStatus::SUCCESS);

  }
}

// Test for ResetAllWALMgr function
TEST_F(TsEngineV2Test, ResetAllWALMgrTest) {
  KStatus s;
  
  // Test 1: Success - ResetAllWALMgr resets all WAL managers successfully
  {
    s = engine_->ResetAllWALMgr(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

// Test for TSxBegin, TSxCommit, and TSxRollback functions
TEST_F(TsEngineV2Test, TransactionOperationsTest) {
  using namespace roachpb;
  KStatus s;
  
  // Create a table for testing
  TSTableID table_id = 99007;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  bool is_dropped = false;
  
  // Test 1: Success - TSxBegin starts a transaction
  {
    std::string txn_begin = "5041921481932015";
    s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 2: Failure - TSxBegin on non-existent table
  {
    TSTableID non_existent_table = 999999;
    std::string txn_begin = "5041921481932016";
    s = engine_->TSxBegin(ctx_, non_existent_table, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 3: Success - TSxCommit commits a transaction
  {
    std::string txn_begin = "5041921481932017";
    // Begin first
    s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Then commit
    s = engine_->TSxCommit(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 4: Failure - TSxCommit on non-existent table
  {
    TSTableID non_existent_table = 999999;
    std::string txn_begin = "5041921481932018";
    s = engine_->TSxCommit(ctx_, non_existent_table, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::FAIL);
  }
  
  // Test 5: Success - TSxRollback rollbacks a transaction
  {
    std::string txn_begin = "5041921481932019";
    // Begin first
    s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Then rollback
    s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  
  // Test 6: Success - TSxRollback on non-existent table with no active transaction
  // Note: When mtr_id == 0 (no active transaction), TSxRollback returns SUCCESS directly
  {
    TSTableID non_existent_table = 999999;
    std::string txn_begin = "5041921481932020";
    s = engine_->TSxRollback(ctx_, non_existent_table, const_cast<char*>(txn_begin.c_str()), is_dropped);
    EXPECT_EQ(s, KStatus::SUCCESS);  // Returns SUCCESS when no active transaction
  }
}

// Test for WAL recovery logic - AlterColumn
TEST_F(TsEngineV2Test, RecoverWALAlterColumnRollback) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88002;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for ALTER
  is_dropped = false;
  std::string txn_begin = "5041921481932021";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform ALTER operation
  KWDBKTSColumn new_col_meta;
  new_col_meta.set_column_id(2);
  new_col_meta.set_name("column_2");
  new_col_meta.set_storage_type(roachpb::BIGINT);
  new_col_meta.set_nullable(true);
  std::string new_col_data;
  new_col_meta.SerializeToString(&new_col_data);
  TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
  KWDBKTSColumn origin_col_meta;
  origin_col_meta.set_column_id(2);
  origin_col_meta.set_name("column_2");
  origin_col_meta.set_storage_type(roachpb::INT);
  std::string origin_col_data;
  origin_col_meta.SerializeToString(&origin_col_data);
  TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
  std::string err_msg;
  s = engine_->AlterColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped,
    new_col_slice, origin_col_slice, 1, 2, AlterType::ALTER_COLUMN_TYPE, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);
  // Rollback the transaction
  s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);

}

// Test for WAL recovery logic - AlterColumn without TSxRollback
TEST_F(TsEngineV2Test, RecoverWALAlterColumn) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88012;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for ALTER
  is_dropped = false;
  std::string txn_begin = "5041921481932022";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform ALTER operation
  KWDBKTSColumn new_col_meta;
  new_col_meta.set_column_id(2);
  new_col_meta.set_name("column_2");
  new_col_meta.set_storage_type(roachpb::BIGINT);
  new_col_meta.set_nullable(true);
  std::string new_col_data;
  new_col_meta.SerializeToString(&new_col_data);
  TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
  KWDBKTSColumn origin_col_meta;
  origin_col_meta.set_column_id(2);
  origin_col_meta.set_name("column_2");
  origin_col_meta.set_storage_type(roachpb::INT);
  std::string origin_col_data;
  origin_col_meta.SerializeToString(&origin_col_data);
  TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
  std::string err_msg;
  s = engine_->AlterColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped,
    new_col_slice, origin_col_slice, 1, 2, AlterType::ALTER_COLUMN_TYPE, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - AddColumn rollback
TEST_F(TsEngineV2Test, RecoverWALAddColumnRollback) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88005;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for ADD COLUMN
  is_dropped = false;
  std::string txn_begin = "5041921481932023";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform AddColumn operation
  KWDBKTSColumn new_col_meta;
  new_col_meta.set_column_id(4);  // New column ID
  new_col_meta.set_name("column_4");
  new_col_meta.set_storage_type(roachpb::FLOAT);
  new_col_meta.set_nullable(true);
  std::string new_col_data;
  new_col_meta.SerializeToString(&new_col_data);
  TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
  
  std::string err_msg;
  s = engine_->AddColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()),
                         is_dropped, new_col_slice, 1, 4, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);
  
  // Rollback the transaction
  s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - AddColumn without explicit TSxRollback
TEST_F(TsEngineV2Test, RecoverWALAddColumn) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88015;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for ADD COLUMN
  is_dropped = false;
  std::string txn_begin = "5041921481932024";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform AddColumn operation
  KWDBKTSColumn new_col_meta;
  new_col_meta.set_column_id(4);  // New column ID
  new_col_meta.set_name("column_4");
  new_col_meta.set_storage_type(roachpb::FLOAT);
  new_col_meta.set_nullable(true);
  std::string new_col_data;
  new_col_meta.SerializeToString(&new_col_data);
  TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
  
  std::string err_msg;
  s = engine_->AddColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()),
                         is_dropped, new_col_slice, 1, 4, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - DropColumn rollback
TEST_F(TsEngineV2Test, RecoverWALDropColumnRollback) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88006;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for DROP COLUMN
  is_dropped = false;
  std::string txn_begin = "5041921481932025";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform DropColumn operation (drop column with ID 4)
  KWDBKTSColumn drop_col_meta;
  drop_col_meta.set_column_id(4);
  drop_col_meta.set_name("column_4");
  drop_col_meta.set_storage_type(roachpb::FLOAT);
  std::string drop_col_data;
  drop_col_meta.SerializeToString(&drop_col_data);
  TSSlice drop_col_slice{const_cast<char*>(drop_col_data.c_str()), drop_col_data.size()};
  
  std::string err_msg;
  s = engine_->DropColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()),
                          is_dropped, drop_col_slice, 1, 4, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);
  
  // Rollback the transaction
  s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  // Recover should complete successfully (DROP COLUMN was rolled back, nothing to replay)
  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - DropColumn without explicit TSxRollback
TEST_F(TsEngineV2Test, RecoverWALDropColumn) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88016;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // Begin transaction for DROP COLUMN
  is_dropped = false;
  std::string txn_begin = "5041921481932026";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform DropColumn operation (drop column with ID 4)
  KWDBKTSColumn drop_col_meta;
  drop_col_meta.set_column_id(4);
  drop_col_meta.set_name("column_4");
  drop_col_meta.set_storage_type(roachpb::FLOAT);
  std::string drop_col_data;
  drop_col_meta.SerializeToString(&drop_col_data);
  TSSlice drop_col_slice{const_cast<char*>(drop_col_data.c_str()), drop_col_data.size()};
  
  std::string err_msg;
  s = engine_->DropColumn(ctx_, table_id, const_cast<char*>(txn_begin.c_str()),
                          is_dropped, drop_col_slice, 1, 4, err_msg);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  // Recover should handle uncommitted transaction and rollback automatically
  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - CreateNormalTagIndex rollback
TEST_F(TsEngineV2Test, RecoverWALCreateIndexRollback) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88003;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }

  // Begin transaction for creating index
  is_dropped = false;
  std::string txn_begin = "5041921481932027";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform CreateNormalTagIndex operation
  uint64_t index_id = 3;
  uint32_t cur_version = 1;
  uint32_t new_version = 2;
  std::vector<uint32_t> index_columns = {3};  // Create index on first tag column
  s = engine_->CreateNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                    is_dropped, cur_version, new_version, index_columns);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  // Rollback the transaction
  s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);

  uint32_t drop_cur_version = 2;
  uint32_t drop_new_version = 3;
  s = engine_->DropNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                  is_dropped, drop_cur_version, drop_new_version);
  EXPECT_TRUE(s == KStatus::SUCCESS);
}

// Test for WAL recovery logic - CreateNormalTagIndex rollback without explicit TSxRollback
TEST_F(TsEngineV2Test, RecoverWALCreateIndex) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88013;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }

  // Begin transaction for creating index
  is_dropped = false;
  std::string txn_begin = "5041921481932028";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform CreateNormalTagIndex operation
  uint64_t index_id = 3;
  uint32_t cur_version = 1;
  uint32_t new_version = 2;
  std::vector<uint32_t> index_columns = {3};  // Create index on first tag column
  s = engine_->CreateNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                    is_dropped, cur_version, new_version, index_columns);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);

  uint32_t drop_cur_version = 2;
  uint32_t drop_new_version = 3;
  s = engine_->DropNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                  is_dropped, drop_cur_version, drop_new_version);
  EXPECT_TRUE(s == KStatus::SUCCESS);
}

// Test for WAL recovery logic - DropNormalTagIndex rollback
TEST_F(TsEngineV2Test, RecoverWALDropIndexRollback) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88004;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }

  // First create an index to drop later
  is_dropped = false;
  uint64_t index_id = 3;
  uint32_t cur_version = 1;
  uint32_t new_version = 2;
  std::vector<uint32_t> index_columns = {3};
  std::string txn_create_idx = "5041921481932029";
  s = engine_->CreateNormalTagIndex(ctx_, table_id, index_id, txn_create_idx.c_str(), is_dropped, cur_version, new_version, index_columns);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  // Begin transaction for dropping index
  is_dropped = false;
  std::string txn_begin = "5041921481932030";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Perform DropNormalTagIndex operation
  uint32_t drop_cur_version = 2;
  uint32_t drop_new_version = 3;
  s = engine_->DropNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                  is_dropped, drop_cur_version, drop_new_version);
  EXPECT_TRUE(s == KStatus::SUCCESS);

  // Rollback the transaction
  s = engine_->TSxRollback(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

// Test for WAL recovery logic - DropNormalTagIndex rollback without explicit TSxRollback
TEST_F(TsEngineV2Test, RecoverWALDropIndex) {
  using namespace roachpb;
  KStatus s;
  kwdbContext_t ctx;
  
  TSTableID table_id = 88014;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  std::shared_ptr<TsTable> ts_table;
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  // Get schema for data insertion
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  
  // Write some initial data first
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts = 1000;
  for (int i = 0; i < 3; ++i) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, start_ts);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    EXPECT_EQ(s, KStatus::SUCCESS);
    start_ts += 1000;
  }
  
  // First create an index and commit it
  is_dropped = false;
  uint64_t index_id = 3;
  uint32_t cur_version = 1;
  uint32_t new_version = 2;
  std::vector<uint32_t> index_columns = {3};
  std::string txn_create_idx = "5041921481932031";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_create_idx.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = engine_->CreateNormalTagIndex(ctx_, table_id, index_id, txn_create_idx.c_str(), is_dropped, cur_version, new_version, index_columns);
  EXPECT_TRUE(s == KStatus::SUCCESS);
  s = engine_->TSxCommit(ctx_, table_id, const_cast<char*>(txn_create_idx.c_str()), is_dropped);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Begin transaction for dropping index, but don't rollback explicitly
  is_dropped = false;
  std::string txn_begin = "5041921481932032";
  s = engine_->TSxBegin(ctx_, table_id, const_cast<char*>(txn_begin.c_str()), is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Perform DropNormalTagIndex operation
  uint32_t drop_cur_version = 2;
  uint32_t drop_new_version = 3;
  s = engine_->DropNormalTagIndex(ctx_, table_id, index_id, txn_begin.c_str(),
                                  is_dropped, drop_cur_version, drop_new_version);
  EXPECT_TRUE(s == KStatus::SUCCESS);
  
  s = engine_->Recover(&ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);

}

// Test for IsTableDropped function
TEST_F(TsEngineV2Test, IsTableDroppedTest) {
  using namespace roachpb;
  
  // Test 1: Table exists and is not dropped (normal case)
  {
    TSTableID table_id = 88020;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    KStatus s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Table should not be dropped
    bool is_dropped = engine_->IsTableDropped(table_id);
    EXPECT_FALSE(is_dropped);
  }
  
  // Test 2: Table exists but has been dropped
  {
    TSTableID table_id = 88021;
    CreateTsTable pb_meta;
    std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    KStatus s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Drop the table
    s = engine_->DropTsTable(ctx_, table_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    // Table should be dropped
    bool is_dropped = engine_->IsTableDropped(table_id);
    EXPECT_TRUE(is_dropped);
  }
  
  // Test 3: Table does not exist (non-existent table ID)
  {
    TSTableID non_existent_table_id = 999999;
    bool is_dropped = engine_->IsTableDropped(non_existent_table_id);
    EXPECT_TRUE(is_dropped);
  }
}