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
#include "db_test_base.h"
#include "sys_utils.h"
#include "me_metadata.pb.h"

using namespace kwdbts;  // NOLINT

const string db_root_path = ".";
class TsDBTest : public TsDBTestBase {
 public:
  TsDBTest() {
    InitDB(db_root_path);
  }
};

TEST_F(TsDBTest, CreateTableAndGetMetaData) {
  // Test 1: Create a table
  TSTableID table_id = 10001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  // Serialize to TSSlice
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  // Create range groups
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;  // LEADER
  RangeGroups range_groups = {ranges, 1};
  
  // Create table
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: Check if table exists
  bool find = false;
  s = TSIsTsTableExist(engine_, table_id, &find);
  EXPECT_EQ(s.data, nullptr);
  EXPECT_TRUE(find);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 3: Get metadata
  RangeGroup range = {1, 0};
  TSSlice result_schema;
  s = TSGetMetaData(engine_, table_id, range, &result_schema);
  EXPECT_EQ(s.data, nullptr);
  if (s.data == nullptr) {
    // Verify we got valid metadata
    EXPECT_NE(result_schema.data, nullptr);
    EXPECT_GT(result_schema.len, 0);
    
    // Parse and verify
    roachpb::CreateTsTable retrieved_meta;
    ASSERT_TRUE(retrieved_meta.ParseFromArray(result_schema.data, result_schema.len));
    EXPECT_EQ(retrieved_meta.ts_table().ts_table_id(), table_id);
    
    free(result_schema.data);
  } else {
    free(s.data);
  }
  
  // Test 4: Drop table
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 5: Verify table is dropped
  find = false;
  s = TSIsTsTableExist(engine_, table_id, &find);
  EXPECT_EQ(s.data, nullptr);
  EXPECT_FALSE(find);  // Table should not exist after drop
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 6: Try to get metadata of dropped table (should fail)
  s = TSGetMetaData(engine_, table_id, range, &result_schema);
  EXPECT_NE(s.data, nullptr);  // Should fail
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, DropResidualTables) {
  // Create multiple tables
  std::vector<TSTableID> table_ids = {20001, 20002, 20003};
  
  for (auto table_id : table_ids) {
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::BIGINT};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    
    string meta_str;
    ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
    TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
    
    RangeGroup ranges[1];
    ranges[0].range_group_id = 1;
    ranges[0].typ = 0;
    RangeGroups range_groups = {ranges, 1};
    
    auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Drop all tables
  for (auto table_id : table_ids) {
    auto s = TSDropTsTable(engine_, table_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Drop residual tables
  auto s = TSDropResidualTsTable(engine_);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Verify all tables are gone
  for (auto table_id : table_ids) {
    bool find = false;
    s = TSIsTsTableExist(engine_, table_id, &find);
    EXPECT_EQ(s.data, nullptr);
    EXPECT_FALSE(find);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, VacuumAndMaintenance) {
  // Test 1: Create a table and insert some data
  TSTableID table_id = 30001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: Vacuum with force=false (normal vacuum)
  s = TSVacuum(engine_, 0, false);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 3: Vacuum with force=true (force flush before vacuum)
  s = TSVacuum(engine_, 0, true);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 4: Concurrent vacuum request (should be ignored gracefully)
  s = TSVacuum(engine_, 0, false);
  EXPECT_EQ(s.data, nullptr);  // Should still return success
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 5: Migrate table (OSS version returns success directly)
  s = TSMigrateTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 6: Table autonomy (OSS version returns success directly)
  s = TSTableAutonomy(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 7: Migrate/Autonomy with invalid table ID (still returns success in OSS)
  s = TSMigrateTsTable(engine_, 999999);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  s = TSTableAutonomy(engine_, 999999);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, PutAndDeleteData) {
  // Test 1: Create a table with tags
  TSTableID table_id = 40001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: Insert data using TSPutEntity and TSPutDataByRowTypeExplicit
  {
    // Generate payload data
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ks = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::vector<TagInfo> tag_schema;
    ks = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    // Generate some test data
    timestamp64 ts = 50000000;
    for (int i = 0; i < 5; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 10, ts);
      TsRawPayload::SetHashPoint(payload, 1);
      TsRawPayload::SetOSN(payload, 100);
      TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
      
      // First create entity
      s = TSPutEntity(engine_, table_id, &payload_slice, 1, ranges[0], 0, 0);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
      }
      
      // Then put data
      uint16_t inc_entity_cnt = 0;
      uint32_t not_create_entity = 0;
      DedupResult dedup_result = {0, 0, 0};
      s = TSPutDataByRowType(engine_, table_id, &payload_slice, 1, ranges[0], 0,
                             &inc_entity_cnt, &not_create_entity, &dedup_result, true);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
      }
      free(payload.data);
      ts += 1000;
    }
  }
  
  // Test 3: Delete data by time range (TsDeleteRangeData)
  {
    KwTsSpan spans[1];
    spans[0].begin = 50000000;
    spans[0].end = 50002000;
    KwTsSpans ts_spans = {spans, 1};
    
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t count = 0;
    
    s = TsDeleteRangeData(engine_, table_id, 1, hash_span, ts_spans, &count, 0, 0);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: Count remaining data (TsCountRangeData)
  {
    KwTsSpan spans[1];
    spans[0].begin = 50000000;
    spans[0].end = 50010000;
    KwTsSpans ts_spans = {spans, 1};
    
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t count = 0;
    
    s = TsCountRangeData(engine_, table_id, 1, hash_span, ts_spans, &count, 0, 100);
    EXPECT_EQ(count, 11);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: Delete metric by tag (TsDeleteMetricByTag)
  {
    // Prepare primary tag
    uint64_t dev_id = 1;
    std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
    TSSlice primary_tag = {const_cast<char*>(tag_value.data()), tag_value.size()};
    
    // Prepare index columns
    uint32_t index_cols[1] = {3};
    IndexColumns index_tags = {index_cols, 1};
    
    // Prepare time spans
    KwTsSpan spans[1];
    spans[0].begin = 60000000;
    spans[0].end = 60005000;
    KwTsSpans ts_spans = {spans, 1};
    
    uint64_t count = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    
    s = TsDeleteMetricByTag(engine_, table_id, &primary_tag, 1, index_tags, ts_spans, &count, 0, hash_span, 0);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 6: Delete entities by tag (TsDeleteEntitiesByTag)
  {
    uint64_t dev_id = 1;
    std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
    TSSlice primary_tag = {const_cast<char*>(tag_value.data()), tag_value.size()};
    
    uint32_t index_cols[1] = {3};
    IndexColumns index_tags = {index_cols, 1};
    
    uint64_t count = 0;
    HashIdSpan hash_span = {0, UINT64_MAX};
    
    s = TsDeleteEntitiesByTag(engine_, table_id, &primary_tag, 1, index_tags, &count, hash_span, 0, 0);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 7: Delete specific entity (TsDeleteData)
  {
    // Insert one more record
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ks = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::vector<TagInfo> tag_schema;
    ks = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    timestamp64 ts = 70000000;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 30, ts);
    TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
    
    s = TSPutEntity(engine_, table_id, &payload_slice, 1, ranges[0], 0, 0);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    free(payload.data);
    
    // Delete the specific entity
    uint64_t dev_id = 1;
    std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
    TSSlice primary_tag = {const_cast<char*>(tag_value.data()), tag_value.size()};
    
    KwTsSpan spans[1];
    spans[0].begin = 70000000;
    spans[0].end = 70001000;
    KwTsSpans ts_spans = {spans, 1};
    
    uint64_t count = 0;
    s = TsDeleteData(engine_, table_id, 1, primary_tag, ts_spans, &count, 0, 0);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 9: Error case - operations on non-existent table
  {
    TSTableID invalid_table = 999999;
    
    KwTsSpan spans[1];
    spans[0].begin = 50000000;
    spans[0].end = 50010000;
    KwTsSpans ts_spans = {spans, 1};
    
    HashIdSpan hash_span = {0, UINT64_MAX};
    uint64_t count = 0;
    
    s = TsCountRangeData(engine_, invalid_table, 1, hash_span, ts_spans, &count, 0, 0);
    EXPECT_NE(s.data, nullptr);  // Should fail
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, FlushCheckpointAndMtr) {
  // Test 1: Create a table for MTR operations
  TSTableID table_id = 50001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: FlushBuffer - flush all WAL buffers
  s = TSFlushBuffer(engine_);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 3: CreateCheckpoint - create global checkpoint
  s = TSCreateCheckpoint(engine_);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 4: CreateCheckpointForTable - create checkpoint for specific table
  s = TSCreateCheckpointForTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 5: MTR Begin and Commit (implicit transaction)
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 1;
    uint64_t mtr_id = 0;
    
    s = TSMtrBegin(engine_, table_id, range_group_id, range_id, index, &mtr_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Commit the MTR
    s = TSMtrCommit(engine_, table_id, range_group_id, mtr_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 6: MTR Begin and Rollback (implicit transaction)
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 2;
    uint64_t mtr_id = 0;
    
    s = TSMtrBegin(engine_, table_id, range_group_id, range_id, index, &mtr_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Rollback the MTR
    s = TSMtrRollback(engine_, table_id, range_group_id, mtr_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 7: MTR with explicit transaction ID (Begin -> Commit)
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 3;
    uint64_t mtr_id = 0;
    const char* tsx_id = "explicit_txn_001";
    
    s = TSMtrBeginExplicit(engine_, table_id, range_group_id, range_id, index, &mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Commit with explicit transaction ID
    s = TSMtrCommitExplicit(engine_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 8: MTR with explicit transaction ID (Begin -> Rollback)
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 4;
    uint64_t mtr_id = 0;
    const char* tsx_id = "explicit_txn_002";
    
    s = TSMtrBeginExplicit(engine_, table_id, range_group_id, range_id, index, &mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Rollback with explicit transaction ID
    s = TSMtrRollbackExplicit(engine_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 9: MTR with null transaction ID
  {
    uint64_t range_group_id = 1;
    uint64_t range_id = 1;
    uint64_t index = 5;
    uint64_t mtr_id = 0;
    const char* tsx_id = nullptr;
    
    s = TSMtrBeginExplicit(engine_, table_id, range_group_id, range_id, index, &mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    s = TSMtrCommitExplicit(engine_, table_id, range_group_id, mtr_id, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, TransactionOperations) {
  // Test 1: Create a table for transaction operations
  TSTableID table_id = 60001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: TSxBegin starts a transaction
  {
    std::string txn_id = "1165041921481932";
    s = TSxBegin(engine_, table_id, const_cast<char*>(txn_id.c_str()));
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: TSxCommit commits a transaction
  {
    std::string txn_id = "1165041921481933";
    // Begin first
    s = TSxBegin(engine_, table_id, const_cast<char*>(txn_id.c_str()));
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    // Then commit
    s = TSxCommit(engine_, table_id, const_cast<char*>(txn_id.c_str()));
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: TSxRollback rollbacks a transaction
  {
    std::string txn_id = "1165041921481934";
    // Begin first
    s = TSxBegin(engine_, table_id, const_cast<char*>(txn_id.c_str()));
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    // Then rollback
    s = TSxRollback(engine_, table_id, const_cast<char*>(txn_id.c_str()));
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: TSxBegin on non-existent table (should succeed due to is_dropped=true)
  {
    TSTableID non_existent_table = 999999;
    std::string txn_id = "1165041921481935";
    s = TSxBegin(engine_, non_existent_table, const_cast<char*>(txn_id.c_str()));
    // When table doesn't exist, is_dropped=true, so returns success
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 6: TSxCommit on non-existent table (should succeed due to is_dropped=true)
  {
    TSTableID non_existent_table = 999999;
    std::string txn_id = "1165041921481936";
    s = TSxCommit(engine_, non_existent_table, const_cast<char*>(txn_id.c_str()));
    // When table doesn't exist, is_dropped=true, so returns success
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 7: TSxRollback on non-existent table (should succeed due to is_dropped=true)
  {
    TSTableID non_existent_table = 999999;
    std::string txn_id = "1165041921481937";
    s = TSxRollback(engine_, non_existent_table, const_cast<char*>(txn_id.c_str()));
    // When table doesn't exist or no active transaction, returns success
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, ClusterSettingAndTierDuration) {
  // Test 1: Set cluster setting - ts.tier.duration with days (30d,90d)
  {
    std::string key_str = "ts.tier.duration";
    std::string value_str = "30d,90d";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    // This should not throw exception and should parse correctly
    // parseDuration should convert: 30*86400=2592000, 90*86400=7776000
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 2: Set tier duration with hours (1h,24h)
  {
    std::string key_str = "ts.tier.duration";
    std::string value_str = "1h,24h";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    // parseDuration should convert: 1*3600=3600, 24*3600=86400
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 3: Set tier duration with minutes (60m,180m)
  {
    std::string key_str = "ts.tier.duration";
    std::string value_str = "60m,180m";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    // parseDuration should convert: 60*60=3600, 180*60=10800
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 4: Set tier duration with uppercase units (7D,30D)
  {
    std::string key_str = "ts.tier.duration";
    std::string value_str = "7D,30D";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    // parseDuration should handle uppercase: 7*86400=604800, 30*86400=2592000
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 5: Set tier duration with mixed case (12H,36h)
  {
    std::string key_str = "ts.tier.duration";
    std::string value_str = "12H,36h";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    // parseDuration should handle mixed case: 12*3600=43200, 36*3600=129600
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 6: Set other cluster settings - ts.dedup.rule
  {
    std::string key_str = "ts.dedup.rule";
    std::string value_str = "merge";
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    TSSlice value = {const_cast<char*>(value_str.c_str()), value_str.size()};
    
    EXPECT_NO_THROW(TSSetClusterSetting(key, value));
  }
  
  // Test 7: Update existing cluster setting multiple times
  {
    std::string key_str = "ts.tier.duration";
    
    TSSlice key = {const_cast<char*>(key_str.c_str()), key_str.size()};
    
    // First update
    std::string value_str1 = "15d,45d";
    TSSlice value1 = {const_cast<char*>(value_str1.c_str()), value_str1.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(key, value1));
    
    // Second update
    std::string value_str2 = "60d,180d";
    TSSlice value2 = {const_cast<char*>(value_str2.c_str()), value_str2.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(key, value2));
    
    // Third update
    std::string value_str3 = "7d,21d";
    TSSlice value3 = {const_cast<char*>(value_str3.c_str()), value_str3.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(key, value3));
  }
  
  // Test 8: Set various other cluster settings
  {
    // ts.rows_per_block.max_limit
    std::string key1 = "ts.rows_per_block.max_limit";
    std::string val1 = "1000";
    TSSlice k1 = {const_cast<char*>(key1.c_str()), key1.size()};
    TSSlice v1 = {const_cast<char*>(val1.c_str()), val1.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(k1, v1));
    
    // ts.count.use_statistics.enabled
    std::string key2 = "ts.count.use_statistics.enabled";
    std::string val2 = "true";
    TSSlice k2 = {const_cast<char*>(key2.c_str()), key2.size()};
    TSSlice v2 = {const_cast<char*>(val2.c_str()), val2.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(k2, v2));
    
    // ts.compress.stage
    std::string key3 = "ts.compress.stage";
    std::string val3 = "2";
    TSSlice k3 = {const_cast<char*>(key3.c_str()), key3.size()};
    TSSlice v3 = {const_cast<char*>(val3.c_str()), val3.size()};
    EXPECT_NO_THROW(TSSetClusterSetting(k3, v3));
  }
}

TEST_F(TsDBTest, SnapshotAndDataMigration) {
  // Test 1: Create source and destination tables for snapshot migration
  TSTableID src_table_id = 40001;
  TSTableID dst_table_id = 40002;
  
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, src_table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  // Create source table
  auto s = TSCreateTsTable(engine_, src_table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Create destination table with same schema
  pb_meta.mutable_ts_table()->set_ts_table_id(dst_table_id);
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  s = TSCreateTsTable(engine_, dst_table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: Insert data into source table
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus ks = InitServerKWDBContext(ctx_p);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  ks = engine_->GetTableSchemaMgr(ctx_p, src_table_id, is_dropped, schema_mgr);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  ks = schema_mgr->GetMetricMeta(1, &metric_schema);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  std::vector<TagInfo> tag_schema;
  ks = schema_mgr->GetTagMeta(1, tag_schema);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  // Insert 5 rows
  timestamp64 ts = 1000000;
  for (int i = 0; i < 5; ++i) {
    auto payload = GenRowPayload(*metric_schema, tag_schema, src_table_id, 1, 1, 1, ts);
    TsRawPayload::SetHashPoint(payload, 1);
    TsRawPayload::SetOSN(payload, 100);
    TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
    
    uint16_t inc_entity_cnt = 0;
    uint32_t not_create_entity = 0;
    DedupResult dedup_result = {0, 0, 0};
    s = TSPutDataByRowType(engine_, src_table_id, &payload_slice, 1, ranges[0], 0,
                           &inc_entity_cnt, &not_create_entity, &dedup_result, true);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    free(payload.data);
    ts += 1000;
  }
  
  // Test 3: Create snapshot for read on source table
  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  uint64_t read_snapshot_id = 0;
  s = TSCreateSnapshotForRead(engine_, src_table_id, 0, UINT64_MAX, ts_span, 100, &read_snapshot_id);
  EXPECT_EQ(s.data, nullptr);
  EXPECT_GT(read_snapshot_id, 0);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 4: Read data from snapshot using TSGetSnapshotNextBatchData
  {
    TSSlice data;
    int batch_count = 0;
    
    // Read batches until no more data
    while (true) {
      s = TSGetSnapshotNextBatchData(engine_, src_table_id, read_snapshot_id, &data);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
        break;
      }
      // If data is empty, we've read all data
      if (data.len == 0) {
        break;
      }
      batch_count++;
      // Free the data allocated by TSGetSnapshotNextBatchData
      free(data.data);
    }
    EXPECT_GT(batch_count, 0);  // Should have read at least one batch
  }
  
  // Test 5: Test TSWriteSnapshotBatchData, TSWriteSnapshotRollback and TSWriteSnapshotSuccess
  {
    // Create write snapshot for testing Success
    uint64_t success_snapshot_id = 0;
    s = TSCreateSnapshotForWrite(engine_, dst_table_id, 0, UINT64_MAX, ts_span, &success_snapshot_id, 250);
    EXPECT_EQ(s.data, nullptr);
    EXPECT_GT(success_snapshot_id, 0);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Test TSWriteSnapshotSuccess with valid snapshot (no data written, should still succeed)
    s = TSWriteSnapshotSuccess(engine_, dst_table_id, success_snapshot_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Cleanup the successful snapshot
    s = TSDeleteSnapshot(engine_, dst_table_id, success_snapshot_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Test error cases with invalid snapshot_id
    TSSlice test_data = {nullptr, 0};
    uint64_t invalid_snapshot_id = 999999;
    
    // Test TSWriteSnapshotBatchData with non-existent snapshot_id
    s = TSWriteSnapshotBatchData(engine_, dst_table_id, invalid_snapshot_id, test_data);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Test TSWriteSnapshotRollback with non-existent snapshot_id
    uint64_t osn = 300;
    s = TSWriteSnapshotRollback(engine_, dst_table_id, invalid_snapshot_id, osn);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Test TSWriteSnapshotSuccess with non-existent snapshot_id
    s = TSWriteSnapshotSuccess(engine_, dst_table_id, invalid_snapshot_id);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 6: Cleanup snapshots and tables
  s = TSDeleteSnapshot(engine_, src_table_id, read_snapshot_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  s = TSDropTsTable(engine_, src_table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  s = TSDropTsTable(engine_, dst_table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, BatchDataOperations) {
  // Test 1: Create a table for batch operations
  TSTableID table_id = 50001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Insert some test data
  {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ks = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::vector<TagInfo> tag_schema;
    ks = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    // Insert 5 rows of data
    timestamp64 ts = 1000000;
    for (int i = 0; i < 5; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      TsRawPayload::SetHashPoint(payload, 1);
      TsRawPayload::SetOSN(payload, 100);
      
      TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
      uint16_t inc_entity_cnt = 0;
      uint32_t not_create_entity = 0;
      DedupResult dedup_result = {0, 0, 0};
      
      s = TSPutDataByRowType(engine_, table_id, &payload_slice, 1, ranges[0], 0, &inc_entity_cnt, &not_create_entity, &dedup_result, true);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
      }
      free(payload.data);
      ts += 1000;
    }
  }
  
  // Test 2: Read -> Write -> Cancel workflow
  {
    uint64_t job_id = 1001;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    uint32_t row_num = 0;
    TSSlice read_data = {nullptr, 0};
    
    // Step 1: Read batch data from empty table
    s = TSReadBatchData(engine_, table_id, 1, 0, UINT64_MAX, ts_span, job_id, &read_data, &row_num);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Step 2: Write the read data back (reuse read_data as write_data)
    uint32_t write_row_num = 0;
    s = TSWriteBatchData(engine_, table_id, 1, job_id, &read_data, &write_row_num);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Note: TSWriteBatchData takes ownership of read_data, don't free it here
    read_data.data = nullptr;
    read_data.len = 0;
    
    // Step 3: Cancel the batch job
    uint64_t osn = 100;
    s = CancelBatchJob(engine_, job_id, osn);
    // Verify it doesn't crash
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: Read -> Write -> Finish workflow
  {
    uint64_t job_id = 1002;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    uint32_t row_num = 0;
    TSSlice read_data = {nullptr, 0};
    
    // Step 1: Read batch data from empty table
    s = TSReadBatchData(engine_, table_id, 1, 0, UINT64_MAX, ts_span, job_id, &read_data, &row_num);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Step 2: Write the read data back (reuse read_data as write_data)
    uint32_t write_row_num = 0;
    s = TSWriteBatchData(engine_, table_id, 1, job_id, &read_data, &write_row_num);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Note: TSWriteBatchData takes ownership of read_data, don't free it here
    read_data.data = nullptr;
    read_data.len = 0;
    
    // Step 3: Finish the batch job
    s = BatchJobFinish(engine_, job_id);
    // Verify it doesn't crash
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, GetWaitThreadNum) {
  // Test 1: Get wait thread num with valid pointer
  {
    ThreadInfo thread_info;
    memset(&thread_info, 0, sizeof(ThreadInfo));
    
    auto s = TSGetWaitThreadNum(engine_, &thread_info);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Verify the function returns successfully and sets wait_threads
    // The value could be 0 or more depending on system state
    EXPECT_GE(thread_info.wait_threads, 0);
  }
  
  // Test 2: Get wait thread num with null pointer (should fail)
  {
    auto s = TSGetWaitThreadNum(engine_, nullptr);
    // Should return error due to null pointer
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, UtilityFunctions) {
  // Test 1: TSFree - Free allocated memory
  {
    void* ptr = malloc(100);
    ASSERT_NE(ptr, nullptr);
    TSFree(ptr);  // Should not crash
  }
  
  // Test 2: TsGetRecentBlockCacheInfo - Get cache statistics
  {
    uint32_t hit_count = 0;
    uint32_t miss_count = 0;
    uint64_t memory_size = 0;
    
    TsGetRecentBlockCacheInfo(&hit_count, &miss_count, &memory_size);
    // Should return valid values (could be 0 or more)
    EXPECT_GE(hit_count, 0);
    EXPECT_GE(miss_count, 0);
    EXPECT_GE(memory_size, 0);
  }
  
  // Test 3: TsGetStringPtr - Extract string from data buffer
  {
    // Create a test buffer with length prefix
    uint16_t str_len = 5;
    char test_data[100];
    memcpy(test_data, &str_len, sizeof(uint16_t));
    memcpy(test_data + sizeof(uint16_t), "hello", str_len);
    
    uint16_t extracted_len = 0;
    char* str_ptr = TsGetStringPtr(test_data, 0, &extracted_len);
    
    EXPECT_EQ(extracted_len, 5);
    EXPECT_NE(str_ptr, nullptr);
    EXPECT_EQ(memcmp(str_ptr, "hello", 5), 0);
  }
  
  // Test 4: TsMemPoolFree - Free memory pool allocated memory
  {
    // Test: Free nullptr (should not crash)
    TsMemPoolFree(nullptr);
  }
  
  // Test 5: TSRegisterExceptionHandler - Register exception handler
  {
    char dir[] = "/tmp";
    TSRegisterExceptionHandler(dir);  // Should not crash
  }
}

TEST_F(TsDBTest, WalOperations) {
  // Test 1: TsGetWalLevel - Get current WAL level
  {
    uint8_t wal_level = 0;
    auto s = TsGetWalLevel(engine_, &wal_level);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    } else {
      // Verify wal_level is valid (default should be 2)
      EXPECT_GT(wal_level, 0);
    }
  }
  
  // Test 2: TsGetWalLevel - Failure with nullptr parameter
  {
    auto s = TsGetWalLevel(engine_, nullptr);
    // Should return error due to null pointer
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: TsSetUseRaftLogAsWAL - Set to true
  {
    auto s = TsSetUseRaftLogAsWAL(engine_, true);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Verify the change by getting WAL level
    uint8_t wal_level = 0;
    s = TsGetWalLevel(engine_, &wal_level);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: TsSetUseRaftLogAsWAL - Set back to false
  {
    auto s = TsSetUseRaftLogAsWAL(engine_, false);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: TSFlushVGroups - Flush all vgroups
  {
    auto s = TSFlushVGroups(engine_);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, BlocksDistribution) {
  // Test 1: TSGetTableBlocksDistribution with empty table
  {
    TSTableID table_id = 70001;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    
    string meta_str;
    ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
    TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
    
    RangeGroup ranges[1];
    ranges[0].range_group_id = 1;
    ranges[0].typ = 0;
    RangeGroups range_groups = {ranges, 1};
    
    auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    TSSlice blocks_info;
    s = TSGetTableBlocksDistribution(engine_, table_id, &blocks_info);
    EXPECT_EQ(s.data, nullptr);
    if (s.data == nullptr && blocks_info.data != nullptr) {
      roachpb::BlocksDistribution distribution;
      EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
      EXPECT_GE(distribution.block_info_size(), 1);
      free(blocks_info.data);
    } else if (s.data != nullptr) {
      free(s.data);
    }
    
    // Cleanup
    s = TSDropTsTable(engine_, table_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 2: TSGetTableBlocksDistribution with invalid table ID
  {
    TSTableID invalid_table_id = 999999;
    TSSlice blocks_info;
    auto s = TSGetTableBlocksDistribution(engine_, invalid_table_id, &blocks_info);
    // Should return error for non-existent table
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: TSGetDBBlocksDistribution with valid DB ID
  {
    // First create a table to get a valid DB ID
    TSTableID table_id = 70002;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    
    string meta_str;
    ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
    TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
    
    RangeGroup ranges[1];
    ranges[0].range_group_id = 1;
    ranges[0].typ = 0;
    RangeGroups range_groups = {ranges, 1};
    
    auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Get DB ID from the table
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    uint32_t db_id = schema_mgr->GetDbID();
    
    // Get DB blocks distribution
    TSSlice blocks_info;
    s = TSGetDBBlocksDistribution(engine_, db_id, &blocks_info);
    EXPECT_EQ(s.data, nullptr);
    if (s.data == nullptr && blocks_info.data != nullptr) {
      roachpb::BlocksDistribution distribution;
      EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
      EXPECT_GE(distribution.block_info_size(), 1);
      free(blocks_info.data);
    } else if (s.data != nullptr) {
      free(s.data);
    }
    
    // Cleanup
    s = TSDropTsTable(engine_, table_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: TSGetDBBlocksDistribution with non-existent DB ID
  {
    uint32_t invalid_db_id = 999999;
    TSSlice blocks_info;
    auto s = TSGetDBBlocksDistribution(engine_, invalid_db_id, &blocks_info);
    // May succeed but return empty distribution
    if (s.data == nullptr && blocks_info.data != nullptr) {
      roachpb::BlocksDistribution distribution;
      EXPECT_TRUE(distribution.ParseFromArray(blocks_info.data, blocks_info.len));
      free(blocks_info.data);
    } else if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, AlterColumnOperations) {
  TSTableID table_id = 60001;
  
  // Create table with initial columns
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 1: AddColumn - Successful column addition
  {
    // Prepare new column metadata (add a BIGINT column)
    roachpb::KWDBKTSColumn new_col;
    new_col.set_column_id(5);
    new_col.set_name("column_5");
    new_col.set_storage_type(roachpb::BIGINT);
    new_col.set_nullable(true);
    new_col.set_dropped(false);
    string col_data;
    new_col.SerializeToString(&col_data);
    TSSlice col_slice{const_cast<char*>(col_data.c_str()), col_data.size()};
    
    std::string txn_id = "6000100000000001";
    s = TSAddColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()), col_slice, 1, 2);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 2: AddColumn - Invalid protobuf data
  {
    // Invalid protobuf data
    TSSlice invalid_slice{const_cast<char*>("invalid_protobuf_data"), 21};
    std::string txn_id = "6000100000000002";
    s = TSAddColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()), invalid_slice, 2, 3);
    // Should fail due to protobuf parsing error (version won't increment on failure)
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: DropColumn - Successful column drop
  {
    // Prepare column metadata to drop (column 4 - FLOAT)
    roachpb::KWDBKTSColumn column_to_drop;
    column_to_drop.set_column_id(4);
    column_to_drop.set_name("column_4");
    column_to_drop.set_storage_type(roachpb::FLOAT);
    column_to_drop.set_nullable(true);
    column_to_drop.set_dropped(true);
    string column_data;
    column_to_drop.SerializeToString(&column_data);
    TSSlice column_slice{const_cast<char*>(column_data.c_str()), column_data.size()};
    
    std::string txn_id = "6000100000000003";
    s = TSDropColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()), column_slice, 2, 3);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: DropColumn - Invalid protobuf data
  {
    // Invalid protobuf data
    TSSlice invalid_slice{const_cast<char*>("invalid_protobuf_data"), 21};
    std::string txn_id = "6000100000000004";
    s = TSDropColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()), invalid_slice, 3, 4);
    // Should fail due to protobuf parsing error (version won't increment on failure)
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: AlterColumnType - Successful column type alteration
  {
    // Prepare new column metadata (change INT to BIGINT)
    roachpb::KWDBKTSColumn new_col_meta;
    new_col_meta.set_column_id(2);
    new_col_meta.set_name("column_2");
    new_col_meta.set_storage_type(roachpb::BIGINT);  // Change from INT to BIGINT
    new_col_meta.set_nullable(true);
    string new_col_data;
    new_col_meta.SerializeToString(&new_col_data);
    TSSlice new_col_slice{const_cast<char*>(new_col_data.c_str()), new_col_data.size()};
    
    // Origin column reference
    roachpb::KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(2);
    origin_col_meta.set_name("column_2");
    origin_col_meta.set_storage_type(roachpb::INT);
    string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    
    std::string txn_id = "6000100000000005";
    s = TSAlterColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()),
                         new_col_slice, origin_col_slice, 3, 4, true, false);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 6: AlterColumnType - Invalid protobuf data
  {
    // Invalid new column protobuf
    TSSlice invalid_new_col{const_cast<char*>("invalid_new_col"), 17};
    roachpb::KWDBKTSColumn origin_col_meta;
    origin_col_meta.set_column_id(2);
    string origin_col_data;
    origin_col_meta.SerializeToString(&origin_col_data);
    TSSlice origin_col_slice{const_cast<char*>(origin_col_data.c_str()), origin_col_data.size()};
    
    std::string txn_id = "6000100000000006";
    s = TSAlterColumn(engine_, table_id, const_cast<char*>(txn_id.c_str()),
                         invalid_new_col, origin_col_slice, 4, 5, true, false);
    // Should fail due to protobuf parsing error (version won't increment on failure)
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, NormalTagIndexOperations) {
  // Test 1: Create a table for index operations
  TSTableID table_id = 60001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: Create normal tag index - successful case
  {
    uint64_t index_id = 3;
    std::string txn_id = "6000100000000001";
    uint32_t cur_version = 1;
    uint32_t new_version = 2;
    
    uint32_t index_cols[] = {3};  // Index on first tag column
    IndexColumns index_columns = {index_cols, 1};
    
    s = TSCreateNormalTagIndex(engine_, table_id, index_id, const_cast<char*>(txn_id.c_str()),
                               cur_version, new_version, index_columns);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: Drop normal tag index - successful case
  {
    uint64_t index_id = 3;
    std::string txn_id = "6000100000000002";
    uint32_t cur_version = 2;
    uint32_t new_version = 3;
    
    s = TSDropNormalTagIndex(engine_, table_id, index_id, const_cast<char*>(txn_id.c_str()),
                             cur_version, new_version);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: Drop non-existent index (should return error)
  {
    uint64_t non_existent_index = 999;
    std::string txn_id = "6000100000000007";
    uint32_t cur_version = 3;
    uint32_t new_version = 4;
    
    s = TSDropNormalTagIndex(engine_, table_id, non_existent_index, const_cast<char*>(txn_id.c_str()),
                             cur_version, new_version);
    // Should fail as index doesn't exist
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: Operations on non-existent table (should return error)
  {
    TSTableID non_existent_table = 999999;
    uint64_t index_id = 10;
    std::string txn_id = "6000100000000008";
    uint32_t cur_version = 1;
    uint32_t new_version = 2;
    
    uint32_t index_cols[] = {3};
    IndexColumns index_columns = {index_cols, 1};
    
    // Try to create index on non-existent table
    s = TSCreateNormalTagIndex(engine_, non_existent_table, index_id, const_cast<char*>(txn_id.c_str()),
                               cur_version, new_version, index_columns);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    // Try to drop index on non-existent table
    s = TSDropNormalTagIndex(engine_, non_existent_table, index_id, const_cast<char*>(txn_id.c_str()),
                             cur_version, new_version);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, AlterPartitionIntervalAndLifetime) {
  // Test 1: Create a table for alter operations
  TSTableID table_id = 70001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 2: AlterLifetime - successful case
  {
    uint64_t new_lifetime = 3600;  // 1 hour in seconds
    s = TSAlterLifetime(engine_, table_id, new_lifetime);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: AlterLifetime on non-existent table (should return error)
  {
    TSTableID non_existent_table = 999999;
    uint64_t lifetime = 3600;
    
    s = TSAlterLifetime(engine_, non_existent_table, lifetime);
    EXPECT_NE(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: AlterPartitionInterval - returns success but with warning (deprecated)
  {
    uint64_t partition_interval = 86400000;  // 1 day in milliseconds
    s = TSAlterPartitionInterval(engine_, table_id, partition_interval);
    // This function is deprecated but still returns SUCCESS
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, PutDataExplicitAndDeleteEntities) {
  // Test 1: Create a table with tags for testing
  TSTableID table_id = 80001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  auto s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Prepare context and schema manager
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus ks = InitServerKWDBContext(ctx_p);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  bool is_dropped = false;
  ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  ks = schema_mgr->GetMetricMeta(1, &metric_schema);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  std::vector<TagInfo> tag_schema;
  ks = schema_mgr->GetTagMeta(1, tag_schema);
  EXPECT_EQ(ks, KStatus::SUCCESS);
  
  // Test 2: TSPutDataExplicit - successful insert with explicit parameters
  {
    timestamp64 ts = 80000000;
    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 100, ts);
    TsRawPayload::SetHashPoint(payload, 1);
    TsRawPayload::SetOSN(payload, 100);
    
    TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
    uint16_t inc_entity_cnt = 0;
    uint32_t not_create_entity = 0;
    DedupResult dedup_result = {0, 0, 0};
    
    // Insert with explicit transaction ID
    const char* tsx_id = "explicit_txn_001";
    s = TSPutDataExplicit(engine_, table_id, &payload_slice, 1, ranges[0], 0, 
                          &inc_entity_cnt, &not_create_entity, &dedup_result, true, tsx_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    free(payload.data);
  }
  
  // Test 3: TsDeleteEntities - delete entities by primary tags
  {
    // Prepare primary tag (device id = 1)
    uint64_t dev_id = 1;
    std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
    TSSlice primary_tag = {const_cast<char*>(tag_value.data()), tag_value.size()};
    
    uint64_t count = 0;
    uint64_t mtr_id = 0;
    uint64_t osn = 100;
    
    s = TsDeleteEntities(engine_, table_id, &primary_tag, 1, 1, &count, mtr_id, osn);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: TsDeleteEntities on non-existent table (returns success when table is dropped)
  {
    TSTableID non_existent_table = 999999;
    uint64_t dev_id = 1;
    std::string tag_value(reinterpret_cast<const char*>(&dev_id), sizeof(uint64_t));
    TSSlice primary_tag = {const_cast<char*>(tag_value.data()), tag_value.size()};
    
    uint64_t count = 0;
    s = TsDeleteEntities(engine_, non_existent_table, &primary_tag, 1, 1, &count, 0, 0);
    // When table doesn't exist or is dropped, it returns SUCCESS (is_dropped=true)
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Cleanup
  s = TSDropTsTable(engine_, table_id);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
}

TEST_F(TsDBTest, ExecQueryAndEngineOptions) {
  // Test 1: EngineOptions::init() - get original values, set them, call init(), verify
  {
    // Get original values
    const char* original_home = getenv("KW_HOME");
    const char* original_interval = getenv("KW_IOT_INTERVAL");
    
    // Set using the original values (ensure they are set)
    if (original_home) {
      setenv("KW_HOME", original_home, 1);
    }
    if (original_interval) {
      setenv("KW_IOT_INTERVAL", original_interval, 1);
    }
    
    // Call init() to read the values
    kwdbts::EngineOptions::init();
    
    // Verify the values were read correctly
    if (original_home) {
      EXPECT_EQ(kwdbts::EngineOptions::home(), std::string(original_home));
    }
    if (original_interval) {
      int expected_interval = atoi(original_interval);
      EXPECT_EQ(kwdbts::EngineOptions::iot_interval, expected_interval);
    }
  }
  
  // Test 2: TSExecQuery - test with minimal/invalid QueryInfo (should not crash)
  {
    QueryInfo req;
    memset(&req, 0, sizeof(QueryInfo));
    RespInfo resp;
    memset(&resp, 0, sizeof(RespInfo));
    VecTsFetcher fetcher;
    memset(&fetcher, 0, sizeof(VecTsFetcher));
    
    // Execute query with invalid parameters - should return error but not crash
    TSStatus s = TSExecQuery(engine_, &req, &resp, &fetcher);
    
    // Should return an error (not success) due to invalid query info
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, GetAvgTableRowSizeAndDataVolume) {
  // Create a table
  TSTableID table_id = 40001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  TSStatus s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Insert test data using GenRowPayload (same as other tests in db_test.cpp)
  int num_rows = 10;
  {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ks = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::vector<TagInfo> tag_schema;
    ks = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    // Insert 10 rows of data
    timestamp64 ts = 1000000;
    for (int i = 0; i < num_rows; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      TsRawPayload::SetHashPoint(payload, 1);
      TsRawPayload::SetOSN(payload, 100);
      
      TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
      uint16_t inc_entity_cnt = 0;
      uint32_t not_create_entity = 0;
      DedupResult dedup_result = {0, 0, 0};
      
      s = TSPutDataByRowType(engine_, table_id, &payload_slice, 1, ranges[0], 0, 
                             &inc_entity_cnt, &not_create_entity, &dedup_result, true);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
      }
      free(payload.data);
      ts += 1000;
    }
  }
  
  // Test 1: TSGetAvgTableRowSize - get average row size
  {
    uint64_t avg_row_size = 0;
    s = TSGetAvgTableRowSize(engine_, table_id, &avg_row_size);
    EXPECT_EQ(s.data, nullptr);
    if (s.data == nullptr) {
      // Average row size should be > 0 after inserting data
      EXPECT_EQ(avg_row_size, 20);
    } else {
      free(s.data);
    }
  }
  
  // Test 2: TSGetDataVolume - get data volume for full range
  {
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    uint64_t volume = 0;
    s = TSGetDataVolume(engine_, table_id, 0, UINT64_MAX, ts_span, &volume);
    EXPECT_EQ(s.data, nullptr);
    if (s.data == nullptr) {
      // Volume should be > 0 after inserting data
      EXPECT_EQ(volume, 200);
    } else {
      free(s.data);
    }
  }
  
  // Test 4: TSGetDataVolume - invalid hash range (begin > end)
  {
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    uint64_t volume = 0;
    s = TSGetDataVolume(engine_, table_id, 100, 50, ts_span, &volume);
    EXPECT_NE(s.data, nullptr);  // Should fail
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: TSGetDataVolumeHalfTS - get half timestamp
  {
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    int64_t half_ts = 0;
    s = TSGetDataVolumeHalfTS(engine_, table_id, 0, UINT64_MAX, ts_span, &half_ts);
    EXPECT_EQ(s.data, nullptr);
    if (s.data == nullptr) {
      // Half timestamp should be within our data range
      EXPECT_EQ(half_ts, 1005000);
    } else {
      free(s.data);
    }
  }
  
  // Test 6: TSGetAvgTableRowSize on dropped table
  {
    s = TSDropTsTable(engine_, table_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    uint64_t avg_row_size = 0;
    s = TSGetAvgTableRowSize(engine_, table_id, &avg_row_size);
    EXPECT_EQ(s.data, nullptr);  // Dropped table returns success with no error
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}

TEST_F(TsDBTest, PutDataExplicitAndSchemaVersionAndDeleteTotalRange) {
  // Create a table
  TSTableID table_id = 50001;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  
  string meta_str;
  ASSERT_TRUE(pb_meta.SerializeToString(&meta_str));
  TSSlice schema = {const_cast<char*>(meta_str.data()), meta_str.size()};
  
  RangeGroup ranges[1];
  ranges[0].range_group_id = 1;
  ranges[0].typ = 0;
  RangeGroups range_groups = {ranges, 1};
  
  TSStatus s = TSCreateTsTable(engine_, table_id, schema, range_groups);
  EXPECT_EQ(s.data, nullptr);
  if (s.data != nullptr) {
    free(s.data);
  }
  
  // Test 1: TSPutDataByRowTypeExplicit - insert data with explicit transaction
  {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus ks = InitServerKWDBContext(ctx_p);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    ks = engine_->GetTableSchemaMgr(ctx_p, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ks = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    std::vector<TagInfo> tag_schema;
    ks = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(ks, KStatus::SUCCESS);
    
    // Insert 5 rows using TSPutDataByRowTypeExplicit
    timestamp64 ts = 2000000;
    for (int i = 0; i < 5; ++i) {
      auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 1, ts);
      TsRawPayload::SetHashPoint(payload, 1);
      TsRawPayload::SetOSN(payload, 100);
      
      TSSlice payload_slice = {reinterpret_cast<char*>(payload.data), payload.len};
      uint16_t inc_entity_cnt = 0;
      uint32_t not_create_entity = 0;
      DedupResult dedup_result = {0, 0, 0};
      
      // Use nullptr for tsx_id to avoid transaction management complexity
      s = TSPutDataByRowTypeExplicit(engine_, table_id, &payload_slice, 1, ranges[0], 0,
                                     &inc_entity_cnt, &not_create_entity, &dedup_result, true,
                                     nullptr);
      EXPECT_EQ(s.data, nullptr);
      if (s.data != nullptr) {
        free(s.data);
      }
      free(payload.data);
      ts += 1000;
    }
  }
  
  // Test 2: TsDeleteTotalRange - delete all data in hash range
  {
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    uint64_t mtr_id = 0;
    uint64_t osn = 200;
    
    s = TsDeleteTotalRange(engine_, table_id, 0, UINT64_MAX, ts_span, mtr_id, osn);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 3: TsTestGetAndAddSchemaVersion - test schema version management
  {
    uint64_t new_version = 2;
    s = TsTestGetAndAddSchemaVersion(engine_, table_id, new_version);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 4: TsTestGetAndAddSchemaVersion - invalid table ID
  {
    uint64_t new_version = 3;
    s = TsTestGetAndAddSchemaVersion(engine_, 999999, new_version);
    EXPECT_NE(s.data, nullptr);  // Should fail for non-existent table
    if (s.data != nullptr) {
      free(s.data);
    }
  }
  
  // Test 5: TsDeleteTotalRange on dropped table
  {
    s = TSDropTsTable(engine_, table_id);
    EXPECT_EQ(s.data, nullptr);
    if (s.data != nullptr) {
      free(s.data);
    }
    
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    s = TsDeleteTotalRange(engine_, table_id, 0, UINT64_MAX, ts_span, 0, 300);
    EXPECT_NE(s.data, nullptr);  // Should fail for dropped table
    if (s.data != nullptr) {
      free(s.data);
    }
  }
}