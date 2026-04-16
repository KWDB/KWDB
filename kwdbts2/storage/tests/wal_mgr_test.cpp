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

#include <unordered_map>
#include <algorithm>

#include "libkwdbts2.h"
#include "st_wal_mgr.h"
#include "st_wal_internal_log_structure.h"
#include "st_wal_internal_logblock.h"
#include "st_wal_internal_buffer_mgr.h"

#include "../../ts_engine/tests/test_util.h"

class TestWALMgr : public ::testing::Test {
 protected:
  kwdbContext_t context_;
  kwdbContext_p ctx_ = nullptr;
  uint64_t tbl_grp_id_ = 123;
  uint64_t table_id_ = 10001;
  EngineOptions opts_;
  std::string test_dir_;

  void SetUp() override {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    test_dir_ = "./wal_mgr_test/";
    opts_.db_path = test_dir_;
    
    // Clean up test directory
    fs::remove_all(test_dir_);
  }

  void TearDown() override {
    // Clean up test directory
    fs::remove_all(test_dir_);
  }
};

// ============================================================================
// WALMgr Constructor/Destructor Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_ConstructorTableBased) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  EXPECT_EQ(wal_mgr->GetMeta().current_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ConstructorVGroupNameBased) {
  std::string vgrp_name = "test_vgroup";
  WALMgr* wal_mgr = new WALMgr(test_dir_, vgrp_name, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ConstructorUserDefinedPath) {
  std::string vgrp_name = "test_vgroup_ud";
  std::string user_path = "./user_defined_wal";
  
  WALMgr* wal_mgr = new WALMgr(test_dir_, vgrp_name, &opts_, user_path);
  ASSERT_NE(wal_mgr, nullptr);
  
  delete wal_mgr;
  fs::remove_all(user_path);
}

// ============================================================================
// WALMgr Initialization Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_Init) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  // Test Init
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Check that WAL was properly initialized
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  EXPECT_GT(current_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_InitForChk) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Get current meta
  WALMeta meta = wal_mgr->GetMeta();
  
  // Create another WALMgr and init with existing meta
  std::string test_dir2 = "./wal_mgr_test2/";
  fs::remove_all(test_dir2);
  WALMgr* wal_mgr2 = new WALMgr(test_dir2, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr2, nullptr);
  
  KStatus init_for_chk_result = wal_mgr2->InitForChk(ctx_, meta);
  EXPECT_EQ(init_for_chk_result, KStatus::SUCCESS);
  
  delete wal_mgr;
  delete wal_mgr2;
  fs::remove_all(test_dir2);
}

// ============================================================================
// WALMgr Write Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some test data
  char test_data[] = "Test WAL data";
  TS_OSN entry_lsn;
  
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data), entry_lsn);
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  EXPECT_GT(entry_lsn, 0);
  
  // Test write without LSN
  KStatus write_result2 = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result2, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteIncompleteWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Create some test log entries
  std::vector<LogEntry*> logs;
  
  // Create an MTR begin entry
  char tsx_id[] = "test_tsx_001";
  MTRBeginEntry* mtr_begin = new MTRBeginEntry(0, 1, tsx_id, 100, 1);
  logs.push_back(mtr_begin);
  
  // Write incomplete WAL
  KStatus write_result = wal_mgr->WriteIncompleteWAL(ctx_, logs);
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  // Clean up
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Insert Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteInsertWAL_Tags) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test payload
  char payload[] = "Insert test payload";
  TSSlice slice{payload, sizeof(payload)};
  
  KStatus insert_result = wal_mgr->WriteInsertWAL(ctx_, 1001, 1000, 50, slice, 101, 201);
  EXPECT_EQ(insert_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteInsertWAL_Metrics) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  char primary_tag[] = "primary_metric";
  char payload[] = "Metrics payload";
  
  TSSlice tag_slice{primary_tag, sizeof(primary_tag)};
  TSSlice payload_slice{payload, sizeof(payload)};
  
  TS_OSN entry_lsn;
  KStatus insert_result = wal_mgr->WriteInsertWAL(ctx_, 1002, 2000, 100, tag_slice, payload_slice, entry_lsn, 102);
  EXPECT_EQ(insert_result, KStatus::SUCCESS);
  EXPECT_GT(entry_lsn, 0);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Update Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteUpdateWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  char new_payload[] = "Updated data";
  char old_payload[] = "Original data";
  
  TSSlice new_slice{new_payload, sizeof(new_payload)};
  TSSlice old_slice{old_payload, sizeof(old_payload)};
  
  KStatus update_result = wal_mgr->WriteUpdateWAL(ctx_, 1003, 3000, 200, new_slice, old_slice, 103, 203);
  EXPECT_EQ(update_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Delete Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteDeleteMetricsWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  std::string primary_tag = "delete_metric";
  std::vector<KwTsSpan> ts_spans{{1000, 2000}, {3000, 4000}};
  
  vector<DelRowSpan> row_spans;
  DelRowSpan span1 = {5000, 1, "1111"};
  DelRowSpan span2 = {6000, 2, "0000"};
  row_spans.push_back(span1);
  row_spans.push_back(span2);
  
  TS_OSN entry_lsn;
  KStatus delete_result = wal_mgr->WriteDeleteMetricsWAL(ctx_, 1004, primary_tag, 
                                                        ts_spans, row_spans, 104, &entry_lsn);
  EXPECT_EQ(delete_result, KStatus::SUCCESS);
  EXPECT_GT(entry_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteDeleteMetricsWAL4V2) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  std::string primary_tag = "delete_metric_v2";
  std::vector<KwTsSpan> ts_spans{{5000, 6000}, {7000, 8000}};
  
  TS_OSN entry_lsn;
  KStatus delete_result = wal_mgr->WriteDeleteMetricsWAL4V2(ctx_, 1005, table_id_, 
                                                           primary_tag, ts_spans, 105, 0, &entry_lsn);
  EXPECT_EQ(delete_result, KStatus::SUCCESS);
  EXPECT_GT(entry_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteDeleteTagWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  std::string primary_tag = "delete_tag";
  char tag_data[] = "tag content";
  TSSlice tag_slice{tag_data, sizeof(tag_data)};
  
  KStatus delete_result = wal_mgr->WriteDeleteTagWAL(ctx_, 1006, primary_tag, 
                                                    1, 2, tag_slice, 106, 206, 50);
  EXPECT_EQ(delete_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Index Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteCreateIndexWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  std::vector<uint32_t> col_ids{1, 2, 3};
  
  KStatus create_index_result = wal_mgr->WriteCreateIndexWAL(ctx_, 1007, 3001, 101, 
                                                           1, 2, col_ids);
  EXPECT_EQ(create_index_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteDropIndexWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Prepare test data
  std::vector<uint32_t> col_ids{4, 5, 6};
  
  KStatus drop_index_result = wal_mgr->WriteDropIndexWAL(ctx_, 1008, 3002, 102, 
                                                        2, 3, col_ids);
  EXPECT_EQ(drop_index_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Transaction Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_WriteMTRWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char tsx_id[] = "mtr_transaction";
  
  KStatus begin_result = wal_mgr->WriteMTRWAL(ctx_, 1009, tsx_id, WALLogType::MTR_BEGIN);
  EXPECT_EQ(begin_result, KStatus::SUCCESS);
  
  KStatus commit_result = wal_mgr->WriteMTRWAL(ctx_, 1010, tsx_id, WALLogType::MTR_COMMIT);
  EXPECT_EQ(commit_result, KStatus::SUCCESS);
  
  KStatus rollback_result = wal_mgr->WriteMTRWAL(ctx_, 1011, tsx_id, WALLogType::MTR_ROLLBACK);
  EXPECT_EQ(rollback_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteTSxWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char tsx_id[] = "tsx_transaction";
  
  KStatus begin_result = wal_mgr->WriteTSxWAL(ctx_, 1012, tsx_id, WALLogType::TS_BEGIN);
  EXPECT_EQ(begin_result, KStatus::SUCCESS);
  
  KStatus commit_result = wal_mgr->WriteTSxWAL(ctx_, 1013, tsx_id, WALLogType::TS_COMMIT);
  EXPECT_EQ(commit_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Read Operations Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_ReadWALLog) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some data first
  char test_data[] = "Read test data";
  TS_OSN entry_lsn;
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data), entry_lsn);
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  // Read back the data
  std::vector<LogEntry*> logs;
  std::vector<uint64_t> end_chk;
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  KStatus read_result = wal_mgr->ReadWALLog(logs, entry_lsn, current_lsn, end_chk);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  // Clean up
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ReadWALLogForMtr) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some MTR data
  char tsx_id[] = "1165041921481932801";
  KStatus begin_result = wal_mgr->WriteMTRWAL(ctx_, 1014, tsx_id, WALLogType::MTR_BEGIN);
  EXPECT_EQ(begin_result, KStatus::SUCCESS);
  
  // Read MTR logs
  std::vector<LogEntry*> logs;
  std::vector<uint64_t> end_chk;
  
  KStatus read_result = wal_mgr->ReadWALLogForMtr(1014, logs, end_chk);
  // May return SUCCESS even if no logs found
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  // Clean up
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Flush and Checkpoint Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_Flush) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some data
  char test_data[] = "Flush test data";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  // Flush the data
  KStatus flush_result = wal_mgr->Flush(ctx_);
  EXPECT_EQ(flush_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_FlushWithoutLock) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some data
  char test_data[] = "Flush without lock test";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  // Flush without lock
  KStatus flush_result = wal_mgr->FlushWithoutLock(ctx_);
  EXPECT_EQ(flush_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_CreateCheckpoint) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Create checkpoint
  KStatus checkpoint_result = wal_mgr->CreateCheckpoint(ctx_);
  EXPECT_EQ(checkpoint_result, KStatus::SUCCESS);
  
  // Check checkpoint LSN
  TS_OSN checkpoint_lsn = wal_mgr->FetchCheckpointLSN();
  EXPECT_GT(checkpoint_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_CreateCheckpointWithoutFlush) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Create checkpoint without flush
  KStatus checkpoint_result = wal_mgr->CreateCheckpointWithoutFlush(ctx_);
  EXPECT_EQ(checkpoint_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Metadata Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_MetadataAccessors) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Test metadata accessors
  WALMeta meta = wal_mgr->GetMeta();
  EXPECT_GT(meta.current_lsn, 0);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  EXPECT_EQ(current_lsn, meta.current_lsn);
  
  TS_OSN flushed_lsn = wal_mgr->FetchFlushedLSN();
  EXPECT_GE(flushed_lsn, 0);
  
  TS_OSN checkpoint_lsn = wal_mgr->FetchCheckpointLSN();
  EXPECT_GE(checkpoint_lsn, 0);
  
  // Test first LSN
  TS_OSN first_lsn = wal_mgr->GetFirstLSN();
  EXPECT_GT(first_lsn, 0);
  
  delete wal_mgr;
}

// ============================================================================
// WALMgr Utility Tests
// ============================================================================

TEST_F(TestWALMgr, TestWALMgr_CloseAndDrop) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Close WAL manager
  KStatus close_result = wal_mgr->Close();
  EXPECT_EQ(close_result, KStatus::SUCCESS);
  
  // Drop WAL files
  KStatus drop_result = wal_mgr->Drop();
  EXPECT_EQ(drop_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ResetWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  // Write some data
  char test_data[] = "Before reset";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  // Reset WAL
  KStatus reset_result = wal_mgr->ResetWAL(ctx_);
  EXPECT_EQ(reset_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_UpdateCheckpointWithoutFlush) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN initial_checkpoint_lsn = wal_mgr->FetchCheckpointLSN();
  
  // Update checkpoint without flush
  TS_OSN new_checkpoint_lsn = initial_checkpoint_lsn + 1000;
  KStatus update_result = wal_mgr->UpdateCheckpointWithoutFlush(ctx_, new_checkpoint_lsn);
  EXPECT_EQ(update_result, KStatus::SUCCESS);
  
  TS_OSN updated_checkpoint_lsn = wal_mgr->FetchCheckpointLSN();
  EXPECT_EQ(updated_checkpoint_lsn, new_checkpoint_lsn);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_UpdateFirstLSN) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN initial_first_lsn = wal_mgr->GetFirstLSN();
  
  // Update first LSN
  TS_OSN new_first_lsn = initial_first_lsn + 1000;
  KStatus update_result = wal_mgr->UpdateFirstLSN(new_first_lsn);
  EXPECT_EQ(update_result, KStatus::SUCCESS);
  
  // Note: This might not be directly observable through public APIs
  // since GetFirstLSN() reads from file header, not a member variable
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteCheckpointWAL_Basic) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN entry_lsn;
  KStatus checkpoint_result = wal_mgr->WriteCheckpointWAL(ctx_, 2001, entry_lsn);
  EXPECT_EQ(checkpoint_result, KStatus::SUCCESS);
  EXPECT_GT(entry_lsn, 0);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteCheckpointWAL_WithPartitions) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  CheckpointPartition partitions[3];
  partitions[0].time_partition = 1;
  partitions[0].offset = 0;
  partitions[1].time_partition = 2;
  partitions[1].offset = 100;
  partitions[2].time_partition = 3;
  partitions[2].offset = 300;
  
  TS_OSN entry_lsn;
  KStatus checkpoint_result = wal_mgr->WriteCheckpointWAL(ctx_, 2002, 500, 3, partitions, entry_lsn);
  EXPECT_EQ(checkpoint_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteSnapshotWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  KwTsSpan span;
  span.begin = 1000;
  span.end = 2000;
  
  KStatus snapshot_result = wal_mgr->WriteSnapshotWAL(ctx_, 3001, 10001, 0, 100, span);
  EXPECT_EQ(snapshot_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteTempDirectoryWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  std::string temp_path = "/tmp/wal_temp_dir";
  KStatus temp_dir_result = wal_mgr->WriteTempDirectoryWAL(ctx_, 3002, temp_path);
  EXPECT_EQ(temp_dir_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteDDLDropWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  KStatus ddl_result = wal_mgr->WriteDDLDropWAL(ctx_, 4001, 50001);
  EXPECT_EQ(ddl_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_WriteDDLAlterWAL) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char column_meta[] = "test_column_metadata";
  TSSlice meta_slice{column_meta, sizeof(column_meta)};
  
  KStatus alter_result = wal_mgr->WriteDDLAlterWAL(ctx_, 4002, 50002, AlterType::ADD_COLUMN, 1, 2, meta_slice);
  EXPECT_EQ(alter_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ResetCurLSNAndFlushMeta) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  TS_OSN new_lsn = current_lsn + 5000;
  
  KStatus reset_result = wal_mgr->ResetCurLSNAndFlushMeta(ctx_, new_lsn);
  EXPECT_EQ(reset_result, KStatus::SUCCESS);
  
  TS_OSN updated_lsn = wal_mgr->FetchCurrentLSN();
  EXPECT_EQ(updated_lsn, new_lsn);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_CleanUp) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "Cleanup test data";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  wal_mgr->CreateCheckpoint(ctx_);
  
  wal_mgr->CleanUp(ctx_);
  
  SUCCEED();
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_RemoveChkFile) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  KStatus remove_result = wal_mgr->RemoveChkFile(ctx_);
  EXPECT_EQ(remove_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_SwitchNextFile) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "Before switch";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  KStatus switch_result = wal_mgr->SwitchNextFile(0);
  EXPECT_EQ(switch_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_SwitchNextFile_WithFirstLsn) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "Before switch with lsn";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  KStatus switch_result = wal_mgr->SwitchNextFile(current_lsn + 1000);
  EXPECT_EQ(switch_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_SwitchLastFile) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "Before last switch";
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data));
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  KStatus switch_result = wal_mgr->SwitchLastFile(ctx_, current_lsn);
  EXPECT_EQ(switch_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_NeedCheckpoint) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  bool need_checkpoint = wal_mgr->NeedCheckpoint();
  EXPECT_FALSE(need_checkpoint);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_SetCurLSN) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN new_lsn = 99999;
  wal_mgr->SetCurLSN(new_lsn);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  EXPECT_EQ(current_lsn, new_lsn);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_GetWALFilePath) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  std::string wal_path = wal_mgr->GetWALFilePath();
  EXPECT_FALSE(wal_path.empty());
  EXPECT_NE(wal_path.find("wal"), std::string::npos);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_GetWALChkFilePath) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  std::string chk_path = wal_mgr->GetWALChkFilePath();
  EXPECT_FALSE(chk_path.empty());
  EXPECT_NE(chk_path.find("chk"), std::string::npos);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ReadWALLogAndSwitchFile) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "Test for switch file read";
  TS_OSN entry_lsn;
  KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, sizeof(test_data), entry_lsn);
  EXPECT_EQ(write_result, KStatus::SUCCESS);
  
  std::vector<LogEntry*> logs;
  std::vector<uint64_t> end_chk;
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  
  KStatus read_result = wal_mgr->ReadWALLogAndSwitchFile(logs, entry_lsn, current_lsn, end_chk);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ReadUncommittedTxnID) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char tsx_id[] = "uncommitted_tsx";
  wal_mgr->WriteMTRWAL(ctx_, 5001, tsx_id, WALLogType::MTR_BEGIN);
  
  std::vector<uint64_t> uncommitted_xid;
  KStatus read_result = wal_mgr->ReadUncommittedTxnID(uncommitted_xid);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ReadWALLogForTSx) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char ts_trans_id[] = "ts_trans_001";
  std::vector<LogEntry*> logs;
  
  KStatus read_result = wal_mgr->ReadWALLogForTSx(ts_trans_id, logs);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_ReadUncommittedWALLog) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char tsx_id[] = "uncommitted_wal_tsx";
  wal_mgr->WriteMTRWAL(ctx_, 6001, tsx_id, WALLogType::MTR_BEGIN);
  
  TS_OSN first_lsn = wal_mgr->GetFirstLSN();
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  
  std::vector<LogEntry*> logs;
  std::vector<uint64_t> end_chk;
  std::vector<uint64_t> uncommitted_xid = {6001};
  
  KStatus read_result = wal_mgr->ReadUncommittedWALLog(logs, first_lsn, current_lsn, end_chk, uncommitted_xid);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_MultipleWriteAndFlush) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  for (int i = 0; i < 50; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "Test data %d", i);
    KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, strlen(test_data));
    EXPECT_EQ(write_result, KStatus::SUCCESS);
    
    if (i % 10 == 0) {
      KStatus flush_result = wal_mgr->Flush(ctx_);
      EXPECT_EQ(flush_result, KStatus::SUCCESS);
    }
  }
  
  KStatus final_flush = wal_mgr->Flush(ctx_);
  EXPECT_EQ(final_flush, KStatus::SUCCESS);
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_Create) {
  std::string create_test_dir = "./wal_mgr_create_test/";
  fs::remove_all(create_test_dir);
  
  WALMgr* wal_mgr = new WALMgr(create_test_dir, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus create_result = wal_mgr->Create(ctx_);
  EXPECT_EQ(create_result, KStatus::SUCCESS);
  
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  EXPECT_GT(current_lsn, 0);
  
  delete wal_mgr;
  fs::remove_all(create_test_dir);
}

TEST_F(TestWALMgr, TestWALMgr_Init_ExistingPath) {
  WALMgr* wal_mgr1 = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr1, nullptr);
  
  KStatus init_result = wal_mgr1->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  char test_data[] = "First init";
  wal_mgr1->WriteWAL(ctx_, test_data, sizeof(test_data));
  wal_mgr1->Flush(ctx_);
  
  delete wal_mgr1;
  
  WALMgr* wal_mgr2 = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr2, nullptr);
  
  KStatus init_result2 = wal_mgr2->Init(ctx_);
  EXPECT_EQ(init_result2, KStatus::SUCCESS);
  
  delete wal_mgr2;
}

TEST_F(TestWALMgr, TestWALMgr_WriteReadSequence) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  TS_OSN first_lsn = 0;
  for (int i = 0; i < 5; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "Sequence test %d", i);
    TS_OSN entry_lsn;
    KStatus write_result = wal_mgr->WriteWAL(ctx_, test_data, strlen(test_data), entry_lsn);
    EXPECT_EQ(write_result, KStatus::SUCCESS);
    
    if (i == 0) {
      first_lsn = entry_lsn;
    }
  }
  
  KStatus flush_result = wal_mgr->Flush(ctx_);
  EXPECT_EQ(flush_result, KStatus::SUCCESS);
  
  std::vector<LogEntry*> logs;
  std::vector<uint64_t> end_chk;
  TS_OSN current_lsn = wal_mgr->FetchCurrentLSN();
  
  KStatus read_result = wal_mgr->ReadWALLog(logs, first_lsn, current_lsn, end_chk);
  EXPECT_EQ(read_result, KStatus::SUCCESS);
  
  for (auto log : logs) {
    delete log;
  }
  
  delete wal_mgr;
}

TEST_F(TestWALMgr, TestWALMgr_CheckpointSequence) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  KStatus init_result = wal_mgr->Init(ctx_);
  EXPECT_EQ(init_result, KStatus::SUCCESS);
  
  for (int i = 0; i < 3; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "Before checkpoint %d", i);
    wal_mgr->WriteWAL(ctx_, test_data, strlen(test_data));
    
    KStatus checkpoint_result = wal_mgr->CreateCheckpoint(ctx_);
    EXPECT_EQ(checkpoint_result, KStatus::SUCCESS);
    
    TS_OSN checkpoint_lsn = wal_mgr->FetchCheckpointLSN();
    EXPECT_GT(checkpoint_lsn, 0);
  }
  
  delete wal_mgr;
}


// ============================================================================
// WALBufferMgr Tests
// ============================================================================

class TestWALBufferMgr : public ::testing::Test {
 protected:
  kwdbContext_t context_;
  kwdbContext_p ctx_ = nullptr;
  uint64_t tbl_grp_id_ = 123;
  uint64_t table_id_ = 10001;
  EngineOptions opts_;
  std::string test_dir_;
  WALFileMgr* file_mgr_{nullptr};
  WALBufferMgr* buffer_mgr_{nullptr};

  void SetUp() override {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    test_dir_ = "./wal_buffer_mgr_test/";
    opts_.db_path = test_dir_;
    
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    
    file_mgr_ = new WALFileMgr(test_dir_, table_id_, &opts_);
    ASSERT_NE(file_mgr_, nullptr);

    TS_OSN first_lsn = BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE;
    auto s = file_mgr_->initWalFile(first_lsn);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    buffer_mgr_ = new WALBufferMgr(&opts_, file_mgr_);
    ASSERT_NE(buffer_mgr_, nullptr);
  }

  void TearDown() override {
    if (buffer_mgr_) {
      delete buffer_mgr_;
      buffer_mgr_ = nullptr;
    }
    if (file_mgr_) {
      file_mgr_->Close();
      delete file_mgr_;
      file_mgr_ = nullptr;
    }
    fs::remove_all(test_dir_);
  }
};

TEST_F(TestWALBufferMgr, Constructor_Basic) {
  ASSERT_NE(buffer_mgr_, nullptr);
}

TEST_F(TestWALBufferMgr, Init_Basic) {
  KStatus s = buffer_mgr_->init(0);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ResetMeta_Basic) {
  KStatus s = buffer_mgr_->init(0);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  buffer_mgr_->ResetMeta();
  
  SUCCEED();
}

TEST_F(TestWALBufferMgr, WriteWAL_SmallData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Test WAL data";
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_GT(lsn_offset, 0);
}

TEST_F(TestWALBufferMgr, WriteWAL_MultipleWrites) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 10; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "Test WAL data %d", i);
    TS_OSN lsn_offset = 0;
    
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_GT(lsn_offset, 0);
  }
}

TEST_F(TestWALBufferMgr, WriteWAL_LargeData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> large_data(4096 * 2, 'A');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, large_data.data(), large_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_GT(lsn_offset, 0);
}

TEST_F(TestWALBufferMgr, WriteWAL_VeryLargeData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> very_large_data(8192 * 3, 'B');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, very_large_data.data(), very_large_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_GT(lsn_offset, 0);
}

TEST_F(TestWALBufferMgr, Flush_Basic) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush test data";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, Flush_MultipleTimes) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 5; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "Flush test %d", i);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    s = buffer_mgr_->flush();
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
}

TEST_F(TestWALBufferMgr, FlushWithoutLock_Basic) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush without lock test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushWithoutLock(false);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, FlushWithoutLock_WithHeader) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush with header test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushWithoutLock(true);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, SetHeaderBlockCheckpointInfo) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockCheckpointInfo(1000, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getCheckpointNo(), 1);
}

TEST_F(TestWALBufferMgr, SetHeaderBlockFirstLSN) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockFirstLSN(5000);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getFirstLSN(), 5000);
}

TEST_F(TestWALBufferMgr, GetCurrentLsn_AfterInit) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  TS_OSN current_lsn = buffer_mgr_->getCurrentLsn();
  EXPECT_GT(current_lsn, 0);
}

TEST_F(TestWALBufferMgr, GetCurrentLsn_AfterWrite) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Get current LSN test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  TS_OSN current_lsn = buffer_mgr_->getCurrentLsn();
  EXPECT_GT(current_lsn, 0);
}

TEST_F(TestWALBufferMgr, GetHeaderBlock_Basic) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_GE(header.getFirstLSN(), 0);
}

TEST_F(TestWALBufferMgr, WriteAndFlush_MultipleBlocks) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 100; ++i) {
    char test_data[256];
    snprintf(test_data, sizeof(test_data), "Block test data iteration %d with some padding", i);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReadWALLogs_InvalidRange) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<LogEntry*> log_entries;
  std::vector<uint64_t> end_chk;
  TS_OSN start_lsn = 1000;
  TS_OSN end_lsn = 100;
  
  s = buffer_mgr_->readWALLogs(log_entries, start_lsn, end_lsn, end_chk);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReadUncommittedTxnID_Empty) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<uint64_t> uncommitted_id;
  TS_OSN end_lsn = buffer_mgr_->getCurrentLsn();
  
  s = buffer_mgr_->readUncommittedTxnID(uncommitted_id, end_lsn, end_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReadUncommittedTxnID_InvalidRange) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<uint64_t> uncommitted_id;
  TS_OSN start_lsn = 1000;
  TS_OSN end_lsn = 100;
  
  s = buffer_mgr_->readUncommittedTxnID(uncommitted_id, start_lsn, end_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_ZeroLength) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "";
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, test_data, 0, lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, Flush_EmptyBuffer) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteFlushWriteSequence) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data1[] = "First write";
  TS_OSN lsn1 = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data1, sizeof(test_data1), lsn1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flush();
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data2[] = "Second write";
  TS_OSN lsn2 = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data2, sizeof(test_data2), lsn2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  EXPECT_GT(lsn2, lsn1);
}

TEST_F(TestWALBufferMgr, CheckpointInfoUpdate) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockCheckpointInfo(100, 1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockCheckpointInfo(200, 2);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockCheckpointInfo(300, 3);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getCheckpointNo(), 3);
}

TEST_F(TestWALBufferMgr, FirstLSNUpdate) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockFirstLSN(1000);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->setHeaderBlockFirstLSN(2000);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getFirstLSN(), 2000);
}

TEST_F(TestWALBufferMgr, StressTest_ManyWrites) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 1000; ++i) {
    char test_data[128];
    snprintf(test_data, sizeof(test_data), "Stress test data %d", i);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    if (i % 100 == 0) {
      s = buffer_mgr_->flush();
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
  }
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_ExactlyBlockSize) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> block_sized_data(4000, 'X');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, block_sized_data.data(), block_sized_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_SplitAcrossBlocks) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> first_write(3000, 'A');
  TS_OSN lsn1 = 0;
  s = buffer_mgr_->writeWAL(ctx_, first_write.data(), first_write.size(), lsn1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> second_write(3000, 'B');
  TS_OSN lsn2 = 0;
  s = buffer_mgr_->writeWAL(ctx_, second_write.data(), second_write.size(), lsn2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  EXPECT_GT(lsn2, lsn1);
}

TEST_F(TestWALBufferMgr, FlushInternal_WithHeader) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush internal test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushInternal(true);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, FlushInternal_WithoutHeader) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush internal without header test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushInternal(false);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReinitAfterFlush) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Before reinit";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flush();
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  buffer_mgr_->ResetMeta();
  
  s = buffer_mgr_->init(0);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReadAllTxnID_Empty) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::unordered_map<uint64_t, txnOp> txn_op;
  TS_OSN end_lsn = buffer_mgr_->getCurrentLsn();
  std::unordered_map<TS_OSN, std::pair<uint64_t, uint64_t>> incomplete_idx;
  
  s = buffer_mgr_->readAllTxnID(txn_op, end_lsn, end_lsn, nullptr, incomplete_idx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ReadAllTxnID_InvalidRange) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::unordered_map<uint64_t, txnOp> txn_op;
  TS_OSN start_lsn = 1000;
  TS_OSN end_lsn = 100;
  std::unordered_map<TS_OSN, std::pair<uint64_t, uint64_t>> incomplete_idx;
  
  s = buffer_mgr_->readAllTxnID(txn_op, start_lsn, end_lsn, nullptr, incomplete_idx);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_MaxBlockSize) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> max_block_data(4072, 'M');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, max_block_data.data(), max_block_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, MultipleFlushOperations) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 50; ++i) {
      char test_data[128];
      snprintf(test_data, sizeof(test_data), "Round %d data %d", round, i);
      TS_OSN lsn_offset = 0;
      s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    s = buffer_mgr_->flush();
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
}

TEST_F(TestWALBufferMgr, WriteWAL_BinaryData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> binary_data(256);
  for (int i = 0; i < 256; ++i) {
    binary_data[i] = static_cast<char>(i);
  }
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, binary_data.data(), binary_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_NullPointer) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, nullptr, 0, lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, FlushAfterMultipleWrites) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 20; ++i) {
    char test_data[256];
    snprintf(test_data, sizeof(test_data), "Test data for flush %d with padding", i);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, SetCheckpointInfoMultipleTimes) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 10; ++i) {
    s = buffer_mgr_->setHeaderBlockCheckpointInfo(i * 100, i);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getCheckpointNo(), 9);
}

TEST_F(TestWALBufferMgr, SetFirstLSNMultipleTimes) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  for (int i = 0; i < 10; ++i) {
    s = buffer_mgr_->setHeaderBlockFirstLSN(i * 1000);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  HeaderBlock header = buffer_mgr_->getHeaderBlock();
  EXPECT_EQ(header.getFirstLSN(), 9000);
}

TEST_F(TestWALBufferMgr, WriteWAL_VerySmallData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char small_data[] = "X";
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, small_data, 1, lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_ExactlyMaxLogSize) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> exact_data(4072, 'E');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, exact_data.data(), exact_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_SlightlyOverMaxLogSize) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> over_data(4080, 'O');
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, over_data.data(), over_data.size(), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, FlushInternalAfterWrite) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush internal after write test";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushInternal(true);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushInternal(false);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, GetCurrentLsnAfterMultipleOperations) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  TS_OSN prev_lsn = 0;
  for (int i = 0; i < 10; ++i) {
    char test_data[64];
    snprintf(test_data, sizeof(test_data), "LSN test %d", i);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, test_data, strlen(test_data), lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    TS_OSN current_lsn = buffer_mgr_->getCurrentLsn();
    EXPECT_GT(current_lsn, prev_lsn);
    prev_lsn = current_lsn;
  }
}

TEST_F(TestWALBufferMgr, WriteWAL_WithSpecialCharacters) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char special_data[] = "Special\n\t\r\nData\x00WithNull";
  TS_OSN lsn_offset = 0;
  
  s = buffer_mgr_->writeWAL(ctx_, special_data, sizeof(special_data), lsn_offset);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, FlushWithoutLockAfterWrite) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Flush without lock after write";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushWithoutLock(true);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  s = buffer_mgr_->flushWithoutLock(false);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, WriteWAL_LargeAmountOfData) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  std::vector<char> large_data(1024 * 1024, 'L');
  size_t chunk_size = 4096;
  
  for (size_t offset = 0; offset < large_data.size(); offset += chunk_size) {
    size_t write_size = std::min(chunk_size, large_data.size() - offset);
    TS_OSN lsn_offset = 0;
    s = buffer_mgr_->writeWAL(ctx_, large_data.data() + offset, write_size, lsn_offset);
    ASSERT_EQ(s, KStatus::SUCCESS);
    
    if (offset % (chunk_size * 10) == 0) {
      s = buffer_mgr_->flush();
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
  }
  
  s = buffer_mgr_->flush();
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestWALBufferMgr, ResetMetaAndReinit) {
  KStatus s = buffer_mgr_->init(0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  char test_data[] = "Before reset";
  TS_OSN lsn_offset = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data, sizeof(test_data), lsn_offset);
  ASSERT_EQ(s, KStatus::SUCCESS);
  
  buffer_mgr_->ResetMeta();
  
  s = buffer_mgr_->init(0);
  EXPECT_EQ(s, KStatus::SUCCESS);
  
  char test_data2[] = "After reinit";
  TS_OSN lsn_offset2 = 0;
  s = buffer_mgr_->writeWAL(ctx_, test_data2, sizeof(test_data2), lsn_offset2);
  EXPECT_EQ(s, KStatus::SUCCESS);
}
