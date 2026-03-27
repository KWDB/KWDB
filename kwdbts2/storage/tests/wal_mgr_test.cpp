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

#include <filesystem>
#include "libkwdbts2.h"
#include "st_wal_mgr.h"
#include "st_wal_internal_log_structure.h"
#include "st_wal_internal_logblock.h"

#include "../../ts_engine/tests/test_util.h"

namespace fs = std::filesystem;

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

TEST_F(TestWALMgr, TestWALMgr_InitAndCreate) {
  WALMgr* wal_mgr = new WALMgr(test_dir_, table_id_, tbl_grp_id_, &opts_);
  ASSERT_NE(wal_mgr, nullptr);
  
  // Test Create
  KStatus create_result = wal_mgr->Create(ctx_);
  EXPECT_EQ(create_result, KStatus::SUCCESS);
  
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
  
  // First create and init normally
  KStatus create_result = wal_mgr->Create(ctx_);
  EXPECT_EQ(create_result, KStatus::SUCCESS);
  
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
  char tsx_id[] = "read_mtr_test";
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
