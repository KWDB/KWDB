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

#include "st_transaction_mgr.h"

#include <gtest/gtest.h>
#include <cstring>
#include <memory>
#include <string>
#include "libkwdbts2.h"
#include "../../ts_engine/tests/test_util.h"

namespace kwdbts {

class TestTSxMgr : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  void SetUp() override {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    opts_.db_path = "./st_transaction_mgr_test/";
    
    // Clean up test directory
    fs::remove_all(opts_.db_path);
    
    // Create and initialize WALMgr
    wal_mgr_ = new WALMgr("./st_transaction_mgr_test/", intToString(tbl_grp_id_), &opts_);
    auto s = wal_mgr_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    
    // Create TSxMgr with real WALMgr
    tsx_mgr_ = new TSxMgr(wal_mgr_);
    
    memset(ts_trans_id_, 0, sizeof(ts_trans_id_));
    strncpy(ts_trans_id_, "test_trans_001", LogEntry::TS_TRANS_ID_LEN - 1);
  }

  void TearDown() override {
    if (tsx_mgr_ != nullptr) {
      delete tsx_mgr_;
      tsx_mgr_ = nullptr;
    }
    if (wal_mgr_ != nullptr) {
      delete wal_mgr_;
      wal_mgr_ = nullptr;
    }
    // Clean up test directory
    fs::remove_all(opts_.db_path);
  }

  kwdbContext_t context_;
  kwdbContext_p ctx_ = nullptr;
  WALMgr* wal_mgr_ = nullptr;
  TSxMgr* tsx_mgr_ = nullptr;
  uint64_t tbl_grp_id_ = 123;
  EngineOptions opts_;
  char ts_trans_id_[LogEntry::TS_TRANS_ID_LEN];
};

// Test TSxBegin - Basic functionality
TEST_F(TestTSxMgr, TestTSxBegin_Basic) {
  KStatus status = tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test TSxBegin - Multiple transactions
TEST_F(TestTSxMgr, TestTSxBegin_MultipleTransactions) {
  char ts_trans_id_2[LogEntry::TS_TRANS_ID_LEN];
  memset(ts_trans_id_2, 0, sizeof(ts_trans_id_2));
  strncpy(ts_trans_id_2, "test_trans_002", LogEntry::TS_TRANS_ID_LEN - 1);
  
  KStatus status1 = tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  KStatus status2 = tsx_mgr_->TSxBegin(ctx_, ts_trans_id_2);
  
  EXPECT_EQ(status1, SUCCESS);
  EXPECT_EQ(status2, SUCCESS);
}

// Test TSxCommit - Basic functionality
TEST_F(TestTSxMgr, TestTSxCommit_Basic) {
  // First begin a transaction
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  // Then commit it
  KStatus status = tsx_mgr_->TSxCommit(ctx_, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test TSxCommit - Commit without begin (should succeed)
TEST_F(TestTSxMgr, TestTSxCommit_NoBegin) {
  KStatus status = tsx_mgr_->TSxCommit(ctx_, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test TSxRollback - Basic functionality
TEST_F(TestTSxMgr, TestTSxRollback_Basic) {
  // First begin a transaction
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  // Then rollback it
  KStatus status = tsx_mgr_->TSxRollback(ctx_, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test TSxRollback - Rollback without begin (should succeed)
TEST_F(TestTSxMgr, TestTSxRollback_NoBegin) {
  KStatus status = tsx_mgr_->TSxRollback(ctx_, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrBegin - Basic functionality with explicit transaction
TEST_F(TestTSxMgr, TestMtrBegin_BasicWithExplicitTxn) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  KStatus status = tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
  EXPECT_NE(mini_trans_id, 0);
}

// Test MtrBegin - Without explicit transaction (default)
TEST_F(TestTSxMgr, TestMtrBegin_WithoutExplicitTxn) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  KStatus status = tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, nullptr);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrBegin - With existing transaction
TEST_F(TestTSxMgr, TestMtrBegin_WithExistingTxn) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  // First begin a TS transaction
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  // MtrBegin should return SUCCESS when TSx exists
  KStatus status = tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrCommit - Basic functionality
TEST_F(TestTSxMgr, TestMtrCommit_Basic) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  // Begin MTR first
  tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, ts_trans_id_);
  
  // Then commit
  KStatus status = tsx_mgr_->MtrCommit(ctx_, mini_trans_id, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrCommit - Without explicit transaction
TEST_F(TestTSxMgr, TestMtrCommit_WithoutExplicitTxn) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, nullptr);
  
  KStatus status = tsx_mgr_->MtrCommit(ctx_, mini_trans_id, nullptr);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrRollback - Basic functionality
TEST_F(TestTSxMgr, TestMtrRollback_Basic) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  // Begin MTR first
  tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, ts_trans_id_);
  
  // Then rollback
  KStatus status = tsx_mgr_->MtrRollback(ctx_, mini_trans_id, ts_trans_id_);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test MtrRollback - Without explicit transaction
TEST_F(TestTSxMgr, TestMtrRollback_WithoutExplicitTxn) {
  uint64_t mini_trans_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 0;
  
  tsx_mgr_->MtrBegin(ctx_, range_id, index, mini_trans_id, nullptr);
  
  KStatus status = tsx_mgr_->MtrRollback(ctx_, mini_trans_id, nullptr);
  
  EXPECT_EQ(status, SUCCESS);
}

// Test getMtrID - Get ID after TSxBegin
TEST_F(TestTSxMgr, TestGetMtrID_AfterTSxBegin) {
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  uint64_t mtr_id = tsx_mgr_->getMtrID(ts_trans_id_);
  
  EXPECT_NE(mtr_id, 0);
}

// Test getMtrID - Get ID with UUID helper
TEST_F(TestTSxMgr, TestGetMtrID_WithUUID) {
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  uint64_t mtr_id = tsx_mgr_->getMtrID(ts_trans_id_);
  
  EXPECT_NE(mtr_id, 0);
}

// Test insertMtrID - Manual insertion
TEST_F(TestTSxMgr, TestInsertMtrID_Manual) {
  uint64_t test_id = 12345;
  tsx_mgr_->insertMtrID(ts_trans_id_, test_id);
  
  uint64_t retrieved_id = tsx_mgr_->getMtrID(ts_trans_id_);
  
  EXPECT_EQ(retrieved_id, test_id);
}

// Test IsExplict - Check explicit transaction
TEST_F(TestTSxMgr, TestIsExplicit_WithExplicitTxn) {
  uint64_t mini_trans_id = 0;
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  // Get the mini_trans_id that was assigned
  uint64_t mtr_id = tsx_mgr_->getMtrID(ts_trans_id_);
  
  bool is_explicit = tsx_mgr_->IsExplict(mtr_id);
  
  EXPECT_TRUE(is_explicit);
}

// Test IsExplict - Check non-explicit transaction
TEST_F(TestTSxMgr, TestIsExplicit_NonExplicit) {
  bool is_explicit = tsx_mgr_->IsExplict(99999);
  
  EXPECT_FALSE(is_explicit);
}

// Test eraseMtrID - Erase by mini_trans_id
TEST_F(TestTSxMgr, TestEraseMtrID_ByMiniTransID) {
  uint64_t mini_trans_id = 0;
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  uint64_t mtr_id = tsx_mgr_->getMtrID(ts_trans_id_);
  tsx_mgr_->eraseMtrID(mtr_id);
  
  // After erasing, getMtrID should return 0
  uint64_t retrieved_id = tsx_mgr_->getMtrID(ts_trans_id_);
  EXPECT_EQ(retrieved_id, 0);
}

// Test full transaction lifecycle: Begin -> Commit
TEST_F(TestTSxMgr, TestFullTxnLifecycle_BeginCommit) {
  // Begin
  KStatus begin_status = tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  EXPECT_EQ(begin_status, SUCCESS);
  
  uint64_t mtr_id_before = tsx_mgr_->getMtrID(ts_trans_id_);
  EXPECT_NE(mtr_id_before, 0);
  
  // Commit
  KStatus commit_status = tsx_mgr_->TSxCommit(ctx_, ts_trans_id_);
  EXPECT_EQ(commit_status, SUCCESS);
  
  // After commit, transaction should be removed
  uint64_t mtr_id_after = tsx_mgr_->getMtrID(ts_trans_id_);
  EXPECT_EQ(mtr_id_after, 0);
}

// Test full transaction lifecycle: Begin -> Rollback
TEST_F(TestTSxMgr, TestFullTxnLifecycle_BeginRollback) {
  // Begin
  KStatus begin_status = tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  EXPECT_EQ(begin_status, SUCCESS);
  
  uint64_t mtr_id_before = tsx_mgr_->getMtrID(ts_trans_id_);
  EXPECT_NE(mtr_id_before, 0);
  
  // Rollback
  KStatus rollback_status = tsx_mgr_->TSxRollback(ctx_, ts_trans_id_);
  EXPECT_EQ(rollback_status, SUCCESS);
  
  // After rollback, transaction should be removed
  uint64_t mtr_id_after = tsx_mgr_->getMtrID(ts_trans_id_);
  EXPECT_EQ(mtr_id_after, 0);
}

// Test multiple MTR operations within a TS transaction
TEST_F(TestTSxMgr, TestMultipleMTROperations) {
  uint64_t mini_trans_id1 = 0, mini_trans_id2 = 0, mini_trans_id3 = 0;
  
  // Begin TS transaction
  tsx_mgr_->TSxBegin(ctx_, ts_trans_id_);
  
  // Multiple MTR operations
  tsx_mgr_->MtrBegin(ctx_, 1, 0, mini_trans_id1, ts_trans_id_);
  tsx_mgr_->MtrCommit(ctx_, mini_trans_id1, ts_trans_id_);
  
  tsx_mgr_->MtrBegin(ctx_, 1, 1, mini_trans_id2, ts_trans_id_);
  tsx_mgr_->MtrCommit(ctx_, mini_trans_id2, ts_trans_id_);
  
  tsx_mgr_->MtrBegin(ctx_, 1, 2, mini_trans_id3, ts_trans_id_);
  tsx_mgr_->MtrRollback(ctx_, mini_trans_id3, ts_trans_id_);
  
  // All should succeed
  EXPECT_NE(mini_trans_id1, 0);
  EXPECT_NE(mini_trans_id2, 0);
  EXPECT_NE(mini_trans_id3, 0);
}

}  // namespace kwdbts
