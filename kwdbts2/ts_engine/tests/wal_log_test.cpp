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
#include "test_util.h"
#include "st_wal_mgr.h"

using namespace kwdbts;  // NOLINT

class TestWALManagerV2 : public ::testing::Test {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  uint64_t table_id_ = 10001;
  uint64_t tbl_grp_id_ = 123;
  WALMgr* wal_;
  EngineOptions opts_;

  TestWALManagerV2() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 1;
    opts_.wal_buffer_size = 4;
    opts_.db_path =  "./wal_log_test/";

    std::filesystem::remove_all(opts_.db_path);
    wal_ = new WALMgr("./wal_log_test/", intToString(tbl_grp_id_), &opts_);
    auto s = wal_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestWALManagerV2() {
    if (wal_ != nullptr) {
      delete wal_;
      wal_ = nullptr;
    }
  }
};

TEST_F(TestWALManagerV2, TestWALDeleteData) {
  uint64_t x_id = 1;
  string p_tag = "11111";
  timestamp64 timestamp = 1680000000;
  uint32_t range_size = 3;
  vector<DelRowSpan> drs;
  DelRowSpan d1 = {36000, 1,  "1111"};
  DelRowSpan d2 = {46000, 2,  "1100"};
  DelRowSpan d3 = {66000, 3,  "0000"};
  drs.push_back(d1);
  drs.push_back(d2);
  drs.push_back(d3);
  TS_LSN entry_lsn;
  KwTsSpan span{timestamp, timestamp + 1,};
  KStatus s = wal_->WriteDeleteMetricsWAL(ctx_, x_id, p_tag, {span}, drs, tbl_grp_id_, &entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, entry_lsn, wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<DeleteLogMetricsEntry*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::DELETE);
  EXPECT_EQ(redo->getXID(), x_id);
  EXPECT_EQ(redo->getTableType(), WALTableType::DATA);
  EXPECT_EQ(redo->getPrimaryTag(), p_tag);
  EXPECT_EQ(redo->start_ts_, 0);
  EXPECT_EQ(redo->end_ts_, 0);
  vector<DelRowSpan> partitions = redo->getRowSpans();
  EXPECT_EQ(partitions.size(), range_size);
  for (int i = 0; i < range_size; i++) {
    EXPECT_EQ(partitions[i].partition_ts, drs[i].partition_ts);
    EXPECT_EQ(partitions[i].blockitem_id, drs[i].blockitem_id);
    EXPECT_EQ(partitions[i].delete_flags[0], drs[i].delete_flags[0]);
  }

  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestWALManagerV2, TestWALDeleteDataV2) {
  uint64_t x_id = 1;
  string p_tag = "11111";
  uint32_t range_size = 3;
  vector<KwTsSpan> drs;
  KwTsSpan d1 = {3600, 7200};
  KwTsSpan d2 = {23600, 27200};
  KwTsSpan d3 = {333600, 337200};
  drs.push_back(d1);
  drs.push_back(d2);
  drs.push_back(d3);

  uint64_t vgrp_id = 3;
  TS_LSN entry_lsn;
  KStatus s = wal_->WriteDeleteMetricsWAL4V2(ctx_, x_id, table_id_, p_tag, drs, vgrp_id, &entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  vector<LogEntry*> redo_logs;
  std::vector<uint64_t> ignore;
  wal_->ReadWALLog(redo_logs, entry_lsn, wal_->FetchCurrentLSN(), ignore);

  EXPECT_EQ(redo_logs.size(), 1);

  auto* redo = reinterpret_cast<DeleteLogMetricsEntryV2*>(redo_logs[0]);
  EXPECT_EQ(redo->getType(), WALLogType::DELETE);
  EXPECT_EQ(redo->getXID(), x_id);
  EXPECT_EQ(redo->getTableType(), WALTableType::DATA_V2);
  EXPECT_EQ(redo->getPrimaryTag(), p_tag);
  EXPECT_EQ(redo->getTableId(), table_id_);
  EXPECT_EQ(redo->getVGroupID(), vgrp_id);
  vector<KwTsSpan> partitions = redo->getTsSpans();
  EXPECT_EQ(partitions.size(), range_size);
  for (int i = 0; i < range_size; i++) {
    EXPECT_EQ(partitions[i].begin, drs[i].begin);
    EXPECT_EQ(partitions[i].end, drs[i].end);
  }

  for (auto& l : redo_logs) {
    delete l;
  }
}
