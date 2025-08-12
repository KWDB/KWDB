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

#include "libkwdbts2.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "gtest/gtest.h"

extern bool g_go_start_service;

std::string kDbPath = "./test_db";  // NOLINT
RangeGroup kDefaultRange{101, 0};

// test CGO interface
class TestDB : public ::testing::Test {
 public:
  TSOptions opts_;
  TSEngine* ts_db_;

  TestDB() {
    system(("rm -rf " + kDbPath + "/*").c_str());
    rmdir(kDbPath.c_str());
    opts_.wal_level = 0;
    opts_.wal_buffer_size = 1;
    opts_.wal_file_size = 4;
    opts_.wal_file_in_group = 3;
    opts_.thread_pool_size = 2;
    opts_.task_queue_size = 10;
    opts_.lg_opts = TsLogOptions{};
    opts_.is_single_node = true;
    opts_.buffer_pool_size = 1024;
    opts_.brpc_addr = TSSlice{"127.0.0.1:27257", 13};
    opts_.cluster_id = TSSlice{"00000000-0000-0000-0000-000000000000", 36};
    ts_db_ = nullptr;
    g_go_start_service = false;
  }

  ~TestDB() {
    // CLOSE engine
    // ts_db_->Close();
    TSClose(ts_db_);
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
    ts_db_ = nullptr;
  }

  void OpenDB() {
    TSSlice s_path{kDbPath.data(), kDbPath.size()};
    opts_.lg_opts.Dir = s_path;
    TSStatus s = TSOpen(&ts_db_, s_path, opts_, nullptr, 1);
    ASSERT_EQ(s.data, nullptr);
    free(s.data);
  }
};

unsigned char kTMeta[] = {
    10, 29, 8, 233, 7, 24, 1, 32, 0, 40, 0, 48, 0, 48, 1, 48, 2, 56, 20, 64, 0, 82, 6, 116, 97, 98, 108, 101, 49, 88,
    0, 18, 22, 8, 0, 18, 6, 107, 95, 116, 105, 109, 101, 24, 0, 32, 0, 40, 8, 48, 0, 56, 0, 66, 0, 18, 20, 8, 1, 18,
    4, 99, 111, 108, 49, 24, 0, 32, 2, 40, 4, 48, 8, 56, 0, 66, 0, 18, 20, 8, 2, 18, 4, 99, 111, 108, 50, 24, 0, 32,
    5, 40, 8, 48, 12, 56, 0, 66, 0, 24, 0};

// test open \ close
TEST_F(TestDB, open) {
  OpenDB();
}

// search table that no exist
TEST_F(TestDB, find_noexist) {
  OpenDB();
  bool find;
  TSTableID table_id = 1001;
  TSStatus s = TSIsTsTableExist(ts_db_, table_id, &find);
  ASSERT_FALSE(find);
  free(s.data);
}

// delete table that no exist.
TEST_F(TestDB, drop_noexist) {
  OpenDB();
  TSTableID table_id = 1001;
  TSStatus s = TSDropTsTable(ts_db_, table_id);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
}

// create one table
TEST_F(TestDB, create) {
  OpenDB();
  TSTableID table_id = 1001;
  TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
  RangeGroups range_groups;
  range_groups.ranges = &kDefaultRange;
  range_groups.len = 1;
  TSStatus s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
}

// create and delete table 
TEST_F(TestDB, create_drop) {
  OpenDB();
  TSTableID table_id = 1001;
  TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
  RangeGroups range_groups;
  range_groups.ranges = &kDefaultRange;
  range_groups.len = 1;
  TSStatus s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
  bool find;
  s = TSIsTsTableExist(ts_db_, table_id, &find);
  ASSERT_TRUE(find);
  free(s.data);
  s = TSDropTsTable(ts_db_, table_id);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
  s = TSIsTsTableExist(ts_db_, table_id, &find);
  ASSERT_FALSE(find);
  free(s.data);
}

// create tables with same name
TEST_F(TestDB, multi_create) {
  OpenDB();
  TSTableID table_id = 1001;
  TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
  RangeGroups range_groups;
  range_groups.ranges = &kDefaultRange;
  range_groups.len = 1;
  TSStatus s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
  s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
  ASSERT_STREQ(s.data, nullptr);
  free(s.data);
}

// create and delete table multi-times.
TEST_F(TestDB, multi_create_drop) {
  OpenDB();
  TSTableID table_id = 1001;
  TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
  RangeGroups range_groups;
  range_groups.ranges = &kDefaultRange;
  range_groups.len = 1;
  for (size_t i = 0; i < 3; i++) {
    TSStatus s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
    ASSERT_STREQ(s.data, nullptr);
    free(s.data);
    s = TSDropTsTable(ts_db_, table_id);
    ASSERT_STREQ(s.data, nullptr);
    free(s.data);
  }
}

// create multiple tables
TEST_F(TestDB, create_multi_table) {
  OpenDB();
  TSTableID table_id = 1001;
  TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
  RangeGroups range_groups;
  range_groups.ranges = &kDefaultRange;
  range_groups.len = 1;
  for (size_t i = 0; i < 10; i++) {
    TSStatus s = TSCreateTsTable(ts_db_, table_id + i, meta, range_groups);
    ASSERT_STREQ(s.data, nullptr);
    free(s.data);
  }
  for (size_t i = 0; i < 10; i++) {
    TSStatus s = TSDropTsTable(ts_db_, table_id + i);
    ASSERT_STREQ(s.data, nullptr);
    free(s.data);
  }
}

// insert some data
TEST_F(TestDB, insert) {
  OpenDB();
  TSTableID table_id = 1001;
  {
    TSSlice meta{reinterpret_cast<char*>(kTMeta), sizeof(kTMeta)};
    RangeGroups range_groups;
    range_groups.ranges = &kDefaultRange;
    range_groups.len = 1;
    TSStatus s = TSCreateTsTable(ts_db_, table_id, meta, range_groups);
    ASSERT_STREQ(s.data, nullptr);
    free(s.data);
  }

  {
    // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
    TSSlice payload;
    TSStatus s = TSPutEntity(ts_db_, table_id, &payload, 0, kDefaultRange, 0);
    ASSERT_EQ(s.data, nullptr);
    free(s.data);
  }
}
