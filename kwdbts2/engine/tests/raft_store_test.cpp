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

#include <unistd.h>
#include "engine.h"
#include "test_util.h"
#include "libkwdbts2.h"
#include "raft_store.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
std::unordered_map<int, string> value = {{1, "value1"},
                                         {2, "value2"},
                                         {3, "value3"},
                                         {4, "value4"},
                                         {5, "value5"},
                                         {6, "value6"}};
class TsRaftStoreTest : public ::testing::Test {
 public:
  RaftStore *raft_store_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

 public:
  TsRaftStoreTest() {
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    raft_store_ = new RaftStore();
    raft_store_->init(engine_root_path + "/rs");
  }

  ~TsRaftStoreTest() {
    if (raft_store_) {
      delete raft_store_;
    }
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TsRaftStoreTest, put) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value[i].data(), value[i].size()};
    indexs[i - 1] = 100 + i;
  }
  TSRaftlog raftlog[3];
  for(uint64_t i = 0; i < 2; i++) {
    raftlog[i] = {i+1, 6, &indexs[0], value_slice};
  }

  uint64_t index =0;
  TSSlice value_status ={value[5].data(), value[5].size()};
  raftlog[2] = {1, 1, &index, &value_status};
  s = raft_store_->WriteRaftLog(ctx_, 3, raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 1, 101, 107, res);
  for (int i = 1; i < 7; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  s = raft_store_->Get(ctx_, 2, 101, 107, res);
  for (int i = 1; i < 7; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  s = raft_store_->Get(ctx_, 1, 0, 1, res);
  EXPECT_EQ(strncmp(res[0].data, value[5].data(), value[5].size()), 0);
  free(res[0].data);
}

TEST_F(TsRaftStoreTest, delete) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  std::unordered_map<int, string> value1;
  for (int i = 1; i < 7; i++) {
    value1[i] = value[i];
  }
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value1[i].data(), value1[i].size()};
    indexs[i - 1] = 100 + i;
  }
  uint64_t del_indexes[2] = {103,106};
  TSRaftlog raftlog[3] = {{1, 6, &indexs[0], value_slice}, {1, 2, del_indexes, nullptr}, {1, 0, nullptr, nullptr}};
  s = raft_store_->WriteRaftLog(ctx_, 2, raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 1, 101, 103, res);
  for (int i = 1; i < 3; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  s = raft_store_->Get(ctx_, 1, 106, 107, res);
  EXPECT_EQ(strncmp(res[0].data, value[6].data(), value[6].size()), 0);
  free(res[0].data);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(raft_store_->Get(ctx_, 1, 103 + i, 104 + i, res), KStatus::FAIL);
  }
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog[2], true);
  EXPECT_EQ(raft_store_->Get(ctx_, 1, 106, 107, res), KStatus::FAIL);
}

TEST_F(TsRaftStoreTest, Init) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value[i].data(), value[i].size()};
    indexs[i - 1] = 100 + i;
  }
  TSRaftlog raftlog[17];
  for (uint64_t i = 0; i < 17; i++) {
    raftlog[i] = {i + 1, 6, &indexs[0], value_slice};
  }
  raft_store_->resizeMax();
  uint64_t index = 0;
  TSSlice value_status = {value[5].data(), value[5].size()};
  s = raft_store_->WriteRaftLog(ctx_, 1, raftlog, true);
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 1, 101, 107, res);
  for (int i = 1; i < 7; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog[1], true);
  s = raft_store_->Get(ctx_, 2, 101, 107, res);
  for (int i = 1; i < 7; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  uint64_t del_indexes[2] = {103,106};
  TSRaftlog del_raftlog = {2, 2, del_indexes, nullptr};
  s = raft_store_->WriteRaftLog(ctx_, 1, &del_raftlog, true);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(raft_store_->Get(ctx_, 2, 103 + i, 104 + i, res), KStatus::FAIL);
  }
  s = raft_store_->Get(ctx_, 2, 101, 103, res);
  for (int i = 1; i < 3; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  for (int i = 2; i < 17; i++) {
    s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog[i], true);
  }
  for (int i = 1; i < 17; i++) {
    if(i == 2) {
      continue;
    }
    s = raft_store_->Get(ctx_, i, 101, 107, res);
    for (int j = 1; j < 7; j++) {
      EXPECT_EQ(strncmp(res[j - 1].data, value[j].data(), value[j].size()), 0);
      free(res[j - 1].data);
    }
  }
  s = raft_store_->Get(ctx_, 2, 101, 103, res);
  for (int i = 1; i < 3; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
  sleep(1);
  // Clear the memory and reload the files.
  raft_store_->reStart();
  raft_store_->init(engine_root_path + "/rs");
  for (int i = 1; i < 17; i++) {
    if (i == 2) {
      continue;
    }
    s = raft_store_->Get(ctx_, i, 101, 107, res);
    for (int j = 1; j < 7; j++) {
      EXPECT_EQ(strncmp(res[j - 1].data, value[j].data(), value[j].size()), 0);
      free(res[j - 1].data);
    }}

  // Verify whether to delete.
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(raft_store_->Get(ctx_, 2, 103 + i, 104 + i, res), KStatus::FAIL);
  }
  s = raft_store_->Get(ctx_, 2, 101, 103, res);
  for (int i = 1; i < 3; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 0);
    free(res[i - 1].data);
  }
}