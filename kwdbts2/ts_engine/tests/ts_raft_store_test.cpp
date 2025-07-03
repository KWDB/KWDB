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
#include <gtest/gtest.h>
#include <unistd.h>
#include "libkwdbts2.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_raft_store.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TsRaftStoreTest : public ::testing::Test {
 public:
  RaftStore *raft_store_;
  RaftStoreOptions raft_options_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;  

 public:
  TsRaftStoreTest() {
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    raft_store_ = new RaftStore(engine_root_path + "/rs", raft_options_);
    auto s = raft_store_->Open();
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TsRaftStoreTest() {
    if (raft_store_) {
      delete raft_store_;
    }
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TsRaftStoreTest, put) {
  char key[] = "key";
  char value[] = "value";
  TSSlice key_slice{key, strlen(key)};
  TSSlice value_slice{value, strlen(value)};

  KStatus s = raft_store_->Put(key_slice, value_slice);
  EXPECT_EQ(s, KStatus::SUCCESS);

  std::string res;
  s = raft_store_->Get(key_slice, res);
  EXPECT_EQ(strncmp(res.c_str(), value, res.size()), 0);

  char key1[] = "key1";
  char value1[] = "value1";
  TSSlice key_slice1{key1, strlen(key1)};
  TSSlice value_slice1{value1, strlen(value1)};

  s = raft_store_->Put(key_slice1, value_slice1);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = raft_store_->Get(key_slice1, res);
  EXPECT_EQ(strncmp(res.c_str(), value1, res.size()), 0);
}
