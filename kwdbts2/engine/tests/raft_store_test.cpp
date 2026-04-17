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
#include "libkwdbts2.h"
#include "raft_store.h"
#include "../../ts_engine/tests/test_util.h"

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
  RaftStore *raft_store_{nullptr};
  kwdbContext_t g_ctx_{};
  kwdbContext_p ctx_{&g_ctx_};

  static void SetUpTestCase() {
    KWDBDynamicThreadPool::GetThreadPool().InitImplicitly();
  }

  static void TearDownTestCase() {
    auto& pool = KWDBDynamicThreadPool::GetThreadPool();
    if (!pool.IsStop()) {
      pool.Stop();
    }
#ifdef WITH_TESTS
    KWDBDynamicThreadPool::Destroy();
#endif
  }

 public:
  TsRaftStoreTest() {
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    InitKWDBContext(ctx_);
    raft_store_ = new RaftStore();
    raft_store_->init(engine_root_path + "/rs");
  }

  ~TsRaftStoreTest() override { delete raft_store_; }
};

TEST_F(TsRaftStoreTest, put) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value[i].data(), value[i].size()};
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  TSRaftlog raftlog[3];
  for(uint64_t i = 0; i < 2; i++) {
    raftlog[i] = {i+1, 6, &indexs[0], all_data, offs};
  }

  uint64_t index = 0;
  uint64_t status_offs[2] = {0, value[5].size()};
  raftlog[2] = {1, 1, &index, value[5].data(), status_offs};
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
  delete[] all_data;
}

TEST_F(TsRaftStoreTest, delete) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  std::unordered_map<int, string> value1;
  for (int i = 1; i < 7; i++) {
    value1[i] = value[i];
  }
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value1[i].data(), value1[i].size()};
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value1[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value1[i].data(), value1[i].size());
    total_len += value1[i].size();
  }

  uint64_t del_indexes[2] = {103,106};
  TSRaftlog raftlog[3] = {{1, 6, &indexs[0], all_data, offs},
                          {1, 2, del_indexes, nullptr, nullptr},
                          {1, 0, nullptr, nullptr, nullptr}};
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
  delete[] all_data;
}

TEST_F(TsRaftStoreTest, Init) {
  KStatus s;
  TSSlice value_slice[6];
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  for (int i = 1; i < 7; i++) {
    value_slice[i - 1] = {value[i].data(), value[i].size()};
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  TSRaftlog raftlog[17];
  for (uint64_t i = 0; i < 17; i++) {
    raftlog[i] = {i + 1, 6, &indexs[0], all_data, offs};
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
  TSRaftlog del_raftlog = {2, 2, del_indexes, nullptr, nullptr};
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
  delete[] all_data;
}

TEST_F(TsRaftStoreTest, GetFirstAndLastIndex) {
  KStatus s;
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  for (int i = 1; i < 7; i++) {
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  TSRaftlog raftlog = {1, 6, &indexs[0], all_data, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  uint64_t first_index = 0;
  s = raft_store_->GetFirstIndex(ctx_, 1, &first_index);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(first_index, 101);

  uint64_t last_index = 0;
  s = raft_store_->GetLastIndex(ctx_, 1, &last_index);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(last_index, 106);

  TSSlice first_value;
  s = raft_store_->GetFirst(ctx_, 1, &first_value);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(strncmp(first_value.data, value[1].data(), value[1].size()), 0);
  free(first_value.data);

  // Test non-existent range
  s = raft_store_->GetFirstIndex(ctx_, 999, &first_index);
  EXPECT_EQ(s, KStatus::FAIL);
  s = raft_store_->GetLastIndex(ctx_, 999, &last_index);
  EXPECT_EQ(s, KStatus::FAIL);
  s = raft_store_->GetFirst(ctx_, 999, &first_value);
  EXPECT_EQ(s, KStatus::FAIL);

  delete[] all_data;
}

TEST_F(TsRaftStoreTest, SyncAndClose) {
  KStatus s;
  uint64_t indexs[1] = {100};
  uint64_t offs[2] = {0, value[1].size()};

  TSRaftlog raftlog = {1, 1, &indexs[0], value[1].data(), offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, false);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = raft_store_->Sync(ctx_);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TsRaftStoreTest, ClearRange) {
  KStatus s;
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  for (int i = 1; i < 7; i++) {
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  TSRaftlog raftlog = {1, 6, &indexs[0], all_data, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Clear range (index_cnt = 0)
  TSRaftlog clear_log = {1, 0, nullptr, nullptr, nullptr};
  s = raft_store_->WriteRaftLog(ctx_, 1, &clear_log, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Verify range is cleared
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 1, 101, 107, res);
  EXPECT_EQ(s, KStatus::FAIL);

  delete[] all_data;
}

TEST_F(TsRaftStoreTest, Truncate) {
  KStatus s;
  uint64_t indexs[6];
  uint64_t offs[7];
  size_t total_len = 0;
  for (int i = 1; i < 7; i++) {
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[6] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i < 7; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  TSRaftlog raftlog = {1, 6, &indexs[0], all_data, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Truncate (index_cnt = 1)
  uint64_t truncate_index = 104;
  TSRaftlog truncate_log = {1, 1, &truncate_index, nullptr, nullptr};
  s = raft_store_->WriteRaftLog(ctx_, 1, &truncate_log, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Verify truncate - indexes >= 104 should be deleted
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 1, 104, 107, res);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Verify indexes < 104 still exist
  s = raft_store_->Get(ctx_, 1, 101, 104, res);
  for (int i = 1; i < 4; i++) {
    EXPECT_EQ(strncmp(res[i - 1].data, value[i].data(), value[i].size()), 3);
    free(res[i - 1].data);
  }

  delete[] all_data;
}

TEST_F(TsRaftStoreTest, GetNonExistentRange) {
  KStatus s;
  TSSlice res[6];
  s = raft_store_->Get(ctx_, 999, 101, 107, res);
  EXPECT_EQ(s, KStatus::FAIL);
}

TEST_F(TsRaftStoreTest, WriteEmptyValue) {
  KStatus s;
  uint64_t indexs[1] = {100};
  uint64_t offs[2] = {0, 0};

  TSRaftlog raftlog = {1, 1, &indexs[0], nullptr, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TSSlice res;
  s = raft_store_->Get(ctx_, 1, 100, 101, &res);
  EXPECT_EQ(s, KStatus::FAIL);
}

TEST_F(TsRaftStoreTest, MultipleRanges) {
  KStatus s;
  uint64_t indexs[3];
  uint64_t offs[4];
  size_t total_len = 0;
  for (int i = 1; i <= 3; i++) {
    indexs[i - 1] = 100 + i;
    offs[i - 1] = total_len;
    total_len += value[i].size();
  }
  offs[3] = total_len;

  char* all_data = new char[total_len];
  total_len = 0;
  for (int i = 1; i <= 3; i++) {
    memcpy(all_data + total_len, value[i].data(), value[i].size());
    total_len += value[i].size();
  }

  // Write to range 1
  TSRaftlog raftlog1 = {1, 3, &indexs[0], all_data, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog1, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Write to range 2
  TSRaftlog raftlog2 = {2, 3, &indexs[0], all_data, offs};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog2, true);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // Verify both ranges
  TSSlice res[3];
  s = raft_store_->Get(ctx_, 1, 101, 104, res);
  EXPECT_EQ(s, KStatus::SUCCESS);
  for (int i = 1; i <= 3; i++) {
    free(res[i - 1].data);
  }

  s = raft_store_->Get(ctx_, 2, 101, 104, res);
  EXPECT_EQ(s, KStatus::SUCCESS);
  for (int i = 1; i <= 3; i++) {
    free(res[i - 1].data);
  }

  delete[] all_data;
}

TEST_F(TsRaftStoreTest, InvalidOperationType) {
  KStatus s;
  // Test with invalid index_cnt (> 2)
  uint64_t indexs[5] = {100, 101, 102, 103, 104};
  TSRaftlog raftlog = {1, 5, indexs, nullptr, nullptr};
  s = raft_store_->WriteRaftLog(ctx_, 1, &raftlog, true);
  EXPECT_EQ(s, KStatus::FAIL);
}
