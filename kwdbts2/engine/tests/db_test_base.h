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

#pragma once

#include <string>

#include "engine.h"
#include "../ts_engine/tests/test_util.h"

namespace kwdbts {

class TsDBTestBase : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    KWDBDynamicThreadPool::GetThreadPool().InitImplicitly();
  }

  static void TearDownTestCase() {
    auto& pool = KWDBDynamicThreadPool::GetThreadPool();
    if (!pool.IsStop()) {
      pool.Stop();
    }
    KWDBDynamicThreadPool::Destroy();
  }

 protected:
  TsDBTestBase() = default;

  ~TsDBTestBase() override {
    DestroyDB();
  }

  void InitDB(const std::string& db_path, bool reset_db_path = true) {
    DestroyDB();
    if (reset_db_path) {
      Remove(db_path + "/tsdb");
      MakeDirectory(db_path);
    }
    TSSlice dir = {const_cast<char*>(db_path.c_str()), db_path.size()};
    TSOptions opt;
    opt.wal_level = 0;
    opt.must_exist = false;
    opt.read_only = false;
    opt.extra_options = {nullptr, 0};
    opt.thread_pool_size = 0;
    opt.task_queue_size = 0;
    opt.buffer_pool_size = 1024;
    opt.lg_opts.Dir = {nullptr, 0};
    opt.lg_opts.LogFileMaxSize = 1024;
    opt.lg_opts.LogFilesCombinedMaxSize = 1024;
    opt.lg_opts.LogFileVerbosityThreshold = DEFAULT_K;
    opt.lg_opts.Trace_on_off_list = {nullptr, 0};
    opt.is_single_node = true;
    opt.brpc_addr = {nullptr, 0};
    opt.cluster_id = {nullptr, 0};
    AppliedRangeIndex index = {1, 1};
    auto s = TSOpen(&engine_, dir, opt, &index, 1);
    EXPECT_EQ(s.data, nullptr);
  }

  void DestroyDB() {
    if (engine_ != nullptr) {
      delete engine_;
      engine_ = nullptr;
    }
  }

  TSEngine* engine_{nullptr};
};

}  // namespace kwdbts



