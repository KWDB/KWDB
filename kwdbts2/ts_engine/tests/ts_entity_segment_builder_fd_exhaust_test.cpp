// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_entity_segment_builder.h"

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_entity_segment_data.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_mem_segment_mgr.h"
#include "ts_version.h"
#include "ts_vgroup.h"

using namespace kwdbts;
using namespace roachpb;

// RAII helper for FD exhaustion - helps clean up automatically
class ExhaustiveFDGuard {
 private:
  std::vector<int> fds_;
  int original_limit_;

 public:
  explicit ExhaustiveFDGuard(int remaining = 10) {
    struct rlimit lim;
    getrlimit(RLIMIT_NOFILE, &lim);
    original_limit_ = lim.rlim_cur;

    // Set a low limit to make test faster
    int new_limit = 64;
    lim.rlim_cur = new_limit;
    lim.rlim_max = new_limit;
    setrlimit(RLIMIT_NOFILE, &lim);

    // Consume FDs until only 'remaining' are left
    while (true) {
      int fd = open("/dev/null", O_RDONLY);
      if (fd < 0) {
        // Failed to open, we've reached the limit
        break;
      }
      fds_.push_back(fd);
      if (fds_.size() >= new_limit - remaining) {
        break;
      }
    }
  }

  ~ExhaustiveFDGuard() {
    for (int fd : fds_) {
      close(fd);
    }
    struct rlimit lim;
    getrlimit(RLIMIT_NOFILE, &lim);
    lim.rlim_cur = original_limit_;
    lim.rlim_max = original_limit_;
    setrlimit(RLIMIT_NOFILE, &lim);
  }
};

// Test class for file descriptor exhaustion scenarios
class TsEntitySegmentBuilderFDExhaustTest : public ::testing::Test {
 protected:
  std::unique_ptr<TsEngineSchemaManager> mgr;
  EngineOptions opts;
  std::unique_ptr<TsVGroup> vgroup;
  kwdbContext_t ctx;
  int original_fd_limit = 0;

  void SetUp() override {
    System("rm -rf schema_fd_test");
    System("rm -rf db_fd_test");

    // Save original file descriptor limit
    struct rlimit lim;
    getrlimit(RLIMIT_NOFILE, &lim);
    original_fd_limit = lim.rlim_cur;

    EngineOptions::mem_segment_max_size = INT32_MAX;
    mgr = std::make_unique<TsEngineSchemaManager>("schema_fd_test");
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2, RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    mgr->Init(nullptr);
    opts.db_path = "db_fd_test";
    vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  }

  void TearDown() override {
    // Restore original file descriptor limit
    struct rlimit lim;
    getrlimit(RLIMIT_NOFILE, &lim);
    lim.rlim_cur = original_fd_limit;
    lim.rlim_max = original_fd_limit;
    setrlimit(RLIMIT_NOFILE, &lim);
  }

  void CreateTable(TSTableID table_id, const std::vector<DataType> &metric_types,
                 const std::vector<AttributeInfo>** metric_schema, std::vector<TagInfo> *tag_schema,
                 std::shared_ptr<TsTableSchemaManager> &schema_mgr) {
    CreateTsTable meta;
    ConstructRoachpbTableWithTypes(&meta, table_id, metric_types);
    ASSERT_EQ(mgr->CreateTable(nullptr, 1, table_id, &meta), SUCCESS);
    ASSERT_EQ(mgr->GetTableSchemaMgr(table_id, schema_mgr), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetMetricMeta(1, metric_schema), KStatus::SUCCESS);
    ASSERT_EQ(schema_mgr->GetTagMeta(1, *tag_schema), KStatus::SUCCESS);
  }
};

// Test that EntitySegmentBuilder handles file open failure gracefully
// This test simulates file descriptor exhaustion (ZDP-51347)
// The key issue is: if Open() fails, w_file_ may be null,
// and the destructor should not crash when trying to access it
TEST_F(TsEntitySegmentBuilderFDExhaustTest, OpenFailureDueToFDExhaustion) {
  EngineOptions::max_rows_per_block = 1000;
  EngineOptions::min_rows_per_block = 1000;

  TSTableID table_id = 123;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT, DataType::DOUBLE};
  const std::vector<AttributeInfo>* metric_schema;
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  auto env = &TsIOEnv::GetInstance();
  std::string test_path = "fd_exhaust_test";
  // Prepare directory before exhausting FDs
  ASSERT_EQ(env->DeleteDir(test_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(test_path), SUCCESS);

  TsVersionManager v_mgr(env, test_path);
  ASSERT_EQ(v_mgr.Recover(false), SUCCESS);

  PartitionIdentifier partition_id = {1, INT64_MIN, INT64_MAX};

  // Exhaust file descriptors so that opening new files will fail
  ExhaustiveFDGuard fd_guard(3);  // Leave only 3 FDs remaining

  // Create EntitySegmentBuilder - this should fail during Open()
  // because we don't have enough file descriptors
  TsEntitySegmentBuilder builder(env, test_path, mgr.get(), &v_mgr, partition_id,
                               nullptr, TsDataSource::Flush);

  // Open should fail due to file descriptor exhaustion
  KStatus open_status = builder.Open();
  EXPECT_EQ(open_status, FAIL) << "Expected Open() to fail due to FD exhaustion";

  // The destructor should not crash even if Open() failed
  // This is the key test for ZDP-51347
  // If code has a bug where it dereferences null pointers in destructor,
  // this will cause a crash
}

// Test specific builder classes with FD exhaustion
// This tests each file builder class individually to ensure they handle
// Open() failure gracefully without crashing in their destructors
TEST_F(TsEntitySegmentBuilderFDExhaustTest, IndividualBuilderFailure) {
  auto env = &TsIOEnv::GetInstance();
  std::string test_path = "builder_fd_test";
  ASSERT_EQ(env->DeleteDir(test_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(test_path), SUCCESS);

  ExhaustiveFDGuard fd_guard(5);  // Leave only 5 FDs

  // Try to create each type of builder and verify they don't crash on destruction

  // Test TsEntitySegmentEntityItemFileBuilder
  {
    std::string file_path = test_path + "/entity_item_test";
    TsEntitySegmentEntityItemFileBuilder builder(env, file_path, 1);
    // Open will fail due to FD exhaustion
    KStatus s = builder.Open();
    // Destructor should not crash even if Open() failed
  }

  // Test TsEntitySegmentBlockItemFileBuilder
  {
    std::string file_path = test_path + "/block_item_test";
    TsEntitySegmentBlockItemFileBuilder builder(env, file_path, 1, 0);
    KStatus s = builder.Open();
    // Destructor should not crash
  }

  // Test TsEntitySegmentBlockFileBuilder
  {
    std::string file_path = test_path + "/block_file_test";
    TsEntitySegmentBlockFileBuilder builder(env, file_path, 1, 0);
    KStatus s = builder.Open();
    // Destructor should not crash
  }

  // Test TsEntitySegmentAggFileBuilder
  {
    std::string file_path = test_path + "/agg_file_test";
    TsEntitySegmentAggFileBuilder builder(env, file_path, 1, 0);
    KStatus s = builder.Open();
    // Destructor should not crash
  }
}

// Test that even after multiple failed Open() attempts, destructors are safe
TEST_F(TsEntitySegmentBuilderFDExhaustTest, MultipleFailedOpenAttempts) {
  auto env = &TsIOEnv::GetInstance();
  std::string test_path = "multi_fail_test";
  ASSERT_EQ(env->DeleteDir(test_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(test_path), SUCCESS);

  ExhaustiveFDGuard fd_guard(2);  // Leave only 2 FDs

  // Try opening multiple builders in sequence
  for (int i = 0; i < 5; i++) {
    std::string file_path = test_path + "/test_" + std::to_string(i);
    TsEntitySegmentEntityItemFileBuilder builder(env, file_path, i + 1);
    KStatus s = builder.Open();
    // Each attempt may fail or succeed depending on available FDs
    // Destructor should not crash in either case
  }
}

// Test EntitySegmentVacuumer with FD exhaustion
TEST_F(TsEntitySegmentBuilderFDExhaustTest, VacuumerOpenFailure) {
  auto env = &TsIOEnv::GetInstance();
  std::string test_path = "vacuumer_fd_test";
  // Prepare directory before exhausting FDs
  ASSERT_EQ(env->DeleteDir(test_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(test_path), SUCCESS);

  TsVersionManager v_mgr(env, test_path);
  ASSERT_EQ(v_mgr.Recover(false), SUCCESS);

  ExhaustiveFDGuard fd_guard(3);  // Leave only 3 FDs

  // Create vacuumer - Open() may fail due to FD exhaustion
  TsEntitySegmentVacuumer vacuumer(test_path, &v_mgr, TsDataSource::Flush);
  KStatus open_status = vacuumer.Open();

  // Open may succeed or fail depending on available FDs
  // Either way, destructor should not crash
}

// Test destructor safety when only some files are successfully opened
TEST_F(TsEntitySegmentBuilderFDExhaustTest, PartialFileOpenSuccess) {
  auto env = &TsIOEnv::GetInstance();
  std::string test_path = "partial_open_test";
  ASSERT_EQ(env->DeleteDir(test_path), SUCCESS);
  ASSERT_EQ(env->NewDirectory(test_path), SUCCESS);

  // Leave enough FDs for some but not all files
  ExhaustiveFDGuard fd_guard(10);  // Leave 10 FDs

  TsVersionManager v_mgr(env, test_path);
  ASSERT_EQ(v_mgr.Recover(false), SUCCESS);

  PartitionIdentifier partition_id = {1, INT64_MIN, INT64_MAX};

  // Create EntitySegmentBuilder
  // Some file opens may succeed, some may fail
  TsEntitySegmentBuilder builder(env, test_path, mgr.get(), &v_mgr, partition_id,
                               nullptr, TsDataSource::Flush);

  KStatus open_status = builder.Open();
  // Regardless of success/failure, destructor should be safe
}
