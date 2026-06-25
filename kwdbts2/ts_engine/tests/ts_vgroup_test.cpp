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

#include "ts_vgroup.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <memory>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_mem_segment_mgr.h"

using namespace kwdbts;  // NOLINT
using namespace roachpb;

class TsVGroupTest : public ::testing::Test {
 protected:
  std::unique_ptr<TsEngineSchemaManager> mgr;
  EngineOptions opts;
  std::unique_ptr<TsVGroup> vgroup;
  kwdbContext_t ctx;

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

  TsVGroupTest() {
    EngineOptions::mem_segment_max_size = INT32_MAX;
  }

  ~TsVGroupTest() override = default;

  void SetUp() override {
    System("rm -rf schema");
    System("rm -rf db001-123");

    mgr = std::make_unique<TsEngineSchemaManager>("schema");
    std::shared_mutex wal_level_mutex;
    TsHashRWLatch tag_lock(EngineOptions::vgroup_max_num * 2, RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK);
    mgr->Init(nullptr);
    opts.db_path = "db001-123";
    vgroup = std::make_unique<TsVGroup>(&opts, 0, mgr.get(), &wal_level_mutex, &tag_lock, false);
    EXPECT_EQ(vgroup->Init(&ctx), KStatus::SUCCESS);
  }
};

// Bug repro: dropping a table before flush triggers crash in ApplyUpdate fast path.
//
// MemSegmentsOnly() on master does not check has_max_lsn_, so when GetBlockSpans
// returns empty (table already dropped), the update enters the fast path with
// has_del_mem_segments_ set.  BuildValidMemSegment finds the memseg and returns
// non-null removed_memseg, hitting assert(removed_memseg == nullptr) at ts_version.cpp:416.
//
// Trigger chain:
//   PutData → Flush → GetBlockSpans (table dropped, rows skipped) → SetMaxLSN(0)
//   → RemoveMemSegment → MemSegmentsOnly() == true → assert crashes
TEST_F(TsVGroupTest, DropTableBeforeFlushCausesAssert) {
  TSTableID table_id = 999;
  std::vector<DataType> metric_types{DataType::TIMESTAMP, DataType::INT};
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  std::vector<TagInfo> tag_schema;
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  CreateTable(table_id, metric_types, &metric_schema, &tag_schema, schema_mgr);

  TSEntityID dev_id = 1;
  auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, dev_id, 100, 123, 1);
  TsRawPayload p{metric_schema};
  p.ParsePayLoadStruct(payload);
  auto ptag = p.GetPrimaryTag();

  ASSERT_EQ(vgroup->PutData(&ctx, schema_mgr, 0, &ptag, dev_id, p, false), KStatus::SUCCESS);
  free(payload.data);

  // Drop before flush: GetBlockSpans skips all rows for a dropped table.
  ASSERT_EQ(mgr->SetTableDropped(table_id), SUCCESS);

  // Crashes here at assert(removed_memseg == nullptr) in ts_version.cpp:416.
  ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
}
