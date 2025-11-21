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
#include "test_util.h"
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestDropTable : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineV2Impl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
    DropTableManager::getInstance().clearAllDroppedTables();
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

 public:
  TestDropTable() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EngineOptions::g_dedup_rule = DedupRule::KEEP;
  }

  ~TestDropTable() {
    if (engine_) {
      delete engine_;
    }
  }
};


TEST_F(TestDropTable, basicDrop) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_FALSE(is_dropped);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_FALSE(is_dropped);

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);


  is_dropped = DropTableManager::getInstance().isTableDropped(table_id);
  ASSERT_TRUE(is_dropped);

  is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::FAIL);
  ASSERT_TRUE(is_dropped);

  table_schema_mgr = nullptr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::FAIL);
  ASSERT_TRUE(is_dropped);
  ASSERT_EQ(table_schema_mgr, nullptr);
}

TEST_F(TestDropTable, hasFlagFile)
{
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::string file_name = opts_.db_path + "/schema/." + to_string(table_id);
  std::ofstream tmp_file(file_name);
  if (tmp_file.is_open())
  {
    tmp_file.close();
  }
  bool is_dropped = engine_->HasDroppedFlag(table_id);
  ASSERT_TRUE(is_dropped);
  is_dropped = false;
  ErrorInfo err_info;
  s = engine_->CheckAndDropTsTable(ctx_, table_id, is_dropped, err_info);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_TRUE(is_dropped);

  is_dropped = false;
  is_dropped = DropTableManager::getInstance().isTableDropped(table_id);
  ASSERT_TRUE(is_dropped);
}