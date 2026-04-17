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
#include "sys_utils.h"
#include "ts_test_base.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestDropTable : public TsEngineTestBase {
 public:
  TestDropTable() {
    EngineOptions::g_dedup_rule = DedupRule::KEEP_EXPERIMENTAL;
    InitContext();
    InitEngine(engine_root_path);
  }
};


TEST_F(TestDropTable, basicDrop) {
  TSTableID table_id = 999;
  fs::path table_schema_path = fs::path(engine_root_path) / "schema" / std::to_string(table_id);
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

  ASSERT_FALSE(ts_table->IsDropped());
  s = engine_->DropTsTable(ctx_, table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  is_dropped = table_schema_mgr->IsDropped();
  ASSERT_TRUE(is_dropped);
  ASSERT_TRUE(IsExists(table_schema_path));

  ASSERT_TRUE(ts_table->IsDropped());
  ts_table.reset();
  table_schema_mgr.reset();

  ASSERT_FALSE(IsExists(table_schema_path));
}
