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

#include <memory>
#include <string>
#include <vector>

#include "ts_engine.h"
#include "libkwdbts2.h"
#include "../../ts_engine/tests/test_util.h"
#include "../../ts_engine/tests/ts_test_base.h"

namespace kwdbts {

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

TSEngineImpl* CreateTestTsEngine(kwdbContext_p ctx, const string& db_path) {
  EngineOptions opts;
  opts.wal_level = 0;
  opts.db_path = db_path;
  system(("rm -rf " + db_path + "/*").c_str());
  auto* ts_engine = new TSEngineImpl(opts);
  auto s = ts_engine->Init(ctx);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ctx->ts_engine = ts_engine;
  KWDBDynamicThreadPool::GetThreadPool().Init(1, ctx);
  return ts_engine;
}

void CloseTestTsEngine(kwdbContext_p ctx) {
  auto* ts_engine = static_cast<TSEngineImpl*>(ctx->ts_engine);
  if (ts_engine) {
    delete ts_engine;
  }
  KWDBDynamicThreadPool::GetThreadPool().Stop();
}

void CreateTableAndPrepareData(kwdbContext_p ctx_, TSTableID table_id, roachpb::CreateTsTable pb_meta,
                               TSEngineImpl* engine_) {
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 10, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  start_ts = 3600;
  pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 2, 10, start_ts);
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

}  // namespace kwdbts
