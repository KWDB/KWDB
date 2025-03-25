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
#include <unistd.h>
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TsEngineV2Test : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineV2Impl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;  

 public:
  TsEngineV2Test() {
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TsEngineV2Test() {
    if (engine_) {
      delete engine_;
    }
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }
};

TEST_F(TsEngineV2Test, empty) {
}

TEST_F(TsEngineV2Test, simpleInsert) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  s = engine_->GetTsSchemaMgr(ctx_, table_id, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<AttributeInfo> metric_schema;
  s = schema_mgr->GetMetricMeta(1, metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema.size(), metric_type.size());
  timestamp64 start_ts = 10086000;
  auto pay_load = GenRowPayload(metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  s = engine_->PutData(ctx_, table_id, 1, &pay_load, true);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TsEngineV2Test, InsertMulitMemSeg) {
  using namespace roachpb;
  TSTableID table_id = 12345;
  CreateTsTable pb_meta;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::DOUBLE};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  s = engine_->GetTsSchemaMgr(ctx_, table_id, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<AttributeInfo> metric_schema;
  s = schema_mgr->GetMetricMeta(1, metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema.size(), metric_type.size());

  timestamp64 ts = 10086000;
  for (int i = 0; i < 100000; ++i) {
    auto pay_load = GenRowPayload(metric_schema, tag_schema , table_id, 1, 1, 1, ts);
    s = engine_->PutData(ctx_, table_id, 2, &pay_load, true);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts += 1000;
  }
}
