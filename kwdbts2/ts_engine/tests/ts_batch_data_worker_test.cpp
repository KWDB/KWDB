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

#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_entity_segment.h"

using namespace kwdbts;  // NOLINT

const string engine_root_path = "./tsdb";
class TsBatchDataWorkerTest : public ::testing::Test {
public:
  EngineOptions opts_;
  TSEngineV2Impl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

public:
  TsBatchDataWorkerTest() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TsBatchDataWorkerTest() {
    if (engine_) {
      delete engine_;
    }
  }
};

TEST_F(TsBatchDataWorkerTest, TestTsBatchDataWorker) {
  TSTableID table_id = 10032;
  roachpb::CreateTsTable pb_meta;
  using namespace roachpb;
  std::vector<DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE,
                                    roachpb::VARCHAR};
  ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
  // ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  std::vector<TagInfo> tag_schema;
  s = schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);
  ASSERT_EQ(metric_schema->size(), metric_type.size());
  timestamp64 start_ts = 10086000;
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);

  // read batch job
  uint64_t read_job_id = 1;
  TSSlice data;
  uint32_t row_num;
  s = engine_->ReadBatchData(ctx_, table_id, 1, 0, UINT32_MAX, {INT64_MIN, INT64_MAX}, read_job_id, &data, &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::string backup_data = std::string(data.data, data.len);

  s = engine_->BatchJobFinish(ctx_, read_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // reopen
  delete engine_;
  Remove(engine_root_path);
  MakeDirectory(engine_root_path);
  engine_ = new TSEngineV2Impl(opts_);
  s = engine_->Init(ctx_);
  MakeDirectory(engine_root_path + "/temp_db_");
  EXPECT_EQ(s, KStatus::SUCCESS);

  // ConstructRoachpbTable(&pb_meta, table_id);
  s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint32_t n_rows;
  uint64_t write_job_id = 2;
  data.data = backup_data.data();
  data.len = backup_data.size();
  // first write
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, row_num);
  // second write
  s = engine_->WriteBatchData(ctx_, table_id, 1, write_job_id, &data, &n_rows);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(n_rows, row_num);
  //  finish
  s = engine_->BatchJobFinish(ctx_, write_job_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  for (uint32_t vgroup_id = 1; vgroup_id <= EngineOptions::vgroup_max_num; vgroup_id++) {
    auto vgroup = engine_->GetTsVGroup(vgroup_id);
    auto p = vgroup->CurrentVersion()->GetPartition(1, 10086);
    if (p == nullptr) {
      continue;
    }
    auto entity_segment = p->GetEntitySegment();
    uint32_t entity_id = entity_segment->GetEntityNum();
    assert(entity_id == 1);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);
    KwTsSpan ts_span{INT64_MIN, INT64_MAX};
    KwOSNSpan osn_span{0, UINT64_MAX};
    STScanRange scan_range{ts_span, osn_span};
    TsBlockItemFilterParams filter{1, table_id, vgroup_id, entity_id, {scan_range}};
    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = entity_segment->GetBlockSpans(filter, block_spans, schema_mgr, schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  ASSERT_EQ(block_spans.size(), 2);
  while (!block_spans.empty()) {
    std::shared_ptr<TsBlockSpan> block_span = block_spans.front();
    std::unique_ptr<TsBitmapBase> bitmap;
    char *ts_col;
    s = block_span->GetFixLenColAddr(0, &ts_col, &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<char *> col_values;
    col_values.resize(2);
    s = block_span->GetFixLenColAddr(1, &col_values[0], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    s = block_span->GetFixLenColAddr(2, &col_values[1], &bitmap);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint64_t osn = *(uint64_t *) (block_span->GetOSNAddr(0));
    for (int idx = 0; idx < block_span->GetRowNum(); ++idx) {
      EXPECT_EQ(*(uint64_t *) (block_span->GetOSNAddr(idx)), osn);
      EXPECT_EQ(block_span->GetTS(idx), 10086000 + idx * 1000);
      EXPECT_EQ(*(timestamp64 *) (ts_col + idx * 8), 10086000 + idx * 1000);
      EXPECT_LE(*(int32_t *) (col_values[0] + idx * 4), 1024);
      EXPECT_LE(*(double *) (col_values[1] + idx * 8), 1024 * 1024);
      kwdbts::DataFlags flag;
      TSSlice var_data;
      s = block_span->GetVarLenTypeColAddr(idx, 3, flag, var_data);
      EXPECT_EQ(s, KStatus::SUCCESS);
      string str(var_data.data, 10);
      EXPECT_EQ(str, "varstring_");
    }
    block_spans.pop_front();
  }
}
