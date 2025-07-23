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

std::string db_path = "./test_db";  // NOLINT

extern bool g_go_start_service;

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

class TestEngineSnapshotImgrate : public ::testing::Test {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngineV2Impl* ts_engine_src_;
  TSEngineV2Impl* ts_engine_desc_;

  virtual void SetUp() override {
    ctx_ = &context_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  TestEngineSnapshotImgrate() {
    auto ret = system(("rm -rf " + db_path + "/*").c_str());
    EXPECT_EQ(ret, 0);
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.db_path = db_path + "/srcdb/";
    // clear path files.
    auto engine = new TSEngineV2Impl(opts_);
    auto s = engine->Init(ctx_);
    if (s != KStatus::SUCCESS) {
      std::cout << "engine init failed." << std::endl;
      exit(1);
    }
    ts_engine_src_ = engine;

    opts_.db_path = db_path + "/descdb/";
    engine = new TSEngineV2Impl(opts_);
    s = engine->Init(ctx_);
    if (s != KStatus::SUCCESS) {
      std::cout << "engine init failed." << std::endl;
      exit(1);
    }
    ts_engine_desc_ = engine;
    g_go_start_service = false;
  }

  ~TestEngineSnapshotImgrate() {
    if (ts_engine_src_ != nullptr) {
      delete ts_engine_src_;
      ts_engine_src_ = nullptr;
    }
    if (ts_engine_desc_ != nullptr) {
      delete ts_engine_desc_;
      ts_engine_desc_ = nullptr;
    }
  }
  
  void InsertData(TSEngineV2Impl* ts_e, TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts, int num, KTimestamp interval = 1000) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    KStatus s = ts_e->GetTableSchemaMgr(ctx_, table_id, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<AttributeInfo> metric_schema;
    s = schema_mgr->GetMetricMeta(1, metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto pay_load = GenRowPayload(metric_schema, tag_schema ,table_id, 1, dev_id, num, start_ts);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = ts_e->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    EXPECT_EQ(s , KStatus::SUCCESS);
    free(pay_load.data);
  }
  uint64_t GetDataNum(TSEngineV2Impl* ts_e, TSTableID table_id, EntityResultIndex dev_id, KwTsSpan ts_span) {
    std::shared_ptr<TsTable> ts_table_dest;
    auto s = ts_e->GetTsTable(ctx_, table_id, ts_table_dest);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_dest);
    uint64_t row_count = 0;
    std::vector<EntityResultIndex> devs{dev_id};
    ctx_->ts_engine = ts_e;
    ts_table_v2->GetEntityRowCount(ctx_, devs, {ts_span}, &row_count);
    return row_count;
  }
};

TEST_F(TestEngineSnapshotImgrate, empty) {
}

// data valume test.
TEST_F(TestEngineSnapshotImgrate, TestDataVolumeIntface) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1001;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  int run_times = 3;
  int entity_num = 3;
  int entity_rows = 10;
  for (size_t j = 0; j < run_times; j++) {
    for (size_t i = 1; i <= entity_num; i++) {
      InsertData(ts_engine_src_, cur_table_id, i, 12345 + 1000 * entity_rows * (i - 1), entity_rows);
      uint64_t row_num;
      s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
      ASSERT_EQ(s, KStatus::SUCCESS);
      while (row_num == 0) {
        std::cout << "sdfsdf " << std::endl;
        s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
        ASSERT_EQ(s, KStatus::SUCCESS);
      }
      ASSERT_EQ(row_num, i * entity_rows);
      uint64_t volume;
      s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ASSERT_EQ(volume, 28 * row_num);
      timestamp64 half_ts;
      s = ts_table->GetDataVolumeHalfTS(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &half_ts);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ASSERT_EQ(half_ts, 12345 + (1000 * entity_rows * i) / 2);
    }
    s = ts_table->DeleteTotalRange(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 1);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t row_num;
    s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(row_num, 0);
  }
}

// snapshot data from src engine to desc engine , only 0 rows.
TEST_F(TestEngineSnapshotImgrate, CreateSnapshotAndInsertOtherEmpty) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<EntityResultIndex> entity_ids;
  for(auto vg : *(ts_engine_desc_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(0, entity_ids.size());
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from src engine to desc engine , only 5 rows.
TEST_F(TestEngineSnapshotImgrate, CreateSnapshotAndInsertOther) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1007
  InsertData(ts_engine_src_, cur_table_id, 1, 12345, 5);
  std::vector<EntityResultIndex> entity_ids;
  for(auto vg : *(ts_engine_src_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  auto row_count = GetDataNum(ts_engine_src_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ASSERT_EQ(row_count, 5);

  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  entity_ids.clear();
  for(auto vg : *(ts_engine_desc_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  // scan table ,check if data is correct in table 1008.
  ctx_->ts_engine = ts_engine_desc_;
  row_count = GetDataNum(ts_engine_desc_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(row_count, 5);
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from src engine to desc engine , three partition each 5 rows.
TEST_F(TestEngineSnapshotImgrate, CreateSnapshotAndInsertPartitions) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1004;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1007
  int partition_num = 3;
  const int64_t interval = 3600 * 24 * 10;
  for (size_t i = 0; i < partition_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1, 12345 + i * interval, 5);
  }
  std::vector<EntityResultIndex> entity_ids;
  for(auto vg : *(ts_engine_src_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  auto row_count = GetDataNum(ts_engine_src_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ASSERT_EQ(row_count, 5 * partition_num);

  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  entity_ids.clear();
  for(auto vg : *(ts_engine_desc_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  // scan table ,check if data is correct in table 1008.
  ctx_->ts_engine = ts_engine_desc_;
  row_count = GetDataNum(ts_engine_desc_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(row_count, 5 * partition_num);
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007, at least 5 * partitions datas. rollback at last.
TEST_F(TestEngineSnapshotImgrate, InsertPartitionsRollback) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1005;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1007
  int partition_num = 5;
  const int64_t interval = 3600 * 24 * 10;
  for (size_t i = 0; i < partition_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1, 12345 + i * interval, 5);
  }
  std::vector<EntityResultIndex> entity_ids;
  for(auto vg : *(ts_engine_src_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  auto row_count = GetDataNum(ts_engine_src_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ASSERT_EQ(row_count, 5 * partition_num);

  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotRollback(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  entity_ids.clear();
  for(auto vg : *(ts_engine_desc_->GetTsVGroups())) {
    if (vg->GetMaxEntityID() > 0) {
      entity_ids.push_back(EntityResultIndex(1, 1, vg->GetVGroupID()));
      break;
    }
  }
  ASSERT_EQ(1, entity_ids.size());
  // scan table ,check if data is correct in table 1008.
  ctx_->ts_engine = ts_engine_desc_;
  row_count = GetDataNum(ts_engine_desc_, cur_table_id, entity_ids[0], {INT64_MIN, INT64_MAX});
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(row_count, 0);
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngineSnapshotImgrate, ConvertManyDataDiffEntitiesFaild1) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1006;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  // input data to  table 1007
  int entity_num = 5;
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1 + i, 12345 + i * 1000, 5);
  }
  uint64_t row_num;
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 5 * entity_num);
  s = ts_table->DeleteTotalRange(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 0);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  entity_store.clear();
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);
  
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008, dest table entity has some data already.
TEST_F(TestEngineSnapshotImgrate, ConvertManyDataSameEntityDestNoEmpty) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  // input data to  table 1007
  int entity_num = 5;
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1 + i, 12345 + i * 1000, 5);
  }
  uint64_t row_num;
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 5 * entity_num);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_desc_, cur_table_id, 1 + i, 123456789 + i * 1000, 5);
  }
   uint64_t count;
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num * 5);

  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num * 5);
  
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008, dest table entity has some data already. migrate three times.
TEST_F(TestEngineSnapshotImgrate, DestNoEmptyThreeTimes) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  // input data to  table 1007
  int entity_num = 5;
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1 + i, 12345 + i * 1000, 5);
  }
  uint64_t row_num;
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 5 * entity_num);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_desc_, cur_table_id, 1 + i, 123456789 + i * 1000, 5);
  }
  uint64_t count;
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num * 5);

  for (size_t i = 0; i < 3; i++) {
    uint64_t desc_snapshot_id;
    s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t snapshot_id;
    s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);

    // migrate data from 1007 to 1008
    TSSlice snapshot_data{nullptr, 0};
    do {
      s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (snapshot_data.data != nullptr) {
        s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
        ASSERT_EQ(s, KStatus::SUCCESS);
        free(snapshot_data.data);
      }
    } while (snapshot_data.len > 0);
    s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num * 5);
  
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008, dest table entity has some data .rollback
TEST_F(TestEngineSnapshotImgrate, ConvertManyDataSameEntityDestNoEmptyRollback) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  // input data to  table 1007
  int entity_num = 5;
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1 + i, 12345 + i * 1000, 5);
  }
  uint64_t row_num;
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 5 * entity_num);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_desc_, cur_table_id, 1 + i, 123456789 + i * 1000, 5);
  }
  uint64_t count;
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num * 5);

  for (size_t i = 0; i < 3; i++) {
    uint64_t desc_snapshot_id;
    s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t snapshot_id;
    s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);

    // migrate data from 1007 to 1008
    TSSlice snapshot_data{nullptr, 0};
    do {
      s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (snapshot_data.data != nullptr) {
        s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
        ASSERT_EQ(s, KStatus::SUCCESS);
        free(snapshot_data.data);
      }
    } while (snapshot_data.len > 0);
    s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_desc_->WriteSnapshotRollback(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  
  ctx_->ts_engine = ts_engine_desc_;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  entity_store.clear();
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ctx_->ts_engine = ts_engine_src_;
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), entity_num);
  
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// multi-snapshot
TEST_F(TestEngineSnapshotImgrate, mulitSnapshot) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  int entity_num = 5;  // set larger to 50000
  int thread_num = 0;  // set larger to 10
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, 1 + i, 12345, 500);
  }
  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([&](int idx){
      KwTsSpan ts_span;
      ts_span.begin = 12345 + i * (500 * 1000) / thread_num;
      ts_span.begin = 12345 + (i + i) * (500 * 1000) / thread_num;
      uint64_t desc_snapshot_id;
      s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, ts_span, &desc_snapshot_id);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint64_t snapshot_id;
      s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, ts_span, &snapshot_id);
      ASSERT_EQ(s, KStatus::SUCCESS);

      // migrate data from 1007 to 1008
      TSSlice snapshot_data{nullptr, 0};
      do {
        s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
        ASSERT_EQ(s, KStatus::SUCCESS);
        if (snapshot_data.data != nullptr) {
          s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
          ASSERT_EQ(s, KStatus::SUCCESS);
          free(snapshot_data.data);
        }
      } while (snapshot_data.len > 0);
      s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
      ASSERT_EQ(s, KStatus::SUCCESS);
      s = ts_engine_desc_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
      ASSERT_EQ(s, KStatus::SUCCESS);
      s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }, i));
  }
  for (size_t i = 0; i < thread_num; i++) {
    threads[i].join();
  }
  
  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
