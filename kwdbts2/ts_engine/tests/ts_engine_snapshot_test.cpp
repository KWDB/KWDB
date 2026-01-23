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
  TSEngineImpl* ts_engine_src_;
  TSEngineImpl* ts_engine_desc_;

  virtual void SetUp() override {
    ctx_ = &context_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
    DropTableManager::getInstance().clearAllDroppedTables();
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
    auto engine = new TSEngineImpl(opts_);
    auto s = engine->Init(ctx_);
    if (s != KStatus::SUCCESS) {
      std::cout << "engine init failed." << std::endl;
      exit(1);
    }
    ts_engine_src_ = engine;

    opts_.db_path = db_path + "/descdb/";
    engine = new TSEngineImpl(opts_);
    s = engine->Init(ctx_);
    MakeDirectory(opts_.db_path + "/temp_db_");
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
  
  void InsertData(TSEngineImpl* ts_e, TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts, int num, KTimestamp interval = 1000, TS_OSN osn = 10) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = ts_e->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, dev_id, num, start_ts);
    TsRawPayload::SetHashPoint(pay_load, 2);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = ts_e->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    EXPECT_EQ(s , KStatus::SUCCESS);
    free(pay_load.data);
  }
  void UpdateTag(TSEngineImpl* ts_e, TSTableID table_id, TSEntityID dev_id, TS_OSN osn) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = ts_e->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, dev_id, 0, 0);
    TsRawPayload::SetHashPoint(pay_load, 2);
    TsRawPayload::SetOSN(pay_load, osn);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = ts_e->PutEntity(ctx_, table_id, 1, &pay_load, 1, 0, is_dropped);
    EXPECT_EQ(s , KStatus::SUCCESS);
    free(pay_load.data);
  }

  void CheckTagValue(std::vector<EntityResultIndex> &entity_id_list, ResultSet &rs, uint64_t count) {
    for (size_t i = 0; i < entity_id_list.size(); i++) {
      uint64_t e_id = entity_id_list[i].entityId;
      std::string e_id_str = intToString(e_id);
      EXPECT_EQ(KUint64((char*)rs.data[0][0]->mem + 216 * i), e_id);
      EXPECT_TRUE(0 == memcmp(rs.data[1][0]->getVarColData(i), e_id_str.data(), e_id_str.length()));
      EXPECT_TRUE(0 == memcmp((char*)rs.data[2][0]->mem  + i * 216, e_id_str.data(), e_id_str.length()));
      EXPECT_EQ(KUint64((char*)rs.data[3][0]->mem  + 1 + i * 9), e_id);
    }
  }

  uint64_t GetDataNum(TSEngineImpl* ts_e, TSTableID table_id, EntityResultIndex dev_id, KwTsSpan ts_span) {
    std::shared_ptr<TsTable> ts_table_dest;
    bool is_dropped = false;
    auto s = ts_e->GetTsTable(ctx_, table_id, ts_table_dest, is_dropped);
    EXPECT_EQ(s , KStatus::SUCCESS);
    auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_dest);
    uint64_t row_count = 0;
    std::vector<EntityResultIndex> devs{dev_id};
    ctx_->ts_engine = ts_e;
    ts_table_v2->GetEntityRowCount(ctx_, devs, {ts_span}, &row_count);
    return row_count;
  }
  std::string GetPrimaryKey(TSEngineImpl* ts_e, TSTableID table_id, TSEntityID dev_id) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = ts_e->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s , KStatus::SUCCESS);
    uint64_t pkey_len = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        pkey_len += tag_schema[i].m_size;
      }
    }
    char* mem = reinterpret_cast<char*>(malloc(pkey_len));
    memset(mem, 0, pkey_len);
    std::string dev_str = intToString(dev_id);
    size_t offset = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        if (tag_schema[i].m_data_type == DATATYPE::VARSTRING) {
          memcpy(mem + offset, dev_str.data(), dev_str.length());
        } else {
          memcpy(mem + offset, (char*)(&dev_id), tag_schema[i].m_size);
        }
        offset += tag_schema[i].m_size;
      }
    }
    auto ret = std::string{mem, pkey_len};
    free(mem);
    return ret;
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
      ASSERT_EQ(volume, 20 * row_num);
      timestamp64 half_ts;
      s = ts_table->GetDataVolumeHalfTS(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &half_ts);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ASSERT_EQ(half_ts, 12345 + (1000 * entity_rows * i) / 2);
    }
    s = ts_table->DeleteTotalRange(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 1, 1);
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
  bool is_dropped = false;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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
  bool is_dropped = false;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
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
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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
  bool is_dropped = false;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
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
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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

  // recover 
  delete ts_engine_desc_;
  ts_engine_desc_ = new TSEngineImpl(opts_);
  ts_engine_desc_->Init(ctx_);

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
  // create desc table 1008
  KStatus s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // create src table 1008
  s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;

  // input data to  table 1007
  int partition_num = 5;
  const int64_t interval = EngineOptions::iot_interval;
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

  std::string pkey_str = GetPrimaryKey(ts_engine_src_, cur_table_id, 1);
  uint64_t count1;
  s = ts_table->DeleteData(ctx_, 1, pkey_str, {{0, 14345}}, &count1, 0, 10000);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count1, 3);

  uint64_t snapshot_id;
  bool is_dropped = false;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t desc_snapshot_id;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
  } while (snapshot_data.len > 0);
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_desc_->GetTsTable(ctx_, cur_table_id, ts_table_dest, is_dropped);
  EXPECT_EQ(s , KStatus::SUCCESS);
  auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_dest);
  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, UINT64_MAX});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  delete iter;

  s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->WriteSnapshotRollback(ctx_, desc_snapshot_id, 0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  entity_id_list.clear();
  rs.clear();
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter;

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
  uint64_t pkey_start = 1;
  for (size_t i = 0; i < entity_num; i++) {
    InsertData(ts_engine_src_, cur_table_id, pkey_start + i, 12345 + i * 1000, 5);
  }
  uint64_t row_num;
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, 5 * entity_num);
  uint64_t del_count;
  auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table);
  std::vector<std::string> pkeys;
  std::string pkey_str = GetPrimaryKey(ts_engine_src_, cur_table_id, pkey_start);
  pkeys.push_back(pkey_str);
  s = ts_table_v2->DeleteEntities(ctx_, pkeys, &del_count, 0, 1000, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_table->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX},  &row_num);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(row_num, (entity_num - 1) * 5);
  std::string pkey_2 = GetPrimaryKey(ts_engine_src_, cur_table_id, 2);
  s = ts_table_v2->DeleteData(ctx_, 1, pkey_2, {{0, 12345678}}, &del_count, 0, 2000);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(del_count, 5);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 4);

  std::vector<k_uint32> scan_cols_t = {0, 1, 2, 3};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, UINT64_MAX});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(4);
  uint32_t count;
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols_t, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 5);
  CheckTagValue(entity_id_list, rs, count);
  delete iter;

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  bool is_dropped = false;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 4);

  std::shared_ptr<TsTable> ts_table_src;
  ctx_->ts_engine = ts_engine_src_;
  s = ts_engine_src_->GetTsTable(ctx_, cur_table_id, ts_table_src, is_dropped);
  EXPECT_EQ(s , KStatus::SUCCESS);
  s = ts_table_src->DeleteTotalRange(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, 1, 111);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_src);
  std::vector<k_uint32> scan_cols = {0};
  osn_spans.clear();
  osn_spans.push_back({0, UINT64_MAX});
 entity_id_list.clear();
  rs.clear();
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter;

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_desc_->GetTsTable(ctx_, cur_table_id, ts_table_dest, is_dropped);
  EXPECT_EQ(s , KStatus::SUCCESS);
  ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_dest);
  entity_id_list.clear();
  rs.clear();
  rs.setColumnNum(4);
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols_t, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, entity_num);
  ASSERT_EQ(entity_id_list.size(), entity_num);
  for (size_t i = 0; i < entity_num; i++) {
    auto cur_key = KUint64(entity_id_list[i].mem.get());
    ASSERT_TRUE(cur_key > 0 && cur_key <= 5);
    OperatorInfoOfRecord* op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[i].op_with_osn.get());
    if (cur_key == 1) {
      ASSERT_EQ(op_osn->osn, 1000);
      ASSERT_EQ(op_osn->type, OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
    } else {
      ASSERT_EQ(op_osn->osn, 10);
      ASSERT_EQ(op_osn->type, OperatorTypeOfRecord::OP_TYPE_INSERT);
    }
  }
  CheckTagValue(entity_id_list, rs, count);
  delete iter;

  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back({INT64_MIN, INT64_MAX});
  kwdbts::TsIterator *m_iter;
  uint32_t total = 0;
  osn_spans.clear();
  osn_spans.push_back({1000, 2000});
  rs.clear();
  rs.setColumnNum(4);
  s = ts_table_v2->GetMetricIteratorByOSN(ctx_, 1, scan_cols, entity_id_list, osn_spans, ts_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    rs.clear();
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (count > 0) {
      if (KUint64(rs.entity_index.mem.get()) == 1) {
        ASSERT_EQ(KUint64(rs.data[1][0]->mem), 1000);
        ASSERT_EQ(KUint8(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
      } else if (KUint64(rs.entity_index.mem.get()) == 2) {
        if (KUint8(rs.data[2][0]->mem) == OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE) {
          ASSERT_EQ(KUint64(rs.data[1][0]->mem), 2000);
          ASSERT_EQ(KUint8(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE);
          ASSERT_EQ(KUint64(rs.data[3][0]->mem), 0);
          ASSERT_EQ(KUint64((char*)(rs.data[3][0]->mem) + 8), 12345678);
        } else {
          ASSERT_EQ(KUint64(rs.data[1][0]->mem), 10);
          ASSERT_EQ(KUint8(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_INSERT);
          ASSERT_EQ(KUint64(rs.data[3][0]->mem), 1);
        }
      }
    }
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  6);
  delete m_iter;

  total = 0;
  osn_spans.clear();
  osn_spans.push_back({1000, 2000 - 1});
  rs.clear();
  s = ts_table_v2->GetMetricIteratorByOSN(ctx_, 1, scan_cols, entity_id_list, osn_spans, ts_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    rs.clear();
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (count > 0) {
      if (KUint64(rs.entity_index.mem.get()) == 1) {
        ASSERT_EQ(KUint64(rs.data[1][0]->mem), 1000);
        ASSERT_EQ(KUint8(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
      } else if (KUint64(rs.entity_index.mem.get()) == 2) {
        ASSERT_EQ(KUint64(rs.data[1][0]->mem), 10);
        ASSERT_EQ(KUint8(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_INSERT);
        ASSERT_EQ(KUint64(rs.data[3][0]->mem), 1);
      }
    }
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  5);
  delete m_iter;
  
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
  bool is_dropped = false;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetRangeRowCount(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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

  std::shared_ptr<TsTable> ts_table_src;
  s = ts_engine_src_->GetTsTable(ctx_, cur_table_id, ts_table_src, is_dropped);
  EXPECT_EQ(s , KStatus::SUCCESS);
  auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_src);
  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, UINT64_MAX});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count1;
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_id_list.size(), entity_num);
  ASSERT_EQ(count1, entity_num);
  delete iter;

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_desc_->GetTsTable(ctx_, cur_table_id, ts_table_dest, is_dropped);
  EXPECT_EQ(s , KStatus::SUCCESS);
  ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table_dest);
  std::vector<void*> pkeys;
  std::vector<std::string> tmps;
  for (size_t i = 0; i < entity_num; i++) {
    tmps.push_back(GetPrimaryKey(ts_engine_desc_, cur_table_id, 1 + i));
    pkeys.push_back(tmps.back().data());
  }
  osn_spans.clear();
  osn_spans.push_back({0, UINT64_MAX});
  entity_id_list.clear();
  count1 = 0;
  ResultSet res;
  res.setColumnNum(1);
  s = ts_table_v2->GetEntityIdListByOSN(ctx_, pkeys, osn_spans, scan_cols, {2}, &entity_id_list, &res, &count1, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_id_list.size(), entity_num);
  ASSERT_EQ(count1, entity_num);

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
  int entity_num = 3;
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
    bool is_dropped = false;
    s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t snapshot_id;
    s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);

    // migrate data from 1007 to 1008
    TSSlice snapshot_data{nullptr, 0};
    do {
      s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (snapshot_data.data != nullptr) {
        s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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
    bool is_dropped = false;
    s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t snapshot_id;
    s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);

    // migrate data from 1007 to 1008
    TSSlice snapshot_data{nullptr, 0};
    do {
      s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (snapshot_data.data != nullptr) {
        s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
        ASSERT_EQ(s, KStatus::SUCCESS);
        free(snapshot_data.data);
      }
    } while (snapshot_data.len > 0);
    s = ts_engine_src_->DeleteSnapshot(ctx_, snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_desc_->WriteSnapshotRollback(ctx_, desc_snapshot_id, 0);
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
  ASSERT_EQ(entity_store.size(), 0);
  
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
      bool is_dropped = false;
      s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, ts_span, &desc_snapshot_id, is_dropped, 100);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint64_t snapshot_id;
      s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, ts_span, &snapshot_id, is_dropped);
      ASSERT_EQ(s, KStatus::SUCCESS);

      // migrate data from 1007 to 1008
      TSSlice snapshot_data{nullptr, 0};
      do {
        s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
        ASSERT_EQ(s, KStatus::SUCCESS);
        if (snapshot_data.data != nullptr) {
          s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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


TEST_F(TestEngineSnapshotImgrate, ConvertUpdateEntities) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1006;
  ConstructRoachpbTable(&meta, cur_table_id);
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_src_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ctx_->ts_engine = ts_engine_src_;
  // input data to  table 1007
  uint64_t pkey_int = 1;
  std::vector<std::string> pkeys;
  std::string pkey_str = GetPrimaryKey(ts_engine_src_, cur_table_id, pkey_int);
  pkeys.push_back(pkey_str);
  uint64_t del_count;
  auto ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table);
  for (size_t i = 0; i < 3; i++) {
    InsertData(ts_engine_src_, cur_table_id, pkey_int, 123456, 5, 10086 + i * 10);
    UpdateTag(ts_engine_src_, cur_table_id, pkey_int, 10087 + i * 10);
    s = ts_table_v2->DeleteEntities(ctx_, pkeys, &del_count, 0, 10088 + i * 10, true);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ASSERT_EQ(del_count, 5);
  }

  InsertData(ts_engine_src_, cur_table_id, pkey_int, 123456, 5, 10186);
  UpdateTag(ts_engine_src_, cur_table_id, pkey_int, 10187);
  s = ts_table_v2->DeleteData(ctx_, 1, pkeys[0], {{0, 123456}}, &del_count, 0, 10188);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(del_count, 1);

  UpdateTag(ts_engine_src_, cur_table_id, pkey_int, 10189);
  std::vector<kwdbts::EntityResultIndex> entity_store;
  s = ts_table_v2->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 1);

  // create table 1008
  s = ts_engine_desc_->CreateTsTable(ctx_, cur_table_id, &meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  bool is_dropped = false;
  s = ts_engine_desc_->CreateSnapshotForWrite(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id, is_dropped, 100);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t snapshot_id;
  s = ts_engine_src_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  do {
    s = ts_engine_src_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data != nullptr) {
      s = ts_engine_desc_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data, is_dropped);
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

  ts_table_v2 = dynamic_pointer_cast<TsTableV2Impl>(ts_table);
  entity_store.clear();
  ctx_->ts_engine = ts_engine_desc_;
  s = ts_table_v2->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(entity_store.size(), 1);

  std::vector<k_uint32> scan_cols = {0};
  std::vector<KwOSNSpan> osn_spans;
  osn_spans.push_back({0, 10187});
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back({INT64_MIN, INT64_MAX});
  BaseEntityIterator *iter;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  ResultSet rs;
  rs.setColumnNum(1);
  uint32_t count;
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 4);
  std::vector<OperatorInfoOfRecord*> op_osns;
  for (size_t i = 0; i < count; i++) {
    op_osns.push_back(reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[i].op_with_osn.get()));
  }
  ASSERT_EQ(op_osns[0]->osn, 10088);
  ASSERT_EQ(op_osns[0]->type, OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  ASSERT_EQ(op_osns[1]->osn, 10098);
  ASSERT_EQ(op_osns[1]->type, OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  ASSERT_EQ(op_osns[2]->osn, 10108);
  ASSERT_EQ(op_osns[2]->type, OperatorTypeOfRecord::OP_TYPE_TAG_DELETE);
  ASSERT_EQ(op_osns[3]->osn, 0);
  ASSERT_EQ(op_osns[3]->type, OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED);
  delete iter;

  kwdbts::TsIterator *m_iter;
  uint32_t total = 0;
  rs.clear();
  rs.setColumnNum(4);
  s = ts_table_v2->GetMetricIteratorByOSN(ctx_, 1, scan_cols, entity_id_list, osn_spans, ts_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  7);  // delete 3 , metric data 4.
  ASSERT_EQ(rs.data[0][3]->count, 4);
  ASSERT_EQ(KUint64(rs.data[0][3]->mem), 124456);
  ASSERT_EQ(KUint64((char*)(rs.data[0][3]->mem) + 24), 127456);
  ASSERT_EQ(KUint64(rs.data[1][2]->mem), 10108);
  ASSERT_EQ(KUint64(rs.data[1][3]->mem), 10);
  ASSERT_EQ(KUint64((char*)(rs.data[1][3]->mem) + 24), 10);
  delete m_iter;

  osn_spans.clear();
  osn_spans.push_back({10187, 10188});
  entity_id_list.clear();
  rs.clear();
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_EQ(op_osn->osn, 0);
  ASSERT_EQ(op_osn->type, OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED);
  delete iter;
  iter = nullptr;

  total = 0;
  rs.clear();
  s = ts_table_v2->GetMetricIteratorByOSN(ctx_, 1, scan_cols, entity_id_list, osn_spans, ts_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  1);
  ASSERT_EQ(rs.data[1][0]->count, 1);
  ASSERT_EQ(KUint64(rs.data[1][0]->mem), 10188);
  ASSERT_EQ(KUint64(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE);
  ASSERT_EQ(KUint64(rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(rs.data[3][0]->mem) + 8), 123456);
  delete m_iter;

  osn_spans.clear();
  osn_spans.push_back({10187, 10189});
  entity_id_list.clear();
  rs.clear();
  s = ts_table_v2->GetTagIteratorByOSN(ctx_, 1, scan_cols, osn_spans, {2}, &iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = iter->Next(&entity_id_list, &rs, &count);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_id_list[0].op_with_osn.get());
  ASSERT_EQ(op_osn->osn, 10189);
  ASSERT_EQ(op_osn->type, OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE);
  delete iter;
  iter = nullptr;

  total = 0;
  rs.clear();
  s = ts_table_v2->GetMetricIteratorByOSN(ctx_, 1, scan_cols, entity_id_list, osn_spans, ts_spans, &m_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  do {
    m_iter->Next(&rs, &count);
    ASSERT_EQ(s, KStatus::SUCCESS);
    total += count;
  } while (count > 0);
  ASSERT_EQ(total,  2);
  ASSERT_EQ(KUint64(rs.data[1][0]->mem), 10188);
  ASSERT_EQ(KUint64(rs.data[1][1]->mem), 10189);
  ASSERT_EQ(KUint64(rs.data[2][0]->mem), OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE);
  ASSERT_EQ(KUint64(rs.data[2][1]->mem), OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE);
  ASSERT_EQ(KUint64(rs.data[3][0]->mem), 0);
  ASSERT_EQ(KUint64((char*)(rs.data[3][0]->mem) + 8), 123456);
  delete m_iter;

  s = ts_engine_src_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_desc_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
