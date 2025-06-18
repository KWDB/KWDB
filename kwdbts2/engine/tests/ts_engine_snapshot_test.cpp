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

#include "engine.h"
#include "test_util.h"
#include "st_config.h"

using namespace kwdbts;  // NOLINT

std::string db_path = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = db_path;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 86400;

extern bool g_go_start_service;

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

class TestEngineSnapshotConvert : public TestBigTableInstance, public ::testing::WithParamInterface<int> {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;

  TestEngineSnapshotConvert() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP = 3;
    CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT = 3;
    CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 100;
    opts_.wal_level = 0;
    opts_.db_path = db_path;
    system(("rm -rf " + db_path + "/*").c_str());
    // clear path files.
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, db_path, opts_, &ts_engine_), KStatus::SUCCESS);
    g_go_start_service = false;
  }

  ~TestEngineSnapshotConvert() {
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
    system(("rm -rf " + db_path + "/*").c_str());
  }
  int row_num = 5;
};

// snapshot data from table 1007 to table 1008 , only 0 rows.
TEST_P(TestEngineSnapshotConvert, CreateSnapshotAndInsertOtherEmpty) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1 = nullptr;
  SubGroupID group_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  auto ts_type = ts_table_dest->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
            scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::FAIL);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , only 5 rows.
TEST_P(TestEngineSnapshotConvert, CreateSnapshotAndInsertOther) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int row_num = 5;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  const KTimestamp start_ts = iot_interval_ * 10000;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  uint64_t snapshot_id = 0;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id = 0;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  while (true) {
    TSSlice snapshot_data{nullptr, 0};
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  k_uint32 entity_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table_dest->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type),
                      convertMSToPrecisionTS( start_ts + row_num * 10, ts_type)};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
          {}, scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , at least 5 * payload datas.
TEST_P(TestEngineSnapshotConvert, ConvertManyData) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int batch_num = 1532;
  int batch_count = snapshot_payload_rows_num / batch_num + 1;
  int partition_num = 3;
  const KTimestamp start_ts = iot_interval_ * 10000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    for (size_t i = 0; i < partition_num; i++) {
      for (size_t j = 0; j < batch_count; j++) {
        char* data_value = GenSomePayloadData(ctx_, batch_num, p_len,
                          start_ts + i * iot_interval_ * 1000 + j * batch_num * 10 , &meta);
        TSSlice payload{data_value, p_len};
        s = tbl_range->PutData(ctx_, payload);
        ASSERT_EQ(s, KStatus::SUCCESS);
        delete[] data_value;
      }
    }
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table_dest->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type),
                      convertMSToPrecisionTS(start_ts + (int64_t)(partition_num * iot_interval_ * 1000), ts_type)};
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
          scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  size_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  ASSERT_EQ(total_count, partition_num * batch_count * batch_num);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , at least 5 * partitions datas.
TEST_P(TestEngineSnapshotConvert, ConvertManyDataDiffEntities) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int batch_num = snapshot_payload_rows_num * 2 + 332;
  int partition_num = 3;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    for (size_t i = 0; i < partition_num; i++) {
      char* data_value = GenSomePayloadData(ctx_, batch_num, p_len,
                start_ts + i * iot_interval_ * 1000 + i * batch_num * 10 , &meta, 10, 0, false);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx_, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table_dest->GetRootTableManager()->GetTsColDataType();
  // scan table ,check if data is correct in table 1008.
  for (size_t i = 0; i < partition_num; i++) {
    k_uint32 entity_id = 1 + i;
    KwTsSpan ts_span =
      {convertMSToPrecisionTS(start_ts + (int64_t)(i * iot_interval_ * 1000), ts_type),
      convertMSToPrecisionTS(start_ts + (int64_t)((i + 1) * iot_interval_ * 1000), ts_type)};
    std::vector<k_uint32> scancols = {0, 1};
    std::vector<Sumfunctype> scanaggtypes;
    TsStorageIterator* iter1;
    ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
                scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
    ResultSet res(scancols.size());
    k_uint32 count;
    bool is_finished = false;
    size_t total_count = 0;
    while (true) {
      ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (is_finished) {
        break;
      }
      total_count += count;
    }
    EXPECT_EQ(total_count, batch_num);
    delete iter1;
  }
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , at least 5 * partitions datas. rollback at last.
TEST_P(TestEngineSnapshotConvert, ConvertManyDataDiffEntitiesFaild1) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);
  int batch_num = snapshot_payload_rows_num * 2 + 332;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    int partition_num = 3;
    for (size_t i = 0; i < partition_num; i++) {
      char* data_value = GenSomePayloadData(ctx_, batch_num, p_len,
                start_ts + i * iot_interval_ * 1000 + i * batch_num * 10 , &meta, 10, 0, false);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx_, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotRollback(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  auto ts_type = ts_table_dest->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scancols = {0, 1};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {1, 2, 3, 4, 5, 6}, {ts_span}, ts_type, scancols, scancols, {},
              scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  size_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  EXPECT_EQ(total_count, 0);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}


// snapshot data from table 1007 to table 1008, dest table entity has some data already.
TEST_P(TestEngineSnapshotConvert, ConvertManyDataSameEntityDestNoEmpty) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int batch_num = snapshot_payload_rows_num * 2 + 332;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  DATATYPE ts_type;
  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX,
                                        {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10, ts_type)}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX,
                                      {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10, ts_type)}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1008
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
      char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts + batch_num * 10, &meta);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx_, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
  }

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
    // scan table ,check if data is correct in table 1008.
  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scancols = {0, 1};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {1}, {ts_span}, ts_type, scancols, scancols, {},
              scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  size_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  EXPECT_EQ(total_count, 2 * batch_num);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}


// snapshot data from table 1007 to table 1008, dest table entity has some data already. migrate three times.
TEST_P(TestEngineSnapshotConvert, DestNoEmptyThreeTimes) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int batch_num = 602;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  DATATYPE ts_type;
  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }
  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1008
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
      char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts + batch_num * 10, &meta);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx_, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
  }
  for (int i = 0; i < 3; i++) {
    uint64_t snapshot_id;
    s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX,
                                          {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10 - 1, ts_type)}, &snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint64_t desc_snapshot_id;
    s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX,
                                        {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10 - 1, ts_type)}, &desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);

    // migrate data from 1007 to 1008
    TSSlice snapshot_data{nullptr, 0};
    while (true) {
      s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      if (snapshot_data.data == nullptr) {
        break;
      }
      s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
      ASSERT_EQ(s, KStatus::SUCCESS);
      free(snapshot_data.data);
    }
    s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
    s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + batch_num * 10 - 1, ts_type)};
  std::vector<k_uint32> scancols = {0, 1};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {1}, {ts_span}, ts_type, scancols, scancols, {},
              scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  size_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  EXPECT_EQ(total_count, batch_num);
  delete iter1;
  ts_span = {convertMSToPrecisionTS(start_ts + batch_num * 10, ts_type), INT64_MAX};
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {1}, {ts_span}, ts_type, scancols, scancols, {},
              scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  EXPECT_EQ(total_count, batch_num);
  delete iter1;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008, dest table entity has some data .rollback
TEST_P(TestEngineSnapshotConvert, ConvertManyDataSameEntityDestNoEmptyRollback) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int batch_num = snapshot_payload_rows_num * 2 + 332;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  DATATYPE ts_type;
  // input data to  table 1007
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX,
                                        {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10, ts_type)}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX,
                                      {INT64_MIN, convertMSToPrecisionTS(start_ts + batch_num * 10, ts_type)}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // input data to  table 1008
  {
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    k_uint32 p_len = 0;
      char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts + batch_num * 10 + 1, &meta);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx_, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
  }

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotRollback(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
    // scan table ,check if data is correct in table 1008.
  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scancols = {0, 1};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, 1, {1}, {ts_span}, ts_type, scancols, scancols, {},
            scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  size_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    total_count += count;
  }
  EXPECT_EQ(total_count, batch_num);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008
// desc node has not created table 1008 yet.
TEST_P(TestEngineSnapshotConvert, CreateSnapshotDescNoTable) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int row_num = 601;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  const KTimestamp start_ts = iot_interval_ * 10 * 1000;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  KTableKey desc_table_id = 1008;
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + row_num * 10, ts_type)};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
              scanaggtypes, 1, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  uint32_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    if (total_count == 0) {
      ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
    }
    total_count += count;
    res.clear();
  }
  ASSERT_EQ(total_count, row_num);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , only 5 rows.
// dest node has 1008 table, but schema version is not newest.
TEST_P(TestEngineSnapshotConvert, CreateSnapshotDestTableVersionLow) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int row_num = 5;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  const KTimestamp start_ts = iot_interval_ * 10;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  // add one column to table 1007
  roachpb::CreateTsTable meta_col;
  roachpb::KWDBKTSColumn* column = meta_col.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(13);
  column->set_name("column13");
  string err_msg;
  size_t col_size = column->ByteSizeLong();
  char* buffer = reinterpret_cast<char*>(malloc(col_size));
  column->SerializeToArray(buffer, col_size);
  TSSlice column_slice{buffer, col_size};
  ASSERT_EQ(column->ParseFromArray(column_slice.data, column_slice.len), true);
  string trans_id = "0000000000000001";
  s = ts_engine_->AddColumn(ctx_, cur_table_id, const_cast<char*>(trans_id.data()), column_slice, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(buffer);

  // delete one column to table 1007
  column = meta_col.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(11);
  column->set_name("column12");
  col_size = column->ByteSizeLong();
  buffer = reinterpret_cast<char*>(malloc(col_size));
  column->SerializeToArray(buffer, col_size);
  column_slice = {buffer, col_size};
  ASSERT_EQ(column->ParseFromArray(column_slice.data, column_slice.len), true);
  trans_id = "0000000000000001";
  s = ts_engine_->DropColumn(ctx_, cur_table_id, const_cast<char*>(trans_id.data()), column_slice, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(buffer);

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + row_num * 10, ts_type)};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
            scanaggtypes, 3, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  uint32_t total_count = 0;
  while (true) {
    ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    if (is_finished) {
      break;
    }
    if (total_count == 0) {
      ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
    }
    total_count += count;
    res.clear();
  }
  ASSERT_EQ(total_count, row_num);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// snapshot data from table 1007 to table 1008 , only 5 rows.
// dest node has 1008 table, but schema version is upper than 1007.
TEST_P(TestEngineSnapshotConvert, CreateSnapshotDestTableVersionHigh) {
  int type = GetParam();
  SnapshotFactory::TestSetType(type);

  int row_num = 5;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  const KTimestamp start_ts = iot_interval_ * 10;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // create table 1008
  roachpb::CreateTsTable meta_desc;
  KTableKey desc_table_id = 1008;
  ConstructRoachpbTable(&meta_desc, "destSnapshot", desc_table_id, iot_interval_, 12);
  s = ts_engine_->CreateTsTable(ctx_, desc_table_id, &meta_desc, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add one column to table 1007
  roachpb::CreateTsTable meta_col;
  roachpb::KWDBKTSColumn* column = meta_col.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(13);
  column->set_name("column13");
  string err_msg;
  size_t col_size = column->ByteSizeLong();
  char* buffer = reinterpret_cast<char*>(malloc(col_size));
  column->SerializeToArray(buffer, col_size);
  TSSlice column_slice{buffer, col_size};
  ASSERT_EQ(column->ParseFromArray(column_slice.data, column_slice.len), true);
  string trans_id = "0000000000000001";
  s = ts_engine_->AddColumn(ctx_, desc_table_id, const_cast<char*>(trans_id.data()), column_slice, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(buffer);

  // delete one column to table 1007
  column = meta_col.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(11);
  column->set_name("column12");
  col_size = column->ByteSizeLong();
  buffer = reinterpret_cast<char*>(malloc(col_size));
  column->SerializeToArray(buffer, col_size);
  column_slice = {buffer, col_size};
  ASSERT_EQ(column->ParseFromArray(column_slice.data, column_slice.len), true);
  trans_id = "0000000000000001";
  s = ts_engine_->DropColumn(ctx_, desc_table_id, const_cast<char*>(trans_id.data()), column_slice, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(buffer);

  uint64_t desc_snapshot_id;
  s = ts_engine_->CreateSnapshotForWrite(ctx_, desc_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // migrate data from 1007 to 1008
  TSSlice snapshot_data{nullptr, 0};
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    s = ts_engine_->WriteSnapshotBatchData(ctx_, desc_snapshot_id, snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    free(snapshot_data.data);
  }
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->WriteSnapshotSuccess(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DeleteSnapshot(ctx_, desc_snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // scan table ,check if data is correct in table 1008.
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + row_num * 10, ts_type)};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  std::shared_ptr<TsTable> ts_table_dest;
  s = ts_engine_->GetTsTable(ctx_, desc_table_id, ts_table_dest);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range_desc;
  s = ts_table_dest->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range_desc);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_range_desc->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols, {},
            scanaggtypes, 3, &iter1, tbl_range_desc, {}, false, false), KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter1;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->DropTsTable(ctx_, desc_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

INSTANTIATE_TEST_CASE_P(InstantiationType, TestEngineSnapshotConvert, ::testing::Values(1));
