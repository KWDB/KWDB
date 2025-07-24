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
#include "ts_table_snapshot.h"

using namespace kwdbts;  // NOLINT

std::string db_path = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = db_path;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 86400;

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

class TestEngineSnapshotTable : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;

  TestEngineSnapshotTable() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = db_path;

    system(("rm -rf " + db_path + "/*").c_str());
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, db_path, opts_, &ts_engine_), KStatus::SUCCESS);

    // using payload snapshot type
    SnapshotFactory::TestSetType(1);
  }

  ~TestEngineSnapshotTable() {
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
    system(("rm -rf " + db_path + "/*").c_str());
  }
  int row_num = 5;
};

// insert 5 rows and generate payload by snapshot.
TEST_F(TestEngineSnapshotTable, CreateSnapshot) {
  int row_num = 5;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

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

  // check payload is correct.
  roachpb::CreateTsTable get_meta;
  s = ts_engine_->GetMetaData(ctx_, cur_table_id, test_range, &get_meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<AttributeInfo> data_schema;
  ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
  std::vector<uint32_t> actual_cols;
  Payload pl(data_schema, actual_cols, payload);
  CheckgenSomePayloadData(ctx_, &pl, start_ts, data_schema);
  delete[] data_value;

  // scan table ,check if data is correct.
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + 5 * 10, ts_type)};
  std::vector<k_uint32> scancols = {0, 1, 2};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scancols, scancols, {},
                                   scanaggtypes, 1, &iter1, tbl_range, {}, false, false),KStatus::SUCCESS);
  ResultSet res(scancols.size());
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter1;

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice snapshot_data{nullptr, 0};
  s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto pl_header = SnapshotPayloadData::ParseData(snapshot_data);
  ASSERT_EQ(pl_header.sn, 1);
  ASSERT_EQ(pl_header.type, TsSnapshotDataType::STORAGE_SCHEMA);
  free(snapshot_data.data);
  snapshot_data.data = nullptr;
  snapshot_data.len = 0;

  s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
  ASSERT_EQ(s, KStatus::SUCCESS);
  pl_header = SnapshotPayloadData::ParseData(snapshot_data);
  ASSERT_EQ(pl_header.sn, 2);
  ASSERT_EQ(pl_header.type, TsSnapshotDataType::PAYLOAD_COL_BASED_DATA);
  ASSERT_GT(snapshot_data.len, 10);
  Payload pl_snapshot1(data_schema, actual_cols, pl_header.data_part);
  ASSERT_EQ(pl_snapshot1.GetRowCount(), 0);

  free(snapshot_data.data);
  snapshot_data.data = nullptr;
  snapshot_data.len = 0;

  s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
  ASSERT_EQ(s, KStatus::SUCCESS);
  pl_header = SnapshotPayloadData::ParseData(snapshot_data);
  ASSERT_EQ(pl_header.sn, 3);
  ASSERT_EQ(pl_header.type, TsSnapshotDataType::PAYLOAD_COL_BASED_DATA);
  ASSERT_GT(snapshot_data.len, 10);
  Payload pl_snapshot2(data_schema, actual_cols, pl_header.data_part);
  ASSERT_EQ(pl_snapshot2.GetRowCount(), row_num);
  CheckgenSomePayloadData(ctx_, &pl_snapshot2, start_ts, data_schema);

  free(snapshot_data.data);
  snapshot_data.data = nullptr;
  snapshot_data.len = 0;

  s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
  ASSERT_EQ(s, KStatus::SUCCESS);
  pl_header = SnapshotPayloadData::ParseData(snapshot_data);
  ASSERT_EQ(pl_header.sn, 4);
  ASSERT_EQ(pl_header.type, TsSnapshotDataType::STORAGE_SCHEMA);
  free(snapshot_data.data);
  snapshot_data.data = nullptr;
  snapshot_data.len = 0;

  s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(snapshot_data.len, 0);
  ASSERT_EQ(snapshot_data.data, nullptr);
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(snapshot_data.data);
  snapshot_data.data = nullptr;
  snapshot_data.len = 0;
}

// insert multi-partition rows and check payload generated by snapshot.
TEST_F(TestEngineSnapshotTable, CreateSnapshotWithPartitions) {
  int row_num = snapshot_payload_rows_num;
  int partition_num = 3;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  k_uint32 p_len = 0;
  for (size_t i = 0; i < partition_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i * iot_interval_ * 1000, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice snapshot_data{nullptr, 0};
  size_t total_rows = partition_num * row_num;
  size_t left_rows = total_rows;
  int times = 0;
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    auto p_data = SnapshotPayloadData::ParseData(snapshot_data);
    if (p_data.type != TsSnapshotDataType::PAYLOAD_COL_BASED_DATA) {
      free(snapshot_data.data);
      continue;
    }
    std::vector<AttributeInfo> data_schema;
    ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
    std::vector<uint32_t> actual_cols;
    Payload pl_snapshot(data_schema, actual_cols, p_data.data_part);
    if (0 == pl_snapshot.GetRowCount()) {
      free(snapshot_data.data);
      continue;
    }
    ASSERT_EQ(pl_snapshot.GetRowCount(), snapshot_payload_rows_num);
    CheckgenSomePayloadData(ctx_, &pl_snapshot, start_ts + times * iot_interval_ * 1000, data_schema);
    times++;
    free(snapshot_data.data);
    left_rows -= pl_snapshot.GetRowCount();
  }
  ASSERT_EQ(left_rows, 0);
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert one partiton ,with many rows.
TEST_F(TestEngineSnapshotTable, CreateSnapshotWithMuchRows) {
  int batch_num = 2345;
  int batch_times = 7;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  k_uint32 p_len = 0;
  for (size_t i = 0; i < batch_times; i++) {
    char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts + i * batch_num * 10, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice snapshot_data{nullptr, 0};
  size_t total_rows = batch_num * batch_times;
  size_t left_rows = total_rows;
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    auto p_data = SnapshotPayloadData::ParseData(snapshot_data);
    if (p_data.type != TsSnapshotDataType::PAYLOAD_COL_BASED_DATA) {
      free(snapshot_data.data);
      continue;
    }
    std::vector<AttributeInfo> data_schema;
    ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
    std::vector<uint32_t> actual_cols;
    Payload pl_snapshot(data_schema, actual_cols, p_data.data_part);
    if (0 == pl_snapshot.GetRowCount()) {
      free(snapshot_data.data);
      continue;
    }
    ASSERT_EQ(pl_snapshot.GetRowCount(), std::min(left_rows, snapshot_payload_rows_num));
    CheckgenSomePayloadData(ctx_, &pl_snapshot, start_ts + (total_rows - left_rows) * 10, data_schema);
    free(snapshot_data.data);
    left_rows -= pl_snapshot.GetRowCount();
  }
  ASSERT_EQ(left_rows, 0);
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert into many entities.
TEST_F(TestEngineSnapshotTable, ConsumeSnapshotPayload) {
  int batch_num = 2345;
  int batch_times = 7;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 12);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  k_uint32 p_len = 0;
  for (size_t i = 0; i < batch_times; i++) {
    char* data_value = GenSomePayloadData(ctx_, batch_num, p_len, start_ts + i * batch_num * 10, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete[] data_value;
  }

  uint64_t snapshot_id;
  s = ts_engine_->CreateSnapshotForRead(ctx_, cur_table_id, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice snapshot_data{nullptr, 0};
  size_t total_rows = batch_num * batch_times;
  size_t left_rows = total_rows;
  while (true) {
    s = ts_engine_->GetSnapshotNextBatchData(ctx_, snapshot_id, &snapshot_data);
    ASSERT_EQ(s, KStatus::SUCCESS);
    if (snapshot_data.data == nullptr) {
      break;
    }
    auto p_data = SnapshotPayloadData::ParseData(snapshot_data);
    if (p_data.type != TsSnapshotDataType::PAYLOAD_COL_BASED_DATA) {
      free(snapshot_data.data);
      continue;
    }
    std::vector<AttributeInfo> data_schema;
    ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
    std::vector<uint32_t> actual_cols;
    Payload pl_snapshot(data_schema, actual_cols, p_data.data_part);
    if (0 == pl_snapshot.GetRowCount()) {
      free(snapshot_data.data);
      continue;
    }
    ASSERT_EQ(pl_snapshot.GetRowCount(), std::min(left_rows, snapshot_payload_rows_num));
    CheckgenSomePayloadData(ctx_, &pl_snapshot, start_ts + (total_rows - left_rows) * 10, data_schema);
    free(snapshot_data.data);
    left_rows -= pl_snapshot.GetRowCount();
  }
  ASSERT_EQ(left_rows, 0);
  s = ts_engine_->DeleteSnapshot(ctx_, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
