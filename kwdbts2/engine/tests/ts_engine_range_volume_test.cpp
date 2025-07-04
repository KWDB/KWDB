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

using namespace kwdbts;  // NOLINT

std::string db_path = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = db_path;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 86400;

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

class TestEngineRangeVolume : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;

  TestEngineRangeVolume() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = db_path;

    system(("rm -rf " + db_path + "/*").c_str());
    // clear path files.
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, db_path, opts_, &ts_engine_), KStatus::SUCCESS);
  }

  ~TestEngineRangeVolume() {
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
    system(("rm -rf " + db_path + "/*").c_str());
  }
  int row_num = 5;
};

// insert only 5 rows. table has only one timestamp64 columns
TEST_F(TestEngineRangeVolume, OneColumn) {
 int row_num = 5;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
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
  delete []data_value;

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * 16);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert only many rows. table only has timestamp64 + short columns
TEST_F(TestEngineRangeVolume, TwoColumns) {
  int row_num = 2374;
  int entities_num = 3;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 2);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  const KTimestamp start_ts = iot_interval_ * 10;
  k_uint32 p_len = 0;
  for (size_t i = 0; i < entities_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i, &meta, 10, 0, false);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete []data_value;
  }
  // total data volume
  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * entities_num * 18);

  // one entity data volume
  uint64_t primary_key = start_ts;
  uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
  s = ts_table->GetDataVolume(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * 18);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert 6 partitions
TEST_F(TestEngineRangeVolume, OneColumnHalf) {
  int row_num = 5;
  int paritition_num = 6;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  k_uint32 p_len = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i * iot_interval_ * 1000, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete []data_value;
  }

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * paritition_num * 16);

  uint64_t primary_key = 100;
  uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
  timestamp64 half_ts;
  s = ts_table->GetDataVolumeHalfTS(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &half_ts);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto start_ts_precision = convertMSToPrecisionTS(start_ts + paritition_num / 2 * iot_interval_ * 1000,
                                                  ts_table->GetRootTableManager()->GetTsColDataType());
  ASSERT_EQ(half_ts, start_ts_precision);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert 7 partitions
TEST_F(TestEngineRangeVolume, OneColumnHalfDiffPartition) {
  int paritition_num = 7;
  vector<int> row_nums_per_partition;
  int last_rows = 8;
  int total_rows = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    row_nums_per_partition.push_back(last_rows);
    total_rows += last_rows;
    last_rows *= 2;
  }

  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  k_uint32 p_len = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_nums_per_partition[i], p_len, start_ts + i * iot_interval_ * 1000, &meta);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete []data_value;
  }

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, total_rows * 16);

  uint64_t primary_key = 100;
  uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
  timestamp64 half_ts;
  s = ts_table->GetDataVolumeHalfTS(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &half_ts);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto start_ts_precision = convertMSToPrecisionTS(start_ts + (paritition_num - 1) * iot_interval_ * 1000,
                                                  ts_table->GetRootTableManager()->GetTsColDataType());
  ASSERT_EQ(half_ts, start_ts_precision);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// row type payload ,table has fixed size column
TEST_F(TestEngineRangeVolume, RowBasedPayloadFixedType) {
  int row_num = 5;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 3);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice row_based_payload = GenSomePayloadDataRowBased(ctx_, row_num, start_ts, &meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload;
  s = ts_table->ConvertRowTypePayload(ctx_, row_based_payload, &payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<AttributeInfo> data_schema;
  ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
  Payload pl(data_schema, {}, payload);
  CheckgenSomePayloadData(ctx_, &pl, start_ts, data_schema);

  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(payload.data);
  delete []row_based_payload.data;

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * 22);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// row type payload ,table has var-type column
TEST_F(TestEngineRangeVolume, RowBasedPayloadVarType) {
  int row_num = 5;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 4);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice row_based_payload = GenSomePayloadDataRowBased(ctx_, row_num, start_ts, &meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload;
  s = ts_table->ConvertRowTypePayload(ctx_, row_based_payload, &payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<AttributeInfo> data_schema;
  ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
  Payload pl(data_schema, {}, payload);
  CheckgenSomePayloadData(ctx_, &pl, start_ts, data_schema);

  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(payload.data);
  delete []row_based_payload.data;

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, row_num * 29);
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// row type payload ,table has var-type column
TEST_F(TestEngineRangeVolume, RowBasedPayloadManyCols) {
  int row_num = 5;
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

  TSSlice row_based_payload = GenSomePayloadDataRowBased(ctx_, row_num, start_ts, &meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload;
  s = ts_table->ConvertRowTypePayload(ctx_, row_based_payload, &payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<AttributeInfo> data_schema;
  ts_table->GetDataSchemaExcludeDropped(ctx_, &data_schema);
  Payload pl(data_schema, {}, payload);
  CheckgenSomePayloadData(ctx_, &pl, start_ts, data_schema);

  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  free(payload.data);
  delete []row_based_payload.data;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert one partition
TEST_F(TestEngineRangeVolume, OneColumnHalfOnePartition) {
  int total_rows = 57;
  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, total_rows, p_len, start_ts, &meta);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete []data_value;


  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, total_rows * 16);

  uint64_t primary_key = 100;
  uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
  timestamp64 half_ts;
  s = ts_table->GetDataVolumeHalfTS(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &half_ts);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(half_ts, convertMSToPrecisionTS(start_ts + (0 + total_rows - 1) / 2 * 10,
              ts_table->GetRootTableManager()->GetTsColDataType()));
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}


// insert 3 partitions
TEST_F(TestEngineRangeVolume, ThreePartitionsHalfTs) {
  vector<int> row_nums_per_partition{3, 51, 21};
  int paritition_num = row_nums_per_partition.size();
  int total_rows = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    total_rows += row_nums_per_partition[i];
  }

  const KTimestamp start_ts = iot_interval_ * 1000;
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  k_uint32 p_len = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_nums_per_partition[i], p_len, start_ts + i * iot_interval_ * 1000, &meta, 10, 0, false);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete []data_value;
  }

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, total_rows * 16);

  // first partition entity start_ts
  for (size_t i = 0; i < row_nums_per_partition.size(); i++) {
    uint64_t primary_key = start_ts + i * iot_interval_ * 1000;
    uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
    timestamp64 half_ts;
    s = ts_table->GetDataVolumeHalfTS(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &half_ts);
    ASSERT_EQ(s, KStatus::SUCCESS);
    auto start_ts_precision = convertMSToPrecisionTS(start_ts + i * iot_interval_ * 1000 + row_nums_per_partition[i] / 2 * 10,
                                                  ts_table->GetRootTableManager()->GetTsColDataType());
    ASSERT_EQ(half_ts, start_ts_precision);
  }

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// insert 3 partitions
TEST_F(TestEngineRangeVolume, ThreePartitionsHalfTsSameHash) {
  vector<int> row_nums_per_partition{3, 51, 21};
  vector<int> partition_index;
  int paritition_num = row_nums_per_partition.size();
  const KTimestamp start_ts = iot_interval_ * 1000;
  int total_rows = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    total_rows += row_nums_per_partition[i];
  }

  // entity with same hash value.
  int i = 0;
  int hash_index = 0;
  uint64_t tag_hash = 0;
  while (hash_index < 3) {
    uint64_t primary_key = start_ts + i * iot_interval_ * 1000;
    uint64_t cur_tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
    if (hash_index == 0) {
      tag_hash = cur_tag_hash;
      hash_index++;
      partition_index.push_back(i);
    } else {
      if (cur_tag_hash == tag_hash) {
        hash_index++;
        partition_index.push_back(i);
      }
    }
    i++;
  }  

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testSnapshot", cur_table_id, iot_interval_, 1);
  std::vector<RangeGroup> ranges{test_range};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, test_range.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // input data to  table 1007
  k_uint32 p_len = 0;
  for (size_t i = 0; i < paritition_num; i++) {
    char* data_value = GenSomePayloadData(ctx_, row_nums_per_partition[i], p_len,
                        start_ts + partition_index[i] * iot_interval_ * 1000, &meta, 10, 0, false);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    delete []data_value;
  }

  uint64_t volume_range;
  s = ts_table->GetDataVolume(ctx_, 0, UINT64_MAX, {INT64_MIN, INT64_MAX}, &volume_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(volume_range, total_rows * 16);

  // three entity with same hash.
  for (size_t i = 0; i < row_nums_per_partition.size(); i++) {
    uint64_t primary_key = start_ts + partition_index[i] * iot_interval_ * 1000;
    uint64_t tag_hash = GetConsistentHashId((char*)(&primary_key), 8, g_testcase_hash_num);
    timestamp64 half_ts;
    s = ts_table->GetDataVolumeHalfTS(ctx_, tag_hash, tag_hash, {INT64_MIN, INT64_MAX}, &half_ts);
    ASSERT_EQ(s, KStatus::SUCCESS);
    auto start_ts_precision = convertMSToPrecisionTS(start_ts + partition_index[1] * iot_interval_ * 1000 + row_nums_per_partition[1] / 2 * 10,
                                                  ts_table->GetRootTableManager()->GetTsColDataType());
    ASSERT_EQ(half_ts, start_ts_precision);
  }

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}
