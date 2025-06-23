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
#include <cstring>

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT The current directory is the storage directory for the big table

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT database path
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{101, 0};

class TestPayload : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestPayload() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestPayload() {
    // CLOSE engine
    EXPECT_EQ(TSEngineImpl::CloseTSEngine(ctx_, ts_engine_), KStatus::SUCCESS);
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  // Store in header
  int row_num_ = 5;
};

TEST_F(TestPayload, insert) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);

  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, insertNull) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenPayloadDataWithNull(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);

  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, columnInsert) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);

  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, columnInsertNull) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenPayloadDataWithNull(ctx_, 100, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts, start_ts + 1000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_null = false;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
//  ASSERT_EQ(count, 100);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(*(static_cast<k_int16*>(static_cast<void*>(res.data[1][0]->mem))), 11);
  ASSERT_EQ(res.data[0][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(!is_null);
  ASSERT_EQ(res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(!is_null);
  ASSERT_EQ(res.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  delete iter;
  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, varColumnInsert) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructVarRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2, 4};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  string test_str1 = "abcdefgh";
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(memcmp(res.data[1][0]->mem, test_str1.c_str(), test_str1.size()), 0);

  string test_str2 = "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < row_num_; ++i) {
    // Verify the third column data varchar
    void* data1 = (res.data[2][0])->getVarColData(i);
    ASSERT_EQ(memcmp(data1, test_str2.c_str(), test_str2.size()), 0);
    ASSERT_EQ((res.data[2][0])->getVarColDataLen(i), 27);
    // Verify the varbinary data in the fifth column, with a subscript of 3 in scan_cols
    void* data2 = (res.data[3][0])->getVarColData(i);
    ASSERT_EQ(memcmp(data2, test_str2.c_str(), test_str2.size()), 0);
    ASSERT_EQ((res.data[3][0])->getVarColDataLen(i), 26);
  }
  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, varColumnInsert1) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructVarRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  KTimestamp get_start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  for (int i = 0; i < 10; i++) {
    KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts, &meta);

    // insert
    // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
    std::shared_ptr<TsEntityGroup> tbl_range;
    s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    TSSlice payload{data_value, p_len};
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
    ASSERT_EQ(s, KStatus::SUCCESS);

    delete[] data_value;
  }
  // Verify the correctness of written data
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  KwTsSpan ts_span = {get_start_ts, get_start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  string test_str = "abcdefgh";
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 10);
  char check_data[8];
  memcpy(check_data, res.data[1][0]->mem, 8);
  ASSERT_EQ(strncmp(check_data, test_str.c_str(), sizeof(check_data)), 0);
  delete iter;

  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, varColumnInsertNull) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructVarRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenPayloadDataWithNull(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2, 4};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));

  string test_str2 = "abcdefghijklmnopqrstuvwxyz";
  bool is_null;
  for (int i = 0; i < row_num_; ++i) {
    // Verify the third column data varchar
    if (i % 2 == 0) {
      ASSERT_EQ(res.data[2][0]->isNull(i, &is_null), KStatus::SUCCESS);
      ASSERT_TRUE(is_null);
      // Verify the varbinary data in the fifth column, with a subscript of 3 in scan_cols
      ASSERT_EQ(res.data[3][0]->isNull(i, &is_null), KStatus::SUCCESS);
      ASSERT_TRUE(is_null);
      continue;
    }
    // Verify the third column data varchar
    void* data1 = (res.data[2][0])->getVarColData(i);
    ASSERT_EQ(memcmp(data1, test_str2.c_str(), test_str2.size()), 0);
    ASSERT_EQ((res.data[2][0])->getVarColDataLen(i), 27);
    // Verify the varbinary data in the fifth column, with a subscript of 3 in scan_cols
    void* data2 = (res.data[3][0])->getVarColData(i);
    ASSERT_EQ(memcmp(data2, test_str2.c_str(), test_str2.size()), 0);
    ASSERT_EQ((res.data[3][0])->getVarColDataLen(i), 26);
  }
  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Test timestamp writing
TEST_F(TestPayload, columnInsertTimestamp) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = 1;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Verify the correctness of written data
  KwTsSpan ts_span = {start_ts, start_ts + 10000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);

  delete iter;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Multi entity concurrent testing
TEST_F(TestPayload, multityVarColumnInsert1) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructVarRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  KTimestamp start_ts_iter = 1;
  // Payload write data count
  int insert_count = 10;
  // Number of threads
  int thread_num = 10;
  std::thread threads[10];
//  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
//      (std::chrono::system_clock::now().time_since_epoch()).count();
  std::atomic<int> add_t = 1;
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&](int n) {
      auto add_1 = add_t.fetch_add(1);
      KTimestamp start_ts = add_1 * 10000;
      k_uint32 p_len = 0;
      CreateLocalContext();
      char* data_value = GenSomePayloadData(ctx, insert_count, p_len, start_ts, &meta, 10, 0, false);

      // insert
      // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
      std::shared_ptr<TsTable> ts_table;
      s = ts_engine_->GetTsTable(ctx, cur_table_id, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
      std::shared_ptr<TsEntityGroup> tbl_range;
      s = ts_table->GetEntityGroup(ctx, kTestRange.range_group_id, &tbl_range);
      ASSERT_EQ(s, KStatus::SUCCESS);
      TSSlice payload{data_value, p_len};
      s = tbl_range->PutData(ctx, payload);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }, 0);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify the correctness of written data
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts_iter, start_ts_iter + 10000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (int i = 1; i <= thread_num; i++) {
    entity_id = i;
    ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
              KStatus::SUCCESS);
    k_uint32 total = 0;
    k_uint32 count = 0;
    bool is_finished = false;
    do {
      ResultSet res1{(k_uint32) scan_cols.size()};
      count = 0;
      ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
      total += count;
    } while (!is_finished);
    ASSERT_EQ(total, 10);
    delete iter;
  }

  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Concurrent testing within the entity
TEST_F(TestPayload, multityColumnInsert) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  // Payload write data count
  int insert_count = 10;
  // Number of threads
  int thread_num = 10;
  std::thread threads[10];
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&, i](int n) {
      KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
          (std::chrono::system_clock::now().time_since_epoch()).count();
      k_uint32 p_len = 0;
      CreateLocalContext();
      char* data_value = GenSomePayloadData(ctx, insert_count, p_len, start_ts + i * 10000, &meta);

      // insert
      // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
      std::shared_ptr<TsTable> ts_table;
      s = ts_engine_->GetTsTable(ctx, cur_table_id, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
      std::shared_ptr<TsEntityGroup> tbl_range;
      s = ts_table->GetEntityGroup(ctx, kTestRange.range_group_id, &tbl_range);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      TSSlice payload{data_value, p_len};
      DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
      s = tbl_range->PutData(ctx, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }, 0);
  }

  for (auto &thread : threads) {
    thread.join();
  }
  KTimestamp start_ts_iter = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  // Verify the correctness of written data
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {0, start_ts_iter + 10000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  k_uint32 total = 0;
  k_uint32 count = 0;
  bool is_finished = false;
  do {
    ResultSet res1{(k_uint32) scan_cols.size()};
    count = 0;
    ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    total += count;
  } while (!is_finished);
  ASSERT_EQ(total, insert_count * thread_num);

  delete iter;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Multiple entity concurrent testing
TEST_F(TestPayload, multityColumnInsert1) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  // Payload write data count
  int insert_count = 10;
  // Number of threads
  int thread_num = 10;
  std::thread threads[10];
//  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
//      (std::chrono::system_clock::now().time_since_epoch()).count();
  std::atomic<int> add_t = 1;
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&](int n) {
      auto add_1 = add_t.fetch_add(1);
      KTimestamp start_ts = add_1 * 10000;
      k_uint32 p_len = 0;
      CreateLocalContext();
      char* data_value = GenSomePayloadData(ctx, insert_count, p_len, start_ts, &meta, 10, 0, false);

      // insert
      // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
      std::shared_ptr<TsTable> ts_table;
      s = ts_engine_->GetTsTable(ctx, cur_table_id, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
      std::shared_ptr<TsEntityGroup> tbl_range;
      s = ts_table->GetEntityGroup(ctx, kTestRange.range_group_id, &tbl_range);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      TSSlice payload{data_value, p_len};
      DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
      s = tbl_range->PutData(ctx, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }, 0);
  }

  for (auto &thread : threads) {
    thread.join();
  }
  KTimestamp start_ts_iter = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  // Verify the correctness of written data
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {0, start_ts_iter + 10000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  for (int i = 1; i <= thread_num; i++) {
    entity_id = i;
    ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
              KStatus::SUCCESS);
    k_uint32 total = 0;
    k_uint32 count = 0;
    bool is_finished = false;
    do {
      ResultSet res1{(k_uint32) scan_cols.size()};
      count = 0;
      ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
      total += count;
    } while (!is_finished);
//    ASSERT_EQ(total, 10);
    delete iter;
  }

  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Concurrent testing within the entity
TEST_F(TestPayload, multityVarColumnInsert) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructVarRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  // Payload write data count
  int insert_count = 100;
  // Number of threads
  int thread_num = 10;
  std::thread threads[10];
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&, i](int n) {
      KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
          (std::chrono::system_clock::now().time_since_epoch()).count();
      k_uint32 p_len = 0;
      CreateLocalContext();
      char* data_value = GenSomePayloadData(ctx, insert_count, p_len, start_ts + i * 10000, &meta);

      // insert
      // INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
      std::shared_ptr<TsTable> ts_table;
      s = ts_engine_->GetTsTable(ctx, cur_table_id, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
      std::shared_ptr<TsEntityGroup> tbl_range;
      s = ts_table->GetEntityGroup(ctx, kTestRange.range_group_id, &tbl_range);
      ASSERT_EQ(s, KStatus::SUCCESS);
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      TSSlice payload{data_value, p_len};
      DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
      s = tbl_range->PutData(ctx, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, kwdbts::DedupRule::KEEP);
      ASSERT_EQ(s, KStatus::SUCCESS);
      delete[] data_value;
    }, 0);
  }

  for (auto &thread : threads) {
    thread.join();
  }
  KTimestamp start_ts_iter = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  // Verify the correctness of written data
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {0, start_ts_iter + 10000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  k_uint32 total = 0;
  k_uint32 count = 0;
  bool is_finished = false;
  do {
    ResultSet res1{(k_uint32) scan_cols.size()};
    count = 0;
    ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    total += count;
  } while (!is_finished);
  ASSERT_EQ(total, insert_count * thread_num);


  delete iter;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestPayload, partitionRowsInsert) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  int insert_num = 1000;
  int block_num = 11;
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  for (int i = 0; i < block_num; i++) {
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, insert_num, p_len, start_ts, &meta);
    s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
    ASSERT_EQ(s, KStatus::SUCCESS);
    TSSlice payload{data_value, p_len};
    s = tbl_range->PutData(ctx_, payload);
    ASSERT_EQ(s, KStatus::SUCCESS);
    start_ts += 100000;
    delete[] data_value;
  }

  KTimestamp end_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  // Verify the correctness of written data
  KwTsSpan ts_span = {0, end_ts + 10000000};
  ts_span = ConvertMsToPrecision(ts_span, ts_table->GetRootTableManager()->GetTsColDataType());
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count = 0;
  k_uint32 total = 0;
  bool is_finished = false;
  do {
    ResultSet res1{(k_uint32) scan_cols.size()};
    count = 0;
    ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    total += count;
  } while (!is_finished);
  ASSERT_EQ(total, insert_num * block_num);
  delete iter;

  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}