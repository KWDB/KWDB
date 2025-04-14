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
#include "utils/compress_utils.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT The current directory is the storage directory for the big table

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT database path
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

extern bool g_engine_initialized;
extern bool g_go_start_service;

RangeGroup kTestRange{1, 0};

class TestEngine : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestEngine() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
    g_engine_initialized = true;
    g_go_start_service = false;
    g_compression.compression_type = kwdbts::CompressionType::GZIP;
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestEngine() {
    // CLOSE engine
    KStatus s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
    EXPECT_EQ(s, KStatus::SUCCESS);
    ts_engine_ = nullptr;
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

//  virtual void SetUp() {
//  }
//
//  virtual void TearDown() {
//  }

  bool CheckIterRows(TsStorageIterator* iter1, k_int32 expect_rows, k_uint32 scalcol_num) {
    ResultSet res{scalcol_num};
    k_uint32 ret_cnt;
    int total_rows = 0;
    bool is_finished = false;
    do {
      KStatus s = iter1->Next(&res, &ret_cnt, &is_finished);
      if (s != KStatus::SUCCESS) {
        return false;
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    return (total_rows == expect_rows);
  }

  // Store in header
  int row_num_ = 5;
};

// Verify the open and close functions
TEST_F(TestEngine, open) {
}

TEST_F(TestEngine, create) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::shared_ptr<TsTable> ts_table;
  auto ss = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(ss, KStatus::FAIL);

  std::vector<RangeGroup> ranges{kTestRange};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// Query non-existent tables
TEST_F(TestEngine, queryNoExist) {
  KTableKey cur_table_id = 123456789;
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  EXPECT_EQ(s, KStatus::FAIL);
  EXPECT_TRUE(ts_table == nullptr);
}

// Delete non-existent tables
TEST_F(TestEngine, dropNoExistTable) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  KStatus s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  // Modify the deletion logic. When the drop table fails, determine whether it returns success or failure at the
  // interface layer based on the returned ErrorInfo. If a non-existent table is dropped, the ErrorCode is equal to
  // BOENOOBJ, and success is returned
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// create the same tables
TEST_F(TestEngine, doubleCreateTable) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  EXPECT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  EXPECT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  EXPECT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTable> ts_table_1;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table_1);
  EXPECT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(ts_table_1, ts_table);
}

TEST_F(TestEngine, MultiUseTable) {
  KStatus s;
  int table_num = 5;
  KTableKey cur_table_id = 123456789;
  std::vector<RangeGroup> ranges{kTestRange};
  for (size_t i = 0; i < table_num; i++) {
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id + i);
    s = ts_engine_->CreateTsTable(ctx_, cur_table_id + i, &meta, ranges);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  std::vector<std::shared_ptr<TsTable>> tables;
  tables.resize(table_num);
  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, tables[i]);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->DropTsTable(ctx_, cur_table_id + i);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  for (size_t i = 0; i < table_num; i++) {
    std::shared_ptr<TsTable> table;
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, table);
    EXPECT_EQ(s, KStatus::FAIL);
  }
}

// Create different tables
TEST_F(TestEngine, CreateSomeTables) {
  KStatus s;
  int table_num = 10;
  KTableKey cur_table_id = 123456789;
  std::vector<RangeGroup> ranges{kTestRange};
  for (size_t i = 0; i < table_num; i++) {
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id + i);
    s = ts_engine_->CreateTsTable(ctx_, cur_table_id + i, &meta, ranges);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  std::shared_ptr<TsTable> ts_table;
  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_TRUE(ts_table->IsExist());
    EXPECT_EQ(ts_table->GetTableId(), cur_table_id + i);

    s = ts_engine_->DropTsTable(ctx_, cur_table_id + i);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, ts_table);
    EXPECT_EQ(s, KStatus::FAIL);
    EXPECT_TRUE(ts_table == nullptr);
  }
}

TEST_F(TestEngine, insert) {
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
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, insertNull) {
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
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, columnInsert) {
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
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, DeleteEntities) {
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
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }
  Payload pd(schema, actual_cols, payload);
  pd.SetLsn(123);
  pd.PrintMetric(std::cout);

  std::vector<string> primary_tags;
  primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
  // delete entities
  uint64_t del_cnt;
  s = tbl_range->DeleteEntities(ctx_, primary_tags, &del_cnt, 0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  // check result
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + row_num_ * 10, ts_type)};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types,
             1, &iter1, tbl_range, {}, false, false), KStatus::SUCCESS);

  ResultSet res{(k_uint32)scan_cols.size()};
  k_uint32 ret_cnt;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &ret_cnt, &is_finished), KStatus::SUCCESS);
  EXPECT_EQ(ret_cnt, 0);
  delete iter1;

  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  entity_id = 2;
  is_finished = false;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types,
              1, &iter1, tbl_range, {}, false, false), KStatus::SUCCESS);
  ASSERT_EQ(iter1->Next(&res, &ret_cnt, &is_finished), KStatus::SUCCESS);
  EXPECT_EQ(ret_cnt, row_num_);
  delete iter1;

  delete[] data_value;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, DeleteData) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, 1000, &meta);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload2, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }
  Payload pd(schema, actual_cols, payload);
  std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {1, convertMSToPrecisionTS(ts + row_num_ * 10, ts_type)};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types,
              1, &iter1, tbl_range, {}, false, false), KStatus::SUCCESS);
  uint64_t count = 0;
  uint64_t total = 0;
  bool is_finished = false;
  do {
    ResultSet res1{(k_uint32)scan_cols.size()};
    count = 0;
    ASSERT_EQ(iter1->Next(&res1, reinterpret_cast<k_uint32*>(&count), &is_finished), KStatus::SUCCESS);
    total += count;
  } while (!is_finished);
  ASSERT_EQ(total, row_num_ * 2);  // insert 10 rows

  // delete
  count = 0;
  KwTsSpan span{1, ts + 10};
  span = ConvertMsToPrecision(span, ts_type);
  s = tbl_range->DeleteData(ctx_, primary_tag, 0, {span}, nullptr, &count, 0, false);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num_);  // delete 7 rows

  // after delete
  count = 0;
  TsStorageIterator* iter2;
  ResultSet res2{(k_uint32)scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ASSERT_EQ(iter2->Next(&res2, reinterpret_cast<k_uint32*>(&count), &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_ - 2);  // left 3 rows

  delete[] data_value;
  delete[] data_value2;
  delete iter1;
  delete iter2;
  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, DeleteExpiredData) {
  // Increase the partition interval
  kwdbts::EngineOptions::iot_interval = 360000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  size_t block_item_max = 10;
  int partition_num = 2;
  size_t block_item_row_max = 1000;
  int write_count = block_item_max * block_item_row_max + 1;
  for (int i = 1; i <= partition_num; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  // delete expired data
  ASSERT_EQ(ts_table->DeleteExpiredData(ctx_, 2 * kwdbts::EngineOptions::iot_interval), KStatus::SUCCESS);

  KTimestamp start_ts = kwdbts::EngineOptions::iot_interval * 1000;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, (partition_num + 1) * start_ts};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false), KStatus::SUCCESS);

  k_uint32 count;
  ResultSet res{(k_uint32)scan_cols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, block_item_row_max);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(2 * kwdbts::EngineOptions::iot_interval * 1000, ts_type));

  delete iter;
  // delete expired data
  ASSERT_EQ(ts_table->DeleteExpiredData(ctx_, 2.5 * kwdbts::EngineOptions::iot_interval), KStatus::SUCCESS);

  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, block_item_row_max);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(2 * kwdbts::EngineOptions::iot_interval * 1000, ts_type));

  delete iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, CompressTsTable) {
  kwdbts::EngineOptions::iot_interval = 360000;
  roachpb::CreateTsTable meta;
  std::vector<string> primary_tags;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  size_t block_item_max = 10;
  int partition_num = 2;
  size_t block_item_row_max = 1000;
  int write_count = block_item_max * block_item_row_max + 1;
  for (int i = 1; i <= partition_num; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    if (i == 1) {
      Payload pd(schema, actual_cols, payload);
      primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
    }
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  // Compress the partition of 360000
  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  ASSERT_EQ(ts_table->Compress(ctx_, 2 * kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);
  // Due to not being fully written, it will not be truly compressed until the second schedule
  g_compression.compression_type = kwdbts::CompressionType::LZ4;
  ASSERT_EQ(ts_table->Compress(ctx_, 2 * kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);

  // Close the table and reopen it to verify the mount function
  tbl_range.reset();
  TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  kwdbts::EngineOptions::iot_interval = 360000;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  KTimestamp start_ts = kwdbts::EngineOptions::iot_interval * 1000;
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS((partition_num + 1) * start_ts, ts_type)};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, write_count * partition_num, scan_cols.size()));
  delete iter;

  // Actual uncompressed partition
  ASSERT_EQ(ts_table->Compress(ctx_, 2.5 * kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);
  // Data check
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, write_count * partition_num, scan_cols.size()));
  delete iter;

  // delete entities
  uint64_t del_cnt;
  KStatus s = tbl_range->DeleteEntities(ctx_, primary_tags, &del_cnt, 0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, write_count * partition_num);
  // Data check
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 0, scan_cols.size()));
  delete iter;

  // Write a new batch of data
  for (int i = 1; i <= partition_num; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  // Compress 360000 and 720000 partitions
  g_compression.compression_type = kwdbts::CompressionType::GZIP;
  g_compression.compression_level = kwdbts::CompressionLevel::LOW;
  ASSERT_EQ(ts_table->Compress(ctx_, 3 * kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);
  // Data check
  entity_id = 2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, write_count * partition_num, scan_cols.size()));
  delete iter;

  // delete entities
  s = tbl_range->DeleteEntities(ctx_, primary_tags, &del_cnt, 0);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(del_cnt, write_count * partition_num);
  // Data check
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 0, scan_cols.size()));
  delete iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, VacuumTsTable) {
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "vacuum_table", cur_table_id);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  // 1. insert disorder, full block
  k_uint32 p_len = 0;
  KTimestamp start_ts = 1000;
  char* data_value = GenSomePayloadData(ctx_, 8, p_len, start_ts, &meta, 1);
  TSSlice payload{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);  // ts: 1000~1007
  delete[] data_value;

  start_ts += 8;
  start_ts += 1;
  data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts, &meta, 1);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);  // ts: 1009
  delete[] data_value;

  start_ts -= 1;  // disorder
  data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts, &meta, 1);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);  // ts: 1008
  delete[] data_value;

  // 2. compress
  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  ASSERT_EQ(ts_table->Compress(ctx_, INT64_MAX, compressed_num, err_info), KStatus::SUCCESS);

  // 3. vacuum. block is full, block_item's min/max ts should be set correctly
  ASSERT_EQ(ts_table->Vacuum(ctx_, 1, err_info), KStatus::SUCCESS);

  // 4. query data using ts_span{1000, 10008}
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  start_ts = 1000;
  KwTsSpan ts_span = {convertMSToPrecisionTS(start_ts, ts_type), convertMSToPrecisionTS(start_ts + 8, ts_type)};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  uint32_t entity_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 9, scan_cols.size()));
  delete iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, DropColumn) {
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456;
  ConstructRoachpbTable(&meta, "test_drop_column", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  char *data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  TSSlice payload{data_value, p_len};

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = tbl_range->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  roachpb::KWDBKTSColumn column;
  column.set_column_id(2);
  column.set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  column.set_name("column2");
  column.set_storage_type(::roachpb::DataType::SMALLINT);
  column.set_storage_len(2);
  string trans_id = "0000000000000009";
  std::string err_msg;
  size_t column_size = column.ByteSizeLong();
  auto* buffer = new char[column_size];
  column.SerializeToArray(buffer, column_size);

  TSSlice column_slice{buffer, column_size};
  ASSERT_EQ(column.ParseFromArray(column_slice.data, column_slice.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, const_cast<char*>(trans_id.data()), column_slice, 1, 2, err_msg);
  delete[] buffer;
  ASSERT_EQ(s, KStatus::SUCCESS);
}

TEST_F(TestEngine, LazyMount) {
  kwdbts::EngineOptions::iot_interval = 3600;
  kwdbts::g_max_mount_cnt_ = 10;
  roachpb::CreateTsTable meta;
  std::vector<string> primary_tags;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int partition_num = 22;
  int write_count = 1;
  for (int i = 1; i <= partition_num; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    if (i == 1) {
      Payload pd(schema, actual_cols, payload);
      primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
    }
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  // Compress all partitions
  ErrorInfo err_info;
  g_compression.compression_type = kwdbts::CompressionType::GZIP;
  g_compression.compression_level = kwdbts::CompressionLevel::LOW;
  uint32_t compressed_num = 0;
  ASSERT_EQ(ts_table->Compress(ctx_, (partition_num+1)*kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);
  // Due to not being fully written, it will not be truly compressed until the second schedule
  ASSERT_EQ(ts_table->Compress(ctx_, (partition_num+1)*kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);

  // Close the table and reopen it to verify the mount function
  tbl_range.reset();
  TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  KTimestamp start_ts = kwdbts::EngineOptions::iot_interval * 1000;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, (partition_num + 1) * start_ts};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, write_count * partition_num, scan_cols.size()));
  delete iter;

  // Compress all partitions and trigger lruchache elimination
  g_compression.compression_type = kwdbts::CompressionType::LZO;
  ASSERT_EQ(ts_table->Compress(ctx_, (partition_num+1)*kwdbts::EngineOptions::iot_interval, compressed_num, err_info), KStatus::SUCCESS);

  // Verify the number of mounted partitions
  string cmd = "cat /proc/mounts | grep $(pwd)/test_db | wc -l";
  string cnt_str;
  executeShell(cmd, cnt_str);
  EXPECT_LE(stoi(cnt_str), kwdbts::g_max_mount_cnt_);
  EXPECT_NE(stoi(cnt_str), 0);

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, partition_interval) {
  uint64_t interval = 3600;
  roachpb::CreateTsTable meta;
  std::vector<string> primary_tags;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id, interval);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 10;
  // Write data to partitions 3600-7199
  KTimestamp start_ts = 4000 * 1000;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  TSSlice payload{data_value, p_len};
  Payload pd(schema, actual_cols, payload);
  primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 10800~14399
  start_ts = 12000 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;

  // Current data distribution
  // 3600~7199 partitions     10 pieces of data
  // 10800~14399 partitions   10 pieces of data

  // Data query
  start_ts = 0;
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts, 4 * 3600 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, write_count * 2, scan_cols.size()));
  delete iter;

  // Modify partition interval to 2000
  interval = 2000;
  ts_table->AlterPartitionInterval(ctx_, interval);

  write_count = 13;
  // Write data to partitions 1600~3599
  start_ts = 1600 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 7200~8799
  start_ts = 8799 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 14400~16399
  start_ts = 14400 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;

  // Current data distribution
  // 1600~3599 partitions     13 pieces of data - interval 2000, actual 2000
  // 3600~7199 partitions     10 pieces of data - interval 3600, actual 3600
  // 7200~8799 partitions     13 pieces of data - interval 2000, actual 1600
  // 10800~14399 partitions   10 pieces of data - interval 3600, actual 3600
  // 14400~16399 partitions   13 pieces of data - interval 2000, actual 2000

  // Data query
  start_ts = 0;
  ts_span = {start_ts, 16400 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 10 * 2 + 13 * 3, scan_cols.size()));
  delete iter;

  // Modify partition interval to 3000
  interval = 3000;
  ts_table->AlterPartitionInterval(ctx_, interval);

  write_count = 16;
  // Write data to partitions 0~1600
  start_ts = 0;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 8800~10799
  start_ts = 9000 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 19400~22400
  start_ts = 19400 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;

  // Current data distribution
  // 0~1599 partitions        16 pieces of data - interval 3000, actual 1600
  // 1600~3599 partitions     13 pieces of data - interval 2000, actual 2000
  // 3600~7199 partitions     10 pieces of data - interval 3600, actual 3600
  // 7200~8799 partitions     13 pieces of data - interval 2000, actual 1600
  // 8800~10799 partitions    16 pieces of data - interval 3000, actual 2000
  // 10800~14399 partitions   10 pieces of data - interval 3600, actual 3600
  // 14400~16399 partitions   13 pieces of data - interval 2000, actual 2000
  // 19400~22399 partitions   16 pieces of data - interval 3000, actual 3000

  // Data query
  start_ts = -1 * 22400 * 1000;
  ts_span = {start_ts, 22400 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 10 * 2 + 13 * 3 + 16 * 3, scan_cols.size()));
  delete iter;

  // Reopen
  tbl_range.reset();
  TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  // Data query
  start_ts = -1 * 22400 * 1000;
  ts_span = {start_ts, 22400 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 10 * 2 + 13 * 3 + 16 * 3, scan_cols.size()));
  delete iter;

  // Write data to existing partitions
  // Write data to partitions 14400~16399
  write_count = 1;
  start_ts = 14500 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;
  // Write data to partitions 16400~19399
  start_ts = 17000 * 1000;
  p_len = 0;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
  ASSERT_EQ(tbl_range->PutData(ctx_, {data_value, p_len}), KStatus::SUCCESS);
  delete[] data_value;

  // Current data distribution
  // 0~1599 partitions        16 pieces of data - interval 3000, actual 1600
  // 1600~3599 partitions     13 pieces of data - interval 2000, actual 2000
  // 3600~7199 partitions     10 pieces of data - interval 3600, actual 3600
  // 7200~8799 partitions     13 pieces of data - interval 2000, actual 1600
  // 8800~10799 partitions    16 pieces of data - interval 3000, actual 2000
  // 10800~14399 partitions   10 pieces of data - interval 3600, actual 3600
  // 14400~16399 partitions   13 + 1 pieces of data - interval 2000, actual 2000
  // 16400~19399 partitions   1 pieces of data - interval 3000, actual 3000
  // 19400~22399 partitions   16 pieces of data - interval 3000, actual 3000

  // Data query
  start_ts = -1 * 22400 * 1000;
  ts_span = {start_ts, 22400 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 10 * 2 + 13 * 3 + 16 * 3 + 1 * 2, scan_cols.size()));
  delete iter;

  // Verify partition status
  ErrorInfo err_info;
  ts_span = {convertSecondToPrecisionTS(-22400, ts_type), convertSecondToPrecisionTS(22400, ts_type)};
  std::vector<TsTimePartition*> p_tables = tbl_range->GetSubEntityGroupManager()->GetPartitionTables(ts_span, group_id, err_info);
  EXPECT_EQ(p_tables.size(), 9);
  EXPECT_EQ(p_tables[0]->minTimestamp(), -1400);
  EXPECT_EQ(p_tables[0]->maxTimestamp(), 1599);
  EXPECT_EQ(p_tables[1]->minTimestamp(), 1600);
  EXPECT_EQ(p_tables[1]->maxTimestamp(), 3599);
  EXPECT_EQ(p_tables[2]->minTimestamp(), 3600);
  EXPECT_EQ(p_tables[2]->maxTimestamp(), 7199);
  EXPECT_EQ(p_tables[3]->minTimestamp(), 7200);
  EXPECT_EQ(p_tables[3]->maxTimestamp(), 8799);
  EXPECT_EQ(p_tables[4]->minTimestamp(), 8800);
  EXPECT_EQ(p_tables[4]->maxTimestamp(), 10799);
  EXPECT_EQ(p_tables[5]->minTimestamp(), 10800);
  EXPECT_EQ(p_tables[5]->maxTimestamp(), 14399);
  EXPECT_EQ(p_tables[6]->minTimestamp(), 14400);
  EXPECT_EQ(p_tables[6]->maxTimestamp(), 16399);
  EXPECT_EQ(p_tables[7]->minTimestamp(), 16400);
  EXPECT_EQ(p_tables[7]->maxTimestamp(), 19399);
  EXPECT_EQ(p_tables[8]->minTimestamp(), 19400);
  EXPECT_EQ(p_tables[8]->maxTimestamp(), 22399);
  // Release partition table
  for (auto p_table : p_tables) {
    tbl_range->GetSubEntityGroupManager()->ReleasePartitionTable(p_table);
  }

  // Drop table
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestEngine, ClusterSetting) {
  kwdbts::EngineOptions::iot_interval = 3600;
  kwdbts::g_max_mount_cnt_ = 10;
  roachpb::CreateTsTable meta;
  std::vector<string> primary_tags;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  vector<AttributeInfo> schema;
  vector<uint32_t> actual_cols;
  for (int i = 0; i < meta.k_column_size(); i++) {
    const auto& col = meta.k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
    if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP = 1;
  CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT = 2;
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;

  // Write the first partition and the first segment -> 17 pieces of data
  int partition_id = 1;
  int write_count = 17;
  for (int i = 1; i <= partition_id; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    if (i == 1) {
      Payload pd(schema, actual_cols, payload);
      primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
    }
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  // Compress the current partition
  ASSERT_EQ(ts_table->Compress(ctx_, kwdbts::EngineOptions::iot_interval + 1, compressed_num, err_info), KStatus::SUCCESS);
  // Due to the first segment not being filled to 90%, it will not be compressed
  ASSERT_EQ(ts_table->Compress(ctx_, kwdbts::EngineOptions::iot_interval + 1, compressed_num, err_info), KStatus::SUCCESS);

  // Check the number of compressed files
  string cmd = "ls -lR | grep sqfs | wc -l";
  string cnt_str;
  executeShell(cmd, cnt_str);
  EXPECT_EQ(stoi(cnt_str), 0);

  // Write the first partition and the first segment -> 3 pieces of data
  // Write the first partition and the second segment -> 10 pieces of data
  write_count = 13;
  for (int i = 1; i <= partition_id; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000 + 17;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  // Compress the current partition
  g_compression.compression_type = kwdbts::CompressionType::LZMA;
  ASSERT_EQ(ts_table->Compress(ctx_, kwdbts::EngineOptions::iot_interval + 1, compressed_num, err_info), KStatus::SUCCESS);
  // Due to the first segment being 90% full, it will be compressed
  // The second segment is not filled to 90% and will not be compressed
  g_compression.compression_type = kwdbts::CompressionType::XZ;
  ASSERT_EQ(ts_table->Compress(ctx_, kwdbts::EngineOptions::iot_interval + 1, compressed_num, err_info), KStatus::SUCCESS);

  // Check query results
  k_uint32 entity_id = 1;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {0, 2 * 3600 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 30, scan_cols.size()));
  delete iter;

  // Check the number of compressed files
  cmd = "ls -lR | grep sqfs | wc -l";
  executeShell(cmd, cnt_str);
  EXPECT_EQ(stoi(cnt_str), 1);

  // Check configuration information
  {
    ts_span = {0, convertSecondToPrecisionTS(3 * 3600, ts_type)};
    std::vector<TsTimePartition *> p_tables = tbl_range->GetSubEntityGroupManager()->GetPartitionTables(
      ts_span, 1, err_info);
    EXPECT_EQ(p_tables.size(), 1);
    EXPECT_EQ(p_tables[0]->getSegmentTable(1)->getBlockMaxRows(), CLUSTER_SETTING_MAX_ROWS_PER_BLOCK);
    EXPECT_EQ(p_tables[0]->getSegmentTable(1)->getBlockMaxNum(), CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT);
    std::shared_ptr<MMapSegmentTable> segment1 = p_tables[0]->getSegmentTable(1);
    EXPECT_NE(segment1, nullptr);
    std::shared_ptr<MMapSegmentTable> segment2 = p_tables[0]->getSegmentTable(3);
    EXPECT_NE(segment2, nullptr);
    std::shared_ptr<MMapSegmentTable> segment3 = p_tables[0]->getSegmentTable(5);
    EXPECT_EQ(segment3, nullptr);
    // Release partition table
    for (auto p_table : p_tables) {
      tbl_range->GetSubEntityGroupManager()->ReleasePartitionTable(p_table);
    }
  }

  // Close TsEngine
  tbl_range.reset();
  TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);

  // cluster setting configuration
  CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT = 5;
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 5;

  // reopen TsEngine
  TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  // Write data to the second partition. The first segment is full
  write_count = 25;
  partition_id = 2;
  for (int i = 2; i <= partition_id; ++i) {
    k_uint32 p_len = 0;
    KTimestamp start_ts = i * kwdbts::EngineOptions::iot_interval * 1000;
    char* data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
  }

  ts_span = {0, 3 * 3600 * 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  EXPECT_TRUE(CheckIterRows(iter, 55, scan_cols.size()));
  delete iter;

  // Check configuration information
  {
    ts_span = {0, 3 * 3600 * 1000};
    ts_span = ConvertMsToPrecision(ts_span, ts_type);
    std::vector<TsTimePartition *> p_tables = tbl_range->GetSubEntityGroupManager()->GetPartitionTables(ts_span,
                                                                                                        1, err_info);
    EXPECT_EQ(p_tables.size(), 2);
    EXPECT_EQ(p_tables[0]->getSegmentTable(1)->getBlockMaxRows(), 10);
    EXPECT_EQ(p_tables[0]->getSegmentTable(1)->getBlockMaxNum(), 2);
    EXPECT_EQ(p_tables[1]->getSegmentTable(1)->getBlockMaxRows(), CLUSTER_SETTING_MAX_ROWS_PER_BLOCK);
    EXPECT_EQ(p_tables[1]->getSegmentTable(1)->getBlockMaxNum(), CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT);
    std::shared_ptr<MMapSegmentTable> segment4 = p_tables[0]->getSegmentTable(1);
    EXPECT_NE(segment4, nullptr);
    std::shared_ptr<MMapSegmentTable> segment5 = p_tables[0]->getSegmentTable(3);
    EXPECT_NE(segment5, nullptr);
    std::shared_ptr<MMapSegmentTable> segment6 = p_tables[0]->getSegmentTable(5);
    EXPECT_EQ(segment6, nullptr);
    std::shared_ptr<MMapSegmentTable> segment7 = p_tables[1]->getSegmentTable(1);
    EXPECT_NE(segment7, nullptr);
    std::shared_ptr<MMapSegmentTable> segment8 = p_tables[1]->getSegmentTable(6);
    EXPECT_EQ(segment8, nullptr);
    // Release partition table
    for (auto p_table : p_tables) {
      tbl_range->GetSubEntityGroupManager()->ReleasePartitionTable(p_table);
    }
  }

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
