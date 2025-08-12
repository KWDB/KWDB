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

#include <st_wal_table.h>
#include "engine.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT big table dir
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

extern bool g_go_start_service;

RangeGroup kTestRange{default_entitygroup_id_in_dist_v2, 0};

class TestEngineWAL : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;
  char buffer1[100] = {0};
  char buffer2[100] = {0};

  TestEngineWAL() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    // delete ts_engine_;
    // ts_engine_ = nullptr;
    setenv("KW_HOME", kDbPath.c_str(), 1);
    // clear data dir
    system(("rm -rf " + kDbPath + "/*").c_str());
    g_go_start_service = false;
  }

  ~TestEngineWAL() {
    // CLOSE engine
    delete ts_engine_;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  k_uint32 GetTableRows(KTableKey table_id, const std::vector<RangeGroup>& rgs, KwTsSpan ts_span,
                        uint16_t entity_num = 1, uint32_t table_version = 1) {
    k_uint32 total_rows = 0;
    std::shared_ptr<TsTable> table;
    ts_engine_->GetTsTable(ctx_, table_id, table);
    auto ts_type = table->GetRootTableManager()->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_type);

    for (auto& rg : rgs) {
      std::shared_ptr<TsEntityGroup> entity_group;
      KStatus s = table->GetEntityGroup(ctx_, rg.range_group_id, &entity_group);
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;
      k_uint32 entity_id = 1;
      SubGroupID group_id = 1;
      while (entity_id <= entity_num) {
        TsStorageIterator* iter1;
        entity_group->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scan_cols, scan_cols,
                                  {}, scan_agg_types, table_version, &iter1, entity_group, {}, false, false);
        total_rows += GetIterRows(iter1, scan_cols.size());
        entity_id++;
        delete iter1;
      }
    }

    return total_rows;
  }

  k_uint32 GetIterRows(TsStorageIterator* iter1, k_uint32 scancol_num) {
    ResultSet res{scancol_num};
    k_uint32 ret_cnt;
    k_uint32 total_rows = 0;
    bool is_finished = false;
    do {
      KStatus s = iter1->Next(&res, &ret_cnt, &is_finished);
      if (s != KStatus::SUCCESS) {
        return 0;
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    return total_rows;
  }

  std::string getPrimaryTag(roachpb::CreateTsTable meta, TSSlice payload) {
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

    return primary_tag;
  }

  TSSlice columnToSlice(roachpb::KWDBKTSColumn* column) {
    memset(buffer1, 0, 100);
    size_t col_size = column->ByteSizeLong();
    column->SerializeToArray(buffer1, col_size);
    TSSlice column_slice{buffer1, col_size};
    return column_slice;
  }

  TSSlice columnToSlice2(roachpb::KWDBKTSColumn* column) {
    memset(buffer2, 0, 100);
    size_t col_size = column->ByteSizeLong();
    column->SerializeToArray(buffer2, col_size);
    TSSlice column_slice{buffer2, col_size};
    return column_slice;
  }

  // header info
  int row_num_ = 5;
};

TEST_F(TestEngineWAL, create) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, CreateSomeTables) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

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

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, insert) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, insertRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), row_num_);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), 0);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, checkpoint) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteData) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<kwdbts::TsTable>  ts_table;
  ErrorInfo err_info;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table, false, err_info);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, 1000, &meta);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  // delete
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t count = 0;
  KwTsSpan span{1, ts + 10,};
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {ConvertMsToPrecision(span, ts_type)},
                             &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num_);  // delete 12 rows

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // after delete
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {ts, ts + 10000}), row_num_ - 2);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteDataRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<kwdbts::TsTable>  ts_table;
  ErrorInfo err_info;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table, false, err_info);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, 1000, &meta);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  // delete
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t count = 0;
  KwTsSpan span{1, ts + 10,};
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {ConvertMsToPrecision(span, ts_type)},
                             &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num_);
  // after delete
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ - 2);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteEntities) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // delete entities
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t del_cnt;
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}), 0);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteEntitiesRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // delete entities
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t del_cnt;
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, updateTag) {
  opts_.wal_level = 2;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  int cnt = 20;
  char* data_value = nullptr;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result), KStatus::SUCCESS);
    delete[] data_value;
  }
  // update
  data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 1, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(ts_engine_->PutEntity(ctx_, cur_table_id, kTestRange.range_group_id, &payload1, 1, 0), KStatus::SUCCESS);
    delete[] data_value;
  }

  // tagiterator
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  TagIterator *iter;
  vector<uint32_t> hps;
  make_hashpoint(&hps);
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags, hps, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tagval = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity_id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    // check tag_column
    for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
      for (int i = 0; i < res.data[tag_idx][0]->count; i++) {
        if (tag_idx == 0) {
          memcpy(&tagval, (char*)res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tagval));
          ASSERT_EQ(tagval, start_ts1 + 1 + (i+fetch_total_count) * 100);
        } else {
          // var
          char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
          ASSERT_EQ(rec_ptr, std::to_string(start_ts1 + 1 + (i)*100));
        }
      }
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  } while (count);
  ASSERT_EQ(fetch_total_count, cnt);
  iter->Close();
  delete iter;
}

TEST_F(TestEngineWAL, EngineApi) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<kwdbts::TsTable>  ts_table;
  ErrorInfo err_info;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table, false, err_info);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len};
  auto pt = getPrimaryTag(meta, payload);
  auto pt2 = getPrimaryTag(meta, payload2);
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ * 2);

  uint64_t count = 0;
  KwTsSpan span{1, start_ts + 10,};

  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {ConvertMsToPrecision(span, ts_type)},
                             &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ * 2 - 2);

  // delete entities
  uint64_t del_cnt;
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt2}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->FlushBuffer(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ - 2);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
  delete[] data_value;
  delete[] data_value2;
}

TEST_F(TestEngineWAL, ShiftWalLevel) {
  opts_.wal_level = 0;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 1000, &meta);
  TSSlice payload2{data_value2, p_len};
  char* data_value3 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 2000, &meta);
  TSSlice payload3{data_value3, p_len};
  char* data_value4 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 3000, &meta);
  TSSlice payload4{data_value4, p_len};

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  MakeDirectory(kDbPath + "/test_db_");
  MakeDirectory(kDbPath + "/test111");
  MakeDirectory(kDbPath + "/_111");

  opts_.wal_level = 1;

  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta2;
  KTableKey cur_table_id2 = 12345;
  ConstructRoachpbTable(&meta2, "testTable", cur_table_id2);
  RangeGroup test_range2{default_entitygroup_id_in_dist_v2, 0};
  std::vector<RangeGroup> ranges2{test_range2};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id2, &meta2, ranges2);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->PutData(ctx_, cur_table_id2, test_range2.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  opts_.wal_level = 2;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  std::vector<KTableKey> table_id_list;
  ts_engine_->GetTableIDList(ctx_, table_id_list);
  EXPECT_EQ(table_id_list.size(), 2);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_ * 2);
  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload3, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  opts_.wal_level = 0;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_ * 3);
  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload4, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->PutData(ctx_, cur_table_id2, test_range2.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}, 2), row_num_ * 2);

  // we don't support multiple ts engines for now because global variable g_pstBufferPoolInfo,
  // so we should close it before openning a new one.
  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  TSEngine* ts_engine2;
  opts_.wal_level = 1;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine2);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // GetTableRows will access ts_engine_, not ts_engine2.
  ts_engine_ = ts_engine2;
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}, 3), row_num_ * 4);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine2);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine2 = nullptr;
  ts_engine_ = nullptr;

  delete[] data_value;
  delete[] data_value2;
  delete[] data_value3;
  delete[] data_value4;
}

TEST_F(TestEngineWAL, TsxAlterColumn) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);
  int column_size = meta.k_column_size();

  // add column
  roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(column_size);
  column->set_name("column" + std::to_string(meta.k_column_size()));
  meta.mutable_ts_table()->set_ts_version(2);
  string err_msg;
  TSSlice column_slice = columnToSlice(column);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add tag
  roachpb::KWDBKTSColumn* column1 = meta.mutable_k_column()->Add();
  column1->set_col_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1->set_storage_type(roachpb::DataType::TIMESTAMP);
  column1->set_storage_len(8);
  column1->set_column_id(column_size + 1);
  column1->set_name("column" + std::to_string(column_size + 1));
  meta.mutable_ts_table()->set_ts_version(3);
  auto column_slice1 = columnToSlice(column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice1, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  meta.mutable_k_column()->DeleteSubrange(2, 1);
  meta.mutable_ts_table()->set_ts_version(4);
  auto column_slice2 = columnToSlice(&column2);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice2, 3, 4, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  meta.mutable_k_column()->DeleteSubrange(5, 1);
  meta.mutable_ts_table()->set_ts_version(5);
  auto column_slice3 = columnToSlice(&column3);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice3, 4, 5, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxMultiAlterColumn) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta_1;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta_1, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta_1, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_1, 10, 0, false);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add column
  roachpb::CreateTsTable meta_2 = meta_1;
  roachpb::KWDBKTSColumn* column = meta_2.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(meta_1.k_column_size() + 1);
  column->set_name("column" + std::to_string(meta_1.k_column_size() + 1));
  meta_2.mutable_ts_table()->set_ts_version(2);
  auto column_slice = columnToSlice(column);
  string err_msg;
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add tag
  roachpb::CreateTsTable meta_3 = meta_2;
  roachpb::KWDBKTSColumn* column1 = meta_3.mutable_k_column()->Add();
  column1->set_col_type(roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1->set_storage_type(roachpb::DataType::TIMESTAMP);
  column1->set_storage_len(8);
  column1->set_column_id(meta_2.k_column_size() + 1);
  column1->set_name("column" + std::to_string(meta_2.k_column_size() + 1));
  meta_3.mutable_ts_table()->set_ts_version(3);
  column_slice = columnToSlice(column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::CreateTsTable meta_4 = meta_3;

  roachpb::KWDBKTSColumn column8 = meta_4.mutable_k_column()->Get(7);
  meta_4.mutable_k_column(7)->set_dropped(true);
  meta_4.mutable_ts_table()->set_ts_version(4);
  column_slice = columnToSlice(&column8);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 3, 4, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // SMALLINT -> INT\BIGINT\VARCHAR(6)
  // add SMALLINT column
  roachpb::CreateTsTable meta_5 = meta_4;
  roachpb::KWDBKTSColumn* smallint_column1 = meta_5.mutable_k_column()->Add();
  smallint_column1->set_storage_type(roachpb::DataType::SMALLINT);
  smallint_column1->set_storage_len(g_all_col_types[roachpb::DataType::SMALLINT].storage_len);
  smallint_column1->set_column_id(meta_4.k_column_size() + 1);
  smallint_column1->set_name("column" + std::to_string(meta_4.k_column_size() + 1));
  meta_5.mutable_ts_table()->set_ts_version(5);
  column_slice = columnToSlice(smallint_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 4, 5, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add SMALLINT column2
  roachpb::CreateTsTable meta_6 = meta_5;
  roachpb::KWDBKTSColumn* smallint_column2 = meta_6.mutable_k_column()->Add();
  smallint_column2->set_storage_type(roachpb::DataType::SMALLINT);
  smallint_column2->set_storage_len(g_all_col_types[roachpb::DataType::SMALLINT].storage_len);
  smallint_column2->set_column_id(meta_5.k_column_size() + 1);
  smallint_column2->set_name("column" + std::to_string(meta_5.k_column_size() + 1));
  meta_6.mutable_ts_table()->set_ts_version(6);
  column_slice = columnToSlice(smallint_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 5, 6, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add SMALLINT column3
  roachpb::CreateTsTable meta_7 = meta_6;
  roachpb::KWDBKTSColumn* smallint_column3 = meta_7.mutable_k_column()->Add();
  smallint_column3->set_storage_type(roachpb::DataType::SMALLINT);
  smallint_column3->set_storage_len(g_all_col_types[roachpb::DataType::SMALLINT].storage_len);
  smallint_column3->set_column_id(meta_6.k_column_size() + 1);
  smallint_column3->set_name("column" + std::to_string(meta_6.k_column_size() + 1));
  meta_7.mutable_ts_table()->set_ts_version(7);
  column_slice = columnToSlice(smallint_column3);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 6, 7, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // INT -> BIGINT\VARCHAR(11)
  // add INT column1
  roachpb::CreateTsTable meta_8 = meta_7;
  roachpb::KWDBKTSColumn* int_column1 = meta_8.mutable_k_column()->Add();
  int_column1->set_storage_type(roachpb::DataType::INT);
  int_column1->set_storage_len(g_all_col_types[roachpb::DataType::INT].storage_len);
  int_column1->set_column_id(meta_7.k_column_size() + 1);
  int_column1->set_name("column" + std::to_string(meta_7.k_column_size() + 1));
  meta_8.mutable_ts_table()->set_ts_version(8);
  column_slice = columnToSlice(int_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 7, 8, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add INT column2
  roachpb::CreateTsTable meta_9 = meta_8;
  roachpb::KWDBKTSColumn* int_column2 = meta_9.mutable_k_column()->Add();
  int_column2->set_storage_type(roachpb::DataType::INT);
  int_column2->set_storage_len(g_all_col_types[roachpb::DataType::INT].storage_len);
  int_column2->set_column_id(meta_8.k_column_size() + 1);
  int_column2->set_name("column" + std::to_string(meta_8.k_column_size() + 1));
  meta_9.mutable_ts_table()->set_ts_version(9);
  column_slice = columnToSlice(int_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 8, 9, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // BIGINT -> VARCHAR(20)
  // add BIGINT column1
  roachpb::CreateTsTable meta_10 = meta_9;
  roachpb::KWDBKTSColumn* bigint_column1 = meta_10.mutable_k_column()->Add();
  bigint_column1->set_storage_type(roachpb::DataType::BIGINT);
  bigint_column1->set_storage_len(g_all_col_types[roachpb::DataType::BIGINT].storage_len);
  bigint_column1->set_column_id(meta_9.k_column_size() + 1);
  bigint_column1->set_name("column" + std::to_string(meta_9.k_column_size() + 1));
  meta_10.mutable_ts_table()->set_ts_version(10);
  column_slice = columnToSlice(bigint_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 9, 10, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // FLOAT -> DOUBLE\VARCHAR(30)
  // add FLOAT column1
  roachpb::CreateTsTable meta_11 = meta_10;
  roachpb::KWDBKTSColumn* float_column1 = meta_11.mutable_k_column()->Add();
  float_column1->set_storage_type(roachpb::DataType::FLOAT);
  float_column1->set_storage_len(g_all_col_types[roachpb::DataType::FLOAT].storage_len);
  float_column1->set_column_id(meta_10.k_column_size() + 1);
  float_column1->set_name("column" + std::to_string(meta_10.k_column_size() + 1));
  meta_11.mutable_ts_table()->set_ts_version(11);
  column_slice = columnToSlice(float_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 10, 11, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add FLOAT column2
  roachpb::CreateTsTable meta_12 = meta_11;
  roachpb::KWDBKTSColumn* float_column2 = meta_12.mutable_k_column()->Add();
  float_column2->set_storage_type(roachpb::DataType::FLOAT);
  float_column2->set_storage_len(g_all_col_types[roachpb::DataType::FLOAT].storage_len);
  float_column2->set_column_id(meta_11.k_column_size() + 1);
  float_column2->set_name("column" + std::to_string(meta_11.k_column_size() + 1));
  meta_12.mutable_ts_table()->set_ts_version(12);
  column_slice = columnToSlice(float_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 11, 12, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // DOUBLE -> VARCHAR(30)
  // add DOUBLE column1
  roachpb::CreateTsTable meta_13 = meta_12;
  roachpb::KWDBKTSColumn* double_column1 = meta_13.mutable_k_column()->Add();
  double_column1->set_storage_type(roachpb::DataType::DOUBLE);
  double_column1->set_storage_len(g_all_col_types[roachpb::DataType::DOUBLE].storage_len);
  double_column1->set_column_id(meta_12.k_column_size() + 1);
  double_column1->set_name("column" + std::to_string(meta_12.k_column_size() + 1));
  meta_13.mutable_ts_table()->set_ts_version(13);
  column_slice = columnToSlice(double_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 12, 13, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // CHAR(20) -> NCHAR(20)\NVARCHAR(20)\VARCHAR(20)
  // add CHAR column1
  roachpb::CreateTsTable meta_14 = meta_13;
  roachpb::KWDBKTSColumn* char_column1 = meta_14.mutable_k_column()->Add();
  char_column1->set_storage_type(roachpb::DataType::CHAR);
  char_column1->set_storage_len(20);
  char_column1->set_column_id(meta_13.k_column_size() + 1);
  char_column1->set_name("column" + std::to_string(meta_13.k_column_size() + 1));
  meta_14.mutable_ts_table()->set_ts_version(14);
  column_slice = columnToSlice(char_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 13, 14, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add CHAR column2
  roachpb::CreateTsTable meta_15 = meta_14;
  roachpb::KWDBKTSColumn* char_column2 = meta_15.mutable_k_column()->Add();
  char_column2->set_storage_type(roachpb::DataType::CHAR);
  char_column2->set_storage_len(20);
  char_column2->set_column_id(meta_14.k_column_size() + 1);
  char_column2->set_name("column" + std::to_string(meta_14.k_column_size() + 1));
  meta_15.mutable_ts_table()->set_ts_version(15);
  column_slice = columnToSlice(char_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 14, 15, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add CHAR column3
  roachpb::CreateTsTable meta_16 = meta_15;
  roachpb::KWDBKTSColumn* char_column3 = meta_16.mutable_k_column()->Add();
  char_column3->set_storage_type(roachpb::DataType::CHAR);
  char_column3->set_storage_len(20);
  char_column3->set_column_id(meta_15.k_column_size() + 1);
  char_column3->set_name("column" + std::to_string(meta_15.k_column_size() + 1));
  meta_16.mutable_ts_table()->set_ts_version(16);
  column_slice = columnToSlice(char_column3);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 15, 16, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NCHAR(20) -> CHAR(80)\VARCHAR(80)\NVARCHAR(20)
  // add NCHAR column1
  roachpb::CreateTsTable meta_17 = meta_16;
  roachpb::KWDBKTSColumn* nchar_column1 = meta_17.mutable_k_column()->Add();
  nchar_column1->set_storage_type(roachpb::DataType::NCHAR);
  nchar_column1->set_storage_len(20);
  nchar_column1->set_column_id(meta_16.k_column_size() + 1);
  nchar_column1->set_name("column" + std::to_string(meta_16.k_column_size() + 1));
  meta_17.mutable_ts_table()->set_ts_version(17);
  column_slice = columnToSlice(nchar_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 16, 17, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add NCHAR column2
  roachpb::CreateTsTable meta_18 = meta_17;
  roachpb::KWDBKTSColumn* nchar_column2 = meta_18.mutable_k_column()->Add();
  nchar_column2->set_storage_type(roachpb::DataType::NCHAR);
  nchar_column2->set_storage_len(20);
  nchar_column2->set_column_id(meta_17.k_column_size() + 1);
  nchar_column2->set_name("column" + std::to_string(meta_17.k_column_size() + 1));
  meta_18.mutable_ts_table()->set_ts_version(18);
  column_slice = columnToSlice(nchar_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 17, 18, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add NCHAR column3
  roachpb::CreateTsTable meta_19 = meta_18;
  roachpb::KWDBKTSColumn* nchar_column3 = meta_19.mutable_k_column()->Add();
  nchar_column3->set_storage_type(roachpb::DataType::NCHAR);
  nchar_column3->set_storage_len(20);
  nchar_column3->set_column_id(meta_18.k_column_size() + 1);
  nchar_column3->set_name("column" + std::to_string(meta_18.k_column_size() + 1));
  meta_19.mutable_ts_table()->set_ts_version(19);
  column_slice = columnToSlice(nchar_column3);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 18, 19, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR(20) -> CHAR(20)\NCHAR(20)\NVARCHAR(20)\SMALLINT\INT\BIGINT\FLOAT\DOUBLE
  // add VARCHAR column1
  roachpb::CreateTsTable meta_20 = meta_19;
  roachpb::KWDBKTSColumn* varchar_column1 = meta_20.mutable_k_column()->Add();
  varchar_column1->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column1->set_storage_len(20);
  varchar_column1->set_column_id(meta_19.k_column_size() + 1);
  varchar_column1->set_name("column" + std::to_string(meta_19.k_column_size() + 1));
  meta_20.mutable_ts_table()->set_ts_version(20);
  column_slice = columnToSlice(varchar_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 19, 20, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column2
  roachpb::CreateTsTable meta_21 = meta_20;
  roachpb::KWDBKTSColumn* varchar_column2 = meta_21.mutable_k_column()->Add();
  varchar_column2->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column2->set_storage_len(20);
  varchar_column2->set_column_id(meta_20.k_column_size() + 1);
  varchar_column2->set_name("column" + std::to_string(meta_20.k_column_size() + 1));
  meta_21.mutable_ts_table()->set_ts_version(21);
  column_slice = columnToSlice(varchar_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 20, 21, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column3
  roachpb::CreateTsTable meta_22 = meta_21;
  roachpb::KWDBKTSColumn* varchar_column3 = meta_22.mutable_k_column()->Add();
  varchar_column3->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column3->set_storage_len(20);
  varchar_column3->set_column_id(meta_21.k_column_size() + 1);
  varchar_column3->set_name("column" + std::to_string(meta_21.k_column_size() + 1));
  meta_22.mutable_ts_table()->set_ts_version(22);
  column_slice = columnToSlice(varchar_column3);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 21, 22, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column4
  roachpb::CreateTsTable meta_23 = meta_22;
  roachpb::KWDBKTSColumn* varchar_column4 = meta_23.mutable_k_column()->Add();
  varchar_column4->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column4->set_storage_len(20);
  varchar_column4->set_column_id(meta_22.k_column_size() + 1);
  varchar_column4->set_name("column" + std::to_string(meta_22.k_column_size() + 1));
  meta_23.mutable_ts_table()->set_ts_version(23);
  column_slice = columnToSlice(varchar_column4);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 22, 23, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column5
  roachpb::CreateTsTable meta_24 = meta_23;
  roachpb::KWDBKTSColumn* varchar_column5 = meta_24.mutable_k_column()->Add();
  varchar_column5->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column5->set_storage_len(20);
  varchar_column5->set_column_id(meta_23.k_column_size() + 1);
  varchar_column5->set_name("column" + std::to_string(meta_23.k_column_size() + 1));
  meta_24.mutable_ts_table()->set_ts_version(24);
  column_slice = columnToSlice(varchar_column5);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 23, 24, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column6
  roachpb::CreateTsTable meta_25 = meta_24;
  roachpb::KWDBKTSColumn* varchar_column6 = meta_25.mutable_k_column()->Add();
  varchar_column6->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column6->set_storage_len(20);
  varchar_column6->set_column_id(meta_24.k_column_size() + 1);
  varchar_column6->set_name("column" + std::to_string(meta_24.k_column_size() + 1));
  meta_25.mutable_ts_table()->set_ts_version(25);
  column_slice = columnToSlice(varchar_column6);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 24, 25, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column7
  roachpb::CreateTsTable meta_26 = meta_25;
  roachpb::KWDBKTSColumn* varchar_column7 = meta_26.mutable_k_column()->Add();
  varchar_column7->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column7->set_storage_len(20);
  varchar_column7->set_column_id(meta_25.k_column_size() + 1);
  varchar_column7->set_name("column" + std::to_string(meta_25.k_column_size() + 1));
  meta_26.mutable_ts_table()->set_ts_version(26);
  column_slice = columnToSlice(varchar_column7);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 25, 26, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add VARCHAR column8
  roachpb::CreateTsTable meta_27 = meta_26;
  roachpb::KWDBKTSColumn* varchar_column8 = meta_27.mutable_k_column()->Add();
  varchar_column8->set_storage_type(roachpb::DataType::VARCHAR);
  varchar_column8->set_storage_len(20);
  varchar_column8->set_column_id(meta_26.k_column_size() + 1);
  varchar_column8->set_name("column" + std::to_string(meta_26.k_column_size() + 1));
  meta_27.mutable_ts_table()->set_ts_version(27);
  column_slice = columnToSlice(varchar_column8);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 26, 27, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);


  // NVARCHAR(20) -> CHAR(80)\VARCHAR(80)\NCHAR(20)
  // add NVARCHAR column1
  roachpb::CreateTsTable meta_28 = meta_27;
  roachpb::KWDBKTSColumn* nvarchar_column1 = meta_28.mutable_k_column()->Add();
  nvarchar_column1->set_storage_type(roachpb::DataType::NVARCHAR);
  nvarchar_column1->set_storage_len(20);
  nvarchar_column1->set_column_id(meta_27.k_column_size() + 1);
  nvarchar_column1->set_name("column" + std::to_string(meta_27.k_column_size() + 1));
  meta_28.mutable_ts_table()->set_ts_version(28);
  column_slice = columnToSlice(nvarchar_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 27, 28, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add NVARCHAR column2
  roachpb::CreateTsTable meta_29 = meta_28;
  roachpb::KWDBKTSColumn* nvarchar_column2 = meta_29.mutable_k_column()->Add();
  nvarchar_column2->set_storage_type(roachpb::DataType::NVARCHAR);
  nvarchar_column2->set_storage_len(20);
  nvarchar_column2->set_column_id(meta_28.k_column_size() + 1);
  nvarchar_column2->set_name("column" + std::to_string(meta_28.k_column_size() + 1));
  meta_29.mutable_ts_table()->set_ts_version(29);
  column_slice = columnToSlice(nvarchar_column2);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 28, 29, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add NVARCHAR column3
  roachpb::CreateTsTable meta_30 = meta_29;
  roachpb::KWDBKTSColumn* nvarchar_column3 = meta_30.mutable_k_column()->Add();
  nvarchar_column3->set_storage_type(roachpb::DataType::NVARCHAR);
  nvarchar_column3->set_storage_len(20);
  nvarchar_column3->set_column_id(meta_29.k_column_size() + 1);
  nvarchar_column3->set_name("column" + std::to_string(meta_29.k_column_size() + 1));
  meta_30.mutable_ts_table()->set_ts_version(30);
  column_slice = columnToSlice(nvarchar_column3);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 29, 30, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // TIMESTAMP -> TIMESTAMPTZ
  // add TIMESTAMP column1
  roachpb::CreateTsTable meta_31 = meta_30;
  roachpb::KWDBKTSColumn* timestamp_column1 = meta_31.mutable_k_column()->Add();
  timestamp_column1->set_storage_type(roachpb::DataType::TIMESTAMP);
  timestamp_column1->set_storage_len(g_all_col_types[roachpb::DataType::TIMESTAMP].storage_len);
  timestamp_column1->set_column_id(meta_30.k_column_size() + 1);
  timestamp_column1->set_name("column" + std::to_string(meta_30.k_column_size() + 1));
  meta_31.mutable_ts_table()->set_ts_version(31);
  column_slice = columnToSlice(timestamp_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 30, 31, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // TIMESTAMPTZ -> TIMESTAMP
  // add TIMESTAMPTZ column1
  roachpb::CreateTsTable meta_32 = meta_31;
  roachpb::KWDBKTSColumn* timestamptz_column1 = meta_32.mutable_k_column()->Add();
  timestamptz_column1->set_storage_type(roachpb::DataType::TIMESTAMPTZ);
  timestamptz_column1->set_storage_len(g_all_col_types[roachpb::DataType::TIMESTAMPTZ].storage_len);
  timestamptz_column1->set_column_id(meta_31.k_column_size() + 1);
  timestamptz_column1->set_name("column" + std::to_string(meta_31.k_column_size() + 1));
  meta_32.mutable_ts_table()->set_ts_version(32);
  column_slice = columnToSlice(timestamptz_column1);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 31, 32, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // alter column type
  // smallint -> int
  roachpb::CreateTsTable meta_33 = meta_32;
  roachpb::KWDBKTSColumn column_old = meta_33.mutable_k_column()->Get(smallint_column1->column_id() - 1);
  meta_33.mutable_k_column(smallint_column1->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::INT].storage_len);
  meta_33.mutable_k_column(smallint_column1->column_id() - 1)->set_storage_type(roachpb::DataType::INT);
  meta_33.mutable_ts_table()->set_ts_version(33);
  roachpb::KWDBKTSColumn column_new = meta_33.mutable_k_column()->Get(smallint_column1->column_id() - 1);
  auto new_column_slice = columnToSlice(&column_new);
  auto old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 32, 33, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // smallint -> BIGINT
  roachpb::CreateTsTable meta_34 = meta_33;
  column_old = meta_34.mutable_k_column()->Get(smallint_column2->column_id() - 1);
  meta_34.mutable_k_column(smallint_column2->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::BIGINT].storage_len);
  meta_34.mutable_k_column(smallint_column2->column_id() - 1)->set_storage_type(roachpb::DataType::BIGINT);
  meta_34.mutable_ts_table()->set_ts_version(34);
  column_new = meta_34.mutable_k_column()->Get(smallint_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 33, 34, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // smallint -> VARCHAR
  roachpb::CreateTsTable meta_35 = meta_34;
  column_old = meta_35.mutable_k_column()->Get(smallint_column3->column_id() - 1);
  meta_35.mutable_k_column(smallint_column3->column_id() - 1)->set_storage_len(20);
  meta_35.mutable_k_column(smallint_column3->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_35.mutable_ts_table()->set_ts_version(35);
  column_new = meta_35.mutable_k_column()->Get(smallint_column3->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 34, 35, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // INT -> BIGINT
  roachpb::CreateTsTable meta_36 = meta_35;
  column_old = meta_36.mutable_k_column()->Get(int_column1->column_id() - 1);
  meta_36.mutable_k_column(int_column1->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::BIGINT].storage_len);
  meta_36.mutable_k_column(int_column1->column_id() - 1)->set_storage_type(roachpb::DataType::BIGINT);
  meta_36.mutable_ts_table()->set_ts_version(36);
  column_new = meta_36.mutable_k_column()->Get(int_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 35, 36, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // INT -> VARCHAR
  roachpb::CreateTsTable meta_37 = meta_36;
  column_old = meta_37.mutable_k_column()->Get(int_column2->column_id() - 1);
  meta_37.mutable_k_column(int_column2->column_id() - 1)->set_storage_len(20);
  meta_37.mutable_k_column(int_column2->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_37.mutable_ts_table()->set_ts_version(37);
  column_new = meta_37.mutable_k_column()->Get(int_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 36, 37, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // BIGINT -> VARCHAR
  roachpb::CreateTsTable meta_38 = meta_37;
  column_old = meta_38.mutable_k_column()->Get(bigint_column1->column_id() - 1);
  meta_38.mutable_k_column(bigint_column1->column_id() - 1)->set_storage_len(20);
  meta_38.mutable_k_column(bigint_column1->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_38.mutable_ts_table()->set_ts_version(38);
  column_new = meta_38.mutable_k_column()->Get(bigint_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 37, 38, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // FLOAT -> DOUBLE
  roachpb::CreateTsTable meta_39 = meta_38;
  column_old = meta_39.mutable_k_column()->Get(float_column1->column_id() - 1);
  meta_39.mutable_k_column(float_column1->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::DOUBLE].storage_len);
  meta_39.mutable_k_column(float_column1->column_id() - 1)->set_storage_type(roachpb::DataType::DOUBLE);
  meta_39.mutable_ts_table()->set_ts_version(39);
  column_new = meta_39.mutable_k_column()->Get(float_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 38, 39, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // FLOAT -> VARCHAR
  roachpb::CreateTsTable meta_40 = meta_39;
  column_old = meta_40.mutable_k_column()->Get(float_column2->column_id() - 1);
  meta_40.mutable_k_column(float_column2->column_id() - 1)->set_storage_len(20);
  meta_40.mutable_k_column(float_column2->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_40.mutable_ts_table()->set_ts_version(40);
  column_new = meta_40.mutable_k_column()->Get(float_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 39, 40, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // DOUBLE -> VARCHAR
  roachpb::CreateTsTable meta_41 = meta_40;
  column_old = meta_41.mutable_k_column()->Get(double_column1->column_id() - 1);
  meta_41.mutable_k_column(double_column1->column_id() - 1)->set_storage_len(20);
  meta_41.mutable_k_column(double_column1->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_41.mutable_ts_table()->set_ts_version(41);
  column_new = meta_41.mutable_k_column()->Get(double_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 40, 41, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // CHAR -> NCHAR
  roachpb::CreateTsTable meta_42 = meta_41;
  column_old = meta_42.mutable_k_column()->Get(char_column1->column_id() - 1);
  meta_42.mutable_k_column(char_column1->column_id() - 1)->set_storage_len(20);
  meta_42.mutable_k_column(char_column1->column_id() - 1)->set_storage_type(roachpb::DataType::NCHAR);
  meta_42.mutable_ts_table()->set_ts_version(42);
  column_new = meta_42.mutable_k_column()->Get(char_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 41, 42, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // CHAR -> VARCHAR
  roachpb::CreateTsTable meta_43 = meta_42;
  column_old = meta_43.mutable_k_column()->Get(char_column2->column_id() - 1);
  meta_43.mutable_k_column(char_column2->column_id() - 1)->set_storage_len(20);
  meta_43.mutable_k_column(char_column2->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_43.mutable_ts_table()->set_ts_version(43);
  column_new = meta_43.mutable_k_column()->Get(char_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 42, 43, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // CHAR -> NVARCHAR
  roachpb::CreateTsTable meta_44 = meta_43;
  column_old = meta_44.mutable_k_column()->Get(char_column3->column_id() - 1);
  meta_44.mutable_k_column(char_column3->column_id() - 1)->set_storage_len(20);
  meta_44.mutable_k_column(char_column3->column_id() - 1)->set_storage_type(roachpb::DataType::NVARCHAR);
  meta_44.mutable_ts_table()->set_ts_version(44);
  column_new = meta_44.mutable_k_column()->Get(char_column3->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 43, 44, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NCHAR -> CHAR
  roachpb::CreateTsTable meta_45 = meta_44;
  column_old = meta_45.mutable_k_column()->Get(nchar_column1->column_id() - 1);
  meta_45.mutable_k_column(nchar_column1->column_id() - 1)->set_storage_len(80);
  meta_45.mutable_k_column(nchar_column1->column_id() - 1)->set_storage_type(roachpb::DataType::CHAR);
  meta_45.mutable_ts_table()->set_ts_version(45);
  column_new = meta_45.mutable_k_column()->Get(nchar_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 44, 45, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NCHAR -> VARCHAR
  roachpb::CreateTsTable meta_46 = meta_45;
  column_old = meta_46.mutable_k_column()->Get(nchar_column2->column_id() - 1);
  meta_46.mutable_k_column(nchar_column2->column_id() - 1)->set_storage_len(80);
  meta_46.mutable_k_column(nchar_column2->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_46.mutable_ts_table()->set_ts_version(46);
  column_new = meta_46.mutable_k_column()->Get(nchar_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 45, 46, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NCHAR -> NVARCHAR
  roachpb::CreateTsTable meta_47 = meta_46;
  column_old = meta_47.mutable_k_column()->Get(nchar_column3->column_id() - 1);
  meta_47.mutable_k_column(nchar_column3->column_id() - 1)->set_storage_len(80);
  meta_47.mutable_k_column(nchar_column3->column_id() - 1)->set_storage_type(roachpb::DataType::NVARCHAR);
  meta_47.mutable_ts_table()->set_ts_version(47);
  column_new = meta_47.mutable_k_column()->Get(nchar_column3->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 46, 47, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> CHAR
  roachpb::CreateTsTable meta_48 = meta_47;
  column_old = meta_48.mutable_k_column()->Get(varchar_column1->column_id() - 1);
  meta_48.mutable_k_column(varchar_column1->column_id() - 1)->set_storage_len(20);
  meta_48.mutable_k_column(varchar_column1->column_id() - 1)->set_storage_type(roachpb::DataType::CHAR);
  meta_48.mutable_ts_table()->set_ts_version(48);
  column_new = meta_48.mutable_k_column()->Get(varchar_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 47, 48, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> NCHAR
  roachpb::CreateTsTable meta_49 = meta_48;
  column_old = meta_49.mutable_k_column()->Get(varchar_column2->column_id() - 1);
  meta_49.mutable_k_column(varchar_column2->column_id() - 1)->set_storage_len(20);
  meta_49.mutable_k_column(varchar_column2->column_id() - 1)->set_storage_type(roachpb::DataType::NCHAR);
  meta_49.mutable_ts_table()->set_ts_version(49);
  column_new = meta_49.mutable_k_column()->Get(varchar_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 48, 49, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> NVARCHAR
  roachpb::CreateTsTable meta_50 = meta_49;
  column_old = meta_50.mutable_k_column()->Get(varchar_column3->column_id() - 1);
  meta_50.mutable_k_column(varchar_column3->column_id() - 1)->set_storage_len(20);
  meta_50.mutable_k_column(varchar_column3->column_id() - 1)->set_storage_type(roachpb::DataType::NVARCHAR);
  meta_50.mutable_ts_table()->set_ts_version(50);
  column_new = meta_50.mutable_k_column()->Get(varchar_column3->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 49, 50, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> SMALLINT
  roachpb::CreateTsTable meta_51 = meta_50;
  column_old = meta_51.mutable_k_column()->Get(varchar_column4->column_id() - 1);
  meta_51.mutable_k_column(varchar_column4->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::SMALLINT].storage_len);
  meta_51.mutable_k_column(varchar_column4->column_id() - 1)->set_storage_type(roachpb::DataType::SMALLINT);
  meta_51.mutable_ts_table()->set_ts_version(51);
  column_new = meta_51.mutable_k_column()->Get(varchar_column4->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 50, 51, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> INT
  roachpb::CreateTsTable meta_52 = meta_51;
  column_old = meta_52.mutable_k_column()->Get(varchar_column5->column_id() - 1);
  meta_52.mutable_k_column(varchar_column5->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::INT].storage_len);
  meta_52.mutable_k_column(varchar_column5->column_id() - 1)->set_storage_type(roachpb::DataType::INT);
  meta_52.mutable_ts_table()->set_ts_version(52);
  column_new = meta_52.mutable_k_column()->Get(varchar_column5->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 51, 52, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> BIGINT
  roachpb::CreateTsTable meta_53 = meta_52;
  column_old = meta_53.mutable_k_column()->Get(varchar_column6->column_id() - 1);
  meta_53.mutable_k_column(varchar_column6->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::BIGINT].storage_len);
  meta_53.mutable_k_column(varchar_column6->column_id() - 1)->set_storage_type(roachpb::DataType::BIGINT);
  meta_53.mutable_ts_table()->set_ts_version(53);
  column_new = meta_53.mutable_k_column()->Get(varchar_column6->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 52, 53, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> FLOAT
  roachpb::CreateTsTable meta_54 = meta_53;
  column_old = meta_54.mutable_k_column()->Get(varchar_column7->column_id() - 1);
  meta_54.mutable_k_column(varchar_column7->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::FLOAT].storage_len);
  meta_54.mutable_k_column(varchar_column7->column_id() - 1)->set_storage_type(roachpb::DataType::FLOAT);
  meta_54.mutable_ts_table()->set_ts_version(54);
  column_new = meta_54.mutable_k_column()->Get(varchar_column7->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 53, 54, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // VARCHAR -> DOUBLE
  roachpb::CreateTsTable meta_55 = meta_54;
  column_old = meta_55.mutable_k_column()->Get(varchar_column8->column_id() - 1);
  meta_55.mutable_k_column(varchar_column8->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::DOUBLE].storage_len);
  meta_55.mutable_k_column(varchar_column8->column_id() - 1)->set_storage_type(roachpb::DataType::DOUBLE);
  meta_55.mutable_ts_table()->set_ts_version(55);
  column_new = meta_55.mutable_k_column()->Get(varchar_column8->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 54, 55, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NVARCHAR -> CHAR
  roachpb::CreateTsTable meta_56 = meta_55;
  column_old = meta_56.mutable_k_column()->Get(nvarchar_column1->column_id() - 1);
  meta_56.mutable_k_column(nvarchar_column1->column_id() - 1)->set_storage_len(80);
  meta_56.mutable_k_column(nvarchar_column1->column_id() - 1)->set_storage_type(roachpb::DataType::CHAR);
  meta_56.mutable_ts_table()->set_ts_version(56);
  column_new = meta_56.mutable_k_column()->Get(nvarchar_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 55, 56, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NVARCHAR -> NCHAR
  roachpb::CreateTsTable meta_57 = meta_56;
  column_old = meta_57.mutable_k_column()->Get(nvarchar_column2->column_id() - 1);
  meta_57.mutable_k_column(nvarchar_column2->column_id() - 1)->set_storage_len(20);
  meta_57.mutable_k_column(nvarchar_column2->column_id() - 1)->set_storage_type(roachpb::DataType::NCHAR);
  meta_57.mutable_ts_table()->set_ts_version(57);
  column_new = meta_57.mutable_k_column()->Get(nvarchar_column2->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 56, 57, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // NVARCHAR -> VARCHAR
  roachpb::CreateTsTable meta_58 = meta_57;
  column_old = meta_58.mutable_k_column()->Get(nvarchar_column3->column_id() - 1);
  meta_58.mutable_k_column(nvarchar_column3->column_id() - 1)->set_storage_len(80);
  meta_58.mutable_k_column(nvarchar_column3->column_id() - 1)->set_storage_type(roachpb::DataType::VARCHAR);
  meta_58.mutable_ts_table()->set_ts_version(58);
  column_new = meta_58.mutable_k_column()->Get(nvarchar_column3->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 57, 58, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // TIMESTAMP -> TIMESTAMPTZ
  roachpb::CreateTsTable meta_59 = meta_58;
  column_old = meta_59.mutable_k_column()->Get(timestamp_column1->column_id() - 1);
  meta_59.mutable_k_column(timestamp_column1->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::TIMESTAMPTZ].storage_len);
  meta_59.mutable_k_column(timestamp_column1->column_id() - 1)->set_storage_type(roachpb::DataType::TIMESTAMPTZ);
  meta_59.mutable_ts_table()->set_ts_version(59);
  column_new = meta_59.mutable_k_column()->Get(timestamp_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 58, 59, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // TIMESTAMPTZ -> TIMESTAMP
  roachpb::CreateTsTable meta_60 = meta_59;
  column_old = meta_60.mutable_k_column()->Get(timestamptz_column1->column_id() - 1);
  meta_60.mutable_k_column(timestamptz_column1->column_id() - 1)->set_storage_len(
      g_all_col_types[roachpb::DataType::TIMESTAMP].storage_len);
  meta_60.mutable_k_column(timestamptz_column1->column_id() - 1)->set_storage_type(roachpb::DataType::TIMESTAMP);
  meta_60.mutable_ts_table()->set_ts_version(60);
  column_new = meta_60.mutable_k_column()->Get(timestamptz_column1->column_id() - 1);
  new_column_slice = columnToSlice(&column_new);
  old_column_slice = columnToSlice2(&column_old);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), new_column_slice, old_column_slice, 59, 60, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // push version_3 data
  start_ts += 10 * 10;
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_3, 10, 0, false);
  TSSlice payload2{data_value2, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value2;

  // push version_30 data
  start_ts += 10 * 10;
  char* data_value3 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_30, 10, 0, false);
  TSSlice payload3{data_value3, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload3, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value3;

  // push version_1 data
  start_ts += 10 * 10;
  char* data_value4 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_1, 10, 0, false);
  TSSlice payload4{data_value4, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload4, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value4;

  // push version_60 data
  start_ts += 10 * 10;
  char* data_value5 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_60, 10, 0, false);
  TSSlice payload5{data_value5, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload5, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value5;

  // push version_32 data
  start_ts += 10 * 10;
  char* data_value6 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta_32, 10, 0, false);
  TSSlice payload6{data_value6, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload6, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value6;

  // select version_1 data
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 1000}, 6, 1), row_num_);
  // select version_60 data
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 1000}, 6, 60), 6 * row_num_);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAddDropColumnRollback) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload2{data_value2, p_len2};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add column
  roachpb::KWDBKTSColumn column;
  column.set_storage_type(roachpb::DataType::TIMESTAMP);
  column.set_storage_len(8);
  column.set_column_id(meta.k_column_size());
  column.set_name("column" + std::to_string(meta.k_column_size()));
  string err_msg;
  size_t col_size = column.ByteSizeLong();
  auto* buffer = new char[col_size];
  column.SerializeToArray(buffer, col_size);

  TSSlice column_slice{buffer, col_size};
  ASSERT_EQ(column.ParseFromArray(column_slice.data, column_slice.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer;
  // add tag
  roachpb::KWDBKTSColumn column1;
  column1.set_col_type(roachpb::KWDBKTSColumn_ColumnType::
                       KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1.set_storage_type(roachpb::DataType::TIMESTAMP);
  column1.set_storage_len(8);
  column1.set_column_id(meta.k_column_size() + 1);
  column1.set_name("column" + std::to_string(meta.k_column_size() + 1));
  size_t col_size1 = column1.ByteSizeLong();
  auto* buffer1 = new char[col_size1];
  column1.SerializeToArray(buffer1, col_size1);
  TSSlice column_slice1{buffer1, col_size1};
  ASSERT_EQ(column1.ParseFromArray(column_slice1.data, column_slice1.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice1, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer1;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  size_t col_size2 = column2.ByteSizeLong();
  auto* buffer2 = new char[col_size2];
  column2.SerializeToArray(buffer2, col_size2);

  TSSlice column_slice2{buffer2, col_size2};
  ASSERT_EQ(column2.ParseFromArray(column_slice2.data, column_slice2.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice2, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer2;
  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  size_t col_size3 = column3.ByteSizeLong();
  auto* buffer3 = new char[col_size3];
  column3.SerializeToArray(buffer3, col_size3);

  TSSlice column_slice3{buffer3, col_size3};
  ASSERT_EQ(column3.ParseFromArray(column_slice3.data, column_slice3.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice3, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer3;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAddDropColumnRecover) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  auto wal2 = new WALMgr(opts_.db_path, 0, 0, &opts_);
  wal2->Init(ctx_);
  auto tsm = TSxMgr(wal2);

  string trans_id = "0000000000000001";
  uint64_t mtr_id = 0;
  s = tsm.TSxBegin(ctx_, trans_id.data());
  EXPECT_EQ(s, KStatus::SUCCESS);
  mtr_id = tsm.getMtrID(trans_id);
  int columns_size = meta.k_column_size();
  // add column
  roachpb::KWDBKTSColumn column;
  column.set_storage_type(roachpb::DataType::TIMESTAMP);
  column.set_storage_len(8);
  column.set_column_id(columns_size);
  column.set_name("column" + std::to_string(meta.k_column_size()));

  size_t column_size = column.ByteSizeLong();
  auto* buffer = KNEW char[column_size];
  column.SerializeToArray(buffer, column_size);
  TSSlice column_slice{buffer, column_size};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, AlterType::ADD_COLUMN, 1, 2, column_slice);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer;

  // add tag
  roachpb::KWDBKTSColumn column1;
  column1.set_col_type(roachpb::KWDBKTSColumn_ColumnType::
                       KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1.set_storage_type(roachpb::DataType::TIMESTAMP);
  column1.set_storage_len(8);
  column1.set_column_id(meta.k_column_size() + 1);
  column1.set_name("column" + std::to_string(meta.k_column_size() + 1));

  size_t column_size1 = column1.ByteSizeLong();
  auto* buffer1 = KNEW char[column_size1];
  column1.SerializeToArray(buffer1, column_size1);
  TSSlice column_slice1{buffer1, column_size1};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, AlterType::ADD_COLUMN, 2, 3, column_slice1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer1;

  s = tsm.TSxCommit(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  mtr_id = tsm.getMtrID(trans_id);
  s = tsm.TSxBegin(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  size_t column_size2 = column2.ByteSizeLong();
  auto* buffer2 = KNEW char[column_size2];
  column2.SerializeToArray(buffer2, column_size2);
  TSSlice column_slice2{buffer2, column_size2};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, AlterType::DROP_COLUMN, 3, 4, column_slice2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer2;

  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  size_t column_size3 = column3.ByteSizeLong();
  auto* buffer3 = KNEW char[column_size3];
  column3.SerializeToArray(buffer3, column_size3);
  TSSlice column_slice3{buffer3, column_size3};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, AlterType::DROP_COLUMN, 4, 5, column_slice3);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer3;

  s = tsm.TSxCommit(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = wal2->Flush(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete wal2;

  // recover
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  k_uint32 p_len2 = 0;
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len2};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;


  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAlterColumnRollback) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload2{data_value2, p_len2};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0 , &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // alter column
  roachpb::KWDBKTSColumn column_old = meta.mutable_k_column()->Get(2);
  roachpb::KWDBKTSColumn column_new = column_old;
  column_new.set_storage_type(roachpb::DataType::BIGINT);
  column_new.set_storage_len(8);
  string err_msg;

  size_t col_size_new = column_new.ByteSizeLong();
  auto* buffer_new = new char[col_size_new];
  column_new.SerializeToArray(buffer_new, col_size_new);
  TSSlice column_slice_new{buffer_new, col_size_new};
  ASSERT_EQ(column_new.ParseFromArray(column_slice_new.data, column_slice_new.len), true);

  size_t col_size_old = column_old.ByteSizeLong();
  auto* buffer_old = new char[col_size_old];
  column_old.SerializeToArray(buffer_old, col_size_old);
  TSSlice column_slice_old{buffer_old, col_size_old};
  ASSERT_EQ(column_old.ParseFromArray(column_slice_old.data, column_slice_old.len), true);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), column_slice_new, column_slice_old, 1, 2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer_new;
  delete[] buffer_old;
  // alter tag
  roachpb::KWDBKTSColumn column_old1 = meta.mutable_k_column()->Get(5);
  roachpb::KWDBKTSColumn column_new1 = column_old1;
  column_new1.set_storage_type(roachpb::DataType::BIGINT);
  column_new1.set_storage_len(8);

  size_t col_size_new1 = column_new1.ByteSizeLong();
  auto* buffer_new1 = new char[col_size_new1];
  column_new1.SerializeToArray(buffer_new1, col_size_new1);
  TSSlice column_slice_new1{buffer_new1, col_size_new1};
  ASSERT_EQ(column_new1.ParseFromArray(column_slice_new1.data, column_slice_new1.len), true);

  size_t col_size_old1 = column_old1.ByteSizeLong();
  auto* buffer_old1 = new char[col_size_old1];
  column_old1.SerializeToArray(buffer_old1, col_size_old1);
  TSSlice column_slice_old1{buffer_old1, col_size_old1};
  ASSERT_EQ(column_old1.ParseFromArray(column_slice_old1.data, column_slice_old1.len), true);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), column_slice_new1, column_slice_old1, 2, 3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer_new1;
  delete[] buffer_old1;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}
