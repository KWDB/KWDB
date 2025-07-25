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

#include "ts_table.h"
#include "st_wal_table.h"
#include "../../engine/tests/test_util.h"
#include "st_logged_entity_group.h"
#include "st_tier_manager.h"

using namespace kwdbts;  // NOLINT

const std::string kDbPath = "./test_ts_wal_table"; // NOLINT
const std::string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";   // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTSWALTable : public TestBigTableInstance {
 public:
  EngineOptions opt_;
  roachpb::CreateTsTable meta_;
  LoggedTsTable* table_;
  KTableKey table_id_ = 10086;
  uint64_t range_group_id_ = 100;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  std::vector<RangeGroup> ranges_;
  DedupResult dedup_result_{0, 0, 0, TSSlice {nullptr, 0}};

  TestTSWALTable() {
    ctx_ = &context_;
    setenv("KW_HOME", kDbPath.c_str(), 1);
    system(("rm -rf " + kDbPath + "/*").c_str());
    InitServerKWDBContext(ctx_);

    opt_.db_path = kDbPath;
    opt_.wal_level = 1;
    opt_.wal_buffer_size = 4;

    ConstructRoachpbTable(&meta_, "testTable", table_id_);
    ranges_.push_back({range_group_id_, 0});
    table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
    Create(&meta_, ranges_);
  }

  vector<AttributeInfo> getSchema() const {
    vector<AttributeInfo> schema;
    for (int i = 0; i < meta_.k_column_size(); i++) {
      const auto& col = meta_.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
        schema.push_back(std::move(col_var));
      }
    }

    return schema;
  }

  vector<uint32_t> getValidCols() const {
    vector<uint32_t> valid_cols;
    for (int i = 0; i < meta_.k_column_size(); i++) {
      const auto& col = meta_.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
        valid_cols.push_back(valid_cols.size());
      }
    }

    return valid_cols;
  }

  vector<TagInfo> getTagSchema() const {
    std::vector<TagInfo> schema;
    for (int i = 0; i < meta_.k_column_size(); i++) {
      const auto& col = meta_.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        schema.push_back(TagInfo{col.column_id(), col_var.type,
                                    static_cast<uint32_t>(col_var.length), 0,
                                    static_cast<uint32_t>(col_var.length),
                                    static_cast<TagType>(col_var.col_flag)});
      }
    }

    return schema;
  }

  void Create(roachpb::CreateTsTable* meta, const std::vector<RangeGroup>& range_groups) {
    KStatus s;

    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> metric_schema;
    for (int i = 0; i < meta->k_column_size(); i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(TagInfo{col.column_id(), col_var.type,
                                    static_cast<uint32_t>(col_var.length), 0,
                                    static_cast<uint32_t>(col_var.length),
                                    static_cast<TagType>(col_var.col_flag)});
      } else {
        metric_schema.push_back(std::move(col_var));
      }
    }

    s = table_->Create(ctx_, metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> table_range;
    for (auto range_group : range_groups) {
      s = table_->CreateEntityGroup(ctx_, range_group, tag_schema, &table_range);
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
  }

  k_uint32 GetTableRows(KTableKey table_id, const std::vector<RangeGroup>& rgs, KwTsSpan ts_span, uint16_t entity_num = 1) {
    k_uint32 total_rows = 0;
    auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
    for (auto& rg : rgs) {
      std::shared_ptr<TsEntityGroup> entity_group;
      KStatus s = table_->GetEntityGroup(ctx_, rg.range_group_id, &entity_group);
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;
      k_uint32 entity_id = 1;
      SubGroupID group_id = 1;
      while (entity_id <= entity_num) {
        TsStorageIterator* iter1;
        entity_group->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter1, entity_group, {}, false, false);
        total_rows += GetIterRows(iter1, scan_cols.size());
        entity_id++;
        delete iter1;
      }
    }

    return total_rows;
  }

  k_uint32 GetIterRows(TsStorageIterator* iter1, k_uint32 scan_cols_size) {
    ResultSet res{scan_cols_size};
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

  ~TestTSWALTable() override {
    table_->DropAll(ctx_, false);
    delete table_;
    table_ = nullptr;
  }
};

// PutData
TEST_F(TestTSWALTable, PutData) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};

  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + 2 * 10};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  SubGroupID group_id = 1;
  TsStorageIterator* iter1;
  ASSERT_EQ(log_eg->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter1, log_eg, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(KInt16((res.data)[1][0]->mem), 11);
  ASSERT_EQ(KInt32((res.data)[2][0]->mem), 2222);

  delete iter1;
  delete[] data_value;
}

TEST_F(TestTSWALTable, batchPutData) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();

  uint32_t payload_num = 5;
  uint32_t row_num = 1;
  vector<TSSlice> payloads;
  for (uint32_t i = 0; i < payload_num; i++) {
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i * 1000, &meta_);
    TSSlice payload{data_value, p_len};
    payloads.push_back(payload);
  }

  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payloads.data(), payloads.size(), 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + 100000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  SubGroupID group_id = 1;
  TsStorageIterator* iter1;
  ASSERT_EQ(log_eg->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter1, log_eg, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, payload_num * row_num);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(KInt16((res.data)[1][0]->mem), 11);
  ASSERT_EQ(KInt32((res.data)[2][0]->mem), 2222);

  for (auto p : payloads) {
    delete[] p.data;
  }

  delete iter1;
}

TEST_F(TestTSWALTable, mulitiInsert) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();

  uint32_t payload_num = 5;
  uint32_t row_num = 2;
  auto* payloads = new vector<TSSlice>();
  for (uint32_t i = 0; i < payload_num; i++) {
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i * 1000, &meta_);
    TSSlice payload{data_value, p_len};
    payloads->push_back(payload);
  }

  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::thread> threads;
  uint32_t batch_times = 1;
  uint32_t thread_num = payload_num;

  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back([=] {
      kwdbContext_t ctx1;
      KStatus s = InitServerKWDBContext(&ctx1);
      ASSERT_EQ(s, KStatus::SUCCESS);

      for (size_t i = 0; i < batch_times; i++) {
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        s = log_eg->PutData(&ctx1, payloads->data(), payloads->size(), 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_, kwdbts::DedupRule::KEEP);
        // s = log_eg->PutData(&ctx1, payloads->at(i), 1);
        ASSERT_EQ(s, KStatus::SUCCESS);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (auto p : *payloads) {
    delete[] p.data;
  }
  delete payloads;

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + 100000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  SubGroupID group_id = 1;
  TsStorageIterator* iter1;
  ASSERT_EQ(log_eg->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, {}, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter1, log_eg, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 total = 0;
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_type));
  ASSERT_EQ(KInt16((res.data)[1][0]->mem), 11);
  ASSERT_EQ(KInt32((res.data)[2][0]->mem), 2222);
  total += count;

  do {
    count = 0;
    ASSERT_EQ(iter1->Next(&res, reinterpret_cast<k_uint32*>(&count), &is_finished), KStatus::SUCCESS);
    total += count;
  } while (!is_finished);
  ASSERT_EQ(total, thread_num * payload_num * row_num * batch_times);

  log_eg->FlushBuffer(ctx_);
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  vector<LogEntry*> redo_logs;
  wal2->ReadWALLog(redo_logs, wal2->FetchCheckpointLSN(), wal2->FetchCurrentLSN());
  // metric log + tag log
  EXPECT_EQ(redo_logs.size(), thread_num * payload_num * batch_times + 1);


  delete iter1;
  delete wal2;
  for (auto& l : redo_logs) {
    delete l;
  }
}

TEST_F(TestTSWALTable, DeleteData) {
  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  k_uint32 row_num = 10;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, ts, &meta_);
  char* data_value2 = GenSomePayloadData(ctx_, row_num, p_len2, 1000, &meta_);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payload2, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = log_eg->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {1, ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num * 2);

  // delete
  Payload pd(getSchema(), getValidCols(), payload);
  std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

  uint64_t  count = 0;
  KwTsSpan span{1, ts + 10};
  span = ConvertMsToPrecision(span, ts_type);
  vector<DelRowSpan> rows;

  // use mtr id 0 for redo case.
  TS_LSN mtr_id = 0;
  s = log_eg->DeleteData(ctx_, primary_tag, 0, {span}, &rows, &count, mtr_id, false, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num);  // delete 12 rows
  // after delete
  ts_span = {1, ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num - 2);

  delete[] data_value;
  delete[] data_value2;
}

TEST_F(TestTSWALTable, putRollback) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  TS_LSN mtr_id = 0;
  ASSERT_EQ(log_eg->MtrBegin(ctx_, range_group_id_, 1, mtr_id), KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, &payload, 1, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), 10);

  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {start_ts, start_ts + 1000}), 0);

  delete[] data_value;
}

// put->begin->delete->rollback(undo delete)
TEST_F(TestTSWALTable, DeleteDataRollback) {
  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  k_uint32 row_num = 10;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, ts, &meta_);
  char* data_value2 = GenSomePayloadData(ctx_, row_num, p_len2, 1000, &meta_);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  auto* payloads = new vector<TSSlice>();
  payloads->push_back(payload);
  payloads->push_back(payload2);

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payloads->data(), 2, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t count = 0;
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span{1, ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num * 2);

  // delete
  Payload pd(getSchema(), getValidCols(), payload);
  std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

  TS_LSN mtr_id = 0;
  ASSERT_EQ(log_eg->MtrBegin(ctx_, range_group_id_, 1, mtr_id), KStatus::SUCCESS);
  count = 0;
  ts_span = {1, ts + 10,};;
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  vector<DelRowSpan> rows;
  // use mtr id 0 for redo case.
  s = log_eg->DeleteData(ctx_, primary_tag, 0, {ts_span}, &rows, &count, mtr_id, false, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num);  // delete

  // after delete
  ts_span = {1, ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num - 2);

  // rollback
  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num * 2);

  delete[] data_value;
  delete[] data_value2;
  delete payloads;
}

// begin->put1->put2->delete->put3->rollback(undo put3,undo delete,undo put2,undo put1,undo put1 tag)
TEST_F(TestTSWALTable, putDeleteRollback) {
  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  k_uint32 p_len3 = 0;
  k_uint32 row_num = 10;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, ts, &meta_);
  char* data_value2 = GenSomePayloadData(ctx_, row_num, p_len2, 1000, &meta_);
  char* data_value3 = GenSomePayloadData(ctx_, row_num, p_len3, ts + 1000, &meta_);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  TSSlice payload3{data_value3, p_len3};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  uint64_t count = 0;

  Payload pd(getSchema(), getValidCols(), payload);
  std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

  TS_LSN mtr_id = 0;
  ASSERT_EQ(log_eg->MtrBegin(ctx_, range_group_id_, 1, mtr_id), KStatus::SUCCESS);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payloads.data(), 2, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span{1, ts + 5000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);  
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num * 2);

  count = 0;
  // delete
  ts_span = {1, ts + 10,};;
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  vector<DelRowSpan> rows;
  // use mtr id 0 for redo case.
  s = log_eg->DeleteData(ctx_, primary_tag, 0, {ts_span}, &rows, &count, mtr_id, false, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num);  // delete
  ts_span = {1, ts + 5000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num - 2);

  // insert
  s = log_eg->PutData(ctx_, payload3, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num - 2 + row_num);

  // rollback
  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), 0);

  delete[] data_value;
  delete[] data_value2;
  delete[] data_value3;
}

// put->delete entity->rollback(undo delete entity)
TEST_F(TestTSWALTable, DeleteEntitiesRollback) {
  int row_num = 10;

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta_);

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  s = log_eg->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span{start_ts, start_ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num);

  Payload pd(getSchema(), getValidCols(), payload);
  std::vector<string> primary_tags;
  primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
  // delete entities
  uint64_t del_cnt;
  TS_LSN x_id = log_eg->FetchCurrentLSN();
  s = log_eg->DeleteEntities(ctx_, primary_tags, &del_cnt, x_id, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num);
  ts_span = {start_ts, start_ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), 0);
  ASSERT_EQ(log_eg->MtrRollback(ctx_, x_id), KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num);

  delete[] data_value;
}

// begin->put1->put2->delete entity->put3->rollback(undo put3,undo delete entity,undo put2,undo put1,undo put1 tag)
TEST_F(TestTSWALTable, putDeleteEntityRollback) {
  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  k_uint32 p_len3 = 0;
  k_uint32 row_num = 10;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len2, 1000, &meta_);
  char* data_value2 = GenSomePayloadData(ctx_, row_num, p_len, ts, &meta_);
  char* data_value3 = GenSomePayloadData(ctx_, row_num, p_len3, ts + 1000, &meta_);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  TSSlice payload3{data_value3, p_len3};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  Payload pd(getSchema(), getValidCols(), payload);

  TS_LSN mtr_id = 0;
  ASSERT_EQ(log_eg->MtrBegin(ctx_, range_group_id_, 1, mtr_id), KStatus::SUCCESS);

  // insert
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payloads.data(), 2, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span{1, ts + 5000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num * 2);

  // delete
  std::vector<string> primary_tags;
  primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
  // delete entities
  uint64_t del_cnt;
  s = log_eg->DeleteEntities(ctx_, primary_tags, &del_cnt, mtr_id, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num * 2);

  // insert
  s = log_eg->PutData(ctx_, payload3, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_span = {1, ts + 5000};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span, 2), row_num);

  // rollback
  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span, 2), 0);

  delete[] data_value;
  delete[] data_value2;
  delete[] data_value3;
}

// put1->flush->add wal(begin,put2,put3,commit)->recover(redo put1,redo put2,redo put3)
TEST_F(TestTSWALTable, putRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KwTsSpan ts_span = {start_ts, start_ts + 5000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);

  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);
  s = log_eg->PutData(ctx_, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  table_->FlushBuffer(ctx_);
  delete table_;

  TS_LSN mtr_id = 0;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  char* data_value2 = GenSomePayloadData(ctx_, 10, p_len, start_ts + 1000, &meta_);
  TSSlice payload2{data_value2, p_len};
  auto pd2 = Payload(getSchema(), getValidCols(), payload2);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload2);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TS_LSN entry_lsn;
  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd2.GetPrimaryTag(), payload2, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  char* data_value3 = GenSomePayloadData(ctx_, 10, p_len, start_ts + 2000, &meta_);
  TSSlice payload3{data_value3, p_len};
  auto pd3 = Payload(getSchema(), getValidCols(), payload3);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd3.GetPrimaryTag(), payload3, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = tsm.MtrCommit(ctx_, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  wal2->Flush(ctx_);
  delete wal2;

  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 30);

  delete[] data_value;
  delete[] data_value2;
  delete[] data_value3;
}

// add wal(begin,put,rollback)->recover(redo put,undo put)
TEST_F(TestTSWALTable, putRollbackRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);

  TSSlice payload{data_value, p_len};
  delete table_;
  table_ = nullptr;

  TS_LSN mtr_id = 0;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);
  auto lsn = wal2->FetchCurrentLSN();
  auto sc = getSchema();
  auto pd1 = Payload(sc, getValidCols(), payload);
  pd1.dedup_rule_ = DedupRule::KEEP;
  pd1.SetLsn(lsn);
  TS_LSN entry_lsn;
  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = tsm.MtrRollback(ctx_, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  wal2->Flush(ctx_);
  delete wal2;

  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);


  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 0);

  delete[] data_value;
}

// add wal(begin,put,delete tag,commit)->recover(redo put,redo delete tag)
TEST_F(TestTSWALTable, deleteEntitiesRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  SubGroupID group_id = 1;

  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);

  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);

  TS_LSN mtr_id = 0;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TS_LSN entry_lsn;
  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  string pt = string(pd1.GetPrimaryTag().data, pd1.GetPrimaryTag().len);
  const TSSlice tag_log{nullptr, 0};
  s = wal2->WriteDeleteTagWAL(ctx_, mtr_id, pt, group_id, entity_id, tag_log);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = tsm.MtrCommit(ctx_, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  wal2->Flush(ctx_);

  delete table_;
  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 0);

  delete[] data_value;
  delete wal2;
}

// put->checkpoint->add wal(begin,delete,commit)->recover(redo delete)
TEST_F(TestTSWALTable, deleteDataRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);
  TS_LSN mtr_id = 0;
  s = log_eg->PutData(ctx_, payload, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 10);
  table_->CreateCheckpoint(ctx_);

  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  string pt = string(pd1.GetPrimaryTag().data, pd1.GetPrimaryTag().len);
  std::vector<DelRowSpan> dtp_list;
  KwTsSpan span{1, start_ts + 1000};
  span = ConvertMsToPrecision(span, ts_type);
  s = log_eg->EvaluateDeleteData(ctx_, pt, {span}, dtp_list, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteDeleteMetricsWAL(ctx_, mtr_id, pt, {span}, dtp_list);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = tsm.MtrCommit(ctx_, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  wal2->Flush(ctx_);

  delete table_;
  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 0);

  delete[] data_value;
  delete wal2;
}

// add wal(begin,put,delete tag)->recover(redo put,redo delete tag)  begin.index=recover.index
TEST_F(TestTSWALTable, incompleteRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  SubGroupID group_id = 1;

  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);

  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);

  TS_LSN mtr_id = 0;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TS_LSN entry_lsn;
  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);

  string pt = string(pd1.GetPrimaryTag().data, pd1.GetPrimaryTag().len);
  const TSSlice tag_log{nullptr, 0};
  s = wal2->WriteDeleteTagWAL(ctx_, mtr_id, pt, group_id, entity_id, tag_log);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // remove tsm.MtrCommit(ctx_, mtr_id);

  wal2->Flush(ctx_);

  delete table_;
  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);
  log_eg->FlushBuffer(ctx_);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 0);

  std::vector<LogEntry*> wal_logs;

  auto wal3_ = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal3_->Init(ctx_);
  wal3_->ReadWALLogForMtr(mtr_id, wal_logs);
  ASSERT_EQ(wal_logs.size(), 1);

  for (auto& log : wal_logs) {
    delete log;
  }

  delete[] data_value;
  delete wal2;
  delete wal3_;
}

// put->checkpoint->add wal(begin,delete tag)->recover(redo delete tag,undo delete tag) begin.index>recover.index
TEST_F(TestTSWALTable, incompleteRollbackRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  TS_LSN mtr_id = 0;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);
  s = log_eg->PutData(ctx_, payload, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 10);
  table_->CreateCheckpoint(ctx_);

  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 2, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  string pt = string(pd1.GetPrimaryTag().data, pd1.GetPrimaryTag().len);

  std::vector<DelRowSpan> dtp_list;
  KwTsSpan span{1, start_ts + 1000};
  span = ConvertMsToPrecision(span, ts_type);
  s = log_eg->EvaluateDeleteData(ctx_, pt, {span}, dtp_list, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteDeleteMetricsWAL(ctx_, mtr_id, pt, {span}, dtp_list);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // remove tsm.MtrCommit(ctx_, mtr_id);

  wal2->Flush(ctx_);
  delete wal2;
  delete table_;

  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);
  log_eg->FlushBuffer(ctx_);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 0);

  std::vector<LogEntry*> wal_logs;

  auto wal3_ = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal3_->Init(ctx_);
  wal3_->ReadWALLogForMtr(mtr_id, wal_logs);
  ASSERT_EQ(wal_logs.size(), 0);

  for (auto& log : wal_logs) {
    delete log;
  }

  delete[] data_value;

  delete wal3_;
}

// put->flush->add wal(begin,delete,rollback)->recover(redo put,redo delete,undo delete)
TEST_F(TestTSWALTable, deleteRollbackRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  k_uint32 entity_id = 1;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  TS_LSN mtr_id = 0;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);
  // First insert 10 rows of data
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payload, mtr_id, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 10);

  // No checkpoint is performed, only wal is written, and the inserted data is repeatedly restored
  // table_->CreateCheckpoint(ctx_);
  table_->FlushBuffer(ctx_);


  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 2, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  string pt = string(pd1.GetPrimaryTag().data, pd1.GetPrimaryTag().len);

  std::vector<DelRowSpan> dtp_list;
  KwTsSpan span{1, start_ts + 1000};
  s = log_eg->EvaluateDeleteData(ctx_, pt, {span}, dtp_list, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteDeleteMetricsWAL(ctx_, mtr_id, pt, {span}, dtp_list);
  EXPECT_EQ(s, KStatus::SUCCESS);

  // rollback after delete
  tsm.MtrRollback(ctx_, mtr_id);

  wal2->Flush(ctx_);

  std::vector<LogEntry*> wal_logs;
  wal2->ReadWALLogForMtr(mtr_id, wal_logs);
  ASSERT_EQ(wal_logs.size(), 3);
  for (auto& log : wal_logs) {
    delete log;
  }
  wal_logs.clear();

  // Indicates the total number of logs. Two logs are duplicate data
  wal2->ReadWALLog(wal_logs, wal2->FetchCheckpointLSN(), wal2->FetchCurrentLSN());
  ASSERT_EQ(wal_logs.size(), 5);

  delete wal2;
  delete table_;

  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);
  log_eg->FlushBuffer(ctx_);
  // rollback after delete, there should be 10 rows
  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 10);

  for (auto& log : wal_logs) {
    delete log;
  }

  delete[] data_value;
}

/*TEST_F(TestTSWALTable, lockEntityCache) {
  std::vector<std::thread> threads;

  threads.emplace_back([=] {
    kwdbContext_t ctx1;
    KStatus s = InitServerKWDBContext(&ctx1);
    std::vector<TagInfo> tag_schema = getTagSchema();

    for (size_t i = 0; i < 100; i++) {
      std::shared_ptr<TsEntityGroup> table_range;
      table_->CreateEntityGroup(&ctx1, {1, 0}, tag_schema, &table_range);

      table_->DropEntityGroup(&ctx1, 1, true);
    }
  });

  threads.emplace_back([=] {
    kwdbContext_t ctx1;
    KStatus s = InitServerKWDBContext(&ctx1);
    for (size_t i = 0; i < 100; i++) {
      table_->CreateCheckpoint(&ctx1);
    }
  });

  for (auto& thread : threads) {
    thread.join();
  }
}*/
// add wal(begin,put,update tag,commit)->recover(redo put,redo update tag)
TEST_F(TestTSWALTable, putEntityRecover) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  KTimestamp start_ts = 1700000000;
  KwTsSpan ts_span = {start_ts, start_ts + 1000};
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);

  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_, 10, 0, false);

  TSSlice payload{data_value, p_len};
  auto pd1 = Payload(getSchema(), getValidCols(), payload);

  TS_LSN mtr_id = 0;
  auto wal2 = new WALMgr(kDbPath + "/", table_id_, range_group_id_, &opt_);
  wal2->Init(ctx_);

  auto tsm = TSxMgr(wal2);
  s = tsm.MtrBegin(ctx_, range_group_id_, 1, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload);
  EXPECT_EQ(s, KStatus::SUCCESS);

  TS_LSN entry_lsn;
  s = wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd1.GetPrimaryTag(), payload, entry_lsn);
  EXPECT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;
  int tag_add = 1;
  char* data_value1 = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_, 10, tag_add, false);
  TSSlice payload1{data_value1, p_len};
  s = wal2->WriteUpdateWAL(ctx_, mtr_id, 0, 0, payload1, payload1);
  EXPECT_EQ(s, KStatus::SUCCESS);

  s = tsm.MtrCommit(ctx_, mtr_id);
  EXPECT_EQ(s, KStatus::SUCCESS);

  wal2->Flush(ctx_);

  delete table_;
  table_ = new LoggedTsTable(ctx_, kDbPath, table_id_, &opt_);
  std::unordered_map<uint64_t, int8_t> ranges;
  table_->Init(ctx_, ranges);
  s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  std::map<uint64_t, uint64_t> applied_indexes;
  applied_indexes.insert(std::pair<uint64_t, uint64_t>(range_group_id_, 1));
  ASSERT_EQ(log_eg->Recover(ctx_, applied_indexes), KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(table_id_, ranges_, {ts_span}), 10);
  log_eg->HashRange().typ = 0;
  // tagiterator
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  std::vector<k_uint32> hps;
  make_hashpoint(&hps);
  TagIterator *iter;
  ASSERT_EQ(table_->GetTagIterator(ctx_, scan_tags,hps, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tag_val = 0;

  ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  // check entityid
  for (int idx = 0; idx < entity_id_list.size(); idx++) {
    ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
    memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
    ASSERT_EQ(ptag, start_ts);
  }
  // check tagcolumn
  for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
    for (int i = 0; i < res.data[tag_idx][0]->count; i++) {
      if (tag_idx == 0) {
        memcpy(&tag_val, res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
        ASSERT_EQ(tag_val, start_ts + tag_add);
      } else {
        // var
        char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
        ASSERT_EQ(rec_ptr, std::to_string(start_ts + tag_add));
      }
    }
  }
  entity_id_list.clear();
  res.clear();

  iter->Close();
  delete iter;

  delete[] data_value1;
  delete wal2;
}

// put->update tag->rollback(undo update tag)
TEST_F(TestTSWALTable, putEntityRollback) {
  int row_num = 10;

  KTimestamp start_ts = 3600000;
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts, &meta_, 10, 0, false);

  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  TSSlice payload{data_value, p_len};
  s = log_eg->PutData(ctx_, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  auto ts_type = table_->GetRootTableManager()->GetTsColDataType();
  KwTsSpan ts_span{start_ts, start_ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num);
  // tagiterator
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  TagIterator *iter;
  std::vector<k_uint32> hps;
  make_hashpoint(&hps);
  ASSERT_EQ(table_->GetTagIterator(ctx_, scan_tags, hps,&iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 tag_val = 0;

  ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  // check entityid
  for (int idx = 0; idx < entity_id_list.size(); idx++) {
    ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
    memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
    ASSERT_EQ(ptag, start_ts);
  }
  // check tagcolumn
  for (int tagidx = 0; tagidx < scan_tags.size(); tagidx++) {
    for (int i = 0; i < res.data[tagidx][0]->count; i++) {
      if (tagidx == 0) {
        memcpy(&tag_val, res.data[tagidx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
        ASSERT_EQ(tag_val, start_ts);
      } else {
        // var
        char* rec_ptr = static_cast<char*>(res.data[tagidx][0]->getVarColData(i));
        ASSERT_EQ(rec_ptr, std::to_string(start_ts));
      }
    }
  }
  entity_id_list.clear();
  res.clear();
  iter->Close();
  delete iter;

  Payload pd(getSchema(), getValidCols(), payload);
  std::vector<string> primary_tags;
  primary_tags.emplace_back(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);
  // put entity
  TS_LSN mtr_id = 0;
  ASSERT_EQ(log_eg->MtrBegin(ctx_, range_group_id_, 1, mtr_id), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;
  char* data_value1 = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_, 10, 1, false);
  TSSlice payload1{data_value1, p_len};
  s = log_eg->PutEntity(ctx_, payload1, mtr_id, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ts_span = {start_ts, start_ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num);

  count = 0;
  TagIterator *iter1;
  ResultSet res1{(k_uint32) scan_tags.size()};
  // std::vector<k_uint32> hps = {0,1,2,3,4,5,6,7,8,9};
  ASSERT_EQ(table_->GetTagIterator(ctx_, scan_tags,hps, &iter1, 1), KStatus::SUCCESS);
  ASSERT_EQ(iter1->Next(&entity_id_list, &res1, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  for (int tagidx = 0; tagidx < scan_tags.size(); tagidx++) {
    for (int i = 0; i < res1.data[tagidx][0]->count; i++) {
      if (tagidx == 0) {
        memcpy(&tag_val, res1.data[tagidx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
        ASSERT_EQ(tag_val, start_ts + 1);
      } else {
        // var
        char* rec_ptr = static_cast<char*>(res1.data[tagidx][0]->getVarColData(i));
        ASSERT_EQ(rec_ptr, std::to_string(start_ts + 1));
      }
    }
  }
  entity_id_list.clear();
  res1.clear();

  delete[] data_value1;
  delete iter1;

  // rollback
  count = 0;
  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ts_span = {start_ts, start_ts + row_num * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(GetTableRows(table_id_, ranges_, ts_span), row_num);
  TagIterator *iter2;
  ResultSet res2{(k_uint32) scan_tags.size()};
  // std::vector<k_uint32> hps = {0,1,2,3,4,5,6,7,8,9};
  ASSERT_EQ(table_->GetTagIterator(ctx_, scan_tags,hps, &iter2, 1), KStatus::SUCCESS);
  ASSERT_EQ(iter2->Next(&entity_id_list, &res2, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  for (int tagidx = 0; tagidx < scan_tags.size(); tagidx++) {
    for (int i = 0; i < res2.data[tagidx][0]->count; i++) {
      if (tagidx == 0) {
        memcpy(&tag_val, res2.data[tagidx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tag_val));
        ASSERT_EQ(tag_val, start_ts);
      } else {
        // var
        char* rec_ptr = static_cast<char*>(res2.data[tagidx][0]->getVarColData(i));
        ASSERT_EQ(rec_ptr, std::to_string(start_ts));
      }
    }
  }
  entity_id_list.clear();
  res2.clear();
  delete iter2;
}

TEST_F(TestTSWALTable, autoCheckpoint) {
  opt_.wal_file_in_group = 2;
  opt_.wal_file_size = 1;
  std::shared_ptr<TsEntityGroup> entity_group;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();

  // This UT is a stress test. For pipeline speed, payload_num is reduced to 300 by default
  uint32_t payload_num = 1;
  uint32_t row_num = 5000;
  vector<TSSlice> payloads;
  for (uint32_t i = 0; i < payload_num; i++) {
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(ctx_, row_num, p_len, start_ts + i * 100000, &meta_);
    TSSlice payload{data_value, p_len};
    payloads.push_back(payload);
  }

  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  s = log_eg->PutData(ctx_, payloads.data(), payloads.size(), 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  for (auto p : payloads) {
    delete[] p.data;
  }
}

TEST_F(TestTSWALTable, PartitionTierChangeRollback) {
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = table_->GetEntityGroup(ctx_, range_group_id_, &entity_group);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<LoggedTsEntityGroup> log_eg = std::static_pointer_cast<LoggedTsEntityGroup>(entity_group);

  char path[4096];
  getcwd(path, 4096);
  std::string abs_path(path);
  string link_path = abs_path + "/link_path";
  string tier_path = abs_path + "/tier_path";
  string old_tier_path = abs_path + "/old_tier_path";
  ASSERT_TRUE(MakeDirectory(tier_path));
  ASSERT_TRUE(MakeDirectory(old_tier_path));
  std::string link_cmd = "ln -s " + old_tier_path + " " + link_path;
  ASSERT_TRUE(System(link_cmd));

  uint64_t mtr_id;
  s = log_eg->BeginPartitionTierChangeMtr(ctx_, 1, link_path, tier_path, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // rollback
  ASSERT_EQ(log_eg->MtrRollback(ctx_, mtr_id), KStatus::SUCCESS);
  ASSERT_EQ(access(tier_path.c_str(), 0), -1);
  ASSERT_EQ(access(link_path.c_str(), 0), 0);
  ASSERT_EQ(access(old_tier_path.c_str(), 0), 0);
}
