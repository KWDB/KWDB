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

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{101, 0};

class TestIterator : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestIterator() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_), KStatus::SUCCESS);
  }

  ~TestIterator() {
    // CLOSE engine
    // ts_engine_->Close();
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  // Store in header
  int row_num_ = 5;
};

// Test the basic functions of TsStorageIterator
TEST_F(TestIterator, basic) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  TSSlice payload1{data_value, p_len};

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts1, start_ts1 + 2 * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));

  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  KTimestamp start_ts2 = 10000 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts2, &meta);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  TsStorageIterator* iter2;
  ts_span = {start_ts1, start_ts2 + 2 * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  is_finished = false;
  ResultSet res1{(k_uint32) scan_cols.size()}, res2{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter2->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 5);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts2, ts_type));

  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;
  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test unordered data
TEST_F(TestIterator, disorder) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1001;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000, disorder_ts = 7200 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 20;
  // Cross write unordered data
  for (int i = 0; i < write_count; ++i) {
    if (i % 2 == 0) {
      data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts + i, &meta);
    } else {
      // Write unordered data
      data_value = GenSomePayloadData(ctx_, 1, p_len, disorder_ts + i, &meta);
    }
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + write_count * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols,
                                   scan_agg_types, 1, &iter, tbl_range, {}, false, false), KStatus::SUCCESS);

  k_uint32 count;
  for (int i = 0; i < write_count / 2; ++i) {
    ResultSet res{(k_uint32) scan_cols.size()};
    bool is_finished = false;
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts + 2 * i, ts_type));
  }
  // read completed
  ResultSet res{(k_uint32) scan_cols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Write data from multiple partitions and meta blocks
TEST_F(TestIterator, multi_partition) {
  // Increase partition time
  kwdbts::EngineOptions::iot_interval = 360000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1002;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;

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
    data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta, 1);
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  KTimestamp start_ts = kwdbts::EngineOptions::iot_interval * 1000;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, (partition_num + 1) * start_ts};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols,
                                   scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false), KStatus::SUCCESS);

  k_uint32 count;
  // First partition table data
  for (int i = 0; i < block_item_max; ++i) {
    ResultSet res{(k_uint32) scan_cols.size()};
    bool is_finished = false;
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, block_item_row_max);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(kwdbts::EngineOptions::iot_interval * 1000 + i * block_item_row_max, ts_type));
  }
  ResultSet res1{(k_uint32) scan_cols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem),
            convertMSToPrecisionTS(kwdbts::EngineOptions::iot_interval * 1000 + block_item_max * block_item_row_max, ts_type));

  // Second partition table data
  for (int i = 0; i < block_item_max; ++i) {
    ResultSet res{(k_uint32) scan_cols.size()};
    bool is_finished = false;
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, block_item_row_max);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem),
          convertMSToPrecisionTS(2 * kwdbts::EngineOptions::iot_interval * 1000 + i * block_item_row_max, ts_type));
  }
  ResultSet res2{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem),
            convertMSToPrecisionTS(2 * kwdbts::EngineOptions::iot_interval * 1000 + block_item_max * block_item_row_max, ts_type));

  ResultSet res3{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test reading aggregation results
TEST_F(TestIterator, aggregation1) {
  size_t block_item_row_max = 1000;
  kwdbts::EngineOptions::iot_interval = 12000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  KTimestamp start_ts1 = 7200 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts1, start_ts1 + 2 * row_num_};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types1 = {Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::SUM, Sumfunctype::COUNT};

  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem),
          convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16((res1.data)[1][0]->mem), 11);
  ASSERT_EQ(KInt32((res1.data)[2][0]->mem), 2 * row_num_ * 2222);
  ASSERT_EQ(KInt16((res1.data)[3][0]->mem), 2 * row_num_);

  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  // Fill a blockitem
  KTimestamp start_ts2 = start_ts1 + 2 * row_num_;
  data_value = GenSomePayloadData(ctx_, block_item_row_max, p_len, start_ts2, &meta, 1);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // Write unordered data
  KTimestamp disorder_ts = 3600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, disorder_ts, &meta, 1);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // Continue writing sequential data
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, 10000 * 1000, &meta, 1);
  TSSlice payload4{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload4), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  //Verify read aggregation results
  TsStorageIterator* iter2;
  KwTsSpan ts_span2 = {start_ts1, 5 * start_ts1};
  ts_span2 = ConvertMsToPrecision(ts_span2, ts_type);
  std::vector<Sumfunctype> scan_agg_types2 = {Sumfunctype::MAX, Sumfunctype::COUNT, Sumfunctype::COUNT, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types2, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem),
            convertMSToPrecisionTS(10000 * 1000 + row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16(res2.data[1][0]->mem), block_item_row_max + 3 * row_num_);
  ASSERT_EQ(KInt16(res2.data[2][0]->mem), block_item_row_max + 3 * row_num_);
  ASSERT_EQ(KInt16(res2.data[3][0]->mem), block_item_row_max + 3 * row_num_);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  TsStorageIterator* iter3;
  KwTsSpan ts_span3 = {0, 12000 * 1000};
  ts_span3 = ConvertMsToPrecision(ts_span3, ts_type);
  std::vector<Sumfunctype> scan_agg_types3 = {Sumfunctype::MIN, Sumfunctype::MAX, Sumfunctype::MAX, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols, scan_cols, scan_agg_types3, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), convertMSToPrecisionTS(disorder_ts, ts_type));
  ASSERT_EQ(KInt16(res3.data[1][0]->mem), 11);
  ASSERT_EQ(KInt32(res3.data[2][0]->mem), 2222);
  ASSERT_EQ(KInt16(res3.data[3][0]->mem), block_item_row_max + 4 * row_num_);

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  TsStorageIterator* iter4;
  KwTsSpan ts_span4 = {0, 12000 * 1000};
  ts_span4 = ConvertMsToPrecision(ts_span4, ts_type);
  std::vector<Sumfunctype> scan_agg_types4 = {Sumfunctype::FIRST, Sumfunctype::LAST, Sumfunctype::FIRST, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span4}, ts_type, scan_cols, scan_cols, scan_agg_types4, 1, &iter4, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res4{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res4.data[0][0]->mem), convertMSToPrecisionTS(disorder_ts, ts_type));
  ASSERT_EQ(KInt16(res4.data[1][0]->mem), 11);
  ASSERT_EQ(KInt32(res4.data[2][0]->mem), 2222);
  ASSERT_EQ(KInt16(res4.data[3][0]->mem), block_item_row_max + 4 * row_num_);

  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter4;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test reading the same column aggregation result
TEST_F(TestIterator, aggregation2) {
  size_t block_item_row_max = 1000;
  kwdbts::EngineOptions::iot_interval = 12000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{{101, 0}, {201, 0}};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  KTimestamp start_ts1 = 7200 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts1, start_ts1 + 2 * row_num_};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 0, 0};
  std::vector<Sumfunctype> scan_agg_types1 = {Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::SUM};

  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(KTimestamp((res1.data)[1][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ((res1.data)[2][0]->mem, nullptr);
  ASSERT_EQ((res1.data)[2][0]->count, 0);

  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  // Fill a blockitem
  KTimestamp start_ts2 =  start_ts1 + 2 * row_num_;
  data_value = GenSomePayloadData(ctx_, block_item_row_max, p_len, start_ts2, &meta, 1);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;

  // Write unordered data
  KTimestamp disorder_ts = 3600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, disorder_ts, &meta, 1);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value;

  // Verify aggregation results
  TsStorageIterator* iter2;
  KwTsSpan ts_span2 = {start_ts1, 5 * start_ts1};
  ts_span2 = ConvertMsToPrecision(ts_span2, ts_type);
  std::vector<Sumfunctype> scan_agg_types2 = {Sumfunctype::LAST, Sumfunctype::LAST_ROW, Sumfunctype::FIRST};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types2, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ + block_item_row_max - 1, ts_type));
  ASSERT_EQ(KTimestamp(res2.data[1][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ + block_item_row_max - 1, ts_type));
  ASSERT_EQ(KTimestamp(res2.data[2][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  // Write null value
  KTimestamp start_ts3 = start_ts1 + 2 * row_num_ + block_item_row_max;
  data_value = GenPayloadDataWithNull(ctx_, row_num_, p_len, start_ts3, &meta, 1);
  TSSlice payload4{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload4), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter3;
  std::vector<k_uint32> scan_cols1 = {0, 2, 2};
  KwTsSpan ts_span3 = {0, 12000 * 1000};
  ts_span3 = ConvertMsToPrecision(ts_span3, ts_type);
  std::vector<Sumfunctype> scan_agg_types3 = {Sumfunctype::LAST, Sumfunctype::LAST, Sumfunctype::LAST_ROW};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols1, scan_cols1, scan_agg_types3, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), convertMSToPrecisionTS(start_ts3 + row_num_ - 1, ts_type));
  ASSERT_EQ(KInt32(res3.data[1][0]->mem), 2222);
//  ASSERT_EQ(res3.data[1][0]->getTimeStamp(), start_ts1 + 2 * row_num_ + block_item_row_max - 1);
  ASSERT_EQ(res3.data[2][0]->mem, nullptr);
  ASSERT_EQ(res3.data[2][0]->count, 1);
//  ASSERT_EQ(res3.data[2][0]->getTimeStamp(), start_ts3 + row_num_ - 1);

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 201, &tbl_range), KStatus::SUCCESS);
  // Write sequential data
  data_value = GenPayloadDataWithNull(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload5{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload5), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter4;
  std::vector<Sumfunctype> scan_agg_types4 = {Sumfunctype::LAST, Sumfunctype::LAST_ROW, Sumfunctype::LAST};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols1, scan_cols1, scan_agg_types4, 1, &iter4, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res4{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res4.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(res4.data[1][0]->mem, nullptr);
  ASSERT_EQ(res4.data[1][0]->count, 1);
//  ASSERT_EQ(res4.data[1][0]->getTimeStamp(), start_ts1 + 2 * row_num_ - 1);
  bool is_null;
  ASSERT_EQ(res4.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, true);

  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter4;

  std::vector<Sumfunctype> scan_agg_types5 = {Sumfunctype::LASTTS, Sumfunctype::LASTROWTS, Sumfunctype::LASTTS};
  TsStorageIterator* iter5;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols1, scan_cols1, scan_agg_types5, 1, &iter5, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res5{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res5.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(res5.data[0][0]->count, 1);
  ASSERT_EQ(KTimestamp(res5.data[1][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(res5.data[1][0]->count, 1);
  ASSERT_EQ(res5.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, false);
  ASSERT_EQ(res5.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, true);

  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter5;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test the results of sum calculation overflow
TEST_F(TestIterator, sum_overflow) {
  roachpb::CreateTsTable meta;
  kwdbts::EngineOptions::iot_interval = 3600;

  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  TSSlice payload;
  k_uint32 p_len = 0;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);
  payload = {data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;

  KTimestamp start_ts2 = 7200 * 1000;
  data_value = GenSomePayloadDataWithBigValue(ctx_, row_num_, p_len, start_ts2, &meta);
  payload = {data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types1 = {Sumfunctype::MAX, Sumfunctype::SUM, Sumfunctype::SUM};

  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts2 + (row_num_ - 1) * 10, ts_type));
  ASSERT_EQ((res1.data)[1][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res1.data)[1][0]->mem), ((k_int64)INT16_MAX / row_num_ + 1) * row_num_ + 11 * row_num_);
  ASSERT_EQ((res1.data)[2][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res1.data)[2][0]->mem), ((k_int64)INT32_MAX / row_num_ + 1) * row_num_ + 2222 * row_num_);

  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  KTimestamp start_ts3 = 10800 * 1000;
  data_value = GenSomePayloadDataWithBigValue(ctx_, 2, p_len, start_ts3, &meta);
  payload = {data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts3 + 10, ts_type));
  ASSERT_EQ((res2.data)[1][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res2.data)[1][0]->mem),
            ((k_int64)INT16_MAX / row_num_ + 1) * row_num_ + 11 * row_num_ + ((k_int64)INT16_MAX / 2 + 1) * 2);
  ASSERT_EQ((res2.data)[2][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res2.data)[2][0]->mem),
            ((k_int64)INT32_MAX / row_num_ + 1) * row_num_ + 2222 * row_num_ + ((k_int64)INT32_MAX / 2 + 1) * 2);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  KTimestamp start_ts4 = 14400 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts4, &meta);
  payload = {data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter3;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), convertMSToPrecisionTS(start_ts4 + 10 * (row_num_ - 1), ts_type));
  ASSERT_EQ((res3.data)[1][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res3.data)[1][0]->mem),
            ((k_int64)INT16_MAX / row_num_ + 1) * row_num_ + 2 * 11 * row_num_ + ((k_int64)INT16_MAX / 2 + 1) * 2);
  ASSERT_EQ((res3.data)[2][0]->is_overflow, false);
  ASSERT_EQ(KInt64((res3.data)[2][0]->mem),
            ((k_int64)INT32_MAX / row_num_ + 1) * row_num_ + 2 * 2222 * row_num_ + ((k_int64)INT32_MAX / 2 + 1) * 2);

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test first/last/last_row
TEST_F(TestIterator, last_row) {
  size_t block_item_row_max = 1000;
  kwdbts::EngineOptions::iot_interval = 12000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{{101, 0},
                                 {201, 0}};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);
  char* data_value;
  k_uint32 p_len = 0;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  // Query before writing data
  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::LAST, Sumfunctype::FIRST, Sumfunctype::LAST_ROW, Sumfunctype::COUNT};

  TsStorageIterator* iter{nullptr};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::FAIL);  // no subgroup
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  delete iter;

  // Write sequential data
  KTimestamp start_ts1 = 7200 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  ResultSet res1{(k_uint32) scan_cols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16((res1.data)[1][0]->mem), 11);
//  ASSERT_EQ((res1.data)[1][0]->getTimeStamp(), start_ts1);
  ASSERT_EQ(KInt32((res1.data)[2][0]->mem), 2222);
//  ASSERT_EQ((res1.data)[2][0]->getTimeStamp(), start_ts1 + 2 * row_num_ - 1);
  ASSERT_EQ(KInt16((res1.data)[3][0]->mem), 2 * row_num_);

  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  // Fill a blockitem
  KTimestamp start_ts2 = start_ts1 + 2 * row_num_;
  data_value = GenSomePayloadData(ctx_, block_item_row_max, p_len, start_ts2, &meta, 1);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // Write unordered data
  KTimestamp disorder_ts = 3600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, disorder_ts, &meta, 1);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // Verify aggregation results
  TsStorageIterator* iter2;
  KwTsSpan ts_span2 = {start_ts1, 5 * start_ts1};
  ts_span2 = ConvertMsToPrecision(ts_span2, ts_type);
  std::vector<Sumfunctype> scan_agg_types2 = {Sumfunctype::LAST, Sumfunctype::LAST_ROW, Sumfunctype::FIRST, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types2, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ + block_item_row_max - 1, ts_type));
  ASSERT_EQ(KInt16(res2.data[1][0]->mem), 11);
//  ASSERT_EQ(res2.data[1][0]->getTimeStamp(), start_ts1 + 2 * row_num_ + block_item_row_max - 1);
  ASSERT_EQ(KInt16(res2.data[2][0]->mem), 2222);
//  ASSERT_EQ(res2.data[2][0]->getTimeStamp(), start_ts1);
  ASSERT_EQ(KInt16(res2.data[3][0]->mem), block_item_row_max + 2 * row_num_);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  // Write null value
  KTimestamp start_ts3 = start_ts1 + 2 * row_num_ + block_item_row_max;
  data_value = GenPayloadDataWithNull(ctx_, row_num_, p_len, start_ts3, &meta, 1);
  TSSlice payload4{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload4), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  TsStorageIterator* iter3;
  KwTsSpan ts_span3 = {0, 12000 * 1000};
  ts_span3 = ConvertMsToPrecision(ts_span3, ts_type);
  std::vector<Sumfunctype> scan_agg_types3 = {Sumfunctype::LAST, Sumfunctype::LAST, Sumfunctype::LAST_ROW, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols, scan_cols, scan_agg_types3, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), convertMSToPrecisionTS(start_ts3 + row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16(res3.data[1][0]->mem), 11);
//  ASSERT_EQ(res3.data[1][0]->getTimeStamp(), start_ts3 + row_num_ - 1);
  ASSERT_EQ(res3.data[2][0]->mem, nullptr);
  ASSERT_EQ(res3.data[2][0]->count, 1);
//  ASSERT_EQ(res3.data[2][0]->getTimeStamp(), start_ts3 + row_num_ - 1);
  ASSERT_EQ(KInt16(res3.data[3][0]->mem), block_item_row_max + 3 * row_num_ + 2);

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  TsStorageIterator* iter4;
  std::vector<Sumfunctype> scan_agg_types4 = {Sumfunctype::LAST, Sumfunctype::LAST_ROW, Sumfunctype::LAST, Sumfunctype::COUNT};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols, scan_cols, scan_agg_types4, 1, &iter4, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res4{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res4.data[0][0]->mem), convertMSToPrecisionTS(start_ts3 + row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16(res4.data[1][0]->mem), 11);
//  ASSERT_EQ(res4.data[1][0]->getTimeStamp(), start_ts3 + row_num_ - 1);
  ASSERT_EQ(KInt16(res4.data[2][0]->mem), 2222);
  ASSERT_EQ(res4.data[2][0]->count, 1);
//  ASSERT_EQ(res4.data[2][0]->getTimeStamp(), start_ts3 - 1);
  ASSERT_EQ(KInt16(res4.data[3][0]->mem), block_item_row_max + 3 * row_num_ + 2);

  ASSERT_EQ(iter4->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter4;

  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 201, &tbl_range), KStatus::SUCCESS);
  // Write sequential data
  data_value = GenPayloadDataWithNull(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload5{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload5), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  TsStorageIterator* iter5;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types4, 1, &iter5, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res5{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res5.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(KInt16(res5.data[1][0]->mem), 11);
//  ASSERT_EQ(res5.data[1][0]->getTimeStamp(), start_ts1 + 2 * row_num_ - 1);
  bool is_null;
  ASSERT_EQ(res5.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, true);
  ASSERT_EQ(KInt16(res5.data[3][0]->mem), row_num_);

  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter5;

  std::vector<Sumfunctype> scan_agg_types5 = {Sumfunctype::LASTTS, Sumfunctype::LASTROWTS, Sumfunctype::LASTTS, Sumfunctype::COUNT};
  TsStorageIterator* iter6;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types5, 1, &iter6, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res6{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter6->Next(&res6, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res6.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(res6.data[0][0]->count, 1);
  ASSERT_EQ(KTimestamp(res6.data[1][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ - 1, ts_type));
  ASSERT_EQ(res6.data[1][0]->count, 1);
  ASSERT_EQ(res6.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, true);
  ASSERT_EQ(KInt16(res6.data[3][0]->mem), row_num_);

  ASSERT_EQ(iter6->Next(&res6, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter6;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test Special handling when only reading first/las
TEST_F(TestIterator, fast_first_last) {
  size_t block_item_row_max = 1000;
  kwdbts::EngineOptions::iot_interval = 3600;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1003;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{{101, 0},
                                 {201, 0}};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  // Write sequential data
  KTimestamp start_ts1 = 7200 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts1 + 2, start_ts1 + 5};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 2, 2};
  std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::LAST, Sumfunctype::LASTTS, Sumfunctype::LAST};
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 5, ts_type));
  ASSERT_EQ(KTimestamp((res.data)[1][0]->mem), convertMSToPrecisionTS(start_ts1 + 5, ts_type));
  ASSERT_EQ(KInt16((res.data)[2][0]->mem), 2222);
//  ASSERT_EQ((res.data)[2][0]->getTimeStamp(), start_ts1 + 5);

  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  // Fill a blockitem
  KTimestamp start_ts2 = start_ts1 + 2 * row_num_;
  data_value = GenSomePayloadData(ctx_, block_item_row_max, p_len, start_ts2, &meta, 1);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;

  // Write unordered data
  KTimestamp disorder_ts = 3600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, disorder_ts, &meta, 1);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value;

  // Verify aggregation results
  TsStorageIterator* iter2;
  KwTsSpan ts_span2 = {start_ts1, 5 * start_ts1};
  ts_span2 = ConvertMsToPrecision(ts_span2, ts_type);
  std::vector<Sumfunctype> scan_agg_types2 = {Sumfunctype::LAST, Sumfunctype::LASTROWTS, Sumfunctype::LASTTS};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types2, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  auto last_ts_precision = convertMSToPrecisionTS(start_ts1 + 2 * row_num_ + block_item_row_max - 1, ts_type);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), last_ts_precision);
  ASSERT_EQ(KTimestamp(res2.data[1][0]->mem), last_ts_precision);
  ASSERT_EQ(KTimestamp(res2.data[2][0]->mem), last_ts_precision);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  // Write null value
  KTimestamp start_ts3 = start_ts1 + 2 * row_num_ + block_item_row_max;
  data_value = GenPayloadDataWithNull(ctx_, row_num_, p_len, start_ts3, &meta, 1);
  TSSlice payload4{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload4), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter3;
  KwTsSpan ts_span3 = {0, 12000 * 1000};
  ts_span3 = ConvertMsToPrecision(ts_span3, ts_type);
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols, scan_cols, scan_agg_types2, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), convertMSToPrecisionTS(start_ts3 + row_num_ - 1, ts_type));
  ASSERT_EQ(KTimestamp(res3.data[1][0]->mem), convertMSToPrecisionTS(start_ts3 + row_num_ - 1, ts_type));
  ASSERT_EQ(KTimestamp(res3.data[2][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * row_num_ + block_item_row_max - 1, ts_type));

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  TsStorageIterator* iter4;
  std::vector<Sumfunctype> scan_agg_types4 = {Sumfunctype::FIRST, Sumfunctype::FIRSTROWTS, Sumfunctype::FIRSTTS};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span3}, ts_type, scan_cols, scan_cols, scan_agg_types4, 1, &iter4, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res4{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res4.data[0][0]->mem), convertMSToPrecisionTS(disorder_ts, ts_type));
  ASSERT_EQ(KTimestamp(res4.data[1][0]->mem), convertMSToPrecisionTS(disorder_ts, ts_type));
  ASSERT_EQ(KTimestamp(res4.data[2][0]->mem), convertMSToPrecisionTS(disorder_ts, ts_type));

  ASSERT_EQ(iter4->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter4;

  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, 201, &tbl_range), KStatus::SUCCESS);
  // Write sequential data
  data_value = GenPayloadDataWithNull(ctx_, 2 * row_num_, p_len, start_ts1, &meta, 1);
  TSSlice payload5{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload5), KStatus::SUCCESS);
  delete[] data_value;

  TsStorageIterator* iter5;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span2}, ts_type, scan_cols, scan_cols, scan_agg_types4, 1, &iter5, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res5{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res5.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(KTimestamp(res5.data[1][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  bool is_null;
  ASSERT_EQ(res5.data[2][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, true);

  ASSERT_EQ(iter5->Next(&res5, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter5;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test result after deleteData
TEST_F(TestIterator, delete_data) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1000;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600 * 1000;
  char* data_value1 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);
  TSSlice payload1{data_value1, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);

  KTimestamp start_ts2 = 1000;
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts2, &meta);
  TSSlice payload2{data_value2, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value2;
  data_value2 = nullptr;

  // before delete
  // Read raw data
  k_uint32 entity_id = 1;
  SubGroupID group_id = 1;
  KwTsSpan ts_span = {1, start_ts1 + row_num_ * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types,1 , &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count = 0;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  delete iter1;

  // Read aggregation result
  std::vector<Sumfunctype> scan_agg_types1 = {Sumfunctype::MIN, Sumfunctype::FIRST, Sumfunctype::COUNT, Sumfunctype::COUNT};
  TsStorageIterator* iter2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts2, ts_type));
  ASSERT_EQ(KInt16(res2.data[1][0]->mem), 11);
//  ASSERT_EQ(res2.data[1][0]->getTimeStamp(), start_ts2);
  ASSERT_EQ(KInt16(res2.data[2][0]->mem), 2 * row_num_);
  ASSERT_EQ(KInt16(res2.data[3][0]->mem), 2 * row_num_);
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter2;

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
  Payload pd(schema, actual_cols, payload1);
  std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

  // delete
  uint64_t delete_count = 0;
  KwTsSpan span{1, start_ts1 + 10};
  span = ConvertMsToPrecision(span, ts_type);
  ASSERT_EQ(tbl_range->DeleteData(ctx_, primary_tag, 0, {span}, nullptr, &delete_count, 0, false), KStatus::SUCCESS);
  ASSERT_EQ(delete_count, 2 + row_num_);  // delete 7 rows

  delete[] data_value1;
  data_value1 = nullptr;

  // after delete
  // Read raw data
  ResultSet res3{(k_uint32) scan_cols.size()};
  TsStorageIterator* iter3;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);  // left 3 rows
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  // Read aggregation result
  TsStorageIterator* iter4;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types1, 1, &iter4, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res4{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KTimestamp(res4.data[0][0]->mem), convertMSToPrecisionTS(start_ts1 + 2 * 10, ts_type));
  ASSERT_EQ(KInt16(res4.data[1][0]->mem), 11);
//  ASSERT_EQ(res4.data[1][0]->getTimeStamp(), start_ts1 + 2 * 10);
  ASSERT_EQ(KInt16(res4.data[2][0]->mem), 3);
  ASSERT_EQ(KInt16(res4.data[3][0]->mem), 3);
  ASSERT_EQ(iter4->Next(&res4, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter4;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Set ts_span range to maximum value
TEST_F(TestIterator, max_span) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1004;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  int partition_count = 5;
  for (int i = 1; i <= partition_count; ++i) {
    KTimestamp start_ts = i * TestBigTableInstance::iot_interval_ * 1000;
    char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);

    delete[] data_value;
    data_value = nullptr;
  }

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  k_uint32 count;
  for (int i = 1; i <= partition_count; ++i) {
    ResultSet res{(k_uint32) scan_cols.size()};
    bool is_finished = false;
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, row_num_);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(i * TestBigTableInstance::iot_interval_ * 1000, ts_type));
  }

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test iterator reading data from multiple ts spans
TEST_F(TestIterator, multi_spans) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1004;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  int partition_count = 5;
  for (int i = 1; i <= partition_count; ++i) {
    KTimestamp start_ts = i * TestBigTableInstance::iot_interval_ * 1000;
    char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  SubGroupID group_id = 1;
  k_uint32 entity_id = 1;
  timestamp64 ts_interval = TestBigTableInstance::iot_interval_ * 1000;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::vector<KwTsSpan> ts_spans1 = {{0, 2 * ts_interval}, {2 * ts_interval, 4 * ts_interval}, {4 * ts_interval, INT64_MAX}};
  std::vector<KwTsSpan> ts_spans;
  for (auto& ts_span : ts_spans1) {
    ts_spans.push_back(ConvertMsToPrecision(ts_span, ts_type));
  }
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_spans}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  k_uint32 count;
  for (int i = 1; i <= partition_count; ++i) {
    ResultSet res{(k_uint32) scan_cols.size()};
    bool is_finished = false;
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, row_num_);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(i * TestBigTableInstance::iot_interval_ * 1000, ts_type));
  }
  delete iter;

  ts_spans1 = {{2 * ts_interval + 10, 3 * ts_interval - 10}, {3 * ts_interval + 20, 4 * ts_interval - 10},
              {4 * ts_interval, 5 * ts_interval - 30}, {6 * ts_interval, 11 * ts_interval}};
  ts_spans.clear();
  for (auto& ts_span : ts_spans1) {
    ts_spans.push_back(ConvertMsToPrecision(ts_span, ts_type));
  }
  scan_agg_types = {Sumfunctype::FIRST, Sumfunctype::LAST, Sumfunctype::SUM, Sumfunctype::COUNT};
  TsStorageIterator* agg_iter;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_spans}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &agg_iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()};
  bool is_finished = false;
  ASSERT_EQ(agg_iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
//  ASSERT_EQ(res1.data[0][0]->getTimeStamp(), 2 * ts_interval + 10);
  ASSERT_EQ(KInt16(res1.data[1][0]->mem), 11);
  ASSERT_EQ(KInt32(res1.data[2][0]->mem), 12 * 2222);
  ASSERT_EQ(KInt16(res1.data[3][0]->mem), 12);

  delete agg_iter;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test null bitmap
TEST_F(TestIterator, null_bitmap) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1005;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 8600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  KTimestamp start_ts2 = 7600 * 1000;
  data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts2, &meta);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  KTimestamp start_ts3 = 9000 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts3, &meta);
  TSSlice payload3{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts1, start_ts3 + 2 * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(res1.data[0][0]->offset, 1);
  bool is_null;
  ASSERT_EQ(res1.data[0][0]->isNull(count - 1, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, false);
  ASSERT_EQ(res1.data[0][0]->isNull(count, &is_null), KStatus::FAIL);

  ResultSet res2{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts3, ts_type));
  ASSERT_EQ(res2.data[0][0]->offset, 2 * row_num_ + 1);

  ResultSet res3{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test var type
TEST_F(TestIterator, varchar) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1005;
  ConstructVarRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);
  TSSlice payload1{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);

  delete[] data_value;
  data_value = nullptr;

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3, 4};
  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);

  bool is_null;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  // varchar
  ASSERT_EQ(res1.data[2][0]->getVarColDataLen(0), test_str.size() + 1);
  void* var_data1 = res1.data[2][0]->getVarColData(0);
  ASSERT_EQ(memcmp(var_data1, test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(res1.data[2][0]->isNull(count - 1, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, false);
  // varbinary
  ASSERT_EQ(res1.data[4][0]->getVarColDataLen(0), test_str.size());
  void* var_data2 = res1.data[4][0]->getVarColData(0);
  ASSERT_EQ(memcmp(var_data2, test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(res1.data[4][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_EQ(is_null, false);

  ASSERT_EQ(iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;

  // Test Aggregation Query
  TsStorageIterator* iter2;
  scan_agg_types = {Sumfunctype::SUM, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::COUNT, Sumfunctype::MIN};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter2, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);

  ASSERT_EQ(res2.data[0][0]->count, 0);
  ASSERT_EQ(res2.data[1][0]->count, 0);
  ASSERT_EQ(res2.data[2][0]->count, 1);
  ASSERT_EQ(memcmp(res2.data[2][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(res2.data[3][0]->count, 1);
  ASSERT_EQ(KInt16(res2.data[3][0]->mem), row_num_);
  ASSERT_EQ(res2.data[4][0]->count, 1);
  ASSERT_EQ(memcmp(res2.data[4][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter2;

  // Test first/last
  TsStorageIterator* iter3;
  scan_cols = {2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4};
  scan_agg_types = {Sumfunctype::FIRST, Sumfunctype::FIRSTTS, Sumfunctype::FIRST_ROW, Sumfunctype::FIRSTROWTS,
                  Sumfunctype::LAST, Sumfunctype::LASTTS, Sumfunctype::LAST_ROW, Sumfunctype::LASTROWTS,
                  Sumfunctype::FIRST, Sumfunctype::FIRSTTS, Sumfunctype::FIRST_ROW, Sumfunctype::FIRSTROWTS,
                  Sumfunctype::LAST, Sumfunctype::LASTTS, Sumfunctype::LAST_ROW, Sumfunctype::LASTROWTS};
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter3, tbl_range, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res3{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(memcmp(res3.data[0][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[1][0]->mem),  convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(memcmp(res3.data[2][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[3][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(memcmp(res3.data[4][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[5][0]->mem), convertMSToPrecisionTS(start_ts1 + 10 * (row_num_ - 1), ts_type));
  ASSERT_EQ(memcmp(res3.data[6][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[7][0]->mem), convertMSToPrecisionTS(start_ts1 + 10 * (row_num_ - 1), ts_type));

  ASSERT_EQ(memcmp(res3.data[8][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[9][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(memcmp(res3.data[10][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[11][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));
  ASSERT_EQ(memcmp(res3.data[12][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[13][0]->mem), convertMSToPrecisionTS(start_ts1 + 10 * (row_num_ - 1), ts_type));
  ASSERT_EQ(memcmp(res3.data[14][0]->getVarColData(0), test_str.c_str(), test_str.size()), 0);
  ASSERT_EQ(KTimestamp(res3.data[15][0]->mem), convertMSToPrecisionTS(start_ts1 + 10 * (row_num_ - 1), ts_type));

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter3;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test TsSortedRowDataIterator
TEST_F(TestIterator, sorted_iter) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1001;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000, disorder_ts = 7200 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 20;
  for (int i = 0; i < write_count; ++i) {
    if (i % 2 == 0) {
      data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts + i, &meta);
    } else {
      data_value = GenSomePayloadData(ctx_, 1, p_len, disorder_ts + i, &meta);
    }
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  std::vector<k_uint32> scancols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scanaggtypes;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter, tbl_range, {}, false, true), KStatus::SUCCESS);

  k_uint32 count;
  bool is_finished = false;
  for (int i = 0; i < write_count / 2; ++i) {
    ResultSet res{(k_uint32) scancols.size()};
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(disorder_ts + 2 * i + 1, ts_type));
  }
  is_finished = false;
  for (int i = 0; i < write_count / 2; ++i) {
    ResultSet res{(k_uint32) scancols.size()};
    ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts + 2 * i, ts_type));
  }
  ResultSet res{(k_uint32) scancols.size()};
  is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test TsTableIterator basic function
TEST_F(TestIterator, tstable) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1006;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  TSSlice payload1{data_value, p_len};

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  KwTsSpan ts_span = {start_ts1, start_ts1 + 2 * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, scan_agg_types, 1, &iter1, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));

  ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  KTimestamp start_ts2 = 10000 * 1000;
  data_value = GenSomePayloadData(ctx_, 2 * row_num_, p_len, start_ts2, &meta);
  TSSlice payload2{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);

  delete[] data_value;
  data_value = nullptr;

  TsIterator* iter2;
  ts_span = {start_ts1, start_ts2 + 2 * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, scan_agg_types, 1, &iter2, {}, false, false),
            KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()}, res2{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter2->Next(&res1, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 5);
  ASSERT_EQ(KTimestamp(res1.data[0][0]->mem), convertMSToPrecisionTS(start_ts1, ts_type));

  ASSERT_EQ(iter2->Next(&res2, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 3);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), convertMSToPrecisionTS(start_ts2, ts_type));

  ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);

  delete iter1;
  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// Test the concurrent read function of TsTableIterator
TEST_F(TestIterator, multi_thread) {
  size_t block_item_row_max = 1000;
  kwdbts::EngineOptions::iot_interval = 12000;
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000, disorder_ts = 3600 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = block_item_row_max;
  // Cross write unordered data
  for (int i = 0; i < write_count; ++i) {
    if (i % 2 == 0) {
      data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts + i, &meta, 1);
    } else {
      // Write unordered data
      data_value = GenSomePayloadData(ctx_, 1, p_len, disorder_ts + i, &meta, 1);
    }
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);

    delete[] data_value;
    data_value = nullptr;
  }

  KwTsSpan ts_span = {start_ts, start_ts + write_count};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;
  TsIterator* iter;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, scan_agg_types, 1, &iter, {}, false, false), KStatus::SUCCESS);

  atomic<int> next_time{0};
  const int k_threads_num = 10;
  std::thread threads[k_threads_num];
  for (int i = 0; i < k_threads_num; i++) {
    threads[i] = std::thread([&](int n) {
      k_uint32 count;
      do {
        ResultSet res{(k_uint32) scan_cols.size()};
        ASSERT_EQ(iter->Next(&res, &count), KStatus::SUCCESS);
        usleep(1);
        if (count != 0) {
          next_time.fetch_add(1);
        }
      } while (count != 0);
    }, 0);
  }

  for (auto& th : threads) {
    th.join();
  }

  // Iterator data read completed
  ResultSet res{(k_uint32) scan_cols.size()};
  ASSERT_EQ(iter->Next(&res, &count), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  // All threads read a total of (write_count/2) times
  ASSERT_EQ(next_time.load(), write_count / 2);

  delete iter;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// disorder data count test
TEST_F(TestIterator, disorderCount) {
  roachpb::CreateTsTable meta;
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
  CLUSTER_SETTING_COUNT_USE_STATISTICS = true;
  KTableKey cur_table_id = 1001;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000, disorder_ts = 7200 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 200;
  // writer disorder data
  for (int i = 0; i < write_count; ++i) {
    if (i % 2 == 0) {
      data_value = GenSomePayloadData(ctx_, 1, p_len, start_ts + i, &meta);
    } else {
      data_value = GenSomePayloadData(ctx_, 1, p_len, disorder_ts + i, &meta);
    }
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + write_count * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scancols = {0};
  std::vector<Sumfunctype> scanaggtypes = {Sumfunctype::COUNT};
  TsStorageIterator* iter;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter, tbl_range, {}, false, true), KStatus::SUCCESS);

  k_uint32 count;
  ResultSet res{(k_uint32) scancols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count / 2);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter;

  ts_span = {0, INT64_MAX};
  TsStorageIterator* iter2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter2, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter2;

  // update the row_written of entity_item
  ErrorInfo err_info;
  ASSERT_EQ(tbl_range->Count(ctx_, err_info), KStatus::SUCCESS);
  ts_span = {start_ts, start_ts + write_count * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  TsStorageIterator* iter3;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter3, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count / 2);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter3;

  ts_span = {0, INT64_MAX};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  TsStorageIterator* iter4;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter4, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter4->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter4;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// multi partition count test
TEST_F(TestIterator, aggCount) {
  // decrease the interval of partition
  kwdbts::EngineOptions::iot_interval = 10;
  roachpb::CreateTsTable meta;
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
  CLUSTER_SETTING_COUNT_USE_STATISTICS = true;
  KTableKey cur_table_id = 1001;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 3000;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta);
  TSSlice payload{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // scan all data
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + write_count * 10};
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scancols = {0};
  std::vector<Sumfunctype> scanaggtypes = {Sumfunctype::COUNT};
  TsStorageIterator* iter;

  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter, tbl_range, {}, false, true), KStatus::SUCCESS);

  k_uint32 count;
  ResultSet res{(k_uint32) scancols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter;

  // scan partial data
  ts_span = {start_ts + 100 * 10, start_ts + 2500 * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  TsStorageIterator* iter2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter2, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), 2401);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter2;

  // update the row_written of entity_item
  ErrorInfo err_info;
  ASSERT_EQ(tbl_range->Count(ctx_, err_info), KStatus::SUCCESS);
  // scan partial data
  TsStorageIterator* iter3;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter3, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), 2401);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter3;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

// compress partition count test
TEST_F(TestIterator, compressCount) {
  // decrease the interval of partition
  kwdbts::EngineOptions::iot_interval = 10;
  roachpb::CreateTsTable meta;
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
  CLUSTER_SETTING_COUNT_USE_STATISTICS = true;
  KTableKey cur_table_id = 1011;
  ConstructRoachpbTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  char* data_value;
  k_uint32 p_len = 0;
  KTimestamp start_ts = 10000 * 1000;
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);

  int write_count = 1893;
  data_value = GenSomePayloadData(ctx_, write_count, p_len, start_ts, &meta);
  TSSlice payload{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  // update the row_written of entity_item
  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  ASSERT_EQ(tbl_range->Compress(ctx_, INT64_MAX, compressed_num, err_info), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->Count(ctx_, err_info), KStatus::SUCCESS);
  // scan all data
  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {0, INT64_MAX};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  std::vector<k_uint32> scancols = {0};
  std::vector<Sumfunctype> scanaggtypes = {Sumfunctype::COUNT};
  TsStorageIterator* iter;

  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter, tbl_range, {}, false, true), KStatus::SUCCESS);

  k_uint32 count;
  ResultSet res{(k_uint32) scancols.size()};
  bool is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), write_count);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter;

  // scan partial data
  ts_span = {start_ts + 100 * 10, start_ts + 1500 * 10};
  ts_span = ConvertMsToPrecision(ts_span, ts_type);
  TsStorageIterator* iter2;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, ts_type, scancols, scancols,
                                   scanaggtypes, 1, &iter2, tbl_range, {}, false, true), KStatus::SUCCESS);
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(KInt16(res.data[0][0]->mem), 1401);
  // Iterator data read completed
  res.clear();
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter2;
  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}