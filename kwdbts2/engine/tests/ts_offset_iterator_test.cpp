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

class TestOffsetIterator : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestOffsetIterator() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // Clean up file directory
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_), KStatus::SUCCESS);
  }

  ~TestOffsetIterator() {
    // CLOSE engine
    // ts_engine_->Close();
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  // Store in header
  int row_num_ = 5;
};

// 单分区 多分区 单设备 多设备 顺序乱序 正序逆序 极端值

TEST_F(TestOffsetIterator, basic) {
  roachpb::CreateTsTable meta;
//  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;

  KTableKey cur_table_id = 1000;
  ConstructOffsetTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts = 3600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, 10000, p_len, start_ts, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  TSSlice payload1{data_value, p_len};

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 5000,
      .limit = 10,
  };
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_GE(total_cnt, 5010);
  ASSERT_LE(total_cnt, 10000);
  ASSERT_GE(ts.size(), 10);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts + iter1->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts + total_cnt * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  // desc
  TsIterator* iter2;
  params.reverse = true;
  params.offset = 3000;
  params.limit = 50;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_GE(total_cnt,3010);
  ASSERT_LE(total_cnt, 10000);
  ASSERT_GE(ts.size(), 50);

  timestamp64 end_ts = start_ts + (10000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_LE(it, end_ts - iter2->GetFilterCount() * 10);
    ASSERT_GE(it, end_ts - total_cnt * 10);
  }

  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestOffsetIterator, multi_partition) {
  roachpb::CreateTsTable meta;
//  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;

  KTableKey cur_table_id = 1000;
  ConstructOffsetTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts1 = 3600 * 1000;
  char* data_value1 = GenSomePayloadData(ctx_, 10000, p_len, start_ts1, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  TSSlice payload1{data_value1, p_len};

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value1;
  data_value1 = nullptr;

  KTimestamp start_ts2 = 7200 * 1000;
  char* data_value2 = GenSomePayloadData(ctx_, 10000, p_len, start_ts2, &meta);
  TSSlice payload2{data_value2, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value2;
  data_value2 = nullptr;

  KTimestamp start_ts3 = 10800 * 1000;
  char* data_value3 = GenSomePayloadData(ctx_, 10000, p_len, start_ts3, &meta);
  TSSlice payload3{data_value3, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
  delete[] data_value3;
  data_value3 = nullptr;

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 15000,
      .limit = 10,
  };
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_GE(total_cnt, 15010);
  ASSERT_LE(total_cnt, 20000);
  ASSERT_GE(ts.size(), 10);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts2 + (iter1->GetFilterCount() - 10000) * 10);
    ASSERT_LE(it, start_ts2 + (total_cnt - 10000) * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  // desc
  TsIterator* iter2;
  params.reverse = true;
  params.offset = 23000;
  params.limit = 50;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2),KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_GE(total_cnt,23050);
  ASSERT_LE(total_cnt, 30000);
  ASSERT_GE(ts.size(), 50);

  timestamp64 end_ts = start_ts1 + (10000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_LE(it, end_ts - (iter2->GetFilterCount() - 20000) * 10);
    ASSERT_GE(it, end_ts - (total_cnt - 20000) * 10);
  }

  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestOffsetIterator, disorder) {
  roachpb::CreateTsTable meta;
//  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;

  KTableKey cur_table_id = 100;
  ConstructOffsetTable(&meta, "test_table", cur_table_id);

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
      data_value = GenSomePayloadData(ctx_, 100, p_len, start_ts + (i / 2) * 100 * 10, &meta);
    } else {
      // Write unordered data
      data_value = GenSomePayloadData(ctx_, 100, p_len, disorder_ts + (i / 2) * 100 * 10, &meta);
    }
    TSSlice payload{data_value, p_len};
    ASSERT_EQ(tbl_range->PutData(ctx_, payload), KStatus::SUCCESS);
    delete[] data_value;
    data_value = nullptr;
  }

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 500,
      .limit = 10,
  };
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res1{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res1, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res1.data[0][0]->mem + 16 * i)));
    }
    res1.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_GE(total_cnt, 510);
  ASSERT_LE(total_cnt, 2000);
  ASSERT_GE(ts.size(), 10);

  for (auto it : ts) {
    ASSERT_GE(it, disorder_ts + iter1->GetFilterCount() * 10);
    ASSERT_LE(it, disorder_ts + total_cnt * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  TsIterator* iter2;
  params.offset = 1500;
  params.limit = 20;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  ResultSet res2{(k_uint32) scan_cols.size()};
  do {
    ASSERT_EQ(iter2->Next(&res2, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res2.data[0][0]->mem + 16 * i)));
    }
    res2.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_GE(total_cnt, 1520);
  ASSERT_LE(total_cnt, 2000);
  ASSERT_GE(ts.size(), 20);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts + (iter2->GetFilterCount() - 1000) * 10);
    ASSERT_LE(it, start_ts + (total_cnt - 1000) * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter2;

  // desc
  TsIterator* iter3;
  ResultSet res3{(k_uint32) scan_cols.size()};
  params.reverse = true;
  params.offset = 1300;
  params.limit = 50;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter3), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter3->Next(&res3, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res3.data[0][0]->mem + 16 * i)));
    }
    res3.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter3->GetFilterCount();

  ASSERT_GE(total_cnt,1350);
  ASSERT_LE(total_cnt, 2000);
  ASSERT_GE(ts.size(), 50);

  timestamp64 end_ts = disorder_ts + (1000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_LE(it, end_ts - (iter3->GetFilterCount() - 1000) * 10);
    ASSERT_GE(it, end_ts - (total_cnt - 1000) * 10);
  }

  delete iter3;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

TEST_F(TestOffsetIterator, extreme) {
  roachpb::CreateTsTable meta;
//  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;

  KTableKey cur_table_id = 1000;
  ConstructOffsetTable(&meta, "test_table", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);

  k_uint32 p_len = 0;
  KTimestamp start_ts = 3600 * 1000;
  char* data_value = GenSomePayloadData(ctx_, 2, p_len, start_ts, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  TSSlice payload1{data_value, p_len};

  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
  delete[] data_value;
  data_value = nullptr;

  KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
  std::vector<Sumfunctype> scan_agg_types;

  // asc
  TsIterator* iter1;
  std::vector<EntityResultIndex> entity_results;
  k_uint32 count;
  std::vector<k_int32> agg_extend_cols;
  std::vector<BlockFilter> block_filter;
  std::vector<KwTsSpan> ts_spans = {ts_span};
  IteratorParams params = {
      .entity_ids = entity_results,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = 1,
      .limit = 1,
  };
  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_cols.size()};
  uint32_t total_cnt = 0;
  vector<timestamp64> ts;
  do {
    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter1->GetFilterCount();

  ASSERT_EQ(total_cnt, 2);
  ASSERT_GE(ts.size(), 1);

  for (auto it : ts) {
    ASSERT_GE(it, start_ts + iter1->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts + total_cnt * 10);
  }

  ts.clear();
  total_cnt = 0;
  delete iter1;

  KTimestamp start_ts2 = 3600 * 1000 + 20;
  char* data_value2 = GenSomePayloadData(ctx_, 10000, p_len, start_ts2, &meta);
  TSSlice payload2{data_value2, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
  delete[] data_value2;
  data_value2 = nullptr;

  TsIterator* iter2;
  params.offset = 10000;
  params.limit = 2;
  ASSERT_EQ(ts_table->GetIterator(ctx_, params, &iter2), KStatus::SUCCESS);

  do {
    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
    for (int i = 0; i < count; ++i) {
      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
    }
    res.clear();
    total_cnt += count;
  } while (count != 0);

  total_cnt += iter2->GetFilterCount();

  ASSERT_EQ(total_cnt,10002);
  ASSERT_GE(ts.size(), 2);

  timestamp64 end_ts = start_ts + (10000 - 1) * 10;
  for (auto it : ts) {
    ASSERT_GE(it, start_ts + iter2->GetFilterCount() * 10);
    ASSERT_LE(it, start_ts + total_cnt * 10);
  }

  delete iter2;

  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
}

//TEST_F(TestOffsetIterator, ts_span) {
//  roachpb::CreateTsTable meta;
//  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
//
//  KTableKey cur_table_id = 1000;
//  ConstructOffsetTable(&meta, "test_table", cur_table_id);
//
//  std::vector<RangeGroup> ranges{kTestRange};
//  ASSERT_EQ(ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges), KStatus::SUCCESS);
//
//  k_uint32 p_len = 0;
//  KTimestamp start_ts1 = 3600 * 1000;
//  char* data_value1 = GenSomePayloadData(ctx_, 10000, p_len, start_ts1, &meta);
//
//  std::shared_ptr<TsTable> ts_table;
//  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
//  TSSlice payload1{data_value1, p_len};
//
//  std::shared_ptr<TsEntityGroup> tbl_range;
//  ASSERT_EQ(ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range), KStatus::SUCCESS);
//  ASSERT_EQ(tbl_range->PutData(ctx_, payload1), KStatus::SUCCESS);
//  delete[] data_value1;
//  data_value1 = nullptr;
//
//  KTimestamp start_ts2 = 7200 * 1000;
//  char* data_value2 = GenSomePayloadData(ctx_, 10000, p_len, start_ts2, &meta);
//  TSSlice payload2{data_value2, p_len};
//  ASSERT_EQ(tbl_range->PutData(ctx_, payload2), KStatus::SUCCESS);
//  delete[] data_value2;
//  data_value2 = nullptr;
//
//  KTimestamp start_ts3 = 10800 * 1000;
//  char* data_value3 = GenSomePayloadData(ctx_, 10000, p_len, start_ts3, &meta);
//  TSSlice payload3{data_value3, p_len};
//  ASSERT_EQ(tbl_range->PutData(ctx_, payload3), KStatus::SUCCESS);
//  delete[] data_value3;
//  data_value3 = nullptr;
//
//  KwTsSpan ts_span = {4000 * 1000, 12000 * 1000};
//  std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
//  std::vector<Sumfunctype> scan_agg_types;
//
//  // asc
//  TsIterator* iter1;
//  std::vector<EntityResultIndex> entity_results;
//  k_uint32 count;
//  ASSERT_EQ(ts_table->GetEntityIndex(ctx_, 0, UINT64_MAX, entity_results), KStatus::SUCCESS);
//  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, {}, scan_agg_types, 1, &iter1, {}, false, false, 15000, 10),
//            KStatus::SUCCESS);
//
//  ResultSet res{(k_uint32) scan_cols.size()};
//  uint32_t total_cnt = 0;
//  vector<timestamp64> ts;
//  do {
//    ASSERT_EQ(iter1->Next(&res, &count), KStatus::SUCCESS);
//    for (int i = 0; i < count; ++i) {
//      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
//    }
//    res.clear();
//    total_cnt += count;
//  } while (count != 0);
//
//  total_cnt += iter1->GetFilterCount();
//
//  ASSERT_GE(total_cnt, 15010);
//  ASSERT_LE(total_cnt, 20000);
//  ASSERT_GE(ts.size(), 10);
//
//  for (auto it : ts) {
//    ASSERT_GE(it, start_ts2 + (iter1->GetFilterCount() - 10000) * 10);
//    ASSERT_LE(it, start_ts2 + (total_cnt - 10000) * 10);
//  }
//
//  ts.clear();
//  total_cnt = 0;
//  delete iter1;
//
//  // desc
//  TsIterator* iter2;
//  ASSERT_EQ(ts_table->GetIterator(ctx_, entity_results, {ts_span}, scan_cols, {}, scan_agg_types, 1, &iter2, {}, true, false, 23000, 50),
//            KStatus::SUCCESS);
//
//  do {
//    ASSERT_EQ(iter2->Next(&res, &count), KStatus::SUCCESS);
//    for (int i = 0; i < count; ++i) {
//      ts.emplace_back(KTimestamp(reinterpret_cast<void*>((intptr_t)res.data[0][0]->mem + 16 * i)));
//    }
//    res.clear();
//    total_cnt += count;
//  } while (count != 0);
//
//  total_cnt += iter2->GetFilterCount();
//
//  ASSERT_GE(total_cnt,23050);
//  ASSERT_LE(total_cnt, 30000);
//  ASSERT_GE(ts.size(), 50);
//
//  timestamp64 end_ts = start_ts1 + (10000 - 1) * 10;
//  for (auto it : ts) {
//    ASSERT_LE(it, end_ts - (iter2->GetFilterCount() - 20000) * 10);
//    ASSERT_GE(it, end_ts - (total_cnt - 20000) * 10);
//  }
//
//  delete iter2;
//
//  ASSERT_EQ(ts_table->DropAll(ctx_), KStatus::SUCCESS);
//}
