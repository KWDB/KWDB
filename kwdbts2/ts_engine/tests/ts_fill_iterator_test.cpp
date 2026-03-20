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

#include <cstdio>
#include "test_util.h"
#include "ts_engine.h"
#include "ts_lru_block_cache.h"
#include "ts_table.h"
#include <atomic>

using namespace kwdbts;

const string engine_root_path = "./tsdb";
extern atomic<int> destroyed_entity_block_file_count;
extern atomic<int> created_entity_block_file_count;

class TestFillIterator : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineImpl *engine_;
  kwdbContext_t g_ctx_;
  kwdbContext_p ctx_;

  virtual void SetUp() override {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  virtual void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

 public:
  TestFillIterator() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineImpl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestFillIterator() {
    if (engine_) {
      delete engine_;
    }
  }
  std::string GetPrimaryKey(TSTableID table_id, TSEntityID dev_id) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
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

TEST_F(TestFillIterator, NONE) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  auto payload = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {start_ts, start_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::NONE;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    KwTsSpan ts_span1 = {INT64_MIN, start_ts - 1};
    ts_span1 = ConvertMsToPrecision(ts_span1, ts_col_type);
    KwTsSpan ts_span2 = {start_ts + 1, INT64_MAX};
    ts_span2 = ConvertMsToPrecision(ts_span2, ts_col_type);
    ts_spans = {ts_span1, ts_span2};

    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestFillIterator, EXACT) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  auto payload = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {start_ts, start_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::EXACT;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));
    bool is_null = false;
    res.data[1][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, false);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    ts_span = {start_ts - 1, start_ts - 1};
    ts_spans = {ts_span};
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts - 1, ts_col_type));
    is_null = false;
    res.data[1][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestFillIterator, PREVIOUS) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 before_fill_ts = 3600;
  std::vector<timestamp64> ts_vecs = {before_fill_ts - 1000, before_fill_ts - 300, before_fill_ts};
  std::vector<int> col1_value_vecs = {100, 200, 300};
  std::vector<bool> col1_isnull_vecs = {true, false, false};
  std::vector<double> col2_value_vecs = {1.0, 2.0, 3.0};
  std::vector<bool> col2_isnull_vecs = {false, true, true};
  auto before_payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 3, ts_vecs, col1_value_vecs, col1_isnull_vecs, col2_value_vecs, col2_isnull_vecs);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &before_payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(before_payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    timestamp64 fill_ts = before_fill_ts;
    KwTsSpan ts_span = {fill_ts, fill_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::PREVIOUS;
    fill_params.before_range = 1000;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 300);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 1.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_ts = before_fill_ts - 1;
    ts_span = {fill_ts, fill_ts};
    ts_spans = {ts_span};
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 200);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 1.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestFillIterator, NEXT) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 after_fill_ts = 3600;
  std::vector<timestamp64> ts_vecs = {after_fill_ts, after_fill_ts + 200, after_fill_ts + 800};
  std::vector<int> col1_value_vecs = {100, 200, 300};
  std::vector<bool> col1_isnull_vecs = {true, false, false};
  std::vector<double> col2_value_vecs = {1.0, 2.0, 3.0};
  std::vector<bool> col2_isnull_vecs = {false, true, false};
  auto after_payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 3, ts_vecs, col1_value_vecs, col1_isnull_vecs, col2_value_vecs, col2_isnull_vecs);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &after_payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(after_payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    timestamp64 fill_ts = after_fill_ts;
    KwTsSpan ts_span = {fill_ts, fill_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::NEXT;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 200);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 1.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_ts = after_fill_ts + 1;
    ts_span = {fill_ts, fill_ts};
    ts_spans = {ts_span};
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 200);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 3.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_ts = after_fill_ts + 1;
    ts_span = {fill_ts, fill_ts};
    ts_spans = {ts_span};
    fill_params.after_range = 500;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 200);
    bool is_null = false;
    res.data[2][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestFillIterator, CLOSER) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 fill_ts = 3600;
  std::vector<timestamp64> ts_vecs = {fill_ts - 1000, fill_ts - 100, fill_ts, fill_ts + 200, fill_ts + 800};
  std::vector<int> col1_value_vecs = {100, 200, 300, 400, 500};
  std::vector<bool> col1_isnull_vecs = {false, true, false, false, false};
  std::vector<double> col2_value_vecs = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::vector<bool> col2_isnull_vecs = {false, true, false, true, true};
  auto after_payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 5, ts_vecs, col1_value_vecs, col1_isnull_vecs, col2_value_vecs, col2_isnull_vecs);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &after_payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(after_payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {fill_ts, fill_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::CLOSER;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 300);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 3.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    uint64_t tmp_count;
    uint64_t p_tag_entity_id = 1;
    std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);
    s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{fill_ts, fill_ts}}, &tmp_count, 0, 1, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 400);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 1.0);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_params.before_range = 300;
    fill_params.after_range = 300;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 400);
    bool is_null = false;
    res.data[2][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestFillIterator, CONSTANT) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  auto payload = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {start_ts - 1, start_ts - 1};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::CONSTANT;
    fill_params.const_data_type = DATATYPE::INT32;
    fill_params.const_data_value = "123";

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts - 1, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 123);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 123);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_params.const_data_type = DATATYPE::INT64;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts - 1, ts_col_type));
    bool is_null = false;
    res.data[1][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 123);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_params.const_data_type = DATATYPE::CHAR;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts - 1, ts_col_type));
    is_null = false;
    res.data[1][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);
    is_null = false;
    res.data[2][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;
  }
}

TEST_F(TestFillIterator, LINEAR) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id);
  std::shared_ptr<TsTable> ts_table;
  auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool is_dropped = false;
  s = engine_->GetTsTable(ctx_, table_id, ts_table, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
  ASSERT_EQ(s , KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 fill_ts = 3600;
  std::vector<timestamp64> ts_vecs = {fill_ts - 1000, fill_ts - 100, fill_ts + 200, fill_ts + 800};
  std::vector<int> col1_value_vecs = {100, 200, 400, 500};
  std::vector<bool> col1_isnull_vecs = {false, true, false, false};
  std::vector<double> col2_value_vecs = {1.0, 2.0, 4.0, 5.0};
  std::vector<bool> col2_isnull_vecs = {false, true, true, false};
  auto after_payload = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 4, ts_vecs, col1_value_vecs, col1_isnull_vecs, col2_value_vecs, col2_isnull_vecs);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &after_payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(after_payload.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {fill_ts, fill_ts};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    fill_params.fill_type = FillType::LINEAR;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 350);
    ASSERT_EQ(KDouble64(res.data[2][0]->mem), 3.22);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_params.before_range = 1000;
    fill_params.after_range = 500;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    ASSERT_EQ(KInt32(res.data[1][0]->mem), 350);
    bool is_null = false;
    res.data[2][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;

    fill_params.before_range = 500;
    fill_params.after_range = 500;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(fill_ts, ts_col_type));
    is_null = false;
    res.data[1][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);
    is_null = false;
    res.data[2][0]->isNull(0, &is_null);
    ASSERT_EQ(is_null, true);

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    res.clear();
    delete ts_iter;
  }
}