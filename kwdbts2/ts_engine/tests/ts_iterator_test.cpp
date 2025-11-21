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

#include "test_util.h"
#include "ts_engine.h"
#include "ts_table.h"

using namespace kwdbts;

const string engine_root_path = "./tsdb";
class TestV2Iterator : public ::testing::Test {
 public:
  EngineOptions opts_;
  TSEngineV2Impl *engine_;
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
  TestV2Iterator() {
    ctx_ = &g_ctx_;
    InitKWDBContext(ctx_);
    opts_.db_path = engine_root_path;
    Remove(engine_root_path);
    MakeDirectory(engine_root_path);
    engine_ = new TSEngineV2Impl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  ~TestV2Iterator() {
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

TEST_F(TestV2Iterator, basic) {
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
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1, start_ts);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
        if (!vgroup || vgroup->GetMaxEntityID() < 1) {
            continue;
        }
        TsStorageIterator* ts_iter;
        k_uint32 entity_id = 1;
        KwTsSpan ts_span = {start_ts, start_ts + 20};
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

        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false);
        ASSERT_EQ(s, KStatus::SUCCESS);

        ResultSet res{(k_uint32) scan_cols.size()};
        k_uint32 count;
        bool is_finished = false;
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));

        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 0);

        delete ts_iter;
    }
}

TEST_F(TestV2Iterator, mulitEntity) {
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
    KTimestamp interval = 100L;
    int entity_num = 30;
    int entity_row_num = 3;
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      TsStorageIterator* ts_iter;
      KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;

      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        std::vector<uint32_t> entity_ids = {entity_id};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter = {};
        std::vector<k_int32> agg_extend_cols = {};
        std::vector<timestamp64> ts_points = {};
        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                          scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                          schema, &ts_iter, vgroup, ts_points, false, false);
        ASSERT_EQ(s, KStatus::SUCCESS);
        ResultSet res{(k_uint32) scan_cols.size()};
        k_uint32 count;
        bool is_finished = false;
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, entity_row_num);
        ASSERT_EQ(KTimestamp(reinterpret_cast<char*>(res.data[0][0]->mem) + (entity_row_num -1) * 8) - KTimestamp(res.data[0][0]->mem), interval * (entity_row_num - 1));
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(count, 0);
        delete ts_iter;
      }
    }
}

TEST_F(TestV2Iterator, multiDBAndEntity) {
    TSTableID table_id = 999;
    int db_num = 3;
    std::shared_ptr<TsTable> ts_table;
    for (size_t i = 1; i <= db_num; i++) {
      roachpb::CreateTsTable pb_meta;
      ConstructRoachpbTable(&pb_meta, table_id, i);
      auto s = engine_->CreateTsTable(ctx_, table_id + i - 1, &pb_meta, ts_table);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    bool is_dropped = false;
    auto s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, table_schema_mgr);
    ASSERT_EQ(s , KStatus::SUCCESS);

    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    std::vector<TagInfo> tag_schema;
    s = table_schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s , KStatus::SUCCESS);

    timestamp64 start_ts = 3600;
    KTimestamp interval = 100L;
    int entity_num = db_num * 10;
    int entity_row_num = 3;
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id + i % db_num, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id + i % db_num, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    int entity_scan_num = 0;
    int entity_result_num = 0;
    for (size_t i = 0; i < db_num; i++) {
      s = engine_->GetTsTable(ctx_, table_id + i, ts_table, is_dropped, false);
      ASSERT_EQ(s , KStatus::SUCCESS);
      vector<EntityResultIndex> entity_store;
      s = dynamic_pointer_cast<TsTableV2Impl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, entity_store);
      ASSERT_EQ(s, KStatus::SUCCESS);
      entity_scan_num += entity_store.size();
      s = engine_->GetTableSchemaMgr(ctx_, table_id + i, is_dropped, table_schema_mgr);
      ASSERT_EQ(s , KStatus::SUCCESS);
      for (auto entity : entity_store) {
        TsStorageIterator* ts_iter;
        KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
        DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
        std::vector<k_uint32> scan_cols = {0, 1, 2};
        std::vector<Sumfunctype> scan_agg_types;
        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        std::vector<uint32_t> entity_ids = {entity.entityId};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter = {};
        std::vector<k_int32> agg_extend_cols = {};
        std::vector<timestamp64> ts_points = {};
        auto vgroup = engine_->GetTsVGroup(entity.subGroupId);
        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false);
        ASSERT_EQ(s, KStatus::SUCCESS);
        ResultSet res{(k_uint32) scan_cols.size()};
        k_uint32 count;
        bool is_finished = false;
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        if (count > 0) {
          ASSERT_EQ(count, entity_row_num);
          ASSERT_EQ(KTimestamp(reinterpret_cast<char*>(res.data[0][0]->mem) + (entity_row_num -1) * 8) - KTimestamp(res.data[0][0]->mem), interval * (entity_row_num - 1));
          ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
          ASSERT_EQ(count, 0);
          entity_result_num++;
        }
        delete ts_iter;
      }
    }
    ASSERT_EQ(entity_scan_num, entity_num);
    ASSERT_EQ(entity_result_num, entity_num);
}

TEST_F(TestV2Iterator, mulitEntityCount) {
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
  KTimestamp interval = 100L;
  int entity_num = 30;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  start_ts += 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        if (count_header.valid_count > 0) {
          ASSERT_EQ(count_header.valid_count, entity_row_num);
        }
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), 2 * entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
  }
}


TEST_F(TestV2Iterator, mulitEntityDeleteCount) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  KTimestamp interval = 100L;
  int entity_num = 30;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  start_ts += 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), 2 * entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, true);
        ASSERT_EQ(count_header.valid_count, 0);
        ASSERT_EQ(count_header.min_ts, INT64_MAX);
        ASSERT_EQ(count_header.max_ts, INT64_MAX);
      }
    }
  }

  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, true);
        ASSERT_EQ(count_header.valid_count, entity_row_num);
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), 2 * entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
  }
  uint64_t tmp_count;
  uint64_t p_tag_entity_id = 3;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);

  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts + entity_row_num / 2 * interval, INT64_MAX}},
                          &tmp_count, 0, 1, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto tag_table = table_schema_mgr->GetTagTable();
  uint32_t v_group_id, del_entity_id;
  ASSERT_TRUE(tag_table->hasPrimaryKey(p_key.data(), p_key.size(), del_entity_id, v_group_id));

  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{start_ts, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    auto partition = partitions[0];
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      auto count_info = partition->GetCountManager();
      TsEntityCountHeader count_header{};
      count_header.entity_id = entity_id;
      s = count_info->GetEntityCountHeader(&count_header);
      if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
        ASSERT_EQ(count_header.is_count_valid, false);
        ASSERT_EQ(count_header.valid_count, 0);
      } else {
        ASSERT_EQ(count_header.is_count_valid, true);
        ASSERT_EQ(count_header.valid_count, entity_row_num);
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
          ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num + entity_row_num / 2);
        } else {
          ASSERT_EQ(KInt16(res.data[0][0]->mem), 2 * entity_row_num);
        }
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }

    partitions.clear();
    partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (auto part : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = part->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id &&
        part->GetPartitionIdentifier() == partition->GetPartitionIdentifier()) {
          ASSERT_EQ(count_header.is_count_valid, true);
          ASSERT_EQ(count_header.valid_count, entity_row_num / 2);
        } else {
          ASSERT_EQ(count_header.is_count_valid, true);
          ASSERT_EQ(count_header.valid_count, entity_row_num);
        }
      }
    }
  }
}

TEST_F(TestV2Iterator, mulitEntityDeleteCountBeforeFlush) {
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

  const std::vector<AttributeInfo>* metric_schema;
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s , KStatus::SUCCESS);

  timestamp64 start_ts = 3600;
  KTimestamp interval = 100L;
  int entity_num = 30;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, is_dropped);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, true);
        ASSERT_EQ(count_header.valid_count, 0);
        ASSERT_EQ(count_header.min_ts, INT64_MAX);
        ASSERT_EQ(count_header.max_ts, INT64_MAX);
      }
    }
  }

  for (size_t i = 0; i < entity_num; i++) {
    uint64_t tmp_count;
    uint64_t p_tag_entity_id = 1 + i;
    std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);

    s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts + entity_row_num / 2 * interval, INT64_MAX}},
                            &tmp_count, 0, 1, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num / 2);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, false);
        ASSERT_EQ(count_header.valid_count, 0);
        ASSERT_EQ(count_header.min_ts, INT64_MAX);
        ASSERT_EQ(count_header.max_ts, INT64_MAX);
      }
    }
  }

  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, false);
        ASSERT_EQ(count_header.valid_count, 0);
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num / 2);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountHeader count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountHeader(&count_header);
        ASSERT_EQ(count_header.is_count_valid, true);
        ASSERT_EQ(count_header.valid_count, entity_row_num / 2);
        ASSERT_EQ(count_header.min_ts >= (start_ts + 1), true);
        ASSERT_EQ(count_header.max_ts <= (start_ts + entity_num + (entity_row_num / 2 - 1) * interval), true);
      }
    }
  }
}
