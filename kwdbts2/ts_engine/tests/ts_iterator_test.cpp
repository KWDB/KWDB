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
#include "ts_test_base.h"
#include "test_util.h"
#include "ts_engine.h"
#include "ts_lru_block_cache.h"
#include "ts_table.h"
#include <atomic>

using namespace kwdbts;

const string engine_root_path = "./tsdb";
extern atomic<int> destroyed_entity_block_file_count;
extern atomic<int> created_entity_block_file_count;

class TestV2Iterator : public TsEngineTestBase {
 public:
  TestV2Iterator(int vgroup_num = 4) {
    InitContext();
    EngineOptions::vgroup_max_num = vgroup_num;
    InitEngine(engine_root_path);
  }

  // Verify that the result is equal to expected
  void VerifyResult(TsStorageIterator* it, k_uint32 col_num, vector<DATATYPE>& col_type, vector<vector<vector<string>>>& expected) {
    k_uint32 count;
    bool is_finished = false;
    ResultSet res{col_num};
    for (int i = 0; i < expected.size(); ++i) {
      // Read the i-th tag's row data from iterator and verify the values with expected
      res.clear();
      ASSERT_EQ(it->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(count, expected[i].size());
      for (int j = 0; j < count; ++ j) {
        for (int k = 0; k < col_num; ++k) {
          bool expect_null = expected[i][j][k] == "<NULL>";
          bool is_null = false;
          res.data[k][0]->isNull(j, &is_null);
          if (expect_null) {
            ASSERT_EQ(is_null, true);
            continue;
          }
          ASSERT_EQ(is_null, false);
          switch (col_type[k]) {
            case DATATYPE::TIMESTAMP64:
              ASSERT_EQ(KTimestamp(res.data[k][0]->mem + j * sizeof(KTimestamp)), parseTimestamp((expected[i][j][k])));
              break;
            case DATATYPE::INT16:
              ASSERT_EQ(KInt16(res.data[k][0]->mem + j * sizeof(int16_t)), stoi(expected[i][j][k]));
              break;
            case DATATYPE::INT32:
              ASSERT_EQ(KInt32(res.data[k][0]->mem + j * sizeof(int32_t)), stoi(expected[i][j][k]));
              break;
            case DATATYPE::INT64:
              ASSERT_EQ(KInt64(res.data[k][0]->mem + j * sizeof(int64_t)), stol(expected[i][j][k]));
              break;
            case DATATYPE::FLOAT:
              ASSERT_EQ(KFloat32(res.data[k][0]->mem + j * sizeof(float)), stof(expected[i][j][k]));
              break;
            case DATATYPE::DOUBLE:
              ASSERT_LE(abs(KDouble64(res.data[k][0]->mem + j * sizeof(double)) - stod(expected[i][j][k])), 0.00001);
              break;
            case DATATYPE::CHAR:
              ASSERT_EQ(KChar(res.data[k][0]->mem + j * 20), *expected[i][j][k].c_str());
              break;
            case DATATYPE::VARSTRING:
              {
                VarColumnBatch* b = static_cast<VarColumnBatch*>(const_cast<Batch*>(res.data[k][0]));
                int16_t str_len = b->getDataLen(j);
                char str_val[1024];
                memcpy(str_val, b->getData(j) + 2, str_len);
                str_val[str_len] = 0;
                ASSERT_EQ(str_val, expected[i][j][k]);
              }
              break;
            case DATATYPE::VARBINARY:
              {
                VarColumnBatch* b = static_cast<VarColumnBatch*>(const_cast<Batch*>(res.data[k][0]));
                int16_t str_len = b->getDataLen(j);
                char str_val[1024];
                memcpy(str_val, b->getData(j) + 2, str_len);
                str_val[str_len] = 0;
                ASSERT_EQ(str_val, expected[i][j][k]);
              }
              break;
            default:
              ASSERT_TRUE(false);
              break;
          }
        }
      }
    }

    // No more data for current tag
    ASSERT_EQ(it->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);
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

class TimeBucketAggV2Iterator : public TestV2Iterator {
public:
  // Set vgroup_num to 1
  TimeBucketAggV2Iterator() : TestV2Iterator(1) {}
};

TEST_F(TestV2Iterator, basic) {
    TSTableID table_id = 999;
    roachpb::CreateTsTable pb_meta;
    std::vector<roachpb::DataType> col_types = {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::DOUBLE, roachpb::DataType::VARCHAR};
    ConstructRoachpbTable(&pb_meta, table_id, 1, col_types);
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
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
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
        std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
        std::vector<Sumfunctype> scan_agg_types;

        std::shared_ptr<MMapMetricsTable> schema;
        ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
        std::vector<uint32_t> entity_ids = {entity_id};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter = {};
        std::vector<k_int32> agg_extend_cols = {};
        std::vector<timestamp64> ts_points = {};
        FillParams fill_params;

        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
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
        FillParams fill_params;
        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                          scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                          schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id + i % db_num, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id + i % db_num, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }
    int entity_scan_num = 0;
    int entity_result_num = 0;
    for (size_t i = 0; i < db_num; i++) {
      s = engine_->GetTsTable(ctx_, table_id + i, ts_table, is_dropped, false);
      ASSERT_EQ(s , KStatus::SUCCESS);
      vector<EntityResultIndex> entity_store;
      s = dynamic_pointer_cast<TsTableImpl>(ts_table)->GetEntityIdByHashSpan(ctx_, {0, UINT64_MAX}, UINT64_MAX, entity_store);
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
        FillParams fill_params;
        auto vgroup = engine_->GetTsVGroup(entity.subGroupId);
        s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  start_ts += 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
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
        TsEntityCountStats count_header{};
        count_header.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_header);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  start_ts += 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();

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
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.max_osn, 10);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
                          &tmp_count, 0, 11, is_dropped);
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
      TsCountStatsFileHeader count_header{};
      s = count_info->GetCountStatsHeader(count_header);
      ASSERT_EQ(count_header.max_osn, 10);
      TsEntityCountStats count_stats{};
      count_stats.entity_id = entity_id;
      s = count_info->GetEntityCountStats(count_stats);
      ASSERT_EQ(count_stats.is_count_valid, true);
      ASSERT_EQ(count_stats.valid_count, entity_row_num);
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {};
      std::vector<timestamp64> ts_points = {};
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
  }
  start_ts += 20 * interval;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    TsRawPayload::SetOSN(pay_load, 20);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
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
      TsCountStatsFileHeader count_header{};
      s = count_info->GetCountStatsHeader(count_header);
      ASSERT_EQ(count_header.max_osn, 20);
      TsEntityCountStats count_stats{};
      count_stats.entity_id = entity_id;
      s = count_info->GetEntityCountStats(count_stats);
      if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
        ASSERT_EQ(count_stats.is_count_valid, false);
        ASSERT_EQ(count_stats.valid_count, 0);
      } else {
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num * 2);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
          ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num * 2 + entity_row_num / 2);
        } else {
          ASSERT_EQ(KInt16(res.data[0][0]->mem), 3 * entity_row_num);
        }
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
  }
}

TEST_F(TestV2Iterator, mulitEntityInvalidCount) {
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
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    auto current = vgroup->CurrentVersion();
    ASSERT_EQ(current->GetVersionNumber(), 0);
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        ASSERT_EQ(count_info, nullptr);
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
    ASSERT_EQ(current->GetVersionNumber(), 0);
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.version_num, 0);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
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
  }
  start_ts = start_ts + 5 * interval;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i,
                                  entity_row_num, start_ts + 1 + i, interval);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  for (const auto& vgroup: *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    std::vector<k_uint32> scan_cols = {0};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};

    auto current = vgroup->CurrentVersion();
    ASSERT_EQ(current->GetVersionNumber(), 0);
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition: partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.version_num, 0);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num / 2 + entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }

    // create new version.
    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
    // check old version.
    for (auto partition: partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.version_num, 0);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num);
      }
    }
    // check the latest version.
    auto latest_version = vgroup->CurrentVersion();
    ASSERT_EQ(latest_version->GetVersionNumber(), 1);
    auto latest_partitions = latest_version->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition: latest_partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.version_num, 1);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, false);
        ASSERT_EQ(count_stats.valid_count, 0);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      if (count > 0) {
        ASSERT_EQ(is_finished, false);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(KInt16(res.data[0][0]->mem), entity_row_num / 2 + entity_row_num);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
    // recalculate count stats.
    ASSERT_EQ(vgroup->RecalcCountStat(), KStatus::SUCCESS);
    // check the latest version.
    latest_version = vgroup->CurrentVersion();
    ASSERT_EQ(latest_version->GetVersionNumber(), 1);
    latest_partitions = latest_version->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition: latest_partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsCountStatsFileHeader count_header{};
        s = count_info->GetCountStatsHeader(count_header);
        ASSERT_EQ(count_header.version_num, 1);
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        ASSERT_EQ(count_stats.is_count_valid, true);
        ASSERT_EQ(count_stats.valid_count, entity_row_num / 2 + entity_row_num);
      }
    }
  }
}

TEST_F(TestV2Iterator, blockCacheDetachMMAP) {
  EngineOptions::max_rows_per_block = 30;
  EngineOptions::min_rows_per_block = 15;
  EngineOptions::g_io_mode = TsIOMode::MMAP;
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
  int insert_times = 4;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();

  for (int j = 0; j < insert_times; ++j) {
    start_ts += 1000;
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }

    for (const auto& vgroup : *ts_vgroups) {
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
      ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
    }
  }

  // Four entity segments were created for four vgroups, so there are four entity block mmap files are open.
  ASSERT_EQ(created_entity_block_file_count - destroyed_entity_block_file_count, 4);

  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 1};
    std::vector<Sumfunctype> scan_agg_types = {};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        if (count_stats.valid_count > 0) {
          ASSERT_EQ(count_stats.valid_count, 4 * entity_row_num);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(count, 30);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(count, 10);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, true);
      ASSERT_EQ(count, 0);
      delete ts_iter;
    }
  }

  // After data query, there is no new entity block mmap file are opened.
  ASSERT_EQ(created_entity_block_file_count - destroyed_entity_block_file_count, 4);
  /* block cache memory size is: 10830 = (30 * 8 bytes + 30 * 8 bytes + (30 * 4 bytes + 1 byte)) * 30
   * 30 entities, 30 rows per entity
   * timestamp column size is 8 bytes
   * osn column size is 8 bytes
   * int column size is 4 bytes, each block has one byte bitmap
   */
  ASSERT_EQ(TsLRUBlockCache::GetInstance().GetMemorySize(), 18030);

  for (int j = 0; j < insert_times; ++j) {
    start_ts += 1000;
    for (size_t i = 0; i < entity_num; i++) {
      auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts + 1 + i, interval);
      s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
      free(pay_load.data);
      ASSERT_EQ(s, KStatus::SUCCESS);
    }

    for (const auto& vgroup : *ts_vgroups) {
      ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
      ASSERT_EQ(vgroup->Compact(), KStatus::SUCCESS);
    }
  }

  /**
   * Four new version entity segments were created due to data insertion and four new entity block mmap files were opened,
   * the blocks in LRU block cache will refer to new entity block mmap files instead of old entity block mmap files, so
   * four old version entity segments and four old entity block mmap files were closed as well which means the total number
   * of opening new entity block files should still be 4.
   */
  ASSERT_EQ(created_entity_block_file_count - destroyed_entity_block_file_count, 4);


  for (const auto& vgroup : *ts_vgroups) {
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 2};
    std::vector<Sumfunctype> scan_agg_types = {};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    for (auto partition : partitions) {
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        auto count_info = partition->GetCountManager();
        TsEntityCountStats count_stats{};
        count_stats.entity_id = entity_id;
        s = count_info->GetEntityCountStats(count_stats);
        if (count_stats.valid_count > 0) {
          ASSERT_EQ(count_stats.valid_count, 8 * entity_row_num);
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
      FillParams fill_params;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
      ASSERT_EQ(s, KStatus::SUCCESS);
      ResultSet res{(k_uint32) scan_cols.size()};
      k_uint32 count;
      bool is_finished = false;
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(count, 30);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(count, 10);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, false);
      ASSERT_EQ(count, 10);
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      ASSERT_EQ(is_finished, true);
      delete ts_iter;
    }
  }

  TsLRUBlockCache::GetInstance().EvictAll();
  /**
   * Cleanup the LRU block cache doen't make any difference since LRU block cache won't hold the entity block mmap
   * files anymore.
   */
  ASSERT_EQ(created_entity_block_file_count - destroyed_entity_block_file_count, 4);
}

TEST_F(TestV2Iterator, overflow) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id, 1, {roachpb::DataType::TIMESTAMP, roachpb::DataType::BIGINT, roachpb::DataType::SMALLINT, roachpb::DataType::INT, roachpb::DataType::FLOAT});
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
  auto pay_load = GenRowPayloadForSumOverflow(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {1, 2, 3, 4};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::SUM, SUM, SUM, SUM};

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    TsStorageIterator* ts_iter1;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter1, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    k_uint32 count;
    bool is_finished = false;
    ResultSet res1{(k_uint32) scan_cols.size()};
    ASSERT_EQ(ts_iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KDouble64(res1.data[0][0]->mem), 1000 * (double)INT64_MAX);

    ASSERT_EQ(ts_iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);
    delete ts_iter1;

    // use pre agg
    vgroup->Flush();

    TsStorageIterator* ts_iter2;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter2, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res2{(k_uint32) scan_cols.size()};
    ASSERT_EQ(ts_iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KDouble64(res2.data[0][0]->mem), 1000 * (double)INT64_MAX);

    ASSERT_EQ(ts_iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);
    delete ts_iter2;
  }
}

TEST_F(TimeBucketAggV2Iterator, overflowTimeBucketAgg) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id, 1, {roachpb::DataType::TIMESTAMP, roachpb::DataType::BIGINT});
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
  auto pay_load = GenRowPayloadForSumOverflow(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
    }
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {1};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::SUM};

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;
    TimeBucketInfo time_bucket_info = {0, INT_MAX};

    TsStorageIterator* ts_iter1;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter1, vgroup, ts_points, false, false, UINT64_MAX, fill_params,
                            time_bucket_info);
    ASSERT_EQ(s, KStatus::SUCCESS);

    k_uint32 count;
    bool is_finished = false;
    ResultSet res1{(k_uint32) scan_cols.size()};
    ASSERT_EQ(ts_iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KDouble64(res1.data[0][0]->mem), 1000 * (double)INT64_MAX);

    ASSERT_EQ(ts_iter1->Next(&res1, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);
    delete ts_iter1;

    // use pre agg
    vgroup->Flush();

    TsStorageIterator* ts_iter2;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter2, vgroup, ts_points, false, false, UINT64_MAX,
                            fill_params, time_bucket_info);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res2{(k_uint32) scan_cols.size()};
    ASSERT_EQ(ts_iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KDouble64(res2.data[0][0]->mem), 1000 * (double)INT64_MAX);

    ASSERT_EQ(ts_iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);
    delete ts_iter2;
  }
}

vector<vector<roachpb::DataType>> timeBucketAgg_metric_types = {
  // test case 1
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::SMALLINT,
    roachpb::DataType::BIGINT, roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE,
    roachpb::DataType::CHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARBINARY
  },
  // test case 2
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::SMALLINT,
    roachpb::DataType::BIGINT, roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE,
    roachpb::DataType::CHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARBINARY
  },
  // test case 3
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::SMALLINT,
    roachpb::DataType::BIGINT, roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE,
    roachpb::DataType::CHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARBINARY
  },
  // test case 4
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::BIGINT, roachpb::DataType::BIGINT,
    roachpb::DataType::INT, roachpb::DataType::INT, roachpb::DataType::INT,
    roachpb::DataType::SMALLINT, roachpb::DataType::SMALLINT, roachpb::DataType::FLOAT,
    roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE, roachpb::DataType::DOUBLE
  },
  // test case 5 (for MAX_EXTEND)
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::SMALLINT,
    roachpb::DataType::BIGINT, roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE,
    roachpb::DataType::CHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARBINARY
  },
  // test case 6 (for MIN_EXTEND)
  {
    roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::SMALLINT,
    roachpb::DataType::BIGINT, roachpb::DataType::FLOAT, roachpb::DataType::DOUBLE,
    roachpb::DataType::CHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARBINARY
  }
};

vector<vector<roachpb::DataType>> timeBucketAgg_tag_types = {
  // test case 1
  {roachpb::DataType::VARCHAR, roachpb::DataType::INT},
  // test case 2
  {roachpb::DataType::VARCHAR, roachpb::DataType::INT},
  // test case 3
  {roachpb::DataType::VARCHAR, roachpb::DataType::INT},
  // test case 4
  {
    roachpb::DataType::VARCHAR, roachpb::DataType::VARCHAR, roachpb::DataType::INT, roachpb::DataType::INT, roachpb::DataType::VARCHAR,
    roachpb::DataType::VARCHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARCHAR, roachpb::DataType::VARCHAR
  },
  // test case 5 (for MAX_EXTEND)
  {roachpb::DataType::VARCHAR, roachpb::DataType::INT},
  // test case 6 (for MIN_EXTEND)
  {roachpb::DataType::VARCHAR, roachpb::DataType::INT}
};

vector<vector<vector<vector<string>>>> timeBucketAgg_metric_data = {
  // test case 1
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.181", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:13.293", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:18.683", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:10.000", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:14.999", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:15.000", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    },
    // tag 3
    {
      {"2026-3-22 12:10:10.181", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:19.999", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:30.000", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"},
      {"2026-3-22 12:10:54.999", "89", "180", "87932", "2509.3", "873.982102", "d", "varchar data 4", "varbinary data 4"}
    }
  },
  // test case 2 (added by copilot)
  {
    // tag 1
    {
      {"2026-3-22 12:10:06.000", "5", "50", "500", "1.0", "2.0", "x", "var1", "bin1"},
      {"2026-3-22 12:10:09.999", "15", "150", "1500", "1.0", "2.0", "y", "var2", "bin2"},
      {"2026-3-22 12:10:11.000", "25", "250", "2500", "1.0", "2.0", "z", "var3", "bin3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:04.999", "7", "70", "700", "1.0", "2.0", "a", "va1", "ba1"},
      {"2026-3-22 12:10:10.000", "17", "170", "1700", "1.0", "2.0", "b", "vb1", "bb1"},
      {"2026-3-22 12:10:14.999", "27", "270", "2700", "1.0", "2.0", "c", "vc1", "bc1"}
    },
    // tag 3
    {
      {"2026-3-22 12:10:15.000", "30", "300", "3000", "1.0", "2.0", "d", "vd1", "bd1"},
      {"2026-3-22 12:10:19.999", "40", "400", "4000", "1.0", "2.0", "e", "ve1", "be1"},
      {"2026-3-22 12:10:20.000", "50", "500", "5000", "1.0", "2.0", "f", "vf1", "bf1"}
    }
  },
  // test case 3
  {
    // tag 1
    {
      {"2026-3-22 12:10:06.000", "5", "<NULL>", "<NULL>", "1.0", "<NULL>", "x", "<NULL>", "bin1"},
      {"2026-3-22 12:10:09.999", "15", "<NULL>", "1500", "1.0", "2.0", "y", "var2", "bin2"},
      {"2026-3-22 12:10:11.000", "25", "<NULL>", "2500", "1.0", "2.0", "z", "var3", "bin3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:04.999", "7", "70", "700", "<NULL>", "2.0", "a", "va1", "ba1"},
      {"2026-3-22 12:10:10.000", "17", "170", "1700", "<NULL>", "2.0", "<NULL>", "<NULL>", "bb1"},
      {"2026-3-22 12:10:14.999", "27", "270", "<NULL>", "<NULL>", "2.0", "c", "<NULL>", "<NULL>"}
    },
    // tag 3
    {
      {"2026-3-22 12:10:15.000", "30", "<NULL>", "3000", "1.0", "2.0", "<NULL>", "<NULL>", "bd1"},
      {"2026-3-22 12:10:19.999", "40", "400", "4000", "1.0", "2.0", "<NULL>", "<NULL>", "be1"},
      {"2026-3-22 12:10:20.000", "50", "500", "<NULL>", "1.0", "<NULL>", "<NULL>", "vf1", "bf1"}
    }
  },
  // test case 4
  {
    // host_0
    {
      {"2023-05-31 10:00:00.000", "-2147483648", "-2147483648", "-2147483648", "-2147483648", "-32768", "-32768", "54.122111", "54.122111", "3.141593", "3.141593"},
      {"2023-05-31 10:00:01.000", "2147483647", "-2147483648", "2147483647", "-2147483648", "32767", "-32768", "-100.1", "54.122111", "-3.141593", "<NULL>"},
      {"2023-05-31 10:00:02.000", "2147483647", "77", "-90", "83", "41", "84", "26", "60", "43", "<NULL>"},
      {"2023-05-31 10:00:04.000", "92", "35", "99", "9", "31", "-1", "2", "24", "96", "69"},
      {"2023-05-31 10:00:03.000", "92", "35", "99", "9", "31", "1", "2", "24", "96", "69"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "92", "35", "99", "9", "-31", "1", "2", "24", "96", "69"}
    },
    // host_1
    {
      {"2023-05-31 10:00:00.000", "84", "2147483647", "-2147483648", "2147483647", "29", "20", "-54.122111", "-54.122111", "53", "74"},
      {"2023-05-31 10:00:01.000", "<NULL>", "2147483647", "2147483647", "2147483647", "32767", "32767", "54.122111", "-54.1221111", "3.141593", "-3.141593"},
      {"2023-05-31 10:00:02.000", "90", "0", "81", "28", "25", "44", "8", "-89", "11", "76"},
      {"2023-05-31 10:00:04.000", "21", "77", "90", "83", "41", "84", "26", "60", "43", "<NULL>"},
      {"2023-05-31 10:00:03.000", "21", "77", "90", "83", "41", "84", "26", "60", "43", "<NULL>"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "21", "77", "90", "83", "41", "-84", "26", "60", "43", "<NULL>"}
    },
    // host_2
    {
      {"2023-05-31 10:00:00.000", "29", "48", "5", "63", "-17", "52", "60", "<NULL>", "93", "1"},
      {"2023-05-31 10:00:01.000", "90", "0", "-81", "28", "25", "44", "8", "89", "11", "76"},
      {"2023-05-31 10:00:02.000", "58", "-2", "24", "61", "22", "63", "6", "44", "<NULL>", "38"},
      {"2023-05-31 10:00:04.000", "90", "0", "81", "28", "25", "44", "8", "-89", "11", "76"},
      {"2023-05-31 10:00:03.000", "90", "0", "81", "28", "25", "44", "8", "89", "11", "76"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "90", "0", "81", "28", "25", "-44", "8", "89", "11", "76"}
    },
    // host_3
    {
      {"2023-05-31 10:00:00.000", "8", "21", "-89", "78", "30", "81", "33", "24", "24", "82"},
      {"2023-05-31 10:00:01.000", "29", "48", "5", "63", "17", "52", "60", "49", "93", "1"},
      {"2023-05-31 10:00:02.000", "84", "11", "53", "87", "29", "20", "54", "77", "53", "74"},
      {"2023-05-31 10:00:04.000", "<NULL>", "2", "24", "61", "22", "63", "6", "44", "<NULL>", "38"},
      {"2023-05-31 10:00:03.000", "58", "<NULL>", "24", "61", "22", "63", "6", "44", "<NULL>", "38"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "58", "<NULL>", "24", "61", "22", "63", "6", "44", "<NULL>", "38"}
    },
    // host_4
    {
      {"2023-05-31 10:00:00.000", "2", "26", "64", "6", "38", "20", "-71", "19", "40", "54"},
      {"2023-05-31 10:00:01.000", "8", "21", "89", "78", "30", "81", "33", "24", "24", "82"},
      {"2023-05-31 10:00:02.000", "29", "48", "-5", "63", "-17", "52", "60", "49", "93", "1"},
      {"2023-05-31 10:00:04.000", "84", "11", "53", "87", "29", "20", "54", "77", "53", "74"},
      {"2023-05-31 10:00:03.000", "84", "11", "53", "87", "29", "20", "54", "77", "53", "74"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "84", "11", "53", "87", "29", "20", "54", "77", "53", "74"}
    },
    // host_5
    {
      {"2023-05-31 10:00:00.000", "-76", "-40", "-63", "-7", "-81", "-20", "-29", "-55", "<NULL>", "-15"},
      {"2023-05-31 10:00:01.000", "2", "26", "64", "6", "38", "20", "71", "<NULL>", "40", "54"},
      {"2023-05-31 10:00:02.000", "8", "21", "89", "-78", "30", "81", "33", "24", "24", "82"},
      {"2023-05-31 10:00:04.000", "29", "48", "5", "-63", "17", "-52", "60", "49", "93", "1"},
      {"2023-05-31 10:00:03.000", "29", "48", "5", "63", "17", "52", "60", "49", "93", "1"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "29", "48", "5", "-63", "17", "52", "60", "49", "93", "1"}
    },
    // host_6
    {
      {"2023-05-31 10:00:00.000", "44", "70", "20", "-67", "65", "11", "7", "92", "0", "31"},
      {"2023-05-31 10:00:01.000", "76", "40", "63", "7", "81", "20", "29", "-55", "20", "15"},
      {"2023-05-31 10:00:02.000", "2", "26", "-64", "6", "-38", "20", "71", "<NULL>", "40", "54"},
      {"2023-05-31 10:00:04.000", "8", "21", "89", "78", "30", "-81", "33", "24", "24", "82"},
      {"2023-05-31 10:00:03.000", "8", "21", "89", "78", "30", "81", "33", "24", "24", "82"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "8", "21", "89", "-78", "30", "81", "33", "24", "24", "82"}
    },
    // host_7
    {
      {"2023-05-31 10:00:00.000", "92", "35", "99", "9", "31", "-1", "2", "24", "96", "69"},
      {"2023-05-31 10:00:01.000", "44", "70", "20", "67", "65", "11", "7", "92", "0", "31"},
      {"2023-05-31 10:00:02.000", "76", "40", "63", "7", "81", "20", "29", "55", "20", "15"},
      {"2023-05-31 10:00:04.000", "2", "26", "-64", "6", "38", "-20", "71", "<NULL>", "40", "54"},
      {"2023-05-31 10:00:03.000", "2", "26", "64", "6", "38", "-20", "71", "<NULL>", "40", "54"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "2", "26", "64", "6", "38", "-20", "71", "<NULL>", "-40", "54"}
    },
    // host_8
    {
      {"2023-05-31 10:00:00.000", "21", "77", "90", "83", "41", "84", "26", "60", "43", "<NULL>"},
      {"2023-05-31 10:00:01.000", "92", "35", "-99", "9", "31", "1", "2", "24", "96", "69"},
      {"2023-05-31 10:00:02.000", "44", "70", "20", "67", "65", "11", "7", "92", "0", "31"},
      {"2023-05-31 10:00:04.000", "76", "40", "63", "7", "81", "20", "29", "55", "20", "15"},
      {"2023-05-31 10:00:03.000", "76", "40", "63", "7", "81", "20", "29", "55", "20", "15"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "76", "40", "-63", "7", "81", "-20", "29", "55", "20", "15"}
    },
    // host_9
    {
      {"2023-05-31 10:00:00.000", "90", "0", "81", "28", "25", "-44", "8", "-89", "11", "76"},
      {"2023-05-31 10:00:01.000", "21", "77", "90", "83", "41", "84", "26", "60", "<NULL>", "36"},
      {"2023-05-31 10:00:02.000", "92", "35", "99", "-9", "31", "1", "-2", "24", "96", "69"},
      {"2023-05-31 10:00:04.000", "44", "70", "20", "67", "65", "11", "7", "92", "0", "31"},
      {"2023-05-31 10:00:03.000", "44", "70", "20", "67", "65", "11", "-7", "92", "0", "31"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:09.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>"},
      {"2023-05-31 10:00:10.000", "44", "70", "20", "67", "65", "11", "7", "-92", "0", "31"}
    }
  },
  // test case 5 (for MAX_EXTEND)
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.181", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:13.293", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:18.683", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    }
  },
  // test case 6 (for MIN_EXTEND)
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.181", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:13.293", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:18.683", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    }
  }
};

vector<vector<vector<string>>> timeBucketAgg_tag_data = {
  // test case 1
  {
    // tag 1
    {"device 1", "238"},
    // tag 2
    {"device 2", "390"},
    // tag 3
    {"device 3", "68"}
  },
  // test case 2
  {
    // tag 1
    {"device 4", "100"},
    // tag 2
    {"device 5", "101"},
    // tag 3
    {"device 6", "102"}
  },
  // test case 3
  {
    // tag 1
    {"device 7", "103"},
    // tag 2
    {"device 8", "104"},
    // tag 3
    {"device 9", "105"}
  },
  // test case 4
  {
    // host_0
    {"host_0", "abc", "111", "6666", "a", "b", "c", "d", "e", "f"},
    // host_1
    {"host_1", "def", "222", "6666", "a", "s", "d", "f", "g", "h"},
    // host_2
    {"host_2", "hij", "-333", "6666", "z", "x", "c", "v", "b", "n"},
    // host_3
    {"host_3", "klm", "444", "6666", "q", "w", "e", "r", "t", "y"},
    // host_4
    {"host_4", "mno", "-555", "6666", "k", "j", "h", "g", "f", "d"},
    // host_5
    {"host_5", "pq", "666", "-6666", "f", "d", "e", "b", "t", "y"},
    // host_6
    {"host_6", "x", "777", "6666", "e", "g", "u", "i", "o", "p"},
    // host_7
    {"host_7", "y", "-888", "-6666", "v", "y", "u", "j", "k", "l"},
    // host_8
    {"host_8", "z", "999", "6666", "c", "t", "y", "u", "r", "m"},
    // host_9
    {"host_9", "zero", "10000", "6666", "f", "r", "n", "m", "t", "y"}
  },
  // test case 5 (for MAX_EXTEND)
  {
    // tag 1
    {"device 1", "238"}
  },
  // test case 6 (for MIN_EXTEND)
  {
    // tag 1
    {"device 1", "168"}
  }
};

vector<vector<uint32_t>> timeBucketAgg_tag_ids = {
  // test case 1
  {1, 2, 3},
  // test case 2
  {4, 5, 6},
  // test case 3
  {7, 8, 9},
  // test case 4
  {10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
  // test case 5 (for MAX_EXTEND)
  {20},
  // test case 6 (for MIN_EXTEND)
  {21}
};

vector<KwTsSpan> timeBucketAgg_ts_span = {
  // test case 1
  {INT64_MIN, INT64_MAX},
  // test case 2
  {INT64_MIN, INT64_MAX},
  // test case 3
  {INT64_MIN, INT64_MAX},
  // test case 4
  // {INT64_MIN, INT64_MAX}
  {1672560000000, 1704096000000},
  // test case 5 (for MAX_EXTEND)
  {INT64_MIN, INT64_MAX},
  // test case 6 (for MIN_EXTEND)
  {INT64_MIN, INT64_MAX}
};

vector<vector<k_uint32>> timeBucketAgg_scan_cols = {
  // test case 1
  {0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8},
  // test case 2
  {0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8},
  // test case 3
  {0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8},
  // test case 4
  {0, 9, 8, 4, 1, 6, 1, 3, 10, 3, 3, 9, 9},
  // test case 5 (for MAX_EXTEND)
  {0, 1, 1, 1, 1, 1, 1, 1, 1, 1},
  // test case 6 (for MIN_EXTEND)
  {0, 1, 1, 1, 1, 1, 1, 1, 1, 1}
};

vector<vector<k_int32>> timeBucketAgg_agg_extend_cols = {
  // test case 1
  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  // test case 2
  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  // test case 3
  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  // test case 4
  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
  // test case 5 (for MAX_EXTEND)
  {-1, 0, 1, 2, 3, 4, 5, 6, 7, 8},
  // test case 6 (for MIN_EXTEND)
  {-1, 0, 1, 2, 3, 4, 5, 6, 7, 8}
};

vector<vector<Sumfunctype>> timeBucketAgg_scan_agg_types = {
  // test case 1
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN
  },
  // test case 2
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN
  },
  // test case 3
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::SUM, Sumfunctype::MAX, Sumfunctype::MIN,
    Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN, Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN
  },
  // test case 4
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::LASTTS, Sumfunctype::SUM, Sumfunctype::FIRSTTS, Sumfunctype::MAX, Sumfunctype::MAX, Sumfunctype::LASTTS,
    Sumfunctype::FIRST_ROW, Sumfunctype::LAST_ROW, Sumfunctype::FIRST, Sumfunctype::LAST, Sumfunctype::FIRSTROWTS, Sumfunctype::LASTROWTS
  },
  // test case 5 (for MAX_EXTEND)
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::MAX_EXTEND, Sumfunctype::MAX, Sumfunctype::MAX_EXTEND, Sumfunctype::MAX_EXTEND, Sumfunctype::MAX_EXTEND,
    Sumfunctype::MAX_EXTEND, Sumfunctype::MAX_EXTEND, Sumfunctype::MAX_EXTEND, Sumfunctype::MAX_EXTEND
  },
  // test case 6 (for MIN_EXTEND)
  {
    Sumfunctype::TIME_BUCKET, Sumfunctype::MIN_EXTEND, Sumfunctype::MIN, Sumfunctype::MIN_EXTEND, Sumfunctype::MIN_EXTEND, Sumfunctype::MIN_EXTEND,
    Sumfunctype::MIN_EXTEND, Sumfunctype::MIN_EXTEND, Sumfunctype::MIN_EXTEND, Sumfunctype::MIN_EXTEND
  }
};

vector<TimeBucketInfo> timeBucketAgg_time_bucket_info = {
  // test case 1
  {0, 5000},
  // test case 2
  {0, 5000},
  // test case 3
  {0, 5000},
  // test case 4
  {0, 2000},
  // test case 5 (for MAX_EXTEND)
  {0, 5000},
  // test case 6 (for MIN_EXTEND)
  {0, 5000}
};

vector<vector<DATATYPE>> timeBucketAgg_out_type = {
  // test case 1
  {
    DATATYPE::TIMESTAMP64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT32, DATATYPE::INT32, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT16, DATATYPE::INT16,
    DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::FLOAT, DATATYPE::FLOAT,
    DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::INT64, DATATYPE::CHAR, DATATYPE::CHAR,
    DATATYPE::INT64, DATATYPE::VARSTRING, DATATYPE::VARSTRING, DATATYPE::INT64, DATATYPE::VARBINARY, DATATYPE::VARBINARY
  },
  // test case 2
  {
    DATATYPE::TIMESTAMP64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT32, DATATYPE::INT32, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT16, DATATYPE::INT16,
    DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::FLOAT, DATATYPE::FLOAT,
    DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::INT64, DATATYPE::CHAR, DATATYPE::CHAR,
    DATATYPE::INT64, DATATYPE::VARSTRING, DATATYPE::VARSTRING, DATATYPE::INT64, DATATYPE::VARBINARY, DATATYPE::VARBINARY
  },
  // test case 3
  {
    DATATYPE::TIMESTAMP64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT32, DATATYPE::INT32, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT16, DATATYPE::INT16,
    DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::FLOAT, DATATYPE::FLOAT,
    DATATYPE::INT64, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::DOUBLE, DATATYPE::INT64, DATATYPE::CHAR, DATATYPE::CHAR,
    DATATYPE::INT64, DATATYPE::VARSTRING, DATATYPE::VARSTRING, DATATYPE::INT64, DATATYPE::VARBINARY, DATATYPE::VARBINARY
  },
  // test case 4
  {
    DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64, DATATYPE::DOUBLE, DATATYPE::TIMESTAMP64, DATATYPE::INT64, DATATYPE::INT16, DATATYPE::TIMESTAMP64,
    DATATYPE::INT32, DATATYPE::DOUBLE, DATATYPE::INT32, DATATYPE::INT32, DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64
  },
  // test case 5 (for MAX_EXTEND)
  {
    DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64, DATATYPE::INT32, DATATYPE::INT16,
    DATATYPE::INT64, DATATYPE::FLOAT, DATATYPE::DOUBLE,
    DATATYPE::CHAR, DATATYPE::VARSTRING, DATATYPE::VARBINARY
  },
  // test case 6 (for MIN_EXTEND)
  {
    DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64, DATATYPE::INT32, DATATYPE::INT16,
    DATATYPE::INT64, DATATYPE::FLOAT, DATATYPE::DOUBLE,
    DATATYPE::CHAR, DATATYPE::VARSTRING, DATATYPE::VARBINARY
  }
};

vector<vector<vector<vector<string>>>> timeBucketAgg_expected_data = {
  // test case 1
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.000", "2", "336", "198", "138", "2", "22", "19", "3", "2", "99999999", "87654321", "12345678", "2", "33.15999984741211", "20.18", "12.98", "2", "237.96", "218.58", "19.38", "2", "b", "a", "2", "varchar data 2", "varchar data 1", "2", "varbinary data 2", "varbinary data 1"},
      {"2026-3-22 12:10:15.000", "1", "20", "20", "20", "1", "28", "28", "28", "1", "35367782", "35367782", "35367782", "1", "386.1969909667969", "386.196991", "386.196991", "1", "538.186", "538.186", "538.186", "1", "c", "c", "1", "varchar data 3", "varchar data 3", "1", "varbinary data 3", "varbinary data 3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:10.000", "2", "336", "198", "138", "2", "22", "19", "3", "2", "99999999", "87654321", "12345678", "2", "33.15999984741211", "20.18", "12.98", "2", "237.96", "218.58", "19.38", "2", "b", "a", "2", "varchar data 2", "varchar data 1", "2", "varbinary data 2", "varbinary data 1"},
      {"2026-3-22 12:10:15.000", "1", "20", "20", "20", "1", "28", "28", "28", "1", "35367782", "35367782", "35367782", "1", "386.1969909667969", "386.196991", "386.196991", "1", "538.186", "538.186", "538.186", "1", "c", "c", "1", "varchar data 3", "varchar data 3", "1", "varbinary data 3", "varbinary data 3"},
    },
    // tag 3
    {
      {"2026-3-22 12:10:10.000", "1", "198", "198", "198", "1", "3", "3", "3", "1", "12345678", "12345678", "12345678", "1", "20.18000030517578", "20.18", "20.18", "1", "19.38", "19.38", "19.38", "1", "a", "a", "1", "varchar data 1", "varchar data 1", "1", "varbinary data 1", "varbinary data 1"},
      {"2026-3-22 12:10:15.000", "1", "138", "138", "138", "1", "19", "19", "19", "1", "87654321", "87654321", "87654321", "1", "12.979999542236328", "12.98", "12.98", "1", "218.58", "218.58", "218.58", "1", "b", "b", "1", "varchar data 2", "varchar data 2", "1", "varbinary data 2", "varbinary data 2"},
      {"2026-3-22 12:10:30.000", "1", "20", "20", "20", "1", "28", "28", "28", "1", "35367782", "35367782", "35367782", "1", "386.1969909667969", "386.196991", "386.196991", "1", "538.186", "538.186", "538.186", "1", "c", "c", "1", "varchar data 3", "varchar data 3", "1", "varbinary data 3", "varbinary data 3"},
      {"2026-3-22 12:10:50.000", "1", "89", "89", "89", "1", "180", "180", "180", "1", "87932", "87932", "87932", "1", "2509.300048828125", "2509.300049", "2509.300049", "1", "873.982102", "873.982102", "873.982102", "1", "d", "d", "1", "varchar data 4", "varchar data 4", "1", "varbinary data 4", "varbinary data 4"}
    }
  },
  // test case 2
  {
    // tag 1
    {
      {"2026-3-22 12:10:05.000", "2", "20", "15", "5", "2", "200", "150", "50", "2", "2000", "1500", "500", "2", "2", "1", "1", "2", "4", "2", "2", "2", "y", "x", "2", "var2", "var1", "2", "bin2", "bin1"},
      {"2026-3-22 12:10:10.000", "1", "25", "25", "25", "1", "250", "250", "250", "1", "2500", "2500", "2500", "1", "1", "1", "1", "1", "2", "2", "2", "1", "z", "z", "1", "var3", "var3", "1", "bin3", "bin3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:00.000", "1", "7", "7", "7", "1", "70", "70", "70", "1", "700", "700", "700", "1", "1", "1", "1", "1", "2", "2", "2", "1", "a", "a", "1", "va1", "va1", "1", "ba1", "ba1"},
      {"2026-3-22 12:10:10.000", "2", "44", "27", "17", "2", "440", "270", "170", "2", "4400", "2700", "1700", "2", "2", "1", "1", "2", "4", "2", "2", "2", "c", "b", "2", "vc1", "vb1", "2", "bc1", "bb1"}
    },
    // tag 3
    {
      {"2026-3-22 12:10:15.000", "2", "70", "40", "30", "2", "700", "400", "300", "2", "7000", "4000", "3000", "2", "2", "1", "1", "2", "4", "2", "2", "2", "e", "d", "2", "ve1", "vd1", "2", "be1", "bd1"},
      {"2026-3-22 12:10:20.000", "1", "50", "50", "50", "1", "500", "500", "500", "1", "5000", "5000", "5000", "1", "1", "1", "1", "1", "2", "2", "2", "1", "f", "f", "1", "vf1", "vf1", "1", "bf1", "bf1"}
    }
  },
  // test case 3
  {
    // tag 1
    {
      {"2026-3-22 12:10:05.000", "2", "20", "15", "5", "0", "<NULL>", "<NULL>", "<NULL>", "1", "1500", "1500", "1500", "2", "2", "1", "1", "1", "2", "2", "2", "2", "y", "x", "1", "var2", "var2", "2", "bin2", "bin1"},
      {"2026-3-22 12:10:10.000", "1", "25", "25", "25", "0", "<NULL>", "<NULL>", "<NULL>", "1", "2500", "2500", "2500", "1", "1", "1", "1", "1", "2", "2", "2", "1", "z", "z", "1", "var3", "var3", "1", "bin3", "bin3"}
    },
    // tag 2
    {
      {"2026-3-22 12:10:00.000", "1", "7", "7", "7", "1", "70", "70", "70", "1", "700", "700", "700", "0", "<NULL>", "<NULL>", "<NULL>", "1", "2", "2", "2", "1", "a", "a", "1", "va1", "va1", "1", "ba1", "ba1"},
      {"2026-3-22 12:10:10.000", "2", "44", "27", "17", "2", "440", "270", "170", "1", "1700", "1700", "1700", "0", "<NULL>", "<NULL>", "<NULL>", "2", "4", "2", "2", "1", "c", "c", "0", "<NULL>", "<NULL>", "1", "bb1", "bb1"}
    },
    // tag 3
    {
      {"2026-3-22 12:10:15.000", "2", "70", "40", "30", "1", "400", "400", "400", "2", "7000", "4000", "3000", "2", "2", "1", "1", "2", "4", "2", "2", "0", "<NULL>", "<NULL>", "0", "<NULL>", "<NULL>", "2", "be1", "bd1"},
      {"2026-3-22 12:10:20.000", "1", "50", "50", "50", "1", "500", "500", "500", "0", "<NULL>", "<NULL>", "<NULL>", "1", "1", "1", "1", "0", "<NULL>", "<NULL>", "<NULL>", "0", "<NULL>", "<NULL>", "1", "vf1", "vf1", "1", "bf1", "bf1"}
    }
  },
  // test case 4
  {
    // host_0
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "108.244222", "2023-05-31 10:00:00.000", "2147483647", "-32768", "2023-05-31 10:00:01.000", "-2147483648", "<NULL>", "-2147483648", "2147483647", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "84", "2023-05-31 10:00:02.000", "2147483647", "84", "2023-05-31 10:00:03.000", "-90", "69", "-90", "99", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "24", "2023-05-31 10:00:04.000", "92", "-1", "2023-05-31 10:00:04.000", "99", "69", "99", "99", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "24", "2023-05-31 10:00:10.000", "92", "1", "2023-05-31 10:00:10.000", "99", "69", "99", "99", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_1
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "-108.2442221", "2023-05-31 10:00:00.000", "84", "32767", "2023-05-31 10:00:00.000", "-2147483648", "-3.141593", "-2147483648", "2147483647", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "-29", "2023-05-31 10:00:02.000", "90", "84", "2023-05-31 10:00:03.000", "81", "<NULL>", "81", "90", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "60", "2023-05-31 10:00:04.000", "21", "84", "2023-05-31 10:00:04.000", "90", "<NULL>", "90", "90", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "60", "2023-05-31 10:00:10.000", "21", "-84", "2023-05-31 10:00:10.000", "90", "<NULL>", "90", "90", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_2
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "89", "2023-05-31 10:00:00.000", "90", "52", "2023-05-31 10:00:01.000", "5", "76", "5", "-81", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "133", "2023-05-31 10:00:02.000", "90", "63", "2023-05-31 10:00:03.000", "24", "76", "24", "81", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "-89", "2023-05-31 10:00:04.000", "90", "44", "2023-05-31 10:00:04.000", "81", "76", "81", "81", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "89", "2023-05-31 10:00:10.000", "90", "-44", "2023-05-31 10:00:10.000", "81", "76", "81", "81", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_3
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "73", "2023-05-31 10:00:00.000", "29", "81", "2023-05-31 10:00:01.000", "-89", "1", "-89", "5", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:02.000", "121", "2023-05-31 10:00:02.000", "84", "63", "2023-05-31 10:00:03.000", "53", "38", "53", "24", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "<NULL>", "44", "2023-05-31 10:00:04.000", "<NULL>", "63", "<NULL>", "24", "38", "24", "24", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "<NULL>", "44", "2023-05-31 10:00:10.000", "58", "63", "2023-05-31 10:00:10.000", "24", "38", "24", "24", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_4
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "43", "2023-05-31 10:00:00.000", "8", "81", "2023-05-31 10:00:01.000", "64", "82", "64", "89", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "126", "2023-05-31 10:00:02.000", "84", "52", "2023-05-31 10:00:03.000", "-5", "74", "-5", "53", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "77", "2023-05-31 10:00:04.000", "84", "20", "2023-05-31 10:00:04.000", "53", "74", "53", "53", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "77", "2023-05-31 10:00:10.000", "84", "20", "2023-05-31 10:00:10.000", "53", "74", "53", "53", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_5
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "-55", "2023-05-31 10:00:00.000", "2", "20", "2023-05-31 10:00:01.000", "-63", "54", "-63", "64", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "73", "2023-05-31 10:00:02.000", "29", "81", "2023-05-31 10:00:03.000", "89", "1", "89", "5", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "49", "2023-05-31 10:00:04.000", "29", "-52", "2023-05-31 10:00:04.000", "5", "1", "5", "5", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "49", "2023-05-31 10:00:10.000", "29", "52", "2023-05-31 10:00:10.000", "5", "1", "5", "5", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_6
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "37", "2023-05-31 10:00:00.000", "76", "20", "2023-05-31 10:00:01.000", "20", "15", "20", "63", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "24", "2023-05-31 10:00:02.000", "8", "81", "2023-05-31 10:00:03.000", "-64", "82", "-64", "89", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "24", "2023-05-31 10:00:04.000", "8", "-81", "2023-05-31 10:00:04.000", "89", "82", "89", "89", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "24", "2023-05-31 10:00:10.000", "8", "81", "2023-05-31 10:00:10.000", "89", "82", "89", "89", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_7
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "116", "2023-05-31 10:00:00.000", "92", "11", "2023-05-31 10:00:01.000", "99", "31", "99", "20", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "55", "2023-05-31 10:00:02.000", "76", "20", "2023-05-31 10:00:03.000", "63", "54", "63", "64", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "<NULL>", "2023-05-31 10:00:04.000", "2", "-20", "2023-05-31 10:00:04.000", "-64", "54", "-64", "-64", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "<NULL>", "2023-05-31 10:00:10.000", "2", "-20", "2023-05-31 10:00:10.000", "64", "54", "64", "64", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_8
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000", "84", "2023-05-31 10:00:00.000", "92", "84", "2023-05-31 10:00:01.000", "90", "69", "90", "-99", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "147", "2023-05-31 10:00:02.000", "76", "20", "2023-05-31 10:00:03.000", "20", "15", "20", "63", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "55", "2023-05-31 10:00:04.000", "76", "20", "2023-05-31 10:00:04.000", "63", "15", "63", "63", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "55", "2023-05-31 10:00:10.000", "76", "-20", "2023-05-31 10:00:10.000", "-63", "15", "-63", "-63", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    },
    // host_9
    {
      {"2023-05-31 10:00:00.000", "2023-05-31 10:00:00.000", "-29", "2023-05-31 10:00:00.000", "90", "84", "2023-05-31 10:00:01.000", "81", "36", "81", "90", "2023-05-31 10:00:00.000", "2023-05-31 10:00:01.000"},
      {"2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000", "116", "2023-05-31 10:00:02.000", "92", "11", "2023-05-31 10:00:03.000", "99", "31", "99", "20", "2023-05-31 10:00:02.000", "2023-05-31 10:00:03.000"},
      {"2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000", "92", "2023-05-31 10:00:04.000", "44", "11", "2023-05-31 10:00:04.000", "20", "31", "20", "20", "2023-05-31 10:00:04.000", "2023-05-31 10:00:04.000"},
      {"2023-05-31 10:00:08.000", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "2023-05-31 10:00:08.000", "2023-05-31 10:00:09.000"},
      {"2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000", "-92", "2023-05-31 10:00:10.000", "44", "11", "2023-05-31 10:00:10.000", "20", "31", "20", "20", "2023-05-31 10:00:10.000", "2023-05-31 10:00:10.000"}
    }
  },
  // test case 5 (for MAX_EXTEND)
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.000", "2026-3-22 12:10:10.181", "198", "3", "12345678", "20.18", "19.38", "a", "varchar data 1", "varbinary data 1"},
      {"2026-3-22 12:10:15.000", "2026-3-22 12:10:18.683", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    }
  },
  // test case 6 (for MIN_EXTEND)
  {
    // tag 1
    {
      {"2026-3-22 12:10:10.000", "2026-3-22 12:10:13.293", "138", "19", "87654321", "12.98", "218.58", "b", "varchar data 2", "varbinary data 2"},
      {"2026-3-22 12:10:15.000", "2026-3-22 12:10:18.683", "20", "28", "35367782", "386.197", "538.186", "c", "varchar data 3", "varbinary data 3"}
    }
  }
};

TEST_F(TimeBucketAggV2Iterator, timeBucketAgg) {
  for (int i = 0; i < timeBucketAgg_metric_data.size(); ++i) {
    TSTableID table_id = 888 + i;
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTable(&pb_meta, table_id, 1,
        timeBucketAgg_metric_types[i],
        timeBucketAgg_tag_types[i],
        true
      );
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
    s = InsertData(*metric_schema, tag_schema ,table_id, 1, timeBucketAgg_metric_data[i], timeBucketAgg_tag_data[i], ctx_, engine_);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
          continue;
      }
      k_uint32 entity_id = 1;
      KwTsSpan ts_span = timeBucketAgg_ts_span[i];
      DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
      ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
      std::vector<k_uint32> scan_cols = timeBucketAgg_scan_cols[i];
      std::vector<Sumfunctype> scan_agg_types = timeBucketAgg_scan_agg_types[i];

      std::shared_ptr<MMapMetricsTable> schema;
      ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
      std::vector<uint32_t> entity_ids = timeBucketAgg_tag_ids[i];
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = timeBucketAgg_agg_extend_cols[i];
      std::vector<timestamp64> ts_points = {};
      FillParams fill_params;
      TimeBucketInfo time_bucket_info = timeBucketAgg_time_bucket_info[i];

      TsStorageIterator* ts_iter1;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter1, vgroup, ts_points, false, false, UINT64_MAX,
                              fill_params, time_bucket_info);
      ASSERT_EQ(s, KStatus::SUCCESS);

      VerifyResult(ts_iter1, scan_cols.size(), timeBucketAgg_out_type[i], timeBucketAgg_expected_data[i]);
      delete ts_iter1;

      // use pre agg
      vgroup->Flush();

      TsStorageIterator* ts_iter2;
      s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                              scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                              schema, &ts_iter2, vgroup, ts_points, false, false, UINT64_MAX,
                              fill_params, time_bucket_info);
      ASSERT_EQ(s, KStatus::SUCCESS);

      VerifyResult(ts_iter2, scan_cols.size(), timeBucketAgg_out_type[i], timeBucketAgg_expected_data[i]);
      delete ts_iter2;
    }
  }
}

TEST_F(TestV2Iterator, aggregation) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> col_types = {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::DOUBLE, roachpb::DataType::VARCHAR};
  ConstructRoachpbTable(&pb_meta, table_id, 1, col_types);
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

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts1 = 86400 * 1000;
  auto payload1 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts1, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload1.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
    std::vector<Sumfunctype> scan_agg_types = {MAX, MIN, SUM, COUNT};

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {-1, -1, -1, -1};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), start_ts1 + 90);
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;

    scan_agg_types = {FIRST, FIRST_ROW, FIRSTTS, FIRSTROWTS};
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), start_ts1);
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;

    scan_agg_types = {LAST, LAST_ROW, LASTTS, LASTROWTS};
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), start_ts1 + 90);
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;

    agg_extend_cols = {1, 1, 2, 2};
    scan_agg_types = {MAX, MAX_EXTEND, MIN, MIN_EXTEND};
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), start_ts1 + 90);
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestV2Iterator, blockfilter1) {
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
  std::vector<timestamp64> ts_vecs = {start_ts, start_ts + 100, start_ts + 200};
  std::vector<int> col1_value_vecs = {100, 200, 300};
  std::vector<bool> col1_isnull_vecs = {true, false, false};
  std::vector<double> col2_value_vecs = {1.0, 2.0, 3.0};
  std::vector<bool> col2_isnull_vecs = {false, true, true};
  auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1, 3, ts_vecs, col1_value_vecs, col1_isnull_vecs, col2_value_vecs, col2_isnull_vecs);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<FilterSpan> spans;
    BlockFilter block_filter1;
    block_filter1.colID = 1;
    block_filter1.filterType = BFT_SPAN;
    FilterSpan filter_span1;
    filter_span1.start.ival = 0;
    filter_span1.startBoundary = FSB_INCLUDE_BOUND;
    filter_span1.end.ival = 200;
    filter_span1.endBoundary = FSB_INCLUDE_BOUND;
    block_filter1.spans = {filter_span1};
    BlockFilter block_filter2;
    block_filter2.colID = 2;
    block_filter2.filterType = BFT_NULL;
    FilterSpan filter_span2{};
    block_filter2.spans = {filter_span2};
    std::vector<BlockFilter> block_filters = {block_filter1, block_filter2};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filters,
                         scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 3);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;

    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filters,
                         scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 3);
    ASSERT_EQ(KTimestamp(res.data[0][0]->mem), convertMSToPrecisionTS(start_ts, ts_col_type));

    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
  }
}

TEST_F(TestV2Iterator, blockfilter2) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> col_types = {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::DOUBLE, roachpb::DataType::VARCHAR};
  ConstructRoachpbTable(&pb_meta, table_id, 1, col_types);
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
  auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 1000, start_ts);
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(pay_load.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<FilterSpan> spans;
    BlockFilter block_filter;
    block_filter.colID = 3;
    block_filter.filterType = BFT_SPAN;
    FilterSpan filter_span;
    char* start_data = (char*)malloc(3);
    memcpy(start_data, "abc", 3);
    filter_span.start.data = start_data;
    filter_span.startBoundary = FSB_INCLUDE_BOUND;
    char* end_data = (char*)malloc(4);
    memcpy(end_data, "abcd", 4);
    filter_span.end.data = end_data;
    filter_span.endBoundary = FSB_INCLUDE_BOUND;
    block_filter.spans = {filter_span};
    std::vector<BlockFilter> block_filters = {block_filter};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filters,
                         scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;

    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filters,
                         scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
    ASSERT_EQ(count, 0);

    delete ts_iter;
    free(start_data);
    free(end_data);
  }
}

TEST_F(TestV2Iterator, early_exit) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> col_types = {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::DOUBLE, roachpb::DataType::VARCHAR};
  ConstructRoachpbTable(&pb_meta, table_id, 1, col_types);
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

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts1 = 86400 * 1000;
  auto payload1 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts1, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload1.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts2 = 86400 * 1000 * 2;
  auto payload2 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts2, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload2.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts3 = 86400 * 1000 * 3;
  auto payload3 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts3, 10);
  s = engine_->PutData(ctx_, table_id, 0, &payload3, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload3.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    uint32_t total_cnt1 = 0, total_cnt2 = 0;
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                        scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                        schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    bool is_finished = false;
    do {
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished, start_ts3 - 1), KStatus::SUCCESS);
      total_cnt1 += count;
    } while (!is_finished);

    ASSERT_EQ(total_cnt1, 20);

    delete ts_iter;

    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, true, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    res.clear();
    do {
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished, start_ts3 - 1), KStatus::SUCCESS);
      total_cnt2 += count;
    } while (!is_finished);

    ASSERT_EQ(total_cnt2, 10);

    delete ts_iter;
  }
}

TEST_F(TestV2Iterator, deduplication) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  std::vector<roachpb::DataType> col_types = {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT, roachpb::DataType::DOUBLE, roachpb::DataType::VARCHAR};
  ConstructRoachpbTable(&pb_meta, table_id, 1, col_types);
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

  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  timestamp64 start_ts1 = 3600;
  auto payload1 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 120, start_ts1);
  s = engine_->PutData(ctx_, table_id, 0, &payload1, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload1.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto payload2 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 100, start_ts1);
  s = engine_->PutData(ctx_, table_id, 0, &payload2, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload2.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    opts_.g_dedup_rule = DedupRule::DISCARD;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    uint32_t total_cnt = 0;
    bool is_finished = false;
    do {
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      total_cnt += count;
    } while (!is_finished);

    ASSERT_EQ(total_cnt, 120);

    delete ts_iter;

    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }

  timestamp64 start_ts2 = 7200;
  auto payload3 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts2);
  s = engine_->PutData(ctx_, table_id, 0, &payload3, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload3.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto payload4 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts2);
  s = engine_->PutData(ctx_, table_id, 0, &payload4, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload4.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto payload5 = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1, 10, start_ts2);
  s = engine_->PutData(ctx_, table_id, 0, &payload5, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
  free(payload5.data);
  ASSERT_EQ(s, KStatus::SUCCESS);

  for (const auto& vgroup : *ts_vgroups) {
    if (!vgroup || vgroup->GetMaxEntityID() < 1) {
      continue;
    }
    TsStorageIterator* ts_iter;
    k_uint32 entity_id = 1;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
    ts_span = ConvertMsToPrecision(ts_span, ts_col_type);
    std::vector<k_uint32> scan_cols = {0, 1, 2, 3};
    std::vector<Sumfunctype> scan_agg_types;

    std::shared_ptr<MMapMetricsTable> schema;
    ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
    std::vector<uint32_t> entity_ids = {entity_id};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    std::vector<BlockFilter> block_filter = {};
    std::vector<k_int32> agg_extend_cols = {};
    std::vector<timestamp64> ts_points = {};
    FillParams fill_params;

    opts_.g_dedup_rule = DedupRule::MERGE;
    s = vgroup->GetIterator(ctx_, 1, entity_ids, ts_spans, block_filter,
                            scan_cols, scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr,
                            schema, &ts_iter, vgroup, ts_points, false, false, UINT64_MAX, fill_params);
    ASSERT_EQ(s, KStatus::SUCCESS);

    ResultSet res{(k_uint32) scan_cols.size()};
    k_uint32 count;
    uint32_t total_cnt = 0;
    bool is_finished = false;
    do {
      ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
      total_cnt += count;
    } while (!is_finished);

    ASSERT_EQ(total_cnt, 130);

    delete ts_iter;

    ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
  }
}
