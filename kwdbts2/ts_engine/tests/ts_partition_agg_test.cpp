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

#include <array>
#include <cstdio>
#include <cstring>
#include "ts_test_base.h"
#include "test_util.h"
#include "ts_agg.h"
#include "ts_engine.h"
#include "ts_lru_block_cache.h"
#include "ts_partition_agg.h"
#include "ts_table.h"
#include <atomic>

using namespace kwdbts;

const string engine_root_path = "./tsdb";
extern atomic<int> destroyed_entity_block_file_count;
extern atomic<int> created_entity_block_file_count;

TEST(TsPartitionAgg, LegacyOffsetLayoutFileCanBeRead) {
  fs::path file_path = "./partition_agg_legacy_layout_test";
  std::remove(file_path.c_str());

  TsFIOEnv env;
  std::unique_ptr<TsAppendOnlyFile> w_file;
  ASSERT_EQ(env.NewAppendOnlyFile(file_path, &w_file, false), KStatus::SUCCESS);

  std::array<uint32_t, 2> offsets{sizeof(uint64_t), sizeof(uint64_t) * 2};
  uint64_t count0 = 7;
  uint64_t count1 = 9;
  TsBufferBuilder entity_agg;
  entity_agg.resize(sizeof(uint32_t) * offsets.size());
  std::memcpy(entity_agg.data(), offsets.data(), sizeof(uint32_t) * offsets.size());
  entity_agg.append(reinterpret_cast<char*>(&count0), sizeof(count0));
  entity_agg.append(reinterpret_cast<char*>(&count1), sizeof(count1));
  ASSERT_EQ(w_file->Append(entity_agg.AsSlice()), KStatus::SUCCESS);

  TsEntityPartitionAggIndex index{123, 1, 1, 10, 20, 3, 0, entity_agg.size(), false, ""};
  TSSlice index_slice{reinterpret_cast<char*>(&index), sizeof(index)};
  ASSERT_EQ(w_file->Append(index_slice), KStatus::SUCCESS);

  TsPartitionAggFooter footer{};
  footer.entity_agg_stats_idx_offset = entity_agg.size();
  footer.max_entity_id = 1;
  footer.file_version = PARTITION_AGG_OFFSET_LAYOUT_VERSION;
  TSSlice footer_slice{reinterpret_cast<char*>(&footer), sizeof(footer)};
  ASSERT_EQ(w_file->Append(footer_slice), KStatus::SUCCESS);
  ASSERT_EQ(w_file->Sync(), KStatus::SUCCESS);
  ASSERT_EQ(w_file->Close(), KStatus::SUCCESS);

  TsPartitionAggReader reader(&env, file_path);
  ASSERT_EQ(reader.Open(), KStatus::SUCCESS);
  EXPECT_FALSE(reader.IsSparseLayout());

  TsEntityPartitionAggIndex read_index;
  read_index.entity_id = 1;
  ASSERT_EQ(reader.GetPartitionAggIndex(read_index), KStatus::SUCCESS);
  EXPECT_EQ(read_index.table_id, 123);
  EXPECT_EQ(read_index.table_version, 1);
  EXPECT_EQ(read_index.agg_offset, 0);
  EXPECT_EQ(read_index.agg_len, entity_agg.size());

  TsSliceGuard agg;
  ASSERT_EQ(reader.GetPartitionAgg(read_index.agg_offset, read_index.agg_len, agg), KStatus::SUCCESS);
  TSSlice col_agg{nullptr, 0};
  ASSERT_EQ(GetOffsetPartitionAggColumnSlice(agg.AsSlice(), offsets.size(), 0, &col_agg), KStatus::SUCCESS);
  ASSERT_EQ(col_agg.len, sizeof(uint64_t));
  uint64_t read_count = 0;
  std::memcpy(&read_count, col_agg.data, sizeof(read_count));
  EXPECT_EQ(read_count, count0);

  ASSERT_EQ(GetOffsetPartitionAggColumnSlice(agg.AsSlice(), offsets.size(), 1, &col_agg), KStatus::SUCCESS);
  ASSERT_EQ(col_agg.len, sizeof(uint64_t));
  std::memcpy(&read_count, col_agg.data, sizeof(read_count));
  EXPECT_EQ(read_count, count1);

  std::remove(file_path.c_str());
}

class TestPartitionAgg : public TsEngineTestBase {
 public:
  void SetUp() override {
    InitContext();
    KWDBDynamicThreadPool::GetThreadPool().Init(8, ctx_);
  }

  void TearDown() override {
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  TestPartitionAgg() {
    InitContext();
    InitEngine(engine_root_path);
  }
};

TEST(TestPartitionAggBuilder, InvalidAppendInputReturnsFail) {
  fs::path filename = "./partition_agg_invalid_append_test";
  fs::remove(filename);
  TsPartitionAggBuilder builder(&TsIOEnv::GetInstance(), filename, 1);
  ASSERT_EQ(builder.Open(), KStatus::SUCCESS);

  char agg_data[] = "agg";
  TsEntityPartitionAggIndex invalid_entity;
  invalid_entity.entity_id = 2;
  ASSERT_EQ(builder.AppendEntityAgg({agg_data, sizeof(agg_data)}, invalid_entity), KStatus::FAIL);

  TsEntityPartitionAggIndex empty_agg;
  empty_agg.entity_id = 1;
  ASSERT_EQ(builder.AppendEntityAgg({agg_data, 0}, empty_agg), KStatus::FAIL);

  ASSERT_EQ(builder.Close(), KStatus::SUCCESS);
  fs::remove(filename);
}

TEST_F(TestPartitionAgg, basicPartitionAgg) {
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

  timestamp64 start_ts1 = 3600;
  KTimestamp interval = 100L;
  int entity_num = 30;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts1, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  timestamp64 start_ts2 = start_ts1 + 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts2, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (const auto& partition : partitions) {
      auto agg_reader = partition->GetAggReader();
      ASSERT_EQ(agg_reader, nullptr);
    }
  }
  std::shared_ptr<MMapMetricsTable> schema;
  ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
  const auto& attrs = *schema->getSchemaInfoExcludeDroppedPtr();
  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->CalcPartitionAgg(), KStatus::SUCCESS);
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 0, 0, 1, 1};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN,
      Sumfunctype::COUNT, Sumfunctype::SUM};
    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (const auto& partition : partitions) {
      auto agg_reader = partition->GetAggReader();
      ASSERT_NE(agg_reader, nullptr);
      ASSERT_TRUE(agg_reader->IsSparseLayout());
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        TsEntityPartitionAggIndex agg_index;
        agg_index.entity_id = entity_id;
        ASSERT_EQ(agg_reader->GetPartitionAggIndex(agg_index), KStatus::SUCCESS);
        ASSERT_EQ(agg_index.max_osn, 10);
        // Aggregate count col
        TsSliceGuard slice;
        ASSERT_EQ(agg_reader->GetPartitionAgg(agg_index.agg_offset, agg_index.agg_len, slice), KStatus::SUCCESS);
        if (partition->GetStartTime() == 0) {
          timestamp64 max_val = start_ts1 + (entity_row_num - 1) * interval;
          ASSERT_EQ(agg_index.min_ts, start_ts1);
          ASSERT_EQ(agg_index.max_ts, max_val);
        } else {
          timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
          ASSERT_EQ(agg_index.min_ts, start_ts2);
          ASSERT_EQ(agg_index.max_ts, max_val);
        }

        // verify agg value
        for (auto col_idx : {0, 1}) {
          TSSlice col_agg_slice{nullptr, 0};
          bool has_column = false;
          ASSERT_EQ(GetSparsePartitionAggColumnSlice(attrs, slice.AsSlice(), col_idx, &col_agg_slice, &has_column),
                    KStatus::SUCCESS);
          ASSERT_TRUE(has_column);
          TsSliceGuard col_agg(col_agg_slice.data, col_agg_slice.len);
          ASSERT_EQ(*reinterpret_cast<uint64_t*>(col_agg.data()), entity_row_num);
          if (col_idx == 0) {
            // verify idx 0 max min
            if (partition->GetStartTime() == 0) {
              timestamp64 max_val = start_ts1 + (entity_row_num - 1) * interval;
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t)), max_val);
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t) + sizeof(timestamp64)), start_ts1);
            } else {
              timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t)), max_val);
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t) + sizeof(timestamp64)), start_ts2);
            }
          } else {
            // verify idx 1 max min
            ASSERT_EQ(*reinterpret_cast<uint64_t*>(col_agg.data()), entity_row_num);
            ASSERT_EQ(*reinterpret_cast<bool*>(col_agg.data() + sizeof(uint64_t) + sizeof(int) * 2) , false);
          }
        }
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
      std::vector<uint32_t> entity_ids = {entity_id};
      std::vector<KwTsSpan> ts_spans = {ts_span};
      std::vector<BlockFilter> block_filter = {};
      std::vector<k_int32> agg_extend_cols = {-1, -1, -1, -1, -1};
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
        ASSERT_EQ(KInt64(res.data[0][0]->mem), 2 * entity_row_num);
        timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
        ASSERT_EQ(KTimestamp(res.data[1][0]->mem), max_val);
        ASSERT_EQ(KTimestamp(res.data[2][0]->mem), start_ts1);
        ASSERT_EQ(KInt64(res.data[3][0]->mem), 2 * entity_row_num);
        ASSERT_NE(res.data[4][0]->mem, nullptr);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
  }
}

TEST_F(TestPartitionAgg, basicPartitionAggDelete) {
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

  timestamp64 start_ts1 = 3600;
  KTimestamp interval = 100L;
  int entity_num = 30;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts1, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  timestamp64 start_ts2 = start_ts1 + 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema ,table_id, 1, 1 + i, entity_row_num, start_ts2, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (const auto& partition : partitions) {
      auto agg_reader = partition->GetAggReader();
      ASSERT_EQ(agg_reader, nullptr);
    }
  }

  uint64_t tmp_count;
  uint64_t p_tag_entity_id = 3;
  std::string p_key = GetPrimaryKey(table_id, p_tag_entity_id);

  s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{start_ts2 + entity_row_num / 2 * interval, INT64_MAX}},
                          &tmp_count, 0, 11, is_dropped);
  ASSERT_EQ(s, KStatus::SUCCESS);
  auto tag_table = table_schema_mgr->GetTagTable();
  uint32_t v_group_id, del_entity_id;
  ASSERT_TRUE(tag_table->hasPrimaryKey(p_key.data(), p_key.size(), del_entity_id, v_group_id));

  std::shared_ptr<MMapMetricsTable> schema;
  ASSERT_EQ(table_schema_mgr->GetMetricSchema(1, &schema), KStatus::SUCCESS);
  const auto& attrs = *schema->getSchemaInfoExcludeDroppedPtr();
  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->CalcPartitionAgg(), KStatus::SUCCESS);
    TsStorageIterator* ts_iter;
    KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
    std::vector<k_uint32> scan_cols = {0, 0, 0, 1, 1};
    std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT, Sumfunctype::MAX, Sumfunctype::MIN,
      Sumfunctype::COUNT, Sumfunctype::SUM};

    auto current = vgroup->CurrentVersion();
    auto partitions = current->GetPartitions(1, {{INT64_MIN, INT64_MAX}}, DATATYPE::TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 2);
    for (const auto& partition : partitions) {
      auto agg_reader = partition->GetAggReader();
      ASSERT_NE(agg_reader, nullptr);
      ASSERT_TRUE(agg_reader->IsSparseLayout());
      for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
        TsEntityPartitionAggIndex agg_index;
        agg_index.entity_id = entity_id;
        ASSERT_EQ(agg_reader->GetPartitionAggIndex(agg_index), KStatus::SUCCESS);
        if (partition->GetStartTime() != 0 && vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
          ASSERT_EQ(agg_index.max_osn, 11);
        } else {
          ASSERT_EQ(agg_index.max_osn, 10);
        }
        if (partition->GetStartTime() == 0) {
          timestamp64 max_val = start_ts1 + (entity_row_num - 1) * interval;
          ASSERT_EQ(agg_index.min_ts, start_ts1);
          ASSERT_EQ(agg_index.max_ts, max_val);
        } else {
          if (vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
            timestamp64 max_val = start_ts2 + (entity_row_num / 2 - 1) * interval;
            ASSERT_EQ(agg_index.min_ts, start_ts2);
            ASSERT_EQ(agg_index.max_ts, max_val);
          } else {
            timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
            ASSERT_EQ(agg_index.min_ts, start_ts2);
            ASSERT_EQ(agg_index.max_ts, max_val);
          }
        }

        // Aggregate count col
        TsSliceGuard slice;
        ASSERT_EQ(agg_reader->GetPartitionAgg(agg_index.agg_offset, agg_index.agg_len, slice), KStatus::SUCCESS);
        // verify agg value
        for (auto col_idx : {0, 1}) {
          TSSlice col_agg_slice{nullptr, 0};
          bool has_column = false;
          ASSERT_EQ(GetSparsePartitionAggColumnSlice(attrs, slice.AsSlice(), col_idx, &col_agg_slice, &has_column),
                    KStatus::SUCCESS);
          ASSERT_TRUE(has_column);
          TsSliceGuard col_agg(col_agg_slice.data, col_agg_slice.len);
          if (partition->GetStartTime() != 0 && vgroup->GetVGroupID() == v_group_id && entity_id == del_entity_id) {
            ASSERT_EQ(*reinterpret_cast<uint32_t*>(col_agg.data()), entity_row_num / 2);
            if (col_idx == 0) {
              // verify idx 0 max min
              timestamp64 max_val = start_ts2 + (entity_row_num / 2 - 1) * interval;
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t)), max_val);
              ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t) + sizeof(timestamp64)), start_ts2);
            } else {
              // verify idx 1 count sum overflow
              ASSERT_EQ(*reinterpret_cast<uint64_t*>(col_agg.data()), entity_row_num / 2);
              ASSERT_EQ(*reinterpret_cast<bool*>(col_agg.data() + sizeof(uint64_t) + sizeof(int) * 2) , false);
            }
          } else {
            ASSERT_EQ(*reinterpret_cast<uint32_t*>(col_agg.data()), entity_row_num);
            if (col_idx == 0) {
              // verify idx 0 max min
              if (partition->GetStartTime() == 0) {
                timestamp64 max_val = start_ts1 + (entity_row_num - 1) * interval;
                ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t)), max_val);
                ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t) + sizeof(timestamp64)), start_ts1);
              } else {
                timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
                ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t)), max_val);
                ASSERT_EQ(*reinterpret_cast<timestamp64*>(col_agg.data() + sizeof(uint64_t) + sizeof(timestamp64)), start_ts2);
              }
            } else {
              // verify idx 1 count sum overflow
              ASSERT_EQ(*reinterpret_cast<uint64_t*>(col_agg.data()), entity_row_num);
              ASSERT_EQ(*reinterpret_cast<bool*>(col_agg.data() + sizeof(uint64_t) + sizeof(int) * 2) , false);
            }
          }
        }
      }
    }
    for (k_uint32 entity_id = 1; entity_id <= vgroup->GetMaxEntityID(); entity_id++) {
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
          ASSERT_EQ(KInt64(res.data[0][0]->mem), entity_row_num + entity_row_num / 2);
          timestamp64 max_val = start_ts2 + (entity_row_num / 2 - 1) * interval;
          ASSERT_EQ(KTimestamp(res.data[1][0]->mem), max_val);
          ASSERT_EQ(KTimestamp(res.data[2][0]->mem), start_ts1);
          ASSERT_EQ(KInt64(res.data[3][0]->mem), entity_row_num + entity_row_num / 2);
        } else {
          ASSERT_EQ(KInt16(res.data[0][0]->mem), 2 * entity_row_num);
          timestamp64 max_val = start_ts2 + (entity_row_num - 1) * interval;
          ASSERT_EQ(KTimestamp(res.data[1][0]->mem), max_val);
          ASSERT_EQ(KTimestamp(res.data[2][0]->mem), start_ts1);
          ASSERT_EQ(KInt64(res.data[3][0]->mem), 2 * entity_row_num);
        }
        ASSERT_NE(res.data[4][0]->mem, nullptr);
        ASSERT_EQ(ts_iter->Next(&res, &count, &is_finished), KStatus::SUCCESS);
        ASSERT_EQ(is_finished, true);
        ASSERT_EQ(count, 0);
      }
      delete ts_iter;
    }
  }
}

TEST_F(TestPartitionAgg, basicPartitionAggDeleteAll) {
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
  ASSERT_EQ(s, KStatus::SUCCESS);

  const std::vector<AttributeInfo>* metric_schema{nullptr};
  s = table_schema_mgr->GetMetricMeta(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<TagInfo> tag_schema;
  s = table_schema_mgr->GetTagMeta(1, tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);

  timestamp64 start_ts1 = 3600;
  KTimestamp interval = 100L;
  int entity_num = 8;
  int entity_row_num = 10;
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i, entity_row_num, start_ts1, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  timestamp64 start_ts2 = start_ts1 + 10000 * 86400;
  for (size_t i = 0; i < entity_num; i++) {
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id, 1, 1 + i, entity_row_num, start_ts2, interval);
    TsRawPayload::SetOSN(pay_load, 10);
    s = engine_->PutData(ctx_, table_id, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    free(pay_load.data);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = engine_->GetTsVGroups();
  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->CalcPartitionAgg(), KStatus::SUCCESS);
  }

  uint64_t tmp_count = 0;
  for (uint64_t entity_id = 1; entity_id <= entity_num; ++entity_id) {
    std::string p_key = GetPrimaryKey(table_id, entity_id);
    s = engine_->DeleteData(ctx_, table_id, 0, p_key, {{INT64_MIN, INT64_MAX}}, &tmp_count, 0, 11, is_dropped);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  for (const auto& vgroup : *ts_vgroups) {
    ASSERT_EQ(vgroup->CalcPartitionAgg(), KStatus::SUCCESS);
  }
}

