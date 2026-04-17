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

#include <fcntl.h>
#include <unistd.h>
#include <variant>
#include "cm_kwdb_context.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

const string alter_type_test_root_path = "./tsdb_alter_type";

class TsAlterTypeTest : public TsEngineTestBase {
 public:
  TsAlterTypeTest() {
    InitContext();
    InitEngine(alter_type_test_root_path);
  }

 protected:
  using MetricValue = std::variant<std::string, int16_t, int32_t, int64_t, float, double>;
  using NumericMetricValue = MetricValue;

  [[nodiscard]] static DATATYPE ToStorageType(roachpb::DataType type) {
    switch (type) {
      case roachpb::VARCHAR:
        return DATATYPE::VARSTRING;
      case roachpb::SMALLINT:
        return DATATYPE::INT16;
      case roachpb::INT:
        return DATATYPE::INT32;
      case roachpb::BIGINT:
        return DATATYPE::INT64;
      case roachpb::FLOAT:
        return DATATYPE::FLOAT;
      case roachpb::DOUBLE:
        return DATATYPE::DOUBLE;
      default:
        return DATATYPE::INVALID;
    }
  }

  static void SetMetricColumnValue(TSRowPayloadBuilder* builder,
                                   DATATYPE metric_storage_type,
                                   int row_idx,
                                   int col_idx,
                                   const MetricValue& value) {
    switch (metric_storage_type) {
      case DATATYPE::VARSTRING: {
        ASSERT_TRUE(std::holds_alternative<std::string>(value));
        const auto& current = std::get<std::string>(value);
        builder->SetColumnValue(row_idx, col_idx, const_cast<char*>(current.c_str()), current.size() + 1);
        break;
      }
      case DATATYPE::INT16: {
        ASSERT_TRUE(std::holds_alternative<int16_t>(value));
        auto current = std::get<int16_t>(value);
        builder->SetColumnValue(row_idx, col_idx, reinterpret_cast<char*>(&current), sizeof(current));
        break;
      }
      case DATATYPE::INT32: {
        ASSERT_TRUE(std::holds_alternative<int32_t>(value));
        auto current = std::get<int32_t>(value);
        builder->SetColumnValue(row_idx, col_idx, reinterpret_cast<char*>(&current), sizeof(current));
        break;
      }
      case DATATYPE::INT64: {
        ASSERT_TRUE(std::holds_alternative<int64_t>(value));
        auto current = std::get<int64_t>(value);
        builder->SetColumnValue(row_idx, col_idx, reinterpret_cast<char*>(&current), sizeof(current));
        break;
      }
      case DATATYPE::FLOAT: {
        ASSERT_TRUE(std::holds_alternative<float>(value));
        auto current = std::get<float>(value);
        builder->SetColumnValue(row_idx, col_idx, reinterpret_cast<char*>(&current), sizeof(current));
        break;
      }
      case DATATYPE::DOUBLE: {
        ASSERT_TRUE(std::holds_alternative<double>(value));
        auto current = std::get<double>(value);
        builder->SetColumnValue(row_idx, col_idx, reinterpret_cast<char*>(&current), sizeof(current));
        break;
      }
      default:
        FAIL() << "Unsupported metric type in SetMetricColumnValue: "
               << static_cast<int>(metric_storage_type);
    }
  }

  [[nodiscard]] std::shared_ptr<TsTableSchemaManager> CreateTableAndGetSchemaMgr(
      TSTableID table_id,
      const std::vector<roachpb::DataType>& metric_type) const {
    roachpb::CreateTsTable pb_meta;
    ConstructRoachpbTableWithTypes(&pb_meta, table_id, metric_type);
    std::shared_ptr<TsTable> ts_table;
    auto s = engine_->CreateTsTable(ctx_, table_id, &pb_meta, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);

    bool is_dropped = false;
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    return schema_mgr;
  }

  void InsertTestData(TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts,
                      int num_rows, uint32_t version = 1) const {
    bool is_dropped = false;
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    ASSERT_EQ(engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr), KStatus::SUCCESS);

    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ASSERT_EQ(schema_mgr->GetMetricMeta(version, &metric_schema), KStatus::SUCCESS);

    std::vector<TagInfo> tag_schema;
    ASSERT_EQ(schema_mgr->GetTagMeta(version, tag_schema), KStatus::SUCCESS);

    auto payload = GenRowPayload(*metric_schema, tag_schema, table_id, version,
                                 dev_id, num_rows, start_ts);
    uint16_t inc_entity_cnt = 0;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    ASSERT_EQ(engine_->PutData(ctx_, table_id, 0, &payload, 1, 0,
                               &inc_entity_cnt, &inc_unordered_cnt, &dedup_result),
              KStatus::SUCCESS);
    free(payload.data);
  }

  void InsertNumericVarcharData(TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts,
                                const std::vector<std::string>& varchar_values,
                                uint32_t version) const {
    std::vector<MetricValue> metric_values;
    metric_values.reserve(varchar_values.size());
    for (const auto& varchar_value : varchar_values) {
      metric_values.emplace_back(varchar_value);
    }
    InsertTypedMetricData(table_id, dev_id, start_ts, roachpb::VARCHAR, metric_values, version);
  }

  void InsertTypedMetricData(TSTableID table_id, TSEntityID dev_id, timestamp64 start_ts,
                             roachpb::DataType metric_type,
                             const std::vector<NumericMetricValue>& metric_values,
                             uint32_t version,
                             size_t metric_col_idx = 1) const {
    bool is_dropped = false;
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    ASSERT_EQ(engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr), KStatus::SUCCESS);

    const std::vector<AttributeInfo>* metric_schema{nullptr};
    ASSERT_EQ(schema_mgr->GetMetricMeta(version, &metric_schema), KStatus::SUCCESS);
    ASSERT_LT(metric_col_idx, metric_schema->size());

    auto metric_storage_type = ToStorageType(metric_type);
    ASSERT_NE(metric_storage_type, DATATYPE::INVALID);
    ASSERT_EQ(static_cast<DATATYPE>((*metric_schema)[metric_col_idx].type), metric_storage_type);

    std::vector<TagInfo> tag_schema;
    ASSERT_EQ(schema_mgr->GetTagMeta(version, tag_schema), KStatus::SUCCESS);

    TSRowPayloadBuilder builder(tag_schema, *metric_schema, static_cast<int>(metric_values.size()));
    builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));

    timestamp64 ts = start_ts;
    for (size_t row_idx = 0; row_idx < metric_values.size(); ++row_idx) {
      for (size_t col_idx = 0; col_idx < metric_schema->size(); ++col_idx) {
        auto col_type = static_cast<DATATYPE>((*metric_schema)[col_idx].type);
        if (col_idx == metric_col_idx) {
          SetMetricColumnValue(&builder, metric_storage_type,
                               static_cast<int>(row_idx), static_cast<int>(col_idx),
                               metric_values[row_idx]);
          continue;
        }

        switch (col_type) {
          case DATATYPE::TIMESTAMP:
          case DATATYPE::TIMESTAMP64: {
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&ts), sizeof(ts));
            break;
          }
          case DATATYPE::VARSTRING: {
            auto value = std::to_string(row_idx + 1);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   const_cast<char*>(value.c_str()), value.size() + 1);
            break;
          }
          case DATATYPE::DOUBLE: {
            double value = 55.555 + static_cast<double>(row_idx);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&value), sizeof(value));
            break;
          }
          case DATATYPE::FLOAT: {
            float value = 33.25f + static_cast<float>(row_idx);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&value), sizeof(value));
            break;
          }
          case DATATYPE::INT16: {
            auto value = static_cast<int16_t>(row_idx + 10);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&value), sizeof(value));
            break;
          }
          case DATATYPE::INT32: {
            auto value = static_cast<int32_t>(row_idx + 100);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&value), sizeof(value));
            break;
          }
          case DATATYPE::INT64: {
            auto value = static_cast<int64_t>(row_idx + 1000);
            builder.SetColumnValue(static_cast<int>(row_idx), static_cast<int>(col_idx),
                                   reinterpret_cast<char*>(&value), sizeof(value));
            break;
          }
          default:
            FAIL() << "Unsupported filler metric type in InsertTypedMetricData: "
                   << (*metric_schema)[col_idx].type;
        }
      }
      ts += 1000;
    }

    TSSlice payload{nullptr, 0};
    builder.Build(table_id, version, &payload);
    uint16_t inc_entity_cnt = 0;
    uint32_t inc_unordered_cnt = 0;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    ASSERT_EQ(engine_->PutData(ctx_, table_id, 0, &payload, 1, 0,
                               &inc_entity_cnt, &inc_unordered_cnt, &dedup_result),
              KStatus::SUCCESS);
    free(payload.data);
  }

  void FlushAllVGroups() const {
    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (vgroup != nullptr) {
        ASSERT_EQ(vgroup->Flush(), KStatus::SUCCESS);
      }
    }
  }

  struct QueryResultHolder {
    KStatus status{KStatus::FAIL};
    // Declaration order is intentional: members are destroyed in reverse order,
    // so `res` releases any iterator-backed batches before `iter` is deleted.
    std::unique_ptr<TsStorageIterator> iter;
    ResultSet res;
    k_uint32 count{0};
    bool is_finished{false};

    explicit QueryResultHolder(k_uint32 col_num) : res(col_num) {}

    [[nodiscard]] bool HasData() const {
      return status == KStatus::SUCCESS && count > 0;
    }
  };

  [[nodiscard]] uint64_t QueryAllRows(const std::shared_ptr<TsTableSchemaManager>& schema_mgr,
                                      uint32_t version,
                                      std::vector<k_uint32> scan_cols) const {
    std::shared_ptr<MMapMetricsTable> schema;
    if (schema_mgr->GetMetricSchema(version, &schema) != KStatus::SUCCESS) {
      return 0;
    }

    uint64_t total = 0;
    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
      }
      for (k_uint32 eid = 1; eid <= vgroup->GetMaxEntityID(); ++eid) {
        TsStorageIterator* ts_iter = nullptr;
        std::vector<uint32_t> eids = {eid};
        KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter;
        std::vector<k_int32> agg_extend_cols;
        std::vector<Sumfunctype> scan_agg_types;
        std::vector<timestamp64> ts_points;
        FillParams fill_params;
        auto s = vgroup->GetIterator(ctx_, version, eids, ts_spans, block_filter,
                                     scan_cols, scan_cols, agg_extend_cols, scan_agg_types,
                                     schema_mgr, schema, &ts_iter, vgroup,
                                     ts_points, false, false, UINT64_MAX, fill_params);
        if (s != KStatus::SUCCESS || ts_iter == nullptr) {
          delete ts_iter;
          continue;
        }
        ResultSet res{static_cast<k_uint32>(scan_cols.size())};
        k_uint32 count = 0;
        bool is_finished = false;
        while (true) {
          s = ts_iter->Next(&res, &count, &is_finished);
          if (s != KStatus::SUCCESS || count == 0) {
            break;
          }
          total += count;
          res.clear();
        }
        delete ts_iter;
      }
    }
    return total;
  }

  [[nodiscard]] uint64_t QueryRowsInSpan(const std::shared_ptr<TsTableSchemaManager>& schema_mgr,
                                         uint32_t version,
                                         const KwTsSpan& ts_span,
                                         std::vector<k_uint32> scan_cols) const {
    std::shared_ptr<MMapMetricsTable> schema;
    if (schema_mgr->GetMetricSchema(version, &schema) != KStatus::SUCCESS) {
      return 0;
    }

    uint64_t total = 0;
    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
      }
      for (k_uint32 eid = 1; eid <= vgroup->GetMaxEntityID(); ++eid) {
        TsStorageIterator* ts_iter = nullptr;
        std::vector<uint32_t> eids = {eid};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter;
        std::vector<k_int32> agg_extend_cols;
        std::vector<Sumfunctype> scan_agg_types;
        std::vector<timestamp64> ts_points;
        FillParams fill_params;
        auto s = vgroup->GetIterator(ctx_, version, eids, ts_spans, block_filter,
                                     scan_cols, scan_cols, agg_extend_cols, scan_agg_types,
                                     schema_mgr, schema, &ts_iter, vgroup,
                                     ts_points, false, false, UINT64_MAX, fill_params);
        if (s != KStatus::SUCCESS || ts_iter == nullptr) {
          delete ts_iter;
          continue;
        }
        ResultSet res{static_cast<k_uint32>(scan_cols.size())};
        k_uint32 count = 0;
        bool is_finished = false;
        while (true) {
          s = ts_iter->Next(&res, &count, &is_finished);
          if (s != KStatus::SUCCESS || count == 0) {
            break;
          }
          total += count;
          res.clear();
        }
        delete ts_iter;
      }
    }
    return total;
  }

  [[nodiscard]] KStatus RunAggQuery(const std::shared_ptr<TsTableSchemaManager>& schema_mgr,
                                    uint32_t version,
                                    std::vector<k_uint32> scan_cols,
                                    std::vector<Sumfunctype> agg_types) const {
    std::shared_ptr<MMapMetricsTable> schema;
    if (schema_mgr->GetMetricSchema(version, &schema) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }

    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
      }
      for (k_uint32 eid = 1; eid <= vgroup->GetMaxEntityID(); ++eid) {
        TsStorageIterator* ts_iter = nullptr;
        std::vector<uint32_t> eids = {eid};
        KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter;
        std::vector<k_int32> agg_extend_cols(scan_cols.size(), -1);
        std::vector<timestamp64> ts_points;
        FillParams fill_params;
        auto s = vgroup->GetIterator(ctx_, version, eids, ts_spans, block_filter,
                                     scan_cols, scan_cols, agg_extend_cols, agg_types,
                                     schema_mgr, schema, &ts_iter, vgroup,
                                     ts_points, false, false, UINT64_MAX, fill_params);
        if (s != KStatus::SUCCESS || ts_iter == nullptr) {
          delete ts_iter;
          return s;
        }
        ResultSet res{static_cast<k_uint32>(scan_cols.size())};
        k_uint32 count = 0;
        bool is_finished = false;
        while (true) {
          s = ts_iter->Next(&res, &count, &is_finished);
          if (s != KStatus::SUCCESS) {
            delete ts_iter;
            return s;
          }
          if (count == 0) {
            break;
          }
          res.clear();
        }
        delete ts_iter;
      }
    }
    return KStatus::SUCCESS;
  }

  [[nodiscard]] QueryResultHolder FetchFirstNonEmptyResult(
      const std::shared_ptr<TsTableSchemaManager>& schema_mgr,
      uint32_t version,
      std::vector<k_uint32> scan_cols,
      std::vector<Sumfunctype> agg_types) const {
    QueryResultHolder result(static_cast<k_uint32>(scan_cols.size()));
    std::shared_ptr<MMapMetricsTable> schema;
    if (schema_mgr->GetMetricSchema(version, &schema) != KStatus::SUCCESS) {
      return result;
    }

    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
      }
      for (k_uint32 eid = 1; eid <= vgroup->GetMaxEntityID(); ++eid) {
        TsStorageIterator* ts_iter = nullptr;
        std::vector<uint32_t> eids = {eid};
        KwTsSpan ts_span = {INT64_MIN, INT64_MAX};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter;
        std::vector<k_int32> agg_extend_cols(scan_cols.size(), -1);
        std::vector<timestamp64> ts_points;
        FillParams fill_params;
        auto s = vgroup->GetIterator(ctx_, version, eids, ts_spans, block_filter,
                                     scan_cols, scan_cols, agg_extend_cols, agg_types,
                                     schema_mgr, schema, &ts_iter, vgroup,
                                     ts_points, false, false, UINT64_MAX, fill_params);
        if (s != KStatus::SUCCESS || ts_iter == nullptr) {
          delete ts_iter;
          continue;
        }
        result.count = 0;
        result.is_finished = false;
        result.status = ts_iter->Next(&result.res, &result.count, &result.is_finished);
        result.iter.reset(ts_iter);
        if (result.HasData()) {
          return result;
        }
        result.res.clear();
        result.iter.reset();
      }
    }
    return result;
  }

  [[nodiscard]] QueryResultHolder FetchFirstNonEmptyResultInSpan(
      const std::shared_ptr<TsTableSchemaManager>& schema_mgr,
      uint32_t version,
      const KwTsSpan& ts_span,
      std::vector<k_uint32> scan_cols) const {
    QueryResultHolder result(static_cast<k_uint32>(scan_cols.size()));
    std::shared_ptr<MMapMetricsTable> schema;
    if (schema_mgr->GetMetricSchema(version, &schema) != KStatus::SUCCESS) {
      return result;
    }

    auto* ts_vgroups = engine_->GetTsVGroups();
    for (const auto& vgroup : *ts_vgroups) {
      if (!vgroup || vgroup->GetMaxEntityID() < 1) {
        continue;
      }
      for (k_uint32 eid = 1; eid <= vgroup->GetMaxEntityID(); ++eid) {
        TsStorageIterator* ts_iter = nullptr;
        std::vector<uint32_t> eids = {eid};
        std::vector<KwTsSpan> ts_spans = {ts_span};
        std::vector<BlockFilter> block_filter;
        std::vector<k_int32> agg_extend_cols(scan_cols.size(), -1);
        std::vector<Sumfunctype> scan_agg_types;
        std::vector<timestamp64> ts_points;
        FillParams fill_params;
        auto s = vgroup->GetIterator(ctx_, version, eids, ts_spans, block_filter,
                                     scan_cols, scan_cols, agg_extend_cols, scan_agg_types,
                                     schema_mgr, schema, &ts_iter, vgroup,
                                     ts_points, false, false, UINT64_MAX, fill_params);
        if (s != KStatus::SUCCESS || ts_iter == nullptr) {
          delete ts_iter;
          continue;
        }
        result.count = 0;
        result.is_finished = false;
        result.status = ts_iter->Next(&result.res, &result.count, &result.is_finished);
        result.iter.reset(ts_iter);
        if (result.HasData()) {
          return result;
        }
        result.res.clear();
        result.iter.reset();
      }
    }
    return result;
  }
};

// ========================= AlterTable API: schema manager ALTER_COLUMN_TYPE =========================
// Covers the interface-layer alter-type behavior on TsTableSchemaManager:
// type verification, idempotency, and non-existent-column failure.
TEST_F(TsAlterTypeTest, SchemaManagerAlterTableAlterColumnType) {
  TSTableID table_id = 21010;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT});
  ASSERT_NE(schema_mgr, nullptr);

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(4);
  alter_col.set_name("column_4");
  alter_col.set_storage_type(roachpb::DOUBLE);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "AlterColumnType failed: " << msg;
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  std::vector<AttributeInfo> cols;
  s = schema_mgr->GetColumnsIncludeDropped(cols, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool found = false;
  for (const auto& col : cols) {
    if (col.id == 4) {
      ASSERT_EQ(col.type, static_cast<int32_t>(DATATYPE::DOUBLE));
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found);

  s = schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  roachpb::KWDBKTSColumn bad_col;
  bad_col.set_column_id(999);
  bad_col.set_name("noexist");
  bad_col.set_storage_type(roachpb::DOUBLE);
  bad_col.set_storage_len(8);
  bad_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);
  s = schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &bad_col, 2, 3, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= AlterTable API: engine schema manager ALTER_COLUMN_TYPE =========================
// Covers the engine-level wrapper forwarding ALTER_COLUMN_TYPE to the table schema manager.
TEST_F(TsAlterTypeTest, EngineSchemaManagerAlterTableAlterColumnType) {
  TSTableID table_id = 21011;
  ASSERT_NE(CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::FLOAT}), nullptr);

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(4);
  alter_col.set_name("column_4");
  alter_col.set_storage_type(roachpb::DOUBLE);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = eng_schema_mgr->AlterTable(ctx_, table_id, AlterType::ALTER_COLUMN_TYPE,
                                      &alter_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "EngineSchemaManager AlterTable failed: " << msg;

  std::shared_ptr<TsTableSchemaManager> tbl_mgr;
  bool is_dropped_flag = false;
  s = eng_schema_mgr->GetTableSchemaMgr(table_id, tbl_mgr, &is_dropped_flag);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(tbl_mgr, nullptr);
  ASSERT_EQ(tbl_mgr->GetCurrentVersion(), 2u);

  std::vector<AttributeInfo> cols;
  s = tbl_mgr->GetColumnsIncludeDropped(cols, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  bool found = false;
  for (const auto& col : cols) {
    if (col.id == 4) {
      ASSERT_EQ(col.type, static_cast<int32_t>(DATATYPE::DOUBLE));
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found);

  s = eng_schema_mgr->AlterTable(ctx_, 99996u, AlterType::ALTER_COLUMN_TYPE,
                                 &alter_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= Alter-type query coverage: fixed -> fixed =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryFixedToFixed) {
  TSTableID table_id = 21007;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertTestData(table_id, 101, 1000, 3, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::BIGINT);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  auto value = *reinterpret_cast<int64_t*>(result.res.data[1][0]->getData(0, sizeof(int64_t)));
  ASSERT_GE(value, 0);
  ASSERT_LE(value, 1024);
}

// ========================= Alter-type query coverage: float -> double =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryFloatToDouble) {
  TSTableID table_id = 21012;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::FLOAT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertTestData(table_id, 104, 1000, 3, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::DOUBLE);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  auto value = *reinterpret_cast<double*>(result.res.data[1][0]->getData(0, sizeof(double)));
  ASSERT_GE(value, 0.0);
  ASSERT_LE(value, 1024.0 * 1024.0);
}

// ========================= Alter-type query coverage: fixed -> var =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryFixedToVar) {
  TSTableID table_id = 21008;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::VARCHAR});
  ASSERT_NE(schema_mgr, nullptr);

  InsertTestData(table_id, 102, 1000, 3, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::VARCHAR);
  alter_col.set_storage_len(64);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_GT(result.res.data[1][0]->getDataLen(0), 0u);
  char first_char = *(result.res.data[1][0]->getData(0) + sizeof(uint16_t));
  ASSERT_GE(first_char, '0');
  ASSERT_LE(first_char, '9');
}

// ========================= Alter-type query coverage: double -> varchar =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryDoubleToVar) {
  TSTableID table_id = 21013;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::DOUBLE, roachpb::VARCHAR});
  ASSERT_NE(schema_mgr, nullptr);

  InsertTestData(table_id, 105, 1000, 3, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::VARCHAR);
  alter_col.set_storage_len(64);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_GT(result.res.data[1][0]->getDataLen(0), 0u);
  auto* converted = result.res.data[1][0]->getData(0) + sizeof(uint16_t);
  ASSERT_GE(*converted, '0');
  ASSERT_LE(*converted, '9');
}

// ========================= Alter-type query coverage: double -> varchar -> float =========================
// Query version 2 to validate DOUBLE->VARCHAR on old rows, then insert numeric VARCHAR rows
// under version 2 and alter to FLOAT. Query only the version-2 timestamp span so the final
// conversion path remains VARCHAR->FLOAT, which is allowed by the conversion rules.
TEST_F(TsAlterTypeTest, AlterTypeQueryDoubleToVarToFloatBySpan) {
  TSTableID table_id = 21015;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::DOUBLE, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertTestData(table_id, 107, 1000, 3, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::VARCHAR);
  alter_col.set_storage_len(64);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result_v2 = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result_v2.status, KStatus::SUCCESS);
  ASSERT_GT(result_v2.count, 0u);
  bool is_null = true;
  ASSERT_EQ(result_v2.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_GT(result_v2.res.data[1][0]->getDataLen(0), 0u);

  InsertNumericVarcharData(table_id, 107, 100000, {"1.25", "2.5", "3.75"}, 2);
  FlushAllVGroups();

  alter_col.set_storage_type(roachpb::FLOAT);
  alter_col.set_storage_len(4);
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 2, 3, msg),
            KStatus::SUCCESS);

  KwTsSpan ts_span = {100000, 102000};
  ASSERT_EQ(QueryRowsInSpan(schema_mgr, 3, ts_span, {0, 1, 2}), 3u);

  auto result_v3 = FetchFirstNonEmptyResultInSpan(schema_mgr, 3, ts_span, {0, 1, 2});
  ASSERT_EQ(result_v3.status, KStatus::SUCCESS);
  ASSERT_GT(result_v3.count, 0u);
  ASSERT_EQ(result_v3.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  float value = *reinterpret_cast<float*>(result_v3.res.data[1][0]->getData(0, sizeof(float)));
  ASSERT_GT(value, 1.0f);
  ASSERT_LT(value, 4.0f);
}

// ========================= Alter-type query coverage: numeric varchar -> float + FIRST/LAST =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryNumericVarToFloatAndAgg) {
  TSTableID table_id = 21009;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 103, 1000, {"12.5", "22.5", "32.5"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::FLOAT);
  alter_col.set_storage_len(4);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = false;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  float value = *reinterpret_cast<float*>(result.res.data[1][0]->getData(0, sizeof(float)));
  ASSERT_GT(value, 10.0f);
  ASSERT_LT(value, 40.0f);

  ASSERT_EQ(RunAggQuery(schema_mgr, 2, {1, 1}, {Sumfunctype::FIRST, Sumfunctype::LAST}),
            KStatus::SUCCESS);
}

// ========================= Alter-type query coverage: numeric varchar -> double + FIRST/LAST =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryNumericVarToDoubleAndAgg) {
  TSTableID table_id = 21014;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 106, 1000, {"101.125", "202.25", "303.5"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::DOUBLE);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_GT(result.count, 0u);

  bool is_null = false;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  double value = *reinterpret_cast<double*>(result.res.data[1][0]->getData(0, sizeof(double)));
  ASSERT_GT(value, 100.0);
  ASSERT_LT(value, 400.0);

  ASSERT_EQ(RunAggQuery(schema_mgr, 2, {1, 1}, {Sumfunctype::FIRST, Sumfunctype::LAST}),
            KStatus::SUCCESS);
}

// ========================= Alter-type query coverage: illegal varchar -> int becomes null =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToIntIllegalBecomeNull) {
  TSTableID table_id = 21016;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 108, 1000, {"1A", "b2", "1.1"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::INT);
  alter_col.set_storage_len(4);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 3u);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 3u);

  bool is_null = false;
  for (k_uint32 i = 0; i < result.count; ++i) {
    ASSERT_EQ(result.res.data[1][0]->isNull(i, &is_null), KStatus::SUCCESS);
    ASSERT_TRUE(is_null);
  }
}

// ========================= Alter-type query coverage: mixed varchar -> int valid/null bitmap =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToIntMixedValidity) {
  TSTableID table_id = 21017;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 109, 1000, {"123", "1A", "-45"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::INT);
  alter_col.set_storage_len(4);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 3u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int32_t*>(result.res.data[1][0]->getData(0, sizeof(int32_t))), 123);

  ASSERT_EQ(result.res.data[1][0]->isNull(1, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  ASSERT_EQ(result.res.data[1][0]->isNull(2, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int32_t*>(result.res.data[1][0]->getData(2, sizeof(int32_t))), -45);
}

// ========================= Alter-type query coverage: mixed varchar -> bigint valid/null bitmap =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToBigIntMixedValidity) {
  TSTableID table_id = 21018;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 110, 1000, {"922337203685", "b2", "-9000"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::BIGINT);
  alter_col.set_storage_len(8);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 3u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int64_t*>(result.res.data[1][0]->getData(0, sizeof(int64_t))), 922337203685LL);

  ASSERT_EQ(result.res.data[1][0]->isNull(1, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  ASSERT_EQ(result.res.data[1][0]->isNull(2, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int64_t*>(result.res.data[1][0]->getData(2, sizeof(int64_t))), -9000LL);
}

// ========================= Alter-type query coverage: varchar -> smallint boundary validity =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToSmallIntBoundaryMixedValidity) {
  TSTableID table_id = 21019;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 111, 1000,
                           {"-32768", "32767", "-32769", "32768", "1A", "b2", "1.1"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::SMALLINT);
  alter_col.set_storage_len(2);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 7u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(0, sizeof(int16_t))), -32768);

  ASSERT_EQ(result.res.data[1][0]->isNull(1, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(1, sizeof(int16_t))), 32767);

  for (k_uint32 row_idx = 2; row_idx < result.count; ++row_idx) {
    ASSERT_EQ(result.res.data[1][0]->isNull(row_idx, &is_null), KStatus::SUCCESS);
    ASSERT_TRUE(is_null);
  }
}

// ========================= Alter-type query coverage: varchar -> smallint alternating bitmap =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToSmallIntAlternatingBitmap) {
  TSTableID table_id = 21020;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 112, 1000,
                           {"12", "1A", "-7", "32768", "0", "1.1", "-32768"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::SMALLINT);
  alter_col.set_storage_len(2);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 7u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(0, sizeof(int16_t))), 12);

  ASSERT_EQ(result.res.data[1][0]->isNull(1, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  ASSERT_EQ(result.res.data[1][0]->isNull(2, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(2, sizeof(int16_t))), -7);

  ASSERT_EQ(result.res.data[1][0]->isNull(3, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  ASSERT_EQ(result.res.data[1][0]->isNull(4, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(4, sizeof(int16_t))), 0);

  ASSERT_EQ(result.res.data[1][0]->isNull(5, &is_null), KStatus::SUCCESS);
  ASSERT_TRUE(is_null);

  ASSERT_EQ(result.res.data[1][0]->isNull(6, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(6, sizeof(int16_t))), -32768);
}

// ========================= Alter-type query coverage: varchar -> smallint clustered invalid bitmap =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToSmallIntClusteredInvalidBitmap) {
  TSTableID table_id = 21021;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 113, 1000,
                           {"-5", "32767", "bad", "32768", "-32769", "x", "8"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::SMALLINT);
  alter_col.set_storage_len(2);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 7u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(0, sizeof(int16_t))), -5);

  ASSERT_EQ(result.res.data[1][0]->isNull(1, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(1, sizeof(int16_t))), 32767);

  for (k_uint32 row_idx = 2; row_idx <= 5; ++row_idx) {
    ASSERT_EQ(result.res.data[1][0]->isNull(row_idx, &is_null), KStatus::SUCCESS);
    ASSERT_TRUE(is_null);
  }

  ASSERT_EQ(result.res.data[1][0]->isNull(6, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(6, sizeof(int16_t))), 8);
}

// ========================= Alter-type query coverage: varchar -> smallint FIRST/LAST skip nulls =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryVarToSmallIntFirstLastSkipNulls) {
  TSTableID table_id = 21022;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::VARCHAR, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  InsertNumericVarcharData(table_id, 114, 1000,
                           {"bad", "-2", "oops", "32767", "NaN", "-32768", "bad"}, 1);
  FlushAllVGroups();

  roachpb::KWDBKTSColumn alter_col;
  alter_col.set_column_id(2);
  alter_col.set_name("column_2");
  alter_col.set_storage_type(roachpb::SMALLINT);
  alter_col.set_storage_len(2);
  alter_col.set_nullable(true);
  alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
            KStatus::SUCCESS);

  auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {1, 1},
                                         {Sumfunctype::FIRST, Sumfunctype::LAST});
  ASSERT_EQ(result.status, KStatus::SUCCESS);
  ASSERT_EQ(result.count, 1u);

  bool is_null = true;
  ASSERT_EQ(result.res.data[0][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[0][0]->getData(0, sizeof(int16_t))), -2);

  ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
  ASSERT_FALSE(is_null);
  ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(0, sizeof(int16_t))), -32768);
}

// ========================= End-to-end coverage for alter-type user path =========================
TEST_F(TsAlterTypeTest, AlterTypeQueryFixedToVarBySpecifiedValues) {
  struct TestCase {
    TSTableID table_id;
    roachpb::DataType source_type;
    NumericMetricValue source_value;
    std::string expected_text;
  };

  const std::vector<TestCase> cases = {
      {21023, roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(-123)}, "-123"},
      {21024, roachpb::INT, NumericMetricValue{static_cast<int32_t>(4567)}, "4567"},
      {21025, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(890123)}, "890123"},
      {21026, roachpb::FLOAT, NumericMetricValue{12.5f}, "12.5"},
      {21027, roachpb::DOUBLE, NumericMetricValue{345.25}, "345.25"},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(testing::Message() << "table_id=" << test_case.table_id);

    auto schema_mgr = CreateTableAndGetSchemaMgr(
        test_case.table_id, {roachpb::TIMESTAMP, test_case.source_type, roachpb::DOUBLE});
    ASSERT_NE(schema_mgr, nullptr);

    InsertTypedMetricData(test_case.table_id, test_case.table_id, 1000,
                          test_case.source_type, {test_case.source_value}, 1);
    FlushAllVGroups();

    roachpb::KWDBKTSColumn alter_col;
    alter_col.set_column_id(2);
    alter_col.set_name("column_2");
    alter_col.set_storage_type(roachpb::VARCHAR);
    alter_col.set_storage_len(64);
    alter_col.set_nullable(true);
    alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

    std::string msg;
    ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
              KStatus::SUCCESS)
        << msg;

    ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 1u);

    auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
    ASSERT_EQ(result.status, KStatus::SUCCESS);
    ASSERT_EQ(result.count, 1u);

    bool is_null = true;
    ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
    ASSERT_FALSE(is_null);
    ASSERT_GT(result.res.data[1][0]->getDataLen(0), 0u);

    std::string actual(result.res.data[1][0]->getData(0) + sizeof(uint16_t));
    ASSERT_EQ(actual, test_case.expected_text);
  }
}

TEST_F(TsAlterTypeTest, AlterTypeQueryFixedToFixedBySpecifiedValues) {
  struct TestCase {
    TSTableID table_id;
    roachpb::DataType target_type;
    NumericMetricValue expected_value;
  };

  auto assert_numeric_value = [](const QueryResultHolder& result,
                                 roachpb::DataType target_type,
                                 const NumericMetricValue& expected_value) {
    bool is_null = true;
    ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
    ASSERT_FALSE(is_null);

    switch (target_type) {
      case roachpb::INT:
        ASSERT_EQ(*reinterpret_cast<int32_t*>(result.res.data[1][0]->getData(0, sizeof(int32_t))),
                  std::get<int32_t>(expected_value));
        break;
      case roachpb::BIGINT:
        ASSERT_EQ(*reinterpret_cast<int64_t*>(result.res.data[1][0]->getData(0, sizeof(int64_t))),
                  std::get<int64_t>(expected_value));
        break;
      default:
        FAIL() << "Unsupported fixed target type: " << target_type;
    }
  };

  const std::vector<TestCase> cases = {
      {21028, roachpb::INT, NumericMetricValue{static_cast<int32_t>(123)}},
      {21029, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(123)}},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(testing::Message() << "table_id=" << test_case.table_id);

    auto schema_mgr = CreateTableAndGetSchemaMgr(
        test_case.table_id, {roachpb::TIMESTAMP, roachpb::SMALLINT, roachpb::DOUBLE});
    ASSERT_NE(schema_mgr, nullptr);

    InsertTypedMetricData(test_case.table_id, test_case.table_id, 1000,
                          roachpb::SMALLINT, {NumericMetricValue{static_cast<int16_t>(123)}}, 1);
    FlushAllVGroups();

    roachpb::KWDBKTSColumn alter_col;
    alter_col.set_column_id(2);
    alter_col.set_name("column_2");
    alter_col.set_storage_type(test_case.target_type);
    alter_col.set_storage_len(test_case.target_type == roachpb::BIGINT ? 8 : 4);
    alter_col.set_nullable(true);
    alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

    std::string msg;
    ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
              KStatus::SUCCESS)
        << msg;

    ASSERT_EQ(QueryAllRows(schema_mgr, 2, {0, 1, 2}), 1u);

    auto result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
    ASSERT_EQ(result.status, KStatus::SUCCESS);
    ASSERT_EQ(result.count, 1u);
    assert_numeric_value(result, test_case.target_type, test_case.expected_value);
  }
}

TEST_F(TsAlterTypeTest, AlterTypeQueryRequestedNumericChainsEndToEnd) {
  struct TestCase {
    TSTableID table_id;
    roachpb::DataType source_type;
    NumericMetricValue source_value;
    std::string expected_varchar;
    roachpb::DataType final_type;
    NumericMetricValue expected_final_value;
  };

  auto assert_numeric_value = [](const QueryResultHolder& result,
                                 roachpb::DataType target_type,
                                 const NumericMetricValue& expected_value) {
    bool is_null = true;
    ASSERT_EQ(result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
    ASSERT_FALSE(is_null);

    switch (target_type) {
      case roachpb::SMALLINT:
        ASSERT_EQ(*reinterpret_cast<int16_t*>(result.res.data[1][0]->getData(0, sizeof(int16_t))),
                  std::get<int16_t>(expected_value));
        break;
      case roachpb::INT:
        ASSERT_EQ(*reinterpret_cast<int32_t*>(result.res.data[1][0]->getData(0, sizeof(int32_t))),
                  std::get<int32_t>(expected_value));
        break;
      case roachpb::BIGINT:
        ASSERT_EQ(*reinterpret_cast<int64_t*>(result.res.data[1][0]->getData(0, sizeof(int64_t))),
                  std::get<int64_t>(expected_value));
        break;
      case roachpb::FLOAT:
        ASSERT_FLOAT_EQ(*reinterpret_cast<float*>(result.res.data[1][0]->getData(0, sizeof(float))),
                        std::get<float>(expected_value));
        break;
      case roachpb::DOUBLE:
        ASSERT_DOUBLE_EQ(*reinterpret_cast<double*>(result.res.data[1][0]->getData(0, sizeof(double))),
                         std::get<double>(expected_value));
        break;
      default:
        FAIL() << "Unsupported final target type: " << target_type;
    }
  };

  const std::vector<TestCase> cases = {
      {21030, roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(321)}, "321",
       roachpb::FLOAT, NumericMetricValue{321.0f}},
      {21031, roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(321)}, "321",
       roachpb::DOUBLE, NumericMetricValue{321.0}},
      {21032, roachpb::INT, NumericMetricValue{static_cast<int32_t>(1234)}, "1234",
       roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(1234)}},
      {21033, roachpb::INT, NumericMetricValue{static_cast<int32_t>(1234)}, "1234",
       roachpb::FLOAT, NumericMetricValue{1234.0f}},
      {21034, roachpb::INT, NumericMetricValue{static_cast<int32_t>(1234)}, "1234",
       roachpb::DOUBLE, NumericMetricValue{1234.0}},
      {21035, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(123)}, "123",
       roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(123)}},
      {21036, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(123456)}, "123456",
       roachpb::INT, NumericMetricValue{static_cast<int32_t>(123456)}},
      {21037, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(123456)}, "123456",
       roachpb::FLOAT, NumericMetricValue{123456.0f}},
      {21038, roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(123456)}, "123456",
       roachpb::DOUBLE, NumericMetricValue{123456.0}},
      {21039, roachpb::FLOAT, NumericMetricValue{2048.0f}, "2048",
       roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(2048)}},
      {21040, roachpb::FLOAT, NumericMetricValue{2048.0f}, "2048",
       roachpb::INT, NumericMetricValue{static_cast<int32_t>(2048)}},
      {21041, roachpb::FLOAT, NumericMetricValue{2048.0f}, "2048",
       roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(2048)}},
      {21042, roachpb::DOUBLE, NumericMetricValue{4096.0}, "4096",
       roachpb::SMALLINT, NumericMetricValue{static_cast<int16_t>(4096)}},
      {21043, roachpb::DOUBLE, NumericMetricValue{65536.0}, "65536",
       roachpb::INT, NumericMetricValue{static_cast<int32_t>(65536)}},
      {21044, roachpb::DOUBLE, NumericMetricValue{65536.0}, "65536",
       roachpb::BIGINT, NumericMetricValue{static_cast<int64_t>(65536)}},
      {21045, roachpb::DOUBLE, NumericMetricValue{123.5}, "123.5",
       roachpb::FLOAT, NumericMetricValue{123.5f}},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(testing::Message() << "table_id=" << test_case.table_id);

    auto schema_mgr = CreateTableAndGetSchemaMgr(
        test_case.table_id, {roachpb::TIMESTAMP, test_case.source_type, roachpb::DOUBLE});
    ASSERT_NE(schema_mgr, nullptr);

    InsertTypedMetricData(test_case.table_id, test_case.table_id, 1000,
                          test_case.source_type, {test_case.source_value}, 1);
    FlushAllVGroups();

    roachpb::KWDBKTSColumn alter_col;
    alter_col.set_column_id(2);
    alter_col.set_name("column_2");
    alter_col.set_storage_type(roachpb::VARCHAR);
    alter_col.set_storage_len(64);
    alter_col.set_nullable(true);
    alter_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

    std::string msg;
    ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 1, 2, msg),
              KStatus::SUCCESS)
        << msg;

    auto varchar_result = FetchFirstNonEmptyResult(schema_mgr, 2, {0, 1, 2}, {});
    ASSERT_EQ(varchar_result.status, KStatus::SUCCESS);
    ASSERT_EQ(varchar_result.count, 1u);

    bool is_null = true;
    ASSERT_EQ(varchar_result.res.data[1][0]->isNull(0, &is_null), KStatus::SUCCESS);
    ASSERT_FALSE(is_null);
    std::string actual_varchar(varchar_result.res.data[1][0]->getData(0) + sizeof(uint16_t));
    ASSERT_EQ(actual_varchar, test_case.expected_varchar);

    alter_col.set_storage_type(test_case.final_type);
    alter_col.set_storage_len(test_case.final_type == roachpb::SMALLINT ? 2 :
                              test_case.final_type == roachpb::BIGINT ? 8 :
                              test_case.final_type == roachpb::FLOAT ? 4 : 8);
    ASSERT_EQ(schema_mgr->AlterTable(ctx_, AlterType::ALTER_COLUMN_TYPE, &alter_col, 2, 3, msg),
              KStatus::SUCCESS)
        << msg;

    ASSERT_EQ(QueryAllRows(schema_mgr, 3, {0, 1, 2}), 1u);

    auto final_result = FetchFirstNonEmptyResult(schema_mgr, 3, {0, 1, 2}, {});
    ASSERT_EQ(final_result.status, KStatus::SUCCESS);
    ASSERT_EQ(final_result.count, 1u);
    assert_numeric_value(final_result, test_case.final_type, test_case.expected_final_value);
  }
}

