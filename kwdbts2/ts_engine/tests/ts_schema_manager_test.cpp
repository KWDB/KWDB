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
#include "cm_kwdb_context.h"
#include "ts_test_base.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "ts_engine.h"
#include "sys_utils.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

const string schema_test_root_path = "./tsdb_schema";

/**
 * @brief Test fixture dedicated to TsTableSchemaManager and TsEngineSchemaManager.
 */
class TsSchemaTest : public TsEngineTestBase {
 public:
  TsSchemaTest() {
    InitContext();
    InitEngine(schema_test_root_path);
  }

 protected:
  /**
   * @brief Helper: create a table and return its TsTableSchemaManager.
   */
  [[nodiscard]] std::shared_ptr<TsTableSchemaManager> CreateTableAndGetSchemaMgr(
      TSTableID table_id,
      const std::vector<roachpb::DataType>& metric_type =
          {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE, roachpb::DOUBLE}) const {
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

};

// ========================= TsTableSchemaManager basic property accessors =========================
// Covers: GetTableId, GetCurrentVersion, GetHashNum, GetTagTable, GetDbID,
//         GetPartitionInterval/SetPartitionInterval, GetLifeTime/SetLifeTime,
//         GetTsColDataType, GetMetricSchemaPath, GetTagSchemaPath, IsDropped,
//         GetCurrentMetricsTable
TEST_F(TsSchemaTest, SchemaManagerBasicProperties) {
  TSTableID table_id = 20001;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // GetTableId
  ASSERT_EQ(schema_mgr->GetTableId(), table_id);
  // GetCurrentVersion
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 1u);
  // GetHashNum
  ASSERT_GT(schema_mgr->GetHashNum(), 0u);
  // GetTagTable
  ASSERT_NE(schema_mgr->GetTagTable(), nullptr);
  // GetDbID
  ASSERT_GT(schema_mgr->GetDbID(), 0u);
  // GetPartitionInterval
  ASSERT_GT(schema_mgr->GetPartitionInterval(), 0u);

  // SetPartitionInterval / GetPartitionInterval
  uint64_t new_interval = 7 * 86400;
  schema_mgr->SetPartitionInterval(new_interval);
  ASSERT_EQ(schema_mgr->GetPartitionInterval(), new_interval);

  // GetLifeTime / SetLifeTime
  LifeTime lt = schema_mgr->GetLifeTime();
  lt.ts = 3600LL * 24 * 30;
  schema_mgr->SetLifeTime(lt);
  LifeTime lt2 = schema_mgr->GetLifeTime();
  ASSERT_EQ(lt2.ts, lt.ts);

  // GetTsColDataType: first column is TIMESTAMP → TIMESTAMP64
  DATATYPE ts_type = schema_mgr->GetTsColDataType();
  ASSERT_EQ(ts_type, DATATYPE::TIMESTAMP64);

  // GetMetricSchemaPath / GetTagSchemaPath
  ASSERT_FALSE(schema_mgr->GetMetricSchemaPath().empty());
  ASSERT_FALSE(schema_mgr->GetTagSchemaPath().empty());

  // IsDropped: table has not been dropped yet
  ASSERT_FALSE(schema_mgr->IsDropped());

  // GetCurrentMetricsTable
  auto cur_metrics = schema_mgr->GetCurrentMetricsTable();
  ASSERT_NE(cur_metrics, nullptr);
}

// ========================= GetColumns* family =========================
// Covers: GetColumnsExcludeDropped, GetColumnsIncludeDropped,
//         GetColumnsExcludeDroppedPtr, GetColumnsIncludeDroppedPtr
//         (both valid and invalid version paths)
TEST_F(TsSchemaTest, SchemaManagerGetColumns) {
  TSTableID table_id = 20002;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT,
                                              roachpb::DOUBLE, roachpb::DOUBLE};
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id, metric_type);
  ASSERT_NE(schema_mgr, nullptr);

  // GetColumnsExcludeDropped with default version=0 (resolves to current version)
  std::vector<AttributeInfo> cols_excl;
  auto s = schema_mgr->GetColumnsExcludeDropped(cols_excl);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_excl.size(), metric_type.size());

  // GetColumnsExcludeDropped with explicit version=1
  std::vector<AttributeInfo> cols_excl_v1;
  s = schema_mgr->GetColumnsExcludeDropped(cols_excl_v1, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_excl_v1.size(), metric_type.size());

  // GetColumnsIncludeDropped with default version
  std::vector<AttributeInfo> cols_incl;
  s = schema_mgr->GetColumnsIncludeDropped(cols_incl);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_incl.size(), metric_type.size());

  // GetColumnsExcludeDroppedPtr
  const std::vector<AttributeInfo>* ptr_excl = nullptr;
  s = schema_mgr->GetColumnsExcludeDroppedPtr(&ptr_excl);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(ptr_excl, nullptr);
  ASSERT_EQ(ptr_excl->size(), metric_type.size());

  // GetColumnsIncludeDroppedPtr
  const std::vector<AttributeInfo>* ptr_incl = nullptr;
  s = schema_mgr->GetColumnsIncludeDroppedPtr(&ptr_incl);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(ptr_incl, nullptr);
  ASSERT_EQ(ptr_incl->size(), metric_type.size());

  // Non-existent version → FAIL for all four variants
  std::vector<AttributeInfo> cols_bad;
  s = schema_mgr->GetColumnsExcludeDropped(cols_bad, 999);
  ASSERT_EQ(s, KStatus::FAIL);

  s = schema_mgr->GetColumnsIncludeDropped(cols_bad, 999);
  ASSERT_EQ(s, KStatus::FAIL);

  const std::vector<AttributeInfo>* bad_ptr = nullptr;
  s = schema_mgr->GetColumnsExcludeDroppedPtr(&bad_ptr, 999);
  ASSERT_EQ(s, KStatus::FAIL);

  s = schema_mgr->GetColumnsIncludeDroppedPtr(&bad_ptr, 999);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= GetMetricSchema / GetTagSchema / GetAllVersions / IsExistTableVersion =========================
TEST_F(TsSchemaTest, SchemaManagerMetricTagSchemaAndVersions) {
  TSTableID table_id = 20003;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // GetMetricSchema with explicit version=1
  std::shared_ptr<MMapMetricsTable> metric_schema;
  auto s = schema_mgr->GetMetricSchema(1, &metric_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(metric_schema, nullptr);

  // GetMetricSchema with version=0 returns current version
  std::shared_ptr<MMapMetricsTable> metric_schema_cur;
  s = schema_mgr->GetMetricSchema(0, &metric_schema_cur);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(metric_schema_cur, nullptr);

  // GetMetricSchema with non-existent version → FAIL
  std::shared_ptr<MMapMetricsTable> metric_schema_bad;
  s = schema_mgr->GetMetricSchema(999, &metric_schema_bad);
  ASSERT_EQ(s, KStatus::FAIL);

  // GetTagSchema
  std::shared_ptr<TagTable> tag_schema;
  s = schema_mgr->GetTagSchema(ctx_, &tag_schema);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(tag_schema, nullptr);

  // GetAllVersions: at least version 1 must exist
  std::vector<uint32_t> versions;
  schema_mgr->GetAllVersions(&versions);
  ASSERT_FALSE(versions.empty());

  // IsExistTableVersion: existing version returns true, non-existent returns false
  ASSERT_TRUE(schema_mgr->IsExistTableVersion(1));
  ASSERT_FALSE(schema_mgr->IsExistTableVersion(999));
}

// ========================= GetIdxForValidCols =========================
TEST_F(TsSchemaTest, SchemaManagerGetIdxForValidCols) {
  TSTableID table_id = 20004;
  std::vector<roachpb::DataType> metric_type{roachpb::TIMESTAMP, roachpb::INT,
                                              roachpb::DOUBLE, roachpb::DOUBLE};
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id, metric_type);
  ASSERT_NE(schema_mgr, nullptr);

  // Explicit version
  std::vector<uint32_t> valid_idx;
  auto s = schema_mgr->GetIdxForValidCols(valid_idx, 1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(valid_idx.size(), metric_type.size());

  // Default version=0 resolves to current version
  std::vector<uint32_t> valid_idx_default;
  s = schema_mgr->GetIdxForValidCols(valid_idx_default);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(valid_idx_default.size(), metric_type.size());

  // Non-existent version → FAIL
  std::vector<uint32_t> invalid_idx;
  s = schema_mgr->GetIdxForValidCols(invalid_idx, 999);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= FindVersionConv / InsertVersionConv =========================
TEST_F(TsSchemaTest, SchemaManagerVersionConv) {
  TSTableID table_id = 20005;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // Cache is initially empty
  std::shared_ptr<SchemaVersionConv> conv;
  ASSERT_FALSE(schema_mgr->FindVersionConv(12345ULL, &conv));

  // Insert a version conversion object
  std::shared_ptr<MMapMetricsTable> scan_metric;
  auto s = schema_mgr->GetMetricSchema(1, &scan_metric);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::vector<uint32_t> blk_cols_extended;
  auto ver_conv = std::make_shared<SchemaVersionConv>(
      1, blk_cols_extended, scan_metric, scan_metric);
  schema_mgr->InsertVersionConv(12345ULL, ver_conv);

  // FindVersionConv succeeds after insert
  std::shared_ptr<SchemaVersionConv> found_conv;
  ASSERT_TRUE(schema_mgr->FindVersionConv(12345ULL, &found_conv));
  ASSERT_NE(found_conv, nullptr);
  ASSERT_EQ(found_conv->scan_version_, 1u);

  // A different key is still not found
  ASSERT_FALSE(schema_mgr->FindVersionConv(99999ULL, &found_conv));
}

// ========================= AlterTable: ADD_COLUMN for metric column =========================
// Covers: alterTableCol(ADD_COLUMN), addMetricForAlter, idempotency, and duplicate-column failure
TEST_F(TsSchemaTest, SchemaManagerAlterTableAddMetricColumn) {
  TSTableID table_id = 20006;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // Build a new BIGINT metric column
  roachpb::KWDBKTSColumn new_col;
  new_col.set_column_id(10);
  new_col.set_name("new_bigint_col");
  new_col.set_storage_type(roachpb::BIGINT);
  new_col.set_storage_len(8);
  new_col.set_nullable(true);
  new_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &new_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "AddColumn failed: " << msg;
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  // Schema at version 2 should have 5 columns (4 original + 1 added)
  std::vector<AttributeInfo> cols_incl;
  s = schema_mgr->GetColumnsIncludeDropped(cols_incl, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_incl.size(), 5u);

  // Idempotency: same (cur_version, new_version) pair → SUCCESS
  s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &new_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Adding the same column again with a newer version → FAIL (column already exists)
  s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &new_col, 2, 3, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= AlterTable: DROP_COLUMN for metric column =========================
// Covers: alterTableCol(DROP_COLUMN), dropped-column visibility, idempotency, and failure paths
TEST_F(TsSchemaTest, SchemaManagerAlterTableDropMetricColumn) {
  TSTableID table_id = 20007;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // Drop column id=3 (DOUBLE)
  roachpb::KWDBKTSColumn drop_col;
  drop_col.set_column_id(3);
  drop_col.set_name("column_3");
  drop_col.set_storage_type(roachpb::DOUBLE);
  drop_col.set_storage_len(8);
  drop_col.set_nullable(true);
  drop_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::DROP_COLUMN, &drop_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "DropColumn failed: " << msg;
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  // GetColumnsExcludeDropped returns 3 columns (1 dropped)
  std::vector<AttributeInfo> cols_excl;
  s = schema_mgr->GetColumnsExcludeDropped(cols_excl, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_excl.size(), 3u);

  // GetColumnsIncludeDropped still returns all 4 columns
  std::vector<AttributeInfo> cols_incl;
  s = schema_mgr->GetColumnsIncludeDropped(cols_incl, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_incl.size(), 4u);

  // Idempotency: same version pair → SUCCESS
  s = schema_mgr->AlterTable(ctx_, AlterType::DROP_COLUMN, &drop_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // Dropping an already-dropped column with a new version → FAIL
  s = schema_mgr->AlterTable(ctx_, AlterType::DROP_COLUMN, &drop_col, 2, 3, msg);
  ASSERT_EQ(s, KStatus::FAIL);

  // Dropping a non-existent column with a new version → FAIL
  roachpb::KWDBKTSColumn noexist_col;
  noexist_col.set_column_id(999);
  noexist_col.set_name("noexist");
  noexist_col.set_storage_type(roachpb::DOUBLE);
  noexist_col.set_storage_len(8);
  noexist_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);
  s = schema_mgr->AlterTable(ctx_, AlterType::DROP_COLUMN, &noexist_col, 2, 3, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}


// ========================= AlterTable: unrecognized storage type → FAIL =========================
// DECIMAL is a valid proto enum value but falls through to the default case in
// parseAttrInfo, which returns FAIL and sets an error message.
TEST_F(TsSchemaTest, SchemaManagerAlterTableUnknownStorageType) {
  TSTableID table_id = 20009;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  roachpb::KWDBKTSColumn bad_col;
  bad_col.set_column_id(10);
  bad_col.set_name("bad_col");
  bad_col.set_storage_type(roachpb::DECIMAL);
  bad_col.set_storage_len(8);
  bad_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &bad_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::FAIL);
  ASSERT_FALSE(msg.empty());
}

// ========================= AlterTable: unsupported AlterType → FAIL =========================
// ALTER_PARTITION_INTERVAL(=4) is not handled inside alterTableCol, so the
// default switch branch returns FAIL.
TEST_F(TsSchemaTest, SchemaManagerAlterTableDefaultAlterType) {
  TSTableID table_id = 20010;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  roachpb::KWDBKTSColumn col;
  col.set_column_id(2);
  col.set_name("column_2");
  col.set_storage_type(roachpb::INT);
  col.set_storage_len(4);
  col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ALTER_PARTITION_INTERVAL, &col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= UndoAlterTable: early-return path =========================
// When cur_version_ equals new_version (i.e. after a successful AlterTable),
// UndoAlterTable returns SUCCESS immediately without rolling back the schema.
TEST_F(TsSchemaTest, SchemaManagerUndoAlterTableEarlyReturn) {
  TSTableID table_id = 20011;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // Apply ADD_COLUMN: version 1 → 2
  roachpb::KWDBKTSColumn new_col;
  new_col.set_column_id(10);
  new_col.set_name("new_col");
  new_col.set_storage_type(roachpb::BIGINT);
  new_col.set_storage_len(8);
  new_col.set_nullable(true);
  new_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &new_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  // UndoAlterTable: cur_version_(2) == new_version(2) triggers early return
  s = schema_mgr->UndoAlterTable(ctx_, AlterType::ADD_COLUMN, &new_col, 1, 2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // Version remains at 2 because the early return skipped the actual rollback
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);
}

// ========================= AlterTable: ADD_COLUMN for a general tag column =========================
// Covers the alterTableTag code path and the UpdateMetricVersion call it triggers.
TEST_F(TsSchemaTest, SchemaManagerAlterTableAddGeneralTag) {
  TSTableID table_id = 20012;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // TYPE_TAG maps to COL_GENERAL_TAG, routing the call through alterTableTag
  roachpb::KWDBKTSColumn new_tag;
  new_tag.set_column_id(20);
  new_tag.set_name("new_general_tag");
  new_tag.set_storage_type(roachpb::INT);
  new_tag.set_storage_len(4);
  new_tag.set_nullable(true);
  new_tag.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_TAG);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &new_tag, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "AddGeneralTag failed: " << msg;
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);
}

// ========================= IsDropped / SetDropped =========================
TEST_F(TsSchemaTest, SchemaManagerSetDropped) {
  TSTableID table_id = 20013;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  ASSERT_FALSE(schema_mgr->IsDropped());
  schema_mgr->SetDropped();
  ASSERT_TRUE(schema_mgr->IsDropped());
}

// ========================= Multi-version AlterTable (ADD + DROP) =========================
// Verifies correct column counts after two consecutive schema changes and that
// GetAllVersions / IsExistTableVersion / GetIdxForValidCols all reflect the new state.
TEST_F(TsSchemaTest, SchemaManagerMultiAlterColumns) {
  TSTableID table_id = 20014;
  auto schema_mgr = CreateTableAndGetSchemaMgr(table_id);
  ASSERT_NE(schema_mgr, nullptr);

  // Step 1: ADD_COLUMN → version 1 to 2
  roachpb::KWDBKTSColumn add_col;
  add_col.set_column_id(15);
  add_col.set_name("multi_add_col");
  add_col.set_storage_type(roachpb::BIGINT);
  add_col.set_storage_len(8);
  add_col.set_nullable(true);
  add_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = schema_mgr->AlterTable(ctx_, AlterType::ADD_COLUMN, &add_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 2u);

  // GetAllVersions must include both version 1 and version 2
  std::vector<uint32_t> versions;
  schema_mgr->GetAllVersions(&versions);
  ASSERT_GE(versions.size(), 2u);

  // Step 2: DROP_COLUMN col=2 → version 2 to 3
  roachpb::KWDBKTSColumn drop_col;
  drop_col.set_column_id(2);
  drop_col.set_name("column_2");
  drop_col.set_storage_type(roachpb::INT);
  drop_col.set_storage_len(4);
  drop_col.set_nullable(true);
  drop_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  s = schema_mgr->AlterTable(ctx_, AlterType::DROP_COLUMN, &drop_col, 2, 3, msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(schema_mgr->GetCurrentVersion(), 3u);

  // At version 3: 5 total columns, 1 dropped → 4 active
  std::vector<AttributeInfo> cols_excl;
  s = schema_mgr->GetColumnsExcludeDropped(cols_excl, 3);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_excl.size(), 4u);

  // All 5 columns (including the dropped one) visible when included
  std::vector<AttributeInfo> cols_incl;
  s = schema_mgr->GetColumnsIncludeDropped(cols_incl, 3);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(cols_incl.size(), 5u);

  // Version existence checks
  ASSERT_TRUE(schema_mgr->IsExistTableVersion(2));
  ASSERT_TRUE(schema_mgr->IsExistTableVersion(3));
  ASSERT_FALSE(schema_mgr->IsExistTableVersion(999));

  // Active column index count matches active column count
  std::vector<uint32_t> valid_idx;
  s = schema_mgr->GetIdxForValidCols(valid_idx, 3);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(valid_idx.size(), 4u);
}

// ========================= TsEngineSchemaManager: IsTableExist / GetTableList / GetAllTableSchemaMgrs =========================
TEST_F(TsSchemaTest, EngineSchemaManagerTableExistAndList) {
  TSTableID table_id1 = 21001;
  TSTableID table_id2 = 21002;

  ASSERT_NE(CreateTableAndGetSchemaMgr(
      table_id1, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE}), nullptr);
  ASSERT_NE(CreateTableAndGetSchemaMgr(
      table_id2, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE}), nullptr);

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();

  // IsTableExist: created tables are found, a random ID is not
  ASSERT_TRUE(eng_schema_mgr->IsTableExist(table_id1));
  ASSERT_TRUE(eng_schema_mgr->IsTableExist(table_id2));
  ASSERT_FALSE(eng_schema_mgr->IsTableExist(99999u));

  // GetTableList returns at least the two tables just created
  std::vector<TSTableID> table_list;
  auto s = eng_schema_mgr->GetTableList(&table_list);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GE(table_list.size(), 2u);

  // GetAllTableSchemaMgrs returns one manager per table
  std::vector<std::shared_ptr<TsTableSchemaManager>> all_mgrs;
  s = eng_schema_mgr->GetAllTableSchemaMgrs(all_mgrs);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GE(all_mgrs.size(), 2u);
}

// ========================= TsEngineSchemaManager: GetDBIDByTableID =========================
TEST_F(TsSchemaTest, EngineSchemaManagerGetDBIDByTableID) {
  TSTableID table_id = 21003;
  ASSERT_NE(CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE}), nullptr);

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();

  // A valid table returns a non-zero database ID
  uint32_t db_id = eng_schema_mgr->GetDBIDByTableID(table_id);
  ASSERT_GT(db_id, 0u);

  // A non-existent table returns 0
  uint32_t invalid_db_id = eng_schema_mgr->GetDBIDByTableID(99998u);
  ASSERT_EQ(invalid_db_id, 0u);
}

// ========================= TsEngineSchemaManager: SetTableDropped =========================
TEST_F(TsSchemaTest, EngineSchemaManagerSetTableDropped) {
  TSTableID table_id = 21004;
  ASSERT_NE(CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE}), nullptr);

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();
  ASSERT_TRUE(eng_schema_mgr->IsTableExist(table_id));

  // SetTableDropped removes the table from the internal map
  auto s = eng_schema_mgr->SetTableDropped(table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // After dropping, GetTableSchemaMgr must fail and report is_dropped=true
  std::shared_ptr<TsTableSchemaManager> tb_mgr;
  bool is_dropped_flag = false;
  s = eng_schema_mgr->GetTableSchemaMgr(table_id, tb_mgr, &is_dropped_flag);
  ASSERT_EQ(s, KStatus::FAIL);
  ASSERT_TRUE(is_dropped_flag);

  // Calling SetTableDropped on a non-existent table is a no-op → SUCCESS
  s = eng_schema_mgr->SetTableDropped(99997u);
  ASSERT_EQ(s, KStatus::SUCCESS);
}

// ========================= TsEngineSchemaManager: AlterTable =========================
// Covers the engine-level AlterTable wrapper and the failure path for an unknown table.
TEST_F(TsSchemaTest, EngineSchemaManagerAlterTable) {
  TSTableID table_id = 21005;
  ASSERT_NE(CreateTableAndGetSchemaMgr(table_id), nullptr);

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();

  // Add a new column through the engine schema manager
  roachpb::KWDBKTSColumn new_col;
  new_col.set_column_id(11);
  new_col.set_name("engine_new_col");
  new_col.set_storage_type(roachpb::BIGINT);
  new_col.set_storage_len(8);
  new_col.set_nullable(true);
  new_col.set_col_type(roachpb::KWDBKTSColumn_ColumnType_TYPE_DATA);

  std::string msg;
  auto s = eng_schema_mgr->AlterTable(ctx_, table_id, AlterType::ADD_COLUMN,
                                       &new_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::SUCCESS) << "EngineSchemaManager AlterTable failed: " << msg;

  // Verify that the table schema manager has advanced to version 2
  std::shared_ptr<TsTableSchemaManager> tbl_mgr;
  bool is_dropped_flag = false;
  s = eng_schema_mgr->GetTableSchemaMgr(table_id, tbl_mgr, &is_dropped_flag);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(tbl_mgr->GetCurrentVersion(), 2u);

  // AlterTable on a non-existent table → FAIL
  s = eng_schema_mgr->AlterTable(ctx_, 99996u, AlterType::ADD_COLUMN,
                                   &new_col, 1, 2, msg);
  ASSERT_EQ(s, KStatus::FAIL);
}

// ========================= TsEngineSchemaManager: GetVGroup =========================
// Verifies that GetVGroup allocates a valid vgroup via consistent hash for a new
// primary key and returns the same vgroup_id for the same key on repeated calls.
TEST_F(TsSchemaTest, EngineSchemaManagerGetVGroup) {
  TSTableID table_id = 21006;
  auto schema_mgr = CreateTableAndGetSchemaMgr(
      table_id, {roachpb::TIMESTAMP, roachpb::INT, roachpb::DOUBLE});
  ASSERT_NE(schema_mgr, nullptr);

  // Construct an 8-byte TIMESTAMP primary key
  int64_t pk_val = 100086LL;
  TSSlice primary_key{reinterpret_cast<char*>(&pk_val), sizeof(pk_val)};

  uint32_t vgroup_id = 0;
  TSEntityID entity_id = 0;
  bool new_tag = false;

  auto& eng_schema_mgr = engine_->GetEngineSchemaManager();
  auto s = eng_schema_mgr->GetVGroup(ctx_, schema_mgr, primary_key,
                                      &vgroup_id, &entity_id, &new_tag);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_GT(vgroup_id, 0u);
  // No data has been inserted, so this is a new tag
  ASSERT_TRUE(new_tag);

  // Same primary key must always hash to the same vgroup
  uint32_t vgroup_id2 = 0;
  TSEntityID entity_id2 = 0;
  bool new_tag2 = false;
  s = eng_schema_mgr->GetVGroup(ctx_, schema_mgr, primary_key,
                                  &vgroup_id2, &entity_id2, &new_tag2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(vgroup_id, vgroup_id2);
}
