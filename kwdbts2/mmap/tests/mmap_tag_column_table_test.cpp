// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <string>
#include <memory>
#include <algorithm>
#include <cstdio>

#include "../include/mmap/mmap_tag_column_table.h"
#include "ts_common.h"

extern uint32_t k_entity_group_id_size;
extern uint32_t k_per_null_bitmap_size;
extern uint64_t BITMAP_PER_ROW_LENGTH;

class TestTagColumn : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/tag_col";
    db_path_ = "/tmp/kwdb_mmap_test/";
    db_name_ = "test_db/";
    mkdir((db_path_ + db_name_).c_str(), 0755);

    tag_info_.m_id = 1;
    tag_info_.m_data_type = DATATYPE::INT32;
    tag_info_.m_length = sizeof(int32_t);
    tag_info_.m_offset = 0;
    tag_info_.m_size = sizeof(int32_t);
    tag_info_.m_tag_type = GENERAL_TAG;
    tag_info_.m_flag = 0;
  }

  void TearDown() override {
    system(("rm -rf " + db_path_ + db_name_).c_str());
  }

  std::string test_path_;
  std::string db_path_;
  std::string db_name_;
  TagInfo tag_info_;
};

TEST_F(TestTagColumn, Constructor_BasicInitialization) {
  TagColumn col(0, tag_info_);

  EXPECT_EQ(col.attributeInfo().m_id, tag_info_.m_id);
  EXPECT_EQ(col.attributeInfo().m_data_type, tag_info_.m_data_type);
}

TEST_F(TestTagColumn, Constructor_WithPrimaryTag) {
  TagInfo ptag_info = tag_info_;
  ptag_info.m_tag_type = PRIMARY_TAG;
  TagColumn col(0, ptag_info);

  col.setPrimaryTag(true);
  EXPECT_TRUE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, Constructor_WithGeneralTag) {
  TagColumn col(0, tag_info_);

  EXPECT_FALSE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, Constructor_NegativeIndex) {
  TagInfo info = tag_info_;
  info.m_id = -1;
  TagColumn col(-1, info);

  EXPECT_EQ(col.attributeInfo().m_id, info.m_id);
}

TEST_F(TestTagColumn, AttributeInfo_GetTagInfo) {
  TagColumn col(0, tag_info_);

  TagInfo& retrieved_info = col.attributeInfo();

  EXPECT_EQ(retrieved_info.m_id, tag_info_.m_id);
  EXPECT_EQ(retrieved_info.m_data_type, tag_info_.m_data_type);
  EXPECT_EQ(retrieved_info.m_length, tag_info_.m_length);
}

TEST_F(TestTagColumn, PrimaryTag_SetAndGet) {
  TagColumn col(0, tag_info_);

  col.setPrimaryTag(true);
  EXPECT_TRUE(col.isPrimaryTag());

  col.setPrimaryTag(false);
  EXPECT_FALSE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, VarTag_NotVarTag) {
  TagColumn col(0, tag_info_);

  EXPECT_FALSE(col.isVarTag());
}

TEST_F(TestTagColumn, VarTag_WithVarString) {
  TagInfo var_info = tag_info_;
  var_info.m_data_type = DATATYPE::VARSTRING;
  TagColumn col(0, var_info);

  EXPECT_FALSE(col.isVarTag());
}

TEST_F(TestTagColumn, StoreOffset_SetAndGet) {
  TagColumn col(0, tag_info_);

  uint32_t test_offset = 1024;
  col.setStoreOffset(test_offset);

  EXPECT_EQ(col.getStoreOffset(), test_offset);
}

TEST_F(TestTagColumn, Resource_Constants) {
  EXPECT_EQ(k_per_null_bitmap_size, 1);
  EXPECT_GT(k_entity_group_id_size, 0);
  EXPECT_EQ(BITMAP_PER_ROW_LENGTH, 64);
}

TEST_F(TestTagColumn, DataType_Int32) {
  TagInfo info = tag_info_;
  info.m_data_type = DATATYPE::INT32;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_data_type, DATATYPE::INT32);
}

TEST_F(TestTagColumn, DataType_Int64) {
  TagInfo info = tag_info_;
  info.m_data_type = DATATYPE::INT64;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_data_type, DATATYPE::INT64);
}

TEST_F(TestTagColumn, DataType_Float) {
  TagInfo info = tag_info_;
  info.m_data_type = DATATYPE::FLOAT;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_data_type, DATATYPE::FLOAT);
}

TEST_F(TestTagColumn, DataType_Double) {
  TagInfo info = tag_info_;
  info.m_data_type = DATATYPE::DOUBLE;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_data_type, DATATYPE::DOUBLE);
}

TEST_F(TestTagColumn, DataType_Bool) {
  TagInfo info = tag_info_;
  info.m_data_type = DATATYPE::BOOL;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_data_type, DATATYPE::BOOL);
}

TEST_F(TestTagColumn, TagType_GeneralTag) {
  TagInfo info = tag_info_;
  info.m_tag_type = GENERAL_TAG;
  TagColumn col(0, info);

  EXPECT_FALSE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, Boundary_ZeroColumnIndex) {
  TagInfo info = tag_info_;
  TagColumn col(0, info);

  EXPECT_EQ(col.attributeInfo().m_id, info.m_id);
}

TEST_F(TestTagColumn, Boundary_LargeColumnIndex) {
  TagInfo info = tag_info_;
  TagColumn col(1000000, info);

  EXPECT_TRUE(true);
}

TEST_F(TestTagColumn, Resource_MultipleColumns) {
  std::vector<TagColumn*> columns;

  for (int i = 0; i < 5; ++i) {
    TagInfo info = tag_info_;
    info.m_id = i + 1;
    columns.push_back(new TagColumn(i, info));
  }

  EXPECT_EQ(columns.size(), 5);

  for (auto col : columns) {
    delete col;
  }
}

class TestMMapTagColumnTable : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    db_path_ = "/tmp/kwdb_mmap_test/";
    tbl_sub_path_ = "test_table/";
    table_name_ = "tag_table";
    table_path_ = tbl_sub_path_ + table_name_;
    entity_group_id_ = 1;
    table_version_ = 1;
    mkdir((db_path_ + tbl_sub_path_).c_str(), 0755);

    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = DATATYPE::STRING;
    ptag_info.m_length = 64;
    ptag_info.m_size = 64;
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    schema_.push_back(ptag_info);

    TagInfo ntag_info;
    ntag_info.m_id = 2;
    ntag_info.m_data_type = DATATYPE::INT32;
    ntag_info.m_length = sizeof(int32_t);
    ntag_info.m_size = sizeof(int32_t);
    ntag_info.m_tag_type = GENERAL_TAG;
    ntag_info.m_flag = 0;
    schema_.push_back(ntag_info);
  }

  void TearDown() override {
    system(("rm -rf " + db_path_ + tbl_sub_path_).c_str());
  }

  MMapTagColumnTable* CreateTable() {
    MMapTagColumnTable* table = new MMapTagColumnTable();
    return table;
  }

  int CreateTableWithData(MMapTagColumnTable* table, ErrorInfo& err_info) {
    int open_result = table->open(table_name_, db_path_, tbl_sub_path_, O_RDWR | O_CREAT | O_EXCL, err_info);
    if (open_result < 0 && err_info.errcode != -2) {
      return err_info.errcode;
    }
    return table->create(schema_, entity_group_id_, table_version_, err_info);
  }

  std::string db_path_;
  std::string tbl_sub_path_;
  std::string table_name_;
  std::string table_path_;
  int32_t entity_group_id_;
  uint32_t table_version_;
  std::vector<TagInfo> schema_;
};

TEST_F(TestMMapTagColumnTable, Constructor_BasicInitialization) {
  MMapTagColumnTable table;

  SUCCEED();
}

TEST_F(TestMMapTagColumnTable, Destructor_BasicCleanup) {
  MMapTagColumnTable* table = new MMapTagColumnTable();
  delete table;
  SUCCEED();
}

TEST_F(TestMMapTagColumnTable, Insert_Basic) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char record[256] = {0};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, record, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Insert_MultipleRows) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  for (int i = 1; i <= 4; ++i) {
    char record[256] = {0};
    size_t row_id = 0;

    int result = table.insert(entity_group_id_, i, 100 + i, 0, OperateType::Insert, record, &row_id);

    EXPECT_GE(result, 0);
  }
}

TEST_F(TestMMapTagColumnTable, Insert_WithDeleteFlag) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char record[256] = {0};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Delete, record, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Insert_WithUpdateFlag) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char record[256] = {0};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Update, record, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Size_AfterCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t size = table.size();

  EXPECT_EQ(size, 0);
}

TEST_F(TestMMapTagColumnTable, Size_AfterInsert) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char record[256] = {0};
  size_t row_id = 0;
  table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, record, &row_id);

  EXPECT_EQ(table.size(), 1);
}

TEST_F(TestMMapTagColumnTable, ActualSize_AfterCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t actual = table.actual_size();

  EXPECT_EQ(actual, 0);
}

TEST_F(TestMMapTagColumnTable, ReserveRowCount_AfterCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t reserve_count = table.reserveRowCount();

  EXPECT_GT(reserve_count, 0);
}

TEST_F(TestMMapTagColumnTable, NumColumn_AfterCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  int num_cols = table.numColumn();

  EXPECT_EQ(num_cols, schema_.size());
}

TEST_F(TestMMapTagColumnTable, RecordSize_AfterCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t record_size = table.recordSize();

  EXPECT_GT(record_size, 0);
}

TEST_F(TestMMapTagColumnTable, IsValidRow_InitiallyFalse) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  bool valid = table.isValidRow(1);

  EXPECT_FALSE(valid);
}

TEST_F(TestMMapTagColumnTable, SetAndUnsetDeleteMark) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  table.setDeleteMark(1);
  EXPECT_TRUE(table.isValidRow(1));

  table.unsetDeleteMark(1);
  EXPECT_FALSE(table.isValidRow(1));
}

TEST_F(TestMMapTagColumnTable, MetaData_Access) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  TagTableMetaData& meta = table.metaData();

  EXPECT_EQ(meta.m_ts_version, table_version_);
  EXPECT_EQ(meta.m_entitygroup_id, entity_group_id_);
}

TEST_F(TestMMapTagColumnTable, GetIncludeDroppedSchemaInfos) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  const std::vector<TagInfo>& infos = table.getIncludeDroppedSchemaInfos();

  EXPECT_EQ(infos.size(), schema_.size());
}

TEST_F(TestMMapTagColumnTable, GetSchemaInfo) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  const std::vector<TagColumn*>& cols = table.getSchemaInfo();

  EXPECT_EQ(cols.size(), schema_.size());
}

TEST_F(TestMMapTagColumnTable, PrimaryTagSize) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t ptag_size = table.primaryTagSize();

  EXPECT_GT(ptag_size, 0);
}

TEST_F(TestMMapTagColumnTable, GetColumnSize) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  size_t col_size = table.getColumnSize(0);

  EXPECT_GT(col_size, 0);
}

TEST_F(TestMMapTagColumnTable, GetTagColOff) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  uint32_t offset = table.getTagColOff(1);

  EXPECT_GE(offset, 0);
}

TEST_F(TestMMapTagColumnTable, GetTagColSize) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  uint32_t size = table.getTagColSize(2);

  EXPECT_GT(size, 0);
}

TEST_F(TestMMapTagColumnTable, Name) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  const std::string& name = table.name();

  EXPECT_EQ(name, table_name_);
}

TEST_F(TestMMapTagColumnTable, Sandbox) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  const std::string& sandbox = table.sandbox();

  EXPECT_EQ(sandbox, tbl_sub_path_);
}

TEST_F(TestMMapTagColumnTable, Sync_Basic) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  table.sync(0);

  SUCCEED();
}

TEST_F(TestMMapTagColumnTable, LinkNTagHashIndex_NullPtr) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  MMapTagColumnTable* old_part = nullptr;
  int result = table.linkNTagHashIndex(table_version_, old_part, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapTagColumnTable, InitNTagHashIndex_Basic) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  int result = table.initNTagHashIndex(err_info);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Resource_MultipleTables) {
  std::vector<std::unique_ptr<MMapTagColumnTable>> tables;

  for (int i = 0; i < 3; ++i) {
    tables.push_back(std::make_unique<MMapTagColumnTable>());
  }

  EXPECT_EQ(tables.size(), 3);
}

TEST_F(TestMMapTagColumnTable, Boundary_ZeroEntityGroupId) {
  MMapTagColumnTable table;
  ErrorInfo err_info;

  int result = table.create(schema_, 0, table_version_, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapTagColumnTable, TagInfo_IsDropped) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  const std::vector<TagInfo>& infos = table.getIncludeDroppedSchemaInfos();

  for (const auto& info : infos) {
    EXPECT_FALSE(info.isDropped());
  }
}

TEST_F(TestMMapTagColumnTable, GetColumnsByRownum_Empty) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  std::vector<uint32_t> src_scan_tags;
  std::vector<TagInfo> result_tag_infos;
  kwdbts::ResultSet res;

  int result = table.getColumnsByRownum(0, src_scan_tags, result_tag_infos, &res);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, SetDropped) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  table.setDropped();

  EXPECT_TRUE(table.isDropped());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
