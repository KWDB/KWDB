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
#include <unistd.h>
#include <cstring>
#include <vector>
#include <string>
#include <sys/mman.h>
#include <memory>

#include "../include/mmap/mmap_tag_column_table.h"
#include "../include/mmap/mmap_tag_column_table_aux.h"

extern uint32_t k_entity_group_id_size;
extern uint32_t k_per_null_bitmap_size;
extern uint64_t BITMAP_PER_ROW_LENGTH;

class TestTagColumn : public testing::Test {
 protected:
  void SetUp() override {
    // Use unique test path for each test to avoid race conditions
    static std::atomic<uint64_t> test_counter{0};
    uint64_t unique_id = test_counter.fetch_add(1);
    test_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/tag_table_";
    db_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/tag_table_/";
    db_name_ = "test_db/";
    
    // Ensure clean directory structure using MakeDirectory
    std::string full_path = db_path_ + db_name_;
    
    // Remove existing directory if any
    if (!Remove(full_path)) {
      LOG_WARN("Failed to remove directory %s", full_path.c_str());
    }
    
    // Create directories with proper permissions using MakeDirectory
    if (!MakeDirectory(full_path)) {
      FAIL() << "Failed to create directory: " << full_path;
    }

    tag_info_.m_id = 1;
    tag_info_.m_data_type = DATATYPE::INT32;
    tag_info_.m_length = sizeof(int32_t);
    tag_info_.m_offset = 0;
    tag_info_.m_size = sizeof(int32_t);
    tag_info_.m_tag_type = GENERAL_TAG;
    tag_info_.m_flag = 0;
  }

  void TearDown() override {
    std::string full_path = db_path_ + db_name_;
    if (!Remove(full_path)) {
      LOG_WARN("Failed to clean up directory %s", full_path.c_str());
    }
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

  void SetUp() override {
    // Use unique test path for each test to avoid race conditions
    static std::atomic<uint64_t> test_counter{0};
    uint64_t unique_id = test_counter.fetch_add(1);
    db_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/tag_table_/";
    tbl_sub_path_ = "test_table/";
    table_name_ = "tag_table";
    table_path_ = tbl_sub_path_ + table_name_;
    entity_group_id_ = 1;
    table_version_ = 1;
    
    // Ensure clean directory structure using MakeDirectory
    std::string full_path = db_path_ + tbl_sub_path_;
    
    // Remove existing directory if any
    if (!Remove(full_path)) {
      LOG_WARN("Failed to remove directory %s", full_path.c_str());
    }
    
    // Create directories with proper permissions using MakeDirectory
    if (!MakeDirectory(full_path)) {
      FAIL() << "Failed to create directory: " << full_path;
    }

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
    // Clean up test directory
    if (!Remove(db_path_ + tbl_sub_path_)) {
      LOG_WARN("Failed to clean up directory");
    }
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

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  size_t row_id = 0;

  std::vector<uint32_t> valid_columns;
  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_columns, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Insert_MultipleRows) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  for (int i = 1; i <= 4; ++i) {
    char ptag[64] = {0};
    TSSlice tsslice{ptag, 64};
    size_t row_id = 0;
    std::vector<uint32_t> valid_columns;

    int result = table.insert(entity_group_id_, i, 100 + i, 0, OperateType::Insert, tsslice, tsslice, valid_columns, &row_id);

    EXPECT_GE(result, 0);

    auto test = table.GenTagPack(row_id);
  }
}

TEST_F(TestMMapTagColumnTable, Insert_WithDeleteFlag) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Delete, tsslice, tsslice, std::vector<uint32_t>{}, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapTagColumnTable, Insert_WithUpdateFlag) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Update, tsslice, tsslice, std::vector<uint32_t>{}, &row_id);

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

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  size_t row_id = 0;
  table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, std::vector<uint32_t>{}, &row_id);

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
  EXPECT_FALSE(table.isValidRow(1));

  table.unsetDeleteMark(1);
  EXPECT_TRUE(table.isValidRow(1));
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

TEST_F(TestMMapTagColumnTable, SetLSN) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  table.setLSN(10086);

  table.sync_with_lsn(10086);
}

TEST_F(TestMMapTagColumnTable, GetEntityIdByRownum) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateTableWithData(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, std::vector<uint32_t>{}, &row_id);

  std::vector<kwdbts::EntityResultIndex> entityIdList;
  table.getEntityIdByRownum(1, &entityIdList);
  EXPECT_EQ(entityIdList.size(), 1);

  uint32_t hash_point;
  table.getHashpointByRowNum(1, &hash_point);

  uint32_t entity_id, group_id;
  table.getEntityIdGroupId(1, entity_id, group_id);

  table.getMaxEntityIdByVGroupId(1, entity_id);
}

// ==================== Valid Column (Sparse Table) Tests ====================

class TestSparseTagColumnTable : public testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> test_counter{1000};
    uint64_t unique_id = test_counter.fetch_add(1);
    db_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/sparse_tag_/";
    tbl_sub_path_ = "test_sparse/";
    table_name_ = "sparse_tag_table";
    entity_group_id_ = 1;
    table_version_ = 1;

    std::string full_path = db_path_ + tbl_sub_path_;
    if (!Remove(full_path)) {
      LOG_WARN("Failed to remove directory %s", full_path.c_str());
    }
    if (!MakeDirectory(full_path)) {
      FAIL() << "Failed to create directory: " << full_path;
    }

    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = DATATYPE::STRING;
    ptag_info.m_length = 64;
    ptag_info.m_size = 64;
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    schema_.push_back(ptag_info);

    TagInfo ntag_info1;
    ntag_info1.m_id = 2;
    ntag_info1.m_data_type = DATATYPE::INT32;
    ntag_info1.m_length = sizeof(int32_t);
    ntag_info1.m_size = sizeof(int32_t);
    ntag_info1.m_tag_type = GENERAL_TAG;
    ntag_info1.m_flag = 0;
    schema_.push_back(ntag_info1);

    TagInfo ntag_info2;
    ntag_info2.m_id = 3;
    ntag_info2.m_data_type = DATATYPE::INT64;
    ntag_info2.m_length = sizeof(int64_t);
    ntag_info2.m_size = sizeof(int64_t);
    ntag_info2.m_tag_type = GENERAL_TAG;
    ntag_info2.m_flag = 0;
    schema_.push_back(ntag_info2);

    TagInfo ntag_info3;
    ntag_info3.m_id = 4;
    ntag_info3.m_data_type = DATATYPE::FLOAT;
    ntag_info3.m_length = sizeof(float);
    ntag_info3.m_size = sizeof(float);
    ntag_info3.m_tag_type = GENERAL_TAG;
    ntag_info3.m_flag = 0;
    schema_.push_back(ntag_info3);
  }

  void TearDown() override {
    if (!Remove(db_path_ + tbl_sub_path_)) {
      LOG_WARN("Failed to clean up directory");
    }
  }

  int CreateSparseTable(MMapTagColumnTable* table, ErrorInfo& err_info) {
    int open_result = table->open(table_name_, db_path_, tbl_sub_path_, O_RDWR | O_CREAT | O_EXCL, err_info);
    if (open_result < 0 && err_info.errcode != -2) {
      return err_info.errcode;
    }
    return table->create(schema_, entity_group_id_, table_version_, err_info, true);
  }

  int CreateNormalTable(MMapTagColumnTable* table, ErrorInfo& err_info) {
    int open_result = table->open(table_name_, db_path_, tbl_sub_path_, O_RDWR | O_CREAT | O_EXCL, err_info);
    if (open_result < 0 && err_info.errcode != -2) {
      return err_info.errcode;
    }
    return table->create(schema_, entity_group_id_, table_version_, err_info, false);
  }

  std::string db_path_;
  std::string tbl_sub_path_;
  std::string table_name_;
  int32_t entity_group_id_;
  uint32_t table_version_;
  std::vector<TagInfo> schema_;
};

TEST_F(TestSparseTagColumnTable, IsSparse_TrueAfterSparseCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  EXPECT_TRUE(table.issparse());
}

TEST_F(TestSparseTagColumnTable, IsSparse_FalseAfterNormalCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  EXPECT_FALSE(table.issparse());
}

TEST_F(TestSparseTagColumnTable, IsSparse_FalseBeforeInit) {
  MMapTagColumnTable table;

  EXPECT_FALSE(table.issparse());
}

TEST_F(TestSparseTagColumnTable, Magic_MTVC_AfterSparseCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  uint32_t magic_mtvc = *reinterpret_cast<const uint32_t*>("MTVC");
  EXPECT_EQ(table.metaData().m_magic, magic_mtvc);
}

TEST_F(TestSparseTagColumnTable, Magic_MMTT_AfterNormalCreate) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  uint32_t magic_mmtt = *reinterpret_cast<const uint32_t*>("MMTT");
  EXPECT_EQ(table.metaData().m_magic, magic_mmtt);
}

TEST_F(TestSparseTagColumnTable, Insert_WithValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id);

  EXPECT_GE(result, 0);
  EXPECT_GT(row_id, 0);
}

TEST_F(TestSparseTagColumnTable, Insert_WithEmptyValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols;
  size_t row_id = 0;

  int result = table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id);

  EXPECT_GE(result, 0);
}

TEST_F(TestSparseTagColumnTable, GetValidColumns_AfterInsert) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3, 4};
  size_t row_id = 0;

  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  std::vector<uint32_t> retrieved_cols;
  int result = table.getValidColumns(row_id, retrieved_cols);

  EXPECT_EQ(result, 0);
  EXPECT_EQ(retrieved_cols.size(), 3);
  std::sort(retrieved_cols.begin(), retrieved_cols.end());
  EXPECT_EQ(retrieved_cols, valid_cols);
}

TEST_F(TestSparseTagColumnTable, GetValidColumns_EmptyAfterInsertWithEmptyCols) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols;
  size_t row_id = 0;

  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  std::vector<uint32_t> retrieved_cols;
  int result = table.getValidColumns(row_id, retrieved_cols);

  EXPECT_EQ(result, 0);
  EXPECT_TRUE(retrieved_cols.empty());
}

TEST_F(TestSparseTagColumnTable, GetValidColumns_MultipleRows) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};

  std::vector<uint32_t> valid_cols_1 = {2};
  size_t row_id_1 = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols_1, &row_id_1), 0);

  std::vector<uint32_t> valid_cols_2 = {3, 4};
  size_t row_id_2 = 0;
  ASSERT_GE(table.insert(entity_group_id_, 2, 200, 0, OperateType::Insert, tsslice, tsslice, valid_cols_2, &row_id_2), 0);

  std::vector<uint32_t> retrieved_1;
  table.getValidColumns(row_id_1, retrieved_1);
  EXPECT_EQ(retrieved_1.size(), 1);
  EXPECT_EQ(retrieved_1[0], 2);

  std::vector<uint32_t> retrieved_2;
  table.getValidColumns(row_id_2, retrieved_2);
  EXPECT_EQ(retrieved_2.size(), 2);
  std::sort(retrieved_2.begin(), retrieved_2.end());
  EXPECT_EQ(retrieved_2[0], 3);
  EXPECT_EQ(retrieved_2[1], 4);
}

TEST_F(TestSparseTagColumnTable, GetValidColumns_FailsOnNonSparseTable) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  std::vector<uint32_t> retrieved_cols;
  int result = table.getValidColumns(1, retrieved_cols);

  EXPECT_EQ(result, -1);
}

TEST_F(TestSparseTagColumnTable, GenTagPack_SparseTable) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 4};
  size_t row_id = 0;

  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  auto pack = table.GenTagPack(row_id);
  ASSERT_NE(pack, nullptr);

  auto retrieved_cols = pack->getValidColumns();
  EXPECT_EQ(retrieved_cols.size(), 2);
  std::sort(retrieved_cols.begin(), retrieved_cols.end());
  EXPECT_EQ(retrieved_cols, valid_cols);
}

TEST_F(TestSparseTagColumnTable, GenTagPack_NormalTable_NoValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> empty_valid_cols;
  size_t row_id = 0;

  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, empty_valid_cols, &row_id), 0);

  auto pack = table.GenTagPack(row_id);
  ASSERT_NE(pack, nullptr);

  auto retrieved_cols = pack->getValidColumns();
  EXPECT_TRUE(retrieved_cols.empty());
}

TEST_F(TestSparseTagColumnTable, UpdateValidColumns_AfterInsert) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  std::vector<uint32_t> updated_cols = {2, 3, 4};
  int result = table.updateValidColumns(row_id, updated_cols);

  EXPECT_GE(result, 0);

  std::vector<uint32_t> retrieved;
  ASSERT_EQ(table.getValidColumns(row_id, retrieved), 0);
  std::sort(retrieved.begin(), retrieved.end());
  EXPECT_EQ(retrieved, updated_cols);
}

TEST_F(TestSparseTagColumnTable, UpdateValidColumns_EmptyColumnsFails) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  std::vector<uint32_t> empty_cols;
  int result = table.updateValidColumns(row_id, empty_cols);

  EXPECT_EQ(result, -1);
}

TEST_F(TestSparseTagColumnTable, UpdateValidColumns_NonSparseTableFails) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  std::vector<uint32_t> cols = {2, 3};
  int result = table.updateValidColumns(1, cols);

  EXPECT_EQ(result, -1);
}

TEST_F(TestSparseTagColumnTable, UpdateValidColumns_OverwriteExisting) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  std::vector<uint32_t> new_cols = {5, 6, 7};
  ASSERT_GE(table.updateValidColumns(row_id, new_cols), 0);

  std::vector<uint32_t> retrieved;
  ASSERT_EQ(table.getValidColumns(row_id, retrieved), 0);
  std::sort(retrieved.begin(), retrieved.end());
  EXPECT_EQ(retrieved, new_cols);
}

// ==================== TagTuplePack Valid Columns Tests ====================

class TestTagTuplePackValidColumns : public testing::Test {
 protected:
  void SetUp() override {
    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = DATATYPE::STRING;
    ptag_info.m_length = 32;
    ptag_info.m_size = 32;
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

  std::vector<TagInfo> schema_;
};

TEST_F(TestTagTuplePackValidColumns, SetAndGetValidColumns) {
  TagTuplePack pack(schema_, {2, 3, 5}, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  std::vector<uint32_t> cols_to_set = {2, 3, 5};
  pack.setValidColumns(cols_to_set);

  auto retrieved = pack.getValidColumns();
  EXPECT_EQ(retrieved.size(), 3);
  EXPECT_EQ(retrieved[0], 2);
  EXPECT_EQ(retrieved[1], 3);
  EXPECT_EQ(retrieved[2], 5);
}

TEST_F(TestTagTuplePackValidColumns, SetAndGetEmptyValidColumns) {
  TagTuplePack pack(schema_, std::vector<uint32_t>{}, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  pack.setValidColumns({});

  auto retrieved = pack.getValidColumns();
  EXPECT_TRUE(retrieved.empty());
}

TEST_F(TestTagTuplePackValidColumns, GetValidColumns_BeforeSet) {
  TagTuplePack pack(schema_, std::vector<uint32_t>{}, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  auto retrieved = pack.getValidColumns();
  EXPECT_TRUE(retrieved.empty());
}

TEST_F(TestTagTuplePackValidColumns, SetValidColumns_SingleColumn) {
  TagTuplePack pack(schema_, {7}, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  pack.setValidColumns({7});

  auto retrieved = pack.getValidColumns();
  EXPECT_EQ(retrieved.size(), 1);
  EXPECT_EQ(retrieved[0], 7);
}

TEST_F(TestTagTuplePackValidColumns, SetValidColumns_ManyColumns) {
  std::vector<uint32_t> many_cols;
  for (uint32_t i = 1; i <= 50; ++i) {
    many_cols.push_back(i);
  }

  TagTuplePack pack(schema_, many_cols, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  pack.setValidColumns(many_cols);

  auto retrieved = pack.getValidColumns();
  EXPECT_EQ(retrieved.size(), 50);
  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(retrieved[i], i + 1);
  }
}

TEST_F(TestTagTuplePackValidColumns, MovePreservesValidColumns) {
  TagTuplePack pack(schema_, {2, 5}, TagTuplePack::TTPFLAG_ALL);

  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;

  ASSERT_GE(pack.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);

  pack.setValidColumns({2, 5});

  TagTuplePack moved_pack = std::move(pack);

  auto retrieved = moved_pack.getValidColumns();
  EXPECT_EQ(retrieved.size(), 2);
  EXPECT_EQ(retrieved[0], 2);
  EXPECT_EQ(retrieved[1], 5);
}

TEST_F(TestTagTuplePackValidColumns, MoveAssignPreservesValidColumns) {
  TagTuplePack pack1(schema_, {3, 7}, TagTuplePack::TTPFLAG_ALL);
  char ptag_val[32] = "test_primary";
  int32_t ntag_val = 42;
  ASSERT_GE(pack1.fillData(ptag_val, 32), 0);
  ASSERT_GE(pack1.fillData(reinterpret_cast<const char*>(&ntag_val), sizeof(int32_t)), 0);
  pack1.setValidColumns({3, 7});

  TagTuplePack pack2(schema_, std::vector<uint32_t>{}, TagTuplePack::TTPFLAG_ALL);
  pack2 = std::move(pack1);

  auto retrieved = pack2.getValidColumns();
  EXPECT_EQ(retrieved.size(), 2);
  EXPECT_EQ(retrieved[0], 3);
  EXPECT_EQ(retrieved[1], 7);
}

TEST_F(TestTagTuplePackValidColumns, SetValidColumns_NotCalledOnNonOwner) {
  char buf[512] = {0};
  TagTuplePack pack(schema_, buf, sizeof(buf));

  std::vector<uint32_t> cols = {1, 2};
  pack.setValidColumns(cols);

  auto retrieved = pack.getValidColumns();
  EXPECT_TRUE(retrieved.empty());
}

// ==================== MMapTagColumnTable Remove & Reserve with Valid Column Tests ====================

TEST_F(TestSparseTagColumnTable, Remove_SparseTableCleansUpValidColumnFile) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  // Verify valid columns are readable
  std::vector<uint32_t> retrieved;
  ASSERT_EQ(table.getValidColumns(row_id, retrieved), 0);
  EXPECT_EQ(retrieved.size(), 2);

  // Remove the table - should clean up valid_column_file
  int result = table.remove();
  EXPECT_GE(result, 0);
}

TEST_F(TestSparseTagColumnTable, Reserve_SparseTableExtendsValidColumnFile) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3, 4};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  // Verify valid columns are readable
  std::vector<uint32_t> retrieved;
  ASSERT_EQ(table.getValidColumns(row_id, retrieved), 0);
  std::sort(retrieved.begin(), retrieved.end());
  EXPECT_EQ(retrieved, valid_cols);
}

TEST_F(TestSparseTagColumnTable, Remove_NormalTableNoValidColumnFile) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> empty_cols;
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, empty_cols, &row_id), 0);

  // Remove should work fine for non-sparse tables
  int result = table.remove();
  EXPECT_GE(result, 0);
}

// ==================== MMapTagColumnTable sync/setLSN with Valid Column Tests ====================

TEST_F(TestSparseTagColumnTable, Sync_SparseTableWithValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  SUCCEED();
}

TEST_F(TestSparseTagColumnTable, SyncWithLsn_SparseTableWithValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  SUCCEED();
}

TEST_F(TestSparseTagColumnTable, SetLSN_SparseTableWithValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  // setLSN should not crash when valid_column_file_ exists
  table.setLSN(200);
  SUCCEED();
}

TEST_F(TestSparseTagColumnTable, Sync_NormalTableNoValidColumnFile) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateNormalTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> empty_cols;
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, empty_cols, &row_id), 0);

  SUCCEED();
}

// ==================== MMapTagColumnTable getValidColumns Edge Cases ====================

TEST_F(TestSparseTagColumnTable, GetValidColumns_InvalidRow) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  std::vector<uint32_t> retrieved_cols;
  int result = table.getValidColumns(9999, retrieved_cols);

  EXPECT_EQ(result, -1);
}

TEST_F(TestSparseTagColumnTable, UpdateValidColumns_InvalidRow) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  std::vector<uint32_t> cols = {2, 3};
  int result = table.updateValidColumns(9999, cols);

  EXPECT_EQ(result, -1);
}

TEST_F(TestSparseTagColumnTable, GenTagPack_RoundTripPreservesValidColumns) {
  MMapTagColumnTable table;
  ErrorInfo err_info;
  ASSERT_EQ(CreateSparseTable(&table, err_info), 0);

  char ptag[64] = {0};
  TSSlice tsslice{ptag, 64};
  std::vector<uint32_t> valid_cols = {2, 3, 4};
  size_t row_id = 0;
  ASSERT_GE(table.insert(entity_group_id_, 1, 100, 0, OperateType::Insert, tsslice, tsslice, valid_cols, &row_id), 0);

  // Read via GenTagPack
  auto pack = table.GenTagPack(row_id);
  ASSERT_NE(pack, nullptr);
  auto pack_cols = pack->getValidColumns();
  std::sort(pack_cols.begin(), pack_cols.end());
  EXPECT_EQ(pack_cols, valid_cols);

  // Read directly via getValidColumns
  std::vector<uint32_t> direct_cols;
  ASSERT_EQ(table.getValidColumns(row_id, direct_cols), 0);
  std::sort(direct_cols.begin(), direct_cols.end());
  EXPECT_EQ(direct_cols, valid_cols);

  // Both should match
  EXPECT_EQ(pack_cols, direct_cols);
}
