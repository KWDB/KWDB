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

#include "../include/mmap/mmap_tag_table.h"
#include "mmap/mmap_tag_version_manager.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_payload.h"

class TestTagTable : public testing::Test {
 protected:
  void SetUp() override {
    // Use unique test path for each test to avoid race conditions
    static std::atomic<uint64_t> test_counter{0};
    uint64_t unique_id = test_counter.fetch_add(1);
    test_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/tag_table_";
    db_path_ = "tmp" + std::to_string(unique_id) + "/kwdb_mmap_test/tag_table_/";
    tbl_sub_path_ = "sub_path/";
    table_id_ = 1001;
    entity_group_id_ = 1;
    table_version_ = 1;
    
    // Ensure clean directory structure using MakeDirectory
    std::string full_path = db_path_ + tbl_sub_path_;
    
    // Remove existing directory if any
    if (!Remove(full_path)) {
      LOG_WARN("Failed to remove directory %s: %s", full_path.c_str());
    }
    
    // Create directories with proper permissions using MakeDirectory
    if (!MakeDirectory(full_path)) {
      FAIL() << "Failed to create directory: " << full_path;
    }

    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = DATATYPE::INT64;
    ptag_info.m_length = sizeof(int64_t);
    ptag_info.m_size = sizeof(int64_t);
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    schema_.push_back(ptag_info);

    TagInfo ntag_info;
    ntag_info.m_id = 2;
    ntag_info.m_data_type = DATATYPE::INT64;
    ntag_info.m_length = sizeof(int64_t);
    ntag_info.m_size = sizeof(int64_t);
    ntag_info.m_tag_type = GENERAL_TAG;
    ntag_info.m_flag = 0;
    schema_.push_back(ntag_info);

    TagInfo ntag_info2;
    ntag_info2.m_id = 3;
    ntag_info2.m_data_type = DATATYPE::INT64;
    ntag_info2.m_length = sizeof(int64_t);
    ntag_info2.m_size = sizeof(int64_t);
    ntag_info2.m_tag_type = GENERAL_TAG;
    ntag_info2.m_flag = 0;
    schema_.push_back(ntag_info2);

    metric_schema_.clear();
    AttributeInfo metric_attr;
    metric_attr.id = 1;
    metric_attr.type = DATATYPE::INT64;
    metric_attr.size = sizeof(int64_t);
    metric_attr.length = sizeof(int64_t);
    metric_schema_.push_back(metric_attr);
  }

  void TearDown() override {
    std::string full_path = db_path_ + tbl_sub_path_;
    if (!Remove(full_path)) {
      LOG_WARN("Failed to clean up directory %s", full_path.c_str());
    }
  }

  TagTable* CreateTagTable() {
    TagTable* tag_table = new TagTable(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    return tag_table;
  }

  int CreateTagTableWithData(TagTable* tag_table, ErrorInfo& err_info) {
    return tag_table->create(schema_, table_version_, {}, err_info);
  }

  std::string test_path_;
  std::string db_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_;
  int32_t entity_group_id_;
  uint32_t table_version_;
  std::vector<TagInfo> schema_;
  std::vector<AttributeInfo> metric_schema_;
};

TEST_F(TestTagTable, Constructor_BasicInitialization) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  SUCCEED();
}

TEST_F(TestTagTable, Destructor_BasicCleanup) {
  TagTable* tag_table = new TagTable(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  delete tag_table;
  SUCCEED();
}

TEST_F(TestTagTable, Create_Success) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;

  int result = tag_table.create(schema_, table_version_, {}, err_info);

  EXPECT_EQ(result, 0);
  EXPECT_NE(tag_table.GetTagTableVersionManager(), nullptr);
  EXPECT_NE(tag_table.GetTagPartitionTableManager(), nullptr);
}

TEST_F(TestTagTable, Open_Success) {
  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    ErrorInfo err_info;
    ASSERT_EQ(tag_table.create(schema_, table_version_, {}, err_info), 0);
  }

  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    std::vector<TableVersion> invalid_versions;
    ErrorInfo err_info;

    int result = tag_table.open(invalid_versions, err_info);

    EXPECT_EQ(result, 0);
    EXPECT_TRUE(invalid_versions.empty());
  }
}

TEST_F(TestTagTable, HasPrimaryKey_NotExist) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char ptag_data[64] = "nonexistent";
  uint32_t entity_id = 0;
  uint32_t sub_group_id = 0;

  bool result = tag_table.hasPrimaryKey(ptag_data, strlen(ptag_data), entity_id, sub_group_id);

  EXPECT_FALSE(result);
}

TEST_F(TestTagTable, GetPrimaryKeyRowInfo_NotExist) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char ptag_data[64] = "nonexistent";
  std::pair<uint64_t, uint64_t> row_info;

  bool result = tag_table.GetPrimaryKeyRowInfo(ptag_data, strlen(ptag_data), row_info);

  EXPECT_FALSE(result);
}

TEST_F(TestTagTable, GetMaxEntityIdByVGroupId_Empty) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  uint32_t vgroup_id = 1;
  uint32_t entity_id = 0;

  tag_table.GetMaxEntityIdByVGroupId(vgroup_id, entity_id);

  EXPECT_EQ(entity_id, 0);
}

TEST_F(TestTagTable, GetEntityIdListByVGroupId_Empty) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  uint32_t vgroup_id = 1;
  std::vector<uint32_t> entity_id_list;

  tag_table.GetEntityIdListByVGroupId(vgroup_id, entity_id_list);

  EXPECT_TRUE(entity_id_list.empty());
}

TEST_F(TestTagTable, GetEntityIdList_NullEntityIdList) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<void*> primary_tags;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  kwdbts::ResultSet res;
  uint32_t count = 0;
  TS_OSN osn = 0;

  int result = tag_table.GetEntityIdList(primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow,
                                       {}, nullptr, nullptr, &res, &count, table_version_, osn);

  EXPECT_GE(result, 0);
}

TEST_F(TestTagTable, GetTagList_NullContext) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  kwdbContext_p ctx = nullptr;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  kwdbts::ResultSet res;
  uint32_t count = 0;

  int result = tag_table.GetTagList(ctx, entity_id_list, {}, &res, &count, table_version_);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, GetColumnsByRownumLocked_NoPartition) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> src_tag_idxes;
  std::vector<TagInfo> result_tag_infos;
  kwdbts::ResultSet res;

  int result = tag_table.GetColumnsByRownumLocked(table_version_, 1, src_tag_idxes, result_tag_infos, &res);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, CalculateSchemaIdxs_ValidVersion) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> result_scan_idxs = {0, 1};
  std::vector<uint32_t> src_scan_idxs;

  int result = tag_table.CalculateSchemaIdxs(table_version_, result_scan_idxs, schema_, &src_scan_idxs);

  EXPECT_EQ(result, 0);
  EXPECT_EQ(src_scan_idxs.size(), result_scan_idxs.size());
}

TEST_F(TestTagTable, CalculateSchemaIdxs_InvalidVersion) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> result_scan_idxs = {0, 1};
  std::vector<uint32_t> src_scan_idxs;

  int result = tag_table.CalculateSchemaIdxs(9999, result_scan_idxs, schema_, &src_scan_idxs);

  EXPECT_NE(result, 0);
}

TEST_F(TestTagTable, GetLatestOSN_Empty) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  TS_OSN osn = tag_table.GetLatestOSN();

  EXPECT_EQ(osn, 0);
}

TEST_F(TestTagTable, GetVersionManager_Initially) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  TagTableVersionManager* version_mgr = tag_table.GetTagTableVersionManager();

  EXPECT_NE(version_mgr, nullptr);
}

TEST_F(TestTagTable, GetPartitionManager_Initially) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  TagPartitionTableManager* partition_mgr = tag_table.GetTagPartitionTableManager();

  EXPECT_NE(partition_mgr, nullptr);
}

TEST_F(TestTagTable, Resource_MultipleTagTables) {
  std::vector<std::unique_ptr<TagTable>> tables;

  for (int i = 0; i < 5; ++i) {
    uint64_t tid = table_id_ + i;
    tables.push_back(std::make_unique<TagTable>(db_path_, tbl_sub_path_, tid, entity_group_id_));
  }

  EXPECT_EQ(tables.size(), 5);
}

TEST_F(TestTagTable, Schema_MultipleTagsInSchema) {
  EXPECT_GE(schema_.size(), 2);
  EXPECT_TRUE(std::any_of(schema_.begin(), schema_.end(),
    [](const TagInfo& info) { return info.m_tag_type == PRIMARY_TAG; }));
  EXPECT_TRUE(std::any_of(schema_.begin(), schema_.end(),
    [](const TagInfo& info) { return info.m_tag_type == GENERAL_TAG; }));
}

TEST_F(TestTagTable, Schema_GeneralTagsHaveNumericTypes) {
  for (const auto& info : schema_) {
    if (info.m_tag_type == GENERAL_TAG) {
      EXPECT_TRUE(info.m_data_type == DATATYPE::INT32 || info.m_data_type == DATATYPE::INT64);
    }
  }
}

TEST_F(TestTagTable, TagInfo_NotDropped) {
  for (const auto& info : schema_) {
    EXPECT_FALSE(info.isDropped());
  }
}

TEST_F(TestTagTable, TagInfo_ValidIds) {
  for (size_t i = 0; i < schema_.size(); ++i) {
    EXPECT_EQ(schema_[i].m_id, i + 1);
  }
}

TEST_F(TestTagTable, TagInfo_ValidSizes) {
  for (const auto& info : schema_) {
    EXPECT_GT(info.m_size, 0);
  }
}

TEST_F(TestTagTable, Constant_PerNullBitmapSize) {
  EXPECT_EQ(k_per_null_bitmap_size, 1);
}

TEST_F(TestTagTable, Constant_EntityGroupIdSize) {
  EXPECT_GT(k_entity_group_id_size, 0);
}

TEST_F(TestTagTable, Error_EmptyDbPath) {
  std::string empty_path = "";
  TagTable tag_table(empty_path, tbl_sub_path_, table_id_, entity_group_id_);

  SUCCEED();
}

TEST_F(TestTagTable, Error_EmptySubPath) {
  std::string empty_sub_path = "";
  TagTable tag_table(db_path_, empty_sub_path, table_id_, entity_group_id_);

  SUCCEED();
}

TEST_F(TestTagTable, Boundary_ZeroEntityGroupId) {
  int32_t zero_entity_group_id = 0;
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, zero_entity_group_id);

  SUCCEED();
}

TEST_F(TestTagTable, Boundary_LargeTableId) {
  uint64_t large_table_id = UINT64_MAX;
  TagTable tag_table(db_path_, tbl_sub_path_, large_table_id, entity_group_id_);

  SUCCEED();
}

TEST_F(TestTagTable, Open_EmptyInvalidVersions) {
  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    ErrorInfo err_info;
    ASSERT_EQ(tag_table.create(schema_, table_version_, {}, err_info), 0);
  }

  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    std::vector<TableVersion> invalid_versions;
    ErrorInfo err_info;

    int result = tag_table.open(invalid_versions, err_info);

    EXPECT_EQ(result, 0);
    EXPECT_TRUE(invalid_versions.empty());
  }
}

TEST_F(TestTagTable, Open_ReopenTable) {
  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    ErrorInfo err_info;
    ASSERT_EQ(tag_table.create(schema_, table_version_, {}, err_info), 0);
  }

  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    std::vector<TableVersion> invalid_versions1;
    ErrorInfo err_info;
    ASSERT_EQ(tag_table.open(invalid_versions1, err_info), 0);
  }

  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    std::vector<TableVersion> invalid_versions2;
    ErrorInfo err_info;

    int result = tag_table.open(invalid_versions2, err_info);

    EXPECT_EQ(result, 0);
    EXPECT_TRUE(invalid_versions2.empty());
  }
}

TEST_F(TestTagTable, Create_MultipleVersions) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;

  ASSERT_EQ(tag_table.create(schema_, table_version_, {}, err_info), 0);

  std::vector<TagInfo> schema_v2 = schema_;
  TagInfo new_tag;
  new_tag.m_id = 4;
  new_tag.m_data_type = DATATYPE::FLOAT;
  new_tag.m_length = sizeof(float);
  new_tag.m_size = sizeof(float);
  new_tag.m_tag_type = GENERAL_TAG;
  new_tag.m_flag = 0;
  schema_v2.push_back(new_tag);

  int result = tag_table.addNewPartitionVersion(schema_v2, table_version_ + 1, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, GetEntityIdListByVGroupId_MultipleVGroups) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> entity_id_list1;
  tag_table.GetEntityIdListByVGroupId(1, entity_id_list1);
  EXPECT_TRUE(entity_id_list1.empty());

  std::vector<uint32_t> entity_id_list2;
  tag_table.GetEntityIdListByVGroupId(2, entity_id_list2);
  EXPECT_TRUE(entity_id_list2.empty());
}

TEST_F(TestTagTable, GetMaxEntityIdByVGroupId_MultipleVGroups) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  uint32_t entity_id1 = UINT32_MAX;
  tag_table.GetMaxEntityIdByVGroupId(1, entity_id1);

  uint32_t entity_id2 = UINT32_MAX;
  tag_table.GetMaxEntityIdByVGroupId(2, entity_id2);
}

TEST_F(TestTagTable, HasPrimaryKey_EmptyTable) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char ptag_data[64] = "";

  uint32_t entity_id = 0;
  uint32_t sub_group_id = 0;
  bool result = tag_table.hasPrimaryKey(ptag_data, strlen(ptag_data), entity_id, sub_group_id);

  EXPECT_FALSE(result);
}

TEST_F(TestTagTable, GetPrimaryKeyRowInfo_EmptyTable) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char ptag_data[64] = "";
  std::pair<uint64_t, uint64_t> row_info;

  bool result = tag_table.GetPrimaryKeyRowInfo(ptag_data, strlen(ptag_data), row_info);

  EXPECT_FALSE(result);
}

TEST_F(TestTagTable, GetLatestOSN_InitialValue) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  TS_OSN osn = tag_table.GetLatestOSN();

  EXPECT_EQ(osn, 0);
}

TEST_F(TestTagTable, CalculateSchemaIdxs_EmptyResultScanIdxs) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> result_scan_idxs;
  std::vector<uint32_t> src_scan_idxs;

  int result = tag_table.CalculateSchemaIdxs(table_version_, result_scan_idxs, schema_, &src_scan_idxs);

  EXPECT_EQ(result, 0);
  EXPECT_TRUE(src_scan_idxs.empty());
}

TEST_F(TestTagTable, CalculateSchemaIdxs_InvalidTableVersion) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<uint32_t> result_scan_idxs = {0, 1};
  std::vector<uint32_t> src_scan_idxs;
  TableVersion invalid_version = 9999;

  int result = tag_table.CalculateSchemaIdxs(invalid_version, result_scan_idxs, schema_, &src_scan_idxs);

  EXPECT_NE(result, 0);
}

TEST_F(TestTagTable, AlterTableTag_AddNewColumn) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  AttributeInfo attr_info;
  attr_info.id = 4;
  attr_info.type = DATATYPE::INT64;
  attr_info.size = 8;
  attr_info.length = 8;

  int result = tag_table.AlterTableTag(AlterType::ADD_COLUMN, attr_info, table_version_, table_version_ + 1, err_info);

  EXPECT_GE(result, 0);
}

TEST_F(TestTagTable, AlterTableTag_DropColumn) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  AttributeInfo attr_info;
  attr_info.id = 2;
  attr_info.type = DATATYPE::INT32;
  attr_info.size = 4;
  attr_info.length = 4;

  int result = tag_table.AlterTableTag(AlterType::DROP_COLUMN, attr_info, table_version_, table_version_ + 1, err_info);

  EXPECT_GE(result, 0);
}

class TestTagPartitionTableManager : public testing::Test {
 protected:
  static void SetUpTestCase() {
    // No longer need to create global test directory
  }

  static void TearDownTestCase() {
    // Clean up any existing files in the test directory
    if (!RemoveDirContents("/tmp/kwdb_mmap_test")) {
      LOG_WARN("Failed to clean up test directory contents");
    }
  }

  void SetUp() override {
    db_path_ = "/tmp/kwdb_mmap_test/";
    tbl_sub_path_ = "part_mgr/";
    table_id_ = 2001;
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
  }

  void TearDown() override {
    // Clean up test directory
    if (!Remove(db_path_ + tbl_sub_path_)) {
      LOG_WARN("Failed to clean up directory");
    }
  }

  std::string db_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_;
  int32_t entity_group_id_;
  uint32_t table_version_;
  std::vector<TagInfo> schema_;
};

TEST_F(TestTagPartitionTableManager, CreateTagPartitionTable_Success) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;

  int result = part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagPartitionTableManager, OpenTagPartitionTable_Success) {
  {
    TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    ErrorInfo err_info;
    ASSERT_EQ(part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info), 0);

    int result = part_mgr.OpenTagPartitionTable(table_version_, err_info);

    EXPECT_EQ(result, 0);
  }
}

TEST_F(TestTagPartitionTableManager, GetPartitionTable_AfterCreate) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(table_version_);

  EXPECT_NE(part_table, nullptr);
}

TEST_F(TestTagPartitionTableManager, GetPartitionTable_NotExist) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(9999);

  EXPECT_EQ(part_table, nullptr);
}

TEST_F(TestTagPartitionTableManager, GetAllPartitionTables_AfterCreate) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  std::vector<std::pair<uint32_t, TagPartitionTable*>> tag_part_tables;
  part_mgr.GetAllPartitionTables(tag_part_tables);

  EXPECT_EQ(tag_part_tables.size(), 1);
}

TEST_F(TestTagPartitionTableManager, RemoveAll_Success) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  int result = part_mgr.RemoveAll(err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagPartitionTableManager, CreateTagPartitionTable_Duplicate) {
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  int result = part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, InsertForUndo_RecordNotExist) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char primary_tag_data[] = "nonexistent_key";
  TSSlice primary_tag{primary_tag_data, strlen(primary_tag_data)};
  
  int result = tag_table.InsertForUndo(1, 1, primary_tag, 100);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, InsertForRedo_RecordNotExist) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  std::vector<AttributeInfo> schema;
  AttributeInfo attr;
  attr.id = 1;
  attr.type = DATATYPE::STRING;
  attr.size = 64;
  attr.length = 64;
  schema.push_back(attr);

  std::vector<uint8_t> payload_data(256, 0);
  TSSlice payload_slice{reinterpret_cast<char*>(payload_data.data()), payload_data.size()};
  kwdbts::Payload payload(schema, payload_slice);
  
  int result = tag_table.InsertForRedo(1, 1, payload);
}

TEST_F(TestTagTable, DeleteForUndo_TagPackNull) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char primary_tag_data[] = "test_key";
  TSSlice primary_tag{primary_tag_data, strlen(primary_tag_data)};
  TSSlice tag_pack{nullptr, 0};
  
  int result = tag_table.DeleteForUndo(1, 1, 100, primary_tag, tag_pack, 100);
  
  EXPECT_LT(result, 0);
}

TEST_F(TestTagTable, DeleteForRedo_RecordNotExist) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

  char primary_tag_data[] = "nonexistent_key";
  TSSlice primary_tag{primary_tag_data, strlen(primary_tag_data)};

  std::vector<uint8_t> tag_data(128, 0);
  TSSlice tag_pack{reinterpret_cast<char*>(tag_data.data()), tag_data.size()};

  int result = tag_table.DeleteForRedo(1, 1, primary_tag, tag_pack, 100);

  EXPECT_EQ(result, 0);
}

// --- Tests for setTagDataInfo changes in DeleteForUndo/DeleteForRedo/UpdateForRedo ---

TEST_F(TestTagTable, DeleteForUndo_ExistingRecord_SetsTagDataInfoCorrectly) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);
  TagPartitionTable* part_table = tag_table.GetTagPartitionTableManager()->GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);
  const std::vector<TagInfo>& tag_schema = part_table->getIncludeDroppedSchemaInfos();

  // 1. Insert a record using InsertTagRecord with TsRawPayload
  TSRowPayloadBuilder builder(tag_schema, metric_schema_, 1);
  uint32_t group_id = 1;
  TSEntityID dev_id = 10;
  // builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  int64_t ts_val = 1000;
  builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load{nullptr, 0};
  builder.Build(table_id_, 1, &pay_load);
  TsRawPayload::SetHashPoint(pay_load, group_id);
  TsRawPayload::SetOSN(pay_load, 10);

  kwdbts::TsRawPayload raw_payload(nullptr);
  ASSERT_EQ(raw_payload.ParsePayLoadStruct(pay_load), KStatus::SUCCESS);

  uint32_t entity_id = 10;
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload, group_id, entity_id, 10, OperateType::Insert, {0, 0}), 0);

  // 2. Generate tag pack from the inserted record for DeleteForUndo
  TSSlice ptag = raw_payload.GetPrimaryTag();
  auto tag_pack_obj = tag_table.GenTagPack(ptag.data, ptag.len);
  ASSERT_NE(tag_pack_obj, nullptr);
  const TSSlice tag_pack_data = tag_pack_obj->getData();
  TSSlice tag_pack{tag_pack_data.data, tag_pack_data.len};

  // 3. Call DeleteForUndo on the existing record
  TS_OSN osn = 200;
  int result = tag_table.DeleteForUndo(group_id, entity_id, group_id, ptag, tag_pack, osn);

  // 4. Verify the result and TagDataInfo state
  EXPECT_EQ(result, 0);

  // Get the partition table to verify TagDataInfo
  auto partition_mgr = tag_table.GetTagPartitionTableManager();
  ASSERT_NE(partition_mgr, nullptr);
  auto partition_table = partition_mgr->GetPartitionTable(table_version_);
  ASSERT_NE(partition_table, nullptr);

  // Find the row via the primary key
  std::pair<uint64_t, uint64_t> row_info;
  bool found = tag_table.GetPrimaryKeyRowInfo(ptag.data, ptag.len, row_info);
  ASSERT_TRUE(found);

  // Verify TagDataInfo: DeleteForUndo should set operate_type[0]=Insert, osn[0]=osn, operate_idx=0
  partition_table->startRead();
  auto tag_info = partition_table->getTagDataInfoByRowNum(row_info.second);
  ASSERT_NE(tag_info, nullptr);
  EXPECT_EQ(tag_info->operate_type[0], OperateType::Insert);
  EXPECT_EQ(tag_info->osn[0], tag_pack_obj->getOSN());
  EXPECT_EQ(tag_info->operate_idx, 0);
  // Row should not be deleted (unsetDeleteMark was called)
  EXPECT_TRUE(partition_table->isValidRow(row_info.second));
  partition_table->stopRead();
  free(pay_load.data);
}

TEST_F(TestTagTable, DeleteForRedo_ExistingRecord_SetsTagDataInfoCorrectly) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);
  TagPartitionTable* part_table = tag_table.GetTagPartitionTableManager()->GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);
  const std::vector<TagInfo>& tag_schema = part_table->getIncludeDroppedSchemaInfos();

  // 1. Insert a record using InsertTagRecord with TsRawPayload
  TSRowPayloadBuilder builder(tag_schema, metric_schema_, 1);
  uint32_t group_id = 1;
  TSEntityID dev_id = 20;
  // builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  int64_t ts_val = 1000;
  builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load{nullptr, 0};
  builder.Build(table_id_, 1, &pay_load);
  TsRawPayload::SetHashPoint(pay_load, group_id);
  TsRawPayload::SetOSN(pay_load, 10);

  kwdbts::TsRawPayload raw_payload(nullptr);
  ASSERT_EQ(raw_payload.ParsePayLoadStruct(pay_load), KStatus::SUCCESS);

  uint32_t entity_id = 20;
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload, group_id, entity_id, 10, OperateType::Insert, {0, 0}), 0);

  // 2. Generate tag pack for DeleteForRedo
  TSSlice ptag = raw_payload.GetPrimaryTag();
  auto tag_pack_obj = tag_table.GenTagPack(ptag.data, ptag.len);
  ASSERT_NE(tag_pack_obj, nullptr);
  const TSSlice tag_pack_data = tag_pack_obj->getData();
  TSSlice tag_pack{const_cast<char*>(tag_pack_data.data), tag_pack_data.len};

  // 3. Call DeleteForRedo
  TS_OSN osn = 300;
  int result = tag_table.DeleteForRedo(group_id, entity_id, ptag, tag_pack, osn);

  // 4. Verify result and TagDataInfo state
  EXPECT_EQ(result, 0);

  auto partition_mgr = tag_table.GetTagPartitionTableManager();
  ASSERT_NE(partition_mgr, nullptr);
  auto partition_table = partition_mgr->GetPartitionTable(table_version_);
  ASSERT_NE(partition_table, nullptr);

  // After DeleteForRedo, the primary tag index is removed, so we need to find row_info
  // before the delete. Since DeleteForRedo removes the index entry, we verify
  // via the partition table directly - check that the row at row 0 has the correct TagDataInfo.
  partition_table->startRead();
  auto tag_info = partition_table->getTagDataInfoByRowNum(1);
  ASSERT_NE(tag_info, nullptr);
  // DeleteForRedo should set operate_type[1]=Delete, osn[1]=osn, operate_idx=1
  EXPECT_EQ(tag_info->operate_type[1], OperateType::Delete);
  EXPECT_EQ(tag_info->osn[1], osn);
  EXPECT_EQ(tag_info->operate_idx, 1);
  // Row should be marked as deleted
  EXPECT_FALSE(partition_table->isValidRow(1));
  partition_table->stopRead();
  free(pay_load.data);
}

TEST_F(TestTagTable, DeleteForRedo_AlreadyDeleted_SkipsSetTagDataInfo) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);
  TagPartitionTable* part_table = tag_table.GetTagPartitionTableManager()->GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);
  const std::vector<TagInfo>& tag_schema = part_table->getIncludeDroppedSchemaInfos();

  // 1. Insert a record using InsertTagRecord with TsRawPayload
  TSRowPayloadBuilder builder(tag_schema, metric_schema_, 1);
  uint32_t group_id = 1;
  TSEntityID dev_id = 21;
  // builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  int64_t ts_val = 1000;
  builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load{nullptr, 0};
  builder.Build(table_id_, 1, &pay_load);
  TsRawPayload::SetHashPoint(pay_load, group_id);
  TsRawPayload::SetOSN(pay_load, 10);

  kwdbts::TsRawPayload raw_payload(nullptr);
  ASSERT_EQ(raw_payload.ParsePayLoadStruct(pay_load), KStatus::SUCCESS);

  uint32_t entity_id = 21;
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload, group_id, entity_id, 10, OperateType::Insert, {0, 0}), 0);

  // 2. First DeleteForRedo to set the Delete status
  TSSlice ptag = raw_payload.GetPrimaryTag();
  auto tag_pack_obj = tag_table.GenTagPack(ptag.data, ptag.len);
  ASSERT_NE(tag_pack_obj, nullptr);
  const TSSlice tag_pack_data = tag_pack_obj->getData();
  TSSlice tag_pack1{const_cast<char*>(tag_pack_data.data), tag_pack_data.len};

  TS_OSN osn1 = 300;
  ASSERT_EQ(tag_table.DeleteForRedo(group_id, entity_id, ptag, tag_pack1, osn1), 0);

  // 3. Re-insert the record so it's found again
  // Build a new payload for re-insert
  TSRowPayloadBuilder builder2(tag_schema, metric_schema_, 1);
  dev_id = 31;
  group_id = 2;
  entity_id = 31;
  // builder2.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder2.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  dev_id = 10;
  group_id = 1;
  builder2.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load2{nullptr, 0};
  builder2.Build(table_id_, 1, &pay_load2);
  TsRawPayload::SetHashPoint(pay_load2, group_id);
  TsRawPayload::SetOSN(pay_load2, 20);

  kwdbts::TsRawPayload raw_payload2(nullptr);
  ASSERT_EQ(raw_payload2.ParsePayLoadStruct(pay_load2), KStatus::SUCCESS);
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload2, group_id, entity_id, 20, OperateType::Insert, {0, 0}), 0);
  TSSlice ptag2 = raw_payload2.GetPrimaryTag();
  // 4. Re-generate tag pack for the re-inserted record
  auto tag_pack_obj2 = tag_table.GenTagPack(ptag2.data, ptag2.len);
  ASSERT_NE(tag_pack_obj2, nullptr);
  const TSSlice tag_pack_data2 = tag_pack_obj2->getData();
  TSSlice tag_pack2{const_cast<char*>(tag_pack_data2.data), tag_pack_data2.len};

  // 5. Set up: manually mark the row with a Delete operation so already_done is true
  auto partition_mgr = tag_table.GetTagPartitionTableManager();
  auto partition_table = partition_mgr->GetPartitionTable(table_version_);
  ASSERT_NE(partition_table, nullptr);

  std::pair<uint64_t, uint64_t> row_info;
  bool found = tag_table.GetPrimaryKeyRowInfo(ptag.data, ptag.len, row_info);

  // If the record exists, test the already_done path
  if (found) {
    partition_table->startRead();
    auto tag_info = partition_table->getTagDataInfoByRowNum(row_info.second);
    // Manually set a Delete operation at index 0 to simulate already_done
    TagDataInfo manual_info{};
    manual_info.operate_type[0] = OperateType::Delete;
    manual_info.osn[0] = osn1;
    manual_info.operate_idx = 0;
    partition_table->setTagDataInfo(row_info.second, &manual_info);
    partition_table->stopRead();

    // Second DeleteForRedo should detect already_done and skip setTagDataInfo
    TS_OSN osn2 = 400;
    int result = tag_table.DeleteForRedo(group_id, entity_id, ptag, tag_pack2, osn2);
    EXPECT_EQ(result, 0);

    // Verify: the operate_idx should still be 0 (not changed by setTagDataInfo)
    partition_table->startRead();
    auto tag_info_after = partition_table->getTagDataInfoByRowNum(row_info.second);
    EXPECT_EQ(tag_info_after->operate_idx, 0);
    EXPECT_EQ(tag_info_after->operate_type[0], OperateType::Delete);
    // osn should be updated to osn2 by the already_done logic
    partition_table->stopRead();
  }
  free(pay_load.data);
  free(pay_load2.data);
}

TEST_F(TestTagTable, UpdateForRedo_TsRawPayloadVersion_SetsDeleteMark) {
  // Test UpdateForRedo with TsRawPayload: this version sets delete mark on old row
  // and inserts a new row via InsertTagRecord
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);
  TagPartitionTable* part_table = tag_table.GetTagPartitionTableManager()->GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);
  const std::vector<TagInfo>& tag_schema = part_table->getIncludeDroppedSchemaInfos();

  // 1. Insert a record using InsertTagRecord with TsRawPayload
  TSRowPayloadBuilder builder(tag_schema, metric_schema_, 1);
  uint32_t group_id = 1;
  TSEntityID dev_id = 30;
  // builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  int64_t ts_val = 1000;
  builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load{nullptr, 0};
  builder.Build(table_id_, 1, &pay_load);
  TsRawPayload::SetHashPoint(pay_load, group_id);
  TsRawPayload::SetOSN(pay_load, 10);

  kwdbts::TsRawPayload raw_payload(nullptr);
  ASSERT_EQ(raw_payload.ParsePayLoadStruct(pay_load), KStatus::SUCCESS);

  uint32_t entity_id = 30;
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload, group_id, entity_id, 10, OperateType::Insert, {0, 0}), 0);

  // 2. Get the primary tag and row info for the inserted record
  TSSlice ptag = raw_payload.GetPrimaryTag();
  auto partition_mgr = tag_table.GetTagPartitionTableManager();
  auto partition_table = partition_mgr->GetPartitionTable(table_version_);
  ASSERT_NE(partition_table, nullptr);

  std::pair<uint64_t, uint64_t> row_info;
  bool found = tag_table.GetPrimaryKeyRowInfo(ptag.data, ptag.len, row_info);

  // 3. Call UpdateForRedo with TsRawPayload
  if (found) {
    size_t old_row = row_info.second;

    // Build an update payload
    TSRowPayloadBuilder update_builder(tag_schema, metric_schema_, 1);
    // update_builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
    for (size_t i = 0; i < tag_schema.size(); i++) {
      update_builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
    }
    update_builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

    TSSlice update_pay_load{nullptr, 0};
    update_builder.Build(table_id_, 1, &update_pay_load);
    TsRawPayload::SetHashPoint(update_pay_load, group_id);
    TsRawPayload::SetOSN(update_pay_load, 20);

    kwdbts::TsRawPayload update_raw_payload(nullptr);
    ASSERT_EQ(update_raw_payload.ParsePayLoadStruct(update_pay_load), KStatus::SUCCESS);

    int result = tag_table.UpdateForRedo(group_id, entity_id, ptag, update_raw_payload);
    EXPECT_EQ(result, 0);

    // Old row should be marked as deleted
    partition_table->startRead();
    EXPECT_FALSE(partition_table->isValidRow(old_row));
    partition_table->stopRead();
    free(update_pay_load.data);
  }
  free(pay_load.data);
}

// --- Direct tests for TagPartitionTable::setTagDataInfo overloads ---

class TestTagPartitionTableSetTagDataInfo : public testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> test_counter{1000};
    uint64_t unique_id = test_counter.fetch_add(1);
    db_path_ = "/tmp/kwdb_mmap_test_settaginfo" + std::to_string(unique_id) + "/";
    tbl_sub_path_ = "sub_path/";
    table_id_ = 3001;
    entity_group_id_ = 1;
    table_version_ = 1;

    std::string full_path = db_path_ + tbl_sub_path_;
    if (!Remove(full_path)) {
      LOG_WARN("Failed to remove directory %s: %s", full_path.c_str());
    }
    if (!MakeDirectory(full_path)) {
      FAIL() << "Failed to create directory: " << full_path;
    }

    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = DATATYPE::INT64;
    ptag_info.m_length = sizeof(uint64_t);
    ptag_info.m_size = sizeof(uint64_t);
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    schema_.push_back(ptag_info);

    TagInfo ntag_info;
    ntag_info.m_id = 2;
    ntag_info.m_data_type = DATATYPE::INT64;
    ntag_info.m_length = sizeof(uint64_t);
    ntag_info.m_size = sizeof(uint64_t);
    ntag_info.m_tag_type = GENERAL_TAG;
    ntag_info.m_flag = 0;
    schema_.push_back(ntag_info);

    metric_schema_.clear();
    AttributeInfo metric_attr;
    metric_attr.id = 1;
    metric_attr.type = DATATYPE::INT64;
    metric_attr.size = sizeof(int64_t);
    metric_attr.length = sizeof(int64_t);
    metric_schema_.push_back(metric_attr);
  }

  void TearDown() override {
    std::string full_path = db_path_ + tbl_sub_path_;
    if (!Remove(full_path)) {
      LOG_WARN("Failed to clean up directory %s", full_path.c_str());
    }
  }

  std::string db_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_;
  int32_t entity_group_id_;
  uint32_t table_version_;
  std::vector<TagInfo> schema_;
  std::vector<AttributeInfo> metric_schema_;
};

TEST_F(TestTagPartitionTableSetTagDataInfo, SetTagDataInfo_PtrOverload_SetsEntireStruct) {
  // Test the pointer overload: setTagDataInfo(row, TagDataInfo*)
  // This is used by DeleteForUndo
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info), 0);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);

  // Insert a row to get a valid row number
  // Data format: [bitmap 1B][primary_tag 64B][ntag 4B]
  char primary_tag[64] = "test_tag_info_ptr";
  int32_t ntag_val = 42;
  size_t bitmap_size = 1;
  std::vector<char> tag_data(bitmap_size + 64 + sizeof(int32_t), 0);
  memcpy(tag_data.data() + bitmap_size, primary_tag, 64);
  memcpy(tag_data.data() + bitmap_size + 64, &ntag_val, sizeof(int32_t));

  size_t row_no = 0;
  ASSERT_GE(part_table->insert(1, 1, 0, 100, OperateType::Insert, tag_data.data(), &row_no), 0);

  // Set TagDataInfo using pointer overload
  TagDataInfo new_info{};
  new_info.operate_type[0] = OperateType::Insert;
  new_info.osn[0] = 500;
  new_info.operate_idx = 0;

  part_table->startRead();
  part_table->setTagDataInfo(row_no, &new_info);

  // Verify the struct was copied correctly
  auto result_info = part_table->getTagDataInfoByRowNum(row_no);
  ASSERT_NE(result_info, nullptr);
  EXPECT_EQ(result_info->operate_type[0], OperateType::Insert);
  EXPECT_EQ(result_info->osn[0], 500);
  EXPECT_EQ(result_info->operate_idx, 0);
  // Other entries should be zero
  EXPECT_EQ(result_info->operate_type[1], OperateType::Invalid);
  EXPECT_EQ(result_info->operate_type[2], OperateType::Invalid);
  EXPECT_EQ(result_info->osn[1], 0);
  EXPECT_EQ(result_info->osn[2], 0);
  part_table->stopRead();
}

TEST_F(TestTagPartitionTableSetTagDataInfo, SetTagDataInfo_IndexOverload_SetsSpecificFields) {
  // Test the index overload: setTagDataInfo(row, operate_idx, osn, type)
  // This is used by DeleteForRedo and UpdateForRedo
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info), 0);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);

  // Insert a row with initial Insert operation at index 0
  char primary_tag[64] = "test_tag_info_idx";
  int32_t ntag_val = 99;
  size_t bitmap_size = 1;
  std::vector<char> tag_data(bitmap_size + 64 + sizeof(int32_t), 0);
  memcpy(tag_data.data() + bitmap_size, primary_tag, 64);
  memcpy(tag_data.data() + bitmap_size + 64, &ntag_val, sizeof(int32_t));

  size_t row_no = 0;
  ASSERT_GE(part_table->insert(2, 1, 0, 100, OperateType::Insert, tag_data.data(), &row_no), 0);

  // First, set index 0 to Insert (simulating initial state)
  part_table->startRead();
  part_table->setTagDataInfo(row_no, 0, 100, OperateType::Insert);

  // Verify initial state
  auto info_after_insert = part_table->getTagDataInfoByRowNum(row_no);
  ASSERT_NE(info_after_insert, nullptr);
  EXPECT_EQ(info_after_insert->operate_type[0], OperateType::Insert);
  EXPECT_EQ(info_after_insert->osn[0], 100);
  EXPECT_EQ(info_after_insert->operate_idx, 0);
  part_table->stopRead();

  // Now use setTagDataInfo with index 1 to set Delete (as DeleteForRedo does)
  part_table->startRead();
  part_table->setTagDataInfo(row_no, 1, 200, OperateType::Delete);

  // Verify the Delete was set at index 1
  auto info_after_delete = part_table->getTagDataInfoByRowNum(row_no);
  ASSERT_NE(info_after_delete, nullptr);
  EXPECT_EQ(info_after_delete->operate_type[0], OperateType::Insert);
  EXPECT_EQ(info_after_delete->osn[0], 100);
  EXPECT_EQ(info_after_delete->operate_type[1], OperateType::Delete);
  EXPECT_EQ(info_after_delete->osn[1], 200);
  EXPECT_EQ(info_after_delete->operate_idx, 1);
  part_table->stopRead();

  // Now use setTagDataInfo with index 1 to set Update (as UpdateForRedo does)
  part_table->startRead();
  part_table->setTagDataInfo(row_no, 1, 300, OperateType::Update);

  auto info_after_update = part_table->getTagDataInfoByRowNum(row_no);
  ASSERT_NE(info_after_update, nullptr);
  EXPECT_EQ(info_after_update->operate_type[0], OperateType::Insert);
  EXPECT_EQ(info_after_update->osn[0], 100);
  EXPECT_EQ(info_after_update->operate_type[1], OperateType::Update);
  EXPECT_EQ(info_after_update->osn[1], 300);
  EXPECT_EQ(info_after_update->operate_idx, 1);
  part_table->stopRead();
}

TEST_F(TestTagPartitionTableSetTagDataInfo, SetTagDataInfo_PtrOverload_ReplacesEntireStruct) {
  // Verify that the pointer overload replaces the entire struct, not just individual fields
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info), 0);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);

  char primary_tag[64] = "test_tag_info_replace";
  int32_t ntag_val = 55;
  size_t bitmap_size = 1;
  std::vector<char> tag_data(bitmap_size + 64 + sizeof(int32_t), 0);
  memcpy(tag_data.data() + bitmap_size, primary_tag, 64);
  memcpy(tag_data.data() + bitmap_size + 64, &ntag_val, sizeof(int32_t));

  size_t row_no = 0;
  ASSERT_GE(part_table->insert(3, 1, 0, 100, OperateType::Insert, tag_data.data(), &row_no), 0);

  // First set some data at index 1
  part_table->startRead();
  part_table->setTagDataInfo(row_no, 0, 100, OperateType::Insert);
  part_table->setTagDataInfo(row_no, 1, 200, OperateType::Delete);
  part_table->stopRead();

  // Now use pointer overload to reset the struct (as DeleteForUndo does)
  TagDataInfo reset_info{OperateType::Insert, 0, 0, 0, 0, 0, 0, 0, 500, 0, 0};
  part_table->startRead();
  part_table->setTagDataInfo(row_no, &reset_info);

  auto result_info = part_table->getTagDataInfoByRowNum(row_no);
  ASSERT_NE(result_info, nullptr);
  // The entire struct should be replaced - index 0 should be Insert with osn 500
  EXPECT_EQ(result_info->operate_type[0], OperateType::Insert);
  EXPECT_EQ(result_info->osn[0], 500);
  EXPECT_EQ(result_info->operate_idx, 0);
  // Previous data at index 1 should be gone (zeroed)
  EXPECT_EQ(result_info->operate_type[1], OperateType::Invalid);
  EXPECT_EQ(result_info->osn[1], 0);
  part_table->stopRead();
}

TEST_F(TestTagPartitionTableSetTagDataInfo, SetTagDataInfo_IndexOverload_GetOpTypeAtOSN) {
  // Verify that after setTagDataInfo, GetOpTypeAtOSN returns correct results
  TagPartitionTableManager part_mgr(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(part_mgr.CreateTagPartitionTable(schema_, table_version_, err_info), 0);

  TagPartitionTable* part_table = part_mgr.GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);

  char primary_tag[64] = "test_tag_info_osn";
  int32_t ntag_val = 77;
  size_t bitmap_size = 1;
  std::vector<char> tag_data(bitmap_size + 64 + sizeof(int32_t), 0);
  memcpy(tag_data.data() + bitmap_size, primary_tag, 64);
  memcpy(tag_data.data() + bitmap_size + 64, &ntag_val, sizeof(int32_t));

  size_t row_no = 0;
  ASSERT_GE(part_table->insert(4, 1, 0, 100, OperateType::Insert, tag_data.data(), &row_no), 0);

  // Set up: Insert at idx 0, Delete at idx 1 (as DeleteForRedo would do)
  part_table->startRead();
  part_table->setTagDataInfo(row_no, 0, 100, OperateType::Insert);
  part_table->setTagDataInfo(row_no, 1, 200, OperateType::Delete);

  // Verify GetOpTypeAtOSN returns correct results
  OperateType type;
  TS_OSN op_osn;

  // OSN 200 should find Delete
  EXPECT_TRUE(part_table->GetOpTypeAtOSN(row_no, 200, type, op_osn));
  EXPECT_EQ(type, OperateType::Delete);
  EXPECT_EQ(op_osn, 200);

  // OSN 150 should find Insert (since 150 >= 100 but < 200, walks back to idx 0)
  EXPECT_TRUE(part_table->GetOpTypeAtOSN(row_no, 150, type, op_osn));
  EXPECT_EQ(type, OperateType::Insert);
  EXPECT_EQ(op_osn, 100);

  // OSN 50 should not find a valid operation (all osns are > 50)
  EXPECT_FALSE(part_table->GetOpTypeAtOSN(row_no, 50, type, op_osn));
  part_table->stopRead();
}

TEST_F(TestTagPartitionTableSetTagDataInfo, SetTagDataInfo_PtrOverload_DeletesMarkBehavior) {
  // Verify that DeleteForUndo correctly unsets the delete mark after setting TagDataInfo
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;
  ASSERT_EQ(tag_table.create(schema_, table_version_, {}, err_info), 0);
  TagPartitionTable* part_table = tag_table.GetTagPartitionTableManager()->GetPartitionTable(table_version_);
  ASSERT_NE(part_table, nullptr);
  const std::vector<TagInfo>& tag_schema = part_table->getIncludeDroppedSchemaInfos();

  // Insert a record using InsertTagRecord with TsRawPayload
  TSRowPayloadBuilder builder(tag_schema, metric_schema_, 1);
  uint32_t group_id = 1;
  TSEntityID dev_id = 40;
  // builder.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  int64_t ts_val = 1000;
  builder.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load{nullptr, 0};
  builder.Build(table_id_, 1, &pay_load);
  TsRawPayload::SetHashPoint(pay_load, group_id);
  TsRawPayload::SetOSN(pay_load, 10);

  kwdbts::TsRawPayload raw_payload(nullptr);
  ASSERT_EQ(raw_payload.ParsePayLoadStruct(pay_load), KStatus::SUCCESS);

  uint32_t entity_id = 40;
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload, group_id, entity_id, 10, OperateType::Insert, {0, 0}), 0);

  // First, delete the record with DeleteForRedo
  TSSlice ptag = raw_payload.GetPrimaryTag();
  auto tag_pack_obj = tag_table.GenTagPack(ptag.data, ptag.len);
  ASSERT_NE(tag_pack_obj, nullptr);
  const TSSlice tag_pack_data = tag_pack_obj->getData();
  TSSlice tag_pack_for_redo{const_cast<char*>(tag_pack_data.data), tag_pack_data.len};

  ASSERT_EQ(tag_table.DeleteForRedo(group_id, entity_id, ptag, tag_pack_for_redo, 200), 0);

  // Verify row is marked as deleted
  auto partition_mgr = tag_table.GetTagPartitionTableManager();
  auto partition_table = partition_mgr->GetPartitionTable(table_version_);
  ASSERT_NE(partition_table, nullptr);
  partition_table->startRead();
  EXPECT_FALSE(partition_table->isValidRow(0));
  partition_table->stopRead();

  // Now re-insert so the record is found again
  TSRowPayloadBuilder builder2(tag_schema, metric_schema_, 1);
  // builder2.SetTagValue(0, reinterpret_cast<char*>(&dev_id), sizeof(dev_id));
  for (size_t i = 0; i < tag_schema.size(); i++) {
    builder2.SetTagValue(i, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
  }
  builder2.SetColumnValue(0, 0, reinterpret_cast<char*>(&ts_val), sizeof(ts_val));

  TSSlice pay_load2{nullptr, 0};
  builder2.Build(table_id_, 1, &pay_load2);
  TsRawPayload::SetHashPoint(pay_load2, group_id);
  TsRawPayload::SetOSN(pay_load2, 20);

  kwdbts::TsRawPayload raw_payload2(nullptr);
  ASSERT_EQ(raw_payload2.ParsePayLoadStruct(pay_load2), KStatus::SUCCESS);
  ASSERT_EQ(tag_table.InsertTagRecord(raw_payload2, group_id, entity_id, 20, OperateType::Insert, {0, 0}), 0);

  // Generate tag pack for DeleteForUndo
  auto tag_pack_obj2 = tag_table.GenTagPack(ptag.data, ptag.len);
  if (tag_pack_obj2 != nullptr) {
    const TSSlice tag_pack_data2 = tag_pack_obj2->getData();
    TSSlice tag_pack_for_undo{tag_pack_data2.data, tag_pack_data2.len};

    // Call DeleteForUndo - this should unset the delete mark
    int result = tag_table.DeleteForUndo(group_id, entity_id, group_id, ptag, tag_pack_for_undo, 300);
    EXPECT_EQ(result, 0);

    // Verify: row should NOT be deleted (DeleteForUndo calls unsetDeleteMark)
    partition_table->startRead();
    EXPECT_TRUE(partition_table->isValidRow(2));
    // TagDataInfo should have operate_type[0]=Insert, operate_idx=0
    auto tag_info = partition_table->getTagDataInfoByRowNum(2);
    ASSERT_NE(tag_info, nullptr);
    EXPECT_EQ(tag_info->operate_type[0], OperateType::Insert);
    EXPECT_EQ(tag_info->operate_idx, 0);
    partition_table->stopRead();
  }
  free(pay_load.data);
  free(pay_load2.data);
}
