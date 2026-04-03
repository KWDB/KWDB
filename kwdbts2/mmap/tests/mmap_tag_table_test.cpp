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
#include "ts_common.h"

class TestTagTable : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/tag_table";
    db_path_ = "/tmp/kwdb_mmap_test/";
    tbl_sub_path_ = "sub_path/";
    table_id_ = 1001;
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

    TagInfo ntag_info2;
    ntag_info2.m_id = 3;
    ntag_info2.m_data_type = DATATYPE::INT64;
    ntag_info2.m_length = sizeof(int64_t);
    ntag_info2.m_size = sizeof(int64_t);
    ntag_info2.m_tag_type = GENERAL_TAG;
    ntag_info2.m_flag = 0;
    schema_.push_back(ntag_info2);
  }

  void TearDown() override {
    system(("rm -rf " + db_path_ + tbl_sub_path_).c_str());
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

TEST_F(TestTagTable, Create_WithHashIndexInfo) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;

  roachpb::NTagIndexInfo idx_info;
  idx_info.add_col_ids(2);
  idx_info.set_index_id(1);
  std::vector<roachpb::NTagIndexInfo> idx_info_vec;
  idx_info_vec.push_back(idx_info);

  int result = tag_table.create(schema_, table_version_, idx_info_vec, err_info);

  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTable, Create_DuplicateVersion) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  ErrorInfo err_info;

  int result1 = tag_table.create(schema_, table_version_, {}, err_info);
  EXPECT_EQ(result1, 0);

  int result2 = tag_table.create(schema_, table_version_, {}, err_info);
  EXPECT_NE(result2, 0);
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

TEST_F(TestTagTable, Remove_Success) {
  {
    TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
    ErrorInfo err_info;
    ASSERT_EQ(CreateTagTableWithData(&tag_table, err_info), 0);

    int result = tag_table.remove(err_info);

    EXPECT_EQ(result, 0);
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

TEST_F(TestTagTable, Schema_PrimaryTagHasStringType) {
  for (const auto& info : schema_) {
    if (info.m_tag_type == PRIMARY_TAG) {
      EXPECT_EQ(info.m_data_type, DATATYPE::STRING);
    }
  }
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
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    db_path_ = "/tmp/kwdb_mmap_test/";
    tbl_sub_path_ = "part_mgr/";
    table_id_ = 2001;
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
  }

  void TearDown() override {
    system(("rm -rf " + db_path_ + tbl_sub_path_).c_str());
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
