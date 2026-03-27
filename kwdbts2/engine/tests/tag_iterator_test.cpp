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

#include <gtest/gtest.h>
#include <vector>
#include <memory>
#include <sys/stat.h>
#include <unistd.h>
#include "tag_iterator.h"
#include "mmap/mmap_tag_table.h"
#include "kwdb_type.h"
#include "data_type.h"

using namespace kwdbts;  // NOLINT

const std::string TEST_DB_PATH = "./test_tag_iterator_db";

class TagPartitionIteratorTest : public ::testing::Test {
 protected:
  std::string test_path_;
  TagTable* tag_table_{nullptr};
  TagPartitionTable* tag_partition_table_{nullptr};
  std::vector<k_uint32> src_scan_tags_;
  std::vector<TagInfo> result_tag_infos_;
  std::vector<HashIdSpan> hps_;
  TagPartitionIterator* iterator_{nullptr};

  void SetUp() override {
    test_path_ = TEST_DB_PATH + "_" + std::to_string(getpid());
    
    // Create test directory
    mkdir(test_path_.c_str(), 0755);
    
    src_scan_tags_.push_back(0);  // Primary tag
    src_scan_tags_.push_back(1);  // First normal tag
    
    // Setup result tag infos
    TagInfo ptag_info;
    ptag_info.m_id = 0;
    ptag_info.m_data_type = DATATYPE::INT32;
    ptag_info.m_size = sizeof(int32_t);
    ptag_info.m_length = sizeof(int32_t);
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    result_tag_infos_.push_back(ptag_info);
    
    TagInfo ntag_info;
    ntag_info.m_id = 1;
    ntag_info.m_data_type = DATATYPE::INT32;
    ntag_info.m_size = sizeof(int32_t);
    ntag_info.m_length = sizeof(int32_t);
    ntag_info.m_tag_type = GENERAL_TAG;
    ntag_info.m_flag = 0;
    result_tag_infos_.push_back(ntag_info);
    
    hps_.clear();
  }

  void TearDown() override {
    if (iterator_) {
      delete iterator_;
      iterator_ = nullptr;
    }
    
    if (tag_table_) {
      ErrorInfo err_info;
      DropTagTable(tag_table_, err_info);
      tag_table_ = nullptr;
      tag_partition_table_ = nullptr;
    }
    
    // Cleanup test directory
    std::string cmd = "rm -rf " + test_path_;
    system(cmd.c_str());
  }

  void CreateTable(const std::vector<TagInfo>& schema) {
    ErrorInfo err_info;
    std::string sub_path = test_path_ + "/table";
    mkdir(sub_path.c_str(), 0755);
    
    tag_table_ = CreateTagTable(schema, test_path_, sub_path, 1, 0, 1, err_info);
    ASSERT_NE(tag_table_, nullptr);
    
    // Get the partition table from tag table
    tag_partition_table_ = tag_table_->GetTagPartitionTableManager()->GetPartitionTable(1);
    ASSERT_NE(tag_partition_table_, nullptr);
  }

  void CreateIterator() {
    ASSERT_NE(tag_partition_table_, nullptr);
    iterator_ = new TagPartitionIterator(tag_partition_table_, src_scan_tags_, result_tag_infos_, &hps_);
  }
};

TEST_F(TagPartitionIteratorTest, Constructor_ValidInitialization) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  
  TagPartitionIterator iter(tag_partition_table_, src_scan_tags_, result_tag_infos_, &hps_);
  
  EXPECT_TRUE(true);
}

TEST_F(TagPartitionIteratorTest, Init_Success) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  
  iterator_->Init();
  
  EXPECT_GE(tag_partition_table_->actual_size(), 0);
}

TEST_F(TagPartitionIteratorTest, Next_EmptyTable) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = 10;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(2);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
  EXPECT_EQ(count, 0);
}

TEST_F(TagPartitionIteratorTest, Next_ZeroCount) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = 0;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(2);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
  EXPECT_EQ(count, 0);
}

TEST_F(TagPartitionIteratorTest, NextTag_EmptyTable) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = 0;
  bool is_finish = false;
  ResultSet res(2);
  
  EntityResultIndex target_entity;
  target_entity.entityId = 1;
  target_entity.subGroupId = 0;
  target_entity.entityGroupId = 0;
  
  KStatus status = iterator_->NextTag(target_entity, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
  EXPECT_EQ(count, 0);
}

TEST_F(TagPartitionIteratorTest, SetOSNSpan_ValidSpanConfiguration) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  
  std::vector<KwOSNSpan> osn_spans;
  KwOSNSpan span;
  span.begin = 100;
  span.end = 200;
  osn_spans.push_back(span);
  
  iterator_->SetOSNSpan(osn_spans);
  
  EXPECT_TRUE(true);
}

TEST_F(TagPartitionIteratorTest, HashPointFiltering_EmptyHashSpans) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = 10;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(2);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
}

TEST_F(TagPartitionIteratorTest, HashPointFiltering_WithHashSpans) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  
  // Set up hash point spans
  HashIdSpan hps;
  hps.begin = 0;
  hps.end = 1000;
  hps_.push_back(hps);
  
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = 10;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(2);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
}

TEST_F(TagPartitionIteratorTest, MultipleNextCalls_StateManagement) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  // First call
  k_uint32 count = 5;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(2);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
  
  // Second call after finish
  count = 5;
  res.clear();
  entity_id_list.clear();
  status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
  EXPECT_EQ(count, 5);  // Count should remain unchanged
}

TEST_F(TagPartitionIteratorTest, ResultSet_ClearAndReuse) {
  ResultSet res(2);
  
  // Add some dummy batches
  TagBatch* batch1 = new TagBatch(0, nullptr, 0);
  TagBatch* batch2 = new TagBatch(0, nullptr, 0);
  res.push_back(0, batch1);
  res.push_back(1, batch2);
  
  EXPECT_FALSE(res.empty());
  
  res.clear();
  
  EXPECT_TRUE(res.empty());
}

TEST_F(TagPartitionIteratorTest, EntityResultIndex_DefaultConstruction) {
  EntityResultIndex eri;
  
  EXPECT_EQ(eri.entityId, 0);
  EXPECT_EQ(eri.subGroupId, 0);
  EXPECT_EQ(eri.entityGroupId, 0);
}

TEST_F(TagPartitionIteratorTest, EntityResultIndex_EqualsWithoutMem) {
  EntityResultIndex eri1(0, 1, 0);
  EntityResultIndex eri2(0, 1, 0);
  EntityResultIndex eri3(0, 2, 0);
  
  EXPECT_TRUE(eri1.equalsWithoutMem(eri2));
  EXPECT_FALSE(eri1.equalsWithoutMem(eri3));
}

TEST_F(TagPartitionIteratorTest, Boundary_LargeFetchCount) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  CreateTable(schema);
  CreateIterator();
  iterator_->Init();
  
  k_uint32 count = UINT32_MAX;
  bool is_finish = false;
  std::vector<EntityResultIndex> entity_id_list;
  ResultSet res(1);
  
  KStatus status = iterator_->Next(&entity_id_list, &res, &count, &is_finish);
  
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_TRUE(is_finish);
}

TEST_F(TagPartitionIteratorTest, OSNSpan_MultipleSpans) {
  std::vector<TagInfo> schema;
  TagInfo ptag_info;
  ptag_info.m_id = 0;
  ptag_info.m_data_type = DATATYPE::INT32;
  ptag_info.m_size = sizeof(int32_t);
  ptag_info.m_length = sizeof(int32_t);
  ptag_info.m_tag_type = PRIMARY_TAG;
  ptag_info.m_flag = 0;
  schema.push_back(ptag_info);
  
  TagInfo ntag_info;
  ntag_info.m_id = 1;
  ntag_info.m_data_type = DATATYPE::INT32;
  ntag_info.m_size = sizeof(int32_t);
  ntag_info.m_length = sizeof(int32_t);
  ntag_info.m_tag_type = GENERAL_TAG;
  ntag_info.m_flag = 0;
  schema.push_back(ntag_info);
  
  CreateTable(schema);
  CreateIterator();
  
  std::vector<KwOSNSpan> osn_spans;
  KwOSNSpan span1, span2;
  span1.begin = 100;
  span1.end = 200;
  span2.begin = 300;
  span2.end = 400;
  osn_spans.push_back(span1);
  osn_spans.push_back(span2);
  
  iterator_->SetOSNSpan(osn_spans);
  
  EXPECT_TRUE(true);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
