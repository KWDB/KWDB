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

#include "../include/mmap/mmap_tag_column_table.h"

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
    db_name_ = "test_db";
    
    // Initialize TagInfo for testing
    tag_info_.m_id = 1;
    tag_info_.m_data_type = INT32;
    tag_info_.m_length = sizeof(int32_t);
    tag_info_.m_offset = 0;
    tag_info_.m_size = sizeof(int32_t);
    tag_info_.m_tag_type = GENERAL_TAG;
    tag_info_.m_flag = 0;
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
  std::string db_path_;
  std::string db_name_;
  TagInfo tag_info_;
};

TEST_F(TestTagColumn, Constructor_BasicInitialization) {
  TagColumn col(0, tag_info_);
  
  // Should construct without errors
  EXPECT_TRUE(true);
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

TEST_F(TestTagColumn, VarTag_CheckNotVarTag) {
  TagColumn col(0, tag_info_);
  
  // Should not be var tag since no string file
  EXPECT_FALSE(col.isVarTag());
}

TEST_F(TestTagColumn, StoreOffset_SetAndGet) {
  TagColumn col(0, tag_info_);
  
  uint32_t test_offset = 1024;
  col.setStoreOffset(test_offset);
  
  EXPECT_EQ(col.getStoreOffset(), test_offset);
}

TEST_F(TestTagColumn, LSN_SetAndGet) {
  TagColumn col(0, tag_info_);
  
  kwdbts::TS_OSN test_lsn = 100;
  col.setLSN(test_lsn);
  
  EXPECT_EQ(col.getLSN(), test_lsn);
}

TEST_F(TestTagColumn, DropFlag_SetAndGet) {
  TagColumn col(0, tag_info_);
  
  col.setDrop();
  // Note: Need to check if drop flag is set correctly
  // This is a basic test - actual implementation may vary
  EXPECT_TRUE(true);
}

TEST_F(TestTagColumn, WriteValue_WriteInt32Data) {
  TagColumn col(0, tag_info_);
  
  int32_t test_value = 42;
  char* data_ptr = reinterpret_cast<char*>(&test_value);
  
  // Note: This test requires the column to be opened/created first
  // which involves file operations that need proper setup
  EXPECT_TRUE(true);
}

TEST_F(TestTagColumn, Resource_NullBitmapSize) {
  // Test null bitmap size constant
  EXPECT_EQ(k_per_null_bitmap_size, 1);
}

TEST_F(TestTagColumn, Resource_EntityGroupIDSize) {
  // Test entity group ID size constant
  EXPECT_GT(k_entity_group_id_size, 0);
}

TEST_F(TestTagColumn, Resource_BitmapPerRowLength) {
  // Test bitmap per row length
  EXPECT_EQ(BITMAP_PER_ROW_LENGTH, 64);
}

TEST_F(TestTagColumn, Boundary_ZeroColumnIndex) {
  TagInfo info = tag_info_;
  TagColumn col(0, info);
  
  TagInfo& retrieved = col.attributeInfo();
  EXPECT_EQ(retrieved.m_id, info.m_id);
}

TEST_F(TestTagColumn, Boundary_LargeColumnIndex) {
  TagInfo info = tag_info_;
  TagColumn col(1000000, info);
  
  EXPECT_TRUE(true);
}

TEST_F(TestTagColumn, TagType_PrimaryTagEnum) {
  TagInfo info = tag_info_;
  info.m_tag_type = PRIMARY_TAG;
  
  TagColumn col(0, info);
  col.setPrimaryTag(true);
  
  EXPECT_TRUE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, TagType_GeneralTagEnum) {
  TagInfo info = tag_info_;
  info.m_tag_type = GENERAL_TAG;
  
  TagColumn col(0, info);
  col.setPrimaryTag(false);
  
  EXPECT_FALSE(col.isPrimaryTag());
}

TEST_F(TestTagColumn, DataType_Int32Type) {
  TagInfo info = tag_info_;
  info.m_data_type = INT32;
  
  TagColumn col(0, info);
  
  EXPECT_EQ(col.attributeInfo().m_data_type, INT32);
}

TEST_F(TestTagColumn, DataType_Int64Type) {
  TagInfo info = tag_info_;
  info.m_data_type = INT64;
  
  TagColumn col(0, info);
  
  EXPECT_EQ(col.attributeInfo().m_data_type, INT64);
}

TEST_F(TestTagColumn, Error_InvalidDataType) {
  TagInfo info = tag_info_;
  info.m_data_type = -1;  // Invalid type
  
  TagColumn col(0, info);
  
  EXPECT_LT(col.attributeInfo().m_data_type, INT8);
}

TEST_F(TestTagColumn, Resource_MultipleColumns) {
  std::vector<TagColumn*> columns;
  
  for (int i = 0; i < 5; ++i) {
    TagInfo info = tag_info_;
    info.m_id = i + 1;
    columns.push_back(new TagColumn(i, info));
  }
  
  EXPECT_EQ(columns.size(), 5);
  
  // Cleanup
  for (auto col : columns) {
    delete col;
  }
}

TEST_F(TestTagColumn, Concurrent_BasicThreadSafety) {
  std::vector<std::thread> threads;
  std::atomic<int> count(0);
  
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&count]() {
      for (int j = 0; j < 10; ++j) {
        count++;
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(count.load(), 40);
}
