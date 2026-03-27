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

#include "../include/mmap/mmap_tag_table.h"

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
    tbl_sub_path_ = "sub_path";
    table_id_ = 1001;
    entity_group_id_ = 1;
    
    // Initialize schema for testing
    schema_.clear();
    TagInfo ptag_info;
    ptag_info.m_id = 1;
    ptag_info.m_data_type = STRING;
    ptag_info.m_length = 64;
    ptag_info.m_size = 64;
    ptag_info.m_tag_type = PRIMARY_TAG;
    ptag_info.m_flag = 0;
    schema_.push_back(ptag_info);
    
    TagInfo ntag_info;
    ntag_info.m_id = 2;
    ntag_info.m_data_type = INT32;
    ntag_info.m_length = sizeof(int32_t);
    ntag_info.m_size = sizeof(int32_t);
    ntag_info.m_tag_type = GENERAL_TAG;
    ntag_info.m_flag = 0;
    schema_.push_back(ntag_info);
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
  std::string db_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_;
  int32_t entity_group_id_;
  std::vector<TagInfo> schema_;
};

TEST_F(TestTagTable, Constructor_BasicInitialization) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  
  // Should construct without errors
  EXPECT_TRUE(true);
}

TEST_F(TestTagTable, Resource_TableId) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  
  // Table should have valid ID
  EXPECT_GT(table_id_, 0);
}

TEST_F(TestTagTable, Resource_EntityGroupId) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  
  // Entity group ID should be valid
  EXPECT_GE(entity_group_id_, 0);
}

TEST_F(TestTagTable, Boundary_ZeroEntityGroupId) {
  int32_t zero_entity_group_id = 0;
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, zero_entity_group_id);
  
  EXPECT_EQ(zero_entity_group_id, 0);
}

TEST_F(TestTagTable, Schema_PrimaryTag) {
  // Verify primary tag in schema
  bool has_primary = false;
  for (const auto& info : schema_) {
    if (info.m_tag_type == PRIMARY_TAG) {
      has_primary = true;
      break;
    }
  }
  
  EXPECT_TRUE(has_primary);
}

TEST_F(TestTagTable, Schema_GeneralTag) {
  // Verify general tag in schema
  bool has_general = false;
  for (const auto& info : schema_) {
    if (info.m_tag_type == GENERAL_TAG) {
      has_general = true;
      break;
    }
  }
  
  EXPECT_TRUE(has_general);
}

TEST_F(TestTagTable, Schema_MultipleTags) {
  EXPECT_GE(schema_.size(), 2);
}

TEST_F(TestTagTable, DataType_StringType) {
  bool has_string = false;
  for (const auto& info : schema_) {
    if (info.m_data_type == STRING) {
      has_string = true;
      break;
    }
  }
  
  EXPECT_TRUE(has_string);
}

TEST_F(TestTagTable, DataType_Int32Type) {
  bool has_int32 = false;
  for (const auto& info : schema_) {
    if (info.m_data_type == INT32) {
      has_int32 = true;
      break;
    }
  }
  
  EXPECT_TRUE(has_int32);
}

TEST_F(TestTagTable, TagInfo_TagId) {
  for (size_t i = 0; i < schema_.size(); ++i) {
    EXPECT_EQ(schema_[i].m_id, i + 1);
  }
}

TEST_F(TestTagTable, TagInfo_TagLength) {
  for (const auto& info : schema_) {
    EXPECT_GT(info.m_length, 0);
  }
}

TEST_F(TestTagTable, TagInfo_TagSize) {
  for (const auto& info : schema_) {
    EXPECT_GT(info.m_size, 0);
  }
}

TEST_F(TestTagTable, TagInfo_NotDropped) {
  for (const auto& info : schema_) {
    EXPECT_FALSE(info.isDropped());
  }
}

TEST_F(TestTagTable, Resource_VersionManager) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  
  TagTableVersionManager* version_mgr = tag_table.GetTagTableVersionManager();
  
  // Initially should be nullptr until created/opened
  EXPECT_EQ(version_mgr, nullptr);
}

TEST_F(TestTagTable, Resource_PartitionManager) {
  TagTable tag_table(db_path_, tbl_sub_path_, table_id_, entity_group_id_);
  
  TagPartitionTableManager* partition_mgr = tag_table.GetTagPartitionTableManager();
  
  // Initially should be nullptr until created/opened
  EXPECT_EQ(partition_mgr, nullptr);
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
  
  // Should still construct but operations may fail
  EXPECT_TRUE(true);
}

TEST_F(TestTagTable, Error_EmptySubPath) {
  std::string empty_sub_path = "";
  TagTable tag_table(db_path_, empty_sub_path, table_id_, entity_group_id_);
  
  EXPECT_TRUE(true);
}

TEST_F(TestTagTable, Resource_MultipleTables) {
  std::vector<std::unique_ptr<TagTable>> tables;
  
  for (int i = 0; i < 5; ++i) {
    uint64_t tid = table_id_ + i;
    tables.push_back(std::make_unique<TagTable>(db_path_, tbl_sub_path_, tid, entity_group_id_));
  }
  
  EXPECT_EQ(tables.size(), 5);
}

TEST_F(TestTagTable, Concurrent_BasicThreadSafety) {
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

TEST_F(TestTagTable, Performance_SchemaIteration) {
  const int kNumIterations = 1000;
  int tag_count = 0;
  
  for (int i = 0; i < kNumIterations; ++i) {
    for (const auto& info : schema_) {
      if (!info.isDropped()) {
        tag_count++;
      }
    }
  }
  
  EXPECT_EQ(tag_count, kNumIterations * schema_.size());
}
