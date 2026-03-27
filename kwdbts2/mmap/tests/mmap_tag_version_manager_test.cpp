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

#include "../include/mmap/mmap_tag_version_manager.h"

class TestTagTableVersionManager : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/version_mgr";
    table_id_ = 1001;
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
  uint64_t table_id_;
};

TEST_F(TestTagTableVersionManager, Constructor_BasicInitialization) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  // Should construct without errors
  EXPECT_TRUE(true);
}

TEST_F(TestTagTableVersionManager, Init_BasicInit) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  ErrorInfo err_info;
  
  int result = version_mgr.Init(err_info);
  
  EXPECT_EQ(result, SUCCESS);
}

TEST_F(TestTagTableVersionManager, CreateTagVersionObject_FirstVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 1;
  ErrorInfo err_info;
  
  TagVersionObject* version_obj = version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  
  // Version object should be created
  EXPECT_NE(version_obj, nullptr);
  if (version_obj) {
    EXPECT_EQ(version_obj->getTableVersion(), ts_version);
  }
}

TEST_F(TestTagTableVersionManager, CreateTagVersionObject_DuplicateVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 2;
  ErrorInfo err_info;
  
  // Create first version
  TagVersionObject* obj1 = version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  // Try to create same version again
  TagVersionObject* obj2 = version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  
  // Second creation should return existing object
  EXPECT_NE(obj1, nullptr);
  EXPECT_EQ(obj1, obj2);
}

TEST_F(TestTagTableVersionManager, OpenTagVersionObject_OpenExisting) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 3;
  ErrorInfo err_info;
  
  // First create
  TagVersionObject* created_obj = version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  EXPECT_NE(created_obj, nullptr);
  
  // Then open (should return cached object)
  TagVersionObject* opened_obj = version_mgr.OpenTagVersionObject(ts_version, err_info);
  
  EXPECT_NE(opened_obj, nullptr);
  if (opened_obj) {
    EXPECT_EQ(opened_obj->getTableVersion(), ts_version);
  }
}

TEST_F(TestTagTableVersionManager, GetVersionObject_RetrieveFromCache) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 4;
  ErrorInfo err_info;
  
  // Create version first
  version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  
  // Then retrieve
  TagVersionObject* retrieved_obj = version_mgr.GetVersionObject(ts_version);
  
  EXPECT_NE(retrieved_obj, nullptr);
  if (retrieved_obj) {
    EXPECT_EQ(retrieved_obj->getTableVersion(), ts_version);
  }
}

TEST_F(TestTagTableVersionManager, GetVersionObject_NonExistentVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  uint32_t non_existent_version = 999;
  
  TagVersionObject* obj = version_mgr.GetVersionObject(non_existent_version);
  
  // Should return nullptr for non-existent version
  EXPECT_EQ(obj, nullptr);
}

TEST_F(TestTagTableVersionManager, UpdateNewestTableVersion_SetLatestVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 5;
  ErrorInfo err_info;
  
  version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  version_mgr.UpdateNewestTableVersion(ts_version);
  
  // Should update without errors
  EXPECT_TRUE(true);
}

TEST_F(TestTagTableVersionManager, SyncCurrentTableVersion_SyncMetadata) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t ts_version = 6;
  ErrorInfo err_info;
  
  version_mgr.CreateTagVersionObject(schema, ts_version, err_info);
  version_mgr.UpdateNewestTableVersion(ts_version);
  version_mgr.SyncCurrentTableVersion();
}

TEST_F(TestTagTableVersionManager, UpdataTagTableVersionManager_UpdateToMax) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  ErrorInfo err_info;
  
  // Create multiple versions
  version_mgr.CreateTagVersionObject(schema, 10, err_info);
  version_mgr.CreateTagVersionObject(schema, 20, err_info);
  version_mgr.CreateTagVersionObject(schema, 30, err_info);
  
  int result = version_mgr.UpdataTagTableVersionManager();
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestTagTableVersionManager, RemoveAll_DeleteAllVersions) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  ErrorInfo err_info;
  
  // Create some versions
  version_mgr.CreateTagVersionObject(schema, 40, err_info);
  version_mgr.CreateTagVersionObject(schema, 50, err_info);
  
  // Remove all
  int result = version_mgr.RemoveAll(err_info);
  
  EXPECT_EQ(result, 0);
  
  // Verify all versions are removed
  TagVersionObject* obj = version_mgr.GetVersionObject(40);
  EXPECT_EQ(obj, nullptr);
}

TEST_F(TestTagTableVersionManager, CloneTagVersionObject_CopyExistingVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_data_type = INT32;
  schema.push_back(info);
  
  uint32_t src_version = 60;
  uint32_t new_version = 61;
  ErrorInfo err_info;
  
  // Create source version
  TagVersionObject* src_obj = version_mgr.CreateTagVersionObject(schema, src_version, err_info);
  EXPECT_NE(src_obj, nullptr);
}

TEST_F(TestTagTableVersionManager, RollbackTableVersion_RemoveSpecificVersion) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  uint32_t rollback_version = 70;
  ErrorInfo err_info;
  
  // Create version to rollback
  version_mgr.CreateTagVersionObject(schema, rollback_version, err_info);
  
  // Rollback
  int result = version_mgr.RollbackTableVersion(rollback_version, err_info);
  
  EXPECT_EQ(result, 0);
  
  // Verify version is removed
  TagVersionObject* obj = version_mgr.GetVersionObject(rollback_version);
  EXPECT_EQ(obj, nullptr);
}

TEST_F(TestTagTableVersionManager, Resource_MultipleVersions) {
  TagTableVersionManager version_mgr(test_path_, "sub_path", table_id_);
  
  std::vector<TagInfo> schema;
  TagInfo info;
  info.m_id = 1;
  info.m_data_type = INT32;
  info.m_length = sizeof(int32_t);
  info.m_size = sizeof(int32_t);
  info.m_tag_type = GENERAL_TAG;
  info.m_flag = 0;
  schema.push_back(info);
  
  ErrorInfo err_info;
  
  // Create multiple versions
  for (uint32_t i = 0; i < 5; ++i) {
    TagVersionObject* obj = version_mgr.CreateTagVersionObject(schema, i + 100, err_info);
    EXPECT_NE(obj, nullptr);
  }
  
  // Verify all versions exist
  for (uint32_t i = 0; i < 5; ++i) {
    TagVersionObject* obj = version_mgr.GetVersionObject(i + 100);
    EXPECT_NE(obj, nullptr);
  }
}

TEST_F(TestTagTableVersionManager, Concurrent_BasicThreadSafety) {
  TagTableVersionManager* version_mgr = new TagTableVersionManager(test_path_, "sub_path", table_id_);
  
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);
  
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([version_mgr, &success_count, i]() {
      std::vector<TagInfo> schema;
      TagInfo info;
      info.m_data_type = INT32;
      schema.push_back(info);
      
      ErrorInfo err_info;
      uint32_t version = 200 + i;
      
      TagVersionObject* obj = version_mgr->CreateTagVersionObject(schema, version, err_info);
      if (obj != nullptr) {
        success_count++;
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(success_count.load(), 4);
  delete version_mgr;
}
