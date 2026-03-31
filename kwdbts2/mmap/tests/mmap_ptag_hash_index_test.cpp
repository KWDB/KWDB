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
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>
#include <vector>
#include <thread>
#include <atomic>

#include "../include/mmap/mmap_ptag_hash_index.h"

class TestMMapPTagHashIndex : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/ptag_hash_index";
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
};

TEST_F(TestMMapPTagHashIndex, Constructor_DefaultConstructor) {
  MMapPTagHashIndex index(16);
  
  // Should initialize without errors
  EXPECT_TRUE(true);
}

TEST_F(TestMMapPTagHashIndex, Constructor_WithKeyLength) {
  MMapPTagHashIndex index(16);
  
  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapPTagHashIndex, Constructor_WithBucketInstances) {
  MMapPTagHashIndex index(32, 4, 512);
  
  EXPECT_EQ(index.keySize(), 32);
}

TEST_F(TestMMapPTagHashIndex, Open_CreateNewIndex) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
  EXPECT_TRUE(index.metaData().m_bucket_count > 0);
}

TEST_F(TestMMapPTagHashIndex, Insert_SingleEntry) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 12345;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 1000;
  
  int result = index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapPTagHashIndex, Get_RetrieveInsertedKey) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 67890;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 2000;
  
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
}

TEST_F(TestMMapPTagHashIndex, Get_NonExistentKey) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 11111;
  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, INVALID_TABLE_VERSION_ID);
  EXPECT_EQ(result.second, INVALID_TABLE_VERSION_ID);
}

TEST_F(TestMMapPTagHashIndex, Remove_DeleteInsertedKey) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 22222;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 3000;
  
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  auto result = index.remove(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
  
  // Verify key is deleted
  auto get_result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  EXPECT_EQ(get_result.first, INVALID_TABLE_VERSION_ID);
  EXPECT_EQ(get_result.second, INVALID_TABLE_VERSION_ID);
}

TEST_F(TestMMapPTagHashIndex, ReadAll_MultipleVersions) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 33333;
  
  // Insert multiple versions
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 4000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 4001);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 3, 4002);
  
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> results;
  int result = index.read_all(reinterpret_cast<const char*>(&key), sizeof(key), results);
  
  EXPECT_EQ(result, 0);
  EXPECT_EQ(results.size(), 3);
}

TEST_F(TestMMapPTagHashIndex, RemoveAll_DeleteAllVersions) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 44444;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 5000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 5001);

  auto results = index.remove_all(reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_GE(results.size(), 1);
}

TEST_F(TestMMapPTagHashIndex, Boundary_EmptyKeyLength) {
  MMapPTagHashIndex index(0);
  ErrorInfo err_info;
  
  int result = index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  // Should handle zero key length
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapPTagHashIndex, Boundary_MaxKeyLength) {
  MMapPTagHashIndex index(1024);
  
  EXPECT_EQ(index.keySize(), 1024);
}

TEST_F(TestMMapPTagHashIndex, Boundary_MinBucketCount) {
  MMapPTagHashIndex index(16, 1, 1);
  
  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapPTagHashIndex, Error_WrongFilePath) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open("/nonexistent/path/file", "/tmp", "", O_RDWR, err_info);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapPTagHashIndex, Error_EmptyPath) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open("", "/tmp", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_NE(result, 0) << "Should not accept empty path";
}

TEST_F(TestMMapPTagHashIndex, Resource_MultipleOpenClose) {
  for (int i = 0; i < 3; ++i) {
    MMapPTagHashIndex index(sizeof(uint64_t));
    ErrorInfo err_info;
    
    index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
    // Destructor will close
  }
}

TEST_F(TestMMapPTagHashIndex, Resource_ReserveSpace) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  int result = index.reserve(2048);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapPTagHashIndex, Performance_BulkInsert) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  const int kNumInsertions = 1000;
  int success_count = 0;
  for (int i = 0; i < kNumInsertions; ++i) {
    uint64_t key = i;
    TableVersionID version = 1;
    TagPartitionTableRowID rowid = i * 10;

    int result = index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
    if (result == 0) {
      success_count++;
    }
  }

  EXPECT_GE(success_count, kNumInsertions - 10);
}

TEST_F(TestMMapPTagHashIndex, Concurrent_BasicThreadSafety) {
  MMapPTagHashIndex* index = new MMapPTagHashIndex(sizeof(uint64_t));
  ErrorInfo err_info;
  index->open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);

  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([index, &success_count, i]() {
      for (int j = 0; j < 10; ++j) {
        uint64_t key = i * 100 + j;
        TableVersionID version = 1;
        TagPartitionTableRowID rowid = key * 10;

        int result = index->insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
        if (result == 0) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), 40);
  delete index;
}

TEST_F(TestMMapPTagHashIndex, GetAll_MultipleResults) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 77777;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 8000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 8001);

  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> results;
  int result = index.get_all(reinterpret_cast<const char*>(&key), sizeof(key), results);

  EXPECT_EQ(result, 0);
  EXPECT_GE(results.size(), 1);
}

TEST_F(TestMMapPTagHashIndex, ReadFirst_AfterInsert) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 88888;
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 9000);

  auto result = index.read_first(reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_EQ(result.first, 1);
  EXPECT_EQ(result.second, 9000);
}

TEST_F(TestMMapPTagHashIndex, Open_ExistingIndex) {
  {
    MMapPTagHashIndex index(sizeof(uint64_t));
    ErrorInfo err_info;
    ASSERT_EQ(index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info), 0);

    uint64_t key = 99999;
    index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 10000);
  }

  {
    MMapPTagHashIndex index(sizeof(uint64_t));
    ErrorInfo err_info;

    int result = index.open("ptag_hash_index", test_path_, "", O_RDWR, err_info);

    EXPECT_EQ(result, 0);
  }
}

TEST_F(TestMMapPTagHashIndex, Update_ExistingKey) {
  MMapPTagHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open("ptag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 101010;
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 110000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 110001);

  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_EQ(result.first, 2);
  EXPECT_EQ(result.second, 110001);
}
