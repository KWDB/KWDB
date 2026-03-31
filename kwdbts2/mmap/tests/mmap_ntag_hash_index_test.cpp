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

#include "../include/mmap/mmap_ntag_hash_index.h"

class TestMMapNTagHashIndex : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/ntag_hash_index";
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
};

TEST_F(TestMMapNTagHashIndex, Constructor_WithParameters) {
  std::vector<uint32_t> col_ids = {1, 2, 3};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  
  EXPECT_EQ(index.keySize(), sizeof(uint64_t));
}

TEST_F(TestMMapNTagHashIndex, Constructor_EmptyColIds) {
  std::vector<uint32_t> empty_col_ids;
  MMapNTagHashIndex index(sizeof(uint64_t), 100, empty_col_ids);
  
  EXPECT_EQ(index.keySize(), sizeof(uint64_t));
}

TEST_F(TestMMapNTagHashIndex, Open_CreateNewIndex) {
  std::vector<uint32_t> col_ids = {1, 2};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  
  int result = index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
  EXPECT_TRUE(index.metaData().m_bucket_count > 0);
}

TEST_F(TestMMapNTagHashIndex, Insert_SingleEntry) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 12345;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 1000;
  
  int result = index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapNTagHashIndex, Get_RetrieveInsertedKey) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 67890;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 2000;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  index.sync(MS_SYNC);

  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
}

TEST_F(TestMMapNTagHashIndex, Get_NonExistentKey) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 11111;
  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, INVALID_TABLE_VERSION_ID);
  EXPECT_EQ(result.second, INVALID_TABLE_VERSION_ID);
}

TEST_F(TestMMapNTagHashIndex, Remove_DeleteInsertedKey) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 22222;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 3000;
  
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  auto result = index.remove(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
}

TEST_F(TestMMapNTagHashIndex, ReadAll_MultipleVersions) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 33333;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 4000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 4001);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 3, 4002);
  index.sync(MS_SYNC);

  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> results;
  int result = index.read_all(reinterpret_cast<const char*>(&key), sizeof(key), results);

  EXPECT_EQ(result, 0);
  EXPECT_GE(results.size(), 1);
}

TEST_F(TestMMapNTagHashIndex, RemoveAll_DeleteAllVersions) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 44444;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 5000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 5001);
  index.sync(MS_SYNC);

  auto results = index.remove_all(reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_GE(results.size(), 1);
}

TEST_F(TestMMapNTagHashIndex, Boundary_LargeKeyLength) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(1024, 100, col_ids);
  
  EXPECT_EQ(index.keySize(), 1024);
}

TEST_F(TestMMapNTagHashIndex, Boundary_MultipleBucketInstances) {
  std::vector<uint32_t> col_ids = {1, 2, 3, 4};
  MMapNTagHashIndex index(16, 8, col_ids, 4, 256);
  ErrorInfo err_info;
  
  int result = index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapNTagHashIndex, Error_WrongFilePath) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  
  int result = index.open("/nonexistent/path/file", "/tmp", "", O_RDWR, err_info);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapNTagHashIndex, Resource_IndexID) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  
  uint32_t index_id = index.getIndexID();
  EXPECT_EQ(index_id, 100);
}

TEST_F(TestMMapNTagHashIndex, Resource_ColIds) {
  std::vector<uint32_t> col_ids = {10, 20, 30};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  
  auto stored_col_ids = index.getTagColIDs();
  EXPECT_EQ(stored_col_ids[0], 10);
  EXPECT_EQ(stored_col_ids[1], 20);
  EXPECT_EQ(stored_col_ids[2], 30);
}

TEST_F(TestMMapNTagHashIndex, Performance_BulkInsert) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

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

TEST_F(TestMMapNTagHashIndex, Concurrent_BasicThreadSafety) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex* index = new MMapNTagHashIndex(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index->open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

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

TEST_F(TestMMapNTagHashIndex, UpdateKeyLen_Basic) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  int result = index.updateKeyLen();

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapNTagHashIndex, Sync_AfterInsert) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 55555;
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 6000);

  int result = index.sync(MS_SYNC);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapNTagHashIndex, Clear_AfterInsert) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 66666;
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 7000);

  int result = index.clear();

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapNTagHashIndex, GetAll_MultipleResults) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 77777;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 8000);
  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 2, 8001);
  index.sync(MS_SYNC);

  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> results;
  int result = index.get_all(reinterpret_cast<const char*>(&key), sizeof(key), results);

  EXPECT_EQ(result, 0);
  EXPECT_GE(results.size(), 1);
}

TEST_F(TestMMapNTagHashIndex, Open_ExistingIndex) {
  {
    std::vector<uint32_t> col_ids = {1};
    MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
    ErrorInfo err_info;
    ASSERT_EQ(index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info), 0);

    uint64_t key = 88888;
    index.insert(reinterpret_cast<const char*>(&key), sizeof(key), 1, 9000);
  }

  {
    std::vector<uint32_t> col_ids = {1};
    MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
    ErrorInfo err_info;

    int result = index.open("ntag_hash_index", test_path_, "", O_RDWR, err_info);

    EXPECT_EQ(result, 0);
  }
}

TEST_F(TestMMapNTagHashIndex, ElementCount_Basic) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t count = index.getElementCount();

  EXPECT_EQ(count, 0);
}

TEST_F(TestMMapNTagHashIndex, Insert_DifferentKeys) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  for (uint64_t i = 0; i < 10; ++i) {
    int result = index.insert(reinterpret_cast<const char*>(&i), sizeof(i), 1, i * 100);
    EXPECT_EQ(result, 0);
  }

  EXPECT_EQ(index.getElementCount(), 10);
}

TEST_F(TestMMapNTagHashIndex, Insert_WithMultipleColIds) {
  std::vector<uint32_t> col_ids = {1, 2, 3};
  MMapNTagHashIndex index(24, 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  char key[24] = {0};
  memcpy(key, "abc123", 6);

  int result = index.insert(key, sizeof(key), 1, 1000);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapNTagHashIndex, GetColIDs_SortOrder) {
  std::vector<uint32_t> col_ids = {30, 10, 20};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);

  auto stored_col_ids = index.getColIDs();

  EXPECT_EQ(stored_col_ids.size(), 3);
  EXPECT_EQ(stored_col_ids[0], 10);
  EXPECT_EQ(stored_col_ids[1], 20);
  EXPECT_EQ(stored_col_ids[2], 30);
}

TEST_F(TestMMapNTagHashIndex, Remove_ByRowID) {
  std::vector<uint32_t> col_ids = {1};
  MMapNTagHashIndex index(sizeof(uint64_t), 100, col_ids);
  ErrorInfo err_info;
  index.open("ntag_hash_index", test_path_, "", O_CREAT | O_RDWR, err_info);

  uint64_t key = 99999;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 10000;

  index.insert(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);

  auto result = index.remove(rowid, version, reinterpret_cast<const char*>(&key), sizeof(key));

  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
}
