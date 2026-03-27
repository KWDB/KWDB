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
#include <cstdint>

#include "../src/mmap/mmap_entity_row_hash_index.cpp"

class TestMMapEntityRowIndex : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/entity_hash_index";
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
};

TEST_F(TestMMapEntityRowIndex, Constructor_DefaultConstructor) {
  MMapEntityRowHashIndex index;
  
  EXPECT_EQ(index.keySize(), sizeof(uint64_t));
}

TEST_F(TestMMapEntityRowIndex, Constructor_WithKeyLength) {
  MMapEntityRowHashIndex index(16);
  
  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapEntityRowIndex, Constructor_WithBucketInstances) {
  MMapEntityRowHashIndex index(8, 4, 512);
  
  EXPECT_EQ(index.keySize(), 8);
}

TEST_F(TestMMapEntityRowIndex, Open_CreateNewIndex) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
  EXPECT_TRUE(index.metaData().m_bucket_count > 0);
  EXPECT_EQ(index.metaData().m_row_count, 0);
}

TEST_F(TestMMapEntityRowIndex, Open_OpenExistingIndex) {
  // First create
  {
    MMapEntityRowHashIndex index(sizeof(uint64_t));
    ErrorInfo err_info;
    index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  }
  
  // Then open existing
  MMapEntityRowHashIndex index2(sizeof(uint64_t));
  ErrorInfo err_info;
  int result = index2.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapEntityRowIndex, Put_InsertSingleKey) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 100;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 1000;
  
  int result = index.put(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapEntityRowIndex, Get_RetrieveInsertedKey) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 200;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 2000;
  
  index.put(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
}

TEST_F(TestMMapEntityRowIndex, Get_NonExistentKey) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 300;
  auto result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, 0);
  EXPECT_EQ(result.second, 0);
}

TEST_F(TestMMapEntityRowIndex, Delete_RemoveKey) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  uint64_t key = 400;
  TableVersionID version = 1;
  TagPartitionTableRowID rowid = 4000;
  
  index.put(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
  
  auto result = index.delete_data(reinterpret_cast<const char*>(&key), sizeof(key));
  
  EXPECT_EQ(result.first, version);
  EXPECT_EQ(result.second, rowid);
  
  // Verify key is deleted
  auto get_result = index.get(reinterpret_cast<const char*>(&key), sizeof(key));
  EXPECT_EQ(get_result.first, 0);
  EXPECT_EQ(get_result.second, 0);
}

TEST_F(TestMMapEntityRowIndex, Boundary_EmptyKey) {
  MMapEntityRowHashIndex index(0);
  ErrorInfo err_info;
  
  int result = index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  // Should handle zero key length
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapEntityRowIndex, Boundary_LargeKeyLength) {
  MMapEntityRowHashIndex index(1024);
  
  EXPECT_EQ(index.keySize(), 1024);
}

TEST_F(TestMMapEntityRowIndex, Boundary_MultipleBucketInstances) {
  MMapEntityRowHashIndex index(16, 8, 256);
  ErrorInfo err_info;
  
  int result = index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapEntityRowIndex, Error_WrongFilePath) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open("/nonexistent/path/file", "/tmp", "", O_RDWR, err_info);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapEntityRowIndex, Error_EmptyPath) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  
  int result = index.open("", "/tmp", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_NE(result, 0) << "Should not accept empty path";
}

TEST_F(TestMMapEntityRowIndex, Resource_MultipleOpenClose) {
  for (int i = 0; i < 3; ++i) {
    MMapEntityRowHashIndex index(sizeof(uint64_t));
    ErrorInfo err_info;
    
    index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
    // Destructor will close
  }
}

TEST_F(TestMMapEntityRowIndex, Resource_ReserveSpace) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  int result = index.reserve(2048);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapEntityRowIndex, LSN_SetAndGet) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  uint64_t test_lsn = 12345;
  index.setLSN(test_lsn);
  
  EXPECT_EQ(index.getLSN(), test_lsn);
}

TEST_F(TestMMapEntityRowIndex, Drop_SetAndCheck) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_FALSE(index.isDroped());
  
  index.setDrop();
  
  EXPECT_TRUE(index.isDroped());
}

TEST_F(TestMMapEntityRowIndex, Concurrent_BasicThreadSafety) {
  MMapEntityRowHashIndex* index = new MMapEntityRowHashIndex(sizeof(uint64_t));
  ErrorInfo err_info;
  index->open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);
  
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([index, &success_count, i]() {
      for (int j = 0; j < 10; ++j) {
        uint64_t key = i * 100 + j;
        index->dataRlock();
        index->get(reinterpret_cast<const char*>(&key), sizeof(key));
        index->dataUnlock();
        success_count++;
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(success_count.load(), 40);
  delete index;
}

TEST_F(TestMMapEntityRowIndex, Performance_MultipleInsertions) {
  MMapEntityRowHashIndex index(sizeof(uint64_t));
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test/", "", O_CREAT | O_RDWR, err_info);
  
  const int kNumInsertions = 100;
  for (int i = 0; i < kNumInsertions; ++i) {
    uint64_t key = i;
    TableVersionID version = 1;
    TagPartitionTableRowID rowid = i * 10;
    
    int result = index.put(reinterpret_cast<const char*>(&key), sizeof(key), version, rowid);
    EXPECT_EQ(result, 0);
  }
}
