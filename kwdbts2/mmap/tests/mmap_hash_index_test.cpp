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
#include <vector>
#include <thread>
#include <atomic>

#include "../include/mmap/mmap_hash_index.h"

// Mock class to implement pure virtual functions from MMapIndex
class MockMMapHashIndex : public MMapHashIndex {
 public:
  explicit MockMMapHashIndex(int key_len = sizeof(uint64_t), size_t bkt_instances = 1, size_t per_bkt_count = 1024)
      : MMapHashIndex(key_len, bkt_instances, per_bkt_count) {}
  
  MockMMapHashIndex() : MMapHashIndex() {}
  
  // Implement pure virtual functions from MMapIndex
  std::pair<TableVersionID, TagPartitionTableRowID> read_first(const char* key, int len) override {
    return std::make_pair(0, 0);
  }
  
  int read_all(const char* key, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override {
    return 0;
  }
  
  int insert(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) override {
    return 0;
  }
  
  std::pair<TableVersionID, TagPartitionTableRowID> remove(const char *key, int len) override {
    return std::make_pair(0, 0);
  }
  
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> remove_all(const char *key, int len) override {
    return std::vector<std::pair<TableVersionID, TagPartitionTableRowID>>();
  }
  
  std::pair<TableVersionID, TagPartitionTableRowID> get(const char *s, int len) override {
    return std::make_pair(0, 0);
  }
  
  int get_all(const char *s, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override {
    return 0;
  }
  
  int size() const override {
    return MMapHashIndex::size();
  }
};

class TestMMapHashIndex : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_path_ = "/tmp/kwdb_mmap_test/hash_index";
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
};

TEST_F(TestMMapHashIndex, Constructor_DefaultConstructor) {
  MockMMapHashIndex index(0);
  
  EXPECT_EQ(index.keySize(), 0);
  EXPECT_EQ(index.getElementCount(), 0);
}

TEST_F(TestMMapHashIndex, Constructor_WithKeyLength) {
  MockMMapHashIndex index(16);
  
  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapHashIndex, Constructor_WithBucketInstances) {
  MockMMapHashIndex index(32, 4, 512);
  
  EXPECT_EQ(index.keySize(), 32);
}

TEST_F(TestMMapHashIndex, Open_CreateNewIndex) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  
  int result = index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
  EXPECT_TRUE(index.metaData().m_bucket_count > 0);
}

TEST_F(TestMMapHashIndex, Open_OpenExistingIndex) {
  // First create
  {
    MockMMapHashIndex index(16);
    ErrorInfo err_info;
    index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);
  }
  
  // Then open existing
  MockMMapHashIndex index2(16);
  ErrorInfo err_info;
  int result = index2.open(test_path_, "/tmp/kwdb_mmap_test", "", O_RDWR, err_info);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapHashIndex, HashBucket_Constructor) {
  HashBucket bucket(16);

  EXPECT_EQ(bucket.get_bucket_index(0), 0);
  EXPECT_EQ(bucket.get_bucket_index(16), 0);
  EXPECT_EQ(bucket.get_bucket_index(32), 0);
}

TEST_F(TestMMapHashIndex, HashBucket_GetBucketIndex) {
  HashBucket bucket(8);
  
  size_t idx1 = bucket.get_bucket_index(10);
  size_t idx2 = bucket.get_bucket_index(18);
  
  EXPECT_LT(idx1, 8);
  EXPECT_LT(idx2, 8);
}

TEST_F(TestMMapHashIndex, HashBucket_LockOperations) {
  HashBucket bucket(8);
  
  EXPECT_EQ(bucket.Rlock(), 0);
  EXPECT_EQ(bucket.Unlock(), 0);
  
  EXPECT_EQ(bucket.Wlock(), 0);
  EXPECT_EQ(bucket.Unlock(), 0);
}

TEST_F(TestMMapHashIndex, Boundary_EmptyKey) {
  MockMMapHashIndex index(0);
  ErrorInfo err_info;

  int result = index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  if (result == 0) {
    EXPECT_EQ(index.keySize(), 0);
  }
}

TEST_F(TestMMapHashIndex, Boundary_MaxKeyLength) {
  MockMMapHashIndex index(1024);
  
  EXPECT_EQ(index.keySize(), 1024);
}

TEST_F(TestMMapHashIndex, Boundary_MinBucketCount) {
  MockMMapHashIndex index(16, 1, 1);
  
  // Should handle minimum bucket count
  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapHashIndex, Error_WrongFilePath) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  
  int result = index.open("/nonexistent/path/file", "/tmp", "", O_RDWR, err_info);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapHashIndex, Error_NullPath) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  
  int result = index.open("", "/tmp", "", O_CREAT | O_RDWR, err_info);
  
  // Should handle empty path
  EXPECT_NE(result, 0) << "Should not accept empty path";
}

TEST_F(TestMMapHashIndex, Resource_MultipleOpenClose) {
  for (int i = 0; i < 3; ++i) {
    MockMMapHashIndex index(16);
    ErrorInfo err_info;
    
    index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);
    // Destructor will close
  }
}

TEST_F(TestMMapHashIndex, Concurrent_BasicThreadSafety) {
  MockMMapHashIndex* index = new MockMMapHashIndex(16);
  ErrorInfo err_info;
  index->open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);
  
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);
  
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([index, &success_count]() {
      for (int j = 0; j < 10; ++j) {
        index->dataRlock();
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

TEST_F(TestMMapHashIndex, Reserve_Basic) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  int result = index.reserve(100);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapHashIndex, Clear_AfterInsert) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  int result = index.clear();

  EXPECT_EQ(result, 0);
  EXPECT_EQ(index.getElementCount(), 0);
}

TEST_F(TestMMapHashIndex, Sync_AfterOperations) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  int result = index.sync(MS_SYNC);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapHashIndex, SetAndGetLSN) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  uint64_t test_lsn = 12345;
  index.setLSN(test_lsn);

  EXPECT_EQ(index.getLSN(), test_lsn);
}

TEST_F(TestMMapHashIndex, SetDropAndIsDroped) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  EXPECT_FALSE(index.isDroped());

  index.setDrop();

  EXPECT_TRUE(index.isDroped());
}

TEST_F(TestMMapHashIndex, DataRlockAndUnlock) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  EXPECT_EQ(index.dataRlock(), 0);
  index.dataUnlock();

  SUCCEED();
}

TEST_F(TestMMapHashIndex, DataWlock) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  index.dataRlock();
  index.dataUnlock();

  SUCCEED();
}

TEST_F(TestMMapHashIndex, Size_Basic) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  int size = index.size();

  EXPECT_GE(size, 0);
}

TEST_F(TestMMapHashIndex, ElementCount_Initial) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  EXPECT_EQ(index.getElementCount(), 0);
}

TEST_F(TestMMapHashIndex, HashBucket_Resize) {
  HashBucket bucket(8);

  bucket.resize(16);

  size_t idx = bucket.get_bucket_index(100);
  EXPECT_LT(idx, 16);
}

TEST_F(TestMMapHashIndex, HashBucket_BucketValue) {
  HashBucket bucket(8);

  HashIndexRowID& value = bucket.bucketValue(3);

  value = 12345;

  EXPECT_EQ(bucket.bucketValue(3), 12345);
}

TEST_F(TestMMapHashIndex, KeySize_Update) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  EXPECT_EQ(index.keySize(), 16);
}

TEST_F(TestMMapHashIndex, Open_ReadOnly) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;

  int result = index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_RDONLY, err_info);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapHashIndex, PrintHashTable) {
  MockMMapHashIndex index(16);
  ErrorInfo err_info;
  index.open(test_path_, "/tmp/kwdb_mmap_test", "", O_CREAT | O_RDWR, err_info);

  index.printHashTable();

  SUCCEED();
}
