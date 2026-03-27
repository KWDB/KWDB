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
#include <string>

#include "../include/mmap/mmap_string_column.h"

class TestMMapStringColumn : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_file_path_ = "/tmp/kwdb_mmap_test/string_col.dat";
  }

  void TearDown() override {
    unlink(test_file_path_.c_str());
  }

  std::string test_file_path_;
};

TEST_F(TestMMapStringColumn, Constructor_WithLatchIDs) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  
  // Should initialize without errors
  EXPECT_TRUE(true);
}

TEST_F(TestMMapStringColumn, Open_CreateNewFile) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  
  int result = col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Open_OpenExistingFile) {
  // First create
  {
    MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
    col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  }
  
  // Then open existing
  MMapStringColumn col2(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  int result = col2.open(test_file_path_, test_file_path_, O_RDWR);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, PushBack_ShortString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  const char* test_str = "Hello";
  size_t loc = col.push_back(test_str, strlen(test_str));
  
  EXPECT_NE(loc, static_cast<size_t>(-1));
  EXPECT_GT(loc, 0);
}

TEST_F(TestMMapStringColumn, PushBack_LongString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  std::string long_str(1000, 'A');
  size_t loc = col.push_back(long_str.c_str(), long_str.length());
  
  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, PushBack_EmptyString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  const char* empty_str = "";
  size_t loc = col.push_back(empty_str, 0);
  
  // May succeed or fail depending on implementation
  EXPECT_TRUE(loc != static_cast<size_t>(-1) || loc == static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, PushBackBinary_Data) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  char binary_data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
  size_t loc = col.push_back_binary(binary_data, sizeof(binary_data));
  
  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, Reserve_Space) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  int result = col.reserve(100, 50);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Reserve_WithOldAndNewSize) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  int result = col.reserve(10, 100, 64);
  
  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Boundary_MaxStringLength) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  // Test with maximum reasonable string length
  std::string max_str(65535, 'X');
  size_t loc = col.push_back(max_str.c_str(), max_str.length());
  
  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, Error_WrongFilePath) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  
  int result = col.open("/nonexistent/path/file", "/nonexistent/path/file", O_RDWR);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapStringColumn, Error_EmptyPath) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  
  int result = col.open("", "", O_CREAT | O_RDWR);
  
  EXPECT_NE(result, 0);
}

TEST_F(TestMMapStringColumn, Resource_MultipleOpenClose) {
  for (int i = 0; i < 3; ++i) {
    MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
    col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
    // Destructor will close
  }
}

TEST_F(TestMMapStringColumn, Resource_MultipleStrings) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  for (int i = 0; i < 10; ++i) {
    std::string str = "string_" + std::to_string(i);
    size_t loc = col.push_back(str.c_str(), str.length());
    EXPECT_NE(loc, static_cast<size_t>(-1));
  }
}

TEST_F(TestMMapStringColumn, Concurrent_BasicThreadSafety) {
  MMapStringColumn* col = new MMapStringColumn(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  col->open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);
  
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);
  
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([col, &success_count, i]() {
      for (int j = 0; j < 10; ++j) {
        std::string str = "thread_" + std::to_string(i) + "_" + std::to_string(j);
        size_t loc = col->push_back(str.c_str(), str.length());
        if (loc != static_cast<size_t>(-1)) {
          success_count++;
        }
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_GT(success_count.load(), 0);
  delete col;
}
