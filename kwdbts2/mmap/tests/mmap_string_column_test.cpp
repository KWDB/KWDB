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

  EXPECT_TRUE(col.memAddr() == nullptr);
  EXPECT_EQ(col.fileLen(), 0);
}

TEST_F(TestMMapStringColumn, Open_CreateNewFile) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);

  int result = col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR);

  ASSERT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Open_OpenExistingFile) {
  {
    MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
    ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);
  }

  MMapStringColumn col2(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  int result = col2.open(test_file_path_, test_file_path_, O_RDWR);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, PushBack_ShortString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* test_str = "Hello";
  size_t loc = col.push_back(test_str, strlen(test_str));

  EXPECT_NE(loc, static_cast<size_t>(-1));
  EXPECT_GE(loc, static_cast<size_t>(MMapStringColumn::startLoc()));
}

TEST_F(TestMMapStringColumn, PushBack_LongString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  std::string long_str(1000, 'A');
  size_t loc = col.push_back(long_str.c_str(), long_str.length());

  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, PushBack_EmptyString) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* empty_str = "";
  size_t loc = col.push_back(empty_str, 0);

  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, PushBackBinary_Data) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  char binary_data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
  size_t loc = col.push_back_binary(binary_data, sizeof(binary_data));

  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, PushBackHexBinary_Data) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  char binary_data[] = {0xDE, 0xAD, 0xBE, 0xEF};
  size_t loc = col.push_back_hexbinary(binary_data, sizeof(binary_data));

  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, Reserve_Space) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  int result = col.reserve(100, 50);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Reserve_WithOldAndNewSize) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  int result = col.reserve(10, 100, 64);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Boundary_MaxStringLength) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

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
    ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);
  }
}

TEST_F(TestMMapStringColumn, Resource_MultipleStrings) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  for (int i = 0; i < 10; ++i) {
    std::string str = "string_" + std::to_string(i);
    size_t loc = col.push_back(str.c_str(), str.length());
    EXPECT_NE(loc, static_cast<size_t>(-1));
  }
}

TEST_F(TestMMapStringColumn, Sync_Basic) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* test_str = "Hello";
  col.push_back(test_str, strlen(test_str));

  int result = col.sync(MS_SYNC);

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, MemAddr_AfterOpen) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  EXPECT_NE(col.memAddr(), nullptr);
}

TEST_F(TestMMapStringColumn, FileLen_AfterOpen) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  EXPECT_GT(col.fileLen(), 0);
}

TEST_F(TestMMapStringColumn, GetStringAddr_AfterPush) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* test_str = "TestString";
  size_t loc = col.push_back(test_str, strlen(test_str));

  char* addr = col.getStringAddr(loc);

  EXPECT_NE(addr, nullptr);
}

TEST_F(TestMMapStringColumn, Size_AfterPush) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  size_t initial_size = col.size();

  const char* test_str = "Hello";
  col.push_back(test_str, strlen(test_str));

  EXPECT_GT(col.size(), initial_size);
}

TEST_F(TestMMapStringColumn, Trim_AfterPush) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* test_str = "TrimMe";
  size_t loc = col.push_back(test_str, strlen(test_str));

  int result = col.trim(loc);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapStringColumn, Remove_File) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  int result = col.remove();

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Munmap_Basic) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  int result = col.munmap();

  EXPECT_EQ(result, 0);
}

TEST_F(TestMMapStringColumn, Rename_Basic) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  std::string new_path = "/tmp/kwdb_mmap_test/renamed_string.dat";
  int result = col.rename(new_path);

  EXPECT_GE(result, 0);
}

TEST_F(TestMMapStringColumn, RealFilePath_AfterOpen) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  EXPECT_FALSE(col.realFilePath().empty());
}

TEST_F(TestMMapStringColumn, MutexLock_Operations) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  col.mutexLock();
  col.mutexUnlock();

  SUCCEED();
}

TEST_F(TestMMapStringColumn, RwLock_Operations) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  col.rdLock();
  col.unLock();

  col.wrLock();
  col.unLock();

  SUCCEED();
}

TEST_F(TestMMapStringColumn, PushBackNoLock_Basic) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  const char* test_str = "NoLockTest";
  size_t loc = col.push_back_nolock(test_str, strlen(test_str));

  EXPECT_NE(loc, static_cast<size_t>(-1));
}

TEST_F(TestMMapStringColumn, StrFile_GetReference) {
  MMapStringColumn col(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
  ASSERT_EQ(col.open(test_file_path_, test_file_path_, O_CREAT | O_RDWR), 0);

  MMapFile& file = col.strFile();

  EXPECT_NE(file.memAddr(), nullptr);
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
