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

#include "mmap/mmap_file.h"
#include "gtest/gtest.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace kwdbts {

class TestMMapFile : public testing::Test {
 protected:
  static void SetUpTestCase() {
    mkdir("/tmp/kwdb_mmap_test", 0755);
  }

  static void TearDownTestCase() {
    system("rm -rf /tmp/kwdb_mmap_test/*");
  }

  void SetUp() override {
    test_file_path_ = "/tmp/kwdb_mmap_test/test.dat";
  }

  void TearDown() override {
    unlink(test_file_path_.c_str());
  }

  std::string test_file_path_;
};

TEST_F(TestMMapFile, Constructor_DefaultConstructor) {
  MMapFile file;

  EXPECT_EQ(file.memAddr(), nullptr);
  EXPECT_EQ(file.fileLen(), 0);
  EXPECT_GT(file.newLen(), 0);
  EXPECT_TRUE(file.filePath().empty());
  EXPECT_TRUE(file.realFilePath().empty());
}

TEST_F(TestMMapFile, Open_WithCreateFlag) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  
  EXPECT_GE(ret, 0);
  EXPECT_EQ(err_info.errcode, 0);
  EXPECT_GT(file.fileLen(), 0);
  EXPECT_NE(file.memAddr(), nullptr);
}

TEST_F(TestMMapFile, Open_WithoutCreateFlag_FileNotExists) {
  MMapFile file;
  ErrorInfo err_info;
  
  std::string non_existent_path = "/tmp/kwdb_mmap_test/non_existent.dat";
  int ret = file.open("non_existent.dat", non_existent_path, O_RDWR);
  
  EXPECT_LT(ret, 0);
}

TEST_F(TestMMapFile, FileProperties_AfterOpen) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 2048, err_info);
  ASSERT_GE(ret, 0);
  
  EXPECT_EQ(file.fileLen(), 4096);
  EXPECT_EQ(file.newLen(), 4096);
  EXPECT_EQ(file.filePath(), "test.dat");
  EXPECT_FALSE(file.readOnly());
}

TEST_F(TestMMapFile, Mremap_ExtendFileSize) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  
  size_t old_size = file.fileLen();
  ret = file.mremap(4096);
  
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(file.fileLen(), 4096);
  EXPECT_GT(file.fileLen(), old_size);
}

TEST_F(TestMMapFile, Sync_SynchronizeToFile) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  
  ret = file.sync(MS_SYNC);
  EXPECT_EQ(ret, 0);
}

TEST_F(TestMMapFile, Remove_DeleteFile) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  
  ret = file.remove();
  EXPECT_EQ(ret, 0);
}

TEST_F(TestMMapFile, ReadOnly_CheckReadOnlyFlag) {
  MMapFile file;
  ErrorInfo err_info;
  
  // Open with read-write
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  EXPECT_FALSE(file.readOnly());
  
  // Open with read-only
  MMapFile file2;
  ret = file2.open("test.dat", test_file_path_, O_RDONLY);
  if (ret >= 0) {
    EXPECT_TRUE(file2.readOnly());
  }
}

TEST_F(TestMMapFile, SetFlags_ModifyFileFlags) {
  MMapFile file;
  ErrorInfo err_info;
  
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  
  int original_flags = file.flags();
  file.setFlags(O_RDONLY);
  
  EXPECT_EQ(file.flags(), O_RDONLY);
  EXPECT_NE(file.flags(), original_flags);
}

TEST_F(TestMMapFile, CopyMember_CopyFromFile) {
  MMapFile file1;
  MMapFile file2;
  ErrorInfo err_info;
  
  int ret = file1.open("test1.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);
  
  file2.copyMember(file1);
  
  EXPECT_EQ(file2.fileLen(), file1.fileLen());
  EXPECT_EQ(file2.flags(), file1.flags());
}

TEST_F(TestMMapFile, Boundary_ZeroSizeFile) {
  MMapFile file;
  ErrorInfo err_info;

  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 0, err_info);

  EXPECT_EQ(ret, 0);
}

TEST_F(TestMMapFile, Boundary_LargeFileSize) {
  MMapFile file;
  ErrorInfo err_info;

  size_t large_size = 10 * 1024 * 1024;  // 10MB
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, large_size, err_info);

  EXPECT_GE(ret, 0);
  EXPECT_EQ(file.fileLen(), large_size);
}

TEST_F(TestMMapFile, Resize_ExtendFile) {
  MMapFile file;
  ErrorInfo err_info;

  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);

  ret = file.resize(4096);

  EXPECT_GE(ret, 0);
  EXPECT_EQ(file.newLen(), 4096);
}

TEST_F(TestMMapFile, Resize_ShrinkFile) {
  MMapFile file;
  ErrorInfo err_info;

  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 2048, err_info);
  ASSERT_GE(ret, 0);

  ret = file.resize(512);

  EXPECT_GE(ret, 0);
}

TEST_F(TestMMapFile, Rename_Basic) {
  MMapFile file;
  ErrorInfo err_info;

  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);

  std::string new_path = "/tmp/kwdb_mmap_test/renamed.dat";
  ret = file.rename(new_path);

  EXPECT_GE(ret, 0);
}

TEST_F(TestMMapFile, Munmap_CloseMapping) {
  MMapFile file;
  ErrorInfo err_info;

  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  ASSERT_GE(ret, 0);

  ret = file.munmap();

  EXPECT_GE(ret, 0);
}

TEST_F(TestMMapFile, CheckError_Basic) {
  MMapFile file;

  file.checkError();

  SUCCEED();
}

TEST_F(TestMMapFile, Flags_InitialState) {
  MMapFile file;

  EXPECT_EQ(file.flags(), 0);
}

TEST_F(TestMMapFile, Flags_AfterOpen) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  EXPECT_EQ(file.flags(), O_RDWR | O_CREAT);
}

TEST_F(TestMMapFile, MemAddr_AfterOpen) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  EXPECT_NE(file.memAddr(), nullptr);
}

TEST_F(TestMMapFile, NewLen_BeforeAndAfterResize) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);
  EXPECT_EQ(file.newLen(), 4096);

  file.resize(2048);
  EXPECT_EQ(file.newLen(), 4096);
}

TEST_F(TestMMapFile, RealFilePath_Basic) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  EXPECT_FALSE(file.realFilePath().empty());
}

TEST_F(TestMMapFile, Boundary_SinglePageSize) {
  MMapFile file;
  ErrorInfo err_info;

  size_t page_size = 4096;
  int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, page_size, err_info);

  EXPECT_GE(ret, 0);
}

TEST_F(TestMMapFile, Open_TruncateFlag) {
  {
    MMapFile file;
    ErrorInfo err_info;

    int ret = file.open("test.dat", test_file_path_, O_RDWR | O_CREAT | O_TRUNC, 1024, err_info);
    ASSERT_GE(ret, 0);
  }

  MMapFile file2;
  ErrorInfo err_info;

  int ret = file2.open("test.dat", test_file_path_, O_RDWR | O_CREAT | O_TRUNC, 2048, err_info);

  EXPECT_GE(ret, 0);
  EXPECT_EQ(file2.fileLen(), 4096);
}

TEST_F(TestMMapFile, Sync_Async) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  int ret = file.sync(MS_ASYNC);

  EXPECT_GE(ret, 0);
}

TEST_F(TestMMapFile, FilePath_Modifiable) {
  MMapFile file;
  ErrorInfo err_info;

  file.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  std::string& path = file.filePath();
  path = "modified.dat";

  EXPECT_EQ(file.filePath(), "modified.dat");
}

TEST_F(TestMMapFile, CopyMember_PreserveState) {
  MMapFile file1;
  MMapFile file2;
  ErrorInfo err_info;

  file1.open("test.dat", test_file_path_, O_RDWR | O_CREAT, 1024, err_info);

  file2.copyMember(file1);

  EXPECT_EQ(file2.newLen(), file1.newLen());
}

}  // namespace kwdbts
