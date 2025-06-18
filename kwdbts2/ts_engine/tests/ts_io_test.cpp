// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_io.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <random>
#include <string_view>
#include <thread>

#include "kwdb_type.h"
#include "libkwdbts2.h"

using namespace kwdbts;  // NOLINT
TEST(MMAP, ReadWrite) {
  std::filesystem::remove("test");
  TsMMapFile* f = new TsMMapFile("test", false);
  f->Append("12345");
  f->Append("12345");
  std::string long_string(10000, 31);
  f->Append(long_string);
  f->Sync();
  delete f;

  auto f2 = new TsMMapFile("test", true);
  f2->MarkDelete();

  char buf[64];
  TSSlice result;
  f2->Read(3, 2, &result, buf);
  ASSERT_TRUE(memcmp(result.data, "45", result.len) == 0);
  delete f2;

  ASSERT_FALSE(std::filesystem::exists("test"));
}

TEST(MMapIOV2, Write) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  std::unique_ptr<TsAppendOnlyFile> wfile;
  std::string filename = "append1";
  auto s = env->NewAppendOnlyFile(filename, &wfile);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);

  EXPECT_EQ(wfile->Append("12345"), SUCCESS);
  EXPECT_EQ(wfile->Append("54321"), SUCCESS);
  EXPECT_EQ(wfile->Append("1"), SUCCESS);
  EXPECT_EQ(wfile->GetFileSize(), 11);
  EXPECT_EQ(wfile->Sync(), SUCCESS);
  EXPECT_EQ(wfile->Close(), SUCCESS);
  wfile.reset();

  // read and check
  ASSERT_TRUE(std::filesystem::exists(filename));
  std::unique_ptr<TsRandomReadFile> rfile;
  s = env->NewRandomReadFile(filename, &rfile);
  ASSERT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->GetFileSize(), 11);
  TSSlice result;
  ASSERT_EQ(rfile->Read(0, rfile->GetFileSize(), &result, nullptr), SUCCESS);
  std::string_view sv{result.data, result.len};
  EXPECT_EQ(sv, "12345543211");

  // reopen and overwrite the file;
  s = env->NewAppendOnlyFile(filename, &wfile);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);

  EXPECT_EQ(wfile->Append("abcde"), SUCCESS);
  EXPECT_EQ(wfile->Append("ABCDE"), SUCCESS);
  EXPECT_EQ(wfile->Append("EDCBA"), SUCCESS);
  EXPECT_EQ(wfile->GetFileSize(), 15);
  wfile.reset();

  ASSERT_TRUE(std::filesystem::exists(filename));
  s = env->NewRandomReadFile(filename, &rfile);
  ASSERT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->GetFileSize(), 15);
  ASSERT_EQ(rfile->Read(0, rfile->GetFileSize(), &result, nullptr), SUCCESS);
  sv = std::string_view{result.data, result.len};
  EXPECT_EQ(sv, "abcdeABCDEEDCBA");

  // reopen but not overwrite the file, append from the end;
  s = env->NewAppendOnlyFile(filename, &wfile, false);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);

  EXPECT_EQ(wfile->Append("qwer"), SUCCESS);
  EXPECT_EQ(wfile->Append("asdf"), SUCCESS);
  EXPECT_EQ(wfile->Append("zxcv"), SUCCESS);
  EXPECT_EQ(wfile->GetFileSize(), 27);
  wfile.reset();

  ASSERT_TRUE(std::filesystem::exists(filename));
  s = env->NewRandomReadFile(filename, &rfile);
  ASSERT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->GetFileSize(), 27);
  ASSERT_EQ(rfile->Read(0, rfile->GetFileSize(), &result, nullptr), SUCCESS);
  sv = std::string_view{result.data, result.len};
  EXPECT_EQ(sv, "abcdeABCDEEDCBAqwerasdfzxcv");

  // reopen but not overwrite the file, append from offset = 6;
  s = env->NewAppendOnlyFile(filename, &wfile, false, 6);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);

  EXPECT_EQ(wfile->Append("test"), SUCCESS);
  EXPECT_EQ(wfile->Append("TEST"), SUCCESS);
  EXPECT_EQ(wfile->Append("TeSt"), SUCCESS);
  EXPECT_EQ(wfile->GetFileSize(), 18);
  wfile.reset();

  ASSERT_TRUE(std::filesystem::exists(filename));
  s = env->NewRandomReadFile(filename, &rfile);
  ASSERT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->GetFileSize(), 18);
  ASSERT_EQ(rfile->Read(0, rfile->GetFileSize(), &result, nullptr), SUCCESS);
  sv = std::string_view{result.data, result.len};
  EXPECT_EQ(sv, "abcdeAtestTESTTeSt");

  std::filesystem::remove(filename);
  // write large data

  s = env->NewAppendOnlyFile(filename, &wfile);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);

  for (size_t i = 0; i < 65536; ++i) {
    char buf[64];
    std::snprintf(buf, 64, "DATADATATESTTEST%016lu", i);
    ASSERT_EQ(wfile->Append(buf), SUCCESS);
  }
  size_t filesize = wfile->GetFileSize();
  wfile.reset();
  EXPECT_EQ(std::filesystem::file_size(filename), filesize);

  ASSERT_TRUE(std::filesystem::exists(filename));
  s = env->NewRandomReadFile(filename, &rfile);
  ASSERT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->GetFileSize(), std::filesystem::file_size(filename));
  for (int i = 0; i < 65536; ++i) {
    int offset = 32 * i;
    ASSERT_EQ(rfile->Prefetch(offset, 32), SUCCESS);
    ASSERT_EQ(rfile->Read(offset, 32, &result, nullptr), SUCCESS);
    sv = std::string_view{result.data, result.len};
    char buf[64];
    std::snprintf(buf, 64, "DATADATATESTTEST%016d", i);
    EXPECT_EQ(sv, buf);
  }
  sv = std::string_view{result.data, result.len};
  std::filesystem::remove(filename);
}

TEST(MMapIOV2, FailedCases) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();

  // open a non-exist file
  std::string filename = "FOOO";
  ASSERT_FALSE(std::filesystem::exists(filename));
  std::unique_ptr<TsRandomReadFile> rfile;
  auto s = env->NewRandomReadFile(filename, &rfile);
  EXPECT_EQ(s, FAIL);

  {
    std::unique_ptr<TsAppendOnlyFile> wfile;
    EXPECT_EQ(env->NewAppendOnlyFile(filename, &wfile), SUCCESS);
    EXPECT_EQ(wfile->Append("0123456789"), SUCCESS);
  }
  s = env->NewRandomReadFile(filename, &rfile);
  EXPECT_EQ(s, SUCCESS);
  EXPECT_EQ(rfile->Prefetch(1000, 1000), FAIL);
  EXPECT_EQ(rfile->Prefetch(5, 1000), SUCCESS);

  TSSlice result;
  EXPECT_EQ(rfile->Read(10, 1000, &result, nullptr), FAIL);
  EXPECT_EQ(rfile->Read(9, 1000, &result, nullptr), SUCCESS);
  std::string_view sv{result.data, result.len};
  EXPECT_EQ(sv, "9");
  std::filesystem::remove(filename);
}

TEST(MMapIOV2, ReadAfterAllocate) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  char block[4096];
  for (int i = 0; i < 4096; ++i) {
    block[i] = i & 0xff;
  }
  TSSlice slice;
  slice.data = block;
  slice.len = 4096;
  std::string filepath = "test";
  std::unique_ptr<TsAppendOnlyFile> wfile;
  auto s = env->NewAppendOnlyFile(filepath, &wfile);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(wfile, nullptr);
  wfile->Append(slice);
  wfile->Sync();

  std::unique_ptr<TsRandomReadFile> rfile;
  s = env->NewRandomReadFile(filepath, &rfile, 4096);
  ASSERT_EQ(s, SUCCESS);
  ASSERT_NE(rfile, nullptr);
  for (int i = 0; i < 10000; ++i) {
    wfile->Append(slice);
  }
  TSSlice result;
  rfile->Read(0, 4096, &result, nullptr);
  for (int i = 0; i < 4096; ++i) {
    ASSERT_EQ(result.data[i], block[i]);
  }
  std::filesystem::remove(filepath);
}

TEST(MMapIOV2, ConcurrentReadWrite) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  std::atomic<size_t> file_size{0};
  std::atomic_bool finished{false};
  std::atomic_bool start{false};
  std::string filename = "concurrent_test";
  std::filesystem::remove(filename);
  auto write_thread = [&](int iblock) {
    std::default_random_engine drng(iblock);
    char block[4096];
    for (int i = 0; i < 4096; ++i) {
      block[i] = (drng() + i) & 0xff;
    }
    TSSlice slice;
    slice.data = block;
    slice.len = 4096;

    std::unique_ptr<TsAppendOnlyFile> wfile;
    auto s = env->NewAppendOnlyFile(filename, &wfile, false);
    start.store(true);
    ASSERT_EQ(s, SUCCESS);
    ASSERT_NE(wfile, nullptr);

    ASSERT_EQ(wfile->Append(slice), SUCCESS);
    ASSERT_EQ(wfile->Sync(), SUCCESS);
    file_size.fetch_add(4096);
  };

  auto read_thread = [&]() {
    while (finished.load() == false) {
      std::unique_ptr<TsRandomReadFile> rfile;
      auto fsize = file_size.load();
      if (fsize == 0) continue;
      ASSERT_EQ(fsize % 4096, 0);
      while (start.load() == false) {
        std::this_thread::yield();
      }
      ASSERT_NE(fsize, 0);
      auto s = env->NewRandomReadFile(filename, &rfile, fsize);
      ASSERT_EQ(s, SUCCESS);
      s = rfile->Prefetch(0, fsize);
      ASSERT_EQ(s, SUCCESS);
      TSSlice result;
      s = rfile->Read(0, fsize, &result, nullptr);
      ASSERT_EQ(s, SUCCESS);
      ASSERT_EQ(result.len, fsize);

      ASSERT_EQ(fsize % 4096, 0);
      int nblocks = fsize / 4096;

      const char* p = result.data;
      for (int iblock = 0; iblock < nblocks; ++iblock) {
        std::default_random_engine drng(iblock);
        for (int i = 0; i < 4096; ++i) {
          char expected = (drng() + i) & 0xff;
          ASSERT_EQ(p[i], expected) << "at block " << iblock << " " << i;
        }
        p += 4096;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back(read_thread);
  }
  for (int i = 0; i < 10000; ++i) {
    write_thread(i);
  }
  finished.store(true);
  for (auto& t : threads) {
    t.join();
  }
  std::filesystem::remove(filename);
}

TEST(MMAP, TsMMapAllocFiletest) {
  std::filesystem::remove("test");
  TsMMapAllocFile* f = new TsMMapAllocFile("test");
  f->Open();
  std::vector<uint64_t> alloc_offsets;
  for (size_t i = 0; i < 100; i++) {
    auto offset = f->AllocateAssigned(10000, 2 + i);
    ASSERT_TRUE(offset != 0);
    alloc_offsets.push_back(offset);
  }
  auto cur_file_size = f->getHeader()->file_len;
  auto cur_alloc_offset = f->getHeader()->alloc_offset;
  for (size_t i = 0; i < alloc_offsets.size(); i++) {
    char* addr = f->GetAddrForOffset(alloc_offsets[i], 1);
    uint8_t fill = 2 + i;
    ASSERT_EQ((uint8_t)(*addr), fill);
  }
  delete f;
  f = new TsMMapAllocFile("test");
  f->Open();
  for (size_t i = 0; i < alloc_offsets.size(); i++) {
    char* addr = f->GetAddrForOffset(alloc_offsets[i], 1);
    uint8_t fill = 2 + i;
    ASSERT_EQ((uint8_t)(*addr), fill);
  }
  ASSERT_TRUE(f->getHeader()->file_len == cur_file_size);
  ASSERT_TRUE(f->getHeader()->alloc_offset == cur_alloc_offset);
  delete f;
}
