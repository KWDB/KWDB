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

#include <gtest/gtest.h>
#include <filesystem>


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

TEST(MMAP, TsMMapAllocFiletest) {
  std::filesystem::remove("test");
  TsMMapAllocFile* f = new TsMMapAllocFile("test");

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
  for (size_t i = 0; i < alloc_offsets.size(); i++) {
    char* addr = f->GetAddrForOffset(alloc_offsets[i], 1);
    uint8_t fill = 2 + i;
    ASSERT_EQ((uint8_t)(*addr), fill);
  }
  ASSERT_TRUE(f->getHeader()->file_len == cur_file_size);
  ASSERT_TRUE(f->getHeader()->alloc_offset == cur_alloc_offset);
  delete f;
}
