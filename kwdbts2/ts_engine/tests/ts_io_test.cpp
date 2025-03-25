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

#include "ts_slice.h"
using namespace kwdbts;  // NOLINT
TEST(MMAP, ReadWrite) {
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
