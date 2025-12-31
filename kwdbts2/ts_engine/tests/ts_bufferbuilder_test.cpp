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

#include "ts_bufferbuilder.h"

#include <gtest/gtest.h>

#include <random>

TEST(TsBufferBuilder, Test1) {
  kwdbts::TsBufferBuilder builder;
  builder.resize(10);
  EXPECT_TRUE(std::all_of(builder.begin(), builder.end(), [](char c) { return c == 0; }));
  builder.resize(20);
  EXPECT_TRUE(std::all_of(builder.begin(), builder.end(), [](char c) { return c == 0; }));
}

TEST(TsBufferBuilder, Test2) {
  kwdbts::TsBufferBuilder builder;
  builder.reserve(100);
  builder.resize(50);
  ASSERT_EQ(builder.end() - builder.begin(), 50);
  ASSERT_EQ(builder.capacity(), 100);
  ASSERT_EQ(builder.size(), 50);
  EXPECT_TRUE(std::all_of(builder.begin(), builder.end(), [](char c) { return c == 0; }));
}

TEST(TsBufferBuilder, WriteTest) {
  char data[1023];
  std::default_random_engine drng(0);
  std::uniform_int_distribution<int> udist(0, 255);
  for (int i = 0; i < 1023; ++i) {
    data[i] = static_cast<char>(udist(drng));
  }
  kwdbts::TsBufferBuilder builder;
  builder.append(data, 1023);
  ASSERT_EQ(builder.size(), 1023);
  ASSERT_EQ(builder.capacity(), 1024);
  while (builder.size() < (64U << 20)) {
    builder.append(data, 1023);
    uint32_t sz = builder.size();
    size_t cap = builder.capacity();
    if (sz < 32U << 20) {
      ASSERT_NE(sz, 0U);
      size_t exp_cap = 1U << (32 - __builtin_clz(sz));
      EXPECT_EQ(builder.capacity(), exp_cap);
    } else {
      EXPECT_EQ(cap & ((1ULL << 20) - 1), 0);
      ASSERT_GE(cap, sz);
      ASSERT_LT(cap - sz, 1U << 20);
    }
  }
}