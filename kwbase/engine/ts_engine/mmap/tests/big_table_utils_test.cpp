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
#include "utils/big_table_utils.h"

using namespace std;
using namespace kwdbts;

class BigTableUtilsTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BigTableUtilsTest, getTsFilePath) {
  EXPECT_EQ(getTsFilePath("//a/b/c"), "a/b/c");
  EXPECT_EQ(getTsFilePath("/a/b/c"), "a/b/c");
  EXPECT_EQ(getTsFilePath("a/b/c"), "a/b/c");
  EXPECT_EQ(getTsFilePath("///test"), "test");
}

TEST_F(BigTableUtilsTest, getTsObjectName) {
  EXPECT_EQ(getTsObjectName("file.ts"), "file");
  EXPECT_EQ(getTsObjectName("/path/file.bt"), "path/file");
  EXPECT_EQ(getTsObjectName("nofilename"), "nofilename");
}

TEST_F(BigTableUtilsTest, nameToEntityBigTablePath) {
  EXPECT_EQ(nameToEntityBigTablePath("test", ".bt"), "test.bt");
  EXPECT_EQ(nameToEntityBigTablePath("test.bt"), "test.bt");
}

TEST_F(BigTableUtilsTest, normalizePath) {
  EXPECT_EQ(normalizePath("//a//b/./c"), "/a/b/c/");
  EXPECT_EQ(normalizePath("a/b//c/"), "a/b/c/");
}

TEST_F(BigTableUtilsTest, isInteger) {
  int64_t val;
  EXPECT_TRUE(isInteger("123", val));
  EXPECT_TRUE(isInteger("-456", val));
  EXPECT_TRUE(isInteger("+789", val));
  EXPECT_FALSE(isInteger("abc", val));
  EXPECT_FALSE(isInteger("12a3", val));
  EXPECT_FALSE(isInteger(nullptr, val));
}

TEST_F(BigTableUtilsTest, setInteger) {
  int n = 0;
  EXPECT_EQ(setInteger(n, "10", 0, 100), 0);
  EXPECT_EQ(n, 10);

  EXPECT_EQ(setInteger(n, "-1", 0, 100), -1);
  EXPECT_EQ(setInteger(n, "200", 0, 100), -1);
  EXPECT_EQ(setInteger(n, "abc", 0, 100), -1);
}

TEST_F(BigTableUtilsTest, getDataTypeSize_by_type) {
  EXPECT_EQ(getDataTypeSize(BOOL), sizeof(bool));
  EXPECT_EQ(getDataTypeSize(INT8), sizeof(int8_t));
  EXPECT_EQ(getDataTypeSize(INT16), sizeof(int16_t));
  EXPECT_EQ(getDataTypeSize(INT32), sizeof(int32_t));
  EXPECT_EQ(getDataTypeSize(INT64), sizeof(int64_t));
  EXPECT_EQ(getDataTypeSize(FLOAT), sizeof(float));
  EXPECT_EQ(getDataTypeSize(DOUBLE), sizeof(double));
  EXPECT_EQ(getDataTypeSize(STRING), sizeof(IDTYPE));
  EXPECT_EQ(getDataTypeSize(VARSTRING), sizeof(intptr_t));
}

TEST_F(BigTableUtilsTest, getDataTypeSize_by_attr) {
  AttributeInfo info{};

  info.type = INT32;
  EXPECT_EQ(getDataTypeSize(info), sizeof(int32_t));

  info.type = CHAR;
  info.max_len = 10;
  EXPECT_EQ(getDataTypeSize(info), 10);

  info.type = STRING;
  info.max_len = 0;
  EXPECT_EQ(getDataTypeSize(info), sizeof(IDTYPE));
}

TEST_F(BigTableUtilsTest, setAttributeInfo) {
  vector<AttributeInfo> infos(2);
  infos[0].type = INT32;
  infos[1].type = INT64;

  int total = setAttributeInfo(infos);

  EXPECT_EQ(infos[0].offset, 0);
  EXPECT_EQ(infos[0].size, 4);
  EXPECT_EQ(infos[1].offset, 4);
  EXPECT_EQ(infos[1].size, 8);
  EXPECT_EQ(total, 12);
}

TEST_F(BigTableUtilsTest, toString) {
  EXPECT_EQ(toString("hello"), "hello");
  EXPECT_EQ(toString(nullptr), "");
}

TEST_F(BigTableUtilsTest, toString_with_len) {
  EXPECT_EQ(toString("hello\0world", 5), "hello");
  EXPECT_EQ(toString("test", 4), "test");
}

TEST_F(BigTableUtilsTest, getDummySchema) {
  auto& s = getDummySchema();
  EXPECT_TRUE(s.empty());
}

TEST_F(BigTableUtilsTest, null_bitmap) {
  unsigned char bm[8] = {0};

  set_null_bitmap(bm, 3);
  EXPECT_EQ(get_null_bitmap(bm, 3), 1 << 3);

  unset_null_bitmap(bm, 3);
  EXPECT_EQ(get_null_bitmap(bm, 3), 0);
}

TEST_F(BigTableUtilsTest, string_convert) {
  EXPECT_EQ(stringToInt("123"), 123);
  EXPECT_EQ(intToString(456), "456");
}