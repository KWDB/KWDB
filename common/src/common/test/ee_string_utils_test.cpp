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


#include "ee_string_utils.h"

#include "gtest/gtest.h"

class TestStringutils : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestStringutils, TestStringutils) {
  EXPECT_EQ(kwdbts::isASCII('a'), true);
  EXPECT_EQ(kwdbts::isASCII('1'), true);
  EXPECT_EQ(kwdbts::isASCII('_'), true);
  EXPECT_EQ(kwdbts::isASCII('\t'), true);

  EXPECT_EQ(kwdbts::isAlphaASCII('a'), true);
  EXPECT_EQ(kwdbts::isAlphaASCII('1'), false);
  EXPECT_EQ(kwdbts::isAlphaASCII('_'), false);
  EXPECT_EQ(kwdbts::isAlphaASCII('\t'), false);

  EXPECT_EQ(kwdbts::isNumericASCII('a'), false);
  EXPECT_EQ(kwdbts::isNumericASCII('1'), true);
  EXPECT_EQ(kwdbts::isNumericASCII('_'), false);
  EXPECT_EQ(kwdbts::isNumericASCII('\t'), false);

  EXPECT_EQ(kwdbts::isAlphaNumericASCII('a'), true);
  EXPECT_EQ(kwdbts::isAlphaNumericASCII('1'), true);
  EXPECT_EQ(kwdbts::isAlphaNumericASCII('_'), false);
  EXPECT_EQ(kwdbts::isAlphaNumericASCII('\t'), false);

  EXPECT_EQ(kwdbts::isWordCharASCII('a'), true);
  EXPECT_EQ(kwdbts::isWordCharASCII('1'), true);
  EXPECT_EQ(kwdbts::isWordCharASCII('_'), true);
  EXPECT_EQ(kwdbts::isWordCharASCII('\t'), false);

  EXPECT_EQ(kwdbts::isWhitespaceASCII('a'), false);
  EXPECT_EQ(kwdbts::isWhitespaceASCII('1'), false);
  EXPECT_EQ(kwdbts::isWhitespaceASCII('_'), false);
  EXPECT_EQ(kwdbts::isWhitespaceASCII('\t'), true);
}

TEST_F(TestStringutils, TestSkipWhitespacesUTF8) {
  char a[] = "\t1\t23";
  const char* pos = kwdbts::skipWhitespacesUTF8(&a[0], &a[strlen(a) - 1]);
  EXPECT_EQ(pos, &a[1]);
}
