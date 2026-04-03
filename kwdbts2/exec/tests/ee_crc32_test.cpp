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

#include "ee_crc32.h"
#include "ee_string.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestCRC32 : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestCRC32, TestCRC32MathFunc) {
  const char* crc32msg = "abc123";
  size_t len = 6;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(crc32msg, len);
  EXPECT_EQ(icode32, 26154185);
  icode32 = kwdbts::kwdb_crc32_ieee(crc32msg, len);
  EXPECT_EQ(icode32, 3473062748);
}

TEST_F(TestCRC32, TestCRC32EmptyString) {
  const char* empty_str = "";
  size_t len = 0;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(empty_str, len);
  EXPECT_EQ(icode32, 0);
  icode32 = kwdbts::kwdb_crc32_ieee(empty_str, len);
  EXPECT_EQ(icode32, 0);
}

TEST_F(TestCRC32, TestCRC32SingleCharacter) {
  const char* single_char = "a";
  size_t len = 1;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(single_char, len);
  EXPECT_EQ(icode32, 3251651376);
  icode32 = kwdbts::kwdb_crc32_ieee(single_char, len);
  EXPECT_EQ(icode32, 3904355907);
}

TEST_F(TestCRC32, TestCRC32LongString) {
  const char* long_str = "This is a long string for CRC32 testing purposes";
  size_t len = 45;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(long_str, len);
  EXPECT_EQ(icode32, 3779522402);
  icode32 = kwdbts::kwdb_crc32_ieee(long_str, len);
  EXPECT_EQ(icode32, 3981844085);
}

TEST_F(TestCRC32, TestCRC32SpecialCharacters) {
  const char* special_chars = "!@#$%^&*()_+";
  size_t len = 12;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(special_chars, len);
  EXPECT_EQ(icode32, 4011793583);
  icode32 = kwdbts::kwdb_crc32_ieee(special_chars, len);
  EXPECT_EQ(icode32, 235097688);
}

TEST_F(TestCRC32, TestCRC32NumbersOnly) {
  const char* numbers = "1234567890";
  size_t len = 10;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(numbers, len);
  EXPECT_EQ(icode32, 4091270398);
  icode32 = kwdbts::kwdb_crc32_ieee(numbers, len);
  EXPECT_EQ(icode32, 639479525);
}

TEST_F(TestCRC32, TestCRC32MixedCase) {
  const char* mixed_case = "AbCdEfGhIjKlMnOpQrStUvWxYz";
  size_t len = 26;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(mixed_case, len);
  EXPECT_EQ(icode32, 68808751);
  icode32 = kwdbts::kwdb_crc32_ieee(mixed_case, len);
  EXPECT_EQ(icode32, 1277644989);
}

TEST_F(TestCRC32, TestCRC32UnicodeChars) {
  const char* unicode_str = "Hello 世界";
  size_t len = 9; // 注意：这里只计算字节长度，不考虑 Unicode 字符数
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(unicode_str, len);
  EXPECT_EQ(icode32, 388030596);
  icode32 = kwdbts::kwdb_crc32_ieee(unicode_str, len);
  EXPECT_EQ(icode32, 2320651618);
}

}  // namespace kwdbts
