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

#include <gtest/gtest.h>
#include "ts_utils.h"

namespace kwdbts {

class TestUtils : public ::testing::Test {
 protected:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}
};

TEST(KWDBFunctions, Bextr) {
    uint64_t input = 0xF123456789ABCDE;
    byte start = 56;
    byte len = 8;
    uint32_t expected = 0x0F;

    // Get from low position
    uint32_t result = bextr(input, start, len);
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, Bextr2) {
    uint64_t input = 0x123456789ABCDEF;
    byte start = 4;
    byte len = 8;
    uint32_t expected = 0xDE;

    // Get from low position
    uint32_t result = bextr(input, start, len);
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, Bextr32) {
  uint32_t input = 0x12345678;
  byte start = 4;
  byte len = 8;
  uint32_t expected = 0x67;

  uint32_t result = bextr32(input, start, len);
  EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, EncodeHash) {
    uint64_t x = 0xF123456789ABCDE;
    byte p = 4, pp = 8;
    uint32_t result = kwdbts::encodeHash(x, p, pp);

    uint32_t expected = 0x1E;
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, DecodeHash) {
  uint32_t k = 0x1E;
  byte p = 4, pp = 8;
  auto result = kwdbts::decodeHash(k, p, pp);

  uint32_t expectedIndex = 0;
  byte expectedResult = 1;
  EXPECT_EQ(result.first, expectedIndex);
  EXPECT_EQ(result.second, expectedResult);
}

TEST(KWDBFunctions, EncodeHash2) {
    uint64_t x = 0x123456789ABCDEF;
    byte p = 4, pp = 8;
    uint32_t result = kwdbts::encodeHash(x, p, pp);

    uint32_t expected = 0x02;
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, DecodeHash2) {
  uint32_t k = 0x02;
  byte p = 4, pp = 8;
  auto result = kwdbts::decodeHash(k, p, pp);

  uint32_t expectedIndex = 0;
  byte expectedResult = 4;
  EXPECT_EQ(result.first, expectedIndex);
  EXPECT_EQ(result.second, expectedResult);
}

TEST(KWDBFunctions, PutUint64LittleEndian) {
    std::vector<byte> buffer;
    uint64_t val = 0xF123456789ABCDE;
    bool success = kwdbts::PutUint64LittleEndian(&buffer, val);
    EXPECT_TRUE(success);
    EXPECT_EQ(buffer.size(), 8);
    EXPECT_EQ(buffer, std::vector<byte>({0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12, 0x0F}));
}

TEST(KWDBFunctions, PutUint64LittleEndian2) {
    std::vector<byte> buffer;
    uint64_t val = 0x123456789ABCDEF;
    bool success = kwdbts::PutUint64LittleEndian(&buffer, val);
    EXPECT_TRUE(success);
    EXPECT_EQ(buffer.size(), 8);
    EXPECT_EQ(buffer, std::vector<byte>({0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01}));
}

TEST(KWDBFunctions, RotateRight) {
    uint64_t v = 0x123456789ABCDEF;
    unsigned int k = 4;
    uint64_t result = rotate_right(v, k);
    uint64_t expected = 0xF0123456789ABCDE;
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, RotateRight2) {
    uint64_t v = 0xF123456789ABCDE;
    unsigned int k = 4;
    uint64_t result = rotate_right(v, k);
    uint64_t expected = 0xE0F123456789ABCD;
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, Uint64FromBytes) {
    std::vector<byte> buffer = {0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01};
    uint64_t expected = 0x123456789ABCDEF;
    uint64_t result = uint64_from_bytes(buffer, 0);
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, Uint32FromBytes) {
    std::vector<byte> buffer = {0x67, 0x45, 0x23, 0xF1};
    uint64_t expected = 0xF1234567;
    uint64_t result = uint32_from_bytes(buffer, 0);
    EXPECT_EQ(result, expected);
}

TEST(KWDBFunctions, Uint16FromBytes) {
    std::vector<byte> buffer = {0x34, 0x12};
    uint64_t expected = 0x1234;
    uint64_t result = uint16_from_bytes(buffer, 0);
    EXPECT_EQ(result, expected);
}

TEST(TestUtils, testHash64) {
    std::vector<byte> buf;
    uint64_t hash = 0;

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(1));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 17760762384136597300U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(2));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 712574109729348829U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(3));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 11369381369180892191U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(4));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 2480255666032686445U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(5));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 10527935691117287462U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(6));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 4394461812288986596U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(7));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 2187079352818243175U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(8));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 9448573243880564272U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(9));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 7973676079474108436U);

    PutUint64LittleEndian(&buf, static_cast<u_int64_t>(10));
    hash = Hash64(buf, 1337);
    EXPECT_EQ(hash, 1936381698930858804U);
}

TEST(KWDBFunctions, MarshalBinaryByte) {
    std::vector<byte> input = {0x01, 0x02, 0x03, 0x04};
    std::vector<byte> expected = {0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04};
    // Converts a byte vector to another byte vector containing size information.
    auto result = kwdbts::marshalBinary_Byte(input);
    ASSERT_EQ(result, expected);
}

TEST(TestUtils, MarshalBinarySet) {
    std::unordered_set<uint32_t> input = {1, 2, 3};
    std::vector<byte> result = kwdbts::marshalBinary_Set(input);

    EXPECT_EQ(result[0], 0x00);
    EXPECT_EQ(result[1], 0x00);
    EXPECT_EQ(result[2], 0x00);
    EXPECT_EQ(result[3], 0x03);
    EXPECT_EQ(result[4], 0x00);
    EXPECT_EQ(result[5], 0x00);
    EXPECT_EQ(result[6], 0x00);
    EXPECT_EQ(result[8], 0x00);
    EXPECT_EQ(result[9], 0x00);
    EXPECT_EQ(result[10], 0x00);
    EXPECT_EQ(result[12], 0x00);
    EXPECT_EQ(result[13], 0x00);
    EXPECT_EQ(result[14], 0x00);
}

TEST(TestUtils, getIndex) {
  uint32_t k = 0x1233;
  byte p = 24, pp = 8;

  uint32_t expected = 0x12;
  // Gets the number of the bucket accord to the hash value
  auto result = getIndex(k, p, pp);
  EXPECT_EQ(result, expected);
}

TEST(TestUtils, getPosVal) {
  uint64_t k = 17760762384136597300U;
  byte p = 14;
  auto result = kwdbts::getPosVal(k, p);

  uint32_t expectedIndex = 15774;
  byte expectedResult = 1;
  EXPECT_EQ(result.first, expectedIndex);
  EXPECT_EQ(result.second, expectedResult);
}

TEST(TestUtils, EncodeFloatAscending) {
  k_float64 k = 0.1;
  std::vector<uint8_t> b;
  auto result = kwdbts::EncodeFloatAscending(b, k);

  std::vector<byte> expected = {5, 63, 185, 153, 153, 153, 153, 153, 154};
  EXPECT_EQ(result, expected);
}

TEST(TestUtils, EncodeStringAscending) {
  KString k = "abc";
  std::vector<uint8_t> b;
  auto result = kwdbts::EncodeStringAscending(b, k);

  std::vector<byte> expected = {18, 97, 98, 99, 0, 1};
  EXPECT_EQ(result, expected);
}

}  // namespace kwdbts
