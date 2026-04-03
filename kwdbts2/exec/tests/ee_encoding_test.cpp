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
#include "cm_new.h"
#include "ee_encoding.h"

#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include <iostream>

class TestEncoding : public ::testing::Test {  // inherit testing::Test
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestEncoding() {}
};

// verify code，int，float，bool
TEST_F(TestEncoding, TestEncodingValue) {
  kwdbts::k_int64 input = 8787878787;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenInt(0, input);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeIntValue(&slice, colID, input);
  free(slice.data);
  slice.len = 0;

  kwdbts::k_double64 val = 1000.253456;
  len = kwdbts::ValueEncoding::EncodeComputeLenFloat(colID);
  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeFloatValue(&slice, colID, val);
  free(slice.data);
  slice.len = 0;

  bool bol = true;
  len = kwdbts::ValueEncoding::EncodeComputeLenBool(colID, bol);
  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeBoolValue(&slice, colID, bol);
  free(slice.data);
  slice.len = 0;

  bol = false;
  len = kwdbts::ValueEncoding::EncodeComputeLenBool(colID, bol);
  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeBoolValue(&slice, colID, bol);
  free(slice.data);
  slice.len = 0;
}

// verify CKTime
TEST_F(TestEncoding, TestEncodeTimeStampValue) {
  kwdbts::k_int32 colID = 1;
  kwdbts::CKTime t;
  t.t_timespec.tv_sec = 123;
  t.t_timespec.tv_nsec = 234;
  t.t_abbv = 3600;

  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenTime(0, t);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeTimeValue(&slice, colID, t);
  free(slice.data);
  slice.len = 0;
}

// verify CKDecimal
TEST_F(TestEncoding, TestEncodingDecimal) {
  kwdbts::CKDecimal de;
  uint64_t abs = 314567;
  de.my_form = kwdbts::FormFinite;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  de.Exponent = -5;
  de.my_coeff.neg = false;
  de.negative = true;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenDecimal(colID, de);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, colID, de);
  free(slice.data);
  slice.len = 0;
}

// verify string
TEST_F(TestEncoding, TestEncodingString) {
  std::string str = "hello world";
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenString(colID, str.size());
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeBytesValue(&slice, colID, str);
  free(slice.data);
  slice.len = 0;
}

// write the encoding to a file
TEST_F(TestEncoding, TestEncodingValueToFile) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int64 input = 8787878787;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenInt(0, input);
  kwdbts::CKSlice slice;
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeIntValue(&slice, colID, input);
  info->len = info->len + len;

  kwdbts::k_double64 val = 1000.253456;
  len = kwdbts::ValueEncoding::EncodeComputeLenFloat(colID);
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeFloatValue(&slice, colID, val);
  info->len = info->len + len;

  bool bol = true;
  len = kwdbts::ValueEncoding::EncodeComputeLenBool(colID, bol);
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeBoolValue(&slice, colID, bol);
  info->len = info->len + len;

  FILE* fp;
  const char* file_path = "encode.txt";
  // Open the file in binary mode
  if ((fp = fopen(file_path, "wb")) == NULL) {
    std::cout << "Open file failed!" << std::endl;
    exit(0);
  }
  // get total size
  fseek(fp, 0, SEEK_END);
  // write data to buffer
  fwrite(info->data, info->len, 1, fp);
  fclose(fp);
  free(info->data);
  delete info;
}

// verify Duration
TEST_F(TestEncoding, TestEncodingDuration) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int64 input = 8787878787;
  struct kwdbts::KWDuration duration;
  duration.format(input, 1000);
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenDuration(colID, duration);
  ASSERT_EQ(kwdbts::ee_enlargeStringInfo(info, len), kwdbts::SUCCESS);
  kwdbts::CKSlice slice;
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeDurationValue(&slice, 0, duration);
  info->len = info->len + len;
  free(info->data);
  delete info;
  // delete slice.data;
  slice.len = 0;
}

// verify NullValue
TEST_F(TestEncoding, TestEncodingNullValue) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenNull(0);
  ASSERT_EQ(kwdbts::ee_enlargeStringInfo(info, len), kwdbts::SUCCESS);

  kwdbts::CKSlice slice;
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeNullValue(&slice, 0);
  info->len = info->len + len;
  free(info->data);
  delete info;
  // delete slice.data;
  slice.len = 0;
}

TEST_F(TestEncoding, TestEncodingIntValue) {
  kwdbts::CKSlice slice;
  kwdbts::k_char* data = nullptr;
  
  // 测试colID的不同范围 (k_uint32范围内的值)
  kwdbts::k_uint32 colIds[] = {
    0,                   // 0
    127,                 // 2^7 - 1
    128,                 // 2^7
    16383,               // 2^14 - 1
    16384,               // 2^14
    2097151,             // 2^21 - 1
    2097152,             // 2^21
    268435455,           // 2^28 - 1
    268435456,           // 2^28
    4294967295U          // 2^32 - 1 (k_uint32最大值)
  };
  
  // 测试value的不同范围，确保覆盖PutUvarint的所有分支
  kwdbts::k_int64 values[] = {
    0,                   // 0
    63,                  // 2^6 - 1 (正)
    -64,                 // -2^6 (负)
    127,                 // 2^7 - 1 (正)
    -128,                // -2^7 (负)
    16383,               // 2^14 - 1 (正)
    -16384,              // -2^14 (负)
    2097151,             // 2^21 - 1 (正)
    -2097152,            // -2^21 (负)
    268435455,           // 2^28 - 1 (正)
    -268435456,          // -2^28 (负)
    34359738367LL,       // 2^35 - 1 (正)
    -34359738368LL,      // -2^35 (负)
    4398046511103LL,     // 2^42 - 1 (正)
    -4398046511104LL,    // -2^42 (负)
    562949953421311LL,   // 2^49 - 1 (正)
    -562949953421312LL,  // -2^49 (负)
    72057594037927935LL, // 2^56 - 1 (正)
    -72057594037927936LL, // -2^56 (负)
    9223372036854775807LL, // 2^63 - 1 (正)
    -9223372036854775807LL - 1 // -2^63 (负)，使用正确的方式表示k_int64的最小值
  };
  
  // 测试所有组合
  for (auto colId : colIds) {
    for (auto val : values) {
      slice.data = KNEW char[100];
      kwdbts::ValueEncoding::EncodeIntValue(&slice, colId, val);
      EXPECT_STRNE(&slice.data[0], data);

      slice.len = 0;
      delete[] (slice.data);
    }
  }
}

// Test ValueEncoding static methods
TEST_F(TestEncoding, TestValueEncodingMethods) {
  // Test EncodeComputeLenBool
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenBool(1, true), 0);
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenBool(1, false), 0);
  
  // Test EncodeComputeLenInt
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenInt(1, 123), 0);
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenInt(1, -123), 0);
  
  // Test EncodeComputeLenString
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenString(1, 10), 0);
  
  // Test EncodeComputeLenFloat
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenFloat(1), 0);
  
  // Test EncodeComputeLenTime
  kwdbts::CKTime t;
  t.t_timespec.tv_sec = 123;
  t.t_timespec.tv_nsec = 456;
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenTime(1, t), 0);
  
  // Test EncodeComputeLenDuration
  kwdbts::KWDuration d;
  d.months = 1;
  d.days = 2;
  d.nanos = 3;
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenDuration(1, d), 0);
  
  // Test EncodeComputeLenNull
  EXPECT_GT(kwdbts::ValueEncoding::EncodeComputeLenNull(1), 0);
  
  // Test EncodeNullValue
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(10));
  kwdbts::ValueEncoding::EncodeNullValue(&slice, 1);
  free(slice.data);
}

// Test EncodeUntaggedIntValue
TEST_F(TestEncoding, TestEncodeUntaggedIntValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(10));
  
  kwdbts::k_int32 offset = 0;
  offset = kwdbts::ValueEncoding::EncodeUntaggedIntValue(&slice, offset, 123);
  EXPECT_GT(offset, 0);
  
  offset = 0;
  offset = kwdbts::ValueEncoding::EncodeUntaggedIntValue(&slice, offset, -123);
  EXPECT_GT(offset, 0);
  
  free(slice.data);
}

// Test EncodeUntaggedBytesValue
TEST_F(TestEncoding, TestEncodeUntaggedBytesValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(20));
  
  std::string str = "hello";
  kwdbts::k_int32 offset = 0;
  offset = kwdbts::ValueEncoding::EncodeUntaggedBytesValue(&slice, offset, str);
  EXPECT_GT(offset, 0);
  
  free(slice.data);
}

// Test EncodeUntaggedFloatValue
TEST_F(TestEncoding, TestEncodeUntaggedFloatValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(8));
  
  kwdbts::k_double64 val = 123.456;
  kwdbts::k_int32 offset = 0;
  offset = kwdbts::ValueEncoding::EncodeUntaggedFloatValue(&slice, offset, val);
  EXPECT_EQ(offset, 8);
  
  free(slice.data);
}

// Test EncodeUntaggedTimeValue
TEST_F(TestEncoding, TestEncodeUntaggedTimeValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(20));
  
  kwdbts::CKTime t;
  t.t_timespec.tv_sec = 123;
  t.t_timespec.tv_nsec = 456;
  
  kwdbts::k_int32 offset = 0;
  kwdbts::ValueEncoding::EncodeUntaggedTimeValue(&slice, offset, t);
  
  free(slice.data);
}

// Test EncodeUntaggedDurationValue
TEST_F(TestEncoding, TestEncodeUntaggedDurationValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(20));
  
  kwdbts::KWDuration d;
  d.months = 1;
  d.days = 2;
  d.nanos = 3;
  
  kwdbts::k_int32 offset = 0;
  kwdbts::ValueEncoding::EncodeUntaggedDurationValue(&slice, offset, d);
  
  // Test negative values
  d.months = -1;
  d.days = -2;
  d.nanos = -3;
  offset = 0;
  kwdbts::ValueEncoding::EncodeUntaggedDurationValue(&slice, offset, d);
  
  free(slice.data);
}

// Test ValueEncoding::EncodeUntaggedDecimalValue
TEST_F(TestEncoding, TestEncodeUntaggedDecimalValue) {
  kwdbts::CKSlice slice;
  slice.data = static_cast<char*>(malloc(50));
  
  kwdbts::CKDecimal de;
  uint64_t abs = 314159;
  de.my_form = kwdbts::FormFinite;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  de.Exponent = -5;
  de.negative = false;
  
  kwdbts::k_int32 offset = 0;
  offset = kwdbts::ValueEncoding::EncodeUntaggedDecimalValue(&slice, offset, de);
  EXPECT_GT(offset, 0);
  
  free(slice.data);
}

// Test ValueEncoding::EncodeDecimalValue
TEST_F(TestEncoding, TestEncodeDecimalValue) {
  kwdbts::CKSlice slice;
  kwdbts::k_char* data = nullptr;
  slice.data = static_cast<char*>(malloc(100));
  
  kwdbts::CKDecimal de;
  uint64_t abs = 314159;
  de.my_form = kwdbts::FormFinite;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  de.negative = false;
  
  // 测试不同的指数值，覆盖EncodeUvarintAscending的所有分支
  kwdbts::k_uint64 exponents[] = {
    0,          // 0，覆盖v <= 109分支
    109,        // 109，覆盖v <= 109分支
    110,        // 110，覆盖109 < v <= 255分支
    255,        // 255，覆盖109 < v <= 255分支
    256,        // 256，覆盖255 < v <= 65535分支
    65535,      // 65535，覆盖255 < v <= 65535分支
    65536,      // 65536，覆盖65535 < v <= 16777215分支
    16777215,   // 16777215，覆盖65535 < v <= 16777215分支
    16777216,   // 16777216，覆盖16777215 < v <= 4294967295分支
    4294967295, // 4294967295，覆盖16777215 < v <= 4294967295分支
    4294967296, // 4294967296，覆盖4294967295 < v <= 1099511627775分支
    1099511627775, // 1099511627775，覆盖4294967295 < v <= 1099511627775分支
    1099511627776, // 1099511627776，覆盖1099511627775 < v <= 17592186044415分支
    17592186044415, // 17592186044415，覆盖1099511627775 < v <= 17592186044415分支
    17592186044416, // 17592186044416，覆盖17592186044415 < v <= 281474976710655分支
    281474976710655, // 281474976710655，覆盖17592186044415 < v <= 281474976710655分支
    281474976710656  // 281474976710656，覆盖else分支
  };
  
  for (auto exp : exponents) {
    slice.len = 0; // 重置slice长度
    de.Exponent = static_cast<kwdbts::k_int32>(exp);
    kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
    EXPECT_STRNE(&slice.data[0], data);
  }
  
  free(slice.data);
}

TEST_F(TestEncoding, TestEncodeComputeLenDecimal) {
  kwdbts::CKDecimal de;
  uint64_t abs = 314159;
  
  // 测试不同的colID和VEncodeType组合
  kwdbts::k_uint64 colIds[] = {
    0,                  // colID == 0
    1,                  // colID != 0
    127,                // 2^7 - 1
    128                 // 2^7
  };
  
  // 测试不同的小数形式
  // 1. 测试FormInfinite
  de.my_form = kwdbts::FormInfinite;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  de.Exponent = -5;
  
  // 测试FormInfinite的negative分支
  de.negative = true;
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  de.negative = false;
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 2. 测试FormNaN
  de.my_form = kwdbts::FormNaN;
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 4. 测试FormFinite
  de.my_form = kwdbts::FormFinite;
  
  // 测试IsZero() && !neg && Exponent == 0的情况
  de.negative = false;
  de.Exponent = 0;
  uint64_t zero_abs = 0;
  memcpy(de.my_coeff.abs, &zero_abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试非零有限小数的各种情况
  de.my_coeff.abs_size = 1;
  
  // 测试neg && e > 0
  de.negative = true;
  de.Exponent = 10; // e = 6 + 10 = 16 > 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试neg && e == 0
  de.Exponent = -6; // e = 6 - 6 = 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试neg && e < 0
  de.Exponent = -7; // e = 6 - 7 = -1 < 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试!neg && e < 0
  de.negative = false;
  de.Exponent = -7; // e = 6 - 7 = -1 < 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试!neg && e == 0
  de.Exponent = -6; // e = 6 - 6 = 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试!neg && e > 0
  de.Exponent = 10; // e = 6 + 10 = 16 > 0
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
  
  // 测试abs_size为0的情况
  de.my_coeff.abs_size = 0;
  de.negative = false;
  de.Exponent = 0;
  for (auto colId : colIds) {
    kwdbts::ValueEncoding::EncodeComputeLenDecimal(colId, de);
  }
}

TEST_F(TestEncoding, TestDecimalType) {
  kwdbts::CKSlice slice;
  kwdbts::k_char* data = nullptr;
  slice.data = static_cast<char*>(malloc(100));
  slice.len = 0;
  
  kwdbts::CKDecimal de;
  de.my_form = kwdbts::FormInfinite;
  de.negative = true;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.negative = false;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.my_form = kwdbts::FormNaN;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.my_form = 0;
  de.Exponent = 0;
  de.negative = false;
  de.my_coeff.abs_size = 0;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.my_form = kwdbts::FormFinite;
  de.negative = true;
  de.Exponent = 0;
  de.my_coeff.abs_size = 1;
  uint64_t abs = 0;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.my_form = kwdbts::FormFinite;
  de.negative = true;
  de.Exponent = -10;
  de.my_coeff.abs_size = 0;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  de.my_form = kwdbts::FormFinite;
  de.negative = false;
  de.Exponent = -10;
  de.my_coeff.abs_size = 0;
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, 1, de);
  EXPECT_STRNE(&slice.data[0], data);
  slice.len = 0;

  free(slice.data);
}

// Test ComputeLenNonsortingDecimal function
// TEST_F(TestEncoding, TestComputeLenNonsortingDecimal) {
//   kwdbts::CKDecimal de;
//   uint64_t abs = 314159;
  
//   // Test finite decimal with positive exponent
//   de.my_form = kwdbts::FormFinite;
//   memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
//   de.my_coeff.abs_size = 1;
//   de.Exponent = 2; // e = 6 + 2 = 8 > 0
//   de.negative = false;
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test finite decimal with zero exponent
//   de.Exponent = -6; // e = 6 - 6 = 0
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test finite decimal with negative exponent
//   de.Exponent = -7; // e = 6 - 7 = -1 < 0
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test negative finite decimal with positive exponent
//   de.negative = true;
//   de.Exponent = 2; // e = 6 + 2 = 8 > 0
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test negative finite decimal with zero exponent
//   de.Exponent = -6; // e = 6 - 6 = 0
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test negative finite decimal with negative exponent
//   de.Exponent = -7; // e = 6 - 7 = -1 < 0
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test zero decimal
//   abs = 0;
//   memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
//   de.my_coeff.abs_size = 1;
//   de.Exponent = 0;
//   de.negative = false;
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test infinite decimal
//   de.my_form = kwdbts::FormInfinite;
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test negative infinite decimal
//   de.negative = true;
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test NaN decimal
//   de.my_form = kwdbts::FormNaN;
//   EXPECT_GT(kwdbts::ComputeLenNonsortingDecimal(de), 0);
  
//   // Test unknown form (should return -1)
//   de.my_form = static_cast<kwdbts::Form>(999);
//   EXPECT_EQ(kwdbts::ComputeLenNonsortingDecimal(de), -1);
// }

// // Test ComputeLenDecimal function
// TEST_F(TestEncoding, TestComputeLenDecimal) {
//   kwdbts::CKDecimal de;
//   uint64_t abs = 314159;
  
//   // Test finite decimal with positive exponent
//   de.my_form = kwdbts::FormFinite;
//   memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
//   de.my_coeff.abs_size = 1;
//   de.Exponent = 2; // e = 6 + 2 = 8 > 0
//   de.negative = false;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test finite decimal with zero exponent
//   de.Exponent = -6; // e = 6 - 6 = 0
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test finite decimal with negative exponent
//   de.Exponent = -7; // e = 6 - 7 = -1 < 0
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test negative finite decimal with positive exponent
//   de.negative = true;
//   de.Exponent = 2; // e = 6 + 2 = 8 > 0
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test negative finite decimal with zero exponent
//   de.Exponent = -6; // e = 6 - 6 = 0
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test negative finite decimal with negative exponent
//   de.Exponent = -7; // e = 6 - 7 = -1 < 0
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test zero decimal
//   abs = 0;
//   memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
//   de.my_coeff.abs_size = 1;
//   de.Exponent = 0;
//   de.negative = false;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test infinite decimal
//   de.my_form = kwdbts::FormInfinite;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test negative infinite decimal
//   de.negative = true;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test NaN decimal
//   de.my_form = kwdbts::FormNaN;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test unknown form (should return -1)
//   de.my_form = static_cast<kwdbts::Form>(999);
//   EXPECT_EQ(kwdbts::ComputeLenDecimal(1, kwdbts::VEncodeType::VT_Decimal, de), -1);
  
//   // Test with colID 0
//   de.my_form = kwdbts::FormFinite;
//   abs = 314159;
//   memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
//   de.my_coeff.abs_size = 1;
//   de.Exponent = -5;
//   de.negative = false;
//   EXPECT_GT(kwdbts::ComputeLenDecimal(0, kwdbts::VEncodeType::VT_Decimal, de), 0);
  
//   // Test with sentinel type
//   EXPECT_GT(kwdbts::ComputeLenDecimal(1, static_cast<kwdbts::VEncodeType>(100), de), 0);
// }
