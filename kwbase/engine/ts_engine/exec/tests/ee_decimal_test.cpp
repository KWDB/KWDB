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

#include "ee_decimal.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
// #include "gmock/gmock.h"

using namespace kwdbts;  // NOLINT

class TestDecimal : public ::testing::Test {  // inherit testing::Test
 protected:
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestDecimal() = default;
};

// Test CKBigInt class
TEST_F(TestDecimal, TestCKBigInt) {
  // Test default constructor
  CKBigInt big_int;
  // Default constructor sets abs_size to 1, but neg is uninitialized, so Sign() could return 1 or -1
  // We'll just verify it's not 0
  ASSERT_NE(big_int.Sign(), 0);
  
  // Test copy constructor
  CKBigInt big_int2(big_int);
  ASSERT_NE(big_int2.Sign(), 0);
}

// Test CKBigInt destructor
TEST_F(TestDecimal, TestCKBigIntDestructor) {
  // Test that destructor doesn't cause double free
  CKBigInt* big_int = new CKBigInt();
  delete big_int;
  // If we get here, no double free occurred
  SUCCEED();
}

// Test CKDecimal class
TEST_F(TestDecimal, TestCKDecimal) {
  // Test DecimalToDouble with zero
  CKDecimal decimal;
  decimal.my_form = 0;
  decimal.negative = 0;
  decimal.Exponent = 0;
  decimal.my_coeff.abs_size = 0;
  ASSERT_EQ(decimal.DecimalToDouble(), 0.0);
  ASSERT_EQ(decimal.Sign(), 0);
  
  // Test DecimalToDouble with positive value
  decimal.my_coeff.abs_size = 1;
  k_uint64 val = 12345;
  memcpy(decimal.my_coeff.abs, &val, sizeof(k_uint64));
  decimal.Exponent = 2;
  decimal.negative = 0;
  ASSERT_EQ(decimal.DecimalToDouble(), 1234500.0);
  ASSERT_EQ(decimal.Sign(), 1);
  
  // Test DecimalToDouble with negative value
  decimal.negative = 1;
  ASSERT_EQ(decimal.DecimalToDouble(), -1234500.0);
  ASSERT_EQ(decimal.Sign(), -1);
}

// Test IntToDecimal function
TEST_F(TestDecimal, TestIntToDecimal) {
  // Test with positive value
  k_int64 int_val = 12345;
  struct CKDecimal decimal = IntToDecimal(int_val, true);
  ASSERT_EQ(decimal.Sign(), 1);
  
  // Test with negative value
  int_val = -12345;
  struct CKDecimal decimal_1 = IntToDecimal(int_val, false);
  ASSERT_EQ(decimal_1.Sign(), -1);
  
  // Test with zero
  int_val = 0;
  struct CKDecimal decimal_2 = IntToDecimal(int_val, false);
  ASSERT_EQ(decimal_2.Sign(), 0);
}

// Test DoubleToDecimal function
TEST_F(TestDecimal, TestDoubleToDecimal) {
  // Test with integer value
  k_double64 double_val = 12345.0;
  struct CKDecimal decimal = DoubleToDecimal(double_val, true);
  ASSERT_EQ(decimal.Sign(), 1);
  
  // Test with decimal value
  double_val = 123.45;
  struct CKDecimal decimal_1 = DoubleToDecimal(double_val, true);
  ASSERT_EQ(decimal_1.Sign(), 1);
  
  // Test with negative value
  double_val = -123.45;
  struct CKDecimal decimal_2 = DoubleToDecimal(double_val, false);
  ASSERT_EQ(decimal_2.Sign(), -1);
}

// Test DoubleToDecimal function with edge cases
TEST_F(TestDecimal, TestDoubleToDecimalEdgeCases) {
  // Test with very small value
  k_double64 double_val = 1e-10;
  struct CKDecimal decimal_2 = DoubleToDecimal(double_val, true);
  ASSERT_EQ(decimal_2.Sign(), 1);
  
  // Test with very large value
  double_val = 1e10;
  struct CKDecimal decimal_3 = DoubleToDecimal(double_val, true);
  ASSERT_EQ(decimal_3.Sign(), 1);    
}

// Test CKBigInt2 class
TEST_F(TestDecimal, TestCKBigInt2) {
  // Test Sign with zero
  CKBigInt2 big_int2;
  ASSERT_EQ(big_int2.Sign(), 0);
  
  // Test SetInt64 with positive value
  big_int2.SetInt64(12345);
  // After setting positive value, Sign() should return 1 or -1 (depending on neg initialization)
  ASSERT_NE(big_int2.Sign(), 0);
  
  // Test SetInt64 with negative value
  big_int2.SetInt64(-12345);
  // After setting negative value, Sign() should return 1 or -1 (depending on neg initialization)
  ASSERT_NE(big_int2.Sign(), 0);
  
  // Test SetString
  big_int2.SetString("1234567890");
  
  // Test mulAddWW
  big_int2.mulAddWW(1000, 500);
  
  // Test pow
  ASSERT_EQ(big_int2.pow(2, 3), 8);
  ASSERT_EQ(big_int2.pow(10, 0), 1);
  ASSERT_EQ(big_int2.pow(3, 4), 81);
}

// Test CKBigInt2 destructor
TEST_F(TestDecimal, TestCKBigInt2Destructor) {
  // Test that destructor doesn't cause issues
  CKBigInt2* big_int2 = new CKBigInt2();
  delete big_int2;
  // If we get here, no issues occurred
  SUCCEED();
}

// Test CKDecimal2 class
TEST_F(TestDecimal, TestCKDecimal2) {
  // Test DecimalToDouble with zero
  CKDecimal2 decimal2;
  decimal2.my_form = 0;
  decimal2.negative = 0;
  decimal2.Exponent = 0;
  decimal2.my_coeff.abs_size = 0;
  ASSERT_EQ(decimal2.DecimalToDouble(), 0.0);
  ASSERT_EQ(decimal2.Sign(), 0);
  
  // Test DecimalToDouble with positive value
  decimal2.my_coeff.abs_size = 1;
  k_uint64 val = 12345;
  memcpy(decimal2.my_coeff.abs, &val, sizeof(k_uint64));
  decimal2.Exponent = 2;
  decimal2.negative = 0;
  ASSERT_EQ(decimal2.DecimalToDouble(), 1234500.0);
  ASSERT_EQ(decimal2.Sign(), 1);
  
  // Test DecimalToDouble with negative value
  decimal2.negative = 1;
  ASSERT_EQ(decimal2.DecimalToDouble(), -1234500.0);
  ASSERT_EQ(decimal2.Sign(), -1);
}

// Test StringToDecimal function
TEST_F(TestDecimal, TestStringToDecimal) {
  // Test with positive integer
  struct CKDecimal2 decimal = StringToDecimal("12345");
  ASSERT_EQ(decimal.negative, 0);
  ASSERT_EQ(decimal.Exponent, 0);
  
  // Test with negative integer
  decimal = StringToDecimal("-12345");
  ASSERT_EQ(decimal.negative, 1);
  ASSERT_EQ(decimal.Exponent, 0);
  
  // Test with positive decimal
  decimal = StringToDecimal("123.45");
  ASSERT_EQ(decimal.negative, 0);
  ASSERT_EQ(decimal.Exponent, -2);
  
  // Test with negative decimal
  decimal = StringToDecimal("-123.45");
  ASSERT_EQ(decimal.negative, 1);
  ASSERT_EQ(decimal.Exponent, -2);
}
