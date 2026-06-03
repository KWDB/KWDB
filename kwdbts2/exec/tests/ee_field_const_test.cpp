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

#include "ee_field_const.h"

#include "ee_global.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestFieldConst : public testing::Test {
 protected:
  static void SetUpTestCase() {
  }

  static void TearDownTestCase() {
  }
};

TEST_F(TestFieldConst, TestFieldConstIntFunc) {
  k_int64 int_type = 1;
  FieldConstInt *field = new FieldConstInt(roachpb::DataType::BIGINT, int_type, sizeof(k_int64));
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestFieldConstIntAllMethods) {
  k_int64 int_type = 12345;
  FieldConstInt *field = new FieldConstInt(roachpb::DataType::BIGINT, int_type, sizeof(k_int64));
  
  // Test ValInt()
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 12345);
  
  // Test ValInt(k_char *ptr)
  k_int64 test_value = 98765;
  k_char *ptr = reinterpret_cast<k_char *>(&test_value);
  value = field->ValInt(ptr);
  EXPECT_EQ(value, 98765);
  
  // Test with negative value
  test_value = -12345;
  ptr = reinterpret_cast<k_char *>(&test_value);
  value = field->ValInt(ptr);
  EXPECT_EQ(value, -12345);
  
  // Test ValReal()
  k_double64 real_value = field->ValReal();
  EXPECT_DOUBLE_EQ(real_value, 12345.0);
  
  // Test ValReal(k_char *ptr)
  test_value = 98765;
  ptr = reinterpret_cast<k_char *>(&test_value);
  real_value = field->ValReal(ptr);
  EXPECT_DOUBLE_EQ(real_value, 98765.0);
  
  // Test get_ptr(RowBatch *batch)
  char *get_ptr_result = field->get_ptr(nullptr);
  EXPECT_NE(get_ptr_result, nullptr);
  k_int64 *value_ptr = reinterpret_cast<k_int64 *>(get_ptr_result);
  EXPECT_EQ(*value_ptr, 12345);
  
  // Test ValStr()
  String str = field->ValStr();
  EXPECT_TRUE(str.ptr_ != nullptr);
  EXPECT_STREQ(str.ptr_, "12345");
  
  // Test ValStr(k_char *ptr)
  test_value = 98765;
  ptr = reinterpret_cast<k_char *>(&test_value);
  str = field->ValStr(ptr);
  EXPECT_TRUE(str.ptr_ != nullptr);
  EXPECT_STREQ(str.ptr_, "98765");
  
  // Test field_to_copy()
  Field *copied_field = field->field_to_copy();
  EXPECT_NE(copied_field, nullptr);
  EXPECT_EQ(copied_field->ValInt(), 12345);
  SafeDeletePointer(copied_field);
  
  // Test fill_template_field(char *ptr)
  k_int64 template_value = 0;
  k_char *template_ptr = reinterpret_cast<k_char *>(&template_value);
  k_bool result = field->fill_template_field(template_ptr);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(template_value, 12345);
  
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestFieldConstDoubleAllMethods) {
  k_double64 double_type = 123.45;
  FieldConstDouble *field = new FieldConstDouble(roachpb::DataType::DOUBLE, double_type);
  
  // Test ValInt()
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 123);
  
  // Test ValInt(k_char *ptr)
  k_double64 test_value = 987.65;
  k_char *ptr = reinterpret_cast<k_char *>(&test_value);
  value = field->ValInt(ptr);
  EXPECT_EQ(value, 987);
  
  // Test ValReal()
  k_double64 real_value = field->ValReal();
  EXPECT_DOUBLE_EQ(real_value, 123.45);
  
  // Test ValReal(k_char *ptr)
  test_value = 987.65;
  ptr = reinterpret_cast<k_char *>(&test_value);
  real_value = field->ValReal(ptr);
  EXPECT_DOUBLE_EQ(real_value, 987.65);
  
  // Test get_ptr(RowBatch *batch)
  char *get_ptr_result = field->get_ptr(nullptr);
  EXPECT_NE(get_ptr_result, nullptr);
  k_double64 *value_ptr = reinterpret_cast<k_double64 *>(get_ptr_result);
  EXPECT_DOUBLE_EQ(*value_ptr, 123.45);
  
  // Test ValStr()
  String str = field->ValStr();
  EXPECT_TRUE(str.ptr_ != nullptr);
  
  // Test ValStr(k_char *ptr)
  test_value = 987.65;
  ptr = reinterpret_cast<k_char *>(&test_value);
  str = field->ValStr(ptr);
  EXPECT_TRUE(str.ptr_ != nullptr);
  
  // Test field_to_copy()
  Field *copied_field = field->field_to_copy();
  EXPECT_NE(copied_field, nullptr);
  EXPECT_EQ(copied_field->ValInt(), 123);
  SafeDeletePointer(copied_field);
  
  // Test fill_template_field(char *ptr)
  k_double64 template_value = 0.0;
  k_char *template_ptr = reinterpret_cast<k_char *>(&template_value);
  k_bool result = field->fill_template_field(template_ptr);
  EXPECT_EQ(result, 0);
  EXPECT_DOUBLE_EQ(template_value, 123.45);
  
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestFieldConstIntervalAllMethods) {
  KString interval_str("5d");
  FieldConstInterval *field = new FieldConstInterval(roachpb::DataType::TIMESTAMP, interval_str);
  
  // Test ValInt()
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 432000000); // 5 days in milliseconds
  
  // Test ValInt(k_char *ptr)
  value = field->ValInt(nullptr);
  EXPECT_EQ(value, 0);
  
  // Test ValInt(k_int64 *val, k_bool negative)
  k_int64 test_value = 1365781921080;
  value = field->ValInt(&test_value, KTRUE);
  EXPECT_EQ(value, 1365349921080); // Subtract 5 days
  
  // Test ValReal()
  k_double64 real_value = field->ValReal();
  EXPECT_DOUBLE_EQ(real_value, 0.0);
  
  // Test ValReal(k_char *ptr)
  real_value = field->ValReal(nullptr);
  EXPECT_DOUBLE_EQ(real_value, 0.0);
  
  // Test get_ptr(RowBatch *batch)
  char *get_ptr_result = field->get_ptr(nullptr);
  EXPECT_NE(get_ptr_result, nullptr);
  
  // Test ValStr()
  String str = field->ValStr();
  EXPECT_TRUE(str.ptr_ != nullptr);
  
  // Test ValStr(k_char *ptr)
  str = field->ValStr(nullptr);
  EXPECT_TRUE(str.ptr_ != nullptr);
  
  // Test field_to_copy()
  Field *copied_field = field->field_to_copy();
  EXPECT_NE(copied_field, nullptr);
  SafeDeletePointer(copied_field);
  
  // Test fill_template_field(char *ptr)
  k_int64 template_value = 0;
  k_char *template_ptr = reinterpret_cast<k_char *>(&template_value);
  k_bool result = field->fill_template_field(template_ptr);
  EXPECT_EQ(result, 0);
  
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestFieldConstStringAllMethods) {
  KString string_value("test_string");
  FieldConstString *field = new FieldConstString(roachpb::DataType::VARCHAR, string_value);
  
  // Test ValInt()
  k_int64 value = field->ValInt();
  // String to int conversion, should be 0 if not a number
  
  // Test ValInt(k_char *ptr)
  KString test_string("12345");
  k_char *ptr = const_cast<k_char *>(test_string.c_str());
  value = field->ValInt(ptr);
  // String to int conversion
  
  // Test ValReal()
  k_double64 real_value = field->ValReal();
  // String to double conversion, should be 0.0 if not a number
  
  // Test ValReal(k_char *ptr)
  test_string = "123.45";
  ptr = const_cast<k_char *>(test_string.c_str());
  real_value = field->ValReal(ptr);
  // String to double conversion
  
  // Test get_ptr(RowBatch *batch)
  char *get_ptr_result = field->get_ptr(nullptr);
  EXPECT_NE(get_ptr_result, nullptr);
  
  // Test ValStr()
  String str = field->ValStr();
  EXPECT_TRUE(str.ptr_ != nullptr);
  EXPECT_STREQ(str.ptr_, "test_string");
  
  // Test ValStr(k_char *ptr)
  test_string = "test_ptr";
  ptr = const_cast<k_char *>(test_string.c_str());
  str = field->ValStr(ptr);
  EXPECT_TRUE(str.ptr_ != nullptr);
  
  // Test field_to_copy()
  Field *copied_field = field->field_to_copy();
  EXPECT_NE(copied_field, nullptr);
  SafeDeletePointer(copied_field);
  
  // Test fill_template_field(char *ptr)
  char template_buffer[20] = {0};
  k_bool result = field->fill_template_field(template_buffer);
  EXPECT_EQ(result, 0);
  EXPECT_STREQ(template_buffer, "test_string");
  
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestGetYMDFormTimestamp) {
  // Test with valid date format
  KString date_str("2023-04-01");
  k_int32 start_pos = 0;
  k_int32 year, month, day;
  
  KStatus status = getYMDFormTimestamp(&date_str, &start_pos, &year, &month, &day);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_EQ(year, 123); // 2023 - 1900
  EXPECT_EQ(month, 3);   // 0-based month
  EXPECT_EQ(day, 1);     // day of month
  EXPECT_EQ(start_pos, 10); // Position after parsing
  
  // Test with no dash (should use default values)
  KString no_dash_str("20230401");
  start_pos = 0;
  status = getYMDFormTimestamp(&no_dash_str, &start_pos, &year, &month, &day);
  EXPECT_EQ(status, KStatus::SUCCESS);
  EXPECT_EQ(year, 70);    // Default year
  EXPECT_EQ(month, 0);     // Default month
  EXPECT_EQ(day, 1);       // Default day
}

TEST_F(TestFieldConst, TestFieldConstDateFunc) {
  KString str_type("2013-04-12");
  FieldConstDate *field = new FieldConstDate(roachpb::DataType::DATE, str_type, 0);
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 1365724800000);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1365724800000.0);
  SafeDeletePointer(field);
}
}  // namespace kwdbts
