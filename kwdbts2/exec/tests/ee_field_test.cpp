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

#include "ee_field.h"

#include "ee_field_const.h"
#include "ee_field_func_math.h"
#include "ee_field_func_string.h"
#include "ee_field_typecast.h"
#include "ee_global.h"
#include "ee_processors.h"
#include "gtest/gtest.h"

namespace kwdbts {

typedef struct FieldMathTest {
  KString func_name;
  k_int64 expect_int;
  k_double64 expect_real;
} FieldMathTest;

typedef struct FieldStringTest {
  KString func_name;
  KString expect_val;
} FieldStringTest;

const FieldMathTest math_tests[] = {
    {.func_name = "sin", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "cos", .expect_int = 1, .expect_real = 1.0},
    {.func_name = "tan", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "asin", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "acos", .expect_int = 1, .expect_real = 1.5707963267948966},
    {.func_name = "atan", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sqrt", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "round", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "abs", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "ceil", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "floor", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "isnan", .expect_int = 0, .expect_real = 0.0},
    // {.func_name = "ln", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "radians", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sign", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "trunc", .expect_int = 0, .expect_real = 0.0},
    // {.func_name = "cot", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sign", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "cbrt", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "exp", .expect_int = 1, .expect_real = 1.0},
    {.func_name = "degrees", .expect_int = 0, .expect_real = 0.0},
};

const FieldStringTest string_tests[] = {
  {.func_name = "concat", .expect_val = "a12a12"},
  {.func_name = "substr", .expect_val = "a12"},
  {.func_name = "lpad", .expect_val = ""},
  {.func_name = "rpad", .expect_val = ""},
  {.func_name = "ltrim", .expect_val = ""},
  {.func_name = "rtrim", .expect_val = ""},
  {.func_name = "left", .expect_val = ""},
  {.func_name = "right", .expect_val = ""},
  // {.func_name = "upper", .expect_val = "A12"},
  // {.func_name = "lower", .expect_val = "a12"},

};
class BaseField : public FieldNum {
 public:
  using FieldNum::FieldNum;
  k_int64 ValInt() { return 0; }
  k_int64 ValInt(char *ptr) { return 0; }

  k_double64 ValReal() { return 0.0; }
  k_double64 ValReal(char *ptr) { return 0.0; }
  String ValStr() {
    String s(3);
    snprintf(s.ptr_, 3 + 1, "%s", "a12");
    s.length_ = strlen(s.ptr_);
    return s;
  }
  String ValStr(char *ptr) {
    String s(3);
    snprintf(s.ptr_, 3 + 1, "%s", "a12");
    s.length_ = strlen(s.ptr_);
    return s;
  }
  Field *field_to_copy() { return new BaseField(*this); }

  k_int64 ValInt(k_int64 *val, k_bool negative) { return 0; }
  k_bool fill_template_field(char *ptr) { return 0; }
  char *get_ptr() { return nullptr; }
};

class BatchStringField : public FieldConstString {
 public:
  BatchStringField(roachpb::DataType datatype, const KString &str)
      : FieldConstString(datatype, str) {}

  String ValStr(char *ptr) override {
    return String(ptr, strlen(ptr), false);
  }
};

class TestFieldIterator : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;
};

TEST_F(TestFieldIterator, TypeCastRoundsFloatAndRejectsOverflow) {
  FieldConstDouble positive(roachpb::DataType::DOUBLE, 1.9);
  FieldTypeCastSigned<k_int64> positive_cast(&positive);
  EXPECT_EQ(positive_cast.ValInt(), 2);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(positive_cast.get_ptr(nullptr)), 2);

  FieldConstDouble negative(roachpb::DataType::DOUBLE, -1.9);
  FieldTypeCastSigned<k_int64> negative_cast(&negative);
  EXPECT_EQ(negative_cast.ValInt(), -2);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(negative_cast.get_ptr(nullptr)), -2);

  EEPgErrorInfo::ResetPgErrorInfo();
  FieldConstDouble overflow(roachpb::DataType::DOUBLE, 1e20);
  FieldTypeCastSigned<k_int64> overflow_cast(&overflow);
  EXPECT_EQ(overflow_cast.ValInt(), KStatus::FAIL);
  EXPECT_TRUE(EEPgErrorInfo::IsError());
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code,
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
  EEPgErrorInfo::ResetPgErrorInfo();
}

TEST_F(TestFieldIterator, TypeCastBoolHandlesFloatAndStringInputs) {
  FieldConstDouble positive(roachpb::DataType::DOUBLE, 0.5);
  FieldTypeCastBool positive_cast(&positive);
  EXPECT_EQ(positive_cast.ValInt(), 1);
  EXPECT_TRUE(*reinterpret_cast<k_bool *>(positive_cast.get_ptr(nullptr)));

  FieldConstDouble negative(roachpb::DataType::DOUBLE, -0.5);
  FieldTypeCastBool negative_cast(&negative);
  EXPECT_EQ(negative_cast.ValInt(), 1);
  EXPECT_TRUE(*reinterpret_cast<k_bool *>(negative_cast.get_ptr(nullptr)));

  FieldConstString mixed_case(roachpb::DataType::VARCHAR, "True");
  FieldTypeCastBool mixed_case_cast(&mixed_case);
  EXPECT_EQ(mixed_case_cast.ValInt(), 1);

  BatchStringField short_form(roachpb::DataType::VARCHAR, "t");
  FieldTypeCastBool short_form_cast(&short_form);
  EXPECT_TRUE(*reinterpret_cast<k_bool *>(short_form_cast.get_ptr(nullptr)));

  BatchStringField false_value(roachpb::DataType::VARCHAR, "FALSE");
  FieldTypeCastBool false_cast(&false_value);
  EXPECT_FALSE(*reinterpret_cast<k_bool *>(false_cast.get_ptr(nullptr)));
}

TEST_F(TestFieldIterator, TypeCastStringTimestampUsesOutputPrecision) {
  constexpr k_int64 kExpectedMillis = 1767250800000;
  constexpr k_int64 kExpectedMillisWithTimezone = 1767222000000;

  FieldConstString constant(roachpb::DataType::VARCHAR,
                            "2026-01-01 07:00:00");
  FieldTypeCastTimestampTz constant_cast(&constant, 3, 0);
  EXPECT_EQ(constant_cast.ValInt(), kExpectedMillis);

  BatchStringField batch(roachpb::DataType::VARCHAR,
                         "2026-01-01 07:00:00");
  FieldTypeCastTimestampTz batch_cast(&batch, 3, 0);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(batch_cast.get_ptr(nullptr)),
            kExpectedMillis);

  FieldConstString with_timezone(roachpb::DataType::VARCHAR,
                                 "2026-01-01 07:00:00+08:00");
  FieldTypeCastTimestampTz timezone_cast(&with_timezone, 3, 0);
  EXPECT_EQ(timezone_cast.ValInt(), kExpectedMillisWithTimezone);
}

TEST_F(TestFieldIterator, TestMathFunc) {
  std::list<Field *> args;
  args.push_back(KNEW BaseField());
  Field *field;
  size_t len = sizeof(math_tests) / sizeof(math_tests[0]);
  for (k_int32 j = 0; j < len; j++) {
    for (k_int32 i = 0; i < mathFuncBuiltinsNum1; i++) {
      if (mathFuncBuiltins1[i].name == math_tests[j].func_name) {
        field = KNEW FieldFuncMath(args, mathFuncBuiltins1[i]);
        break;
      }
    }
    EXPECT_EQ(field->ValInt(), math_tests[j].expect_int)
        << math_tests[j].func_name;
    EXPECT_DOUBLE_EQ(field->ValReal(), math_tests[j].expect_real)
        << math_tests[j].func_name;
    SafeDeletePointer(field);
  }
  // auto *field_sum = KNEW FieldFuncMath(args, mathFuncBuiltins1[0]);

  // BaseOperator *noop_iter = NewIterator<NoopIterator>(ok_iter_);
  for (auto a : args) {
    SafeDeletePointer(a);
  }
}

TEST_F(TestFieldIterator, TestStringFunc) {
  std::list<Field *> args;
  args.push_back(KNEW BaseField());
  args.push_back(KNEW BaseField());

  Field *field;
  size_t len = sizeof(string_tests) / sizeof(string_tests[0]);
  for (k_int32 j = 0; j < len; j++) {
    field = KNEW FieldFuncString(string_tests[j].func_name, args);

    EXPECT_EQ(field->ValStr().ptr_, string_tests[j].expect_val)
        << string_tests[j].func_name;
    SafeDeletePointer(field);
  }
  // auto *field_sum = KNEW FieldFuncMath(args, mathFuncBuiltins1[0]);

  // BaseOperator *noop_iter = NewIterator<NoopIterator>(ok_iter_);
  for (auto a : args) {
    SafeDeletePointer(a);
  }
}

// Test FieldNum
// TEST_F(TestFieldIterator, TestFieldNum) {
//   FieldNum field_num(0, roachpb::DataType::INT, sizeof(k_int32));
  
//   // Test basic properties
//   EXPECT_EQ(field_num.get_storage_type(), roachpb::DataType::INT);
//   EXPECT_EQ(field_num.get_storage_length(), sizeof(k_int32));
  
//   // Test field_to_copy
//   Field *copied = field_num.field_to_copy();
//   EXPECT_NE(copied, nullptr);
//   SafeDeletePointer(copied);
// }

// Test FieldChar
TEST_F(TestFieldIterator, TestFieldChar) {
  FieldChar field_char(0, roachpb::DataType::CHAR, 10);
  
  // Test basic properties
  EXPECT_EQ(field_char.get_storage_type(), roachpb::DataType::CHAR);
  EXPECT_EQ(field_char.get_storage_length(), 10);
  
  // Test field_to_copy
  Field *copied = field_char.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldNchar
TEST_F(TestFieldIterator, TestFieldNchar) {
  FieldNchar field_nchar(0, roachpb::DataType::NCHAR, 10);
  
  // Test basic properties
  EXPECT_EQ(field_nchar.get_storage_type(), roachpb::DataType::NCHAR);
  EXPECT_EQ(field_nchar.get_storage_length(), 10);
  
  // Test field_to_copy
  Field *copied = field_nchar.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldBool
TEST_F(TestFieldIterator, TestFieldBool) {
  FieldBool field_bool(0, roachpb::DataType::BOOL, sizeof(k_bool));
  
  // Test basic properties
  EXPECT_EQ(field_bool.get_storage_type(), roachpb::DataType::BOOL);
  EXPECT_EQ(field_bool.get_storage_length(), sizeof(k_bool));
  
  // Test field_to_copy
  Field *copied = field_bool.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldShort
TEST_F(TestFieldIterator, TestFieldShort) {
  FieldShort field_short(0, roachpb::DataType::SMALLINT, sizeof(k_int16));
  
  // Test basic properties
  EXPECT_EQ(field_short.get_storage_type(), roachpb::DataType::SMALLINT);
  EXPECT_EQ(field_short.get_storage_length(), sizeof(k_int16));
  
  // Test field_to_copy
  Field *copied = field_short.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldInt
TEST_F(TestFieldIterator, TestFieldInt) {
  FieldInt field_int(0, roachpb::DataType::INT, sizeof(k_int32));
  
  // Test basic properties
  EXPECT_EQ(field_int.get_storage_type(), roachpb::DataType::INT);
  EXPECT_EQ(field_int.get_storage_length(), sizeof(k_int32));
  
  // Test field_to_copy
  Field *copied = field_int.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldLonglong
TEST_F(TestFieldIterator, TestFieldLonglong) {
  FieldLonglong field_longlong(0, roachpb::DataType::BIGINT, sizeof(k_int64));
  
  // Test basic properties
  EXPECT_EQ(field_longlong.get_storage_type(), roachpb::DataType::BIGINT);
  EXPECT_EQ(field_longlong.get_storage_length(), sizeof(k_int64));
  
  // Test field_to_copy
  Field *copied = field_longlong.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldTimestampTZ
TEST_F(TestFieldIterator, TestFieldTimestampTZ) {
  FieldTimestampTZ field_timestamp_tz(0, roachpb::DataType::TIMESTAMPTZ, sizeof(k_int64));
  
  // Test basic properties
  EXPECT_EQ(field_timestamp_tz.get_storage_type(), roachpb::DataType::TIMESTAMPTZ);
  EXPECT_EQ(field_timestamp_tz.get_storage_length(), sizeof(k_int64));
  
  // Test field_to_copy
  Field *copied = field_timestamp_tz.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldFloat
TEST_F(TestFieldIterator, TestFieldFloat) {
  FieldFloat field_float(0, roachpb::DataType::FLOAT, sizeof(k_float32));
  
  // Test basic properties
  EXPECT_EQ(field_float.get_storage_type(), roachpb::DataType::FLOAT);
  EXPECT_EQ(field_float.get_storage_length(), sizeof(k_float32));
  
  // Test field_to_copy
  Field *copied = field_float.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldDouble
TEST_F(TestFieldIterator, TestFieldDouble) {
  FieldDouble field_double(0, roachpb::DataType::DOUBLE, sizeof(k_double64));
  
  // Test basic properties
  EXPECT_EQ(field_double.get_storage_type(), roachpb::DataType::DOUBLE);
  EXPECT_EQ(field_double.get_storage_length(), sizeof(k_double64));
  
  // Test field_to_copy
  Field *copied = field_double.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldDecimal
TEST_F(TestFieldIterator, TestFieldDecimal) {
  FieldDecimal field_decimal(0, roachpb::DataType::DECIMAL, sizeof(k_int64));
  
  // Test basic properties
  EXPECT_EQ(field_decimal.get_storage_type(), roachpb::DataType::DECIMAL);
  EXPECT_EQ(field_decimal.get_storage_length(), sizeof(k_int64));
  
  // Test field_to_copy
  Field *copied = field_decimal.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldSumInt
TEST_F(TestFieldIterator, TestFieldSumInt) {
  FieldSumInt field_sum_int(0, roachpb::DataType::INT, sizeof(k_int32));
  
  // Test basic properties
  EXPECT_EQ(field_sum_int.get_storage_type(), roachpb::DataType::DECIMAL);
  EXPECT_EQ(field_sum_int.get_storage_length(), sizeof(k_int32));
  
  // Test field_to_copy
  Field *copied = field_sum_int.field_to_copy();
  // EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldBlob
TEST_F(TestFieldIterator, TestFieldBlob) {
  FieldBlob field_blob(0, roachpb::DataType::BINARY, 100);
  
  // Test basic properties
  EXPECT_EQ(field_blob.get_storage_type(), roachpb::DataType::BINARY);
  EXPECT_EQ(field_blob.get_storage_length(), 100);
  
  // Test field_to_copy
  Field *copied = field_blob.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldVarchar
TEST_F(TestFieldIterator, TestFieldVarchar) {
  FieldVarchar field_varchar(0, roachpb::DataType::VARCHAR, 100);
  
  // Test basic properties
  EXPECT_EQ(field_varchar.get_storage_type(), roachpb::DataType::VARCHAR);
  EXPECT_EQ(field_varchar.get_storage_length(), 100);
  
  // Test field_to_copy
  Field *copied = field_varchar.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldTagVarchar
TEST_F(TestFieldIterator, TestFieldTagVarchar) {
  FieldTagVarchar field_tag_varchar(0, roachpb::DataType::VARCHAR, 100);
  
  // Test basic properties
  EXPECT_EQ(field_tag_varchar.get_storage_type(), roachpb::DataType::VARCHAR);
  EXPECT_EQ(field_tag_varchar.get_storage_length(), 100);
  
  // Test field_to_copy
  Field *copied = field_tag_varchar.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldVarBlob
TEST_F(TestFieldIterator, TestFieldVarBlob) {
  FieldVarBlob field_var_blob(0, roachpb::DataType::VARBINARY, 100);
  
  // Test basic properties
  EXPECT_EQ(field_var_blob.get_storage_type(), roachpb::DataType::VARBINARY);
  EXPECT_EQ(field_var_blob.get_storage_length(), 100);
  
  // Test field_to_copy
  Field *copied = field_var_blob.field_to_copy();
  EXPECT_NE(copied, nullptr);
  SafeDeletePointer(copied);
}

// Test FieldSumStatisticTagSum
TEST_F(TestFieldIterator, TestFieldSumStatisticTagSum) {
  BaseField *base_field = new BaseField(0, roachpb::DataType::INT, sizeof(k_int32));
  FieldSumStatisticTagSum field_sum_statistic_tag_sum(base_field);
  
  // Test field_to_copy
  Field *copied = field_sum_statistic_tag_sum.field_to_copy();
  // EXPECT_NE(copied, nullptr);
  
  SafeDeletePointer(copied);
  SafeDeletePointer(base_field);
}

// Test FieldSumStatisticTagCount
TEST_F(TestFieldIterator, TestFieldSumStatisticTagCount) {
  BaseField *base_field = new BaseField(0, roachpb::DataType::INT, sizeof(k_int32));
  FieldSumStatisticTagCount field_sum_statistic_tag_count(base_field);
  
  // Test field_to_copy
  Field *copied = field_sum_statistic_tag_count.field_to_copy();
  // EXPECT_NE(copied, nullptr);
  
  SafeDeletePointer(copied);
  SafeDeletePointer(base_field);
}

}  // namespace kwdbts
