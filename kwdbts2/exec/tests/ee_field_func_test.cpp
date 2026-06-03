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

#include "ee_field_func.h"

#include <limits>

#include "ee_field_const.h"
#include "ee_global.h"
#include "gtest/gtest.h"
#include "pgcode.h"

namespace kwdbts {
class TestFieldFunc : public testing::Test {
 protected:
  void SetUp() override {
    EEPgErrorInfo::ResetPgErrorInfo();
  }

  void TearDown() override {
    EEPgErrorInfo::ResetPgErrorInfo();
  }

  static void SetUpTestCase() {
  }

  static void TearDownTestCase() {
  }
};

namespace {

void ExpectPgError(k_int32 code, const char* msg) {
  ASSERT_TRUE(EEPgErrorInfo::IsError());
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, code);
  EXPECT_STREQ(EEPgErrorInfo::GetPgErrorInfo().msg, msg);
}

class SequenceIntField : public Field {
 public:
  explicit SequenceIntField(std::initializer_list<k_int64> values)
      : values_(values), current_(0), index_(0) {
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
    type_ = FIELD_CONSTANT;
  }

  k_int64 ValInt() override {
    k_int64 value = values_[CurrentIndex()];
    Advance();
    return value;
  }
  k_int64 ValInt(k_char *ptr) override {
    k_int64 value = *reinterpret_cast<k_int64 *>(ptr);
    Advance();
    return value;
  }
  k_double64 ValReal() override { return ValInt(); }
  k_double64 ValReal(k_char *ptr) override { return ValInt(ptr); }
  String ValStr() override {
    String s(32);
    snprintf(s.ptr_, 33, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
    return s;
  }
  String ValStr(k_char *ptr) override {
    String s(32);
    snprintf(s.ptr_, 33, "%ld", ValInt(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
  }
  Field *field_to_copy() override { return KNEW SequenceIntField(*this); }
  k_bool fill_template_field(char *ptr) override {
    memcpy(ptr, &current_, sizeof(current_));
    return 0;
  }
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override {
    current_ = values_[CurrentIndex()];
    return reinterpret_cast<char *>(&current_);
  }
  k_bool is_nullable() override { return false; }

 private:
  size_t CurrentIndex() const {
    return index_ < values_.size() ? index_ : values_.size() - 1;
  }
  void Advance() {
    if (index_ + 1 < values_.size()) {
      ++index_;
    }
  }

  std::vector<k_int64> values_;
  k_int64 current_;
  size_t index_;
};

class SequenceDoubleField : public Field {
 public:
  explicit SequenceDoubleField(std::initializer_list<k_double64> values)
      : values_(values), current_(0.0), index_(0) {
    sql_type_ = roachpb::DataType::DOUBLE;
    storage_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(k_double64);
    type_ = FIELD_CONSTANT;
  }

  k_int64 ValInt() override { return ValReal(); }
  k_int64 ValInt(k_char *ptr) override { return ValReal(ptr); }
  k_double64 ValReal() override {
    k_double64 value = values_[CurrentIndex()];
    Advance();
    return value;
  }
  k_double64 ValReal(k_char *ptr) override {
    k_double64 value = *reinterpret_cast<k_double64 *>(ptr);
    Advance();
    return value;
  }
  String ValStr() override {
    String s(32);
    snprintf(s.ptr_, 33, "%.6f", ValReal());
    s.length_ = strlen(s.ptr_);
    return s;
  }
  String ValStr(k_char *ptr) override {
    String s(32);
    snprintf(s.ptr_, 33, "%.6f", ValReal(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
  }
  Field *field_to_copy() override { return KNEW SequenceDoubleField(*this); }
  k_bool fill_template_field(char *ptr) override {
    memcpy(ptr, &current_, sizeof(current_));
    return 0;
  }
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override {
    current_ = values_[CurrentIndex()];
    return reinterpret_cast<char *>(&current_);
  }
  k_bool is_nullable() override { return false; }

 private:
  size_t CurrentIndex() const {
    return index_ < values_.size() ? index_ : values_.size() - 1;
  }
  void Advance() {
    if (index_ + 1 < values_.size()) {
      ++index_;
    }
  }

  std::vector<k_double64> values_;
  k_double64 current_;
  size_t index_;
};

class BrokenIntField : public Field {
 public:
  BrokenIntField() {
    sql_type_ = roachpb::DataType::BIGINT;
    storage_type_ = roachpb::DataType::BIGINT;
    storage_len_ = sizeof(k_int64);
    type_ = FIELD_CONSTANT;
    allow_null_ = false;
  }

  k_int64 ValInt() override { return 0; }
  k_int64 ValInt(k_char *ptr) override { return 0; }
  k_double64 ValReal() override { return 0.0; }
  k_double64 ValReal(k_char *ptr) override { return 0.0; }
  String ValStr() override { return String(""); }
  String ValStr(k_char *ptr) override { return String(""); }
  Field *field_to_copy() override { return KNEW BrokenIntField(*this); }
  k_bool fill_template_field(char *ptr) override { return 0; }
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override { return nullptr; }
  k_bool is_nullable() override { return false; }
};

class EncodedStringField : public FieldConstString {
 public:
  EncodedStringField(roachpb::DataType datatype, const KString &str)
      : FieldConstString(datatype, str), encoded_(sizeof(k_uint16) + str.size() + 1, '\0') {
    k_uint16 len = static_cast<k_uint16>(str.size());
    memcpy(encoded_.data(), &len, sizeof(len));
    memcpy(encoded_.data() + sizeof(len), str.data(), str.size());
    encoded_[sizeof(k_uint16) + str.size()] = '\0';
  }

  char *get_ptr(RowBatch *batch) override {
    return encoded_.data();
  }

  Field *field_to_copy() override { return KNEW EncodedStringField(*this); }

 private:
  std::vector<char> encoded_;
};

}  // namespace

TEST_F(TestFieldFunc, TestFieldPlusFunc) {
  k_int64 a = 1;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncPlus *field = KNEW FieldFuncPlus(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "11");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldMinusFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncMinus *field = KNEW FieldFuncMinus(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldDivideFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncDivide *field = KNEW FieldFuncDivide(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldDividezFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncDividez *field = KNEW FieldFuncDividez(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldMultFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncMult *field = KNEW FieldFuncMult(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldRemainderFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncRemainder *field = KNEW FieldFuncRemainder(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 3);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 3.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldPercentFunc) {
  k_int64 a = 1;
  k_int64 b = 2;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncPercent *field = KNEW FieldFuncPercent(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldPowerFunc) {
  k_int64 a = 2;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncPower *field = KNEW FieldFuncPower(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldModFunc) {
  k_int64 a = 1;
  k_int64 b = 2;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncMod *field = KNEW FieldFuncMod(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldDateTruncFunc) {
  KString a = "second";
  k_int64 b = 1712804880000;
  k_int8 time_zone = 0;
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncDateTrunc *field = KNEW FieldFuncDateTrunc(FieldConstValA, FieldConstValB, time_zone);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1712804880000);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1712804880000.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "17128048");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldExtractFunc) {
  KString a = "day";
  k_int64 b = 1712804880000;
  k_int8 time_zone = 0;
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncExtract *field = KNEW FieldFuncExtract(FieldConstValA, FieldConstValB, time_zone);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 11);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 11.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "11");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldTimeBucketFunc) {
  KString a = "300s";
  k_int64 b = 1690600319688;
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::TIMESTAMPTZ, b, sizeof(k_int64));

  std::list<Field *> args;
  args.push_back(FieldConstValB);
  args.push_back(FieldConstValA);
  FieldFuncTimeBucket *field = KNEW FieldFuncTimeBucket(args, 8);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1690600200000);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1690600200000.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "16906002");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}

TEST_F(TestFieldFunc, TestFieldCoalesceFunc) {
  KString a = "2013-04-12";
  k_int64 b = 1;
  FieldConstDate *FieldConstValA = KNEW FieldConstDate(roachpb::DataType::DATE, a, 0);
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncCoalesce *field = KNEW FieldFuncCoalesce(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1365724800000);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1365724800000.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "2013-04-12");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldCrc32CFunc) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncCrc32C *field = KNEW FieldFuncCrc32C(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 26154185);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 26154185.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "26154185");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}

TEST_F(TestFieldFunc, TestFieldCrc32IFunc) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncCrc32I *field = KNEW FieldFuncCrc32I(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 3473062748);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 3473062748.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "34730627");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}

TEST_F(TestFieldFunc, TestFieldFnv32Func) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncFnv32 *field = KNEW FieldFuncFnv32(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 3613024805);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 3613024805.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "36130248");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}
TEST_F(TestFieldFunc, TestFieldFnv32aFunc) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncFnv32a *field = KNEW FieldFuncFnv32a(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 951228933);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 951228933.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "95122893");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}
TEST_F(TestFieldFunc, TestFieldFnv64Func) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncFnv64 *field = KNEW FieldFuncFnv64(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2635828873713441413);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2635828873713441413.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "26358288");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}
TEST_F(TestFieldFunc, TestFieldFnv64aFunc) {
  KString a = "abc";
  KString b = "123";
  FieldConstString *FieldConstValA = KNEW FieldConstString(roachpb::DataType::CHAR, a);
  FieldConstString *FieldConstValB = KNEW FieldConstString(roachpb::DataType::CHAR, b);
  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  FieldFuncFnv64a *field = KNEW FieldFuncFnv64a(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 7119243511811735397);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 7119243511811735397.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "71192435");

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}

TEST_F(TestFieldFunc, TestFieldLeftShiftFunc) {
  k_int64 a = 4;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncLeftShift *field = KNEW FieldFuncLeftShift(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 8);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 8.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "8");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldRightShiftFunc) {
  k_int64 a = 4;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncRightShift *field = KNEW FieldFuncRightShift(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "2");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldWidthBucketFunc) {
  k_int64 a = 0;
  k_int64 b = -1000;
  k_int64 c = 2000;
  k_int64 d = 3;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldConstInt *FieldConstValC = KNEW FieldConstInt(roachpb::DataType::BIGINT, c, sizeof(k_int64));
  FieldConstInt *FieldConstValD = KNEW FieldConstInt(roachpb::DataType::BIGINT, d, sizeof(k_int64));

  std::list<Field *> args;
  args.push_back(FieldConstValA);
  args.push_back(FieldConstValB);
  args.push_back(FieldConstValC);
  args.push_back(FieldConstValD);

  FieldFuncWidthBucket *field = KNEW FieldFuncWidthBucket(args);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 2.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "2");
  SafeDeletePointer(field);
  for (auto e : args) {
    SafeDeletePointer(e);
  }
}

TEST_F(TestFieldFunc, TestFieldAndCalFunc) {
  k_int64 a = 1;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncAndCal *field = KNEW FieldFuncAndCal(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "1");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldOrCalFunc) {
  k_int64 a = 1;
  k_int64 b = 0;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncOrCal *field = KNEW FieldFuncOrCal(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "1");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldNotCalFunc) {
  k_int64 a = 1;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncNotCal *field = KNEW FieldFuncNotCal(FieldConstValA, FieldConstValB);

  k_int64 ival = field->ValInt();
  EXPECT_EQ(ival, -2);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, -2.0);
  kwdbts::String strval = field->ValStr();
  EXPECT_STREQ(strval.c_str(), "-2");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldPlusGetPtrWithDouble) {
  k_int64 a = 2;
  k_double64 b = 1.5;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstDouble *FieldConstValB = KNEW FieldConstDouble(roachpb::DataType::DOUBLE, b);
  FieldFuncPlus *field = KNEW FieldFuncPlus(FieldConstValA, FieldConstValB);

  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_DOUBLE_EQ(*reinterpret_cast<k_double64 *>(ptr), 3.5);
  EXPECT_DOUBLE_EQ(field->ValReal(), 3.5);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldPlusOverflowReportsError) {
  k_int64 a = std::numeric_limits<k_int64>::max();
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, b, sizeof(k_int64));
  FieldFuncPlus *field = KNEW FieldFuncPlus(FieldConstValA, FieldConstValB);

  EXPECT_EQ(field->ValInt(), 0);
  ExpectPgError(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range");

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldMinusTimestampPrecisionScale) {
  k_int64 a = 10;
  k_int64 b = 1;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, a, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP_MICRO, b, sizeof(k_int64));
  FieldFuncMinus *field = KNEW FieldFuncMinus(FieldConstValA, FieldConstValB);

  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(ptr), 9999);
  EXPECT_EQ(field->ValInt(), 9999);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldDivideNullableReportsDivisionErrors) {
  k_int64 one = 1;
  k_int64 zero = 0;
  FieldConstInt *FieldConstValOne = KNEW FieldConstInt(roachpb::DataType::BIGINT, one, sizeof(k_int64));
  FieldConstInt *FieldConstValZero = KNEW FieldConstInt(roachpb::DataType::BIGINT, zero, sizeof(k_int64));
  FieldFuncDivide *field = KNEW FieldFuncDivide(FieldConstValOne, FieldConstValZero);

  EXPECT_FALSE(field->is_nullable());
  ExpectPgError(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  EEPgErrorInfo::ResetPgErrorInfo();

  FieldFuncDivide *undefined_field = KNEW FieldFuncDivide(FieldConstValZero, FieldConstValZero);
  EXPECT_FALSE(undefined_field->is_nullable());
  ExpectPgError(ERRCODE_DIVISION_BY_ZERO, "division undefined");

  SafeDeletePointer(undefined_field);
  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValOne);
  SafeDeletePointer(FieldConstValZero);
}

TEST_F(TestFieldFunc, TestFieldDividezGetPtrWithDouble) {
  k_int64 a = 7;
  k_double64 b = 2.0;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, a, sizeof(k_int64));
  FieldConstDouble *FieldConstValB = KNEW FieldConstDouble(roachpb::DataType::DOUBLE, b);
  FieldFuncDividez *field = KNEW FieldFuncDividez(FieldConstValA, FieldConstValB);

  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_DOUBLE_EQ(*reinterpret_cast<k_double64 *>(ptr), 3.0);
  EXPECT_EQ(field->ValInt(), 3);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldPercentCoversDoubleAndZeroModulus) {
  k_double64 a = 5.5;
  k_double64 b = 2.0;
  FieldConstDouble *FieldConstValA = KNEW FieldConstDouble(roachpb::DataType::DOUBLE, a);
  FieldConstDouble *FieldConstValB = KNEW FieldConstDouble(roachpb::DataType::DOUBLE, b);
  FieldFuncPercent *field = KNEW FieldFuncPercent(FieldConstValA, FieldConstValB);

  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_DOUBLE_EQ(*reinterpret_cast<k_double64 *>(ptr), 1.5);
  EXPECT_DOUBLE_EQ(field->ValReal(), 1.5);

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);

  k_int64 one = 1;
  k_int64 zero = 0;
  FieldConstInt *FieldConstValOne = KNEW FieldConstInt(roachpb::DataType::BIGINT, one, sizeof(k_int64));
  FieldConstInt *FieldConstValZero = KNEW FieldConstInt(roachpb::DataType::BIGINT, zero, sizeof(k_int64));
  FieldFuncPercent *zero_field = KNEW FieldFuncPercent(FieldConstValOne, FieldConstValZero);

  EXPECT_FALSE(zero_field->is_nullable());
  ExpectPgError(ERRCODE_DIVISION_BY_ZERO, "zero modulus");

  SafeDeletePointer(zero_field);
  SafeDeletePointer(FieldConstValOne);
  SafeDeletePointer(FieldConstValZero);
}

TEST_F(TestFieldFunc, TestFieldDateTruncQuarterAndInvalidUnit) {
  k_int64 ts = 1715776496000;
  k_int8 time_zone = 0;
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, ts, sizeof(k_int64));

  KString quarter = "quarter";
  EncodedStringField *FieldConstValQuarter = KNEW EncodedStringField(roachpb::DataType::CHAR, quarter);
  FieldFuncDateTrunc *quarter_field = KNEW FieldFuncDateTrunc(FieldConstValQuarter, FieldConstValTs, time_zone);

  EXPECT_EQ(quarter_field->ValInt(), 1711929600000);
  char *quarter_ptr = quarter_field->get_ptr(nullptr);
  ASSERT_NE(quarter_ptr, nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(quarter_ptr), 1711929600000);

  SafeDeletePointer(quarter_field);
  SafeDeletePointer(FieldConstValQuarter);

  KString invalid = "badunit";
  FieldConstString *FieldConstValInvalid = KNEW FieldConstString(roachpb::DataType::CHAR, invalid);
  FieldFuncDateTrunc *invalid_field = KNEW FieldFuncDateTrunc(FieldConstValInvalid, FieldConstValTs, time_zone);

  EXPECT_EQ(invalid_field->ValInt(), 0);
  ExpectPgError(ERRCODE_INVALID_PARAMETER_VALUE, "unsupported timespan");

  SafeDeletePointer(invalid_field);
  SafeDeletePointer(FieldConstValInvalid);
  SafeDeletePointer(FieldConstValTs);
}

TEST_F(TestFieldFunc, TestFieldDateTruncAdditionalUnits) {
  k_int64 ts = 1715776496000;
  k_int8 time_zone = 0;
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, ts, sizeof(k_int64));

  struct CaseItem {
    const char *unit;
    k_int64 expected;
  } cases[] = {
      {"day", 1715731200000},
      {"week", 1715558400000},
      {"month", 1714521600000},
      {"year", 1704067200000},
      {"century", 946684800000},
  };

  for (const auto &item : cases) {
    KString unit = item.unit;
    FieldConstString *FieldConstValUnit = KNEW FieldConstString(roachpb::DataType::CHAR, unit);
    FieldFuncDateTrunc *field = KNEW FieldFuncDateTrunc(FieldConstValUnit, FieldConstValTs, time_zone);
    EXPECT_EQ(field->ValInt(), item.expected);
    SafeDeletePointer(field);
    SafeDeletePointer(FieldConstValUnit);
  }

  SafeDeletePointer(FieldConstValTs);
}

TEST_F(TestFieldFunc, TestFieldDateTruncNullableStates) {
  k_int64 ts = 1715776496000;
  k_int8 time_zone = 0;
  FieldConstNull *FieldConstNullUnit = KNEW FieldConstNull(roachpb::DataType::NULLVAL, 0);
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, ts, sizeof(k_int64));
  FieldFuncDateTrunc *field = KNEW FieldFuncDateTrunc(FieldConstNullUnit, FieldConstValTs, time_zone);

  EXPECT_TRUE(field->is_nullable());

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstNullUnit);
  SafeDeletePointer(FieldConstValTs);
}

TEST_F(TestFieldFunc, TestFieldExtractHourGetPtr) {
  KString unit = "hour";
  k_int64 ts = 1715776496000;
  k_int8 time_zone = 0;
  EncodedStringField *FieldConstValUnit = KNEW EncodedStringField(roachpb::DataType::CHAR, unit);
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, ts, sizeof(k_int64));
  FieldFuncExtract *field = KNEW FieldFuncExtract(FieldConstValUnit, FieldConstValTs, time_zone);

  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(field->ValInt(), 12);
  EXPECT_FALSE(EEPgErrorInfo::IsError());

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstValUnit);
  SafeDeletePointer(FieldConstValTs);
}

TEST_F(TestFieldFunc, TestFieldTimeBucketMonthInterval) {
  KString interval = "1month";
  k_int64 ts = 1715776496000;
  FieldConstString *FieldConstValInterval = KNEW FieldConstString(roachpb::DataType::CHAR, interval);
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMPTZ, ts, sizeof(k_int64));

  std::list<Field *> args;
  args.push_back(FieldConstValTs);
  args.push_back(FieldConstValInterval);
  FieldFuncTimeBucket *field = KNEW FieldFuncTimeBucket(args, 0);

  EXPECT_EQ(field->ValInt(), 1714521600000);
  EXPECT_DOUBLE_EQ(field->ValReal(), 1714521600000.0);

  SafeDeletePointer(field);
  for (auto c : args) {
    SafeDeletePointer(c);
  }
}

TEST_F(TestFieldFunc, TestFieldWidthBucketSpecialCases) {
  {
    std::list<Field *> args;
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 10, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 0, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 20, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 0, sizeof(k_int64)));
    FieldFuncWidthBucket *field = KNEW FieldFuncWidthBucket(args);
    EXPECT_EQ(field->ValInt(), 0);
    SafeDeletePointer(field);
    for (auto c : args) {
      SafeDeletePointer(c);
    }
  }

  {
    std::list<Field *> args;
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 10, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 5, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 5, sizeof(k_int64)));
    args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 4, sizeof(k_int64)));
    FieldFuncWidthBucket *field = KNEW FieldFuncWidthBucket(args);
    EXPECT_EQ(field->ValInt(), std::numeric_limits<k_int64>::min());
    SafeDeletePointer(field);
    for (auto c : args) {
      SafeDeletePointer(c);
    }
  }
}

TEST_F(TestFieldFunc, TestFieldCoalesceNullFallbackAndNullable) {
  FieldConstNull *FieldConstNullVal = KNEW FieldConstNull(roachpb::DataType::BIGINT, sizeof(k_int64));
  FieldConstInt *FieldConstVal = KNEW FieldConstInt(roachpb::DataType::BIGINT, 42, sizeof(k_int64));
  FieldFuncCoalesce *field = KNEW FieldFuncCoalesce(FieldConstNullVal, FieldConstVal);

  EXPECT_EQ(field->ValInt(), 42);
  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(ptr), 42);
  EXPECT_FALSE(field->is_nullable());

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstNullVal);
  SafeDeletePointer(FieldConstVal);

  FieldConstNull *FieldConstNullA = KNEW FieldConstNull(roachpb::DataType::BIGINT, sizeof(k_int64));
  FieldConstNull *FieldConstNullB = KNEW FieldConstNull(roachpb::DataType::BIGINT, sizeof(k_int64));
  FieldFuncCoalesce *nullable_field = KNEW FieldFuncCoalesce(FieldConstNullA, FieldConstNullB);

  EXPECT_TRUE(nullable_field->is_nullable());

  SafeDeletePointer(nullable_field);
  SafeDeletePointer(FieldConstNullA);
  SafeDeletePointer(FieldConstNullB);
}

TEST_F(TestFieldFunc, TestShiftOperatorsReportOutOfRange) {
  k_int64 value = 4;
  k_int64 too_large = 64;
  FieldConstInt *FieldConstValA = KNEW FieldConstInt(roachpb::DataType::BIGINT, value, sizeof(k_int64));
  FieldConstInt *FieldConstValB = KNEW FieldConstInt(roachpb::DataType::BIGINT, too_large, sizeof(k_int64));
  FieldFuncLeftShift *left_shift = KNEW FieldFuncLeftShift(FieldConstValA, FieldConstValB);

  EXPECT_EQ(left_shift->ValInt(), 0);
  ExpectPgError(ERRCODE_INVALID_PARAMETER_VALUE, "shift argument out of range");

  EEPgErrorInfo::ResetPgErrorInfo();

  k_int64 negative = -1;
  FieldConstInt *FieldConstValC = KNEW FieldConstInt(roachpb::DataType::BIGINT, negative, sizeof(k_int64));
  FieldFuncRightShift *right_shift = KNEW FieldFuncRightShift(FieldConstValA, FieldConstValC);

  EXPECT_EQ(right_shift->ValInt(), 0);
  ExpectPgError(ERRCODE_INVALID_PARAMETER_VALUE, "shift argument out of range");

  SafeDeletePointer(right_shift);
  SafeDeletePointer(FieldConstValC);
  SafeDeletePointer(left_shift);
  SafeDeletePointer(FieldConstValA);
  SafeDeletePointer(FieldConstValB);
}

TEST_F(TestFieldFunc, TestFieldCastCheckTs) {
  KString value = "123";
  KString type = "int4";
  EncodedStringField *FieldConstVal = KNEW EncodedStringField(roachpb::DataType::CHAR, value);
  EncodedStringField *FieldConstType = KNEW EncodedStringField(roachpb::DataType::CHAR, type);
  FieldFuncCastCheckTs *field = KNEW FieldFuncCastCheckTs(FieldConstVal, FieldConstType);

  EXPECT_EQ(field->ValInt(), 1);
  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EEPgErrorInfo::ResetPgErrorInfo();

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstVal);
  SafeDeletePointer(FieldConstType);
}

TEST_F(TestFieldFunc, TestFieldCurrentDateAndNowFamily) {
  FieldFuncCurrentDate current_date;
  EXPECT_EQ(current_date.ValStr().length(), 10U);
  ASSERT_NE(current_date.get_ptr(nullptr), nullptr);

  k_int64 prec = 3;
  FieldConstInt *FieldConstPrec = KNEW FieldConstInt(roachpb::DataType::BIGINT, prec, sizeof(k_int64));
  FieldFuncCurrentTimeStamp current_timestamp(FieldConstPrec, static_cast<k_int8>(0));
  ASSERT_NE(current_timestamp.get_ptr(nullptr), nullptr);

  FieldFuncNow now_field;
  EXPECT_FALSE(now_field.ValStr().empty());
  ASSERT_NE(now_field.get_ptr(nullptr), nullptr);

  SafeDeletePointer(FieldConstPrec);
}

TEST_F(TestFieldFunc, TestFieldCurrentTimestampInvalidPrecision) {
  k_int64 prec = 10;
  FieldConstInt *FieldConstPrec = KNEW FieldConstInt(roachpb::DataType::BIGINT, prec, sizeof(k_int64));
  FieldFuncCurrentTimeStamp field(FieldConstPrec, static_cast<k_int8>(0));

  EXPECT_EQ(field.ValInt(), 0);
  EXPECT_TRUE(std::string(EEPgErrorInfo::GetPgErrorInfo().msg).find("out of range") != std::string::npos);

  SafeDeletePointer(FieldConstPrec);
}

TEST_F(TestFieldFunc, TestFieldExpStrftimeAndTimeOfDayAndRandom) {
  k_int64 ts = 1715776496000;
  KString format = "%F %T";
  FieldConstInt *FieldConstValTs = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, ts, sizeof(k_int64));
  EncodedStringField *FieldConstValFormat = KNEW EncodedStringField(roachpb::DataType::CHAR, format);
  FieldFuncExpStrftime *strftime_field = KNEW FieldFuncExpStrftime(FieldConstValTs, FieldConstValFormat, 0);

  EXPECT_STREQ(strftime_field->ValStr().c_str(), "2024-05-15 12:34:56");
  char *strftime_ptr = strftime_field->get_ptr(nullptr);
  ASSERT_NE(strftime_ptr, nullptr);
  EXPECT_STREQ(strftime_field->ValStr().c_str(), "2024-05-15 12:34:56");

  FieldFuncTimeOfDay time_of_day(8);
  EXPECT_GT(time_of_day.ValInt(), 0);
  EXPECT_FALSE(time_of_day.ValStr().empty());
  ASSERT_NE(time_of_day.get_ptr(nullptr), nullptr);

  FieldFuncRandom random_field;
  k_double64 rand_val = random_field.ValReal();
  EXPECT_GE(rand_val, 0.0);
  EXPECT_LE(rand_val, 1.0);
  char *rand_ptr = random_field.get_ptr(nullptr);
  ASSERT_NE(rand_ptr, nullptr);
  k_double64 rand_ptr_val = *reinterpret_cast<k_double64 *>(rand_ptr);
  EXPECT_GE(rand_ptr_val, 0.0);
  EXPECT_LE(rand_ptr_val, 1.0);

  SafeDeletePointer(strftime_field);
  SafeDeletePointer(FieldConstValTs);
  SafeDeletePointer(FieldConstValFormat);
}

TEST_F(TestFieldFunc, TestFieldAgeGetPtr) {
  k_int64 newer = 10;
  k_int64 older = 1;
  FieldConstInt *FieldConstNewer = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP, newer, sizeof(k_int64));
  FieldConstInt *FieldConstOlder = KNEW FieldConstInt(roachpb::DataType::TIMESTAMP_MICRO, older, sizeof(k_int64));
  FieldFuncAge *field = KNEW FieldFuncAge(FieldConstNewer, FieldConstOlder);

  EXPECT_EQ(field->ValInt(), 9999);
  char *ptr = field->get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int64 *>(ptr), 9999);
  EXPECT_FALSE(field->ValStr().empty());

  SafeDeletePointer(field);
  SafeDeletePointer(FieldConstNewer);
  SafeDeletePointer(FieldConstOlder);
}

TEST_F(TestFieldFunc, TestFieldCaseThenElse) {
  FieldConstInt *cond_false = KNEW FieldConstInt(roachpb::DataType::BIGINT, 0, sizeof(k_int64));
  FieldConstInt *cond_true = KNEW FieldConstInt(roachpb::DataType::BIGINT, 1, sizeof(k_int64));
  FieldConstInt *val11 = KNEW FieldConstInt(roachpb::DataType::BIGINT, 11, sizeof(k_int64));
  FieldConstInt *val22 = KNEW FieldConstInt(roachpb::DataType::BIGINT, 22, sizeof(k_int64));
  FieldConstInt *val33 = KNEW FieldConstInt(roachpb::DataType::BIGINT, 33, sizeof(k_int64));

  FieldFuncThen *then_false = KNEW FieldFuncThen(cond_false, val11);
  FieldFuncThen *then_true = KNEW FieldFuncThen(cond_true, val22);
  FieldFuncElse *else_field = KNEW FieldFuncElse(val33);

  EXPECT_EQ(then_false->ValInt(), 0);
  EXPECT_FALSE(then_false->is_condition_met());
  EXPECT_EQ(then_true->ValInt(), 22);
  EXPECT_TRUE(then_true->is_condition_met());
  EXPECT_EQ(else_field->ValInt(), 33);
  EXPECT_TRUE(else_field->is_condition_met());

  std::list<Field *> args;
  args.push_back(then_false);
  args.push_back(then_true);
  args.push_back(else_field);
  FieldFuncCase *case_field = KNEW FieldFuncCase(args);

  EXPECT_EQ(case_field->ValInt(), 22);
  ASSERT_NE(case_field->get_ptr(nullptr), nullptr);
  EXPECT_FALSE(case_field->is_nullable());

  SafeDeletePointer(case_field);
  SafeDeletePointer(then_false);
  SafeDeletePointer(then_true);
  SafeDeletePointer(else_field);
  SafeDeletePointer(cond_false);
  SafeDeletePointer(cond_true);
  SafeDeletePointer(val11);
  SafeDeletePointer(val22);
  SafeDeletePointer(val33);
}

TEST_F(TestFieldFunc, TestHashFuncsGetPtr) {
  KString a = "abc";
  KString b = "123";
  auto make_args = [&]() {
    std::list<Field *> args;
    args.push_back(KNEW EncodedStringField(roachpb::DataType::CHAR, a));
    args.push_back(KNEW EncodedStringField(roachpb::DataType::CHAR, b));
    return args;
  };

  {
    auto args = make_args();
    FieldFuncCrc32C field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
  {
    auto args = make_args();
    FieldFuncCrc32I field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
  {
    auto args = make_args();
    FieldFuncFnv32 field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
  {
    auto args = make_args();
    FieldFuncFnv32a field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
  {
    auto args = make_args();
    FieldFuncFnv64 field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
  {
    auto args = make_args();
    FieldFuncFnv64a field(args);
    char *ptr = field.get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_FALSE(EEPgErrorInfo::IsError());
    for (auto &c : args) { SafeDeletePointer(c); }
  }
}

TEST_F(TestFieldFunc, TestFieldDiff) {
  SequenceIntField int_values({10, 15});
  FieldFuncDiff int_diff(&int_values);
  EXPECT_EQ(int_diff.ValInt(), 0);
  EXPECT_EQ(int_diff.ValInt(), 5);

  SequenceDoubleField double_values({1.5, 2.25});
  FieldFuncDiff double_diff(&double_values);
  EXPECT_DOUBLE_EQ(double_diff.ValReal(), 0.0);
  EXPECT_DOUBLE_EQ(double_diff.ValReal(), 0.75);

  SequenceIntField batch_values({100, 120});
  FieldFuncDiff batch_diff(&batch_values);
  EXPECT_STREQ(batch_diff.get_ptr(nullptr), "");
  char *ptr = batch_diff.get_ptr(nullptr);
  ASSERT_NE(ptr, nullptr);

  Field *copy = int_diff.field_to_copy();
  ASSERT_NE(copy, nullptr);
  SafeDeletePointer(copy);
}

TEST_F(TestFieldFunc, TestFieldToCopyFactories) {
  FieldConstInt *lhs = KNEW FieldConstInt(roachpb::DataType::BIGINT, 2, sizeof(k_int64));
  FieldConstInt *rhs = KNEW FieldConstInt(roachpb::DataType::BIGINT, 1, sizeof(k_int64));
  FieldConstDouble *double_rhs = KNEW FieldConstDouble(roachpb::DataType::DOUBLE, 1.5);
  KString unit = "day";
  FieldConstString *unit_field = KNEW FieldConstString(roachpb::DataType::CHAR, unit);
  FieldFuncPlus plus(lhs, rhs);
  FieldFuncMinus minus(lhs, rhs);
  FieldFuncMult mult(lhs, rhs);
  FieldFuncDivide divide(lhs, rhs);
  FieldFuncDividez dividez(lhs, rhs);
  FieldFuncPercent percent(lhs, rhs);
  FieldFuncPower power(lhs, rhs);
  FieldFuncMod mod(lhs, rhs);
  FieldFuncAndCal and_cal(lhs, rhs);
  FieldFuncOrCal or_cal(lhs, rhs);
  FieldFuncNotCal not_cal(lhs, rhs);
  FieldFuncLeftShift left_shift(lhs, rhs);
  FieldFuncRightShift right_shift(lhs, rhs);
  FieldFuncDateTrunc date_trunc(unit_field, lhs, 0);
  FieldFuncExtract extract(unit_field, lhs, 0);
  FieldFuncCurrentDate current_date;
  FieldFuncCurrentTimeStamp current_timestamp(rhs, static_cast<k_int8>(0));
  FieldFuncNow now_field;
  FieldFuncTimeOfDay time_of_day(0);
  FieldFuncRandom random_field;
  FieldFuncAge age(lhs, rhs);
  FieldFuncCoalesce coalesce(lhs, rhs);

  std::list<Field *> hash_args;
  hash_args.push_back(KNEW FieldConstString(roachpb::DataType::CHAR, KString("abc")));
  hash_args.push_back(KNEW FieldConstString(roachpb::DataType::CHAR, KString("123")));
  FieldFuncCrc32C crc32c(hash_args);
  FieldFuncFnv64 fnv64(hash_args);

  std::list<Field *> bucket_args;
  bucket_args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 0, sizeof(k_int64)));
  bucket_args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, -1000, sizeof(k_int64)));
  bucket_args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 2000, sizeof(k_int64)));
  bucket_args.push_back(KNEW FieldConstInt(roachpb::DataType::BIGINT, 3, sizeof(k_int64)));
  FieldFuncWidthBucket width_bucket(bucket_args);

  Field *copies[] = {
      plus.field_to_copy(), minus.field_to_copy(), mult.field_to_copy(), divide.field_to_copy(), dividez.field_to_copy(),
      percent.field_to_copy(), power.field_to_copy(), mod.field_to_copy(), and_cal.field_to_copy(), or_cal.field_to_copy(),
      not_cal.field_to_copy(), left_shift.field_to_copy(), right_shift.field_to_copy(), date_trunc.field_to_copy(),
      extract.field_to_copy(), current_date.field_to_copy(), current_timestamp.field_to_copy(), now_field.field_to_copy(),
      time_of_day.field_to_copy(), random_field.field_to_copy(), age.field_to_copy(), coalesce.field_to_copy(),
      crc32c.field_to_copy(), fnv64.field_to_copy()
  };

  for (auto *copy : copies) {
    ASSERT_NE(copy, nullptr);
    SafeDeletePointer(copy);
  }

  SafeDeletePointer(lhs);
  SafeDeletePointer(rhs);
  SafeDeletePointer(double_rhs);
  SafeDeletePointer(unit_field);
  for (auto c : hash_args) {
    delete c;
  }
  for (auto c : bucket_args) {
    delete c;
  }
}

TEST_F(TestFieldFunc, TestGetPtrNullInputReportsError) {
  auto expect_null_error = [](Field *field, const char *msg) {
    char *ptr = field->get_ptr(nullptr);
    ASSERT_NE(ptr, nullptr);
    EXPECT_STREQ(ptr, "");
    ExpectPgError(ERRCODE_INVALID_TEXT_REPRESENTATION, msg);
    EEPgErrorInfo::ResetPgErrorInfo();
  };

  BrokenIntField *null_int = KNEW BrokenIntField();
  FieldConstInt *one = KNEW FieldConstInt(roachpb::DataType::BIGINT, 1, sizeof(k_int64));

  FieldFuncPlus plus(null_int, one);
  expect_null_error(&plus, "could not parse \"\" field plus, get null value");
  FieldFuncMinus minus(null_int, one);
  expect_null_error(&minus, "could not parse \"\" field minus, get null value");
  FieldFuncMult mult(null_int, one);
  expect_null_error(&mult, "could not parse \"\" field mult, get null value");
  FieldFuncDivide divide(null_int, one);
  expect_null_error(&divide, "could not parse \"\" field divide, get null value");
  FieldFuncDividez dividez(null_int, one);
  expect_null_error(&dividez, "could not parse \"\" field dividez, get null value");
  FieldFuncRemainder remainder(null_int, one);
  expect_null_error(&remainder, "could not parse \"\" field remainder, get null value");
  FieldFuncPercent percent(null_int, one);
  expect_null_error(&percent, "could not parse \"\" field percent, get null value");
  FieldFuncPower power(null_int, one);
  expect_null_error(&power, "could not parse \"\" field power, get null value");
  FieldFuncMod mod(null_int, one);
  expect_null_error(&mod, "could not parse \"\" field mod, get null value");
  FieldFuncAndCal and_cal(null_int, one);
  expect_null_error(&and_cal, "could not parse \"\" field and_cal, get null value");
  FieldFuncOrCal or_cal(null_int, one);
  expect_null_error(&or_cal, "could not parse \"\" field or_cal, get null value");
  FieldFuncNotCal not_cal(null_int, one);
  expect_null_error(&not_cal, "could not parse \"\" field not_cal, get null value");
  FieldFuncLeftShift left_shift(null_int, one);
  expect_null_error(&left_shift, "could not parse \"\" field left shift, get null value");
  FieldFuncRightShift right_shift(null_int, one);
  expect_null_error(&right_shift, "could not parse \"\" field right_shift, get null value");

  SafeDeletePointer(null_int);
  SafeDeletePointer(one);
}

}  // namespace kwdbts
