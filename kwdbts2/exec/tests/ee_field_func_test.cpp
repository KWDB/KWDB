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

#include "ee_field_const.h"
#include "ee_global.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestFieldFunc : public testing::Test {
 protected:
  static void SetUpTestCase() {
  }

  static void TearDownTestCase() {
  }
};

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

}  // namespace kwdbts
