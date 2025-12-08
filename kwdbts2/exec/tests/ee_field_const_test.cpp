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

TEST_F(TestFieldConst, TestFieldConstDoubleFunc) {
  k_double64 data = 1.0;
  FieldConstDouble *field = new FieldConstDouble(roachpb::DataType::DOUBLE, data);
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 1);
  k_double64 dval = field->ValReal();
  EXPECT_DOUBLE_EQ(dval, 1.0);
  SafeDeletePointer(field);
}

TEST_F(TestFieldConst, TestFieldConstIntervalFunc) {
  KString str_type("5d");
  FieldConstInterval *field = new FieldConstInterval(roachpb::DataType::TIMESTAMP, str_type);
  k_int64 value = field->ValInt();
  EXPECT_EQ(value, 432000000);
  k_int64 data = 1365781921080;
  value = field->ValInt(&data, KTRUE);
  EXPECT_EQ(value, 1365349921080);
  SafeDeletePointer(field);
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
