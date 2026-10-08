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
#include "gtest/gtest.h"
#include "ee_ast_element_type.h"
namespace kwdbts {
class ElementTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
// Test the constructor and type settings of the Element class
TEST(ElementTest, ConstructorAndTypeSetting) {
  Element element(INT_TYPE, KFALSE);
  EXPECT_EQ(element.operators, INT_TYPE);
  EXPECT_FALSE(element.is_operator);
  EXPECT_FALSE(element.is_func);
  EXPECT_FALSE(element.is_negative);

  element.SetType(FLOAT_TYPE);
  EXPECT_EQ(element.operators, FLOAT_TYPE);
}

// Test the negative setting of the Element class
TEST(ElementTest, NegativeSetting) {
  Element element(INT_TYPE, KFALSE);
  EXPECT_FALSE(element.is_negative);

  element.SetNegative(KTRUE);
  EXPECT_TRUE(element.is_negative);
}

// Test the function settings of the Element class
TEST(ElementTest, FunctionSetting) {
  Element element(INT_TYPE, KFALSE);
  EXPECT_FALSE(element.is_func);

  element.SetFunc(KTRUE);
  EXPECT_TRUE(element.is_func);
}

// Test how the Element class behaves in storing different types of data
TEST(ElementTest, ValueStorage) {
  Element int_element(INT_TYPE, 42);
  int_element.value.number.int_type = 42;
  EXPECT_EQ(int_element.value.number.int_type, 42);

  Element string_element(KString("test"));
  EXPECT_STREQ(string_element.value.string_type.c_str(), "test");

// The test stores floating-point values
  Element float_element(1.23f);
  EXPECT_FLOAT_EQ(float_element.value.number.float_type, 1.23f);

  // The test stores double-precision floating-point values
  Element double_element(1.23456789);
  EXPECT_DOUBLE_EQ(double_element.value.number.decimal, 1.23456789);
}

}  // namespace kwdbts


