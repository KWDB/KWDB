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

#include "ee_binary_expr.h"
#include "gtest/gtest.h"
namespace kwdbts {
class BinaryExprTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
// Test the default constructor of the Binarijecker class
TEST(BinaryExprTest, DefaultConstructor) {
  BinaryExpr expr;
  EXPECT_FALSE(expr.is_leaf);
  EXPECT_FALSE(expr.is_negative);
  EXPECT_EQ(expr.operator_type, OPENING_BRACKET);
  EXPECT_EQ(expr.reference_ptr, nullptr);
  EXPECT_EQ(expr.const_ptr, nullptr);
  EXPECT_EQ(expr.left, nullptr);
  EXPECT_EQ(expr.right, nullptr);
}

// Test the parametric constructor of the Binarijecker class
TEST(BinaryExprTest, ParameterizedConstructor) {
  BinaryExpr expr(KTRUE);
  EXPECT_TRUE(expr.is_leaf);
  EXPECT_FALSE(expr.is_negative);  // default false
  EXPECT_EQ(expr.operator_type, OPENING_BRACKET);
}

// Test the Settenega Tiff method of the Binarijeker class
TEST(BinaryExprTest, SetNegative) {
  BinaryExpr expr;
  EXPECT_FALSE(expr.is_negative);

  expr.SetNegative(KTRUE);
  EXPECT_TRUE(expr.is_negative);
}

// Test the sub-expression links of the BinaryExpr class
TEST(BinaryExprTest, SubexpressionsLinking) {
  ExprPtr left = std::make_shared<BinaryExpr>();
  ExprPtr right = std::make_shared<BinaryExpr>();
  BinaryExpr parent;
  parent.left = left;
  parent.right = right;

  EXPECT_EQ(parent.left, left);
  EXPECT_EQ(parent.right, right);
}

}  // namespace kwdbts
