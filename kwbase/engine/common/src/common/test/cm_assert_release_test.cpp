// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <fstream>
#include "gtest/gtest.h"

#ifdef K_DEBUG
// Undefine the K_DEBUG macro to unit test Assert and AssertWithReturnValue in Release mode
#define KWDBTS_SERVER_SRC_COMMON_TEST_CM_ASSERT_RELEASE_TEST_K_DEBUG_DEFINED_
#undef K_DEBUG
#endif
#include "kwdb_type.h"
#include "cm_assert.h"

namespace kwdbts {

class TestKWDBAssert: public ::testing::Test {
 public:
  KStatus testAssertReturn(bool test_for_assert_with_return, int err) {
    AssertWithReturnValue(test_for_assert_with_return, err);
    return KStatus::SUCCESS;
  }
};

// Test normal
TEST_F(TestKWDBAssert, sampleTestTrue) {
  int test_just_test_a = 3;
  Assert(test_just_test_a > 2);
  KStatus ret = testAssertReturn(test_just_test_a > 2, 3);
  EXPECT_EQ(ret, KStatus::SUCCESS);
}

// Test cases for Assert macros
TEST_F(TestKWDBAssert, AssertTrueAndFalse) {
  extern kwdbContext_p g_kwdb_context;
  remove(g_kwdb_context->assert_file_name);
  int test_just_test_a = 3;
  Assert(test_just_test_a > 2);
  Assert(test_just_test_a > 4);
  std::ifstream in(g_kwdb_context->assert_file_name, std::ios::in);
  std::istreambuf_iterator<char> beg(in), end;
  std::string a(beg, end);
  in.close();
  EXPECT_NE(a.length(), 0);
  int ops = a.find("test_just_test_a > 4");
  EXPECT_GT(ops, 0);
  ops = a.find("test_just_test_a > 2");
  EXPECT_EQ(ops, -1);
}

// AssertWithReturnValue Correct test case
TEST_F(TestKWDBAssert, AssertRetTrue) {
  extern kwdbContext_p g_kwdb_context;
  remove(g_kwdb_context->assert_file_name);
  int test_just_test_a = 3;
  KStatus err = testAssertReturn(test_just_test_a > 2, 3);
  EXPECT_EQ(err, KStatus::SUCCESS);
  int ret = access(g_kwdb_context->assert_file_name, F_OK);
  EXPECT_EQ(ret, -1);
}

// AssertWithReturnValue Error test case
TEST_F(TestKWDBAssert, AssertRetFalse) {
  extern kwdbContext_p g_kwdb_context;
  remove(g_kwdb_context->assert_file_name);
  int test_just_test_a = 3;
  KStatus err = testAssertReturn(test_just_test_a > 4, 3);
  EXPECT_EQ(err, KStatus::FAIL);
  std::ifstream in(g_kwdb_context->assert_file_name, std::ios::in);
  std::istreambuf_iterator<char> beg(in), end;
  std::string a(beg, end);
  EXPECT_NE(a.length(), 0);
  int ops = a.find("test_for_assert_with_return");
  EXPECT_GT(ops, 0);
}

}  //  namespace kwdbts

#ifdef KWDBTS_SERVER_SRC_COMMON_TEST_CM_ASSERT_RELEASE_TEST_K_DEBUG_DEFINED_
// Restores the K_DEBUG macro definition
#define K_DEBUG
#endif
