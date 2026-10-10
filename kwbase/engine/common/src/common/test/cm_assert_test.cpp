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

#include "gtest/gtest.h"

#ifndef K_DEBUG
// Undefine the K_DEBUG macro to unit test Assert and AssertWithReturnValue in Release mode
#define KWDBTS_SERVER_SRC_COMMON_TEST_CM_ASSERT_RELEASE_TEST_K_DEBUG_NOT_DEFINED_
#define K_DEBUG
#endif

#include "cm_assert.h"

namespace kwdbts {

class TestKWDBAssert: public ::testing::Test {
 public:
  int testAssertReturn(bool test_for_assert_with_return, int err) {
    kwdbContext_t kwdb_context;
    kwdbContext_p ctx = &kwdb_context;
    InitServerKWDBContext(ctx);
    AssertWithReturnValue(test_for_assert_with_return, err);
    return 0;
  }
};

// Test normal
TEST_F(TestKWDBAssert, sampleTestTrue) {
  int test_just_test_a = 3;
  Assert(test_just_test_a > 2);
  int ret = testAssertReturn(test_just_test_a > 2, 3);
  EXPECT_EQ(ret, 0);
}

}  //  namespace kwdbts

#ifdef KWDBTS_SERVER_SRC_COMMON_TEST_CM_ASSERT_RELEASE_TEST_K_DEBUG_NOT_DEFINED_
// Restores the K_DEBUG macro definition
#undef K_DEBUG
#endif
