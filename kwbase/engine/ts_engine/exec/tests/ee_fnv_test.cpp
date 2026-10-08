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

#include "ee_fnv.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestFnv : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
TEST_F(TestFnv, TestFnvMathFunc) {
  const char* fnvmsg = "abc123";
  size_t len = 6;
  kwdbts::k_uint32 icode32 = kwdbts::fnv1_hash32(fnvmsg, len);
  EXPECT_EQ(icode32, 3613024805);
  icode32 = kwdbts::fnv1a_hash32(fnvmsg, len);
  EXPECT_EQ(icode32, 951228933);
  kwdbts::k_uint64 icode64 = kwdbts::fnv1_hash64(fnvmsg, len);
  EXPECT_EQ(icode64, 2635828873713441413);
  icode64 = kwdbts::fnv1a_hash64(fnvmsg, len);
  EXPECT_EQ(icode64, 7119243511811735397);
}

}  // namespace kwdbts
