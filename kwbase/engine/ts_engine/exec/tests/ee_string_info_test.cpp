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

#include "ee_string_info.h"

#include "gtest/gtest.h"

class TestStringInfo : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestStringInfo, TestCreateStringInfo) {
  kwdbts::EE_StringInfo esi = kwdbts::ee_makeStringInfo();
  EXPECT_EQ((esi != nullptr && esi->len == 0), true);
  kwdbts::KStatus status = kwdbts::ee_sendint(esi, 1, 1);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  EXPECT_EQ(esi->len, 1);

  status = kwdbts::ee_sendint(esi, 2147483647, 2);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  EXPECT_EQ(esi->len, 3);

  status = kwdbts::ee_sendint(esi, 2147483647, 4);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  EXPECT_EQ(esi->len, 7);
  free(esi->data);
  delete esi;
}
