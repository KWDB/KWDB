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
#include "lg_severity.h"

// GTEST_API_ int main(int argc, char **argv) {
//  std::cout << "Running main() from gtest_main.cc\n";
//
//  testing::InitGoogleTest(&argc, argv);
//  return RUN_ALL_TESTS();
// }

class SeverityTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(SeverityTest, SeverityToStr) {
  EXPECT_STREQ(SeverityToStr(kwdbts::LogSeverity::DEBUG), "D");
  EXPECT_STREQ(SeverityToStr(kwdbts::LogSeverity::INFO), "I");
  EXPECT_STREQ(SeverityToStr(kwdbts::LogSeverity::WARN), "W");
  EXPECT_STREQ(SeverityToStr(kwdbts::LogSeverity::ERROR), "E");
  EXPECT_STREQ(SeverityToStr(kwdbts::LogSeverity::FATAL), "F");
}
