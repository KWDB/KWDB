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

#include "cm_module.h"

#include "gtest/gtest.h"

GTEST_API_ int main(int argc, char **argv) {
  std::cout << "Running main() from gtest_main.cc\n";

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class ModuleTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(ModuleTest, KwdbModuleToStr) {
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::ST), "st");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::EE), "ee");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::TH), "th");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::MM), "mm");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::CN), "cn");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::PS), "ps");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::SV), "sv");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::ME), "me");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::CM), "cm");
  EXPECT_STREQ(KwdbModuleToStr(kwdbts::KwdbModule::NW), "nw");
}
TEST_F(ModuleTest, GetModuleByName) {
  kwdbts::KwdbModule module;
  EXPECT_EQ(GetModuleByName(nullptr, &module), kwdbts::KStatus::FAIL);
  EXPECT_EQ(GetModuleByName("cn", static_cast<kwdbts::KwdbModule *>(nullptr)),
            kwdbts::KStatus::FAIL);
  // Invalid module name
  EXPECT_EQ(GetModuleByName("ab", &module), kwdbts::KStatus::FAIL);
  EXPECT_EQ(GetModuleByName("cnn", &module), kwdbts::KStatus::FAIL);
  // Valid module name
  EXPECT_EQ(GetModuleByName("cn", &module), kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::CN);
  EXPECT_EQ(GetModuleByName("mm", &module), kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::MM);
  EXPECT_EQ(GetModuleByName("cm", &module), kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::CM);
}
TEST_F(ModuleTest, GetModuleByFileName) {
  kwdbts::KwdbModule module;

  EXPECT_EQ(GetModuleByFileName(nullptr, &module), kwdbts::KStatus::FAIL);
  EXPECT_EQ(GetModuleByFileName("cn", static_cast<kwdbts::KwdbModule *>(nullptr)),
            kwdbts::KStatus::FAIL);
  // Invalid module name
  EXPECT_EQ(GetModuleByFileName("ab", &module), kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::UN);

  EXPECT_EQ(GetModuleByFileName("cn_conn.cpp", &module),
            kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::CN);

  EXPECT_EQ(GetModuleByFileName("src/cn_conn.cpp", &module),
            kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::CN);

  EXPECT_EQ(GetModuleByFileName("/kwdbts/server/src/cn_conn.cpp", &module),
            kwdbts::KStatus::SUCCESS);
  EXPECT_EQ(module, kwdbts::KwdbModule::CN);
}
