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

#define private public
#include "ee_cpuinfo.h"
#undef private

namespace kwdbts {
class TestCpuinfo : public ::testing::Test {
 protected:
  void SetUp() override {
    CpuInfo::initialized_ = false;
    CpuInfo::num_cores_ = 1;
  }
};

TEST_F(TestCpuinfo, TestGetCores) {
  EXPECT_EQ(CpuInfo::Get_Num_Cores(), 0);
  CpuInfo::Init();
  EXPECT_NE(CpuInfo::Get_Num_Cores(), 0);

  const k_int32 first_init_value = CpuInfo::Get_Num_Cores();
  CpuInfo::Init();
  EXPECT_EQ(CpuInfo::Get_Num_Cores(), first_init_value);
}

TEST_F(TestCpuinfo, InitReturnsImmediatelyWhenAlreadyInitialized) {
  CpuInfo::initialized_ = true;
  CpuInfo::num_cores_ = 4;
  CpuInfo::Init();
  EXPECT_EQ(CpuInfo::Get_Num_Cores(), 4);
}
}  // namespace kwdbts
