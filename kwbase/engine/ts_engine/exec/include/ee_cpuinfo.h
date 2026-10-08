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
// Created by liuxiupeng .
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "kwdb_type.h"
namespace kwdbts {

// get cpu info
class CpuInfo {
 public:
  static void Init();
  static k_int32 Get_Num_Cores() {
    if (initialized_) {
      return num_cores_;
    }
    return 0;
  }

 private:
  static k_int32 num_cores_;  // cpu core num
  static k_bool initialized_;
};
};  // namespace kwdbts
