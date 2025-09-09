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

#include <chrono>
#include <fstream>
#include <thread>
#include <unordered_set>
#include <random>
#include <ctime>
#include "st_tier.h"
#include "sys_utils.h"


namespace kwdbts {
constexpr char tier_cfg_file[] = "ts-tier.cfg";

KStatus TsTier::Init(const std::string& ts_store_path) {
  return SUCCESS;
}

}  // namespace kwdbts
