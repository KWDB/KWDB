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

#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include "ts_common.h"

namespace kwdbts {

class TsTierPartitionManager;
class TsTier {
 public:
  static TsTier& GetInstance() {
    static TsTier instance;
    return instance;
  }

  TsTier(const TsTier&) = delete;
  TsTier& operator=(const TsTier&) = delete;

  KStatus Init(const std::string& ts_store_path);

  bool TierEnabled() const {
    return tier_enabled_;
  }

 private:
  bool tier_enabled_ = false;

  TsTier() = default;
};


}  // namespace kwdbts
