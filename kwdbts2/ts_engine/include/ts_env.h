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

#pragma once

#include <map>
#include <mutex>
#include "ts_compressor.h"
#include "ts_common.h"

namespace kwdbts {
class TsEnvInstance {
 private:
  std::mutex obj_create_mutex_;
  ColumnCompressorMgr* compressor_mgr_{nullptr};


 private:
  TsEnvInstance() = default;

 public:
  static TsEnvInstance& GetInstance() {
    static TsEnvInstance instance;
    return instance;
  }
  TsEnvInstance(const TsEnvInstance&) = delete;
  TsEnvInstance& operator=(const TsEnvInstance&) = delete;

  bool SetCompressorPolicy(ColumnCompressorPolicy policy) {
    return Compressor()->ResetPolicy(policy);
  }

  ColumnCompressorMgr* Compressor() {
    if (compressor_mgr_ != nullptr) {
      return compressor_mgr_;
    }
    obj_create_mutex_.lock();
    if (compressor_mgr_ == nullptr) {
      auto ins = new ColumnCompressorMgr();
      if (ins->Init()) {
        compressor_mgr_ = ins;
      } else {
        LOG_ERROR("ColumnCompressorMgr init failed.");
      }
    }
    obj_create_mutex_.unlock();
    return compressor_mgr_;
  }
};

}  //  namespace kwdbts
