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
// Created by liuxiupeng on 2024/01/04.
//

#include "ee_cpuinfo.h"

#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "lg_api.h"

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

namespace kwdbts {

k_int32 CpuInfo::num_cores_ = 1;
k_bool CpuInfo::initialized_ = false;

void CpuInfo::Init() {
  if (initialized_) return;
  num_cores_ = std::thread::hardware_concurrency();
  int num_cores = 0;

  // Read from /sys/fs/cgroup/cpu/cpu.cfs_quota_us
  std::ifstream quota_info("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
  if (quota_info.is_open()) {
    std::string quota_str;
    quota_info >> quota_str;
    quota_info.close();
    k_int32 quota = std::stoi(quota_str);
    if (quota != -1) {
      std::ifstream period_info("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
      if (period_info.is_open()) {
        std::string period_info_str;
        period_info >> period_info_str;
        period_info.close();
        k_int32 period = std::stoi(period_info_str);
        num_cores = quota / period;
      }
    }
  } else {
    std::ifstream file("/sys/fs/cgroup/cpu.max");
    if (file.is_open()) {
      std::string line;
      std::getline(file, line);
      file.close();
      try {
        int pos = line.find(" ");
        int quota = stoi(line.substr(0, pos));
        int period = stoi(line.substr(pos + 1));
        num_cores = quota / period < 1 ? 1 : quota / period;
      } catch (...) {
        LOG_ERROR("Read cpu.max failed, content: %s", line.c_str());
      }
    }
  }

  if (num_cores > 0) {
    num_cores_ = num_cores;
  }
  initialized_ = true;
}

}  // namespace kwdbts
