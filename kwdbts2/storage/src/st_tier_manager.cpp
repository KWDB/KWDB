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

#include "st_tier_manager.h"
#include <dirent.h>
#include <unistd.h>
#include <cstdlib>
#include <chrono>
#include "sys_utils.h"
#include "st_wal_mgr.h"

namespace kwdbts {
constexpr char tier_disk_partition_root_path[] = "partitions";
constexpr char tier_disk_partition_list_file[] = "tier_partition.lst";
constexpr int partition_directory_level = 4;

int TsTierPartitionManager::CalculateTierLevelByTimestamp(timestamp64 p_max_ts) {
    return 0;
}

std::string TsTierPartitionManager::parsePartitionDirToOneLevel(const std::string& partition_full_path) {
  int scan_level = 0;
  int end_idx = partition_full_path.size();
  std::string OneLevelName;

  for (int i = partition_full_path.size() - 1; i >= 0; --i) {
    if (partition_full_path[i] == '/') {
      if (i + 1 < end_idx) {
        OneLevelName = partition_full_path.substr(i + 1, end_idx - i - 1) + (scan_level > 0 ? "_" : "") + OneLevelName;
        ++scan_level;
        if (scan_level >= partition_directory_level) {
          break;
        }
      }
      end_idx = i;
    }
  }
  return OneLevelName;
}

KStatus TsTierPartitionManager::MakePartitionDir(std::string partition_full_path, int level,
                                                 ErrorInfo& error_info) {
  if (!MakeDirectory(partition_full_path, error_info)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTierPartitionManager::RMPartitionDir(std::string partition_full_path, ErrorInfo& error_info) {
  error_info.clear();
  // A soft link to a directory cannot be deleted with a slash, remove the last '/', then exec system()
  if (partition_full_path.back() == '/') {
    partition_full_path = partition_full_path.substr(0, partition_full_path.length() - 1);
  }
  std::string real_path = ParseLinkDirToReal(partition_full_path, error_info);
  if (error_info.errcode != 0) {
    LOG_ERROR("parse link path [%s] failed. errmsg: %s", partition_full_path.c_str(), error_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  if (!Remove(real_path, error_info)) {
    LOG_ERROR("remove [%s] failed. errmsg: %s", real_path.c_str(), error_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  if (!System("rm -rf " + partition_full_path)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTierPartitionManager::MVPartitionDir(const std::string& partition_full_path, std::vector<std::string>& files,
                                               int to_level, std::function<bool(std::function<bool()>)> f,
                                               ErrorInfo& error_info) {
  return KStatus::SUCCESS;
}

KStatus TsTierPartitionManager::GetPartitionCurrentLevel(const std::string& partition_full_path, int* level,
                                                  ErrorInfo& error_info) {
  *level = 0;
  return KStatus::SUCCESS;
}

KStatus TsTierPartitionManager::GetPartitionCount(int level, const std::string& disk_path, int* count,
                                                  ErrorInfo& error_info) {
  return KStatus::SUCCESS;
}

KStatus TsTierPartitionManager::Recover(const std::string &link_path, const std::string& tier_path, ErrorInfo& error_info) {
  std::string real_path = ParseLinkDirToReal(link_path, error_info);
  if (real_path.compare(tier_path) == 0) {
    return KStatus::SUCCESS;
  }
  Remove(tier_path, error_info);
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
