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

#include <string>
#include <vector>
#include "st_tier.h"

extern uint64_t g_duration_level0;
extern uint64_t g_duration_level1;

namespace kwdbts {

class LoggedTsEntityGroup;

class TsTierPartitionManager {
 public:
  static TsTierPartitionManager& GetInstance() {
    static TsTierPartitionManager instance_;
    return instance_;
  }

  void SetWALMgr(LoggedTsEntityGroup* wal) {
    wal_mgr_ = wal;
    ctx_ = &ctx_obj_;
    InitKWDBContext(ctx_);
  }

  static int CalculateTierLevelByTimestamp(timestamp64 p_max_ts);

  /**
  * @brief Creates a partition directory at the specified level if tiering is configured
  *
  * @param partition_full_path The full path of the partition to be created.
  * @param level The tier level at which the partition should be created.
  * @param error_info Reference to an ErrorInfo object to store error details if any.
  * @return KStatus::SUCCESS if the directory is created successfully, otherwise KStatus::FAIL.
  */
  KStatus MakePartitionDir(std::string partition_full_path, int level, ErrorInfo& error_info);

  /**
  * @brief Removes a partition directory.
  *
  * This function remove the specified partition directory, if it's a symbolic link, remove the absolute path first.
  *
  * @param partition_full_path The full path of the partition directory to be removed.
  * @param error_info Reference to an ErrorInfo object to store error details if the operation fails.
  * @return KStatus::SUCCESS if the operation is successful, otherwise KStatus::FAIL.
  */
  KStatus RMPartitionDir(std::string partition_full_path, ErrorInfo& error_info);

  /**
  * @brief Moves a partition directory to a specified tier level.
  *
  * @param partition_full_path The full path of the partition to be moved.
  * @param to_level The tier level to which the partition should be moved.
  * @param error_info Reference to an ErrorInfo object to store error details if any.
  * @return KStatus::SUCCESS if the directory is moved successfully, otherwise KStatus::FAIL.
  */
  KStatus MVPartitionDir(const std::string& partition_full_path, std::vector<std::string>& files, int to_level,
                         std::function<bool(std::function<bool()>)> f, ErrorInfo& error_info);

  /**
  * @brief Retrieves the tier level of a given partition.
  *
  * This function determines the tier level of the specified partition by parsing its link path.
  *
  * @param partition_full_path The full path of the partition.
  * @param level Pointer to an integer where the tier level will be stored.
  * @param error_info Reference to an ErrorInfo object to store error details if any.
  * @return KStatus::SUCCESS if the operation is successful, otherwise KStatus::FAIL.
  */
  KStatus GetPartitionCurrentLevel(const std::string& partition_full_path, int* level, ErrorInfo& error_info);

  KStatus GetPartitionCount(int level, const std::string& disk_path, int* count, ErrorInfo& error_info);

  KStatus Recover(const std::string &link_path, const std::string& tier_path, ErrorInfo& error_info);

 private:
  std::string parsePartitionDirToOneLevel(const std::string& partition_full_path);

 private:
  LoggedTsEntityGroup* wal_mgr_{nullptr};
  kwdbContext_p ctx_;
  kwdbContext_t ctx_obj_;
};

}  // namespace kwdbts
