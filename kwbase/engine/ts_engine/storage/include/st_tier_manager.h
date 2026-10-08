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
  KStatus MakePartitionDir(const std::string& partition_full_path, int level, ErrorInfo& error_info);


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

 private:
  std::string parsePartitionDirToOneLevel(const std::string& partition_full_path);

 private:
  LoggedTsEntityGroup* wal_mgr_{nullptr};
  kwdbContext_p ctx_;
  kwdbContext_t ctx_obj_;
};

}  // namespace kwdbts
