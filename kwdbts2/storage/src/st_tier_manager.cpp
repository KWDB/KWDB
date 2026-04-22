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
#include <array>
#include <cstdlib>
#include <chrono>
#include "sys_utils.h"
#include "st_wal_mgr.h"

namespace kwdbts {
constexpr char tier_disk_partition_root_path[] = "partitions";
constexpr int partition_directory_level = 2;
constexpr char path_separator = '/';
constexpr char flatten_separator = '_';

int TsTierPartitionManager::CalculateTierLevelByTimestamp(timestamp64 p_max_ts) {
    return 0;
}

std::string TsTierPartitionManager::parsePartitionDirToOneLevel(const std::string& partition_full_path) {
  std::array<std::pair<size_t, size_t>, partition_directory_level> segments{};
  size_t segment_count = 0;
  size_t scan_end = partition_full_path.size();

  while (scan_end > 0 && partition_full_path[scan_end - 1] == path_separator) {
    --scan_end;
  }
  if (scan_end == 0) {
    return "";
  }

  while (scan_end > 0 && segment_count < segments.size()) {
    size_t slash_pos = partition_full_path.rfind(path_separator, scan_end - 1);
    size_t segment_begin = slash_pos == std::string::npos ? 0 : slash_pos + 1;
    if (segment_begin < scan_end) {
      segments[segment_count++] = {segment_begin, scan_end - segment_begin};
    }
    if (slash_pos == std::string::npos) {
      break;
    }
    scan_end = slash_pos;
    while (scan_end > 0 && partition_full_path[scan_end - 1] == path_separator) {
      --scan_end;
    }
  }

  size_t reserved_size = segment_count == 0 ? 0 : segment_count - 1;
  for (size_t i = 0; i < segment_count; ++i) {
    reserved_size += segments[i].second;
  }

  std::string one_level_name;
  one_level_name.reserve(reserved_size);
  for (size_t i = segment_count; i > 0; --i) {
    if (!one_level_name.empty()) {
      one_level_name += flatten_separator;
    }
    one_level_name.append(partition_full_path, segments[i - 1].first, segments[i - 1].second);
  }
  return one_level_name;
}

KStatus TsTierPartitionManager::MakePartitionDir(const std::string& partition_full_path, int level,
                                                 ErrorInfo& error_info) {
  if (!MakeDirectory(partition_full_path, error_info)) {
    return KStatus::FAIL;
  }
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

}  // namespace kwdbts
