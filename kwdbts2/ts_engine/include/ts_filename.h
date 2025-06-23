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
#include <cstdint>
#include <string>

#include "data_type.h"
#include "ts_version.h"
namespace kwdbts {
inline std::string LastSegmentFileName(uint64_t file_number) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "last.ver-%012lu", file_number);
  return buffer;
}

inline std::string VGroupDirName(uint32_t vgroup_id) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "vg_%03u", vgroup_id);
  return buffer;
}

inline std::string PartitionDirName(PartitionIdentifier partition_id) {
  char buffer[64];
  auto [database_id, start] = partition_id;
  std::snprintf(buffer, sizeof(buffer), "db%02d-%014ld", database_id, start);
  return buffer;
}

};  // namespace kwdbts
