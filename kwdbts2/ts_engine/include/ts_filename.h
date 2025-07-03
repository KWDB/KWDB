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
#include <string_view>

#include "ts_version.h"
namespace kwdbts {

const char entity_item_file_name[] = "header.e";
const char block_item_file_name[] = "header.b";
const char block_data_file_name[] = "block";
const char block_agg_file_name[] = "agg";

inline std::string LastSegmentFileName(uint64_t file_number) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "last.ver-%012lu", file_number);
  return buffer;
}

inline std::string EntityHeaderFileName(uint64_t file_number) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "%s.ver-%012lu", entity_item_file_name, file_number);
  return buffer;
}

inline std::string VGroupDirName(uint32_t vgroup_id) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "vg_%03u", vgroup_id);
  return buffer;
}

inline std::string PartitionDirName(PartitionIdentifier partition_id) {
  char buffer[64];
  auto [database_id, start, _] = partition_id;
  std::snprintf(buffer, sizeof(buffer), "db%05d_%+014ld", database_id, start);
  return buffer;
}

inline std::string VersionUpdateName(uint64_t file_number) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "TSVERSION-%012lu", file_number);
  return buffer;
}

inline std::string CurrentVersionName() { return "CURRENT"; }

inline std::string TempFileName(const std::string& filename) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), ".%s.kwdbts", filename.data());
  return buffer;
}

};  // namespace kwdbts
