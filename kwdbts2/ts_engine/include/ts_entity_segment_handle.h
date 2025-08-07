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

// this file is just aimed to avoid circular dependency
// for ts_version.h and ts_entity_segment.h

#pragma once
#include <cstdint>
namespace kwdbts {
struct MetaFileInfo {
  uint64_t file_number;
  uint64_t length;
};

struct EntitySegmentHandleInfo {
  MetaFileInfo datablock_info;
  MetaFileInfo header_b_info;
  MetaFileInfo agg_info;
  uint64_t header_e_file_number;
};
}  // namespace kwdbts
