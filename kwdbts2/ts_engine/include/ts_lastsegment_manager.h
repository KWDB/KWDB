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

#include <fcntl.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "kwdb_type.h"
#include "lt_rw_latch.h"
#include "ts_lastsegment.h"

namespace kwdbts {

const uint32_t MAX_LAST_SEGMENT_NUM = 3;
const uint32_t MAX_COMPACT_NUM = 10;

class TsLastSegmentManager {
 private:
  std::filesystem::path dir_path_;
  std::map<uint32_t, std::shared_ptr<TsLastSegment>> last_segments_;

  std::atomic<uint32_t> current_file_number_ = 0;
  std::atomic<uint32_t> n_lastsegment_ = 0;

  std::shared_mutex s_mutex_;

  std::string LastSegmentFileName(uint32_t file_number) const;

 public:
  explicit TsLastSegmentManager(const string& dir_path) : dir_path_(dir_path) {}

  ~TsLastSegmentManager() {}

  KStatus NewLastSegmentFile(std::unique_ptr<TsFile>* last_segment, uint32_t* ver);
  KStatus OpenLastSegmentFile(uint32_t file_number, std::shared_ptr<TsLastSegment>* lastsegment);

  void GetCompactLastSegments(std::vector<std::shared_ptr<TsLastSegment>>& result);

  bool NeedCompact();

  void ClearLastSegments(uint32_t ver);
};

}  // namespace kwdbts
