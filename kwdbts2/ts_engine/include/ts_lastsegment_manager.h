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
#include <string>
#include <vector>

#include "kwdb_type.h"
#include "lt_rw_latch.h"
#include "ts_lastsegment.h"

namespace kwdbts {

const uint32_t MAX_LAST_SEGMENT_NUM = 3;

class TsLastSegmentManager {
 private:
  std::filesystem::path dir_path_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  KRWLatch rw_latch_;

  std::atomic<uint32_t> ver_ = 0;
  std::atomic<uint32_t> n_lastsegment_ = 0;

  int rdLock() { return RW_LATCH_S_LOCK(&rw_latch_); }
  int wrLock() { return RW_LATCH_X_LOCK(&rw_latch_); }
  int unLock() { return RW_LATCH_UNLOCK(&rw_latch_); }

 public:
  explicit TsLastSegmentManager(const string& dir_path)
      : dir_path_(dir_path), rw_latch_(RWLATCH_ID_LAST_SEGMENT_MANAGER_RWLOCK) {}

  ~TsLastSegmentManager() {}

  KStatus NewLastSegment(std::unique_ptr<TsLastSegment>* last_segment);
  void TakeLastSegmentOwnership(std::unique_ptr<TsLastSegment>&& last_segment);

  void GetCompactLastSegments(std::vector<std::shared_ptr<TsLastSegment>>& result);

  bool NeedCompact();

  void ClearLastSegments(uint32_t ver);
};

}  // namespace kwdbts
