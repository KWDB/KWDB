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

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "data_type.h"
#include "kwdb_type.h"
#include "ts_entity_segment.h"
#include "ts_lastsegment.h"
#include "ts_mem_segment_mgr.h"
#include "ts_segment.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {
using PartitionInfo = std::pair<uint32_t, timestamp64>;  // (dbid, ptime);

class TsPartitionVersion {
 private:
  std::vector<std::shared_ptr<TsMemSegment>> mem_segments_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  std::shared_ptr<TsEntitySegment> entity_segment_;

  uint32_t database_id_;
  timestamp64 start_time_, end_time_;

 public:
  explicit TsPartitionVersion(uint32_t database_id, timestamp64 start_time, timestamp64 end_time)
      : database_id_(database_id), start_time_(start_time), end_time_(end_time) {}

  std::vector<std::shared_ptr<TsSegmentBase>> GetAllSegments() const {
    std::vector<std::shared_ptr<TsSegmentBase>> result;
    result.reserve(mem_segments_.size() + last_segments_.size() + 1);
    result.insert(result.end(), mem_segments_.begin(), mem_segments_.end());
    result.insert(result.end(), last_segments_.begin(), last_segments_.end());
    result.push_back(entity_segment_);
    return result;
  }
};
class TsVGroupVersion {
  friend class TsVersionManager;

 private:
  std::vector<std::shared_ptr<const TsPartitionVersion>> partitions_;

 public:
  std::vector<std::shared_ptr<const TsPartitionVersion>> GetAllPartitions() const {
    return partitions_;
  }
};

class TsVersionUpdate {
  friend class TsVersionManager;

 private:
  std::map<PartitionInfo, uint64_t> valid_lastsegs_;
  std::map<PartitionInfo, uint64_t> delete_lastsegs_;
  std::shared_ptr<TsMemSegment> valid_memseg_;

 public:
  void AddLastSegment(uint32_t dbid, timestamp64 ptime, uint64_t file_no) {
    valid_lastsegs_.insert({PartitionInfo{dbid, ptime}, file_no});
  }
  void DeleteLastSegment(uint32_t dbid, timestamp64 ptime, uint64_t file_no) {
    delete_lastsegs_.insert({PartitionInfo{dbid, ptime}, file_no});
  }
  void ResetMemSegment(std::shared_ptr<TsMemSegment> mem) { valid_memseg_ = mem; }
};

class TsVersionManager {
 private:
  mutable std::mutex mu_;

  std::shared_ptr<const TsVGroupVersion> current_;

 public:
  KStatus Recover();
  void ApplyUpdate(const TsVersionUpdate &update) { std::unique_lock lk{mu_}; }
  std::shared_ptr<const TsVGroupVersion> Current() const {
    std::unique_lock lk{mu_};
    return current_;
  }
};

}  // namespace kwdbts