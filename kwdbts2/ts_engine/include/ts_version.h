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
#include <map>
#include <memory>

#include "data_type.h"
#include "kwdb_type.h"
#include "settings.h"
#include "ts_lastsegment.h"
#include "ts_mem_segment_mgr.h"
#include "ts_segment.h"

namespace kwdbts {
using DatabaseID = uint32_t;
using PartitionIdx = int64_t;
using PartitionIdentifier = std::tuple<DatabaseID, timestamp64>;  // (dbid, start_time);

class TsVGroupVersion;
class TsEntitySegment;
class TsPartitionVersion {
  friend class TsVersionManager;
  friend class TsVGroupVersion;

 private:
  std::list<std::shared_ptr<TsMemSegment>> mem_segments_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  std::shared_ptr<TsEntitySegment> entity_segment_;

  timestamp64 start_time_, end_time_;

  PartitionIdentifier partition_info_;

  bool directory_created_ = false;

  // Only TsVersionManager can create TsPartitionVersion
  explicit TsPartitionVersion(timestamp64 start_time, timestamp64 end_time,
                              PartitionIdentifier partition_info)
      : start_time_(start_time), end_time_(end_time), partition_info_(partition_info) {}

 public:
  TsPartitionVersion(const TsPartitionVersion &) = default;
  TsPartitionVersion &operator=(const TsPartitionVersion &) = default;
  TsPartitionVersion(TsPartitionVersion &&) = default;

  std::vector<std::shared_ptr<TsSegmentBase>> GetAllSegments() const;

  bool HasDirectoryCreated() const { return directory_created_; }

  DatabaseID GetDatabaseID() const { return std::get<0>(partition_info_); }

  timestamp64 GetStartTime() const { return start_time_; }
  timestamp64 GetEndTime() const { return end_time_; }

  PartitionIdentifier GetPartitionIdentifier() const { return partition_info_; }

  bool NeedCompact() const { return last_segments_.size() > EngineOptions::max_last_segment_num; }
  std::vector<std::shared_ptr<TsLastSegment>> GetCompactLastSegments() const;

  std::shared_ptr<TsEntitySegment> GetEntitySegments() const { return entity_segment_; }
};
class TsVGroupVersion {
  friend class TsVersionManager;

 private:
  std::map<PartitionIdentifier, std::shared_ptr<TsPartitionVersion>> partitions_;
  // TsVGroup *vgroup_ = nullptr;

 public:
  std::vector<std::shared_ptr<const TsPartitionVersion>> GetAllPartitions() const;
  std::vector<std::shared_ptr<const TsPartitionVersion>> GetAllPartitions(uint32_t dbid) const;

  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitionsToCompact() const;

  // timestamp is in ptime
  std::shared_ptr<const TsPartitionVersion> GetPartition(uint32_t dbid,
                                                         timestamp64 timestamp) const;

  // TsVGroup *GetVGroup() const { return vgroup_; }
};

class TsVersionUpdate {
  friend class TsVersionManager;

 private:
  std::set<PartitionIdentifier> partitions_to_create_;
  std::map<PartitionIdentifier, std::set<uint64_t>> new_lastsegs_;
  std::map<PartitionIdentifier, std::set<uint64_t>> delete_lastsegs_;
  std::list<std::shared_ptr<TsMemSegment>> valid_memseg_;
  std::shared_ptr<TsEntitySegment> entity_segment_;

  std::set<PartitionIdentifier> updated_partitions_;

  std::mutex mu_;

  bool empty_ = true;

 public:
  void PartitionDirCreated(const PartitionIdentifier &partition_id) {
    partitions_to_create_.insert(partition_id);
    updated_partitions_.insert(partition_id);
    empty_ = false;
  }
  void AddLastSegment(const PartitionIdentifier &partition_id, uint64_t file_number) {
    std::unique_lock lk{mu_};
    new_lastsegs_[partition_id].insert(file_number);
    updated_partitions_.insert(partition_id);
    empty_ = false;
  }
  void DeleteLastSegment(const PartitionIdentifier &partition_id, uint64_t file_number) {
    std::unique_lock lk{mu_};
    delete_lastsegs_[partition_id].insert(file_number);
    updated_partitions_.insert(partition_id);
    empty_ = false;
  }

  void SetValidMemSegments(const std::list<std::shared_ptr<TsMemSegment>> &mem) {
    valid_memseg_ = mem;
    empty_ = false;
  }

  void SetEntitySegment(std::shared_ptr<TsEntitySegment> entity_segment) {
    entity_segment_ = entity_segment;
    empty_ = false;
  }
};

class TsVersionManager {
 private:
  mutable std::shared_mutex mu_;

  std::shared_ptr<const TsVGroupVersion> current_;

  uint64_t next_file_number_ = 0;

  uint32_t vgroup_id_;

  const EngineOptions options_;

 public:
  explicit TsVersionManager(const EngineOptions &engine_options, uint32_t vgroup_id)
      : options_{engine_options}, vgroup_id_(vgroup_id) {}
  KStatus Recover() {
    current_ = std::make_shared<TsVGroupVersion>();
    return SUCCESS;
  }
  KStatus ApplyUpdate(const TsVersionUpdate &update);

  std::shared_ptr<const TsVGroupVersion> Current() const {
    std::shared_lock lk{mu_};
    return current_;
  }

  uint64_t NewFileNumber() { return next_file_number_++; }
  void AddPartition(DatabaseID dbid, timestamp64 start);
};

}  // namespace kwdbts