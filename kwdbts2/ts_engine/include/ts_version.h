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

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <utility>

#include "data_type.h"
#include "ts_entity_segment.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {

class TsVGroupVersion;
class TsEngineVersion {
 private:
  std::vector<std::shared_ptr<const TsVGroupVersion>> children_;

 public:
  int GetNumOfVGroups() const;
  std::shared_ptr<const TsVGroupVersion> GetVGroupVersion(int id) const;
};

class TsVGroupVersion {
 private:
  using PartitionInfo = std::pair<uint32_t, timestamp64>;
  std::map<PartitionInfo, std::vector<uint64_t>> valid_lastseg_;

 public:
  std::shared_ptr<TsVGroupPartition> GetPartition(uint32_t db_id, timestamp64 p_time) const;
  std::vector<PartitionInfo> GetAllValidPartitions() const;
};

class TsVersionUpdate {
  friend class TsVGroupVersionManager;

 private:
 public:
  void AddLastSegmentFile(uint32_t db_id, timestamp64 ptime, uint32_t file_number);
  void DeleteFile(const std::string& path);
};

class TsVGroupVersionManager;
class TsEngineVersionManager {
  friend TsVGroupVersionManager;

 private:
  uint64_t version_seqno_;

  void ApplyUpdateInner(uint64_t);

 public:
  std::shared_ptr<const TsEngineVersion> Current() const;
};

class TsVGroupVersionManager {
 private:
  int vgroup_id_;
  std::filesystem::path path_;
  TsEngineVersionManager* root_;

 public:
  explicit TsVGroupVersionManager(int vgroup_id) : vgroup_id_(vgroup_id) {}
  void ApplyUpdate(const TsVersionUpdate& update) {
    auto current = root_->Current();
    auto vg_version = current->GetVGroupVersion(vgroup_id_);
    auto all_partitions = vg_version->GetAllValidPartitions();
  }
};

enum class RecordType {
  k,
};

}  // namespace kwdbts