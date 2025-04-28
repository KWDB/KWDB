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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>

#include "data_type.h"
#include "kwdb_type.h"
#include "ts_entity_segment.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {

class TsVGroupVersion;
class TsEngineVersion {
 private:
  std::vector<std::shared_ptr<const TsVGroupVersion>> children_;
};

class TsVGroupVersion {
 private:
  struct PartitionInfo {
    uint32_t db_id;
    timestamp64 ptime;
    bool operator<(const PartitionInfo& rhs) const {
      return db_id < rhs.db_id || (db_id == rhs.db_id && (ptime < rhs.ptime));
    }
  };
  std::map<PartitionInfo, std::vector<uint64_t>> valid_lastseg_;

 public:
  std::shared_ptr<TsVGroupPartition> GetPartition(uint32_t db_id, timestamp64 p_time) const;
};

class TsVersionUpdate {
 public:
  void AddLastSegment(uint32_t db_id, timestamp64 ptime, uint32_t file_number);
  void DeleteFile(const std::string& path);
};

class TsVGroupVersionManager;
class TsEngineVersionManager {
  friend TsVGroupVersionManager;

 private:
  std::vector<TsVGroupVersionManager*> children_;

 public:
  std::shared_ptr<const TsEngineVersion> current() const;
};

class TsVGroupVersionManager {
 private:
  std::filesystem::path path_;
  uint64_t lastseg_file_number_;
  TsEngineVersionManager* root_;

 public:
  explicit TsVGroupVersionManager(const std::string& root_path);
};

}  // namespace kwdbts