// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

namespace kwdbts {

class PartitionIntervalRecorder {
 private:
  std::unordered_map<uint32_t, int64_t> partition_interval_map_;
  mutable std::shared_mutex mtx_;

 public:
  static PartitionIntervalRecorder* GetInstance() {
    static PartitionIntervalRecorder recorder;
    return &recorder;
  }

  PartitionIntervalRecorder() = default;

  KStatus RecordInterval(uint32_t db_id, int64_t interval) {
    {
      std::shared_lock lk{mtx_};
      if (partition_interval_map_.find(db_id) != partition_interval_map_.end()) {
        if (interval != partition_interval_map_[db_id]) {
          LOG_ERROR("Partition interval of db_id %d is %ld, not %ld", db_id, partition_interval_map_[db_id], interval);
          return FAIL;
        }
        return SUCCESS;
      }
    }
    std::unique_lock lk{mtx_};
    if (partition_interval_map_.find(db_id) != partition_interval_map_.end()) {
      if (interval != partition_interval_map_[db_id]) {
        LOG_ERROR("Partition interval of db_id %d is %ld, not %ld", db_id, partition_interval_map_[db_id], interval);
        return FAIL;
      }
      return SUCCESS;
    }
    partition_interval_map_[db_id] = interval;
    return SUCCESS;
  }

  int64_t GetInterval(uint32_t db_id) const {
    std::shared_lock lk{mtx_};
    auto it = partition_interval_map_.find(db_id);
    if (it == partition_interval_map_.end()) {
      return EngineOptions::default_partition_interval;
    }
    return it->second;
  }
};
}  // namespace kwdbts
