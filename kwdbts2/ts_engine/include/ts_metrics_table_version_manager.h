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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <deque>
#include "mmap/mmap_metrics_table.h"
#include "sys_utils.h"

namespace kwdbts {

class MetricsVersionManager {
  fs::path metric_schema_path_;
  KTableKey table_id_{0};
  uint32_t cur_metric_version_{0};
  // metric schema of current version
  std::shared_ptr<MMapMetricsTable> cur_metric_table_{nullptr};
  // schemas of all versions
  std::unordered_map<uint32_t, std::shared_ptr<MMapMetricsTable>> metric_tables_;
  std::deque<uint32_t> opened_versions_;
  KRWLatch schema_rw_lock_;

 public:
  MetricsVersionManager(const fs::path& metric_schema_path, KTableKey table_id) :
                        metric_schema_path_(metric_schema_path), table_id_(table_id),
                        schema_rw_lock_(RWLATCH_ID_METRIC_VERSION_RWLOCK) {}

  ~MetricsVersionManager();

  KStatus Init(std::vector<uint32_t>& remove_versions, std::vector<uint32_t>& invalid_metric_versions,
               uint32_t tag_cur_version);

  void InsertNull(uint32_t ts_version);

  KStatus CreateTable(kwdbContext_p ctx, std::vector<AttributeInfo> meta, uint32_t db_id, uint32_t ts_version,
                      int64_t life_time, uint64_t partition_interval, uint64_t hash_num, ErrorInfo& err_info);

  KStatus AddOneVersion(uint32_t ts_version, std::shared_ptr<MMapMetricsTable> metrics_table);

  void UpdateOpenedVersions(uint32_t ts_version);

  std::shared_ptr<MMapMetricsTable> GetMetricsTable(uint32_t ts_version, bool lock = true);

  std::shared_ptr<MMapMetricsTable> GetCurrentMetricsTable() {
    rdLock();
    Defer defer([&]() { unLock(); });
    assert(cur_metric_table_ != nullptr);
    assert(cur_metric_version_ == cur_metric_table_->GetVersion());
    return cur_metric_table_;
  }

  uint32_t GetCurrentMetricsVersion() {
    rdLock();
    Defer defer([&]() { unLock(); });
    return cur_metric_version_;
  }

  void GetAllVersions(std::vector<uint32_t>* table_versions);

  LifeTime GetLifeTime();

  void SetLifeTime(LifeTime life_time);

  uint64_t GetPartitionInterval();

  void SetPartitionInterval(uint64_t partition_interval);

  uint32_t GetDbID();

  void Sync(const kwdbts::TS_OSN& check_lsn, ErrorInfo& err_info);

  KStatus SetDropped();

  bool IsDropped();

  KStatus RemoveAll();

  KStatus UndoAlterCol(uint32_t old_version, uint32_t new_version);

  uint64_t GetHashNum();

 private:
  int rdLock();

  int wrLock();

  int unLock();

  std::shared_ptr<MMapMetricsTable> open(uint32_t ts_version, ErrorInfo& err_info);
};

}  // namespace kwdbts
