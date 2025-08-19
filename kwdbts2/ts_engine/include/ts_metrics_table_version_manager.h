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
#include "mmap/mmap_metrics_table.h"

namespace kwdbts {

class MetricsVersionManager {
  std::string table_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_{0};
  uint32_t cur_metric_version_{0};
  // metric schema of current version
  std::shared_ptr<MMapMetricsTable> cur_metric_table_;
  // schemas of all versions
  std::unordered_map<uint32_t, std::shared_ptr<MMapMetricsTable>> metric_tables_;
  uint64_t partition_interval_;
  KRWLatch schema_rw_lock_;

 public:
  MetricsVersionManager(const std::string& table_path, const std::string& sub_path, uint64_t table_id) :
                            table_path_(table_path), tbl_sub_path_(sub_path), table_id_(table_id),
                            schema_rw_lock_(RWLATCH_ID_METRIC_VERSION_RWLOCK) {}

  ~MetricsVersionManager();

  KStatus Init();

  void InsertNull(uint32_t ts_version);

  KStatus CreateTable(kwdbContext_p ctx, std::vector<AttributeInfo> meta, uint64_t db_id, uint32_t ts_version,
                      int64_t life_time, uint64_t hash_num, ErrorInfo& err_info);

  void AddOneVersion(uint32_t ts_version, std::shared_ptr<MMapMetricsTable> metrics_table);

  std::shared_ptr<MMapMetricsTable> GetMetricsTable(uint32_t ts_version, bool lock = true);

  std::shared_ptr<MMapMetricsTable> GetCurrentMetricsTable(bool lock = true) {
    return GetMetricsTable(cur_metric_version_, lock);
  }

  uint32_t GetCurrentMetricsVersion() const { return cur_metric_version_; }

  void GetAllVersions(std::vector<uint32_t>* table_versions);

  LifeTime GetLifeTime() const;

  void SetLifeTime(LifeTime life_time) const;

  uint64_t GetPartitionInterval() const;

  uint64_t GetDbID() const;

  void Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info);

  KStatus SetDropped();

  bool IsDropped();

  KStatus RemoveAll();

  KStatus UndoAlterCol(uint32_t old_version, uint32_t new_version);

  uint64_t GetHashNum() const;

 private:
  int rdLock();

  int wrLock();

  int unLock();

  std::shared_ptr<MMapMetricsTable> open(uint32_t ts_version, ErrorInfo& err_info);
};

}  // namespace kwdbts
