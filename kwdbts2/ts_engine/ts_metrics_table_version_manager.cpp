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

#include "include/ts_metrics_table_version_manager.h"

#include <dirent.h>
#include <sys/stat.h>

#include "sys_utils.h"

namespace kwdbts {
inline string IdToSchemaFileName(const KTableKey& table_id, uint32_t ts_version) {
  return nameToEntityBigTablePath(std::to_string(table_id), s_bt + "_" + std::to_string(ts_version));
}

impl_latch_virtual_func(MetricsVersionManager, &schema_rw_lock_)

MetricsVersionManager::~MetricsVersionManager() {
  metric_tables_.clear();
  opened_versions_.clear();
}

KStatus MetricsVersionManager::Init() {
  uint32_t max_table_version = 0;
  // load all versions
  DIR* dir_ptr = opendir(metric_schema_path_.c_str());
  if (dir_ptr) {
    string prefix = std::to_string(table_id_) + s_bt + '_';
    size_t prefix_len = prefix.length();
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 || entry->d_name[0] == '_') {
        continue;
      }
      fs::path full_path = metric_schema_path_ / entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        closedir(dir_ptr);
        return FAIL;
      }
      if (S_ISREG(file_stat.st_mode) &&
          strncmp(entry->d_name, prefix.c_str(), prefix_len) == 0) {
        uint32_t ts_version = std::stoi(entry->d_name + prefix_len);
        InsertNull(ts_version);
        if (ts_version > max_table_version) {
          max_table_version = ts_version;
        }
      }
    }
    closedir(dir_ptr);
  }
  // Open only the schema of the latest version.
  auto tmp_schema = std::make_shared<MMapMetricsTable>();
  string schema_file_name  = IdToSchemaFileName(table_id_, max_table_version);
  ErrorInfo err_info;
  tmp_schema->open(schema_file_name , metric_schema_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("schema[%s] open error : %s", schema_file_name .c_str(), err_info.errmsg.c_str());
    return FAIL;
  }
  // Save to map cache
  auto s = AddOneVersion(max_table_version, tmp_schema);
  if (s != SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

void MetricsVersionManager::InsertNull(uint32_t ts_version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  auto iter = metric_tables_.find(ts_version);
  if (iter != metric_tables_.end()) {
    iter->second.reset();
    metric_tables_.erase(iter);
  }
  metric_tables_.insert_or_assign(ts_version, nullptr);
}

KStatus MetricsVersionManager::CreateTable(kwdbContext_p ctx, std::vector<AttributeInfo> meta, uint32_t db_id,
                                           uint32_t ts_version, int64_t lifetime, uint64_t partition_interval,
                                           uint64_t hash_num, ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  string schema_file_name  = IdToSchemaFileName(table_id_, ts_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_schema = std::make_shared<MMapMetricsTable>();
  if (tmp_schema->open(schema_file_name , metric_schema_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_schema->create(meta, ts_version, partition_interval, encoding, err_info, false, hash_num);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("schema[%s] create error : %s", schema_file_name .c_str(), err_info.errmsg.c_str());
    tmp_schema->remove();
    return FAIL;
  }
  tmp_schema->setDBid(db_id);
  // Set lifetime
  int32_t precision = 1;
  switch (meta[0].type) {
  case TIMESTAMP64:
    precision = 1000;
    break;
  case TIMESTAMP64_MICRO:
    precision = 1000000;
    break;
  case TIMESTAMP64_NANO:
    precision = 1000000000;
    break;
  default:
    assert(false);
    break;
  }
  LifeTime life_time {lifetime, precision};
  tmp_schema->SetLifeTime(life_time);
  LOG_INFO("Create table %lu with life time[%ld:%d], version:%d.", table_id_, life_time.ts, life_time.precision, ts_version);
  tmp_schema->setObjectReady();
  // Save to map cache
  metric_tables_.insert_or_assign(ts_version, tmp_schema);
  if (ts_version > cur_metric_version_) {
    cur_metric_table_ = tmp_schema;
    cur_metric_version_ = ts_version;
  }
  if (EngineOptions::force_sync_file) {
    tmp_schema->Sync();
  }
  UpdateOpenedVersions(ts_version);
  return KStatus::SUCCESS;
}

KStatus MetricsVersionManager::AddOneVersion(uint32_t ts_version, std::shared_ptr<MMapMetricsTable> metrics_table) {
  wrLock();
  Defer defer([&]() { unLock(); });
  auto iter = metric_tables_.find(ts_version);
  if (iter != metric_tables_.end() && iter->second != nullptr) {
    return FAIL;
  }
  metric_tables_.insert_or_assign(ts_version, metrics_table);
  if (cur_metric_version_ < ts_version) {
    cur_metric_table_ = metrics_table;
    cur_metric_version_ = ts_version;
  }
  UpdateOpenedVersions(ts_version);
  return SUCCESS;
}

void MetricsVersionManager::UpdateOpenedVersions(uint32_t ts_version) {
  opened_versions_.push_back(ts_version);
  if (opened_versions_.size() > EngineOptions::metric_schema_cache_capacity) {
    uint32_t pop_version = opened_versions_.front();
    auto metric_iter = metric_tables_.find(pop_version);
    if (metric_iter != metric_tables_.end()) {
      // Only set to nullptr, cannot erase the version
      metric_iter->second = nullptr;
    }
    opened_versions_.pop_front();
  }
}

std::shared_ptr<MMapMetricsTable> MetricsVersionManager::GetMetricsTable(uint32_t ts_version, bool lock) {
  bool need_open = false;
  // Try to get the schema using a read lock
  {
    if (lock) {
      rdLock();
    }
    Defer defer([&]() { if (lock) { unLock(); }});
    if (ts_version == 0 || ts_version == cur_metric_version_) {
      return cur_metric_table_;
    }
    auto iter = metric_tables_.find(ts_version);
    if (iter != metric_tables_.end()) {
      if (!iter->second) {
        need_open = true;
      } else {
        return iter->second;
      }
    }
  }
  if (!need_open) {
    return nullptr;
  }
  // Open the schema using a write lock
  if (lock) {
    wrLock();
  }
  Defer defer([&]() { if (lock) { unLock(); }});
  auto iter = metric_tables_.find(ts_version);
  if (iter != metric_tables_.end()) {
    if (!iter->second) {
      ErrorInfo err_info;
      iter->second = open(iter->first, err_info);
    }
    return iter->second;
  }
  return nullptr;
}

void MetricsVersionManager::GetAllVersions(std::vector<uint32_t> *table_versions) {
  rdLock();
  Defer defer([&]() { unLock(); });
  for (auto& version : metric_tables_) {
    table_versions->push_back(version.first);
  }
}

LifeTime MetricsVersionManager::GetLifeTime() {
  return GetCurrentMetricsTable()->GetLifeTime();
}

void MetricsVersionManager::SetLifeTime(LifeTime life_time) {
  GetCurrentMetricsTable()->SetLifeTime(life_time);
}

uint64_t MetricsVersionManager::GetPartitionInterval() {
  return GetCurrentMetricsTable()->metaData()->partition_interval;
}

void MetricsVersionManager::SetPartitionInterval(uint64_t partition_interval) {
  GetCurrentMetricsTable()->SetPartitionInterval(partition_interval);
}

uint32_t MetricsVersionManager::GetDbID() {
  return GetCurrentMetricsTable()->metaData()->db_id;
}

void MetricsVersionManager::Sync(const kwdbts::TS_OSN& check_lsn, ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  for (auto& root_table : metric_tables_) {
    if (root_table.second) {
      root_table.second->Sync(check_lsn, err_info);
    }
  }
}

KStatus MetricsVersionManager::SetDropped() {
  wrLock();
  Defer defer([&]() { unLock(); });
  std::vector<std::shared_ptr<MMapMetricsTable>> completed_tables;
  // Iterate through all versions of the schema, updating the drop flag
  for (auto& root_table : metric_tables_) {
    if (!root_table.second) {
      ErrorInfo err_info;
      root_table.second = open(root_table.first, err_info);
      if (!root_table.second) {
        LOG_ERROR("schema[%s] set drop failed", IdToSchemaFileName(table_id_, root_table.first).c_str());
        // rollback
        for (auto completed_table : completed_tables) {
          completed_table->setNotDropped();
        }
        return FAIL;
      }
    }
    root_table.second->setDropped();
    completed_tables.push_back(root_table.second);
  }
  return SUCCESS;
}

bool MetricsVersionManager::IsDropped() {
  return GetCurrentMetricsTable()->isDropped();
}

KStatus MetricsVersionManager::RemoveAll() {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Remove all schemas
  for (auto& root_table : metric_tables_) {
    if (!root_table.second) {
      Remove(metric_schema_path_ / IdToSchemaFileName(table_id_, root_table.first));
    } else {
      root_table.second->remove();
    }
  }
  metric_tables_.clear();
  return SUCCESS;
}

KStatus MetricsVersionManager::UndoAlterCol(uint32_t old_version, uint32_t new_version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  LOG_INFO("UndoAlterCol begin, table id [%lu], old version [%u], new version [%u]", table_id_, old_version, new_version);
  if (new_version < cur_metric_version_) {
    LOG_ERROR("UndoAlterCol Unexpected error: current version is [%u], but new version is [%u] when alter",
              cur_metric_version_, new_version);
    return FAIL;;
  }
  if (cur_metric_version_ < old_version) {
    LOG_ERROR("UndoAlterCol Unexpected error: current version is [%u], but want to roll back to version [%u]",
              cur_metric_version_, old_version);
    return FAIL;
  }

  auto undo_schema = GetMetricsTable(new_version, false);
  if (undo_schema != nullptr) {
    metric_tables_.erase(new_version);
    if (opened_versions_.back() == new_version) {
      opened_versions_.pop_back();
    }
    if (cur_metric_table_->GetVersion() == new_version) {
      cur_metric_table_.reset();
    }
    undo_schema->remove();
  }

  auto prev_schema = GetMetricsTable(old_version, false);
  if (prev_schema == nullptr) {
    LOG_ERROR("UndoAlterCol failed: metric version %u is null", old_version);
    return FAIL;
  }
  cur_metric_table_ = prev_schema;
  cur_metric_version_ = old_version;
  LOG_INFO("UndoAlterCol succeed, table id [%lu], old version [%u], new version [%u]", table_id_, old_version, new_version);
  return SUCCESS;
}

uint64_t MetricsVersionManager::GetHashNum() {
  return GetCurrentMetricsTable()->hashNum();
}

std::shared_ptr<MMapMetricsTable> MetricsVersionManager::open(uint32_t ts_version, ErrorInfo& err_info) {
  auto tmp_schema = std::make_shared<MMapMetricsTable>();
  string schema_file_name  = IdToSchemaFileName(table_id_, ts_version);
  tmp_schema->open(schema_file_name , metric_schema_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("schema[%s] open failed: %s", schema_file_name .c_str(), err_info.errmsg.c_str())
    return nullptr;
  }
  UpdateOpenedVersions(ts_version);
  return tmp_schema;
}
}  //  namespace kwdbts
