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

namespace kwdbts {
inline string IdToSchemaPath(const KTableKey& table_id, uint32_t ts_version) {
  return nameToEntityBigTablePath(std::to_string(table_id), s_bt + "_" + std::to_string(ts_version));
}

impl_latch_virtual_func(MetricsVersionManager, &schema_rw_lock_)

MetricsVersionManager::~MetricsVersionManager() {
  metric_tables_.clear();
}

KStatus MetricsVersionManager::Init() {
  uint32_t max_table_version = 0;
  string real_path = table_path_ + tbl_sub_path_;
  // load all versions
  DIR* dir_ptr = opendir(real_path.c_str());
  if (dir_ptr) {
    string prefix = std::to_string(table_id_) + s_bt + '_';
    size_t prefix_len = prefix.length();
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
          }
      std::string full_path = real_path + entry->d_name;
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
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  string bt_path = IdToSchemaPath(table_id_, max_table_version);
  ErrorInfo err_info;
  tmp_bt->open(bt_path, table_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("schema[%s] open error : %s", bt_path.c_str(), err_info.errmsg.c_str());
    return FAIL;
  }
  // Save to map cache
  AddOneVersion(max_table_version, tmp_bt);
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
  metric_tables_.insert({ts_version, nullptr});
}

KStatus MetricsVersionManager::CreateTable(kwdbContext_p ctx, std::vector<AttributeInfo> meta, uint64_t db_id,
                                                      uint32_t ts_version, int64_t lifetime, uint64_t hash_num,
                                                      ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  string bt_path = IdToSchemaPath(table_id_, ts_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  if (tmp_bt->open(bt_path, table_path_, tbl_sub_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_bt->create(meta, ts_version, tbl_sub_path_, partition_interval_, encoding, err_info, false, hash_num);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] create error : %s", bt_path.c_str(), err_info.errmsg.c_str());
    tmp_bt->remove();
    return FAIL;
  }
  tmp_bt->metaData()->schema_version_of_latest_data = ts_version;
  tmp_bt->metaData()->db_id = db_id;
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
  tmp_bt->SetLifeTime(life_time);
  LOG_INFO("Create table %lu with life time[%ld:%d], version:%d.", table_id_, life_time.ts, life_time.precision, ts_version);
  tmp_bt->setObjectReady();
  // Save to map cache
  metric_tables_.insert({ts_version, tmp_bt});
  cur_metric_table_ = tmp_bt;
  cur_metric_version_ = ts_version;
  return KStatus::SUCCESS;
}

void MetricsVersionManager::AddOneVersion(uint32_t ts_version, std::shared_ptr<MMapMetricsTable> metrics_table) {
  wrLock();
  Defer defer([&]() { unLock(); });
  auto iter = metric_tables_.find(ts_version);
  if (iter != metric_tables_.end()) {
    iter->second.reset();
    metric_tables_.erase(iter);
  }
  metric_tables_.insert({ts_version, metrics_table});
  if (cur_metric_version_ < ts_version) {
    cur_metric_table_ = metrics_table;
    cur_metric_version_ = ts_version;
    partition_interval_ = metrics_table->partitionInterval();
  }
}

std::shared_ptr<MMapMetricsTable> MetricsVersionManager::GetMetricsTable(uint32_t ts_version, bool lock) {
  bool need_open = false;
  // Try to get the root table using a read lock
  {
    if (lock) {
      rdLock();
    }
    Defer defer([&]() { if (lock) { unLock(); }});
    if (ts_version == 0 || ts_version == cur_metric_version_) {
      return cur_metric_table_;
    }
    auto bt_it = metric_tables_.find(ts_version);
    if (bt_it != metric_tables_.end()) {
      if (!bt_it->second) {
        need_open = true;
      } else {
        return bt_it->second;
      }
    }
  }
  if (!need_open) {
    return nullptr;
  }
  // Open the root table using a write lock
  if (lock) {
    wrLock();
  }
  Defer defer([&]() { if (lock) { unLock(); }});
  auto bt_it = metric_tables_.find(ts_version);
  if (bt_it != metric_tables_.end()) {
    if (!bt_it->second) {
      ErrorInfo err_info;
      bt_it->second = open(bt_it->first, err_info);
    }
    return bt_it->second;
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

LifeTime MetricsVersionManager::GetLifeTime() const {
  return cur_metric_table_->GetLifeTime();
}

void MetricsVersionManager::SetLifeTime(LifeTime life_time) const {
  cur_metric_table_->SetLifeTime(life_time);
}

uint64_t MetricsVersionManager::GetPartitionInterval() const {
  return partition_interval_;
}

uint64_t MetricsVersionManager::GetDbID() const {
  if (cur_metric_table_ && cur_metric_table_->metaData()) {
    return cur_metric_table_->metaData()->db_id;
  } else {
    LOG_ERROR("cur_metric_schema_ is nullptr");
    return 0;
  }
}

void MetricsVersionManager::Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info) {
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
  // Iterate through all versions of the root table, updating the drop flag
  for (auto& root_table : metric_tables_) {
    if (!root_table.second) {
      ErrorInfo err_info;
      root_table.second = open(root_table.first, err_info);
      if (!root_table.second) {
        LOG_ERROR("root table[%s] set drop failed", IdToSchemaPath(table_id_, root_table.first).c_str());
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
  rdLock();
  Defer defer([&]() { unLock(); });
  return cur_metric_table_->isDropped();
}

KStatus MetricsVersionManager::RemoveAll() {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Remove all root tables
  for (auto& root_table : metric_tables_) {
    if (!root_table.second) {
      Remove(table_path_ + IdToSchemaPath(table_id_, root_table.first));
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
  if (cur_metric_version_ < old_version) {
    LOG_ERROR("UndoAlterCol Unexpected error: current version is [%u], but want to roll back to version [%u]",
              cur_metric_version_, old_version);
    return FAIL;
  }

  auto new_bt = GetMetricsTable(new_version, false);
  if (new_bt != nullptr) {
    metric_tables_.erase(new_version);
    if (cur_metric_table_->GetVersion() == new_version) {
      cur_metric_table_.reset();
    }
    new_bt->remove();
  }

  auto old_bt = GetMetricsTable(old_version, false);
  cur_metric_table_ = old_bt;
  cur_metric_version_ = old_version;
  partition_interval_ = old_bt->partitionInterval();
  LOG_INFO("UndoAlterCol succeed, table id [%lu], old version [%u], new version [%u]", table_id_, old_version, new_version);
  return SUCCESS;
}

uint64_t MetricsVersionManager::GetHashNum() const {
  return cur_metric_table_->hashNum();
}

std::shared_ptr<MMapMetricsTable> MetricsVersionManager::open(uint32_t ts_version, ErrorInfo& err_info) {
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  string bt_path = IdToSchemaPath(table_id_, ts_version);
  tmp_bt->open(bt_path, table_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] open failed: %s", bt_path.c_str(), err_info.errmsg.c_str())
    return nullptr;
  }
  return tmp_bt;
}
}  //  namespace kwdbts
