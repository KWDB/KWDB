//
// Created by root on 7/3/25.
//

#include "include/ts_metrics_table_version_manager.h"

namespace kwdbts {
inline string IdToSchemaPath(const KTableKey& table_id, uint32_t ts_version) {
  return nameToEntityBigTablePath(std::to_string(table_id), s_bt + "_" + std::to_string(ts_version));
}

MetricsTableVersionManager::~MetricsTableVersionManager() {
  metric_tables_.clear();
}

void MetricsTableVersionManager::InitVersions(uint32_t ts_version) {
  auto iter = metric_tables_.find(ts_version);
  if (iter != metric_tables_.end()) {
    iter->second.reset();
    metric_tables_.erase(iter);
  }
  metric_tables_.insert({ts_version, nullptr});
}

KStatus MetricsTableVersionManager::CreateMetricsTable(kwdbContext_p ctx, std::vector<AttributeInfo> meta, uint64_t db_id,
                                                      uint32_t ts_version, int64_t lifetime, ErrorInfo& err_info) {
  // Create a new version schema
  string bt_path = IdToSchemaPath(table_id_, ts_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  if (tmp_bt->open(bt_path, db_path_, tbl_sub_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_bt->create(meta, ts_version, tbl_sub_path_, partition_interval_, encoding, err_info, false);
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
  case TIMESTAMP64_LSN:
  case TIMESTAMP64:
    precision = 1000;
    break;
  case TIMESTAMP64_LSN_MICRO:
  case TIMESTAMP64_MICRO:
    precision = 1000000;
    break;
  case TIMESTAMP64_LSN_NANO:
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

void MetricsTableVersionManager::AddMetricsTable(uint32_t ts_version, std::shared_ptr<MMapMetricsTable> metrics_table) {
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

std::shared_ptr<MMapMetricsTable> MetricsTableVersionManager::GetMetricsTable(uint32_t ts_version, bool lock) {
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

void MetricsTableVersionManager::GetAllVersions(std::vector<uint32_t> *table_versions) {
  for (auto& version : metric_tables_) {
    table_versions->push_back(version.first);
  }
}

LifeTime MetricsTableVersionManager::GetLifeTime() const {
  return cur_metric_table_->GetLifeTime();
}

void MetricsTableVersionManager::SetLifeTime(LifeTime life_time) const {
  cur_metric_table_->SetLifeTime(life_time);
}

uint64_t MetricsTableVersionManager::GetPartitionInterval() const {
  return partition_interval_;
}

uint64_t MetricsTableVersionManager::GetDbID() const {
  if (cur_metric_table_ && cur_metric_schema_->metaData()) {
    return cur_metric_table_->metaData()->db_id;
  } else {
    LOG_ERROR("cur_metric_schema_ is nullptr");
    return 0;
  }
}
} // kwdbts