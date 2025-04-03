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

#include "ts_table_schema_manager.h"
#include <dirent.h>
#include "engine.h"
#include "sys_utils.h"
#include "column_utils.h"

namespace kwdbts {
inline string IdToSchemaPath(const KTableKey& table_id, uint32_t ts_version) {
  return nameToEntityBigTablePath(std::to_string(table_id), s_bt + "_" + std::to_string(ts_version));
}

int TsTableSchemaManager::getColumnIndex(const AttributeInfo& attr_info) {
  int col_no = -1;
  std::vector<AttributeInfo> schema_info;
  GetColumnsIncludeDropped(schema_info);
  for (int i = 0; i < schema_info.size(); ++i) {
    if ((schema_info[i].id == attr_info.id) && (!schema_info[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  return col_no;
}

KStatus TsTableSchemaManager::alterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                            uint32_t cur_version, uint32_t new_version, string& msg) {
  ErrorInfo err_info;
  if (tag_table_->AlterTableTag(alter_type, attr_info, cur_version, new_version, err_info) < 0) {
    LOG_ERROR("AlterTableTag failed. error: %s ", err_info.errmsg.c_str());
    msg = err_info.errmsg;
    return FAIL;
  }
  if (UpdateVersion(cur_version, new_version) != SUCCESS) {
    msg = "Update table version error";
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::alterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                            uint32_t cur_version, uint32_t new_version, string& msg) {
  ErrorInfo err_info;
  auto col_idx = getColumnIndex(attr_info);
  auto latest_version = GetCurrentVersion();
  vector<AttributeInfo> schema;
  KStatus s = GetColumnsIncludeDropped(schema, cur_version);
  if (s != SUCCESS) {
    msg = "schema version " + to_string(cur_version) + " does not exists";
    return FAIL;
  }
  switch (alter_type) {
    case ADD_COLUMN:
      if (col_idx >= 0 && latest_version == new_version) {
        return SUCCESS;
      }
    schema.emplace_back(attr_info);
    break;
    case DROP_COLUMN:
      if (col_idx < 0 && latest_version == new_version) {
        return SUCCESS;
      }
    schema[col_idx].setFlag(AINFO_DROPPED);
    break;
    case ALTER_COLUMN_TYPE: {
      if (col_idx < 0) {
        LOG_ERROR("alter column type failed: column (id %u) does not exists, table id = %lu", attr_info.id, table_id_);
        msg = "column does not exist";
        return FAIL;
      }
      if (latest_version == new_version) {
        return SUCCESS;
      }
      auto& col_info = schema[col_idx];
      col_info.type = attr_info.type;
      col_info.size = attr_info.size;
      col_info.length = attr_info.length;
      col_info.max_len = attr_info.max_len;
      break;
    }
    default:
      return FAIL;
  }
  s = AddMetricSchema(schema, cur_version, new_version, err_info);
  if (s != SUCCESS) {
    msg = err_info.errmsg;
    LOG_ERROR("add new version schema failed for alter table: table id %lu, new_version %u", table_id_, new_version);
    return s;
  }
  if (tag_table_->GetTagTableVersionManager()->SyncFromMetricsTableVersion(cur_version, new_version) < 0) {
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                                         uint32_t cur_version, uint32_t new_version, string& msg) {
  AttributeInfo attr_info;
  KStatus s = GetColAttrInfo(ctx, *column, attr_info, false);
  if (s != SUCCESS) {
    msg = "Unknown column/tag type";
    return s;
  }
  LOG_INFO("AlterTable begin. table_id: %lu alter_type: %hhu cur_version: %u new_version: %u is_general_tag: %d",
           table_id_, alter_type, cur_version, new_version, attr_info.isAttrType(COL_GENERAL_TAG));
  if (alter_type == AlterType::ALTER_COLUMN_TYPE) {
    getDataTypeSize(attr_info);  // update max_len
  }
  if (attr_info.isAttrType(COL_GENERAL_TAG)) {
    s = alterTableTag(ctx, alter_type, attr_info, cur_version, new_version, msg);
  } else if (attr_info.isAttrType(COL_TS_DATA)) {
    s = alterTableCol(ctx, alter_type, attr_info, cur_version, new_version, msg);
  }
  LOG_INFO("AlterTable end. table_id: %lu alter_type: %hhu ", table_id_, alter_type);
  return s;
}

std::shared_ptr<MMapMetricsTable> TsTableSchemaManager::open(uint32_t ts_version, ErrorInfo& err_info) {
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  string bt_path = IdToSchemaPath(table_id_, ts_version);
  tmp_bt->open(bt_path, schema_root_path_, metric_schema_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] open failed: %s", bt_path.c_str(), err_info.errmsg.c_str())
    return nullptr;
  }
  return tmp_bt;
}

TsTableSchemaManager::~TsTableSchemaManager() {
  wrLock();
  Defer defer([&]() { unLock(); });
  metric_schemas_.clear();
}

KStatus TsTableSchemaManager::Init(kwdbContext_p ctx) {
  uint32_t max_table_version = 0;
  string real_path = schema_root_path_ + metric_schema_path_;
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
        // By default, it is not enabled
        metric_schemas_.insert({ts_version, nullptr});
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
  tmp_bt->open(bt_path, schema_root_path_, metric_schema_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("schema[%s] open error : %s", bt_path.c_str(), err_info.errmsg.c_str());
    return FAIL;
  }
  // Save to map cache
  put(max_table_version, tmp_bt);
  return SUCCESS;
}

void TsTableSchemaManager::put(uint32_t ts_version, const std::shared_ptr<MMapMetricsTable>& schema) {
  wrLock();
  Defer defer([&]() { unLock(); });
  auto iter = metric_schemas_.find(ts_version);
  if (iter != metric_schemas_.end()) {
    iter->second.reset();
    metric_schemas_.erase(iter);
  }
  metric_schemas_.insert({ts_version, schema});
  if (cur_schema_version_ < ts_version) {
    cur_metric_schema_ = schema;
    cur_schema_version_ = ts_version;
    partition_interval_ = schema->partitionInterval();
  }
}

std::shared_ptr<MMapMetricsTable> TsTableSchemaManager::Get(uint32_t ts_version, bool lock) {
  bool need_open = false;
  // Try to get the root table using a read lock
  {
    if (lock) {
      rdLock();
    }
    Defer defer([&]() { if (lock) { unLock(); }});
    if (ts_version == 0 || ts_version == cur_schema_version_) {
      return cur_metric_schema_;
    }
    auto bt_it = metric_schemas_.find(ts_version);
    if (bt_it != metric_schemas_.end()) {
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
  auto bt_it = metric_schemas_.find(ts_version);
  if (bt_it != metric_schemas_.end()) {
    if (!bt_it->second) {
      ErrorInfo err_info;
      bt_it->second = open(bt_it->first, err_info);
    }
    return bt_it->second;
  }
  return nullptr;
}

KStatus TsTableSchemaManager::CreateTable(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                          uint32_t ts_version, ErrorInfo& err_info) {
  if (ts_version == 0) {
    LOG_ERROR("cannot create version 0 table, table id [%lu]", table_id_)
    return FAIL;
  }
  wrLock();
  Defer defer([&]() { unLock(); });
  if (metric_schemas_.find(ts_version) != metric_schemas_.end()) {
    LOG_INFO("Creating root table that already exists.");
    return SUCCESS;
  }
  if (cur_schema_version_ >= ts_version) {
    LOG_ERROR("cannot create low version table: current version[%d], create version[%d]",
              cur_schema_version_, ts_version)
    return FAIL;
  }
  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  auto s = TSEngineImpl::parseMetaSchema(ctx, meta, metric_schema, tag_schema);
  if (s != SUCCESS) {
    return s;
  }
  for (auto& attr : metric_schema) {
    attr.version = ts_version;
  }
  // Create a new version schema
  string bt_path = IdToSchemaPath(table_id_, ts_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  if (tmp_bt->open(bt_path, schema_root_path_, metric_schema_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_bt->create(metric_schema, ts_version, metric_schema_path_, partition_interval_, encoding, err_info, false);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] create error : %s", bt_path.c_str(), err_info.errmsg.c_str());
    tmp_bt->remove();
    return FAIL;
  }
  tmp_bt->metaData()->schema_version_of_latest_data = ts_version;
  tmp_bt->setObjectReady();
  // Save to map cache
  metric_schemas_.insert({ts_version, tmp_bt});
  cur_metric_schema_ = tmp_bt;

  tag_table_ = std::make_shared<TagTable>(schema_root_path_, tag_schema_path_, table_id_, 1);
  if (tag_table_->create(tag_schema, ts_version, err_info) < 0) {
    LOG_ERROR("failed to create the tag table %s%lu, error: %s",
              tag_schema_path_.c_str(), table_id_, err_info.errmsg.c_str());
    return FAIL;
  }

  // Update the latest version of table and other information
  cur_schema_version_ = ts_version;
  partition_interval_ = tmp_bt->partitionInterval();
  return SUCCESS;
}

KStatus TsTableSchemaManager::AddMetricSchema(vector<AttributeInfo>& schema, uint32_t cur_version,
                                              uint32_t new_version, ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  if (metric_schemas_.find(new_version) != metric_schemas_.end()) {
    LOG_INFO("metric schema version %d already exists, table id %lu", new_version, table_id_);
    return SUCCESS;
  }
  if (cur_schema_version_ >= new_version) {
    LOG_ERROR("cannot add low version schema: current version[%d], create version[%d]",
              cur_schema_version_, new_version)
    return FAIL;
  }
  for (auto& attr : schema) {
    attr.version = new_version;
  }
  // Create a new version schema
  string bt_path = IdToSchemaPath(table_id_, new_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_bt = std::make_shared<MMapMetricsTable>();
  if (tmp_bt->open(bt_path, schema_root_path_, metric_schema_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_bt->create(schema, new_version, metric_schema_path_, partition_interval_, encoding, err_info, false);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] create error : %s", bt_path.c_str(), err_info.errmsg.c_str());
    tmp_bt->remove();
    return FAIL;
  }

  // Copy the metadata of the previous version
  if (cur_version) {
    auto src_bt = metric_schemas_[cur_version];
    tmp_bt->metaData()->has_data = src_bt->metaData()->has_data;
    tmp_bt->metaData()->actul_size = src_bt->metaData()->actul_size;
    // tmp_bt->metaData()->life_time = src_bt->metaData()->life_time;
    tmp_bt->metaData()->partition_interval = src_bt->metaData()->partition_interval;
    tmp_bt->metaData()->num_node = src_bt->metaData()->num_node;
    tmp_bt->metaData()->is_dropped = src_bt->metaData()->is_dropped;
    tmp_bt->metaData()->min_ts = src_bt->metaData()->min_ts;
    tmp_bt->metaData()->max_ts = src_bt->metaData()->max_ts;
    // Version compatibility
    if (src_bt->metaData()->schema_version_of_latest_data == 0) {
      tmp_bt->metaData()->schema_version_of_latest_data = new_version;
    } else {
      tmp_bt->metaData()->schema_version_of_latest_data = src_bt->metaData()->schema_version_of_latest_data;
    }
  } else {
    tmp_bt->metaData()->schema_version_of_latest_data = new_version;
  }
  tmp_bt->setObjectReady();
  // Save to map cache
  metric_schemas_.insert({new_version, tmp_bt});
  cur_metric_schema_ = tmp_bt;
  // Update the latest version of table and other information
  cur_schema_version_ = new_version;
  partition_interval_ = tmp_bt->partitionInterval();
  return SUCCESS;
}


KStatus TsTableSchemaManager::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version,
                                        roachpb::CreateTsTable* meta) {
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  std::vector<AttributeInfo> metric_meta;
  metric_schemas_.find(version);
  auto s = GetMetricMeta(version, metric_meta);
  if (s != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed.");
    return s;
  }
  for (auto col_var : metric_meta) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    if (!ParseToColumnInfo(col_var, *col)) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate metric Schema", col_var.type);
      return FAIL;
    }
  }
  std::vector<TagInfo> tag_meta;
  s = GetTagMeta(version, tag_meta);
  if (s != SUCCESS) {
    LOG_ERROR("GetTagMeta failed.");
    return s;
  }
  for (auto tag_info : tag_meta) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    // XXX Notice: tag_info don't has tag column name,
    if (!ParseTagColumnInfo(tag_info, *col)) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate tag Schema", tag_info.m_data_type);
      return FAIL;
    }
    // Set storage length.
    if (col->has_storage_len() && col->storage_len() == 0) {
      col->set_storage_len(tag_info.m_size);
    }
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetColumnsExcludeDropped(std::vector<AttributeInfo>& schema, uint32_t ts_version) {
  std::shared_ptr<MMapMetricsTable> schema_table = Get(ts_version);
  if (!schema_table) {
    LOG_ERROR("schema version [%u] does not exists", ts_version);
    return FAIL;
  }
  schema = schema_table->getSchemaInfoExcludeDropped();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetColumnsIncludeDropped(std::vector<AttributeInfo>& schema, uint32_t ts_version) {
  std::shared_ptr<MMapMetricsTable> schema_table = Get(ts_version);
  if (!schema_table) {
    LOG_ERROR("schema version [%u] does not exists", ts_version);
    return FAIL;
  }
  schema = schema_table->getSchemaInfoIncludeDropped();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetMetricMeta(uint32_t version, std::vector<AttributeInfo>& info) {
  return GetColumnsExcludeDropped(info, version);
}

KStatus TsTableSchemaManager::GetTagMeta(uint32_t version, std::vector<TagInfo>& info) {
  const auto pt = tag_table_->GetTagPartitionTableManager()->GetPartitionTable(version);
  if (pt == nullptr) {
    return FAIL;
  }
  auto tag_cols = pt->getSchemaInfo();
  for (auto tag_col : tag_cols) {
    auto tag_info = tag_col->attributeInfo();
    info.push_back(tag_info);
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetMetricSchema(kwdbContext_p ctx, uint32_t version,
                                              std::shared_ptr<MMapMetricsTable>* schema) {
  if (version == 0) {
    version = cur_schema_version_;
  }
  auto it = metric_schemas_.find(version);
  if (it == metric_schemas_.end()) {
    return FAIL;
  }
  *schema = it->second;
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetTagSchema(kwdbContext_p ctx, std::shared_ptr<TagTable>* schema) {
  *schema = tag_table_;
  return SUCCESS;
}

void TsTableSchemaManager::GetAllVersions(std::vector<uint32_t> *table_versions) {
  rdLock();
  Defer defer([&]() { unLock(); });
  for (auto& version : metric_schemas_) {
    table_versions->push_back(version.first);
  }
}

uint32_t TsTableSchemaManager::GetCurrentVersion() const {
  return cur_schema_version_;
}

uint64_t TsTableSchemaManager::GetPartitionInterval() const {
  return partition_interval_;
}

impl_latch_virtual_func(TsTableSchemaManager, &schema_rw_lock_)

int TsTableSchemaManager::Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  for (auto& root_table : metric_schemas_) {
    if (root_table.second) {
      root_table.second->Sync(check_lsn, err_info);
    }
  }
  return 0;
}

KStatus TsTableSchemaManager::SetDropped() {
  wrLock();
  Defer defer([&]() { unLock(); });
  std::vector<std::shared_ptr<MMapMetricsTable>> completed_tables;
  // Iterate through all versions of the root table, updating the drop flag
  for (auto& root_table : metric_schemas_) {
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

bool TsTableSchemaManager::IsDropped() {
  rdLock();
  Defer defer([&]() { unLock(); });
  return cur_metric_schema_->isDropped();
}

bool TsTableSchemaManager::TrySetCompressStatus(bool desired) {
  bool expected = !desired;
  if (is_compressing_.compare_exchange_strong(expected, desired)) {
    return true;
  }
  return false;
}

void TsTableSchemaManager::SetCompressStatus(bool status) {
  is_compressing_.store(status);
}

KStatus TsTableSchemaManager::RemoveAll() {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Remove all root tables
  for (auto& root_table : metric_schemas_) {
    if (!root_table.second) {
      Remove(schema_root_path_ + IdToSchemaPath(table_id_, root_table.first));
    } else {
      root_table.second->remove();
    }
  }
  metric_schemas_.clear();
  return SUCCESS;
}

KStatus TsTableSchemaManager::RollBack(uint32_t old_version, uint32_t new_version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  if (cur_schema_version_ == old_version) {
    return SUCCESS;
  }
  if (cur_schema_version_ == new_version) {
    // Get the previous version of root table
    auto bt = Get(old_version, false);
    // Clear the current version of the data
    metric_schemas_.erase(new_version);
    cur_metric_schema_->remove();
    cur_metric_schema_.reset();
    // Update the latest version information
    cur_metric_schema_ = bt;
    cur_schema_version_ = old_version;
    partition_interval_ = bt->partitionInterval();
  } else if (cur_schema_version_ < old_version) {
    LOG_ERROR("incorrect version: current table version is [%u], but roll back to version is [%u]",
              cur_schema_version_, old_version);
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::UpdateVersion(uint32_t cur_version, uint32_t new_version) {
  std::vector<AttributeInfo> schema;
  auto s = GetColumnsIncludeDropped(schema, cur_version);
  if (s != SUCCESS) {
    return s;
  }
  ErrorInfo err_info;
  s = AddMetricSchema(schema, cur_version, new_version, err_info);
  if (s != SUCCESS) {
    LOG_ERROR("UpdateVersion failed: table id = %lu, new_version = %u", table_id_, new_version);
    return s;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::UpdateTableVersionOfLastData(uint32_t version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  Get(cur_schema_version_, false)->tableVersionOfLatestData() = version;
  return SUCCESS;
}

uint32_t TsTableSchemaManager::GetTableVersionOfLatestData() {
  rdLock();
  Defer defer([&]() { unLock(); });
  uint32_t version = Get(cur_schema_version_, false)->tableVersionOfLatestData();
  // Version compatibility
  return version == 0 ? 1 : version;
}

KStatus TsTableSchemaManager::GetColAttrInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                                  AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64;
      }
      attr_info.max_len = 3;
      break;
    case roachpb::TIMESTAMP_MICRO:
    case roachpb::TIMESTAMPTZ_MICRO:
    if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN_MICRO;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64_MICRO;
      }
      attr_info.max_len = 6;
      break;
    case roachpb::TIMESTAMP_NANO:
    case roachpb::TIMESTAMPTZ_NANO:
      if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN_NANO;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64_NANO;
      }
      attr_info.max_len = 9;
      break;
    case roachpb::SMALLINT:
      attr_info.type = DATATYPE::INT16;
      break;
    case roachpb::INT:
      attr_info.type = DATATYPE::INT32;
      break;
    case roachpb::BIGINT:
      attr_info.type = DATATYPE::INT64;
      break;
    case roachpb::FLOAT:
      attr_info.type = DATATYPE::FLOAT;
      break;
    case roachpb::DOUBLE:
      attr_info.type = DATATYPE::DOUBLE;
      break;
    case roachpb::BOOL:
      attr_info.type = DATATYPE::BYTE;
      break;
    case roachpb::CHAR:
      attr_info.type = DATATYPE::CHAR;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::BINARY:
    case roachpb::NCHAR:
      attr_info.type = DATATYPE::BINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::VARCHAR:
      attr_info.type = DATATYPE::VARSTRING;
      attr_info.max_len = col.storage_len() - 1;  // because varchar len will +1 when store
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::SDECHAR:
    case roachpb::SDEVARCHAR:
      attr_info.type = DATATYPE::STRING;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return FAIL;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN);
  attr_info.length = col.storage_len();
  if (!col.nullable()) {
    attr_info.setFlag(AINFO_NOT_NULL);
  }
  if (col.dropped()) {
    attr_info.setFlag(AINFO_DROPPED);
  }
  attr_info.col_flag = static_cast<ColumnFlag>(col.col_type());
  attr_info.version = 1;

  return SUCCESS;
}

DATATYPE TsTableSchemaManager::GetTsColDataType() {
  rdLock();
  Defer defer([&]() { unLock(); });
  return (DATATYPE)(Get(cur_schema_version_, false)->getSchemaInfoExcludeDropped()[0].type);
}

const vector<uint32_t>& TsTableSchemaManager::GetIdxForValidCols(uint32_t table_version) {
  rdLock();
  Defer defer([&]() { unLock(); });
  return Get(table_version, false)->getIdxForValidCols();
}

TSTableID TsTableSchemaManager::GetTableID() {
  return table_id_;
}

}  //  namespace kwdbts

