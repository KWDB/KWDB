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
#include <cstdint>
#include "engine.h"
#include "sys_utils.h"
#include "column_utils.h"
#include "ts_partition_interval_recorder.h"

namespace kwdbts {
inline string IdToSchemaFileName(const KTableKey& table_id, uint32_t ts_version) {
  return nameToEntityBigTablePath(std::to_string(table_id), s_bt + "_" + std::to_string(ts_version));
}

int TsTableSchemaManager::getColumnIndex(const AttributeInfo& attr_info) {
  int col_no = -1;
  const std::vector<AttributeInfo>* schema_info{nullptr};
  GetColumnsIncludeDroppedPtr(&schema_info);
  for (int i = 0; i < schema_info->size(); ++i) {
    if ((*schema_info)[i].id == attr_info.id && !(*schema_info)[i].isFlag(AINFO_DROPPED)) {
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
    LOG_ERROR("AlterTableTag failed, table id %lu error: %s ", table_id_, err_info.errmsg.c_str());
    msg = err_info.errmsg;
    return FAIL;
  }
  if (UpdateMetricVersion(cur_version, new_version) != SUCCESS) {
    msg = "Update metric version error";
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::alterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                            uint32_t cur_version, uint32_t new_version, string& msg) {
  ErrorInfo err_info;
  auto col_idx = getColumnIndex(attr_info);
  auto latest_version = cur_version_;
  vector<AttributeInfo> schema;
  KStatus s = GetColumnsIncludeDropped(schema, cur_version);
  if (s != SUCCESS) {
    msg = "schema version " + to_string(cur_version) + " does not exists";
    return FAIL;
  }
  switch (alter_type) {
    case ADD_COLUMN:
      if (col_idx >= 0) {
        if (latest_version == new_version) {
          LOG_WARN("alterTableCol: table id %lu, column id %u already exists", table_id_, attr_info.id);
          return SUCCESS;
        }
        LOG_WARN("alterTableCol failed: table id %lu, column id %u already exists, cur version %u, expect version %u",
                  table_id_, attr_info.id, latest_version, new_version);
        return FAIL;
      }
      schema.emplace_back(attr_info);
      break;
    case DROP_COLUMN:
      if (col_idx < 0) {
        if (latest_version == new_version) {
          LOG_WARN("alterTableCol: column %u has been dropped, table id %lu", attr_info.id, table_id_)
          return SUCCESS;
        }
        LOG_WARN("drop column failed: table id %lu, column id %u, column name %s", table_id_, attr_info.id, attr_info.name);
        return FAIL;
      }
      schema[col_idx].setFlag(AINFO_DROPPED);
      break;
    case ALTER_COLUMN_TYPE: {
      if (col_idx < 0) {
        LOG_ERROR("alter column type failed: table id %lu, column (id %u) does not exists", table_id_, attr_info.id);
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
  s = addMetricForAlter(schema, cur_version, new_version, err_info);
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
  RW_LATCH_X_LOCK(&table_version_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
  AttributeInfo attr_info;
  KStatus s = parseAttrInfo(*column, attr_info, false);
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
  if (s != SUCCESS) {
    LOG_ERROR("AlterTable failed. table_id: %lu alter_type: %hhu cur_version: %u new_version: %u is_general_tag: %d",
           table_id_, alter_type, cur_version, new_version, attr_info.isAttrType(COL_GENERAL_TAG));
    return s;
  }
  if (new_version > cur_version_) {
    cur_version_ = new_version;
  }
  LOG_INFO("AlterTable succeeded. table_id: %lu alter_type: %hhu cur_version: %u new_version: %u is_general_tag: %d",
           table_id_, alter_type, cur_version, new_version, attr_info.isAttrType(COL_GENERAL_TAG));
  return SUCCESS;
}

KStatus TsTableSchemaManager::UndoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                       uint32_t cur_version, uint32_t new_version) {
  RW_LATCH_X_LOCK(&table_version_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
  if (cur_version_ == new_version) {
    return SUCCESS;
  }
  ErrorInfo err_info;
  auto s = UndoAlterCol(cur_version, new_version);
  if (s != SUCCESS) {
    return s;
  }
  if (tag_table_->UndoAlterTagTable(cur_version, new_version, err_info) < 0) {
    LOG_ERROR("UndoAlterTagTable failed, table id %lu error: %s ", table_id_, err_info.errmsg.c_str());
    return FAIL;
  }
  cur_version_ = cur_version;
  return SUCCESS;
}

std::shared_ptr<MMapMetricsTable> TsTableSchemaManager::open(uint32_t ts_version, ErrorInfo& err_info) {
  auto tmp_schema = std::make_shared<MMapMetricsTable>();
  string schema_file_name  = IdToSchemaFileName(table_id_, ts_version);
  tmp_schema->open(schema_file_name , table_path_ / metric_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("metric schema [%s] open failed: %s", schema_file_name.c_str(), err_info.errmsg.c_str())
    return nullptr;
  }
  return tmp_schema;
}

bool TsTableSchemaManager::IsSchemaDirsExist() {
  return IsExists(table_path_ / metric_path_) && IsExists(table_path_ / tag_path_);
}

KStatus TsTableSchemaManager::Init() {
  tag_table_ = std::make_shared<TagTable>(table_path_, tag_path_, table_id_, 1);
  ErrorInfo err_info;
  std::vector<TableVersion> invalid_tag_versions;
  if (tag_table_->open(invalid_tag_versions, err_info) < 0) {
    LOG_ERROR("failed to open the tag table %s%lu, error: %s",
              tag_path_.c_str(), table_id_, err_info.errmsg.c_str());
  }

  metric_mgr_ = std::make_shared<MetricsVersionManager>(table_path_ / metric_path_, table_id_);
  uint32_t tag_cur_version = tag_table_->GetTagTableVersionManager()->GetCurrentTableVersion();

  std::vector<uint32_t> invalid_metric_versions;
  auto s = metric_mgr_->Init(invalid_tag_versions, invalid_metric_versions, tag_cur_version);
  if (s != SUCCESS) {
    LOG_ERROR("table %lu metric manager init failed", table_id_)
    return s;
  }
  if (!invalid_metric_versions.empty()) {
    for (auto& tag_version : invalid_metric_versions) {
      removeTagVersion(tag_version);
    }
    return SUCCESS;
  }

  uint32_t metric_cur_version = metric_mgr_->GetCurrentMetricsVersion();
  assert(metric_cur_version == tag_cur_version);
  if (metric_cur_version != 0) {
    hash_num_ = metric_mgr_->GetHashNum();
  }
  if (metric_cur_version > tag_cur_version) {
    for (uint32_t i = tag_cur_version + 1; i <= metric_cur_version; i++) {
      removeTagVersion(i);
    }
  }
  cur_version_ = metric_cur_version;
  LOG_INFO("Table [%lu] schema manager init success", table_id_);
  return SUCCESS;
}

KStatus TsTableSchemaManager::CreateTable(kwdbContext_p ctx, roachpb::CreateTsTable* meta, uint32_t db_id,
                                          uint32_t ts_version, ErrorInfo& err_info) {
  RW_LATCH_X_LOCK(&table_version_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
  if (ts_version == cur_version_) {
    LOG_WARN("CreateTable: table %lu version [%u] already exists", table_id_, ts_version);
    return SUCCESS;
  }
  if (metric_mgr_ == nullptr) {
    metric_mgr_ = std::make_shared<MetricsVersionManager>(table_path_ / metric_path_, table_id_);
  }
  if (metric_mgr_->GetMetricsTable(ts_version) != nullptr) {
    if (ts_version == cur_version_) {
      LOG_WARN("CreateTable: table %lu version [%u] already exists", table_id_, ts_version);
      return SUCCESS;
    }
    LOG_INFO("CreateTable: metric version [%u] already exists, but current version is [%u]", ts_version, cur_version_);
    return SUCCESS;
  }
  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  auto s = parseMetaToSchema(meta, metric_schema, tag_schema);
  if (s != SUCCESS) {
    return s;
  }
  for (auto& attr : metric_schema) {
    attr.version = ts_version;
  }
  hash_num_ = meta->ts_table().hash_num();
  uint64_t interval = meta->ts_table().partition_interval();
  s = metric_mgr_->CreateTable(ctx, metric_schema, db_id, ts_version, meta->ts_table().life_time(),
                               interval, hash_num_, err_info);
  if (s != SUCCESS) {
    LOG_ERROR("failed to create the metric table %lu version [%u], error: %s",
              table_id_, ts_version, err_info.errmsg.c_str());
    return s;
  }
  s = PartitionIntervalRecorder::GetInstance()->RecordInterval(db_id, interval);
  if (s != SUCCESS) {
    LOG_ERROR("Record partition interval of db_id %d failed, interval %ld", db_id, interval);
    return s;
  }

  if (tag_table_ == nullptr || tag_table_->GetTagTableVersionManager()->GetCurrentTableVersion() == 0) {
    tag_table_ = std::make_shared<TagTable>(table_path_, tag_path_, table_id_, 1);
    std::vector<roachpb::NTagIndexInfo> idx_info;
    for (int i = 0; i < meta->index_info_size(); i++) {
      idx_info.emplace_back(meta->index_info(i));
    }
    if (tag_table_->create(tag_schema, ts_version, idx_info, err_info) < 0) {
      LOG_ERROR("failed to create the tag table [%lu] %s, error: %s",
                table_id_, tag_path_.c_str(), err_info.errmsg.c_str());
      return FAIL;
    }
  } else {
    std::vector<roachpb::NTagIndexInfo> idx_info;
    for (int i = 0; i < meta->index_info_size(); i++) {
      idx_info.emplace_back(meta->index_info(i));
    }
    // Note:: "idx_info" is the index that exists in the current version.
    if (tag_table_->AddNewPartitionVersion(tag_schema, ts_version, err_info, idx_info) < 0) {
      LOG_ERROR("CreateTable failed: table id %lu, add tag new version[%d] failed", table_id_, ts_version);
      return FAIL;
    }
  }
  if (ts_version > cur_version_) {
    cur_version_ = ts_version;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::addMetricForAlter(vector<AttributeInfo>& schema, uint32_t cur_version,
                                              uint32_t new_version, ErrorInfo& err_info) {
  if (metric_mgr_->GetMetricsTable(new_version) != nullptr) {
    LOG_INFO("metric schema version %d already exists, table id %lu", new_version, table_id_);
    return SUCCESS;
  }
  for (auto& attr : schema) {
    attr.version = new_version;
  }
  // Create a new version schema
  string schema_file_name  = IdToSchemaFileName(table_id_, new_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto tmp_schema = std::make_shared<MMapMetricsTable>();
  if (tmp_schema->open(schema_file_name , table_path_ / metric_path_, MMAP_CREAT_EXCL, err_info) >= 0
      || err_info.errcode == KWECORR) {
    tmp_schema->create(schema, new_version, metric_mgr_->GetPartitionInterval(),
                    encoding, err_info, hash_num_);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("metric schema [%s] create error : %s", schema_file_name.c_str(), err_info.errmsg.c_str());
    tmp_schema->remove();
    return FAIL;
  }

  // Copy the metadata of the previous version
  if (cur_version) {
    auto src_bt = metric_mgr_->GetMetricsTable(cur_version);
    if (src_bt == nullptr) {
      LOG_ERROR("addMetricForAlter failed: table id %lu, version %u not exists", table_id_, cur_version);
      return FAIL;
    }
    tmp_schema->metaData()->actul_size = src_bt->metaData()->actul_size;
    tmp_schema->metaData()->life_time = src_bt->metaData()->life_time;
    tmp_schema->metaData()->precision = src_bt->metaData()->precision;
    tmp_schema->metaData()->partition_interval = src_bt->metaData()->partition_interval;
    tmp_schema->metaData()->num_node = src_bt->metaData()->num_node;
    tmp_schema->metaData()->is_dropped = src_bt->metaData()->is_dropped;
    tmp_schema->metaData()->db_id = src_bt->metaData()->db_id;
    tmp_schema->metaData()->hash_num = src_bt->metaData()->hash_num;
  } else {
    tmp_schema->metaData()->db_id = GetDbID();
  }
  time(&tmp_schema->metaData()->create_time);
  tmp_schema->metaData()->schema_version = new_version;

  // The current version must already exist.
  tmp_schema->SetLifeTime(metric_mgr_->GetLifeTime());
  tmp_schema->setObjectReady();

  // Save to map cache
  auto s = metric_mgr_->AddOneVersion(new_version, tmp_schema);
  if (s != SUCCESS) {
    LOG_ERROR("addOneVersion failed: table id %lu, version %u", table_id_, new_version);
    tmp_schema->remove();
    return s;
  }
  if (EngineOptions::force_sync_file) {
    tmp_schema->Sync();
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::parseMetaToSchema(roachpb::CreateTsTable* meta,
                                                std::vector<AttributeInfo>& metric_schema,
                                                std::vector<TagInfo>& tag_schema) {
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    AttributeInfo attr_info;
    KStatus s = parseAttrInfo(col, attr_info, i == 0);
    if (s != KStatus::SUCCESS) {
      return s;
    }

    if (attr_info.isAttrType(COL_GENERAL_TAG) || attr_info.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(TagInfo{col.column_id(), attr_info.type,
                                             static_cast<uint32_t>(attr_info.length), 0,
                                             static_cast<uint32_t>(attr_info.size),
                                             attr_info.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG,
                                             attr_info.flag});
    } else {
      metric_schema.push_back(attr_info);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTableSchemaManager::removeTagVersion(uint32_t version) {
  char* error;
  size_t data_len = 0;
  char* data = getTableMetaByVersion(table_id_, version, &data_len, &error);
  Defer defer{[&]() { free(data); }};
  if (error != nullptr) {
    LOG_ERROR("getTableMetaByVersion failed. table id %lu, version %u, msg: %s", table_id_, version, error);
    free(error);
    return FAIL;
  }
  roachpb::CreateTsTable meta;
  if (!meta.ParseFromString({data, data_len})) {
    LOG_ERROR("Parse schema From String failed.");
    return FAIL;
  }
  std::vector<roachpb::NTagIndexInfo> idx_info;
  for (int i = 0; i < meta.index_info_size(); i++) {
    idx_info.emplace_back(meta.index_info(i));
  }
  ErrorInfo err_info;
  tag_table_->CleanInvalidPartition(version, idx_info, err_info);
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetMeta(kwdbContext_p ctx, uint32_t version, roachpb::CreateTsTable* meta) {
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  const std::vector<AttributeInfo>* metric_meta{nullptr};
  auto s = GetMetricMeta(version, &metric_meta);
  if (s != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed, table id %lu", table_id_);
    return s;
  }
  for (auto col_var : *metric_meta) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    if (!ParseToColumnInfo(col_var, *col)) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate metric Schema, table id %lu", col_var.type, table_id_);
      return FAIL;
    }
  }
  std::vector<TagInfo> tag_meta;
  s = GetTagMeta(version, tag_meta);
  if (s != SUCCESS) {
    LOG_ERROR("GetTagMeta failed, table id %lu", table_id_);
    return s;
  }
  for (auto tag_info : tag_meta) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    // XXX Notice: tag_info don't has tag column name,
    if (!ParseTagColumnInfo(tag_info, *col)) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate tag Schema, table id %lu", tag_info.m_data_type, table_id_);
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
  std::shared_ptr<MMapMetricsTable> schema_table = getMetricsTable(ts_version);
  if (!schema_table) {
    LOG_ERROR("Table %lu schema version [%u] does not exists", table_id_, ts_version);
    return FAIL;
  }
  schema = *schema_table->getSchemaInfoExcludeDroppedPtr();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetColumnsIncludeDropped(std::vector<AttributeInfo>& schema, uint32_t ts_version) {
  std::shared_ptr<MMapMetricsTable> schema_table = getMetricsTable(ts_version);
  if (!schema_table) {
    LOG_ERROR("Table %lu schema version [%u] does not exists", table_id_, ts_version);
    return FAIL;
  }
  schema = *schema_table->getSchemaInfoIncludeDroppedPtr();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetColumnsExcludeDroppedPtr(const std::vector<AttributeInfo>** schema, uint32_t ts_version) {
  auto schema_table = getMetricsTable(ts_version);
  if (!schema_table) {
    LOG_ERROR("Table [%lu] schema version [%u] does not exists", table_id_, ts_version);
    return FAIL;
  }
  *schema = schema_table->getSchemaInfoExcludeDroppedPtr();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetColumnsIncludeDroppedPtr(const std::vector<AttributeInfo>** schema, uint32_t ts_version) {
  auto schema_table = getMetricsTable(ts_version);
  if (!schema_table) {
    LOG_ERROR("Table [%lu] schema version [%u] does not exists", table_id_, ts_version);
    return FAIL;
  }
  *schema = schema_table->getSchemaInfoIncludeDroppedPtr();
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetMetricMeta(uint32_t version, const std::vector<AttributeInfo>** info) {
  return GetColumnsExcludeDroppedPtr(info, version);
}

KStatus TsTableSchemaManager::GetTagMeta(uint32_t version, std::vector<TagInfo>& info) {
  auto version_obj = tag_table_->GetTagTableVersionManager()->GetVersionObject(version);
  if (!version_obj) {
    LOG_ERROR("GetVersionObject not found. table id %lu, table version: %u ", table_id_, version);
    return FAIL;
  }
  auto real_version = version_obj->metaData()->m_real_used_version_;
  TagPartitionTable* tag_pt = nullptr;
  uint32_t dest_version = version >= real_version ? real_version : version;
  tag_pt = tag_table_->GetTagPartitionTableManager()->GetPartitionTable(dest_version);
  if (tag_pt == nullptr) {
    LOG_ERROR("GetPartitionTable not found. table id %lu, table version: %u ", table_id_, dest_version);
    return FAIL;
  }

  auto tag_cols = tag_pt->getIncludeDroppedSchemaInfos();
  for (auto tag_info : tag_cols) {
    if (!tag_info.isDropped()) {
      info.push_back(tag_info);
    }
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetMetricSchema(uint32_t version,
                                              std::shared_ptr<MMapMetricsTable>* schema) {
  auto dst_version = version == 0 ? cur_version_ : version;
  *schema = getMetricsTable(dst_version);
  if (*schema == nullptr) {
    LOG_WARN("table %lu schema version [%u] does not exists", table_id_, dst_version);
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::GetTagSchema(kwdbContext_p ctx, std::shared_ptr<TagTable>* schema) {
  if (tag_table_ == nullptr) {
    return FAIL;
  }
  *schema = tag_table_;
  return SUCCESS;
}

void TsTableSchemaManager::GetAllVersions(std::vector<uint32_t> *table_versions) {
  return metric_mgr_->GetAllVersions(table_versions);
}

LifeTime TsTableSchemaManager::GetLifeTime() const {
  return metric_mgr_->GetLifeTime();
}

void TsTableSchemaManager::SetLifeTime(LifeTime life_time) const {
  metric_mgr_->SetLifeTime(life_time);
}

uint64_t TsTableSchemaManager::GetPartitionInterval() const {
  return metric_mgr_->GetPartitionInterval();
}

void TsTableSchemaManager::SetPartitionInterval(uint64_t partition_interval) const {
  metric_mgr_->SetPartitionInterval(partition_interval);
}

uint32_t TsTableSchemaManager::GetDbID() const {
  return metric_mgr_->GetDbID();
}

KStatus TsTableSchemaManager::SetDropped() {
  return metric_mgr_->SetDropped();
}

bool TsTableSchemaManager::IsDropped() {
  return metric_mgr_->IsDropped();
}

KStatus TsTableSchemaManager::RemoveAll() {
  return metric_mgr_->RemoveAll();
}

KStatus TsTableSchemaManager::UndoAlterCol(uint32_t old_version, uint32_t new_version) {
  return metric_mgr_->UndoAlterCol(old_version, new_version);
}

KStatus TsTableSchemaManager::UpdateMetricVersion(uint32_t cur_version, uint32_t new_version) {
  std::vector<AttributeInfo> schema;
  auto s = GetColumnsIncludeDropped(schema, cur_version);
  if (s != SUCCESS) {
    return s;
  }
  ErrorInfo err_info;
  s = addMetricForAlter(schema, cur_version, new_version, err_info);
  if (s != SUCCESS) {
    LOG_ERROR("UpdateVersion failed: table id %lu, new_version %u", table_id_, new_version);
    return s;
  }
  return SUCCESS;
}

KStatus TsTableSchemaManager::parseAttrInfo(const roachpb::KWDBKTSColumn& col,
                                            AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      attr_info.type = DATATYPE::TIMESTAMP64;
      attr_info.max_len = 3;
      break;
    case roachpb::TIMESTAMP_MICRO:
    case roachpb::TIMESTAMPTZ_MICRO:
      attr_info.type = DATATYPE::TIMESTAMP64_MICRO;
      attr_info.max_len = 6;
      break;
    case roachpb::TIMESTAMP_NANO:
    case roachpb::TIMESTAMPTZ_NANO:
      attr_info.type = DATATYPE::TIMESTAMP64_NANO;
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
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return FAIL;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN - 1);
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

DATATYPE TsTableSchemaManager::GetTsColDataType() const {
  return static_cast<DATATYPE>((*GetCurrentMetricsTable()->getSchemaInfoExcludeDroppedPtr())[0].type);
}

KStatus TsTableSchemaManager::GetIdxForValidCols(vector<uint32_t>& cols, uint32_t table_version) {
  auto metric_table = getMetricsTable(table_version);
  if (metric_table == nullptr) {
    LOG_ERROR("Couldn't find metrics table %lu with version %u", table_id_, table_version);
    return FAIL;
  }
  cols = metric_table->getIdxForValidCols();
  return SUCCESS;
}

bool TsTableSchemaManager::FindVersionConv(uint64_t key, std::shared_ptr<SchemaVersionConv>* version_conv) {
  RW_LATCH_S_LOCK(&ver_conv_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&ver_conv_rw_lock_); }};
  const auto iter = version_conv_map.find(key);
  if (iter == version_conv_map.end()) {
    return false;
  }
  *version_conv = iter->second;
  return true;
}

void TsTableSchemaManager::InsertVersionConv(uint64_t key, const shared_ptr<SchemaVersionConv>& ver_conv) {
  RW_LATCH_X_LOCK(&ver_conv_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&ver_conv_rw_lock_); }};
  version_conv_map.insert(make_pair(key, ver_conv));
}

KStatus TsTableSchemaManager::CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,
                                                   const uint64_t index_id, const uint32_t cur_version,
                                                   const uint32_t new_version,
                                                   const std::vector<uint32_t/* tag column id*/>& tags) {
    RW_LATCH_X_LOCK(&table_version_rw_lock_);
    Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
    ErrorInfo errorInfo;
    errorInfo.errcode = tag_table_->CreateHashIndex(O_CREAT, tags, index_id, cur_version, new_version, errorInfo);
    if (errorInfo.errcode < 0) {
        return FAIL;
    }
    auto s = UpdateMetricVersion(cur_version, new_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Update metric version failed, table id %lu", table_id_);
      return s;
    }
    if (new_version > cur_version_) {
      cur_version_ = new_version;
    }
    return SUCCESS;
}

KStatus TsTableSchemaManager::DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,
                                                 const uint32_t cur_version, const uint32_t new_version,
                                                 const uint64_t index_id) {
    RW_LATCH_X_LOCK(&table_version_rw_lock_);
    Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
    LOG_INFO("DropNormalTagIndex table_id:%lu, index_id:%lu, cur_version:%d, new_version:%d", table_id_, index_id,
      cur_version, new_version)
    ErrorInfo errorInfo;
    errorInfo.errcode = tag_table_->DropHashIndex(index_id, cur_version, new_version, errorInfo);
    if (errorInfo.errcode < 0) {
        return FAIL;
    }
    auto s = UpdateMetricVersion(cur_version, new_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Update metric version error, table id %lu", table_id_);
      return s;
    }
    if (new_version > cur_version_) {
      cur_version_ = new_version;
    }
    return SUCCESS;
}

KStatus TsTableSchemaManager::UndoCreateHashIndex(uint32_t index_id, uint32_t old_version, uint32_t new_version,
                                                  ErrorInfo& err_info) {
  RW_LATCH_X_LOCK(&table_version_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
  if (cur_version_ == new_version) {
    return SUCCESS;
  }
  LOG_INFO("UndoCreateHashIndex table_id:%lu, index_id:%u, cur_version:%d, new_version:%d", table_id_, index_id,
    old_version, new_version)
  ErrorInfo errorInfo;
  errorInfo.errcode = tag_table_->UndoCreateHashIndex(index_id, old_version, new_version, errorInfo);
  if (errorInfo.errcode < 0) {
    return FAIL;
  }
  auto s = UndoAlterCol(old_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBack table version error, table id %lu", table_id_);
    return s;
  }
  cur_version_ = old_version;
  return SUCCESS;
}

KStatus TsTableSchemaManager::UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t old_version,
                      uint32_t new_version, ErrorInfo& err_info) {
  RW_LATCH_X_LOCK(&table_version_rw_lock_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(&table_version_rw_lock_); }};
  if (cur_version_ == new_version) {
    return SUCCESS;
  }
  LOG_INFO("UndoDropHashIndex table_id:%lu, index_id:%u, cur_version:%d, new_version:%d", table_id_, index_id, old_version,
    new_version)
  ErrorInfo errorInfo;
  errorInfo.errcode = tag_table_->UndoDropHashIndex(tags, index_id, old_version, new_version, errorInfo);
  if (errorInfo.errcode < 0) {
    return FAIL;
  }
  auto s = UndoAlterCol(old_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBack metric version failed when undo drop hash index, table id %lu", table_id_);
    return s;
  }
  cur_version_ = old_version;
  return SUCCESS;
}

vector<uint32_t> TsTableSchemaManager::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
    std::vector<uint32_t> ret{};
    ret = tag_table_->GetNTagIndexInfo(ts_version, index_id);
    return ret;
}

bool TsTableSchemaManager::IsExistTableVersion(uint32_t version) {
  if (version == cur_version_) {
    return true;
  }
  if (getMetricsTable(version) == nullptr) {
    LOG_ERROR("Couldn't find metrics table with version %u, table id %lu", version, table_id_);
    return false;
  }
  int retry = 6;
  while (retry > 0) {
    TagVersionObject* tagVersionObject = tag_table_->GetTagTableVersionManager()->GetVersionObject(version);
    if (tagVersionObject && tagVersionObject->isValid()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    retry--;
  }
  if (retry > 0) {
    return true;
  } else {
    LOG_ERROR("Couldn't find table %lu tag with version %u", table_id_, version);
    return false;
  }
}
}  //  namespace kwdbts

