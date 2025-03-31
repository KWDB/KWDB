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

#include "ts_engine_schema_manager.h"

#include <ts_hyperloglog.h>

#include "kwdb_type.h"
#include "lg_api.h"
#include "ts_table_schema_manager.h"
#include "sys_utils.h"

extern const int storage_engine_vgroup_max_num = 1;
unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
std::mt19937 gen(seed);

namespace kwdbts {

TsEngineSchemaManager::TsEngineSchemaManager(const string& schema_root_path) : root_path_(schema_root_path) {
  auto exists = IsExists(schema_root_path);
  if (exists == false) {
    ErrorInfo error_info;
    MakeDirectory(schema_root_path, error_info);
  }
}

TsEngineSchemaManager::~TsEngineSchemaManager() {
}

KStatus TsEngineSchemaManager::Init(kwdbContext_p ctx) {
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  auto it = table_schema_mgrs_.find(table_id);
  if (it != table_schema_mgrs_.end()) {
    return KStatus::FAIL;
  }
  uint32_t ts_version = 1;
  if (meta->ts_table().has_ts_version()) {
    ts_version = meta->ts_table().ts_version();
  }
  uint64_t partition_interval = EngineOptions::iot_interval;
  if (meta->ts_table().has_partition_interval()) {
    partition_interval = meta->ts_table().partition_interval();
  }

  string metric_schema_path = root_path_.string() + "/metric_" + std::to_string(table_id);
  ErrorInfo err_info;
  MakeDirectory(metric_schema_path, err_info);
  string tag_schema_path = root_path_.string() + "/tag_" + std::to_string(table_id);
  MakeDirectory(tag_schema_path, err_info);
  auto tb_schema_mgr = std::make_unique<TsTableSchemaManager>(root_path_.string() + "/", table_id);
  KStatus s = tb_schema_mgr->CreateTable(ctx, meta, ts_version, err_info);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  // TODO(zqh): rwlock
  table_schema_mgrs_[table_id] = std::move(tb_schema_mgr);
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version,
                                       roachpb::CreateTsTable* meta) {
  KStatus s = KStatus::FAIL;
  // Get Metric Meta
  auto tb_schema = table_schema_mgrs_.find(table_id);
  if (tb_schema != table_schema_mgrs_.end()) {
    s = tb_schema->second->GetMeta(ctx, table_id, version, meta);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Table[%ld]-version[%d] get metric schema failed.", table_id, version);
      return s;
    }
  } else {
    auto schema_mgr = std::make_unique<TsTableSchemaManager>(root_path_, table_id);
    s = schema_mgr->Init(ctx);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    s = schema_mgr->GetMeta(ctx, table_id, version, meta);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Table[%ld]-version[%d] get metric schema failed.", table_id, version);
      return s;
    }
    table_schema_mgrs_[table_id] = std::move(schema_mgr);
  }
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetTableMetricSchema(kwdbContext_p ctx, TSTableID tbl_id, uint32_t version,
                                                    std::shared_ptr<MMapMetricsTable>* metric_schema) const {
  auto schema_mgr = table_schema_mgrs_.find(tbl_id);
  if (schema_mgr != table_schema_mgrs_.end()) {
    return schema_mgr->second->GetMetricSchema(ctx, version, metric_schema);
  }
  return KStatus::FAIL;
}

KStatus TsEngineSchemaManager::GetVGroup(kwdbContext_p ctx, TSTableID tbl_id, TSSlice primary_key,
                                             uint32_t* tbl_grp_id, TSEntityID* entity_id, bool* new_tag) const {
  std::shared_ptr<TsTableSchemaManager> tb_schema;
  KStatus s = GetTableSchemaMgr(tbl_id, tb_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaManager failed, table id: %lu", tbl_id);
    return s;
  }

  std::shared_ptr<TagTable> tag_schema;
  tb_schema->GetTagSchema(ctx, &tag_schema);
  uint32_t entityid, groupid;
  if (tag_schema->hasPrimaryKey(primary_key.data, primary_key.len, entityid, groupid)) {
    *entity_id = entityid;
    *tbl_grp_id = groupid;
    return KStatus::SUCCESS;
  }
  // TODO(qinlipeng) lock tag
  // [1, 3]
  std::uniform_int_distribution<int> distrib(1, storage_engine_vgroup_max_num);
  *tbl_grp_id = distrib(gen);
  *new_tag = true;
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::SetTableID2DBID(kwdbContext_p ctx, TSTableID table_id, uint32_t database_id) {
  // TODO(zhangzirui): LOCK
  auto it = table_2_db_.find(table_id);
  if (it == table_2_db_.end()) {
    table_2_db_[table_id] = database_id;
    return KStatus::SUCCESS;
  }
  if (it->second == database_id) {
    return KStatus::SUCCESS;
  }
  LOG_ERROR("table %lu has non-consistent database id, previous: %u, new: %u", table_id, it->second, database_id);
  return KStatus::FAIL;
}

uint32_t TsEngineSchemaManager::GetDBIDByTableID(TSTableID table_id) const {
  auto it = table_2_db_.find(table_id);
  assert(it != table_2_db_.end());
  return it->second;
}

KStatus TsEngineSchemaManager::AlterTable(kwdbContext_p ctx, const KTableKey& table_id, AlterType alter_type,
                                          roachpb::KWDBKTSColumn *column, uint32_t cur_version,
                                          uint32_t new_version, string &msg) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  auto s = GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return tb_schema_mgr->AlterTable(ctx, alter_type, column, cur_version, new_version, msg);
}

}  //  namespace kwdbts

