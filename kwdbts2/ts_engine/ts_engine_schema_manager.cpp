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

#include <memory>
#include <regex>

#include "kwdb_type.h"
#include "lg_api.h"
#include "sys_utils.h"
#include "ts_table_schema_manager.h"

unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
std::mt19937 gen(seed);

namespace kwdbts {

TsEngineSchemaManager::TsEngineSchemaManager(const string& schema_root_path) :
    schema_root_path_(schema_root_path), mgrs_rw_latch_(RWLATCH_ID_SCHEMA_MGRS_RWLOCK) {
  auto exists = IsExists(schema_root_path_);
  if (exists == false) {
    ErrorInfo error_info;
    MakeDirectory(schema_root_path_, error_info);
  }
}

TsEngineSchemaManager::~TsEngineSchemaManager() {}

KStatus TsEngineSchemaManager::Init(kwdbContext_p ctx) {
  wrLock();
  Defer defer([&]() { unLock(); });
  // init table schema manager
  std::regex num_regex("^[0-9]+$");
  try {
    if (!fs::exists(schema_root_path_)) {
      LOG_ERROR("Schema directory does not exist: %s", schema_root_path_.c_str());
      return KStatus::FAIL;
    }
    if (!table_schema_mgrs_.empty()) {
      LOG_WARN("Table schema managers is not empty before initialized");
      table_schema_mgrs_.clear();
    }
    for (const auto& table_entry : fs::directory_iterator(schema_root_path_)) {
      if (fs::is_directory(table_entry)) {
        std::string dir_name = table_entry.path().filename().string();
        if (std::regex_match(dir_name, num_regex)) {
          auto table_id = atoi(dir_name.c_str());
          auto tb_schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_, table_id);
          KStatus s = tb_schema_mgr->Init();
          if (s != KStatus::SUCCESS) {
            return s;
          }
          table_schema_mgrs_[table_id] = std::move(tb_schema_mgr);
        }
      }
    }
  } catch (const fs::filesystem_error& e) {
    LOG_ERROR("Filesystem error: %s", e.what());
  } catch (const std::exception& e) {
    LOG_ERROR("Error: %s", e.what());
  }
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::CreateTable(kwdbContext_p ctx, const uint64_t& db_id, const KTableKey& table_id,
                                           roachpb::CreateTsTable* meta) {
  {
    rdLock();
    Defer defer([&]() { unLock(); });
    auto it = table_schema_mgrs_.find(table_id);
    if (it != table_schema_mgrs_.end()) {
      return KStatus::SUCCESS;
    }
  }
  wrLock();
  Defer defer([&]() { unLock(); });
  auto it = table_schema_mgrs_.find(table_id);
  if (it != table_schema_mgrs_.end()) {
    return KStatus::SUCCESS;
  }
  uint32_t ts_version = 1;
  if (meta->ts_table().has_ts_version()) {
    ts_version = meta->ts_table().ts_version();
  }

  // TODO(zzr): customize partition interval
  // uint64_t partition_interval = EngineOptions::iot_interval;
  // if (meta->ts_table().has_partition_interval()) {
  //   partition_interval = meta->ts_table().partition_interval();
  // }

  ErrorInfo err_info;
  string table_path = schema_root_path_.string() + "/" + std::to_string(table_id) + "/";
  MakeDirectory(table_path, err_info);
  string metric_schema_path = table_path + "metric";
  MakeDirectory(metric_schema_path, err_info);
  string tag_schema_path = table_path + "tag";
  MakeDirectory(tag_schema_path, err_info);

  auto tbl_schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_, table_id);
  KStatus s = tbl_schema_mgr->CreateTable(ctx, meta, db_id, ts_version, err_info);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  table_schema_mgrs_[table_id] = std::move(tbl_schema_mgr);
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version,
                                       roachpb::CreateTsTable* meta) {
  KStatus s = KStatus::FAIL;
  // Get Metric Meta
  rdLock();
  auto it = table_schema_mgrs_.find(table_id);
  if (it != table_schema_mgrs_.end()) {
    std::shared_ptr<TsTableSchemaManager> tb_schema = it->second;
    unLock();
    s = tb_schema->GetMeta(ctx, table_id, version, meta);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Table[%ld]-version[%d] get metric schema failed.", table_id, version);
      return s;
    }
  } else {
    unLock();
    wrLock();
    Defer defer([&]() { unLock(); });
    it = table_schema_mgrs_.find(table_id);
    if (it != table_schema_mgrs_.end()) {
      s = it->second->GetMeta(ctx, table_id, version, meta);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Table[%ld]-version[%d] get metric schema failed.", table_id, version);
        return s;
      }
      return KStatus::SUCCESS;
    }
    auto schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_, table_id);
    s = schema_mgr->Init();
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
                                                    std::shared_ptr<MMapMetricsTable>* metric_schema) {
  rdLock();
  auto it = table_schema_mgrs_.find(tbl_id);
  if (it != table_schema_mgrs_.end()) {
    std::shared_ptr<TsTableSchemaManager> tb_schema = it->second;
    unLock();
    return tb_schema->GetMetricSchema(version, metric_schema);
  } else {
    unLock();
    wrLock();
    Defer defer([&]() { unLock(); });
    it = table_schema_mgrs_.find(tbl_id);
    if (it != table_schema_mgrs_.end()) {
      return it->second->GetMetricSchema(version, metric_schema);
    }
    auto schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_, tbl_id);
    KStatus s = schema_mgr->Init();
    if (s != KStatus::SUCCESS) {
      return s;
    }
    s = schema_mgr->GetMetricSchema(version, metric_schema);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Table[%ld]-version[%d] get metric schema failed.", tbl_id, version);
      return s;
    }
    table_schema_mgrs_[tbl_id] = std::move(schema_mgr);
  }
  return KStatus::FAIL;
}

bool TsEngineSchemaManager::IsTableExist(TSTableID tbl_id) {
  auto tbl_schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_.string(), tbl_id);
  if (tbl_schema_mgr->IsSchemaDirsExist()) {
    return true;
  }
  return false;
}

KStatus TsEngineSchemaManager::GetTableList(std::vector<TSTableID>* table_ids) {
  // scan all directory.
  std::error_code ec;
  fs::directory_iterator dir_iter{schema_root_path_, ec};
  std::unordered_map<TSTableID, int> table_scan_times;
  if (ec.value() != 0) {
    LOG_ERROR("GetTableList failed, reason: %s", ec.message().c_str());
    return KStatus::FAIL;
  }
  for (const auto& it : dir_iter) {
    std::string fname = it.path().filename();
    auto split_pos = fname.find("_");
    if (split_pos == std::string::npos) {
      continue;
    }
    TSTableID tbl_id = std::stol(fname.substr(split_pos + 1));
    table_scan_times[tbl_id] += 1;
  }
  for (auto kv : table_scan_times) {
    if (kv.second != 2) {
      LOG_WARN("table[%lu] just has %d directory.", kv.first, kv.second);
    }
    table_ids->push_back(kv.first);
  }
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetTableSchemaMgr(TSTableID tbl_id,
                                                 std::shared_ptr<TsTableSchemaManager>& tb_schema_mgr) {
  assert(tbl_id != 0);
  rdLock();
  auto it = table_schema_mgrs_.find(tbl_id);
  if (it == table_schema_mgrs_.end()) {
    unLock();
    wrLock();
    Defer defer([&]() { unLock(); });
    it = table_schema_mgrs_.find(tbl_id);
    if (it != table_schema_mgrs_.end()) {
      tb_schema_mgr = it->second;
      return KStatus::SUCCESS;
    }
    auto schema_mgr = std::make_unique<TsTableSchemaManager>(schema_root_path_, tbl_id);
    KStatus s = schema_mgr->Init();
    if (s != KStatus::SUCCESS) {
      return s;
    }
    table_schema_mgrs_[tbl_id] = std::move(schema_mgr);
    tb_schema_mgr = table_schema_mgrs_.find(tbl_id)->second;
  } else {
    tb_schema_mgr = it->second;
    unLock();
  }
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetAllTableSchemaMgrs(std::vector<std::shared_ptr<TsTableSchemaManager>>& tb_schema_mgr) {
  rdLock();
  for (auto it : table_schema_mgrs_) {
    tb_schema_mgr.emplace_back(it.second);
  }
  unLock();
  return KStatus::SUCCESS;
}

KStatus TsEngineSchemaManager::GetVGroup(kwdbContext_p ctx, TSTableID tbl_id, TSSlice primary_key,
                                             uint32_t* vgroup_id, TSEntityID* entity_id, bool* new_tag) {
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
    *vgroup_id = groupid;
    *new_tag = false;
    return KStatus::SUCCESS;
  }
  // use consistent hash to allocate vgroup id
  *vgroup_id = GetConsistentVgroupId(primary_key.data, primary_key.len, EngineOptions::vgroup_max_num);
  *new_tag = true;
  return KStatus::SUCCESS;
}

uint32_t TsEngineSchemaManager::GetDBIDByTableID(TSTableID table_id) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  KStatus s = GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s == KStatus::SUCCESS) {
    return tb_schema_mgr->GetDbID();
  }
  return 0;
}

KStatus TsEngineSchemaManager::AlterTable(kwdbContext_p ctx, const KTableKey& table_id, AlterType alter_type,
                                          roachpb::KWDBKTSColumn* column, uint32_t cur_version, uint32_t new_version,
                                          string& msg) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  auto s = GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return tb_schema_mgr->AlterTable(ctx, alter_type, column, cur_version, new_version, msg);
}

impl_latch_virtual_func(TsEngineSchemaManager, &mgrs_rw_latch_)

}  //  namespace kwdbts
