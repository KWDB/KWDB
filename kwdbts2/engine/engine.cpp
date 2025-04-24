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

#include "engine.h"

#include <dirent.h>
#include <iostream>
#include <utility>
#include <shared_mutex>

#include "ee_dml_exec.h"
#include "ee_executor.h"
#include "st_wal_table.h"
#include "st_byrl_table.h"
#include "sys_utils.h"
#include "ts_table.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "ee_exec_pool.h"
#include "st_tier.h"

#ifndef KWBASE_OSS
#include "ts_config_autonomy.h"
#endif

extern std::map<std::string, std::string> g_cluster_settings;
extern DedupRule g_dedup_rule;
extern std::shared_mutex g_settings_mutex;
extern bool g_go_start_service;

namespace kwdbts {
int32_t EngineOptions::iot_interval  = 864000;
string EngineOptions::home_;  // NOLINT
size_t EngineOptions::ps_ = sysconf(_SC_PAGESIZE);

#define DEFAULT_NS_ALIGN_SIZE       2  // 16GB name service

int EngineOptions::ns_align_size_ = DEFAULT_NS_ALIGN_SIZE;
int EngineOptions::table_type_ = ROW_TABLE;
int EngineOptions::double_precision_ = 12;
int EngineOptions::float_precision_ = 6;
int64_t EngineOptions::max_anon_memory_size_ = 1*1024*1024*1024;  // 1G
int EngineOptions::dt32_base_year_ = 2000;
bool EngineOptions::zero_if_null_ = false;
#if defined(_WINDOWS_)
const char BigObjectConfig::slash_ = '\\';
#else
const char EngineOptions::slash_ = '/';
#endif
bool EngineOptions::is_single_node_ = false;
int EngineOptions::table_cache_capacity_ = 1000;
std::atomic<int64_t> kw_used_anon_memory_size;


void EngineOptions::init() {
  char * env_var = getenv(ENV_KW_HOME);
  if (env_var) {
    home_ = string(env_var);
  } else {
    home_ =  getenv(ENV_CLUSTER_CONFIG_HOME);
  }

  env_var = getenv(ENV_KW_IOT_INTERVAL);
  if (env_var) {
    setInteger(iot_interval, string(env_var), 30);
  }
}

TSEngineImpl::TSEngineImpl(kwdbContext_p ctx, const std::string& ts_store_path, const EngineOptions& engine_options) :
                           ts_store_path_(ts_store_path), options_(engine_options) {
  LogInit();
  tables_lock_ = new KLatch(LATCH_ID_TSTABLE_CACHE_LOCK);
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(EngineOptions::table_cache_capacity_, true);
}

TSEngineImpl::~TSEngineImpl() {
  delete tsx_manager_sys_;
  tsx_manager_sys_ = nullptr;

  delete wal_sys_;
  wal_sys_ = nullptr;

  DestoryExecutor();
  delete tables_cache_;
  delete tables_lock_;
  KWDBDynamicThreadPool::GetThreadPool().Stop();
  LOG_DESTORY();
}

KStatus TSEngineImpl::CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                                    std::vector<RangeGroup> ranges) {
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;

  MUTEX_LOCK(tables_lock_);
  Defer defer([&]() {
    MUTEX_UNLOCK(tables_lock_);
  });
  if (IsExists(options_.db_path + '/' + std::to_string(table_id))) {
    LOG_INFO("CreateTsTable skip, TsTable[%lu] is exists", table_id);
    return KStatus::SUCCESS;
  }

  std::shared_ptr<TsTable> table = nullptr;
  UpdateSetting(ctx);
  switch (options_.wal_level) {
  case WALMode::OFF:
    table = std::make_shared<TsTable>(ctx, options_.db_path, table_id);
    break;
  case WALMode::ON:
  case WALMode::SYNC:
  {
    s = wal_sys_->WriteDDLCreateWAL(ctx, 0, table_id, meta, &ranges);
    if (s == FAIL) {
      return s;
    }
    table = std::make_shared<LoggedTsTable>(ctx, options_.db_path, table_id, &options_);
  }
    break;
  case WALMode::BYRL:
  {
    s = wal_sys_->WriteDDLCreateWAL(ctx, 0, table_id, meta, &ranges);
    if (s == FAIL) {
      return s;
    }
    LOG_INFO("create RaftLoggedTsTable %lu", table_id);
    table = std::make_shared<RaftLoggedTsTable>(ctx, options_.db_path, table_id);
  }
    break;
  default:
    LOG_WARN("invalid wal level %d", options_.wal_level);
    break;
  }

  if (IsExists(options_.db_path + '/' + std::to_string(table_id))) {
      LOG_WARN("CreateTsTable TsTable[%lu] is exists, starting to create NTAG index.", table_id);
      for (int i = 0; i < meta->index_info_size(); i ++) {
          std::vector<uint32_t> col_ids;
          for (int idx = 0; idx < meta->index_info(i).col_ids_size(); idx++) {
              col_ids.push_back(meta->index_info(i).col_ids(idx));
          }
          s = table->createNormalTagIndex(ctx, 0, meta->index_info(i).index_id(), meta->ts_table().ts_version(),
                                          meta->ts_table().ts_version(), col_ids);
          if (s != KStatus::SUCCESS) {
              LOG_ERROR("Failed to create ntag hash index, index id:%d.", meta->index_info(i).index_id())
              return s;
          }
      }
      return FAIL;
  }

  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  s = parseMetaSchema(ctx, meta, metric_schema, tag_schema);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint32_t ts_version = 1;
  if (meta->ts_table().has_ts_version()) {
    ts_version = meta->ts_table().ts_version();
  }
  uint64_t partition_interval = EngineOptions::iot_interval;
  if (meta->ts_table().has_partition_interval()) {
    partition_interval = meta->ts_table().partition_interval();
  }
  s = table->Create(ctx, metric_schema, ts_version, partition_interval);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  for (int i = 0; i < ranges.size(); i++) {
    std::shared_ptr<TsEntityGroup> table_range;
    s = table->CreateEntityGroup(ctx, ranges[i], tag_schema, &table_range);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  }

  for (int i = 0; i < meta->index_info_size(); i ++) {
      std::vector<uint32_t> col_ids;
      for (int idx = 0; idx < meta->index_info(i).col_ids_size(); idx++) {
          col_ids.push_back(meta->index_info(i).col_ids(idx));
      }
      s = table->createNormalTagIndex(ctx, 0, meta->index_info(i).index_id(), meta->ts_table().ts_version(),
                                      meta->ts_table().ts_version(), col_ids);
      if (s != KStatus::SUCCESS) {
          LOG_ERROR("Failed to create ntag hash index, index id:%d.", meta->index_info(i).index_id())
          return s;
      }
  }
  LOG_INFO("Table %d create NTAG index success.", table_id)

#ifndef KWBASE_OSS
  TsConfigAutonomy::UpdateTableStatisticInfo(ctx, table, true);
#endif
  tables_cache_->Put(table_id, table);
  auto it = tables_range_groups_.find(table_id);
  if (it != tables_range_groups_.end()) {
    tables_range_groups_.erase(it);
  }
  std::unordered_map<uint64_t, int8_t> range_groups;
  for (auto & range : ranges) {
    range_groups.insert({range.range_group_id, range.typ});
  }
  tables_range_groups_.insert({table_id, range_groups});
  LOG_INFO("Create TsTable %lu success.", table_id);
  return s;
}

KStatus TSEngineImpl::DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) {
  LOG_INFO("start drop table %ld", table_id);
  {
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, true, err_info);
    if (s == FAIL) {
      s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
      if (s == FAIL) {
        LOG_ERROR("drop table [%ld] failed", table_id);
      } else {
        LOG_INFO("drop table [%ld] succeeded", table_id);
      }
      return s;
    }

    if (options_.wal_level > 0) {
      s = wal_sys_->WriteDDLDropWAL(ctx, 0, table_id);
      if (s == FAIL) {
        LOG_INFO("drop table %ld failed", table_id);
        return s;
      }
    }

    table->SetDropped();
  }
  tables_cache_->EraseAndCheckRef(table_id);

  MUTEX_LOCK(tables_lock_);
  Defer defer([&]() {
    MUTEX_UNLOCK(tables_lock_);
  });
  auto it = tables_range_groups_.find(table_id);
  if (it != tables_range_groups_.end()) {
    tables_range_groups_.erase(it);
  }
#ifndef KWBASE_OSS
  TsConfigAutonomy::RemoveTableStatisticInfo(table_id);
#endif
  LOG_INFO("drop table [%ld] succeeded", table_id);
  return SUCCESS;
}

KStatus TSEngineImpl::DropResidualTsTable(kwdbContext_p ctx) {
  std::list<std::pair<KTableKey, std::shared_ptr<TsTable>>> tables = tables_cache_->GetAllValues();
  std::vector<KTableKey> table_ids;
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    table_ids.emplace_back(it->first);
  }
  tables.clear();

  for (auto table_id : table_ids) {
    bool is_exist = checkTableMetaExist(table_id);
    if (!is_exist) {
      KStatus s = DropTsTable(ctx, table_id);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("drop table [%ld] succeeded", table_id);
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("GetTsTable failed")
    return s;
  }

  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  s = table->Compress(ctx, ts, compressed_num, err_info);
  if (s == KStatus::FAIL) {
    LOG_ERROR("table[%lu] compress failed", table_id);
  }
  return s;
}

KStatus TSEngineImpl::GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                                 bool create_if_not_exist, ErrorInfo& err_info, uint32_t version) {
  ts_table = nullptr;
  // 0. First, query the cache based on table_id and pt_time
  std::shared_ptr<TsTable> table = tables_cache_->Get(table_id);
  if (table && !table->IsDropped()) {
    ts_table = table;
  } else if (table && table->IsDropped()) {
    err_info.setError(KWENOOBJ);
    return FAIL;
  } else {
    MUTEX_LOCK(tables_lock_);
    Defer defer([&]() {
      MUTEX_UNLOCK(tables_lock_);
    });
    table = tables_cache_->Get(table_id);
    if (table && !table->IsDropped()) {
      ts_table = table;
    } else if (table && table->IsDropped()) {
      return FAIL;
    } else {
      UpdateSetting(ctx);
      switch (options_.wal_level) {
        case WALMode::OFF:
          table = std::make_shared<TsTable>(ctx, options_.db_path, table_id);
          break;
        case WALMode::ON:
        case WALMode::SYNC:
          table = std::make_shared<LoggedTsTable>(ctx, options_.db_path, table_id, &options_);
          break;
        case WALMode::BYRL:
          table = std::make_shared<RaftLoggedTsTable>(ctx, options_.db_path, table_id);
          break;
        default:
        LOG_WARN("invalid wal level %d", options_.wal_level);
          break;
      }

      std::unordered_map<k_uint64, int8_t> range_groups;
      auto it = tables_range_groups_.find(table_id);
      if (it != tables_range_groups_.end()) {
        range_groups = it->second;
      }
      KStatus s = table->Init(ctx, range_groups, err_info);
      if (s == KStatus::SUCCESS) {
        if (table->IsDropped()) {
          LOG_ERROR("GetTsTable failed: table [%lu] is dropped", table_id)
          return FAIL;
        }
        #ifndef KWBASE_OSS
        TsConfigAutonomy::UpdateTableStatisticInfo(ctx, table, true);
        #endif
        tables_cache_->Put(table_id, table);
        // Load into cache
        ts_table = table;
      } else {
        LOG_INFO("open table [%lu] failed.", table_id)
      }
    }
  }
  // if table no exist. try get schema from go level.
  if (ts_table == nullptr && create_if_not_exist) {
    LOG_INFO("try creating table[%lu] by schema from rocksdb. ", table_id);
    if (!g_go_start_service) {  // unit test from c, just return falsed.
      return KStatus::FAIL;
    }
    char* error;
    size_t data_len = 0;
    char* data = getTableMetaByVersion(table_id, 0, &data_len, &error);
    if (error != nullptr) {
      LOG_ERROR("getTableMetaByVersion error: %s.", error);
      return KStatus::FAIL;
    }
    roachpb::CreateTsTable meta;
    if (!meta.ParseFromString({data, data_len})) {
      LOG_ERROR("Parse schema From String failed.");
      return KStatus::FAIL;
    }
    CreateTsTable(ctx, table_id, &meta, {{default_entitygroup_id_in_dist_v2, 1}});  // no need check result.
    table = tables_cache_->Get(table_id);
    if (table == nullptr || table->IsDropped()) {
      LOG_ERROR("failed during upper version.");
      return KStatus::FAIL;
    }
    ts_table = table;
  }

  if (ts_table == nullptr) {
    LOG_ERROR("GetTsTable[%lu] failed", table_id)
    return KStatus::FAIL;
  }

  if (version != 0 && ts_table->CheckAndAddSchemaVersion(ctx, table_id, version) != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed", table_id);
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TSEngineImpl::GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range,
                                  roachpb::CreateTsTable* meta) {
  std::shared_ptr<TsTable> table;
  ErrorInfo err_info;
  KStatus s = GetTsTable(ctx, table_id, table, true, err_info);
  if (s == FAIL) {
    s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
    return s;
  }
  uint32_t cur_table_version = table->GetCurrentTableVersion();
  LOG_INFO("TSEngineImpl::GetMetaData Begin! table_id: %lu table_version: %u ",
     table_id, cur_table_version);
  // Construct roachpb::CreateTsTable.
  // Set table configures.
  auto ts_table = meta->mutable_ts_table();
  ts_table->set_ts_table_id(table_id);
  ts_table->set_ts_version(cur_table_version);
  ts_table->set_partition_interval(table->GetPartitionInterval());

  // Get table data schema.
  std::vector<AttributeInfo> data_schema;
  s = table->GetDataSchemaIncludeDropped(ctx, &data_schema, cur_table_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetDataSchemaIncludeDropped failed during GetMetaData, table id is %ld.", table_id)
    return s;
  }
  // Get table tag schema.
  std::vector<TagInfo> tag_schema_info;
  s = table->GetTagSchemaIncludeDropped(ctx, range, &tag_schema_info, cur_table_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTagSchema failed during GetMetaData, table id is %ld.", table_id)
    return s;
  }
  // Use data schema and tag schema to construct meta.
  s = table->GenerateMetaSchema(ctx, meta, data_schema, tag_schema_info, cur_table_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("generateMetaSchema failed during GetMetaData, table id is %ld.", table_id)
    return s;
  }
  return s;
}

KStatus TSEngineImpl::PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                TSSlice* payload, int payload_num, uint64_t mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;
  s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("GetTsTable failed, table id: %lu", table_id);
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  for (size_t i = 0; i < payload_num; i++) {
    auto pl_schema_version = Payload::GetTsVsersionFromPayload(&(payload[i]));
    if (table->CheckAndAddSchemaVersion(ctx, table_id, pl_schema_version) != KStatus::SUCCESS) {
      LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed.", table_id);
      return KStatus::FAIL;
    }
    if (nullptr == table->GetMetricsTableMgr()->GetRootTable(pl_schema_version, true)) {
      LOG_ERROR("table[%lu] cannot found version[%u].", table_id, pl_schema_version);
      return KStatus::FAIL;
    }
    // Get EntityGroup and call PutData to write data
    TSSlice primary_key = Payload::GetPrimaryKeyFromPayload(&(payload[i]));
    s = table->GetEntityGroupByPrimaryKey(ctx, primary_key, &table_range);
    if (s == FAIL) {
      LOG_ERROR("PutEntity failed, GetEntityGroup failed %lu", range_group_id)
      return s;
    }
    s = table_range->PutEntity(ctx, payload[i], mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("PutEntity failed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id)
      return s;
    }
  }
  LOG_DEBUG("PutEntity succeed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id);
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                              TSSlice* payload, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                              uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL) {
  std::shared_ptr<TsTable> table;
  KStatus s;
  s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("PutData failed, GetTsTable failed, table id: %lu", table_id)
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  for (size_t i = 0; i < payload_num; i++) {
    auto pl_schema_version = Payload::GetTsVsersionFromPayload(&(payload[i]));
    if (table->CheckAndAddSchemaVersion(ctx, table_id, pl_schema_version) != KStatus::SUCCESS) {
      LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed.", table_id);
      return KStatus::FAIL;
    }
    if (nullptr == table->GetMetricsTableMgr()->GetRootTable(pl_schema_version, true)) {
      LOG_ERROR("table[%lu] cannot found version[%u].", table_id, pl_schema_version);
      return KStatus::FAIL;
    }
    // Get EntityGroup and call PutData to write data
    TSSlice primary_key = Payload::GetPrimaryKeyFromPayload(&(payload[i]));
    s = table->GetEntityGroupByPrimaryKey(ctx, primary_key, &table_range);
    if (s == FAIL) {
      LOG_ERROR("PutData failed, GetEntityGroup failed %lu", range_group_id)
      return s;
    }
    dedup_result->payload_num = payload_num;
    dedup_result->dedup_rule = static_cast<int>(g_dedup_rule);
    s = table_range->PutData(ctx, &(payload[i]), 1, mtr_id, inc_entity_cnt,
                             inc_unordered_cnt, dedup_result, g_dedup_rule, writeWAL);
    if (s == FAIL) {
      LOG_ERROR("PutData failed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id)
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                      HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                      uint64_t mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteRangeData failed: GetTsTable failed, table id [%lu]", table_id)
    return s;
  }
  s = table->DeleteRangeData(ctx, range_group_id, hash_span, ts_spans, count, mtr_id);
  return s;
}

KStatus TSEngineImpl::DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                 std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                 uint64_t mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteData failed: GetTsTable failed, table id [%lu]", table_id)
    return s;
  }
  s = table->DeleteData(ctx, range_group_id, primary_tag, ts_spans, count, mtr_id);
  return s;
}

KStatus TSEngineImpl::DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                     std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteEntities failed: GetTsTable failed, table id [%lu]", table_id)
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteEntities failed: GetEntityGroup failed, range group id [%lu]", range_group_id)
    return s;
  }

  if (table_range) {
    s = table_range->DeleteEntities(ctx, primary_tags, count, mtr_id);
    if (s == KStatus::FAIL) {
      return s;
    } else {
      return KStatus::SUCCESS;
    }
  }
  return KStatus::FAIL;
}

KStatus TSEngineImpl::GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) {
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) {
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) {
  ctx->ts_engine = this;
  KStatus ret = DmlExec::ExecQuery(ctx, req, resp);
  return ret;
}

KStatus TSEngineImpl::LogInit() {
  LogConf cfg = {
    options_.lg_opts.path.c_str(),
    options_.lg_opts.file_max_size,
    options_.lg_opts.dir_max_size,
    options_.lg_opts.level
  };
  LOG_INIT(cfg);
  if (options_.lg_opts.trace_on_off != "") {
    TRACER.SetTraceConfigStr(options_.lg_opts.trace_on_off);
  }
  // comment's breif: if you want to check LOG/TRACER 's function can call next lines
  // LOG_ERROR("TEST FOR log");
  // TRACE_MM_LEVEL1("TEST FOR TRACE aaaaa\n");
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Init(kwdbContext_p ctx) {
  // TODO(jiadx): Create the root directory and test the initialization of GO layer nodes
  char* env_home = getenv("KW_HOME");
  if (env_home == nullptr) {
    LOG_ERROR("no env : KW_HOME ")
    return KStatus::FAIL;
  }
  InitExecutor(ctx, options_);
  LOG_INFO("wal setting: %s.", getWalModeString((WALMode)options_.wal_level).c_str());
  wal_sys_ = KNEW WALMgr(options_.db_path, 0, 0, &options_);
  tsx_manager_sys_ = KNEW TSxMgr(wal_sys_);
  KStatus s = wal_sys_->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Init fail.")
    return s;
  }
  tables_cache_->Init();

  EngineOptions::init();
  s = TsTier::GetInstance().Init(ts_store_path_);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Storage tier init failed")
    return s;
  }

  // loop and recover all the TS tables, and then close them.
  // all incoming request should be blocked before the recover jobs are done.
  s = Recover(ctx);

  // the range applied indexes are only used by recovery process,
  // empty it when the job is done.
  range_indexes_map_.clear();

  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Recovery fail.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::OpenTSEngine(kwdbContext_p ctx, const std::string& primary_ts_path, const EngineOptions& engine_config,
                                   TSEngine** engine) {
  return OpenTSEngine(ctx, primary_ts_path, engine_config, engine, nullptr, 0);
}

KStatus TSEngineImpl::OpenTSEngine(kwdbContext_p ctx, const std::string& ts_store_path, const EngineOptions& engine_config,
                                   TSEngine** engine, AppliedRangeIndex* applied_indexes, size_t range_num) {
  char* env_home = getenv("KW_HOME");
  if (env_home == nullptr) {
    setenv("KW_HOME", engine_config.db_path.c_str(), 1);
  }
  ErrorInfo err_info;
  // initBigObjectApplication(err_info);

  auto* t_engine = new TSEngineImpl(ctx, ts_store_path, engine_config);

  // Initialize the map for Applied Range Indexes.
  t_engine->initRangeIndexMap(applied_indexes, range_num);
  // Initialize: Check if the engine has been opened and if the file system is valid
  // Check WAL and REDO/UNDO data
  // Check range
  // Start various backend threads, such as compression
  KStatus s = t_engine->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSEngineImpl::Init fail.")
    delete t_engine;
    return s;
  }
  *engine = t_engine;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::checkpoint(kwdbts::kwdbContext_p ctx) {
  wal_sys_->Lock();
  KStatus s = wal_sys_->CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to create checkpoint for TS Engine.")
    return s;
  }
  wal_sys_->Unlock();

  return SUCCESS;
}

KStatus TSEngineImpl::CreateCheckpoint(kwdbts::kwdbContext_p ctx) {
  if (options_.wal_level == 0) {
    return KStatus::SUCCESS;
  }
  LOG_DEBUG("creating checkpoint ...");

  if (options_.wal_level == 3) {
    goPrepareFlush();
  }
  // Traverse all EntityGroups in each timeline of the current node
  // For each EntityGroup, call the interface of WALMgr to start the timing library checkpoint operation
  std::list<std::pair<KTableKey, std::shared_ptr<TsTable>>> tables = tables_cache_->GetAllValues();
  auto iter = tables.begin();
  while (iter != tables.end()) {
    if (!iter->second || iter->second->IsDropped()) {
      iter++;
      continue;
    }
    // ignore checkpoint error here, will continue to do the checkpoint for next TS table.
    // for the failed one, will do checkpoint again in next interval.
    iter->second->CreateCheckpoint(ctx);
    iter++;
  }
  if (options_.wal_level == 3) {
    goFlushed();
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CreateCheckpointForTable(kwdbts::kwdbContext_p ctx, TSTableID table_id) {
  if (options_.wal_level == 0) {
    return KStatus::SUCCESS;
  }
  LOG_DEBUG("creating checkpoint for table %lu...", table_id);

  std::shared_ptr<TsTable> table = tables_cache_->Get(table_id);
  if (!table || table->IsDropped()) {
    LOG_WARN("table %lu doesn't exist, %p", table_id, table.get());
    return KStatus::FAIL;
  }
  return table->CreateCheckpoint(ctx);
}

KStatus TSEngineImpl::recover(kwdbts::kwdbContext_p ctx) {
  /*
  *   DDL alter table crash recovery logic, always enabled
  * 1. The log only contains the beginning, indicating that the storage alter has not been performed and the copy has not received a successful message.
  *    The log is discarded. The restored schema is old.
  * 2 logs include the begin alter commit, which indicates that the storage alter has been completed.
  *    If the replica crashes after receiving the commit successfully or before receiving the commit,
  *    there is no need to redo, discard the logs, and restore the schema to a new one.
  * 3 logs include a start alter rollback, which indicates that the storage alter has been completed and it is uncertain whether the rollback has been completed.
  *    The replica crashes after receiving a successful rollback or before receiving a rollback,
  *    Call undo alter to roll back. If the storage determines that the rollback has already occurred, it will be directly reversed. After recovery, the schema will be old.
  * 4 logs include begin alter. It is uncertain whether the storage alter has been completed. If the copy fails to receive a commit, it crashes and calls undo alter,
  *    If an alter has already been executed, it is necessary to clean up the new ones and keep the old ones. The restored schema is old.
   */
  KStatus s;

  TS_LSN checkpoint_lsn = wal_sys_->FetchCheckpointLSN();
  TS_LSN current_lsn = wal_sys_->FetchCurrentLSN();

  std::vector<LogEntry*> redo_logs;
  Defer defer{[&]() {
    for (auto& log : redo_logs) {
      delete log;
    }
  }};

  s = wal_sys_->ReadWALLog(redo_logs, checkpoint_lsn, current_lsn);
  if (s == KStatus::FAIL && !redo_logs.empty()) {
    LOG_ERROR("Failed to read the TS Engine WAL logs.")
#ifdef WITH_TESTS
    return s;
#endif
  }

  std::unordered_map<TS_LSN, LogEntry*> incomplete;
  for (auto wal_log : redo_logs) {
    // From checkpoint loop to the latest commit, including only ddl
    auto mtr_id = wal_log->getXID();

    switch (wal_log->getType()) {
      case WALLogType::TS_BEGIN: {
        incomplete.insert(std::pair<TS_LSN, LogEntry*>(mtr_id, wal_log));
        tsx_manager_sys_->insertMtrID(wal_log->getTsxID().c_str(), mtr_id);
        break;
      }
      case WALLogType::TS_COMMIT: {
        incomplete.erase(mtr_id);
        tsx_manager_sys_->eraseMtrID(mtr_id);
        break;
      }
      case WALLogType::TS_ROLLBACK: {
        if (!incomplete[mtr_id]) {
          break;
        }
        switch (incomplete[mtr_id]->getType()) {
          case WALLogType::DDL_ALTER_COLUMN: {
            DDLEntry* ddl_log = reinterpret_cast<DDLEntry*>(incomplete[mtr_id]);
            uint64_t table_id = ddl_log->getObjectID();
            TsTable table = TsTable(ctx, options_.db_path, table_id);
            std::unordered_map<uint64_t, int8_t> range_groups;
            s = table.Init(ctx, range_groups);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to init table %ld.", table_id)
            #ifdef WITH_TESTS
              return s;
            #endif
            }
            if (table.IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table.UndoAlterTable(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover alter table %ld.", table_id)
            #ifdef WITH_TESTS
              return s;
            #endif
            } else {
              table.TSxClean(ctx);
            }
            break;
          }
          case WALLogType::CREATE_INDEX: {
            CreateIndexEntry* index_log = reinterpret_cast<CreateIndexEntry*>(incomplete[mtr_id]);
            uint64_t table_id = index_log->getObjectID();
            TsTable table = TsTable(ctx, options_.db_path, table_id);
            std::unordered_map<uint64_t, int8_t> range_groups;
            s = table.Init(ctx, range_groups);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to init table %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            }
            if (table.IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table.UndoCreateIndex(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover create index %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            } else {
              table.TSxClean(ctx);
            }
            break;
          }
          case WALLogType::DROP_INDEX: {
            DropIndexEntry* index_log = reinterpret_cast<DropIndexEntry*>(incomplete[mtr_id]);
            uint64_t table_id = index_log->getObjectID();
            TsTable table = TsTable(ctx, options_.db_path, table_id);
            std::unordered_map<uint64_t, int8_t> range_groups;
            s = table.Init(ctx, range_groups);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to init table %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            }
            if (table.IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table.UndoDropIndex(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover drop index %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            } else {
              table.TSxClean(ctx);
            }
            break;
          }
          default:
            LOG_ERROR("The unknown WALLogType type is not processed.")
            break;
        }
        incomplete.erase(mtr_id);
        tsx_manager_sys_->eraseMtrID(mtr_id);
        break;
      }
      case WALLogType::DDL_ALTER_COLUMN: {
        DDLEntry* ddl_log = reinterpret_cast<DDLEntry*>(wal_log);
        incomplete[mtr_id] = ddl_log;
        break;
      }
      case WALLogType::CREATE_INDEX: {
        CreateIndexEntry* ddl_log = reinterpret_cast<CreateIndexEntry*>(wal_log);
        incomplete[mtr_id] = ddl_log;
        break;
      }
      case WALLogType::DROP_INDEX: {
        DropIndexEntry* ddl_log = reinterpret_cast<DropIndexEntry*>(wal_log);
        incomplete[mtr_id] = ddl_log;
        break;
      }
      default:
        LOG_ERROR("The unknown WALLogType type is not processed.")
        break;
    }
  }


  // recover incomplete wal logs.
  for (auto wal_log : incomplete) {
    switch (wal_log.second->getType()) {
      case WALLogType::CREATE_INDEX: {
        CreateIndexEntry* index_log = reinterpret_cast<CreateIndexEntry*>(wal_log.second);
        uint64_t table_id = index_log->getObjectID();
        TsTable table = TsTable(ctx, options_.db_path, table_id);
        std::unordered_map<uint64_t, int8_t> range_groups;
        s = table.Init(ctx, range_groups);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to init table %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        }
        if (table.IsDropped()) {
          LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
          continue;
        }
        s = table.UndoCreateIndex(ctx, index_log);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to recover create index %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        } else {
          table.TSxClean(ctx);
        }
        break;
      }
      case WALLogType::DROP_INDEX: {
        DropIndexEntry* index_log = reinterpret_cast<DropIndexEntry*>(wal_log.second);
        uint64_t table_id = index_log->getObjectID();
        TsTable table = TsTable(ctx, options_.db_path, table_id);
        std::unordered_map<uint64_t, int8_t> range_groups;
        s = table.Init(ctx, range_groups);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to init table %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        }
        if (table.IsDropped()) {
          LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
          continue;
        }

        s = table.UndoDropIndex(ctx, index_log);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to recover drop index %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        } else {
          table.TSxClean(ctx);
        }
        break;
      }
      default:
        LOG_ERROR("The unknown WALLogType type is not processed.")
        break;
    }
  }
  incomplete.clear();

  return SUCCESS;
}

KStatus TSEngineImpl::Recover(kwdbts::kwdbContext_p ctx) {
  LOG_INFO("recover Start.");
  KStatus s = recover(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to recover DDL")
#ifdef WITH_TESTS
    return s;
#endif
  }

  if (options_.wal_level == 0 || options_.wal_level == 3) {
    return KStatus::SUCCESS;
  }

  // Traverse all EntityGroups in each timeline of the current node
  // For each EntityGroup, call the interface of WALMgr to obtain checkpoint information and start log recovery operations
  std::vector<KTableKey> table_id_list;
  GetTableIDList(ctx, table_id_list);

  UpdateSetting(ctx);
  for (KTableKey table_id : table_id_list) {
    LoggedTsTable table = LoggedTsTable(ctx, options_.db_path, table_id, &options_);
    std::unordered_map<uint64_t, int8_t> range_groups;
    s = table.Init(ctx, range_groups);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to init table %ld.", table_id)
#ifdef WITH_TESTS
        return s;
#else
        continue;
#endif
    }
    if (table.IsDropped()) {
      LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
      continue;
    }

    LOG_DEBUG("Start recover table %ld", table_id);
    s = table.Recover(ctx, range_indexes_map_);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to recover table %ld.", table_id)
#ifdef WITH_TESTS
      return s;
#endif
    } else {
      s = table.CreateCheckpoint(ctx);
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to CreateCheckpoint table %ld.", table_id)
#ifdef WITH_TESTS
        return s;
#endif
      }
    }
  }

  LOG_INFO("Recover success.");
  range_indexes_map_.clear();
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::FlushBuffer(kwdbContext_p ctx) {
  if (options_.wal_level == 0) {
    return KStatus::SUCCESS;
  }
  LOG_DEBUG("Start flush WAL buffer, wal_sys_ %p", wal_sys_);

  KStatus s = wal_sys_->Flush(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Flush Buffer failed.")
    return s;
  }

  if (options_.wal_level == 3) {
    return KStatus::SUCCESS;
  }

  std::list<std::pair<KTableKey, std::shared_ptr<TsTable>>> tables = tables_cache_->GetAllValues();
  auto iter = tables.begin();
  while (iter != tables.end()) {
    if (!iter->second || iter->second->IsDropped()) {
      iter++;
      continue;
    }
    // ignore the recover error, record it into ts engine log.
    iter->second->FlushBuffer(ctx);
    iter++;
  }

  return SUCCESS;
}

KStatus TSEngineImpl::TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                 uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  if (options_.wal_level == 0 || options_.wal_level == 3) {
    mtr_id = 0;
    return KStatus::SUCCESS;
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrBegin failed, GetTsTable failed, table id: %lu", table_id)
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  // TODO(liangbo01) need open all entitygroup mtr. not one.
  s = table->GetEntityGroup(ctx, default_entitygroup_id_in_dist_v2, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrBegin failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    return s;
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return FAIL;
    }

    s = entity_group->MtrBegin(ctx, range_id, index, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to begin the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return s;
    } else {
      LOG_DEBUG("Succeed to begin the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      return KStatus::SUCCESS;
    }
  }
  return KStatus::FAIL;
}

KStatus TSEngineImpl::TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                                  uint64_t range_group_id, uint64_t mtr_id) {
  if (options_.wal_level == 0 || options_.wal_level == 3) {
    return KStatus::SUCCESS;
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetTsTable failed, table id: %lu", table_id)
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, 1, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    return s;
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return FAIL;
    }

    s = entity_group->MtrCommit(ctx, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to commit the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return s;
    } else {
      LOG_DEBUG("Succeed to commit the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      return KStatus::SUCCESS;
    }
  }
  return KStatus::FAIL;
}

KStatus TSEngineImpl::TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                                    uint64_t range_group_id, uint64_t mtr_id) {
  if (options_.wal_level == 0 || options_.wal_level == 3 || mtr_id == 0) {
    return KStatus::SUCCESS;
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrRollback failed, GetTsTable failed, table id: %lu", table_id)
    return s;
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    return s;
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return FAIL;
    }

    s = entity_group->MtrRollback(ctx, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to rollback the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      return s;
    } else {
      LOG_DEBUG("Succeed to rollback the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      return KStatus::SUCCESS;
    }
  }
  return KStatus::FAIL;
}

KStatus TSEngineImpl::TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  tsx_manager_sys_->TSxBegin(ctx, transaction_id);

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

  s = table->CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to CreateCheckpoint table %ld.", table_id)
#ifdef WITH_TESTS
    return s;
#endif
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id != 0) {
    if (tsx_manager_sys_->TSxCommit(ctx, transaction_id) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal failed, table id: %lu", table_id)
      return s;
    }
  }

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

  s = table->TSxClean(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, Failed to clean the TS transaction, table id: %lu", table->GetTableId())
    return s;
  }

  if (checkpoint(ctx) == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, system wal checkpoint failed, table id: %lu", table_id)
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id == 0) {
    if (checkpoint(ctx) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal checkpoint failed, table id: %lu", table_id)
      return s;
    }

    return KStatus::SUCCESS;
  }

  s = tsx_manager_sys_->TSxRollback(ctx, transaction_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, TSxRollback failed, table id: %lu", table_id)
    return s;
  }

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

  std::vector<LogEntry*> logs;
  s = wal_sys_->ReadWALLogForMtr(mtr_id, logs);
  if (s == KStatus::FAIL && !logs.empty()) {
    for (auto log : logs) {
      delete log;
    }
    return s;
  }

  std::reverse(logs.begin(), logs.end());
  for (auto log : logs) {
    if (log->getXID() == mtr_id &&  s != FAIL) {
      switch (log->getType()) {
        case WALLogType::DDL_ALTER_COLUMN: {
          s = table->UndoAlterTable(ctx, log);
          if (s == KStatus::SUCCESS) {
            table->TSxClean(ctx);
          }
          break;
        }
        case WALLogType::CREATE_INDEX: {
          s = table->UndoCreateIndex(ctx, log);
          if (s == KStatus::SUCCESS) {
            table->TSxClean(ctx);
          }
          break;
        }
        case WALLogType::DROP_INDEX: {
          s = table->UndoDropIndex(ctx, log);
          if (s == KStatus::SUCCESS) {
            table->TSxClean(ctx);
          }
          break;
        }
        default:
          break;
      }
    }
    delete log;
  }

  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, Failed to ROLLBACK the TS transaction, table id: %lu", table_id)
    tables_cache_->EraseAndCheckRef(table_id);
    return s;
  }

  if (checkpoint(ctx) == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, system wal checkpoint failed, table id: %lu", table_id)
    return s;
  }

  return KStatus::SUCCESS;
}

void TSEngineImpl::GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) {
  DIR* dir_ptr = opendir((options_.db_path).c_str());
  if (dir_ptr) {
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      std::string full_path = options_.db_path + '/' + entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        closedir(dir_ptr);
        return;
      }
      if (S_ISDIR(file_stat.st_mode)) {
        bool is_table = true;
        for (int i = 0; i < strlen(entry->d_name); i++) {
          if (entry->d_name[i] < '0' || entry->d_name[i] > '9') {
            is_table = false;
            break;
          }
        }
        if (!is_table) {
          continue;
        }

        KTableKey table_id = std::stoull(entry->d_name);
        table_id_list.push_back(table_id);
      }
    }
    closedir(dir_ptr);
  }
}

KStatus TSEngineImpl::parseMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                      std::vector<AttributeInfo>& metric_schema,
                                      std::vector<TagInfo>& tag_schema) {
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo attr_info;
    KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, col, attr_info, i == 0);
    if (s != KStatus::SUCCESS) {
      return s;
    }

    if (attr_info.isAttrType(COL_GENERAL_TAG) || attr_info.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(std::move(TagInfo{col.column_id(), attr_info.type,
                                             static_cast<uint32_t>(attr_info.length), 0,
                                             static_cast<uint32_t>(attr_info.size),
                                             attr_info.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG,
                                             attr_info.flag}));
    } else {
      metric_schema.push_back(std::move(attr_info));
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CloseTSEngine(kwdbContext_p ctx, TSEngine* engine) {
  engine->CreateCheckpoint(ctx);
  delete engine;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::UpdateSetting(kwdbContext_p ctx) {
  // After changing the WAL configuration parameters, the already opened table will not change,
  // and the newly opened table will follow the new configuration.
  string value;

  if (GetClusterSetting(ctx, "ts.wal.wal_level", &value) == SUCCESS) {
    options_.wal_level = std::stoll(value);
    LOG_INFO("update wal level to %hhu", options_.wal_level)
  }

  if (GetClusterSetting(ctx, "ts.wal.buffer_size", &value) == SUCCESS) {
    options_.wal_buffer_size = std::stoll(value);
    LOG_INFO("update wal buffer size to %hu Mib", options_.wal_buffer_size)
  }

  if (GetClusterSetting(ctx, "ts.wal.file_size", &value) == SUCCESS) {
    options_.wal_file_size = std::stoll(value);
    LOG_INFO("update wal file size to %hu Mib", options_.wal_file_size)
  }

  if (GetClusterSetting(ctx, "ts.wal.files_in_group", &value) == SUCCESS) {
    options_.wal_file_in_group = std::stoll(value);
    LOG_INFO("update wal file num in group to %hu", options_.wal_file_in_group)
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value) {
  std::shared_lock<std::shared_mutex> lock(g_settings_mutex);
  std::map<std::string, std::string>::iterator iter = g_cluster_settings.find(key);
  if (iter != g_cluster_settings.end()) {
    *value = iter->second;
    return KStatus::SUCCESS;
  } else {
    return KStatus::FAIL;
  }
}

void TSEngineImpl::AlterTableCacheCapacity(int capacity) {
  tables_cache_->SetCapacity(capacity);
}

KStatus TSEngineImpl::AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) {
  ErrorInfo err_info;
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table, true, err_info, cur_version);
  if (s == KStatus::FAIL) {
    err_msg = "Table does not exist";
    return s;
  }

  // Get transaction ID.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  // Write Alter DDL into WAL, which type is ADD_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ADD_COLUMN, cur_version, new_version, column);
  if (s != KStatus::SUCCESS) {
    err_msg = "Write WAL error";
    return s;
  }

  s = table->AlterTable(ctx, AlterType::ADD_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                 TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) {
  ErrorInfo err_info;
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table, true, err_info, cur_version);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Get transaction id.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    return KStatus::FAIL;
  }

  // Write Alter DDL into WAL, which type is DROP_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::DROP_COLUMN, cur_version, new_version, column);
  if (s == KStatus::FAIL) {
    return s;
  }

  s = table->AlterTable(ctx, AlterType::DROP_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Table alters partition interval.
  s = table->AlterPartitionInterval(ctx, partition_interval);
  return s;
}

// Gets the number of remaining threads from the thread pool
KStatus TSEngineImpl::GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) {
  // Get wait thread num
  k_uint32 wait_threads = ExecPool::GetInstance().GetWaitThreadNum();

  // Prepare response
  auto *return_info = static_cast<ThreadInfo *>(resp);
  if (return_info == nullptr) {
    LOG_ERROR("invalid resp pointer")
    return KStatus::FAIL;
  }

  return_info->wait_threads = wait_threads;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                      TSSlice new_column, TSSlice origin_column,
                                      uint32_t cur_version, uint32_t new_version, string& err_msg) {
  ErrorInfo err_info;
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table, true, err_info, cur_version);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Get transaction id.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Write Alter DDL into WAL, which type is ALTER_COLUMN_TYPE.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ALTER_COLUMN_TYPE, cur_version, new_version, origin_column);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn new_col_meta;
  if (!new_col_meta.ParseFromArray(new_column.data, new_column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    return KStatus::FAIL;
  }
  s = table->AlterTable(ctx, AlterType::ALTER_COLUMN_TYPE, &new_col_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    return s;
  }
  *version = table->GetMetricsTableMgr()->GetCurrentTableVersion();
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                           const char* transaction_id, const uint32_t cur_version,
                                           const uint32_t new_version,
                                           const std::vector<uint32_t/* tag column id*/> &index_schema) {
    LOG_INFO("TSEngine CreateNormalTagIndex start, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
              table_id, index_id, cur_version, new_version)
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, true, err_info, cur_version);
    if (s == KStatus::FAIL) {
        return s;
    }

    // Get transaction id.
    uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

    // Write create index DDL into WAL, which type is Create_Normal_TagIndex.
    s = wal_sys_->WriteCreateIndexWAL(ctx, x_id, table_id, index_id, cur_version, new_version, index_schema);
    if (s == KStatus::FAIL) {
      return s;
    }
    // create index
    return table->CreateNormalTagIndex(ctx, x_id, index_id, cur_version, new_version, index_schema);
}

KStatus TSEngineImpl::DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                         const char* transaction_id,  const uint32_t cur_version,
                                         const uint32_t new_version) {
    LOG_INFO("TSEngine DropNormalTagIndex start, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
             table_id, index_id, cur_version, new_version)
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, true, err_info, cur_version);
    if (s == KStatus::FAIL) {
        LOG_ERROR("drop normal tag index, failed to get ts table.")
        return s;
    }

    // Get transaction id.
    uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

    std::vector<uint32_t> tags = table->GetNTagIndexInfo(cur_version, index_id);
    if (tags.empty()) {
      LOG_ERROR("drop normal tag index, ntag info is empty.")
      return FAIL;
    }

    // Write create index DDL into WAL, which type is Create_Normal_TagIndex.
    s = wal_sys_->WriteDropIndexWAL(ctx, x_id, table_id, index_id, cur_version, new_version, tags);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Drop normal tag index write wal failed.")
      return s;
    }

    return table->DropNormalTagIndex(ctx, x_id, cur_version, new_version, index_id);
}

KStatus TSEngineImpl::AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                          const char* transaction_id, const uint32_t old_version, const uint32_t new_version,
                                          const std::vector<uint32_t/* tag column id*/> &new_index_schema) {
  return SUCCESS;
}

KStatus TSEngineImpl::GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) {
  if (wal_level == nullptr) {
    LOG_ERROR("wal_level is nullptr");
    return KStatus::FAIL;
  }
  *wal_level = options_.wal_level;
  return KStatus::SUCCESS;
}

int AggCalculator::cmp(void* l, void* r) {
  switch (type_) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
    case DATATYPE::BOOL:
    case DATATYPE::BINARY: {
      k_int32 ret = memcmp(l, r, size_);
      return ret;
    }
    case DATATYPE::INT16: {
      k_int32 ret = (*(static_cast<k_int16*>(l))) - (*(static_cast<k_int16*>(r)));
      return ret;
    }
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP: {
      k_int64 diff = (*(static_cast<k_int32*>(l))) - (*(static_cast<k_int32*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO: {
      double diff = (*(static_cast<k_int64*>(l))) - (*(static_cast<k_int64*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::TIMESTAMP64_LSN:
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
    case DATATYPE::TIMESTAMP64_LSN_NANO: {
      double diff = (*(static_cast<TimeStamp64LSN*>(l))).ts64 - (*(static_cast<TimeStamp64LSN*>(r))).ts64;
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::FLOAT: {
      double diff = (*(static_cast<float*>(l))) - (*(static_cast<float*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::DOUBLE: {
      double diff = (*(static_cast<double*>(l))) - (*(static_cast<double*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::STRING: {
      k_int32 ret = strncmp(static_cast<char*>(l), static_cast<char*>(r), size_);
      return ret;
    }
      break;
    default:
      break;
  }
  return false;
}

bool AggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool AggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

void* AggCalculator::GetMax(void* base, bool need_to_new) {
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max) > 0) {
      max = current;
    }
  }
  if (base && cmp(base, max) > 0) {
    max = base;
  }
  if (need_to_new && max) {
    void* new_max = malloc(size_);
    memcpy(new_max, max, size_);
    max = new_max;
  }
  return max;
}

void* AggCalculator::GetMin(void* base, bool need_to_new) {
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!min || cmp(current, min) < 0) {
      min = current;
    }
  }
  if (base && cmp(base, min) < 0) {
    min = base;
  }
  if (need_to_new && min) {
    void* new_min = malloc(size_);
    memcpy(new_min, min, size_);
    min = new_min;
  }
  return min;
}

template<class T>
bool AddAggInteger(T& a, T b) {
  T c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = a + b;
  return false;
}

template<class T1, class T2>
bool AddAggInteger(T1& a, T2 b) {
  T1 c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = a + b;
  return false;
}

template<class T1, class T2>
void AddAggFloat(T1& a, T2 b) {
  a = a + b;
}

template<class T>
void SubAgg(T& a, T b) {
  a = a - b;
}

void* AggCalculator::changeBaseType(void* base) {
  void* sum_base = malloc(sizeof(double));
  memset(sum_base, 0, sizeof(double));
  switch (type_) {
    case DATATYPE::INT8:
      *(static_cast<double*>(sum_base)) = *(static_cast<int8_t*>(base));
      break;
    case DATATYPE::INT16:
      *(static_cast<double*>(sum_base)) = *(static_cast<int16_t*>(base));
      break;
    case DATATYPE::INT32:
      *(static_cast<double*>(sum_base)) = *(static_cast<int32_t*>(base));
      break;
    case DATATYPE::INT64:
      *(static_cast<double*>(sum_base)) = *(static_cast<int64_t*>(base));
      break;
    case DATATYPE::FLOAT:
      *(static_cast<double*>(sum_base)) = *(static_cast<float*>(base));
    default:
      break;
  }
  free(base);
  base = nullptr;
  return sum_base;
}

bool AggCalculator::GetSum(void** sum_res, void* base, bool is_overflow) {
  if (!isSumType(type_)) {
    *sum_res = nullptr;
    return false;
  }
  void* sum_base = nullptr, *new_sum_base = nullptr;
  if (base) {
    if (!is_overflow && is_overflow_) {
      sum_base = changeBaseType(base);
    } else if (is_overflow && !is_overflow_) {
      new_sum_base = base;
    } else {
      sum_base = base;
    }
  } else {
    sum_base = malloc(sum_size_);
    memset(sum_base, 0, sum_size_);
  }

  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    switch (sum_type_) {
      case DATATYPE::INT8:
        if (sum_base) {
          if (AddAggInteger<int64_t, int8_t>((*(static_cast<int64_t*>(sum_base))),
                                             (*(static_cast<int8_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int8_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int8_t*>(current))));
        }
        break;
      case DATATYPE::INT16:
        if (sum_base) {
          if (AddAggInteger<int64_t, int16_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int16_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int16_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int16_t*>(current))));
        }
        break;
      case DATATYPE::INT32:
        if (sum_base) {
          if (AddAggInteger<int64_t, int32_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int32_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int32_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int32_t*>(current))));
        }
        break;
      case DATATYPE::INT64:
        if (sum_base) {
          if (AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int64_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int64_t*>(current))));
        }
        break;
      case DATATYPE::DOUBLE:
        AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
        break;
      case DATATYPE::FLOAT:
        AddAggFloat<double, float>((*(static_cast<double*>(sum_base))), (*(static_cast<float*>(current))));
        break;
      default:
        break;
    }
  }
  *sum_res = sum_base ? sum_base : new_sum_base;
  return (new_sum_base != nullptr) || is_overflow_;
}

bool AggCalculator::CalAllAgg(void* min_base, void* max_base, void* sum_base, void* count_base,
                              bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;

  if (block_first_line && sum_base) {
    memset(sum_base, 0, size_);
  }

  bool is_overflow = false;
  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }

    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max) > 0) {
      max = current;
    }
    if (!min || cmp(current, min) < 0) {
      min = current;
    }
    if (isSumType(type_)) {
      // When a memory overflow has occurred, there is no need to calculate the sum result again
      if (is_overflow) {
        continue;
      }
      // sum
      switch (type_) {
        case DATATYPE::INT8:
          is_overflow = AddAggInteger<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          is_overflow = AddAggInteger<int16_t>((*(static_cast<int16_t*>(sum_base))),
                                               (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          is_overflow = AddAggInteger<int32_t>((*(static_cast<int32_t*>(sum_base))),
                                               (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          is_overflow = AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))),
                                               (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          AddAggFloat<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_LSN:
          break;
        default:
          break;
      }
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
  return is_overflow;
}

int VarColAggCalculator::cmp(void* l, void* r) {
  uint16_t l_len = *(reinterpret_cast<uint16_t*>(l));
  uint16_t r_len = *(reinterpret_cast<uint16_t*>(r));
  uint16_t len = min(l_len, r_len);
  void* l_data = reinterpret_cast<void*>((intptr_t)(l) + sizeof(uint16_t));
  void* r_data = reinterpret_cast<void*>((intptr_t)(r) + sizeof(uint16_t));
  k_int32 ret = memcmp(l_data, r_data, len);
  return (ret == 0) ? (l_len - r_len) : ret;
}

bool VarColAggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool VarColAggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

std::shared_ptr<void> VarColAggCalculator::GetMax(std::shared_ptr<void> base) {
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* var_data = var_mem_[i].get();
    if (!max || cmp(var_data, max) > 0) {
      max = var_data;
    }
  }
  if (base && cmp(base.get(), max) > 0) {
    max = base.get();
  }

  uint16_t len = *(reinterpret_cast<uint16_t*>(max));
  void* data = std::malloc(len + MMapStringColumn::kStringLenLen);
  memcpy(data, max, len + MMapStringColumn::kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

std::shared_ptr<void> VarColAggCalculator::GetMin(std::shared_ptr<void> base) {
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* var_data = var_mem_[i].get();
    if (!min || cmp(var_data, min) < 0) {
      min = var_data;
    }
  }
  if (base && cmp(base.get(), min) < 0) {
    min = base.get();
  }
  uint16_t len = *(reinterpret_cast<uint16_t*>(min));
  void* data = std::malloc(len + MMapStringColumn::kStringLenLen);
  memcpy(data, min, len + MMapStringColumn::kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

void VarColAggCalculator::CalAllAgg(void* min_base, void* max_base, std::shared_ptr<void> var_min_base,
                                    std::shared_ptr<void> var_max_base, void* count_base,
                                    bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;
  void* var_min = block_first_line ? nullptr : var_min_base.get();
  void* var_max = block_first_line ? nullptr : var_max_base.get();

  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    void* var_data = var_mem_[i].get();
    if (!max || cmp(var_data, var_max) > 0) {
      max = current;
      var_max = var_data;
    }
    if (!min || cmp(var_data, var_min) < 0) {
      min = current;
      var_min = var_data;
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
}

void AggCalculator::UndoAgg(void* min_base, void* max_base, void* sum_base, void* count_base) {
//  if (block_first_line && sum_base) {
//    memset(sum_base, 0, size_);
//  }

  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) -= 1;
    }

    if (isSumType(type_)) {
      void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
//      if (!max || (cmp(current, max) > 0)) {
//        max = current;
//      }
//      if (!min_base || (cmp(current, min) < 0)) {
//        min = current;
//      }

      // sum
      switch (type_) {
        case DATATYPE::INT8:
          SubAgg<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          SubAgg<int16_t>((*(static_cast<int16_t*>(sum_base))), (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          SubAgg<int32_t>((*(static_cast<int32_t*>(sum_base))), (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          SubAgg<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          SubAgg<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          SubAgg<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_LSN:
          break;
        default:
          break;
      }
    }
  }
}

}  //  namespace kwdbts
