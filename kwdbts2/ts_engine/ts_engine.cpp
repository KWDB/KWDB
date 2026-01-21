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

#include "ts_engine.h"

#include <dirent.h>
#include <future>
#include <cstdint>
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <map>
#include <string>
#include <memory>
#include <utility>
#include "kwdb_type.h"
#include "lg_api.h"
#include "settings.h"
#include "ts_flush_manager.h"
#include "ts_payload.h"
#include "ee_global.h"
#include "ee_executor.h"
#include "ts_compressor_impl.h"
#include "ts_table_v2_impl.h"
#include "sys_utils.h"
#include "ts_std_utils.h"

// V2
int EngineOptions::vgroup_max_num = 4;
DedupRule EngineOptions::g_dedup_rule = DedupRule::OVERRIDE;
TsIOMode EngineOptions::g_io_mode = TsIOMode::FIO_AND_MMAP;
size_t EngineOptions::mem_segment_max_size = 128 << 20;
int32_t EngineOptions::mem_segment_max_height = 12;
uint32_t EngineOptions::max_last_segment_num = 3;
uint32_t EngineOptions::max_compact_num = 10;
size_t EngineOptions::max_rows_per_block = 4096;
size_t EngineOptions::min_rows_per_block = 512;
int64_t EngineOptions::default_partition_interval = 3600 * 24 * 10;
// default block cache max size is set to 1G
int64_t EngineOptions::block_cache_max_size = 1024 * 1024 * 1024;
uint8_t EngineOptions::compress_stage = 2;
bool EngineOptions::compress_last_segment = false;
#ifdef KWBASE_OSS
bool EngineOptions::force_sync_file = false;
#else
bool EngineOptions::force_sync_file = true;
#endif
size_t EngineOptions::last_cache_max_size = 1 << 30;
double EngineOptions::block_filter_sampling_ratio = 0.2;
int EngineOptions::count_stats_recalc_cycle = 60 * 5;
uint32_t EngineOptions::metric_schema_cache_capacity = 100;

extern std::map<std::string, std::string> g_cluster_settings;
extern std::shared_mutex g_settings_mutex;
extern bool g_go_start_service;

namespace kwdbts {

unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
std::mt19937 gen(seed);
const char schema_directory[] = "schema";
constexpr char vroup_cfg_file[] = "ts-vgroup.cfg";

KStatus loadVGroupCfg(const fs::path& ts_store_path, std::map<int, std::string>& vgroup_cfg) {
  fs::path vgroup_cfg_path = ts_store_path / std::string(vroup_cfg_file);
  std::ifstream ifs(vgroup_cfg_path);
  if (!ifs.is_open()) {
    return SUCCESS;
  }

  std::string line;
  while (std::getline(ifs, line)) {
    auto first_non_space = line.find_first_not_of(" \t\n\r");
    if (first_non_space == std::string::npos) {
      continue;
    }
    if (line[first_non_space] == '#') {
      continue;
    }

    std::istringstream ss(line);
    std::string id_str, dir_path;
    std::getline(ss, id_str, ',');
    std::getline(ss, dir_path, ',');

    auto tmp_path = dir_path;

    dir_path.erase(0, dir_path.find_first_not_of(" \t\n\r"));
    dir_path.erase(dir_path.find_last_not_of(" \t\n\r") + 1);
    while (!dir_path.empty() && dir_path.back() == '/') {
      dir_path.pop_back();
    }
    if (dir_path.empty()) {
      LOG_ERROR("User defined vgroup disk error: Invalid disk path [%s]", tmp_path.c_str());
      return FAIL;
    }
#if defined(__GNUC__) && (__GNUC__ < 8)
    dir_path = lexically_normal(dir_path);
#else
    dir_path = fs::path(dir_path).lexically_normal().string();
#endif
    int vgroup_id;
    try {
      vgroup_id = std::stoi(id_str);
      if (vgroup_id <= 0 || vgroup_id > EngineOptions::vgroup_max_num) {
        LOG_ERROR("User defined vgroup disk error: Invalid vgroup id %d", vgroup_id);
        return FAIL;
      }
    } catch (std::exception& e) {
      LOG_ERROR("User defined vgroup disk error: Invalid vgroup id %s", id_str.c_str());
      return FAIL;
    }
    if (!DirExists(dir_path)) {
      LOG_ERROR("User defined vgroup disk error: Directory [%s] does not exists ", dir_path.c_str());
      return FAIL;
    }
    if (access(dir_path.c_str(), R_OK | W_OK) != 0) {
      LOG_ERROR("User defined vgroup disk error: No R/W rights to directory [%s]", dir_path.c_str());
      return FAIL;
    }

    if (vgroup_cfg.find(vgroup_id) != vgroup_cfg.end()) {
      LOG_ERROR("User defined vgroup disk error: Duplicate vgroup id %d", vgroup_id);
      return FAIL;
    }
    vgroup_cfg[vgroup_id] = dir_path;
  }
  auto count = vgroup_cfg.size();
  if (count && count != EngineOptions::vgroup_max_num) {
    LOG_ERROR("User defined vgroup disk error: The count of VGroup should be %d, but %lu groups are configured",
               EngineOptions::vgroup_max_num, count);
    return FAIL;
  }
  return SUCCESS;
}

TSEngineImpl::TSEngineImpl(const EngineOptions& engine_options)
    : options_(engine_options),
      read_batch_workers_lock_(RWLATCH_ID_READ_BATCH_DATA_JOB_RWLOCK),
      write_batch_workers_lock_(RWLATCH_ID_WRITE_BATCH_DATA_JOB_RWLOCK),
      tag_lock_(EngineOptions::vgroup_max_num * 2, RWLATCH_ID_ENGINE_INSERT_TAG_RWLOCK) {
  LogInit();
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(EngineOptions::table_cache_capacity_, true);
  char* vgroup_num = getenv("KW_VGROUP_NUM");
  if (vgroup_num != nullptr) {
    char *endptr;
    EngineOptions::vgroup_max_num = strtol(vgroup_num, &endptr, 10);
    assert(*endptr == '\0');
  }
  interval_recorder_ = PartitionIntervalRecorder::GetInstance();
  char* io_mode = getenv("KW_IO_MODE");
  if (io_mode != nullptr) {
    char* endptr;
    int64_t mode = strtol(io_mode, &endptr, 10);
    if (mode >= 0 && mode <= 2) {
      EngineOptions::g_io_mode = static_cast<TsIOMode>(mode);
    }
  }
  TsFlushJobPool::GetInstance().Start();
}

TSEngineImpl::~TSEngineImpl() {
  DestoryExecutor();
#ifndef WITH_TESTS
  BrMgr::GetInstance().Destroy();
#endif
  TsFlushJobPool::GetInstance().StopAndWait();
  vgroups_.clear();
  SafeDeletePointer(tables_cache_);
}

KStatus TSEngineImpl::FlushVGroups(kwdbContext_p ctx) {
  for (auto vgroup : vgroups_) {
    KStatus s = vgroup->Flush();
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to flush metric file.")
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::SortWALFile(kwdbContext_p ctx) {
  if (!IsExists(wal_mgr_->GetWALChkFilePath())) {
    return KStatus::SUCCESS;
  }
  std::vector<LogEntry*> cur_eng_logs;
  Defer defer_engine{[&]() {
    for (auto& log : cur_eng_logs) {
      delete log;
    }
  }};
  std::vector<uint64_t> ignore;
  bool is_end_chk = false;

  TS_OSN last_lsn = wal_mgr_->GetFirstLSN();
  auto res = wal_mgr_->ReadWALLog(cur_eng_logs, last_lsn, wal_mgr_->FetchCurrentLSN(), ignore);
  if (!cur_eng_logs.empty()) {
    if ((*cur_eng_logs.end())->getType() == WALLogType::END_CHECKPOINT) {
      is_end_chk = true;
    }
  }
  if (is_end_chk) {
    if (wal_mgr_->RemoveChkFile(ctx) == KStatus::FAIL) {
      LOG_ERROR("RemoveChkFile fail while Sorting WAL File.")
      return KStatus::FAIL;
    }
    for (auto vgroup : vgroups_) {
      if (vgroup->GetWALManager()->RemoveChkFile(ctx) == KStatus::FAIL) {
        LOG_ERROR("Remove vgroup[%d] wal file fail while Sorting WAL File.", vgroup->GetVGroupID())
        return KStatus::FAIL;
      }
    }
  } else {
    // remove engine cur, rename chk to cur, update meta
    if (wal_mgr_->ResetCurLSNAndFlushMeta(ctx, last_lsn) == KStatus::FAIL) {
      LOG_ERROR("ResetCurLSNAndFlushMeta failed.")
      return KStatus::FAIL;
    }
    wal_mgr_.release();
    auto cur_eng_path = options_.db_path + "/wal/engine/" + "kwdb_wal.cur";
    auto chk_eng_path = options_.db_path + "/wal/engine/" + "kwdb_wal.chk";
    if (Remove(cur_eng_path) == KStatus::FAIL) {
      LOG_ERROR("Remove wal file fail while Sorting WAL File.")
      return KStatus::FAIL;
    }
    if (-1 == rename(chk_eng_path.c_str(), cur_eng_path.c_str())) {
      LOG_ERROR("Failed to rename WAL file.")
      return KStatus::FAIL;
    }
    wal_mgr_ = std::make_unique<WALMgr>(options_.db_path, "engine", &options_);
    res = wal_mgr_->Init(ctx);
    if (res == KStatus::FAIL) {
      LOG_ERROR("Failed to initialize WAL manager")
      return res;
    }

    // append cur log to chk log, rename chk to cur
    for (auto vgroup : vgroups_) {
      if (!IsExists(vgroup->GetWALManager()->GetWALChkFilePath())) {
        continue;
      }
      std::vector<LogEntry*> append_logs;
      Defer defer_append{[&]() {
        for (auto& log : append_logs) {
          delete log;
        }
      }};
      WALMgr* v_wal = vgroup->GetWALManager();
      TS_OSN v_last_lsn = v_wal->GetFirstLSN();
      std::vector<uint64_t> v_ignore;
      if (v_wal->ReadWALLog(append_logs, v_last_lsn, v_wal->FetchCurrentLSN(), ignore) == KStatus::FAIL) {
        LOG_ERROR("Failed to read WAL from vgroup file.")
        return KStatus::FAIL;
      }
      if (v_wal->SwitchLastFile(ctx, v_last_lsn) == KStatus::FAIL) {
        LOG_ERROR("Failed to SwitchLastFile vgroup file.")
        return KStatus::FAIL;
      }
      if (v_wal->WriteIncompleteWAL(ctx, append_logs) == KStatus::FAIL) {
        LOG_ERROR("Failed to WriteIncompleteWAL to vgroup wal file.")
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Init(kwdbContext_p ctx) {
#ifndef WITH_TESTS
  // init brpc for multi-node mode
  if (!EngineOptions::isSingleNode()) {
    if (BrMgr::GetInstance().Init(ctx, options_) != KStatus::SUCCESS) {
      LOG_ERROR("BrMgr init failed")
      return KStatus::FAIL;
    }
  }
#endif

  PreClearDroppedTables();
  fs::path db_path{options_.db_path};
  assert(!db_path.empty());
  schema_mgr_ = std::make_unique<TsEngineSchemaManager>(db_path / schema_directory);
  KStatus s = schema_mgr_->Init(ctx);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  InitExecutor(ctx, options_);

  std::map<int, std::string> vgroup_cfg;
  s = loadVGroupCfg(options_.ts_store_path_, vgroup_cfg);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  bool vgroup_configured = vgroup_cfg.size() == EngineOptions::vgroup_max_num ? true : false;
  vgroups_.clear();
  fs::path wal_path = db_path / "wal";
  s = TsIOEnv::GetInstance().NewDirectory(wal_path);
  if (s == FAIL) {
    LOG_ERROR("Failed to create directory: %s", wal_path.c_str());
    return s;
  }

  for (int vgroup_id = 1; vgroup_id <= EngineOptions::vgroup_max_num; vgroup_id++) {
    std::unique_ptr<TsVGroup> vgroup{nullptr};
    if (vgroup_configured) {
      vgroup = std::make_unique<TsVGroup>(&options_, vgroup_id, schema_mgr_.get(), &wal_level_mutex_, &tag_lock_,
                                          vgroup_cfg[vgroup_id]);
    } else {
      vgroup = std::make_unique<TsVGroup>(&options_, vgroup_id, schema_mgr_.get(), &wal_level_mutex_, &tag_lock_);
    }
    s = vgroup->Init(ctx);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    uint32_t entity_id = 0;
    s = GetMaxEntityIdByVGroupId(ctx, vgroup_id, entity_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetMaxEntityIdByVGroupId failed, vgroup id:%d", vgroup_id);
    }
    vgroup->InitEntityID(entity_id);
    vgroups_.push_back(std::move(vgroup));
  }
  LOG_INFO("TS engine WAL level is: %d", options_.wal_level);
  wal_mgr_ = std::make_unique<WALMgr>(options_.db_path, "engine", &options_);
  auto res = wal_mgr_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  if (SortWALFile(ctx) != KStatus::SUCCESS) {
    LOG_ERROR("Failed to SortWALFile.")
    return res;
  }

  wal_sys_ = std::make_unique<WALMgr>(options_.db_path, "ddl", &options_);
  tsx_manager_sys_ = std::make_unique<TSxMgr>(wal_sys_.get());
  s = wal_sys_->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Init fail.")
    return s;
  }

// try catch exception if malloc fail
  try {
    s = Recover(ctx);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Recover fail. Now Reset ALL WAL.")
      if (wal_mgr_->ResetWAL(ctx) == KStatus::FAIL) {
        LOG_ERROR("Reset Engine WAL Failed.")
        return KStatus::FAIL;
      }
      if (wal_sys_->ResetWAL(ctx) == KStatus::FAIL) {
        LOG_ERROR("Reset DDL WAL Failed.")
        return KStatus::FAIL;
      }
      for (auto vgroup : vgroups_) {
        if (vgroup->GetWALManager()->ResetWAL(ctx) == KStatus::FAIL) {
          LOG_ERROR("Reset VWAL[%d] Failed.", vgroup->GetVGroupID())
          return KStatus::FAIL;
        }
      }
    }
  } catch (...) {
    LOG_ERROR("Recover fail. Now Reset ALL WAL.")
    if (wal_mgr_->ResetWAL(ctx) == KStatus::FAIL) {
      LOG_ERROR("Reset Engine WAL Failed.")
      return KStatus::FAIL;
    }
    if (wal_sys_->ResetWAL(ctx) == KStatus::FAIL) {
      LOG_ERROR("Reset DDL WAL Failed.")
      return KStatus::FAIL;
    }
    for (auto vgroup : vgroups_) {
      if (vgroup->GetWALManager()->ResetWAL(ctx) == KStatus::FAIL) {
        LOG_ERROR("Reset VWAL[%d] Failed.", vgroup->GetVGroupID())
        return KStatus::FAIL;
      }
    }
  }

  return KStatus::SUCCESS;
}

void TSEngineImpl::PreClearDroppedTables() {
  fs::path db_path{options_.db_path};
  std::error_code ec;
  fs::directory_iterator dir_iter{db_path / schema_directory, ec};
  if (ec.value() != 0) {
    return;
  }
  for (const auto& it : dir_iter) {
    std::string fname = it.path().filename();
    auto split_pos = fname.find(".");
    if (split_pos != std::string::npos) {
      // This directory might not exist, but we don't mind, we just need to delete.
      Remove(db_path / schema_directory / fname.substr(split_pos + 1));
      Remove(db_path / schema_directory / fname);
      auto table_id = std::stol(fname.substr(split_pos + 1));
      DropTableManager::getInstance().markTableDropped(table_id);
    }
  }
}

TsVGroup* TSEngineImpl::GetVGroupByID(kwdbContext_p ctx, uint32_t vgroup_id) {
  if (EngineOptions::vgroup_max_num < vgroup_id || vgroup_id <= 0) {
    LOG_ERROR("vgroup_id is wrong! vgroup_max_num is [%d], vgroup_id is [%u], vgroups_ size is [%zu]",
              EngineOptions::vgroup_max_num, vgroup_id, vgroups_.size())
  }
  assert(EngineOptions::vgroup_max_num >= vgroup_id);
  return vgroups_[vgroup_id - 1].get();
}

KStatus TSEngineImpl::CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
  std::vector<RangeGroup> ranges, bool not_get_table) {
  std::shared_ptr<TsTable> ts_table;
  return CreateTsTable(ctx, table_id, meta, ts_table);
}

KStatus TSEngineImpl::CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable *meta,
  std::shared_ptr<TsTable>& ts_table) {
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;
  table_mutex_.lock();
  Defer defer{[&]() {
    table_mutex_.unlock();
  }};
  ts_table = tables_cache_->Get(table_id);
  if (ts_table != nullptr) {
    LOG_INFO("TsTable %lu exist. no need create again.", table_id);
    return KStatus::SUCCESS;
  }

  uint32_t db_id = 1;
  if (meta->ts_table().has_database_id()) {
    db_id = meta->ts_table().database_id();
  }

  s = schema_mgr_->CreateTable(ctx, db_id, table_id, meta);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("schema Create Table[%lu] failed.", table_id);
    return s;
  }
  LOG_INFO("Create TsTable %lu success.", table_id);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = schema_mgr_->GetTableSchemaMgr(table_id, table_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get table schema manager [%lu] failed.", table_id);
    return s;
  }
  ts_table = std::make_shared<TsTableV2Impl>(table_schema_mgr, vgroups_);
  tables_cache_->Put(table_id, ts_table);
  return s;
}

KStatus TSEngineImpl::CheckAndDropTsTable(kwdbContext_p ctx, const KTableKey& table_id,
                    bool& is_dropped, ErrorInfo& err_info) {
  if (DropTableManager::getInstance().isTableDropped(table_id)) {
    is_dropped = true;
    LOG_INFO("Find table: %ld in DropTableManager, already dropped.", table_id);
    return KStatus::SUCCESS;
  }
  if (tables_cache_->Exists(table_id)) {
    tables_cache_->Erase(table_id);
  }
  KStatus s = ProcessDrop(table_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Process drop table failed, table id: %ld", table_id);
  }
  is_dropped = true;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                    bool& is_dropped, bool create_if_not_exist, ErrorInfo& err_info, uint32_t version) {
  ctx->ts_engine = this;
  ts_table = tables_cache_->Get(table_id);
  if (ts_table == nullptr) {
    if (DropTableManager::getInstance().isTableDropped(table_id)) {
      is_dropped = true;
      LOG_INFO("Find table: %ld in DropTableManager, already dropped.", table_id);
      return KStatus::FAIL;
    }
    if (HasDroppedFlag(table_id)) {
      ProcessDrop(table_id);
      // set flag, insert into dropped_tables_ and remove table schema
      is_dropped = true;
      return KStatus::FAIL;
    }
    // 1. if table exist, open table.
    if (schema_mgr_->IsTableExist(table_id)) {
      std::shared_ptr<TsTableSchemaManager> schema;
      KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, schema);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("can not GetTableSchemaMgr table[%lu]", table_id);
        return KStatus::FAIL;
      }
      auto table = std::make_shared<TsTableV2Impl>(schema, vgroups_);
      if (table.get() != nullptr) {
        ts_table = table;
        tables_cache_->Put(table_id, ts_table);
      } else {
        LOG_ERROR("make TsTableV2Impl failed for table[%lu]", table_id);
        return KStatus::FAIL;
      }
    }
  }

  if (ts_table == nullptr) {
    if (!create_if_not_exist) {
      LOG_ERROR("cannot found table[%lu], and cannot create it.", table_id);
      err_info.errcode = KWENOOBJ;
      return KStatus::FAIL;
    }
    // 2. if table no exist. try get schema from go level.
    LOG_INFO("try creating table[%lu] by schema from rocksdb. ", table_id);
    if (!g_go_start_service) {  // unit test from c, just return falsed.
      return KStatus::FAIL;
    }
    char* error;
    size_t data_len = 0;
    char* data = getTableMetaByVersion(table_id, version, &data_len, &error);
    Defer defer{[&]() { free(data); }};
    if (error != nullptr) {
      LOG_ERROR("getTableMetaByVersion error: %s.", error);
      free(error);
      return KStatus::FAIL;
    }
    roachpb::CreateTsTable meta;
    if (!meta.ParseFromString({data, data_len})) {
      LOG_ERROR("Parse schema From String failed.");
      return KStatus::FAIL;
    }
    CreateTsTable(ctx, table_id, &meta, ts_table);  // no need check result.
    if (ts_table == nullptr) {
      LOG_ERROR("table[%lu] create failed.", table_id);
      return KStatus::FAIL;
    }
  }
  // 3. check if table has certain version. if not found, add certain version from rocksdb.
  if (ts_table != nullptr) {
    if (version != 0 && ts_table->CheckAndAddSchemaVersion(ctx, table_id, version) != KStatus::SUCCESS) {
      LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed", table_id);
      return KStatus::FAIL;
    }
  } else {
    LOG_ERROR("table[%lu] open failed.", table_id);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::ProcessDrop(const KTableKey& table_id) {
  if (!HasDroppedFlag(table_id)) {
    createDroppedFlag(table_id);
  }
  // set flag, insert into dropped_tables_ and remove table schema
  // is_dropped = true;
  if (schema_mgr_->IsTableExist(table_id)) {
    auto s = schema_mgr_->DropTableSchemaMgr(table_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DropTableSchemaMgr failed, table id:%ld", table_id);
      return s;
    }
  }
  DropTableManager::getInstance().markTableDropped(table_id);
  removeDroppedFlag(table_id);
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTableSchemaMgr(kwdbContext_p ctx, const KTableKey& table_id, bool& is_dropped,
                                          std::shared_ptr<TsTableSchemaManager>& schema) {
  std::shared_ptr<TsTable> tb;
  ErrorInfo err_info;
  // Try to obtain the table(will create table if table is not created) to avoid incomplete asynchronous table creation,
  // if the table is not created, the table schema manager cannot be retrieved.
  KStatus s = GetTsTable(ctx, table_id, tb, is_dropped, true, err_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get TsTable failed, table id: %lu, error info: %s", table_id, err_info.errmsg.c_str());
    return s;
  }
  // TODO(liangbo01)  need input change version
  s = schema_mgr_->GetTableSchemaMgr(table_id, schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr failed. table id: %lu", table_id);
  }
  return s;
}

KStatus TSEngineImpl::CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                           const char* transaction_id, bool& is_dropped, const uint32_t cur_version,
                                           const uint32_t new_version, const std::vector<uint32_t> &index_schema) {
    LOG_INFO("TSEngine CreateNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             table_id, index_id, cur_version, new_version)
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, cur_version);
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
                                         const char* transaction_id, bool& is_dropped, const uint32_t cur_version,
                                         const uint32_t new_version) {
    LOG_INFO("TSEngine DropNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             table_id, index_id, cur_version, new_version)
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, cur_version);
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
  const char* transaction_id, bool& is_dropped, const uint32_t old_version, const uint32_t new_version,
  const std::vector<uint32_t/* tag column id*/> &new_index_schema) {
    return SUCCESS;
}

KStatus TSEngineImpl::putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id,
                                   TsRawPayload &payload) {
  ErrorInfo err_info;
  // 1. Write tag data
  uint8_t payload_data_flag = payload.GetRowType();
  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY) {
    // tag
    LOG_DEBUG("tag bt insert hashPoint=%hu", payload.GetHashPoint());

    auto tbl_version = payload.GetTableVersion();
    std::shared_ptr<kwdbts::TsTable> ts_table;
    bool is_dropped = false;
    KStatus s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, tbl_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, tbl_version, err_info.errmsg.c_str());
      return s;
    }

    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTableSchemaMgr failed. table id: %lu", table_id);
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed get table id[%ld] version id[%d] tag schema.", table_id, tbl_version);
      return s;
    }
    err_info.errcode = tag_table->InsertTagRecord(payload, groupid, entity_id, payload.GetOSN(), OperateType::Insert);
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::InsertTagData(kwdbContext_p ctx, const std::shared_ptr<TsTableSchemaManager>& tb_schema,
                                    uint64_t mtr_id, TSSlice payload_data, bool write_wal, uint32_t& vgroup_id,
                                    TSEntityID& entity_id, bool& new_tag) {
  TSSlice primary_key = TsRawPayload::GetPrimaryKeyFromSlice(payload_data);
  KStatus s = schema_mgr_->GetVGroup(ctx, tb_schema, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  auto vgroup = GetVGroupByID(ctx, vgroup_id);
  assert(vgroup != nullptr);
  if (new_tag) {
    uint64_t hash_point = t1ha1_le(primary_key.data, primary_key.len);
    tag_lock_.WrLock(hash_point);
    Defer defer{[&](){
      tag_lock_.Unlock(hash_point);
    }};
    s = schema_mgr_->GetVGroup(ctx, tb_schema, primary_key, &vgroup_id, &entity_id, &new_tag);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    vgroup = GetVGroupByID(ctx, vgroup_id);
    if (new_tag) {
      if (EnableWAL() && write_wal) {
        wal_level_mutex_.lock_shared();
        s = vgroup->GetWALManager()->WriteInsertWAL(ctx, mtr_id, 0, 0, payload_data, vgroup_id, tb_schema->GetTableId());
        wal_level_mutex_.unlock_shared();
        if (s == KStatus::FAIL) {
          LOG_ERROR("failed WriteInsertWAL for new tag.");
          return s;
        }
      }
      TsRawPayload p;
      s = p.ParsePayLoadStruct(payload_data);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("ParsePayLoadStruct failed.");
        return s;
      }
      entity_id = vgroup->AllocateEntityID();
      s = putTagData(ctx, tb_schema->GetTableId(), vgroup_id, entity_id, p);
      if (s != KStatus::SUCCESS) {
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
  TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* not_create_entity,
  DedupResult* dedup_result, bool write_wal, const char* tsx_id) {
  std::shared_ptr<kwdbts::TsTable> ts_table;
  ErrorInfo err_info;
  TSEntityID entity_id;
  // size_t payload_size = 0;
  dedup_result->payload_num = payload_num;
  dedup_result->dedup_rule = static_cast<int>(EngineOptions::g_dedup_rule);
  bool is_dropped = false;
  bool not_new_tag = (not_create_entity != nullptr && 1 == *not_create_entity);
  uint16_t created_tag_num = 0;
  for (size_t i = 0; i < payload_num; i++) {
    TSSlice& cur_pd = payload_data[i];
    auto tbl_version = TsRawPayload::GetTableVersionFromSlice(cur_pd);
    auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, tbl_version);
    if (s != KStatus::SUCCESS) {
      if (is_dropped) {
        LOG_INFO("Table[%ld] has already been dropped.", table_id);
        return KStatus::SUCCESS;
      }
      LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, tbl_version, err_info.errmsg.c_str());
      return s;
    }
    uint32_t vgroup_id;
    if (tsx_id != nullptr) {
      mtr_id = GetVGroupByID(ctx, 1)->GetMtrIDByTsxID(tsx_id);
    }
    std::shared_ptr<TsTableSchemaManager> tb_schema;
    s = GetTableSchemaMgr(ctx, table_id, is_dropped, tb_schema);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTableSchemaManager failed, table id: %lu", table_id);
      return s;
    }
    if (not_new_tag) {
      bool new_tag = false;
      TSSlice primary_key = TsRawPayload::GetPrimaryKeyFromSlice(cur_pd);
      KStatus s = schema_mgr_->GetVGroup(ctx, tb_schema, primary_key, &vgroup_id, &entity_id, &new_tag);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetVGroup failed, table id: %lu", table_id);
        return s;
      }
      if (new_tag) {
        LOG_INFO("not_create_entity, so ingore this payload. table id: %lu", table_id);
        return KStatus::SUCCESS;
      }
    }
    bool new_tag = false;
    s = InsertTagData(ctx, tb_schema, mtr_id, cur_pd, write_wal, vgroup_id, entity_id, new_tag);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("put tag data failed. table[%lu].", table_id);
      return s;
    }
    if (new_tag) {
      created_tag_num += 1;
    }
    auto vgroup = GetVGroupByID(ctx, vgroup_id);
    // payload_size += cur_pd.len;
    s =  dynamic_pointer_cast<TsTableV2Impl>(ts_table)->PutData(ctx, vgroup, &cur_pd, 1, mtr_id, entity_id,
            not_create_entity, dedup_result, (DedupRule)(dedup_result->dedup_rule), write_wal);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("put data failed. table[%lu].", table_id);
      return s;
    }
  }
  *inc_entity_cnt = created_tag_num;
  return KStatus::SUCCESS;
}

// TODO(wal): add WAL
KStatus TSEngineImpl::PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                  TSSlice* payload_data, int payload_num, uint64_t mtr_id, bool& is_dropped) {
  std::shared_ptr<kwdbts::TsTable> ts_table;
  ErrorInfo err_info;
  uint32_t vgroup_id;
  TSEntityID entity_id;

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = GetTableSchemaMgr(ctx, table_id, is_dropped, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  for (size_t i = 0; i < payload_num; i++) {
    TsRawPayload p;
    s = p.ParsePayLoadStruct(payload_data[i]);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("ParsePayLoadStruct failed.");
      return s;
    }
    TSSlice primary_key = p.GetPrimaryTag();
    auto tbl_version = p.GetTableVersion();
    s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, tbl_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, tbl_version, err_info.errmsg.c_str());
      return s;
    }
    bool new_tag;
    s = schema_mgr_->GetVGroup(ctx, tb_schema_manager, primary_key, &vgroup_id, &entity_id, &new_tag);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    auto vgroup = GetVGroupByID(ctx, vgroup_id);
    assert(vgroup != nullptr);

    uint64_t hash_point = t1ha1_le(primary_key.data, primary_key.len);
    tag_lock_.WrLock(hash_point);
    Defer defer{[&](){
      tag_lock_.Unlock(hash_point);
    }};
    if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len)) {
      return KStatus::SUCCESS;
    }
    if (EnableWAL()) {
      // get old payload
      auto tag_pack = tag_table->GenTagPack(primary_key.data, primary_key.len);
      if (UNLIKELY(nullptr == tag_pack)) {
        return KStatus::FAIL;
      }
      wal_level_mutex_.lock_shared();
      s = vgroup->GetWALManager()->WriteUpdateWAL(ctx, mtr_id, 0, 0, payload_data[i], tag_pack->getData(), vgroup_id,
                                                  table_id);
      wal_level_mutex_.unlock_shared();
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to WriteUpdateWAL while PutEntity")
        return s;
      }
    }

    err_info.errcode = tag_table->UpdateTagRecord(p, vgroup_id, entity_id, err_info, p.GetOSN());
    if (err_info.errcode < 0) {
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable *meta) {
  return schema_mgr_->GetMeta(ctx, table_id, version, meta);
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

KStatus TSEngineImpl::AddColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, bool& is_dropped,
  TSSlice column, uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, cur_version, err_info.errmsg.c_str());
    return s;
  }
  // Get transaction ID.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Write Alter DDL into WAL, which type is ADD_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ADD_COLUMN, cur_version, new_version, column);
  if (s != KStatus::SUCCESS) {
    err_msg = "Write WAL error";
    LOG_ERROR("%s", err_msg.c_str());
    return s;
  }
  s = ts_table->AlterTable(ctx, AlterType::ADD_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Add column failed, table id: %lu, cur_version: %d, new_version: %d, error message: %s.",
          table_id, cur_version, new_version, err_msg.c_str());
  }
  return s;
}

KStatus TSEngineImpl::DropColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, bool& is_dropped,
  TSSlice column, uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, cur_version, err_info.errmsg.c_str());
    return s;
  }
  // Get transaction ID.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Write Alter DDL into WAL, which type is DROP_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::DROP_COLUMN, cur_version, new_version, column);
  if (s != KStatus::SUCCESS) {
    err_msg = "Write WAL error";
    LOG_ERROR("%s", err_msg.c_str());
    return s;
  }
  s = ts_table->AlterTable(ctx, AlterType::DROP_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Drop column failed, table id: %lu, cur_version: %d, new_version: %d, error message: %s.",
          table_id, cur_version, new_version, err_msg.c_str());
  }
  return s;
}

KStatus TSEngineImpl::AlterColumnType(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id,
                                        bool& is_dropped, TSSlice new_column, TSSlice origin_column,
                                        uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn new_col_meta;
  if (!new_col_meta.ParseFromArray(new_column.data, new_column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, cur_version, err_info.errmsg.c_str());
    return s;
  }
  // Get transaction ID.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Write Alter DDL into WAL, which type is ALTER_COLUMN_TYPE.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ALTER_COLUMN_TYPE, cur_version, new_version, origin_column);
  if (s != KStatus::SUCCESS) {
    err_msg = "Write WAL error";
    LOG_ERROR("%s", err_msg.c_str());
    return s;
  }
  s = ts_table->AlterTable(ctx, AlterType::ALTER_COLUMN_TYPE, &new_col_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Alter column type failed, table id: %lu, cur_version: %d, new_version: %d, error message: %s.",
    table_id, cur_version, new_version, err_msg.c_str());
  }
  return s;
}

KStatus TSEngineImpl::AlterLifetime(kwdbContext_p ctx, const KTableKey& table_id, uint64_t lifetime, bool& is_dropped) {
  LOG_INFO("Alter life time on table %lu start.", table_id);
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  KStatus s = GetTableSchemaMgr(ctx, table_id, is_dropped, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TSEngineImpl get table schema manager failed, table id: %lu", table_id);
    return s;
  }
  auto old_life_time = tb_schema_mgr->GetLifeTime();
  // convert second to millisecond, * 1000
  tb_schema_mgr->SetLifeTime(LifeTime{static_cast<int64_t>(lifetime), 1000});
  LOG_INFO("Alter table life time success, change from %lu[precision:%d] to %lu[precision:%d]",
    old_life_time.ts, old_life_time.precision, lifetime, 1000);
  return KStatus::SUCCESS;
}
std::vector<std::shared_ptr<TsVGroup>>* TSEngineImpl::GetTsVGroups() {
  return &vgroups_;
}

std::shared_ptr<TsVGroup> TSEngineImpl::GetTsVGroup(uint32_t vgroup_id) {
  if (CheckGroupID(vgroup_id, false) == KStatus::FAIL) {
    LOG_ERROR("Failed to obtain the vgroup id!");
  }
  return vgroups_[vgroup_id - 1];
}

KStatus TSEngineImpl::TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                   uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id) {
  if (!EnableWAL()) {
    return KStatus::SUCCESS;
  }
  if (tsx_id != nullptr) {
    exist_explict_txn.store(true);
  }
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  auto vgroup = GetVGroupByID(ctx, 1);
  return vgroup->MtrBegin(ctx, range_id, index, mtr_id, tsx_id);
}

KStatus TSEngineImpl::TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                                    uint64_t range_group_id, uint64_t mtr_id, const char* tsx_id) {
  if (!EnableWAL()) {
    return KStatus::SUCCESS;
  }
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  auto vgroup = GetVGroupByID(ctx, 1);
  return vgroup->MtrCommit(ctx, mtr_id, tsx_id);
}

KStatus TSEngineImpl::TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                                      uint64_t range_group_id, uint64_t mtr_id, bool skip_log, const char* tsx_id) {
  EnterFunc()
//  1. Write ROLLBACK log;
//  2. Backtrace WAL logs based on xID to the BEGIN log of the Mini-Transaction.
//  3. Invoke the reverse operation based on the type of each log:
//    1) For INSERT operations, add the DELETE MARK to the corresponding data;
//    2) For the DELETE operation, remove the DELETE MARK of the corresponding data;
//    3) For ALTER operations, roll back to the previous schema version;
//  4. If the rollback fails, a system log is generated and an error exit is reported.
  if (!EnableWAL()) {
    Return(KStatus::SUCCESS);
  }
  KStatus s;

  if (!skip_log) {
    auto vgroup = GetVGroupByID(ctx, 1);
    s = vgroup->MtrRollback(ctx, mtr_id, false, tsx_id);
    if (s == FAIL) {
      Return(s);
    }
  }
  if (tsx_id != nullptr && mtr_id == 0) {
    return SUCCESS;
  }

  std::vector<LogEntry*> engine_wal_logs;
  std::vector<uint64_t> ignore;
  std::vector<LogEntry*> rollback_logs;
  Defer defer_engine{[&]() {
    for (auto& log : engine_wal_logs) {
      delete log;
    }
  }};
  s = wal_mgr_->ReadWALLog(engine_wal_logs, wal_mgr_->GetFirstLSN(), wal_mgr_->FetchCurrentLSN(), ignore);
  if (s == FAIL && !engine_wal_logs.empty()) {
    Return(s)
  }

  for (auto log : engine_wal_logs) {
    if (log->getXID() == mtr_id) {
      rollback_logs.emplace_back(log);
    }
  }

  std::reverse(rollback_logs.begin(), rollback_logs.end());
  for (auto log : rollback_logs) {
    auto vgrp_id = log->getVGroupID();
    // todo(xy), 0 means the wal log does not need to rollback.
    if (vgrp_id == 0) {
      continue;
    }
    auto vgrp = GetVGroupByID(ctx, vgrp_id);
    if (vgrp == nullptr) {
      LOG_ERROR("GetVGroupByID fail, vgroup id : %lu", vgrp_id)
      return KStatus::FAIL;
    }
    if (vgrp->rollback(ctx, log, true) == KStatus::FAIL) {
      LOG_ERROR("rollback fail, vgroup id : %lu", vgrp_id)
      return KStatus::FAIL;
    }
  }

  // for range
  for (auto vgrp : vgroups_) {
    std::vector<LogEntry*> wal_logs;
    Defer defer{[&]() {
      for (auto& log : wal_logs) {
        delete log;
      }
    }};
    s = vgrp->ReadWALLogForMtr(mtr_id, wal_logs);
    if (s == FAIL && !wal_logs.empty()) {
      Return(s)
    }
    std::reverse(wal_logs.begin(), wal_logs.end());
    for (auto wal_log : wal_logs) {
      if (wal_log->getXID() == mtr_id && s != FAIL) {
        s = vgrp->rollback(ctx, wal_log);
      }
    }
  }
  Return(KStatus::SUCCESS)
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

KStatus TSEngineImpl::TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  s = tsx_manager_sys_->TSxBegin(ctx, transaction_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed")
    return s;
  }
  s = GetTsTable(ctx, table_id, table, is_dropped);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

//  s = CreateCheckpoint(ctx);
//  if (s == KStatus::FAIL) {
//    LOG_ERROR("Failed to CreateCheckpoint.")
//  #ifdef WITH_TESTS
//    return s;
//  #endif
//  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                  bool& is_dropped) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id != 0) {
    if (tsx_manager_sys_->TSxCommit(ctx, transaction_id) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal failed, table id: %lu", table_id)
      return KStatus::FAIL;
    }
  }

  s = GetTsTable(ctx, table_id, table, is_dropped);
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

KStatus TSEngineImpl::TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                    bool& is_dropped) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id == 0) {
    if (checkpoint(ctx) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal checkpoint failed, table id: %lu", table_id)
      return KStatus::FAIL;
    }

    return KStatus::SUCCESS;
  }

  s = tsx_manager_sys_->TSxRollback(ctx, transaction_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, TSxRollback failed, table id: %lu", table_id)
    return s;
  }

  s = GetTsTable(ctx, table_id, table, is_dropped);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

  std::vector<LogEntry*> logs;
  std::vector<uint64_t> ignore;
  s = wal_sys_->ReadWALLogForMtr(mtr_id, logs, ignore);
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

KStatus TSEngineImpl::RemoveChkFile(kwdbContext_p ctx, uint32_t vgroup_id) {
  auto vgroup = GetVGroupByID(ctx, vgroup_id);
  return vgroup->RemoveChkFile(ctx);
}

KStatus TSEngineImpl::ParallelRemoveChkFiles(kwdbContext_p ctx) {
  std::vector<std::future<KStatus >> res;

  for (auto vgroup : vgroups_) {
    res.emplace_back(std::async(std::launch::async,
                                    [this, ctx, vgroup_id = vgroup->GetVGroupID()]
                                    { return RemoveChkFile(ctx, vgroup_id); }));
  }

  for (int i = 0; i < res.size(); i++) {
    if (res[i].get() != KStatus::SUCCESS) {
      LOG_ERROR("Failed to ParallelRemoveChkFiles")
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CreateCheckpoint(kwdbContext_p ctx) {
  LOG_INFO("WAL Checkpoint Start.")
  Defer defer_log {[&]() {
    LOG_INFO("WAL Checkpoint Complete.")
  }};
  Defer defer_explict {[&]() {
    exist_explict_txn.store(false);
  }};
  // use raft log
  if (options_.use_raft_log_as_wal) {
    goPrepareFlush();
    // trig all vgroup flush
    for (const auto &vgrp : vgroups_) {
      auto s = vgrp->Flush();
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to flush metric file.")
        return s;
      }
    }
    goFlushed();
    return KStatus::SUCCESS;
  }
  /*
   * use wal
   * 1. read chk log from chk file.
   * 2. read wal log from all vgroup
   * 3. merge chk log and wal log
   * 4. rewrite wal log to new chk file
   * 5. trig all vgroup flush
   * 6. write EndWAL to chk file
   * 7. trig all vgroup write checkpoint wal and update checkpoint LSN
   * 8. remove vgroup wal file and old chk file
   */
  if (!EnableWAL()) {
    return KStatus::SUCCESS;
  } else if (EngineOptions::isSingleNode() || !exist_explict_txn.load()) {
    std::vector<uint64_t> vgrp_lsn;
    // 1. switch engine wal file
    KStatus s = wal_mgr_->SwitchNextFile();
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to switch chk file.")
      return s;
    }

    // 2. switch vgroup wal file
    for (const auto &vgrp : vgroups_) {
      vgrp->GetWALManager()->Lock();
      auto cur_lsn = vgrp->GetWALManager()->FetchCurrentLSN();
      vgrp_lsn.emplace_back(cur_lsn);
      s = vgrp->GetWALManager()->SwitchNextFile(cur_lsn);
      if (s == KStatus::FAIL) {
        vgrp->GetWALManager()->Unlock();
        LOG_ERROR("Failed to switch vgroup chk file.")
        return s;
      }
      vgrp->GetWALManager()->Unlock();
    }

    // 3. trig all vgroup flush
    for (const auto &vgrp : vgroups_) {
      s = vgrp->Flush();
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to flush metric file.")
        return s;
      }
    }

    // 4. write end chkckpoint wal
    TS_OSN end_lsn;
    uint64_t lsn_len = vgrp_lsn.size() * sizeof(uint64_t);
    auto v_lsn = std::make_unique<char[]>(lsn_len);
    int location = 0;
    for (auto it : vgrp_lsn) {
      memcpy(v_lsn.get() + location, &it, sizeof(uint64_t));
      location += sizeof(uint64_t);
    }
    auto end_chk_log = EndCheckpointEntry::construct(WALLogType::END_CHECKPOINT, 0, lsn_len, v_lsn.get());
    s = wal_mgr_->WriteWAL(ctx, end_chk_log, EndCheckpointEntry::fixed_length + lsn_len, end_lsn);
    delete []end_chk_log;
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to write end checkpoint wal.")
      return s;
    }

    for (const auto &vgrp : vgroups_) {
      s = vgrp->GetWALManager()->RemoveChkFile(ctx);
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to Remove vgroup ChkFile.")
        return s;
      }
    }
    // 5. remove old chk file
    s = wal_mgr_->RemoveChkFile(ctx);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to remove chk file.")
      return s;
    }
    return KStatus::SUCCESS;
  }
  std::vector<LogEntry*> logs;
  std::unordered_map<uint32_t, uint64_t> vgrp_lsn;
  KStatus s;
  Defer defer{[&]() {
    for (auto& log : logs) {
      delete log;
    }
  }};
  // 1. read chk log from chk file.
  std::vector<uint64_t> vgroup_lsn;
  TS_OSN last_lsn = wal_mgr_->GetFirstLSN();
  s = wal_mgr_->ReadWALLog(logs, last_lsn, wal_mgr_->FetchCurrentLSN(), vgroup_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to read wal log from chk file, with Last LSN [%lu] and Current LSN [%lu]",
              last_lsn, wal_mgr_->FetchCurrentLSN())
    return s;
  }
  if (vgroup_lsn.empty()) {
    LOG_INFO("Cannot detect the end checkpoint wal, skipping this file's content.")
    for (auto& log : logs) {
      delete log;
    }
    logs.clear();
  }

  s = wal_mgr_->SwitchNextFile();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to switch chk file.")
    return s;
  }

  // 2.read mtr id first, only read uncommitted txn from all vgroups.
  auto vgroup_mtr = GetVGroupByID(ctx, 1);
  std::vector<uint64_t> uncommitted_xid;
  s = vgroup_mtr->GetWALManager()->ReadUncommittedTxnID(uncommitted_xid);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadUncommittedTxnID.")
    return s;
  }

  // 3. read wal log from all vgroup
  for (const auto &vgrp : vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_OSN lsn = 0;
    if (vgrp->GetVGroupID() <= vgroup_lsn.size()) {
      lsn = vgroup_lsn[vgrp->GetVGroupID() - 1];
    }
    if (vgrp->ReadWALLogFromLastCheckpoint(ctx, vlogs, lsn, uncommitted_xid) == KStatus::FAIL) {
      LOG_ERROR("Failed to ReadWALLogFromLastCheckpoint from vgroup : %d", vgrp->GetVGroupID())
      return KStatus::FAIL;
    }
    uint32_t vgrp_id = vgrp->GetVGroupID();
    vgrp_lsn.emplace(vgrp_id, lsn);

    logs.insert(logs.end(), vlogs.begin(), vlogs.end());
  }

  // 4. rewrite incomplete wal
  if (wal_mgr_->WriteIncompleteWAL(ctx, logs) == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteIncompleteWAL.")
    return KStatus::FAIL;
  }

  // 5. trig all vgroup flush
  for (const auto &vgrp : vgroups_) {
    s = vgrp->Flush();
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to flush metric file.")
      return s;
    }
  }

  // 6.write EndWAL to chk file
  TS_OSN end_lsn;
  uint64_t lsn_len = vgrp_lsn.size() * sizeof(uint64_t);
  auto v_lsn = std::make_unique<char[]>(lsn_len);
  int location = 0;
  for (auto it : vgrp_lsn) {
    memcpy(v_lsn.get() + location, &(it.second), sizeof(uint64_t));
    location += sizeof(uint64_t);
  }
  auto end_chk_log = EndCheckpointEntry::construct(WALLogType::END_CHECKPOINT, 0, lsn_len, v_lsn.get());
  s = wal_mgr_->WriteWAL(ctx, end_chk_log, EndCheckpointEntry::fixed_length + lsn_len, end_lsn);
  delete []end_chk_log;
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to write end checkpoint wal.")
    return s;
  }

  // 7. remove vgroup wal file.
  s = ParallelRemoveChkFiles(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ParallelRemoveChkFiles.")
    return s;
  }

  // 8. remove old chk file
  s = wal_mgr_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to remove chk file.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version, bool& is_dropped) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  *version = ts_table->GetCurrentTableVersion();
  return KStatus::SUCCESS;
}

void TSEngineImpl::GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) {
  schema_mgr_->GetTableList(&table_id_list);
}

KStatus TSEngineImpl::GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,
  RangeGroup range, roachpb::CreateTsTable* meta, bool& is_dropped) {
  std::shared_ptr<TsTable> table;
  ErrorInfo err_info;
  KStatus s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, 0);
  if (s == FAIL) {
    s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
    return s;
  }
  auto table_v2 = dynamic_pointer_cast<TsTableV2Impl>(table);
  uint32_t cur_table_version = table->GetCurrentTableVersion();
  LOG_INFO("TSEngineImpl::GetMetaData Begin! table_id: %lu table_version: %u ",
     table_id, cur_table_version);
  // Construct roachpb::CreateTsTable.
  // Set table configures.
  auto ts_table = meta->mutable_ts_table();
  ts_table->set_ts_table_id(table_id);
  ts_table->set_ts_version(cur_table_version);
  ts_table->set_partition_interval(table_v2->GetSchemaManager()->GetPartitionInterval());
  ts_table->set_hash_num(table->GetHashNum());

  // Get table data schema.
  const std::vector<AttributeInfo>* data_schema{nullptr};
  s = table_v2->GetSchemaManager()->GetColumnsIncludeDroppedPtr(&data_schema, cur_table_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetDataSchemaIncludeDropped failed during GetMetaData, table id is %ld.", table_id)
    return s;
  }
  // Get table tag schema.
  std::shared_ptr<TagTable> tag_schema;
  s = table_v2->GetSchemaManager()->GetTagSchema(ctx, &tag_schema);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTagSchema failed during GetTagSchema, table id is %ld.", table_id)
    return s;
  }
  auto tag_version = tag_schema->GetTagTableVersionManager()->GetVersionObject(cur_table_version);
  if (tag_version == nullptr) {
    LOG_ERROR("GetTagSchema failed during GetVersionObject, table id is %ld.", table_id)
    return s;
  }
  std::vector<TagInfo> tag_schema_info = tag_version->getIncludeDroppedSchemaInfos();
  // Use data schema and tag schema to construct meta.
  s = table->GenerateMetaSchema(ctx, meta, *data_schema, tag_schema_info, cur_table_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("generateMetaSchema failed during GetMetaData, table id is %ld.", table_id)
    return s;
  }
  return s;
}

KStatus TSEngineImpl::DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                        HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                        uint64_t mtr_id, uint64_t osn, bool& is_dropped) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteRangeData(ctx, range_group_id, hash_span, ts_spans, count, mtr_id, osn);
}

KStatus TSEngineImpl::DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                    uint64_t mtr_id, uint64_t osn, bool& is_dropped) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteData(ctx, range_group_id, primary_tag, ts_spans, count, mtr_id, osn);
}

KStatus TSEngineImpl::DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                        std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id,
                        bool& is_dropped, uint64_t osn) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return (dynamic_pointer_cast<TsTableV2Impl>(ts_table))->DeleteEntities(ctx, primary_tags, count, mtr_id, osn, true);
}

KStatus TSEngineImpl::DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_grp_id,
                            const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id, bool& is_dropped,
                            uint64_t osn) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteRangeEntities(ctx, range_grp_id, hash_span, count, mtr_id, osn, true);
}

KStatus TSEngineImpl::CountRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                        HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                        uint64_t mtr_id, uint64_t osn) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  bool is_dropped = false;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->CountRangeData(ctx, range_group_id, hash_span, ts_spans, count, mtr_id, osn);
}

KStatus TSEngineImpl::ReadBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t begin_hash,
                      uint64_t end_hash, KwTsSpan ts_span, uint64_t job_id, TSSlice* data,
                      uint32_t* row_num, bool& is_dropped) {
  std::string key = TsReadBatchDataWorker::GenKey(table_id, table_version, begin_hash, end_hash, ts_span);
  RW_LATCH_S_LOCK(&read_batch_workers_lock_);
  auto workers_it = read_batch_data_workers_.find(job_id);
  if (workers_it != read_batch_data_workers_.end()) {
    auto worker_it = workers_it->second.find(key);
    if (worker_it != workers_it->second.end()) {
      std::shared_ptr<TsBatchDataWorker> worker = worker_it->second;
      RW_LATCH_UNLOCK(&read_batch_workers_lock_);
      return worker->Read(ctx, data, row_num);
    }
  }
  RW_LATCH_UNLOCK(&read_batch_workers_lock_);

  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, is_dropped, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  vector<EntityResultIndex> entity_indexes;
  s = ts_table->GetEntityIdByHashSpan(ctx, {begin_hash, end_hash}, entity_indexes);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entity_ids by hash span[%lu, %lu]", begin_hash, end_hash);
    return s;
  }
  if (entity_indexes.empty()) {
    *row_num = 0;
    return KStatus::SUCCESS;
  }
  RW_LATCH_X_LOCK(&read_batch_workers_lock_);
  workers_it = read_batch_data_workers_.find(job_id);
  if (workers_it != read_batch_data_workers_.end()) {
    auto worker_it = workers_it->second.find(key);
    if (worker_it != workers_it->second.end()) {
      std::shared_ptr<TsBatchDataWorker> worker = worker_it->second;
      RW_LATCH_UNLOCK(&read_batch_workers_lock_);
      return worker->Read(ctx, data, row_num);
    }
  }
  std::shared_ptr<TsBatchDataWorker> worker = std::make_shared<TsReadBatchDataWorker>(this, table_id, table_version,
                                                                                      ts_span, job_id, entity_indexes);
  s = worker->Init(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to init read batch data worker.");
    RW_LATCH_UNLOCK(&read_batch_workers_lock_);
    return s;
  }
  workers_it = read_batch_data_workers_.find(job_id);
  if (workers_it != read_batch_data_workers_.end()) {
    workers_it->second.insert({key, worker});
  } else {
    read_batch_data_workers_[job_id].insert({key, worker});
  }
  RW_LATCH_UNLOCK(&read_batch_workers_lock_);
  return worker->Read(ctx, data, row_num);
}

KStatus TSEngineImpl::WriteBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t job_id,
                         TSSlice* data, uint32_t* row_num, bool& is_dropped) {
  std::shared_ptr<TsBatchDataWorker> worker = nullptr;
  RW_LATCH_S_LOCK(&write_batch_workers_lock_);
  auto it = write_batch_data_workers_.find(job_id);
  if (it != write_batch_data_workers_.end()) {
    worker = it->second;
  }
  RW_LATCH_UNLOCK(&write_batch_workers_lock_);
  if (worker != nullptr) {
    return worker->Write(ctx, table_id, table_version, data, row_num);
  }
  RW_LATCH_X_LOCK(&write_batch_workers_lock_);
  it = write_batch_data_workers_.find(job_id);
  if (it != write_batch_data_workers_.end()) {
    worker = it->second;
  } else {
    worker = std::make_shared<TsWriteBatchDataWorker>(this, job_id);
    KStatus s = worker->Init(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsWriteBatchDataWorker init failed");
      RW_LATCH_UNLOCK(&write_batch_workers_lock_);
      return s;
    }
    write_batch_data_workers_[job_id] = worker;
  }
  RW_LATCH_UNLOCK(&write_batch_workers_lock_);
  return worker->Write(ctx, table_id, table_version, data, row_num);
}

KStatus TSEngineImpl::CancelBatchJob(kwdbContext_p ctx, uint64_t job_id, uint64_t osn) {
  // handle write worker
  {
    RW_LATCH_X_LOCK(&write_batch_workers_lock_);
    auto write_it = write_batch_data_workers_.find(job_id);
    if (write_it != write_batch_data_workers_.end()) {
      write_it->second->Cancel(ctx);
      LOG_INFO("Cancel write batch data, job_id[%lu]", job_id);
    }
    write_batch_data_workers_.erase(job_id);
    RW_LATCH_UNLOCK(&write_batch_workers_lock_);
  }
  // handle read worker
  {
    RW_LATCH_X_LOCK(&read_batch_workers_lock_);
    auto read_it = read_batch_data_workers_.find(job_id);
    if (read_it != read_batch_data_workers_.end()) {
      for (auto& kv : read_it->second) {
        kv.second->Cancel(ctx);
      }
      LOG_INFO("Cancel read batch data, job_id[%lu]", job_id);
    }
    read_batch_data_workers_.erase(job_id);
    RW_LATCH_UNLOCK(&read_batch_workers_lock_);
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::AllWriteBatchJobFinish(kwdbContext_p ctx) {
  RW_LATCH_X_LOCK(&write_batch_workers_lock_);
  std::list<std::weak_ptr<TsBatchDataWorker>> weak_workers;
  {
    for (auto& worker : write_batch_data_workers_) {
      weak_workers.push_back(worker.second);
      worker.second->Finish(ctx);
      LOG_INFO("Finish write batch data succeeded, job_id[%lu]", worker.first);
    }
    write_batch_data_workers_.clear();
  }
  // wait all weak_workers to be expired
  {
    while (!weak_workers.empty()) {
      auto it = weak_workers.begin();
      while (it != weak_workers.end()) {
        if (it->expired()) {
          it = weak_workers.erase(it);
        } else {
          ++it;
        }
      }
      if (!weak_workers.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  }
  RW_LATCH_UNLOCK(&write_batch_workers_lock_);
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::BatchJobFinish(kwdbContext_p ctx, uint64_t job_id) {
  // handle write worker
  {
    RW_LATCH_X_LOCK(&write_batch_workers_lock_);
    auto write_it = write_batch_data_workers_.find(job_id);
    if (write_it != write_batch_data_workers_.end()) {
      while (write_it->second.use_count() > 1) {
        LOG_INFO("Waiting for write batch data to finish, job_id[%lu]", job_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      write_it->second->Finish(ctx);
      LOG_INFO("Finish write batch data succeeded, job_id[%lu]", job_id);
    }
    write_batch_data_workers_.erase(job_id);
    RW_LATCH_UNLOCK(&write_batch_workers_lock_);
  }
  // handle read worker
  {
    RW_LATCH_X_LOCK(&read_batch_workers_lock_);
    auto job_it = read_batch_data_workers_.find(job_id);
    if (job_it != read_batch_data_workers_.end()) {
      for (auto& kv : job_it->second) {
        kv.second->Finish(ctx);
      }
      LOG_INFO("Finish read batch data, job_id[%lu]", job_id);
    }
    read_batch_data_workers_.erase(job_id);
    RW_LATCH_UNLOCK(&read_batch_workers_lock_);
  }
  return KStatus::SUCCESS;
}

// check if table is dropped from rocksdb.
KStatus TSEngineImpl::DropResidualTsTable(kwdbContext_p ctx) {
  std::vector<TSTableID> tables;
  auto s = schema_mgr_->GetTableList(&tables);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TSEngineImpl::DropResidualTsTable failed.");
    return s;
  }
  for (auto table_id : tables) {
    bool is_exist = checkTableMetaExist(table_id);
    if (!is_exist) {
      s = DropTsTable(ctx, table_id);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("drop table [%ld] failed", table_id);
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) {
  ErrorInfo err_info;
  bool is_dropped = false;
  auto s = CheckAndDropTsTable(ctx, table_id, is_dropped, err_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("DropTsTable table[%lu] failed.", table_id);
  }
  return s;
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

  TS_OSN checkpoint_lsn = wal_sys_->FetchCheckpointLSN();
  TS_OSN current_lsn = wal_sys_->FetchCurrentLSN();

  std::vector<LogEntry*> redo_logs;
  Defer defer{[&]() {
    for (auto& log : redo_logs) {
      delete log;
    }
  }};

  std::vector<uint64_t> ignore;
  s = wal_sys_->ReadWALLog(redo_logs, checkpoint_lsn, current_lsn, ignore);
  if (s == KStatus::FAIL && !redo_logs.empty()) {
    LOG_ERROR("Failed to read the TS Engine WAL logs.")
#ifdef WITH_TESTS
    return s;
#endif
  }

  std::unordered_map<TS_OSN, LogEntry*> incomplete;
  for (auto wal_log : redo_logs) {
    // From checkpoint loop to the latest commit, including only ddl
    auto mtr_id = wal_log->getXID();

    switch (wal_log->getType()) {
      case WALLogType::TS_BEGIN: {
        incomplete.insert(std::pair<TS_OSN, LogEntry*>(mtr_id, wal_log));
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
            std::shared_ptr<TsTable> table;
            ErrorInfo err_info;
            bool is_dropped = false;
            s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info);
            if (s == KStatus::FAIL) {
              if (is_dropped) {
                LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
                continue;
              }
              return s;
            }
            if (table->IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table->UndoAlterTable(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover alter table %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            } else {
              table->TSxClean(ctx);
            }
            break;
          }
          case WALLogType::CREATE_INDEX: {
            CreateIndexEntry* index_log = reinterpret_cast<CreateIndexEntry*>(incomplete[mtr_id]);
            uint64_t table_id = index_log->getObjectID();
            std::shared_ptr<TsTable> table;
            ErrorInfo err_info;
            bool is_dropped = false;
            s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, index_log->getCurTsVersion());
            if (s == KStatus::FAIL) {
              if (is_dropped) {
                LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
                continue;
              }
              return s;
            }
            if (table->IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table->UndoCreateIndex(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover create index %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            } else {
              table->TSxClean(ctx);
            }
            break;
          }
          case WALLogType::DROP_INDEX: {
            DropIndexEntry* index_log = reinterpret_cast<DropIndexEntry*>(incomplete[mtr_id]);
            uint64_t table_id = index_log->getObjectID();
            std::shared_ptr<TsTable> table;
            ErrorInfo err_info;
            bool is_dropped = false;
            s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, index_log->getCurTsVersion());
            if (s == KStatus::FAIL) {
              if (is_dropped) {
                LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
                continue;
              }
              return s;
            }
            if (table->IsDropped()) {
              LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
              continue;
            }

            s = table->UndoDropIndex(ctx, incomplete[mtr_id]);
            if (s == KStatus::FAIL) {
              LOG_ERROR("Failed to recover drop index %ld.", table_id)
              #ifdef WITH_TESTS
              return s;
              #endif
            } else {
              table->TSxClean(ctx);
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
        std::shared_ptr<TsTable> table;
        ErrorInfo err_info;
        bool is_dropped = false;
        s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, index_log->getCurTsVersion());
        if (s == KStatus::FAIL) {
          if (is_dropped) {
            LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
            continue;
          }
          return s;
        }
        if (table->IsDropped()) {
          LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
          continue;
        }
        s = table->UndoCreateIndex(ctx, index_log);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to recover create index %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        } else {
          table->TSxClean(ctx);
        }
        break;
      }
      case WALLogType::DROP_INDEX: {
        DropIndexEntry* index_log = reinterpret_cast<DropIndexEntry*>(wal_log.second);
        uint64_t table_id = index_log->getObjectID();
        std::shared_ptr<TsTable> table;
        ErrorInfo err_info;
        bool is_dropped = false;
        s = GetTsTable(ctx, table_id, table, is_dropped, true, err_info, index_log->getCurTsVersion());
        if (s == KStatus::FAIL) {
          if (is_dropped) {
            LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
            continue;
          }
          return s;
        }
        if (table->IsDropped()) {
          LOG_INFO("table[%lu] is dropped and does not require recover", table_id);
          continue;
        }

        s = table->UndoDropIndex(ctx, index_log);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to recover drop index %ld.", table_id)
          #ifdef WITH_TESTS
          return s;
          #endif
        } else {
          table->TSxClean(ctx);
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

KStatus TSEngineImpl::Recover(kwdbContext_p ctx) {
  /*
   * 1. get engine chk wal log.
   * 2. get all vgroup wal log from last checkpoint lsn.
   * 3. merge wal and apply wal
   */
  if (!EnableWAL()) {
    return KStatus::SUCCESS;
  }
  LOG_INFO("Recover start.");

  // ddl recover
  KStatus s = recover(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to recover DDL WAL.")
    return KStatus::FAIL;
  }
  // 1. get engine chk wal log.
  std::vector<LogEntry*> logs;
  Defer defer{[&]() {
    for (auto& log : logs) {
      delete log;
    }
  }};
  std::vector<uint64_t> vgroup_lsn;
  TS_OSN last_lsn = wal_mgr_->GetFirstLSN();
  s = wal_mgr_->ReadWALLog(logs, last_lsn, wal_mgr_->FetchCurrentLSN(), vgroup_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLog from chk file while recovering, with Last LSN [%lu] and Current LSN [%lu]. Now"
              " reset WAL.",
              last_lsn, wal_mgr_->FetchCurrentLSN())
    s = wal_mgr_->ResetWAL(ctx);
    if (s == KStatus::FAIL) {
      return KStatus::FAIL;
    }
  }
  if (vgroup_lsn.empty()) {
    LOG_INFO("Cannot detect the end checkpoint wal, skipping this file's content.")
    logs.clear();
  }

  // 2.read mtr id first, only read uncommitted txn .
  auto vgroup_mtr = GetVGroupByID(ctx, 1);
  std::unordered_map<uint64_t, txnOp> txn_op;
  std::unordered_map<TS_OSN, std::pair<uint64_t, uint64_t>> incomplete;
  s = vgroup_mtr->GetWALManager()->ReadAllTxnID(txn_op, vgroup_mtr, incomplete);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadAllTxnID.")
    return s;
  }

  // 3. get all vgroup wal log && apply all wal log
  for (const auto &vgrp : vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_OSN lsn = 0;
    if (vgrp->GetVGroupID() < vgroup_lsn.size()) {
      lsn = vgroup_lsn[vgrp->GetVGroupID() - 1];
    }

    if (vgrp->ReadLogAndApplyFromLastCheckpoint(ctx, vlogs, lsn, txn_op) == KStatus::FAIL) {
      LOG_ERROR("Failed to ReadWALLogFromLastCheckpoint from vgroup : %d, Now reset WAL.", vgrp->GetVGroupID())
      vlogs.clear();
      s = vgrp->GetWALManager()->ResetWAL(ctx);
      if (s == KStatus::FAIL) {
        return KStatus::FAIL;
      }
    }
  }

  // 4. do rollback
  for (auto txn : txn_op) {
    if (txn.second == txnOp::rollback) {
      if (TSMtrRollback(ctx, 0, 0, txn.first, true) == KStatus::FAIL) return KStatus::FAIL;
    }
    if (txn.second == txnOp::begin) {
      TS_OSN mtr_id = txn.first;
      auto xid_rangeID = incomplete[mtr_id];
      if (vgroup_mtr->IsExplict(mtr_id)) {
        break;
      }
      uint64_t applied_index = GetAppliedIndex(xid_rangeID.second, range_indexes_map_);
      if (xid_rangeID.first <= applied_index) {
        auto vgroup = GetVGroupByID(ctx, 1);
        s = vgroup->MtrCommit(ctx, mtr_id);
        if (s == FAIL) return s;
      } else {
        if (TSMtrRollback(ctx, 0, 0, mtr_id) == KStatus::FAIL) return KStatus::FAIL;
      }
    }
  }

  // 5. trig all vgroup flush
  for (const auto &vgrp : vgroups_) {
    s = vgrp->Flush();
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to flush metric file.")
      return s;
    }
  }
  LOG_INFO("Recover success.");
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

KStatus TSEngineImpl::UpdateAtomicLSN() {
  for (auto vgrp : vgroups_) {
    vgrp->UpdateAtomicOSN();
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::UpdateSetting(kwdbContext_p ctx) {
  // After changing the WAL configuration parameters, the already opened table will not change,
  // and the newly opened table will follow the new configuration.
  string value;

  if (GetClusterSetting(ctx, "ts.wal.wal_level", &value) == SUCCESS) {
    if (std::stoll(value) != options_.wal_level) {
      wal_level_mutex_.lock();
      // wlock all vgroup rwlock
      KStatus s = CreateCheckpoint(ctx);
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to CreateCheckpoint while UpdateSetting.")
      }
      options_.wal_level = std::stoll(value);
      LOG_INFO("update wal level to %hhu", options_.wal_level)
      if (!EnableWAL()) {
        UpdateAtomicLSN();
      }
      wal_level_mutex_.unlock();
      // unlock
    }
  }

  return KStatus::SUCCESS;
}

uint64_t TSEngineImpl::insertToSnapshotCache(TsRangeImgrationInfo& snapshot) {
  auto now = std::chrono::system_clock::now();
  uint64_t snapshot_id = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  {
    snapshot_mutex_.lock();
    // snapshot_id must unique. if generated snaphsot_id already exists, we need change one.
    while (snapshots_.find(snapshot_id) != snapshots_.end()) {
      snapshot_id += 1;
    }
    snapshot.id = snapshot_id;
    snapshots_[snapshot_id] = snapshot;
    snapshot_mutex_.unlock();
  }
  return snapshot_id;
}

KStatus TSEngineImpl::CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id,
uint64_t begin_hash, uint64_t end_hash, const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table, is_dropped, false);
  if (s == FAIL) {
    LOG_ERROR("cannot find table [%lu]", table_id);
    return s;
  }
  // force finish all write jobs
  s = AllWriteBatchJobFinish(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("CreateSnapshotForRead AllWriteBatchJobFinish failed.");
    return s;
  }

  TsRangeImgrationInfo ts_snapshot_info;
  ts_snapshot_info.begin_hash = begin_hash;
  ts_snapshot_info.end_hash = end_hash;
  ts_snapshot_info.ts_span = ts_span;
  ts_snapshot_info.type = 0;
  ts_snapshot_info.table_id = table_id;
  ts_snapshot_info.table = table;
  ts_snapshot_info.package_id = 0;
  ts_snapshot_info.imgrated_rows = 0;
  ts_snapshot_info.batch_read_finished = false;
  // todo(liangbo01) maybe we need use available version.
  ts_snapshot_info.table_version = table->GetCurrentTableVersion();
  ts_snapshot_info.del_iter = std::make_shared<STTableRangeDelAndTagInfo>(
      reinterpret_pointer_cast<TsTableV2Impl>(ts_snapshot_info.table),
      ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash, ts_snapshot_info.table_version);
  s = ts_snapshot_info.del_iter->Init();
  if (s == KStatus::FAIL) {
    LOG_ERROR("CreateSnapshotForRead STTableRangeDelAndTagInfo [%lu] failed.", table_id);
    return s;
  }
  *snapshot_id = insertToSnapshotCache(ts_snapshot_info);
  ts_snapshot_info.id = *snapshot_id;
  uint64_t count;
  s = table->GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &count);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetRangeRowCount [%lu] failed.", table_id);
    return s;
  }
  LOG_INFO("CreateSnapshotForRead [%lu] version[%u] range hash[%lu ~ %lu], ts[%ld ~ %ld] need imgrating rows[%lu].",
      table_id, ts_snapshot_info.table_version, begin_hash, end_hash, ts_span.begin, ts_span.end, count);
  return KStatus::SUCCESS;
}
KStatus TSEngineImpl::CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id, uint64_t begin_hash,
  uint64_t end_hash, const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped, uint64_t osn) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table, is_dropped, true);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTsTable [%lu] failed.", table_id);
    return s;
  }
  TsRangeImgrationInfo ts_snapshot_info;
  ts_snapshot_info.begin_hash = begin_hash;
  ts_snapshot_info.end_hash = end_hash;
  ts_snapshot_info.ts_span = ts_span;
  ts_snapshot_info.type = 1;
  ts_snapshot_info.table_id = table_id;
  ts_snapshot_info.table = table;
  ts_snapshot_info.table_version = 0;
  ts_snapshot_info.package_id = 0;
  ts_snapshot_info.imgrated_rows = 0;
  ts_snapshot_info.batch_read_finished = false;
  ts_snapshot_info.del_iter = std::make_shared<STTableRangeDelAndTagInfo>(
      reinterpret_pointer_cast<TsTableV2Impl>(ts_snapshot_info.table),
      ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash, ts_snapshot_info.table_version);
  s = ts_snapshot_info.del_iter->Init();
  if (s == KStatus::FAIL) {
    LOG_ERROR("CreateSnapshotForRead STTableRangeDelAndTagInfo [%lu] failed.", table_id);
    return s;
  }
  *snapshot_id = insertToSnapshotCache(ts_snapshot_info);
  ts_snapshot_info.id = *snapshot_id;
  uint64_t count;
  s = table->GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &count);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetRangeRowCount [%lu] failed.", table_id);
    return s;
  }
  if (count > 0) {
    LOG_WARN("range hash[%lu ~ %lu], ts[%ld ~ %ld] has row [%lu], we clear them now.",
      begin_hash, end_hash, ts_span.begin, ts_span.end, count);
    s = table->DeleteTotalRange(ctx, begin_hash, end_hash, ts_span, 1, osn);
    if (s == KStatus::FAIL) {
      LOG_ERROR("DeleteTotalRange [%lu] failed.", table_id);
      return s;
    }
  }
  LOG_INFO("CreateSnapshotForWrite [%lu] succeeded. range hash[%lu ~ %lu], ts[%ld ~ %ld] has row [%lu]",
           table_id, begin_hash, end_hash, ts_span.begin, ts_span.end, count)
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data, bool& is_dropped) {
  TsRangeImgrationInfo ts_snapshot_info;
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    if (snapshots_.find(snapshot_id) != snapshots_.end()) {
      ts_snapshot_info = snapshots_[snapshot_id];
    } else {
      LOG_ERROR("GetSnapshotNextBatchData failed, cannot found snapshot [%lu]", snapshot_id);
      return KStatus::FAIL;
    }
  }
  uint32_t row_num = 0;
  TSSlice batch_data = {nullptr, 0};
  TSSlice del_data{nullptr, 0};
  Defer free_del_data([&]() { free(del_data.data); });
  if (!ts_snapshot_info.batch_read_finished) {
    bool is_del_iter_finished = false;
    // gen data from tabledelteinfo.
    auto s = ts_snapshot_info.del_iter->GetNextDeleteInfo(ctx, &del_data, &is_del_iter_finished);
    if (s == KStatus::FAIL) {
      LOG_ERROR("GetNextDeleteInfo snapshot [%lu] failed.", snapshot_id);
      return s;
    }
    if (is_del_iter_finished) {
      ts_snapshot_info.batch_read_finished = true;
    }
  }
  if (ts_snapshot_info.batch_read_finished) {
    auto s = ReadBatchData(ctx, ts_snapshot_info.table_id, ts_snapshot_info.table_version, ts_snapshot_info.begin_hash,
        ts_snapshot_info.end_hash, ts_snapshot_info.ts_span, ts_snapshot_info.id, &batch_data, &row_num, is_dropped);
    if (s == KStatus::FAIL) {
      LOG_ERROR("ReadBatchData snapshot [%lu] failed.", snapshot_id);
      return s;
    }
    if (row_num == 0) {
      s = BatchJobFinish(ctx, ts_snapshot_info.id);
      if (s == KStatus::FAIL) {
        LOG_ERROR("BatchJobFinish snapshot [%lu] failed.", snapshot_id);
        return s;
      }
    }
  }
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    TsRangeImgrationInfo& map_info = snapshots_[snapshot_id];
    map_info.package_id += 1;
    map_info.imgrated_rows += row_num;
    map_info.batch_read_finished = ts_snapshot_info.batch_read_finished;
  }
  if (row_num > 0 || del_data.len > 0) {
    if (!STPackageSnapshotData::PackageData(ts_snapshot_info.package_id + 1, ts_snapshot_info.table_id,
            ts_snapshot_info.table_version, batch_data, row_num, del_data, data)) {
      LOG_ERROR("PackageData snapshot [%lu] failed.", snapshot_id);
      return KStatus::FAIL;
    }
  } else {
    *data = {nullptr, 0};
  }
  LOG_DEBUG("GetSnapshotNextBatchData succeeded, snapshot[%lu] row_num[%u]", snapshot_id, row_num);
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data, bool& is_dropped) {
  TsRangeImgrationInfo ts_snapshot_info;
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    if (snapshots_.find(snapshot_id) != snapshots_.end()) {
      ts_snapshot_info = snapshots_[snapshot_id];
    } else {
      LOG_ERROR("WriteSnapshotBatchData failed, cannot found snapshot [%lu]", snapshot_id);
      return KStatus::FAIL;
    }
  }
  if (data.data == nullptr || data.len == 0) {
    LOG_WARN("WriteSnapshotBatchData ignore null data.");
    return KStatus::SUCCESS;
  }
  assert(data.len >= 20);
  uint32_t package_id = 0;
  TSTableID table_id = 0;
  uint32_t table_version = 0;
  uint32_t row_num = 0;
  TSSlice batch_data{nullptr, 0};
  TSSlice del_data{nullptr, 0};
  bool ok = STPackageSnapshotData::UnpackageData(data, package_id, table_id, table_version, batch_data, row_num, del_data);
  if (!ok) {
    LOG_ERROR("UnpackageData failed, last package id [%u].", ts_snapshot_info.package_id);
    return KStatus::FAIL;
  }
  if (package_id - 1 != ts_snapshot_info.package_id) {
    if (package_id - 1 > ts_snapshot_info.package_id) {
      LOG_ERROR("last package id [%u] is behind of current package [%u], failed.",
        ts_snapshot_info.package_id, package_id);
      return KStatus::FAIL;
    }
    LOG_WARN("last package id [%u] not front of current package [%u], ignore it.",
      ts_snapshot_info.package_id, package_id);
    return KStatus::SUCCESS;
  }
  if (row_num > 0) {
    auto s = WriteBatchData(ctx, table_id, table_version, ts_snapshot_info.id, &batch_data, &row_num, is_dropped);
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteBatchData snapshot [%lu] failed.", snapshot_id);
      return s;
    }
  }
  if (del_data.len > 0) {
    auto s = ts_snapshot_info.del_iter->WriteDelAndTagInfo(ctx, del_data, tag_lock_);
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteDelAndTagInfo snapshot [%lu] failed.", snapshot_id);
      return s;
    }
  }
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    snapshots_[snapshot_id].package_id = package_id;
    snapshots_[snapshot_id].imgrated_rows += row_num;
  }
  LOG_DEBUG("WriteSnapshotBatchData succeeded, snapshot[%lu] row_num[%u]", snapshot_id, row_num);
  return KStatus::SUCCESS;
}
KStatus TSEngineImpl::WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) {
  TsRangeImgrationInfo ts_snapshot_info;
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    if (snapshots_.find(snapshot_id) != snapshots_.end()) {
      ts_snapshot_info = snapshots_[snapshot_id];
    } else {
      LOG_ERROR("WriteSnapshotSuccess failed, cannot found snapshot [%lu]", snapshot_id);
      return KStatus::FAIL;
    }
  }
  auto s = BatchJobFinish(ctx, snapshot_id);
  if (s != KStatus::SUCCESS) {
      LOG_ERROR("BatchJobFinish failed.");
  }
  s = ts_snapshot_info.del_iter->CommitDeleteInfo(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_WARN("CommitDeleteInfo failed. [%d] table[%lu] range hash[%lu ~ %lu], ts[%ld ~ %ld]]. osn scan may not right.",
      ts_snapshot_info.type, ts_snapshot_info.table_id, ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash,
      ts_snapshot_info.ts_span.begin, ts_snapshot_info.ts_span.end);
  }
LOG_INFO("WriteSnapshotSuccess [%d] table[%lu] range hash[%lu ~ %lu], ts[%ld ~ %ld] row count[%lu].",
      ts_snapshot_info.type, ts_snapshot_info.table_id, ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash,
      ts_snapshot_info.ts_span.begin, ts_snapshot_info.ts_span.end, ts_snapshot_info.imgrated_rows);
  return s;
}
KStatus TSEngineImpl::WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id, uint64_t osn) {
  TsRangeImgrationInfo ts_snapshot_info;
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    if (snapshots_.find(snapshot_id) != snapshots_.end()) {
      ts_snapshot_info = snapshots_[snapshot_id];
    } else {
      LOG_ERROR("WriteSnapshotRollback failed, cannot found snapshot [%lu]", snapshot_id);
      return KStatus::FAIL;
    }
  }
  auto s = CancelBatchJob(ctx, snapshot_id, osn);
  if (s != KStatus::SUCCESS) {
      LOG_ERROR("CancelBatchJob failed.");
  }
  uint64_t count;
  s = ts_snapshot_info.table->DeleteTotalRange(ctx, ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash,
    ts_snapshot_info.ts_span, 1, osn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteTotalRange [%lu] failed.", ts_snapshot_info.table_id);
    return s;
  }
  s = DeleteSnapshot(ctx, snapshot_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("DeleteSnapshot failed.");
    return s;
  }
  LOG_INFO("WriteSnapshotRollback [%d] table[%lu] range hash[%lu ~ %lu], ts[%ld ~ %ld] row count[%lu].",
      ts_snapshot_info.type, ts_snapshot_info.table_id, ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash,
      ts_snapshot_info.ts_span.begin, ts_snapshot_info.ts_span.end, ts_snapshot_info.imgrated_rows);
  LOG_INFO("WriteSnapshotRollback succeeded, snapshot[%lu]", snapshot_id);
  return s;
}
KStatus TSEngineImpl::DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) {
  auto s = BatchJobFinish(ctx, snapshot_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BatchJobFinish failed.");
    return s;
  }
  TsRangeImgrationInfo ts_snapshot_info{0, 0, 0, 0, {0, 0}, 0, 0, 0, 0, nullptr, false, nullptr};
  {
    snapshot_mutex_.lock();
    Defer defer{[&](){
      snapshot_mutex_.unlock();
    }};
    if (snapshots_.find(snapshot_id) != snapshots_.end()) {
      ts_snapshot_info = snapshots_[snapshot_id];
      snapshots_.erase(snapshot_id);
    }
  }
  if (ts_snapshot_info.table_id != 0) {
    uint64_t count = 0;
    ts_snapshot_info.table->GetRangeRowCount(ctx, ts_snapshot_info.begin_hash, ts_snapshot_info.end_hash,
      {INT64_MIN, INT64_MAX}, &count);
    LOG_INFO("DeleteSnapshot [%d] table[%lu] range hash[%lu ~ %lu] row count[%lu].",
        ts_snapshot_info.type, ts_snapshot_info.table_id, ts_snapshot_info.begin_hash,
        ts_snapshot_info.end_hash, count);
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::FlushBuffer(kwdbContext_p ctx) {
  if (options_.wal_level != WALMode::OFF && !options_.use_raft_log_as_wal) {
    {
      wal_mgr_->Lock();
      wal_mgr_->Flush(ctx);
      wal_mgr_->Unlock();
    }
    for (auto& vg : vgroups_) {
      auto wal = vg->GetWALManager();
      wal->Lock();
      wal->Flush(ctx);
      wal->Unlock();
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) {
  if (wal_level == nullptr) {
    LOG_ERROR("wal_level is nullptr");
    return KStatus::FAIL;
  }
  *wal_level = options_.wal_level;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::SetUseRaftLogAsWAL(kwdbContext_p ctx, bool use) {
  options_.use_raft_log_as_wal = use;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) {
  return GetWaitThreadNum(ctx, resp);
}

// get max entity id
KStatus TSEngineImpl::GetMaxEntityIdByVGroupId(kwdbContext_p ctx, uint32_t vgroup_id, uint32_t& entity_id) {
  std::vector<std::shared_ptr<TsTableSchemaManager>> tb_schema_manager;
  KStatus s = GetAllTableSchemaMgrs(tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get all schema manager failed.");
  }
  std::shared_ptr<TagTable> tag_table;
  for (auto schema_mgr : tb_schema_manager) {
    s = schema_mgr->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    tag_table->GetMaxEntityIdByVGroupId(vgroup_id, entity_id);
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Vacuum(kwdbContext_p ctx, bool force) {
  for (const auto& vgroup : vgroups_) {
    vgroup->Vacuum(ctx, force);
  }
  return SUCCESS;
}

double divideAndRound(double a, double b, int precision) {
  double result = a / b;
  double factor = std::pow(10.0, precision);
  return std::round(result * factor) / factor;
}

KStatus ConstructTableBlocksDistribution(const std::shared_ptr<TsTableSchemaManager>& tb_schema_mgr,
                                VGroupBlocksInfo* vg_blocks_info, roachpb::BlocksDistribution* blocks_distribution) {
  const std::vector<AttributeInfo>* metric_meta{nullptr};
  KStatus s = tb_schema_mgr->GetMetricMeta(tb_schema_mgr->GetCurrentVersion(), &metric_meta);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetMetricMeta failed.");
    return s;
  }
  uint32_t row_len = 0;
  for (const auto& info : *metric_meta) {
    row_len += info.length;
  }
  if (vg_blocks_info->last_segments_info_.blocks_size_ != 0) {
    vg_blocks_info->last_segments_info_.compression_ratio_ = static_cast<float>(row_len) *
      vg_blocks_info->last_segments_info_.rows_num_ / vg_blocks_info->last_segments_info_.blocks_size_;
  }
  if (vg_blocks_info->entity_segments_info_.blocks_size_ != 0) {
    vg_blocks_info->entity_segments_info_.compression_ratio_ = static_cast<float>(row_len) *
      vg_blocks_info->entity_segments_info_.rows_num_ / vg_blocks_info->entity_segments_info_.blocks_size_;
  }

  roachpb::BlockInfo* last_segments_info = blocks_distribution->add_block_info();
  last_segments_info->set_level("last segments");
  last_segments_info->set_blocks_num(vg_blocks_info->last_segments_info_.blocks_num_);
  last_segments_info->set_blocks_size(vg_blocks_info->last_segments_info_.blocks_size_);
  last_segments_info->set_avg_size(vg_blocks_info->last_segments_info_.avg_size_);
  last_segments_info->set_compression_ratio(vg_blocks_info->last_segments_info_.compression_ratio_);
  last_segments_info->set_last_seg_level0(vg_blocks_info->last_segments_info_.last_seg_level0);
  last_segments_info->set_last_seg_level1(vg_blocks_info->last_segments_info_.last_seg_level1);
  last_segments_info->set_last_seg_level2(vg_blocks_info->last_segments_info_.last_seg_level2);

  roachpb::BlockInfo* entity_segments_info = blocks_distribution->add_block_info();
  entity_segments_info->set_level("entity segments");
  entity_segments_info->set_blocks_num(vg_blocks_info->entity_segments_info_.blocks_num_);
  entity_segments_info->set_blocks_size(vg_blocks_info->entity_segments_info_.blocks_size_);
  entity_segments_info->set_avg_size(vg_blocks_info->entity_segments_info_.avg_size_);
  entity_segments_info->set_compression_ratio(vg_blocks_info->entity_segments_info_.compression_ratio_);

  roachpb::BlockInfo* total_blocks_info = blocks_distribution->add_block_info();
  total_blocks_info->set_level("total");
  total_blocks_info->set_blocks_num(
    vg_blocks_info->last_segments_info_.blocks_num_ + vg_blocks_info->entity_segments_info_.blocks_num_);
  total_blocks_info->set_blocks_size(
    vg_blocks_info->last_segments_info_.blocks_size_ + vg_blocks_info->entity_segments_info_.blocks_size_);
  total_blocks_info->set_avg_size(0);
  total_blocks_info->set_compression_ratio(0);
  if (total_blocks_info->blocks_num() && total_blocks_info->blocks_size()) {
    total_blocks_info->set_avg_size(static_cast<double>(total_blocks_info->blocks_size()) / total_blocks_info->blocks_num());
    uint64_t total_row_num = vg_blocks_info->last_segments_info_.rows_num_ + vg_blocks_info->entity_segments_info_.rows_num_;
    total_blocks_info->set_compression_ratio(
      static_cast<float>(row_len) * total_row_num / total_blocks_info->blocks_size());
    total_blocks_info->set_original_size(row_len * total_row_num);
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetTableBlocksDistribution(TSTableID table_id, TSSlice* blocks_info) {
  VGroupBlocksInfo vg_blocks_info;
  uint32_t db_id = schema_mgr_->GetDBIDByTableID(table_id);
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    if (tb_schema_mgr == nullptr) {
      LOG_INFO("Table[%lu] has already been dropped.", table_id);
    }
    return s;
  }

  for (const auto& vgroup : vgroups_) {
    VGroupBlocksInfo tmp;
    std::vector<uint32_t> entity_ids;
    tb_schema_mgr->GetTagTable()->GetEntityIdListByVGroupId(vgroup->GetVGroupID(), entity_ids);
    vgroup->GetTableBlocksDistribution(db_id, table_id, entity_ids, &tmp);
    vg_blocks_info.Add(tmp);
  }
  roachpb::BlocksDistribution blocks_distribution;
  s = ConstructTableBlocksDistribution(tb_schema_mgr, &vg_blocks_info, &blocks_distribution);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  blocks_info->len = blocks_distribution.ByteSizeLong();
  blocks_info->data = static_cast<char*>(malloc(blocks_info->len));
  if (!blocks_distribution.SerializeToArray(blocks_info->data, blocks_info->len)) {
    LOG_ERROR("Serialize roachpb::BlocksDistribution to string failed.");
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TSEngineImpl::GetDBBlocksDistribution(uint32_t db_id, TSSlice* blocks_info) {
  VGroupBlocksInfo db_blocks_info;
  std::vector<std::shared_ptr<TsTableSchemaManager>> tb_schema_mgrs{};
  KStatus s = GetAllTableSchemaMgrs(tb_schema_mgrs);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAllTableSchemaMgrs failed.");
    return  s;
  }
  uint64_t original_data_size = 0;
  for (const auto& tb_schema_mgr : tb_schema_mgrs) {
    if (tb_schema_mgr->GetDbID() == db_id) {
      VGroupBlocksInfo tb_blocks_info;
      for (const auto& vgroup : vgroups_) {
        VGroupBlocksInfo tmp;
        std::vector<uint32_t> entity_ids;
        tb_schema_mgr->GetTagTable()->GetEntityIdListByVGroupId(vgroup->GetVGroupID(), entity_ids);
        vgroup->GetTableBlocksDistribution(db_id, tb_schema_mgr->GetTableId(), entity_ids, &tmp);
        tb_blocks_info.Add(tmp);
      }
      const std::vector<AttributeInfo>* metric_meta{nullptr};
      s = tb_schema_mgr->GetMetricMeta(tb_schema_mgr->GetCurrentVersion(), &metric_meta);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetMetricMeta failed.");
        return s;
      }
      uint32_t row_len = 0;
      for (const auto& info : *metric_meta) {
        row_len += info.length;
      }
      if (tb_blocks_info.last_segments_info_.rows_num_ != 0) {
        original_data_size += row_len * tb_blocks_info.last_segments_info_.rows_num_;
      }
      if (tb_blocks_info.entity_segments_info_.rows_num_ != 0) {
        original_data_size += row_len * tb_blocks_info.entity_segments_info_.rows_num_;
      }
      db_blocks_info.Add(tb_blocks_info);
    }
  }

  roachpb::BlocksDistribution blocks_distribution;
  roachpb::BlockInfo* total_blocks_info = blocks_distribution.add_block_info();
  total_blocks_info->set_level("total");
  total_blocks_info->set_blocks_num(
    db_blocks_info.last_segments_info_.blocks_num_ + db_blocks_info.entity_segments_info_.blocks_num_);
  total_blocks_info->set_blocks_size(
    db_blocks_info.last_segments_info_.blocks_size_ + db_blocks_info.entity_segments_info_.blocks_size_);
  total_blocks_info->set_avg_size(0);
  total_blocks_info->set_compression_ratio(0);
  if (total_blocks_info->blocks_num() && total_blocks_info->blocks_size()) {
    total_blocks_info->set_avg_size(static_cast<double>(total_blocks_info->blocks_size()) / total_blocks_info->blocks_num());
    total_blocks_info->set_compression_ratio(static_cast<float>(original_data_size) / total_blocks_info->blocks_size());
  }

  total_blocks_info->set_last_seg_level0(db_blocks_info.last_segments_info_.last_seg_level0);
  total_blocks_info->set_last_seg_level1(db_blocks_info.last_segments_info_.last_seg_level1);
  total_blocks_info->set_last_seg_level2(db_blocks_info.last_segments_info_.last_seg_level2);
  total_blocks_info->set_original_size(original_data_size);

  blocks_info->len = blocks_distribution.ByteSizeLong();
  blocks_info->data = static_cast<char*>(malloc(blocks_info->len));
  if (!blocks_distribution.SerializeToArray(blocks_info->data, blocks_info->len)) {
    LOG_ERROR("Serialize roachpb::BlocksDistribution to string failed.");
    return KStatus::FAIL;
  }
  return SUCCESS;
}

void TSEngineImpl::createDroppedFlag(TSTableID table_id) {
  std::string file_name = options_.db_path + "/" + schema_directory + "/." + to_string(table_id);
  std::ofstream tmp_file(file_name);
  if (tmp_file.is_open()) {
    tmp_file.close();
  } else {
    LOG_ERROR("Failed to create drop flag file for table:%ld", table_id);
  }
}

void TSEngineImpl::removeDroppedFlag(TSTableID table_id) {
  std::string file_name = options_.db_path + "/" + schema_directory + "/." + to_string(table_id);
  if (fs::exists(file_name)) {
    fs::remove(file_name);
  }
}

bool TSEngineImpl::HasDroppedFlag(TSTableID table_id) {
  std::string file_name = options_.db_path + "/" + schema_directory + "/." + to_string(table_id);
  if (fs::exists(file_name)) {
    return true;
  }
  return false;
}
}  // namespace kwdbts
