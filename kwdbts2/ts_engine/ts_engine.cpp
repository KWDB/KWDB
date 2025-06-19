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
#include <filesystem>
#include <memory>
#include <utility>
#include "ts_payload.h"
#include "ee_global.h"
#include "ee_executor.h"
#include "ts_table_v2_impl.h"

// V2
int EngineOptions::vgroup_max_num = 6;
DedupRule EngineOptions::g_dedup_rule = DedupRule::OVERRIDE;
size_t EngineOptions::mem_segment_max_size = 64 << 20;
int32_t EngineOptions::mem_segment_max_height = 12;
uint32_t EngineOptions::max_last_segment_num = 2;
uint32_t EngineOptions::max_compact_num = 10;
size_t EngineOptions::max_rows_per_block = 4096;
size_t EngineOptions::min_rows_per_block = 1000;

extern std::map<std::string, std::string> g_cluster_settings;
extern DedupRule g_dedup_rule;
extern std::shared_mutex g_settings_mutex;
extern bool g_go_start_service;

namespace kwdbts {

unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
std::mt19937 gen(seed);
const char schema_directory[]= "schema";

TSEngineV2Impl::TSEngineV2Impl(const EngineOptions& engine_options) : options_(engine_options), flush_mgr_(vgroups_) {
  LogInit();
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(EngineOptions::table_cache_capacity_, true);
  char* vgroup_num = getenv("KW_VGROUP_NUM");
  if (vgroup_num != nullptr) {
    char *endptr;
    EngineOptions::vgroup_max_num = strtol(vgroup_num, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* mem_segment_max_size = getenv("KW_MAX_SEGMENT_MAX_SIZE");
  if (mem_segment_max_size != nullptr) {
    char *endptr;
    EngineOptions::mem_segment_max_size = strtol(mem_segment_max_size, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* mem_segment_max_height = getenv("KW_MAX_SEGMENT_MAX_HEIGHT");
  if (mem_segment_max_height != nullptr) {
    char *endptr;
    EngineOptions::mem_segment_max_height = strtol(mem_segment_max_height, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* max_last_segment_num = getenv("KW_MAX_LAST_SEGMENT_NUM");
  if (max_last_segment_num != nullptr) {
    char *endptr;
    EngineOptions::max_last_segment_num = strtol(max_last_segment_num, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* max_compact_num = getenv("KW_MAX_COMPACT_NUM");
  if (max_compact_num != nullptr) {
    char *endptr;
    EngineOptions::max_compact_num = strtol(max_compact_num, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* max_rows_per_block = getenv("KW_MAX_ROWS_PER_BLOCK");
  if (max_rows_per_block != nullptr) {
    char *endptr;
    EngineOptions::max_rows_per_block = strtol(max_rows_per_block, &endptr, 10);
    assert(*endptr == '\0');
  }
  char* min_rows_per_block = getenv("KW_MIN_ROWS_PER_BLOCK");
  if (min_rows_per_block != nullptr) {
    char *endptr;
    EngineOptions::min_rows_per_block = strtol(min_rows_per_block, &endptr, 10);
    assert(*endptr == '\0');
  }
}

TSEngineV2Impl::~TSEngineV2Impl() {
  DestoryExecutor();
  vgroups_.clear();
  SafeDeletePointer(tables_cache_);
}

KStatus TSEngineV2Impl::Init(kwdbContext_p ctx) {
  std::filesystem::path db_path{options_.db_path};
  assert(!db_path.empty());
  schema_mgr_ = std::make_unique<TsEngineSchemaManager>(db_path / schema_directory);
  KStatus s = schema_mgr_->Init(ctx);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  InitExecutor(ctx, options_);

  vgroups_.clear();
  for (size_t i = 0; i < EngineOptions::vgroup_max_num; i++) {
    auto vgroup =
        std::make_unique<TsVGroup>(options_, i + 1, schema_mgr_.get());
    s = vgroup->Init(ctx);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    vgroups_.push_back(std::move(vgroup));
  }

  wal_mgr_ = std::make_unique<WALMgr>(options_.db_path, "engine", &options_);
  auto res = wal_mgr_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  wal_sys_ = std::make_unique<WALMgr>(options_.db_path, "ddl", &options_);
  tsx_manager_sys_ = std::make_unique<TSxMgr>(wal_sys_.get());
  s = wal_sys_->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Init fail.")
    return s;
  }

  s = Recover(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Recover fail.")
    return s;
  }
  return KStatus::SUCCESS;
}

TsVGroup* TSEngineV2Impl::GetVGroupByID(kwdbContext_p ctx, uint32_t vgroup_id) {
  assert(EngineOptions::vgroup_max_num >= vgroup_id);
  return vgroups_[vgroup_id - 1].get();
}

KStatus TSEngineV2Impl::CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
  std::vector<RangeGroup> ranges) {
  std::shared_ptr<TsTable> ts_table;
  return CreateTsTable(ctx, table_id, meta, ts_table);
}

KStatus TSEngineV2Impl::CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable *meta,
  std::shared_ptr<TsTable>& ts_table) {
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;
  ts_table = tables_cache_->Get(table_id);
  if (ts_table != nullptr) {
    LOG_INFO("TsTable %lu exist. no need create again.", table_id);
    return KStatus::SUCCESS;
  }

  uint64_t db_id = 1;
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

KStatus TSEngineV2Impl::GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                    bool create_if_not_exist, ErrorInfo& err_info, uint32_t version) {
  ts_table = tables_cache_->Get(table_id);
  if (ts_table == nullptr) {
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
    if (error != nullptr) {
      LOG_ERROR("getTableMetaByVersion error: %s.", error);
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

KStatus TSEngineV2Impl::CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                           const char* transaction_id, const uint32_t cur_version,
                                           const uint32_t new_version,
                                           const std::vector<uint32_t/* tag column id*/> &index_schema) {
    LOG_INFO("TSEngine CreateNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
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

KStatus TSEngineV2Impl::DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                         const char* transaction_id,  const uint32_t cur_version,
                                         const uint32_t new_version) {
    LOG_INFO("TSEngine DropNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
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

KStatus TSEngineV2Impl::AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                          const char* transaction_id, const uint32_t old_version, const uint32_t new_version,
                                          const std::vector<uint32_t/* tag column id*/> &new_index_schema) {
    return SUCCESS;
}

KStatus TSEngineV2Impl::putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id,
                                   TsRawPayload &payload) {
  ErrorInfo err_info;
  // 1. Write tag data
  uint8_t payload_data_flag = payload.GetRowType();
  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY) {
    // tag
    LOG_DEBUG("tag bt insert hashPoint=%hu", payload.GetHashPoint());
    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    KStatus s = GetTableSchemaMgr(ctx, table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    err_info.errcode = tag_table->InsertTagRecord(payload, groupid, entity_id);
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                  TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                  uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool write_wal) {
  std::shared_ptr<kwdbts::TsTable> ts_table;
  ErrorInfo err_info;
  uint32_t vgroup_id;
  TSEntityID entity_id;
  size_t payload_size = 0;
  dedup_result->payload_num = payload_num;
  dedup_result->dedup_rule = static_cast<int>(EngineOptions::g_dedup_rule);
  for (size_t i = 0; i < payload_num; i++) {
    TsRawPayload p{payload_data[i]};
    TSSlice primary_key = p.GetPrimaryTag();
    auto tbl_version = p.GetTableVersion();
    auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, tbl_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, tbl_version, err_info.errmsg.c_str());
      return s;
    }
    bool new_tag;
    s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    auto vgroup = GetVGroupByID(ctx, vgroup_id);
    assert(vgroup != nullptr);
    if (new_tag) {
      if (options_.wal_level != WALMode::OFF && write_wal) {
        // no need lock, lock inside.
        s = vgroup->GetWALManager()->WriteInsertWAL(ctx, mtr_id, 0, 0, payload_data[i], vgroup_id);
        if (s == KStatus::FAIL) {
          LOG_ERROR("failed WriteInsertWAL for new tag.");
          return s;
        }
      }
      entity_id = vgroup->AllocateEntityID();
      s = putTagData(ctx, table_id, vgroup_id, entity_id, p);
      if (s != KStatus::SUCCESS) {
        return s;
      }
      inc_entity_cnt++;
    }
    payload_size += p.GetData().len;
    // s = ts_table->PutData(ctx, vgroup_id, &payload_data[i], 1,
    //                       mtr_id, reinterpret_cast<uint16_t*>(&entity_id),
    //                       inc_unordered_cnt, dedup_result, (DedupRule)(dedup_result->dedup_rule));
    s =  dynamic_pointer_cast<TsTableV2Impl>(ts_table)->PutData(ctx, vgroup, p, entity_id, mtr_id,
            inc_unordered_cnt, dedup_result, (DedupRule)(dedup_result->dedup_rule), write_wal);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("put data failed. table[%lu].", table_id);
      return s;
    }
  }
  flush_mgr_.Count(payload_size);
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable *meta) {
  return schema_mgr_->GetMeta(ctx, table_id, version, meta);
}

KStatus TSEngineV2Impl::LogInit() {
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

KStatus TSEngineV2Impl::AddColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, TSSlice column,
                                  uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, cur_version);
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
    return s;
  }
  return ts_table->AlterTable(ctx, AlterType::ADD_COLUMN, &column_meta, cur_version, new_version, err_msg);
}

KStatus TSEngineV2Impl::DropColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, TSSlice column,
                                   uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, cur_version);
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
    return s;
  }
  return ts_table->AlterTable(ctx, AlterType::DROP_COLUMN, &column_meta,
                              cur_version, new_version, err_msg);
}

KStatus TSEngineV2Impl::AlterColumnType(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id,
                                        TSSlice new_column, TSSlice origin_column, uint32_t cur_version,
                                        uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn new_col_meta;
  if (!new_col_meta.ParseFromArray(new_column.data, new_column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, cur_version);
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
    return s;
  }
  return ts_table->AlterTable(ctx, AlterType::ALTER_COLUMN_TYPE, &new_col_meta,
                              cur_version, new_version, err_msg);
}

KStatus TSEngineV2Impl::AlterLifetime(kwdbContext_p ctx, const KTableKey& table_id, uint64_t lifetime) {
  LOG_INFO("Alter life time on table %lu start.", table_id);
  std::shared_ptr<TsTableSchemaManager> tb_schema_mgr;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TSEngineV2Impl get table schema manager failed, table id: %lu", table_id);
    return s;
  }
  auto old_life_time = tb_schema_mgr->GetLifeTime();
  // convert second to millisecond, * 1000
  tb_schema_mgr->SetLifeTime(LifeTime{static_cast<int64_t>(lifetime), 1000});
  LOG_INFO("Alter table life time success, change from %lu[precision:%d] to %lu[precision:%d]",
    old_life_time.ts, old_life_time.precision, lifetime, 1000);
  return KStatus::SUCCESS;
}
std::vector<std::shared_ptr<TsVGroup>>* TSEngineV2Impl::GetTsVGroups() {
  return &vgroups_;
}

KStatus TSEngineV2Impl::TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                   uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  if (options_.wal_level == WALMode::OFF) {
    return KStatus::SUCCESS;
  }
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  std::uniform_int_distribution<int> distrib(1, EngineOptions::vgroup_max_num);
  auto vgroup = GetVGroupByID(ctx, distrib(gen));
  return vgroup->MtrBegin(ctx, range_id, index, mtr_id);
}

KStatus TSEngineV2Impl::TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                                    uint64_t range_group_id, uint64_t mtr_id) {
  if (options_.wal_level == WALMode::OFF) {
    return KStatus::SUCCESS;
  }
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  std::uniform_int_distribution<int> distrib(1, EngineOptions::vgroup_max_num);
  auto vgroup = GetVGroupByID(ctx, distrib(gen));
  return vgroup->MtrCommit(ctx, mtr_id);
}

KStatus TSEngineV2Impl::TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                                      uint64_t range_group_id, uint64_t mtr_id) {
  EnterFunc()
//  1. Write ROLLBACK log;
//  2. Backtrace WAL logs based on xID to the BEGIN log of the Mini-Transaction.
//  3. Invoke the reverse operation based on the type of each log:
//    1) For INSERT operations, add the DELETE MARK to the corresponding data;
//    2) For the DELETE operation, remove the DELETE MARK of the corresponding data;
//    3) For ALTER operations, roll back to the previous schema version;
//  4. If the rollback fails, a system log is generated and an error exit is reported.
  if (options_.wal_level == WALMode::OFF) {
    return KStatus::SUCCESS;
  }
  KStatus s;

  std::uniform_int_distribution<int> distrib(1, EngineOptions::vgroup_max_num);
  auto vgroup = GetVGroupByID(ctx, distrib(gen));
  s = vgroup->MtrRollback(ctx, mtr_id);
  if (s == FAIL) {
    return s;
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
      delete wal_log;
    }
  }
  Return(s)
}

KStatus TSEngineV2Impl::checkpoint(kwdbts::kwdbContext_p ctx) {
  wal_sys_->Lock();
  KStatus s = wal_sys_->CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to create checkpoint for TS Engine.")
    return s;
  }
  wal_sys_->Unlock();

  return SUCCESS;
}

KStatus TSEngineV2Impl::TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  s = tsx_manager_sys_->TSxBegin(ctx, transaction_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed")
    return s;
  }
  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed, The target table is not available, table id: %lu", table_id)
    return s;
  }

  s = CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to CreateCheckpoint.")
  #ifdef WITH_TESTS
    return s;
  #endif
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id != 0) {
    if (tsx_manager_sys_->TSxCommit(ctx, transaction_id) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal failed, table id: %lu", table_id)
      return KStatus::FAIL;
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

KStatus TSEngineV2Impl::TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
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

  s = GetTsTable(ctx, table_id, table);
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

KStatus TSEngineV2Impl::CreateCheckpoint(kwdbContext_p ctx) {
  /*
   * 1. read chk log from chk file.
   * 2. read wal log from all vgroup
   * 3. merge chk log and wal log
   * 4. rewrite wal log to new chk file
   * 5. trig all vgroup flush
   * 6. write EndWAL to chk file
   * 7. trig all vgroup write checkpoint wal and update checkpoint LSN
   * 8. remove vgroup wal file and old chk file
   */
  if (options_.wal_level == WALMode::OFF) {
    return KStatus::SUCCESS;
  }
  std::vector<LogEntry*> logs;
  std::vector<LogEntry*> rewrite;
  std::unordered_map<uint32_t, uint64_t> vgrp_lsn;
  KStatus s;
  Defer defer{[&]() {
    for (auto& log : logs) {
      delete log;
    }
  }};
  // 1. read chk log from chk file.
  std::vector<uint64_t> vgroup_lsn;
  s = wal_mgr_->ReadWALLog(logs, wal_mgr_->FetchCheckpointLSN(), wal_mgr_->FetchCurrentLSN(), vgroup_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to read wal log from chk file.")
    return s;
  }
  if (vgroup_lsn.empty()) {
    LOG_INFO("Cannot detect the end checkpoint wal, skipping this file's content.")
    logs.clear();
  }
  s = wal_mgr_->SwitchNextFile();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to switch chk file.")
    return s;
  }

  // 2. read wal log from all vgroup
  for (const auto &vgrp : vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_LSN lsn = 0;
    if (vgrp->GetVGroupID() <= vgroup_lsn.size()) {
      lsn = vgroup_lsn[vgrp->GetVGroupID() - 1];
    }
    if (vgrp->ReadWALLogFromLastCheckpoint(ctx, vlogs, lsn) == KStatus::FAIL) {
      LOG_ERROR("Failed to ReadWALLogFromLastCheckpoint from vgroup : %d", vgrp->GetVGroupID())
      return KStatus::FAIL;
    }
    uint32_t vgrp_id = vgrp->GetVGroupID();
    vgrp_lsn.emplace(vgrp_id, lsn);

    logs.insert(logs.end(), vlogs.begin(), vlogs.end());
  }
  // 3. merge chk log and wal log
  std::vector<uint64_t> commit;

  for (auto log : logs) {
    switch (log->getType()) {
      case MTR_COMMIT : {
        commit.emplace_back(log->getXID());
      }
      default:
        continue;
    }
  }

  for (auto log : logs) {
    bool skip = false;
    for (auto xid : commit) {
      if (log->getXID() == xid) {
        skip = true;
        break;
      }
    }
    if (!skip) {
      if (log->getType() != WALLogType::CHECKPOINT) {
        if (!EngineOptions::isSingleNode()) {
          rewrite.emplace_back(log);
        }
      }
    }
  }

  // 4. rewrite wal log to chk file
  wal_mgr_ = nullptr;
  wal_mgr_ = std::make_unique<WALMgr>(options_.db_path, "engine", &options_);
  s = wal_mgr_->ResetWAL(ctx, true);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to reset wal log file before write incomplete log.")
    return s;
  }
  if (wal_mgr_->WriteIncompleteWAL(ctx, rewrite) == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteIncompleteWAL.")
    return KStatus::FAIL;
  }
  rewrite.clear();

  // 5. trig all vgroup flush
  // for (const auto &vgrp : vgroups_) {
  //   s = vgrp->Flush();
  //   if (s == KStatus::FAIL) {
  //     LOG_ERROR("Failed to flush metric file.")
  //     return s;
  //   }
  // }

  // 6.write EndWAL to chk file
  TS_LSN lsn;
  uint64_t lsn_len = vgrp_lsn.size() * sizeof(uint64_t);
  char* v_lsn = new char[lsn_len];
  int location = 0;
  for (auto it : vgrp_lsn) {
    memcpy(v_lsn + location, &(it.second), sizeof(uint64_t));
    location += sizeof(uint64_t);
  }
  auto end_chk_log = EndCheckpointEntry::construct(WALLogType::END_CHECKPOINT, 0, lsn_len, v_lsn);
  s = wal_mgr_->WriteWAL(ctx, end_chk_log, EndCheckpointEntry::fixed_length + lsn_len, lsn);
  delete []end_chk_log;
  delete []v_lsn;
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to write end checkpoint wal.")
    return s;
  }

  // 7. a). update checkpoint LSN .
  //    b). trig all vgroup write checkpoint wal.
  //    c). remove vgroup wal file.
  for (const auto &vgrp : vgroups_) {
    TS_LSN lsn = 0;
    uint32_t vgrp_id = vgrp->GetVGroupID();
    auto it = vgrp_lsn.find(vgrp_id);
    if (it != vgrp_lsn.end()) {
      lsn = it->second;
    } else {
      LOG_ERROR("Failed to find vgroup lsn from map.")
      return KStatus::FAIL;
    }
    s = vgrp->UpdateLSN(ctx, lsn);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to update vgroup checkpoint lsn.")
      return s;
    }
  }

  // 8. remove old chk file
  s = wal_mgr_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to remove chk file.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                        HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                        uint64_t mtr_id) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteRangeData(ctx, range_group_id, hash_span, ts_spans, count, mtr_id);
}

KStatus TSEngineV2Impl::DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                    uint64_t mtr_id) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteData(ctx, range_group_id, primary_tag, ts_spans, count, mtr_id);
}

KStatus TSEngineV2Impl::DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                        std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return (dynamic_pointer_cast<TsTableV2Impl>(ts_table))->DeleteEntities(ctx, primary_tags, count, mtr_id);
}

KStatus TSEngineV2Impl::DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_grp_id,
                            const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) {
  ErrorInfo err_info;
  std::shared_ptr<kwdbts::TsTable> ts_table;
  auto s = GetTsTable(ctx, table_id, ts_table, true, err_info, 0);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, 0, err_info.errmsg.c_str());
    return s;
  }
  ctx->ts_engine = this;
  return ts_table->DeleteRangeEntities(ctx, range_grp_id, hash_span, count, mtr_id);
}

// check if table is dropped from rocksdb.
KStatus TSEngineV2Impl::DropResidualTsTable(kwdbContext_p ctx) {
  std::vector<TSTableID> tables;
  auto s = schema_mgr_->GetTableList(&tables);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TSEngineV2Impl::DropResidualTsTable failed.");
    return s;
  }
  for (auto table_id : tables) {
    bool is_exist = checkTableMetaExist(table_id);
    if (!is_exist) {
      KStatus s = DropTsTable(ctx, table_id);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("drop table [%ld] failed", table_id);
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) {
  // todo(liangbo01) to implemented.
  return KStatus::FAIL;
}

KStatus TSEngineV2Impl::recover(kwdbts::kwdbContext_p ctx) {
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

  std::vector<uint64_t> ignore;
  s = wal_sys_->ReadWALLog(redo_logs, checkpoint_lsn, current_lsn, ignore);
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
            std::shared_ptr<TsTable> table;
            ErrorInfo err_info;
            KStatus s = GetTsTable(ctx, table_id, table, true, err_info);
            if (s == KStatus::FAIL) {
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
            KStatus s = GetTsTable(ctx, table_id, table, true, err_info, index_log->getCurTsVersion());
            if (s == KStatus::FAIL) {
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
            KStatus s = GetTsTable(ctx, table_id, table, true, err_info, index_log->getCurTsVersion());
            if (s == KStatus::FAIL) {
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
        KStatus s = GetTsTable(ctx, table_id, table, true, err_info, index_log->getCurTsVersion());
        if (s == KStatus::FAIL) {
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
        KStatus s = GetTsTable(ctx, table_id, table, true, err_info, index_log->getCurTsVersion());
        if (s == KStatus::FAIL) {
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

KStatus TSEngineV2Impl::Recover(kwdbContext_p ctx) {
  /*
   * 1. get engine chk wal log.
   * 2. get all vgroup wal log from last checkpoint lsn.
   * 3. merge wal and apply wal
   */
  if (options_.wal_level == WALMode::OFF) {
    return KStatus::SUCCESS;
  }

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
  s = wal_mgr_->ReadWALLog(logs, wal_mgr_->FetchCheckpointLSN(), wal_mgr_->FetchCurrentLSN(), vgroup_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLog from chk file while recovering.")
    return KStatus::FAIL;
  }
  if (vgroup_lsn.empty()) {
    LOG_INFO("Cannot detect the end checkpoint wal, skipping this file's content.")
    logs.clear();
  }

  // 2. get all vgroup wal log
  for (const auto &vgrp : vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_LSN lsn = 0;
    if (vgrp->GetVGroupID() < vgroup_lsn.size()) {
      lsn = vgroup_lsn[vgrp->GetVGroupID() - 1];
    }

    if (vgrp->ReadLogFromLastCheckpoint(ctx, vlogs, lsn) == KStatus::FAIL) {
      LOG_ERROR("Failed to ReadWALLogFromLastCheckpoint from vgroup : %d", vgrp->GetVGroupID())
      return KStatus::FAIL;
    }
    logs.insert(logs.end(), vlogs.begin(), vlogs.end());
  }

  // 3. apply redo log
  std::unordered_map<TS_LSN, MTRBeginEntry*> incomplete;
  for (auto wal_log : logs) {
    if (wal_log->getType() == WALLogType::MTR_BEGIN)  {
      auto log = reinterpret_cast<MTRBeginEntry *>(wal_log);
      incomplete.insert(std::pair<TS_LSN, MTRBeginEntry *>(log->getXID(), log));
    }
  }

  for (auto wal_log : logs) {
    switch (wal_log->getType()) {
      case WALLogType::MTR_ROLLBACK: {
        auto log = reinterpret_cast<MTREntry*>(wal_log);
        auto x_id = log->getXID();
        if (TSMtrRollback(ctx, 0, 0, x_id) == KStatus::FAIL) return KStatus::FAIL;
        incomplete.erase(log->getXID());
        break;
      }
      case WALLogType::CHECKPOINT:
      case WALLogType::MTR_BEGIN: {
        // do nothing
        break;
      }
      case WALLogType::MTR_COMMIT: {
        auto log = reinterpret_cast<MTREntry*>(wal_log);
        incomplete.erase(log->getXID());
        break;
      }
      default:
        auto vgrp_id = wal_log->getVGroupID();
        TsVGroup* vg = GetVGroupByID(ctx, vgrp_id);
        vg->ApplyWal(ctx, wal_log, incomplete);
    }
  }
  // 4. rollback incomplete wal
  for (auto& it : incomplete) {
    TS_LSN mtr_id = it.first;
    auto log_entry = it.second;
    uint64_t applied_index = GetAppliedIndex(log_entry->getRangeID(), range_indexes_map_);
    if (it.second->getIndex() <= applied_index) {
      std::uniform_int_distribution<int> distrib(1, EngineOptions::vgroup_max_num);
      auto vgroup = GetVGroupByID(ctx, distrib(gen));
      s = vgroup->MtrCommit(ctx, mtr_id);
      if (s == FAIL) return s;
    } else {
      if (TSMtrRollback(ctx, 0, 0, mtr_id) == KStatus::FAIL) return KStatus::FAIL;
    }
  }

  // TODO(zzr): Recover TsVersion for each VGroup.
  // After WAL RedoPut, TsVersion should be updated.
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value) {
  std::shared_lock<std::shared_mutex> lock(g_settings_mutex);
  std::map<std::string, std::string>::iterator iter = g_cluster_settings.find(key);
  if (iter != g_cluster_settings.end()) {
    *value = iter->second;
    return KStatus::SUCCESS;
  } else {
    return KStatus::FAIL;
  }
}

KStatus TSEngineV2Impl::UpdateSetting(kwdbContext_p ctx) {
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

}  // namespace kwdbts
