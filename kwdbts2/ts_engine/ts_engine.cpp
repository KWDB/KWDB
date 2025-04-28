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
DedupRule EngineOptions::g_dedup_rule = DedupRule::KEEP;
size_t EngineOptions::mem_segment_max_size = 64 << 20;
int32_t EngineOptions::mem_segment_max_height = 12;
uint32_t EngineOptions::max_last_segment_num = 2;
uint32_t EngineOptions::max_compact_num = 10;
size_t EngineOptions::max_rows_per_block = 4096;
size_t EngineOptions::min_rows_per_block = 1000;

namespace kwdbts {

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
    auto vgroup = std::make_unique<TsVGroup>(options_, i + 1, schema_mgr_.get());
    s = vgroup->Init(ctx);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    vgroups_.push_back(std::move(vgroup));
  }

  wal_mgr_ = std::make_unique<WALMgr>(options_.db_path, "", &options_);
  auto res = wal_mgr_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  return KStatus::SUCCESS;
}

TsVGroup* TSEngineV2Impl::GetVGroupByID(kwdbContext_p ctx, uint32_t vgroup_id) {
  assert(EngineOptions::vgroup_max_num >= vgroup_id);
  return vgroups_[vgroup_id - 1].get();
}

KStatus TSEngineV2Impl::CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable *meta) {
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;

  uint32_t vgroup_id = 1;
  if (meta->ts_table().has_database_id()) {
    vgroup_id = meta->ts_table().database_id();
  }
  schema_mgr_->SetTableID2DBID(ctx, table_id, meta->ts_table().database_id());

  s = schema_mgr_->CreateTable(ctx, table_id, meta);
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
  std::shared_ptr<TsTable> ts_table = std::make_shared<TsTableV2Impl>(table_schema_mgr, vgroups_);
  tables_cache_->Put(table_id, ts_table);
  return s;
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
      if (options_.wal_level != WALMode::OFF) {
        // no need lock, lock inside.
        s = vgroup->GetWALManager()->WriteInsertWAL(ctx, mtr_id, 0, 0, payload_data[i]);
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
            inc_unordered_cnt, dedup_result, (DedupRule)(dedup_result->dedup_rule));
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
  return ts_table->AlterTable(ctx, AlterType::ALTER_COLUMN_TYPE, &new_col_meta,
                              cur_version, new_version, err_msg);
}

std::vector<std::shared_ptr<TsVGroup>>* TSEngineV2Impl::GetTsVGroups() {
  return &vgroups_;
}

KStatus TSEngineV2Impl::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  return tsx_mgr_->MtrBegin(ctx, range_id, index, mtr_id);
}

KStatus TSEngineV2Impl::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id) {
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  return tsx_mgr_->MtrCommit(ctx, mtr_id);
}

KStatus TSEngineV2Impl::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool skip_log) {
  EnterFunc()
//  1. Write ROLLBACK log;
//  2. Backtrace WAL logs based on xID to the BEGIN log of the Mini-Transaction.
//  3. Invoke the reverse operation based on the type of each log:
//    1) For INSERT operations, add the DELETE MARK to the corresponding data;
//    2) For the DELETE operation, remove the DELETE MARK of the corresponding data;
//    3) For ALTER operations, roll back to the previous schema version;
//  4. If the rollback fails, a system log is generated and an error exit is reported.

  KStatus s;
  if (!skip_log) {
    s = tsx_mgr_->MtrRollback(ctx, mtr_id);
    if (s == FAIL) Return(s)
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
  std::vector<LogEntry*> logs;
  std::vector<LogEntry *> rewrite;
  std::unordered_map<uint64_t, uint64_t>  vgrp_lsn;
  Defer defer{[&]() {
    for (auto& log : logs) {
      delete log;
    }
    for (auto& log : rewrite) {
      delete log;
    }
  }};
  // 1. read chk log from chk file.
  wal_mgr_->ReadWALLog(logs, 0, wal_mgr_->FetchCurrentLSN());

  // 2. read wal log from all vgroup
  for (const auto &vgrp: vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_LSN lsn;
    if (vgrp->ReadWALLogFromLastCheckpoint(ctx, vlogs, lsn) == KStatus::FAIL) {
      LOG_ERROR("Failed to CreateCheckpointInternal for vgroup : %d", vgrp->GetVGroupID())
      return KStatus::FAIL;
    }
    vgrp_lsn[vgrp->GetVGroupID()] = lsn;
    logs.insert(logs.end(), vlogs.begin(), vlogs.end());
  }
  // 3. merge chk log and wal log
  std::vector<uint64_t> commit;

  for (auto log: logs) {
    switch (log->getType()) {
      case MTR_COMMIT : {
        commit.emplace_back(log->getXID());
      }
      default:
        continue;
    }
  }

  for (auto log: logs) {
    bool skip = false;
    for (auto xid: commit) {
      if (log->getXID() == xid) {
        skip = true;
        break;
      }
    }
    if (!skip) {
      rewrite.emplace_back(log);
    }
  }

  // 4. rewrite wal log to chk file
   if (wal_mgr_->WriteIncompleteWAL(ctx, rewrite) == KStatus::FAIL) {
     LOG_ERROR("Failed to WriteIncompleteWAL.")
     return KStatus::FAIL;
   }

  // 5. trig all vgroup flush
  for (const auto &vgrp: vgroups_) {
//    vgrp.Sync();
  }

  // 6.write EndWAL to chk file
  auto end_chk_log = EndCheckpointEntry::construct(WALLogType::END_CHECKPOINT, 0, 0);
  wal_mgr_->WriteWAL(ctx, end_chk_log, EndCheckpointEntry::fixed_length);

  // 7. a). update checkpoint LSN .
  //    b). trig all vgroup write checkpoint wal.
  //    c). remove vgroup wal file.
  for (const auto &vgrp: vgroups_) {
    vgrp->WriteCheckpointWALAndUpdateLSN(ctx, vgrp_lsn[vgrp->GetVGroupID()]);
  }

  // 8. remove vgroup wal file and old chk file
  wal_mgr_->RemoveChkFile(ctx);

  // after checkpoint, engine wal mgr meta sync?
  return wal_mgr_->CreateCheckpoint(ctx);
}

KStatus TSEngineV2Impl::Recover(kwdbContext_p ctx) {
  //todo recover logic
  /*
   * 1. get engine chk wal log.
   * 2. get all vgroup wal log from last checkpoint lsn.
   * 3. merge wal and apply wal
   */

  // 1. get engine chk wal log.
  std::vector<LogEntry*> logs;
  KStatus s = wal_mgr_->ReadWALLog(logs, 0, wal_mgr_->FetchCurrentLSN());
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLog from chk file while recovering.")
    return KStatus::FAIL;
  }

  // 2. get all vgroup wal log
  for (const auto &vgrp: vgroups_) {
    std::vector<LogEntry *> vlogs;
    TS_LSN lsn;
    if (vgrp->ReadLogFromLastCheckpoint(ctx, vlogs, lsn) == KStatus::FAIL) {
      LOG_ERROR("Failed to ReadWALLogFromLastCheckpoint for vgroup : %d", vgrp->GetVGroupID())
      return KStatus::FAIL;
    }
    logs.insert(logs.end(), vlogs.begin(), vlogs.end());
  }

  // 3. apply redo log
  std::unordered_map<TS_LSN, MTRBeginEntry*> incomplete;
  for (auto wal_log : logs) {
    switch (wal_log->getType()) {
      case WALLogType::MTR_ROLLBACK: {
        auto log = reinterpret_cast<MTREntry*>(wal_log);
        auto x_id = log->getXID();
        if (MtrRollback(ctx, x_id, true) == KStatus::FAIL) return s;
        incomplete.erase(log->getXID());
        break;
      }
      case WALLogType::CHECKPOINT: {
        //todo need write checkpoint wal to every vgroup wal file?
//      KStatus s = wal_manager_->CreateCheckpointWithoutFlush(ctx);
//      if (s == FAIL) return s;
        break;
      }
      case WALLogType::MTR_BEGIN: {
        auto log = reinterpret_cast<MTRBeginEntry*>(wal_log);
        incomplete.insert(std::pair<TS_LSN, MTRBeginEntry*>(log->getXID(), log));
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
      s = tsx_mgr_->MtrCommit(ctx, mtr_id);
      if (s == FAIL) return s;
    } else {
      s = MtrRollback(ctx, mtr_id);
      if (s == FAIL) return s;
    }
  }

  return KStatus::SUCCESS;
}

}  // namespace kwdbts
