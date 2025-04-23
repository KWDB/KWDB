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

#include <libkwdbts2.h>
#include <regex>
#include <limits>
#include <thread>
#include "include/engine.h"
#include "cm_exception.h"
#include "cm_backtrace.h"
#include "cm_fault_injection.h"
#include "cm_task.h"
#include "perf_stat.h"
#include "lru_cache_manager.h"
#include "st_config.h"
#include "sys_utils.h"
#include "ee_mempool.h"
#include "st_tier.h"
#ifndef KWBASE_OSS
#include "ts_config_autonomy.h"
#endif

std::map<std::string, std::string> g_cluster_settings;
DedupRule g_dedup_rule = kwdbts::DedupRule::OVERRIDE;
std::shared_mutex g_settings_mutex;
bool g_engine_initialized = false;
bool g_go_start_service = true;
TSEngine* g_engine_ = nullptr;

std::atomic<bool> g_is_vacuuming{false};
uint64_t g_vacuum_sleep_time = 1000;
std::atomic<bool> g_is_migrating{false};
uint64_t g_duration_level0{30 * 24 * 60 * 60};
uint64_t g_duration_level1{90 * 24 * 60 * 60};

TSStatus TSOpen(TSEngine** engine, TSSlice dir, TSOptions options,
                AppliedRangeIndex* applied_indexes, size_t range_num) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  EngineOptions opts;
  std::string ts_store_path(dir.data, dir.len);
  opts.db_path = ts_store_path + "/tsdb";
  // TODO(rongtianyang): set wal level by cluster setting rather than env val.
  // If cluster setting support Dynamic-Update, cancel this env val.
  char* wal_env = getenv("KW_WAL_LEVEL");
  if (wal_env != nullptr) {
    opts.wal_level = *wal_env - '0';
  } else {
    opts.wal_level = options.wal_level;
  }
  EngineOptions::is_single_node_ = options.is_single_node;
  opts.wal_buffer_size = options.wal_buffer_size;
  opts.wal_file_size = options.wal_file_size;
  opts.wal_file_in_group = options.wal_file_in_group;

  // TODO(LSY): log settings from kwbase start params
  string lg_path = ts_store_path;
  try {
    opts.lg_opts.path = string(options.lg_opts.Dir.data, options.lg_opts.Dir.len);
  } catch (...) {
    cerr << "InitTsServerLog Error! log path is nullptr. using current dir to log\n";
    opts.lg_opts.path = ts_store_path;
  }
  opts.lg_opts.file_max_size = options.lg_opts.LogFileMaxSize;
  opts.lg_opts.level = kLgSeverityMap.find(options.lg_opts.LogFileVerbosityThreshold)->second;
  opts.lg_opts.dir_max_size = options.lg_opts.LogFilesCombinedMaxSize;
  try {
    opts.lg_opts.trace_on_off = string(options.lg_opts.Trace_on_off_list.data, options.lg_opts.Trace_on_off_list.len);
  } catch (...) {
    opts.lg_opts.trace_on_off = "";
  }

  opts.thread_pool_size = options.thread_pool_size;
  opts.task_queue_size = options.task_queue_size;
  opts.buffer_pool_size = options.buffer_pool_size;

  setenv("KW_HOME", ts_store_path.c_str(), 1);

#ifndef K_DO_NOT_SHIP
  char* port_str;
  if (port_str = getenv("KW_ERR_INJECT_PORT")) {
    int port = atoi(port_str);
    if (port > 0) {
      k_int64 server_args[1] = {port};
      s = CreateTask(ctx, &server_args, "InjectFaultServer", "TSOpen", InjectFaultServer);
      if (s == KStatus::FAIL) {
        return ToTsStatus("CreateTask[InjectFaultServer] Internal Error!");
      }
    }
  }
#endif

  // check mksquashfs & unsquashfs
  std::string cmd = "which mksquashfs > /dev/null 2>&1";
  if (!System(cmd, false)) {
    cerr << "mksquashfs is not installed, please install squashfs-tools\n";
    return ToTsStatus("mksquashfs is not installed, please install squashfs-tools");
  }
  cmd = "which unsquashfs > /dev/null 2>&1";
  if (!System(cmd, false)) {
    cerr << "unsquashfs is not installed, please install squashfs-tools\n";
    return ToTsStatus("unsquashfs is not installed, please install squashfs-tools");
  }

  InitCompressInfo(opts.db_path);

  TSEngine* ts_engine;
  s = TSEngineImpl::OpenTSEngine(ctx, ts_store_path, opts, &ts_engine, applied_indexes, range_num);
  if (s == KStatus::FAIL) {
    return ToTsStatus("OpenTSEngine Internal Error!");
  }
  *engine = ts_engine;
  g_engine_ = ts_engine;
  g_engine_initialized = true;
  return kTsSuccess;
}

TSStatus TSCreateTsTable(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups) {
  KWDB_DURATION(StStatistics::Get().create_table);
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  INJECT_DATA_FAULT(FAULT_CONTEXT_INIT_FAIL, s, KStatus::FAIL, nullptr);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;  // Convert according to schema protobuf
  if (!meta.ParseFromArray(schema.data, schema.len)) {
    return ToTsStatus("ParseFromArray Internal Error!");
  }

  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  s = engine->CreateTsTable(ctx_p, table_id, &meta, ranges);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("CreateTsTable Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetMetaData(TSEngine* engine, TSTableID table_id, RangeGroup range, TSSlice* schema) {
  KWDB_DURATION(StStatistics::Get().create_table);
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  INJECT_DATA_FAULT(FAULT_CONTEXT_INIT_FAIL, s, KStatus::FAIL, nullptr);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;  // Convert according to schema protobuf
  s = engine->GetMetaData(ctx_p, table_id, range, &meta);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  string meta_str;
  if (!meta.SerializeToString(&meta_str)) {
    return ToTsStatus("SerializeToArray Internal Error!");
  }
  schema->len = meta_str.size();
  schema->data = static_cast<char*>(malloc(schema->len));
  memcpy(schema->data, meta_str.data(), meta_str.size());
  return kTsSuccess;
}

TSStatus TSGetRangeGroups(TSEngine* engine, TSTableID table_id, RangeGroups *range_groups) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  if (range_groups == nullptr) {
    return ToTsStatus("RangeGroups is nullptr!");
  }
  s = engine->GetRangeGroups(ctx_p, table_id, range_groups);
  return kTsSuccess;
}

TSStatus TSUpdateRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroups range_groups) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  for (int i = 0; i < range_groups.len; ++i) {
    LOG_INFO("table %lu, update rangeGroup %lu to %s", table_id, range_groups.ranges[i].range_group_id,
      range_groups.ranges[i].typ == 0 ? "leader" : "follower");
  }
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  for (int i = 0; i < ranges.size(); i++) {
    s = engine->UpdateRangeGroup(ctx_p, table_id, ranges[i]);
    if (s != KStatus::SUCCESS) {
        LOG_INFO("table %lu, update rangeGroup %lu to %s,error!!!!!!!!!!!!!!!!",
                  table_id, range_groups.ranges[i].range_group_id,
                  range_groups.ranges[i].typ == 0 ? "leader" : "follower");
        // TODO(fyx): tmp ignore no exist RangeGroup Update Error
      return ToTsStatus("UpdateRangeGroup Error!");
    }
  }
  return kTsSuccess;
}

TSStatus TSCreateRangeGroup(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups) {
  KWDB_DURATION(StStatistics::Get().create_group);
  kwdbContext_t context;
  LOG_INFO("table %lu, create rangeGroup %lu to %s", table_id, range_groups.ranges[0].range_group_id,
            range_groups.ranges[0].typ == 0 ? "leader" : "follower")
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;
  // parse from schema protobuf
  if (!meta.ParseFromArray(schema.data, schema.len)) {
    return ToTsStatus("ParseFromArray Internal Error!");
  }
  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  for (int i = 0; i < ranges.size(); i++) {
    s = engine->CreateRangeGroup(ctx_p, table_id, &meta, ranges[i]);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("table %lu, create rangeGroupFailedAAAAAAAAA %lu to %s", table_id, range_groups.ranges[0].range_group_id,
                range_groups.ranges[0].typ == 0 ? "leader" : "follower")
      return ToTsStatus("CreateRangeGroup Error!");
    }
  }
  return kTsSuccess;
}

TSStatus TSIsTsTableExist(TSEngine* engine, TSTableID table_id, bool* find) {
  *find = KFALSE;
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> tags_table;
  s = engine->GetTsTable(ctx_p, table_id, tags_table);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  if (tags_table != nullptr) {
    *find = tags_table->IsExist();
  }
  return kTsSuccess;
}

TSStatus TSDropTsTable(TSEngine* engine, TSTableID table_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DropTsTable(ctx_p, table_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DropTsTable Error!");
  }
  return kTsSuccess;
}

TSStatus TSDropResidualTsTable(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DropResidualTsTable(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DropResidualTsTable Error!");
  }
  return kTsSuccess;
}

TSStatus TSCompressTsTable(TSEngine* engine, TSTableID table_id, timestamp64 ts) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  LOG_INFO("compress table[%lu] start, end_ts: %lu", table_id, ts);
  std::shared_ptr<TsTable> table;
  s = engine->GetTsTable(ctx_p, table_id, table, false);
  if (s != KStatus::SUCCESS) {
    LOG_INFO("The current node does not have the table[%lu], skip compress", table_id);
    return kTsSuccess;
  }
  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  s = table->Compress(ctx_p, ts, compressed_num, err_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("compress table[%lu] failed", table_id);
    return ToTsStatus("CompressTsTable Error!");
  }
  LOG_INFO("compress table[%lu] end, the number of compressed segment is %u", table_id, compressed_num);
  return kTsSuccess;
}

TSStatus TSCompressImmediately(TSEngine* engine, uint64_t goCtxPtr, TSTableID table_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  ctx_p->relation_ctx = goCtxPtr;
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  LOG_INFO("compress table[%lu] start", table_id);
  std::shared_ptr<TsTable> table;
  s = engine->GetTsTable(ctx_p, table_id, table, false);
  if (s != KStatus::SUCCESS) {
    LOG_INFO("The current node does not have the table[%lu], skip compress", table_id);
    return kTsSuccess;
  }
  ErrorInfo err_info;
  uint32_t compressed_num = 0;
  s = table->Compress(ctx_p, INT64_MAX, compressed_num, err_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("compress table[%lu] failed", table_id);
    return ToTsStatus("compress error, reason: " + err_info.errmsg);
  }
  LOG_INFO("compress table[%lu] end, the number of compressed segment is %u", table_id, compressed_num);
  return kTsSuccess;
}

TSStatus TSVacuumTsTable(TSEngine* engine, TSTableID table_id, uint32_t ts_version) {
  bool expected = false;
  if (!g_is_vacuuming.compare_exchange_strong(expected, true)) {
    LOG_INFO("The engine is vacuuming, ignore vacuum request");
    return kTsSuccess;
  }
  Defer defer([&](){ g_is_vacuuming.store(false); });
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  ErrorInfo err_info;
  std::shared_ptr<TsTable> table;
  s = engine->GetTsTable(ctx_p, table_id, table, false, err_info, ts_version);
  if (s != KStatus::SUCCESS) {
    LOG_INFO("The current node does not have the table[%lu], skip vacuum", table_id);
    return kTsSuccess;
  }
  err_info.clear();
  s = table->Vacuum(ctx_p, ts_version, err_info);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("VacuumTsTable Error");
  }
  return kTsSuccess;
}

TSStatus TSMigrateTsTable(TSEngine* engine, TSTableID table_id) {
  if (TsTier::GetInstance().TierEnabled()) {
    bool expected = false;
    if (!g_is_migrating.compare_exchange_strong(expected, true)) {
      LOG_INFO("The engine is migrating tiered storage data, ignore migrate request");
      return kTsSuccess;
    }
    Defer defer([&](){ g_is_migrating.store(false); });
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus s = InitServerKWDBContext(ctx_p);
    if (s != KStatus::SUCCESS) {
      return ToTsStatus("InitServerKWDBContext Error!");
    }
    std::shared_ptr<TsTable> table;
    s = engine->GetTsTable(ctx_p, table_id, table, false);
    if (s != KStatus::SUCCESS) {
      LOG_INFO("The current node does not have the table[%lu], skip migrate", table_id);
      return kTsSuccess;
    }
    s = table->TierMigrate();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("migrate table[%lu] failed", table_id);
      return ToTsStatus("MigrateTsTable Error!");
    }
  }
  return kTsSuccess;
}

TSStatus TSTableAutonomy(TSEngine* engine, TSTableID table_id) {
#ifdef KWBASE_OSS
  return kTsSuccess;
#else
  return TsConfigAutonomy::UpdateTableStatisticInfo(engine, table_id);
#endif
}

TSStatus TSPutEntity(TSEngine* engine, TSTableID table_id, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                     uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->PutEntity(ctx_p, table_id, range_group.range_group_id, payload, payload_num, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("PutEntity Error!");
  }
  return kTsSuccess;
}

TSStatus TSPutData(TSEngine* engine, TSTableID table_id, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                   uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt, DedupResult* dedup_result,
                   bool writeWAL) {
  KWDB_DURATION(StStatistics::Get().ts_put);
  // The CGO calls the interface, and the GO layer code will call this interface to write data
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  // Parsing table_id from payload
  TSTableID tmp_table_id = *reinterpret_cast<uint64_t*>(payload[0].data + Payload::table_id_offset_);
  // hash_point_id_offset_=16 , hash_point_id_size_=2
  // uint16_t hash_point;
  // memcpy(&hash_point, payload[0].data+Payload::hash_point_id_offset_, Payload::hash_point_id_size_);
  // LOG_ERROR("TSPUT DATA HASH POINT = %d", hash_point);
  // Parse range_group_id from payload
  uint64_t tmp_range_group_id = 1;
  s = engine->PutData(ctx_p, tmp_table_id, tmp_range_group_id, payload, payload_num, mtr_id,
                      inc_entity_cnt, inc_unordered_cnt, dedup_result, writeWAL);
  if (s != KStatus::SUCCESS) {
    std::ostringstream ss;
    ss << tmp_range_group_id;
    return ToTsStatus("PutData Error! RangeGroup:" + ss.str());
  }
  return TSStatus{nullptr, 0};
}

TSStatus TSExecQuery(TSEngine* engine, QueryInfo* req, RespInfo* resp, TsFetcher* fetchers, void* fetcher) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  auto *fet = static_cast<VecTsFetcher *>(fetcher);
  if (fet != nullptr && fet->collected) {
    fet->TsFetchers = fetchers;
    ctx_p->fetcher = fetcher;
  }
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Execute(ctx_p, req, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Execute Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetWaitThreadNum(TSEngine* engine, void* resp) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetTsWaitThreadNum(ctx_p, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Get ts wait threads num Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteEntities(TSEngine* engine, TSTableID table_id, TSSlice* primary_tags, size_t primary_tags_num,
                          uint64_t range_group_id, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::vector<string> p_tags;
  for (size_t i = 0; i < primary_tags_num; ++i) {
    p_tags.emplace_back(primary_tags[i].data, primary_tags[i].len);
  }
  s = engine->DeleteEntities(ctx_p, table_id, range_group_id, p_tags, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteEntities Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteRangeData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      HashIdSpan hash_span, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::vector<KwTsSpan> spans(ts_spans.spans, ts_spans.spans + ts_spans.len);
  s = engine->DeleteRangeData(ctx_p, table_id, range_group_id, hash_span, spans, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteRangeData Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      TSSlice primary_tag, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::string p_tag(primary_tag.data, primary_tag.len);
  std::vector<KwTsSpan> spans(ts_spans.spans, ts_spans.spans + ts_spans.len);
  s = engine->DeleteData(ctx_p, table_id, range_group_id, p_tag, spans, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteData Error!");
  }
  return kTsSuccess;
}

TSStatus TSFlushBuffer(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->FlushBuffer(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("FlushBuffer Error!");
  }
  return kTsSuccess;
}

TSStatus TSCreateCheckpoint(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->CreateCheckpoint(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Checkpoint Error!");
  }
  return kTsSuccess;
}

TSStatus TSCreateCheckpointForTable(TSEngine* engine, TSTableID table_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->CreateCheckpointForTable(ctx_p, table_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Checkpoint Error!");
  }
  return kTsSuccess;
}

TSStatus TSMtrBegin(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                    uint64_t range_id, uint64_t index, uint64_t* mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrBegin(ctx_p, table_id, range_group_id, range_id, index, *mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to begin the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSMtrCommit(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrCommit(ctx_p, table_id, range_group_id, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to commit the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSMtrRollback(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrRollback(ctx_p, table_id, range_group_id, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to rollback the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxBegin(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxBegin(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to begin the TS transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxCommit(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxCommit(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to commit the TS transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxRollback(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxRollback(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to rollback the TS transaction!");
  }
  return kTsSuccess;
}

// Parse configuration parameter for hot and cold data tiering duration, such as '1d'.
int parseDuration(const std::string& duration_str) {
  char unit = duration_str.back();
  int value = std::stoi(duration_str.substr(0, duration_str.size() - 1));

  switch (unit) {
    case 'm':
    case 'M':
      return value * 60;
    case 'h':
    case 'H':
      return value * 3600;
    case 'd':
    case 'D':
      return value * 86400;
    default:
      throw std::invalid_argument("Invalid tier duration unit: " + std::string(1, unit));
  }
}

// Parse configuration parameter for hot and cold data tiering duration, such as '30d,90d'.
void parseDurations(const std::string& input) {
  std::vector<std::string> durations;
  std::istringstream ss(input);
  std::string token;

  while (std::getline(ss, token, ',')) {
    durations.push_back(token);
  }
  g_duration_level0 = parseDuration(durations[0]);
  g_duration_level1 = parseDuration(durations[1]);
}

void TriggerSettingCallback(const std::string& key, const std::string& value) {
  if (TRACE_CONFIG_NAME == key) {
    TRACER.SetTraceConfigStr(value);
  } else if ("ts.dedup.rule" == key) {
    if ("override" == value) {
      g_dedup_rule = kwdbts::DedupRule::OVERRIDE;
    } else if ("merge" == value) {
      g_dedup_rule = kwdbts::DedupRule::MERGE;
    } else if ("keep" == value) {
      g_dedup_rule = kwdbts::DedupRule::KEEP;
    } else if ("reject" == value) {
      g_dedup_rule = kwdbts::DedupRule::REJECT;
    } else if ("discard" == value) {
      g_dedup_rule = kwdbts::DedupRule::DISCARD;
    }
  } else if ("ts.mount.max_limit" == key) {
    g_max_mount_cnt_ = atoi(value.c_str());
  } else if ("ts.cached_partitions_per_subgroup.max_limit" == key) {
    g_partition_caches_mgr.SetCapacity(atoi(value.c_str()));
  } else if ("ts.entities_per_subgroup.max_limit" == key) {
    CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP = atoi(value.c_str());
  } else if ("ts.rows_per_block.max_limit" == key) {
    CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = atoi(value.c_str());
  } else if ("ts.blocks_per_segment.max_limit" == key) {
    CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT = atoi(value.c_str());
  } else if ("ts.compress_interval" == key) {
    kwdbts::g_compress_interval = atoi(value.c_str());
  } else if ("ts.compression.type" == key) {
    CompressionType type = kwdbts::CompressionType::GZIP;
    if ("gzip" == value) {
      type = kwdbts::CompressionType::GZIP;
    } else if ("lz4" == value) {
      type = kwdbts::CompressionType::LZ4;
    } else if ("lzma" == value) {
      type = kwdbts::CompressionType::LZMA;
    } else if ("lzo" == value) {
      type = kwdbts::CompressionType::LZO;
    } else if ("xz" == value) {
      type = kwdbts::CompressionType::XZ;
    } else if ("zstd" == value) {
      type = kwdbts::CompressionType::ZSTD;
    }
    if (type != kwdbts::CompressionType::GZIP) {
      if (g_mk_squashfs_option.compressions.find(type) ==
          g_mk_squashfs_option.compressions.end()) {
        LOG_WARN("mksquashfs does not support the %s algorithm and uses gzip by default. "
                 "Please upgrade the mksquashfs version.", value.c_str())
        type = kwdbts::CompressionType::GZIP;
      } else if (g_mount_option.mount_compression_types.find(type) ==
                 g_mount_option.mount_compression_types.end()) {
        LOG_WARN("mount does not support the %s algorithm and uses gzip by default. "
                 "Upgrade to a linux kernel version that supports this algorithm or map /boot:/boot if using docker",
                 value.c_str())
        type = kwdbts::CompressionType::GZIP;
      }
    }
    g_compression = g_mk_squashfs_option.compressions.find(type)->second;
  } else if ("ts.compression.level" == key) {
    kwdbts::CompressionLevel level = kwdbts::CompressionLevel::MIDDLE;
    if ("low" == value) {
      level = kwdbts::CompressionLevel::LOW;
    } else if ("middle" == value) {
      level = kwdbts::CompressionLevel::MIDDLE;
    } else if ("high" == value) {
      level = kwdbts::CompressionLevel::HIGH;
    }
    for (auto& compression : g_mk_squashfs_option.compressions) {
      compression.second.compression_level = level;
    }
    g_compression.compression_level = level;
  } else if ("ts.vacuum_interval" == key) {
    g_vacuum_interval = atoi(value.c_str());
  } else if ("immediate_compression.threads" == key) {
    g_mk_squashfs_option.processors_immediate = atoi(value.c_str());
  } else if ("ts.count.use_statistics.enabled" == key) {
    if ("true" == value) {
      CLUSTER_SETTING_COUNT_USE_STATISTICS = true;
    } else {
      CLUSTER_SETTING_COUNT_USE_STATISTICS = false;
    }
  } else if ("ts.disk_free_space.alert_threshold" == key) {
    g_free_space_alert_threshold = atoll(value.c_str());
  } else if ("ts.table_cache.capacity" == key) {
    EngineOptions::table_cache_capacity_ = atoi(value.c_str());
    if (g_engine_) {
      g_engine_->AlterTableCacheCapacity(EngineOptions::table_cache_capacity_);
    }
  } else if ("ts.tier.duration" == key) {
    parseDurations(value);
  } else if ("ts.auto_vacuum.sleep" == key) {
    g_vacuum_sleep_time = atoll(value.c_str());
  }
#ifndef KWBASE_OSS
  else if ("ts.storage.autonomy.mode" == key) {  // NOLINT
    if ("auto" == value) {
      CLUSTER_SETTING_STORAGE_AUTONOMY_ENABLE = true;
    } else if ("manual" == value) {
      CLUSTER_SETTING_STORAGE_AUTONOMY_ENABLE = false;
    }
  } else if ("ts.entities_per_subgroup.growth" == key) {
    CLUSTER_SETTING_ENTITIES_PER_SUBGROUP_GROWTH = atof(value.c_str());
  }
#endif
  else {  // NOLINT
    LOG_INFO("Cluster setting %s has no callback function.", key.c_str());
  }
}

void TSSetClusterSetting(TSSlice key, TSSlice value) {
  std::string key_set;
  std::string value_set;

  try {
    key_set = string(key.data, key.len);
  } catch (...) {
    LOG_ERROR("cluster setting get key %s failed!", key.data);
    return;
  }

  try {
    value_set = string(value.data, value.len);
  } catch (...) {
    LOG_ERROR("cluster setting %s get value %s failed!", key.data, value.data);
    return;
  }

  // callback
  TriggerSettingCallback(key_set, value_set);

  // save cluster setting to map
  std::shared_lock<std::shared_mutex> rlock(g_settings_mutex);
  std::map<std::string, std::string>::iterator iter = g_cluster_settings.find(key_set);
  if (iter == g_cluster_settings.end()) {
      rlock.unlock();
      std::map<std::string, std::string>::value_type value(key_set, value_set);
      std::unique_lock<std::shared_mutex> wlock(g_settings_mutex);
      g_cluster_settings.insert(value);
      wlock.unlock();
  } else {
    iter->second = value_set;
  }
  return;
}

TSStatus TSDeleteExpiredData(TSEngine* engine, TSTableID table_id, KTimestamp end_ts) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb, false);
  if (s != KStatus::SUCCESS) {
    LOG_INFO("The current node does not have the table[%lu], skip delete expired data", table_id);
    return kTsSuccess;
  }
  LOG_INFO("table[%lu] delete expired data start, expired data end time[%ld]", table_id, end_ts);
  // May be data that has expired but not deleted, so we don't care about start,
  // just delete all data older than the end timestamp.
  s = ts_tb->DeleteExpiredData(ctx_p, end_ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] delete expired data failed", table_id);
    return ToTsStatus("TsTable delete expired data Error!");
  }
  LOG_INFO("table[%lu] delete expired data succeeded", table_id);
  return kTsSuccess;
}

TSStatus TSGetAvgTableRowSize(TSEngine* engine, TSTableID table_id, uint64_t* row_size) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  s = ts_tb->GetAvgTableRowSize(ctx_p, row_size);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] getdatavoluem failed", table_id);
    return ToTsStatus("TsTable getdatavolume Error!");
  }
  return kTsSuccess;
}

// Query the total amount of data within the range (an approximate value is sufficient)
TSStatus TSGetDataVolume(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                        KwTsSpan ts_span, uint64_t* volume) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  s = ts_tb->GetDataVolume(ctx_p, begin_hash, end_hash, ts_span, volume);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] getdatavoluem failed", table_id);
    return ToTsStatus("TsTable getdatavolume Error!");
  }
  if (begin_hash > end_hash) {
    return ToTsStatus("begin hash larger than end hash.");
  }
  *volume = 0;
  if (begin_hash < end_hash) {
    uint64_t scan_all_begin_hash = begin_hash + 1;
    uint64_t scan_all_end_hash = end_hash - 1;
    if (ts_span.begin == INT64_MIN) {
      scan_all_begin_hash = begin_hash;
    }
    if (ts_span.end == INT64_MAX) {
      scan_all_end_hash = end_hash;
    }
    if (scan_all_begin_hash > begin_hash) {
      uint64_t scan_part_volume = 0;
      s = ts_tb->GetDataVolume(ctx_p, begin_hash, begin_hash, {ts_span.begin, INT64_MAX}, &scan_part_volume);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("table[%lu] GetDataVolume failed", table_id);
        return ToTsStatus("TsTable getdatavolume Error!");
      }
      *volume += scan_part_volume;
    }
    if (scan_all_end_hash >= scan_all_begin_hash) {
      uint64_t scan_all_volume = 0;
      s = ts_tb->GetDataVolume(ctx_p, begin_hash, end_hash, {INT64_MIN, INT64_MAX}, &scan_all_volume);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("table[%lu] GetDataVolume failed", table_id);
        return ToTsStatus("TsTable getdatavolume Error!");
      }
      *volume += scan_all_volume;
    }
    if (scan_all_end_hash < end_hash) {
      uint64_t scan_part_volume = 0;
      s = ts_tb->GetDataVolume(ctx_p, begin_hash, begin_hash, {INT64_MIN, ts_span.end}, &scan_part_volume);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("table[%lu] GetDataVolume failed", table_id);
        return ToTsStatus("TsTable getdatavolume Error!");
      }
      *volume += scan_part_volume;
    }
  } else {
    s = ts_tb->GetDataVolume(ctx_p, begin_hash, end_hash, ts_span, volume);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("table[%lu] getdatavoluem failed", table_id);
      return ToTsStatus("TsTable getdatavolume Error!");
    }
  }
  LOG_DEBUG("TSGetDataVolume range{%lu/%ld - %lu/%ld}, total volumne %lu",
              begin_hash, ts_span.begin, end_hash, ts_span.end, *volume);
  return kTsSuccess;
}

// The timestamp when querying half of the total data within the range (an approximate value is sufficient)
TSStatus TSGetDataVolumeHalfTS(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                               KwTsSpan ts_span, int64_t* half_ts) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  s = ts_tb->GetDataVolumeHalfTS(ctx_p, begin_hash, end_hash, ts_span, half_ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] GetDataVolumeHalfTS failed", table_id);
    return ToTsStatus("GetDataVolumeHalfTS Error!");
  }
  return kTsSuccess;
}

// Input data in Payload format based online storage mode
TSStatus TSPutDataByRowType(TSEngine* engine, TSTableID table_id, TSSlice* payload_row, size_t payload_num,
                            RangeGroup range_group, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                            uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  // input parameter table_id is not correct, not use this parameter anymore.
  // Parsing table_id from payload
  TSTableID tmp_table_id = *reinterpret_cast<uint64_t*>(payload_row[0].data + Payload::table_id_offset_);
  // Parse range_group_id from payload
  uint64_t tmp_range_group_id = *reinterpret_cast<uint16_t*>(payload_row[0].data + Payload::hash_point_id_offset_);

  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, tmp_table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  TSSlice payload;
  for (size_t i = 0; i < payload_num; i++) {
    s = ts_tb->ConvertRowTypePayload(ctx_p, payload_row[i], &payload);
    if (s != KStatus::SUCCESS) {
      uint32_t pl_version = Payload::GetTsVsersionFromPayload(&payload_row[i]);
      if (ts_tb->CheckAndAddSchemaVersion(ctx_p, tmp_table_id, pl_version) != KStatus::SUCCESS) {
        LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed", tmp_table_id);
        return ToTsStatus("CheckAndAddSchemaVersion Error!");
      }
      if (ts_tb->ConvertRowTypePayload(ctx_p, payload_row[i], &payload) != KStatus::SUCCESS) {
        LOG_ERROR("table[%lu] ConvertRowTypePayload failed", tmp_table_id);
        return ToTsStatus("ConvertRowTypePayload Error!");
      }
    }
    // todo(liangbo01) current interface dedup result no support multi-payload insert.
    s = engine->PutData(ctx_p, tmp_table_id, tmp_range_group_id, &payload, payload_num, mtr_id,
                        inc_entity_cnt, inc_unordered_cnt, dedup_result, writeWAL);
    if (s != KStatus::SUCCESS) {
      free(payload.data);
      std::ostringstream ss;
      ss << tmp_range_group_id;
      return ToTsStatus("PutData Error!,RangeGroup:" + ss.str());
    }
    free(payload.data);
  }
  return kTsSuccess;
}

TSStatus TsTestGetAndAddSchemaVersion(TSEngine* engine, TSTableID table_id, uint64_t version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }

  if (ts_tb->CheckAndAddSchemaVersion(ctx_p, table_id, version) != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] CheckAndAddSchemaVersion failed", table_id);
    return ToTsStatus("CheckAndAddSchemaVersion Error!");
  }

  return kTsSuccess;
}

TSStatus TsDeleteTotalRange(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                            KwTsSpan ts_span, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> table;
  s = engine->GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TsDeleteTotalRange failed: GetTsTable failed, table id [%lu]", table_id)
    return ToTsStatus("get tstable Error!");
  }
  s = table->DeleteTotalRange(ctx, begin_hash, end_hash, ts_span, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteRangeData Error!");
  }
  // no need drop table, disttributed level will call drop table interface.
  return kTsSuccess;
}

// Create a snapshot object to read local data
TSStatus TSCreateSnapshotForRead(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                                 KwTsSpan ts_span, uint64_t* snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->CreateSnapshotForRead(ctx_p, table_id, begin_hash, end_hash, ts_span, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("CreateSnapshot Error!");
  }
  return kTsSuccess;
}

// Return the data that needs to be transmitted this time. If the data is 0, it means that all data has been queried
TSStatus TSGetSnapshotNextBatchData(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id, TSSlice* data) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->GetSnapshotNextBatchData(ctx_p, snapshot_id, data);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetSnapshotData Error!");
  }
  return kTsSuccess;
}

// Create an object to receive data at the dest node
TSStatus TSCreateSnapshotForWrite(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                                  KwTsSpan ts_span, uint64_t* snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->CreateSnapshotForWrite(ctx_p, table_id, begin_hash, end_hash, ts_span, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitSnapshot Error!");
  }
  return kTsSuccess;
}

// dest node, after receiving data, writes the data to storage
TSStatus TSWriteSnapshotBatchData(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id, TSSlice data) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->WriteSnapshotBatchData(ctx_p, snapshot_id, data);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("WriteSnapshotBatchData Error!");
  }
  return kTsSuccess;
}

// All writes completed, this snapshot is successful, call this function
TSStatus TSWriteSnapshotSuccess(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->WriteSnapshotSuccess(ctx_p, snapshot_id);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("WriteSnapshotBatchData Error!");
  }
  return kTsSuccess;
}

// The snapshot failed, or in other scenarios, the data written this time needs to be rolled back
TSStatus TSWriteSnapshotRollback(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->WriteSnapshotRollback(ctx_p, snapshot_id);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("WriteSnapshotBatchData Error!");
  }
  return kTsSuccess;
}

// Delete snapshot object
TSStatus TSDeleteSnapshot(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DeleteSnapshot(ctx_p, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DropSnapshot Error!");
  }
  return kTsSuccess;
}

TSStatus TSClose(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  LOG_INFO("TSClose")
  engine->CreateCheckpoint(ctx);
  delete engine;
  return TSStatus{nullptr, 0};
}

void TSFree(void* ptr) {
  free(ptr);
}

void TSRegisterExceptionHandler(char *dir) {
  kwdbts::RegisterExceptionHandler(dir);
  kwdbts::RegisterBacktraceSignalHandler();
}

TSStatus TSAddColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                     uint32_t cur_version, uint32_t new_version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error");
  }

  string err_msg;
  s = engine->AddColumn(ctx_p, table_id, transaction_id, column, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    if (err_msg.empty()) {
      err_msg = "unknown error";
    }
    return ToTsStatus(err_msg);
  }

  return kTsSuccess;
}

TSStatus TSDropColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                      uint32_t cur_version, uint32_t new_version) {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus s = InitServerKWDBContext(ctx_p);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("InitServerKWDBContext Error");
    }
    string err_msg;
    s = engine->DropColumn(ctx_p, table_id, transaction_id, column, cur_version, new_version, err_msg);
    if (s != KStatus::SUCCESS) {
      if (err_msg.empty()) {
        err_msg = "unknown error";
      }
      return ToTsStatus(err_msg);
    }

    return kTsSuccess;
}

TSStatus TSAlterColumnType(TSEngine* engine, TSTableID table_id, char* transaction_id,
                           TSSlice new_column, TSSlice origin_column,
                           uint32_t cur_version, uint32_t new_version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error");
  }

  string err_msg;
  s = engine->AlterColumnType(ctx_p, table_id, transaction_id, new_column, origin_column,
                              cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    if (err_msg.empty()) {
      err_msg = "unknown error";
    }
    return ToTsStatus(err_msg);
  }

    return kTsSuccess;
}

TSStatus TSCreateNormalTagIndex(TSEngine* engine, TSTableID table_id, uint64_t index_id, char* transaction_id,
                                uint32_t cur_version, uint32_t new_version, IndexColumns index_columns) {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus s = InitServerKWDBContext(ctx_p);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("InitServerKWDBContext Error!");
    }
    std::vector<uint32_t> columns(index_columns.index_column, index_columns.index_column + index_columns.len);
    s = engine->CreateNormalTagIndex(ctx_p, table_id, index_id, transaction_id, cur_version, new_version, columns);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("CreateNormalTagIndex Error!");
    }
    return kTsSuccess;
}

TSStatus TSDropNormalTagIndex(TSEngine* engine, TSTableID table_id, uint64_t index_id, char* transaction_id,
                              uint32_t cur_version, uint32_t new_version) {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus s = InitServerKWDBContext(ctx_p);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("InitServerKWDBContext Error!");
    }
    s = engine->DropNormalTagIndex(ctx_p, table_id, index_id, transaction_id, cur_version, new_version);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("TSDropNormalTagIndex Error!");
    }
    return kTsSuccess;
}


TSStatus TSAlterPartitionInterval(TSEngine* engine, TSTableID table_id, uint64_t partition_interval) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->AlterPartitionInterval(ctx_p, table_id, partition_interval);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("AlterPartitionInterval Error!");
  }
  return kTsSuccess;
}

TSStatus TSDeleteRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroup range) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DeleteRangeGroup(ctx_p, table_id, range);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("DeleteRangeGroup Error!");
  }
  return kTsSuccess;
}

bool TSDumpAllThreadBacktrace(char* folder, char* now_time_stamp) {
  return kwdbts::DumpAllThreadBacktrace(folder, now_time_stamp);
}

TSStatus TsGetTableVersion(TSEngine* engine, TSTableID table_id, uint32_t* version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetTableVersion(ctx_p, table_id, version);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTableVersion Error!");
  }
  return kTsSuccess;
}

void TsMemPoolFree(void *data) {
  kwdbts::EE_MemPoolFree(g_pstBufferPoolInfo, static_cast<k_char*>(data));
}

char* TsGetStringPtr(void *data, uint32_t offset,  uint16_t *len) {
  char *ptr = static_cast<char*>(data) + offset;
  *len = 0;
  memcpy(len, ptr, sizeof(uint16_t));
  return ptr + sizeof(uint16_t);
}

TSStatus TsGetWalLevel(TSEngine* engine, uint8_t *wal_level) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetWalLevel(ctx_p, wal_level);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetWalLevel Error!");
  }
  return kTsSuccess;
}

TSStatus TSCountTsTable(TSEngine* engine, TSTableID table_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  LOG_DEBUG("count table[%lu] start", table_id);
  std::shared_ptr<TsTable> table;
  s = engine->GetTsTable(ctx_p, table_id, table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("The current node does not have the table[%lu], skip count", table_id);
    return kTsSuccess;
  }
  ErrorInfo err_info;
  s = table->Count(ctx_p, err_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("count table[%lu] failed", table_id);
    return ToTsStatus("CountTsTable Error!");
  }
  LOG_DEBUG("count table[%lu] end", table_id);
  return kTsSuccess;
}
