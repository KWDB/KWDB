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

#include "ts_vgroup.h"
#include <pthread.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <algorithm>
#include <numeric>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>
#include <regex>
#include <shared_mutex>
#include <list>
#include <string>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_block.h"
#include "ts_bufferbuilder.h"
#include "ts_common.h"
#include "ts_compatibility.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_builder.h"
#include "ts_filename.h"
#include "ts_flush_manager.h"
#include "ts_io.h"
#include "ts_iterator_v2_impl.h"
#include "ts_lastsegment_builder.h"
#include "ts_mem_segment_mgr.h"
#include "ts_partition_count_mgr.h"
#include "ts_segment.h"
#include "ts_std_utils.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"
#include "ts_partition_interval_recorder.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {

// todo(liangbo01) using normal path for mem_segment.
TsVGroup::TsVGroup(EngineOptions* engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
                   std::shared_mutex* engine_mutex, TsHashRWLatch* tag_lock, bool enable_compact_thread)
    : vgroup_id_(vgroup_id),
      schema_mgr_(schema_mgr),
      tag_lock_(tag_lock),
      path_(fs::path(engine_options->db_path) / VGroupDirName(vgroup_id)),
      max_entity_id_(0),
      engine_options_(engine_options),
      engine_wal_level_mutex_(engine_mutex),
      version_manager_(std::make_unique<TsVersionManager>(&TsIOEnv::GetInstance(), path_)),
      mem_segment_mgr_(std::make_unique<TsMemSegmentManager>(this, version_manager_.get())),
      enable_compact_thread_(enable_compact_thread) {}

TsVGroup::TsVGroup(EngineOptions* engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
                   std::shared_mutex* engine_mutex, TsHashRWLatch* tag_lock,  const std::string& user_defined_path,
                   bool enable_compact_thread)
    : vgroup_id_(vgroup_id),
      schema_mgr_(schema_mgr),
      tag_lock_(tag_lock),
      path_(fs::path(engine_options->db_path) / VGroupDirName(vgroup_id)),
      max_entity_id_(0),
      engine_options_(engine_options),
      engine_wal_level_mutex_(engine_mutex),
      version_manager_(std::make_unique<TsVersionManager>(&TsIOEnv::GetInstance(), path_)),
      mem_segment_mgr_(std::make_unique<TsMemSegmentManager>(this, version_manager_.get())),
      enable_compact_thread_(enable_compact_thread) {
  if (!user_defined_path.empty()) {
    user_defined_path_ = fs::path(user_defined_path) / VGroupDirName(vgroup_id);
  }
}

TsVGroup::~TsVGroup() {
  enable_compact_thread_ = false;
  closeCompactThread();
  enable_recalc_count_thread_ = false;
  closeRecalcCountThread();
  for (auto it : entity_latest_row_) {
    if (it.second.last_payload.data != nullptr) {
      free(it.second.last_payload.data);
      it.second.last_payload.data = nullptr;
    }
  }
  cur_mem_size_ = 0;
}

KStatus TsVGroup::Init(kwdbContext_p ctx) {
  KStatus s;
  if (user_defined_path_.empty()) {
    s = TsIOEnv::GetInstance().NewDirectory(path_);
  } else {
    s = TsIOEnv::GetInstance().NewDirectory(user_defined_path_);
    bool ok = CreateDirSymLink(user_defined_path_, path_);
    if (!ok) {
      LOG_ERROR("Init VGroup %u failed, please check user defined vgroup path", vgroup_id_);
      return FAIL;
    }
  }
  if (s == FAIL) {
    LOG_ERROR("Failed to create directory: %s", path_.c_str());
    return s;
  }

  s = version_manager_->Recover();
  if (s == FAIL) {
    LOG_ERROR("recover vgroup version failed, path: %s", path_.c_str());
    return s;
  }

  auto version = version_manager_->Current();
  auto partitions = version->GetAllPartitions();
  bool need_recalc_count = false;
  for (auto& [par_id, partition] : partitions) {
    if (!partition->GetCountManager() && (partition->GetAllLastSegments().size() > 0 || partition->GetEntitySegment())) {
      need_recalc_count = true;
      break;
    }
  }
  if (need_recalc_count) {
    s = ResetCountStat();
    LOG_ERROR("vgroup [%u] recover recalculate count stat failed.", vgroup_id_);
  }

  initCompactThread();
  initRecalcCountThread();

  if (user_defined_path_.empty()) {
    wal_manager_ = std::make_unique<WALMgr>(engine_options_->db_path, VGroupDirName(vgroup_id_), engine_options_);
  } else {
    wal_manager_ = std::make_unique<WALMgr>(engine_options_->db_path, VGroupDirName(vgroup_id_),
      engine_options_, user_defined_path_);
  }
  tsx_manager_ = std::make_unique<TSxMgr>(wal_manager_.get());
  auto res = wal_manager_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }
  UpdateAtomicOSN();

  auto mem = mem_segment_mgr_->CurrentMemSegment();
  TsVersionUpdate update;
  update.AddMemSegment(std::move(mem));
  version_manager_->ApplyUpdate(&update);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  // no need do anything.
  return KStatus::SUCCESS;
}

KStatus TsVGroup::PutData(kwdbContext_p ctx, const std::shared_ptr<TsTableSchemaManager>& tb_schema,
                          uint64_t mtr_id, TSSlice* primary_tag, TSEntityID entity_id,
                          TSSlice* payload, bool write_wal) {
  if (EnableWAL() && write_wal) {
    LockSharedLevelMutex();
    TS_OSN entry_lsn = 0;
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mtr_id, 0, 0, *primary_tag, *payload, entry_lsn, vgroup_id_);
    UnLockSharedLevelMutex();
    if (s == KStatus::FAIL) {
      wal_manager_->Unlock();
      return s;
    }
    // unlock current lsn
    wal_manager_->Unlock();
  }

  auto s = mem_segment_mgr_->PutData(*payload, tb_schema, entity_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("mem_segment_mgr_.PutData Failed.")
    return FAIL;
  }
    // update vgroup entity_id.
  return KStatus::SUCCESS;
}

fs::path TsVGroup::GetPath() const { return path_; }

TSEntityID TsVGroup::AllocateEntityID() {
  std::lock_guard<std::mutex> lock1(entity_id_mutex_);
  std::unique_lock<std::shared_mutex> lock2(entity_latest_row_mutex_);
  uint64_t max_entity_id = ++max_entity_id_;
  entity_latest_row_[max_entity_id].status = TsEntityLatestRowStatus::Uninitialized;
  return max_entity_id;
}

TSEntityID TsVGroup::GetMaxEntityID() const {
  std::lock_guard<std::mutex> lock(entity_id_mutex_);
  return max_entity_id_;
}

void TsVGroup::InitEntityID(TSEntityID entity_id) {
  std::lock_guard<std::mutex> lock1(entity_id_mutex_);
  std::unique_lock<shared_mutex> lock2(entity_latest_row_mutex_);
  max_entity_id_ = entity_id;
  for (int i = 1; i <= max_entity_id_; ++i) {
    entity_latest_row_[i].status = TsEntityLatestRowStatus::Recovering;
  }
}

KStatus TsVGroup::RemoveChkFile(kwdbContext_p ctx) {
  wal_manager_->Lock();
  Defer defer{[&]() {
    wal_manager_->Unlock();
  }};
  // remove chk file
  KStatus s = wal_manager_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to RemoveChkFile.")
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_OSN& last_lsn,
                                               std::vector<uint64_t> uncommitted_xid) {
  // 1. read chk wal log
  // 2. switch new file
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;
  TS_OSN first_lsn = wal_manager_->GetFirstLSN();
  last_lsn = wal_manager_->FetchCurrentLSN();
  auto next_first_lsn = last_lsn;
  WALMeta meta = wal_manager_->GetMeta();
  KStatus s = wal_manager_->SwitchNextFile(next_first_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to switch next WAL file.")
    return s;
  }
  wal_manager_->Unlock();

  // new tmp wal mgr to read chk wal file
  WALMgr tmp_wal = WALMgr(engine_options_->db_path, VGroupDirName(vgroup_id_), engine_options_, true);
  tmp_wal.InitForChk(ctx, meta);
  s = tmp_wal.ReadUncommittedWALLog(logs, first_lsn, last_lsn, ignore, uncommitted_xid);
  if (s == FAIL) {
    LOG_ERROR("Failed to ReadUncommittedWALLog");
    return FAIL;
  }
  return s;
}

KStatus TsVGroup::ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_OSN& last_lsn) {
  // 1. read chk wal log
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;

  // TODO(xy): code review here, last_lsn is not used, should we remove it?
  // TS_OSN chk_lsn = wal_manager_->FetchCheckpointLSN();
  // if (last_lsn != 0) {
  //   chk_lsn = last_lsn;
  // }

  KStatus s =
      wal_manager_->ReadWALLog(logs, wal_manager_->GetFirstLSN(), wal_manager_->FetchCurrentLSN(), ignore);
  last_lsn = wal_manager_->FetchCurrentLSN();
  wal_manager_->Unlock();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLog.")
  }
  return s;
}

KStatus TsVGroup::ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs) {
  std::vector<uint64_t> ignore;
  return wal_manager_->ReadWALLogForMtr(mtr_trans_id, logs, ignore);
}

TsEngineSchemaManager* TsVGroup::GetSchemaMgr() const {
  return schema_mgr_;
}

// todo(liangbo01) create partition should at data inserting wal.
// if at here, inserting and deleting at same time. maybe sql exec over, data all exist.
// but restart service, data all disappeared.
// solution: 1. delete item not store in partition. 2. creating partition before wal insert.

// KStatus TsVGroup::makeSurePartitionExist(TSTableID table_id, const std::list<TSMemSegRowData>& rows) {
//   std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
//   auto s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
//   if (s != KStatus::SUCCESS) {
//     LOG_ERROR("GetTableSchemaMgr failed. table[%lu]", table_id);
//     return s;
//   }
//   auto ts_type = tb_schema_mgr->GetTsColDataType();
//   for (auto& row : rows) {
//     auto p_time = convertTsToPTime(row.ts, ts_type);
//     version_manager_->AddPartition(row.database_id, p_time);
//   }
//   return KStatus::SUCCESS;
// }

KStatus TsVGroup::redoPut(kwdbContext_p ctx, kwdbts::TS_OSN log_lsn, const TSSlice& payload, uint64_t osn) {
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaManager failed, table id: %lu", table_id);
    return s;
  }

  s = schema_mgr_->GetVGroup(ctx, tb_schema_manager, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint8_t payload_data_flag = p.GetRowType();
  if (new_tag) {
    vgroup_id = GetVGroupID();
    entity_id = AllocateEntityID();
    // 1. Write tag data
    assert(payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY);
    LOG_DEBUG("tag bt insert hashPoint=%hu", p.GetHashPoint());
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id, osn, OperateType::Insert);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  } else {
    assert(vgroup_id == this->GetVGroupID());
  }

  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::DATA_ONLY) {
    s = mem_segment_mgr_->PutData(payload, tb_schema_manager, entity_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("failed putdata.");
      return s;
    }
  }
  return s;
}

KStatus TsVGroup::GetLastRowEntity(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                   pair<timestamp64, uint32_t>& last_row_entity) {
  KTableKey table_id = table_schema_mgr->GetTableId();
  {
    std::shared_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    if (last_row_entity_.count(table_id)) {
      last_row_entity = last_row_entity_[table_id];
      return KStatus::SUCCESS;
    }
  }
  uint32_t db_id = table_schema_mgr->GetDbID();
  DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
  auto current = CurrentVersion();
  auto ts_partitions = current->GetPartitions(db_id, {{INT64_MIN, INT64_MAX}}, ts_col_type);
  if (ts_partitions.empty()) {
    std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    last_row_entity = {INT64_MIN, 0};
    return KStatus::SUCCESS;
  }
  bool last_row_found = false;
  // TODO(liumengzhen) : set correct lsn
  TS_OSN scan_lsn = UINT64_MAX;
  std::vector<KwTsSpan> ts_spans = {{INT64_MIN, INT64_MAX}};
  TsScanFilterParams filter{db_id, table_id, vgroup_id_, 0, ts_col_type,
                            scan_lsn, ts_spans};

  std::shared_ptr<TagTable> tag_schema;
  table_schema_mgr->GetTagSchema(ctx, &tag_schema);
  std::vector<uint32_t> entity_id_list;
  tag_schema->GetEntityIdListByVGroupId(vgroup_id_, entity_id_list);
  std::shared_ptr<MMapMetricsTable> metric_schema = table_schema_mgr->GetCurrentMetricsTable();
  for (int i = ts_partitions.size() - 1; !last_row_found && i >= 0; --i) {
    std::shared_ptr<TsBlockSpan> last_block_span = nullptr;
    for (auto &entity_id : entity_id_list) {
      std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans;
      filter.entity_id_ = entity_id;
      KStatus ret = ts_partitions[i]->GetBlockSpans(filter, &ts_block_spans, table_schema_mgr,
                                                   metric_schema);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpan failed.");
        return KStatus::FAIL;
      }
      if (ts_block_spans.empty()) continue;

      timestamp64 last_ts = INT64_MIN;
      std::shared_ptr<TsBlockSpan> last_ts_block_span = nullptr;
      while (!ts_block_spans.empty()) {
        auto block_span = ts_block_spans.front();
        ts_block_spans.pop_front();
        timestamp64 cur_ts = block_span->GetLastTS();
        if (cur_ts > last_ts) {
          last_ts = cur_ts;
          last_ts_block_span.swap(block_span);
        }
      }

      if (last_block_span == nullptr || last_block_span->GetLastTS() < last_ts) {
        last_block_span.swap(last_ts_block_span);
      }
    }
    if (last_block_span == nullptr) continue;
    last_row_entity = {last_block_span->GetLastTS(), last_block_span->GetEntityID()};
    last_row_found = true;
  }
  std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
  if (!last_row_entity_.count(table_id) || last_row_entity.first >= last_row_entity_[table_id].first) {
    last_row_entity_[table_id] = last_row_entity;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetEntityLastRow(std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                   uint32_t entity_id, const std::vector<KwTsSpan>& ts_spans,
                                   timestamp64& entity_last_ts) {
  entity_last_ts = INVALID_TS;
  std::shared_lock<std::shared_mutex> lock1(entity_latest_row_mutex_);
  TsTableLastRow& last_row = entity_latest_row_[entity_id];
  if (last_row.status == TsEntityLatestRowStatus::Uninitialized) {
    return KStatus::SUCCESS;
  }
  if (last_row.status == TsEntityLatestRowStatus::Valid) {
    if (checkTimestampWithSpans(ts_spans, last_row.last_ts, last_row.last_ts) != TimestampCheckResult::NonOverlapping) {
      entity_last_ts = last_row.last_ts;
    }
    return KStatus::SUCCESS;
  }
  lock1.unlock();

  uint32_t db_id = table_schema_mgr->GetDbID();
  KTableKey table_id = table_schema_mgr->GetTableId();
  DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
  auto current = CurrentVersion();
  auto ts_partitions = current->GetPartitions(db_id, {{INT64_MIN, INT64_MAX}}, ts_col_type);
  if (ts_partitions.empty()) {
    std::unique_lock<std::shared_mutex> lock2(entity_latest_row_mutex_);
    last_row.status = TsEntityLatestRowStatus::Valid;
    last_row.last_ts = INT64_MIN;
    return KStatus::SUCCESS;
  }

  // TODO(liumengzhen) : set correct lsn
  TS_OSN scan_lsn = UINT64_MAX;
  std::vector<KwTsSpan> spans = {{INT64_MIN, INT64_MAX}};
  TsScanFilterParams filter{db_id, table_id, vgroup_id_, entity_id, ts_col_type, scan_lsn, spans};
  std::shared_ptr<TsBlockSpan> last_block_span = nullptr;
  std::shared_ptr<MMapMetricsTable> metric_schema = table_schema_mgr->GetCurrentMetricsTable();
  for (int i = ts_partitions.size() - 1; i >= 0; --i) {
    std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans;
    KStatus ret = ts_partitions[i]->GetBlockSpans(filter, &ts_block_spans, table_schema_mgr,
                                                 metric_schema);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpan failed.");
      return KStatus::FAIL;
    }
    if (ts_block_spans.empty()) continue;

    timestamp64 max_ts = INT64_MIN;
    while (!ts_block_spans.empty()) {
      auto block_span = ts_block_spans.front();
      ts_block_spans.pop_front();
      timestamp64 cur_ts = block_span->GetLastTS();
      if (cur_ts > max_ts) {
        max_ts = cur_ts;
        last_block_span.swap(block_span);
      }
    }
    break;
  }
  std::unique_lock<std::shared_mutex> lock3(entity_latest_row_mutex_);
  if (last_block_span != nullptr) {
    if (!entity_latest_row_.count(entity_id) || last_block_span->GetLastTS() >= last_row.last_ts) {
      last_row.is_payload_valid = false;
      last_row.last_ts = last_block_span->GetLastTS();
    }
    if (checkTimestampWithSpans(ts_spans, last_row.last_ts, last_row.last_ts) != TimestampCheckResult::NonOverlapping) {
      entity_last_ts = last_row.last_ts;
    }
  }
  last_row.status = TsEntityLatestRowStatus::Valid;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::ConvertBlockSpanToResultSet(const std::vector<k_uint32>& kw_scan_cols,
                                              const TsBlockSpan& ts_blk_span,
                                              const std::vector<AttributeInfo>& attrs,
                                              ResultSet* res) {
  KStatus ret;
  for (int i = 0; i < kw_scan_cols.size(); ++i) {
    Batch* batch;
    auto kw_col_idx = kw_scan_cols[i];
    if (!ts_blk_span.IsColExist(kw_col_idx)) {
      batch = new AggBatch(nullptr, 0);
    } else {
      if (!ts_blk_span.IsVarLenType(kw_col_idx)) {
        char* value;
        std::unique_ptr<TsBitmapBase> bitmap;
        ret = ts_blk_span.GetFixLenColAddr(kw_col_idx, &value, &bitmap);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return ret;
        }
        if (!attrs[kw_scan_cols[i]].isFlag(AINFO_NOT_NULL) && bitmap->At(0) != DataFlags::kValid) {
          batch = new AggBatch(nullptr, 0);
        } else {
          char* buffer = static_cast<char*>(malloc(attrs[kw_col_idx].size));
          memcpy(buffer, value, attrs[kw_col_idx].size);
          batch = new AggBatch(static_cast<void*>(buffer), 1);
          batch->is_new = true;
        }
      } else {
        TSSlice var_data;
        DataFlags var_bitmap;
        ret = ts_blk_span.GetVarLenTypeColAddr(0, kw_col_idx, var_bitmap, var_data);
        if (var_bitmap != DataFlags::kValid) {
          batch = new AggBatch(nullptr, 0);
        } else {
          char* buffer = static_cast<char*>(malloc(var_data.len + kStringLenLen));
          KUint16(buffer) = var_data.len;
          memcpy(buffer + kStringLenLen, var_data.data, var_data.len);
          std::shared_ptr<void> ptr(buffer, free);
          batch = new AggBatch(ptr, 1);
        }
      }
    }
    res->push_back(i, batch);
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetEntityLastRowBatch(uint32_t entity_id, uint32_t scan_version,
                                        std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                        std::shared_ptr<MMapMetricsTable>& schema,
                                        std::shared_ptr<TsRawPayloadRowParser>& parser,
                                        const std::vector<KwTsSpan>& ts_spans, const std::vector<k_uint32>& scan_cols,
                                        timestamp64& entity_last_ts, bool& last_payload_valid, ResultSet* res) {
  std::shared_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
  if (!entity_latest_row_.count(entity_id)
      || entity_latest_row_[entity_id].status == TsEntityLatestRowStatus::Recovering
      || !entity_latest_row_.count(entity_id) || !entity_latest_row_[entity_id].is_payload_valid) {
    return KStatus::SUCCESS;
  }
  last_payload_valid = true;
  TsTableLastRow& last_row = entity_latest_row_[entity_id];
  if (last_row.status != TsEntityLatestRowStatus::Valid
      || TimestampCheckResult::NonOverlapping == checkTimestampWithSpans(ts_spans, last_row.last_ts,
                                                 last_row.last_ts)) {
    return KStatus::SUCCESS;
  }

  uint32_t db_id = schema->metaData()->db_id;
  KTableKey table_id = table_schema_mgr->GetTableId();
  TSMemSegRowData last_row_data(db_id, table_id, last_row.version, entity_id);
  // TODO(liumengzhen) : set correct lsn
  last_row_data.SetData(last_row.last_ts, UINT64_MAX);
  last_row_data.SetRowData(last_row.last_payload);
  std::shared_ptr<TsMemSegBlock> mem_block = std::make_shared<TsMemSegBlock>(nullptr);
  mem_block->SetMemoryAddrSafe();
  mem_block->InsertRow(&last_row_data);

  const vector<AttributeInfo>& attrs = schema->getSchemaInfoExcludeDropped();

  std::shared_ptr<TSBlkDataTypeConvert> convert = nullptr;
  if (last_row.version != scan_version) {
    convert = std::make_shared<TSBlkDataTypeConvert>(last_row.version, scan_version, table_schema_mgr);
    KStatus s = convert->Init();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TSBlkDataTypeConvert Init failed.");
      return KStatus::FAIL;
    }
  } else {
    if (parser == nullptr) {
      parser = std::make_shared<TsRawPayloadRowParser>(&attrs);
    }
    mem_block->SetParser(parser);
  }

  TsBlockSpan block_span(vgroup_id_, entity_id, std::move(mem_block), 0, 1,
                          convert, scan_version, &attrs);
  auto ret = ConvertBlockSpanToResultSet(scan_cols, block_span, attrs, res);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("ConvertBlockSpanToResultSet failed");
    return KStatus::FAIL;
  }
  entity_last_ts = last_row.last_ts;
  return KStatus::SUCCESS;
}

void TsVGroup::compactRoutine(void* args) {
  while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && enable_compact_thread_) {
    // If the thread pool stops or the system is no longer running, exit the loop
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !enable_compact_thread_) {
      break;
    }
    // Execute compact tasks
    bool compacted = false;
    auto s = Compact(&compacted);

    if (!compacted || s == FAIL) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  }
}

void TsVGroup::initCompactThread() {
  if (!enable_compact_thread_) {
    return;
  }
  KWDBOperatorInfo kwdb_operator_info;
  // Set the name and owner of the operation
  kwdb_operator_info.SetOperatorName("VGroup::CompactThread");
  kwdb_operator_info.SetOperatorOwner("VGroup");
  time_t now;
  // Record the start time of the operation
  kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
  // Start asynchronous thread
  compact_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&TsVGroup::compactRoutine, this, std::placeholders::_1), this, &kwdb_operator_info);
  if (compact_thread_id_ < 1) {
    // If thread creation fails, record error message
    LOG_ERROR("VGroup compact thread create failed");
  }
}

void TsVGroup::closeCompactThread() {
  if (compact_thread_id_ > 0) {
    // Waiting for the compact thread to complete
    KWDBDynamicThreadPool::GetThreadPool().JoinThread(compact_thread_id_, 0);
  }
}

KStatus TsVGroup::PartitionCompact(std::shared_ptr<const TsPartitionVersion> partition, bool call_by_vacuum) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  auto partition_id = partition->GetPartitionIdentifier();
  if (!partition->TrySetBusy(PartitionStatus::Compacting)) {
    auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
    LOG_INFO("partition skip compact, root_path: %s", root_path.c_str());
    return KStatus::FAIL;
  }
  partition = version_manager_->Current()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
  Defer defer{[&]() {
    partition->ResetStatus();
  }};
  TsVersionUpdate update;
  // 1. Get all the last segments that need to be compacted.
  std::vector<std::shared_ptr<TsLastSegment>> last_segments;
  int level = -1, group = -1;
  if (!call_by_vacuum) {
    last_segments = partition->GetCompactLastSegments(&level, &group);
  } else {
    last_segments = partition->GetAllLastSegments();
  }
  if (last_segments.empty()) {
    return KStatus::SUCCESS;
  }
  auto entity_segment = partition->GetEntitySegment();

  auto partition_name = PartitionDirName(partition_id);;
  auto root_path = this->GetPath() / partition_name;

  {
    std::stringstream ss;
    for (const auto& l : last_segments) {
      ss << l->GetFileNumber() << ", ";
    }
    LOG_INFO("Compact %s at vgroup: %d, level: %d, group: %d, last segments:(%s)", partition_name.c_str(),
             this->vgroup_id_, level, group, ss.str().c_str());
  }
  // 2. Build the column block.
  std::stringstream ss;
  TsSegmentWriteStats entity_stats;
  std::vector<std::shared_ptr<TsBlockSpan>> residual_spans;
  {
    std::vector<std::shared_ptr<TsBlockSpan>> block_spans;
    for (auto& last_segment : last_segments) {
      std::list<std::shared_ptr<TsBlockSpan>> curr_block_spans;
      auto s = last_segment->GetBlockSpans(curr_block_spans, schema_mgr_);
      if (s == FAIL) {
        LOG_ERROR("get block spans failed. vgroup: %d, lastsegment: %u", this->vgroup_id_,
                  last_segment->GetFileNumber());
        return FAIL;
      }
      std::move(curr_block_spans.begin(), curr_block_spans.end(), std::back_inserter(block_spans));
    }

    TsEntitySegmentBuilder builder(env, root_path.string(), schema_mgr_, version_manager_.get(), partition_id,
                                   entity_segment);
    builder.PutBlockSpans(std::move(block_spans));

    KStatus s = builder.Open();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder open failed", path_.c_str());
      return s;
    }

    s = builder.Compact(call_by_vacuum, &update, &residual_spans, &entity_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder build failed", path_.c_str());
      return s;
    }

    EntitySegmentMetaInfo info;
    update.GetEntitySegmentInfo(partition_id, &info);
    ss << "entity segment: [" << info.datablock_info.length << "," << info.header_b_info.length << ","
       << info.agg_info.length << "," << info.header_e_file_number << "];";
  }

  // 3. write last segment
  if (!residual_spans.empty()) {
    assert(!call_by_vacuum);
    ss << "last segment: ";
    int next_level = std::min<int>(level + 1, TsPartitionVersion::LastSegmentContainer::kMaxLevel - 1);

    std::map<int, std::vector<std::shared_ptr<TsBlockSpan>>> grouped_spans;
    std::map<int, std::unique_ptr<TsLastSegmentBuilder>> grouped_builders;
    for (auto& span : residual_spans) {
      int group = TsPartitionVersion::LastSegmentContainer::GetGroupByEntityID(next_level, span->GetEntityID());
      grouped_spans[group].push_back(std::move(span));
    }

    TsIOEnv* env = &TsIOEnv::GetInstance();
    for (auto it = grouped_spans.begin(); it != grouped_spans.end(); ++it) {
      std::unique_ptr<TsAppendOnlyFile> last_segment;
      uint64_t file_number = version_manager_->NewFileNumber();
      auto filepath = root_path / LastSegmentFileName(file_number);
      auto s = env->NewAppendOnlyFile(filepath, &last_segment);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, new last segment failed.")
        return s;
      }
      grouped_builders[it->first] =
          std::make_unique<TsLastSegmentBuilder>(schema_mgr_, std::move(last_segment), file_number);
    }

    ss << "{";
    for (auto it = grouped_spans.begin(); it != grouped_spans.end(); ++it) {
      TsSegmentWriteStats stats;
      auto& span_vec = it->second;
      auto& builder = grouped_builders[it->first];
      for (auto& span : span_vec) {
        auto s = builder->PutBlockSpan(span);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("TsEntitySegmentBuilder::Compact failed, TsLastSegmentBuilder put failed.")
        }
      }
      auto s = builder->Finalize(&stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, TsLastSegmentBuilder finalize failed.")
        return s;
      }
      LOG_INFO("LastSegment %lu in %s created by Compaction, level: %d, group: %d", builder->GetFileNumber(),
               root_path.string().c_str(), next_level, it->first);
      char log_buf[64];
      std::snprintf(log_buf, sizeof(log_buf), "%lu: (%d, %d), ", builder->GetFileNumber(), level, group);
      ss << log_buf;
      update.AddLastSegment(partition_id, {builder->GetFileNumber(), next_level, it->first});
    }
    ss << "}";
  } else {
    ss << "no last segment created";
  }

  // 3. Set the compacted version.
  for (auto& last_segment : last_segments) {
    update.DeleteLastSegment(partition->GetPartitionIdentifier(), last_segment->GetFileNumber());
  }

  LOG_INFO("Compact end. %s", ss.str().c_str());

  // 4. Update the version.
  return version_manager_->ApplyUpdate(&update);
}

KStatus TsVGroup::Compact(bool* compacted) {
  auto current = version_manager_->Current();
  std::vector<std::shared_ptr<const TsPartitionVersion>> partitions = current->GetPartitionsToCompact();
  if (partitions.empty()) {
    if (compacted != nullptr) {
      *compacted = false;
    }
    return KStatus::SUCCESS;
  }
  if (compacted != nullptr) {
    *compacted = true;
  }
  // Compact partitions
  bool success{true};
  for (auto it = partitions.rbegin(); it != partitions.rend(); ++it) {
    const auto& cur_partition = *it;
    KStatus s = PartitionCompact(cur_partition);
    if (s != KStatus::SUCCESS) {
      success = false;
      continue;
    }
  }
  if (!success) {
    LOG_ERROR("compact failed.");
    return FAIL;
  }
  return KStatus::SUCCESS;
}

static auto SplitBlockSpansByPartition(const TsVGroupVersion* current, std::vector<std::shared_ptr<TsBlockSpan>> spans)
    -> std::pair<bool, std::unordered_map<const TsPartitionVersion*, std::vector<std::shared_ptr<TsBlockSpan>>>> {
  std::unordered_map<const TsPartitionVersion*, std::vector<std::shared_ptr<TsBlockSpan>>> result;
  for (auto& span : spans) {
    auto db_id = static_cast<TsMemSegBlock*>(span->GetTsBlockRaw())->GetDBId();
    auto current_span = span;
    while (current_span != nullptr && current_span->GetRowNum() != 0) {
      DATATYPE ts_type = current_span->GetTSType();
      timestamp64 first_ts = convertTsToPTime(current_span->GetFirstTS(), ts_type);
      timestamp64 last_ts = convertTsToPTime(current_span->GetLastTS(), ts_type);
      auto partition = current->GetPartition(db_id, first_ts);
      if (partition == nullptr) {
        LOG_WARN("cannot find partition: retry later.")
        return {false, {}};
      }
      if (last_ts < partition->GetEndTime()) {
        result[partition.get()].push_back(std::move(current_span));
        continue;
      }
      int split_idx = *std::upper_bound(
          IndexRange{0}, IndexRange(current_span->GetRowNum()), partition->GetEndTime(),
          [&](timestamp64 val, int idx) { return val <= convertTsToPTime(current_span->GetTS(idx), ts_type); });
      assert(split_idx > 0);

      std::shared_ptr<TsBlockSpan> front_span;
      current_span->SplitFront(split_idx, front_span);
      result[partition.get()].push_back(std::move(front_span));
    }
  }
  return {true, result};
}

std::vector<TsEntityCountStats> GetFlushInfoFromSpans(const std::vector<std::shared_ptr<TsBlockSpan>>& spans) {
  assert(!spans.empty());
  std::vector<int> transition_idx;
  transition_idx.push_back(0);
  for (int i = 1; i < spans.size(); ++i) {
    if (spans[i]->GetEntityID() != spans[i - 1]->GetEntityID()) {
      transition_idx.push_back(i);
    }
  }
  transition_idx.push_back(spans.size());
  std::vector<TsEntityCountStats> result;
  result.reserve(transition_idx.size() - 1);
  for (int i = 0; i + 1 < transition_idx.size(); ++i) {
    int start_idx = transition_idx[i];
    int end_idx = transition_idx[i + 1];
    assert(start_idx < end_idx);
    assert(spans[start_idx]->GetEntityID() == spans[end_idx - 1]->GetEntityID());
    TsEntityCountStats flush_info;
    flush_info.table_id = spans[start_idx]->GetTableID();
    flush_info.entity_id = spans[start_idx]->GetEntityID();
    int sum = 0;
    for (int j = start_idx; j < end_idx; ++j) {
      sum += spans[j]->GetRowNum();
    }
    flush_info.valid_count = sum;
    flush_info.min_ts = spans[start_idx]->GetFirstTS();
    flush_info.max_ts = spans[end_idx - 1]->GetLastTS();
    flush_info.is_count_valid = true;

    result.push_back(std::move(flush_info));
  }
  return result;
}

static uint64_t GetMaxOsn(const std::vector<std::shared_ptr<TsBlockSpan>>& sorted_spans) {
  uint64_t max_osn = 0;
  for (auto& span : sorted_spans) {
    uint64_t tmp_min_osn, tmp_max_osn;
    span->GetMinAndMaxOSN(tmp_min_osn, tmp_max_osn);
    if (tmp_max_osn > max_osn) {
      max_osn = tmp_max_osn;
    }
  }
  return max_osn;
}

static KStatus FlushToLastSegment(TsIOEnv* env, TsEngineSchemaManager* schema_mgr, TsVersionManager* version_mgr,
                                  const TsPartitionVersion* partition,
                                  const std::vector<std::shared_ptr<TsBlockSpan>>& spans, TsVersionUpdate* update,
                                  TsSegmentWriteStats* stats) {
  auto lastseg_file_number = version_mgr->NewFileNumber();
  auto filepath = partition->GetPartitionPath() / LastSegmentFileName(lastseg_file_number);
  std::unique_ptr<TsAppendOnlyFile> lastseg_file;
  auto s = env->NewAppendOnlyFile(filepath, &lastseg_file);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("flush failed, new last segment failed.");
    return s;
  }
  TsLastSegmentBuilder lastseg_builder(schema_mgr, std::move(lastseg_file), lastseg_file_number);
  for (auto& span : spans) {
    lastseg_builder.PutBlockSpan(span);
  }
  lastseg_builder.Finalize(stats);
  update->AddLastSegment(partition->GetPartitionIdentifier(), LastSegmentMetaInfo{lastseg_file_number, 0, 0});
  return SUCCESS;
}

KStatus TsVGroup::FlushImmSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  TsIOEnv* env = &TsIOEnv::GetInstance();

  std::unordered_set<const TsPartitionVersion*> flushed_partitions;

  std::vector<std::shared_ptr<TsBlockSpan>> sorted_spans;
  TsVersionUpdate update;
  {
    std::list<std::shared_ptr<TsBlockSpan>> all_block_spans;
    auto s = mem_seg->GetBlockSpans(all_block_spans, schema_mgr_);
    if (s == FAIL) {
      LOG_ERROR("cannot get block spans.");
      return FAIL;
    }
    sorted_spans.reserve(all_block_spans.size());
    std::move(all_block_spans.begin(), all_block_spans.end(), std::back_inserter(sorted_spans));
    update.SetMaxLSN(GetMaxOsn(sorted_spans));
  }

  auto current = version_manager_->Current();

  const auto [success, partitioned_spans] = SplitBlockSpansByPartition(current.get(), std::move(sorted_spans));
  if (!success) {
    return KStatus::FAIL;
  }

  std::vector<const TsPartitionVersion*> locked_partitions;
  locked_partitions.reserve(partitioned_spans.size());
  Defer unlock_partitions([&]() {
    for (auto partition : locked_partitions) {
      partition->ResetStatus();
    }
  });

  std::stringstream ss;
  TsSegmentWriteStats total_entity_stats;
  TsSegmentWriteStats total_last_stats;
  std::map<PartitionIdentifier, std::vector<TsEntityCountStats>> flush_infos;
  for (auto& [partition, spans] : partitioned_spans) {
    if (spans.empty()) continue;
    auto partition_id = partition->GetPartitionIdentifier();
    flush_infos[partition_id] = GetFlushInfoFromSpans(spans);
    if (flushed_partitions.find(partition) == flushed_partitions.end()) {
      if (!partition->HasDirectoryCreated()) {
        auto path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
        auto s = env->NewDirectory(path);
        if (s != SUCCESS) {
          LOG_ERROR("cannot create directory for partition.");
          return FAIL;
        }
      }
      update.PartitionDirCreated(partition_id);
      flushed_partitions.insert(partition);
    }

    if (partition->TrySetBusy(PartitionStatus::Compacting)) {
      locked_partitions.push_back(partition);
      TsIOEnv* mem_env = &TsMemoryIOEnv::GetInstance();
      EntitySegmentMetaInfo mem_entity_info;
      auto root_path = this->GetPath() / PartitionDirName(partition_id);
      std::vector<std::shared_ptr<TsBlockSpan>> lastseg_spans;
      std::vector<std::shared_ptr<TsBlockSpan>> written_spans;
      {
        TsEntitySegmentBuilder entityseg_builder(mem_env, root_path, schema_mgr_, version_manager_.get(),
                                                 partition->GetPartitionIdentifier(), nullptr);
        auto s = entityseg_builder.Open();
        if (s == FAIL) {
          return s;
        }
        entityseg_builder.PutBlockSpans(std::move(spans));
        TsVersionUpdate temp_update;
        TsSegmentWriteStats stats;
        s = entityseg_builder.Compact(false, &temp_update, &lastseg_spans, &stats);
        if (s == FAIL) {
          LOG_ERROR("flush to entity segment failed.");
          return FAIL;
        }
        total_entity_stats += stats;
        temp_update.GetEntitySegmentInfo(partition_id, &mem_entity_info);
      }

      if (!lastseg_spans.empty()) {
        TsSegmentWriteStats lastseg_stats;
        auto s = FlushToLastSegment(env, schema_mgr_, version_manager_.get(), partition, lastseg_spans, &update,
                                    &lastseg_stats);
        total_last_stats += lastseg_stats;
        if (s == FAIL) {
          return s;
        }
      }

      TsEntitySegment mem_entity_segment({mem_env, mem_env}, root_path, mem_entity_info, nullptr);
      auto s = mem_entity_segment.Open();
      if (s == FAIL) {
        LOG_ERROR("open mem entity segment failed.");
        return s;
      }
      mem_entity_segment.MarkDeleteAll();

      if (mem_entity_segment.GetEntityNum() != 0) {
        TsMemEntitySegmentModifier modifier(&mem_entity_segment);
        EntitySegmentMetaInfo info;
        auto newer_partition = version_manager_->Current()->GetPartition(partition_id);
        auto base_entity_segment = newer_partition->GetEntitySegment();
        s = modifier.PersistToDisk(base_entity_segment.get(), &info);
        if (s == FAIL) {
          return FAIL;
        }
        update.SetEntitySegment(partition_id, info, false);
      }

    } else {
      // cannot fetch lock, drop all memory entity segment data and flush to last segment.
      TsSegmentWriteStats lastseg_stats;
      auto s = FlushToLastSegment(env, schema_mgr_, version_manager_.get(), partition, spans, &update, &lastseg_stats);
      if (s == FAIL) {
        return FAIL;
      }
      total_last_stats += lastseg_stats;
    }
  }
  update.RemoveMemSegment(mem_seg);

  for (auto& [par_id, info] : flush_infos) {
    uint64_t file_number = version_manager_->NewFileNumber();
    update.AddCountFile(par_id, {file_number, info});
  }
  update.SetCountStatsType(CountStatsStatus::FlushImmOrWriteBatch);

  auto s = version_manager_->ApplyUpdate(&update);
  if (s == FAIL) {
    LOG_ERROR("apply update failed.");
    return s;
  }

  char entity_log_buf[128];
  if (total_entity_stats.written_blocks != 0) {
    auto log = total_entity_stats.ToString();
    std::snprintf(entity_log_buf, sizeof(entity_log_buf), "%s write into EntitySegment;", log.c_str());
  } else {
    std::snprintf(entity_log_buf, sizeof(entity_log_buf), "no data write into EntitySegment");
  }
  char last_log_buf[128];
  if (total_last_stats.written_blocks != 0) {
    auto log = total_last_stats.ToString();
    std::snprintf(last_log_buf, sizeof(last_log_buf), "%s write into LastSegment;", log.c_str());
  } else {
    std::snprintf(last_log_buf, sizeof(last_log_buf), "no data write into LastSegment");
  }
  LOG_INFO("vgroup: %d flush success. %lu partitions flushed. %s; %s", vgroup_id_, partitioned_spans.size(),
           entity_log_buf, last_log_buf);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetIterator(kwdbContext_p ctx, uint32_t version, vector<uint32_t>& entity_ids,
                              std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                              std::vector<k_uint32>& scan_cols, std::vector<k_uint32>& ts_scan_cols,
                              std::vector<k_int32>& agg_extend_cols,
                              std::vector<Sumfunctype>& scan_agg_types,
                              std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                              std::shared_ptr<MMapMetricsTable>& schema, TsStorageIterator** iter,
                              const std::shared_ptr<TsVGroup>& vgroup,
                              const std::vector<timestamp64>& ts_points, bool reverse, bool sorted) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support
  // case). TS_OSN read_lsn = GetOptimisticReadLsn();
  TsStorageIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    ts_iter = new TsSortedRawDataIteratorV2Impl(vgroup, version, entity_ids, ts_spans, block_filter, scan_cols,
                                                ts_scan_cols, table_schema_mgr, schema, ASC);
  } else {
    // need call Next function times: entity_ids.size(), no matter Next return what.
    ts_iter = new TsAggIteratorV2Impl(vgroup, version, entity_ids, ts_spans, block_filter, scan_cols, ts_scan_cols,
                                      agg_extend_cols, scan_agg_types, ts_points, table_schema_mgr, schema);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetDelInfoByOSN(kwdbContext_p ctx, TSTableID tbl_id, uint32_t entity_id, std::vector<KwOSNSpan>& osn_span,
  std::vector<KwTsSpan>* del_spans) {
  auto current = CurrentVersion();
  auto db_id = schema_mgr_->GetDBIDByTableID(tbl_id);
  if (db_id == 0) {
    LOG_ERROR("cannot find db by table[%lu]", tbl_id);
    return KStatus::FAIL;
  }
  auto ts_partitions = current->GetDBAllPartitions(db_id);
  list<KwTsSpan> del_items;
  for (auto p : ts_partitions) {
    list<KwTsSpan> del_item;
    auto s = p->GetDelRangeByOSN(entity_id, osn_span, del_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get delete info failed for partition[%ld], db [%u]", p->GetStartTime(), db_id);
      return s;
    }
    del_items.insert(del_items.end(), del_item.begin(), del_item.end());
  }
  MergeTsSpans(del_items, del_spans);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetDelInfoWithOSN(kwdbContext_p ctx, TSTableID tbl_id, uint32_t entity_id,
  list<STDelRange>* del_spans) {
  auto current = CurrentVersion();
  auto db_id = schema_mgr_->GetDBIDByTableID(tbl_id);
  if (db_id == 0) {
    LOG_ERROR("cannot find db by table[%lu]", tbl_id);
    return KStatus::FAIL;
  }
  auto ts_partitions = current->GetDBAllPartitions(db_id);
  list<STDelRange> del_items;
  std::vector<KwOSNSpan> osn_span;
  osn_span.push_back({0, UINT64_MAX});
  for (auto p : ts_partitions) {
    list<STDelRange> del_item;
    auto s = p->GetDelRangeWithOSN(entity_id, osn_span, del_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get delete info failed for partition[%ld], db [%u]", p->GetStartTime(), db_id);
      return s;
    }
    del_items.insert(del_items.end(), del_item.begin(), del_item.end());
  }
  DeplicateTsSpans(del_items, del_spans);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetMetricIteratorByOSN(kwdbContext_p ctx, const std::shared_ptr<TsVGroup>& vgroup,
    std::vector<EntityResultIndex>& entity_ids, std::vector<k_uint32>& scan_cols, std::vector<k_uint32>& ts_scan_cols,
    std::vector<KwOSNSpan>& osn_span,
    uint32_t version, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr, TsStorageIterator** iter) {
  auto ts_iter = new TsRawDataIteratorV2ImplByOSN(vgroup, version, entity_ids,
    scan_cols, ts_scan_cols, osn_span, table_schema_mgr);
  KStatus s = ts_iter->Init();
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetBlockSpans(TSTableID table_id, uint32_t entity_id, KwTsSpan ts_span, DATATYPE ts_col_type,
                                std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                                std::shared_ptr<const TsVGroupVersion>& current,
                                std::list<std::shared_ptr<TsBlockSpan>>* block_spans) {
  uint32_t db_id = schema_mgr_->GetDBIDByTableID(table_id);
  current = version_manager_->Current();
  std::vector<KwTsSpan> ts_spans{ts_span};
  auto ts_partitions = current->GetPartitions(db_id, ts_spans, ts_col_type);
  std::shared_ptr<MMapMetricsTable> metric_schema;
  KStatus s = table_schema_mgr->GetMetricSchema(table_version, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetMetricSchema failed.");
    return s;
  }
  for (int32_t index = 0; index < ts_partitions.size(); ++index) {
    TsScanFilterParams filter{db_id, table_id, vgroup_id_, entity_id, ts_col_type, UINT64_MAX, ts_spans};
    auto partition_version = ts_partitions[index];
    std::list<std::shared_ptr<TsBlockSpan>> cur_block_span;
    s = partition_version->GetBlockSpans(filter, &cur_block_span, table_schema_mgr, metric_schema);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition_version GetBlockSpan failed.");
      return s;
    }
    block_spans->splice(block_spans->begin(), cur_block_span);
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::rollback(kwdbContext_p ctx, LogEntry* wal_log, bool from_chk) {
  KStatus s;
  uint64_t lsn = wal_log->getLSN();
  if (from_chk) {
    lsn = wal_log->getOldLSN();
  }

  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        return undoPut(ctx, lsn, log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return undoPutTag(ctx, lsn, log->getPayload());
      }
    }
    case WALLogType::UPDATE: {
      auto update_log = reinterpret_cast<UpdateLogEntry*>(wal_log);
      if (update_log->getTableType() == WALTableType::TAG) {
        auto log = reinterpret_cast<UpdateLogTagsEntry*>(wal_log);
        return undoUpdateTag(ctx, lsn, log->getPayload(), log->getOldPayload());
      }
      break;
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;
      assert(t_type != WALTableType::DATA);
      if (t_type == WALTableType::DATA_V2) {
        auto log = reinterpret_cast<DeleteLogMetricsEntryV2*>(del_log);
        p_tag = log->getPrimaryTag();
        TSTableID table_id = log->getTableId();
        vector<KwTsSpan> ts_spans = log->getTsSpans();
        return undoDeleteData(ctx, table_id, p_tag, lsn, ts_spans);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        TSSlice primary_tag = log->getPrimaryTag();
        TSSlice tags = log->getTags();
        uint64_t osn = log->getOSN();
        return undoDeleteTag(ctx, log->getTableID(), primary_tag, lsn, log->group_id_, log->entity_id_, tags, osn);
      }
    }

    case WALLogType::CHECKPOINT:
      break;
    case WALLogType::MTR_BEGIN:
      break;
    case WALLogType::MTR_COMMIT:
      break;
    case WALLogType::MTR_ROLLBACK:
      break;
    case WALLogType::TS_BEGIN:
      break;
    case WALLogType::TS_COMMIT:
      break;
    case WALLogType::TS_ROLLBACK:
      break;
    case WALLogType::DDL_CREATE:
      break;
    case WALLogType::DDL_DROP:
      break;
    case WALLogType::DDL_ALTER_COLUMN:
      break;
    case WALLogType::DB_SETTING:
      break;
    case WALLogType::RANGE_SNAPSHOT: {
      //      auto snapshot_log = reinterpret_cast<SnapshotEntry*>(wal_log);
      //      if (snapshot_log == nullptr) {
      //        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
      //        return KStatus::FAIL;
      //      }
      //      HashIdSpan hash_span;
      //      KwTsSpan ts_span;
      //      snapshot_log->GetRangeInfo(&hash_span, &ts_span);
      //      uint64_t count = 0;
      //      auto s = DeleteRangeData(ctx, hash_span, 0, {ts_span}, nullptr, &count,
      //      snapshot_log->getXID(), false); if (s != KStatus::SUCCESS) {
      //        LOG_ERROR(" WAL rollback snapshot delete range data faild.");
      //        return KStatus::FAIL;
      //      }
      //      break;
    }
    case WALLogType::SNAPSHOT_TMP_DIRCTORY: {
      auto temp_path_log = reinterpret_cast<TempDirectoryEntry*>(wal_log);
      if (temp_path_log == nullptr) {
        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
        return KStatus::FAIL;
      }
      std::string path = temp_path_log->GetPath();
      if (!Remove(path)) {
        LOG_ERROR(" WAL rollback cannot remove path[%s]", path.c_str());
        return KStatus::FAIL;
      }
      break;
    }
    // TODO(xy): code review here, the following cases are not handled.
    case WALLogType::CREATE_INDEX:
    case WALLogType::DROP_INDEX:
    case WALLogType::END_CHECKPOINT: {
      assert(false);
      break;
    }
  }

  return SUCCESS;
}

KStatus TsVGroup::ApplyWal(kwdbContext_p ctx, LogEntry* wal_log,
                           std::unordered_map<TS_OSN, MTRBeginEntry*>& incomplete) {
  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      std::string p_tag;
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        p_tag = log->getPrimaryTag();
        return redoPut(ctx, log->getLSN(), log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return redoPutTag(ctx, log->getLSN(), log->getPayload());
      }
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;
      assert(t_type != WALTableType::DATA);
      if (t_type == WALTableType::DATA_V2) {
        auto log = reinterpret_cast<DeleteLogMetricsEntryV2*>(del_log);
        p_tag = log->getPrimaryTag();
        TSTableID table_id = log->getTableId();
        vector<KwTsSpan> ts_spans = log->getTsSpans();
        return redoDeleteData(ctx, table_id, p_tag, log->getOSN(), ts_spans);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        auto p_tag_slice = log->getPrimaryTag();
        auto tag_slice = log->getTags();
        return redoDeleteTag(ctx, log->getTableID(), p_tag_slice, log->getOSN(), log->group_id_, log->entity_id_, tag_slice);
      }
    }
    case WALLogType::UPDATE: {
      auto update_log = reinterpret_cast<UpdateLogEntry*>(wal_log);
      WALTableType t_type = update_log->getTableType();
      std::string p_tag;
      if (t_type == WALTableType::TAG) {
        auto log = reinterpret_cast<UpdateLogTagsEntry*>(update_log);
        auto slice = log->getPayload();
        return redoUpdateTag(ctx, log->getLSN(), slice);
      }
      break;
    }
      // There is nothing to do while applying CHECKPOINT WAL.
    case WALLogType::CHECKPOINT: {
      KStatus s = wal_manager_->CreateCheckpointWithoutFlush(ctx);
      if (s == FAIL) return s;
      break;
    }
    case WALLogType::MTR_BEGIN: {
      auto log = reinterpret_cast<MTRBeginEntry*>(wal_log);
      incomplete.insert(std::pair<TS_OSN, MTRBeginEntry*>(log->getXID(), log));
      break;
    }
    case WALLogType::MTR_COMMIT: {
      auto log = reinterpret_cast<MTREntry*>(wal_log);
      incomplete.erase(log->getXID());
      break;
    }
    default:
      break;
  }
  return KStatus::SUCCESS;
}

uint32_t TsVGroup::GetVGroupID() { return vgroup_id_; }

KStatus TsVGroup::DeleteEntity(kwdbContext_p ctx, TSTableID table_id, std::string& p_tag, TSEntityID e_id,
                               uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  auto tag_table = tb_schema_manager->GetTagTable();

  uint64_t hash_point = t1ha1_le(p_tag.data(), p_tag.size());
  if (tag_lock_ != nullptr) {
    tag_lock_->WrLock(hash_point);
  }
  Defer defer{[&](){
    if (tag_lock_ != nullptr) {
      tag_lock_->Unlock(hash_point);
    }
  }};
  if (!tag_table->hasPrimaryKey(p_tag.data(), p_tag.size())) {
    LOG_ERROR("Cannot found this Primary tag.");
    return KStatus::FAIL;
  }

  if (EnableWAL() && user_del) {
    auto tag_pack = tag_table->GenTagPack(p_tag.data(), p_tag.size());
    if (UNLIKELY(nullptr == tag_pack)) {
      return KStatus::FAIL;
    }
    LockSharedLevelMutex();
    s = wal_manager_->WriteDeleteTagWAL(ctx, mtr_id, p_tag, vgroup_id_, e_id, tag_pack->getData(), vgroup_id_,
                                        table_id, osn);
    UnLockSharedLevelMutex();
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteDeleteTagWAL failed.");
      return s;
    }
  }

  // if any error, end the delete loop and return ERROR to the caller.
  // Delete tag and its index
  ErrorInfo err_info;
  std::pair<uint64_t, uint64_t> ignore;
  tag_table->DeleteTagRecord(p_tag.data(), p_tag.size(), err_info, osn,
    user_del ? OperateType::Delete : OperateType::DeleteBySnapshot, ignore);
  if (err_info.errcode < 0) {
    LOG_ERROR("delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
    return KStatus::FAIL;
  }
  if (*count != 0) {
    // todo(liangbo01) we should delete current entity metric datas.
    std::vector<KwTsSpan> ts_spans;
    ts_spans.push_back({INT64_MIN, INT64_MAX});
    // delete current entity metric datas.
    s = DropMetricEntity(ctx, table_id, e_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::DeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& p_tag, TSEntityID e_id,
  const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del) {
  std::vector<DelRowSpan> dtp_list;
  // todo(xy): need to initialize lsn if wal_level = off
  TS_OSN current_lsn = 0;
  if (EnableWAL() && user_del) {
    LockSharedLevelMutex();
    KStatus s = wal_manager_->WriteDeleteMetricsWAL4V2(ctx, mtr_id, tbl_id, p_tag, ts_spans, vgroup_id_, osn, &current_lsn);
    UnLockSharedLevelMutex();
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteDeleteTagWAL failed.");
      return s;
    }
  } else {
    current_lsn = OSNInc();
  }

  // delete current entity metric datas.
  return DeleteData(ctx, tbl_id, e_id, osn, ts_spans, user_del);
}

KStatus TsVGroup::deleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, KwOSNSpan osn,
const std::vector<KwTsSpan>& ts_spans, bool user_del) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(tbl_id, tb_schema_mgr);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTableSchemaMgr failed.");
    return s;
  }
  auto db_id = tb_schema_mgr->GetDbID();
  auto ts_type = tb_schema_mgr->GetTsColDataType();

  std::vector<std::shared_ptr<const TsPartitionVersion>> ps =
    version_manager_->Current()->GetPartitions(db_id, ts_spans, ts_type);
  if (ps.size() == 0) {
    auto ptime = convertTsToPTime(ts_spans.back().end, ts_type);
    uint64_t cur_time = time(nullptr);
    if (ptime > cur_time) {
      ptime = cur_time;
    }
    s = version_manager_->AddPartition(db_id, ptime);
    if (s == KStatus::FAIL) {
      LOG_ERROR("AddPartition [%lu] of current time failed.", ptime);
      return s;
    }
  }
  ps = version_manager_->Current()->GetPartitions(db_id, ts_spans, ts_type);
  for (auto& p : ps) {
    s = p->DeleteData(e_id, ts_spans, osn, user_del);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData partition[%u/%lu] failed!",
              std::get<0>(p->GetPartitionIdentifier()), std::get<1>(p->GetPartitionIdentifier()));
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::DropMetricEntity(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id) {
  auto s = TrasvalAllPartition(ctx, tbl_id, {KwTsSpan{INT64_MIN, INT64_MAX}},
  [&](std::shared_ptr<const TsPartitionVersion> p) -> KStatus {
    auto ret = p->DropEntity(e_id);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData partition[%u/%lu] failed!",
              std::get<0>(p->GetPartitionIdentifier()), std::get<1>(p->GetPartitionIdentifier()));
      return ret;
    }
    return ret;
  });
  return KStatus::SUCCESS;
}

KStatus TsVGroup::DeleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, TS_OSN osn,
const std::vector<KwTsSpan>& ts_spans, bool user_del) {
  assert(osn != UINT64_MAX);
  return deleteData(ctx, tbl_id, e_id, {0, osn}, ts_spans, user_del);
}

KStatus TsVGroup::GetEntitySegmentBuilder(std::shared_ptr<const TsPartitionVersion>& partition,
                                          std::shared_ptr<TsEntitySegmentBuilder>& builder) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  PartitionIdentifier partition_id = partition->GetPartitionIdentifier();
  auto it = write_batch_segment_builders_.find(partition_id);
  if (it == write_batch_segment_builders_.end()) {
    while (!partition->TrySetBusy(PartitionStatus::BatchDataWriting)) {
      sleep(1);
    }
    partition = version_manager_->Current()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
    auto entity_segment = partition->GetEntitySegment();

    auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
    builder = std::make_shared<TsEntitySegmentBuilder>(env, root_path.string(), schema_mgr_, version_manager_.get(),
                                                       partition_id, entity_segment);
    KStatus s = builder->Open();
    if (s != KStatus::SUCCESS) {
      partition->ResetStatus();
      LOG_ERROR("Open entity segment builder failed.");
      return s;
    }
    write_batch_segment_builders_[partition_id] = builder;
  } else {
    builder = it->second;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::WriteBatchData(TSTableID tbl_id, uint32_t table_version, TSEntityID entity_id, timestamp64 p_time,
                                 uint32_t batch_version, TSSlice data) {
  auto current = version_manager_->Current();
  uint32_t database_id = schema_mgr_->GetDBIDByTableID(tbl_id);
  if (database_id == 0) {
    return KStatus::FAIL;
  }
  auto partition = current->GetPartition(database_id, p_time);
  if (partition == nullptr) {
    KStatus s = version_manager_->AddPartition(database_id, p_time);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    current = version_manager_->Current();
    partition = current->GetPartition(database_id, p_time);
    if (partition == nullptr) {
      LOG_ERROR("cannot find partition: database_id[%u], p_time[%lu]", database_id, p_time);
      return KStatus::FAIL;
    }
  }

  std::shared_ptr<TsEntitySegmentBuilder> builder;
  KStatus s = GetEntitySegmentBuilder(partition, builder);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntitySegmentBuilder failed.");
    return s;
  }
  s = builder->WriteBatch(tbl_id, entity_id, table_version, batch_version, data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("WriteBatch failed.");
    return s;
  }

  ResetEntityLatestRow(entity_id, INT64_MAX);
  ResetEntityMaxTs(tbl_id, INT64_MAX, entity_id);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::FinishWriteBatchData() {
  TsVersionUpdate update;
  std::set<PartitionIdentifier> partition_ids;
  bool success = true;
  for (auto& kv : write_batch_segment_builders_) {
    partition_ids.insert(kv.first);
    update.PartitionDirCreated(kv.first);
    KStatus s = kv.second->WriteBatchFinish(&update);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Finish entity segment builder failed");
      success = false;
    }
    uint64_t file_number = version_manager_->NewFileNumber();
    update.AddCountFile(kv.first, {file_number, kv.second->FlushInfos()});
  }
  write_batch_segment_builders_.clear();
  if (success) {
    update.SetCountStatsType(CountStatsStatus::FlushImmOrWriteBatch);
    version_manager_->ApplyUpdate(&update);
  }
  for (auto& k : partition_ids) {
    auto partition = version_manager_->Current()->GetPartition(std::get<0>(k), std::get<1>(k));
    partition->ResetStatus();
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::CancelWriteBatchData() {
  std::set<PartitionIdentifier> partition_ids;
  for (auto& kv : write_batch_segment_builders_) {
    partition_ids.insert(kv.first);
    kv.second->WriteBatchCancel();
  }
  write_batch_segment_builders_.clear();
  for (auto p_id : partition_ids) {
    auto partition = version_manager_->Current()->GetPartition(std::get<0>(p_id), std::get<1>(p_id));
    partition->ResetStatus();
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::getEntityIdByPTag(kwdbContext_p ctx, TSTableID table_id, TSSlice& ptag, TSEntityID* entity_id) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s =  tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }
  uint32_t sub_group_id = 0;
  uint32_t cur_entity_id = 0;
  if (!tag_table->hasPrimaryKey(ptag.data, ptag.len, cur_entity_id, sub_group_id)) {
    LOG_INFO("getEntityIdByPTag failed, table id[%lu]", table_id);
    *entity_id = 0;
  } else {
    *entity_id = cur_entity_id;
    assert(sub_group_id == this->GetVGroupID());
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoPut(kwdbContext_p ctx, TS_OSN log_lsn, TSSlice payload) {
  auto table_id = TsRawPayload::GetTableIDFromSlice(payload);
  TSSlice primary_key = TsRawPayload::GetPrimaryKeyFromSlice(payload);
  auto tbl_version = TsRawPayload::GetTableVersionFromSlice(payload);
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr failed.");
    return s;
  }
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  tb_schema_mgr->GetColumnsExcludeDroppedPtr(&metric_schema, tbl_version);
  TsRawPayload p(metric_schema);
  s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  TSEntityID entity_id;
  s = getEntityIdByPTag(ctx, table_id, primary_key, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("undoPut failed.");
    return s;
  }
  if (entity_id > 0) {
    auto row_num = p.GetRowCount();
    std::vector<KwTsSpan> ts_spans;
    for (size_t i = 0; i < row_num; i++) {
      timestamp64 cur_ts = p.GetTS(i);
      ts_spans.push_back({cur_ts, cur_ts});
    }
    s = deleteData(ctx, table_id, entity_id, {p.GetOSN(), p.GetOSN()}, ts_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("deleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::TrasvalAllPartition(kwdbContext_p ctx, TSTableID tbl_id,
const std::vector<KwTsSpan>& ts_spans, std::function<KStatus(std::shared_ptr<const TsPartitionVersion>)> func) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(tbl_id, tb_schema_mgr);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTableSchemaMgr failed.");
    return s;
  }
  auto db_id = tb_schema_mgr->GetDbID();
  auto ts_type = tb_schema_mgr->GetTsColDataType();

  std::vector<std::shared_ptr<const TsPartitionVersion>> ps =
    version_manager_->Current()->GetPartitions(db_id, ts_spans, ts_type);
  for (auto& p : ps) {
    s = func(p);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("func failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_OSN log_lsn,
const std::vector<KwTsSpan>& ts_spans) {
  TSEntityID entity_id;
  TSSlice p_tag{primary_tag.data(), primary_tag.length()};
  auto s = getEntityIdByPTag(ctx, tbl_id, p_tag, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getEntityIdByPTag failed.");
    return s;
  }
  if (entity_id > 0) {
    s = TrasvalAllPartition(ctx, tbl_id, ts_spans,
      [&](std::shared_ptr<const TsPartitionVersion> p) -> KStatus {
        auto ret = p->UndoDeleteData(entity_id, ts_spans, {0, log_lsn});
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("UndoDeleteData partition[%u/%lu] failed!",
              std::get<0>(p->GetPartitionIdentifier()), std::get<1>(p->GetPartitionIdentifier()));
          return ret;
        }
        return ret;
      });
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TrasvalAllPartition failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_OSN log_lsn,
                                 const std::vector<KwTsSpan>& ts_spans) {
  TSEntityID entity_id;
  TSSlice p_tag{primary_tag.data(), primary_tag.length()};
  auto s = getEntityIdByPTag(ctx, tbl_id, p_tag, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getEntityIdByPTag failed.");
    return s;
  }
  if (entity_id > 0) {
    s = DeleteData(ctx, tbl_id, entity_id, log_lsn, ts_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoPutTag(kwdbContext_p ctx, kwdbts::TS_OSN log_lsn, const TSSlice& payload) {
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  std::shared_ptr<TsTableSchemaManager> tb_schema;
  s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaManager failed, table id: %lu", table_id);
    return s;
  }

  s = schema_mgr_->GetVGroup(ctx, tb_schema, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint8_t payload_data_flag = p.GetRowType();
  if (new_tag) {
    vgroup_id = GetVGroupID();
    entity_id = AllocateEntityID();
    // 1. Write tag data
    assert(payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY);
    LOG_DEBUG("tag bt insert hashPoint=%hu", p.GetHashPoint());
    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id, p.GetOSN(), OperateType::Insert);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  }
  return s;
}

KStatus TsVGroup::undoPutTag(kwdbContext_p ctx, TS_OSN log_lsn, const TSSlice& payload) {
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("undoPutTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  int res = tag_table->InsertForUndo(group_id, entity_id, primary_key, p.GetOSN());
  if (res < 0) {
    LOG_ERROR("undoPutTag: InsertForUndo failed, primary tag[%s]", primary_key.data)
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_OSN log_lsn, const TSSlice& payload, uint64_t osn) {
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoUpdateTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }
  int res;
  res = tag_table->UpdateForRedo(group_id, entity_id, primary_key, p);
  if (res < 0) {
    LOG_ERROR("redoUpdateTag: UpdateForRedo failed, primary tag[%s].", primary_key.data)
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::undoUpdateTag(kwdbContext_p ctx, TS_OSN log_lsn, TSSlice payload, const TSSlice& old_payload,
                                uint64_t osn) {
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("undoUpdateTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  if (tag_table->UpdateForUndo(group_id, entity_id, p.GetHashPoint(), primary_key, old_payload, p.GetOSN()) < 0) {
    LOG_ERROR("undoUpdateTag: UpdateForUndo failed, primary tag[%s].", primary_key.data)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus TsVGroup::redoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_key, kwdbts::TS_OSN log_lsn,
                                uint32_t group_id, uint32_t entity_id, TSSlice& tags, uint64_t osn) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }

  ErrorInfo err_info;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoDeleteTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  int res = tag_table->DeleteForRedo(group_id, entity_id, primary_key, tags);
  if (res) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_key, TS_OSN log_lsn,
                                uint32_t group_id, uint32_t entity_id, TSSlice& tags, uint64_t osn) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }

  ErrorInfo err_info;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoDeleteTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }
  int res = tag_table->DeleteForUndo(group_id, entity_id, tb_schema_manager->GetHashNum(), primary_key, tags);
  if (res < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  return tsx_manager_->MtrBegin(ctx, range_id, index, mtr_id, tsx_id);
}

KStatus TsVGroup::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id, const char* tsx_id) {
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  if (tsx_id != nullptr) {
    mtr_id = tsx_manager_->getMtrID(tsx_id);
  }
  return tsx_manager_->MtrCommit(ctx, mtr_id, tsx_id);
}

KStatus TsVGroup::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip, const char* tsx_id) {
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  //  1. Write ROLLBACK log;
  KStatus s;
  if (!is_skip) {
    if (tsx_id != nullptr) {
      mtr_id = tsx_manager_->getMtrID(tsx_id);
    }
    s = tsx_manager_->MtrRollback(ctx, mtr_id, tsx_id);
    if (s == FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

bool TsVGroup::getDroppedTables(uint32_t db_id, std::unordered_set<TSTableID>* dropped_table_ids, bool force) {
  bool has_dropped = false;
  std::vector<TSTableID> table_ids;
  // vacuum immediately, we can check table dropped by rocksdb's meta,
  // don't care about parallel r&w
  if (force) {
    KStatus s = schema_mgr_->GetTableList(&table_ids);
    if (s != SUCCESS) {
      LOG_WARN("GetTableList failed.");
      return has_dropped;
    }
    for (auto tb_id : table_ids) {
      if (!checkTableMetaExist(tb_id)) {
        if (schema_mgr_->GetDBIDByTableID(tb_id) == db_id) {
          has_dropped = true;
          dropped_table_ids->insert(tb_id);
        }
      }
    }
    return has_dropped;
  }
  // not vacuum immediately, check table dropped by schema manager,
  // this is original code logic.
  std::vector<std::shared_ptr<TsTableSchemaManager>> tb_schema_mgr;
  KStatus s = schema_mgr_->GetAllTableSchemaMgrs(tb_schema_mgr);
  if (s != SUCCESS) {
    LOG_WARN("GetAllTableSchemaMgrs failed.");
    return has_dropped;
  }
  for (const auto& tb_mgr : tb_schema_mgr) {
    if (tb_mgr->IsDropped()) {
      has_dropped = true;
      dropped_table_ids->insert(tb_mgr->GetTableId());
    }
  }
  return has_dropped;
}

KStatus TsVGroup::Vacuum(kwdbContext_p ctx, bool force) {
  KStatus s = KStatus::SUCCESS;
  auto current = version_manager_->Current();
  auto all_partitions = current->GetPartitions();

  for (auto& [db_id, partitions] : all_partitions) {
    const int n = force ? partitions.size() : partitions.size() - 1;
    for (int i = 0; i < n; i++) {
      // There is an issue with the current logic for determining whether the ctx has been cancelled.
      // if (force && ctx->relation_ctx != 0 && isCanceledCtx(ctx->relation_ctx)) {
      //   LOG_INFO("Context has been canceled, stop vacuum.");
      //   return KStatus::SUCCESS;
      // }
      s = KStatus::SUCCESS;
      auto& partition = partitions[i];
      auto partition_id = partition->GetPartitionIdentifier();
      auto root_path = this->GetPath() / PartitionDirName(partition_id);
      bool need_vacuum = false;
      if (partition->GetLastSegmentsCount() != 0) {
        // force compact historical partition
        s = PartitionCompact(partition, true);
        if (s != SUCCESS) {
          LOG_ERROR("PartitionCompact failed, [%s]", partition->GetPartitionIdentifierStr().c_str());
          continue;
        }
        partition = version_manager_->Current()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
      }
      std::unordered_set<TSTableID> dropped_table_ids;
      need_vacuum = getDroppedTables(db_id, &dropped_table_ids, force);
      if (!need_vacuum) {
        s = partition->NeedVacuumEntitySegment(root_path, schema_mgr_, force, need_vacuum);
        if (s != SUCCESS) {
          LOG_ERROR("NeedVacuumEntitySegment failed.");
          continue;
        }
      }
      if (need_vacuum) {
        s = VacuumPartition(ctx, partition, dropped_table_ids, force);
        if (s != SUCCESS) {
          LOG_WARN("Vacuum partition [vgroup_%d]-[%ld, %ld) failed", vgroup_id_,
                    partition->GetStartTime(), partition->GetEndTime() - 1);
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::VacuumPartition(kwdbContext_p ctx, shared_ptr<const TsPartitionVersion> partition,
  std::unordered_set<TSTableID> dropped_table_ids, bool force) {
  if (force) {
    while (!partition->TrySetBusy(PartitionStatus::Vacuuming)) {
      this_thread::sleep_for(std::chrono::seconds(1));
      // if (ctx->relation_ctx != 0 && isCanceledCtx(ctx->relation_ctx)) {
      //   LOG_INFO("Context has been canceled, stop vacuum.");
      //   return KStatus::SUCCESS;
      // }
    }
  } else {
    if (!partition->TrySetBusy(PartitionStatus::Vacuuming)) {
      return SUCCESS;
    }
  }
  Defer defer{[&]() {
    partition->ResetStatus();
  }};

  auto entity_segment = partition->GetEntitySegment();
  if (entity_segment == nullptr) {
    return SUCCESS;
  }

  auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
  auto max_entity_id = entity_segment->GetEntityNum();

  auto partition_id = partition->GetPartitionIdentifier();
  auto root_path = this->GetPath() / PartitionDirName(partition_id);
  auto vacuumer = std::make_unique<TsEntitySegmentVacuumer>(root_path, this->version_manager_.get());
  bool cancel_vacuumer = false;
  auto s = vacuumer->Open();
  if (s != SUCCESS) {
    cancel_vacuumer = true;
    return s;
  }
  Defer defer2{[&]() {
    if (cancel_vacuumer) {
      vacuumer->Cancel();
    }
  }};
  auto handle_info = vacuumer->GetHandleInfo();
  LOG_INFO("Vacuum partition [vgroup_%d]-[%ld, %ld) begin, handle info {%lu, %lu, %lu, %lu}",
            vgroup_id_, partition->GetStartTime(), partition->GetEndTime() - 1,
            handle_info.header_e_file_number, handle_info.header_b_info.file_number,
            handle_info.datablock_info.file_number, handle_info.agg_info.file_number);

  auto mem_segments = partition->GetAllMemSegments();
  std::list<std::pair<TSEntityID, TS_OSN>> entity_max_lsn;
  for (uint32_t entity_id = 1; entity_id <= max_entity_id; entity_id++) {
    TsEntityItem entity_item;
    bool found = false;
    s = entity_segment->GetEntityItem(entity_id, entity_item, found);
    if (s != SUCCESS) {
      LOG_ERROR("Vacuum failed, GetEntityItem [%u] failed", entity_id);
      cancel_vacuumer = true;
      return s;
    }
    bool is_dropped = dropped_table_ids.find(entity_item.table_id) != dropped_table_ids.end();
    if (!found || 0 == entity_item.cur_block_id || is_dropped) {
      TsEntityItem empty_entity_item{entity_id};
      empty_entity_item.table_id = entity_item.table_id;
      s = vacuumer->AppendEntityItem(empty_entity_item);
      if (s != SUCCESS) {
        LOG_ERROR("Vacuum failed, AppendEntityItem failed");
        cancel_vacuumer = true;
        return s;
      }
      if (is_dropped) {
        entity_max_lsn.emplace_back(entity_id, UINT64_MAX);
      }
      continue;
    }
    // not dropped
    std::shared_ptr<TsTableSchemaManager> tb_schema_mgr{nullptr};
    s = schema_mgr_->GetTableSchemaMgr(entity_item.table_id, tb_schema_mgr);
    if (s != SUCCESS) {
      cancel_vacuumer = true;
      return s;
    }
    auto life_time = tb_schema_mgr->GetLifeTime();
    int64_t start_ts = INT64_MIN;
    int64_t end_ts = INT64_MAX;
    if (life_time.ts != 0) {
      start_ts = (now.time_since_epoch().count() - life_time.ts) * life_time.precision;
    }
    KwTsSpan ts_span = {start_ts, end_ts};
    std::vector<KwTsSpan> ts_spans = {ts_span};
    TsScanFilterParams filter{partition->GetDatabaseID(), entity_item.table_id, vgroup_id_,
                              entity_id, tb_schema_mgr->GetTsColDataType(), UINT64_MAX, ts_spans};
    TsBlockItemFilterParams block_data_filter;
    s = partition->getFilter(filter, block_data_filter);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("getFilter failed");
      cancel_vacuumer = true;
      return s;
    }

    std::list<shared_ptr<TsBlockSpan>> block_spans;
    std::shared_ptr<MMapMetricsTable> metric_schema;
    auto scan_version = tb_schema_mgr->GetCurrentVersion();
    s = tb_schema_mgr->GetMetricSchema(scan_version, &metric_schema);
    if (s != SUCCESS) {
      LOG_ERROR("Vacuum failed, GetMetricSchema failed");
      cancel_vacuumer = true;
      return s;
    }
    s = entity_segment->GetBlockSpans(block_data_filter, block_spans, tb_schema_mgr, metric_schema);
    if (s != SUCCESS) {
      LOG_ERROR("Vacuum failed, GetBlockSpans failed");
      cancel_vacuumer = true;
      return s;
    }
    TsEntityItem cur_entity_item = {entity_id};
    cur_entity_item.table_id = entity_item.table_id;
    for (auto& block_span : block_spans) {
      TsBufferBuilder data;
      s = block_span->GetCompressData(&data);
      if (s != SUCCESS) {
        LOG_ERROR("Vacuum failed, GetCompressData failed");
        cancel_vacuumer = true;
        return s;
      }
      uint32_t col_count = block_span->GetColCount();
      uint32_t col_offsets_len = (col_count + 1) * sizeof(uint32_t);
      auto last_col_tail_offset = *reinterpret_cast<uint32_t *>(data.data() + col_count * sizeof(uint32_t));
      auto block_data_len = col_offsets_len + last_col_tail_offset;
      auto block_agg_len = data.size() - block_data_len;
      auto block_data = data.SubSlice(0, block_data_len);
      auto block_agg = data.SubSlice(block_data_len, block_agg_len);

      TsEntitySegmentBlockItem blk_item;
      blk_item.entity_id = entity_item.entity_id;
      blk_item.table_version = scan_version;
      blk_item.n_cols = block_span->GetColCount() + 1;
      blk_item.n_rows = block_span->GetRowNum();
      blk_item.min_ts = block_span->GetFirstTS();
      blk_item.max_ts = block_span->GetLastTS();
      blk_item.block_len = block_data.len;
      blk_item.agg_len = block_agg.len;
      blk_item.block_version = CURRENT_BLOCK_VERSION;
      s = vacuumer->AppendBlock(block_data, &blk_item.block_offset);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Vacuum failed, AppendBlock failed");
        cancel_vacuumer = true;
        return s;
      }
      s = vacuumer->AppendAgg(block_agg, &blk_item.agg_offset);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Vacuum failed, AppendAgg failed");
        cancel_vacuumer = true;
        return s;
      }
      blk_item.prev_block_id = cur_entity_item.cur_block_id;
      s = vacuumer->AppendBlockItem(blk_item);  // block_id is set when append
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Vacuum failed, AppendBlockItem failed");
        cancel_vacuumer = true;
        return s;
      }
      cur_entity_item.cur_block_id = blk_item.block_id;
      cur_entity_item.row_written += blk_item.n_rows;
      if (blk_item.max_ts > cur_entity_item.max_ts) {
        cur_entity_item.max_ts = blk_item.max_ts;
      }
      if (blk_item.min_ts < cur_entity_item.min_ts) {
        cur_entity_item.min_ts = blk_item.min_ts;
      }
    }
    s = vacuumer->AppendEntityItem(cur_entity_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Vacuum failed, AppendEntityItem failed");
      cancel_vacuumer = true;
      return s;
    }
    {
      // check weather mem segment has data for one entity
      KwTsSpan partition_ts_span = {partition->GetTsColTypeStartTime(tb_schema_mgr->GetTsColDataType()),
                                    partition->GetTsColTypeEndTime(tb_schema_mgr->GetTsColDataType())};
      STScanRange scan_range = {partition_ts_span, {0, UINT64_MAX}};
      DatabaseID db_id = std::get<0>(partition->GetPartitionIdentifier());
      TsBlockItemFilterParams param {db_id, entity_item.table_id, vgroup_id_, entity_id, {scan_range}};
      std::list<shared_ptr<TsBlockSpan>> mem_block_spans;
      std::shared_ptr<MMapMetricsTable> partition_metric_schema;
      s = tb_schema_mgr->GetMetricSchema(0, &partition_metric_schema);
      if (s != SUCCESS) {
        LOG_ERROR("Vacuum failed, GetMetricSchema failed");
        cancel_vacuumer = true;
        return s;
      }
      for (auto& mem_segment : mem_segments) {
        s = mem_segment->GetBlockSpans(param, mem_block_spans, tb_schema_mgr, partition_metric_schema);
        if (s != SUCCESS) {
          LOG_ERROR("Vacuum failed, GetBlockSpans for mem segment failed");
          cancel_vacuumer = true;
          return s;
        }
      }
      if (mem_block_spans.empty()) {
        entity_max_lsn.emplace_back(entity_id, UINT64_MAX);
      }
    }
  }
  TsVersionUpdate update;
  auto info = vacuumer->GetHandleInfo();
  update.SetEntitySegment(partition->GetPartitionIdentifier(), info, true);
  vacuumer.reset();
  version_manager_->ApplyUpdate(&update);

  s = partition->RmDeleteItems(entity_max_lsn);
  if (s != KStatus::SUCCESS) {
    LOG_INFO("delete delitem failed. can ignore this.");
  }
  LOG_INFO("Vacuum partition [vgroup_%d]-[%ld, %ld) succeeded", vgroup_id_, partition->GetStartTime(),
                                                                partition->GetEndTime() - 1);
  return SUCCESS;
}

BlocksDistribution GetEntityDistribution(const std::shared_ptr<TsEntitySegment>& entity_segment,
                                         const uint32_t& entity_id) {
  std::vector<TsEntitySegmentBlockItemWithData> entity_items;
  BlocksDistribution blocks_distribution;
  KStatus s = entity_segment->GetAllBlockItems(entity_id, &entity_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityDistribution failed.");
    blocks_distribution.blocks_num_ = 0;
    blocks_distribution.blocks_size_ = 0;
    blocks_distribution.rows_num_ = 0;
    return blocks_distribution;
  }
  if (!entity_items.empty()) {
    blocks_distribution.blocks_num_ += entity_items.size();
    for (auto& entity_item : entity_items) {
      blocks_distribution.blocks_size_ += entity_item.block_item->block_len;
      blocks_distribution.rows_num_ += entity_item.block_item->n_rows;
    }
  }
  return blocks_distribution;
}

KStatus TsVGroup::GetTableBlocksDistribution(uint32_t target_db_id, TSTableID table_id,
  const std::vector<uint32_t>& entity_ids, VGroupBlocksInfo* blocks_info) {
  auto current = version_manager_->Current();
  auto all_partitions = current->GetDBAllPartitions(target_db_id);
  for (const auto& partition : all_partitions) {
    // last segments distribution
    std::vector<uint32_t> last_segments_level_count = partition->GetAllLevelLastSegmentsCount();
    blocks_info->last_segments_info_.last_seg_level0 = last_segments_level_count[0];
    blocks_info->last_segments_info_.last_seg_level1 = last_segments_level_count[1];
    blocks_info->last_segments_info_.last_seg_level2 = last_segments_level_count[2];

    std::vector<std::shared_ptr<TsLastSegment>> all_last_segments;
    all_last_segments = partition->GetAllLastSegments();
    for (const auto& last_segment : all_last_segments) {
      std::vector<TsLastSegmentBlockIndex> block_indices;
      KStatus s = last_segment->GetAllBlockIndex(&block_indices);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetAllBlockIndex failed, db_id:%d, table_id:%lu, partition:%s",
          target_db_id, table_id, partition->GetPartitionIdentifierStr().c_str());
        return s;
      }
      auto n = block_indices.size();
      for (size_t j = 0; j < n; j++) {
        auto block_idx = block_indices[j];
        if (block_idx.table_id == table_id) {
          blocks_info->last_segments_info_.blocks_num_++;
          auto block_size = last_segment->GetBlockSize(j);
          if (block_size == static_cast<size_t>(-1)) {
            LOG_ERROR("GetBlockSize failed, db_id:%d, table_id:%lu, partition:%s",
              target_db_id, table_id, partition->GetPartitionIdentifierStr().c_str());
            return KStatus::FAIL;
          }
          blocks_info->last_segments_info_.blocks_size_ += block_size;
          uint64_t row_count = 0;
          s = last_segment->GetBlockRowCount(j, &row_count);
          if (s != KStatus::SUCCESS) {
            return s;
          }
          blocks_info->last_segments_info_.rows_num_ += row_count;
        }
      }
    }

    // entity segment distribution
    std::shared_ptr<TsEntitySegment> entity_segment = partition->GetEntitySegment();
    if (entity_segment != nullptr) {
      blocks_info->entity_segments_info_.blocks_size_ += entity_segment->GetAggFileSize();
      for (auto entity_id : entity_ids) {
        blocks_info->entity_segments_info_.Add(GetEntityDistribution(entity_segment, entity_id));
      }
    }
  }
  return SUCCESS;
}

KStatus TsVGroup::GetDBBlocksDistribution(uint32_t target_db_id, VGroupBlocksInfo* blocks_info) {
  auto current = version_manager_->Current();
  auto all_partitions = current->GetDBAllPartitions(target_db_id);
  for (const auto& partition : all_partitions) {
    // last segments distribution
    std::vector<std::shared_ptr<TsLastSegment>> all_last_segments;
    all_last_segments = partition->GetAllLastSegments();
    for (const auto& last_segment : all_last_segments) {
      size_t block_count = last_segment->GetBlockCount();
      blocks_info->last_segments_info_.blocks_num_ += block_count;
      TsLastSegmentBlockIndex* block_index;
      KStatus s = last_segment->GetBlockIndex(0, &block_index);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetAllBlockIndex failed, db_id:%d, partition:%s", target_db_id,
          partition->GetPartitionIdentifierStr().c_str());
        return s;
      }
      blocks_info->last_segments_info_.blocks_size_ += block_index->info_offset;
      for (size_t i = 0; i < block_count; i++) {
        TsLastSegmentBlockInfoWithData* info;
        s = last_segment->GetBlockInfo(i, &info);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("Last segment GetBlockInfo failed.");
          return s;
        }
        blocks_info->last_segments_info_.rows_num_ += info->nrow;
      }
    }

    // entity segment distribution
    std::shared_ptr<TsEntitySegment> entity_segment = partition->GetEntitySegment();
    if (entity_segment != nullptr) {
      auto max_entity_id = entity_segment->GetEntityNum();
      for (uint64_t entity_id = 0; entity_id < max_entity_id; entity_id++) {
        blocks_info->entity_segments_info_.Add(GetEntityDistribution(entity_segment, entity_id));
      }
    }
  }
  return SUCCESS;
}

void TsVGroup::recalcCountRoutine(void* args) {
  while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && enable_recalc_count_thread_) {
    // If the thread pool stops or the system is no longer running, exit the loop
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !enable_recalc_count_thread_) {
      break;
    }
    KStatus s = RecalcCountStat();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("RecalcCountStat failed.")
    }
    {
      std::unique_lock<std::mutex> lock(recalc_count_mutex_);
      count_cv_.wait_for(lock, EngineOptions::count_stats_recalc_cycle == 0 ?
        std::chrono::minutes(5) : std::chrono::seconds(EngineOptions::count_stats_recalc_cycle),
        [this]() { return !enable_recalc_count_thread_; });
    }
  }
}

void TsVGroup::initRecalcCountThread() {
#ifdef WITH_TESTS
  return;
#endif
  if (!enable_recalc_count_thread_) {
    return;
  }
  KWDBOperatorInfo kwdb_operator_info;
  // Set the name and owner of the operation
  kwdb_operator_info.SetOperatorName("VGroup::RecalcCountThread");
  kwdb_operator_info.SetOperatorOwner("VGroup");
  time_t now;
  // Record the start time of the operation
  kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
  // Start asynchronous thread
  recalc_count_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&TsVGroup::recalcCountRoutine, this, std::placeholders::_1), this, &kwdb_operator_info);
  if (recalc_count_thread_id_ < 1) {
    // If thread creation fails, record error message
    LOG_ERROR("VGroup recalc count thread create failed");
  }
}

void TsVGroup::closeRecalcCountThread() {
  if (recalc_count_thread_id_ > 0) {
    // Wake up potentially dormant count threads
    enable_recalc_count_thread_ = false;
    count_cv_.notify_all();
    // Waiting for the count thread to complete
    KWDBDynamicThreadPool::GetThreadPool().JoinThread(recalc_count_thread_id_, 0);
  }
}

KStatus TsVGroup::AddRecalcEntity(PartitionIdentifier partition_id, TSTableID table_id, TSEntityID entity_id) {
  std::unique_lock<std::mutex> lock(recalc_count_mutex_);
  recalc_count_entities_[partition_id][table_id].emplace(entity_id);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::RecalcCountStat() {
  std::map<PartitionIdentifier, std::map<TSTableID, std::unordered_set<TSEntityID>>> recalc_map;
  {
    std::unique_lock<std::mutex> lock(recalc_count_mutex_);
    recalc_map.swap(recalc_count_entities_);
  }
  if (recalc_map.empty() || EngineOptions::count_stats_recalc_cycle == 0) {
    return KStatus::SUCCESS;
  }
  auto current_version = version_manager_->Current();
  uint64_t version_num = current_version->GetVersionNumber();
  TS_OSN max_osn = current_version->GetMaxOSN();
  KStatus s;
  for (auto& [par_id, tables] : recalc_map) {
    auto par_version = current_version->GetPartition(par_id);
    if (par_version) {
      TsVersionUpdate update;
      for (auto& [tb_id, entities] : tables) {
        std::shared_ptr<TsTableSchemaManager> tb_schema;
        s = schema_mgr_->GetTableSchemaMgr(tb_id, tb_schema);
        if (s != KStatus::SUCCESS) {
          if (tb_schema == nullptr) {
            continue;
          }
          LOG_ERROR("Get table schema manager [%lu] failed.", tb_id);
          return s;
        }
        std::shared_ptr<TagTable> tag_table;
        kwdbContext_t ctx;
        auto s = tb_schema->GetTagSchema(&ctx, &tag_table);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("Failed get table id[%ld] tag schema.", tb_schema->GetTableId());
          return s;
        }
        DATATYPE ts_col_type = tb_schema->GetTsColDataType();
        std::vector<KwTsSpan> ts_spans = {{par_version->GetTsColTypeStartTime(ts_col_type),
                                       par_version->GetTsColTypeEndTime(ts_col_type)}};
        TsScanFilterParams filter{tb_schema->GetDbID(), tb_schema->GetTableId(), GetVGroupID(), 0,
          ts_col_type, UINT64_MAX, ts_spans};
        std::vector<TsEntityCountStats> flush_infos;
        for (auto& entity_id : entities) {
          if (!tag_table->HasEntityTag(GetVGroupID(), entity_id)) {
            continue;
          }
          filter.entity_id_ = entity_id;
          std::list<shared_ptr<TsBlockSpan>> block_spans;
          s = par_version->GetBlockSpans(filter, &block_spans, tb_schema,
            tb_schema->GetCurrentMetricsTable(), nullptr, true);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("RecalculateCountStat get block spans failed.");
            return s;
          }
          TsBlockSpanSortedIterator iter(block_spans, schema_mgr_, EngineOptions::g_dedup_rule);
          iter.Init();
          std::shared_ptr<TsBlockSpan> dedup_block_span;
          bool is_finished = false;
          TsEntityCountStats flush_info{tb_schema->GetTableId(), entity_id, INT64_MAX, INT64_MIN, 0, true, ""};
          while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
            if (flush_info.min_ts > dedup_block_span->GetFirstTS()) {
              flush_info.min_ts = dedup_block_span->GetFirstTS();
            }
            if (flush_info.max_ts < dedup_block_span->GetLastTS()) {
              flush_info.max_ts = dedup_block_span->GetLastTS();
            }
            flush_info.valid_count += dedup_block_span->GetRowNum();
          }
          if (flush_info.min_ts != INT64_MAX && flush_info.max_ts != INT64_MIN) {
            flush_infos.emplace_back(flush_info);
          }
        }
        uint64_t file_number = version_manager_->NewFileNumber();
        update.AddCountFile(par_id, {file_number, flush_infos});
      }
      update.SetCountStatsType(CountStatsStatus::Recalculate);
      update.SetVersionNum(version_num);
      update.SetMaxLSN(max_osn);
      auto s = version_manager_->ApplyUpdate(&update);
      if (s != SUCCESS) {
        LOG_ERROR("partition [%s] count stat recalculate failed.", par_version->GetPartitionPath().c_str());
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::ResetCountStat() {
  std::vector<std::shared_ptr<TsTableSchemaManager> > tb_schema_manager;
  KStatus s = schema_mgr_->GetAllTableSchemaMgrs(tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get all table schema mgrs failed.")
  }

  std::map<std::shared_ptr<TsTableSchemaManager>, std::vector<uint32_t>> table_entity_map;
  for (auto& tb_schema : tb_schema_manager) {
    DatabaseID db_id = tb_schema->GetDbID();
    std::shared_ptr<TagTable> tag_table;
    kwdbContext_t ctx;
    auto s = tb_schema->GetTagSchema(&ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed get table id[%ld] tag schema.", tb_schema->GetTableId());
      return s;
    }
    std::vector<uint32_t> entity_id_list;
    tag_table->GetEntityIdListByVGroupId(vgroup_id_, entity_id_list);
    table_entity_map.emplace(tb_schema, entity_id_list);
  }
  std::shared_ptr<const TsVGroupVersion> cur_version = version_manager_->Current();
  auto all_partitions = cur_version->GetAllPartitions();

  TsVersionUpdate update;
  for (auto& [par_id, par_version] : all_partitions) {
    for (auto& [tb, entities] : table_entity_map) {
      if (tb->GetDbID() != std::get<0>(par_id)) {
        continue;
      }
      std::vector<TsEntityCountStats> flush_infos;
      for (auto& entity : entities) {
        TsEntityCountStats flush_info{tb->GetTableId(), entity, INT64_MAX, INT64_MIN, 0, false, ""};
        flush_infos.emplace_back(flush_info);
      }
      uint64_t file_number = version_manager_->NewFileNumber();
      update.AddCountFile(par_id, {file_number, flush_infos});
    }
  }
  update.SetCountStatsType(CountStatsStatus::UpgradeRecover);
  version_manager_->ApplyUpdate(&update);
  return KStatus::SUCCESS;
}
}  //  namespace kwdbts
