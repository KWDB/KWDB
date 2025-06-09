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

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "sys_utils.h"
#include "ts_iterator_v2_impl.h"
#include "ts_lastsegment_builder.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {

static const uint64_t interval = 3600 * 24 * 30;  // 30 days.

// todo(liangbo01) using normal path for mem_segment.
TsVGroup::TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id,
                   TsEngineSchemaManager* schema_mgr, bool enable_compact_thread)
    : vgroup_id_(vgroup_id), schema_mgr_(schema_mgr), partitions_latch_(RWLATCH_ID_VGROUP_PARTITIONS_RWLOCK),
      mem_segment_mgr_(this), path_(engine_options.db_path + "/" + GetFileName()),
      entity_counter_(0), engine_options_(engine_options),
      enable_compact_thread_(enable_compact_thread) {
  initCompactThread();
}

TsVGroup::~TsVGroup() {
  enable_compact_thread_ = false;
  closeCompactThread();
  if (config_file_ != nullptr) {
    config_file_->sync(MS_SYNC);
    delete config_file_;
    config_file_ = nullptr;
  }
}

KStatus TsVGroup::Init(kwdbContext_p ctx) {
  MakeDirectory(path_);
  wal_manager_ = std::make_unique<WALMgr>(engine_options_.db_path, GetFileName(), &engine_options_);
  tsx_manager_ = std::make_unique<TSxMgr>(wal_manager_.get());
  auto res = wal_manager_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  config_file_ = new MMapFile();
  string cf_file_path = path_.string() + "/vg_config";
  int flag = MMAP_CREAT_EXCL;
  bool exist = false;
  if (IsExists(cf_file_path)) {
    flag = MMAP_OPEN_NORECURSIVE;
    exist = true;
  }
  int error_code = config_file_->open(cf_file_path, flag);
  if (error_code < 0) {
    LOG_ERROR("Open config file failed, error code: %d, path: %s", error_code, cf_file_path.c_str());
    return KStatus::FAIL;
  }
  if (!exist) {
    config_file_->resize(4096);
    entity_counter_ = 0;
  } else {
    entity_counter_ = KUint32(config_file_->memAddr());
  }

  // recover partitions
  std::error_code ec;
  std::filesystem::directory_iterator dir_iter{path_, ec};
  if (ec.value() != 0) {
    LOG_ERROR("TsVGroup::Init fail, reason: %s", ec.message().c_str());
    return FAIL;
  }
  std::regex re("db([0-9]+)-(-?[0-9]+)");
  for (const auto& it : dir_iter) {
    std::string fname = it.path().filename();
    std::smatch res;
    bool ok = std::regex_match(fname, res, re);
    if (!ok) {
      continue;
    }
    uint32_t dbid = std::stoi(res.str(1));
    timestamp64 ptime = std::stoll(res.str(2));
    if (partitions_.find(dbid) == partitions_.end()) {
      partitions_[dbid] = std::make_unique<PartitionManager>(this, dbid, interval);
    }
    partitions_[dbid]->Get(ptime, true);
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroup::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  // no need do anything.
  return KStatus::SUCCESS;
}

KStatus TsVGroup::PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag,
                          TSEntityID entity_id, TSSlice* payload, bool write_wal) {
  TS_LSN current_lsn = 1;
  if (engine_options_.wal_level != WALMode::OFF && write_wal) {
    TS_LSN entry_lsn = 0;
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mtr_id, 0, 0, *primary_tag, *payload, entry_lsn, vgroup_id_);
    if (s == KStatus::FAIL) {
      wal_manager_->Unlock();
      return s;
    }
    // unlock current lsn
    wal_manager_->Unlock();

    if (entry_lsn != current_lsn) {
      LOG_ERROR("expected lsn is %lu, but got %lu ", current_lsn, entry_lsn);
      return KStatus::FAIL;
    }
  }
  return mem_segment_mgr_.PutData(*payload, entity_id, current_lsn);
}

std::filesystem::path TsVGroup::GetPath() const {
  return path_;
}

std::string TsVGroup::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "vg_%03u", vgroup_id_);
  return buffer;
}

uint32_t TsVGroup::AllocateEntityID() {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t new_id = entity_counter_ + 1;
  if (saveToFile(new_id) == 0) {
    entity_counter_ = new_id;
    return new_id;
  } else {
    throw std::runtime_error("Failed to persist the new ID to file");
  }
  return 0;
}

uint32_t TsVGroup::GetMaxEntityID() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entity_counter_;
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload) {
  // no need lock, lock inside.
  return wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, prepared_payload, vgroup_id_);
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload) {
  TS_LSN entry_lsn = 0;
  // lock current lsn: Lock the current LSN until the log is written to the cache
  wal_manager_->Lock();
  TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();
  KStatus s = wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, primary_tag, prepared_payload, entry_lsn, vgroup_id_);
  if (s == KStatus::FAIL) {
    wal_manager_->Unlock();
    return s;
  }
  // unlock current lsn
  wal_manager_->Unlock();

  if (entry_lsn != current_lsn) {
    LOG_ERROR("expected lsn is %lu, but got %lu ", current_lsn, entry_lsn);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::UpdateLSN(kwdbContext_p ctx, TS_LSN chk_lsn) {
  // 1.UpdateLSN
  wal_manager_->Lock();
  Defer defer{[&]() {
    wal_manager_->Unlock();
  }
  };
  KStatus s = wal_manager_->UpdateCheckpointWithoutFlush(ctx, chk_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteCheckpointWAL.")
    return FAIL;
  }
  wal_manager_->Flush(ctx);
  // 2. remove chk file
  s = wal_manager_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to RemoveChkFile.")
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn) {
  // 1. read chk wal log
  // 2. switch new file
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;
  KStatus  s = wal_manager_->ReadWALLogAndSwitchFile(logs, wal_manager_->FetchCheckpointLSN(),
                                                     wal_manager_->FetchCurrentLSN(), ignore);
  last_lsn = wal_manager_->FetchCurrentLSN();
  wal_manager_->Unlock();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLogAndSwitchFile.")
  }
  return s;
}

KStatus TsVGroup::ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn) {
  // 1. read chk wal log
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;
  TS_LSN chk_lsn = wal_manager_->FetchCheckpointLSN();
  if (last_lsn != 0) {
    chk_lsn = last_lsn;
  }
  KStatus  s = wal_manager_->ReadWALLog(logs, wal_manager_->FetchCheckpointLSN(), wal_manager_->FetchCurrentLSN(),
                                        ignore);
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

KStatus TsVGroup::CreateCheckpointInternal(kwdbContext_p ctx) {
  return KStatus::SUCCESS;
}

TsEngineSchemaManager* TsVGroup::GetSchemaMgr() const {
  return schema_mgr_;
}

TsMemSegmentManager* TsVGroup::GetMemSegmentMgr() {
  return &mem_segment_mgr_;
}

std::shared_ptr<TsVGroupPartition> TsVGroup::GetPartition(uint32_t database_id, timestamp64 p_time) {
  RW_LATCH_S_LOCK(&partitions_latch_);
  auto partition_manager = partitions_[database_id].get();
  RW_LATCH_UNLOCK(&partitions_latch_);
  if (partition_manager == nullptr) {
    // TODO(zzr): interval should be fetched form global setting;
    RW_LATCH_X_LOCK(&partitions_latch_);
    if (partitions_[database_id].get() == nullptr) {
      partitions_[database_id] = std::make_unique<PartitionManager>(this, database_id, interval);
    }
    partition_manager = partitions_[database_id].get();
    RW_LATCH_UNLOCK(&partitions_latch_);
  }
  return partition_manager->Get(p_time, true);
}

KStatus TsVGroup::redoPut(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p{payload};
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  auto s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
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
    s =  tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  } else {
    assert(vgroup_id == this->GetVGroupID());
  }

  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::DATA_ONLY) {
    s = mem_segment_mgr_.PutData(payload, entity_id, log_lsn);
  }
  return s;
}


int TsVGroup::saveToFile(uint32_t new_id) const {
  uint32_t entity_id = new_id;
  memcpy(reinterpret_cast<char*>(config_file_->memAddr()), &entity_id, sizeof(entity_id));
  // return config_file_->sync(MS_SYNC);
  return 0;
}

void TsVGroup::compactRoutine(void* args) {
  while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && enable_compact_thread_) {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    // Check every 2 seconds if compact is necessary
    cv_.wait_for(lock, std::chrono::seconds(2), [this] { return !enable_compact_thread_; });
    lock.unlock();
    // If the thread pool stops or the system is no longer running, exit the loop
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !enable_compact_thread_) {
      break;
    }
    // Execute compact tasks
    Compact();
  }
}

void TsVGroup::initCompactThread() {
  if (!enable_compact_thread_) {
    return;
  }
  KWDBOperatorInfo kwdb_operator_info;
  // Set the name and owner of the operation
  kwdb_operator_info.SetOperatorName("TsVGroupPartition::CompactThread");
  kwdb_operator_info.SetOperatorOwner("TsVGroupPartition");
  time_t now;
  // Record the start time of the operation
  kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
  // Start asynchronous thread
  compact_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
    std::bind(&TsVGroup::compactRoutine, this, std::placeholders::_1), this,
    &kwdb_operator_info);
  if (compact_thread_id_ < 1) {
    // If thread creation fails, record error message
    LOG_ERROR("TsVGroupPartition compact thread create failed");
  }
}

void TsVGroup::closeCompactThread() {
  if (compact_thread_id_ > 0) {
    // Wake up potentially dormant compact threads
    cv_.notify_all();
    // Waiting for the compact thread to complete
    KWDBDynamicThreadPool::GetThreadPool().JoinThread(compact_thread_id_, 0);
  }
}

inline bool PartitionLessThan(std::shared_ptr<TsVGroupPartition>& x, std::shared_ptr<TsVGroupPartition>& y) {
  return x->StartTs() > y->StartTs();
}

KStatus TsVGroup::Compact(int thread_num) {
  std::vector<std::shared_ptr<TsVGroupPartition>> vgroup_partitions;
  RW_LATCH_S_LOCK(&partitions_latch_);
  for (auto& partition : partitions_) {
    std::vector<std::shared_ptr<TsVGroupPartition>> v = partition.second->GetCompactPartitions();
    vgroup_partitions.insert(vgroup_partitions.end(), v.begin(), v.end());
  }
  RW_LATCH_UNLOCK(&partitions_latch_);
  // Prioritize the latest partition
  std::sort(vgroup_partitions.begin(), vgroup_partitions.end(), PartitionLessThan);
  // Compact partitions
  std::vector<std::thread> workers;
  for (uint32_t thread_idx = 0; thread_idx < thread_num; thread_idx++) {
    workers.emplace_back([&, thread_idx]() {
      for (size_t idx = thread_idx; idx < vgroup_partitions.size(); idx += thread_num) {
        KStatus s = vgroup_partitions[idx]->Compact();
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("TsVGroupPartition[%s] compact failed", vgroup_partitions[idx]->GetFileName().c_str())
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::FlushImmSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  if (!mem_seg->SetFlushing()) {
    LOG_ERROR("cannot set status for mem segment.");
    return KStatus::FAIL;
  }
  std::unordered_map<std::shared_ptr<TsVGroupPartition>, TsLastSegmentBuilder> builders;
  struct LastRowInfo {
    TSTableID cur_table_id = 0;
    uint32_t database_id = 0;
    uint32_t cur_table_version = 0;
    std::vector<AttributeInfo> info;
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  };
  LastRowInfo last_row_info;
  bool flush_success = true;

  mem_seg->Traversal([&](TSMemSegRowData* tbl) -> bool {
    // 1. get table schema manager.
    if (last_row_info.cur_table_id != tbl->table_id) {
      auto s =  schema_mgr_->GetTableSchemaMgr(tbl->table_id, last_row_info.schema_mgr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] schemainfo.", tbl->table_id);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_id = tbl->table_id;
      last_row_info.database_id = schema_mgr_->GetDBIDByTableID(tbl->table_id);
      last_row_info.cur_table_version = 0;
    }
    // 2. get table schema info of certain version.
    if (last_row_info.cur_table_version != tbl->table_version) {
      last_row_info.info.clear();
      auto s = last_row_info.schema_mgr->GetColumnsExcludeDropped(last_row_info.info, tbl->table_version);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] version[%u] schema info.", tbl->table_id, tbl->table_version);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_version = tbl->table_version;
    }
    // 3. get partition for metric data.
    auto partition =
        GetPartition(last_row_info.database_id, tbl->ts, (DATATYPE)last_row_info.info[0].type);
    auto it = builders.find(partition);
    if (it == builders.end()) {
      std::unique_ptr<TsFile> last_segment;
      uint32_t file_number;
      partition->NewLastSegmentFile(&last_segment, &file_number);
      auto result = builders.insert(
          {partition, TsLastSegmentBuilder{schema_mgr_, std::move(last_segment), file_number}});
      it = result.first;
    }
    // 4. insert data into segment builder.
    TsLastSegmentBuilder& builder = it->second;
    auto s = builder.PutRowData(tbl->table_id, tbl->table_version, tbl->entity_id, tbl->lsn, tbl->row_data);
    if (s != SUCCESS) {
      LOG_ERROR("PutRowData failed.");
      flush_success = false;
      return false;
    }
    return true;
  }, true);
  // todo(liangbo01) deleting all new created files.
  if (!flush_success) {
    LOG_ERROR("faile flush memsegment to last segment.");
    return KStatus::FAIL;
  }

  for (auto& kv : builders) {
    auto s = kv.second.Finalize();
    if (s == FAIL) {
      LOG_ERROR("last segment Finalize failed.");
      return KStatus::FAIL;
    }
    kv.first->PublicLastSegment(kv.second.GetFileNumber());
  }
  // todo(liangbo01) add all new files into new_file_list.
  std::list<TsLastSegment> new_file_list;
  //  todo(liangbo01) atomic: mem segment delete, and last segments load.
  mem_seg->SetDeleting();
  mem_segment_mgr_.RemoveMemSegment(mem_seg);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                                   std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                                   std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                                   std::vector<Sumfunctype> scan_agg_types,
                                   std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                   uint32_t table_version, TsStorageIterator** iter,
                                   std::shared_ptr<TsVGroup> vgroup,
                                   std::vector<timestamp64> ts_points, bool reverse, bool sorted) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support case).
  // TS_LSN read_lsn = GetOptimisticReadLsn();
  TsStorageIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    if (sorted) {
      ts_iter = new TsSortedRawDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
                                                  ts_scan_cols, table_schema_mgr, table_version, ASC);
    } else {
      ts_iter = new TsRawDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
                                            ts_scan_cols, table_schema_mgr, table_version);
    }
  } else {
    ts_iter = new TsAggIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols, ts_scan_cols,
                                      scan_agg_types, ts_points, table_schema_mgr, table_version);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::rollback(kwdbContext_p ctx, LogEntry* wal_log) {
  KStatus s;

  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        return undoPut(ctx, log->getLSN(), log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return undoPutTag(ctx, log->getLSN(), log->getPayload());
      }
    }
    case WALLogType::UPDATE: {
      auto update_log = reinterpret_cast<UpdateLogEntry*>(wal_log);
      if (update_log->getTableType() == WALTableType::TAG) {
        auto log = reinterpret_cast<UpdateLogTagsEntry*>(wal_log);
        return undoUpdateTag(ctx, log->getLSN(), log->getPayload(), log->getOldPayload());
      }
      break;
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;

      if (t_type == WALTableType::DATA) {
        auto log = reinterpret_cast<DeleteLogMetricsEntry*>(del_log);
        p_tag = log->getPrimaryTag();
        vector<DelRowSpan> partitions = log->getRowSpans();
        return undoDelete(ctx, p_tag, log->getLSN(), partitions);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        TSSlice primary_tag = log->getPrimaryTag();
        TSSlice tags = log->getTags();
        return undoDeleteTag(ctx, primary_tag, log->getLSN(), log->group_id_, log->entity_id_, tags);
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
    case WALLogType::RANGE_SNAPSHOT:
    {
//      auto snapshot_log = reinterpret_cast<SnapshotEntry*>(wal_log);
//      if (snapshot_log == nullptr) {
//        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
//        return KStatus::FAIL;
//      }
//      HashIdSpan hash_span;
//      KwTsSpan ts_span;
//      snapshot_log->GetRangeInfo(&hash_span, &ts_span);
//      uint64_t count = 0;
//      auto s = DeleteRangeData(ctx, hash_span, 0, {ts_span}, nullptr, &count, snapshot_log->getXID(), false);
//      if (s != KStatus::SUCCESS) {
//        LOG_ERROR(" WAL rollback snapshot delete range data faild.");
//        return KStatus::FAIL;
//      }
//      break;
    }
    case WALLogType::SNAPSHOT_TMP_DIRCTORY:
    {
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
    case WALLogType::PARTITION_TIER_CHANGE:
    {
//      auto tier_log = reinterpret_cast<PartitionTierChangeEntry*>(wal_log);
//      if (tier_log == nullptr) {
//        LOG_ERROR(" WAL rollback cannot prase partition tier log.");
//        return KStatus::FAIL;
//      }
//      ErrorInfo err_info;
//      auto s = TsTierPartitionManager::GetInstance().Recover(tier_log->GetLinkPath(), tier_log->GetTierPath(), err_info);
//      if (s != KStatus::SUCCESS) {
//        LOG_ERROR(" WAL rollback partition tier change faild. %s", err_info.errmsg.c_str());
//        return KStatus::FAIL;
//      }
//      break;
    }
  }

  return SUCCESS;
}

KStatus TsVGroup::ApplyWal(kwdbContext_p ctx, LogEntry* wal_log,
                 std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete) {
  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      std::string p_tag;
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        p_tag = log->getPrimaryTag();
        return redoPut(ctx, p_tag, log->getLSN(), log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return redoPutTag(ctx, log->getLSN(), log->getPayload());
      }
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;

      if (t_type == WALTableType::DATA) {
        auto log = reinterpret_cast<DeleteLogMetricsEntry*>(del_log);
        p_tag = log->getPrimaryTag();
        vector<DelRowSpan> partitions = log->getRowSpans();
        return redoDelete(ctx, p_tag, log->getLSN(), partitions);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        auto p_tag_slice = log->getPrimaryTag();
        auto tag_slice = log->getTags();
        return redoDeleteTag(ctx, p_tag_slice, log->getLSN(), log->group_id_, log->entity_id_, tag_slice);
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
      incomplete.insert(std::pair<TS_LSN, MTRBeginEntry*>(log->getXID(), log));
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

uint32_t TsVGroup::GetVGroupID() {
  return vgroup_id_;
}

KStatus TsVGroup::undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, TSSlice old_payload) {
  return KStatus::SUCCESS;
}

/**
 * Undoes deletion of rows within a specified entity group.
 *
 * @param ctx Pointer to the database context.
 * @param primary_tag Primary tag identifying the entity.
 * @param log_lsn LSN of the log for ensuring atomicity and consistency of the operation.
 * @param rows Collection of row spans to be undeleted.
 * @return Status of the operation, success or failure.
 */
KStatus TsVGroup::undoDelete(kwdbContext_p ctx, std::string& primary_tag, TS_LSN log_lsn,
                   const std::vector<DelRowSpan>& rows) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, TS_LSN log_lsn,
                      uint32_t group_id, uint32_t entity_id, TSSlice& tags) {
  return KStatus::SUCCESS;
}

/**
 * redoPut redo a put operation. This function is utilized during log recovery to redo a put operation.
 *
 * @param ctx The context of the operation.
 * @param primary_tag The primary tag associated with the data being operated on.
 * @param log_lsn The log sequence number indicating the position in the log of this operation.
 * @param payload The actual data payload being put into the database, provided as a slice.
 *
 * @return KStatus The status of the operation, indicating success or failure.
 */
KStatus TsVGroup::redoPut(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                const TSSlice& payload) {
  TsRawPayload p{payload};
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;
  uint32_t vgroup_id;
  TSEntityID entity_id;

  auto s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  if (new_tag) {
    LOG_WARN("cannot find tag info for this parimary key.");
    return KStatus::SUCCESS;
  }
  assert(vgroup_id == GetVGroupID());
  uint8_t payload_data_flag = p.GetRowType();
  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::DATA_ONLY) {
    s = mem_segment_mgr_.PutData(payload, entity_id, log_lsn);
  } else {
    LOG_WARN("no data need inserted.");
  }
  return s;
}

KStatus TsVGroup::redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p{payload};
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  auto s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
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
    s =  tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  } else {
    assert(vgroup_id == this->GetVGroupID());
  }
  return s;
}

KStatus TsVGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDelete(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                   const vector<DelRowSpan>& rows) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn,
                      uint32_t group_id, uint32_t entity_id, TSSlice& payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoCreateHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoCreateHashIndex(uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDropHashIndex(uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  return tsx_manager_->MtrBegin(ctx, range_id, index, mtr_id);
}

KStatus TsVGroup::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id) {
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  return tsx_manager_->MtrCommit(ctx, mtr_id);
}

KStatus TsVGroup::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip) {
//  1. Write ROLLBACK log;
  KStatus s;
  if (!is_skip) {
    s = tsx_manager_->MtrRollback(ctx, mtr_id);
    if (s == FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

std::shared_ptr<TsVGroupPartition> PartitionManager::Get(int64_t timestamp, bool create_if_not_exist) {
  int idx = timestamp / interval_;
  RW_LATCH_S_LOCK(&partitions_latch_);
  auto it = partitions_.find(idx);
  if (it == partitions_.end()) {
    RW_LATCH_UNLOCK(&partitions_latch_);
    if (!create_if_not_exist) {
      return nullptr;
    }

    int64_t start = idx * interval_;
    int64_t end = start + interval_;
    auto root = vgroup_->GetPath();
    RW_LATCH_X_LOCK(&partitions_latch_);
    it = partitions_.find(idx);
    if (it == partitions_.end()) {
      auto partition = std::make_shared<TsVGroupPartition>(root, database_id_, vgroup_->GetSchemaMgr(), start, end);
      partition->Open();
      partitions_[idx] = partition;
      RW_LATCH_UNLOCK(&partitions_latch_);
      return partition;
    }
  }
  RW_LATCH_UNLOCK(&partitions_latch_);
  return it->second;
}

std::vector<std::shared_ptr<TsVGroupPartition>> PartitionManager::GetAllPartitions() {
  std::vector<std::shared_ptr<TsVGroupPartition>> partitions;
  RW_LATCH_S_LOCK(&partitions_latch_);
  for (auto& kv : partitions_) {
    if (kv.second != nullptr) {
      partitions.push_back(kv.second);
    }
  }
  RW_LATCH_UNLOCK(&partitions_latch_);
  return partitions;
}

std::vector<std::shared_ptr<TsVGroupPartition>> PartitionManager::GetCompactPartitions() {
  std::vector<std::shared_ptr<TsVGroupPartition>> partitions;
  RW_LATCH_S_LOCK(&partitions_latch_);
  for (auto& kv : partitions_) {
    if (kv.second != nullptr && kv.second->NeedCompact()) {
      partitions.push_back(kv.second);
    }
  }
  RW_LATCH_UNLOCK(&partitions_latch_);
  return partitions;
}

}  //  namespace kwdbts
