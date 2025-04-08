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
#include <memory>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "sys_utils.h"
#include "ts_iterator_v2_impl.h"
#include "ts_lastsegment_builder.h"
#include "ts_vgroup_partition.h"

namespace kwdbts {

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

  return KStatus::SUCCESS;
}

KStatus TsVGroup::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  // no need do anything.
  return KStatus::SUCCESS;
}

KStatus TsVGroup::PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag,
                          TSEntityID entity_id, TSSlice* payload) {
  TS_LSN current_lsn = 1;
  if (engine_options_.wal_level != WALMode::OFF) {
    TS_LSN entry_lsn = 0;
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mtr_id, 0, 0, *primary_tag, *payload, entry_lsn);
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

KStatus TsVGroup::WriteCheckpointWALAndUpdateLSN(kwdbContext_p ctx, TS_LSN chk_lsn) {
  // 1.UpdateLSN
  KStatus s = wal_manager_->UpdateCheckpointWithoutFlush(ctx, chk_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteCheckpointWAL.")
    return FAIL;
  }
  // 2.WriteCheckpointWAL
  TS_LSN lsn;
  s = wal_manager_->WriteCheckpointWAL(ctx, 0, lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteCheckpointWAL.")
    return FAIL;
  }
  // 3. remove chk file
  s = wal_manager_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteCheckpointWAL.")
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn) {
  // 1. read chk wal log
  // 2. switch new file
  wal_manager_->Lock();
  KStatus  s = wal_manager_->ReadWALLogAndSwitchFile(logs, wal_manager_->FetchCheckpointLSN(),
                                                     wal_manager_->FetchCurrentLSN());
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
  KStatus  s = wal_manager_->ReadWALLog(logs, wal_manager_->FetchCheckpointLSN(),
                                                     wal_manager_->FetchCurrentLSN());
  last_lsn = wal_manager_->FetchCurrentLSN();
  wal_manager_->Unlock();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLogAndSwitchFile.")
  }
  return s;
}

KStatus TsVGroup::ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs) {
  return wal_manager_->ReadWALLogForMtr(mtr_trans_id, logs);
}

KStatus TsVGroup::CreateCheckpointInternal(kwdbContext_p ctx) {


}
//KStatus TsVGroup::CreateCheckpointInternal(kwdbContext_p ctx) {
//  // get checkpoint_lsn
//  TS_LSN checkpoint_lsn = wal_manager_->FetchCurrentLSN();
//
//  if (new_tag_bt_ == nullptr || root_bt_manager_ == nullptr) {
//    LOG_ERROR("Failed to fetch current LSN.")
//    return FAIL;
//  }
//
//  // call Tag.flush, pass Checkpoint_LSN as parameter
//  new_tag_bt_->sync_with_lsn(checkpoint_lsn);
//
//  /**
//   * Call the Flush method of the Metrics table with the input parameter Current LSN
//   * During the recovery process, check the dat aof each Entity based on Checkpoint LSN to determine the offset of
//   * each Entity at the Checkpoint
//   * The Checkpoint log of this solution is smaller, but the recovery time is longer.
//   *
//   * Another solution is to record the offset of each Entity at Checkpoint moment in the Checkpoint logs
//   * The offset is used to reset the current write offset of the corresponding Entity while recovery
//   * In this solution, the Checkpoint log is large, but the recovery time is reduced.
//   *
//   * At present, solution 1 is used, and later performance evaluation is needed to determine the final scheme.
//  **/
//  ErrorInfo err_info;
//  map<uint32_t, uint64_t> rows;
//  if (root_bt_manager_->Sync(checkpoint_lsn, err_info) != 0) {
//    LOG_ERROR("Failed to flush the Metrics table.")
//    return FAIL;
//  }
//  // force sync metric data into files.
//  ebt_manager_->sync(0);
//
//  // update wal metadata lsn
//  wal_manager_->CreateCheckpoint(ctx);
//
//  // construct CHECKPOINT log entry
//  TS_LSN entry_lsn;
//  KStatus s = wal_manager_->WriteCheckpointWAL(ctx, 0, entry_lsn);
//
//  if (s == KStatus::FAIL) {
//    LOG_ERROR("Failed to construct/write checkpoint WAL Log Entry.")
//    return s;
//  }
//
//  // TODO(liuwei) check whether entry_lsn is equal to checkpoint_lsn. if not, need to Re-Checkpoint
//  s = wal_manager_->Flush(ctx);
//  if (s == KStatus::FAIL) {
//    LOG_ERROR("Failed to flush the WAL log buffer to ensure the checkpoint is done.")
//    return s;
//  }
//
//  return SUCCESS;
//}

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
      uint64_t interval = 3600 * 24 * 30;  // 30 days.
      partitions_[database_id] = std::make_unique<PartitionManager>(this, database_id, interval);
    }
    partition_manager = partitions_[database_id].get();
    RW_LATCH_UNLOCK(&partitions_latch_);
  }
  return partition_manager->Get(p_time, true);
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
    std::vector<std::shared_ptr<TsVGroupPartition>> v = partition.second->GetPartitionArray();
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
      auto s = last_row_info.schema_mgr->GetMetricAttr(last_row_info.info, tbl->table_version);
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
      ts_iter = new TsSortedRowDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
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
}

uint32_t TsVGroup::GetVGroupID() {
  return vgroup_id_;
}

KStatus TsVGroup::undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {

}

KStatus TsVGroup::undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, TSSlice old_payload) {

}

/**
 * @brief undoPut undo a put operation. This function is used to undo a previously executed put operation.
 *
 * @param ctx The context of the database, providing necessary environment for the operation.
 * @param log_lsn The log sequence number identifying the specific log entry to be undone.
 * @param payload A slice of the transaction log containing the data needed to reverse the put operation.
 *
 * @return KStatus The status of the undo operation, indicating success or specific failure reasons.
 */
KStatus TsVGroup::undoPut(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {

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

}

KStatus TsVGroup::undoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, TS_LSN log_lsn,
                      uint32_t group_id, uint32_t entity_id, TSSlice& tags) {

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

}

KStatus TsVGroup::redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {

}

KStatus TsVGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {

}

KStatus TsVGroup::redoDelete(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                   const vector<DelRowSpan>& rows) {

}

KStatus TsVGroup::redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn,
                      uint32_t group_id, uint32_t entity_id, TSSlice& payload) {

}

KStatus TsVGroup::redoCreateHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version) {

}

KStatus TsVGroup::undoCreateHashIndex(uint32_t index_id, uint32_t ts_version) {

}

KStatus TsVGroup::redoDropHashIndex(uint32_t index_id, uint32_t ts_version) {

}

KStatus TsVGroup::undoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version) {

}

//TsVGroup::TsPartitionedFlush::TsPartitionedFlush(TsVGroup* group, rocksdb::InternalIterator* iter)
//    : vgroup_(group), iter_(iter) {}
//
//rocksdb::Status TsVGroup::TsPartitionedFlush::FlushFromMem() {
//  iter_->SeekToFirst();
//  TsEngineSchemaManager* schema_mgr = vgroup_->schema_mgr_;
//
//  rocksdb::FullKey full_key;
//  TsInternalKey ts_key;
//  std::unordered_map<TsVGroupPartition*, TsLastSegmentBuilder> builders;
//
//  std::shared_ptr<MMapMetricsTable> table_schema;
//  std::vector<AttributeInfo> metric_schema;
//
//  std::unique_ptr<TsRawPayloadRowParser> parser;
//  TSTableID last_table_id = -1;
//  uint32_t last_version = -1;
//  uint32_t db_id = -1;
//  TsVGroupPartition* partition = nullptr;
//
//  for (; iter_->Valid(); iter_->Next()) {
//    rocksdb::ParseFullKey(iter_->key(), &full_key);
//    rocksdb::SequenceNumber seq_no = full_key.sequence;
//    ts_key.Decode(full_key.user_key);
//    if (last_table_id != ts_key.table_id) {
//      db_id = schema_mgr->GetDBIDByTableID(ts_key.table_id);
//    }
//
//    if (!(last_table_id == ts_key.table_id && last_version == ts_key.version)) {
//      schema_mgr->GetTableMetricSchema(nullptr, ts_key.table_id, ts_key.version, &table_schema);
//      assert(table_schema != nullptr);
//      metric_schema = table_schema->getSchemaInfoExcludeDropped();
//      parser = std::make_unique<TsRawPayloadRowParser>(metric_schema);
//      last_table_id = ts_key.table_id;
//      last_version = ts_key.version;
//    }
//
//    auto val = iter_->value();
//    assert(!metric_schema.empty());
//    // TsRawPayload payload_prev{{const_cast<char*>(val.data()), val.size()}, metric_schema};
//    TsRawPayloadV2 payload{{const_cast<char*>(val.data()), val.size()}};
//    auto row_iter = payload.GetRowIterator();
//
//    int row_cnt = 0;
//    for (; row_iter.Valid(); row_iter.Next()) {
//      auto row_data = row_iter.Value();
//      ++row_cnt;
//      timestamp64 ts = parser->GetTimestamp(row_data);
//      if (partition == nullptr || ts >= partition->EndTs() || ts < partition->StartTs()) {
//        partition = this->vgroup_->GetPartition(db_id, ts, (DATATYPE)metric_schema[0].type);
//      }
//
//      auto it = builders.find(partition);
//      if (it == builders.end()) {
//        std::shared_ptr<TsLastSegment> last_segment;
//        partition->NewLastSegment(last_segment);
//        auto result =
//            builders.insert({partition, TsLastSegmentBuilder{schema_mgr, last_segment}});
//        it = result.first;
//      }
//
//      TsLastSegmentBuilder& builder = it->second;
//      auto s =
//          builder.PutRowData(ts_key.table_id, ts_key.version, ts_key.entity_id, seq_no, row_data);
//      if (s != SUCCESS) {
//        return rocksdb::Status::Incomplete("flush error");
//      }
//    }
//
//    int nrows = payload.GetRowCount();
//    assert(nrows == row_cnt);
//    // assert(payload.GetRowCount() == payload_prev.GetRowCount());
//  }
//  for (auto& kv : builders) {
//    auto s = kv.second.Finalize();
//    if (s == FAIL) return rocksdb::Status::Incomplete("flush error");
//    kv.second.Flush();
//  }
//  return rocksdb::Status::OK();
//}

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

std::vector<std::shared_ptr<TsVGroupPartition>> PartitionManager::GetPartitionArray() {
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

}  //  namespace kwdbts
