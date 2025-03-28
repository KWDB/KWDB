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

#include "st_wal_table.h"
#include "st_logged_entity_group.h"
#include "sys_utils.h"
#include "st_tier_manager.h"

extern DedupRule g_dedup_rule;
namespace kwdbts {

LoggedTsTable::LoggedTsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id, EngineOptions* opt)
    : TsTable(ctx, db_path, table_id), engine_opt_(opt) {
    logged_tstable_rwlock_ = new LoggedTsTableRWLatch(RWLATCH_ID_LOGGED_TSTABLE_RWLOCK);
}

LoggedTsTable::~LoggedTsTable() {
  delete logged_tstable_rwlock_;
  logged_tstable_rwlock_ = nullptr;
}

KStatus LoggedTsTable::Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups, ErrorInfo& err_info) {
  return TsTable::Init(ctx, range_groups, err_info);
}

KStatus LoggedTsTable::CreateCheckpoint(kwdbContext_p ctx) {
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  auto entity_groups_repl = entity_groups_;
  RW_LATCH_UNLOCK(entity_groups_mtx_);
  for (const auto& tbl_range : entity_groups_repl) {
    KStatus s = tbl_range.second->CreateCheckpoint(ctx);
    if (s == FAIL) {
      // ignore the checkpoint error, record it into ts engine log.
      LOG_ERROR("Failed to create a checkpoint for entity group %lu of table id %lu",
                tbl_range.second->HashRange().range_group_id, table_id_)
    }
  }
  return SUCCESS;
}

KStatus LoggedTsTable::Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes) {
  RW_LATCH_S_LOCK(logged_tstable_rwlock_);
  for (const auto& tbl_range : entity_groups_) {
    std::shared_ptr<LoggedTsEntityGroup> entity_group = std::static_pointer_cast<LoggedTsEntityGroup>(tbl_range.second);
    KStatus s = entity_group->Recover(ctx, applied_indexes);
    if (s == FAIL) {
      // ignore the recover error, record it into ts engine log.
      LOG_ERROR("Failed to recover the entity group %lu of table %lu",
                entity_group->HashRange().range_group_id, table_id_)

#ifdef WITH_TESTS
      RW_LATCH_UNLOCK(logged_tstable_rwlock_);
      return s;
#endif
    }
  }

  RW_LATCH_UNLOCK(logged_tstable_rwlock_);
  return SUCCESS;
}

KStatus LoggedTsTable::FlushBuffer(kwdbContext_p ctx) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  for (const auto& tbl_range : entity_groups_) {
    KStatus s = tbl_range.second->FlushBuffer(ctx);
    if (s == FAIL) {
      // ignore the flush error, record it into ts engine log.
      LOG_ERROR("Failed to flush the WAL buffer for entity group %lu of table id %lu",
                tbl_range.second->HashRange().range_group_id, table_id_)
    }
  }

  RW_LATCH_UNLOCK(entity_groups_mtx_);
  return SUCCESS;
}

LoggedTsEntityGroup::LoggedTsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_bt_manager,
                                         const string& db_path, const KTableKey& table_id, const RangeGroup& range,
                                         const string& tbl_sub_path, EngineOptions* opt)
    : TsEntityGroup(ctx, root_bt_manager, db_path, table_id, range, tbl_sub_path), engine_opt_(opt) {
  wal_manager_ = KNEW WALMgr(db_path, table_id, range.range_group_id, opt);
  tsx_manager_ = KNEW TSxMgr(wal_manager_);
  optimistic_read_lsn_ = wal_manager_->FetchCurrentLSN();
  wal_manager_->SetCheckpointObject(this);
  logged_mutex_ = new LoggedTsEntityGroupLatch(LATCH_ID_LOGGED_TSSUBENTITY_GROUP_MUTEX);
}

LoggedTsEntityGroup::~LoggedTsEntityGroup() {
  if (tsx_manager_ != nullptr) {
    delete tsx_manager_;
    tsx_manager_ = nullptr;
  }

  if (wal_manager_ != nullptr) {
    delete wal_manager_;
    wal_manager_ = nullptr;
  }
  if (logged_mutex_ != nullptr) {
    delete logged_mutex_;
    logged_mutex_ = nullptr;
  }
}

KStatus LoggedTsEntityGroup::Create(kwdbContext_p ctx, vector<TagInfo>& tag_schema,  uint32_t ts_version) {
  EnterFunc();
  // Call supper class's Create method.
  KStatus s = TsEntityGroup::Create(ctx, tag_schema, ts_version);
  if (s == KStatus::FAIL) {
    Return(KStatus::FAIL)
  }

  // Create WAL manager.
  s = wal_manager_->Create(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to create the WAL manager for table id %lu", table_id_)
    Return(KStatus::FAIL)
  }

  Return(KStatus::SUCCESS)
}

KStatus LoggedTsEntityGroup::OpenInit(kwdbContext_p ctx) {
  EnterFunc()
  // Call supper class's OpenInit method.
  KStatus s = TsEntityGroup::OpenInit(ctx);
  if (s == KStatus::FAIL || new_tag_bt_ == nullptr) {
    LOG_ERROR("Failed to initialize the EntityGroup for table id %lu", table_id_)
    Return(KStatus::FAIL)
  }

  // Initialize WAL manager.
  s = wal_manager_->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager for table id %lu", table_id_)
    Return(KStatus::FAIL)
  }

  Return(KStatus::SUCCESS)
}

KStatus LoggedTsEntityGroup::PutEntity(kwdbContext_p ctx, TSSlice& payload, uint64_t mtr_id) {
  // require Tag lock. To prevent updating same Primary tag.
  // TODO(liuwei): Using EntityGroup.Mutex protect tag table temporarily. Need to replace with TagTable.Mutex
  // replace it in TsEntityGroup.PutEntity\PutData at the same time
  MUTEX_LOCK(logged_mutex_);
  ErrorInfo err_info;
  Payload pd(root_bt_manager_, payload);
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    MUTEX_UNLOCK(logged_mutex_);
    return KStatus::FAIL;
  }
  // 1. query tag table
  TSSlice tmp_slice = pd.GetPrimaryTag();
  uint32_t entityid, sub_groupid;
  if (new_tag_bt_->hasPrimaryKey(tmp_slice.data, tmp_slice.len, entityid, sub_groupid)) {
    TagTuplePack* tag_pack = new_tag_bt_->GenTagPack(tmp_slice.data, tmp_slice.len);
    if (UNLIKELY(nullptr == tag_pack)) {
      return KStatus::FAIL;
    }
    KStatus s = wal_manager_->WriteUpdateWAL(ctx, mtr_id, 0, 0, payload, tag_pack->getData());
    delete tag_pack;
    if (s == KStatus::FAIL) {
      MUTEX_UNLOCK(logged_mutex_);
      return s;
    }
    // update
    if (new_tag_bt_->UpdateTagRecord(pd, sub_groupid, entityid, err_info) < 0) {
      LOG_ERROR("Update tag record failed. error: %s ", err_info.errmsg.c_str());
      releaseTagTable();
      MUTEX_UNLOCK(logged_mutex_);
      return KStatus::FAIL;
    }
  }
  releaseTagTable();
  MUTEX_UNLOCK(logged_mutex_);
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::PutData(kwdbContext_p ctx, TSSlice* payloads, int length, uint64_t mtr_id,
                                     uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                                     DedupResult* dedup_result, DedupRule dedup_rule, bool writeWAL) {
  // Iterate through the payloads array and invoke the PutData method to ensure that all payloads are successfully written.
  for (int i = 0; i < length; i++) {
    KStatus s = LoggedTsEntityGroup::PutData(ctx, payloads[i], mtr_id, inc_entity_cnt,
                                             inc_unordered_cnt, dedup_result, dedup_rule, writeWAL);
    if (s == FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

// If wal is enabled, this interface will be used.
KStatus LoggedTsEntityGroup::PutData(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id,
                                     uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                                     DedupResult* dedup_result, DedupRule dedup_rule, bool write_wal) {
  ErrorInfo err_info;
  // 1. Check whether the timing table is available and construct the payload
  // The Payload struct encapsulates the interface for reading data, allowing for quick retrieval of corresponding data
  // during writing.
  // 2. Get or create the corresponding entity based on the primary_tag, and write the tag to the wal log
  // mini_trans_id is a mini transaction in the sequence table, ensuring the atomicity of a single raftLog write in the
  // replica layer.
  // 3. Lock the current LSN until the log is written to the cache. The LSN of a batch of payloads is consistent,
  // and the LSN is strictly increasing but not necessarily continuous
  // 4. Write the payload into the wal log, and then write the payload into the cache.
  // 5. The latest LSN that can be read for the last update
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("The target TS table is not available.");
    return KStatus::FAIL;
  }
  Defer defer{[&]() {
    releaseTagTable();
  }};

  uint32_t group_id, entity_id;
  // payload verification
  Payload pd(root_bt_manager_, payload);
  pd.dedup_rule_ = dedup_rule;
  {
    TSSlice tmp_slice = pd.GetPrimaryTag();
    // Locking, concurrency control, and the putTagData operation must not be executed repeatedly
    MUTEX_LOCK(logged_mutex_);
    Defer defer{[&]() { MUTEX_UNLOCK(logged_mutex_); }};
    if (!new_tag_bt_->hasPrimaryKey(tmp_slice.data, tmp_slice.len, entity_id, group_id)) {
      // not found
      ++(*inc_entity_cnt);
      if (write_wal) {
        // no need lock, lock inside.
        KStatus s = wal_manager_->WriteInsertWAL(ctx, mini_trans_id, 0, 0, payload);
        if (s == KStatus::FAIL) {
          LOG_ERROR("failed WriteInsertWAL for new tag.");
          return s;
        }
      }
      // put tag into tag table.
      std::string tmp_str = std::to_string(table_id_);
      uint64_t tag_hash = TsTable::GetConsistentHashId(tmp_str.data(), tmp_str.size());
      std::string primary_tags;
      err_info.errcode = ebt_manager_->AllocateEntity(primary_tags, tag_hash, &group_id, &entity_id);
      if (err_info.errcode < 0) {
        return KStatus::FAIL;
      }
      // insert tag table
      if (KStatus::SUCCESS != putTagData(ctx, group_id, entity_id, pd)) {
        return KStatus::FAIL;
      }
    }
  }

  if (pd.GetFlag() == Payload::TAG_ONLY) {
    return KStatus::SUCCESS;
  }

  if (pd.GetRowCount() == 0) {
    LOG_WARN("payload has no row to insert.");
    return KStatus::SUCCESS;
  }

  TS_LSN entry_lsn = 0;
  if (write_wal) {
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();

    pd.SetLsn(current_lsn);
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mini_trans_id, 0, 0, pd.GetPrimaryTag(), payload, entry_lsn);
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
    // Call putDataColumnar to write data into the specified entity by column
    s = putDataColumnar(ctx, group_id, entity_id, pd, inc_unordered_cnt, dedup_result);
    if (s == KStatus::FAIL) {
      return s;
    }
    // The TS data has been persisted, update the Optimistic Read LSN.
    SetOptimisticReadLsn(entry_lsn);
  } else {
    pd.SetLsn(1);
    KStatus s = putDataColumnar(ctx, group_id, entity_id, pd, inc_unordered_cnt, dedup_result);
    if (s == KStatus::FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::EvaluateDeleteData(kwdbContext_p ctx, const string& primary_tag,
                                                const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>& rows,
                                                uint64_t mtr_id) {
  uint64_t count = 0;
  return TsEntityGroup::DeleteData(ctx, primary_tag, 0, ts_spans, &rows, &count, mtr_id, true);
}

KStatus LoggedTsEntityGroup::DeleteData(kwdbContext_p ctx, const string& primary_tag, TS_LSN lsn,
                                        const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>* rows,
                                        uint64_t* count, uint64_t mtr_id, bool evaluate_del) {
  EnterFunc();

  // The method that generates DelTimePartition using time range needs to move MMapPartitionTable
  // need to guarantee that there is no other thread edit range data between
  // generating DelTimePartition and Deleting Data
  // Now guarantee it by Entity mutex

  // Lock current entity
  // TODO(liuwei) should lock the target Entity before DELETE action id DONE.
  uint32_t entity_id, subgroup_id;
  ErrorInfo err_info;
  new_tag_bt_->hasPrimaryKey(const_cast<char*>(primary_tag.c_str()), primary_tag.size(), entity_id, subgroup_id);
  std::vector<DelRowSpan> dtp_list;

  TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();

  KStatus s = EvaluateDeleteData(ctx, primary_tag, ts_spans, dtp_list, mtr_id);
  if (s == KStatus::FAIL) {
    Return(s)
  }

  s = wal_manager_->WriteDeleteMetricsWAL(ctx, mtr_id, primary_tag, ts_spans, dtp_list);
  if (s == KStatus::FAIL) {
    Return(s)
  }

  // Call the parent class's DeleteData
  s = TsEntityGroup::DeleteData(ctx, primary_tag, current_lsn, ts_spans, rows, count, mtr_id, false);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to delete data")
    Return(s)
  }

  // unlock current entity

  Return(KStatus::SUCCESS);
}

KStatus LoggedTsEntityGroup::DeleteEntities(kwdbContext_p ctx, const std::vector<std::string>& primary_tags,
                                            uint64_t* count, uint64_t mtr_id) {
  EnterFunc()
  *count = 0;
  for (const auto& p_tags : primary_tags) {
    // TODO(liuwei) should lock the current PRIMARY TAG before the DELETE action is DONE.
    uint32_t sub_group_id, entity_id;
    if (!new_tag_bt_->hasPrimaryKey(p_tags.data(), p_tags.size(), entity_id, sub_group_id)) {
      LOG_INFO("primary key[%s] dose not exist, no need to delete", p_tags.c_str())
      continue;
    }

    TagTuplePack* tag_pack = new_tag_bt_->GenTagPack(p_tags.data(), p_tags.size());
    if (UNLIKELY(nullptr == tag_pack)) {
      return KStatus::FAIL;
    }
    KStatus s = wal_manager_->WriteDeleteTagWAL(ctx, mtr_id, p_tags, sub_group_id, entity_id,
                                                tag_pack->getData());
    delete tag_pack;
    if (s == KStatus::FAIL) {
      Return(s)
    }

    // call the DeleteEntity of super class to trigger the DELETE function.
    // if any error, end the delete loop and return ERROR to the caller.
    KStatus status = DeleteEntity(ctx, p_tags, count, mtr_id);

    if (status == KStatus::FAIL) {
      LOG_ERROR("Failed to delete entity by primary key %s", p_tags.c_str())
      Return(KStatus::FAIL)
    }
  }

  Return(KStatus::SUCCESS);
}

KStatus LoggedTsEntityGroup::CreateCheckpointInternal(kwdbContext_p ctx) {
  EnterFunc()
  // get checkpoint_lsn
  TS_LSN checkpoint_lsn = wal_manager_->FetchCurrentLSN();

  if (new_tag_bt_ == nullptr || root_bt_manager_ == nullptr) {
    LOG_ERROR("Failed to fetch current LSN.")
    Return(FAIL)
  }

  // call Tag.flush, pass Checkpoint_LSN as parameter
  new_tag_bt_->sync_with_lsn(checkpoint_lsn);

  /**
   * Call the Flush method of the Metrics table with the input parameter Current LSN
   * During the recovery process, check the dat aof each Entity based on Checkpoint LSN to determine the offset of
   * each Entity at the Checkpoint
   * The Checkpoint log of this solution is smaller, but the recovery time is longer.
   *
   * Another solution is to record the offset of each Entity at Checkpoint moment in the Checkpoint logs
   * The offset is used to reset the current write offset of the corresponding Entity while recovery
   * In this solution, the Checkpoint log is large, but the recovery time is reduced.
   *
   * At present, solution 1 is used, and later performance evaluation is needed to determine the final scheme.
  **/
  ErrorInfo err_info;
  map<uint32_t, uint64_t> rows;
  if (root_bt_manager_->Sync(checkpoint_lsn, err_info) != 0) {
    LOG_ERROR("Failed to flush the Metrics table.")
    Return(FAIL)
  }
  // force sync metric data into files.
  ebt_manager_->sync(0);

  // update wal metadata lsn
  wal_manager_->CreateCheckpoint(ctx);

  // construct CHECKPOINT log entry
  TS_LSN entry_lsn;
  KStatus s = wal_manager_->WriteCheckpointWAL(ctx, 0, entry_lsn);

  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to construct/write checkpoint WAL Log Entry.")
    Return(s)
  }

  // TODO(liuwei) check whether entry_lsn is equal to checkpoint_lsn. if not, need to Re-Checkpoint
  s = wal_manager_->Flush(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to flush the WAL log buffer to ensure the checkpoint is done.")
    Return(s)
  }

  Return(SUCCESS);
}

KStatus LoggedTsEntityGroup::CreateCheckpoint(kwdbContext_p ctx) {
  EnterFunc()
  // lock
  MUTEX_LOCK(logged_mutex_);
  wal_manager_->Lock();


  if (CreateCheckpointInternal(ctx) == FAIL) {
    wal_manager_->Unlock();
    MUTEX_UNLOCK(logged_mutex_);
    Return(FAIL)
  }

  // After Flush CHECKPOINT log, release walMgr mutex. No other WAL operations are allowed while Checkpoint.
  wal_manager_->Unlock();
  MUTEX_UNLOCK(logged_mutex_);
  Return(SUCCESS)
}

KStatus LoggedTsEntityGroup::BeginSnapshotMtr(kwdbContext_p ctx, uint64_t range_id, uint64_t index,
            const SnapshotRange& range, uint64_t &mtr_id) {
  KStatus s = MtrBegin(ctx, range_id, index, mtr_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginSnapshotMtr failed during MtrBegin.")
    return s;
  }
  s = wal_manager_->WriteSnapshotWAL(ctx, mtr_id, range.table_id, range.hash_span.begin, range.hash_span.end, range.ts_span);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginSnapshotMtr failed during WriteSnapshotWAL.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::BeginPartitionTierChangeMtr(kwdbContext_p ctx, uint64_t range_id,
                                              std::string link_path, std::string tier_path, uint64_t &mtr_id) {
  KStatus s = MtrBegin(ctx, range_id, UINT64_MAX, mtr_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginPartitionTierChangeMtr failed during MtrBegin.")
    return s;
  }
  s = wal_manager_->WritePartitionTierWAL(ctx, mtr_id, link_path, tier_path);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginPartitionTierChangeMtr failed during WritePartitionTierWAL.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::WriteTempDirectoryLog(kwdbContext_p ctx, uint64_t mtr_id, std::string path) {
  auto s = wal_manager_->WriteTempDirectoryWAL(ctx, mtr_id, path);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("WriteTempDirectoryLog failed during WriteTempDirectoryWAL.")
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes) {
  EnterFunc()
  KStatus s;
//  1. Check the latest CHECKPOINT_LSN of the current EntityGroup and play back logs from this point
//     until the last valid WAL log.
//  2. If a Mini-Transaction BEGIN log is encountered, record the xID of this Mini-Transaction to the unfinished
//     transaction list, and apply subsequent logs according to normal logic.
//  3. If the COMMIT log of Mini-Transaction is encountered, clear the corresponding record in the list of
//     uncompleted transactions.
//  4. If you encounter a Mini-Transaction ROLLBACK log, clear the records in the list of uncompleted transactions
//     and proceed to the rollback process based on the xID.
//  5. After WAL reads, if the list of outstanding transactions is not empty:
//     1) If the Index of the Range to which mini-transaction belongs is <= applied index, raftlog of the replica layer
//     has been applied successfully. You only need to add the corresponding COMMIT log entries.
//     2) Otherwise, an error occurs at the copy layer and the ROLLBACK operation is not sent to the storage layer.
//     In this case, the Recover operation must call the ROLLBACK method to roll back data. The replica layer reissues
//     this raftlog after TsEngine starts/recovers successfully.

  std::unordered_map<TS_LSN, MTRBeginEntry*> incomplete;

  TS_LSN checkpoint_lsn = wal_manager_->FetchCheckpointLSN();
  TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();

  std::vector<LogEntry*> redo_logs;
  Defer defer{[&]() {
    for (auto& log : redo_logs) {
      delete log;
    }
  }};

  s = wal_manager_->ReadWALLog(redo_logs, checkpoint_lsn, current_lsn);
  if (s == FAIL && !redo_logs.empty()) {
    Return(s)
  }

  for (auto& log : redo_logs) {
    s = applyWal(ctx, log, incomplete);
    if (s == FAIL) Return(s)
  }

  s = wal_manager_->ResetWAL(ctx);
  if (s == FAIL) {
    Return(s)
  }

  for (auto& it : incomplete) {
    TS_LSN mtr_id = it.first;
    auto log_entry = it.second;
    uint64_t applied_index = GetAppliedIndex(log_entry->getRangeID(), applied_indexes);
    if (it.second->getIndex() <= applied_index) {
      s = tsx_manager_->MtrCommit(ctx, mtr_id);
      if (s == FAIL) Return(s)
    } else {
      s = MtrRollback(ctx, mtr_id);
      if (s == FAIL) Return(s)
    }
  }

  Return(KStatus::SUCCESS)
}

KStatus LoggedTsEntityGroup::applyWal(kwdbContext_p ctx, LogEntry* wal_log,
                                      std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete) {
  KStatus s;
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
      s = wal_manager_->CreateCheckpointWithoutFlush(ctx);
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
    case WALLogType::MTR_ROLLBACK: {
      auto log = reinterpret_cast<MTREntry*>(wal_log);
      auto x_id = log->getXID();

      s = MtrRollback(ctx, x_id, true);
      if (s == FAIL) return s;

      incomplete.erase(log->getXID());
      break;
    }
    default:
      break;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::Drop(kwdbContext_p ctx, bool is_force) {
  EnterFunc()
  KStatus status = TsEntityGroup::Drop(ctx, is_force);
  if (status == SUCCESS) {
    // cleanup WAL resources.
    if (tsx_manager_ != nullptr) {
      delete tsx_manager_;
      tsx_manager_ = nullptr;
    }

    if (wal_manager_ != nullptr) {
      wal_manager_->Drop();
      delete wal_manager_;
      wal_manager_ = nullptr;
    }

    Return(SUCCESS)
  }
  Return(FAIL)
}

KStatus LoggedTsEntityGroup::FlushBuffer(kwdbContext_p ctx) {
  wal_manager_->Lock();
  KStatus status = wal_manager_->Flush(ctx);
  wal_manager_->Unlock();
  return status;
}

KStatus LoggedTsEntityGroup::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  return tsx_manager_->MtrBegin(ctx, range_id, index, mtr_id);
}

KStatus LoggedTsEntityGroup::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id) {
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  return tsx_manager_->MtrCommit(ctx, mtr_id);
}

KStatus LoggedTsEntityGroup::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool skip_log) {
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
    s = tsx_manager_->MtrRollback(ctx, mtr_id);
    if (s == FAIL) Return(s)
  }

  std::vector<LogEntry*> wal_logs;
  s = wal_manager_->ReadWALLogForMtr(mtr_id, wal_logs);
  if (s == FAIL && !wal_logs.empty()) {
    Return(s)
  }

  std::reverse(wal_logs.begin(), wal_logs.end());
  for (auto wal_log : wal_logs) {
    if (wal_log->getXID() == mtr_id && s != FAIL) {
      s = rollback(ctx, wal_log);
    }

    delete wal_log;
  }

  Return(s)
}

KStatus LoggedTsEntityGroup::rollback(kwdbContext_p ctx, LogEntry* wal_log) {
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
      auto snapshot_log = reinterpret_cast<SnapshotEntry*>(wal_log);
      if (snapshot_log == nullptr) {
        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
        return KStatus::FAIL;
      }
      HashIdSpan hash_span;
      KwTsSpan ts_span;
      snapshot_log->GetRangeInfo(&hash_span, &ts_span);
      uint64_t count = 0;
      auto s = DeleteRangeData(ctx, hash_span, 0, {ts_span}, nullptr, &count, snapshot_log->getXID(), false);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR(" WAL rollback snapshot delete range data faild.");
        return KStatus::FAIL;
      }
      break;
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
      auto tier_log = reinterpret_cast<PartitionTierChangeEntry*>(wal_log);
      if (tier_log == nullptr) {
        LOG_ERROR(" WAL rollback cannot prase partition tier log.");
        return KStatus::FAIL;
      }
      ErrorInfo err_info;
      auto s = TsTierPartitionManager::GetInstance().Recover(tier_log->GetLinkPath(), tier_log->GetTierPath(), err_info);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR(" WAL rollback partition tier change faild. %s", err_info.errmsg.c_str());
        return KStatus::FAIL;
      }
      break;
    }
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::undoPut(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {
  Payload pd(root_bt_manager_, payload);
  auto primary_tag = pd.GetPrimaryTag();
  uint32_t entity_id{0}, group_id{0};
  if (!new_tag_bt_->hasPrimaryKey(primary_tag.data, primary_tag.len, entity_id, group_id)) {
    LOG_WARN("undoPut: can not find primary tag: [%s]", primary_tag.data);
    return KStatus::SUCCESS;
  }

  KwTsSpan span{};
  int32_t row_count = pd.GetRowCount();
  if (row_count < 1) {
    return KStatus::SUCCESS;
  }
  ErrorInfo err_info;
  auto sub_manager = GetSubEntityGroupManager();
  TsSubEntityGroup* sub_group = sub_manager->GetSubGroup(group_id, err_info, false);
  if (err_info.errcode < 0) {
    LOG_ERROR("undoPut error : no subgroup %u", group_id);
    return KStatus::FAIL;
  }
  sub_group->MutexLockEntity(entity_id);
  Defer defer{[&]() {
    sub_group->MutexUnLockEntity(entity_id);
  }};

  timestamp64 last_p_time = INVALID_TS;
  TsTimePartition* p_bt = nullptr;
  int batch_start = pd.GetStartRowId();
  timestamp64 p_time = 0;

  // Examine the timestamp of the first piece of data within the payload to verify the partition being written to
  KTimestamp first_ts_p = pd.GetTimestamp(pd.GetStartRowId());
  KTimestamp first_ts = convertTsToPTime(first_ts_p, root_bt_manager_->GetTsColDataType());
  timestamp64 first_max_ts;

  last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);
  k_int32 row_id = 0;

  // Based on the obtained partition timestamp, determine whether the current payload needs to switch partitions.
  // If necessary, first call UndoPut to write the current partition data.
  // Then continue to call nextPayload until the traversal of the Payload data is complete.
  while (payloadNextSlice(sub_group, pd, last_p_time, batch_start, &row_id, &p_time)) {
    p_bt = ebt_manager_->GetPartitionTable(last_p_time, group_id, err_info, true);
    if (err_info.errcode == KWEDROPPEDOBJ) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      LOG_ERROR("GetPartitionTable error: %s", err_info.errmsg.c_str());
      err_info.clear();
      batch_start = row_id;
      continue;
    }
    if (err_info.errcode < 0) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      LOG_ERROR("GetPartitionTable error: %s", err_info.errmsg.c_str());
      return KStatus::FAIL;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition[%s] is invalid, unable to execute UndoPut", p_bt->tbl_sub_path().c_str());
      ebt_manager_->ReleasePartitionTable(p_bt);
      batch_start = row_id;
      continue;
    }
    last_p_time = p_time;

    // UndoPut undoes a previously performed put operation for a given entity.
    err_info.errcode = p_bt->UndoPut(entity_id, log_lsn, batch_start, row_id - batch_start, &pd, err_info);

    if (err_info.errcode < 0) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      LOG_ERROR("UndoPut error: %s", err_info.errmsg.c_str());
      return KStatus::FAIL;
    }
    batch_start = row_id;
    ebt_manager_->ReleasePartitionTable(p_bt);
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::redoPut(kwdbContext_p ctx, string& primary_tag, kwdbts::TS_LSN log_lsn,
                                     const TSSlice& payload) {
  uint32_t entity_id{0}, group_id{0};
  if (!new_tag_bt_->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, group_id)) {
    LOG_WARN("redoPut: can not find primary tag: [%s]", primary_tag.c_str());
    return KStatus::SUCCESS;
  }

  Payload pd(root_bt_manager_, payload);
  pd.dedup_rule_ = g_dedup_rule;
  ErrorInfo err_info;
  auto sub_manager = GetSubEntityGroupManager();
  TsSubEntityGroup* sub_group = sub_manager->GetSubGroup(group_id, err_info, false);
  if (err_info.errcode < 0) {
    LOG_ERROR("LoggedTsEntityGroup::redoPut error : no subgroup %u", group_id);
    return KStatus::FAIL;
  }
  sub_group->MutexLockEntity(entity_id);
  Defer defer{[&]() {
    sub_group->MutexUnLockEntity(entity_id);
  }};

  timestamp64 last_p_time = INVALID_TS;
  TsTimePartition* p_bt = nullptr;
  int batch_start = pd.GetStartRowId();
  timestamp64 p_time = 0;

  bool all_success = true;
  std::vector<BlockSpan> alloc_spans;

  unordered_map<TsTimePartition*, PutAfterProcessInfo*> after_process_info;
  std::unordered_set<timestamp64> dup_set;

  // Examine the timestamp of the first piece of data within the payload to verify the partition being written to
  KTimestamp first_ts_p = pd.GetTimestamp(pd.GetStartRowId());
  KTimestamp first_ts = convertTsToPTime(first_ts_p, root_bt_manager_->GetTsColDataType());
  timestamp64 first_max_ts;

  last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);
  k_int32 row_id = 0;
  std::unordered_map<KTimestamp, MetricRowID> partition_ts_map;

  // Based on the obtained partition timestamp, determine whether the current payload needs to switch partitions.
  // If necessary, first call RedoPut to write the current partition data.
  // Then continue to call nextPayload until the traversal of the Payload data is complete.
  while (payloadNextSlice(sub_group, pd, last_p_time, batch_start, &row_id, &p_time)) {
    p_bt = ebt_manager_->GetPartitionTable(last_p_time, group_id, err_info, true);
    if (err_info.errcode == KWEDROPPEDOBJ) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      LOG_WARN("GetPartitionTable error: %s", err_info.errmsg.c_str());
      err_info.clear();
      batch_start = row_id;
      continue;
    }
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTable error: %s", err_info.errmsg.c_str());
      all_success = false;
      ebt_manager_->ReleasePartitionTable(p_bt);
      break;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition[%s] is invalid, unable to execute RedoPut", p_bt->tbl_sub_path().c_str());
      ebt_manager_->ReleasePartitionTable(p_bt);
      batch_start = row_id;
      continue;
    }
    last_p_time = p_time;

    std::vector<BlockSpan> cur_alloc_spans;
    std::vector<MetricRowID> to_deleted_real_rows;

    err_info.errcode = p_bt->RedoPut(ctx, entity_id, log_lsn, batch_start, row_id - batch_start, &pd,
                                     &cur_alloc_spans, &to_deleted_real_rows, &partition_ts_map, p_time, err_info);

    // After handling a partition, record the information to be processed subsequently
    recordPutAfterProInfo(after_process_info, p_bt, cur_alloc_spans, to_deleted_real_rows);

    if (err_info.errcode < 0) {
      LOG_ERROR("RedoPut error: %s", err_info.errmsg.c_str());
      all_success = false;
      break;
    }
    batch_start = row_id;
  }

  // Writing completed, processing after_process_info recorded earlier
  putAfterProcess(after_process_info, entity_id, all_success);

  if (!all_success) {
    if (err_info.errcode == KWEDUPREJECT) {
      return KStatus::SUCCESS;
    }
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::undoDelete(kwdbContext_p ctx, string& primary_tag, TS_LSN log_lsn,
                                        const std::vector<DelRowSpan>& rows) {
  uint32_t entity_id{0}, subgroup_id{0};
  if (!new_tag_bt_->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, subgroup_id)) {
    LOG_WARN("undoDelete: can not find primary tag: [%s]", primary_tag.c_str());
    return KStatus::SUCCESS;
  }

  map<timestamp64, vector<DelRowSpan>> partition_del_rows;
  for (auto& row_span : rows) {
    auto iter = partition_del_rows.find(row_span.partition_ts);
    if (iter == partition_del_rows.end()) {
      partition_del_rows[row_span.partition_ts] = std::vector<DelRowSpan>{row_span};
    } else {
      iter->second.emplace_back(row_span);
    }
  }
  ErrorInfo err_info;
  for (auto& del_rows : partition_del_rows) {
    TsTimePartition* p_bt;
    p_bt = ebt_manager_->GetPartitionTable(del_rows.first, subgroup_id, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTable error : %s", err_info.errmsg.c_str());
      ebt_manager_->ReleasePartitionTable(p_bt);
      if (err_info.errcode == KWEDROPPEDOBJ) {
        err_info.clear();
        continue;
      }
      return KStatus::FAIL;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition is invalid.");
      err_info.setError(KWEDUPREJECT, "Partition is invalid.");
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    int res = p_bt->UndoDelete(entity_id, log_lsn, &(del_rows.second), err_info);
    if (res < 0) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      return KStatus::FAIL;
    }

    ebt_manager_->ReleasePartitionTable(p_bt);
  }

  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::redoDelete(kwdbContext_p ctx, string& primary_tag, kwdbts::TS_LSN log_lsn,
                                        const vector<DelRowSpan>& rows) {
  uint32_t entity_id{0}, subgroup_id{0};
  if (!new_tag_bt_->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, subgroup_id)) {
    LOG_WARN("redoDelete: can not find primary tag: [%s]", primary_tag.c_str());
    return KStatus::SUCCESS;
  }

  map<timestamp64, vector<DelRowSpan>> partition_del_rows;
  for (auto& row_span : rows) {
    auto iter = partition_del_rows.find(row_span.partition_ts);
    if (iter == partition_del_rows.end()) {
      partition_del_rows[row_span.partition_ts] = std::vector<DelRowSpan>{row_span};
    } else {
      iter->second.emplace_back(row_span);
    }
  }
  ErrorInfo err_info;
  for (auto& del_rows : partition_del_rows) {
    TsTimePartition* p_bt;
    p_bt = ebt_manager_->GetPartitionTable(del_rows.first, subgroup_id, err_info);
    if (err_info.errcode < 0) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      if (err_info.errcode == KWEDROPPEDOBJ) {
        err_info.clear();
        continue;
      }
      LOG_ERROR("GetPartitionTable error : %s", err_info.errmsg.c_str());
      return KStatus::FAIL;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition is invalid.");
      err_info.setError(KWEDUPREJECT, "Partition is invalid.");
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    int res = p_bt->RedoDelete(entity_id, log_lsn, &(del_rows.second), err_info);
    if (res < 0) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      return KStatus::FAIL;
    }

    ebt_manager_->ReleasePartitionTable(p_bt);
  }

  return KStatus::SUCCESS;
}

KStatus LoggedTsEntityGroup::undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {
  Payload pd(root_bt_manager_, payload);
  auto p_tag = pd.GetPrimaryTag();

  uint32_t entity_id, group_id;
  ErrorInfo err_info;
  if (!new_tag_bt_->hasPrimaryKey(p_tag.data, p_tag.len, entity_id, group_id)) {
    LOG_WARN("undoPutTag: can not find primary tag[%s].", p_tag.data)
    return KStatus::SUCCESS;
  }

  int res = new_tag_bt_->InsertForUndo(group_id, entity_id, p_tag);
  if (res < 0) {
    LOG_ERROR("undoPutTag: InsertForUndo failed, primary tag[%s]", p_tag.data)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  Payload pd(root_bt_manager_, payload);
  auto p_tag = pd.GetPrimaryTag();

  uint32_t entity_id, group_id;
  ErrorInfo err_info;
  if (!new_tag_bt_->hasPrimaryKey(p_tag.data, p_tag.len, entity_id, group_id)) {
    std::string tmp_str = std::to_string(table_id_);
    uint64_t tag_hash = TsTable::GetConsistentHashId(tmp_str.data(), tmp_str.size());
    std::string primary_tags;
    err_info.errcode = ebt_manager_->AllocateEntity(primary_tags, tag_hash, &group_id, &entity_id);
  }

  if (new_tag_bt_->InsertForRedo(group_id, entity_id, pd) < 0) {
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, TSSlice old_payload) {
  Payload pd(root_bt_manager_, payload);
  auto p_tag = pd.GetPrimaryTag();

  uint32_t entity_id, group_id;
  ErrorInfo err_info;
  if (!new_tag_bt_->hasPrimaryKey(p_tag.data, p_tag.len, entity_id, group_id)) {
    LOG_ERROR("undoUpdateTag: can not find primary tag[%s].", p_tag.data)
    return KStatus::FAIL;
  }

  if (new_tag_bt_->UpdateForUndo(group_id, entity_id, p_tag, old_payload) < 0) {
    LOG_ERROR("undoUpdateTag: UpdateForUndo failed, primary tag[%s].", p_tag.data)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  Payload pd(root_bt_manager_, payload);
  auto p_tag = pd.GetPrimaryTag();

  uint32_t entity_id, group_id;
  ErrorInfo err_info;
  if (!new_tag_bt_->hasPrimaryKey(p_tag.data, p_tag.len, entity_id, group_id)) {
    LOG_ERROR("redoUpdateTag has not primary tag[%s].", p_tag.data)
    return KStatus::FAIL;
  }

  int res;
  res = new_tag_bt_->UpdateForRedo(group_id, entity_id, p_tag, pd);
  if (res < 0) {
    LOG_ERROR("redoUpdateTag: UpdateForRedo failed, primary tag[%s].", p_tag.data)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::undoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, TS_LSN log_lsn,
                                           uint32_t group_id, uint32_t entity_id, TSSlice& tags) {
  int res = new_tag_bt_->DeleteForUndo(group_id, entity_id, primary_tag, tags);
  if (res < 0) {
    return KStatus::FAIL;
  }

  uint64_t count;
  ErrorInfo err_info;
  auto sub_manager = GetSubEntityGroupManager();
  TsSubEntityGroup* sub_group = sub_manager->GetSubGroup(group_id, err_info, false);
  if (err_info.errcode < 0) {
    LOG_ERROR("undoDeleteTag error : no subgroup %u", group_id);
    return KStatus::FAIL;
  }
  sub_group->MutexLockEntity(entity_id);
  Defer defer{[&]() {
    sub_group->MutexUnLockEntity(entity_id);
  }};
  res = sub_group->UndoDeleteEntity(entity_id, log_lsn, &count, err_info);
  if (res) {
    LOG_ERROR("undoDeleteTag: UndoDeleteEntity failed, errcode:%d", res)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus LoggedTsEntityGroup::redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn,
                                           uint32_t group_id, uint32_t entity_id, TSSlice& tags) {
  ErrorInfo err_info;
  int res = new_tag_bt_->DeleteForRedo(group_id, entity_id, primary_tag, tags);
  if (res) {
    return KStatus::FAIL;
  }

  // clean up entities
  uint64_t count;
  TsSubEntityGroup* p_bt = ebt_manager_->GetSubGroup(group_id, err_info, false);
  if (p_bt) {
    p_bt->DeleteEntity(entity_id, 0, &count, err_info);
  }

  return SUCCESS;
}

TS_LSN LoggedTsEntityGroup::FetchCurrentLSN() {
  return wal_manager_->FetchCurrentLSN();
}

}  //  namespace kwdbts
