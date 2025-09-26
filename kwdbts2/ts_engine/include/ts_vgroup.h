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
#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

#include "data_type.h"
#include "iterator.h"
#include "kwdb_type.h"
#include "st_transaction_mgr.h"
#include "st_wal_mgr.h"
#include "ts_engine_schema_manager.h"
#include "ts_mem_segment_mgr.h"
#include "ts_version.h"

namespace kwdbts {

class TsEntitySegmentBuilder;

enum class TsEntityLatestRowStatus {
  Uninitialized = 0,
  Recovering,
  Valid,
};

/**
 * table group used for organizing a series of table(super table of device).
 * in current time vgroup is same as database
 */
// const pointer
class TsVGroup {
 private:
  uint32_t vgroup_id_{0};
  TsEngineSchemaManager* schema_mgr_ = nullptr;

  std::shared_mutex s_mu_;
  TsHashRWLatch* tag_lock_;

  fs::path path_;

  // max entity id of this vgroup
  uint64_t max_entity_id_{0};

  // mutex for initialize/allocate/get max_entity_id_
  mutable std::mutex entity_id_mutex_;

  EngineOptions* engine_options_ = nullptr;

  std::shared_mutex* engine_wal_level_mutex_ = nullptr;
  std::unique_ptr<WALMgr> wal_manager_ = nullptr;
  std::unique_ptr<TSxMgr> tsx_manager_ = nullptr;

  std::unique_ptr<TsVersionManager> version_manager_ = nullptr;
  std::unique_ptr<TsMemSegmentManager> mem_segment_mgr_ = nullptr;

  std::map<PartitionIdentifier, std::shared_ptr<TsEntitySegmentBuilder>> write_batch_segment_builders_;

  // compact thread flag
  bool enable_compact_thread_{true};
  // Id of the compact thread
  KThreadID compact_thread_id_{0};
  // Conditional variable
  std::condition_variable cv_;
  // Mutexes for condition variables
  std::mutex cv_mutex_;

  std::atomic<uint64_t> max_osn_{LOG_BLOCK_HEADER_SIZE + BLOCK_SIZE};

  mutable std::shared_mutex last_row_entity_mutex_;
  std::unordered_map<uint32_t, bool> last_row_entity_checked_;
  std::unordered_map<uint32_t, pair<timestamp64, uint32_t>> last_row_entity_;

  struct TsTableLastRow {
    bool is_payload_valid = false;  // for restart
    uint32_t version = 0;
    timestamp64 last_ts = INT64_MIN;
    TSSlice last_payload;
  };
  mutable std::shared_mutex entity_latest_row_mutex_;
  std::unordered_map<uint32_t, TsEntityLatestRowStatus> entity_latest_row_checked_;
  std::unordered_map<uint32_t, TsTableLastRow> entity_latest_row_;
  std::unordered_map<uint32_t, size_t> entity_latest_row_mem_size_;

 public:
  TsVGroup() = delete;

  TsVGroup(EngineOptions* engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
           std::shared_mutex* engine_mutex, TsHashRWLatch* tag_lock, bool enable_compact_thread = true);

  ~TsVGroup();

  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta);

  KStatus PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag, TSEntityID entity_id,
                  TSSlice* payload, bool write_wal);

  fs::path GetPath() const;

  std::string GetFileName() const;

  TSEntityID AllocateEntityID();

  TSEntityID GetMaxEntityID() const;

  uint64_t GetMaxOSN() const { return CurrentVersion()->GetMaxOSN(); }

  void InitEntityID(TSEntityID entity_id);

  void LockLevelMutex() {
    if (engine_wal_level_mutex_ != nullptr) {
      engine_wal_level_mutex_->lock();
    }
  }

  void UnLockLevelMutex() {
    if (engine_wal_level_mutex_ != nullptr) {
      engine_wal_level_mutex_->unlock();
    }
  }

  void LockSharedLevelMutex() {
    if (engine_wal_level_mutex_ != nullptr) {
      engine_wal_level_mutex_->lock_shared();
    }
  }

  void UnLockSharedLevelMutex() {
    if (engine_wal_level_mutex_ != nullptr) {
      engine_wal_level_mutex_->unlock_shared();
    }
  }

  void UpdateAtomicOSN() {
    max_osn_.store(GetMaxOSN());
  }

  bool EnableWAL() {
    return engine_options_->wal_level != WALMode::OFF && !engine_options_->use_raft_log_as_wal;
  }

  uint64_t OSNInc() {
    return max_osn_.fetch_add(1, std::memory_order_relaxed);
  }

  TsEngineSchemaManager* GetEngineSchemaMgr() { return schema_mgr_; }

  WALMgr* GetWALManager() { return wal_manager_.get(); }

  std::shared_ptr<const TsVGroupVersion> CurrentVersion() const { return version_manager_->Current(); }

  // flush all mem segment data into last segment.
  KStatus Flush() {
    auto current = mem_segment_mgr_->CurrentMemSegment();
    if (mem_segment_mgr_->SwitchMemSegment(current.get())) {
      // Flush imm segment.
      return FlushImmSegment(current);
    }
    return SUCCESS;
  }

  uint64_t GetMtrIDByTsxID(const char* ts_trans_id) {
    return tsx_manager_->getMtrID(ts_trans_id);
  }

  void SetMtrIDByTsxID(uint64_t uuid, const char* ts_trans_id) {
    return tsx_manager_->insertMtrID(ts_trans_id, uuid);
  }

  bool IsExplict(uint64_t mini_trans_id) {
    return tsx_manager_->IsExplict(mini_trans_id);
  }

  KStatus Compact();


  KStatus FlushImmSegment(const std::shared_ptr<TsMemSegment>& segment);

  KStatus RemoveChkFile(kwdbContext_p ctx);

  KStatus ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs,
                                       TS_LSN& last_lsn, std::vector<uint64_t> uncommitted_xid);

  KStatus ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn);

  KStatus ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs);

  KStatus GetIterator(kwdbContext_p ctx, uint32_t version, vector<uint32_t>& entity_ids,
                      std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                      std::vector<k_uint32>& scan_cols, std::vector<k_uint32>& ts_scan_cols,
                      std::vector<k_int32>& agg_extend_cols,
                      std::vector<Sumfunctype>& scan_agg_types,
                      std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                      std::shared_ptr<MMapMetricsTable>& schema, TsStorageIterator** iter,
                      const std::shared_ptr<TsVGroup>& vgroup,
                      const std::vector<timestamp64>& ts_points, bool reverse, bool sorted);

  KStatus GetBlockSpans(TSTableID table_id, uint32_t entity_id, KwTsSpan ts_span, DATATYPE ts_col_type,
                        std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                        std::shared_ptr<const TsVGroupVersion>& current,
                        std::list<std::shared_ptr<TsBlockSpan>>* block_spans);

  KStatus rollback(kwdbContext_p ctx, LogEntry* wal_log, bool from_chk = false);

  KStatus ApplyWal(kwdbContext_p ctx, LogEntry* wal_log, std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete);

  uint32_t GetVGroupID();

  KStatus DeleteEntity(kwdbContext_p ctx, TSTableID table_id, std::string& p_tag, TSEntityID e_id, uint64_t* count,
                       uint64_t mtr_id, uint64_t osn = 0, bool user_del = true);
  KStatus DeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& p_tag, TSEntityID e_id,
                    const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del);

  KStatus DeleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, TS_LSN lsn,
                    const std::vector<KwTsSpan>& ts_spans, bool user_del = true);
  KStatus deleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, KwLSNSpan lsn,
                    const std::vector<KwTsSpan>& ts_spans, bool user_del = true);

  KStatus undoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
  const std::vector<KwTsSpan>& ts_spans);
  KStatus redoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
  const std::vector<KwTsSpan>& ts_spans);

  KStatus GetEntitySegmentBuilder(std::shared_ptr<const TsPartitionVersion>& partition,
                                  std::shared_ptr<TsEntitySegmentBuilder>& builder);

  KStatus WriteBatchData(TSTableID tbl_id, uint32_t table_version, TSEntityID entity_id,
                         timestamp64 p_time, TSSlice data);

  KStatus FinishWriteBatchData();

  KStatus CancelWriteBatchData();

  TsEngineSchemaManager* GetSchemaMgr() const;

  /**
   * @brief undoPut undo a put operation. This function is used to undo a previously executed put operation.
   *
   * @param ctx The context of the database, providing necessary environment for the operation.
   * @param log_lsn The log sequence number identifying the specific log entry to be undone.
   * @param payload A slice of the transaction log containing the data needed to reverse the put operation.
   *
   * @return KStatus The status of the undo operation, indicating success or specific failure reasons.
   */
  KStatus undoPut(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload);

  KStatus getEntityIdByPTag(kwdbContext_p ctx, TSTableID table_id, TSSlice& ptag, TSEntityID* entity_id);

  KStatus undoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_tag, TS_LSN log_lsn,
                        uint32_t group_id, uint32_t entity_id, TSSlice& tags, uint64_t osn = 0);

  KStatus redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload, uint64_t osn = 0);

  KStatus undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, const TSSlice& old_payload,
                        uint64_t osn = 0);

  KStatus redoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn,
                        uint32_t group_id, uint32_t entity_id, TSSlice& tags, uint64_t osn = 0);

  /**
   * @brief Start a mini-transaction for the current EntityGroup.
   * @param[in] table_id Identifier of TS table.
   * @param[in] range_id Unique ID associated to a Raft consensus group, used to identify the current write batch.
   * @param[in] index The lease index of current write batch.
   * @param[out] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id = nullptr);

  /**
   * @brief Submit the mini-transaction for the current EntityGroup.
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id, const char* tsx_id = nullptr);

  /**
   * @brief Roll back the mini-transaction of the current EntityGroup.
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip = false, const char* tsx_id = nullptr);
  KStatus redoPut(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload, uint64_t osn = 0);

  KStatus GetLastRowEntity(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                           pair<timestamp64, uint32_t>& last_row_entity);

  bool isLastRowEntityPayloadValid(KTableKey table_id) {
    std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    return last_row_entity_checked_.count(table_id) && last_row_entity_checked_[table_id];
  }

  void UpdateEntityAndMaxTs(KTableKey table_id, timestamp64 max_ts, EntityID entity_id) {
    std::unique_lock<std::shared_mutex> lock1(last_row_entity_mutex_);
    if (!last_row_entity_.count(table_id) || max_ts >= last_row_entity_[table_id].first) {
      std::shared_lock<std::shared_mutex> lock2(entity_latest_row_mutex_);
      if (entity_latest_row_checked_[entity_id] == TsEntityLatestRowStatus::Valid) {
        last_row_entity_checked_[table_id] = true;
      }
      last_row_entity_[table_id] = {max_ts, entity_id};
    }
  }

  void ResetEntityMaxTs(KTableKey table_id, timestamp64 max_ts, EntityID entity_id) {
    std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    if (last_row_entity_.count(table_id) && max_ts >= last_row_entity_[table_id].first) {
      if (entity_id == last_row_entity_[table_id].second) {
        last_row_entity_.erase(table_id);
        last_row_entity_checked_[table_id] = false;
      }
    }
  }

  KStatus GetEntityLastRow(std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                           uint32_t entity_id, const std::vector<KwTsSpan>& ts_spans,
                           timestamp64& entity_last_ts);

  KStatus GetEntityLastRowBatch(uint32_t entity_id, uint32_t scan_version,
                                std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                const std::vector<KwTsSpan>& ts_spans, const std::vector<k_uint32>& scan_cols,
                                timestamp64& entity_last_ts, ResultSet* res);

  bool isEntityLatestRowPayloadValid(EntityID entity_id) {
    std::shared_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    return entity_latest_row_checked_.count(entity_id) &&
           entity_latest_row_checked_[entity_id] != TsEntityLatestRowStatus::Recovering &&
           entity_latest_row_[entity_id].is_payload_valid;
  }

  void UpdateEntityLatestRow(EntityID entity_id, timestamp64 max_ts, const TSSlice& payload, uint32_t version) {
    std::unique_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    if (!entity_latest_row_.count(entity_id) || max_ts >= entity_latest_row_[entity_id].last_ts) {
      // update last payload
      if (CLUSTER_SETTING_USE_LAST_ROW_OPTIMIZATION) {
        assert(payload.len > 0);
        assert(payload.data != nullptr);
        char* payload_data = nullptr;
        if (entity_latest_row_checked_[entity_id] != TsEntityLatestRowStatus::Valid) {
          if (entity_latest_row_checked_[entity_id] == TsEntityLatestRowStatus::Uninitialized) {
            entity_latest_row_checked_[entity_id] = TsEntityLatestRowStatus::Valid;
          }
          size_t page_size = getpagesize();
          size_t alloc_size = std::max(page_size, payload.len);
          payload_data = static_cast<char*>(malloc(alloc_size));
          if (nullptr == payload_data) {
            entity_latest_row_[entity_id].last_ts = max_ts;
            LOG_ERROR("malloc failed. malloc size is %lu", alloc_size);
            return;
          }
          entity_latest_row_mem_size_[entity_id] = alloc_size;
        } else if (payload.len > entity_latest_row_mem_size_[entity_id]) {
          size_t& old_size = entity_latest_row_mem_size_[entity_id];
          char* old_mem = entity_latest_row_[entity_id].last_payload.data;
          size_t realloc_size = old_size * 1.25;
          size_t alloc_size = std::max(realloc_size, payload.len);
          payload_data = static_cast<char*>(realloc(old_mem, alloc_size));
          if (nullptr == payload_data) {
            entity_latest_row_[entity_id].last_ts = max_ts;
            entity_latest_row_[entity_id].is_payload_valid = false;
            LOG_ERROR("realloc failed. realloc size is %lu", alloc_size);
            return;
          }
          entity_latest_row_mem_size_[entity_id] = alloc_size;
        } else {
          payload_data = entity_latest_row_[entity_id].last_payload.data;
        }
        entity_latest_row_[entity_id].is_payload_valid = true;
        entity_latest_row_[entity_id].version = version;
        entity_latest_row_[entity_id].last_payload.len = payload.len;
        if (entity_latest_row_[entity_id].last_payload.data != payload_data) {
          entity_latest_row_[entity_id].last_payload.data = payload_data;
        }
        memcpy(entity_latest_row_[entity_id].last_payload.data, payload.data, payload.len);
      }
      entity_latest_row_[entity_id].last_ts = max_ts;
    }
  }

  void ResetEntityLatestRow(EntityID entity_id, timestamp64 max_ts) {
    std::unique_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    if (entity_latest_row_.count(entity_id) && max_ts >= entity_latest_row_[entity_id].last_ts) {
      if (CLUSTER_SETTING_USE_LAST_ROW_OPTIMIZATION) {
        free(entity_latest_row_[entity_id].last_payload.data);
        entity_latest_row_[entity_id].last_payload.data = nullptr;
      }
      entity_latest_row_.erase(entity_id);
      entity_latest_row_mem_size_.erase(entity_id);
      entity_latest_row_checked_[entity_id] = TsEntityLatestRowStatus::Recovering;
    }
  }

  KStatus Vacuum();

 private:
  // check partition of rows exist. if not creating it.
  // KStatus makeSurePartitionExist(TSTableID table_id, const std::list<TSMemSegRowData>& rows);

  KStatus TrasvalAllPartition(kwdbContext_p ctx, TSTableID tbl_id,
    const std::vector<KwTsSpan>& ts_spans, std::function<KStatus(std::shared_ptr<const TsPartitionVersion>)> func);

  int saveToFile(uint32_t new_id) const;
  // Thread scheduling executes compact tasks to clean up items that require erasing.
  void compactRoutine(void* args);
  // Initialize compact thread.
  void initCompactThread();
  // Close compact thread.
  void closeCompactThread();

  KStatus PartitionCompact(std::shared_ptr<const TsPartitionVersion> partition, bool call_by_vacuum = false);

  KStatus ConvertBlockSpanToResultSet(const std::vector<k_uint32>& kw_scan_cols, shared_ptr<TsBlockSpan>& ts_blk_span,
                                      const vector<AttributeInfo>& attrs, ResultSet* res);
};

}  // namespace kwdbts
