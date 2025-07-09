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

#include <cstdint>
#include <filesystem>
#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "st_wal_mgr.h"
#include "ts_engine_schema_manager.h"
#include "ts_mem_segment_mgr.h"
#include "ts_version.h"

namespace kwdbts {

class TsEntitySegmentBuilder;

enum class TsExclusiveStatus{
  NONE = 0,
  COMPACTE,
  WRITE_BATCH,
  VACUUM,
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

  TsMemSegmentManager mem_segment_mgr_;

  std::filesystem::path path_;

  // max entity id of this vgroup
  uint64_t max_entity_id_{0};

  // mutex for initialize/allocate/get max_entity_id_
  mutable std::mutex entity_id_mutex_;

  EngineOptions engine_options_;

  std::unique_ptr<WALMgr> wal_manager_ = nullptr;
  std::unique_ptr<TSxMgr> tsx_manager_ = nullptr;

  std::unique_ptr<TsVersionManager> version_manager_ = nullptr;

  std::map<PartitionIdentifier, std::shared_ptr<TsEntitySegmentBuilder>> write_batch_segment_builders_;
  std::shared_mutex builders_mutex_;

  // compact thread flag
  bool enable_compact_thread_{true};
  // Id of the compact thread
  KThreadID compact_thread_id_{0};
  // Conditional variable
  std::condition_variable cv_;
  // Mutexes for condition variables
  std::mutex cv_mutex_;

  // Flushing Mutex
  std::mutex flush_mutex_;

  std::atomic<TsExclusiveStatus> comp_vacuum_status_{TsExclusiveStatus::NONE};

 public:
  TsVGroup() = delete;

  TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
           bool enable_compact_thread = true);

  ~TsVGroup();

  KStatus Init(kwdbContext_p ctx);

  KStatus SetReady();

  KStatus CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta);

  KStatus PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag, TSEntityID entity_id,
                  TSSlice* payload, bool write_wal);

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;

  TSEntityID AllocateEntityID();

  TSEntityID GetMaxEntityID() const;

  void InitEntityID(TSEntityID entity_id);

  TsEngineSchemaManager* GetEngineSchemaMgr() { return schema_mgr_; }

  WALMgr* GetWALManager() { return wal_manager_.get(); }

  std::shared_ptr<const TsVGroupVersion> CurrentVersion() const { return version_manager_->Current(); }

  // flush all mem segment data into last segment.
  KStatus Flush() {
    std::lock_guard lk{flush_mutex_};
    std::shared_ptr<TsMemSegment> imm_segment;
    mem_segment_mgr_.SwitchMemSegment(&imm_segment);
    assert(imm_segment.get() != nullptr);

    // Update vresion before flush.
    TsVersionUpdate update;
    std::list<std::shared_ptr<TsMemSegment>> memsegs;
    mem_segment_mgr_.GetAllMemSegments(&memsegs);
    update.SetValidMemSegments(memsegs);

    version_manager_->ApplyUpdate(&update);

    // Flush imm segment.
    KStatus s = FlushImmSegment(imm_segment);
    return s;
  }

  void ResetTsExclusiveStatus() {
    comp_vacuum_status_.store(TsExclusiveStatus::NONE);
  }

  bool TrySetTsExclusiveStatus(TsExclusiveStatus desired) {
    TsExclusiveStatus expected = TsExclusiveStatus::NONE;
    if (comp_vacuum_status_.compare_exchange_strong(expected, desired)) {
      return true;
    }
    return false;
  }

  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* imm_segment) { mem_segment_mgr_.SwitchMemSegment(imm_segment); }

  KStatus Compact();

  KStatus FlushImmSegment(const std::shared_ptr<TsMemSegment>& segment);
  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload);

  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload);

  KStatus UpdateLSN(kwdbContext_p ctx, TS_LSN chk_lsn);

  KStatus ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn);

  KStatus ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn);

  KStatus ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs);

  KStatus CreateCheckpointInternal(kwdbContext_p ctx);

  KStatus GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                      std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                      std::vector<k_int32> agg_extend_cols,
                      std::vector<Sumfunctype> scan_agg_types,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                      uint32_t table_version, TsStorageIterator** iter,
                      std::shared_ptr<TsVGroup> vgroup,
                      std::vector<timestamp64> ts_points, bool reverse, bool sorted);

  KStatus GetBlockSpans(TSTableID table_id, uint32_t entity_id, KwTsSpan ts_span, DATATYPE ts_col_type,
                        std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                        std::list<std::shared_ptr<TsBlockSpan>>* block_spans);

  KStatus rollback(kwdbContext_p ctx, LogEntry* wal_log);

  KStatus ApplyWal(kwdbContext_p ctx, LogEntry* wal_log, std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete);

  uint32_t GetVGroupID();

  KStatus DeleteEntity(kwdbContext_p ctx, TSTableID table_id, std::string& p_tag, TSEntityID e_id, uint64_t* count,
                       uint64_t mtr_id);
  KStatus DeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& p_tag, TSEntityID e_id,
                     const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id);

  KStatus DeleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, TS_LSN lsn,
                    const std::vector<KwTsSpan>& ts_spans);
  KStatus deleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, KwLSNSpan lsn,
                              const std::vector<KwTsSpan>& ts_spans);

  KStatus WriteBatchData(kwdbContext_p ctx, TSTableID tbl_id, uint32_t table_version, TSEntityID entity_id,
                         timestamp64 ts, DATATYPE ts_col_type, TSSlice data);

  KStatus FinishWriteBatchData();

  KStatus ClearWriteBatchData();

   /**
   * Undoes deletion of rows within a specified entity group.
   *
   * @param ctx Pointer to the database context.
   * @param primary_tag Primary tag identifying the entity.
   * @param log_lsn LSN of the log for ensuring atomicity and consistency of the operation.
   * @param rows Collection of row spans to be undeleted.
   * @return Status of the operation, success or failure.
   */
  KStatus undoDelete(kwdbContext_p ctx, std::string& primary_tag, TS_LSN log_lsn,
                     const std::vector<DelRowSpan>& rows);
  KStatus undoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
  const std::vector<KwTsSpan>& ts_spans);
  KStatus redoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
  const std::vector<KwTsSpan>& ts_spans);

  TsEngineSchemaManager* GetSchemaMgr() const;

  KStatus undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload);

  KStatus undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, TSSlice old_payload);

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

  KStatus undoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, TS_LSN log_lsn,
                        uint32_t group_id, uint32_t entity_id, TSSlice& tags);

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
  KStatus redoPut(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoDelete(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                     const vector<DelRowSpan>& rows);

  KStatus redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn, uint32_t group_id,
                        uint32_t entity_id, TSSlice& payload);

  KStatus redoCreateHashIndex(const std::vector<uint32_t>& tags, uint32_t index_id, uint32_t ts_version);

  KStatus undoCreateHashIndex(uint32_t index_id, uint32_t ts_version);

  KStatus redoDropHashIndex(uint32_t index_id, uint32_t ts_version);

  KStatus undoDropHashIndex(const std::vector<uint32_t>& tags, uint32_t index_id, uint32_t ts_version);

  /**
   * @brief Start a mini-transaction for the current EntityGroup.
   * @param[in] table_id Identifier of TS table.
   * @param[in] range_id Unique ID associated to a Raft consensus group, used to identify the current write batch.
   * @param[in] index The lease index of current write batch.
   * @param[out] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id);

  /**
   * @brief Submit the mini-transaction for the current EntityGroup.
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id);

  /**
   * @brief Roll back the mini-transaction of the current EntityGroup.
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip = false);

 private:
  // check partition of rows exist. if not creating it.
  KStatus makeSurePartitionExist(TSTableID table_id, const std::list<TSMemSegRowData>& rows);

  KStatus TrasvalAllPartition(kwdbContext_p ctx, TSTableID tbl_id,
    const std::vector<KwTsSpan>& ts_spans, std::function<KStatus(std::shared_ptr<const TsPartitionVersion>)> func);

  KStatus redoPut(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  // Thread scheduling executes compact tasks to clean up items that require erasing.
  void compactRoutine(void* args);
  // Initialize compact thread.
  void initCompactThread();
  // Close compact thread.
  void closeCompactThread();
};

}  // namespace kwdbts
