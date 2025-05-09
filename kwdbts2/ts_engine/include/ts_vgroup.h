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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kwdb_type.h"
#include "ts_engine_schema_manager.h"
#include "ts_vgroup_partition.h"
#include "ts_mem_segment_mgr.h"
#include "st_wal_mgr.h"

namespace kwdbts {

// Interval class, left close right open -> [start , end)
// 简化
struct TimeInterval {
  int64_t start, end;

 public:
  explicit TimeInterval(int64_t start, int64_t end) : start(start), end(end) { assert(end >= start); }
  bool operator<(const TimeInterval& rhs) const { return this->end < rhs.end; }
  bool operator>(const TimeInterval& rhs) const { return this->end > rhs.end; }
  int64_t GetInterval() const { return end - start; }

  // For Debug
  TimeInterval operator&(const TimeInterval& rhs) const {
    if (start > rhs.end || end < rhs.start) {
      return TimeInterval{0, 0};
    }
    int64_t newstart = 0, newend = 0;
    newstart = start > rhs.start ? start : rhs.start;
    newend = end < rhs.end ? end : rhs.end;
    return TimeInterval{newstart, newend};
  }
};

class TsVGroup;
class PartitionManager {
 private:
  std::unordered_map<int, std::shared_ptr<TsVGroupPartition>> partitions_;
  KRWLatch partitions_latch_;
  TsVGroup* vgroup_;
  int database_id_;
  uint64_t interval_;

 public:
  PartitionManager(TsVGroup* vgroup, int database_id, uint64_t interval)
      : partitions_latch_(RWLATCH_ID_VGROUP_PARTITION_MGR_RWLOCK), vgroup_(vgroup),
        database_id_(database_id), interval_(interval) {}
  std::shared_ptr<TsVGroupPartition> Get(int64_t timestamp, bool create_if_not_exist);
  std::vector<std::shared_ptr<TsVGroupPartition>> GetPartitionArray();
  void SetInterval(int64_t interval) { interval_ = interval; }

  void GetPartitions(std::unordered_map<int, std::shared_ptr<TsVGroupPartition>>* map) {
    RW_LATCH_S_LOCK(&partitions_latch_);
    Defer defer{[&]{ RW_LATCH_UNLOCK(&partitions_latch_); }};
    *map = partitions_;
  }
};

/**
 * table group used for organizing a series of table(super table of device).
 * in current time vgroup is same as database
 */
// const pointer
class TsVGroup {
 private:
  uint32_t vgroup_id_;
  TsEngineSchemaManager* schema_mgr_{nullptr};
  //  <database_id, Manager>
  std::map<uint32_t, std::unique_ptr<PartitionManager>> partitions_;
  KRWLatch partitions_latch_;

  TsMemSegmentManager mem_segment_mgr_;

  std::filesystem::path path_;

  uint64_t entity_counter_;

  mutable std::mutex mutex_;

  MMapFile* config_file_;

  EngineOptions engine_options_;

  std::unique_ptr<WALMgr> wal_manager_ = nullptr;
  std::unique_ptr<TSxMgr> tsx_manager_ = nullptr;

  // compact thread flag
  bool enable_compact_thread_{true};
  // Id of the compact thread
  KThreadID compact_thread_id_{0};
  // Conditional variable
  std::condition_variable cv_;
  // Mutexes for condition variables
  std::mutex cv_mutex_;

 public:
  TsVGroup() = delete;

  TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
           bool enable_compact_thread = true);

  ~TsVGroup();

  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta);

  inline std::shared_ptr<TsVGroupPartition> GetPartition(uint32_t database_id, timestamp64 ts, DATATYPE ts_type) {
    auto p_time = convertTsToPTime(ts, ts_type);
    return GetPartition(database_id, p_time);
  }

  KStatus PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag,
                  TSEntityID entity_id, TSSlice* payload);

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;

  uint32_t AllocateEntityID();

  uint32_t GetMaxEntityID() const;

  TsEngineSchemaManager* GetEngineSchemaMgr() {
    return schema_mgr_;
  }

  WALMgr* GetWALManager() {
    return wal_manager_.get();
  }

  // flush all mem segment data into last segment.
  KStatus Flush() {
    std::shared_ptr<TsMemSegment> imm_segment;
    mem_segment_mgr_.SwitchMemSegment(&imm_segment);
    KStatus s = KStatus::SUCCESS;
    if (imm_segment.get() != nullptr) {
      s = FlushImmSegment(imm_segment);
    }
    return s;
  }

  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* imm_segment) {
    mem_segment_mgr_.SwitchMemSegment(imm_segment);
  }

  KStatus Compact(int thread_num = 1);

  KStatus FlushImmSegment(const std::shared_ptr<TsMemSegment>& segment);
  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload);

  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload);

  KStatus UpdateLSN(kwdbContext_p ctx, TS_LSN chk_lsn);

  KStatus ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN &last_lsn);

  KStatus ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN &last_lsn);

  KStatus ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs);

  KStatus CreateCheckpointInternal(kwdbContext_p ctx);

  KStatus GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                      std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                      std::vector<Sumfunctype> scan_agg_types,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                      uint32_t table_version, TsStorageIterator** iter,
                      std::shared_ptr<TsVGroup> vgroup,
                      std::vector<timestamp64> ts_points, bool reverse, bool sorted);

  KStatus rollback(kwdbContext_p ctx, LogEntry* wal_log);

  KStatus ApplyWal(kwdbContext_p ctx, LogEntry* wal_log,
                   std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete);

  uint32_t GetVGroupID();

  TsEngineSchemaManager* GetSchemaMgr() const;

  TsMemSegmentManager* GetMemSegmentMgr();

  std::map<uint32_t, std::unique_ptr<PartitionManager>>& GetPartitionManagers() {
    return partitions_;
  }
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
  KStatus redoPut(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                  const TSSlice& payload);

  KStatus redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload);

  KStatus redoDelete(kwdbContext_p ctx, std::string& primary_tag, kwdbts::TS_LSN log_lsn,
                     const vector<DelRowSpan>& rows);

  KStatus redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn,
                        uint32_t group_id, uint32_t entity_id, TSSlice& payload);

  KStatus redoCreateHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version);

  KStatus undoCreateHashIndex(uint32_t index_id, uint32_t ts_version);

  KStatus redoDropHashIndex(uint32_t index_id, uint32_t ts_version);

  KStatus undoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t ts_version);

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
  // p_time should generated by function convertTsToPTime.
  std::shared_ptr<TsVGroupPartition> GetPartition(uint32_t database_id, timestamp64 p_time);

  int saveToFile(uint32_t new_id) const;

  // Thread scheduling executes compact tasks to clean up items that require erasing.
  void compactRoutine(void* args);
  // Initialize compact thread.
  void initCompactThread();
  // Close compact thread.
  void closeCompactThread();
};


}  // namespace kwdbts
