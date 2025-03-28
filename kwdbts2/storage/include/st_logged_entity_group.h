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

#include <map>
#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include "ts_table.h"
#include "st_wal_mgr.h"
#include "st_transaction_mgr.h"
#include "lt_rw_latch.h"

namespace kwdbts {

class WALMgr;
class TSxMgr;

class LoggedTsEntityGroup : public TsEntityGroup {
 public:
  LoggedTsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_bt_manager, const string& db_path,
                      const KTableKey& table_id, const RangeGroup& range, const string& tbl_sub_path, EngineOptions* opt);

  ~LoggedTsEntityGroup() override;

  KStatus Create(kwdbContext_p ctx, vector<TagInfo>& tag_schema, uint32_t ts_version = 1) override;

  KStatus OpenInit(kwdbContext_p ctx) override;

  KStatus Drop(kwdbContext_p ctx, bool is_force) override;

  /**
   * @brief PutEntity is responsible for writing Entity tags values and supports modification of Tag values.
   * If the primary tag does not exist, write the tags data.
   * If the primary tag already exists and there are other tag values in the payload, update the tag value.
   * If there is time series data in the payload, write it to the time series table.
   * @param[in] payload   PayLoad object of tags value, which contains primary tag information
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus PutEntity(kwdbContext_p ctx, TSSlice& payload_data, uint64_t mtr_id) override;

  /**
   * PutData writes the Tag value and time series data to the entity
   *
   * @param ctx Database Context
   * @param payload_data  Comprises tag values and time-series data
   * @param mini_trans_id A unique transaction ID is recorded to ensure data consistency.
   * @param dedup_result Stores the deduplication results of this operation, exclusively for Reject and Discard modes.
   * @param dedup_rule The deduplication rule defaults to OVERRIDE.
   * @return Return the status code of the operation, indicating its success or failure.
   */
  KStatus PutData(kwdbContext_p ctx, TSSlice payload_data, TS_LSN mini_trans_id,
                  uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                  DedupResult* dedup_result, DedupRule dedup_rule = DedupRule::OVERRIDE, bool write_wal = true) override;

  /**
   * PutData writes the Tag value and time series data to the entity
   *
   * @param[in] payloads Comprises tag values and time-series data
   * @param[in] length  The length of the payloads array
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @param dedup_result Stores the deduplication results of this operation, exclusively for Reject and Discard modes.
   * @param dedup_rule The deduplication rule defaults to OVERRIDE.
   * @return Return the status code of the operation, indicating its success or failure.
   */
  KStatus PutData(kwdbContext_p ctx, TSSlice* payloads, int length, uint64_t mtr_id,
                  uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                  DedupResult* dedup_result, DedupRule dedup_rule = DedupRule::OVERRIDE, bool writeWAL = true) override;

  /**
   * @brief Mark the deletion of time series data in a specified range.
   * @param[in] table_id  ID of the time series table
   * @param[in] primary_tag Identifies a specific entity
   * @param[in] ts_spans  The range of time series data
   * @param[out] count  The number of deleted data rows
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus DeleteData(kwdbContext_p ctx, const string& primary_tag, TS_LSN lsn, const std::vector<KwTsSpan>& ts_spans,
                     vector<DelRowSpan>* rows, uint64_t* count, uint64_t mtr_id, bool evaluate_del) override;

  KStatus EvaluateDeleteData(kwdbContext_p ctx, const string& primary_tag,
                             const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>& rows, uint64_t mtr_id);

  /**
   * @brief Delete Entity and time series data in batches, typically used for Range migration.
   * @param[in] table_id   Time series table ID
   * @param[in] primary_tags The primary_tag of the Entity to be deleted in batch.
   * When the HASH IDs are within a Range, the EntityGroup file will be deleted directly.
   * @param[in] count The number of data rows to be deleted
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  KStatus DeleteEntities(kwdbContext_p ctx, const std::vector<std::string>& primary_tags,
                         uint64_t* count, uint64_t mtr_id) override;

  /**
   * @brief  Flush the WAL cache of the current EntityGroup to the disk file.
   *
   * @return KStatus
   */
  KStatus FlushBuffer(kwdbContext_p ctx) override;

  /**
   * @brief Start the checkpoint operation of the current EntityGroup without a lock.
   *
   * @return KStatus
   */
  KStatus CreateCheckpointInternal(kwdbContext_p ctx);

  /**
    * @brief Start the checkpoint operation of the current EntityGroup.
    *
    * @return KStatus
    */
  KStatus CreateCheckpoint(kwdbContext_p ctx)  override;

  /**
    * @brief Start snapshot transaction of the current EntityGroup.
    *
    * @return KStatus
    */
  KStatus BeginSnapshotMtr(kwdbContext_p ctx, uint64_t range_id, uint64_t index,
                           const SnapshotRange& range, uint64_t &mtr_id);

  KStatus BeginSnapshotMtr(kwdbContext_p ctx, uint64_t range_id,
                          std::string link_path, std::string tier_path, uint64_t &mtr_id);
  /**
    * @brief write temp directory info to WAL log.
    *
    * @return KStatus
    */
  KStatus WriteTempDirectoryLog(kwdbContext_p ctx, uint64_t mtr_id, std::string path);

  KStatus BeginPartitionTierChangeMtr(kwdbContext_p ctx, uint64_t range_id,
                                      std::string link_path, std::string tier_path, uint64_t &mtr_id);

  /**
    * @brief Start the log recovery operation of the current EntityGroup.
    *
    * @return KStatus
    */
  KStatus Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes);

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
  KStatus MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool skip_log = false);

  /**
  * Get the current LSN
  * @return Current LSN
  */
  TS_LSN FetchCurrentLSN();

  static uint64_t GetAppliedIndex(const uint64_t range_id, const std::map<uint64_t, uint64_t>& range_indexes_map) {
    const auto iter = range_indexes_map.find(range_id);
    if (iter == range_indexes_map.end()) {
      return 0;
    }
    return iter->second;
  }


 private:
  KStatus applyWal(kwdbContext_p ctx, LogEntry* wal_log, std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete);

  KStatus rollback(kwdbContext_p ctx, LogEntry* wal_log);

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

  EngineOptions* engine_opt_;
  WALMgr* wal_manager_ = nullptr;
  TSxMgr* tsx_manager_ = nullptr;
  using LoggedTsEntityGroupLatch = KLatch;
  LoggedTsEntityGroupLatch* logged_mutex_;
};

}  // namespace kwdbts
