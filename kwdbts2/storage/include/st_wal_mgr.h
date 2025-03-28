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

#include <string>
#include <vector>
#include <fstream>
#include "st_wal_internal_buffer_mgr.h"
#include "st_wal_internal_logfile_mgr.h"
#include "st_wal_internal_log_structure.h"
#include "st_logged_entity_group.h"
#include "lt_rw_latch.h"

namespace kwdbts {

class LoggedTsEntityGroup;

/**
 * Write-Ahead-Logging Management class, each EntityGroup have a WALMgr instance; Meanwhile, The global WALMgr is
 * defined at the TS Engine level to record DDL operations.
 */
class WALMgr {
 public:
  WALMgr(const string& db_path, const KTableKey& table_id, uint64_t entity_grp_id, EngineOptions* opt);

  ~WALMgr();

  KStatus Create(kwdbContext_p ctx);

  /**
   * Init WAL mgr, includes checking and creating the WAL log group file directory for the current EntityGroup,
   * checking and creating the WAL metadata file and log file.
   * @param ctx
   * @return
   */
  KStatus Init(kwdbContext_p ctx);

  /**
   * Write WAL log entry into WAL Buffer.
   * @param[in] wal_log
   * @param[out] entry_lsn The LSN of the WAL log entry.
   * @return
   */
  KStatus WriteWAL(kwdbContext_p ctx, k_char* wal_log, size_t length, TS_LSN& entry_lsn);

  inline KStatus writeWALInternal(kwdbContext_p ctx, k_char* wal_log, size_t length, TS_LSN& entry_lsn);

  /**
   * Write WAL log entry into WAL Buffer.
   * @param wal_log
   * @return
   */
  inline KStatus WriteWAL(kwdbContext_p ctx, k_char* wal_log, size_t length);

  inline KStatus writeLogEntry(kwdbContext_p ctx, LogEntry& logEntry) {
    auto wal_log = logEntry.encode();
    size_t log_len = logEntry.getLen();
    TS_LSN current_lsn = 0;
    KStatus status = WriteWAL(ctx, wal_log, log_len, current_lsn);

    delete[] wal_log;
    return status;
  }

  /**
   * Flush WAL into log file.
   * @param ctx
   * @return
   */
  KStatus Flush(kwdbContext_p ctx);

  /**
   * Synchronize data to disk by calling the FLush method of Tag tables and Metrics tables to ensure the
   * persistence of time-series data to save recovery time after an outage.
   * @param ctx
   * @return
   */
  KStatus CreateCheckpoint(kwdbContext_p ctx);

  KStatus CreateCheckpointWithoutFlush(kwdbContext_p ctx);

  KStatus Close();

  KStatus Drop();

  /**
   * Construct the log entry for the INSERT Tag operation.
   * @param[in] ctx
   * @param[in] x_id Mini-transaction ID, default value is 0.
   * @param[in] time_partition Time range of data
   * @param[in] offset  The current row counter of Tag table
   * @param[in] prepared_payload Payload of INSERT operation
   * @return
   */
  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                         size_t offset, TSSlice prepared_payload);

  /**
   * Construct the log entry for the UPDATE Tag operation.
   * @param[in] ctx
   * @param[in] x_id Mini-transaction ID, default value is 0.
   * @param[in] time_partition Time range of data
   * @param[in] offset  The current row counter of Tag/Metrics table
   * @param[in] new_payload New payload of UPDATE operation
   * @param[in] old_payload Old payload of UPDATE operation
   * @return
   */
  KStatus WriteUpdateWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                         size_t offset, TSSlice new_payload, TSSlice old_payload);

  /**
   * Construct the log entry for the INSERT Metrics operation.
   * @param[in] ctx
   * @param[in] x_id Mini-transaction ID, default value is 0.
   * @param[in] time_partition Time range of data
   * @param[in] offset  The current row counter of Tag/Metrics table
   * @param[in] primary_tag primary_tag
   * @param[in] prepared_payload Payload of INSERT operation
   * @param[out] entry_lsn The lsn of WAL log entry
   * @return
   */
  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                         size_t offset, TSSlice primary_tag, TSSlice prepared_payload, TS_LSN& entry_lsn);

  /**
   * Construct the log entry for the DELETE Metrics operation.
   * @param[in] ctx
   * @param[in] x_id Mini-transaction ID, default value is 0.
   * @param[in] primary_tag primary tag
   * @param[in] ts_spans Time range of deleted data
   * @return
   */
  KStatus WriteDeleteMetricsWAL(kwdbContext_p ctx, uint64_t x_id, const string& primary_tag,
                                const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>& row_spans);

  /**
   * Construct the log entry for the DELETE Tag operation.
   * @param[in] ctx
   * @param[in] x_id Mini-transaction ID, default value is 0.
   * @param[in] tag_pack Primary Tag
   * @param[in] sub_group_id sub group id
   * @param[in] entity_id entity id
   * @param[in] tag_pack deleted tag
   * @return
   */
  KStatus WriteDeleteTagWAL(kwdbContext_p ctx, uint64_t x_id, const string& primary_tag,
                            uint32_t sub_group_id, uint32_t entity_id, TSSlice tag_pack);

  /**
   * Construct the log entry for the CREATE INDEX operation.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param index_id index ID.
   * @param cur_ts_version  current version id.
   * @param new_ts_version  new version id.
   * @param col_ids the cols index contains.
   * @return
   */
  KStatus WriteCreateIndexWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id, uint32_t index_id,
                              uint32_t cur_ts_version, uint32_t new_ts_version, std::vector<uint32_t> col_ids);

  /**
 * Construct the log entry for the DROP INDEX operation.
 * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param index_id index ID.
   * @param cur_ts_version  current version id.
   * @param new_ts_version  new version id.
   * @param col_ids the cols index contains.
 * @return
 */
  KStatus WriteDropIndexWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id, uint32_t index_id,
                          uint32_t cur_ts_version, uint32_t new_ts_version, std::vector<uint32_t> col_ids);
  /**
   * Construct the log entry for the CREATE operation.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param object_id table ID
   * @param meta  Time-series table schema
   * @param ranges RangeGroups
   * @return
   */
  KStatus WriteDDLCreateWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id,
                            roachpb::CreateTsTable* meta, std::vector<RangeGroup>* ranges);

  /**
   * Construct the log entry for the DROP operation.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param object_id table ID
   * @return
   */
  KStatus WriteDDLDropWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id);

  /**
   * Construct the log entry for the ALTER operation.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param object_id table ID
   * @param alter_type  alter type
   * @param meta  Time-series table new schema
   * @param new_version  next schema version after succeed
   * @return
   */
  KStatus WriteDDLAlterWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id,
                           AlterType alter_type, uint32_t cur_version, uint32_t new_version, TSSlice& column_meta);

  /**
   * Construct the log entry for the Mini-transaction.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param tsx_id TS Transaction ID, default value is 0.
   * @param log_type Log type (BEGIN COMMIT or ROLLBACK)
   * @return
   */
  KStatus WriteMTRWAL(kwdbContext_p ctx, uint64_t x_id, const char* tsx_id, WALLogType log_type);

  /**
   * Construct the log entry for the TSx.
   * @param ctx
   * @param tsx_id Mini-transaction ID, default value is 0.
   * @param log_type Log type (BEGIN COMMIT or ROLLBACK)
   */
  KStatus WriteTSxWAL(kwdbContext_p ctx, uint64_t x_id, const char* ts_trans_id, WALLogType log_type);

  /**
   * Construct the log entry for the Checkpoint.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param tag_offset Tag table offset
   * @param range_size range size
   * @param time_partitions time partitions list
   * @param entry_lsn entry lsn
   * @return
   */
  KStatus WriteCheckpointWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t tag_offset,
                             uint32_t range_size, CheckpointPartition* time_partitions, TS_LSN& entry_lsn);

  /**
   * Construct the log entry for the Checkpoint.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param tag_offset Tag table offset
   * @param entry_lsn entry lsn
   * @return
   */
  KStatus WriteCheckpointWAL(kwdbContext_p ctx, uint64_t x_id, TS_LSN& entry_lsn);

  /**
   * Construct the log entry for the snapshot.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param tbl_id  table id
   * @param b_hash, e_hash, span  range info
   * @return
   */
  KStatus WriteSnapshotWAL(kwdbContext_p ctx, uint64_t x_id, TSTableID tbl_id, uint64_t b_hash,
                           uint64_t e_hash, KwTsSpan span);

  /**
   * Construct the log entry for the temp directory.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param path  temp directory abslutly path.
   * @return
   */
  KStatus WriteTempDirectoryWAL(kwdbContext_p ctx, uint64_t x_id, std::string path);

  /**
   * Construct the log entry for partition change directory event.
   * @param ctx
   * @param x_id Mini-transaction ID, default value is 0.
   * @param link_path  directory in tree.
   * @param tier_path  desc directory abslutly path.
   * @return
   */
  KStatus WritePartitionTierWAL(kwdbContext_p ctx, uint64_t x_id, std::string link_path, std::string tier_path);
  /**
   * Read WAL log entry.
   * @param start_lsn start LSN
   * @param end_lsn end LSN
   * @param[out] logs WAL log entries list
   * @return
   */
  KStatus ReadWALLog(std::vector<LogEntry*>& logs, TS_LSN start_lsn, TS_LSN end_lsn);

  /**
   * Read the relevant log entry according to the Mini-Transaction ID.
   * @param mtr_trans_id Mini-Transaction ID
   * @param[out] logs WAL log entries list
   * @return
   */
  KStatus ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs);

  /**
   *  Read the relevant log entry according to the TS Transaction ID.
   * @param ts_trans_id TS Transaction ID
   * @param[out] logs WAL log entries list
   * @return
   */
  KStatus ReadWALLogForTSx(char* ts_trans_id, std::vector<LogEntry*>& logs);

  /**
   * Get current LSN
   * @return current LSN
   */
  TS_LSN FetchCurrentLSN() const;

  /**
   * Fetch the LSN of the WAL that has been flushed.
   * @return
   */
  TS_LSN FetchFlushedLSN() const;

  /**
  * Fetch the LSN of the WAL that has been checkpoint.
  * @return
  */
  TS_LSN FetchCheckpointLSN() const;

  /**
  * Lock the current metadata, including the LSN ,etc.
  */
  void Lock();

  /**
  * Unlock the current metadata, including the LSN ,etc.
  */
  void Unlock();

  /**
  * Clean up the expired WAL files.
  */
  void CleanUp(kwdbContext_p ctx);

  /**
   * Reset WAL files.
   */
  KStatus ResetWAL(kwdbContext_p ctx);

  /**
  * NeedCheckpoint
  */
  bool NeedCheckpoint();

  /**
  * Set the EntityGroup to be checkpoint.
  * @param eg LoggedTsEntityGroup instance
  */
  void SetCheckpointObject(LoggedTsEntityGroup* eg);

 protected:
  /**
   * Init metadata of WAL group files.
   * @param ctx
   * @return
   */
  KStatus initWalMeta(kwdbContext_p ctx, bool is_create);

  /**
   * Synchronize the metadata to disk.
   * @param ctx
   * @return
   */
  KStatus flushMeta(kwdbContext_p ctx);


 private:
  string db_path_;
  KTableKey table_id_;
  uint64_t entity_grp_id_;
  string wal_path_;

  WALBufferMgr* buffer_mgr_{nullptr};
  WALFileMgr* file_mgr_{nullptr};

  EngineOptions* opt_;
  WALMeta meta_{};
  std::fstream meta_file_;
  using WALMgrLatch = KLatch;
  WALMgrLatch* meta_mutex_;
  LoggedTsEntityGroup* eg_{nullptr};
};
}  // namespace kwdbts
