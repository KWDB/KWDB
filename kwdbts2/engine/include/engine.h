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
#include <memory>
#include <utility>
#include <list>
#include <set>
#include <unordered_map>
#include <string>
#include <vector>
#include <atomic>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "settings.h"
#include "cm_kwdb_context.h"
#include "ts_table.h"
#include "st_logged_entity_group.h"

using namespace kwdbts; // NOLINT
const TSStatus kTsSuccess = {NULL, 0};

inline TSStatus ToTsStatus(const char* s, size_t len) {
  TSStatus result;
  result.len = len;
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s, len);
  return result;
}

inline TSStatus ToTsStatus(std::string s) {
  if (s.empty()) {
    return kTsSuccess;
  }
  TSStatus result;
  result.len = s.size();
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s.data(), s.size());
  return result;
}

// class kwdbts::TsTable;
/**
 * @brief TSEngine interface
 */
struct TSEngine {
  virtual ~TSEngine() {}

  /**
   * @brief create ts table
   * @param[in] table_id
   * @param[in] meta     schema info with protobuf
   *
   * @return KStatus
   */
  virtual KStatus CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                                std::vector<RangeGroup> ranges) = 0;

  /**
 * @brief drop ts table
 * @param[in] table_id
 *
 * @return KStatus
 */
  virtual KStatus DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) = 0;

  virtual KStatus CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                       const char* transaction_id, const uint32_t cur_version,
                                       const uint32_t new_version,
                                       const std::vector<uint32_t/* tag column id*/> &index_schema) = 0;

  virtual KStatus DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                     const char* transaction_id, const uint32_t cur_version,
                                     const uint32_t new_version) = 0;

  virtual KStatus AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                                      const char* transaction_id, const uint32_t old_version, const uint32_t new_version,
                                      const std::vector<uint32_t/* tag column id*/> &new_index_schema) = 0;

  /**
   * @brief Compress the segment whose maximum timestamp in the time series table is less than ts
   * @param[in] table_id id of the time series table
   * @param[in] ts A timestamp that needs to be compressed
   * @param[in] enable_vacuum Whether to start vacuum
   *
   * @return KStatus
   */
  virtual KStatus CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) = 0;

  /**
   * @brief get ts table object
   * @param[in] table_id
   * @param[out] ts_table
   *
   * @return KStatus
   */
  virtual KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                             ErrorInfo& err_info = getDummyErrorInfo(), uint32_t version = 0) = 0;

  /**
  * @brief get meta info of ts table
  * @param[in] table_id
  * @param[in] meta
  *
  * @return KStatus
  */
  virtual KStatus GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range,
                              roachpb::CreateTsTable* meta) = 0;

  /**
   * @brief Entity tags insert ,support update
   *            if primary tag no exists in ts table, insert to
   *            if primary tag exists in ts table, and payload has tag value, update
   * @param[in] table_id
   * @param[in] range_group_id RangeGroup ID
   * @param[in] payload    payload stores primary tag
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                            TSSlice* payload_data, int payload_num, uint64_t mtr_id) = 0;

  /**
   * @brief Entity Tag value and time series data writing. Tag value modification is not supported.
   *
   * @param[in] table_id ID of the time series table, used to uniquely identify the data table
   * @param[in] range_group_id RangeGroup ID
   * @param[in] payload Comprises tag values and time-series data
   * @param[in] payload_num payload num
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @param[in] dedup_result Stores the deduplication results of this operation,
   *                         exclusively for Reject and Discard modes.
   *
   * @return KStatus
   */
  virtual KStatus PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                          TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                          uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL = true) = 0;

  /**
   * @brief Delete data of some specified entities within a specified time range by marking
   * @param[in] table_id    ID of the time series table
   * @param[in] range_group_id  RangeGroup ID
   * @param[in] hash_span   Entities within certain hash range
   * @param[in] ts_spans    Time range for deleting data
   * @param[out] count  Number of deleted data rows
   * @param[in] mtr_id  Mini-transaction id for TS table
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                             HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                             uint64_t mtr_id) = 0;

  /**
   * @brief Mark the deletion of time series data within the specified range.
   * @param[in] table_id       ID
   * @param[in] range_group_id RangeGroup ID
   * @param[in] primary_tag    entity
   * @param[in] ts_spans
   * @param[out] count         delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                             std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                             uint64_t mtr_id) = 0;

  /**
   * @brief Batch delete Entity and sequential data.
   * @param[in] table_id       ID
   * @param[in] range_group_id RangeGroup ID
   * @param[in] primary_tags   entities
   * @param[out] count         delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                 std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) = 0;

  /**
  * @brief get batch data in tmp memroy
  * @param[out] TsWriteBatch
  *
  * @return KStatus
  */
  virtual KStatus GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) = 0;

  /**
  * @brief TsWriteBatch store to storage engine.
  * @param[in] TsWriteBatch
  *
  * @return KStatus
  */
  virtual KStatus ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) = 0;

  /**
   * @brief  create new EntityGroup, if no table_id, should give meta object.
   * @param[in] table_id   ID
   * @param[in] meta
   * @param[in] range RangeGroup info
   *
   * @return KStatus
   */
  virtual KStatus CreateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id,
                                   roachpb::CreateTsTable* meta, const RangeGroup& range) {
    return KStatus::FAIL;
  }

  /**
   * @brief get all range groups
   * @param[in]  table_id   ID
   * @param[out] groups     range group info
   *
   * @return KStatus
   */
  virtual KStatus GetRangeGroups(kwdbContext_p ctx, const KTableKey& table_id, RangeGroups *groups) {
    return KStatus::FAIL;
  }

  /**
   * @brief update range group type
   * @param[in] table_id   ID
   * @param[in] range  RangeGroup info
   *
   * @return KStatus
   */
  virtual KStatus UpdateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) {
    return KStatus::FAIL;
  }

  /**
   * @brief  delete range group ,used for snapshot
   * @param[in] table_id   ID
   * @param[in] range      RangeGroup
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) {
    return KStatus::FAIL;
  }

  /**
    * @brief create snapshot, data read from current node.
    * @param[in] table_id              ts table ID
    * @param[in] begin_hash,end_hash  Entity primary tag  hashID
    * @param[in] ts_span              timestamp span
    * @param[out] snapshot_id         generated snapshot id
    *
    * @return KStatus
    */
  virtual KStatus CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id,
                                 uint64_t begin_hash, uint64_t end_hash,
                                 const KwTsSpan& ts_span, uint64_t* snapshot_id) {
    return KStatus::FAIL;
  }

  /**
   * @brief delete snapshot object and temporary directory.
   * @param[in] snapshot_id
   *
   * @return KStatus
   */
  virtual KStatus DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) {
    return KStatus::FAIL;
  }

  /**
  * @brief  get snapshot data  batch by batch
  * @param[in] snapshot_id   ts table ID
  * @param[out] data          bytes
  *
  * @return KStatus
  */
  virtual KStatus GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data) {
    return KStatus::FAIL;
  }

  /**
    * @brief create snapshot for receiving data and store to current dest node.
    * @param[in] table_id              ts table ID
    * @param[in] begin_hash,end_hash  Entity primary tag  hashID
    * @param[in] ts_span              timestamp span
    * @param[out] snapshot_id         generated snapshot id
    *
    * @return KStatus
    */
  virtual KStatus CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id,
                                   uint64_t begin_hash, uint64_t end_hash,
                                   const KwTsSpan& ts_span, uint64_t* snapshot_id) {
    return KStatus::FAIL;
  }


  /**
   * @brief receive data and store to current dest node batch by batch
   * @param[in] data  bytes
   *
   * @return KStatus
   */
  virtual KStatus WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data) {
    return KStatus::FAIL;
  }
  virtual KStatus WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) {
    return KStatus::FAIL;
  }

  virtual KStatus WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id) {
    return KStatus::FAIL;
  }

  /**
   * @brief delete hash data, used for data migrating
   * @param[in] table_id   ID
   * @param[in] range_group_id RangeGroup ID
   * @param[in] hash_span Entity primary tag of hashID
   * @param[out] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                                      const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) {
    return KStatus::FAIL;
  }

  /**
 * @brief  calculate pushdown
 * @param[in] req
 * @param[out]  resp
 *
 * @return KStatus
 */
  virtual KStatus Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) = 0;

  /**
  * @brief Flush wal to disk.
  *
  * @return KStatus
  */
  virtual KStatus FlushBuffer(kwdbContext_p ctx) = 0;

  /**
    * @brief create check point for wal
    *
    * @return KStatus
    */
  virtual KStatus CreateCheckpoint(kwdbContext_p ctx) = 0;

  /**
    * @brief create check point for target table
    * @param[in] table_id   ID of the table
    * 
    * @return KStatus
    */
  virtual KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id) = 0;

  /**
    * @brief recover transactions, while restart
    *
    * @return KStatus
    */
  virtual KStatus Recover(kwdbContext_p ctx) = 0;

  /**
    * @brief begin mini-transaction
    * @param[in] table_id Identifier of TS table.
    * @param[in] range_id Unique ID associated to a Raft consensus group, used to identify the current write batch.
    * @param[in] index The lease index of current write batch.
    * @param[out] mtr_id Mini-transaction id for TS table.
    *
    * @return KStatus
    */
  virtual KStatus TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                             uint64_t range_id, uint64_t index, uint64_t& mtr_id) = 0;

  /**
    * @brief commit mini-transaction
    * @param[in] table_id Identifier of TS table.
    * @param[in] range_group_id The target EntityGroup ID.
    * @param[in] mtr_id Mini-transaction id for TS table.
    *
    * @return KStatus
    */
  virtual KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                              uint64_t range_group_id, uint64_t mtr_id) = 0;

  /**
    * @brief rollback mini-transaction
    * @param[in] table_id Identifier of TS table.
    * @param[in] range_group_id The target EntityGroup ID.
    * @param[in] mtr_id Mini-transaction id for TS table.
    *
    * @return KStatus
    */
  virtual KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                                uint64_t range_group_id, uint64_t mtr_id) = 0;

  /**
    * @brief begin one transaction.
    * @param[in] table_id  ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) = 0;

  /**
    * @brief commit one transaction.
    * @param[in] table_id   ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) = 0;

  /**
    * @brief rollback one transaction.
    * @param[in] table_id ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) = 0;

  virtual void GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) = 0;

  virtual KStatus UpdateSetting(kwdbContext_p ctx) = 0;

  /**
    * @brief Add a column to the time series table
    *
    * @param[in] table_id   ID of the time series table
    * @param[in] transaction_id Distributed transaction ID
    * @param[in] column Column information to add
    * @param[out] msg   The reason of failure
    *
    * @return KStatus
    */
  virtual KStatus AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                            TSSlice column, uint32_t cur_version, uint32_t new_version, string& msg) = 0;

  /**
    * @brief Drop a column from the time series table
    *
    * @param[in] table_id   ID of the time series table
    * @param[in] transaction_id Distributed transaction ID
    * @param[in] column Column information to drop
    * @param[out] msg   The reason of failure
    *
    * @return KStatus
    */
  virtual KStatus DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                             TSSlice column, uint32_t cur_version, uint32_t new_version, string& msg) = 0;

  virtual KStatus AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) = 0;

  /**
    * @brief Modify a column type of the time series table
    *
    * @param[in] table_id   ID of the time series table
    * @param[in] transaction_id Distributed transaction ID
    * @param[in] new_column The column type to change to
    * @param[in] origin_column The column type before the change
    * @param[out] msg   The reason of failure
    *
    * @return KStatus
    */
  virtual KStatus AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                  TSSlice new_column, TSSlice origin_column,
                                  uint32_t cur_version, uint32_t new_version, string& msg) = 0;

  /**
   * @brief : Gets the number of remaining threads from the thread pool and
   *          available memory from system
   *
   * @param[out] : resp Return the execution result
   *
   * @return : KStatus
   */
  virtual KStatus GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) = 0;

  /**
  * @brief Get current version of series table
  *
  * @param[in] table_id   ID of the time series table
  * @param[out] version   Table version
  *
  * @return KStatus
  */
  virtual KStatus GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version) = 0;

  /**
  * @brief Get current wal level of the engine
  *
  * @param[out] wal_level   wal level
  *
  * @return KStatus
  */
  virtual KStatus GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) = 0;

  /**
   * @brief Alter table cache capacity.
   * @param ctx
   * @param capacity
   */
  virtual void AlterTableCacheCapacity(int capacity) = 0;
};

namespace kwdbts {

/**
 * @brief TSEngineImpl
 */
class TSEngineImpl : public TSEngine {
 public:
  TSEngineImpl(kwdbContext_p ctx, const std::string& ts_store_path, const EngineOptions& engine_options);

  ~TSEngineImpl() override;

  KStatus CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                        std::vector<RangeGroup> ranges) override;

  KStatus DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) override;

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                               const char* transaction_id, const uint32_t cur_version, const uint32_t new_version,
                               const std::vector<uint32_t/* tag column id*/> &index_schema) override;

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                             const char* transaction_id,  const uint32_t cur_version,
                             const uint32_t new_version) override;

  KStatus AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                              const char* transaction_id, const uint32_t old_version, const uint32_t new_version,
                              const std::vector<uint32_t/* tag column id*/> &new_index_schema) override;

  KStatus CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) override;

  KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                     ErrorInfo& err_info = getDummyErrorInfo(), uint32_t version = 0) override;

  KStatus
  GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range, roachpb::CreateTsTable* meta) override;

  KStatus PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    TSSlice* payload_data, int payload_num, uint64_t mtr_id) override;

  KStatus PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                  TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                  uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL = true) override;

  KStatus DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                          HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                          uint64_t mtr_id) override;

  KStatus DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                     uint64_t mtr_id) override;

  KStatus DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                         std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) override;

  KStatus GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) override;

  KStatus ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) override;

  KStatus CreateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id,
                           roachpb::CreateTsTable* meta, const RangeGroup& range) override;

  KStatus GetRangeGroups(kwdbContext_p ctx, const KTableKey& table_id, RangeGroups *groups) override;

  KStatus UpdateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) override;

  KStatus DeleteRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) override;

  KStatus CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id,
                                 uint64_t begin_hash, uint64_t end_hash,
                                 const KwTsSpan& ts_span, uint64_t* snapshot_id) override;

  KStatus DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) override;

  KStatus GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data) override;

  KStatus CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id,
                                   uint64_t begin_hash, uint64_t end_hash,
                                   const KwTsSpan& ts_span, uint64_t* snapshot_id) override;

  KStatus WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data) override;

  KStatus WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) override;
  KStatus WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id) override;

  KStatus DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                              const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) override;

  KStatus Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) override;

  KStatus FlushBuffer(kwdbContext_p ctx) override;

  KStatus CreateCheckpoint(kwdbContext_p ctx) override;

  KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id) override;

  KStatus Recover(kwdbContext_p ctx) override;

  KStatus TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     uint64_t range_id, uint64_t index, uint64_t& mtr_id) override;

  KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                      uint64_t range_group_id, uint64_t mtr_id) override;

  KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                        uint64_t range_group_id, uint64_t mtr_id) override;

  KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  void GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) override;

  KStatus UpdateSetting(kwdbContext_p ctx) override;

  KStatus LogInit();

  KStatus AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                    TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                     TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) override;

  KStatus AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                          TSSlice new_column, TSSlice origin_column,
                          uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  /**
  * @brief : Gets the number of remaining threads from the thread pool
  *         and available memory from system
  *
  * @param[out] : resp Return the execution result
  *
  * @return : KStatus
  */
  KStatus GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) override;

  KStatus GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version) override;

  KStatus GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) override;

  virtual KStatus Init(kwdbContext_p ctx);

  /**
   * @brief open ts engine
   * @param[out] engine
   *
   * @return KStatus
   */
  static KStatus OpenTSEngine(kwdbContext_p ctx, const std::string& primary_ts_path, const EngineOptions& engine_config,
                              TSEngine** engine);

  static KStatus OpenTSEngine(kwdbContext_p ctx, const std::string& ts_store_path, const EngineOptions& engine_config,
                              TSEngine** engine, AppliedRangeIndex* applied_indexes, size_t range_num);

  /**
   * @brief close ts engine
   * @param[out] engine
   *
   * @return KStatus
   */
  static KStatus CloseTSEngine(kwdbContext_p ctx, TSEngine* engine);

  /**
   * @brief AE Get cluster setting.
   * @param[in]   key      setting name
   * @param[out]  value    setting value, valid when func return SUCCESS.
   * @return KStatus
  */
  KStatus GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value);

  /**
   * @brief Alter table cache capacity.
   * @param ctx
   * @param capacity
   */
  void AlterTableCacheCapacity(int capacity);

  int IsSingleNode();

 private:
  string ts_store_path_;
  EngineOptions options_;
  SharedLruUnorderedMap<KTableKey, TsTable>* tables_cache_{};
  KLatch* tables_lock_;

  // store all snapshot objects of this storage engine.
  std::unordered_map<uint64_t, TsTableEntitiesSnapshot*> snapshots_;

  //  engine tables range_groups info, table open with range_group filled here.
  // std::unordered_map<uint64_t, int8_t> store table all RangeGroups, uint64_t: range_group_id, int8_t: typ
  std::unordered_map<KTableKey, std::unordered_map<uint64_t, int8_t>> tables_range_groups_;
  // LogWriter *lg_;
  WALMgr* wal_sys_{nullptr};
  TSxMgr* tsx_manager_sys_{nullptr};
  std::map<uint64_t, uint64_t> range_indexes_map_{};

  KStatus parseMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta, std::vector<AttributeInfo>& metric_schema,
                          std::vector<TagInfo>& tag_schema);

  // insert snapshot object into map snapshots_, and allocate snaphost id for this object.
  uint64_t insertToSnapshots(TsTableEntitiesSnapshot* snapshot);
  // get snapshot object from map snaphosts_. if not found return nullptr.
  TsTableEntitiesSnapshot* getSnapshot(uint64_t snapshot_id);

  void initRangeIndexMap(AppliedRangeIndex* applied_indexes, uint64_t range_num) {
    if (applied_indexes != nullptr) {
      for (int i = 0; i < range_num; i++) {
        range_indexes_map_[applied_indexes[i].range_id] = applied_indexes[i].applied_index;
      }
    }
    LOG_INFO("map for applied range indexes is initialized.");
  }

  /**
   * @brief DDL WAL recover.
   * @return KStatus
  */
  KStatus recover(kwdbContext_p ctx);

  /**
   * @brief ts engine WAL checkpoint.
   * @return KStatus
  */
  KStatus checkpoint(kwdbContext_p ctx);

  /**
   * @brief get wal mode desc string
   * @return
  */
  std::string getWalModeString(WALMode mode) {
    switch (mode) {
    case WALMode::OFF:
      return "None WAL";
    case WALMode::ON:
      return "WAL without sync";
    case WALMode::SYNC:
      return " WAL with sync";
    case WALMode::BYRL:
      return "WAL in raft log";
    default:
      return "not found";
    }
  }
};

class AggCalculator {
 public:
  AggCalculator(void* mem, DATATYPE type, int32_t size, int32_t count) :
      mem_(mem), type_(type), size_(size), count_(count) {
    if (is_overflow_) {
      sum_type_ = (DATATYPE)DOUBLE;
      sum_size_ = sizeof(double);
    } else {
      sum_type_ = type_;
      sum_size_ = getSumSize(type_);
    }
  }

  AggCalculator(void* mem, DATATYPE type, int32_t size, int32_t count, bool is_overflow) :
      mem_(mem), type_(type), size_(size), count_(count), is_overflow_(is_overflow) {
    if (is_overflow_) {
      sum_type_ = (DATATYPE)DOUBLE;
      sum_size_ = sizeof(double);
    } else {
      sum_type_ = type_;
      sum_size_ = getSumSize(type_);
    }
  }

  AggCalculator(void* mem, void* bitmap, size_t first_row, DATATYPE type, int32_t size, int32_t count) :
      mem_(mem), bitmap_(bitmap), first_row_(first_row), type_(type), size_(size), count_(count) {
    if (is_overflow_) {
      sum_type_ = (DATATYPE)DOUBLE;
      sum_size_ = sizeof(double);
    } else {
      sum_type_ = type_;
      sum_size_ = getSumSize(type_);
    }
  }

  void* GetMax(void* base = nullptr, bool need_to_new = false);

  void* GetMin(void* base = nullptr, bool need_to_new = false);

  bool GetSum(void** sum_res, void* base = nullptr, bool is_overflow = false);

  bool CalAllAgg(void* min_base, void* max_base, void* sum_base, void* count_base,
                 bool block_first_line, const BlockSpan& span);

  void UndoAgg(void* min_base, void* max_base, void* sum_base, void* count_base);

 private:
  int cmp(void* l, void* r);

  bool isnull(size_t row);

  bool isDeleted(char* delete_flags, size_t row);

  void* changeBaseType(void* base);

 private:
  void* mem_;
  void* bitmap_ = nullptr;
  size_t first_row_;
  DATATYPE type_;
  int32_t size_;
  uint16_t count_;
  bool is_overflow_ = false;
  DATATYPE sum_type_;
  int32_t sum_size_;
};

class VarColAggCalculator {
 public:
  VarColAggCalculator(const std::vector<std::shared_ptr<void>>& var_mem, int32_t count) : var_mem_(var_mem), count_(count) {}

  VarColAggCalculator(const std::vector<std::shared_ptr<void>>& var_mem, void* bitmap,
                      size_t first_row, int32_t size, int32_t count) :
                      var_mem_(var_mem), bitmap_(bitmap), first_row_(first_row), size_(size), count_(count) {
  }

  VarColAggCalculator(void* mem, const std::vector<std::shared_ptr<void>>& var_mem, void* bitmap,
                      size_t first_row, int32_t size, int32_t count) :
                      mem_(mem), var_mem_(var_mem), bitmap_(bitmap), first_row_(first_row), size_(size), count_(count) {
  }

  std::shared_ptr<void> GetMax(std::shared_ptr<void> base = nullptr);

  std::shared_ptr<void> GetMin(std::shared_ptr<void> base = nullptr);

  void CalAllAgg(void* min_base, void* max_base, std::shared_ptr<void> var_min_base,
                 std::shared_ptr<void> var_max_base, void* count_base, bool block_first_line, const BlockSpan& span);

  static void CalAllAgg(std::list<std::shared_ptr<void>> var_values, int* min_idx, int* max_idx) {
    void* var_max;
    void* var_min;
    *min_idx = -1;
    *max_idx = -1;
    int i = 0;
    auto iter = var_values.begin();
    while (iter != var_values.end()) {
      auto cur_value = iter->get();
      iter++;
      if (*max_idx < 0 || cmp(cur_value, var_max)) {
        *max_idx = i;
        var_max = cur_value;
      }
      if (*min_idx < 0 || !cmp(cur_value, var_min)) {
        *min_idx = i;
        var_min = cur_value;
      }
      i++;
    }
  }

 private:
  static int cmp(void* l, void* r);
  bool isnull(size_t row);
  bool isDeleted(char* delete_flags, size_t row);

 private:
  void* mem_;
  std::vector<std::shared_ptr<void>> var_mem_;
  void* bitmap_ = nullptr;
  size_t first_row_;
  int32_t size_;
  uint16_t count_;
};

}  //  namespace kwdbts
