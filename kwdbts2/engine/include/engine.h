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
#include "br_mgr.h"
#include "ts_table_schema_manager.h"

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
                                std::vector<RangeGroup> ranges, bool not_get_table = true) = 0;

  /**
 * @brief drop ts table
 * @param[in] table_id
 *
 * @return KStatus
 */
  virtual KStatus DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) = 0;

  virtual KStatus DropResidualTsTable(kwdbContext_p ctx) = 0;

  virtual KStatus CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
  const char* transaction_id, bool& is_dropped, const uint32_t cur_version, const uint32_t new_version,
  const std::vector<uint32_t/* tag column id*/> &index_schema) = 0;

  virtual KStatus DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
  const char* transaction_id, bool& is_dropped, const uint32_t cur_version, const uint32_t new_version) = 0;

  virtual KStatus AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
    const char* transaction_id, bool& is_dropped, const uint32_t old_version, const uint32_t new_version,
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
   * @param[in] table_id id of the time series table
   * @param[out] ts_table ts table
   * @param[in] create_if_not_exist whether to create ts table if not exist
   * @param[out] err_info error info
   * @param[in] version version of the table
   *
   * @return KStatus
   */
  virtual KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                             bool& is_dropped, bool create_if_not_exist = true, ErrorInfo& err_info = getDummyErrorInfo(),
                             uint32_t version = 0) = 0;


  virtual KStatus GetTableSchemaMgr(kwdbContext_p ctx, const KTableKey& table_id, bool& is_dropped,
                                 std::shared_ptr<TsTableSchemaManager>& schema) {
    return KStatus::FAIL;
  }

  /**
  * @brief get meta info of ts table
  * @param[in] table_id
  * @param[in] meta
  *
  * @return KStatus
  */
  virtual KStatus GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range,
                              roachpb::CreateTsTable* meta, bool& is_dropped) = 0;

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
  virtual KStatus
  PutEntity(kwdbContext_p ctx, const KTableKey &table_id, uint64_t range_group_id, TSSlice *payload_data,
            int payload_num, uint64_t mtr_id, bool& is_dropped) = 0;

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
    TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
    DedupResult* dedup_result, bool writeWAL = true, const char* tsx_id = nullptr) = 0;

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
  virtual KStatus DeleteRangeData(kwdbContext_p ctx, const KTableKey &table_id, uint64_t range_group_id,
                                  HashIdSpan &hash_span, const std::vector<KwTsSpan> &ts_spans,
                                  uint64_t *count, uint64_t mtr_id, uint64_t osn, bool& is_dropped) = 0;

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
  virtual KStatus DeleteData(kwdbContext_p ctx, const KTableKey &table_id, uint64_t range_group_id,
                             std::string &primary_tag, const std::vector<KwTsSpan> &ts_spans, uint64_t *count,
                             uint64_t mtr_id, uint64_t osn, bool& is_dropped) = 0;

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
  virtual KStatus DeleteEntities(kwdbContext_p ctx, const KTableKey &table_id, uint64_t range_group_id,
                                 std::vector<std::string> primary_tags, uint64_t *count, uint64_t mtr_id,
                                 bool& is_dropped, uint64_t osn = 0) = 0;

  /**
 * @brief Count data of some specified entities within a specified time range by marking
 * @param[in] table_id    ID of the time series table
 * @param[in] range_group_id  RangeGroup ID
 * @param[in] hash_span   Entities within certain hash range
 * @param[in] ts_spans    Time range for deleting data
 * @param[out] count  Number of data rows
 * @param[in] mtr_id  Mini-transaction id for TS table
 *
 * @return KStatus
 */
  virtual KStatus CountRangeData(kwdbContext_p ctx, const KTableKey &table_id, uint64_t range_group_id,
                                  HashIdSpan &hash_span, const std::vector<KwTsSpan> &ts_spans,
                                  uint64_t *count, uint64_t mtr_id, uint64_t osn) = 0;

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
    uint64_t begin_hash, uint64_t end_hash, const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped) {
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
  virtual KStatus GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data, bool& is_dropped) {
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
                                   const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped, uint64_t osn = 0) {
    return KStatus::FAIL;
  }


  /**
   * @brief receive data and store to current dest node batch by batch
   * @param[in] data  bytes
   *
   * @return KStatus
   */
  virtual KStatus WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data, bool& is_dropped) {
    return KStatus::FAIL;
  }
  virtual KStatus WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) {
    return KStatus::FAIL;
  }

  virtual KStatus WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id, uint64_t osn) {
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
                                      const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id,
                                      bool& is_dropped, uint64_t osn = 0) {
    return KStatus::FAIL;
  }

  virtual KStatus ReadBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t begin_hash,
                                uint64_t end_hash, KwTsSpan ts_span, uint64_t job_id, TSSlice* data,
                                uint32_t* row_num, bool& is_dropped) {
    return FAIL;
  }

  virtual KStatus WriteBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t job_id,
                                 TSSlice* data, uint32_t* row_num, bool& is_dropped) {
    return FAIL;
  }

  virtual KStatus CancelBatchJob(kwdbContext_p ctx, uint64_t job_id, uint64_t osn) {
    return FAIL;
  }

  virtual KStatus BatchJobFinish(kwdbContext_p ctx, uint64_t job_id) {
    return FAIL;
  }

  virtual KStatus AllWriteBatchJobFinish(kwdbContext_p ctx) {
    return FAIL;
  }

  /**
 * @brief  calculate pushdown
 * @param[in] req
 * @param[out]  resp
 *
 * @return KStatus
 */
  virtual KStatus Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp);

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
  virtual KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id, bool& is_dropped) = 0;

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
                             uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id = nullptr) = 0;

  /**
    * @brief commit mini-transaction
    * @param[in] table_id Identifier of TS table.
    * @param[in] range_group_id The target EntityGroup ID.
    * @param[in] mtr_id Mini-transaction id for TS table.
    *
    * @return KStatus
    */
  virtual KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                              uint64_t range_group_id, uint64_t mtr_id, const char* tsx_id = nullptr) = 0;

  /**
    * @brief rollback mini-transaction
    * @param[in] table_id Identifier of TS table.
    * @param[in] range_group_id The target EntityGroup ID.
    * @param[in] mtr_id Mini-transaction id for TS table.
    *
    * @return KStatus
    */
  virtual KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id, uint64_t mtr_id,
                                bool skip_log = false, const char* tsx_id = nullptr) = 0;
  /**
    * @brief begin one transaction.
    * @param[in] table_id  ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) = 0;

  /**
    * @brief commit one transaction.
    * @param[in] table_id   ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) = 0;

  /**
    * @brief rollback one transaction.
    * @param[in] table_id ID
    * @param[in] range_group_id RangeGroup ID
    * @param[in] transaction_id transaction ID
    *
    * @return KStatus
    */
  virtual KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) = 0;

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
  virtual KStatus AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
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
  virtual KStatus DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
                             TSSlice column, uint32_t cur_version, uint32_t new_version, string& msg) = 0;

  virtual KStatus AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) = 0;

  virtual KStatus AlterLifetime(kwdbContext_p ctx, const KTableKey& table_id, uint64_t lifetime, bool& is_dropped) = 0;
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
  virtual KStatus AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
    TSSlice new_column, TSSlice origin_column, uint32_t cur_version, uint32_t new_version, string& msg) = 0;

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
  virtual KStatus GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version, bool& is_dropped) = 0;

  /**
  * @brief Get current wal level of the engine
  *
  * @param[out] wal_level   wal level
  *
  * @return KStatus
  */
  virtual KStatus GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) = 0;

  /**
   * @brief Set whether use raft log as WAL
   *
   * @param[in] use means use raft log as WAL or not
   *
   * @return KStatus
   */
  virtual KStatus SetUseRaftLogAsWAL(kwdbContext_p ctx, bool use) = 0;

  virtual KStatus FlushVGroups(kwdbContext_p ctx) = 0;

  /**
   * @brief Alter table cache capacity.
   * @param ctx
   * @param capacity
   */
  virtual void AlterTableCacheCapacity(int capacity) = 0;

  virtual KStatus Vacuum(kwdbContext_p ctx, bool force) {
    return SUCCESS;
  }

  virtual KStatus GetTableBlocksDistribution(TSTableID table_id, TSSlice* blocks_info) = 0;

  virtual KStatus GetDBBlocksDistribution(uint32_t db_id, TSSlice* blocks_info) = 0;

 protected:
  SharedFixedUnorderedMap<KTableKey, TsTable>* tables_cache_{};
};

namespace kwdbts {
  template<class T>
bool AddAggInteger(T& a, T b) {
  T c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = c;
  return false;
}

template<class T1, class T2>
bool AddAggInteger(T1& a, T2 b) {
  T1 c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = c;
  return false;
}

template<class T1, class T2>
void AddAggFloat(T1& a, T2 b) {
  a = a + b;
}

template<class T>
void SubAgg(T& a, T b) {
  a = a - b;
}

}  //  namespace kwdbts
