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
#include <unordered_set>
#include <string>
#include <vector>
#include "ts_common.h"
#include "libkwdbts2.h"
#include "cm_kwdb_context.h"
#include "cm_func.h"
#include "lg_api.h"
#include "iterator.h"
#include "tag_iterator.h"
#include "payload.h"
#include "mmap/mmap_tag_column_table.h"
#include "st_wal_internal_log_structure.h"
#include "lt_rw_latch.h"
#include "mmap/mmap_tag_table.h"

namespace kwdbts {

class TsStorageIterator;
class BaseEntityIterator;
class MetaIterator;
class EntityGroupTagIterator;
class EntityGroupMetaIterator;

// in distributed Verison2, every ts table has just one entitygroup
const uint64_t default_entitygroup_id_in_dist_v2 = 1;

enum OperatorTypeOfRecord : uint8_t {
  OP_TYPE_UNKNOWN = 0,
  OP_TYPE_INSERT,
  OP_TYPE_TAG_UPDATE,
  OP_TYPE_TAG_DELETE,
  OP_TYPE_METRIC_DELETE,
  OP_TYPE_TAG_EXISTED,  // TAG created before osn range.
};

struct OperatorInfoOfRecord {
  OperatorTypeOfRecord type;
  TS_OSN osn;
  uint32_t p_tag_id;
  uint32_t row_num;
  OperatorInfoOfRecord(OperatorTypeOfRecord t, TS_OSN o, uint32_t ptag_id, int rownum) :
    type(t), osn(o), p_tag_id(ptag_id), row_num(rownum) {}
};

class TsTable {
 public:
  TsTable();

  TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id);

  virtual ~TsTable();

  /**
   * @brief Is the current table created and does it really exist
   *
   * @return bool
   */
  virtual bool IsExist() {
    return false;
  }

  virtual KStatus CheckAndAddSchemaVersion(kwdbContext_p ctx, const KTableKey& table_id, uint64_t version) = 0;

  /**
   * @brief Query Table Column Definition
   *
   * @return std::vector<AttributeInfo>
   */
  KStatus GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema,
                                      uint32_t table_version = 0);

  /**
   * @brief Query Table Column Definition
   *
   * @return std::vector<AttributeInfo>
   */
  KStatus GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema);

  // convert schema info to protobuf
  virtual KStatus GenerateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                             const std::vector<AttributeInfo>& metric_schema,
                             std::vector<TagInfo>& tag_schema, uint32_t schema_version) = 0;
  /**
   * @brief get table id
   *
   * @return KTableKey
   */
  virtual KTableKey GetTableId() {
    return table_id_;
  }

  virtual uint32_t GetCurrentTableVersion() = 0;

  /**
   * @brief create ts table
   * @param[in] metric_schema schema
   *
   * @return KStatus
   */
  virtual KStatus Create(kwdbContext_p ctx, vector<AttributeInfo>& metric_schema, uint32_t ts_version = 1,
                         uint64_t partition_interval = kwdbts::EngineOptions::iot_interval, uint64_t hash_num = 2000);

  std::string GetStoreDirectory() {
    return db_path_ + tbl_sub_path_;
  }

  virtual KStatus GetAvgTableRowSize(kwdbContext_p ctx, uint64_t* row_size) = 0;

  virtual KStatus GetDataVolume(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, uint64_t* volume) = 0;

  virtual KStatus GetDataVolumeHalfTS(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, timestamp64* half_ts) = 0;

  /**
   * @brief drop all data in range. if table is empty,we will drop table directory at same time.
   * @param[in] ts_span   timestamp span
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   *
   * @return KStatus
   */
  virtual KStatus DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                    KwTsSpan ts_span, uint64_t mtr_id, uint64_t osn) = 0;

  /**
   * @brief Get range row count.
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   * @param[in] ts_span   timestamp span
   *
   * @return KStatus
   */
  virtual KStatus GetRangeRowCount(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                            KwTsSpan ts_span, uint64_t* count) = 0;

  /**
   * @brief Delete data within a hash range, usually used for data migration.
   * @param[in] range_group_id RangeGroupID
   * @param[in] hash_span The range of hash IDs to be deleted from the data
   * @param[out] count delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                      uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del) = 0;

  /**
   * @brief Delete data based on the hash id range and timestamp range.
   * @param[in] range_group_id RangeGroupID
   * @param[in] hash_span The range of hash IDs to be deleted from the data
   * @param[in] ts_spans The range of timestamps to be deleted from the data
   * @param[out] count The number of rows of data that have been deleted
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @return
   */
  virtual KStatus DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan &hash_span,
                            const std::vector<KwTsSpan> &ts_spans, uint64_t *count, uint64_t mtr_id, uint64_t osn) = 0;

  /**
   * @brief Delete data based on the primary tag and timestamp range.
   * @param[in] range_group_id RangeGroupID
   * @param[in] primary_tag The primary tag of the deleted data
   * @param[in] ts_spans The range of timestamps to be deleted from the data
   * @param[out] count The number of rows of data that have been deleted
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @return KStatus
   */
  virtual KStatus DeleteData(kwdbContext_p ctx, uint64_t range_group_id, std::string &primary_tag,
                             const std::vector<KwTsSpan> &ts_spans, uint64_t *count, uint64_t mtr_id, uint64_t osn) = 0;

  virtual KStatus CountRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan &hash_span,
                                  const std::vector<KwTsSpan> &ts_spans, uint64_t *count, uint64_t mtr_id, uint64_t osn) = 0;

  /**
    * @brief Create the iterator TsStorageIterator for the timeline and query the data of all entities within the Leader EntityGroup
    * @param[in] ts_span
    * @param[in] scan_cols  column to read
    * @param[in] scan_agg_types Read column agg type array for filtering block statistics information
    * @param[in] table_version The maximum table version that needs to be queried
    * @param[out] TsStorageIterator*
    */
  virtual KStatus GetNormalIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter) = 0;

  virtual KStatus GetOffsetIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter) = 0;

  virtual KStatus GetIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter);

  // scan metric data by osn range. return all rows
  virtual KStatus GetMetricIteratorByOSN(kwdbContext_p ctx, k_uint32 table_version, std::vector<k_uint32>& scan_cols,
    std::vector<EntityResultIndex>& entity_ids, std::vector<KwOSNSpan>& osn_span, std::vector<KwTsSpan>& ts_spans,
    TsIterator** iter) = 0;

  // scan tag data by osn range. return all rows
  virtual KStatus GetTagIteratorByOSN(kwdbContext_p ctx, k_uint32 table_version, std::vector<k_uint32>& scan_cols,
    std::vector<KwOSNSpan>& osn_span, std::vector<HashIdSpan>* hps, BaseEntityIterator** iter) = 0;
  // scan tag by primary key.
  virtual KStatus GetEntityIdListByOSN(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
            std::vector<KwOSNSpan>& osn_span,
            std::vector<k_uint32>& scan_cols,
            std::vector<HashIdSpan>* hps,
            std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
            uint32_t table_version) = 0;
  /**
   * @brief get entityId List
   * @param[in] primary_tags primaryTag
   * @param[in] scan_tags    scan tag
   * @param[out] entityId List
   * @param[out] res
   * @param[out] count
   *
   * @return KStatus
   */
  virtual KStatus
  GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                  const std::vector<uint64_t/*index_id*/> &tags_index_id,
                  const std::vector<void*> tags,
                  TSTagOpType op_type,
                  const std::vector<uint32_t>& scan_tags,
                  const std::vector<HashIdSpan>* hps,
                  std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                  uint32_t table_version = 1) = 0;

  /**
 * @brief get entity row
 * @param[in] scan_tags    scan tag
 * @param[in] entityId List
 * @param[out] res
 * @param[out] count
 *
 * @return KStatus
 */
  virtual KStatus
  GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
             const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count,
             uint32_t table_version) = 0;

  virtual KStatus GetEntityIdByHashSpan(kwdbContext_p ctx, const HashIdSpan& hash_span,
                                        vector<EntityResultIndex>& entity_store) = 0;

  /**
   * @brief Create an iterator TsStorageIterator for Tag tables
   * @param[in] scan_tags tag index
   * @param[out] BaseEntityIterator**
   */
  virtual KStatus GetTagIterator(kwdbContext_p ctx,
                                 std::vector<uint32_t> scan_tags,
                                 std::vector<HashIdSpan>* hps,
                                 BaseEntityIterator** iter, k_uint32 table_version) = 0;

  virtual KStatus AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                             uint32_t cur_version, uint32_t new_version, string& msg) = 0;

  virtual KStatus CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                       const uint32_t cur_version, const uint32_t new_version,
                                       const std::vector<uint32_t/* tag column id*/>&) = 0;

  virtual std::vector<uint32_t> GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) = 0;

  virtual KStatus DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,
                                     const uint32_t cur_version, const uint32_t new_version, const uint64_t index_id) = 0;

  virtual KStatus UndoCreateIndex(kwdbContext_p ctx, LogEntry* log) = 0;

  virtual KStatus UndoDropIndex(kwdbContext_p ctx, LogEntry* log) = 0;

  KStatus UndoAlterTable(kwdbContext_p ctx, LogEntry* log);

  virtual KStatus undoAlterTable(kwdbContext_p ctx, AlterType alter_type,
                                 roachpb::KWDBKTSColumn* column, uint32_t cur_version, uint32_t new_version) = 0;

  virtual KStatus AlterPartitionInterval(kwdbContext_p ctx, uint64_t partition_interval);

  virtual uint64_t GetPartitionInterval();

  virtual void SetDropped();

  virtual bool IsDropped();

  /**
    * @brief clean ts table
    *
    * @return KStatus
    */
  virtual KStatus TSxClean(kwdbContext_p ctx) = 0;

  virtual uint64_t GetHashNum();

  virtual KStatus GetLastRowEntity(kwdbContext_p ctx, EntityResultIndex& entity_id, timestamp64& entity_last_ts) = 0;

  virtual KStatus GetLastRowBatch(kwdbContext_p ctx, uint32_t table_version, std::vector<uint32_t> scan_cols,
                                  ResultSet* res, k_uint32* count, bool& valid);

 protected:
  string db_path_;
  KTableKey table_id_;
  string tbl_sub_path_;
  uint64_t hash_num_ = 0;

//  MMapTagColumnTable* tag_bt_;

  std::atomic_bool is_dropped_;

 protected:
  using TsTableEntityGrpsRwLatch = KRWLatch;
  TsTableEntityGrpsRwLatch* entity_groups_mtx_{nullptr};
  using TsTableVersionRwLatch = KRWLatch;
  TsTableVersionRwLatch* table_version_rw_lock_;

 private:
  using TsTableSnapshotLatch = KLatch;
  TsTableSnapshotLatch* snapshot_manage_mtx_{nullptr};

  void latchLock() {
    MUTEX_LOCK(snapshot_manage_mtx_);
  }

  void latchUnlock() {
    MUTEX_UNLOCK(snapshot_manage_mtx_);
  }
};


struct PartitionPayload {
  int32_t start_row;
  int32_t end_row;
};

}  // namespace kwdbts
