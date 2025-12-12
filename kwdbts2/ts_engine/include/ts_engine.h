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

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cm_kwdb_context.h"
#include "engine.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_batch_data_worker.h"
#include "ts_common.h"
#include "ts_engine_schema_manager.h"
#include "ts_flush_manager.h"
#include "ts_partition_interval_recorder.h"
#include "ts_table_v2_impl.h"
#include "ts_version.h"
#include "ts_vgroup.h"
#include "ts_table_del_info.h"
#include "ts_drop_manager.h"

extern bool g_go_start_service;

namespace kwdbts {

struct TsRangeImgrationInfo {
  uint64_t id;    // snapshot ID
  uint8_t type;   // type , 0: Build snapshot (source side read), 1: write snapshot (target side merge)
  uint64_t begin_hash;
  uint64_t end_hash;
  KwTsSpan ts_span;
  KTableKey table_id;
  uint32_t table_version;
  uint32_t package_id;
  uint64_t imgrated_rows;
  std::shared_ptr<TsTable> table;
  bool batch_read_finished;
  std::shared_ptr<STTableRangeDelAndTagInfo> del_iter;
};

/**
 * @brief TSEngineV2Impl
 */
class TSEngineImpl : public TSEngine {
 private:
  std::unique_ptr<TsEngineSchemaManager> schema_mgr_ = nullptr;
  std::vector<std::shared_ptr<TsVGroup>> vgroups_;
  int vgroup_max_num_{0};
  EngineOptions options_;
  std::mutex table_mutex_;
  std::mutex snapshot_mutex_;
  std::shared_mutex wal_level_mutex_;
  std::unordered_map<uint64_t, TsRangeImgrationInfo> snapshots_;
  std::unique_ptr<WALMgr> wal_mgr_ = nullptr;
  std::map<uint64_t, uint64_t> range_indexes_map_;
  std::unique_ptr<WALMgr> wal_sys_ = nullptr;
  std::unique_ptr<TSxMgr> tsx_manager_sys_ = nullptr;

  std::unordered_map<uint64_t, std::unordered_map<std::string, std::shared_ptr<TsBatchDataWorker>>> read_batch_data_workers_;
  KRWLatch read_batch_workers_lock_;
  std::unordered_map<uint64_t, std::shared_ptr<TsBatchDataWorker>> write_batch_data_workers_;
  KRWLatch write_batch_workers_lock_;
  std::atomic<bool> exist_explict_txn = false;

  TsHashRWLatch tag_lock_;
  // std::unique_ptr<TsMemSegmentManager> mem_seg_mgr_ = nullptr;
  PartitionIntervalRecorder* interval_recorder_ = nullptr;

 public:
  explicit TSEngineImpl(const EngineOptions& engine_options);

  ~TSEngineImpl() override;

  KStatus CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                        std::vector<RangeGroup> ranges, bool not_get_table) override;

  KStatus DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) override;

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
    const char* transaction_id, bool& is_dropped, const uint32_t cur_version, const uint32_t new_version,
    const std::vector<uint32_t/* tag column id*/> &index_schema) override;

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
    const char* transaction_id, bool& is_dropped, const uint32_t cur_version, const uint32_t new_version) override;

  KStatus AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
    const char* transaction_id, bool& is_dropped, const uint32_t old_version, const uint32_t new_version,
    const std::vector<uint32_t/* tag column id*/> &new_index_schema) override;

  KStatus CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) override {
    LOG_WARN("should not use CompressTsTable any more.");
    return KStatus::SUCCESS;
  }

  KStatus CheckAndDropTsTable(kwdbContext_p ctx, const KTableKey& table_id, bool& is_dropped, ErrorInfo& err_info);

  KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table, bool& is_dropped,
                     bool create_if_not_exist = true, ErrorInfo& err_info = getDummyErrorInfo(),
                     uint32_t version = 0) override;

  KStatus ProcessDrop(const KTableKey& table_id);

  std::vector<std::shared_ptr<TsVGroup>>* GetTsVGroups();

  std::shared_ptr<TsVGroup> GetTsVGroup(uint32_t vgroup_id);

  KStatus GetTableSchemaMgr(kwdbContext_p ctx, const KTableKey& table_id, bool& is_dropped,
                         std::shared_ptr<TsTableSchemaManager>& schema) override;

  KStatus GetAllTableSchemaMgrs(std::vector<std::shared_ptr<TsTableSchemaManager>>& tb_schema_mgr) {
    auto s = schema_mgr_->GetAllTableSchemaMgrs(tb_schema_mgr);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    return KStatus::SUCCESS;
  }

  KStatus InsertTagData(kwdbContext_p ctx, const std::shared_ptr<TsTableSchemaManager>& tb_schema,
                        uint64_t mtr_id, TSSlice payload_data, bool write_wal, uint32_t& vgroup, TSEntityID& entity_id);

  KStatus
  GetMetaData(kwdbContext_p ctx, const KTableKey& table_id, RangeGroup range, roachpb::CreateTsTable* meta,
              bool& is_dropped) override;

  KStatus PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    TSSlice* payload_data, int payload_num, uint64_t mtr_id, bool& is_dropped) override;

  KStatus PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
    TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
    DedupResult* dedup_result, bool writeWAL = true, const char* tsx_id = nullptr) override;

  KStatus DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                          HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                          uint64_t mtr_id, uint64_t osn, bool& is_dropped) override;

  KStatus DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                     uint64_t mtr_id, uint64_t osn, bool& is_dropped) override;

  KStatus DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                         std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id,
                         bool& is_dropped, uint64_t osn) override;

  KStatus CountRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                          HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                          uint64_t mtr_id, uint64_t osn) override;

  KStatus GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) override {
    LOG_WARN("should not use GetBatchRepr any more.");
    return KStatus::SUCCESS;
  }

  KStatus ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) override {
    LOG_WARN("should not use ApplyBatchRepr any more.");
    return KStatus::SUCCESS;
    }

  // range imgration snapshot using interface...............begin................................
  KStatus CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id, uint64_t begin_hash, uint64_t end_hash,
                              const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped) override;
  KStatus DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) override;
  KStatus GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data, bool& is_dropped) override;
  KStatus CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id, uint64_t begin_hash, uint64_t end_hash,
                              const KwTsSpan& ts_span, uint64_t* snapshot_id, bool& is_dropped, uint64_t osn) override;
  KStatus WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data, bool& is_dropped) override;
  KStatus WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) override;
  KStatus WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id, uint64_t osn) override;
  // range imgration snapshot using interface...............end................................
  KStatus DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                              const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id,
                              bool& is_dropped, uint64_t osn) override;

  KStatus ReadBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t begin_hash,
                        uint64_t end_hash, KwTsSpan ts_span, uint64_t job_id, TSSlice* data,
                        uint32_t* row_num, bool& is_dropped) override;

  KStatus WriteBatchData(kwdbContext_p ctx, TSTableID table_id, uint64_t table_version, uint64_t job_id,
                         TSSlice* data, uint32_t* row_num, bool& is_dropped) override;

  KStatus CancelBatchJob(kwdbContext_p ctx, uint64_t job_id, uint64_t osn) override;

  KStatus BatchJobFinish(kwdbContext_p ctx, uint64_t job_id) override;


  KStatus FlushBuffer(kwdbContext_p ctx) override;

  KStatus CreateCheckpoint(kwdbContext_p ctx) override;

  KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id, bool& is_dropped) override {
    LOG_WARN("should not use CreateCheckpointForTable any more.");
    return KStatus::SUCCESS;
  }

  KStatus Recover(kwdbContext_p ctx) override;

  // get max entity id
  KStatus GetMaxEntityIdByVGroupId(kwdbContext_p ctx, uint32_t vgroup_id, uint32_t& entity_id);

  KStatus TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id = nullptr) override;

  KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                      uint64_t range_group_id, uint64_t mtr_id, const char* tsx_id = nullptr) override;

  KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id, uint64_t mtr_id,
                        bool skip_log = false, const char* tsx_id = nullptr) override;

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

  KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) override;

  KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) override;

  KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped) override;

  void GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) override;

  KStatus UpdateSetting(kwdbContext_p ctx) override;

  KStatus UpdateAtomicLSN();

  KStatus LogInit();

  KStatus AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
                    TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
                     TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id, bool& is_dropped,
    TSSlice new_column, TSSlice origin_column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) override {
    LOG_WARN("should not use AlterPartitionInterval any more.");
    return KStatus::SUCCESS;
  }

  KStatus AlterLifetime(kwdbContext_p ctx, const KTableKey& table_id, uint64_t lifetime, bool& is_dropped) override;

  KStatus GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) override;
  KStatus GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version, bool& is_dropped) override;
  KStatus GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) override;
  KStatus SetUseRaftLogAsWAL(kwdbContext_p ctx, bool use) override;
  static KStatus CloseTSEngine(kwdbContext_p ctx, TSEngine* engine) { return KStatus::SUCCESS; }
  KStatus GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value);
  void AlterTableCacheCapacity(int capacity)  override {}

  KStatus SortWALFile(kwdbContext_p ctx);

  KStatus FlushVGroups(kwdbContext_p ctx) override;

  // init all engine.
  KStatus Init(kwdbContext_p ctx);

  void PreClearDroppedTables();

  KStatus CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable* meta,
                        std::shared_ptr<TsTable>& ts_table);

  KStatus GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta);

  std::string GetDbDir() const { return options_.db_path; }

  WALMode GetWalMode() {
    return static_cast<WALMode>(options_.wal_level);
  }

  std::unique_ptr<TsEngineSchemaManager>& GetEngineSchemaManager() {
    return schema_mgr_;
  }

  KStatus DropResidualTsTable(kwdbContext_p ctx) override;

  static uint64_t GetAppliedIndex(const uint64_t range_id, const std::map<uint64_t, uint64_t>& range_indexes_map) {
    const auto iter = range_indexes_map.find(range_id);
    if (iter == range_indexes_map.end()) {
      return 0;
    }
    return iter->second;
  }

  KStatus Vacuum(kwdbContext_p ctx, bool force) override;
  void initRangeIndexMap(AppliedRangeIndex* applied_indexes, uint64_t range_num) {
    if (applied_indexes != nullptr) {
      for (int i = 0; i < range_num; i++) {
        range_indexes_map_[applied_indexes[i].range_id] = applied_indexes[i].applied_index;
      }
    }
    LOG_INFO("map for applied range indexes is initialized.");
  }

  bool EnableWAL() {
    return options_.wal_level != WALMode::OFF && !options_.use_raft_log_as_wal;
  }

  KStatus RemoveChkFile(kwdbContext_p ctx, uint32_t vgroup_id);

  KStatus ParallelRemoveChkFiles(kwdbContext_p ctx);

  bool HasDroppedFlag(TSTableID id);

 private:
  TsVGroup* GetVGroupByID(kwdbContext_p ctx, uint32_t vgroup_id);

  KStatus putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id, TsRawPayload& payload);

  uint64_t insertToSnapshotCache(TsRangeImgrationInfo& snapshot);

  void createDroppedFlag(TSTableID table_id);

  void removeDroppedFlag(TSTableID table_id);
};

}  //  namespace kwdbts
