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
#include "ts_vgroup.h"
#include "ts_engine_schema_manager.h"
#include "engine.h"
#include "ts_table_v2_impl.h"
#include "ts_flush_manager.h"
extern bool g_go_start_service;

namespace kwdbts {

/**
 * @brief TSEngineV2Impl
 */
class TSEngineV2Impl : public TSEngine {
 private:
  std::unique_ptr<TsEngineSchemaManager> schema_mgr_ = nullptr;
  std::vector<std::shared_ptr<TsVGroup>> vgroups_;
  int vgroup_max_num_{0};
  EngineOptions options_;
  std::mutex table_mutex_;
  TsLSNFlushManager flush_mgr_;
  std::unique_ptr<WALMgr> wal_mgr_ = nullptr;
  std::map<uint64_t, uint64_t> range_indexes_map_{};
  std::unique_ptr<WALMgr> wal_sys_{nullptr};
  std::unique_ptr<TSxMgr> tsx_manager_sys_{nullptr};

  // std::unique_ptr<TsMemSegmentManager> mem_seg_mgr_ = nullptr;

 public:
  explicit TSEngineV2Impl(const EngineOptions& engine_options);

  ~TSEngineV2Impl() override;

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

  KStatus CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) override {
    return KStatus::SUCCESS;
  }

  KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                     bool create_if_not_exist = true, ErrorInfo& err_info = getDummyErrorInfo(),
                     uint32_t version = 0) override;

  std::vector<std::shared_ptr<TsVGroup>>* GetTsVGroups();
  KStatus GetTableSchemaMgr(kwdbContext_p ctx, const KTableKey& table_id,
                         std::shared_ptr<TsTableSchemaManager>& schema) override {
    // TODO(liangbo01)  need input change version
    auto s = schema_mgr_->GetTableSchemaMgr(table_id, schema);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    return KStatus::SUCCESS;
  }

  KStatus
  GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range, roachpb::CreateTsTable* meta) override {
    // TODO(liumengzhen) check version
    return KStatus::SUCCESS;
  }

  KStatus PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    TSSlice* payload_data, int payload_num, uint64_t mtr_id) override { return KStatus::SUCCESS; }

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

  KStatus GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) override { return KStatus::SUCCESS; }

  KStatus ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) override { return KStatus::SUCCESS; }

  KStatus CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id,
                                 uint64_t begin_hash, uint64_t end_hash,
                                 const KwTsSpan& ts_span, uint64_t* snapshot_id) override { return KStatus::SUCCESS; }

  KStatus DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) override { return KStatus::SUCCESS; }

  KStatus GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data) override {
    return KStatus::SUCCESS;
  }

  KStatus CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id,
                                   uint64_t begin_hash, uint64_t end_hash,
                                   const KwTsSpan& ts_span, uint64_t* snapshot_id) override { return KStatus::SUCCESS; }

  KStatus WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data) override {
    return KStatus::SUCCESS;
  }

  KStatus WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) override { return KStatus::SUCCESS; }
  KStatus WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id) override { return KStatus::SUCCESS; }

  KStatus DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                              const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) override;


  KStatus FlushBuffer(kwdbContext_p ctx) override { return KStatus::SUCCESS; }

  KStatus CreateCheckpoint(kwdbContext_p ctx) override;

  KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id) override { return KStatus::SUCCESS; }

  KStatus Recover(kwdbContext_p ctx) override;

  KStatus TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     uint64_t range_id, uint64_t index, uint64_t& mtr_id) override;

  KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                      uint64_t range_group_id, uint64_t mtr_id) override;

  KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                        uint64_t range_group_id, uint64_t mtr_id) override;

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

  KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override;

  void GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) override { exit(0); }

  KStatus UpdateSetting(kwdbContext_p ctx) override;

  KStatus LogInit();

  KStatus AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                    TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                     TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) override;

  KStatus AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                          TSSlice new_column, TSSlice origin_column, uint32_t cur_version,
                          uint32_t new_version, string& err_msg) override;

  KStatus AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) override {
    return KStatus::SUCCESS;
  }

  KStatus GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) override { return KStatus::SUCCESS; }
  KStatus GetTableVersion(kwdbContext_p ctx, TSTableID table_id, uint32_t* version) override {
    return KStatus::SUCCESS;
  }
  KStatus GetWalLevel(kwdbContext_p ctx, uint8_t* wal_level) override { return KStatus::SUCCESS; }
  static KStatus CloseTSEngine(kwdbContext_p ctx, TSEngine* engine) { return KStatus::SUCCESS; }
  KStatus GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value);
  void AlterTableCacheCapacity(int capacity)  override {}

  // init all engine.
  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable* meta, std::shared_ptr<TsTable>& ts_table);

  KStatus GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta);

  KStatus SwitchMemSegments(TS_LSN lsn) {
    return flush_mgr_.FlashMemSegment(lsn);
  }

  TS_LSN GetFinishedLSN() {
    return flush_mgr_.GetFinishedLSN();
  }

  KStatus DropResidualTsTable(kwdbContext_p ctx) override;

  static uint64_t GetAppliedIndex(const uint64_t range_id, const std::map<uint64_t, uint64_t>& range_indexes_map) {
    const auto iter = range_indexes_map.find(range_id);
    if (iter == range_indexes_map.end()) {
      return 0;
    }
    return iter->second;
  }

 private:
  TsVGroup* GetVGroupByID(kwdbContext_p ctx, uint32_t vgroup_id);

  KStatus putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id, TsRawPayload& payload);
};

}  //  namespace kwdbts
