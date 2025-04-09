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

namespace kwdbts {

/**
 * @brief TSEngineV2Impl
 */
class TSEngineV2Impl : public TSEngine {
 private:
  std::unique_ptr<TsEngineSchemaManager> schema_mgr_ = nullptr;
  std::vector<std::shared_ptr<TsVGroup>> table_grps_;
  int table_grp_max_num_{0};
  EngineOptions options_;
  std::unordered_map<TSTableID, std::shared_ptr<TsTableV2Impl>> tables_;
  std::mutex table_mutex_;
  TsLSNFlushManager flush_mgr_;

  // std::unique_ptr<TsMemSegmentManager> mem_seg_mgr_ = nullptr;

 public:
  explicit TSEngineV2Impl(const EngineOptions& engine_options);

  ~TSEngineV2Impl() override;

  KStatus CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                        std::vector<RangeGroup> ranges) override { return CreateTsTable(ctx, table_id, meta); }

  KStatus DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) override { return KStatus::SUCCESS; }

  KStatus CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) override {
    return KStatus::SUCCESS;
  }

  KStatus GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& ts_table,
                     ErrorInfo& err_info = getDummyErrorInfo(), uint32_t version = 0) override {
    // TODO(liangbo01)  need input change version
    KStatus s = KStatus::SUCCESS;
    ts_table = tables_cache_->Get(table_id);
    if (ts_table == nullptr) {
      std::shared_ptr<TsTableSchemaManager> schema;
      auto s = schema_mgr_->GetTableSchemaMgr(table_id, schema);
      if (s == KStatus::SUCCESS) {
        auto table = std::make_shared<TsTableV2Impl>(schema, table_grps_);
        if (table.get() != nullptr) {
          tables_[table_id] = table;
          ts_table = table;
          tables_cache_->Put(table_id, ts_table);
        } else {
          LOG_ERROR("make TsTableV2Impl failed for table[%lu]", table_id);
          s = KStatus::FAIL;
        }
      } else {
        LOG_ERROR("can not GetTableSchemaMgr table[%lu]", table_id);
        s = KStatus::FAIL;
      }
      table_mutex_.unlock();
    }

    if (s == KStatus::SUCCESS) {
      // todo(liangbo01) if version no exist.
    }
    return s;
  }

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
    return KStatus::SUCCESS;
  }

  KStatus PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                    TSSlice* payload_data, int payload_num, uint64_t mtr_id) override { return KStatus::SUCCESS; }

  KStatus PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                  TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                  uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL = true) override;

  KStatus DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                          HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                          uint64_t mtr_id) override { return KStatus::SUCCESS; }

  KStatus DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                     uint64_t mtr_id) override { return KStatus::SUCCESS; }

  KStatus DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                         std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) override {
    return KStatus::SUCCESS;
  }

  KStatus GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) override { return KStatus::SUCCESS; }

  KStatus ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) override { return KStatus::SUCCESS; }

  KStatus CreateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id,
                           roachpb::CreateTsTable* meta, const RangeGroup& range) override { return KStatus::SUCCESS; }

  KStatus GetRangeGroups(kwdbContext_p ctx, const KTableKey& table_id, RangeGroups *groups) override {
    return KStatus::SUCCESS;
  }

  KStatus UpdateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) override {
    return KStatus::SUCCESS;
  }

  KStatus DeleteRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) override {
    return KStatus::SUCCESS;
  }

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
                              const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) override {
    return KStatus::SUCCESS;
  }


  KStatus FlushBuffer(kwdbContext_p ctx) override { return KStatus::SUCCESS; }

  KStatus CreateCheckpoint(kwdbContext_p ctx) override { return KStatus::SUCCESS; }

  KStatus CreateCheckpointForTable(kwdbContext_p ctx, TSTableID table_id) override { return KStatus::SUCCESS; }

  KStatus Recover(kwdbContext_p ctx) override { return KStatus::SUCCESS; }

  KStatus TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                     uint64_t range_id, uint64_t index, uint64_t& mtr_id) override { return KStatus::SUCCESS; }

  KStatus TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                      uint64_t range_group_id, uint64_t mtr_id) override { return KStatus::SUCCESS; }

  KStatus TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                        uint64_t range_group_id, uint64_t mtr_id) override { return KStatus::SUCCESS; }

  KStatus TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override {
    return KStatus::SUCCESS;
  }

  KStatus TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override {
    return KStatus::SUCCESS;
  }

  KStatus TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) override {
    return KStatus::SUCCESS;
  }

  void GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) override { exit(0); }

  KStatus UpdateSetting(kwdbContext_p ctx) override { return KStatus::SUCCESS; }

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
  KStatus GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value) { return KStatus::SUCCESS; }
  void AlterTableCacheCapacity(int capacity)  override {}

  // init all engine.
  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable* meta);

  KStatus GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta);

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                               const char* transaction_id, const uint32_t cur_version, const uint32_t new_version,
                               const std::vector<uint32_t/* tag column id*/> &index_schema) override {return FAIL; }

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                             const char* transaction_id,  const uint32_t cur_version,
                             const uint32_t new_version) override {return FAIL; }

  KStatus AlterNormalTagIndex(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t index_id,
                              const char* transaction_id, const uint32_t old_version, const uint32_t new_version,
                              const std::vector<uint32_t/* tag column id*/> &new_index_schema) override {return FAIL; }

  KStatus SwitchMemSegments(TS_LSN lsn) {
    return flush_mgr_.FlashMemSegment(lsn);
  }

  TS_LSN GetFinishedLSN() {
    return flush_mgr_.GetFinishedLSN();
  }

 private:
  TsVGroup* GetVGroupByID(kwdbContext_p ctx, uint32_t table_grp_id);

  KStatus putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id, TsRawPayload& payload);
};

}  //  namespace kwdbts
