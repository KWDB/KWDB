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
#include <utility>
#include <vector>
#include <memory>
#include "ts_table.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

class TsVGroup;

class TsTableV2Impl : public TsTable {
 private:
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  const std::vector<std::shared_ptr<TsVGroup>>& vgroups_;

 public:
  TsTableV2Impl(std::shared_ptr<TsTableSchemaManager> table_schema,
                const std::vector<std::shared_ptr<TsVGroup>>& vgroups) :
            TsTable(nullptr, "./wrong/", 0), table_schema_mgr_(table_schema), vgroups_(vgroups) {}

  ~TsTableV2Impl();


  KTableKey GetTableId() override {
    return table_schema_mgr_->GetTableId();
  }

  uint32_t GetCurrentTableVersion() override {
    return table_schema_mgr_->GetCurrentVersion();
  }

  std::shared_ptr<TsTableSchemaManager> GetSchemaManager() {
    return table_schema_mgr_;
  }

  KStatus PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                          DedupResult* dedup_result, const DedupRule& dedup_rule) override;

  KStatus PutData(kwdbContext_p ctx, TsVGroup* v_group, TsRawPayload& p,
                  TSEntityID entity_id, uint64_t mtr_id, uint32_t* inc_unordered_cnt,
                  DedupResult* dedup_result, const DedupRule& dedup_rule, bool write_wal);

  KStatus GetTagIterator(kwdbContext_p ctx,
                          std::vector<uint32_t> scan_tags,
                          const vector<uint32_t> hps,
                          BaseEntityIterator** iter, k_uint32 table_version) override;

  KStatus GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                          const std::vector<uint64_t/*index_id*/> &tags_index_id,
                          const std::vector<void*> tags,
                          TSTagOpType op_type,
                          const std::vector<uint32_t>& scan_tags,
                          std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                          uint32_t table_version = 1) override;

  KStatus GetNormalIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                            std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                            std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                            TsIterator** iter, std::vector<timestamp64> ts_points,
                            bool reverse, bool sorted) override;

  KStatus GetOffsetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                            vector<KwTsSpan>& ts_spans, std::vector<k_uint32> scan_cols, k_uint32 table_version,
                            TsIterator** iter, k_uint32 offset, k_uint32 limit, bool reverse) override;

  KStatus AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version, string& msg) override;

  KStatus CheckAndAddSchemaVersion(kwdbContext_p ctx, const KTableKey& table_id, uint64_t version) override;

  LifeTime GetLifeTime() {
    return table_schema_mgr_->GetLifeTime();
  }

  void SetLifeTime(LifeTime ts) {
    table_schema_mgr_->SetLifeTime(ts);
  }

  inline void updateTsSpan(int64_t ts, std::vector<KwTsSpan>& ts_spans) {
    // Delete all spans that ts > span.end
    auto new_end = std::remove_if(ts_spans.begin(), ts_spans.end(),
        [ts](const KwTsSpan& span) { return ts > span.end; });
    ts_spans.erase(new_end, ts_spans.end());

    // Update the begin for the remaining spans
    for (auto& span : ts_spans) {
      if (ts > span.begin) {
        span.begin = ts;
      }
    }
  }

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                               const uint32_t cur_version, const uint32_t new_version,
                               const std::vector<uint32_t/* tag column id*/>&) override;

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,
                             const uint32_t cur_version, const uint32_t new_version, const uint64_t index_id) override;

  vector<uint32_t> GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) override;
};

}  // namespace kwdbts
