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
#include <memory>
#include "ts_table.h"
#include "ts_vgroup.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

class TsTableV2Impl : public TsTable {
 private:
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  const std::vector<std::shared_ptr<TsVGroup>>& table_grps_;

 public:
  TsTableV2Impl(std::shared_ptr<TsTableSchemaManager> table_schema, const std::vector<std::shared_ptr<TsVGroup>>& table_grps) :
            TsTable(nullptr, "./wrong/", 0), table_schema_mgr_(table_schema), table_grps_(table_grps) {}

  ~TsTableV2Impl();


  KTableKey GetTableId() override {
    return table_schema_mgr_->GetTableId();
  }

  uint32_t GetCurrentTableVersion() override {
    return table_schema_mgr_->GetCurrentVersion();
  }

  KStatus PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                          DedupResult* dedup_result, const DedupRule& dedup_rule) override;

  KStatus GetTagIterator(kwdbContext_p ctx,
                          std::vector<uint32_t> scan_tags,
                          const vector<uint32_t> hps,
                          BaseEntityIterator** iter, k_uint32 table_version) override;
  KStatus GetNormalIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                            std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                            std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                            TsIterator** iter, std::vector<timestamp64> ts_points,
                            bool reverse, bool sorted) override;

};

}  // namespace kwdbts
