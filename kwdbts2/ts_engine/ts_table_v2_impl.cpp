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

#include "ts_table_v2_impl.h"

namespace kwdbts {
TsTableV2Impl::TsTableV2Impl(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr) {
  table_schema_mgr_ = table_schema_mgr;
}

TsTableV2Impl::TsTableV2Impl(kwdbContext_p ctx, const std::string &db_path,
        const KTableKey &table_id)
: TsTable(ctx, db_path, table_id) {}

TsTableV2Impl::~TsTableV2Impl() = default;

KStatus TsTableV2Impl::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::vector<uint32_t> hps,
                                TagIterator** iter, k_uint32 table_version) {
  TSEngineV2Impl* ts_engine = static_cast<TSEngineV2Impl*>ctx->ts_engine;
  std::vector<std::unique_ptr<TsVGroup>>* tsVGroups = ts_engine->GetTsVGroup();

  std::vector<TsVGroupTagIterator*> vg_tag_iters;
  TsVGroupTagIterator* vg_tag_iter = nullptr;

  for (auto vGroupItem : tsVGroups) {
    if (!EngineOptions::isSingleNode()) {
        vg_tag_iter = new TsVGroupTagIterator(vGroupItem.second, tag_bt_,
            table_version, scan_tags, hps);
      } else {
        vg_tag_iter = new TsVGroupTagIterator(vGroupItem.second, tag_bt_,
            table_version, scan_tags, hps);
      }
    }
    if (!vg_tag_iter) {
      return KStatus::FAIL;
    }
    vg_tag_iters.emplace_back(std::move(vg_tag_iter));
  }
  std::vector<EntityGroupTagIterator*> eg_tag_iters;
  EntityGroupTagIterator* eg_tag_iter = nullptr;

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    if (!EngineOptions::isSingleNode()) {
      tbl_range.second->GetTagIterator(ctx, tbl_range.second, scan_tags, table_version, &eg_tag_iter, hps);
    } else {
      tbl_range.second->GetTagIterator(ctx, tbl_range.second, scan_tags, table_version, &eg_tag_iter);
    }

    if (!eg_tag_iter) {
      return KStatus::FAIL;
    }
    eg_tag_iters.emplace_back(std::move(eg_tag_iter));
    eg_tag_iter = nullptr;
  }

  TagIterator* tag_iter = new TagIterator(eg_tag_iters);
  if (KStatus::SUCCESS != tag_iter->Init()) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
