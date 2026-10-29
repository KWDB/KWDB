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

#include "ts_db_schema_manager.h"

#include <sys/types.h>

#include "settings.h"
#include "ts_engine_schema_manager.h"

namespace kwdbts {
// Scan every table's tag table and accumulate, per (db_id, vgroup_id), the
// max entity id ever allocated; used as the recovery floor for the counters.
KStatus TsDBSchemaManager::LoadCountersFromTagTable(TsEngineSchemaManager* schema_mgr) {
  kwdbContext_t ctx;
  std::vector<std::shared_ptr<TsTableSchemaManager>> tb_schema_managers;
  schema_mgr->GetAllTableSchemaMgrs(tb_schema_managers);
  auto max_vgroup_id = EngineOptions::vgroup_max_num;
  for (const auto& tb_schema_mgr : tb_schema_managers) {
    // Skip degenerate tables (e.g. metric schema missing after a partial
    // create): they never allocated an entity id, and GetDbID() would assert
    // on their empty metric manager.
    if (tb_schema_mgr->GetCurrentVersion() == 0) {
      continue;
    }
    auto db_id = tb_schema_mgr->GetDbID();
    std::shared_ptr<TagTable> tag_table;
    KStatus s = tb_schema_mgr->GetTagSchema(&ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("failed to get tag schema of table %lu while recovering entity id counters",
                tb_schema_mgr->GetTableId());
      return s;
    }
    auto& vg_max_eid_map = max_entity_id_map_[db_id];
    for (int vid = 1; vid <= max_vgroup_id; vid++) {
      // Accumulates: only raises the stored value, so iterating multiple
      // tables of the same db keeps the per-(db, vgroup) max.
      tag_table->GetMaxEntityIdByVGroupId(vid, vg_max_eid_map[vid]);
    }
  }
  return SUCCESS;
}
}  // namespace kwdbts
