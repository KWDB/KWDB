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

#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include <list>
#include <set>
#include <unordered_map>
#include <filesystem>
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include "kwdb_type.h"
#include "ts_common.h"
#include "libkwdbts2.h"
#include "cm_kwdb_context.h"
#include "cm_func.h"
#include "lg_api.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

/**
 * table group used for organizing all schema info of table
 */
class TsEngineSchemaManager {
 public:
  TsEngineSchemaManager() = delete;

  explicit TsEngineSchemaManager(const std::string& schema_root_path);

  ~TsEngineSchemaManager();

  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTable(kwdbContext_p ctx, const uint64_t& db_id, const KTableKey& table_id, roachpb::CreateTsTable* meta);

  KStatus GetTableMetricSchema(kwdbContext_p ctx, TSTableID tbl_id, uint32_t version,
                               std::shared_ptr<MMapMetricsTable>* metric_schema);

  bool IsTableExist(TSTableID tbl_id);

  KStatus GetTableSchemaMgr(TSTableID tbl_id, std::shared_ptr<TsTableSchemaManager>& tb_schema_mgr);

  KStatus GetAllTableSchemaMgrs(std::vector<std::shared_ptr<TsTableSchemaManager>>& tb_schema_mgr);

  KStatus GetTableList(std::vector<TSTableID>* table_ids);

  KStatus GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta);

  // Get or allocate vgroup_id and entity_id
  KStatus GetVGroup(kwdbContext_p ctx, TSTableID tbl_id, TSSlice primary_key,
                        uint32_t* vgroup_id, TSEntityID* entity_id, bool* new_tag);

  uint32_t GetDBIDByTableID(TSTableID table_id);

  KStatus AlterTable(kwdbContext_p ctx, const KTableKey& table_id, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version, string& msg);

  int rdLock();

  int wrLock();

  int unLock();

 protected:
  std::filesystem::path schema_root_path_;
  uint32_t vgroup_id_;
  string tbl_sub_path_;
  std::unordered_map<TSTableID, std::shared_ptr<TsTableSchemaManager>> table_schema_mgrs_;
  KRWLatch mgrs_rw_latch_;
};

}  // namespace kwdbts
