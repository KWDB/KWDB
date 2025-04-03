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

#include "ts_engine.h"

#include <dirent.h>
#include <filesystem>
#include <memory>
#include <utility>
#include "ts_env.h"
#include "ts_payload.h"
#include "ee_global.h"
#include "ee_executor.h"
#include "ts_table_v2_impl.h"
#include "ts_instance_params.h"

namespace kwdbts {
const int storage_engine_vgroup_max_num = 10;
const char schema_directory[]= "schema";

TSEngineV2Impl::TSEngineV2Impl(const EngineOptions& engine_options) : options_(engine_options), flush_mgr_(table_grps_) {
  LogInit();
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(EngineOptions::table_cache_capacity_, true);
}

TSEngineV2Impl::~TSEngineV2Impl() {
  DestoryExecutor();
  table_grps_.clear();
  SafeDeletePointer(tables_cache_);
}

KStatus TSEngineV2Impl::Init(kwdbContext_p ctx) {
  std::filesystem::path db_path{options_.db_path};
  assert(!db_path.empty());
  schema_mgr_ = std::make_unique<TsEngineSchemaManager>(db_path / schema_directory);
  KStatus s = schema_mgr_->Init(ctx);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  InitExecutor(ctx, options_);

  table_grps_.clear();
  for (size_t i = 0; i < storage_engine_vgroup_max_num; i++) {
    auto tbl_grp = std::make_unique<TsVGroup>(options_, i + 1, schema_mgr_.get());
    s = tbl_grp->Init(ctx);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    table_grps_.push_back(std::move(tbl_grp));
  }

  // Compressor
  auto compressor = TsEnvInstance::GetInstance().Compressor();
  compressor->Init();
  compressor->ResetPolicy(LIGHT_COMRESS);

  return KStatus::SUCCESS;
}

TsVGroup* TSEngineV2Impl::GetVGroupByID(kwdbContext_p ctx, uint32_t table_grp_id) {
  assert(storage_engine_vgroup_max_num >= table_grp_id);
  return table_grps_[table_grp_id - 1].get();
}

KStatus TSEngineV2Impl::CreateTsTable(kwdbContext_p ctx, TSTableID table_id, roachpb::CreateTsTable* meta) {
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;

  uint32_t vgroup_id = 1;
  if (meta->ts_table().has_database_id()) {
    vgroup_id = meta->ts_table().database_id();
  }
  schema_mgr_->SetTableID2DBID(ctx, table_id, meta->ts_table().database_id());

  s = schema_mgr_->CreateTable(ctx, table_id, meta);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("schema Create Table[%lu] failed.", table_id);
    return s;
  }
  LOG_INFO("Create TsTable %lu success.", table_id);

  std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
  s = schema_mgr_->GetTableSchemaMgr(table_id, table_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get table schema manager [%lu] failed.", table_id);
    return s;
  }
  std::shared_ptr<TsTable> ts_table = std::make_shared<TsTableV2Impl>(table_schema_mgr, table_grps_);
  tables_cache_->Put(table_id, ts_table);
  return s;
}

KStatus TSEngineV2Impl::putTagData(kwdbContext_p ctx, TSTableID table_id, uint32_t groupid, uint32_t entity_id,
  TsRawPayload& payload) {
  ErrorInfo err_info;
  // 1. Write tag data
  uint8_t payload_data_flag = payload.GetRowType();
  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY) {
    // tag
    LOG_DEBUG("tag bt insert hashPoint=%hu", payload.GetHashPoint());
    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    KStatus s = GetTableSchemaMgr(ctx, table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    err_info.errcode = tag_table->InsertTagRecord(payload, groupid, entity_id);
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                  TSSlice* payload_data, int payload_num, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                  uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool write_wal) {
  std::shared_ptr<kwdbts::TsTable> ts_table;
  ErrorInfo err_info;
  uint32_t tbl_grp_id;
  TSEntityID entity_id;
  for (size_t i = 0; i < payload_num; i++) {
    TsRawPayload p{payload_data[i]};
    TSSlice primary_key = p.GetPrimaryTag();
    auto tbl_version = p.GetTableVersion();
    auto s = GetTsTable(ctx, table_id, ts_table, err_info, tbl_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot found table[%lu] with version[%u], errmsg[%s]", table_id, tbl_version, err_info.errmsg.c_str());
      return s;
    }
    bool new_tag;
    s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &tbl_grp_id, &entity_id, &new_tag);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    auto tbl_grp = GetVGroupByID(ctx, tbl_grp_id);
    assert(tbl_grp != nullptr);
    if (new_tag) {
      if (write_wal) {
        // no need lock, lock inside.
        s = tbl_grp->GetWALManager()->WriteInsertWAL(ctx, mtr_id, 0, 0, payload_data[i]);
        if (s == KStatus::FAIL) {
          LOG_ERROR("failed WriteInsertWAL for new tag.");
          return s;
        }
      }
      entity_id = tbl_grp->AllocateEntityID();
      s = putTagData(ctx, table_id, tbl_grp_id, entity_id, p);
      if (s != KStatus::SUCCESS) {
        return s;
      }
      inc_entity_cnt++;
    }
  }
  
  dedup_result->payload_num = payload_num;
  dedup_result->dedup_rule = static_cast<int>(TsEngineInstanceParams::g_dedup_rule);
  auto s = ts_table->PutData(ctx, range_group_id, payload_data, payload_num,
                        mtr_id, inc_entity_cnt, inc_unordered_cnt, dedup_result, TsEngineInstanceParams::g_dedup_rule);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("put data failed. table[%lu].", table_id);
    return s;
  }
  return s;
}

KStatus TSEngineV2Impl::GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta) {
  return schema_mgr_->GetMeta(ctx, table_id, version, meta);
}

KStatus TSEngineV2Impl::LogInit() {
  LogConf cfg = {
    options_.lg_opts.path.c_str(),
    options_.lg_opts.file_max_size,
    options_.lg_opts.dir_max_size,
    options_.lg_opts.level
  };
  LOG_INIT(cfg);
  if (options_.lg_opts.trace_on_off != "") {
    TRACER.SetTraceConfigStr(options_.lg_opts.trace_on_off);
  }
  // comment's breif: if you want to check LOG/TRACER 's function can call next lines
  // LOG_ERROR("TEST FOR log");
  // TRACE_MM_LEVEL1("TEST FOR TRACE aaaaa\n");
  return KStatus::SUCCESS;
}

KStatus TSEngineV2Impl::AddColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, TSSlice column,
                                  uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  return schema_mgr_->AlterTable(ctx, table_id, AlterType::ADD_COLUMN, &column_meta,
                                 cur_version, new_version, err_msg);
}

KStatus TSEngineV2Impl::DropColumn(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id, TSSlice column,
                                   uint32_t cur_version, uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    return KStatus::FAIL;
  }
  return schema_mgr_->AlterTable(ctx, table_id, AlterType::DROP_COLUMN, &column_meta,
                                 cur_version, new_version, err_msg);
}

KStatus TSEngineV2Impl::AlterColumnType(kwdbContext_p ctx, const KTableKey &table_id, char *transaction_id,
                                        TSSlice new_column, TSSlice origin_column, uint32_t cur_version,
                                        uint32_t new_version, string &err_msg) {
  roachpb::KWDBKTSColumn new_col_meta;
  if (!new_col_meta.ParseFromArray(new_column.data, new_column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    return KStatus::FAIL;
  }
  return schema_mgr_->AlterTable(ctx, table_id, AlterType::ALTER_COLUMN_TYPE, &new_col_meta,
                                 cur_version, new_version, err_msg);
}

std::vector<std::shared_ptr<TsVGroup>>* TSEngineV2Impl::GetTsVGroups() {
  return &table_grps_;
}

}  // namespace kwdbts
