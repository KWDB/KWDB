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

extern int storage_engine_vgroup_max_num = 10;
namespace kwdbts {

const char schema_directory[]= "schema";

TSEngineV2Impl::TSEngineV2Impl(const EngineOptions& engine_options) : options_(engine_options) {
  LogInit();
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(EngineOptions::table_cache_capacity_, true);
  char* vgroup_num = getenv("KW_VGROUP_NUM");
  if (vgroup_num != nullptr) {
    char *endptr;
    storage_engine_vgroup_max_num = strtol(vgroup_num, &endptr, 10);
    assert(*endptr == '\0');
  }
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
  std::shared_ptr<TsTable> ts_table = std::make_shared<TsTableV2Impl>(ctx, table_schema_mgr);
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

KStatus TSEngineV2Impl::PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id,
                                TSSlice* payload, bool write_wal) {
  TsRawPayload p{*payload};
  TSEntityID entity_id = 0;
  uint32_t tbl_grp_id = 0;
  bool new_tag = false;

  KStatus s = schema_mgr_->GetVGroup(ctx, table_id, p.GetPrimaryTag(), &tbl_grp_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  auto tbl_grp = GetVGroupByID(ctx, tbl_grp_id);
  assert(tbl_grp != nullptr);
  if (new_tag) {
    if (options_.wal_level != WALMode::OFF) {
      KStatus s = tbl_grp->WriteInsertWAL(ctx, mtr_id, *payload);
      if (s == KStatus::FAIL) {
        LOG_ERROR("failed WriteInsertWAL for new tag");
        return s;
      }
    }
    entity_id = tbl_grp->AllocateEntityID();
    s = putTagData(ctx, table_id, tbl_grp_id, entity_id, p);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  }

  if (options_.wal_level != WALMode::OFF) {
    KStatus s = tbl_grp->WriteInsertWAL(ctx, mtr_id, p.GetPrimaryTag(), *payload);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("putdata failed. because wal failed. table id[%lu], group id[%u]", table_id, tbl_grp_id);
      return s;
    }
  }
  uint8_t payload_data_flag = p.GetRowType();
  if (payload_data_flag != DataTagFlag::TAG_ONLY) {
    s = tbl_grp->PutData(ctx, table_id, entity_id, payload);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("putdata failed. table id[%lu], group id[%u]", table_id, tbl_grp_id);
      return s;
    }
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
