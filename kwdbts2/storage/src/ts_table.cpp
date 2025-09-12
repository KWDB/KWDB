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

#include <dirent.h>
#include <iostream>
#include "engine.h"
#include "mmap/mmap_metrics_table.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_table.h"
#include "perf_stat.h"
#include "sys_utils.h"
#include "lt_cond.h"
#include "ee_global.h"
#include "payload_builder.h"

extern DedupRule g_dedup_rule;
extern bool g_go_start_service;
namespace kwdbts {

KStatus TsTable::GetLastRowBatch(kwdbContext_p ctx, uint32_t table_version, std::vector<uint32_t> scan_cols,
                               ResultSet* res, k_uint32* count, bool& valid) {
  return KStatus::SUCCESS;
}
MMapRootTableManager* TsTable::CreateMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                          vector<AttributeInfo>& schema, uint32_t table_version,
                                                          uint64_t partition_interval, ErrorInfo& err_info,
                                                          uint64_t hash_num) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id, partition_interval,
                                                                  hash_num);
  KStatus s = tmp_bt_manager->CreateRootTable(schema, table_version, err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

MMapRootTableManager* TsTable::OpenMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                        ErrorInfo& err_info) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id);
  KStatus s = tmp_bt_manager->Init(err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

TsTable::TsTable() {
  is_dropped_.store(false);
}

TsTable::TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id)
    : db_path_(db_path), table_id_(table_id) {
  entity_bt_manager_ = nullptr;
  tbl_sub_path_ = std::to_string(table_id_) + "/";
  db_path_ = db_path_ + "/";
  is_dropped_.store(false);
  entity_groups_mtx_ = new TsTableEntityGrpsRwLatch(RWLATCH_ID_TS_TABLE_ENTITYGRPS_RWLOCK);
  snapshot_manage_mtx_ = new TsTableSnapshotLatch(LATCH_ID_TSTABLE_SNAPSHOT_MUTEX);
  table_version_rw_lock_ = new TsTableVersionRwLatch(RWLATCH_ID_TABLE_VERSION_RWLOCK);
}

TsTable::~TsTable() {
  if (is_dropped_) {
    kwdbContext_t context;
    kwdbContext_p ctx = &context;
    KStatus s = InitServerKWDBContext(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("InitServerKWDBContext Error!");
    }
    // s = DropAll(ctx);
    // if (s != KStatus::SUCCESS) {
    //   LOG_ERROR("DropAll Error!");
    // }
  }
  if (entity_bt_manager_) {
    delete entity_bt_manager_;
  }
  if (entity_groups_mtx_) {
    delete entity_groups_mtx_;
    entity_groups_mtx_ = nullptr;
  }
  if (snapshot_manage_mtx_) {
    delete snapshot_manage_mtx_;
    snapshot_manage_mtx_ = nullptr;
  }
  if (table_version_rw_lock_) {
    delete table_version_rw_lock_;
    table_version_rw_lock_ = nullptr;
  }
}

// Check that the directory name is a numeric
bool IsNumber(struct dirent* dir) {
  for (int i = 0; i < strlen(dir->d_name); ++i) {
    if (!isdigit(dir->d_name[i])) {
      // Iterate over each character and determine if it's a number
      return false;
    }
  }
  return true;
}

KStatus TsTable::Create(kwdbContext_p ctx, vector<AttributeInfo>& metric_schema,
                        uint32_t ts_version, uint64_t partition_interval, uint64_t hash_num) {
  if (entity_bt_manager_ != nullptr) {
    LOG_ERROR("Entity Bigtable already exist.");
    return KStatus::FAIL;
  }
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path)) {
      return KStatus::FAIL;
    }
  }

  hash_num_ = hash_num;
  ErrorInfo err_info;
  // Create entity table
  entity_bt_manager_ = CreateMMapRootTableManager(db_path_, tbl_sub_path_, table_id_, metric_schema, ts_version,
                                                  partition_interval, err_info, hash_num_);
  if (err_info.errcode < 0) {
    LOG_ERROR("createTable fail, table_id[%lu], msg[%s]", table_id_, err_info.errmsg.c_str());
  }

  if (entity_bt_manager_ == nullptr) {
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema,
                                             uint32_t table_version) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoIncludeDropped(data_schema, table_version);
}

KStatus TsTable::GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoExcludeDropped(data_schema);
}

KStatus TsTable::TierMigrate() {
  return KStatus::SUCCESS;
}

KStatus TsTable::ConvertRowTypePayload(kwdbContext_p ctx,  TSSlice payload_row, TSSlice* payload) {
  uint32_t pl_version = Payload::GetTsVsersionFromPayload(&payload_row);
  MMapMetricsTable* root_table = entity_bt_manager_->GetRootTable(pl_version);
  if (root_table == nullptr) {
    LOG_ERROR("table[%lu] cannot found version[%u].", table_id_, pl_version);
    return KStatus::FAIL;
  }
  const std::vector<AttributeInfo>& data_schema = root_table->getSchemaInfoExcludeDropped();
  PayloadStTypeConvert pl(payload_row, data_schema);
  auto s = pl.build(entity_bt_manager_, payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not parse current row-based payload.");
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter) {
  if (params.offset != 0) {
    return GetOffsetIterator(ctx, params, iter);
  } else {
    return GetNormalIterator(ctx, params, iter);
  }
}

KStatus TsTable::UndoAlterTable(kwdbContext_p ctx, LogEntry* log) {
  auto alter_log = reinterpret_cast<DDLAlterEntry*>(log);
  auto alter_type = alter_log->getAlterType();
  switch (alter_type) {
    case AlterType::ADD_COLUMN:
    case AlterType::DROP_COLUMN:
    case AlterType::ALTER_COLUMN_TYPE: {
      auto cur_version = alter_log->getCurVersion();
      auto new_version = alter_log->getNewVersion();
      auto slice = alter_log->getColumnMeta();
      roachpb::KWDBKTSColumn column;
      bool res = column.ParseFromArray(slice.data, slice.len);
      if (!res) {
        LOG_ERROR("Failed to parse the WAL log")
        return KStatus::FAIL;
      }
      if (undoAlterTable(ctx, alter_type, &column, cur_version, new_version) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
    case ALTER_PARTITION_INTERVAL: {
      uint64_t interval = 0;
      memcpy(&interval, alter_log->getData(), sizeof(interval));
      if (AlterPartitionInterval(ctx, interval) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::AlterPartitionInterval(kwdbContext_p ctx, uint64_t partition_interval) {
  entity_bt_manager_->SetPartitionInterval(partition_interval);
  return KStatus::SUCCESS;
}

uint64_t TsTable::GetPartitionInterval() {
  return entity_bt_manager_->GetPartitionInterval();
}

void TsTable::SetDropped() {
  is_dropped_.store(true);
  entity_bt_manager_->SetDropped();
}

bool TsTable::IsDropped() {
  return is_dropped_.load();
}

uint64_t beginOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;  // 24 hours * 60 minutes * 60 seconds * 1000 ms
  return timestamp - (timestamp % day_ms);
}

uint64_t endOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;
  return beginOfDay(timestamp) + day_ms;
}

uint64_t TsTable::GetHashNum() {
  return hash_num_;
}

}  //  namespace kwdbts
