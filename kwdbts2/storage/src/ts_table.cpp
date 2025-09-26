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

TsTable::TsTable() {
  is_dropped_.store(false);
}

TsTable::TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id)
    : db_path_(db_path), table_id_(table_id) {
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
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path)) {
      return KStatus::FAIL;
    }
  }

  hash_num_ = hash_num;
  ErrorInfo err_info;

  if (err_info.errcode < 0) {
    LOG_ERROR("createTable fail, table_id[%lu], msg[%s]", table_id_, err_info.errmsg.c_str());
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema,
                                             uint32_t table_version) {
  return FAIL;
}

KStatus TsTable::GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema) {
  return FAIL;
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
  return FAIL;
}

uint64_t TsTable::GetPartitionInterval() {
  return 0;
}

void TsTable::SetDropped() {
  is_dropped_.store(true);
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
