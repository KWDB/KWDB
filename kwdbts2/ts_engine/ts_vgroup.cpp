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

#include "ts_vgroup.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "sys_utils.h"
#include "ts_comparator.h"
#include "ts_db_impl.h"
#include "ts_format.h"
#include "ts_io.h"
#include "ts_last_segment_manager.h"
#include "ts_payload.h"
#include "ts_vgroup_partition.h"
#include "ts_iterator_v2_impl.h"

namespace kwdbts {

TsEnv TsVGroup::env_;

// todo(liangbo01) using normal path for mem_segment.
TsVGroup::TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr)
    : vgroup_id_(vgroup_id), schema_mgr_(schema_mgr), mem_segment_mgr_(this),
      path_(engine_options.db_path + "/" + GetFileName()),
      entity_counter_(0), engine_options_(engine_options) {}

TsVGroup::~TsVGroup() {
  if (db_ != nullptr) {
    rocksdb::FlushOptions flush_opts;
    db_->Flush(flush_opts);
    db_->Close();
    delete db_;
  }
  if (config_file_ != nullptr) {
    config_file_->sync(MS_SYNC);
    delete config_file_;
    config_file_ = nullptr;
  }
}

KStatus TsVGroup::Init(kwdbContext_p ctx) {
  rocksdb::Options options;
  options.create_if_missing = true;
  // options.write_buffer_size = 256;
  options.env = &env_;
  options.comparator = TsComparator();
  options.max_background_jobs = 4;
  options.max_write_buffer_number = 10;
  options.max_background_flushes = 4;
  MakeDirectory(path_);
  wal_manager_ = std::make_unique<WALMgr>(engine_options_.db_path, GetFileName(), &engine_options_);
  auto res = wal_manager_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  auto s = rocksdb::TsDBImpl::Open(options, path_.string() + "/mem", this, &db_);
  if (!s.ok()) {
    return KStatus::FAIL;
  }
  config_file_ = new MMapFile();
  string cf_file_path = path_.string() + "/vgroup_config";
  int flag = MMAP_CREAT_EXCL;
  bool exist = false;
  if (IsExists(cf_file_path)) {
    flag = MMAP_OPEN_NORECURSIVE;
    exist = true;
  }
  int error_code = config_file_->open(cf_file_path, flag);
  if (error_code < 0) {
    LOG_ERROR("Open config file failed, error code: %d, path: %s", error_code, cf_file_path.c_str());
    return KStatus::FAIL;
  }
  if (!exist) {
    config_file_->resize(4096);
    entity_counter_ = 0;
  } else {
    entity_counter_ = KUint32(config_file_->memAddr());
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroup::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  // no need do anything.
  return KStatus::SUCCESS;
}

KStatus TsVGroup::PutData(kwdbContext_p ctx, TSTableID table_id, TSEntityID entity_id, TSSlice* payload) {
  return mem_segment_mgr_.PutData(*payload, entity_id);
}

std::filesystem::path TsVGroup::GetPath() const {
  return path_;
}

std::string TsVGroup::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "group_%02u", vgroup_id_);
  return buffer;
}

uint32_t TsVGroup::AllocateEntityID() {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t new_id = entity_counter_ + 1;
  if (saveToFile(new_id) == 0) {
    entity_counter_ = new_id;
    return new_id;
  } else {
    throw std::runtime_error("Failed to persist the new ID to file");
  }
  return 0;
}

uint32_t TsVGroup::GetMaxEntityID() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entity_counter_;
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload) {
  // no need lock, lock inside.
  return wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, prepared_payload);
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload) {
  TS_LSN entry_lsn = 0;
  // lock current lsn: Lock the current LSN until the log is written to the cache
  wal_manager_->Lock();
  TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();
  KStatus s = wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, primary_tag, prepared_payload, entry_lsn);
  if (s == KStatus::FAIL) {
    wal_manager_->Unlock();
    return s;
  }
  // unlock current lsn
  wal_manager_->Unlock();

  if (entry_lsn != current_lsn) {
    LOG_ERROR("expected lsn is %lu, but got %lu ", current_lsn, entry_lsn);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

TsEngineSchemaManager* TsVGroup::GetSchemaMgr() const {
  return schema_mgr_;
}

TsVGroupPartition* TsVGroup::GetPartition(uint32_t database_id, timestamp64 p_time) {
  auto partition_manager = partitions_[database_id].get();
  if (partition_manager == nullptr) {
    // TODO(zzr): interval should be fetched form global setting;
    uint64_t interval = 3600 * 24 * 30;  // 30 days.
    partitions_[database_id] = std::make_unique<PartitionManager>(this, database_id, interval);
    partition_manager = partitions_[database_id].get();
  }
  return partition_manager->Get(p_time, true);
}

int TsVGroup::saveToFile(uint32_t new_id) const {
  uint32_t entity_id = new_id;
  memcpy(reinterpret_cast<char*>(config_file_->memAddr()), &entity_id, sizeof(entity_id));
  // return config_file_->sync(MS_SYNC);
  return 0;
}

KStatus TsVGroup::FlushImmSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  if (!mem_seg->SetFlushing()) {
    LOG_ERROR("cannot set status for mem segment.");
    return KStatus::FAIL;
  }
  std::unordered_map<TsVGroupPartition*, TsLastSegmentBuilder> builders;
  struct LastRowInfo {
    TSTableID cur_table_id = 0;
    uint32_t database_id = 0;
    uint32_t cur_table_version = 0;
    std::vector<AttributeInfo> info;
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  };
  LastRowInfo last_row_info;
  bool flush_success = true;

  mem_seg->Traversal([&](TSMemSegRowData* tbl) -> bool {
    // 1. get table schema manager.
    if (last_row_info.cur_table_id != tbl->table_id) {
      auto s =  schema_mgr_->GetTableSchemaMgr(tbl->table_id, last_row_info.schema_mgr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] schemainfo.", tbl->table_id);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_id = tbl->table_id;
      last_row_info.database_id = schema_mgr_->GetDBIDByTableID(tbl->table_id);
      last_row_info.cur_table_version = 0;
    }
    // 2. get table schema info of certain version.
    if (last_row_info.cur_table_version != tbl->table_version) {
      last_row_info.info.clear();
      auto s = last_row_info.schema_mgr->GetColumnsExcludeDropped(last_row_info.info, tbl->table_version);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] version[%u] schema info.", tbl->table_id, tbl->table_version);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_version = tbl->table_version;
    }
    // 3. get partition for metric data. 
    auto partition = GetPartition(last_row_info.database_id, tbl->ts, (DATATYPE)last_row_info.info[0].type);
    auto it = builders.find(partition);
    if (it == builders.end()) {
      std::shared_ptr<TsLastSegment> last_segment;
      partition->NewLastSegment(last_segment);
      auto result =  builders.insert({partition, TsLastSegmentBuilder{schema_mgr_, last_segment}});
      it = result.first;
      
    }
    // 4. insert data into segment builder.
    TsLastSegmentBuilder& builder = it->second;
    auto s = builder.PutRowData(tbl->table_id, tbl->table_version, tbl->entity_id, tbl->lsn, tbl->row_data);
    if (s != SUCCESS) {
      LOG_ERROR("PutRowData failed.");
      flush_success = false;
      return false;
    }
    return true;
  });
  // todo(liangbo01) deleting all new created files.
  if (!flush_success) {
    LOG_ERROR("faile flush memsegment to last segment.");
    return KStatus::FAIL;
  }

  for (auto& kv : builders) {
    auto s = kv.second.Finalize();
    if (s == FAIL){
      LOG_ERROR("last segment Finalize failed.");
      return KStatus::FAIL;
    }
    kv.second.Flush();
  }
  // todo(liangbo01) add all new files into new_file_list.
  std::list<TsLastSegment> new_file_list;
  //  todo(liangbo01) atomic: mem segment delete, and last segments load.
  mem_seg->SetDeleting();
  mem_segment_mgr_.RemoveMemSegment(mem_seg);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                                   std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                                   std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                                   std::vector<Sumfunctype> scan_agg_types,
                                   std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                   uint32_t table_version, TsStorageIterator** iter,
                                   std::shared_ptr<TsVGroup> vgroup,
                                   std::vector<timestamp64> ts_points, bool reverse, bool sorted) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support case).
  // TS_LSN read_lsn = GetOptimisticReadLsn();
  TsStorageIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    if (sorted) {
      ts_iter = new TsSortedRowDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
                                                  ts_scan_cols, table_schema_mgr, table_version, ASC);
    } else {
      ts_iter = new TsRawDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
                                            ts_scan_cols, table_schema_mgr, table_version);
    }
  } else {
    ts_iter = new TsAggIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols, ts_scan_cols,
                                      scan_agg_types, ts_points, table_schema_mgr, table_version);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

rocksdb::DB* TsVGroup::GetDB() {
  return db_;
}

uint32_t TsVGroup::GetVGroupID() {
  return vgroup_id_;
}

TsVGroup::TsPartitionedFlush::TsPartitionedFlush(TsVGroup* group, rocksdb::InternalIterator* iter)
    : vgroup_(group), iter_(iter) {}

rocksdb::Status TsVGroup::TsPartitionedFlush::FlushFromMem() {
  iter_->SeekToFirst();
  TsEngineSchemaManager* schema_mgr = vgroup_->schema_mgr_;

  rocksdb::FullKey full_key;
  TsInternalKey ts_key;
  std::unordered_map<TsVGroupPartition*, TsLastSegmentBuilder> builders;

  std::shared_ptr<MMapMetricsTable> table_schema;
  std::vector<AttributeInfo> metric_schema;

  std::unique_ptr<TsRawPayloadRowParser> parser;
  TSTableID last_table_id = -1;
  uint32_t last_version = -1;
  uint32_t db_id = -1;
  TsVGroupPartition* partition = nullptr;

  for (; iter_->Valid(); iter_->Next()) {
    rocksdb::ParseFullKey(iter_->key(), &full_key);
    rocksdb::SequenceNumber seq_no = full_key.sequence;
    ts_key.Decode(full_key.user_key);
    if (last_table_id != ts_key.table_id) {
      db_id = schema_mgr->GetDBIDByTableID(ts_key.table_id);
    }

    if (!(last_table_id == ts_key.table_id && last_version == ts_key.version)) {
      schema_mgr->GetTableMetricSchema(nullptr, ts_key.table_id, ts_key.version, &table_schema);
      assert(table_schema != nullptr);
      metric_schema = table_schema->getSchemaInfoExcludeDropped();
      parser = std::make_unique<TsRawPayloadRowParser>(metric_schema);
      last_table_id = ts_key.table_id;
      last_version = ts_key.version;
    }

    auto val = iter_->value();
    assert(!metric_schema.empty());
    // TsRawPayload payload_prev{{const_cast<char*>(val.data()), val.size()}, metric_schema};
    TsRawPayloadV2 payload{{const_cast<char*>(val.data()), val.size()}};
    auto row_iter = payload.GetRowIterator();

    int row_cnt = 0;
    for (; row_iter.Valid(); row_iter.Next()) {
      auto row_data = row_iter.Value();
      ++row_cnt;
      timestamp64 ts = parser->GetTimestamp(row_data);
      if (partition == nullptr || ts >= partition->EndTs() || ts < partition->StartTs()) {
        partition = this->vgroup_->GetPartition(db_id, ts, (DATATYPE)metric_schema[0].type);
      }

      auto it = builders.find(partition);
      if (it == builders.end()) {
        std::shared_ptr<TsLastSegment> last_segment;
        partition->NewLastSegment(last_segment);
        auto result =
            builders.insert({partition, TsLastSegmentBuilder{schema_mgr, last_segment}});
        it = result.first;
      }

      TsLastSegmentBuilder& builder = it->second;
      auto s =
          builder.PutRowData(ts_key.table_id, ts_key.version, ts_key.entity_id, seq_no, row_data);
      if (s != SUCCESS) {
        return rocksdb::Status::Incomplete("flush error");
      }
    }

    int nrows = payload.GetRowCount();
    assert(nrows == row_cnt);
    // assert(payload.GetRowCount() == payload_prev.GetRowCount());
  }
  for (auto& kv : builders) {
    auto s = kv.second.Finalize();
    if (s == FAIL) return rocksdb::Status::Incomplete("flush error");
    kv.second.Flush();
  }
  return rocksdb::Status::OK();
}

TsVGroupPartition* PartitionManager::Get(int64_t timestamp, bool create_if_not_exist) {
  int idx = timestamp / interval_;
  auto it = partitions_.find(idx);
  if (it == partitions_.end()) {
    if (!create_if_not_exist) {
      return nullptr;
    }

    int64_t start = idx * interval_;
    int64_t end = start + interval_;
    auto root = vgroup_->GetPath();
    auto partition = std::make_unique<TsVGroupPartition>(root, database_id_, vgroup_->GetSchemaMgr(), start, end);
    partition->Open();
    auto [it, success] = partitions_.emplace(idx, std::move(partition));
    assert(success);
    return it->second.get();
  }
  return it->second.get();
}

}  //  namespace kwdbts
