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
#include <filesystem>
#include <memory>
#include <algorithm>
#include <numeric>
#include <unordered_set>
#include <utility>
#include <vector>
#include <regex>
#include <shared_mutex>
#include <list>
#include <string>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "sys_utils.h"
#include "ts_block.h"
#include "ts_common.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_builder.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_iterator_v2_impl.h"
#include "ts_lastsegment_builder.h"
#include "ts_mem_segment_mgr.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"

namespace kwdbts {

// todo(liangbo01) using normal path for mem_segment.
TsVGroup::TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
                   bool enable_compact_thread)
    : vgroup_id_(vgroup_id),
      schema_mgr_(schema_mgr),
      mem_segment_mgr_(this),
      path_(std::filesystem::path(engine_options.db_path) / VGroupDirName(vgroup_id)),
      max_entity_id_(0),
      engine_options_(engine_options),
      version_manager_(std::make_unique<TsVersionManager>(engine_options.io_env, path_)),
      enable_compact_thread_(enable_compact_thread) {}

TsVGroup::~TsVGroup() {
  enable_compact_thread_ = false;
  closeCompactThread();
}

KStatus TsVGroup::Init(kwdbContext_p ctx) {
  auto s = engine_options_.io_env->NewDirectory(path_);
  if (s == FAIL) {
    LOG_ERROR("Failed to create directory: %s", path_.c_str());
    return s;
  }

  s = version_manager_->Recover();
  if (s == FAIL) {
    LOG_ERROR("recover vgroup version failed, path: %s", path_.c_str());
    return s;
  }
  initCompactThread();

  wal_manager_ = std::make_unique<WALMgr>(engine_options_.db_path, VGroupDirName(vgroup_id_), &engine_options_);
  tsx_manager_ = std::make_unique<TSxMgr>(wal_manager_.get());
  auto res = wal_manager_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroup::SetReady() {
  TsVersionUpdate update;
  std::list<std::shared_ptr<TsMemSegment>> mems;
  mem_segment_mgr_.GetAllMemSegments(&mems);
  update.SetValidMemSegments(mems);
  return version_manager_->ApplyUpdate(&update);
}

KStatus TsVGroup::CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta) {
  // no need do anything.
  return KStatus::SUCCESS;
}

KStatus TsVGroup::PutData(kwdbContext_p ctx, TSTableID table_id, uint64_t mtr_id, TSSlice* primary_tag,
                          TSEntityID entity_id, TSSlice* payload, bool write_wal) {
  TS_LSN current_lsn = 1;
  if (engine_options_.wal_level != WALMode::OFF && write_wal) {
    TS_LSN entry_lsn = 0;
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    current_lsn = wal_manager_->FetchCurrentLSN();
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mtr_id, 0, 0, *primary_tag, *payload, entry_lsn, vgroup_id_);
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
  }
  // TODO(limeng04): import and export current lsn that temporarily use wal
  if (engine_options_.wal_level != WALMode::OFF && !write_wal) {
    current_lsn = wal_manager_->FetchCurrentLSN();
  }
  std::list<TSMemSegRowData> rows;
  auto s = mem_segment_mgr_.PutData(*payload, entity_id, current_lsn, &rows);
  if (s == KStatus::FAIL) {
    LOG_ERROR("mem_segment_mgr_.PutData Failed.")
    return FAIL;
  }
  // creating partition directory while inserting data into memroy.
  s = makeSurePartitionExist(table_id, rows);
  if (s == KStatus::FAIL) {
    LOG_ERROR("makeSurePartitionExist Failed.")
    return FAIL;
  }
  return KStatus::SUCCESS;
}

std::filesystem::path TsVGroup::GetPath() const { return path_; }

TSEntityID TsVGroup::AllocateEntityID() {
  std::lock_guard<std::mutex> lock(entity_id_mutex_);
  return ++max_entity_id_;
}

TSEntityID TsVGroup::GetMaxEntityID() const {
  std::lock_guard<std::mutex> lock(entity_id_mutex_);
  return max_entity_id_;
}

void TsVGroup::InitEntityID(TSEntityID entity_id) {
  std::lock_guard<std::mutex> lock(entity_id_mutex_);
  max_entity_id_ = entity_id;
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload) {
  // no need lock, lock inside.
  return wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, prepared_payload, vgroup_id_);
}

KStatus TsVGroup::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload) {
  TS_LSN entry_lsn = 0;
  // lock current lsn: Lock the current LSN until the log is written to the cache
  wal_manager_->Lock();
  TS_LSN current_lsn = wal_manager_->FetchCurrentLSN();
  KStatus s = wal_manager_->WriteInsertWAL(ctx, x_id, 0, 0, primary_tag, prepared_payload, entry_lsn, vgroup_id_);
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

KStatus TsVGroup::UpdateLSN(kwdbContext_p ctx, TS_LSN chk_lsn) {
  // 1.UpdateLSN
  wal_manager_->Lock();
  Defer defer{[&]() {
    wal_manager_->Unlock();
  }};
  KStatus s = wal_manager_->UpdateCheckpointWithoutFlush(ctx, chk_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to WriteCheckpointWAL.")
    return FAIL;
  }
  wal_manager_->Flush(ctx);
  // 2. remove chk file
  s = wal_manager_->RemoveChkFile(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to RemoveChkFile.")
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::ReadWALLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn) {
  // 1. read chk wal log
  // 2. switch new file
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;
  KStatus s = wal_manager_->ReadWALLogAndSwitchFile(logs, wal_manager_->FetchCheckpointLSN(),
                                                    wal_manager_->FetchCurrentLSN(), ignore);
  last_lsn = wal_manager_->FetchCurrentLSN();
  wal_manager_->Unlock();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLogAndSwitchFile.")
  }
  return s;
}

KStatus TsVGroup::ReadLogFromLastCheckpoint(kwdbContext_p ctx, std::vector<LogEntry*>& logs, TS_LSN& last_lsn) {
  // 1. read chk wal log
  wal_manager_->Lock();
  std::vector<uint64_t> ignore;

  // TODO(xy): code review here, last_lsn is not used, should we remove it?
  // TS_LSN chk_lsn = wal_manager_->FetchCheckpointLSN();
  // if (last_lsn != 0) {
  //   chk_lsn = last_lsn;
  // }

  KStatus s =
      wal_manager_->ReadWALLog(logs, wal_manager_->FetchCheckpointLSN(), wal_manager_->FetchCurrentLSN(), ignore);
  last_lsn = wal_manager_->FetchCurrentLSN();
  wal_manager_->Unlock();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to ReadWALLog.")
  }
  return s;
}

KStatus TsVGroup::ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs) {
  std::vector<uint64_t> ignore;
  return wal_manager_->ReadWALLogForMtr(mtr_trans_id, logs, ignore);
}

KStatus TsVGroup::CreateCheckpointInternal(kwdbContext_p ctx) { return KStatus::SUCCESS; }

TsEngineSchemaManager* TsVGroup::GetSchemaMgr() const {
  return schema_mgr_;
}

// todo(liangbo01) create partition should at data inserting wal.
// if at here, inserting and deleting at same time. maybe sql exec over, data all exist.
// but restart service, data all disappeared.
// solution: 1. delete item not store in partition. 2. creating partition before wal insert.
KStatus TsVGroup::makeSurePartitionExist(TSTableID table_id, const std::list<TSMemSegRowData>& rows) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr failed. table[%lu]", table_id);
    return s;
  }
  auto ts_type = tb_schema_mgr->GetTsColDataType();
  for (auto& row : rows) {
    auto p_time = convertTsToPTime(row.ts, ts_type);
    version_manager_->AddPartition(row.database_id, p_time);
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoPut(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p{payload};
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  auto s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint8_t payload_data_flag = p.GetRowType();
  if (new_tag) {
    vgroup_id = GetVGroupID();
    entity_id = AllocateEntityID();
    // 1. Write tag data
    assert(payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY);
    LOG_DEBUG("tag bt insert hashPoint=%hu", p.GetHashPoint());
    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  } else {
    assert(vgroup_id == this->GetVGroupID());
  }

  if (payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::DATA_ONLY) {
    std::list<TSMemSegRowData> rows;
    s = mem_segment_mgr_.PutData(payload, entity_id, log_lsn, &rows);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("failed putdata.");
      return s;
    }
    s = makeSurePartitionExist(table_id, rows);
    if (s == KStatus::FAIL) {
      LOG_ERROR("makeSurePartitionExist Failed.")
      return FAIL;
    }
  }
  return s;
}

void TsVGroup::compactRoutine(void* args) {
  while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && enable_compact_thread_) {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    // Check every 2 seconds if compact is necessary
    cv_.wait_for(lock, std::chrono::seconds(2), [this] { return !enable_compact_thread_; });
    lock.unlock();
    // If the thread pool stops or the system is no longer running, exit the loop
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !enable_compact_thread_) {
      break;
    }
    // Execute compact tasks
    Compact();
  }
}

void TsVGroup::initCompactThread() {
  if (!enable_compact_thread_) {
    return;
  }
  KWDBOperatorInfo kwdb_operator_info;
  // Set the name and owner of the operation
  kwdb_operator_info.SetOperatorName("VGroup::CompactThread");
  kwdb_operator_info.SetOperatorOwner("VGroup");
  time_t now;
  // Record the start time of the operation
  kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
  // Start asynchronous thread
  compact_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&TsVGroup::compactRoutine, this, std::placeholders::_1), this, &kwdb_operator_info);
  if (compact_thread_id_ < 1) {
    // If thread creation fails, record error message
    LOG_ERROR("VGroup compact thread create failed");
  }
}

void TsVGroup::closeCompactThread() {
  if (compact_thread_id_ > 0) {
    // Wake up potentially dormant compact threads
    cv_.notify_all();
    // Waiting for the compact thread to complete
    KWDBDynamicThreadPool::GetThreadPool().JoinThread(compact_thread_id_, 0);
  }
}

KStatus TsVGroup::Compact() {
  while (!TrySetTsExclusiveStatus(TsExclusiveStatus::COMPACT)) {
    sleep(1);
  }
  Defer defer([this]() {
    ResetTsExclusiveStatus();
  });
  auto current = version_manager_->Current();
  auto partitions = current->GetPartitionsToCompact();

  // Prioritize the latest partition
  using PartitionPtr = std::shared_ptr<const TsPartitionVersion>;
  std::sort(partitions.begin(), partitions.end(), [](const PartitionPtr& left, const PartitionPtr& right) {
    return left->GetStartTime() > right->GetStartTime();
  });

  // Compact partitions
  TsVersionUpdate update;
  std::atomic_bool success{true};
  for (size_t idx = 0; idx < partitions.size(); ++idx) {
    const auto& cur_partition = partitions[idx];

    // 1. Get all the last segments that need to be compacted.
    auto last_segments = cur_partition->GetCompactLastSegments();
    auto entity_segment = cur_partition->GetEntitySegment();

    auto root_path = this->GetPath() / PartitionDirName(cur_partition->GetPartitionIdentifier());
    uint64_t new_entity_header_num = version_manager_->NewFileNumber();

    // 2. Build the column block.
    {
      TsEntitySegmentBuilder builder(root_path.string(), schema_mgr_, version_manager_.get(),
                                     cur_partition->GetPartitionIdentifier(), entity_segment, new_entity_header_num,
                                     last_segments);
      KStatus s = builder.Open();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder open failed", path_.c_str());
        success = false;
        break;
      }
      s = builder.Compact(&update);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder build failed", path_.c_str());
        success = false;
        break;
      }
    }

    // 3. Set the compacted version.
    for (auto& last_segment : last_segments) {
      update.DeleteLastSegment(cur_partition->GetPartitionIdentifier(), last_segment->GetFileNumber());
    }
  }

  if (!success) {
    LOG_ERROR("compact failed.");
    return FAIL;
  }
  // 4. Update the version.
  return version_manager_->ApplyUpdate(&update);
}

KStatus TsVGroup::FlushImmSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  if (!mem_seg->SetFlushing()) {
    LOG_ERROR("cannot set status for mem segment.");
    return KStatus::FAIL;
  }
  struct LastRowInfo {
    TSTableID cur_table_id = 0;
    uint32_t database_id = 0;
    uint32_t cur_table_version = 0;
    std::vector<AttributeInfo> info;
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  };
  LastRowInfo last_row_info;

  TsIOEnv* env = &TsMMapIOEnv::GetInstance();

  std::unordered_map<std::shared_ptr<const TsPartitionVersion>, TsLastSegmentBuilder> builders;
  std::unordered_map<std::shared_ptr<const TsPartitionVersion>, TsLastSegmentBuilder2> builders2;
  std::unordered_set<std::shared_ptr<const TsPartitionVersion>> new_created_partitions;
  TsVersionUpdate update;

  std::vector<std::shared_ptr<TsBlockSpan>> sorted_spans;

  {
    std::list<std::shared_ptr<TsBlockSpan>> all_block_spans;
    auto s = mem_seg->GetBlockSpans(all_block_spans, schema_mgr_);
    if (s == FAIL) {
      LOG_ERROR("cannot get block spans.");
      return FAIL;
    }
    sorted_spans.reserve(all_block_spans.size());
    std::move(all_block_spans.begin(), all_block_spans.end(), std::back_inserter(sorted_spans));
    std::sort(sorted_spans.begin(), sorted_spans.end(),
              [](const std::shared_ptr<TsBlockSpan>& left, const std::shared_ptr<TsBlockSpan>& right) {
                using Helper = std::tuple<TSTableID, TSEntityID, timestamp64, TS_LSN>;
                auto left_helper =
                    Helper(left->GetTableID(), left->GetEntityID(), left->GetFirstTS(), *left->GetLSNAddr(0));
                auto right_helper =
                    Helper(right->GetTableID(), right->GetEntityID(), right->GetFirstTS(), *right->GetLSNAddr(0));
                return left_helper < right_helper;
              });
  }

  auto current = version_manager_->Current();

  for (auto span : sorted_spans) {
    auto table_id = span->GetTableID();
    auto table_version = span->GetTableVersion();
    std::shared_ptr<TsTableSchemaManager> table_schema_manager;
    auto s = schema_mgr_->GetTableSchemaMgr(table_id, table_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get table[%lu] schemainfo.", table_id);
      return KStatus::FAIL;
    }

    std::vector<AttributeInfo> info;
    s = table_schema_manager->GetColumnsExcludeDropped(info, span->GetTableVersion());
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get table[%lu] version[%u] schema info.", table_id, table_version);
      return KStatus::FAIL;
    }

    std::shared_ptr<TsBlockSpan> current_span = span;
    while (current_span != nullptr) {
      timestamp64 first_ts = convertTsToPTime(current_span->GetFirstTS(), static_cast<DATATYPE>(info[0].type));
      timestamp64 last_ts = convertTsToPTime(current_span->GetLastTS(), static_cast<DATATYPE>(info[0].type));
      auto partition = current->GetPartition(table_schema_manager->GetDbID(), first_ts);
      if (partition == nullptr) {
        LOG_WARN("cannot find partition: retry later.")
        return KStatus::FAIL;
      }

      if (!partition->HasDirectoryCreated() && new_created_partitions.find(partition) == new_created_partitions.end()) {
        // create directory for partition.
        auto path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
        auto s = env->NewDirectory(path);
        if (s != SUCCESS) {
          LOG_ERROR("cannot create directory for partition.");
          return FAIL;
        }
        update.PartitionDirCreated(partition->GetPartitionIdentifier());
        new_created_partitions.insert(partition);
      }

      if (partition->IsMemoryOnly() && new_created_partitions.find(partition) == new_created_partitions.end()) {
        update.PartitionDirCreated(partition->GetPartitionIdentifier());
        new_created_partitions.insert(partition);
      }

      std::shared_ptr<TsBlockSpan> span_to_flush;
      if (last_ts < partition->GetEndTime()) {
        // all the data in this span is whithin the current partition, no need to split.
        span_to_flush = std::move(current_span);
        current_span = nullptr;
      } else {
        // split the span into two parts.
        std::vector<int> iota_vec(current_span->GetRowNum());
        std::iota(iota_vec.begin(), iota_vec.end(), 0);
        auto split_it =
            std::upper_bound(iota_vec.begin(), iota_vec.end(), partition->GetEndTime(), [&](timestamp64 val, int idx) {
              return val < convertTsToPTime(current_span->GetTS(idx), static_cast<DATATYPE>(info[0].type));
            });

        int split_idx = *split_it;
        std::shared_ptr<TsBlockSpan> new_span;
        current_span->SplitBack(split_idx, new_span);
        span_to_flush = std::move(current_span);
        current_span = std::move(new_span);
      }

      auto it = builders2.find(partition);
      if (it == builders2.end()) {
        std::unique_ptr<TsAppendOnlyFile> last_segment;
        uint64_t file_number = version_manager_->NewFileNumber();
        auto path =
            this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier()) / LastSegmentFileName(file_number);
        auto s = env->NewAppendOnlyFile(path, &last_segment);
        if (s == FAIL) {
          LOG_ERROR("cannot create last segment file.");
          return FAIL;
        }

        auto result = builders2.insert({partition, TsLastSegmentBuilder2{schema_mgr_, std::move(last_segment),
                                                                         static_cast<uint32_t>(file_number)}});
        it = result.first;
      }
      TsLastSegmentBuilder2& builder = it->second;
      s = builder.PutBlockSpan(span_to_flush);
      if (s == FAIL) {
        LOG_ERROR("PutBlockSpan failed.");
        return FAIL;
      }
    }
  }

  for (auto& [k, v] : builders2) {
    auto s = v.Finalize();
    if (s == FAIL) {
      return FAIL;
    }
    update.AddLastSegment(k->GetPartitionIdentifier(), v.GetFileNumber());
  }

  mem_seg->SetDeleting();
  mem_segment_mgr_.RemoveMemSegment(mem_seg);
  std::list<std::shared_ptr<TsMemSegment>> mems;
  mem_segment_mgr_.GetAllMemSegments(&mems);
  update.SetValidMemSegments(mems);

  version_manager_->ApplyUpdate(&update);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                                   std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                                   std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                                   std::vector<k_int32> agg_extend_cols,
                                   std::vector<Sumfunctype> scan_agg_types,
                                   std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                   uint32_t table_version, TsStorageIterator** iter,
                                   std::shared_ptr<TsVGroup> vgroup,
                                   std::vector<timestamp64> ts_points, bool reverse, bool sorted) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support
  // case). TS_LSN read_lsn = GetOptimisticReadLsn();
  TsStorageIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    ts_iter = new TsSortedRawDataIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols,
                                                  ts_scan_cols, table_schema_mgr, table_version, ASC);
  } else {
    // need call Next function times: entity_ids.size(), no matter Next return what.
    ts_iter = new TsAggIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type, scan_cols, ts_scan_cols,
                                      agg_extend_cols, scan_agg_types, ts_points, table_schema_mgr, table_version);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetBlockSpans(TSTableID table_id, uint32_t entity_id, KwTsSpan ts_span, DATATYPE ts_col_type,
                                std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version,
                                std::list<std::shared_ptr<TsBlockSpan>>* block_spans) {
  uint32_t db_id = schema_mgr_->GetDBIDByTableID(table_id);
  auto current = version_manager_->Current();
  std::vector<KwTsSpan> ts_spans{ts_span};
  auto ts_partitions = current->GetPartitions(db_id, ts_spans, ts_col_type);
  for (int32_t index = 0; index < ts_partitions.size(); ++index) {
    TsScanFilterParams filter{db_id, table_id, vgroup_id_, entity_id,
                              ts_col_type, wal_manager_->FetchCurrentLSN(), ts_spans};
    auto partition_version = ts_partitions[index];
    std::list<std::shared_ptr<TsBlockSpan>> cur_block_span;
    auto s = partition_version->GetBlockSpan(filter, &cur_block_span, table_schema_mgr, table_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition_version GetBlockSpan failed.");
      return s;
    }
    block_spans->splice(block_spans->begin(), cur_block_span);
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::rollback(kwdbContext_p ctx, LogEntry* wal_log, bool from_chk) {
  KStatus s;
  uint64_t lsn = wal_log->getLSN();
  if (from_chk) {
    lsn = wal_log->getOldLSN();
  }

  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        return undoPut(ctx, lsn, log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return undoPutTag(ctx, lsn, log->getPayload());
      }
    }
    case WALLogType::UPDATE: {
      auto update_log = reinterpret_cast<UpdateLogEntry*>(wal_log);
      if (update_log->getTableType() == WALTableType::TAG) {
        auto log = reinterpret_cast<UpdateLogTagsEntry*>(wal_log);
        return undoUpdateTag(ctx, lsn, log->getPayload(), log->getOldPayload());
      }
      break;
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;
      assert(t_type != WALTableType::DATA);
      if (t_type == WALTableType::DATA_V2) {
        auto log = reinterpret_cast<DeleteLogMetricsEntryV2*>(del_log);
        p_tag = log->getPrimaryTag();
        TSTableID table_id = log->getTableId();
        vector<KwTsSpan> ts_spans = log->getTsSpans();
        return undoDeleteData(ctx, table_id, p_tag, lsn, ts_spans);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        TSSlice primary_tag = log->getPrimaryTag();
        TSSlice tags = log->getTags();
        return undoDeleteTag(ctx, primary_tag, lsn, log->group_id_, log->entity_id_, tags);
      }
    }

    case WALLogType::CHECKPOINT:
      break;
    case WALLogType::MTR_BEGIN:
      break;
    case WALLogType::MTR_COMMIT:
      break;
    case WALLogType::MTR_ROLLBACK:
      break;
    case WALLogType::TS_BEGIN:
      break;
    case WALLogType::TS_COMMIT:
      break;
    case WALLogType::TS_ROLLBACK:
      break;
    case WALLogType::DDL_CREATE:
      break;
    case WALLogType::DDL_DROP:
      break;
    case WALLogType::DDL_ALTER_COLUMN:
      break;
    case WALLogType::DB_SETTING:
      break;
    case WALLogType::RANGE_SNAPSHOT: {
      //      auto snapshot_log = reinterpret_cast<SnapshotEntry*>(wal_log);
      //      if (snapshot_log == nullptr) {
      //        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
      //        return KStatus::FAIL;
      //      }
      //      HashIdSpan hash_span;
      //      KwTsSpan ts_span;
      //      snapshot_log->GetRangeInfo(&hash_span, &ts_span);
      //      uint64_t count = 0;
      //      auto s = DeleteRangeData(ctx, hash_span, 0, {ts_span}, nullptr, &count,
      //      snapshot_log->getXID(), false); if (s != KStatus::SUCCESS) {
      //        LOG_ERROR(" WAL rollback snapshot delete range data faild.");
      //        return KStatus::FAIL;
      //      }
      //      break;
    }
    case WALLogType::SNAPSHOT_TMP_DIRCTORY: {
      auto temp_path_log = reinterpret_cast<TempDirectoryEntry*>(wal_log);
      if (temp_path_log == nullptr) {
        LOG_ERROR(" WAL rollback cannot prase temp dirctory object.");
        return KStatus::FAIL;
      }
      std::string path = temp_path_log->GetPath();
      if (!Remove(path)) {
        LOG_ERROR(" WAL rollback cannot remove path[%s]", path.c_str());
        return KStatus::FAIL;
      }
      break;
    }
    case WALLogType::PARTITION_TIER_CHANGE: {
      //      auto tier_log = reinterpret_cast<PartitionTierChangeEntry*>(wal_log);
      //      if (tier_log == nullptr) {
      //        LOG_ERROR(" WAL rollback cannot prase partition tier log.");
      //        return KStatus::FAIL;
      //      }
      //      ErrorInfo err_info;
      //      auto s = TsTierPartitionManager::GetInstance().Recover(tier_log->GetLinkPath(),
      //      tier_log->GetTierPath(), err_info); if (s != KStatus::SUCCESS) {
      //        LOG_ERROR(" WAL rollback partition tier change faild. %s", err_info.errmsg.c_str());
      //        return KStatus::FAIL;
      //      }
      //      break;
    }

    // TODO(xy): code review here, the following cases are not handled.
    case WALLogType::CREATE_INDEX:
    case WALLogType::DROP_INDEX:
    case WALLogType::END_CHECKPOINT: {
      assert(false);
      break;
    }
  }

  return SUCCESS;
}

KStatus TsVGroup::ApplyWal(kwdbContext_p ctx, LogEntry* wal_log,
                           std::unordered_map<TS_LSN, MTRBeginEntry*>& incomplete) {
  switch (wal_log->getType()) {
    case WALLogType::INSERT: {
      auto insert_log = reinterpret_cast<InsertLogEntry*>(wal_log);
      std::string p_tag;
      // tag or metrics
      if (insert_log->getTableType() == WALTableType::DATA) {
        auto log = reinterpret_cast<InsertLogMetricsEntry*>(wal_log);
        p_tag = log->getPrimaryTag();
        return redoPut(ctx, log->getLSN(), log->getPayload());
      } else {
        auto log = reinterpret_cast<InsertLogTagsEntry*>(wal_log);
        return redoPutTag(ctx, log->getLSN(), log->getPayload());
      }
    }
    case WALLogType::DELETE: {
      auto del_log = reinterpret_cast<DeleteLogEntry*>(wal_log);
      WALTableType t_type = del_log->getTableType();
      std::string p_tag;
      assert(t_type != WALTableType::DATA);
      if (t_type == WALTableType::DATA_V2) {
        auto log = reinterpret_cast<DeleteLogMetricsEntryV2*>(del_log);
        p_tag = log->getPrimaryTag();
        TSTableID table_id = log->getTableId();
        vector<KwTsSpan> ts_spans = log->getTsSpans();
        return redoDeleteData(ctx, table_id, p_tag, log->getLSN(), ts_spans);
      } else {
        auto log = reinterpret_cast<DeleteLogTagsEntry*>(del_log);
        auto p_tag_slice = log->getPrimaryTag();
        auto tag_slice = log->getTags();
        return redoDeleteTag(ctx, p_tag_slice, log->getLSN(), log->group_id_, log->entity_id_, tag_slice);
      }
    }
    case WALLogType::UPDATE: {
      auto update_log = reinterpret_cast<UpdateLogEntry*>(wal_log);
      WALTableType t_type = update_log->getTableType();
      std::string p_tag;
      if (t_type == WALTableType::TAG) {
        auto log = reinterpret_cast<UpdateLogTagsEntry*>(update_log);
        auto slice = log->getPayload();
        return redoUpdateTag(ctx, log->getLSN(), slice);
      }
      break;
    }
      // There is nothing to do while applying CHECKPOINT WAL.
    case WALLogType::CHECKPOINT: {
      KStatus s = wal_manager_->CreateCheckpointWithoutFlush(ctx);
      if (s == FAIL) return s;
      break;
    }
    case WALLogType::MTR_BEGIN: {
      auto log = reinterpret_cast<MTRBeginEntry*>(wal_log);
      incomplete.insert(std::pair<TS_LSN, MTRBeginEntry*>(log->getXID(), log));
      break;
    }
    case WALLogType::MTR_COMMIT: {
      auto log = reinterpret_cast<MTREntry*>(wal_log);
      incomplete.erase(log->getXID());
      break;
    }
    default:
      break;
  }
  return KStatus::SUCCESS;
}

uint32_t TsVGroup::GetVGroupID() { return vgroup_id_; }

KStatus TsVGroup::DeleteEntity(kwdbContext_p ctx, TSTableID table_id, std::string& p_tag, TSEntityID e_id,
                               uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  auto tag_table = tb_schema_manager->GetTagTable();
  TagTuplePack* tag_pack = tag_table->GenTagPack(p_tag.data(), p_tag.size());
  if (UNLIKELY(nullptr == tag_pack)) {
    return KStatus::FAIL;
  }
  s = wal_manager_->WriteDeleteTagWAL(ctx, mtr_id, p_tag, vgroup_id_, e_id, tag_pack->getData(), vgroup_id_);
  delete tag_pack;
  if (s == KStatus::FAIL) {
    LOG_ERROR("WriteDeleteTagWAL failed.");
    return s;
  }
  // if any error, end the delete loop and return ERROR to the caller.
  // Delete tag and its index
  ErrorInfo err_info;
  tag_table->DeleteTagRecord(p_tag.data(), p_tag.size(), err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
    return KStatus::FAIL;
  }
  if (*count != 0) {
    // todo(liangbo01) we should delete current entity metric datas.
    TS_LSN cur_lsn = wal_manager_->FetchCurrentLSN();
    std::vector<KwTsSpan> ts_spans;
    ts_spans.push_back({INT64_MIN, INT64_MAX});
    // delete current entity metric datas.
    s = DeleteData(ctx, table_id, e_id, cur_lsn, ts_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::DeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& p_tag, TSEntityID e_id,
                             const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  std::vector<DelRowSpan> dtp_list;
  TS_LSN current_lsn;
  KStatus s = wal_manager_->WriteDeleteMetricsWAL4V2(ctx, mtr_id, tbl_id, p_tag, ts_spans, vgroup_id_, &current_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("WriteDeleteTagWAL failed.");
    return s;
  }
  // delete current entity metric datas.
  return DeleteData(ctx, tbl_id, e_id, current_lsn, ts_spans);
}

KStatus TsVGroup::deleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, KwLSNSpan lsn,
const std::vector<KwTsSpan>& ts_spans) {
  auto s = TrasvalAllPartition(ctx, tbl_id, ts_spans,
  [&](std::shared_ptr<const TsPartitionVersion> p) -> KStatus {
    auto ret = p->DeleteData(e_id, ts_spans, lsn);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData partition[%u/%lu] failed!",
              std::get<0>(p->GetPartitionIdentifier()), std::get<1>(p->GetPartitionIdentifier()));
      return ret;
    }
    return ret;
  });
  return KStatus::SUCCESS;
}

KStatus TsVGroup::DeleteData(kwdbContext_p ctx, TSTableID tbl_id, TSEntityID e_id, TS_LSN lsn,
const std::vector<KwTsSpan>& ts_spans) {
  if (lsn == UINT64_MAX) {  // make sure lsn is not larger than current lsn.
    wal_manager_->Lock();
    lsn = wal_manager_->FetchCurrentLSN() - 1;  // not same with any allocated lsn.
    wal_manager_->Unlock();
  }
  return deleteData(ctx, tbl_id, e_id, {0, lsn}, ts_spans);
}

KStatus TsVGroup::undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, TSSlice old_payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::WriteBatchData(kwdbContext_p ctx, TSTableID tbl_id, uint32_t table_version, TSEntityID entity_id,
                                 timestamp64 ts, DATATYPE ts_col_type, TSSlice data) {
  while (!TrySetTsExclusiveStatus(TsExclusiveStatus::WRITE_BATCH)) {
    sleep(1);
  }
  auto current = version_manager_->Current();
  uint32_t database_id = schema_mgr_->GetDBIDByTableID(tbl_id);
  if (database_id == 0) {
    return KStatus::FAIL;
  }
  auto p_time = convertTsToPTime(ts, ts_col_type);
  auto partition = current->GetPartition(database_id, p_time);
  if (partition == nullptr) {
    version_manager_->AddPartition(database_id, p_time);
    current = version_manager_->Current();
    partition = current->GetPartition(database_id, p_time);
    if (partition == nullptr) {
      LOG_ERROR("cannot find partition: database_id[%u], p_time[%lu]", database_id, p_time);
      return KStatus::FAIL;
    }
  }
  PartitionIdentifier partition_id = partition->GetPartitionIdentifier();
  std::shared_ptr<TsEntitySegmentBuilder> builder = nullptr;
  {
    std::unique_lock lock{builders_mutex_};
    auto it = write_batch_segment_builders_.find(partition_id);
    if (it == write_batch_segment_builders_.end()) {
      auto entity_segment = partition->GetEntitySegment();

      auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
      uint64_t new_entity_header_num = version_manager_->NewFileNumber();

      builder = std::make_shared<TsEntitySegmentBuilder>(root_path.string(), partition_id,
                                                         entity_segment, new_entity_header_num);
      KStatus s = builder->Open();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Open entity segment builder failed.");
        return s;
      }
      write_batch_segment_builders_[partition_id] = builder;
    } else {
      builder = it->second;
    }
  }
  return builder->WriteBatch(entity_id, table_version, data);
}

KStatus TsVGroup::FinishWriteBatchData() {
  TsVersionUpdate update;
  std::unique_lock lock{builders_mutex_};
  for (auto& kv : write_batch_segment_builders_) {
    KStatus s = kv.second->WriteBatchFinish(&update);
    if (s != KStatus::SUCCESS) {
      write_batch_segment_builders_.clear();
      ResetTsExclusiveStatus();
      LOG_ERROR("Finish entity segment builder failed");
      return s;
    }
  }
  write_batch_segment_builders_.clear();
  version_manager_->ApplyUpdate(&update);
  ResetTsExclusiveStatus();
  return KStatus::SUCCESS;
}

KStatus TsVGroup::ClearWriteBatchData() {
  std::unique_lock lock{builders_mutex_};
  write_batch_segment_builders_.clear();
  return KStatus::SUCCESS;
}

KStatus TsVGroup::getEntityIdByPTag(kwdbContext_p ctx, TSTableID table_id, TSSlice& ptag, TSEntityID* entity_id) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TagTable> tag_table;
  s =  tb_schema_manager->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
    return s;
  }
  uint32_t sub_group_id = 0;
  uint32_t cur_entity_id = 0;
  if (!tag_table->hasPrimaryKey(ptag.data, ptag.len, cur_entity_id, sub_group_id)) {
    LOG_INFO("getEntityIdByPTag failed, table id[%lu]", table_id);
    *entity_id = 0;
  } else {
    *entity_id = cur_entity_id;
    assert(sub_group_id == this->GetVGroupID());
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoPut(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload) {
  TsRawPayload tmp_p{payload};
  auto table_id = tmp_p.GetTableID();
  TSSlice primary_key = tmp_p.GetPrimaryTag();
  auto tbl_version = tmp_p.GetTableVersion();
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr failed.");
    return s;
  }
  std::vector<AttributeInfo> metric_schema;
  tb_schema_mgr->GetColumnsExcludeDropped(metric_schema, tbl_version);
  TsRawPayload p(payload, metric_schema);
  TSEntityID entity_id;
  s = getEntityIdByPTag(ctx, table_id, primary_key, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("undoPut failed.");
    return s;
  }
  if (entity_id > 0) {
    auto row_num = p.GetRowCount();
    std::vector<KwTsSpan> ts_spans;
    for (size_t i = 0; i < row_num; i++) {
      timestamp64 cur_ts = p.GetTS(i);
      ts_spans.push_back({cur_ts, cur_ts});
    }
    s = deleteData(ctx, table_id, entity_id, {log_lsn, log_lsn}, ts_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("deleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::TrasvalAllPartition(kwdbContext_p ctx, TSTableID tbl_id,
const std::vector<KwTsSpan>& ts_spans, std::function<KStatus(std::shared_ptr<const TsPartitionVersion>)> func) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(tbl_id, tb_schema_mgr);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTableSchemaMgr failed.");
    return s;
  }
  auto db_id = tb_schema_mgr->GetDbID();
  auto ts_type = tb_schema_mgr->GetTsColDataType();

  std::vector<std::shared_ptr<const TsPartitionVersion>> ps =
    version_manager_->Current()->GetPartitions(db_id, ts_spans, ts_type);
  for (auto& p : ps) {
    s = func(p);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("func failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
const std::vector<KwTsSpan>& ts_spans) {
  TSEntityID entity_id;
  TSSlice p_tag{primary_tag.data(), primary_tag.length()};
  auto s = getEntityIdByPTag(ctx, tbl_id, p_tag, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getEntityIdByPTag failed.");
    return s;
  }
  if (entity_id > 0) {
    s = TrasvalAllPartition(ctx, tbl_id, ts_spans,
      [&](std::shared_ptr<const TsPartitionVersion> p) -> KStatus {
        auto ret = p->UndoDeleteData(entity_id, ts_spans, {0, log_lsn});
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("UndoDeleteData partition[%u/%lu] failed!",
              std::get<0>(p->GetPartitionIdentifier()), std::get<1>(p->GetPartitionIdentifier()));
          return ret;
        }
        return ret;
      });
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TrasvalAllPartition failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDeleteData(kwdbContext_p ctx, TSTableID tbl_id, std::string& primary_tag, TS_LSN log_lsn,
                                 const std::vector<KwTsSpan>& ts_spans) {
  TSEntityID entity_id;
  TSSlice p_tag{primary_tag.data(), primary_tag.length()};
  auto s = getEntityIdByPTag(ctx, tbl_id, p_tag, &entity_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getEntityIdByPTag failed.");
    return s;
  }
  if (entity_id > 0) {
    s = DeleteData(ctx, tbl_id, entity_id, log_lsn, ts_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DeleteData failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, TS_LSN log_lsn, uint32_t group_id,
                                uint32_t entity_id, TSSlice& tags) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoPutTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p{payload};
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();
  auto tbl_version = p.GetTableVersion();
  bool new_tag;

  uint32_t vgroup_id;
  TSEntityID entity_id;

  auto s = schema_mgr_->GetVGroup(ctx, table_id, primary_key, &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint8_t payload_data_flag = p.GetRowType();
  if (new_tag) {
    vgroup_id = GetVGroupID();
    entity_id = AllocateEntityID();
    // 1. Write tag data
    assert(payload_data_flag == DataTagFlag::DATA_AND_TAG || payload_data_flag == DataTagFlag::TAG_ONLY);
    LOG_DEBUG("tag bt insert hashPoint=%hu", p.GetHashPoint());
    std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
    s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Get schema manager failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
    std::shared_ptr<TagTable> tag_table;
    s = tb_schema_manager->GetTagSchema(ctx, &tag_table);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTagSchema failed, table id[%lu]", table_id);
      return s;
    }
    auto err_no = tag_table->InsertTagRecord(p, vgroup_id, entity_id);
    if (err_no < 0) {
      LOG_ERROR("InsertTagRecord failed, table id[%lu]", table_id);
      return KStatus::FAIL;
    }
  } else {
    assert(vgroup_id == this->GetVGroupID());
  }
  return s;
}

KStatus TsVGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoDeleteTag(kwdbContext_p ctx, TSSlice& primary_tag, kwdbts::TS_LSN log_lsn, uint32_t group_id,
                                uint32_t entity_id, TSSlice& payload) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::redoCreateHashIndex(const std::vector<uint32_t>& tags, uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoCreateHashIndex(uint32_t index_id, uint32_t ts_version) { return KStatus::SUCCESS; }

KStatus TsVGroup::redoDropHashIndex(uint32_t index_id, uint32_t ts_version) { return KStatus::SUCCESS; }

KStatus TsVGroup::undoDropHashIndex(const std::vector<uint32_t>& tags, uint32_t index_id, uint32_t ts_version) {
  return KStatus::SUCCESS;
}

KStatus TsVGroup::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  return tsx_manager_->MtrBegin(ctx, range_id, index, mtr_id);
}

KStatus TsVGroup::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id) {
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  return tsx_manager_->MtrCommit(ctx, mtr_id);
}

KStatus TsVGroup::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip) {
  //  1. Write ROLLBACK log;
  KStatus s;
  if (!is_skip) {
    s = tsx_manager_->MtrRollback(ctx, mtr_id);
    if (s == FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
