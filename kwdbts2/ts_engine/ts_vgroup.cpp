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
#include "ts_block.h"
#include "ts_common.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_builder.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_iterator_v2_impl.h"
#include "ts_lastsegment_builder.h"
#include "ts_mem_segment_mgr.h"
#include "ts_std_utils.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"

namespace kwdbts {

// todo(liangbo01) using normal path for mem_segment.
TsVGroup::TsVGroup(EngineOptions* engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr,
                   std::shared_mutex* engine_mutex, bool enable_compact_thread)
    : vgroup_id_(vgroup_id),
      schema_mgr_(schema_mgr),
      path_(fs::path(engine_options->db_path) / VGroupDirName(vgroup_id)),
      max_entity_id_(0),
      engine_options_(engine_options),
      engine_wal_level_mutex_(engine_mutex),
      version_manager_(std::make_unique<TsVersionManager>(engine_options->io_env, path_)),
      mem_segment_mgr_(std::make_unique<TsMemSegmentManager>(this, version_manager_.get())),
      enable_compact_thread_(enable_compact_thread) {}

TsVGroup::~TsVGroup() {
  enable_compact_thread_ = false;
  closeCompactThread();
}

KStatus TsVGroup::Init(kwdbContext_p ctx) {
  auto s = engine_options_->io_env->NewDirectory(path_);
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

  wal_manager_ = std::make_unique<WALMgr>(engine_options_->db_path, VGroupDirName(vgroup_id_), engine_options_);
  tsx_manager_ = std::make_unique<TSxMgr>(wal_manager_.get());
  auto res = wal_manager_->Init(ctx);
  if (res == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize WAL manager")
    return res;
  }
  UpdateAtomicLSN();

  return KStatus::SUCCESS;
}

KStatus TsVGroup::SetReady() {
  TsVersionUpdate update;
  std::list<std::shared_ptr<TsMemSegment>> mems;
  mem_segment_mgr_->GetAllMemSegments(&mems);
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
  if (EnableWAL() && write_wal) {
    LockSharedLevelMutex();
    TS_LSN entry_lsn = 0;
    // lock current lsn: Lock the current LSN until the log is written to the cache
    wal_manager_->Lock();
    current_lsn = wal_manager_->FetchCurrentLSN();
    KStatus s = wal_manager_->WriteInsertWAL(ctx, mtr_id, 0, 0, *primary_tag, *payload, entry_lsn, vgroup_id_);
    UnLockSharedLevelMutex();
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
  } else {
    current_lsn = LSNInc();
  }
  // TODO(limeng04): import and export current lsn that temporarily use wal
  if (EnableWAL() && !write_wal) {
    current_lsn = wal_manager_->FetchCurrentLSN();
  }
  auto s = mem_segment_mgr_->PutData(*payload, entity_id, current_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("mem_segment_mgr_.PutData Failed.")
    return FAIL;
  }
    // update vgroup entity_id.
  return KStatus::SUCCESS;
}

fs::path TsVGroup::GetPath() const { return path_; }

TSEntityID TsVGroup::AllocateEntityID() {
  std::lock_guard<std::mutex> lock1(entity_id_mutex_);
  std::unique_lock<std::shared_mutex> lock2(entity_latest_row_mutex_);
  uint64_t max_entity_id = ++max_entity_id_;
  entity_latest_row_checked_[max_entity_id] = TsEntityLatestRowStatus::Uninitialized;
  return max_entity_id;
}

TSEntityID TsVGroup::GetMaxEntityID() const {
  std::lock_guard<std::mutex> lock(entity_id_mutex_);
  return max_entity_id_;
}

void TsVGroup::InitEntityID(TSEntityID entity_id) {
  std::lock_guard<std::mutex> lock1(entity_id_mutex_);
  std::unique_lock<shared_mutex> lock2(entity_latest_row_mutex_);
  max_entity_id_ = entity_id;
  for (int i = 1; i <= max_entity_id_; ++i) {
    entity_latest_row_checked_[i] = TsEntityLatestRowStatus::Recovering;
  }
}

KStatus TsVGroup::RemoveChkFile(kwdbContext_p ctx) {
  wal_manager_->Lock();
  Defer defer{[&]() {
    wal_manager_->Unlock();
  }};
  // remove chk file
  KStatus s = wal_manager_->RemoveChkFile(ctx);
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
  TS_LSN first_lsn = wal_manager_->GetFirstLSN();
  last_lsn = wal_manager_->FetchCurrentLSN();
  auto next_first_lsn = last_lsn;
  if (last_lsn < GetMaxLSN()) {
    next_first_lsn = GetMaxLSN();
  }
  WALMeta meta = wal_manager_->GetMeta();
  KStatus s = wal_manager_->SwitchNextFile(next_first_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to switch next WAL file.")
    return s;
  }
  wal_manager_->Unlock();

  // new tmp wal mgr to read chk wal file
  WALMgr tmp_wal = WALMgr(engine_options_->db_path, VGroupDirName(vgroup_id_), engine_options_, true);
  tmp_wal.InitForChk(ctx, meta);
  s = tmp_wal.ReadWALLog(logs, first_lsn, last_lsn, ignore);
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
      wal_manager_->ReadWALLog(logs, wal_manager_->GetFirstLSN(), wal_manager_->FetchCurrentLSN(), ignore);
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

TsEngineSchemaManager* TsVGroup::GetSchemaMgr() const {
  return schema_mgr_;
}

// todo(liangbo01) create partition should at data inserting wal.
// if at here, inserting and deleting at same time. maybe sql exec over, data all exist.
// but restart service, data all disappeared.
// solution: 1. delete item not store in partition. 2. creating partition before wal insert.

// KStatus TsVGroup::makeSurePartitionExist(TSTableID table_id, const std::list<TSMemSegRowData>& rows) {
//   std::shared_ptr<kwdbts::TsTableSchemaManager> tb_schema_mgr;
//   auto s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_mgr);
//   if (s != KStatus::SUCCESS) {
//     LOG_ERROR("GetTableSchemaMgr failed. table[%lu]", table_id);
//     return s;
//   }
//   auto ts_type = tb_schema_mgr->GetTsColDataType();
//   for (auto& row : rows) {
//     auto p_time = convertTsToPTime(row.ts, ts_type);
//     version_manager_->AddPartition(row.database_id, p_time);
//   }
//   return KStatus::SUCCESS;
// }

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
    s = mem_segment_mgr_->PutData(payload, entity_id, log_lsn);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("failed putdata.");
      return s;
    }
  }
  return s;
}

KStatus TsVGroup::GetLastRowEntity(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                   pair<timestamp64, uint32_t>& last_row_entity) {
  KTableKey table_id = table_schema_mgr->GetTableId();
  {
    std::shared_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    if (last_row_entity_checked_[table_id] && last_row_entity_.count(table_id)) {
      last_row_entity = last_row_entity_[table_id];
      return KStatus::SUCCESS;
    }
  }
  DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
  uint32_t db_id = GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  auto current = CurrentVersion();
  auto ts_partitions = current->GetPartitions(db_id, {{INT64_MIN, INT64_MAX}}, ts_col_type);
  {
    std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
    if (ts_partitions.empty()) {
      last_row_entity = {INT64_MIN, 0};
      last_row_entity_checked_[table_id] = true;
      return KStatus::SUCCESS;
    }
  }
  bool last_row_found = false;
  // TODO(liumengzhen) : set correct lsn
  TS_LSN scan_lsn = UINT64_MAX;
  std::vector<KwTsSpan> ts_spans = {{INT64_MIN, INT64_MAX}};
  TsScanFilterParams filter{db_id, table_id, vgroup_id_, 0, ts_col_type,
                            scan_lsn, ts_spans};

  std::shared_ptr<TagTable> tag_schema;
  table_schema_mgr->GetTagSchema(ctx, &tag_schema);
  std::vector<uint32_t> entity_id_list;
  tag_schema->GetEntityIdListByVGroupId(vgroup_id_, entity_id_list);
  std::shared_ptr<MMapMetricsTable> metric_schema = table_schema_mgr->GetCurrentMetricsTable();
  for (int i = ts_partitions.size() - 1; !last_row_found && i >= 0; --i) {
    std::shared_ptr<TsBlockSpan> last_block_span = nullptr;
    for (auto &entity_id : entity_id_list) {
      std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans;
      filter.entity_id_ = entity_id;
      KStatus ret = ts_partitions[i]->GetBlockSpans(filter, &ts_block_spans, table_schema_mgr,
                                                   metric_schema);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpan failed.");
        return KStatus::FAIL;
      }
      if (ts_block_spans.empty()) continue;

      TsBlockSpanSortedIterator iter(ts_block_spans, EngineOptions::g_dedup_rule);
      iter.Init();
      bool is_finished = false;
      std::shared_ptr<TsBlockSpan> dedup_block_span;
      std::vector<shared_ptr<TsBlockSpan>> sorted_ts_block_spans;
      while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
        sorted_ts_block_spans.push_back(dedup_block_span);
      }
      ts_block_spans.clear();
      dedup_block_span.reset();

      if (last_block_span == nullptr || last_block_span->GetLastTS() < sorted_ts_block_spans.back()->GetLastTS()) {
        last_block_span = sorted_ts_block_spans.back();
      }
    }
    if (last_block_span == nullptr) continue;
    last_row_entity = {last_block_span->GetLastTS(), last_block_span->GetEntityID()};
    last_row_found = true;
  }
  std::unique_lock<std::shared_mutex> lock(last_row_entity_mutex_);
  if (!last_row_entity_.count(table_id) || last_row_entity.first >= last_row_entity_[table_id].first) {
    last_row_entity_[table_id] = last_row_entity;
  }
  last_row_entity_checked_[table_id] = true;
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetEntityLastRow(std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                   uint32_t entity_id, const std::vector<KwTsSpan>& ts_spans,
                                   timestamp64& entity_last_ts) {
  entity_last_ts = INVALID_TS;
  {
    std::shared_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    if (entity_latest_row_checked_[entity_id] == TsEntityLatestRowStatus::Uninitialized) {
      return KStatus::SUCCESS;
    }
    if (entity_latest_row_checked_[entity_id] == TsEntityLatestRowStatus::Valid
        && checkTimestampWithSpans(ts_spans, entity_latest_row_[entity_id],
                                   entity_latest_row_[entity_id]) != TimestampCheckResult::NonOverlapping) {
      entity_last_ts = entity_latest_row_[entity_id];
      return KStatus::SUCCESS;
    }
  }
  KTableKey table_id = table_schema_mgr->GetTableId();
  DATATYPE ts_col_type = table_schema_mgr->GetTsColDataType();
  uint32_t db_id = GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  auto current = CurrentVersion();
  auto ts_partitions = current->GetPartitions(db_id, {{INT64_MIN, INT64_MAX}}, ts_col_type);
  if (ts_partitions.empty()) {
    std::unique_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    entity_latest_row_checked_[entity_id] = TsEntityLatestRowStatus::Valid;
    entity_latest_row_[entity_id] = INT64_MIN;
    return KStatus::SUCCESS;
  }

  // TODO(liumengzhen) : set correct lsn
  TS_LSN scan_lsn = UINT64_MAX;
  std::vector<KwTsSpan> spans = {{INT64_MIN, INT64_MAX}};
  TsScanFilterParams filter{db_id, table_id, vgroup_id_, entity_id, ts_col_type,
                            scan_lsn, spans};
  std::shared_ptr<TsBlockSpan> last_block_span = nullptr;
  std::shared_ptr<MMapMetricsTable> metric_schema = table_schema_mgr->GetCurrentMetricsTable();
  for (int i = ts_partitions.size() - 1; i >= 0; --i) {
    std::list<std::shared_ptr<TsBlockSpan>> ts_block_spans;
    KStatus ret = ts_partitions[i]->GetBlockSpans(filter, &ts_block_spans, table_schema_mgr,
                                                 metric_schema);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpan failed.");
      return KStatus::FAIL;
    }
    if (ts_block_spans.empty()) continue;

    TsBlockSpanSortedIterator iter(ts_block_spans, EngineOptions::g_dedup_rule);
    iter.Init();
    bool is_finished = false;
    std::shared_ptr<TsBlockSpan> dedup_block_span;
    std::vector<shared_ptr<TsBlockSpan>> sorted_ts_block_spans;
    while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
      sorted_ts_block_spans.push_back(dedup_block_span);
    }
    ts_block_spans.clear();
    dedup_block_span.reset();

    last_block_span = sorted_ts_block_spans.back();
    sorted_ts_block_spans.clear();
    break;
  }
  if (last_block_span != nullptr) {
    std::unique_lock<std::shared_mutex> lock(entity_latest_row_mutex_);
    if (!entity_latest_row_.count(entity_id) || last_block_span->GetLastTS() >= entity_latest_row_[entity_id]) {
      entity_latest_row_[entity_id] = last_block_span->GetLastTS();
      entity_latest_row_checked_[entity_id] = TsEntityLatestRowStatus::Valid;
    }
    if (checkTimestampWithSpans(ts_spans, entity_latest_row_[entity_id],
                                entity_latest_row_[entity_id]) != TimestampCheckResult::NonOverlapping) {
      entity_last_ts = entity_latest_row_[entity_id];
    }
  }
  return KStatus::SUCCESS;
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

KStatus TsVGroup::PartitionCompact(std::shared_ptr<const TsPartitionVersion> partition, bool call_by_vacuum) {
  auto partition_id = partition->GetPartitionIdentifier();
  if (!partition->TrySetBusy(PartitionStatus::Compacting)) {
    auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
    LOG_INFO("partition skip compact, root_path: %s", root_path.c_str());
    return KStatus::FAIL;
  }
  partition = version_manager_->Current()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
  Defer defer{[&]() {
    partition->ResetStatus();
  }};
  TsVersionUpdate update;
  // 1. Get all the last segments that need to be compacted.
  std::vector<std::shared_ptr<TsLastSegment>> last_segments;
  if (!call_by_vacuum) {
    last_segments = partition->GetCompactLastSegments();
  } else {
    last_segments = partition->GetAllLastSegments();
  }
  if (last_segments.empty()) {
    return KStatus::SUCCESS;
  }
  auto entity_segment = partition->GetEntitySegment();

  auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());

  // 2. Build the column block.
  {
    TsEntitySegmentBuilder builder(root_path.string(), schema_mgr_, version_manager_.get(),
                                   partition->GetPartitionIdentifier(), entity_segment,
                                   last_segments);
    KStatus s = builder.Open();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder open failed", path_.c_str());
      return s;
    }
    s = builder.Compact(call_by_vacuum, &update);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition[%s] compact failed, TsEntitySegmentBuilder build failed", path_.c_str());
      return s;
    }
  }

  // 3. Set the compacted version.
  for (auto& last_segment : last_segments) {
    update.DeleteLastSegment(partition->GetPartitionIdentifier(), last_segment->GetFileNumber());
  }

  // 4. Update the version.
  return version_manager_->ApplyUpdate(&update);
}

KStatus TsVGroup::Compact() {
  auto current = version_manager_->Current();
  std::vector<std::shared_ptr<const TsPartitionVersion>> partitions = current->GetPartitionsToCompact();
  // Compact partitions
  bool success{true};
  for (auto it = partitions.rbegin(); it != partitions.rend(); ++it) {
    const auto& cur_partition = *it;
    KStatus s = PartitionCompact(cur_partition);
    if (s != KStatus::SUCCESS) {
      success = false;
      continue;
    }
  }
  if (!success) {
    LOG_ERROR("compact failed.");
    return FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::FlushImmSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();

  std::unordered_map<std::shared_ptr<const TsPartitionVersion>, TsLastSegmentBuilder> builders;
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
    if (EngineOptions::g_dedup_rule == DedupRule::KEEP) {
      sorted_spans.reserve(all_block_spans.size());
      std::move(all_block_spans.begin(), all_block_spans.end(), std::back_inserter(sorted_spans));
      std::sort(sorted_spans.begin(), sorted_spans.end(),
                [](const std::shared_ptr<TsBlockSpan>& left, const std::shared_ptr<TsBlockSpan>& right) {
                  using Helper = std::tuple<TSEntityID, timestamp64, TS_LSN>;
                  auto left_helper = Helper(left->GetEntityID(), left->GetFirstTS(), *left->GetLSNAddr(0));
                  auto right_helper = Helper(right->GetEntityID(), right->GetFirstTS(), *right->GetLSNAddr(0));
                  return left_helper < right_helper;
                });
    } else {
      TsBlockSpanSortedIterator iter(all_block_spans, EngineOptions::g_dedup_rule);
      iter.Init();
      std::shared_ptr<TsBlockSpan> dedup_block_span;
      bool is_finished = false;
      while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
        sorted_spans.push_back(std::move(dedup_block_span));
      }
    }
  }

  auto current = version_manager_->Current();

  TSEntityID cur_entity  = 0;
  uint64_t entity_row_num = 0;
  std::shared_ptr<const TsPartitionVersion> cur_partition = nullptr;
  timestamp64 min_ts = INT64_MAX;
  timestamp64 max_ts = INT64_MIN;

  for (auto span : sorted_spans) {
    if (cur_entity != span->GetEntityID()) {
      if (cur_entity != 0) {
        TsEntityFlushInfo entity_flush_info = {cur_entity, min_ts, max_ts, entity_row_num, ""};
        cur_partition->GetCountManager()->AddFlushEntityAgg(entity_flush_info);
      }
      cur_entity = span->GetEntityID();
      entity_row_num = 0;
      cur_partition = nullptr;
    }
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
    while (current_span != nullptr && current_span->GetRowNum() != 0) {
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
        s = env->NewDirectory(path);
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

      if (cur_partition == nullptr) {
        cur_partition = partition;
        min_ts = current_span->GetFirstTS();
      } else if (cur_partition->GetPartitionIdentifier() != partition->GetPartitionIdentifier()) {
        TsEntityFlushInfo entity_flush_info = {cur_entity, min_ts, max_ts, entity_row_num, ""};
        cur_partition->GetCountManager()->AddFlushEntityAgg(entity_flush_info);
        cur_partition = partition;
        entity_row_num = 0;
        min_ts = current_span->GetFirstTS();
      }
      std::shared_ptr<TsBlockSpan> span_to_flush;
      if (last_ts < partition->GetEndTime()) {
        // all the data in this span is whithin the current partition, no need to split.
        span_to_flush = std::move(current_span);
        current_span = nullptr;
        max_ts = span_to_flush->GetLastTS();
        entity_row_num += span_to_flush->GetRowNum();
      } else {
        // split the span into two parts.
        // find the first row that satisfies current_span->GetTS(idx) => partition->GetEndTime()
        int split_idx = *std::upper_bound(IndexRange{0}, IndexRange(current_span->GetRowNum()), partition->GetEndTime(),
                                          [&](timestamp64 val, int idx) {
                                            return val <= convertTsToPTime(current_span->GetTS(idx),
                                                                          static_cast<DATATYPE>(info[0].type));
                                          });

        std::shared_ptr<TsBlockSpan> front_span;
        current_span->SplitFront(split_idx, front_span);
        span_to_flush = std::move(front_span);
        max_ts = span_to_flush->GetLastTS();
        entity_row_num += span_to_flush->GetRowNum();
      }

      auto it = builders.find(partition);
      if (it == builders.end()) {
        std::unique_ptr<TsAppendOnlyFile> last_segment;
        uint64_t file_number = version_manager_->NewFileNumber();
        auto path =
            this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier()) / LastSegmentFileName(file_number);
        s = env->NewAppendOnlyFile(path, &last_segment);
        if (s == FAIL) {
          LOG_ERROR("cannot create last segment file.");
          return FAIL;
        }

        auto result = builders.insert({partition, TsLastSegmentBuilder{schema_mgr_, std::move(last_segment),
                                                                         static_cast<uint32_t>(file_number)}});
        it = result.first;
      }
      TsLastSegmentBuilder& builder = it->second;
      s = builder.PutBlockSpan(span_to_flush);
      if (s == FAIL) {
        LOG_ERROR("PutBlockSpan failed.");
        return FAIL;
      }
    }
  }
  if (entity_row_num != 0) {
    TsEntityFlushInfo entity_flush_info = {cur_entity, min_ts, max_ts, entity_row_num, ""};
    cur_partition->GetCountManager()->AddFlushEntityAgg(entity_flush_info);
  }

  for (auto& [k, v] : builders) {
    auto s = v.Finalize();
    if (s == FAIL) {
      return FAIL;
    }
    update.AddLastSegment(k->GetPartitionIdentifier(), v.GetFileNumber());
    update.SetMaxLSN(v.GetMaxLSN());
  }

  mem_segment_mgr_->RemoveMemSegment(mem_seg);
  std::list<std::shared_ptr<TsMemSegment>> mems;
  mem_segment_mgr_->GetAllMemSegments(&mems);
  update.SetValidMemSegments(mems);

  version_manager_->ApplyUpdate(&update);
  return KStatus::SUCCESS;
}

KStatus TsVGroup::GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                              std::vector<KwTsSpan> ts_spans, std::vector<BlockFilter> block_filter, DATATYPE ts_col_type,
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
    ts_iter = new TsSortedRawDataIteratorV2Impl(vgroup, entity_ids, ts_spans, block_filter, ts_col_type, scan_cols,
                                                ts_scan_cols, table_schema_mgr, table_version, ASC);
  } else {
    // need call Next function times: entity_ids.size(), no matter Next return what.
    ts_iter = new TsAggIteratorV2Impl(vgroup, entity_ids, ts_spans, block_filter, ts_col_type, scan_cols, ts_scan_cols,
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
                                std::shared_ptr<const TsVGroupVersion>& current,
                                std::list<std::shared_ptr<TsBlockSpan>>* block_spans) {
  uint32_t db_id = schema_mgr_->GetDBIDByTableID(table_id);
  current = version_manager_->Current();
  std::vector<KwTsSpan> ts_spans{ts_span};
  auto ts_partitions = current->GetPartitions(db_id, ts_spans, ts_col_type);
  std::shared_ptr<MMapMetricsTable> metric_schema = table_schema_mgr->GetCurrentMetricsTable();
  for (int32_t index = 0; index < ts_partitions.size(); ++index) {
    TS_LSN lsn;
    if (EnableWAL()) {
      lsn = wal_manager_->FetchCurrentLSN();
    } else {
      lsn = LSNInc();
    }
    TsScanFilterParams filter{db_id, table_id, vgroup_id_, entity_id, ts_col_type, lsn, ts_spans};
    auto partition_version = ts_partitions[index];
    std::list<std::shared_ptr<TsBlockSpan>> cur_block_span;
    auto s = partition_version->GetBlockSpans(filter, &cur_block_span, table_schema_mgr, metric_schema);
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
        return undoDeleteTag(ctx, log->getTableID(), primary_tag, lsn, log->group_id_, log->entity_id_, tags);
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
        return redoDeleteTag(ctx, log->getTableID(), p_tag_slice, log->getLSN(), log->group_id_, log->entity_id_, tag_slice);
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
  TS_LSN cur_lsn = 0;
  auto tag_table = tb_schema_manager->GetTagTable();
  if (EnableWAL()) {
    TagTuplePack* tag_pack = tag_table->GenTagPack(p_tag.data(), p_tag.size());
    if (UNLIKELY(nullptr == tag_pack)) {
      return KStatus::FAIL;
    }
    LockSharedLevelMutex();
    s = wal_manager_->WriteDeleteTagWAL(ctx, mtr_id, p_tag, vgroup_id_, e_id, tag_pack->getData(), vgroup_id_,
                                        table_id);
    cur_lsn = wal_manager_->FetchCurrentLSN();
    UnLockSharedLevelMutex();
    delete tag_pack;
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteDeleteTagWAL failed.");
      return s;
    }
  } else {
    cur_lsn = LSNInc();
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
  // todo(xy): need to initialize lsn if wal_level = off
  TS_LSN current_lsn = 0;
  if (EnableWAL()) {
    LockSharedLevelMutex();
    KStatus s = wal_manager_->WriteDeleteMetricsWAL4V2(ctx, mtr_id, tbl_id, p_tag, ts_spans, vgroup_id_, &current_lsn);
    UnLockSharedLevelMutex();
    if (s == KStatus::FAIL) {
      LOG_ERROR("WriteDeleteTagWAL failed.");
      return s;
    }
  } else {
    current_lsn = LSNInc();
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

KStatus TsVGroup::GetEntitySegmentBuilder(std::shared_ptr<const TsPartitionVersion>& partition,
                                          std::shared_ptr<TsEntitySegmentBuilder>& builder) {
  PartitionIdentifier partition_id = partition->GetPartitionIdentifier();
  auto it = write_batch_segment_builders_.find(partition_id);
  if (it == write_batch_segment_builders_.end()) {
    while (!partition->TrySetBusy(PartitionStatus::BatchDataWriting)) {
      sleep(1);
    }
    partition = version_manager_->Current()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
    auto entity_segment = partition->GetEntitySegment();

    auto root_path = this->GetPath() / PartitionDirName(partition->GetPartitionIdentifier());
    builder = std::make_shared<TsEntitySegmentBuilder>(root_path.string(), version_manager_.get(), partition_id,
                                                       entity_segment);
    KStatus s = builder->Open();
    if (s != KStatus::SUCCESS) {
      partition->ResetStatus();
      LOG_ERROR("Open entity segment builder failed.");
      return s;
    }
    write_batch_segment_builders_[partition_id] = builder;
  } else {
    builder = it->second;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::WriteBatchData(kwdbContext_p ctx, TSTableID tbl_id, uint32_t table_version,
                                 TSEntityID entity_id, timestamp64 p_time, TS_LSN lsn, TSSlice data) {
  auto current = version_manager_->Current();
  uint32_t database_id = schema_mgr_->GetDBIDByTableID(tbl_id);
  if (database_id == 0) {
    return KStatus::FAIL;
  }

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

  std::shared_ptr<TsEntitySegmentBuilder> builder;
  KStatus s = GetEntitySegmentBuilder(partition, builder);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntitySegmentBuilder failed.");
    return s;
  }
  TsEntityFlushInfo flush_info{};
  s = builder->WriteBatch(tbl_id, entity_id, table_version, lsn, data, &flush_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("WriteBatch failed.");
    return s;
  }

  ResetEntityLatestRow(entity_id, INT64_MAX);
  ResetEntityMaxTs(tbl_id, INT64_MAX, entity_id);

  s = partition->GetCountManager()->AddFlushEntityAgg(flush_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("AddFlushEntityAgg failed.");
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::FinishWriteBatchData() {
  TsVersionUpdate update;
  std::set<PartitionIdentifier> partition_ids;
  bool success = true;
  for (auto& kv : write_batch_segment_builders_) {
    partition_ids.insert(kv.first);
    KStatus s = kv.second->WriteBatchFinish(&update);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Finish entity segment builder failed");
      success = false;
    }
  }
  write_batch_segment_builders_.clear();
  if (success) {
    version_manager_->ApplyUpdate(&update);
  }
  for (auto p_id : partition_ids) {
    auto partition = version_manager_->Current()->GetPartition(std::get<0>(p_id), std::get<1>(p_id));
    partition->ResetStatus();
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::CancelWriteBatchData() {
  std::set<PartitionIdentifier> partition_ids;
  auto current = version_manager_->Current();
  for (auto& kv : write_batch_segment_builders_) {
    partition_ids.insert(kv.first);
    kv.second->WriteBatchCancel(current);
  }
  write_batch_segment_builders_.clear();
  for (auto p_id : partition_ids) {
    auto partition = current->GetPartition(std::get<0>(p_id), std::get<1>(p_id));
    partition->ResetStatus();
  }
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
  TsRawPayload p(payload, &metric_schema);
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
  }
  return s;
}

KStatus TsVGroup::undoPutTag(kwdbContext_p ctx, TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p(payload);
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
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

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("undoPutTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  int res = tag_table->InsertForUndo(group_id, entity_id, primary_key);
  if (res < 0) {
    LOG_ERROR("undoPutTag: InsertForUndo failed, primary tag[%s]", primary_key.data)
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::redoUpdateTag(kwdbContext_p ctx, kwdbts::TS_LSN log_lsn, const TSSlice& payload) {
  TsRawPayload p(payload);
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
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

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoUpdateTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }
  int res;
  res = tag_table->UpdateForRedo(group_id, entity_id, primary_key, p);
  if (res < 0) {
    LOG_ERROR("redoUpdateTag: UpdateForRedo failed, primary tag[%s].", primary_key.data)
    return KStatus::FAIL;
  }
  return SUCCESS;
}

KStatus TsVGroup::undoUpdateTag(kwdbContext_p ctx, TS_LSN log_lsn, TSSlice payload, const TSSlice& old_payload) {
  TsRawPayload p(payload);
  auto table_id = p.GetTableID();
  TSSlice primary_key = p.GetPrimaryTag();

  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
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

  ErrorInfo err_info;
  uint32_t entity_id, group_id;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("undoUpdateTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  if (tag_table->UpdateForUndo(group_id, entity_id, p.GetHashPoint(), primary_key, old_payload) < 0) {
    LOG_ERROR("undoUpdateTag: UpdateForUndo failed, primary tag[%s].", primary_key.data)
    return KStatus::FAIL;
  }

  return SUCCESS;
}

KStatus TsVGroup::redoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_key, kwdbts::TS_LSN log_lsn,
                                uint32_t group_id, uint32_t entity_id, TSSlice& tags) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
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

  ErrorInfo err_info;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoDeleteTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }

  int res = tag_table->DeleteForRedo(group_id, entity_id, primary_key, tags);
  if (res) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::undoDeleteTag(kwdbContext_p ctx, uint64_t table_id, TSSlice& primary_key, TS_LSN log_lsn,
                                uint32_t group_id, uint32_t entity_id, TSSlice& tags) {
  std::shared_ptr<TsTableSchemaManager> tb_schema_manager;
  KStatus s = schema_mgr_->GetTableSchemaMgr(table_id, tb_schema_manager);
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

  ErrorInfo err_info;
  if (!tag_table->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, group_id)) {
    LOG_WARN("redoDeleteTag: can not find primary tag[%s].", primary_key.data)
    return KStatus::SUCCESS;
  }
  int res = tag_table->DeleteForUndo(group_id, entity_id, tb_schema_manager->GetHashNum(), primary_key, tags);
  if (res < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mtr_id, const char* tsx_id) {
  // Invoke the TSxMgr interface to start the Mini-Transaction and write the BEGIN log entry
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  return tsx_manager_->MtrBegin(ctx, range_id, index, mtr_id, tsx_id);
}

KStatus TsVGroup::MtrCommit(kwdbContext_p ctx, uint64_t& mtr_id, const char* tsx_id) {
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  // Call the TSxMgr interface to COMMIT the Mini-Transaction and write the COMMIT log entry
  if (tsx_id != nullptr) {
    mtr_id = tsx_manager_->getMtrID(tsx_id);
  }
  return tsx_manager_->MtrCommit(ctx, mtr_id, tsx_id);
}

KStatus TsVGroup::MtrRollback(kwdbContext_p ctx, uint64_t& mtr_id, bool is_skip, const char* tsx_id) {
  LockSharedLevelMutex();
  Defer defer{[&]() {
    UnLockSharedLevelMutex();
  }};
  //  1. Write ROLLBACK log;
  KStatus s;
  if (!is_skip) {
    if (tsx_id != nullptr) {
      mtr_id = tsx_manager_->getMtrID(tsx_id);
    }
    s = tsx_manager_->MtrRollback(ctx, mtr_id, tsx_id);
    if (s == FAIL) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroup::Vacuum() {
  KStatus s = KStatus::SUCCESS;
  TS_LSN cur_lsn;
  if (EnableWAL()) {
    cur_lsn = wal_manager_->FetchCurrentLSN();
  } else {
    cur_lsn = LSNInc();
  }
  auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
  auto current = version_manager_->Current();
  auto all_partitions = current->GetPartitions();

  for (auto& [db_id, partitions] : all_partitions) {
    if (partitions.size() == 1) {
      continue;
    }
    for (int i = 0; i < partitions.size() - 1; i++) {
      const auto& partition = partitions[i];
      auto partition_id = partition->GetPartitionIdentifier();
      auto root_path = this->GetPath() / PartitionDirName(partition_id);
      bool need_vacuum = false;
      s = partition->NeedVacuumEntitySegment(root_path, schema_mgr_, need_vacuum);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NeedVacuumEntitySegment failed.");
        continue;
      }
      if (!need_vacuum) {
        LOG_DEBUG("no need vacuum partition [%s]", partition->GetPartitionIdentifierStr().c_str());
        continue;
      }

      // force compact historical partition
      s = PartitionCompact(partition, true);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("PartitionCompact failed.");
        continue;
      }
      if (!partition->TrySetBusy(PartitionStatus::Vacuuming)) {
        continue;
      }
      Defer defer{[&]() {
        partition->ResetStatus();
      }};

      auto entity_segment = partition->GetEntitySegment();
      if (entity_segment == nullptr) {
        continue;
      }

      LOG_INFO("Vacuum partition [vgroup_%d]-[%ld, %ld) begin", vgroup_id_, partition->GetStartTime(),
                                                                partition->GetEndTime() - 1);
      auto max_entity_id = entity_segment->GetEntityNum();

      auto vacuumer = std::make_unique<TsEntitySegmentVacuumer>(root_path, this->version_manager_.get());
      vacuumer->Open();

      auto mem_segments = partition->GetAllMemSegments();
      std::list<std::pair<TSEntityID, TS_LSN>> entity_max_lsn;
      for (uint32_t entity_id = 1; entity_id <= max_entity_id; entity_id++) {
        TsEntityItem entity_item;
        bool found = false;
        s = entity_segment->GetEntityItem(entity_id, entity_item, found);
        if (s != SUCCESS) {
          LOG_ERROR("Vacuum failed, GetEntityItem failed")
          return s;
        }
        if (!found || 0 == entity_item.cur_block_id) {
          TsEntityItem empty_entity_item{entity_id};
          empty_entity_item.table_id = entity_item.table_id;
          s = vacuumer->AppendEntityItem(empty_entity_item);
          if (s != SUCCESS) {
            LOG_ERROR("Vacuum failed, AppendEntityItem failed")
            return s;
          }
          continue;
        }
        std::shared_ptr<TsTableSchemaManager> tb_schema_mgr{nullptr};
        s = schema_mgr_->GetTableSchemaMgr(entity_item.table_id, tb_schema_mgr);
        if (s != SUCCESS) {
          return s;
        }
        if (tb_schema_mgr->IsDropped()) {
          TsEntityItem empty_entity_item{entity_id};
          empty_entity_item.table_id = entity_item.table_id;
          s = vacuumer->AppendEntityItem(empty_entity_item);
          if (s != SUCCESS) {
            LOG_ERROR("Vacuum failed, AppendEntityItem failed")
            return s;
          }
          entity_max_lsn.emplace_back(entity_id, UINT64_MAX);
          continue;
        }
        auto life_time = tb_schema_mgr->GetLifeTime();
        int64_t start_ts = INT64_MIN;
        int64_t end_ts = INT64_MAX;
        if (life_time.ts != 0) {
          start_ts = (now.time_since_epoch().count() - life_time.ts) * life_time.precision;
        }
        KwTsSpan ts_span = {start_ts, end_ts};
        TsScanFilterParams filter{partition->GetDatabaseID(), entity_item.table_id, vgroup_id_,
                                  entity_id, tb_schema_mgr->GetTsColDataType(), UINT64_MAX, {ts_span}};
        TsBlockItemFilterParams block_data_filter;
        s = partition->getFilter(filter, block_data_filter);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("getFilter failed");
          return s;
        }

        std::list<shared_ptr<TsBlockSpan>> block_spans;
        std::shared_ptr<MMapMetricsTable> metric_schema;
        s = tb_schema_mgr->GetMetricSchema(0, &metric_schema);
        if (s != SUCCESS) {
          LOG_ERROR("Vacuum failed, GetMetricSchema failed")
          return s;
        }
        s = entity_segment->GetBlockSpans(block_data_filter, block_spans, tb_schema_mgr, metric_schema);
        if (s != SUCCESS) {
          LOG_ERROR("Vacuum failed, GetBlockSpans failed")
          return s;
        }
        TsEntityItem cur_entity_item = {entity_id};
        cur_entity_item.table_id = entity_item.table_id;
        for (auto& block_span : block_spans) {
          string data;
          block_span->GetCompressData(data);
          uint32_t col_count = block_span->GetColCount();
          uint32_t col_offsets_len = (col_count + 1) * sizeof(uint32_t);
          auto last_col_tail_offset = *reinterpret_cast<uint32_t *>(data.data() + col_count * sizeof(uint32_t));
          auto block_data_len = col_offsets_len + last_col_tail_offset;
          auto block_agg_len = data.size() - block_data_len;
          string block_data = data.substr(0, block_data_len);
          string block_agg = data.substr(block_data_len, block_agg_len);

          TsEntitySegmentBlockItem blk_item;
          blk_item.entity_id = entity_item.entity_id;
          blk_item.table_version = block_span->GetTableVersion();
          blk_item.n_cols = block_span->GetColCount() + 1;
          blk_item.n_rows = block_span->GetRowNum();
          blk_item.min_ts = block_span->GetFirstTS();
          blk_item.max_ts = block_span->GetLastTS();
          blk_item.block_len = block_data.size();
          blk_item.agg_len = block_agg.size();
          s = vacuumer->AppendBlock({block_data.data(), block_data.size()}, &blk_item.block_offset);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("Vacuum failed, AppendBlock failed")
            return s;
          }
          s = vacuumer->AppendAgg({block_agg.data(), block_agg.size()}, &blk_item.agg_offset);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("Vacuum failed, AppendAgg failed")
            return s;
          }
          blk_item.prev_block_id = cur_entity_item.cur_block_id;
          s = vacuumer->AppendBlockItem(blk_item);  // block_id is set when append
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("Vacuum failed, AppendBlockItem failed")
            return s;
          }
          cur_entity_item.cur_block_id = blk_item.block_id;
          cur_entity_item.row_written += blk_item.n_rows;
          if (blk_item.max_ts > cur_entity_item.max_ts) {
            cur_entity_item.max_ts = blk_item.max_ts;
          }
          if (blk_item.min_ts < cur_entity_item.min_ts) {
            cur_entity_item.min_ts = blk_item.min_ts;
          }
        }
        s = vacuumer->AppendEntityItem(cur_entity_item);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("Vacuum failed, AppendEntityItem failed")
          return s;
        }
        {
          // check weather mem segment has data for one entity
          KwTsSpan partition_ts_span = {partition->GetTsColTypeStartTime(tb_schema_mgr->GetTsColDataType()),
                                        partition->GetTsColTypeEndTime(tb_schema_mgr->GetTsColDataType())};
          STScanRange scan_range = {partition_ts_span, {0, cur_lsn}};
          DatabaseID db_id = std::get<0>(partition->GetPartitionIdentifier());
          TsBlockItemFilterParams param {db_id, entity_item.table_id, vgroup_id_, entity_id, {scan_range}};
          std::list<shared_ptr<TsBlockSpan>> mem_block_spans;
          std::shared_ptr<MMapMetricsTable> metric_schema;
          s = tb_schema_mgr->GetMetricSchema(0, &metric_schema);
          if (s != SUCCESS) {
            LOG_ERROR("Vacuum failed, GetMetricSchema failed")
            return s;
          }
          for (auto& mem_segment : mem_segments) {
            mem_segment->GetBlockSpans(param, mem_block_spans, tb_schema_mgr, metric_schema);
          }
          if (mem_block_spans.empty()) {
            entity_max_lsn.emplace_back(entity_id, cur_lsn);
          }
        }
      }
      TsVersionUpdate update;
      auto info = vacuumer->GetHandleInfo();
      update.SetEntitySegment(partition->GetPartitionIdentifier(), info, true);
      vacuumer.reset();
      version_manager_->ApplyUpdate(&update);

      s = partition->RmDeleteItems(entity_max_lsn);
      if (s != KStatus::SUCCESS) {
        LOG_INFO("delete delitem failed. can ignore this.");
      }
      LOG_INFO("Vacuum partition [vgroup_%d]-[%ld, %ld) succeeded", vgroup_id_, partition->GetStartTime(),
                                                                    partition->GetEndTime() - 1);
    }
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
