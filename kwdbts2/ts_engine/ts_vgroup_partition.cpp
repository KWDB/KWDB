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

#include <cstdio>
#include <filesystem>
#include <memory>
#include <utility>

#include "ts_block_segment.h"
#include "ts_vgroup_partition.h"
#include "ts_env.h"
#include "ts_last_segment_manager.h"
#include "ts_metric_block.h"

namespace kwdbts {

TsVGroupPartition::TsVGroupPartition(std::filesystem::path root, int database_id, TsEngineSchemaManager* schema_mgr,
                                     int64_t start, int64_t end, bool enable_compact_thread)
    : database_id_(database_id),
      schema_mgr_(schema_mgr),
      start_(start),
      end_(end),
      path_(root / GetFileName()),
      last_segment_mgr_(path_),
      enable_compact_thread_(enable_compact_thread) {
  partition_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_MMAP_GROUP_PARTITION_RWLOCK);
  initCompactThread();
}

TsVGroupPartition::~TsVGroupPartition() {
  enable_compact_thread_ = false;
  closeCompactThread();
}

KStatus TsVGroupPartition::Open() {
  std::filesystem::create_directories(path_);
  blk_segment_ = std::make_unique<TsBlockSegment>(path_);
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::Compact(int thread_num) {
  // TODO(limeng04): Compact all the last segments in the historical partition.
  // 1. Get all the last segments that need to be compacted.
  std::vector<std::shared_ptr<TsLastSegment>> last_segments = last_segment_mgr_.GetCompactLastSegments();
  if (last_segments.empty()) {
    return KStatus::SUCCESS;
  }
  // 2. Build the column block.
  TsBlockSegmentBuilder builder(last_segments, this);
  KStatus s = builder.BuildAndFlush(thread_num);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("partition[%s] compact failed", path_.c_str());
    return s;
  }
  // 3. Set the compacted version.
  last_segment_mgr_.ClearLastSegments(last_segments.back()->GetVersion());
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::AppendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                                                TSSlice block_data, TSSlice block_agg, uint32_t row_num) {
  // generating new block item info ,and append to block segment.
  TsBlockSegmentBlockItem blk_item;
  blk_item.Info().entity_id = entity_id;
  blk_item.Info().table_version = table_version;
  blk_item.Info().row_count = row_num;
  // TODO(limeng04): block_item_info.max_ts & min_ts

  KStatus s = blk_segment_->AppendBlockData(&blk_item, block_data, block_agg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("insert into block segment of partition[%s] failed.", path_.c_str());
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::NewLastSegment(std::unique_ptr<TsLastSegment>* last_segment) {
  return last_segment_mgr_.NewLastSegment(last_segment);
}

void TsVGroupPartition::PublicLastSegment(std::unique_ptr<TsLastSegment>&& last_segment) {
  last_segment_mgr_.TakeLastSegmentOwnership(std::move(last_segment));
}

std::filesystem::path TsVGroupPartition::GetPath() const { return path_; }

std::string TsVGroupPartition::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "db%02d-%010ld", database_id_, start_);
  return buffer;
}

void TsVGroupPartition::compactRoutine(void* args) {
  while (!KWDBDynamicThreadPool::GetThreadPool().IsCancel() && enable_compact_thread_) {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    // Check every 10 seconds if compact is necessary
    cv_.wait_for(lock, std::chrono::seconds(5), [this] { return !enable_compact_thread_; });
    lock.unlock();
    // If the thread pool stops or the system is no longer running, exit the loop
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || !enable_compact_thread_) {
      break;
    }
    // Execute compact tasks
    if (last_segment_mgr_.NeedCompact()) {
      this->Compact();
    }
  }
}

void TsVGroupPartition::initCompactThread() {
  if (!enable_compact_thread_) {
    return;
  }
  KWDBOperatorInfo kwdb_operator_info;
  // Set the name and owner of the operation
  kwdb_operator_info.SetOperatorName("TsVGroupPartition::CompactThread");
  kwdb_operator_info.SetOperatorOwner("TsVGroupPartition");
  time_t now;
  // Record the start time of the operation
  kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
  // Start asynchronous thread
  compact_thread_id_ = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
    std::bind(&TsVGroupPartition::compactRoutine, this, std::placeholders::_1), this,
    &kwdb_operator_info);
  if (compact_thread_id_ < 1) {
    // If thread creation fails, record error message
    LOG_ERROR("TsVGroupPartition compact thread create failed");
  }
}

void TsVGroupPartition::closeCompactThread() {
  if (compact_thread_id_ > 0) {
    // Wake up potentially dormant compact threads
    cv_.notify_all();
    // Waiting for the compact thread to complete
    KWDBDynamicThreadPool::GetThreadPool().JoinThread(compact_thread_id_, 0);
  }
}

}  //  namespace kwdbts
