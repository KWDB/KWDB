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

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <utility>

#include "ts_block_segment.h"
#include "ts_vgroup_partition.h"
#include "ts_env.h"
#include "ts_lastsegment_manager.h"
#include "ts_metric_block.h"

namespace kwdbts {

TsVGroupPartition::TsVGroupPartition(std::filesystem::path root, int database_id, TsEngineSchemaManager* schema_mgr,
                                     int64_t start, int64_t end)
    : database_id_(database_id),
      schema_mgr_(schema_mgr),
      start_(start),
      end_(end),
      path_(root / GetFileName()),
      last_segment_mgr_(path_) {
  partition_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_MMAP_GROUP_PARTITION_RWLOCK);
}

TsVGroupPartition::~TsVGroupPartition() {}

KStatus TsVGroupPartition::Open() {
  std::filesystem::create_directories(path_);
  blk_segment_ = std::make_unique<TsBlockSegment>(path_);
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::Compact() {
  if (!last_segment_mgr_.NeedCompact()) {
    return KStatus::SUCCESS;
  }
  // 1. Get all the last segments that need to be compacted.
  std::vector<std::shared_ptr<TsLastSegment>> last_segments;
  last_segment_mgr_.GetCompactLastSegments(last_segments);
  if (last_segments.empty()) {
    return KStatus::SUCCESS;
  }
  // 2. Build the column block.
  TsBlockSegmentBuilder builder(last_segments, this);
  KStatus s = builder.BuildAndFlush();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("partition[%s] compact failed", path_.c_str());
    return s;
  }
  // 3. Set the compacted version.
  last_segment_mgr_.ClearLastSegments(last_segments.back()->GetVersion());
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::AppendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                                                uint32_t col_num, uint32_t row_num, timestamp64 max_ts, timestamp64 min_ts,
                                                TSSlice block_data, TSSlice block_agg) {
  // generating new block item info ,and append to block segment.
  TsBlockSegmentBlockItem blk_item;
  blk_item.entity_id = entity_id;
  blk_item.table_version = table_version;
  blk_item.n_cols = col_num;
  blk_item.n_rows = row_num;
  blk_item.max_ts = max_ts;
  blk_item.min_ts = min_ts;

  KStatus s = blk_segment_->AppendBlockData(blk_item, block_data, block_agg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("insert into block segment of partition[%s] failed.", path_.c_str());
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::NewLastSegmentFile(std::unique_ptr<TsFile>* last_segment,
                                              uint32_t* ver) {
  return last_segment_mgr_.NewLastSegmentFile(last_segment, ver);
}

void TsVGroupPartition::PublicLastSegment(uint32_t file_number) {
  last_segment_mgr_.OpenLastSegmentFile(file_number);
}

std::filesystem::path TsVGroupPartition::GetPath() const { return path_; }

std::string TsVGroupPartition::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "db%02d-%010ld", database_id_, start_);
  return buffer;
}

}  //  namespace kwdbts
