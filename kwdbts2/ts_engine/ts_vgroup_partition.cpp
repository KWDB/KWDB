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

#include "ts_block_segment.h"
#include "ts_vgroup_partition.h"
#include "ts_env.h"
#include "ts_metric_block.h"

namespace kwdbts {

TsVGroupPartition::TsVGroupPartition(std::filesystem::path root, int database_id, int64_t start, int64_t end)
    : database_id_(database_id),
      start_(start),
      end_(end),
      path_(root / GetFileName()),
      blk_segment_(std::make_unique<TsBlockSegment>(path_)),
      last_segments_(path_) {
  partition_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_MMAP_GROUP_PARTITION_RWLOCK);
}

TsVGroupPartition::~TsVGroupPartition() {}

KStatus TsVGroupPartition::Open() {
  // todo(liangbo01) load files.
  std::filesystem::create_directories(path_);
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::Compact(TSTableID table_id, TSEntityID entity_id, const std::vector<AttributeInfo>& schema,
                                   uint32_t table_version, uint32_t num, const std::vector<TsBlockColData>& col_datas) {
  TsMetricBlockBuilder builder(schema, num);
  TSSlice block_data = builder.Build(col_datas);
  if (block_data.len == 0) {
    LOG_ERROR("building column block failed.");
    return KStatus::FAIL;
  }
  Defer defer{[&]() { free(block_data.data); }};
  // todo(liangbo01) generate agg block info
  TSSlice block_agg;
  block_agg.data = reinterpret_cast<char*>(malloc(1));
  block_agg.len = 1;
  if (block_agg.len == 0) {
    LOG_ERROR("building column agg failed.");
    return KStatus::FAIL;
  }
  Defer defer1{[&]() { free(block_agg.data); }};

  KStatus s = appendToBlockSegment(table_id, entity_id, table_version, block_data, block_agg, num);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("column block append To Block Segment failed.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::appendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                                                TSSlice block_data, TSSlice block_agg, uint32_t row_num) {
  // generating new block item info ,and append to block segment.
  TsBlockItem blk_item;
  blk_item.Info().entity_id = entity_id;
  blk_item.Info().schema_version = table_version;
  blk_item.Info().row_count = row_num;
  // todo(limeng04): block_item_info.max_ts & min_ts

  KStatus s = blk_segment_->AppendBlockData(&blk_item, block_data, block_agg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("insert into block segment of partition[%s] failed.", path_.c_str());
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::FlushToLastSegment(const std::string &piece) {
  // todo add function.
  return KStatus::SUCCESS;
}

std::filesystem::path TsVGroupPartition::GetPath() const { return path_; }

std::string TsVGroupPartition::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "db%02d-%010ld", database_id_, start_);
  return buffer;
}

}  //  namespace kwdbts
