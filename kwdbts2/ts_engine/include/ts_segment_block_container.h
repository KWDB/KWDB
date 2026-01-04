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
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ts_entity_segment.h"

namespace kwdbts {

/**
 * TsSegmentBlockContainer is used to contain the blocks for a specific entity segment block file.
 */
class TsSegmentBlockContainer {
 protected:
  std::shared_ptr<TsSegmentFile> segment_file_;
  std::vector<std::shared_ptr<TsEntityBlock>> entity_blocks_;
  KRWLatch entity_blocks_rw_latch_;
  KRWLatch segment_file_rw_latch_;

 public:
  explicit TsSegmentBlockContainer(const std::shared_ptr<TsSegmentFile>& segment_file);

  // Only for LRU block cache unit tests
  explicit TsSegmentBlockContainer(uint32_t max_blocks);

  ~TsSegmentBlockContainer();

  // Upgrade segment file to latest version
  void UpgradeSegmentFile(const std::shared_ptr<TsSegmentFile>& segment_file);

  // Add a new entity block into container
  void AddEntityBlock(uint64_t block_id, std::shared_ptr<TsEntityBlock> block) {
    RW_LATCH_X_LOCK(&entity_blocks_rw_latch_);
    if (block_id > entity_blocks_.size()) {
      entity_blocks_.resize(block_id);
    }
    entity_blocks_[block_id - 1] = block;
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
  }

  // Remove a block with specific block id from container
  void RemoveEntityBlock(uint64_t block_id)  {
    RW_LATCH_X_LOCK(&entity_blocks_rw_latch_);
    entity_blocks_[block_id - 1] = nullptr;
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
  }

  // Get a block with specific block id in container
  std::shared_ptr<TsEntityBlock> GetEntityBlock(uint64_t block_id) {
    RW_LATCH_S_LOCK(&entity_blocks_rw_latch_);
    std::shared_ptr<TsEntityBlock> block;
    if (block_id > entity_blocks_.size()) {
      block = nullptr;
    } else {
      block = entity_blocks_[block_id - 1];
    }
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
    return block;
  }

  KStatus GetBlockData(TsEntityBlock* block, TsSliceGuard* data) {
    RW_LATCH_S_LOCK(&segment_file_rw_latch_);
    KStatus res = segment_file_->GetBlockData(block, data);
    RW_LATCH_UNLOCK(&segment_file_rw_latch_);
    return res;
  }

  KStatus GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>* metric_schema,
                            TsEntityBlock* block, TsScanStats* ts_scan_stats) {
    RW_LATCH_S_LOCK(&segment_file_rw_latch_);
    KStatus res = segment_file_->GetColumnBlock(col_idx, metric_schema, block, ts_scan_stats);
    RW_LATCH_UNLOCK(&segment_file_rw_latch_);
    return res;
  }

  KStatus GetAggData(TsEntityBlock *block, TsSliceGuard* data) {
    RW_LATCH_S_LOCK(&segment_file_rw_latch_);
    KStatus res = segment_file_->GetAggData(block, data);
    RW_LATCH_UNLOCK(&segment_file_rw_latch_);
    return res;
  }

  KStatus GetColumnAgg(int32_t col_idx, TsEntityBlock* block, TsScanStats* ts_scan_stats) {
    RW_LATCH_S_LOCK(&segment_file_rw_latch_);
    KStatus res = segment_file_->GetColumnAgg(col_idx, block, ts_scan_stats);
    RW_LATCH_UNLOCK(&segment_file_rw_latch_);
    return res;
  }

  const EntitySegmentMetaInfo& GetHandleInfo();

  std::string GetPath();

  std::string GetHandleInfoStr();
};
}  // namespace kwdbts
