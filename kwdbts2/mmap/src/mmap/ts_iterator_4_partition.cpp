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

#include "mmap/ts_Iterator_4_partition.h"

// return -1 means partition tables scan over.
int TsPartitionIterator::nextBlockItem() {
  cur_block_item_ = nullptr;
  while (true) {
    if (block_item_queue_.empty()) {
      if (nextEntity() < 0) {
        return -1;
      }
      continue;
    }
    cur_block_item_ = block_item_queue_.front();
    block_item_queue_.pop_front();

    if (nullptr == cur_block_item_) {
      LOG_WARN("BlockItem[] error: No space has been allocated");
      continue;
    }
    return 0;
  }
}

KStatus TsPartitionIterator::Next(ResultSet* res, k_uint32* count, bool full_block, TsBlockFullData* block_data) {
  *count = 0;
  while (true) {
    if (cur_block_item_ == nullptr) {
      if (nextBlockItem() < 0) {
        // all scan over.
        *count = 0;
        return KStatus::SUCCESS;
      }
    }
    if (full_block) {
      *count = partition_->getMaxRowsInBlock(cur_block_item_);
      bool block_item_ignored = false;
      KStatus s = fillblockItemData(cur_block_item_, block_data, &block_item_ignored);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Can not get current block data [%d], in path [%s]",
                cur_block_item_->block_id, partition_->GetPath().c_str());
        return s;
      }
      block_data->rows = partition_->getMaxRowsInBlock(cur_block_item_);
      // the data query within the current BlockItem is completed, switch to the next block.
      cur_blockdata_offset_ = 1;
      cur_block_item_ = nullptr;
      if (!block_item_ignored) {
        *count = block_data->rows;
        res->entity_index = EntityResultIndex(params_.entity_group_id, params_.entity_ids[cur_entity_index_], params_.sub_group_id);
        LOG_DEBUG("res block id %d, entity id %u , count %d", block_data->block_item.block_id, block_data->block_item.entity_id, *count);
        return KStatus::SUCCESS;
      } else {
        // current blockitem data is all filtered by  ts span, so we need try again.
        continue;
      }
    } else {
      KStatus s = blockItemNext(res, count);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Can not get current block data [%d], in path [%s]",
                cur_block_item_->block_id, partition_->GetPath().c_str());
        return s;
      }
      // If the data query within the current BlockItem is completed, switch to the next block.
      if (cur_blockdata_offset_ > cur_block_item_->publish_row_count) {
        cur_blockdata_offset_ = 1;
        cur_block_item_ = nullptr;
      }
      if (*count > 0) {
        res->entity_index = EntityResultIndex(params_.entity_group_id, params_.entity_ids[cur_entity_index_], params_.sub_group_id);
        LOG_DEBUG("res entityId %d, hashpoint %d , count %d, startTime %ld, endTime %ld",
          res->entity_index.entityId,  res->entity_index.hash_point, *count , params_.ts_spans.data()->begin, params_.ts_spans.data()->end);
        return KStatus::SUCCESS;
      } else {
        res->clear();
      }
    }
  }
  LOG_ERROR("cannot run here.");
  return KStatus::FAIL;
}

KStatus TsPartitionIterator::fillblockItemData(BlockItem* block_item, TsBlockFullData* block_data, bool* ignored) {
  std::shared_ptr<MMapSegmentTable> segment_tbl = partition_->getSegmentTable(cur_block_item_->block_id);
  if (segment_tbl == nullptr) {
    LOG_ERROR("Can not find segment use block [%d], in path [%s]",
              cur_block_item_->block_id, partition_->GetPath().c_str());
    return FAIL;
  }
  if (nullptr == segment_iter_ || segment_iter_->segment_id() != segment_tbl->segment_id()) {
    if (segment_iter_ != nullptr) {
      delete segment_iter_;
      segment_iter_ = nullptr;
    }
    segment_iter_ = new MMapSegmentTableIterator(segment_tbl, params_.ts_spans, params_.kw_scan_cols,
                                                 params_.ts_scan_cols, params_.attrs);
  }
  timestamp64 block_min_ts, block_max_ts;
  TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
  TimestampCheckResult res = checkTimestampWithSpans(params_.ts_spans, block_max_ts, block_max_ts);
  if (res == kwdbts::TimestampCheckResult::NonOverlapping) {
    *ignored = true;
    return KStatus::SUCCESS;
  }
  KStatus s = segment_iter_->GetFullBlock(cur_block_item_, block_data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("fill block item data failed at get block data");
    return s;
  }
  *ignored = false;
  return KStatus::SUCCESS;
}

KStatus TsPartitionIterator::blockItemNext(ResultSet* res, k_uint32* count) {
  uint32_t first_row = 1;
  MetricRowID first_real_row = cur_block_item_->getRowID(first_row);
  std::shared_ptr<MMapSegmentTable> segment_tbl = partition_->getSegmentTable(cur_block_item_->block_id);
  if (segment_tbl == nullptr) {
    LOG_ERROR("Can not find segment use block [%d], in path [%s]",
              cur_block_item_->block_id, partition_->GetPath().c_str());
    return FAIL;
  }
  if (nullptr == segment_iter_ || segment_iter_->segment_id() != segment_tbl->segment_id()) {
    if (segment_iter_ != nullptr) {
      delete segment_iter_;
      segment_iter_ = nullptr;
    }
    segment_iter_ = new MMapSegmentTableIterator(segment_tbl, params_.ts_spans, params_.kw_scan_cols,
                                                 params_.ts_scan_cols, params_.attrs);
  }

  bool has_data = false;
  // Sequential read optimization, if the maximum and minimum timestamps of a BlockItem are within the ts_span range,
  // there is no need to determine the timestamps for each row data.
  timestamp64 block_min_ts, block_max_ts;
  TsTimePartition::GetBlkMinMaxTs(cur_block_item_, segment_tbl.get(), block_min_ts, block_max_ts);
  TimestampCheckResult ts_check_res = checkTimestampWithSpans(params_.ts_spans, block_min_ts, block_max_ts);
  if (cur_block_item_->publish_row_count > 0 &&
      cur_blockdata_offset_ == 1 &&
      cur_block_item_->publish_row_count == cur_block_item_->alloc_row_count &&
      ts_check_res == kwdbts::TimestampCheckResult::FullyContained) {
    k_uint32 cur_row = 1;
    while (cur_row <= cur_block_item_->alloc_row_count) {
      if (!segment_tbl->IsRowVaild(cur_block_item_, cur_row)) {
        break;
      }
      ++cur_row;
    }
    if (cur_row > cur_block_item_->publish_row_count) {
      cur_blockdata_offset_ = cur_row;
      *count = cur_block_item_->publish_row_count;
      first_row = 1;
      first_real_row = cur_block_item_->getRowID(first_row);
      has_data = true;
    }
  }
  // If the data has been queried through the sequential reading optimization process, assemble Batch and return it;
  // Otherwise, traverse the data within the current BlockItem one by one,
  // and return the maximum number of consecutive data that meets the condition.
  if (has_data) {
    segment_iter_->GetBatch(cur_block_item_, first_row, res, *count);
  } else {
    segment_iter_->Next(cur_block_item_, &cur_blockdata_offset_, res, count);
  }
  LOG_DEBUG("res entityId %d, hashpoint %d , count %d, startTime %ld, endTime %ld", res->entity_index.entityId,
            res->entity_index.hash_point, *count ,params_.ts_spans.data()->begin, params_.ts_spans.data()->end);

  return KStatus::SUCCESS;
}
