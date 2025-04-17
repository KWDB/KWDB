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

#include <utility>
#include "engine.h"
#include "iterator.h"
#include "perf_stat.h"

enum NextBlkStatus {
  find_one = 1,
  scan_over = 2,
  error = 3
};


namespace kwdbts {

Batch* CreateAggBatch(void* mem, const std::shared_ptr<MMapSegmentTable>& segment_table) {
  if (mem) {
    return new AggBatch(mem, 1, segment_table);
  } else {
    return new AggBatch(nullptr, 0, segment_table);
  }
}

Batch* CreateAggBatch(const std::shared_ptr<void>& mem, const std::shared_ptr<MMapSegmentTable>& segment_table) {
  if (mem) {
    return new AggBatch(mem, 1, segment_table);
  } else {
    return new AggBatch(nullptr, 0, segment_table);
  }
}

// Agreement between storage layer and execution layer:
// 1. The SUM aggregation result of integer type returns the int64 type uniformly without overflow;
//    In case of overflow, return double type
// 2. The return type for floating-point numbers is double
// This function is used for type conversion of SUM aggregation results.
bool ChangeSumType(DATATYPE type, void* base, void** new_base) {
  if (type != DATATYPE::INT8 && type != DATATYPE::INT16 && type != DATATYPE::INT32 && type != DATATYPE::FLOAT) {
    *new_base = base;
    return false;
  }
  void* sum_base = malloc(8);
  memset(sum_base, 0, 8);
  switch (type) {
    case DATATYPE::INT8:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int8_t*>(base));
      break;
    case DATATYPE::INT16:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int16_t*>(base));
      break;
    case DATATYPE::INT32:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int32_t*>(base));
      break;
    case DATATYPE::FLOAT:
      *(static_cast<double*>(sum_base)) = *(static_cast<float*>(base));
  }
  *new_base = sum_base;
  return true;
}

TsStorageIterator::TsStorageIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id,
                                     uint32_t subgroup_id, vector<uint32_t>& entity_ids,
                                     std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                     std::vector<uint32_t>& kw_scan_cols, std::vector<uint32_t>& ts_scan_cols,
                                     uint32_t table_version)
                       : entity_group_id_(entity_group_id),
                         subgroup_id_(subgroup_id),
                         entity_ids_(entity_ids),
                         ts_spans_(ts_spans),
                         ts_col_type_(ts_col_type),
                         kw_scan_cols_(kw_scan_cols),
                         ts_scan_cols_(ts_scan_cols),
                         table_version_(table_version),
                         entity_group_(entity_group) {
  entity_group_->RdDropLock();
}

TsStorageIterator::~TsStorageIterator() {
  if (segment_iter_ != nullptr) {
    delete segment_iter_;
    segment_iter_ = nullptr;
  }
  entity_group_->DropUnlock();
}

void TsStorageIterator::fetchBlockItems(k_uint32 entity_id) {
  cur_partition_table_->GetAllBlockItems(entity_id, block_item_queue_, is_reversed_);
}

KStatus TsStorageIterator::Init(bool is_reversed) {
  is_reversed_ = is_reversed;
  auto sub_grp_mgr = entity_group_->GetSubEntityGroupManager();
  if (sub_grp_mgr == nullptr) {
    LOG_ERROR("can not found sub entitygroup manager for entitygroup [%lu].", entity_group_id_);
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  auto sub_grp = sub_grp_mgr->GetSubGroup(subgroup_id_, err_info);
  if (!err_info.isOK()) {
    LOG_ERROR("can not found sub entitygroup for entitygroup [%lu], err_msg: %s.",
              entity_group_id_, err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  partition_table_iter_ = sub_grp->GetPTIterator(ts_spans_);
  partition_table_iter_->Reset(is_reversed_);
  return entity_group_->GetRootTableManager()->GetSchemaInfoIncludeDropped(&attrs_, table_version_);
}

// return -1 means all partition tables scan over.
int TsStorageIterator::nextBlockItem(k_uint32 entity_id, timestamp64 ts) {
  cur_block_item_ = nullptr;
  // block_item_queue_ saves the BlockItem object pointer of the partition table for the current query
  // Calling the Next function once can retrieve data within a maximum of one BlockItem.
  // If a BlockItem query is completed, the next BlockItem object needs to be obtained:
  // 1. If there are still BlockItems that have not been queried in block_item_queue_, obtain them in block_item_queue_
  // 2. If there are no more BlockItems that have not been queried in block_item_queue_,
  //    switch the partition table to retrieve all block_item_queue_ in the next partition table, and then proceed to step 1
  // 3. If all partition tables have been queried, return -1
  while (true) {
    if (block_item_queue_.empty()) {
      if (segment_iter_ != nullptr) {
        delete segment_iter_;
        segment_iter_ = nullptr;
      }
      KStatus s = partition_table_iter_->Next(&cur_partition_table_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("failed get next partition at entityid[%u] entitygroup [%lu]", entity_id, entity_group_id_);
        return NextBlkStatus::error;
      }
      if (cur_partition_table_ == nullptr) {
        // all partition table scan over.
        return NextBlkStatus::scan_over;
      }
      fetchBlockItems(entity_id);
      continue;
    }
    cur_block_item_ = block_item_queue_.front();
    block_item_queue_.pop_front();
    if (ts != INVALID_TS) {
      std::shared_ptr<MMapSegmentTable> segment_tbl = cur_partition_table_->getSegmentTable(cur_block_item_->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]",
                  cur_block_item_->block_id, cur_partition_table_->GetPath().c_str());
        return NextBlkStatus::error;
      }
      timestamp64 min_ts, max_ts;
      TsTimePartition::GetBlkMinMaxTs(cur_block_item_, segment_tbl.get(), min_ts, max_ts);
      if ((is_reversed_ && max_ts < ts) || (!is_reversed_ && min_ts > ts)) {
        cur_block_item_ = nullptr;
        continue;
      }
    }

    if (cur_block_item_ == nullptr) {
      LOG_WARN("BlockItem[] error: No space has been allocated");
      continue;
    }
    // all rows in block is deleted.
    if (cur_block_item_->isBlockEmpty()) {
      cur_block_item_ = nullptr;
      continue;
    }
    return NextBlkStatus::find_one;
  }
}

bool TsStorageIterator::getCurBlockSpan(BlockItem* cur_block, std::shared_ptr<MMapSegmentTable>& segment_tbl,
                                        uint32_t* first_row, uint32_t* count) {
  bool has_data = false;
  *count = 0;
  // Sequential read optimization, if the maximum and minimum timestamps of a BlockItem are within the ts_span range,
  // there is no need to determine the timestamps for each BlockItem.
  timestamp64 blk_min_ts, blk_max_ts;
  TsTimePartition::GetBlkMinMaxTs(cur_block, segment_tbl.get(), blk_min_ts, blk_max_ts);
  if (cur_block->is_agg_res_available && cur_block->publish_row_count > 0
      && cur_block->publish_row_count == cur_block->alloc_row_count
      && cur_blockdata_offset_ == 1
      && cur_block->getDeletedCount() == 0
      && isTimestampWithinSpans(ts_spans_, blk_min_ts, blk_max_ts)) {
    has_data = true;
    *first_row = 1;
    *count = cur_block->publish_row_count;
    cur_blockdata_offset_ = *first_row + *count;
  }
  // If it is not achieved sequential reading optimization process,
  // the data under the BlockItem will be traversed one by one,
  // and the maximum number of consecutive data that meets the query conditions will be obtained.
  // The aggregation result of this continuous data will be further obtained in the future.
  while (cur_blockdata_offset_ <= cur_block->alloc_row_count) {
    bool is_deleted = !segment_tbl->IsRowVaild(cur_block, cur_blockdata_offset_);
    // If the data in the *blk_offset row is not within the ts_span range or has been deleted,
    // continue to verify the data in the next row.
    timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddrByBlk(cur_block->block_id, cur_blockdata_offset_ - 1, 0));
    if (is_deleted || !checkIfTsInSpan(cur_ts)) {
      ++cur_blockdata_offset_;
      if (has_data) {
        break;
      }
      continue;
    }

    if (!has_data) {
      has_data = true;
      *first_row = cur_blockdata_offset_;
    }
    ++(*count);
    ++cur_blockdata_offset_;
  }
  return has_data;
}

KStatus TsRawDataIterator::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().it_next);
  *count = 0;
  *is_finished = false;
  if (cur_entity_idx_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  while (true) {
    // If cur_block_item_ is a null pointer and attempts to call nextBlockItem to retrieve a new BlockItem for querying:
    // 1. nextBlockItem ended normally, query cur_block_item_
    // 2. If nextBlockItem returns -1, it indicates that all data on the current entity has been queried and
    //    needs to be switched to the next entity before attempting to retrieve cur_block_item_
    // 3. If all entities have been queried, the current query process ends and returns directly
    if (!cur_block_item_) {
      auto ret = nextBlockItem(entity_ids_[cur_entity_idx_], ts);
      if (ret == NextBlkStatus::scan_over) {
        if (++cur_entity_idx_ >= entity_ids_.size()) {
          *is_finished = true;
          return KStatus::SUCCESS;
        }
        // current entity scan over, we need scan next entity in same subgroup, and scan same partiton tables.
        partition_table_iter_->Reset(is_reversed_);
        // current entity scan is over for now, there is no rush to move on to scan next entity,
        // we need to return and let the table scan to move on to the next entity, and then come back in
        // to scan the data of next entity
        return KStatus::SUCCESS;
      } else if (ret == NextBlkStatus::error) {
        LOG_ERROR("can not get next block item.");
        return KStatus::FAIL;
      }
      continue;
    }
    TsTimePartition* cur_pt = cur_partition_table_;
    if (ts != INVALID_TS) {
      if (!is_reversed_ && convertSecondToPrecisionTS(cur_pt->minTimestamp(), ts_col_type_) > ts) {
        // At this time, if no data smaller than ts exists, -1 is returned directly, and the query is ended
        nextEntity();
        return SUCCESS;
      } else if (is_reversed_ && convertSecondToPrecisionTS(cur_pt->maxTimestamp(), ts_col_type_) < ts) {
        // In this case, if no data larger than ts exists, -1 is returned and the query is completed
        nextEntity();
        return SUCCESS;
      }
    }
    uint32_t first_row = 1;
    MetricRowID first_real_row = cur_block_item_->getRowID(first_row);

    std::shared_ptr<MMapSegmentTable> segment_tbl = cur_partition_table_->getSegmentTable(cur_block_item_->block_id);
    if (segment_tbl == nullptr) {
      LOG_ERROR("Can not find segment use block [%u], in path [%s]",
                cur_block_item_->block_id, cur_partition_table_->GetPath().c_str());
      return FAIL;
    }
    if (segment_tbl->schemaVersion() > table_version_) {
      cur_blockdata_offset_ = 1;
      nextBlockItem(entity_ids_[cur_entity_idx_], ts);
      continue;
    }
    if (nullptr == segment_iter_ || segment_iter_->segment_id() != segment_tbl->segment_id()) {
      if (segment_iter_ != nullptr) {
        delete segment_iter_;
        segment_iter_ = nullptr;
      }
      segment_iter_ = new MMapSegmentTableIterator(segment_tbl, ts_spans_, kw_scan_cols_, ts_scan_cols_, attrs_);
    }

    bool has_data = getCurBlockSpan(cur_block_item_, segment_tbl, &first_row, count);
    // If the data has been queried through the sequential reading optimization process, assemble Batch and return it;
    // Otherwise, traverse the data within the current BlockItem one by one,
    // and return the maximum number of consecutive data that meets the condition.
    if (has_data) {
      auto s = segment_iter_->GetBatch(cur_block_item_, first_row, res, *count);
      if (s != KStatus::SUCCESS) {
        return s;
      }
    }
    LOG_DEBUG("block item[%u, %u] scan rows %u.", cur_block_item_->entity_id, cur_block_item_->block_id, *count);
    // If the data query within the current BlockItem is completed, switch to the next block.
    if (cur_blockdata_offset_ > cur_block_item_->publish_row_count) {
      cur_blockdata_offset_ = 1;
      cur_block_item_ = nullptr;
    }
    if (*count > 0) {
      KWDB_STAT_ADD(StStatistics::Get().it_num, *count);
      LOG_DEBUG("res entityId is %u, entitygrp is %lu , count is %u, startTime is %ld, endTime is %ld",
        entity_ids_[cur_entity_idx_], entity_group_id_, *count, ts_spans_.data()->begin, ts_spans_.data()->end);
      res->entity_index = {entity_group_id_, entity_ids_[cur_entity_idx_], subgroup_id_};
      return SUCCESS;
    }
  }
  KWDB_STAT_ADD(StStatistics::Get().it_num, *count);
  return SUCCESS;
}

void TsFirstLastRow::Reset() {
  delObjects();
  no_first_last_type_ = true;
  first_pairs_.assign(scan_agg_types_.size(), {});
  last_pairs_.assign(scan_agg_types_.size(), {});
  first_agg_valid_ = 0;
  last_agg_valid_ = 0;
  for (size_t i = 0; i < scan_agg_types_.size(); i++) {
    if (scan_agg_types_[i] == Sumfunctype::FIRST || scan_agg_types_[i] == Sumfunctype::FIRSTTS) {
      no_first_last_type_ = false;
    } else if (scan_agg_types_[i] == Sumfunctype::FIRST_ROW || scan_agg_types_[i] == Sumfunctype::FIRSTROWTS) {
      no_first_last_type_ = false;
    } else if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
      no_first_last_type_ = false;
    } else if (scan_agg_types_[i] == Sumfunctype::LAST_ROW || scan_agg_types_[i] == Sumfunctype::LASTROWTS) {
      no_first_last_type_ = false;
    }
    if (!TsStorageIterator::IsFirstAggType(scan_agg_types_[i])) {
      first_agg_valid_ += 1;
    }
    if (!TsStorageIterator::IsLastTsAggType(scan_agg_types_[i])) {
      last_agg_valid_ += 1;
    }
    first_pairs_[i].first = i;
    first_pairs_[i].second.partion_tbl = nullptr;
    first_pairs_[i].second.segment_tbl.reset();
    first_pairs_[i].second.row_ts = INVALID_TS;
    last_pairs_[i].first = i;
    last_pairs_[i].second.partion_tbl = nullptr;
    last_pairs_[i].second.segment_tbl.reset();
    last_pairs_[i].second.row_ts = INVALID_TS;
  }
  first_row_pair_.partion_tbl = nullptr;
  first_row_pair_.segment_tbl.reset();
  first_row_pair_.row_ts = INVALID_TS;
  first_row_pair_.is_null.assign(scan_agg_types_.size(), true);
  last_row_pair_.partion_tbl = nullptr;
  last_row_pair_.segment_tbl.reset();
  last_row_pair_.row_ts = INVALID_TS;
  last_row_pair_.is_null.assign(scan_agg_types_.size(), true);
}

KStatus TsFirstLastRow::UpdateFirstRow(timestamp64 ts, MetricRowID row_id, TsTimePartition* partiton_table,
                                      std::shared_ptr<MMapSegmentTable>& segment_tbl,
                                      ColBlockBitmaps& col_bitmap) {
  bool ts_using = false;
  if (FirstAggRowValid() && first_max_ts_ != INVALID_TS && first_max_ts_ <= ts) {
    return KStatus::SUCCESS;
  }
  for (int i = 0; i < first_pairs_.size(); ++i) {
    if (!TsStorageIterator::IsFirstAggType(scan_agg_types_[i])) {
      continue;
    }
    timestamp64 first_ts = first_pairs_[i].second.row_ts;
    // If the timestamp corresponding to the data in this row is less than the first value of the record and
    // is non-empty, update it.
    if ((first_ts == INVALID_TS || first_ts > ts) && !col_bitmap.IsColNull(i, row_id.offset_row)) {
      if (first_pairs_[i].second.partion_tbl == nullptr) {
        partiton_table->incRefCount();
      } else if (first_pairs_[i].second.partion_tbl != partiton_table) {
        partiton_table->incRefCount();
        ReleaseTable(first_pairs_[i].second.partion_tbl);
      }
      first_pairs_[i].second = std::move(TsRowTableInfo{partiton_table, segment_tbl, ts, row_id});
      if (first_ts == INVALID_TS)
        first_agg_valid_++;
      ts_using = true;
    }
  }
  timestamp64 first_row_ts = first_row_pair_.row_ts;
  // If the timestamp corresponding to the data in this row is less than the first record, update it.
  if ((first_row_ts == INVALID_TS || first_row_ts > ts)) {
    if (first_row_pair_.partion_tbl == nullptr) {
      partiton_table->incRefCount();
    } else if (first_row_pair_.partion_tbl != partiton_table) {
      partiton_table->incRefCount();
      ReleaseTable(first_row_pair_.partion_tbl);
    }
    vector<bool> is_null;
    for (int i = 0; i < scan_agg_types_.size(); ++i) {
      is_null.emplace_back(col_bitmap.IsColNull(i, row_id.offset_row));
    }
    first_row_pair_ = std::move(TsRowTableInfo{partiton_table, segment_tbl, ts, row_id, is_null});
    if (first_row_ts == INVALID_TS) {
      first_agg_valid_++;
    }
    ts_using = true;
  }
  if (ts_using && (first_max_ts_ == INVALID_TS || first_max_ts_ < ts)) {
    first_max_ts_ = ts;
  }
  return KStatus::SUCCESS;
}

KStatus TsFirstLastRow::UpdateLastRow(timestamp64 ts, MetricRowID row_id,
                          TsTimePartition* partiton_table,
                          std::shared_ptr<MMapSegmentTable>& segment_tbl,
                          ColBlockBitmaps& col_bitmap) {
  bool ts_using = false;
  if (LastAggRowValid() && last_min_ts_ != INVALID_TS && last_min_ts_ >= ts) {
    return KStatus::SUCCESS;
  }
  for (int i = 0; i < last_pairs_.size(); ++i) {
    if (!TsStorageIterator::IsLastTsAggType(scan_agg_types_[i])) {
      continue;
    }
    timestamp64 last_ts = last_pairs_[i].second.row_ts;
    // If the timestamp corresponding to the data in this row is greater than the last value of the record and
    // is non-empty, update it.
    if ((last_ts_points_.empty() || last_ts_points_[i] == INVALID_TS || ts <= last_ts_points_[i]) &&
        (last_ts == INVALID_TS || last_ts < ts) && !col_bitmap.IsColNull(i, row_id.offset_row)) {
      if (last_pairs_[i].second.partion_tbl == nullptr) {
        partiton_table->incRefCount();
      } else if (last_pairs_[i].second.partion_tbl != partiton_table) {
        partiton_table->incRefCount();
        ReleaseTable(last_pairs_[i].second.partion_tbl);
      }
      last_pairs_[i].second = std::move(TsRowTableInfo{partiton_table, segment_tbl, ts, row_id});
      if (last_ts == INVALID_TS) {
        last_agg_valid_++;
      }
      ts_using = true;
    }
  }
  timestamp64 last_row_ts = last_row_pair_.row_ts;
  // If the timestamp corresponding to the data in this row is greater than the last record, update it.
  if ((last_row_ts == INVALID_TS || last_row_ts < ts)) {
    if (last_row_pair_.partion_tbl == nullptr) {
      partiton_table->incRefCount();
    } else if (last_row_pair_.partion_tbl  != partiton_table) {
      partiton_table->incRefCount();
      ReleaseTable(last_row_pair_.partion_tbl);
    }
    vector<bool> is_null;
    for (int i = 0; i < scan_agg_types_.size(); ++i) {
      is_null.emplace_back(col_bitmap.IsColNull(i, row_id.offset_row));
    }
    last_row_pair_ = std::move(TsRowTableInfo{partiton_table, segment_tbl, ts, row_id, is_null});
    if (last_row_ts == INVALID_TS)
      last_agg_valid_++;
    ts_using = true;
  }
  if (ts_using && (last_min_ts_ == INVALID_TS || last_min_ts_ > ts)) {
    last_min_ts_ = ts;
  }
  return KStatus::SUCCESS;
}

int TsFirstLastRow::getActualColAggBatch(TsTimePartition* p_bt, shared_ptr<MMapSegmentTable>& segment_tbl,
                                        MetricRowID real_row, uint32_t ts_col,
                                        const AttributeInfo& attr, Batch** b) {
  uint32_t actual_col_type = segment_tbl->GetColInfo(ts_col).type;
  bool is_var_type = actual_col_type == VARSTRING || actual_col_type == VARBINARY;
  // Encapsulation Batch Result:
  // 1. If a column type conversion occurs, it is necessary to convert the data in the real_row
  //    and write the original data into the newly applied space
  // 2. If no column type conversion occurs, directly read the original data stored in the file
  if (actual_col_type != attr.type) {
    void* old_mem = nullptr;
    std::shared_ptr<void> old_var_mem = nullptr;
    if (!is_var_type) {
      old_mem = segment_tbl->columnAddr(real_row, ts_col);
    } else {
      old_var_mem = segment_tbl->varColumnAddr(real_row, ts_col);
    }
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    int err_code = p_bt->ConvertDataTypeToMem(static_cast<DATATYPE>(actual_col_type),
                                              static_cast<DATATYPE>(attr.type),
                                              attr.size, old_mem, old_var_mem, &new_mem);
    if (err_code < 0) {
      LOG_ERROR("failed ConvertDataType from %u to %u", actual_col_type, attr.type);
      return FAIL;
    }
    *b = new AggBatch(new_mem, 1, segment_tbl);
  } else {
    if (!is_var_type) {
      *b = new AggBatch(segment_tbl->columnAddr(real_row, ts_col), 1, segment_tbl);
    } else {
      *b = new AggBatch(segment_tbl->varColumnAddr(real_row, ts_col), 1, segment_tbl);
    }
  }
  return 0;
}

KStatus TsFirstLastRow::GetAggBatch(TsAggIterator* iter, u_int32_t col_idx, size_t agg_idx,
                                    const AttributeInfo& col_attr, Batch** agg_batch) {
  switch (scan_agg_types_[agg_idx]) {
    case FIRST: {
      // Read the first_pairs_ result recorded during the traversal process.
      // If not found, return nullptr. Otherwise, obtain the data address based on the partition table index and row id.
      auto& first_row_info = first_pairs_[agg_idx];
      auto p_table = first_row_info.second.partion_tbl;
      auto& seg_table = first_row_info.second.segment_tbl;
      if (p_table == nullptr) {
        *agg_batch = CreateAggBatch(nullptr, nullptr);
      } else {
        int err_code = getActualColAggBatch(p_table, seg_table, first_row_info.second.row_id,
                                            ts_scan_cols_[agg_idx], col_attr, agg_batch);
        if (err_code < 0) {
          LOG_ERROR("getActualColBatch failed.");
          return FAIL;
        }
      }
      break;
    }
    case FIRSTTS: {
      auto& first_row_info = first_pairs_[agg_idx];
      auto p_table = first_row_info.second.partion_tbl;
      auto& segment_tbl = first_row_info.second.segment_tbl;
      if (p_table == nullptr) {
        *agg_batch = new AggBatch(nullptr, 0, nullptr);
      } else {
        // only get the first timestamp column, no need check weather type changed.
        *agg_batch = new AggBatch(segment_tbl->columnAddr(first_row_info.second.row_id, 0), 1, segment_tbl);
      }
      break;
    }
    case FIRST_ROW: {
      // Read the first_row_pairs_ result recorded during the traversal process.
      // If not found, return nullptr. Otherwise, obtain the data address based on the partition table index and row id.
      TsTimePartition* cur_pt = first_row_pair_.partion_tbl;
      auto& segment_tbl = first_row_pair_.segment_tbl;
      if (cur_pt == nullptr) {
        *agg_batch = CreateAggBatch(nullptr, nullptr);
      } else {
        MetricRowID real_row = first_row_pair_.row_id;
        timestamp64 first_row_ts = first_row_pair_.row_ts;
        if (segment_tbl->isColExist(col_idx)) {
          if (first_row_pair_.is_null[agg_idx]) {
            std::shared_ptr<void> first_row_data(nullptr);
            *agg_batch = new AggBatch(first_row_data, 1, nullptr);
          } else {
            int err_code = getActualColAggBatch(cur_pt, segment_tbl, real_row, ts_scan_cols_[agg_idx], col_attr, agg_batch);
            if (err_code < 0) {
              LOG_ERROR("getActualColBatch failed.");
              return FAIL;
            }
          }
        } else {
          *agg_batch = CreateAggBatch(nullptr, nullptr);
        }
      }
      break;
    }
    case FIRSTROWTS: {
      TsTimePartition* cur_pt = first_row_pair_.partion_tbl;
      auto& segment_tbl = first_row_pair_.segment_tbl;
      if (cur_pt == nullptr) {
        *agg_batch = new AggBatch(nullptr, 0, nullptr);
      } else {
        MetricRowID& real_row = first_row_pair_.row_id;
        *agg_batch = new AggBatch(segment_tbl->columnAddr(real_row, 0), 1, segment_tbl);
      }
      break;
    }
    case Sumfunctype::LAST: {
        KWDB_DURATION(StStatistics::Get().agg_last);
        auto& last_row_info = last_pairs_[agg_idx];
        TsTimePartition* cur_pt = last_row_info.second.partion_tbl;
        auto& segment_tbl = last_row_info.second.segment_tbl;
        // Read the last_pairs_ result recorded during the traversal process.
        // If not found, return nullptr. Otherwise, obtain the data address based on the partition table index and row id.
        if (cur_pt == nullptr) {
          *agg_batch = CreateAggBatch(nullptr, nullptr);
        } else {
          MetricRowID real_row = last_row_info.second.row_id;
          timestamp64 last_ts = last_row_info.second.row_ts;
          int err_code = getActualColAggBatch(cur_pt, segment_tbl, real_row, ts_scan_cols_[agg_idx], col_attr, agg_batch);
          if (err_code < 0) {
            LOG_ERROR("getActualColBatch failed.");
            return KStatus::FAIL;
          }
        }
        break;
      }
      case Sumfunctype::LAST_ROW: {
        KWDB_DURATION(StStatistics::Get().agg_lastrow);
        TsTimePartition* cur_pt = last_row_pair_.partion_tbl;
        auto& segment_tbl = last_row_pair_.segment_tbl;
        // Read the last_row_pair_ result recorded during the traversal process.
        // If not found, return nullptr. Otherwise, obtain the data address based on the partition table index and row id.
        if (cur_pt == nullptr) {
          *agg_batch = CreateAggBatch(nullptr, nullptr);
        } else {
          MetricRowID real_row = last_row_pair_.row_id;
          timestamp64 last_row_ts = last_row_pair_.row_ts;
          if (segment_tbl->isColExist(col_idx)) {
            if (last_row_pair_.is_null[agg_idx]) {
              std::shared_ptr<void> last_row_data(nullptr);
              *agg_batch = new AggBatch(last_row_data, 1, nullptr);
            } else {
              int err_code = getActualColAggBatch(cur_pt, segment_tbl, real_row, ts_scan_cols_[agg_idx],
                                                  col_attr, agg_batch);
              if (err_code < 0) {
                LOG_ERROR("getActualColBatch failed.");
                return FAIL;
              }
            }
          } else {
            *agg_batch = CreateAggBatch(nullptr, nullptr);
          }
        }
        break;
      }
      case Sumfunctype::LASTTS: {
        KWDB_DURATION(StStatistics::Get().agg_lastts);
        auto& last_row_info = last_pairs_[agg_idx];
        TsTimePartition* cur_pt = last_row_info.second.partion_tbl;
        auto& segment_tbl = last_row_info.second.segment_tbl;
        if (cur_pt == nullptr) {
          *agg_batch = new AggBatch(nullptr, 0, nullptr);
        } else {
          *agg_batch = new AggBatch(segment_tbl->columnAddr(last_row_info.second.row_id, 0), 1, segment_tbl);
        }
        break;
      }
      case Sumfunctype::LASTROWTS: {
        TsTimePartition* cur_pt = last_row_pair_.partion_tbl;
        auto& segment_tbl = last_row_pair_.segment_tbl;
        if (cur_pt == nullptr) {
          *agg_batch = new AggBatch(nullptr, 0, nullptr);
        } else {
          *agg_batch = new AggBatch(segment_tbl->columnAddr(last_row_pair_.row_id, 0), 1, segment_tbl);
        }
        break;
      }
    default:
      LOG_DEBUG("no need parse agg type [%d] in agg batch.", scan_agg_types_[agg_idx]);
      break;
  }
  return KStatus::SUCCESS;
}

// first/first_row aggregate type query optimization function:
// Starting from the first data entry of the first BlockItem in the smallest partition table, if the timestamp of the
// data being queried is equal to min_ts recorded in the EntityItem,
// it can be confirmed that the data record with the smallest timestamp has been queried.
// Due to the fact that most temporal data is written in sequence, this approach is likely to quickly find the data
// with the smallest timestamp and avoid subsequent invalid data traversal, accelerating the query time
// for first/first_row types.
KStatus TsAggIterator::findFirstDataByIter(timestamp64 ts) {
  if (hasFoundFirstAggData()) {
    return KStatus::SUCCESS;
  }
  partition_table_iter_->Reset();
  block_item_queue_.clear();
  cur_block_item_ = nullptr;
  while (true) {
    TsTimePartition* cur_pt = nullptr;
    partition_table_iter_->Next(&cur_pt);
    block_item_queue_.clear();
    if (cur_pt == nullptr) {
      // all partition scan over.
      break;
    }
    if (ts != INVALID_TS) {
      if (!is_reversed_ && convertSecondToPrecisionTS(cur_pt->minTimestamp(), ts_col_type_) > ts) {
        break;
      } else if (is_reversed_ && convertSecondToPrecisionTS(cur_pt->maxTimestamp(), ts_col_type_) < ts) {
        break;
      }
    }

    cur_pt->GetAllBlockItems(entity_ids_[cur_entity_idx_], block_item_queue_);
    auto entity_item = cur_pt->getEntityItem(entity_ids_[cur_entity_idx_]);
    // Obtain the minimum timestamp for the current query entity.
    // Once a record's timestamp is traversed to be equal to it,
    // it indicates that the query result has been found and no additional data needs to be traversed.
    timestamp64 min_entity_ts = entity_item->min_ts;
    while (!block_item_queue_.empty()) {
      BlockItem* block_item = block_item_queue_.front();
      cur_block_item_ = block_item;
      block_item_queue_.pop_front();
      if (!block_item || !block_item->publish_row_count) {
        continue;
      }
      // all rows in block is deleted.
      if (block_item->isBlockEmpty()) {
        continue;
      }
      std::shared_ptr<MMapSegmentTable> segment_tbl = cur_pt->getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, cur_pt->GetPath().c_str());
        return KStatus::FAIL;
      }
      timestamp64 min_ts, max_ts;
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
      // If the time range of the BlockItem is not within the ts_span range, continue traversing the next BlockItem.
      if (!isTimestampInSpans(ts_spans_, min_ts, max_ts)) {
        continue;
      }
      // first agg all filled. while just need ts that smaller than agg using max ts.
      if ((hasFoundFirstAggData()) && first_last_row_.GetFirstMaxTs() <= min_ts) {
        continue;
      }
      bool has_found = false;
      // save bitmap for all blocks, the first map key is col index
      if (getBlockBitmap(segment_tbl, block_item, 1) != KStatus::SUCCESS) {
        LOG_ERROR("getBlockBitmap failed.");
        return KStatus::FAIL;
      }
      // If there is no first_row type query, it can be skipped directly when all data in the current block is null.
      if (no_first_row_type_) {
        bool all_deleted = true;
        for (int i = 0; i < scan_agg_types_.size(); ++i) {
          if (IsFirstAggType(scan_agg_types_[i]) && !col_blk_bitmaps_.IsColAllNull(i, block_item->publish_row_count)) {
            all_deleted = false;
            break;
          }
        }
        if (all_deleted) {
          continue;
        }
      }
      // Traverse all data of this BlockItem
      uint32_t cur_row_offset = 1;
      while (cur_row_offset <= block_item->publish_row_count) {
        bool is_deleted = !segment_tbl->IsRowVaild(block_item, cur_row_offset);
        // If the data in the cur_row_offset row is not within the ts_span range or has been deleted,
        // continue to verify the data in the next row.
        MetricRowID real_row = block_item->getRowID(cur_row_offset);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(real_row, 0));
        if (is_deleted || !checkIfTsInSpan(cur_ts)) {
          ++cur_row_offset;
          continue;
        }
        // Update variables that record the query results of first/first_row
        first_last_row_.UpdateFirstRow(cur_ts, real_row, cur_pt, segment_tbl, col_blk_bitmaps_);
        // If all queried columns and their corresponding query types already have results,
        // and the timestamp of the updated data is equal to the minimum timestamp of the entity, then the query can end.
        if ((hasFoundFirstAggData()) && cur_ts == min_entity_ts) {
          has_found = true;
          break;
        }
        ++cur_row_offset;
      }
      if (has_found) {
        break;
      }
    }
    if (hasFoundFirstAggData()) {
      break;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIterator::findFirstData(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_first);
  KStatus s = findFirstDataByIter(ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("find last data failed.");
    return s;
  }
  return genBatchData(res, count);
}

// last/last_row aggregate type query optimization function:
// Starting from the last data entry of the last BlockItem in the largest partition table,
// if the timestamp of the data being queried is equal to the max_ts recorded in the EntityItem,
// it can be confirmed that the data record with the largest timestamp has been queried.
// Due to the fact that most temporal data is written in sequence,
// this approach significantly improves query speed compared to traversing from the head.
KStatus TsAggIterator::findLastDataByIter(timestamp64 ts) {
  if (hasFoundLastAggData()) {
    return KStatus::SUCCESS;
  }
  partition_table_iter_->Reset(true);
  block_item_queue_.clear();
  cur_block_item_ = nullptr;
  while (true) {
    TsTimePartition* cur_pt = nullptr;
    partition_table_iter_->Next(&cur_pt);
    block_item_queue_.clear();
    if (cur_pt == nullptr) {
      // all partition scan over.
      break;
    }
    if (ts != INVALID_TS) {
      if (!is_reversed_ && convertSecondToPrecisionTS(cur_pt->minTimestamp(), ts_col_type_) > ts) {
        break;
      } else if (is_reversed_ && convertSecondToPrecisionTS(cur_pt->maxTimestamp(), ts_col_type_) < ts) {
        break;
      }
    }

    cur_pt->GetAllBlockItems(entity_ids_[cur_entity_idx_], block_item_queue_);
    auto entity_item = cur_pt->getEntityItem(entity_ids_[cur_entity_idx_]);
    // Obtain the maximum timestamp of the current query entity.
    // Once a record's timestamp is traversed to be equal to it,
    // it indicates that the query result has been found and no additional data needs to be traversed.
    timestamp64 max_entity_ts = entity_item->max_ts;
    while (!block_item_queue_.empty()) {
      BlockItem* block_item = block_item_queue_.back();
      cur_block_item_ = block_item;
      block_item_queue_.pop_back();
      if (!block_item || !block_item->publish_row_count) {
        continue;
      }
      // all rows in block is deleted.
      if (block_item->isBlockEmpty()) {
        continue;
      }
      std::shared_ptr<MMapSegmentTable> segment_tbl = cur_pt->getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]",
                  block_item->block_id, cur_pt->GetPath().c_str());
        return KStatus::FAIL;
      }
      timestamp64 min_ts, max_ts;
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
      // If the time range of the BlockItem is not within the ts_span range, continue traversing the next BlockItem.
      if (!isTimestampInSpans(ts_spans_, min_ts, max_ts)) {
        continue;
      }
      // all agg is filled. so we just need ts that max than agg using ts.
      if (hasFoundLastAggData() && first_last_row_.GetLastMinTs() >= max_ts) {
        continue;
      }
      bool has_found = false;
      // save bitmap for all blocks, the first map key is col index
      // last row no need check bitmap
      if (getBlockBitmap(segment_tbl, block_item, 2) != KStatus::SUCCESS) {
        LOG_ERROR("getBlockBitmap failed.");
        return KStatus::FAIL;
      }
      // If there is no last row type query, it can be skipped directly when the data in the current block is all null.
      if (no_last_row_type_) {
        bool all_deleted = true;
        for (int i = 0; i < scan_agg_types_.size(); ++i) {
          if (IsLastAggType(scan_agg_types_[i]) && !col_blk_bitmaps_.IsColAllNull(i, block_item->publish_row_count)) {
            all_deleted = false;
            break;
          }
        }
        if (all_deleted) {
          continue;
        }
      }
      // Traverse all data of this BlockItem
      uint32_t cur_row_offset = block_item->publish_row_count;
      while (cur_row_offset > 0) {
        bool is_deleted = !segment_tbl->IsRowVaild(block_item, cur_row_offset);
        // If the data in the cur_row_offset_ row is not within the ts_span range or has been deleted,
        // continue to verify the data in the next row.
        MetricRowID real_row = block_item->getRowID(cur_row_offset);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(real_row, 0));
        if (is_deleted || !checkIfTsInSpan(cur_ts)) {
          --cur_row_offset;
          continue;
        }
        // Update variables that record the query results of last/last_row
        first_last_row_.UpdateLastRow(cur_ts, real_row, cur_pt, segment_tbl, col_blk_bitmaps_);
        // If all queried columns and their corresponding query types already have query results,
        // and the timestamp of the updated data is equal to the maximum timestamp of the entity, then the query can end.
        if (hasFoundLastAggData() && (!entity_item->is_disordered || cur_ts == max_entity_ts)) {
          has_found = true;
          break;
        }
        --cur_row_offset;
      }
      if (has_found) {
        break;
      }
    }
    if (hasFoundLastAggData()) {
      break;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIterator::findLastData(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_last);
  *count = 0;
  KStatus s = findLastDataByIter(ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("find last data failed.");
    return s;
  }
  return genBatchData(res, count);
}

KStatus TsAggIterator::genBatchData(ResultSet* res, k_uint32* count) {
  KWDB_DURATION(StStatistics::Get().agg_first_last);
  *count = 0;
  // The data traversal is completed, and the variables of the last/last_row query result updated by the
  // updateLastCols function are encapsulated as Batch and added to res to be returned to the execution layer.
  for (k_uint32 i = 0; i < scan_agg_types_.size(); ++i) {
    k_int32 ts_col = -1;
    if (i < ts_scan_cols_.size()) {
      ts_col = ts_scan_cols_[i];
    }
    if (ts_col < 0) {
      continue;
    }
    Batch* cur_batch = nullptr;
    KStatus s = first_last_row_.GetAggBatch(this, ts_col, i, attrs_[ts_col], &cur_batch);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get col[%d] agg value failed.", ts_col);
      return KStatus::FAIL;
    }
    res->push_back(i, cur_batch);
  }
  res->entity_index = {entity_group_id_, entity_ids_[cur_entity_idx_], subgroup_id_};
  if (isAllAggResNull(res)) {
    *count = 0;
    res->clear();
  } else {
    *count = 1;
  }
  return SUCCESS;
}

KStatus TsAggIterator::findFirstLastData(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_first);
  *count = 0;
  KStatus s;
  if (!only_last_type_) {
    s = findFirstDataByIter(ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot find first data at first last data.");
      return s;
    }
  }
  if (!only_first_type_) {
    s = findLastDataByIter(ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot find last data at first last data.");
      return s;
    }
  }
  s = genBatchData(res, count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot genBatch data at first last data.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIterator::countDataUseStatistics(ResultSet* res, k_uint32* count, timestamp64 ts) {
  *count = 0;
  partition_table_iter_->Reset();
  block_item_queue_.clear();
  cur_block_item_ = nullptr;
  std::vector<timestamp64> partition_need_traverse;
  while (partition_table_iter_->Valid()) {
    KStatus s = partition_table_iter_->Next(&cur_partition_table_);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("countDataUseStatistics failed. next partition at entitygroup [%lu]", entity_group_id_);
      return KStatus::FAIL;
    }
    if (cur_partition_table_ == nullptr) {
      // all partition table scan over.
      break;
    }
    if (!isTimestampWithinSpans(ts_spans_,
                                convertSecondToPrecisionTS(cur_partition_table_->minTimestamp(), ts_col_type_),
                                convertSecondToPrecisionTS(cur_partition_table_->maxTimestamp(), ts_col_type_))) {
      partition_need_traverse.push_back(cur_partition_table_->minTimestamp());
    } else {
      auto entity_item = cur_partition_table_->getEntityItem(entity_ids_[cur_entity_idx_]);
      auto* b = new AggBatch(malloc(sizeof(k_uint64)), 1, nullptr);
      b->is_new = true;
      *static_cast<k_uint64*>(b->mem) = entity_item->row_written;
      res->push_back(0, b);
    }
  }
  KStatus s = KStatus::SUCCESS;
  if (partition_need_traverse.size() > 0) {
    ErrorInfo err_info;
    auto sub_grp = entity_group_->GetSubEntityGroupManager()->GetSubGroup(subgroup_id_, err_info);
    partition_table_iter_ = std::make_shared<TsSubGroupPTIterator>(sub_grp, partition_need_traverse);
    do {
      s = traverseAllBlocks(res, count, ts);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("countDataUseStatitics failed at traverseAllBlocks. [%lu]", entity_group_id_);
        return KStatus::FAIL;
      }
    } while (*count != 0);
  }
  return s;
}

KStatus TsAggIterator::countDataAllBlocks(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_blocks);
  *count = 0;
  k_uint64 total_count = 0;
  partition_table_iter_->Reset();
  block_item_queue_.clear();
  cur_block_item_ = nullptr;
  while (true) {
    TsTimePartition* cur_pt = nullptr;
    partition_table_iter_->Next(&cur_pt);
    block_item_queue_.clear();
    if (cur_pt == nullptr) {
      // all partition scan over.
      break;
    }
    if (ts != INVALID_TS) {
      if (!is_reversed_ && convertSecondToPrecisionTS(cur_pt->minTimestamp(), ts_col_type_) > ts) {
        break;
      } else if (is_reversed_ && convertSecondToPrecisionTS(cur_pt->maxTimestamp(), ts_col_type_) < ts) {
        break;
      }
    }

    cur_pt->GetAllBlockItems(entity_ids_[cur_entity_idx_], block_item_queue_);
    auto entity_item = cur_pt->getEntityItem(entity_ids_[cur_entity_idx_]);
    // Obtain the minimum timestamp for the current query entity.
    // Once a record's timestamp is traversed to be equal to it,
    // it indicates that the query result has been found and no additional data needs to be traversed.
    timestamp64 min_entity_ts = entity_item->min_ts;
    while (!block_item_queue_.empty()) {
      BlockItem* block_item = block_item_queue_.front();
      cur_block_item_ = block_item;
      block_item_queue_.pop_front();
      if (!block_item || !block_item->publish_row_count) {
        continue;
      }
      // all rows in block is deleted.
      if (block_item->isBlockEmpty()) {
        continue;
      }
      std::shared_ptr<MMapSegmentTable> segment_tbl = cur_pt->getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, cur_pt->GetPath().c_str());
        return KStatus::FAIL;
      }
      timestamp64 min_ts, max_ts;
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
      // If the time range of the BlockItem is not within the ts_span range, continue traversing the next BlockItem.
      if (!isTimestampInSpans(ts_spans_, min_ts, max_ts)) {
        continue;
      }
      if (isTimestampWithinSpans(ts_spans_, min_ts, max_ts)) {
        total_count += block_item->getNonNullRowCount();
      } else {
        // Traverse all data of this BlockItem
        uint32_t cur_row_offset = 1;
        while (cur_row_offset <= block_item->publish_row_count) {
          bool is_deleted = !segment_tbl->IsRowVaild(block_item, cur_row_offset);
          if (is_deleted) {
            ++cur_row_offset;
            continue;
          }
          // If the data in the cur_row_offset row is not within the ts_span range or has been deleted,
          // continue to verify the data in the next row.
          MetricRowID real_row = block_item->getRowID(cur_row_offset);
          timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(real_row, 0));
          if (!checkIfTsInSpan(cur_ts)) {
            ++cur_row_offset;
            continue;
          }
          ++cur_row_offset;
          ++total_count;
        }
      }
    }
  }

  if (total_count != 0) {
    Batch* b = new AggBatch(malloc(sizeof(uint64_t)), 1, nullptr);
    *static_cast<uint64_t*>(b->mem) = total_count;
    b->is_new = true;
    res->push_back(0, b);
    res->entity_index = {entity_group_id_, entity_ids_[cur_entity_idx_], subgroup_id_};
    *count = 1;
  }
  return SUCCESS;
}

KStatus TsAggIterator::getBlockBitmap(std::shared_ptr<MMapSegmentTable>& segment_tbl, BlockItem* block_item, int type) {
  for (int i = 0; i < ts_scan_cols_.size(); i++) {
    auto& col_idx = ts_scan_cols_[i];
    if (!segment_tbl->isColExist(col_idx)) {
      col_blk_bitmaps_.SetColBitMap(i, nullptr);
      continue;
    }
    if (all_agg_cols_not_null_) {
      continue;;
    }
    AttributeInfo actual_col = segment_tbl->GetColInfo(col_idx);
    // column not allow null value. so all values in block are valid.
    if (actual_col.isFlag(AINFO_NOT_NULL)) {
      col_blk_bitmaps_.SetColBitMapVaild(i);
      continue;
    }
    void* bitmap = segment_tbl->columnNullBitmapAddr(block_item->block_id, col_idx);
    bool need_free_bitmap = false;
    if (!isSameType(actual_col, attrs_[col_idx])) {
      auto s = getActualColBitmap(segment_tbl, block_item->block_id, 1, attrs_[col_idx], col_idx,
                                  block_item->publish_row_count, &bitmap, need_free_bitmap);
      if (s != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    }
    // cache bitmap info, to avoid page fault.
    col_blk_bitmaps_.SetColBitMap(i, reinterpret_cast<char*>(bitmap), need_free_bitmap);
    if (need_free_bitmap) {
      free(bitmap);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIterator::traverseAllBlocks(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_blocks);
  *count = 0;
  while (true) {
    if (!cur_block_item_) {
      auto ret = nextBlockItem(entity_ids_[cur_entity_idx_]);
      if (ret == NextBlkStatus::scan_over) {
        break;
      } else if (ret == NextBlkStatus::error) {
        LOG_ERROR("failed at traverseAllBlocks.");
        return KStatus::FAIL;
      }
      continue;
    }
    BlockItem* cur_block = cur_block_item_;
    TsTimePartition* cur_pt = cur_partition_table_;
    if (ts != INVALID_TS) {
      if (!is_reversed_ && convertSecondToPrecisionTS(cur_pt->minTimestamp(), ts_col_type_) > ts) {
        return SUCCESS;
      } else if (is_reversed_ && convertSecondToPrecisionTS(cur_pt->maxTimestamp(), ts_col_type_) < ts) {
        return SUCCESS;
      }
    }
    std::shared_ptr<MMapSegmentTable> segment_tbl = cur_partition_table_->getSegmentTable(cur_block->block_id);
    if (segment_tbl == nullptr) {
      LOG_ERROR("Can not find segment use block [%u], in path [%s]",
                cur_block->block_id, cur_partition_table_->GetPath().c_str());
      return KStatus::FAIL;
    }
    if (segment_tbl->schemaVersion() > table_version_) {
      cur_blockdata_offset_ = 1;
      nextBlockItem(entity_ids_[cur_entity_idx_]);
      continue;
    }
    // save bitmap for all blocks, the first map key is col index
    if (!only_last_row_type_ && getBlockBitmap(segment_tbl, cur_block, 0) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    uint32_t first_row = 1;
    bool has_data = getCurBlockSpan(cur_block, segment_tbl, &first_row, count);
    MetricRowID first_real_row = cur_block->getRowID(first_row);
    // If qualified data is obtained, further obtain the aggregation result of this continuous data
    // and package Batch to be added to res to return.
    // 1. If the data obtained is for the entire BlockItem and the aggregation results stored in the BlockItem are
    //    available and the query column has not undergone type conversion, then the aggregation results stored in
    //    the BlockItem can be directly obtained.
    //    The query for SUM type belongs to a special case:
    //    (1) If type overflow is identified, it needs to be recalculated.
    //    (2) If there is no type overflow, the read aggregation result needs to be converted to a type.
    // 2. If the above situation is not met, it is necessary to calculate the aggregation result
    //    and use the AggCalculator/VarColAggCalculator class to calculate the aggregation result.
    //    There is also a special case where the column being queried has undergone column type conversion,
    //    and the obtained continuous data needs to be first converted to the column type of the query
    //    through getActualColMemAndBitmap.
    if (has_data) {
      // Add all queried column data to the res result.
      for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
        k_int32 col_idx = -1;
        if (i < ts_scan_cols_.size()) {
          col_idx = ts_scan_cols_[i];
        }
        if (scan_agg_types_[i] == COUNT && col_blk_bitmaps_.IsColSpanNull(i, first_row, *count)) {
          Batch* b;
          b = new AggBatch(malloc(sizeof(uint64_t)), 1, nullptr);
          *static_cast<uint64_t*>(b->mem) = 0;
          b->is_new = true;
          res->push_back(i, b);
          continue;
        }
        if (col_idx < 0 || col_blk_bitmaps_.IsColSpanNull(i, first_row, *count) ||
            !colTypeHasAggResult((DATATYPE)attrs_[col_idx].type, scan_agg_types_[i])) {
          continue;
        }
        AttributeInfo col_info = segment_tbl->GetColInfo(col_idx);
        Batch* b;
        if (*count < cur_block->publish_row_count || !cur_block->is_agg_res_available ||
            !isSameType(col_info, attrs_[col_idx])) {
          void* mem = segment_tbl->columnAddr(first_real_row, col_idx);
          void* bitmap = col_blk_bitmaps_.GetColBitMapAddr(i);
          void* sum;
          bool need_free_bitmap = false;
          std::shared_ptr<void> new_mem;
          std::vector<std::shared_ptr<void>> new_var_mem;
          if (!isSameType(col_info, attrs_[col_idx])) {
            auto s = getActualColMemAndBitmap(segment_tbl, cur_block->block_id, first_row, col_idx, *count,
                                              &new_mem, new_var_mem, &bitmap, need_free_bitmap);
            if (s != KStatus::SUCCESS) {
              return s;
            }
            if (isAllNull(reinterpret_cast<char*>(bitmap), first_row, *count)) {
              continue;
            }
          }
          switch (scan_agg_types_[i]) {
            case Sumfunctype::MAX:
            case Sumfunctype::MIN: {
              if (!isSameType(col_info, attrs_[col_idx])) {
                if (!isVarLenType(attrs_[col_idx].type)) {
                  AggCalculator agg_cal(new_mem.get(), bitmap, first_row,
                                        DATATYPE(attrs_[col_idx].type), attrs_[col_idx].size, *count);
                  if (scan_agg_types_[i] == Sumfunctype::MAX) {
                    b = CreateAggBatch(agg_cal.GetMax(nullptr, true), nullptr);
                  } else {
                    b = CreateAggBatch(agg_cal.GetMin(nullptr, true), nullptr);
                  }
                  b->is_new = true;
                } else {
                  VarColAggCalculator agg_cal(new_var_mem, new_var_mem.size());
                  if (scan_agg_types_[i] == Sumfunctype::MAX) {
                    b = CreateAggBatch(agg_cal.GetMax(), nullptr);
                  } else {
                    b = CreateAggBatch(agg_cal.GetMin(), nullptr);
                  }
                }
              } else {
                if (!isVarLenType(attrs_[col_idx].type)) {
                  AggCalculator agg_cal(mem, bitmap, first_row,
                                        DATATYPE(attrs_[col_idx].type), attrs_[col_idx].size, *count);
                  void* min_max_res = (scan_agg_types_[i] == Sumfunctype::MAX) ? agg_cal.GetMax() : agg_cal.GetMin();
                  void* new_min_max_res = malloc(attrs_[col_idx].size);
                  memcpy(new_min_max_res, min_max_res, attrs_[col_idx].size);
                  b = CreateAggBatch(new_min_max_res, nullptr);
                  b->is_new = true;
                } else {
                  vector<shared_ptr<void>> var_mem_data;
                  for (k_uint32 j = 0; j < *count; ++j) {
                    std::shared_ptr<void> data = nullptr;
                    if (!col_blk_bitmaps_.IsColNull(i, first_row + j)) {
                      data = segment_tbl->varColumnAddrByBlk(cur_block_item_->block_id, first_row + j - 1, col_idx);
                    }
                    var_mem_data.push_back(data);
                  }
                  VarColAggCalculator agg_cal(var_mem_data, bitmap, first_row, attrs_[col_idx].size, *count);
                  if (scan_agg_types_[i] == Sumfunctype::MAX) {
                    b = CreateAggBatch(agg_cal.GetMax(), nullptr);
                  } else {
                    b = CreateAggBatch(agg_cal.GetMin(), nullptr);
                  }
                }
              }
              break;
            }
            case Sumfunctype::SUM: {
              bool is_overflow = false;
              if (!isSameType(col_info, attrs_[col_idx])) {
                AggCalculator agg_cal(new_mem.get(), bitmap, first_row,
                                      DATATYPE(attrs_[col_idx].type), attrs_[col_idx].size, *count);
                is_overflow = agg_cal.GetSum(&sum);
              } else {
                bitmap = segment_tbl->columnNullBitmapAddr(first_real_row.block_id, col_idx);
                AggCalculator agg_cal(mem, bitmap, first_row, DATATYPE(col_info.type), col_info.size, *count);
                is_overflow = agg_cal.GetSum(&sum);
              }
              b = CreateAggBatch(sum, nullptr);
              b->is_new = true;
              b->is_overflow = is_overflow;
              break;
            }
            case Sumfunctype::COUNT: {
              uint16_t notnull_count = 0;
              for (uint32_t j = 0; j < *count; ++j) {
                if (!IsObjectColNull(static_cast<char*>(bitmap), first_real_row.offset_row - 1 + j)) {
                  ++notnull_count;
                }
              }
              b = new AggBatch(malloc(sizeof(uint64_t)), 1, nullptr);
              *static_cast<uint64_t*>(b->mem) = notnull_count;
              b->is_new = true;
              break;
            }
            default:
              break;
          }
          if (need_free_bitmap) {
            free(bitmap);
          }
        } else {
          if (scan_agg_types_[i] == Sumfunctype::SUM && cur_block->is_overflow) {
            // If a type overflow is identified, the SUM result needs to be recalculated and cannot be read directly.
            AggCalculator agg_cal(segment_tbl->columnAddr(first_real_row, col_idx),
                                  col_blk_bitmaps_.GetColBitMapAddr(i), first_row,
                                  DATATYPE(col_info.type), col_info.size, *count);
            void* sum;
            bool is_overflow = agg_cal.GetSum(&sum);
            b = CreateAggBatch(sum, nullptr);
            b->is_new = true;
            b->is_overflow = is_overflow;
          } else if (scan_agg_types_[i] == Sumfunctype::SUM) {
            // Convert the obtained SUM result to the type agreed upon with the execution layer.
            void* new_sum_base;
            void* sum_base = segment_tbl->columnAggAddr(first_real_row.block_id, col_idx, scan_agg_types_[i]);
            bool is_new = ChangeSumType(DATATYPE(attrs_[col_idx].type), sum_base, &new_sum_base);
            b = CreateAggBatch(new_sum_base, segment_tbl);
            b->is_new = is_new;
          } else if (scan_agg_types_[i] == Sumfunctype::COUNT) {
            void* agg_res = segment_tbl->columnAggAddr(first_real_row.block_id, col_idx, scan_agg_types_[i]);
            void* new_agg_res = malloc(sizeof(uint64_t));
            *static_cast<uint64_t*>(new_agg_res) = *static_cast<uint16_t*>(agg_res);
            b = CreateAggBatch(new_agg_res, nullptr);
            b->is_new = true;
          } else {
            if (!isVarLenType(attrs_[col_idx].type)) {
              void* agg_res = segment_tbl->columnAggAddr(first_real_row.block_id, col_idx, scan_agg_types_[i]);
              void* new_agg_res = malloc(attrs_[col_idx].size);
              memcpy(new_agg_res, agg_res, attrs_[col_idx].size);
              b = CreateAggBatch(new_agg_res, nullptr);
              b->is_new = true;
            } else {
              std::shared_ptr<void> var_mem = segment_tbl->varColumnAggAddr(first_real_row, col_idx, scan_agg_types_[i]);
              b = CreateAggBatch(var_mem, segment_tbl);
            }
          }
        }
        res->push_back(i, b);
      }
      if (cur_blockdata_offset_ > cur_block->alloc_row_count) {
        cur_blockdata_offset_ = 1;
        cur_block_item_ = nullptr;
      }
      return SUCCESS;
    }
    if (cur_blockdata_offset_ > cur_block->alloc_row_count) {
      cur_blockdata_offset_ = 1;
      cur_block_item_ = nullptr;
    }
  }
  return SUCCESS;
}

// Convert the obtained continuous data into a query type and write it into the new application space.
KStatus
TsAggIterator::getActualColMemAndBitmap(std::shared_ptr<MMapSegmentTable>& segment_tbl, BLOCK_ID block_id, size_t start_row,
                                        uint32_t col_idx, k_uint32 count, std::shared_ptr<void>* mem,
                                        std::vector<std::shared_ptr<void>>& var_mem, void** bitmap, bool& need_free_bitmap) {
  auto& schema_info = segment_tbl->getSchemaInfo();
  // There are two situations to handle:
  // 1. convert to fixed length types,  which can be further divided into:
  //    (1) other types to fixed length types
  //    (2) conversion between the same fixed length type but different lengths
  // 2. convert to variable length types
  if (!isVarLenType(attrs_[col_idx].type)) {
    if (schema_info[col_idx].type != attrs_[col_idx].type) {
      // Conversion from other types to fixed length types.
      char* value = static_cast<char*>(malloc(attrs_[col_idx].size * count));
      memset(value, 0, attrs_[col_idx].size * count);
      KStatus s = ConvertToFixedLen(segment_tbl, value, block_id,
                                    (DATATYPE)(schema_info[col_idx].type), (DATATYPE)(attrs_[col_idx].type),
                                    attrs_[col_idx].size, start_row, count, col_idx, bitmap, need_free_bitmap);
      if (s != KStatus::SUCCESS) {
        free(value);
        return KStatus::FAIL;
      }
      std::shared_ptr<void> ptr(value, free);
      *mem = ptr;
    } else if (schema_info[col_idx].size != attrs_[col_idx].size) {
      // Conversion between same fixed length type, but different lengths.
      char* value = static_cast<char*>(malloc(attrs_[col_idx].size * count));
      memset(value, 0, attrs_[col_idx].size * count);
      for (k_uint32 idx = start_row - 1; idx < count; ++idx) {
        memcpy(value + idx * attrs_[col_idx].size,
               segment_tbl->columnAddrByBlk(block_id, idx, col_idx), schema_info[col_idx].size);
      }
      std::shared_ptr<void> ptr(value, free);
      *mem = ptr;
    }
  } else {
    for (k_uint32 j = 0; j < count; ++j) {
      if (IsObjectColNull(reinterpret_cast<char*>(*bitmap), start_row + j - 1)) {
        continue;
      }
      std::shared_ptr<void> data = nullptr;
      // Convert other types to variable length data types.
      data = ConvertToVarLen(segment_tbl, block_id, static_cast<DATATYPE>(schema_info[col_idx].type),
                             static_cast<DATATYPE>(attrs_[col_idx].type), start_row + j - 1, col_idx);
      var_mem.push_back(data);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIterator::Init(bool is_reversed) {
  KStatus s = TsStorageIterator::Init(is_reversed);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  all_agg_cols_not_null_ = true;
  for (size_t i = 0; i < scan_agg_types_.size(); i++) {
    k_int32 col_idx = -1;
    if (i < ts_scan_cols_.size()) {
      col_idx = ts_scan_cols_[i];
    }
    if (!attrs_[col_idx].isFlag(AINFO_NOT_NULL)) {
      all_agg_cols_not_null_ = false;
    }
    if (!first_last_row_.HaslastTsPoint()) {
      if (scan_agg_types_[i] == Sumfunctype::LAST && attrs_[col_idx].isFlag(AINFO_NOT_NULL)) {
        scan_agg_types_[i] = Sumfunctype::LAST_ROW;
      }
      if (scan_agg_types_[i] == Sumfunctype::LASTTS && attrs_[col_idx].isFlag(AINFO_NOT_NULL)) {
        scan_agg_types_[i] = Sumfunctype::LASTROWTS;
      }
    }
  }
  only_first_type_ = onlyHasFirstAggType();
  only_last_type_ = onlyHasLastAggType();
  only_last_row_type_ = onlyLastRowAggType();
  only_first_last_type_ = onlyHasFirstLastAggType();
  only_count_ts_ = onlyCountTs();
  if (!col_blk_bitmaps_.Init(scan_agg_types_.size(), all_agg_cols_not_null_)) {
    LOG_ERROR("col_blk_bitmaps_ new memory failed.");
    return KStatus::FAIL;
  }
  first_last_row_.Init(ts_scan_cols_, scan_agg_types_);
  return SUCCESS;
}

KStatus TsAggIterator::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().agg_next);
  *count = 0;
  if (cur_entity_idx_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  KStatus s;
  // If only queries related to first/last aggregation types are involved, the optimization process can be followed.
  if (only_first_type_ || only_last_type_ || only_first_last_type_) {
    if (only_first_type_) {
      s = findFirstData(res, count, ts);
    } else if (only_last_type_) {
      s = findLastData(res, count, ts);
    } else if (only_first_last_type_) {
      s = findFirstLastData(res, count, ts);
    }
    reset();
    return s;
  }
  if (only_count_ts_) {
    s = countDataAllBlocks(res, count, ts);
    reset();
    return s;
  }
  if (!partition_table_iter_->Valid()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  if (!hasFoundFirstAggData()) {
    s = findFirstDataByIter(ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("findFirstDataByIter failed.");
      return s;
    }
  }
  if (!hasFoundLastAggData()) {
    s = findLastDataByIter(ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("findLastDataByIter failed.");
      return s;
    }
  }

  partition_table_iter_->Reset();
  block_item_queue_.clear();
  cur_block_item_ = nullptr;
  ResultSet result{(k_uint32) kw_scan_cols_.size()};
  // Continuously calling the traceAllBlocks function to obtain
  // the intermediate aggregation result of all data for the current query entity.
  // When the count is 0, it indicates that the query is complete.
  // Further integration and calculation of the results in the variable result are needed in the future.
  do {
    s = traverseAllBlocks(&result, count, ts);
    if (s != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
  } while (*count != 0);

  if (result.empty() && !first_last_row_.NeedFirstLastAgg()) {
    reset();
    return KStatus::SUCCESS;
  }

  // By calling the AggCalculator/VarColAggCalculator function,
  // integrate the intermediate results to obtain the final aggregated query result of an entity.
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 col_idx = -1;
    if (i < ts_scan_cols_.size()) {
      col_idx = ts_scan_cols_[i];
    }
    if (col_idx < 0) {
      LOG_ERROR("TsAggIterator::Next : no column : %d", kw_scan_cols_[i]);
      continue;
    }
    switch (scan_agg_types_[i]) {
      case Sumfunctype::MAX:
      case Sumfunctype::MIN: {
        KWDB_DURATION(StStatistics::Get().agg_min);
        if (result.data[i].empty()) {
          Batch* b = CreateAggBatch(nullptr, nullptr);
          res->push_back(i, b);
        } else {
          if (!isVarLenType(attrs_[col_idx].type)) {
            bool need_to_new = false;
            void* agg_base = nullptr;
            for (auto it : result.data[i]) {
              if (it->is_new) need_to_new = true;
              AggCalculator agg_cal(it->mem, DATATYPE(attrs_[col_idx].type), attrs_[col_idx].size, 1);
              if (scan_agg_types_[i] == Sumfunctype::MAX) {
                agg_base = agg_cal.GetMax(agg_base);
              } else if (scan_agg_types_[i] == Sumfunctype::MIN) {
                agg_base = agg_cal.GetMin(agg_base);
              }
            }
            if (need_to_new) {
              void* new_agg_base = malloc(attrs_[col_idx].size);
              memcpy(new_agg_base, agg_base, attrs_[col_idx].size);
              agg_base = new_agg_base;
            }
            Batch* b = new AggBatch(agg_base, 1, nullptr);
            b->is_new = need_to_new;
            res->push_back(i, b);
          } else {
            std::shared_ptr<void> agg_base = nullptr;
            for (auto it : result.data[i]) {
              VarColAggCalculator agg_cal({reinterpret_cast<const AggBatch*>(it)->var_mem_}, 1);
              if (scan_agg_types_[i] == Sumfunctype::MAX) {
                agg_base = agg_cal.GetMax(agg_base);
              } else if (scan_agg_types_[i] == Sumfunctype::MIN) {
                agg_base = agg_cal.GetMin(agg_base);
              }
            }
            Batch* b = new AggBatch(agg_base, 1, nullptr);
            res->push_back(i, b);
          }
        }
        break;
      }
      case Sumfunctype::SUM: {
        KWDB_DURATION(StStatistics::Get().agg_sum);
        void *sum_base = nullptr;
        bool is_overflow = false;
        for (auto it : result.data[i]) {
          AggCalculator agg_cal(it->mem, getSumType(DATATYPE(attrs_[col_idx].type)),
                                getSumSize(DATATYPE(attrs_[col_idx].type)), 1, it->is_overflow);
          if (agg_cal.GetSum(&sum_base, sum_base, is_overflow)) {
            is_overflow = true;
          }
        }
        Batch* b = CreateAggBatch(sum_base, nullptr);
        b->is_new = true;
        b->is_overflow = is_overflow;
        res->push_back(i, b);
        break;
      }
      case Sumfunctype::COUNT: {
        KWDB_DURATION(StStatistics::Get().agg_count);
        k_uint64 total_count = 0;
        for (auto it : result.data[i]) {
          total_count += *static_cast<k_uint64*>(it->mem);
        }
        auto* b = new AggBatch(malloc(sizeof(k_uint64)), 1, nullptr);
        b->is_new = true;
        *static_cast<k_uint64*>(b->mem) = total_count;
        res->push_back(i, b);
        break;
      }
      default:
      {
        Batch* b = nullptr;
        KStatus s = first_last_row_.GetAggBatch(this, col_idx, i, attrs_[col_idx], &b);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("parse col [%d] failed.", col_idx);
          return KStatus::FAIL;
        }
        if (b != nullptr) {
          res->push_back(i, b);
        }
        break;
      }
    }
  }
  res->entity_index = {entity_group_id_, entity_ids_[cur_entity_idx_], subgroup_id_};
  if (isAllAggResNull(res)) {
    *count = 0;
    res->clear();
  } else {
    *count = 1;
  }
  // An entity query has been completed and requires resetting the state variables in the iterator.
  reset();
  result.clear();
  return SUCCESS;
}

KStatus TsTableIterator::Next(ResultSet* res, k_uint32* count, timestamp64 ts) {
  *count = 0;
  MUTEX_LOCK(&latch_);
  Defer defer{[&]() { MUTEX_UNLOCK(&latch_); }};

  KStatus s;
  bool is_finished;
  do {
    is_finished = false;
    if (current_iter_ >= iterators_.size()) {
      break;
    }

    s = iterators_[current_iter_]->Next(res, count, &is_finished, ts);
    if (s == FAIL) {
      return s;
    }
    // when is_finished is true,
    // it indicates that a TsStorageIterator iterator query has ended and continues to read the next one.
    if (is_finished) current_iter_++;
  } while (is_finished);

  return KStatus::SUCCESS;
}

void TsSortedRowDataIterator::fetchBlockSpans(k_uint32 entity_id) {
  cur_partition_table_->GetAllBlockSpans(entity_id, ts_spans_, block_spans_, UINT32_MAX, is_reversed_);
}

int TsSortedRowDataIterator::nextBlockSpan(k_uint32 entity_id) {
  cur_block_span_ = BlockSpan{};
  while (true) {
    if (block_spans_.empty()) {
      if (segment_iter_ != nullptr) {
        delete segment_iter_;
        segment_iter_ = nullptr;
      }
      KStatus s = partition_table_iter_->Next(&cur_partition_table_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("failed get next partition at entityid[%u] entitygroup [%lu]", entity_id, entity_group_id_);
        return -2;
      }
      if (cur_partition_table_ == nullptr) {
        // all partition table scan over.
        return -1;
      }
      fetchBlockSpans(entity_id);
      continue;
    }
    cur_block_span_ = block_spans_.front();
    block_spans_.pop_front();

    if (!cur_block_span_.block_item) {
      LOG_WARN("BlockItem[] error: No space has been allocated");
      continue;
    }
    return 0;
  }
}

KStatus TsSortedRowDataIterator::Init(bool is_reversed) {
  is_reversed_ = is_reversed;
  auto sub_grp_mgr = entity_group_->GetSubEntityGroupManager();
  if (sub_grp_mgr == nullptr) {
    LOG_ERROR("can not found sub entitygroup manager for entitygroup [%lu].", entity_group_id_);
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  auto sub_grp = sub_grp_mgr->GetSubGroup(subgroup_id_, err_info);
  if (!err_info.isOK()) {
    LOG_ERROR("can not found sub entitygroup for entitygroup [%lu], err_msg: %s.",
              entity_group_id_, err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  partition_table_iter_ = sub_grp->GetPTIterator(ts_spans_);
  partition_table_iter_->Reset(is_reversed_);
  return entity_group_->GetRootTableManager()->GetSchemaInfoIncludeDropped(&attrs_, table_version_);
}

KStatus TsSortedRowDataIterator::GetBatch(std::shared_ptr<MMapSegmentTable>& segment_tbl, BlockItem* cur_block_item,
                                          size_t block_start_idx, ResultSet* res, k_uint32 count) {
  // Put data from all columns to the res result
  auto& schema_info = segment_tbl->getSchemaInfo();
  ErrorInfo err_info;
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 ts_col = -1;
    if (i < ts_scan_cols_.size()) {
      ts_col = ts_scan_cols_[i];
    }
    Batch* b;
    if (ts_col >= 0 && segment_tbl->isColExist(ts_col)) {
      void* bitmap_addr = segment_tbl->getBlockHeader(cur_block_item->block_id, ts_col);
      if (attrs_[ts_col].type != VARSTRING && attrs_[ts_col].type != VARBINARY) {
        if (schema_info[ts_col].type != attrs_[ts_col].type) {
          // convert other types to fixed-length type
          char* value = static_cast<char*>(malloc(attrs_[ts_col].size * count));
          memset(value, 0, attrs_[ts_col].size * count);
          bool need_free_bitmap = false;
          KStatus s = ConvertToFixedLen(segment_tbl, value, cur_block_item->block_id,
                                        static_cast<DATATYPE>(schema_info[ts_col].type),
                                        static_cast<DATATYPE>(attrs_[ts_col].type),
                                        attrs_[ts_col].size, block_start_idx, count, ts_col, &bitmap_addr, need_free_bitmap);
          if (s != KStatus::SUCCESS) {
            free(value);
            return s;
          }
          b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_tbl);
          b->is_new = true;
          b->need_free_bitmap = need_free_bitmap;
        } else {
          if (schema_info[ts_col].size != attrs_[ts_col].size) {
            // convert same fixed-length type to different length
            char* value = static_cast<char*>(malloc(attrs_[ts_col].size * count));
            memset(value, 0, attrs_[ts_col].size * count);
            for (int idx = 0; idx < count; idx++) {
              memcpy(value + idx * attrs_[ts_col].size,
                     segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx + idx - 1, ts_col),
                     schema_info[ts_col].size);
            }
            b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_tbl);
            b->is_new = true;
          } else {
            b = new Batch(segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx - 1, ts_col),
                          count, bitmap_addr, block_start_idx, segment_tbl);
          }
        }
      } else {
        b = new VarColumnBatch(count, bitmap_addr, block_start_idx, segment_tbl);
        for (k_uint32 j = 0; j < count; ++j) {
          std::shared_ptr<void> data = nullptr;
          bool is_null;
          if (b->isNull(j, &is_null) != KStatus::SUCCESS) {
            delete b;
            b = nullptr;
            return KStatus::FAIL;
          }
          if (is_null) {
            data = nullptr;
          } else {
            if (schema_info[ts_col].type != attrs_[ts_col].type) {
              // convert other types to variable length
              data = ConvertToVarLen(segment_tbl, cur_block_item->block_id,
                                     static_cast<DATATYPE>(schema_info[ts_col].type),
                                     static_cast<DATATYPE>(attrs_[ts_col].type), block_start_idx + j - 1, ts_col);
            } else {
              data = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, block_start_idx + j - 1, ts_col);
            }
          }
          b->push_back(data);
        }
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      b = new Batch(bitmap, count, bitmap, block_start_idx, segment_tbl);
    }
    res->push_back(i, b);
  }
  return KStatus::SUCCESS;
}

KStatus TsSortedRowDataIterator::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().it_next);
  *count = 0;
  if (cur_entity_idx_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  while (true) {
    if (!cur_block_span_.block_item) {
      int ret = nextBlockSpan(entity_ids_[cur_entity_idx_]);
      if (ret == -1) {
        if (++cur_entity_idx_ >= entity_ids_.size()) {
          *is_finished = true;
          break;
        }
        partition_table_iter_->Reset(is_reversed_);
      } else if (ret == -2) {
        LOG_ERROR("can not get next block item.");
        return KStatus::FAIL;
      }
      continue;
    }
    TsTimePartition* cur_pt = cur_partition_table_;
    BlockItem* cur_block_item = cur_block_span_.block_item;
    std::shared_ptr<MMapSegmentTable> segment_tbl = cur_pt->getSegmentTable(cur_block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_ERROR("Can not find segment use block [%u], in path [%s]", cur_block_item->block_id, cur_pt->GetPath().c_str());
      return FAIL;
    }
    if (ts != INVALID_TS) {
      timestamp64 min_ts = KTimestamp(segment_tbl->columnAddrByBlk(cur_block_item->block_id,
                                                           cur_block_span_.start_row, 0));
      timestamp64 max_ts = KTimestamp(segment_tbl->columnAddrByBlk(cur_block_item->block_id,
                                                           cur_block_span_.start_row + cur_block_span_.row_num - 1, 0));
      if (!is_reversed_ && min_ts > ts) {
        // At this time, if no data smaller than ts exists, -1 is returned directly, and the query is ended
        nextEntity();
        return SUCCESS;
      } else if (is_reversed_ && max_ts < ts) {
        // In this case, if no data larger than ts exists, -1 is returned and the query is completed
        nextEntity();
        return SUCCESS;
      }
    }
    if (!cur_block_span_.row_num) {
      continue;
    }

    *count = cur_block_span_.row_num;
    uint32_t first_row = cur_block_span_.start_row + 1;
    MetricRowID first_real_row = cur_block_item->getRowID(first_row);

    cur_block_span_ = BlockSpan{};
    GetBatch(segment_tbl, cur_block_item, first_row, res, *count);
    res->entity_index = {entity_group_id_, entity_ids_[cur_entity_idx_], subgroup_id_};
    KWDB_STAT_ADD(StStatistics::Get().it_num, *count);
    return SUCCESS;
  }
  KWDB_STAT_ADD(StStatistics::Get().it_num, *count);
  return SUCCESS;
}

KStatus TsOffsetIterator::divideBlockSpans(timestamp64 begin_ts, timestamp64 end_ts,
                                       uint32_t* lower_cnt, deque<BlockSpan>& lower_block_span_) {
  int size = filter_block_spans_.size();
  timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
  for (int i = 0; i < size; ++i) {
    BlockSpan block_span = filter_block_spans_.front();
    filter_block_spans_.pop_front();

    uint32_t subgroup_id = block_span.subgroup_id;
    timestamp64 min_ts = block_span.min_ts, max_ts = block_span.max_ts;
    if ((is_reversed_ && min_ts > mid_ts) || (!is_reversed_ && max_ts <= mid_ts)) {
      *lower_cnt += block_span.row_num;
      lower_block_span_.push_back(block_span);
    } else if ((is_reversed_ && max_ts <= min_ts) || (!is_reversed_ && min_ts > mid_ts)) {
      filter_block_spans_.push_back(block_span);
    } else {
      std::shared_ptr<MMapSegmentTable> segment_tbl =
          cur_partition_table_[subgroup_id]->getSegmentTable(block_span.block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]",
                  block_span.block_item->block_id, cur_partition_table_[subgroup_id]->GetPath().c_str());
        return KStatus::FAIL;
      }

      bool is_lower_part;
      uint32_t first_row = block_span.start_row;
      for (int j = block_span.start_row; j < block_span.start_row + block_span.row_num; ++j) {
        MetricRowID row_id = block_span.block_item->getRowID(j + 1);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
        if (j == block_span.start_row) {
          is_lower_part = is_reversed_ ? (cur_ts > mid_ts) : (cur_ts <= mid_ts);
          min_ts = max_ts = cur_ts;
        } else if (is_lower_part && ((is_reversed_ && cur_ts <= mid_ts) || (!is_reversed_ && cur_ts > mid_ts))) {
          *lower_cnt += (j - first_row);
          lower_block_span_.push_back({block_span.block_item, first_row, j - first_row, subgroup_id, min_ts, max_ts});
          first_row = j;
          is_lower_part = false;
          min_ts = max_ts = cur_ts;
        } else if (!is_lower_part && ((is_reversed_ && cur_ts > mid_ts) || (!is_reversed_ && cur_ts <= mid_ts))) {
          filter_block_spans_.push_back({block_span.block_item, first_row, j - first_row, subgroup_id, min_ts, max_ts});
          first_row = j;
          is_lower_part = true;
          min_ts = max_ts = cur_ts;
        } else {
          min_ts = min(min_ts, cur_ts);
          max_ts = max(max_ts, cur_ts);
        }
      }
      if (first_row < block_span.row_num + block_span.start_row) {
        if (is_lower_part) {
          *lower_cnt += (block_span.start_row + block_span.row_num - first_row);
          lower_block_span_.push_back({block_span.block_item, first_row,
                                       block_span.start_row + block_span.row_num - first_row,
                                       subgroup_id, min_ts, max_ts});
        } else {
          filter_block_spans_.push_back({block_span.block_item, first_row,
                                         block_span.start_row + block_span.row_num - first_row,
                                         subgroup_id, min_ts, max_ts});
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIterator::filterLower(uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(cur_partition_table_.begin()->second->minTimestamp(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(cur_partition_table_.begin()->second->maxTimestamp(), ts_col_type_);
  // 精度 10 常量
  while (!filter_end_) {
    uint32_t lower_cnt = 0;
    deque<BlockSpan> lower_block_span_;
    if (divideBlockSpans(begin_ts, end_ts, &lower_cnt, lower_block_span_) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;

    if (filter_cnt_ + lower_cnt < offset_) {
      is_reversed_ ? end_ts = mid_ts : begin_ts = mid_ts;
      filter_cnt_ += lower_cnt;
    } else {
      is_reversed_ ? begin_ts = mid_ts : end_ts = mid_ts;
      while (!filter_block_spans_.empty()) {
        block_spans_.push_back(filter_block_spans_.front());
        *cnt += filter_block_spans_.front().row_num;
        filter_block_spans_.pop_front();
      }
      while (!lower_block_span_.empty()) {
        filter_block_spans_.push_back(lower_block_span_.front());
        lower_block_span_.pop_front();
      }
    }
    filter_end_ = (filter_cnt_ > (offset_ - deviation_)) || (end_ts - begin_ts < t_time_);
  }
  while (!filter_block_spans_.empty()) {
    block_spans_.push_back(filter_block_spans_.front());
    *cnt += filter_block_spans_.front().row_num;
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIterator::filterUpper(uint32_t filter_num, uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(cur_partition_table_.begin()->second->minTimestamp(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(cur_partition_table_.begin()->second->maxTimestamp(), ts_col_type_);
  bool filter_end = false;
  while (!filter_end) {
    uint32_t lower_cnt = 0;
    deque<BlockSpan> lower_block_span_;
    timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
    if (divideBlockSpans(begin_ts, end_ts, &lower_cnt, lower_block_span_) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    if (lower_cnt >= filter_num) {
      is_reversed_ ? begin_ts = mid_ts : end_ts = mid_ts;
      std::deque<BlockSpan>().swap(filter_block_spans_);
      while (!lower_block_span_.empty()) {
        filter_block_spans_.push_back(lower_block_span_.front());
        lower_block_span_.pop_front();
      }
    } else {
      is_reversed_ ? end_ts = mid_ts : begin_ts = mid_ts;
      while (!lower_block_span_.empty()) {
        block_spans_.push_back(lower_block_span_.front());
        lower_block_span_.pop_front();
      }
      *cnt += lower_cnt;
      filter_num -= lower_cnt;
    }
    // TODO(liumengzhen): optimize end condition
    filter_end = (filter_num <= 0) || (end_ts - begin_ts < t_time_);
  }
  while (!filter_block_spans_.empty()) {
    *cnt += filter_block_spans_.front().row_num;
    block_spans_.push_back(filter_block_spans_.front());
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

int TsOffsetIterator::nextBlockSpan() {
  while (true) {
    if (block_spans_.empty()) {
      if (segment_iter_.second != nullptr) {
        delete segment_iter_.second;
        segment_iter_ = {0, nullptr};
      }
      if (p_time_it_ == p_times_.end()) {
        // all partition scan over.
        return NextBlkStatus::scan_over;
      }
      if (queried_cnt >= offset_ + limit_ - filter_cnt_) {
        return NextBlkStatus::scan_over;
      }
      uint32_t row_cnt = 0;
      cur_partition_table_.clear();
      for (auto subgroup_id : p_time_it_->second) {
        std::shared_ptr<TsSubGroupPTIterator> cur_iter = partition_table_iter_[subgroup_id];
        TsTimePartition* p_table = nullptr;
        KStatus s = cur_iter->Next(&p_table);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("failed get next partition");
          return NextBlkStatus::error;
        }
        cur_partition_table_[subgroup_id] = p_table;
        uint32_t cnt = 0;
        if (fetchBlockItems(subgroup_id, &cnt) != KStatus::SUCCESS) {
          LOG_ERROR("failed fetchBlockItems");
          return NextBlkStatus::error;
        }
        row_cnt += cnt;
      }
      ++p_time_it_;
      if (0 == row_cnt) {
        continue;
      }
      filter_end_ = (filter_cnt_ > (offset_ - deviation_));
      if (!filter_end_) {
        if (filter_cnt_ + row_cnt <= offset_) {
          filter_cnt_ += row_cnt;
          row_cnt = 0;
          std::deque<BlockSpan>().swap(filter_block_spans_);
          filter_end_ = (filter_cnt_ > (offset_ - deviation_));
          continue;
        } else {
          if (filterLower(&row_cnt) != KStatus::SUCCESS) {
            return NextBlkStatus::error;
          }
        }
      } else {
        while (!filter_block_spans_.empty()) {
          block_spans_.push_back(filter_block_spans_.front());
          filter_block_spans_.pop_front();
        }
      }

      uint32_t need_to_be_returned = offset_ + limit_ - filter_cnt_ - queried_cnt;
      if (need_to_be_returned <= (row_cnt / 2)) {
        while (!block_spans_.empty()) {
          filter_block_spans_.push_back(block_spans_.front());
          block_spans_.pop_front();
        }
        if (filterUpper(need_to_be_returned, &row_cnt) != KStatus::SUCCESS) {
          return NextBlkStatus::error;
        }
      }
      queried_cnt += row_cnt;
      continue;
    }

    cur_block_span_ = block_spans_.front();
    block_spans_.pop_front();

    if (cur_block_span_.block_item == nullptr) {
      LOG_WARN("BlockItem[] error: No space has been allocated");
      continue;
    }
    return NextBlkStatus::find_one;
  }
  return 0;
}

KStatus TsOffsetIterator::fetchBlockItems(uint32_t subgroup_id, uint32_t* cnt) {
  uint32_t row_cnt = 0;
  deque<BlockItem*> blocks_queue;
  TsTimePartition* p_table = cur_partition_table_[subgroup_id];
  p_table->GetAllBlockItems(entity_ids_[subgroup_id], blocks_queue);
  while (!blocks_queue.empty()) {
    BlockItem* block_item = blocks_queue.front();
    blocks_queue.pop_front();
    if (p_table->getEntityItem(block_item->entity_id)->is_deleted) {
      continue;
    }
    std::shared_ptr<MMapSegmentTable> segment_tbl = p_table->getSegmentTable(block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, p_table->GetPath().c_str());
      return KStatus::FAIL;
    }
    timestamp64 min_ts, max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
    if (!isTimestampInSpans(ts_spans_, min_ts, max_ts) || !block_item->publish_row_count) {
      continue;
    } else if (isTimestampWithinSpans(ts_spans_, min_ts, max_ts) && !segment_tbl->HasRowNotValid(block_item)) {
      row_cnt += block_item->publish_row_count;
      filter_block_spans_.push_back(BlockSpan{block_item, 0, block_item->publish_row_count,
                                                 subgroup_id, min_ts, max_ts});
    } else {
      min_ts = INT64_MAX;
      max_ts = INT64_MIN;
      uint32_t first_row = 1;
      for (uint32_t i = 1; i <= block_item->publish_row_count; ++i) {
        MetricRowID row_id = block_item->getRowID(i);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
        if (!(CheckIfTsInSpan(cur_ts, ts_spans_)) || !segment_tbl->IsRowVaild(block_item, i)) {
          if (i > first_row) {
            row_cnt += (i - first_row);
            filter_block_spans_.push_back({block_item, first_row - 1, i - first_row, subgroup_id, min_ts, max_ts});
          }
          min_ts = INT64_MAX;
          max_ts = INT64_MIN;
          first_row = i + 1;
        } else {
          min_ts = min(min_ts, cur_ts);
          max_ts = max(max_ts, cur_ts);
        }
      }
      if (first_row <= block_item->publish_row_count) {
        row_cnt += block_item->publish_row_count - first_row + 1;
        filter_block_spans_.push_back({block_item, first_row - 1, block_item->publish_row_count - first_row + 1,
                                          subgroup_id, min_ts, max_ts});
      }
    }
  }
  *cnt = row_cnt;
  return KStatus::SUCCESS;
}

TsOffsetIterator::TsOffsetIterator(std::shared_ptr<TsEntityGroup>& entity_group, uint64_t entity_group_id,
                                   std::map<SubGroupID, std::vector<EntityID>>& entity_ids,
                                   std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                   std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                                   uint32_t table_version, uint32_t offset, uint32_t limit) :
                                   latch_(LATCH_ID_TSTABLE_ITERATOR_MUTEX),
                                   entity_group_id_(entity_group_id),
                                   entity_ids_(entity_ids),
                                   ts_spans_(ts_spans),
                                   ts_col_type_(ts_col_type),
                                   kw_scan_cols_(kw_scan_cols),
                                   ts_scan_cols_(ts_scan_cols),
                                   table_version_(table_version),
                                   entity_group_(entity_group),
                                   offset_(offset),
                                   limit_(limit) {
  entity_group_->RdDropLock();
}

TsOffsetIterator::~TsOffsetIterator() {
  if (segment_iter_.second != nullptr) {
    delete segment_iter_.second;
    segment_iter_.second = nullptr;
  }
  entity_group_->DropUnlock();
}

KStatus TsOffsetIterator::Init(bool is_reversed) {
  GetTerminationTime();
  is_reversed_ = is_reversed;
  auto sub_grp_mgr = entity_group_->GetSubEntityGroupManager();
  if (sub_grp_mgr == nullptr) {
    LOG_ERROR("can not found sub entitygroup manager for entitygroup [%lu].", entity_group_id_);
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  for (auto it : entity_ids_) {
    uint32_t subgroup_id = it.first;
    auto sub_grp = sub_grp_mgr->GetSubGroup(subgroup_id, err_info);
    if (!err_info.isOK()) {
      LOG_ERROR("can not found sub entitygroup for entitygroup [%lu], err_msg: %s.",
                entity_group_id_, err_info.errmsg.c_str());
      return KStatus::FAIL;
    }
    std::shared_ptr<TsSubGroupPTIterator> p_iter = sub_grp->GetPTIterator(ts_spans_);
    std::vector<timestamp64> partitions = p_iter->GetPartitions();
    p_iter->Reset(is_reversed_);
    for (auto p_time : partitions) {
      p_times_[p_time].push_back(subgroup_id);
    }
    partition_table_iter_[subgroup_id] = p_iter;
  }
  p_time_it_ = p_times_.begin();
  return entity_group_->GetRootTableManager()->GetSchemaInfoIncludeDropped(&attrs_, table_version_);
}

KStatus TsOffsetIterator::Next(ResultSet* res, k_uint32* count, timestamp64 ts) {
  KWDB_DURATION(StStatistics::Get().it_next);
  *count = 0;
  MUTEX_LOCK(&latch_);
  Defer defer{[&]() { MUTEX_UNLOCK(&latch_); }};
  while (true) {
    // If cur_block_item_ is a null pointer and attempts to call nextBlockItem to retrieve a new BlockItem for querying:
    // 1. nextBlockItem ended normally, query cur_block_item_
    // 2. If nextBlockItem returns -1, it indicates that all data on the current entity has been queried and
    //    needs to be switched to the next entity before attempting to retrieve cur_block_item_
    // 3. If all entities have been queried, the current query process ends and returns directly
    if (!cur_block_span_.block_item) {
      auto ret = nextBlockSpan();
      if (ret == NextBlkStatus::scan_over) {
        return KStatus::SUCCESS;
      } else if (ret == NextBlkStatus::error) {
        LOG_ERROR("can not get next block item.");
        return KStatus::FAIL;
      }
      continue;
    }
    uint32_t subgroup_id = cur_block_span_.subgroup_id;
    BlockItem* cur_block_item = cur_block_span_.block_item;
    TsTimePartition* p_table = cur_partition_table_[subgroup_id];
    std::shared_ptr<MMapSegmentTable> segment_tbl = p_table->getSegmentTable(cur_block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_ERROR("Can not find segment use block [%d], in path [%s]",
                cur_block_item->block_id, p_table->GetPath().c_str());
      return KStatus::FAIL;
    }
    if (segment_tbl->schemaVersion() > table_version_) {
      nextBlockSpan();
      continue;
    }
    if (nullptr == segment_iter_.second || segment_iter_.first != subgroup_id ||
        segment_iter_.second->segment_id() != segment_tbl->segment_id()) {
      if (segment_iter_.second != nullptr) {
        delete segment_iter_.second;
        segment_iter_.second = nullptr;
      }
      auto* it = new MMapSegmentTableIterator(segment_tbl, ts_spans_, kw_scan_cols_, ts_scan_cols_, attrs_);
      segment_iter_ = {subgroup_id, it};
    }

    *count = cur_block_span_.row_num;
    uint32_t first_row = cur_block_span_.start_row + 1;
    auto s = segment_iter_.second->GetBatch(cur_block_item, first_row, res, *count);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    cur_block_span_ = BlockSpan{};
    uint32_t entity_id = cur_block_item->entity_id;
    res->entity_index = {entity_group_id_, entity_id, subgroup_id};
    return SUCCESS;
  }
  KWDB_STAT_ADD(StStatistics::Get().it_num, *count);
  return SUCCESS;
}

}  // namespace kwdbts
