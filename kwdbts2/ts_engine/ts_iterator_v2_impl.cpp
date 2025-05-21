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
#include <limits>
#include <cstring>
#include "ts_vgroup.h"
#include "ts_iterator_v2_impl.h"

namespace kwdbts {

TsStorageIteratorV2Impl::TsStorageIteratorV2Impl() {
}

TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                                  std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                                  std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                                                  std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                                  uint32_t table_version) {
  vgroup_ = vgroup;
  entity_ids_ = entity_ids;
  ts_spans_ = ts_spans;
  ts_col_type_ = ts_col_type;
  kw_scan_cols_ = kw_scan_cols;
  ts_scan_cols_ = ts_scan_cols;
  table_schema_mgr_ = table_schema_mgr;
  table_version_ = table_version;
}

TsStorageIteratorV2Impl::~TsStorageIteratorV2Impl() {
}

KStatus TsStorageIteratorV2Impl::Init(bool is_reversed) {
  KStatus ret;
  ret = table_schema_mgr_->GetColumnsIncludeDropped(attrs_, table_version_);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id_);

  auto& partition_managers = vgroup_->GetPartitionManagers();
  auto it = partition_managers.find(db_id_);

  if (it != partition_managers.end() && it->second) {
    auto* partition_manager = it->second.get();
    std::unordered_map<int, std::shared_ptr<TsVGroupPartition>> partitions;
    partition_manager->GetPartitions(&partitions);

    ts_partitions_.clear();
    if (!partitions.empty()) {
      for (const auto& [idx, partition_ptr] : partitions) {
        if (!partition_ptr) continue;
        timestamp64 p_start = convertSecondToPrecisionTS(partition_ptr->StartTs(), ts_col_type_);
        timestamp64 p_end = convertSecondToPrecisionTS(partition_ptr->EndTs(), ts_col_type_);
        if (isTimestampInSpans(ts_spans_, p_start, p_end)) {
          ts_partitions_.emplace_back(partition_ptr);
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  // TODO(Yongyan): scan next batch
  return KStatus::FAIL;
}

inline KStatus TsStorageIteratorV2Impl::AddMemSegmentBlockSpans() {
  TsBlockItemFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
  return vgroup_->GetMemSegmentMgr()->GetBlockSpans(filter, ts_block_spans_);
}

inline KStatus TsStorageIteratorV2Impl::AddLastSegmentBlockSpans() {
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    std::vector<std::shared_ptr<TsLastSegment>> last_segments =
      ts_partitions_[cur_partition_index_]->GetLastSegmentMgr()->GetAllLastSegments();
    TsBlockItemFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    for (std::shared_ptr<TsLastSegment> last_segment : last_segments) {
      if (last_segment->GetBlockSpans(filter, ts_block_spans_) != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

inline KStatus TsStorageIteratorV2Impl::AddEntitySegmentBlockSpans() {
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    TsBlockItemFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    return ts_partitions_[cur_partition_index_]->GetEntitySegment()->GetBlockSpans(filter, ts_block_spans_);
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::ScanPartitionBlockSpans() {
  KStatus ret;
  if (cur_partition_index_ == 0) {
    // Scan memory segment while scanning first parition.
    ret = AddMemSegmentBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize mem segment iterator of current partition(%d) for current entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }
  }

  ret = AddLastSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to initialize last segment iterator of partition(%d) for entity(%d).",
              cur_partition_index_, entity_ids_[cur_entity_index_]);
    return KStatus::FAIL;
  }

  ret = AddEntitySegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to initialize block segment iterator of partition(%d) for entity(%d).",
              cur_partition_index_, entity_ids_[cur_entity_index_]);
    return ret;
  }

  return ret;
}

KStatus TsStorageIteratorV2Impl::ScanEntityBlockSpans() {
  KStatus ret;
  ret = AddMemSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to initialize mem segment iterator of current partition(%d) for current entity(%d).",
              cur_partition_index_, entity_ids_[cur_entity_index_]);
    return KStatus::FAIL;
  }

  for (cur_partition_index_=0; cur_partition_index_ < ts_partitions_.size(); ++cur_partition_index_) {
    ret = AddLastSegmentBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize last segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }

    ret = AddEntitySegmentBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize block segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return ret;
    }
  }
  return ret;
}

KStatus TsStorageIteratorV2Impl::ConvertBlockSpanToResultSet(shared_ptr<TsBlockSpan> ts_blk_span,
                                                              ResultSet* res, k_uint32* count) {
  *count = ts_blk_span->GetRowNum();
  KStatus ret;
  std::vector<uint32_t> blk_scan_cols;
  std::vector<AttributeInfo> blk_schema_valid;
  auto s = GetBlkScanColsInfo(ts_blk_span->GetTableVersion(), blk_scan_cols, blk_schema_valid);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  for (int i = 0; i < ts_scan_cols_.size(); ++i) {
    k_uint32 col_idx = ts_scan_cols_[i];
    auto blk_col_idx = blk_scan_cols[i];
    Batch* batch;
    if (blk_col_idx == UINT32_MAX) {
      // column is dropped at block version.
      void* bitmap = nullptr;
      batch = new Batch(bitmap, *count, bitmap, 1, nullptr);
    } else {
      unsigned char* bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(*count)));
      if (bitmap == nullptr) {
        return KStatus::FAIL;
      }
      memset(bitmap, 0x00, KW_BITMAP_SIZE(*count));
      if (!isVarLenType(attrs_[col_idx].type)) {
        TsBitmap ts_bitmap;
        char* value;
        char* res_value = static_cast<char*>(malloc(attrs_[col_idx].size * (*count)));
        ret = ts_blk_span->GetFixLenColAddr(blk_col_idx, blk_schema_valid, attrs_[col_idx], &value, ts_bitmap);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return ret;
        }
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          if (ts_bitmap[row_idx] != DataFlags::kValid) {
            set_null_bitmap(bitmap, row_idx);
          }
        }
        memcpy(res_value, value, attrs_[col_idx].size * (*count));

        batch = new Batch(static_cast<void *>(res_value), *count, bitmap, 1, nullptr);
        batch->is_new = true;
        batch->need_free_bitmap = true;
      } else {
        batch = new VarColumnBatch(*count, bitmap, 1, nullptr);
        DataFlags bitmap_var;
        TSSlice var_data;
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          ret = ts_blk_span->GetVarLenTypeColAddr(
            row_idx, blk_col_idx, blk_schema_valid, attrs_[col_idx], bitmap_var, var_data);
          if (bitmap_var != DataFlags::kValid) {
            set_null_bitmap(bitmap, row_idx);
            batch->push_back(nullptr);
          } else {
            char* buffer = static_cast<char*>(malloc(var_data.len + kStringLenLen));
            KUint16(buffer) = var_data.len;
            memcpy(buffer + kStringLenLen, var_data.data, var_data.len);
            std::shared_ptr<void> ptr(buffer, free);
            batch->push_back(ptr);
          }
        }
        batch->need_free_bitmap = true;
      }
    }
    res->push_back(i, batch);
  }
  res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};

  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::GetBlkScanColsInfo(uint32_t version,
                                 std::vector<uint32_t>& scan_cols, vector<AttributeInfo>& valid_schema) {
  std::shared_ptr<MMapMetricsTable> blk_version;
  KStatus ret = table_schema_mgr_->GetMetricSchema(version, &blk_version);
  if (ret != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed. table version [%u]", version);
    return ret;
  }
  auto& blk_schema_all = blk_version->getSchemaInfoIncludeDropped();
  valid_schema = blk_version->getSchemaInfoExcludeDropped();
  auto blk_valid_cols = blk_version->getIdxForValidCols();

  if (const auto it = blk_scan_cols_.find(version); it != blk_scan_cols_.end()) {
    scan_cols = it->second;
    return KStatus::SUCCESS;
  }

  // calculate column index in current block
  std::vector<uint32_t> blk_scan_cols;
  blk_scan_cols.resize(ts_scan_cols_.size());
  for (size_t i = 0; i < ts_scan_cols_.size(); i++) {
    if (!blk_schema_all[ts_scan_cols_[i]].isFlag(AINFO_DROPPED)) {
      bool found = false;
      size_t j = 0;
      for (; j < blk_valid_cols.size(); j++) {
        if (blk_valid_cols[j] == ts_scan_cols_[i]) {
          found = true;
          break;
        }
      }
      if (!found) {
        blk_scan_cols[i] = UINT32_MAX;
      } else {
        blk_scan_cols[i] = j;
      }
    } else {
      blk_scan_cols[i] = UINT32_MAX;
    }
  }
  blk_scan_cols_.insert({version, blk_scan_cols});
  scan_cols = blk_scan_cols;
  return KStatus::SUCCESS;
}

TsRawDataIteratorV2Impl::TsRawDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup,
                                                  vector<uint32_t>& entity_ids,
                                                  std::vector<KwTsSpan>& ts_spans,
                                                  DATATYPE ts_col_type,
                                                  std::vector<k_uint32>& kw_scan_cols,
                                                  std::vector<k_uint32>& ts_scan_cols,
                                                  std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                                  uint32_t table_version) :
                          TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type,
                                                                            kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                                            table_version) {
}

TsRawDataIteratorV2Impl::~TsRawDataIteratorV2Impl() {
}

inline KStatus TsRawDataIteratorV2Impl::NextBlockSpan(ResultSet* res, k_uint32* count) {
  shared_ptr<TsBlockSpan> ts_block = ts_block_spans_.front();
  ts_block_spans_.pop_front();
  return ConvertBlockSpanToResultSet(ts_block, res, count);
}

KStatus TsRawDataIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  KStatus ret;
  while (ts_block_spans_.empty()) {
    if (cur_entity_index_ == -1 || ++cur_partition_index_ >= ts_partitions_.size()) {
      ++cur_entity_index_;
      if (cur_entity_index_ >= entity_ids_.size()) {
        // All entities are scanned.
        *count = 0;
        *is_finished = true;
        return KStatus::SUCCESS;
      }
      cur_partition_index_ = 0;
    }
    ScanPartitionBlockSpans();
  }
  // Return one block span data each time.
  ret = NextBlockSpan(res, count);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get next block span for entity: %d, cur_partition_index_: %d.",
                entity_ids_[cur_entity_index_], cur_partition_index_);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

TsSortedRawDataIteratorV2Impl::TsSortedRawDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup,
                                                              vector<uint32_t>& entity_ids,
                                                              std::vector<KwTsSpan>& ts_spans,
                                                              DATATYPE ts_col_type,
                                                              std::vector<k_uint32>& kw_scan_cols,
                                                              std::vector<k_uint32>& ts_scan_cols,
                                                              std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                                              uint32_t table_version,
                                                              SortOrder order_type) :
                          TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type,
                                                                            kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                                            table_version) {
}

TsSortedRawDataIteratorV2Impl::~TsSortedRawDataIteratorV2Impl() {
}

KStatus TsSortedRawDataIteratorV2Impl::ScanAndSortEntityData() {
  if (cur_entity_index_ < entity_ids_.size()) {
    // scan row data for current entity
    KStatus ret = ScanEntityBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to scan block spans for entity(%d).", entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }
    if (ts_block_spans_.empty()) {
      block_span_sorted_iterator_ = nullptr;
    } else {
      // sort the block span data
      block_span_sorted_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(ts_block_spans_, is_reversed_);
      ret = block_span_sorted_iterator_->Init();
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to init block span sorted iterator for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsSortedRawDataIteratorV2Impl::MoveToNextEntity() {
  ++cur_entity_index_;
  return ScanAndSortEntityData();
}

KStatus TsSortedRawDataIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  KStatus ret;
  bool is_done = true;
  shared_ptr<TsBlockSpan> block_span;
  do {
    if (block_span_sorted_iterator_) {
      ret = block_span_sorted_iterator_->Next(block_span, &is_done);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to get next block span for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
    }
    if (cur_entity_idx_ == -1 || is_done) {
      ++cur_entity_index_;
      if (cur_entity_index_ >= entity_ids_.size()) {
        // All entities are scanned.
        *count = 0;
        *is_finished = true;
        return KStatus::SUCCESS;
      }
      ScanAndSortEntityData();
    }
  } while (is_done);

  return ConvertBlockSpanToResultSet(block_span, res, count);
}

TsAggIteratorV2Impl::TsAggIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                          std::vector<KwTsSpan>& ts_spans, DATATYPE ts_col_type,
                                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                                          std::vector<Sumfunctype>& scan_agg_types, std::vector<timestamp64>& ts_points,
                                          std::shared_ptr<TsTableSchemaManager> table_schema_mgr, uint32_t table_version) :
                          TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(vgroup, entity_ids, ts_spans, ts_col_type,
                                                                            kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                                            table_version),
                          scan_agg_types_(scan_agg_types) {
}

TsAggIteratorV2Impl::~TsAggIteratorV2Impl() {
}

inline bool TsAggIteratorV2Impl::onlyHasFirstLastAggType() {
  for (auto& agg_type : scan_agg_types_) {
    if (!(IsFirstAggType(agg_type) || IsLastAggType(agg_type))) {
      return false;
    }
  }
  return true;
}

inline bool TsAggIteratorV2Impl::onlyCountTs() {
  if (CLUSTER_SETTING_COUNT_USE_STATISTICS && scan_agg_types_.size() == 1
      && scan_agg_types_[0] == Sumfunctype::COUNT && ts_scan_cols_.size() == 1 && ts_scan_cols_[0] == 0) {
    return true;
  }
  return false;
}

KStatus TsAggIteratorV2Impl::Init(bool is_reversed) {
  KStatus s = TsStorageIteratorV2Impl::Init(is_reversed);
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
    /* if a col is not null type, we can change the first/last to first_row/last_row to speed up the aggregation
     * which also can be done during query optimization.
     */
  }
  only_first_last_type_ = onlyHasFirstLastAggType();
  only_count_ts_ = onlyCountTs();
  cur_entity_index_ = 0;
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  std::vector<TSSlice> final_agg_data(ts_scan_cols_.size(), TSSlice{nullptr, 0});
  std::vector<bool> is_overflow(ts_scan_cols_.size(), false);

  KStatus ret;
  // If only queries related to first/last aggregation types are involved, the optimization process can be followed.
  ret = only_first_last_type_ ? AggregateFirstLastOnly(final_agg_data)
                              : AggregateBlockSpans(final_agg_data, is_overflow);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  res->clear();
  for (k_uint32 i = 0; i < ts_scan_cols_.size(); ++i) {
    TSSlice& slice = final_agg_data[i];
    Batch* b;
    if (slice.data == nullptr) {
      b = new AggBatch(nullptr, 0, nullptr);
    } else if (!isVarLenType(attrs_[ts_scan_cols_[i]].type) || scan_agg_types_[i] == Sumfunctype::COUNT) {
      b = new AggBatch(slice.data, 1, nullptr);
      b->is_new = true;
      b->is_overflow = is_overflow[i];
    } else {
      std::shared_ptr<void> ptr(slice.data, free);
      b = new AggBatch(ptr, 1, nullptr);
    }
    res->push_back(i, b);
  }

  res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};
  res->col_num_ = ts_scan_cols_.size();
  *count = 1;

  *is_finished = false;
  ++cur_entity_index_;
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::AggregateBlockSpans(std::vector<TSSlice>& final_agg_data, std::vector<bool>& is_overflow) {
  KStatus ret = AddMemSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to initialize mem segment iterator of current partition(%d) for current entity(%d).",
              cur_partition_index_, entity_ids_[cur_entity_index_]);
    return KStatus::FAIL;
  }

  for (cur_partition_index_=0; cur_partition_index_ < ts_partitions_.size(); ++cur_partition_index_) {
    ret = AddLastSegmentBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize last segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }

    ret = AddEntitySegmentBlockSpans();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize block segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return ret;
    }
  }


  std::vector<k_uint32> first_cols;
  std::vector<k_uint32> last_cols;
  std::vector<k_uint32> normal_cols;
  for (size_t i = 0; i < scan_agg_types_.size(); ++i) {
    if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
      last_cols.push_back(i);
    } else if (scan_agg_types_[i] == Sumfunctype::FIRST || scan_agg_types_[i] == Sumfunctype::FIRSTTS) {
      first_cols.push_back(i);
    } else {
      normal_cols.push_back(i);
      if (scan_agg_types_[i] == Sumfunctype::COUNT) {
        final_agg_data[i].len = sizeof(uint64_t);
        final_agg_data[i].data = static_cast<char*>(malloc(final_agg_data[i].len));
        memset(final_agg_data[i].data, 0, final_agg_data[i].len);
      }
    }
  }

  while (!ts_block_spans_.empty()) {
    shared_ptr<TsBlockSpan> blk_span = ts_block_spans_.front();
    ts_block_spans_.pop_front();

    std::vector<uint32_t> blk_scan_cols;
    std::vector<AttributeInfo> blk_schema_valid;
    auto s = GetBlkScanColsInfo(blk_span->GetTableVersion(), blk_scan_cols, blk_schema_valid);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    for (k_uint32 idx : normal_cols) {
      bool overflow = is_overflow[idx];
      s = blk_span->GetAggResult(blk_scan_cols[idx], blk_schema_valid, attrs_[blk_scan_cols[idx]],
                                scan_agg_types_[idx], final_agg_data[idx], overflow);

      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Failed to compute aggregation for col_idx(%u)",
                  blk_scan_cols[idx]);
        return s;
      }
      is_overflow[idx] = overflow;
    }
  }

  return KStatus::SUCCESS;
}

inline bool PartitionLessThan(std::shared_ptr<TsVGroupPartition>& a, std::shared_ptr<TsVGroupPartition> b) {
  return a->StartTs() < b->StartTs();
}

KStatus TsAggIteratorV2Impl::AggregateFirstLastOnly(std::vector<TSSlice>& final_agg_data) {
  std::vector<AggCandidate> candidates(scan_agg_types_.size());
  std::list<k_uint32> first_col_idxs;
  std::list<k_uint32> last_col_idxs;
  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    if (scan_agg_types_[i] == Sumfunctype::FIRST || scan_agg_types_[i] == Sumfunctype::FIRSTTS) {
      first_col_idxs.push_back(i);
    } else if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
      last_col_idxs.push_back(i);
    }
  }
  KStatus ret = AddMemSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  ret = UpdateFirstLastCandidates(first_col_idxs, last_col_idxs, candidates);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  
  std::sort(ts_partitions_.begin(), ts_partitions_.end(), PartitionLessThan);

  int first_partition_idx;
  for (first_partition_idx = 0; first_partition_idx < ts_partitions_.size(); ++first_partition_idx) {
    if (first_col_idxs.size() > 0) {
      // There are still some first agg functions without candidates, so we need to scan current partition.
      cur_partition_index_ = first_partition_idx;
      ret = AddLastSegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = AddEntitySegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = UpdateFirstLastCandidates(first_col_idxs, last_col_idxs, candidates);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    } else {
      // We already got the first candidates for all first agg functions
      break;
    }
  }

  int last_partition_idx;
  for (last_partition_idx = ts_partitions_.size() - 1; last_partition_idx >= first_partition_idx; --last_partition_idx) {
    if (last_col_idxs.size() > 0) {
      // There are still some last agg functions without candidates, so we need to scan current partition.
      cur_partition_index_ = last_partition_idx;
      ret = AddLastSegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = AddEntitySegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = UpdateFirstLastCandidates(first_col_idxs, last_col_idxs, candidates);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    } else {
      // We already got the last candidates for all last agg functions
      break;
    }
  }

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    const auto& c = candidates[i];
    const k_uint32 col_idx = ts_scan_cols_[i];
    final_agg_data[i].len = attrs_[col_idx].size;
    if (c.blk_span == nullptr) {
      final_agg_data[i] = {nullptr, 0};
    } else if (scan_agg_types_[i] == Sumfunctype::FIRSTTS || scan_agg_types_[i] == Sumfunctype::LASTTS) {
      final_agg_data[i].data = static_cast<char*>(malloc(sizeof(int64_t)));
      memcpy(final_agg_data[i].data, &c.ts, sizeof(int64_t));
      final_agg_data[i].len = sizeof(int64_t);
    } else {
      std::shared_ptr<MMapMetricsTable> blk_version;
      ret = table_schema_mgr_->GetMetricSchema(c.blk_span->GetTableVersion(), &blk_version);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      auto& schema_info = blk_version->getSchemaInfoExcludeDropped();
      TSBlkDataTypeConvert convert(c.blk_span->GetTsBlock().get(), c.blk_span->GetStartRow(), c.blk_span->GetRowNum());
      if (!isVarLenType(attrs_[col_idx].type)) {
        char* value = nullptr;
        TsBitmap bitmap;
        ret = convert.GetFixLenColAddr(col_idx, schema_info,
                                       attrs_[col_idx], &value, bitmap);
        if (ret != KStatus::SUCCESS) return ret;

        final_agg_data[i].data = static_cast<char*>(malloc(final_agg_data[i].len));
        memcpy(final_agg_data[i].data,
               value + c.row_idx * final_agg_data[i].len,
               final_agg_data[i].len);
      } else {
        TSSlice slice;
        DataFlags flag;
        ret = convert.GetVarLenTypeColAddr(c.row_idx, col_idx, schema_info,
                                           attrs_[col_idx], flag, slice);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetVarLenTypeColAddr failed.");
          return ret;
        }
        final_agg_data[i].len = slice.len + kStringLenLen;
        final_agg_data[i].data = static_cast<char*>(malloc(final_agg_data[i].len));
        KUint16(final_agg_data[i].data) = slice.len;
        memcpy(final_agg_data[i].data + kStringLenLen, slice.data, slice.len);
      }
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::UpdateFirstLastCandidates(std::shared_ptr<TsBlockSpan>& block_span,
                                                        const std::vector<AttributeInfo>& schema,
                                                        std::list<uint32_t>& first_col_idxs,
                                                        std::list<uint32_t>& last_col_idxs,
                                                        bool remove_last_col_with_candidate,
                                                        std::vector<AggCandidate>& candidates) {
  KStatus ret;
  int row_num = block_span->GetRowNum();
  if (!first_col_idxs.empty()) {
    int first_col_num = first_col_idxs.size();
    for (int i = 0; i < first_col_num; ++i) {
      uint32_t first_col_idx = first_col_idxs.front();
      first_col_idxs.pop_front();
      uint32_t blk_col_idx = ts_scan_cols_[first_col_idx];
      AggCandidate& candidate = candidates[first_col_idx];
      TsBitmap bitmap;
      ret = block_span->GetColBitmap(blk_col_idx, schema, bitmap);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      for (int row_idx = 0; row_idx < row_num; ++row_idx) {
        if (bitmap[i] == DataFlags::kNull) {
          continue;
        }
        int64_t ts = block_span->GetTS(row_idx);
        if (!candidate.blk_span || candidate.ts > ts) {
          candidate.blk_span = block_span;
          candidate.ts = ts;
          candidate.row_idx = row_idx;
          break;
        }
      }
      if (!candidate.blk_span) {
        // Candidate is not found yet, so need to put first col index back.
        first_col_idxs.push_back(first_col_idx);
      }
    }
  }

  if (!last_col_idxs.empty()) {
    int last_col_num = last_col_idxs.size();
    for (int i = 0; i < last_col_num; ++i) {
      uint32_t last_col_idx = last_col_idxs.front();
      last_col_idxs.pop_front();
      uint32_t blk_col_idx = ts_scan_cols_[last_col_idx];
      AggCandidate& candidate = candidates[last_col_idx];
      TsBitmap bitmap;
      ret = block_span->GetColBitmap(blk_col_idx, schema, bitmap);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      for (int row_idx = row_num - 1; row_idx >= 0; --row_idx) {
        if (bitmap[i] == DataFlags::kNull) {
          continue;
        }
        int64_t ts = block_span->GetTS(row_idx);
        if (!candidate.blk_span || candidate.ts < ts) {
          candidate.blk_span = block_span;
          candidate.ts = ts;
          candidate.row_idx = row_idx;
          break;
        }
      }
      if (!candidate.blk_span || !remove_last_col_with_candidate) {
        // Candidate is not found yet, so need to put last col index back.
        last_col_idxs.push_back(last_col_idx);
      }
    }
  }
  return KStatus::SUCCESS;
}
        
inline bool BlockSpanLessThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetFirstTS() < b->GetFirstTS();
}

inline bool BlockSpanGreaterThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetLastTS() > b->GetLastTS();
}
                                                  
KStatus TsAggIteratorV2Impl::UpdateFirstLastCandidates(std::list<k_uint32>& first_col_idxs,
                                                        std::list<k_uint32>& last_col_idxs,
                                                        std::vector<AggCandidate>& candidates) {
  if (ts_block_spans_.empty()) {
    return KStatus::SUCCESS;
  }
  KStatus ret;
  std::vector<shared_ptr<TsBlockSpan>> ts_block_spans;
  while (!ts_block_spans_.empty()) {
    ts_block_spans.push_back(ts_block_spans_.front());
    ts_block_spans_.pop_front();
  }

  int block_span_idx = 0;
  if (!first_col_idxs.empty()) {
    sort(ts_block_spans.begin(), ts_block_spans.end(), BlockSpanLessThan);
    while (!first_col_idxs.empty() && block_span_idx < ts_block_spans.size()) {
      shared_ptr<TsBlockSpan> blk_span = ts_block_spans[block_span_idx];

      std::shared_ptr<MMapMetricsTable> blk_version;
      ret = table_schema_mgr_->GetMetricSchema(blk_span->GetTableVersion(), &blk_version);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

      ret = UpdateFirstLastCandidates(blk_span, schema_info, first_col_idxs, last_col_idxs, false, candidates);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ++block_span_idx;
    }
  }

  if (!last_col_idxs.empty()) {
    sort(ts_block_spans.begin() + block_span_idx, ts_block_spans.end(), BlockSpanGreaterThan);
    int block_span_backward_idx = ts_block_spans.size() - 1;
    while (last_col_idxs.size() > 0 && block_span_backward_idx >= block_span_idx) {
      shared_ptr<TsBlockSpan> blk_span = ts_block_spans[block_span_backward_idx];
      ts_block_spans.pop_back();

      std::shared_ptr<MMapMetricsTable> blk_version;
      KStatus ret = table_schema_mgr_->GetMetricSchema(blk_span->GetTableVersion(), &blk_version);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

      ret = UpdateFirstLastCandidates(blk_span, schema_info, first_col_idxs, last_col_idxs, true, candidates);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      --block_span_backward_idx;
    }
  }

  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
