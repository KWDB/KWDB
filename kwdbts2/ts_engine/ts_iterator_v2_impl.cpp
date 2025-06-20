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
#include "ts_entity_partition.h"
#include "engine.h"
#include "ee_global.h"

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
  is_reversed_ = is_reversed;
  KStatus ret;
  ret = table_schema_mgr_->GetColumnsIncludeDropped(attrs_, table_version_);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id_);

  auto current = vgroup_->CurrentVersion();
  auto partitions_v = current->GetPartitions(db_id_);

  ts_partitions_.clear();
  for (const auto& partition_ptr : partitions_v) {
    TsPartition ts_partition;
    ts_partition.ts_partition_range.begin = convertSecondToPrecisionTS(partition_ptr->GetStartTime(), ts_col_type_);
    ts_partition.ts_partition_range.end = convertSecondToPrecisionTS(partition_ptr->GetEndTime(), ts_col_type_);
    if (isTimestampInSpans(ts_spans_, ts_partition.ts_partition_range.begin, ts_partition.ts_partition_range.end)) {
      ts_partition.ts_partition_version = partition_ptr;
      ts_partitions_.emplace_back(ts_partition);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  // TODO(Yongyan): scan next batch
  return KStatus::FAIL;
}

inline void TsStorageIteratorV2Impl::UpdateTsSpans(timestamp64 ts) {
  if (ts != INVALID_TS && !ts_spans_.empty()) {
    if (!is_reversed_) {
      int i = ts_spans_.size() - 1;
      while (i >= 0 && ts_spans_[i].begin > ts) {
        --i;
      }
      if (i >= 0) {
        ts_spans_[i].end = min(ts_spans_[i].end, ts);
      }
      if (i < ts_spans_.size() - 1) {
        ts_spans_.erase(ts_spans_.begin() + (i + 1), ts_spans_.end());
      }
    } else {
      int i = 0;
      while (i < ts_spans_.size() && ts_spans_[i].end < ts) {
        ++i;
      }
      if (i < ts_spans_.size()) {
        ts_spans_[i].begin = max(ts_spans_[i].begin, ts);
      }
      if (i > 0) {
        ts_spans_.erase(ts_spans_.begin(), ts_spans_.begin() + (i - 1));
      }
    }
  }
}

inline bool TsStorageIteratorV2Impl::IsFilteredOut(timestamp64 begin_ts, timestamp64 end_ts, timestamp64 ts) {
  return ts != INVALID_TS && (!is_reversed_ && begin_ts > ts || is_reversed_ && end_ts < ts);
}

KStatus TsStorageIteratorV2Impl::ScanPartitionBlockSpans() {
  KStatus ret = KStatus::SUCCESS;
  /*
   * TODO(Yongyan): Refacter scanning partition block span to scan
   * memory segment data under partition after ts version is implemented.
   */
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
    TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
    std::list<std::shared_ptr<TsMemSegment>> mems;
    vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
    ret = e_paritition.Init(mems);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetAllMemSegments failed.");
      return ret;
    }
    ret = e_paritition.GetBlockSpan(&ts_block_spans_);
  }
  return ret;
}

KStatus TsStorageIteratorV2Impl::ScanPartitionBlockSpans(timestamp64 ts) {
  KStatus ret = KStatus::SUCCESS;
  /*
   * TODO(Yongyan): Refacter scanning partition block span to scan
   * memory segment data under partition after ts version is implemented.
   */
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
    if (IsFilteredOut(ts_partitions_[cur_partition_index_].ts_partition_range.begin,
                      ts_partitions_[cur_partition_index_].ts_partition_range.end, ts))  {
      partition_version = nullptr;
    }
    TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
    std::list<std::shared_ptr<TsMemSegment>> mems;
    vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
    ret = e_paritition.Init(mems);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetAllMemSegments failed.");
      return ret;
    }
    ret = e_paritition.GetBlockSpan(&ts_block_spans_);
  }
  return ret;
}

KStatus TsStorageIteratorV2Impl::ScanEntityBlockSpans(timestamp64 ts) {
  ts_block_spans_.clear();
  UpdateTsSpans(ts);
  for (cur_partition_index_ = 0; cur_partition_index_ < ts_partitions_.size(); ++cur_partition_index_) {
    TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
    if (IsFilteredOut(ts_partitions_[cur_partition_index_].ts_partition_range.begin,
                      ts_partitions_[cur_partition_index_].ts_partition_range.end, ts))  {
      partition_version = nullptr;
    }
    TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
    std::list<std::shared_ptr<TsMemSegment>> mems;
    vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
    auto ret = e_paritition.Init(mems);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetAllMemSegments failed.");
      return ret;
    }
    std::list<std::shared_ptr<TsBlockSpan>> cur_block_span;
    ret = e_paritition.GetBlockSpan(&cur_block_span);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
    ts_block_spans_.splice(ts_block_spans_.begin(), cur_block_span);
  }

  return KStatus::SUCCESS;
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

inline KStatus TsRawDataIteratorV2Impl::NextBlockSpan(ResultSet* res, k_uint32* count, timestamp64 ts) {
  while (!ts_block_spans_.empty()) {
    shared_ptr<TsBlockSpan> ts_block = ts_block_spans_.front();
    ts_block_spans_.pop_front();
    if (!IsFilteredOut(ts_block->GetFirstTS(), ts_block->GetLastTS(), ts)) {
      return ConvertBlockSpanToResultSet(ts_block, res, count);
    }
  }
  *count = 0;
  return KStatus::SUCCESS;
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
    ScanPartitionBlockSpans(ts);
  }
  // Return one block span data each time.
  ret = NextBlockSpan(res, count, ts);
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

KStatus TsSortedRawDataIteratorV2Impl::ScanAndSortEntityData(timestamp64 ts) {
  if (cur_entity_index_ < entity_ids_.size()) {
    // scan row data for current entity
    KStatus ret = ScanEntityBlockSpans(ts);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to scan block spans for entity(%d).", entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }
    if (ts_block_spans_.empty()) {
      block_span_sorted_iterator_ = nullptr;
    } else {
      // sort the block span data
      block_span_sorted_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(ts_block_spans_, EngineOptions::g_dedup_rule,
                                                                                is_reversed_);
      ret = block_span_sorted_iterator_->Init();
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to init block span sorted iterator for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

inline KStatus TsSortedRawDataIteratorV2Impl::MoveToNextEntity(timestamp64 ts) {
  ++cur_entity_index_;
  return ScanAndSortEntityData(ts);
}

KStatus TsSortedRawDataIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  KStatus ret;
  *count = 0;
  if (cur_entity_index_ == -1) {
    ret = MoveToNextEntity(ts);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }
  if (cur_entity_index_ >= entity_ids_.size()) {
    // All entities are scanned.
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  bool is_done = true;
  shared_ptr<TsBlockSpan> block_span;
  if (block_span_sorted_iterator_) {
    do {
      ret = block_span_sorted_iterator_->Next(block_span, &is_done);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to get next block span for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
      if (!is_done && !IsFilteredOut(block_span->GetFirstTS(), block_span->GetLastTS(), ts)) {
        // Found a block span which might contain satisfied rows.
        ret = ConvertBlockSpanToResultSet(block_span, res, count);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        if (*count > 0) {
          // Return the result set.
          return KStatus::SUCCESS;
        }
      }
    } while (!is_done);
    // No more satisfied rows found, we need to return 0 count for current entity.
    return MoveToNextEntity(ts);
  } else {
    // No satisfied rows found, we need to return 0 count for current entity.
    return MoveToNextEntity(ts);
  }
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
  last_ts_points_ = ts_points;
}

TsAggIteratorV2Impl::~TsAggIteratorV2Impl() {
}

inline bool PartitionLessThan(TsPartition& a, TsPartition& b) {
  return a.ts_partition_version->GetStartTime() < b.ts_partition_version->GetEndTime();
}

KStatus TsAggIteratorV2Impl::Init(bool is_reversed) {
  KStatus s = TsStorageIteratorV2Impl::Init(is_reversed);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  /* if a col is not null type, we can change the first/last to first_row/last_row to speed up the aggregation
   * which also can be done during query optimization.
   */

  final_agg_data_.resize(ts_scan_cols_.size(), TSSlice{nullptr, 0});
  is_overflow_.reserve(ts_scan_cols_.size());

  has_first_row_col_ = false;
  has_last_row_col_ = false;
  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    switch (scan_agg_types_[i]) {
      case Sumfunctype::LAST:
      case Sumfunctype::LASTTS:
        if (last_ts_points_.empty()) {
          if (last_map_.find(ts_scan_cols_[i]) == last_map_.end()) {
            last_col_idxs_.push_back(i);
            last_map_[ts_scan_cols_[i]] = i;
          }
        } else {
          last_col_idxs_.push_back(i);
        }
        break;
      case Sumfunctype::FIRST:
      case Sumfunctype::FIRSTTS:
        if (first_map_.find(ts_scan_cols_[i]) == first_map_.end()) {
          first_col_idxs_.push_back(i);
          first_map_[ts_scan_cols_[i]] = i;
        }
        break;
      case Sumfunctype::COUNT:
        count_col_idxs_.push_back(i);
        break;
      case Sumfunctype::SUM:
        sum_col_idxs_.push_back(i);
        break;
      case Sumfunctype::MAX:
        max_col_idxs_.push_back(i);
        break;
      case Sumfunctype::MIN:
        min_col_idxs_.push_back(i);
        break;
      case Sumfunctype::LAST_ROW:
      case Sumfunctype::LASTROWTS:
        has_last_row_col_ = true;
        break;
      case Sumfunctype::FIRST_ROW:
      case Sumfunctype::FIRSTROWTS:
        has_first_row_col_ = true;
        break;
      default:
        LOG_ERROR("Agg function type is not supported in storage engine: %d.", scan_agg_types_[i]);
        return KStatus::FAIL;
        break;
    }
  }
  candidates_.resize(ts_scan_cols_.size());
  for (auto first_col_idx : first_col_idxs_) {
    candidates_[first_col_idx].ts = INT64_MAX;
  }
  for (auto last_col_idx : last_col_idxs_) {
    candidates_[last_col_idx].ts = INT64_MIN;
  }
  first_last_only_agg_ = (count_col_idxs_.size() + sum_col_idxs_.size() + max_col_idxs_.size() + min_col_idxs_.size() == 0);

  if (first_col_idxs_.size() > 0 || last_col_idxs_.size() > 0 || has_first_row_col_ || has_last_row_col_) {
    std::sort(ts_partitions_.begin(), ts_partitions_.end(), PartitionLessThan);
  }

  max_first_ts_ = (first_col_idxs_.size() > 0 || has_first_row_col_) ? INT64_MAX : INT64_MIN;
  min_last_ts_ = (last_col_idxs_.size() > 0 || has_last_row_col_) ? INT64_MIN : INT64_MAX;

  cur_entity_index_ = 0;

  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  final_agg_data_.clear();
  final_agg_data_.resize(ts_scan_cols_.size(), TSSlice{nullptr, 0});
  for (auto first_col_idx : first_col_idxs_) {
    candidates_[first_col_idx].blk_span = nullptr;
  }
  for (auto last_col_idx : last_col_idxs_) {
    candidates_[last_col_idx].blk_span = nullptr;
  }
  is_overflow_.clear();
  is_overflow_.resize(ts_scan_cols_.size(), false);

  for (auto count_col_idx : count_col_idxs_) {
    final_agg_data_[count_col_idx].len = sizeof(uint64_t);
    final_agg_data_[count_col_idx].data = static_cast<char*>(malloc(final_agg_data_[count_col_idx].len));
    memset(final_agg_data_[count_col_idx].data, 0, final_agg_data_[count_col_idx].len);
  }

  if (has_first_row_col_) {
    first_row_candidate_.blk_span = nullptr;
  }
  if (has_last_row_col_) {
    last_row_candidate_.blk_span = nullptr;
  }

  KStatus ret;
  ret = Aggregate();
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  res->clear();
  for (k_uint32 i = 0; i < ts_scan_cols_.size(); ++i) {
    TSSlice& slice = final_agg_data_[i];
    Batch* b;
    if (slice.data == nullptr) {
      b = new AggBatch(nullptr, 0, nullptr);
    } else if (!isVarLenType(attrs_[ts_scan_cols_[i]].type) || scan_agg_types_[i] == Sumfunctype::COUNT) {
      b = new AggBatch(slice.data, 1, nullptr);
      b->is_new = true;
      b->is_overflow = is_overflow_[i];
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

KStatus TsAggIteratorV2Impl::Aggregate() {
  int first_partition_idx = 0;
  for (; first_partition_idx < ts_partitions_.size(); ++first_partition_idx) {
    if (ts_partitions_[first_partition_idx].ts_partition_range.begin < max_first_ts_) {
      cur_partition_index_ = first_partition_idx;
      TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
      auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
      TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
      std::list<std::shared_ptr<TsMemSegment>> mems;
      vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
      auto ret = e_paritition.Init(mems);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetAllMemSegments failed.");
        return ret;
      }
      ret = e_paritition.GetBlockSpan(&ts_block_spans_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      ret = UpdateAggregation();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    } else {
      break;
    }
  }

  int last_partition_idx = ts_partitions_.size() - 1;
  for (; last_partition_idx >= first_partition_idx; --last_partition_idx) {
    if (ts_partitions_[last_partition_idx].ts_partition_range.end > min_last_ts_) {
      cur_partition_index_ = last_partition_idx;
      TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
      auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
      TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
      std::list<std::shared_ptr<TsMemSegment>> mems;
      vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
      auto ret = e_paritition.Init(mems);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetAllMemSegments failed.");
        return ret;
      }
      ret = e_paritition.GetBlockSpan(&ts_block_spans_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      ret = UpdateAggregation();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    } else {
      break;
    }
  }

  if (!first_last_only_agg_) {
    for (; first_partition_idx <= last_partition_idx; ++first_partition_idx) {
      cur_partition_index_ = first_partition_idx;
      TsScanFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
      auto partition_version = ts_partitions_[cur_partition_index_].ts_partition_version;
      TsEntityPartition e_paritition(partition_version, scan_lsn_, ts_col_type_, filter);
      std::list<std::shared_ptr<TsMemSegment>> mems;
      vgroup_->GetMemSegmentMgr()->GetAllMemSegments(&mems);
      auto ret = e_paritition.Init(mems);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetAllMemSegments failed.");
        return ret;
      }
      ret = e_paritition.GetBlockSpan(&ts_block_spans_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      ret = UpdateAggregation();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    Sumfunctype agg_type = scan_agg_types_[i];
    if (agg_type == Sumfunctype::COUNT || agg_type == Sumfunctype::SUM
        || agg_type == Sumfunctype::MAX || agg_type ==Sumfunctype::MIN) {
      continue;
    }
    const auto& c = (agg_type == Sumfunctype::FIRST || agg_type == Sumfunctype::FIRSTTS) ?
                    candidates_[first_map_[ts_scan_cols_[i]]] :
                      ((agg_type == Sumfunctype::LAST || agg_type == Sumfunctype::LASTTS) ?
                      candidates_[last_ts_points_.empty() ? last_map_[ts_scan_cols_[i]] : i] :
                        ((agg_type == Sumfunctype::FIRST_ROW || agg_type == Sumfunctype::FIRSTROWTS) ?
                          first_row_candidate_ : last_row_candidate_));
    const k_uint32 col_idx = ts_scan_cols_[i];
    final_agg_data_[i].len = attrs_[col_idx].size;
    if (c.blk_span == nullptr) {
      final_agg_data_[i] = {nullptr, 0};
    } else if (agg_type == Sumfunctype::FIRSTTS || agg_type == Sumfunctype::LASTTS
              || agg_type == Sumfunctype::FIRSTROWTS || agg_type == Sumfunctype::LASTROWTS) {
      final_agg_data_[i].data = static_cast<char*>(malloc(sizeof(timestamp64)));
      memcpy(final_agg_data_[i].data, &c.ts, sizeof(timestamp64));
      final_agg_data_[i].len = sizeof(timestamp64);
    } else {
      std::shared_ptr<MMapMetricsTable> blk_version;
      auto ret = table_schema_mgr_->GetMetricSchema(c.blk_span->GetTableVersion(), &blk_version);
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
        if (ret != KStatus::SUCCESS) {
          return ret;
        }

        if (bitmap[c.row_idx] != DataFlags::kValid) {
          final_agg_data_[i] = {nullptr, 0};
        } else {
          final_agg_data_[i].data = static_cast<char*>(malloc(final_agg_data_[i].len));
          memcpy(final_agg_data_[i].data,
                value + c.row_idx * final_agg_data_[i].len,
                final_agg_data_[i].len);
        }
      } else {
        TSSlice slice;
        DataFlags flag;
        ret = convert.GetVarLenTypeColAddr(c.row_idx, col_idx, schema_info,
                                           attrs_[col_idx], flag, slice);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetVarLenTypeColAddr failed.");
          return ret;
        }
        if (flag != DataFlags::kValid) {
          final_agg_data_[i] = {nullptr, 0};
        } else {
          final_agg_data_[i].len = slice.len + kStringLenLen;
          final_agg_data_[i].data = static_cast<char*>(malloc(final_agg_data_[i].len));
          KUint16(final_agg_data_[i].data) = slice.len;
          memcpy(final_agg_data_[i].data + kStringLenLen, slice.data, slice.len);
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

inline void TsAggIteratorV2Impl::UpdateTsSpans() {
  if (first_last_only_agg_) {
    if (max_first_ts_ < min_last_ts_) {
      int ts_span_idx = 0;
      while (ts_span_idx < ts_spans_.size() && ts_spans_[ts_span_idx].end < max_first_ts_) {
        ++ts_span_idx;
      }

      if (ts_span_idx < ts_spans_.size()) {
        if (ts_spans_[ts_span_idx].begin < max_first_ts_) {
          if (ts_spans_[ts_span_idx].end > min_last_ts_) {
            ts_spans_.insert(ts_spans_.begin() + ts_span_idx + 1, {max_first_ts_, ts_spans_[ts_span_idx].end});
          }
          ts_spans_[ts_span_idx].end = max_first_ts_;
          ++ts_span_idx;
        }
        while (ts_span_idx < ts_spans_.size() && ts_spans_[ts_span_idx].end <= min_last_ts_) {
          ts_spans_.erase(ts_spans_.begin() + ts_span_idx);
        }
        if (ts_span_idx < ts_spans_.size()) {
          ts_spans_[ts_span_idx].begin = max(ts_spans_[ts_span_idx].begin, min_last_ts_);
        }
      }
    }
  }
}

inline bool FirstTSLessThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetFirstTS() < b->GetFirstTS();
}

inline bool LastTSLessThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetLastTS() < b->GetLastTS();
}

KStatus TsAggIteratorV2Impl::UpdateAggregation() {
  if (ts_block_spans_.empty()) {
    return KStatus::SUCCESS;
  }
  KStatus ret;

  std::vector<shared_ptr<TsBlockSpan>> ts_block_spans;
  TsBlockSpanSortedIterator iter(ts_block_spans_, EngineOptions::g_dedup_rule);
  iter.Init();
  std::shared_ptr<TsBlockSpan> dedup_block_span;
  bool is_finished = false;
  while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
    ts_block_spans.push_back(dedup_block_span);
  }
  ts_block_spans_.clear();
  dedup_block_span = nullptr;

  int block_span_idx = 0;
  if (!first_col_idxs_.empty() || has_first_row_col_) {
    if (first_col_idxs_.empty()) {
      auto min_it = std::min_element(ts_block_spans.begin(), ts_block_spans.end(), LastTSLessThan);
      if (first_row_candidate_.ts > (*min_it)->GetFirstTS()) {
        first_row_candidate_.blk_span = *min_it;
        first_row_candidate_.ts = first_row_candidate_.blk_span->GetFirstTS();
        first_row_candidate_.row_idx = 0;
        max_first_ts_ = first_row_candidate_.ts;
      }
    } else {
      sort(ts_block_spans.begin(), ts_block_spans.end(), FirstTSLessThan);
      if (has_first_row_col_) {
        if (first_row_candidate_.ts > ts_block_spans[0]->GetFirstTS()) {
          first_row_candidate_.blk_span = ts_block_spans[0];
          first_row_candidate_.ts = first_row_candidate_.blk_span->GetFirstTS();
          first_row_candidate_.row_idx = 0;
        }
      }
      while (block_span_idx < ts_block_spans.size()) {
        shared_ptr<TsBlockSpan> blk_span = ts_block_spans[block_span_idx];
        if (blk_span->GetFirstTS() < max_first_ts_) {
          std::shared_ptr<MMapMetricsTable> blk_version;
          ret = table_schema_mgr_->GetMetricSchema(blk_span->GetTableVersion(), &blk_version);
          if (ret != KStatus::SUCCESS) {
            return ret;
          }
          auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

          ret = UpdateAggregation(blk_span, schema_info);
          if (ret != KStatus::SUCCESS) {
            return ret;
          }
          ++block_span_idx;
        } else {
          break;
        }
      }
    }
  }

  int block_span_backward_idx = ts_block_spans.size() - 1;;
  if (has_last_row_col_) {
    auto max_it = std::max_element(ts_block_spans.begin(), ts_block_spans.end(), LastTSLessThan);
    if (last_row_candidate_.ts < (*max_it)->GetLastTS()) {
      last_row_candidate_.blk_span = *max_it;
      last_row_candidate_.ts = last_row_candidate_.blk_span->GetLastTS();
      last_row_candidate_.row_idx = last_row_candidate_.blk_span->GetRowNum() - 1;
      if (last_col_idxs_.size() == 0) {
        min_last_ts_ = last_row_candidate_.ts;
      }
    }
  }
  if (last_col_idxs_.size() > 0) {
    sort(ts_block_spans.begin() + block_span_idx, ts_block_spans.end(), LastTSLessThan);
    while (block_span_idx <= block_span_backward_idx) {
      shared_ptr<TsBlockSpan> blk_span = ts_block_spans[block_span_backward_idx];
      if (blk_span->GetLastTS() > min_last_ts_) {
        std::shared_ptr<MMapMetricsTable> blk_version;
        KStatus ret = table_schema_mgr_->GetMetricSchema(blk_span->GetTableVersion(), &blk_version);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

        ret = UpdateAggregation(blk_span, schema_info);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        --block_span_backward_idx;
      } else {
        break;
      }
    }
  }

  if (!first_last_only_agg_) {
    for (; block_span_idx <= block_span_backward_idx; ++block_span_idx) {
      shared_ptr<TsBlockSpan> blk_span = ts_block_spans[block_span_idx];

      std::shared_ptr<MMapMetricsTable> blk_version;
      KStatus ret = table_schema_mgr_->GetMetricSchema(blk_span->GetTableVersion(), &blk_version);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

      ret = UpdateAggregation(blk_span, schema_info);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }
  UpdateTsSpans();
  return KStatus::SUCCESS;
}

inline void TsAggIteratorV2Impl::InitAggData(TSSlice& agg_data) {
  agg_data.data = static_cast<char*>(malloc(agg_data.len));
  memset(agg_data.data, 0, agg_data.len);
}

inline void TsAggIteratorV2Impl::InitSumValue(void* data, int32_t type) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::INT16:
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
    case DATATYPE::INT64:
      *static_cast<int64_t*>(data) = 0;
      break;
    case DATATYPE::FLOAT:
    case DATATYPE::DOUBLE:
      *static_cast<double*>(data) = 0.0;
      break;
    default:
      break;
  }
}

inline int TsAggIteratorV2Impl::valcmp(void* l, void* r, int32_t type, int32_t size) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
    case DATATYPE::BOOL:
    case DATATYPE::BINARY: {
      k_int32 ret = memcmp(l, r, size);
      return ret;
    }
    case DATATYPE::INT16: {
      // k_int32 ret = (*(static_cast<k_int16*>(l))) - (*(static_cast<k_int16*>(r)));
      k_int16 lv = *(static_cast<k_int16*>(l));
      k_int16 rv = *(static_cast<k_int16*>(r));
      k_int32 diff = static_cast<k_int32>(lv) - static_cast<k_int32>(rv);
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP: {
      // k_int64 diff = (*(static_cast<k_int32*>(l))) - (*(static_cast<k_int32*>(r)));
      k_int32 lv = *(static_cast<k_int32*>(l));
      k_int32 rv = *(static_cast<k_int32*>(r));
      k_int64 diff = static_cast<k_int64>(lv) - static_cast<k_int64>(rv);
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO: {
      double diff = (*(static_cast<k_int64*>(l))) - (*(static_cast<k_int64*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::TIMESTAMP64_LSN:
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
    case DATATYPE::TIMESTAMP64_LSN_NANO: {
      double diff = (*(static_cast<TimeStamp64LSN*>(l))).ts64 - (*(static_cast<TimeStamp64LSN*>(r))).ts64;
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::FLOAT: {
      double diff = (*(static_cast<float*>(l))) - (*(static_cast<float*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::DOUBLE: {
      double diff = (*(static_cast<double*>(l))) - (*(static_cast<double*>(r)));
      return diff >= 0 ? (diff > 0 ? 1 : 0) : -1;
    }
    case DATATYPE::STRING: {
      k_int32 ret = strncmp(static_cast<char*>(l), static_cast<char*>(r), size);
      return ret;
    }
      break;
    default:
      break;
  }
  return false;
}

inline void TsAggIteratorV2Impl::ConvertToDoubleIfOverflow(uint32_t col_idx, TSSlice& agg_data) {
  if (is_overflow_[col_idx]) {
    *reinterpret_cast<double*>(agg_data.data) = *reinterpret_cast<int64_t*>(agg_data.data);
  }
}

inline KStatus TsAggIteratorV2Impl::AddSumNotOverflowYet(uint32_t col_idx,
                                                          int32_t type,
                                                          void* current,
                                                          TSSlice& agg_data) {
  switch (type) {
    case DATATYPE::INT8:
      is_overflow_[col_idx] = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int8_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, agg_data);
      break;
    case DATATYPE::INT16:
      is_overflow_[col_idx] = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int16_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, agg_data);
      break;
    case DATATYPE::INT32:
      is_overflow_[col_idx] = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int32_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, agg_data);
      break;
    case DATATYPE::INT64:
      is_overflow_[col_idx] = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int64_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, agg_data);
      break;
    case DATATYPE::FLOAT:
      AddAggFloat<double>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<float*>(current));
      break;
    case DATATYPE::DOUBLE:
      AddAggFloat<double>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<double*>(current));
      break;
    default:
      LOG_ERROR("Not supported for sum, datatype: %d", type);
      return KStatus::FAIL;
      break;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsAggIteratorV2Impl::AddSumOverflow(int32_t type,
                                                    void* current,
                                                    TSSlice& agg_data) {
  switch (type) {
    case DATATYPE::INT8:
      AddAggFloat<double, int64_t>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<int8_t*>(current));
      break;
    case DATATYPE::INT16:
      AddAggFloat<double, int64_t>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<int16_t*>(current));
      break;
    case DATATYPE::INT32:
      AddAggFloat<double, int64_t>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<int32_t*>(current));
      break;
    case DATATYPE::INT64:
      AddAggFloat<double, int64_t>(
          *reinterpret_cast<double*>(agg_data.data),
          *reinterpret_cast<int64_t*>(current));
      break;
    case DATATYPE::FLOAT:
    case DATATYPE::DOUBLE:
      LOG_ERROR("Overflow not supported for sum, datatype: %d", type);
      return KStatus::FAIL;
      break;
    default:
      LOG_ERROR("Not supported for sum, datatype: %d", type);
      return KStatus::FAIL;
      break;
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::UpdateAggregation(std::shared_ptr<TsBlockSpan>& block_span,
                                                const std::vector<AttributeInfo>& schema) {
  KStatus ret;
  int row_idx;
  int row_num = block_span->GetRowNum();

  // Aggregate first col
  if (first_col_idxs_.size() > 0 && block_span->GetFirstTS() < max_first_ts_) {
    if (first_last_only_agg_) {
      // update max_first_ts_ at the same time if it's first/last only agg
      max_first_ts_ = INT64_MIN;
    }
    for (auto first_col_idx : first_col_idxs_) {
      AggCandidate& candidate = candidates_[first_col_idx];
      if (candidate.blk_span && candidate.ts <= block_span->GetFirstTS()) {
        // No need to scan the rows
      } else {
        uint32_t blk_col_idx = ts_scan_cols_[first_col_idx];
        TsBitmap bitmap;
        ret = block_span->GetColBitmap(blk_col_idx, schema, bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        for (row_idx = 0; row_idx < row_num; ++row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
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
      }
      if (first_last_only_agg_) {
        // update max_first_ts_ at the same time if it's first/last only agg
        max_first_ts_ = max(max_first_ts_, candidate.ts);
      }
    }
  }

  // Aggregate last col
  if (last_col_idxs_.size() > 0 && block_span->GetLastTS() > min_last_ts_) {
    if (first_last_only_agg_) {
      // update max_first_ts_ at the same time if it's first/last only agg
      min_last_ts_ = INT64_MAX;
    }
    for (auto last_col_idx : last_col_idxs_) {
      AggCandidate& candidate = candidates_[last_col_idx];
      if (candidate.blk_span && candidate.ts >= block_span->GetLastTS()) {
        // No need to scan the rows
      } else {
        uint32_t blk_col_idx = ts_scan_cols_[last_col_idx];
        TsBitmap bitmap;
        ret = block_span->GetColBitmap(blk_col_idx, schema, bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        for (row_idx = row_num - 1; row_idx >= 0; --row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          int64_t ts = block_span->GetTS(row_idx);
          if ((last_ts_points_.empty() || ts <= last_ts_points_[last_col_idx])
              && (!candidate.blk_span || candidate.ts < ts)) {
            candidate.blk_span = block_span;
            candidate.ts = ts;
            candidate.row_idx = row_idx;
            break;
          }
        }
      }
      if (first_last_only_agg_) {
        // update max_first_ts_ at the same time if it's first/last only agg
        min_last_ts_ = min(min_last_ts_, candidate.ts);
      }
    }
  }

  // Aggregate count col
  for (int i = 0; i < count_col_idxs_.size(); ++i) {
    uint32_t count_col_idx = count_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[count_col_idx];
    if (schema[blk_col_idx].isFlag(AINFO_NOT_NULL)) {
      KUint64(final_agg_data_[count_col_idx].data) += block_span->GetRowNum();
    } else {
      if (block_span->HasPreAgg()) {
        // Use pre agg to calculate count
        uint16_t block_span_count;
        ret = block_span->GetPreCount(blk_col_idx, block_span_count);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        KUint64(final_agg_data_[count_col_idx].data) += block_span_count;
      } else {
        TsBitmap bitmap;
        ret = block_span->GetColBitmap(blk_col_idx, schema, bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        for (row_idx = 0; row_idx < row_num; ++row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          ++KUint64(final_agg_data_[count_col_idx].data);
        }
      }
    }
  }

  // Aggregate sum col
  for (int i = 0; i < sum_col_idxs_.size(); ++i) {
    uint32_t sum_col_idx = sum_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[sum_col_idx];
    if (block_span->HasPreAgg()) {
      // Use pre agg to calculate sum
      void* current;
      bool pre_sum_is_overflow;
      ret = block_span->GetPreSum(blk_col_idx, schema[blk_col_idx].size, current, pre_sum_is_overflow);
      if (ret != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
      if (current) {
        TSSlice& agg_data = final_agg_data_[sum_col_idx];
        if (agg_data.data == nullptr) {
          agg_data.len = sizeof(int64_t);
          InitAggData(agg_data);
          InitSumValue(agg_data.data, schema[blk_col_idx].type);
        }
        if (!is_overflow_[sum_col_idx]) {
          if (!pre_sum_is_overflow) {
            ret = AddSumNotOverflowYet(sum_col_idx, schema[blk_col_idx].type, current, agg_data);
          } else {
            ret = AddSumNotOverflowYet(sum_col_idx, DATATYPE::DOUBLE, current, agg_data);
          }
        } else {
          if (!pre_sum_is_overflow) {
            ret = AddSumOverflow(schema[blk_col_idx].type, current, agg_data);
          } else {
            ret = AddSumOverflow(DATATYPE::DOUBLE, current, agg_data);
          }
        }
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
      }
    } else {
      char* value = nullptr;
      TsBitmap bitmap;
      auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, attrs_[blk_col_idx], &value, bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetFixLenColAddr failed.");
        return s;
      }

      int32_t size = (blk_col_idx == 0 ? 16 : schema[blk_col_idx].size);
      for (row_idx = 0; row_idx < row_num; ++row_idx) {
        if (bitmap[row_idx] != DataFlags::kValid) {
          continue;
        }
        void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
        TSSlice& agg_data = final_agg_data_[sum_col_idx];
        if (agg_data.data == nullptr) {
          agg_data.len = sizeof(int64_t);
          InitAggData(agg_data);
          InitSumValue(agg_data.data, schema[blk_col_idx].type);
        }
        if (!is_overflow_[sum_col_idx]) {
          ret = AddSumNotOverflowYet(sum_col_idx, schema[blk_col_idx].type, current, agg_data);
          if (ret != KStatus::SUCCESS) {
            return KStatus::FAIL;
          }
        }
        if (is_overflow_[sum_col_idx]) {
          ret = AddSumOverflow(schema[blk_col_idx].type, current, agg_data);
          if (ret != KStatus::SUCCESS) {
            return KStatus::FAIL;
          }
        }
      }
    }
  }

  // Aggregate max col
  for (int i = 0; i < max_col_idxs_.size(); ++i) {
    uint32_t max_col_idx = max_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[max_col_idx];
    TSSlice& agg_data = final_agg_data_[max_col_idx];
    int32_t type = schema[blk_col_idx].type;
    if (isSameType(schema[blk_col_idx], attrs_[blk_col_idx]) && block_span->HasPreAgg()) {
      // Use pre agg to calculate max
      if (!isVarLenType(type)) {
        void* pre_max;
        int32_t size = (blk_col_idx == 0 ? 16 : schema[blk_col_idx].size);
        ret = block_span->GetPreMax(blk_col_idx, pre_max);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (agg_data.data == nullptr) {
          agg_data.len = size;
          InitAggData(agg_data);
          memcpy(agg_data.data, pre_max, size);
        } else if (valcmp(pre_max, agg_data.data, type, size) > 0) {
          memcpy(agg_data.data, pre_max, size);
        }
      } else {
        TSSlice pre_max;
        ret = block_span->GetVarPreMax(blk_col_idx, pre_max);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (pre_max.data) {
          string pre_max_val(pre_max.data, pre_max.len);
          if (agg_data.data) {
            string current_max({agg_data.data + kStringLenLen, agg_data.len});
            if (current_max < pre_max_val) {
              free(agg_data.data);
              agg_data.data = nullptr;
            }
          }
          if (agg_data.data == nullptr) {
            agg_data.len = pre_max_val.length() + kStringLenLen;
            agg_data.data = static_cast<char*>(malloc(agg_data.len));
            KUint16(agg_data.data) = pre_max_val.length();
            memcpy(agg_data.data + kStringLenLen, pre_max_val.c_str(), pre_max_val.length());
          }
        }
      }
    } else {
      if (!isVarLenType(type)) {
        char* value = nullptr;
        TsBitmap bitmap;
        auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, attrs_[blk_col_idx], &value, bitmap);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }

        int32_t size = (blk_col_idx == 0 ? 16 : schema[blk_col_idx].size);
        for (row_idx = 0; row_idx < row_num; ++row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
          if (agg_data.data == nullptr) {
            agg_data.len = size;
            InitAggData(agg_data);
            memcpy(agg_data.data, current, size);
          } else if (valcmp(current, agg_data.data, type, size) > 0) {
            memcpy(agg_data.data, current, size);
          }
        }
      } else {
        std::vector<string> var_rows;
        KStatus ret;
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          TSSlice slice;
          DataFlags flag;
          ret = block_span->GetVarLenTypeColAddr(row_idx, blk_col_idx, schema, attrs_[blk_col_idx], flag, slice);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("GetVarLenTypeColAddr failed.");
            return ret;
          }
          if (flag == DataFlags::kValid) {
            var_rows.emplace_back(slice.data, slice.len);
          }
        }
        if (!var_rows.empty()) {
          auto max_it = std::max_element(var_rows.begin(), var_rows.end());
          if (agg_data.data) {
            string current_max({agg_data.data + kStringLenLen, agg_data.len});
            if (current_max < *max_it) {
              free(agg_data.data);
              agg_data.data = nullptr;
            }
          }
          if (agg_data.data == nullptr) {
            // Can we use the memory in var_rows?
            agg_data.len = max_it->length() + kStringLenLen;
            agg_data.data = static_cast<char*>(malloc(agg_data.len));
            KUint16(agg_data.data) = max_it->length();
            memcpy(agg_data.data + kStringLenLen, max_it->c_str(), max_it->length());
          }
        }
      }
    }
  }

  // Aggregate min col
  for (int i = 0; i < min_col_idxs_.size(); ++i) {
    uint32_t min_col_idx = min_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[min_col_idx];
    TSSlice& agg_data = final_agg_data_[min_col_idx];
    int32_t type = schema[blk_col_idx].type;
    if (isSameType(schema[blk_col_idx], attrs_[blk_col_idx]) && block_span->HasPreAgg()) {
      // Use pre agg to calculate min
      if (!isVarLenType(type)) {
        void* pre_min;
        int32_t size = (blk_col_idx == 0 ? 16 : schema[blk_col_idx].size);
        ret = block_span->GetPreMin(blk_col_idx, size, pre_min);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (agg_data.data == nullptr) {
          agg_data.len = size;
          InitAggData(agg_data);
          memcpy(agg_data.data, pre_min, size);
        } else if (valcmp(pre_min, agg_data.data, type, size) < 0) {
          memcpy(agg_data.data, pre_min, size);
        }
      } else {
        TSSlice pre_min;
        ret = block_span->GetVarPreMin(blk_col_idx, pre_min);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (pre_min.data) {
          string pre_min_val(pre_min.data, pre_min.len);
          if (agg_data.data) {
            string current_min({agg_data.data + kStringLenLen, agg_data.len});
            if (current_min > pre_min_val) {
              free(agg_data.data);
              agg_data.data = nullptr;
            }
          }
          if (agg_data.data == nullptr) {
            agg_data.len = pre_min_val.length() + kStringLenLen;
            agg_data.data = static_cast<char*>(malloc(agg_data.len));
            KUint16(agg_data.data) = pre_min_val.length();
            memcpy(agg_data.data + kStringLenLen, pre_min_val.c_str(), pre_min_val.length());
          }
        }
      }
    } else {
      if (!isVarLenType(type)) {
        char* value = nullptr;
        TsBitmap bitmap;
        auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, attrs_[blk_col_idx], &value, bitmap);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }

        int32_t size = (blk_col_idx == 0 ? 16 : schema[blk_col_idx].size);
        for (row_idx = 0; row_idx < row_num; ++row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
          if (agg_data.data == nullptr) {
            agg_data.len = size;
            InitAggData(agg_data);
            memcpy(agg_data.data, current, size);
          } else if (valcmp(current, agg_data.data, type, size) < 0) {
            memcpy(agg_data.data, current, size);
          }
        }
      } else {
        std::vector<string> var_rows;
        KStatus ret;
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          TSSlice slice;
          DataFlags flag;
          ret = block_span->GetVarLenTypeColAddr(row_idx, blk_col_idx, schema, attrs_[blk_col_idx], flag, slice);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("GetVarLenTypeColAddr failed.");
            return ret;
          }
          if (flag == DataFlags::kValid) {
            var_rows.emplace_back(slice.data, slice.len);
          }
        }
        if (!var_rows.empty()) {
          auto min_it = std::min_element(var_rows.begin(), var_rows.end());
          if (agg_data.data) {
            string current_min({agg_data.data + kStringLenLen, agg_data.len});
            if (current_min > *min_it) {
              free(agg_data.data);
              agg_data.data = nullptr;
            }
          }
          if (agg_data.data == nullptr) {
            // Can we use the memory in var_rows?
            agg_data.len = min_it->length() + kStringLenLen;
            agg_data.data = static_cast<char*>(malloc(agg_data.len));
            KUint16(agg_data.data) = min_it->length();
            memcpy(agg_data.data + kStringLenLen, min_it->c_str(), min_it->length());
          }
        }
      }
    }
  }

  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
