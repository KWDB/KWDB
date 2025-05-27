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
  ts_block_spans_.clear();
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

inline bool PartitionLessThan(std::shared_ptr<TsVGroupPartition>& a, std::shared_ptr<TsVGroupPartition> b) {
  return a->StartTs() < b->StartTs();
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
  candidates_.reserve(ts_scan_cols_.size());
  has_first_row_col_ = false;
  has_last_row_col_ = false;
  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    switch (scan_agg_types_[i]) {
      case Sumfunctype::LAST:
      case Sumfunctype::LASTTS:
        if (last_map_.find(ts_scan_cols_[i]) == last_map_.end()) {
          last_col_idxs_.push_back(i);
          last_map_[ts_scan_cols_[i]] = i;
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
  first_col_ts_.resize(first_col_idxs_.size(), INT_MAX);
  last_col_ts_.resize(last_col_idxs_.size(), INT_MIN);
  first_last_only_agg_ = (count_col_idxs_.size() + sum_col_idxs_.size() + max_col_idxs_.size() + min_col_idxs_.size() == 0);

  if (origin_first_col_idxs_.size() > 0) {
    std::sort(ts_partitions_.begin(), ts_partitions_.end(), PartitionLessThan);
  }

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
  candidates_.clear();
  candidates_.resize(ts_scan_cols_.size(), {0, 0, nullptr});
  is_overflow_.clear();
  is_overflow_.resize(ts_scan_cols_.size(), false);

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    if (scan_agg_types_[i] == Sumfunctype::COUNT) {
      final_agg_data_[i].len = sizeof(uint64_t);
      final_agg_data_[i].data = static_cast<char*>(malloc(final_agg_data_[i].len));
      memset(final_agg_data_[i].data, 0, final_agg_data_[i].len);
    }
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
  KStatus ret = AddMemSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  ret = UpdateAggregation();
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  int first_partition_idx = 0;
  for (; first_partition_idx < ts_partitions_.size(); ++first_partition_idx) {
    if (ts_partitions_[first_partition_idx]->StartTs() < max_first_ts_) {
      cur_partition_index_ = first_partition_idx;
      ret = AddLastSegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = AddEntitySegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
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
    if (ts_partitions_[last_partition_idx]->EndTs() > min_last_ts_) {
      cur_partition_index_ = last_partition_idx;
      ret = AddLastSegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = AddEntitySegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
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
      ret = AddLastSegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = AddEntitySegmentBlockSpans();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ret = UpdateAggregation();
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }

  if (first_last_only_agg_) {
    if (has_first_row_col_) {
      max_first_ts_ = max(max_first_ts_, first_row_candidate_.ts);
    }
    if (has_last_row_col_) {
      min_last_ts_ = min(min_last_ts_, last_row_candidate_.ts);
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
                      candidates_[last_map_[ts_scan_cols_[i]]] :
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
  if (first_col_idxs_.size() > 0) {
    max_first_ts_ = first_col_ts_.size() > 0 ? max_element(first_col_ts_.begin(), first_col_ts_.end) : INT_MIN;
  }
  if (last_col_idxs_.size() > 0) {
    min_last_ts_ = last_col_ts_.size() > 0 ? min_element(last_col_ts_.begin(), last_col_ts_.end) : INT_MAX;
  }
  if (first_last_only_agg_) {
    if (max_first_ts_ < min_last_ts_) {
      int ts_span_idx = 0;
      while (ts_span_idx < ts_spans_.size() && ts_spans_[ts_span_idx].end < max_first_ts_) {
        ++ts_span_idx;
      }

      if (ts_span_idx < ts_spans_.size()) {
        int64_t end = ts_spans_[ts_span_idx].end;
        if (ts_spans_[ts_span_idx].end > min_last_ts_) {
          if (ts_spans_[ts_span_idx].begin < max_first_ts_) {
            ts_spans_.insert(ts_spans_.begin() + ts_span_idx + 1, {min_last_ts_, ts_spans_[ts_span_idx].end});
            ts_spans_[ts_span_idx].end = max_first_ts_;
            ++ts_span_idx;
          }
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
  std::vector<shared_ptr<TsBlockSpan>> ts_block_spans(ts_block_spans_.begin(), ts_block_spans_.end());

  int block_span_idx = 0;
  if (!first_col_idxs_.empty() || has_first_row_col_) {
    if (first_col_idxs_.empty()) {
      auto min_it = std::min_element(ts_block_spans.begin(), ts_block_spans.end(), LastTSLessThan);
      if (first_row_candidate_.ts > (*min_it)->GetFirstTS()) {
        first_row_candidate_.blk_span = *min_it;
        first_row_candidate_.ts = first_row_candidate_.blk_span->GetFirstTS();
        first_row_candidate_.row_idx = 0;
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

          ret = UpdateAggregation(blk_span, schema_info, false);
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

        ret = UpdateAggregation(blk_span, schema_info, true);
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

      ret = UpdateAggregation(blk_span, schema_info, false);
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

KStatus TsAggIteratorV2Impl::UpdateAggregation(std::shared_ptr<TsBlockSpan>& block_span,
                                                const std::vector<AttributeInfo>& schema,
                                                bool remove_last_col) {
  KStatus ret;
  int row_idx;
  int row_num = block_span->GetRowNum();

  // Aggregate first col
  if (first_col_idxs_.size() > 0 && block_span->GetFirstTS() < max_first_ts_) {
    for (auto first_col_idx : first_col_idxs_) {
      AggCandidate& candidate = candidates_[first_col_idx];
      if (candidate.blk_span && candidate.ts <= block_span->GetFirstTS()) {
        // No need to scan this first agg anymore for the rest block spans.
        continue;
      }
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
          if (first_last_only_agg_) {
            max_first_ts_ = max(max_first_ts_, ts);
          }
          break;
        }
      }
    }
  }

  // Aggregate last col
  int last_col_num = last_col_idxs_.size();
  for (int i = 0; i < last_col_num; ++i) {
    uint32_t last_col_idx = last_col_idxs_.front();
    last_col_idxs_.pop_front();
    AggCandidate& candidate = candidates_[last_col_idx];
    if (candidate.blk_span && candidate.ts >= block_span->GetLastTS()) {
      if (!remove_last_col) {
        last_col_idxs_.push_back(last_col_idx);
      }
      continue;
    }
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
      if (!candidate.blk_span || candidate.ts < ts) {
        candidate.blk_span = block_span;
        candidate.ts = ts;
        candidate.row_idx = row_idx;
        if (first_last_only_agg_) {
          min_last_ts_ = min(min_last_ts_, ts);
        }
        break;
      }
    }
    if (row_idx < row_num - 1) {
      last_col_idxs_.push_back(last_col_idx);
    } else {
      if (!remove_last_col) {
        last_col_idxs_.push_back(last_col_idx);
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

  // Aggregate sum col
  for (int i = 0; i < sum_col_idxs_.size(); ++i) {
    uint32_t sum_col_idx = sum_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[sum_col_idx];
    char* value = nullptr;
    TsBitmap bitmap;
    auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, schema[blk_col_idx], &value, bitmap);
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
        switch (schema[blk_col_idx].type) {
          case DATATYPE::INT8:
            is_overflow_[sum_col_idx] = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int8_t*>(current));
            break;
          case DATATYPE::INT16:
            is_overflow_[sum_col_idx] = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int16_t*>(current));
            break;
          case DATATYPE::INT32:
            is_overflow_[sum_col_idx] = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int32_t*>(current));
            break;
          case DATATYPE::INT64:
            is_overflow_[sum_col_idx] = AddAggInteger<int64_t>(
                *reinterpret_cast<int64_t*>(agg_data.data),
                *reinterpret_cast<int64_t*>(current));
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
            LOG_ERROR("Not supported for sum, datatype: %d", schema[blk_col_idx].type);
            return KStatus::FAIL;
            break;
        }
        if (is_overflow_[sum_col_idx]) {
          *reinterpret_cast<double*>(agg_data.data) = *reinterpret_cast<int64_t*>(agg_data.data);
        }
      }
      if (is_overflow_[sum_col_idx]) {
        switch (schema[blk_col_idx].type) {
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
            LOG_ERROR("Overflow not supported for sum, datatype: %d", schema[blk_col_idx].type);
            return KStatus::FAIL;
            break;
          default:
            LOG_ERROR("Not supported for sum, datatype: %d", schema[blk_col_idx].type);
            return KStatus::FAIL;
            break;
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
    if (!isVarLenType(type)) {
      char* value = nullptr;
      TsBitmap bitmap;
      auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, schema[blk_col_idx], &value, bitmap);
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
        ret = block_span->GetVarLenTypeColAddr(row_idx, blk_col_idx, schema, schema[blk_col_idx], flag, slice);
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

  // Aggregate min col
  for (int i = 0; i < min_col_idxs_.size(); ++i) {
    uint32_t min_col_idx = min_col_idxs_[i];
    uint32_t blk_col_idx = ts_scan_cols_[min_col_idx];
    TSSlice& agg_data = final_agg_data_[min_col_idx];
    int32_t type = schema[blk_col_idx].type;
    if (!isVarLenType(type)) {
      char* value = nullptr;
      TsBitmap bitmap;
      auto s = block_span->GetFixLenColAddr(blk_col_idx, schema, schema[blk_col_idx], &value, bitmap);
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
        ret = block_span->GetVarLenTypeColAddr(row_idx, blk_col_idx, schema, schema[blk_col_idx], flag, slice);
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

  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
