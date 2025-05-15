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
  return vgroup_->GetMemSegmentMgr()->GetBlockSpans(filter, &ts_block_spans_);
}

inline KStatus TsStorageIteratorV2Impl::AddLastSegmentBlockSpans() {
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    std::vector<std::shared_ptr<TsLastSegment>> last_segments =
      ts_partitions_[cur_partition_index_]->GetLastSegmentMgr()->GetAllLastSegments();
    TsBlockItemFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    for (std::shared_ptr<TsLastSegment> last_segment : last_segments) {
      if (last_segment->GetBlockSpans(filter, &ts_block_spans_) != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

inline KStatus TsStorageIteratorV2Impl::AddEntitySegmentBlockSpans() {
  if (cur_entity_index_ < entity_ids_.size() && cur_partition_index_ < ts_partitions_.size()) {
    TsBlockItemFilterParams filter{db_id_, table_id_, entity_ids_[cur_entity_index_], ts_spans_};
    return ts_partitions_[cur_partition_index_]->GetEntitySegment()->GetBlockSpans(filter, &ts_block_spans_);
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

KStatus TsStorageIteratorV2Impl::ConvertBlockSpanToResultSet(TsBlockSpan& ts_blk_span,
                                                              ResultSet* res, k_uint32* count) {
  *count = ts_blk_span.GetRowNum();
  KStatus ret;
  std::shared_ptr<MMapMetricsTable> blk_version;
  ret = table_schema_mgr_->GetMetricSchema(nullptr, ts_blk_span.GetTableVersion(), &blk_version);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("GetMetricSchema faile. table version [%u]", ts_blk_span.GetTableVersion());
    return ret;
  }
  auto& blk_version_schema_all = blk_version->getSchemaInfoIncludeDropped();
  auto& blk_version_schema_valid = blk_version->getSchemaInfoExcludeDropped();
  auto blk_version_valid = blk_version->getIdxForValidCols();
  // calculate columns in current tsblock need to scan.
  std::vector<uint32_t> blk_scan_cols;
  blk_scan_cols.resize(ts_scan_cols_.size());
  for (size_t i = 0; i < ts_scan_cols_.size(); i++) {
    if (!blk_version_schema_all[ts_scan_cols_[i]].isFlag(AINFO_DROPPED)) {
      bool found = false;
      size_t j = 0;
      for (; j < blk_version_valid.size(); j++) {
        if (blk_version_valid[j] == ts_scan_cols_[i]) {
          found = true;
          break;
        }
      }
      if (!found) {
        blk_scan_cols[i] = UINT32_MAX;
        LOG_INFO("not found blk col index for col id[%u].", ts_scan_cols_[i]);
      } else {
        blk_scan_cols[i] = j;
      }
    } else {
      // column is dropped at block version.
      LOG_INFO("column is dropped at[%u] index for col id[%u].", ts_blk_span.GetTableVersion(), ts_scan_cols_[i]);
      blk_scan_cols[i] = UINT32_MAX;
    }
  }

  for (int i = 0; i < kw_scan_cols_.size(); ++i) {
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
        ret = ts_blk_span.GetFixLenColAddr(blk_col_idx, blk_version_schema_valid, attrs_[col_idx], &value, ts_bitmap);
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
          ret = ts_blk_span.GetVarLenTypeColAddr(
            row_idx, blk_col_idx, blk_version_schema_valid, attrs_[col_idx], bitmap_var, var_data);
          if (bitmap_var != DataFlags::kValid) {
            set_null_bitmap(bitmap, row_idx);
            batch->push_back(nullptr);
          } else {
            char* buffer = static_cast<char*>(malloc(var_data.len + 2 + 1));
            KUint16(buffer) = var_data.len;
            memcpy(buffer + 2, var_data.data, var_data.len);
            *(buffer + var_data.len + 2) = 0;
            std::shared_ptr<void> ptr(buffer, free);
            batch->push_back(ptr);
          }
        }
        batch->is_new = true;
        batch->need_free_bitmap = true;
      }
    }
    res->push_back(i, batch);
  }
  res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};

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
  TsBlockSpan ts_block = ts_block_spans_.front();
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
  TsBlockSpan block_span;
  do {
    if (block_span_sorted_iterator_) {
      ret = block_span_sorted_iterator_->Next(&block_span, &is_done);
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

KStatus TsAggIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  if (cur_entity_index_ == -1) {
    cur_entity_index_ = 0;
  }
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

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

  ret = AggregateBlockSpans(res, count);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to aggregate spans for entity(%d).", entity_ids_[cur_entity_index_]);
    return ret;
  }

  *is_finished = false;
  ++cur_entity_index_;
  return KStatus::SUCCESS;
}

void InitMin(void* ptr, DATATYPE type, size_t size) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
      *static_cast<int8_t*>(ptr) = std::numeric_limits<int8_t>::max();
      break;
    case DATATYPE::BOOL:
      *static_cast<bool*>(ptr) = true;
      break;
    case DATATYPE::INT16:
      *static_cast<int16_t*>(ptr) = std::numeric_limits<int16_t>::max();
      break;
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
      *static_cast<int32_t*>(ptr) = std::numeric_limits<int32_t>::max();
      break;
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      *static_cast<int64_t*>(ptr) = std::numeric_limits<int64_t>::max();
      break;
    case DATATYPE::FLOAT:
      *static_cast<float*>(ptr) = std::numeric_limits<float>::max();
      break;
    case DATATYPE::DOUBLE:
      *static_cast<double*>(ptr) = std::numeric_limits<double>::max();
      break;
    case DATATYPE::STRING:
    case DATATYPE::BINARY:
      memset(ptr, 0xFF, size);
      break;
    case DATATYPE::TIMESTAMP64_LSN:
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
    case DATATYPE::TIMESTAMP64_LSN_NANO:
      static_cast<TimeStamp64LSN*>(ptr)->ts64 = std::numeric_limits<int64_t>::max();
      break;
    default:
      break;
  }
}

void InitMax(void* ptr, DATATYPE type, size_t size) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
      *static_cast<int8_t*>(ptr) = std::numeric_limits<int8_t>::min();
      break;
    case DATATYPE::BOOL:
      *static_cast<bool*>(ptr) = false;
      break;
    case DATATYPE::INT16:
      *static_cast<int16_t*>(ptr) = std::numeric_limits<int16_t>::min();
      break;
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
      *static_cast<int32_t*>(ptr) = std::numeric_limits<int32_t>::min();
      break;
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      *static_cast<int64_t*>(ptr) = std::numeric_limits<int64_t>::min();
      break;
    case DATATYPE::FLOAT:
      *static_cast<float*>(ptr) = std::numeric_limits<float>::lowest();
      break;
    case DATATYPE::DOUBLE:
      *static_cast<double*>(ptr) = std::numeric_limits<double>::lowest();
      break;
    case DATATYPE::STRING:
    case DATATYPE::BINARY:
      memset(ptr, 0x00, size);
      break;
    case DATATYPE::TIMESTAMP64_LSN:
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
    case DATATYPE::TIMESTAMP64_LSN_NANO:
      static_cast<TimeStamp64LSN*>(ptr)->ts64 = std::numeric_limits<int64_t>::min();
      break;
    default:
      break;
  }
}

void InitSum(void* ptr, DATATYPE type) {
  switch (type) {
    case DATATYPE::INT8:
      *static_cast<int8_t*>(ptr) = 0;
      break;
    case DATATYPE::INT16:
      *static_cast<int16_t*>(ptr) = 0;
      break;
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
      *static_cast<int32_t*>(ptr) = 0;
      break;
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      *static_cast<int64_t*>(ptr) = 0;
      break;
    case DATATYPE::FLOAT:
      *static_cast<float*>(ptr) = 0.0f;
      break;
    case DATATYPE::DOUBLE:
      *static_cast<double*>(ptr) = 0.0;
      break;
    default:
      break;
  }
}


KStatus TsAggIteratorV2Impl::AggregateBlockSpans(ResultSet* res, k_uint32* count) {
  if (ts_block_spans_.empty()) {
    return KStatus::FAIL;
  }

  std::vector<TSSlice> final_agg_data(kw_scan_cols_.size(), TSSlice{nullptr, 0});
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
    }
  }

  struct AggValue {
    uint64_t count = 0;
    void* sum = nullptr;
    void* max = nullptr;
    void* min = nullptr;
  };
  std::vector<AggValue> agg_results(kw_scan_cols_.size());

  for (k_uint32 idx : normal_cols) {
    size_t size = attrs_[kw_scan_cols_[idx]].size;
    agg_results[idx].sum = malloc(size);
    agg_results[idx].max = malloc(size);
    agg_results[idx].min = malloc(size);
    // memset(agg_results[idx].sum, 0, size);
    InitMin(agg_results[idx].min, static_cast<DATATYPE>(attrs_[kw_scan_cols_[idx]].type), attrs_[kw_scan_cols_[idx]].size);
    InitMax(agg_results[idx].max, static_cast<DATATYPE>(attrs_[kw_scan_cols_[idx]].type), attrs_[kw_scan_cols_[idx]].size);
    InitSum(agg_results[idx].sum, static_cast<DATATYPE>(attrs_[kw_scan_cols_[idx]].type));
  }

  while (!ts_block_spans_.empty()) {
    TsBlockSpan blk_span = ts_block_spans_.front();
    ts_block_spans_.pop_front();

    KStatus ret;
    std::shared_ptr<MMapMetricsTable> blk_version;
    ret = table_schema_mgr_->GetMetricSchema(nullptr, blk_span.GetTableVersion(), &blk_version);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetMetricSchema failed. table version [%u]", blk_span.GetTableVersion());
      return ret;
    }
    auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

    for (k_uint32 idx : normal_cols) {
      std::vector<Sumfunctype> agg_types = {scan_agg_types_[idx]};

      ret = blk_span.GetAggResult(
        kw_scan_cols_[idx], schema_info, attrs_[kw_scan_cols_[idx]], agg_types,
        &agg_results[idx].count,
        agg_results[idx].max,
        agg_results[idx].min,
        agg_results[idx].sum);

      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to compute aggregation for col_id(%u).", kw_scan_cols_[idx]);
        return ret;
      }
    }
  }

  if (!last_cols.empty()) {
    KStatus ret = AggregateLastColumns(last_cols, final_agg_data);
    if (ret != KStatus::SUCCESS) return ret;
  }

  res->clear();
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    TSSlice& slice = final_agg_data[i];
    size_t size = attrs_[kw_scan_cols_[i]].size;
    switch (scan_agg_types_[i]) {
      case Sumfunctype::COUNT:
        slice.len = sizeof(uint64_t);
        slice.data = static_cast<char*>(malloc(slice.len));
        memset(slice.data, 0, slice.len);
        memcpy(slice.data, &agg_results[i].count, slice.len);
        break;
      case Sumfunctype::SUM:
        slice.len = sizeof(uint64_t);
        slice.data = static_cast<char*>(malloc(slice.len));
        memset(slice.data, 0, slice.len);
        memcpy(slice.data, agg_results[i].sum, size);
        break;
      case Sumfunctype::MAX:
        slice.len = size;
        slice.data = static_cast<char*>(malloc(size));
        memset(slice.data, 0, size);
        memcpy(slice.data, agg_results[i].max, size);
        break;
      case Sumfunctype::MIN:
        slice.len = size;
        slice.data = static_cast<char*>(malloc(size));
        memset(slice.data, 0, size);
        memcpy(slice.data, agg_results[i].min, size);
        break;
      default:
        if (slice.data == nullptr || slice.len == 0) {
          Batch* b = new AggBatch(nullptr, 0, nullptr);
          res->push_back(i, b);
          continue;
        }
        char* data_copy = reinterpret_cast<char*>(malloc(slice.len));
        memcpy(data_copy, slice.data, slice.len);
        Batch* b = new AggBatch(data_copy, 1, nullptr);
        b->is_new = true;
        b->need_free_bitmap = true;
        res->push_back(i, b);
        continue;
    }

    Batch* b = new AggBatch(slice.data, 1, nullptr);
    b->is_new = true;
    b->need_free_bitmap = true;

    res->push_back(i, b);
  }

  res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};
  res->col_num_ = kw_scan_cols_.size();
  *count = 1;

  for (auto& val : agg_results) {
    free(val.sum);
    free(val.max);
    free(val.min);
  }

  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::AggregateLastColumns(
  const std::vector<k_uint32>& last_cols,
  std::vector<TSSlice>& final_agg_data) {
  std::vector<LastCandidate> candidates(last_cols.size());

  KStatus ret = AddMemSegmentBlockSpans();
  if (ret != KStatus::SUCCESS) return ret;

  ret = UpdateLastCandidatesFromBlockSpans(last_cols, candidates);
  if (ret != KStatus::SUCCESS) return ret;

  std::vector<std::pair<int, int64_t>> sorted_partitions;
  for (int i = 0; i < ts_partitions_.size(); ++i) {
    sorted_partitions.emplace_back(i, ts_partitions_[i]->EndTs());
  }
  std::sort(sorted_partitions.begin(), sorted_partitions.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

  for (const auto& [partition_idx, end_ts] : sorted_partitions) {
    cur_partition_index_ = partition_idx;

    bool skip = true;
    for (size_t j = 0; j < last_cols.size(); ++j) {
      if (!candidates[j].valid && candidates[j].ts < end_ts) {
        skip = false;
        break;
      }
    }
    if (skip) continue;

    ret = AddLastSegmentBlockSpans();
    if (ret != KStatus::SUCCESS) return ret;
    ret = AddEntitySegmentBlockSpans();
    if (ret != KStatus::SUCCESS) return ret;

    ret = UpdateLastCandidatesFromBlockSpans(last_cols, candidates);
    if (ret != KStatus::SUCCESS) return ret;
  }

  // After determining best candidate per column, resolve value
  for (size_t j = 0; j < last_cols.size(); ++j) {
    const auto& c = candidates[j];
    uint32_t col_idx = last_cols[j];
    final_agg_data[col_idx].len = attrs_[kw_scan_cols_[col_idx]].size;

    if (!c.valid) {
      final_agg_data[col_idx] = {nullptr, 0};
    } else if (scan_agg_types_[col_idx] == Sumfunctype::LASTTS) {
      final_agg_data[col_idx].data = static_cast<char*>(malloc(sizeof(int64_t)));
      memcpy(final_agg_data[col_idx].data, &c.ts, sizeof(int64_t));
      final_agg_data[col_idx].len = sizeof(int64_t);
    } else if (scan_agg_types_[col_idx] == Sumfunctype::LAST) {
      std::shared_ptr<MMapMetricsTable> blk_version;
      ret = table_schema_mgr_->GetMetricSchema(nullptr, c.blk_span.GetTableVersion(), &blk_version);
      if (ret != KStatus::SUCCESS) return ret;
      auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

      TSBlkDataTypeConvert convert(c.blk_span.GetTsBlock().get(), c.blk_span.GetStartRow(), c.blk_span.GetRowNum());
      char* value = nullptr;
      TsBitmap bitmap;
      ret = convert.GetFixLenColAddr(kw_scan_cols_[col_idx], schema_info,
        attrs_[kw_scan_cols_[col_idx]], &value, bitmap);
      if (ret != KStatus::SUCCESS) return ret;

      final_agg_data[col_idx].data = static_cast<char*>(malloc(final_agg_data[col_idx].len));
      memcpy(final_agg_data[col_idx].data, value + c.row_idx * final_agg_data[col_idx].len,
             final_agg_data[col_idx].len);
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::UpdateLastCandidatesFromBlockSpans(
    const std::vector<k_uint32>& last_cols,
    std::vector<LastCandidate>& candidates) {
  while (!ts_block_spans_.empty()) {
    TsBlockSpan blk_span = ts_block_spans_.front();
    ts_block_spans_.pop_front();

    timestamp64 blk_min_ts, blk_max_ts;
    blk_span.GetTSRange(&blk_min_ts, &blk_max_ts);

    bool all_skip = true;
    for (size_t j = 0; j < last_cols.size(); ++j) {
      if (blk_max_ts > candidates[j].ts) {
        all_skip = false;
        break;
      }
    }
    if (all_skip) continue;

    std::shared_ptr<MMapMetricsTable> blk_version;
    KStatus ret = table_schema_mgr_->GetMetricSchema(nullptr, blk_span.GetTableVersion(), &blk_version);
    if (ret != KStatus::SUCCESS) return ret;
    auto& schema_info = blk_version->getSchemaInfoExcludeDropped();

    for (size_t j = 0; j < last_cols.size(); ++j) {
      if (blk_max_ts <= candidates[j].ts) continue;
      int64_t ts = INT64_MIN;
      int row_idx = -1;
      ret = blk_span.GetLastInfo(
          kw_scan_cols_[last_cols[j]], schema_info, attrs_[kw_scan_cols_[last_cols[j]]], &ts, &row_idx);
      if (ret != KStatus::SUCCESS) return ret;

      if (ts > candidates[j].ts) {
        candidates[j] = {ts, row_idx, blk_span, row_idx != -1};
      }
    }
  }
  return KStatus::SUCCESS;
}


}  //  namespace kwdbts
