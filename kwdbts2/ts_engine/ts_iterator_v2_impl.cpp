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
#include <cassert>
#include <limits>
#include <cstring>
#include <list>
#include <memory>
#include <vector>
#include <algorithm>
#include <string>
#include "ts_vgroup.h"
#include "ts_iterator_v2_impl.h"
#include "engine.h"
#include "ee_global.h"

namespace kwdbts {
int64_t TsMaxMilliTimestamp = 31556995200000;  // be associated with 'kwbase/pkg/sql/sem/tree/type_check.go'
int64_t TsMaxMicroTimestamp = 31556995200000000;

KStatus ConvertBlockSpanToResultSet(const std::vector<k_uint32>& kw_scan_cols, const vector<AttributeInfo>& attrs,
                                    shared_ptr<TsBlockSpan>& ts_blk_span, ResultSet* res, k_uint32* count) {
  *count = ts_blk_span->GetRowNum();
  KStatus ret;
  for (int i = 0; i < kw_scan_cols.size(); ++i) {
    auto kw_col_idx = kw_scan_cols[i];
    Batch* batch;
    if (!ts_blk_span->IsColExist(kw_col_idx)) {
      // column is dropped at block version.
      void* bitmap = nullptr;
      batch = new Batch(bitmap, *count, bitmap, 1, nullptr);
    } else {
      unsigned char* bitmap = nullptr;
      if (!attrs[kw_scan_cols[i]].isFlag(AINFO_NOT_NULL)) {
        bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(*count)));
        if (bitmap == nullptr) {
          return KStatus::FAIL;
        }
        memset(bitmap, 0x00, KW_BITMAP_SIZE(*count));
      }
      if (!ts_blk_span->IsVarLenType(kw_col_idx)) {
        TsBitmap ts_bitmap;
        char* value;
        ret = ts_blk_span->GetFixLenColAddr(kw_col_idx, &value, ts_bitmap, false);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return ret;
        }
        if (!attrs[kw_scan_cols[i]].isFlag(AINFO_NOT_NULL)) {
          for (int row_idx = 0; row_idx < *count; ++row_idx) {
            if (ts_bitmap[row_idx] != DataFlags::kValid) {
              set_null_bitmap(bitmap, row_idx);
            }
          }
        }

        batch = new Batch(static_cast<void *>(value), *count, bitmap, 1, nullptr);
        batch->is_new = false;
      } else {
        batch = new VarColumnBatch(*count, bitmap, 1, nullptr);
        DataFlags bitmap_var;
        TSSlice var_data;
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          ret = ts_blk_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, bitmap_var, var_data);
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
      }
      if (!attrs[kw_scan_cols[i]].isFlag(AINFO_NOT_NULL)) {
        batch->need_free_bitmap = true;
      }
    }
    res->push_back(i, batch);
  }
  res->entity_index = {1, (uint32_t)ts_blk_span->GetEntityID(), ts_blk_span->GetVGroupID()};

  return KStatus::SUCCESS;
}

TsStorageIteratorV2Impl::TsStorageIteratorV2Impl() {
}

// https://leetcode.cn/problems/merge-intervals/description/
static std::vector<KwTsSpan> SortAndMergeSpan(const std::vector<KwTsSpan>& ts_spans) {
  if (ts_spans.empty()) {
    return {};
  }
  std::vector<KwTsSpan> sorted_spans = ts_spans;
  std::sort(sorted_spans.begin(), sorted_spans.end(),
            [](const KwTsSpan& a, const KwTsSpan& b) { return a.begin < b.begin; });
  std::vector<KwTsSpan> merged_spans;
  KwTsSpan merged_span = sorted_spans[0];
  for (size_t i = 1; i < sorted_spans.size(); ++i) {
    if (sorted_spans[i].begin <= merged_span.end) {
      merged_span.end = std::max(merged_span.end, sorted_spans[i].end);
      continue;
    }
    merged_spans.push_back(merged_span);
    merged_span = sorted_spans[i];
  }
  merged_spans.push_back(merged_span);
  return merged_spans;
}

TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(const std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                                 std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                                                 std::vector<k_uint32>& kw_scan_cols,
                                                 std::vector<k_uint32>& ts_scan_cols,
                                                 std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                                 std::shared_ptr<MMapMetricsTable>& schema) {
  vgroup_ = vgroup;
  entity_ids_ = entity_ids;
  ts_spans_ = SortAndMergeSpan(ts_spans);
  block_filter_ = block_filter;
  ts_col_type_ = schema->GetTsColDataType();
  ts_scan_cols_ = ts_scan_cols;
  kw_scan_cols_ = kw_scan_cols;
  table_schema_mgr_ = table_schema_mgr;
  schema_ = std::move(schema);
}

TsStorageIteratorV2Impl::~TsStorageIteratorV2Impl() {
}

KStatus TsStorageIteratorV2Impl::Init(bool is_reversed) {
  is_reversed_ = is_reversed;
  attrs_ = schema_->getSchemaInfoExcludeDropped();
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = table_schema_mgr_->GetDbID();

  auto current = vgroup_->CurrentVersion();
  ts_partitions_ = current->GetPartitions(db_id_, ts_spans_, ts_col_type_);

  filter_ = std::make_shared<TsScanFilterParams>(db_id_, table_id_, vgroup_->GetVGroupID(),
                                                  0, ts_col_type_, scan_lsn_, ts_spans_);
  return KStatus::SUCCESS;
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
  return  (!is_reversed_ && begin_ts > ts) || (is_reversed_ && end_ts < ts);
}

KStatus TsStorageIteratorV2Impl::getBlockSpanMinMaxValue(std::shared_ptr<TsBlockSpan>& block_span, uint32_t col_id,
                                                          uint32_t type, void*& min, void*& max) {
  TsBitmap bitmap;
  char* value = nullptr;
  auto s = block_span->GetFixLenColAddr(col_id, &value, bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetFixLenColAddr failed.");
    return s;
  }
  uint32_t row_num = block_span->GetRowNum();
  int32_t size = block_span->GetColSize(col_id);
  for (int row_idx = 0; row_idx < row_num; ++row_idx) {
    if (bitmap[row_idx] != DataFlags::kValid) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
    if (min == nullptr) {
      min = malloc(size);
      memcpy(min, current, size);
    } else if (cmp(min, current, type, size) > 0) {
      memcpy(min, current, size);
    }
    if (max == nullptr) {
      max = malloc(size);
      memcpy(max, current, size);
    } else if (cmp(current, max, type, size) > 0) {
      memcpy(max, current, size);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::getBlockSpanVarMinMaxValue(std::shared_ptr<TsBlockSpan>& block_span,
                                                            uint32_t col_id, uint32_t type, TSSlice& min,
                                                            TSSlice& max) {
  KStatus ret;
  std::vector<string> var_rows;
  uint32_t row_num = block_span->GetRowNum();
  for (int row_idx = 0; row_idx < row_num; ++row_idx) {
    TSSlice slice;
    DataFlags flag;
    ret = block_span->GetVarLenTypeColAddr(row_idx, col_id, flag, slice);
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
    if (min.data) {
      string current_max({min.data, min.len});
      if (current_max > *min_it) {
        free(min.data);
        min.data = nullptr;
      }
    }
    if (min.data == nullptr) {
      min.len = min_it->length();
      min.data = static_cast<char*>(malloc(min.len));
      memcpy(min.data, min_it->c_str(), min.len);
    }
    auto max_it = std::max_element(var_rows.begin(), var_rows.end());
    if (max.data) {
      string current_max({max.data, max.len});
      if (current_max < *max_it) {
        free(max.data);
        max.data = nullptr;
      }
    }
    if (max.data == nullptr) {
      max.len = max_it->length();
      max.data = static_cast<char*>(malloc(max.len));
      memcpy(max.data, max_it->c_str(), max_it->length());
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::isBlockFiltered(std::shared_ptr<TsBlockSpan>& block_span, bool& is_filtered) {
  is_filtered = false;
  KStatus ret;
  for (const auto& filter : block_filter_) {
    uint32_t col_id = filter.colID;
    BlockFilterType filter_type = filter.filterType;
    std::vector<FilterSpan> filter_spans = filter.spans;
    switch (filter_type) {
      case BlockFilterType::BFT_NULL:
      case BlockFilterType::BFT_NOTNULL: {
        if (filter_type == BlockFilterType::BFT_NULL) {
          bool is_all_not_null = false;
          if (!block_span->IsColExist(col_id)) {
            break;
          } else if (block_span->IsColNotNull(col_id)) {
            is_all_not_null = true;
          } else if (block_span->HasPreAgg()) {
            // Use pre agg to calculate count
            uint16_t pre_count{0};
            ret = block_span->GetPreCount(col_id, pre_count);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            if (pre_count == block_span->GetRowNum()) {
              is_all_not_null = true;
            }
          } else {
            uint32_t col_count{0};
            ret = block_span->GetCount(col_id, col_count);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            if (col_count == block_span->GetRowNum()) {
              is_all_not_null = true;
            }
          }
          if (is_all_not_null && filter.spans.empty()) {
            is_filtered = true;
            return KStatus::SUCCESS;
          }
          if (!is_all_not_null) break;
        } else {
          bool is_all_null = false;
          if (!block_span->IsColExist(col_id)) {
            is_all_null = true;
          } else if (block_span->HasPreAgg()) {
            // Use pre agg to calculate count
            uint16_t pre_count{0};
            ret = block_span->GetPreCount(col_id, pre_count);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            if (!pre_count) {
              is_all_null = true;
            }
          } else {
            uint32_t col_count{0};
            ret = block_span->GetCount(col_id, col_count);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            if (!col_count) {
              is_all_null = true;
            }
          }
          if (is_all_null && filter.spans.empty()) {
            is_filtered = true;
            return KStatus::SUCCESS;
          }
          if (!is_all_null) break;
        }
        [[fallthrough]];
      }
      case BlockFilterType::BFT_SPAN: {
        if (!block_span->IsColExist(col_id)) {
          // No data for this column in this block span.
          is_filtered = true;
          return KStatus::SUCCESS;
        }
        SpanValue min, max;
        void* min_addr{nullptr};
        void* max_addr{nullptr};
        bool is_new = false;
        Defer defer{[&]() {
          if (is_new) {
            if (isVarLenType(attrs_[col_id].type)) {
              free(min.data);
              free(max.data);
            } else {
              free(min_addr);
              free(max_addr);
            }
          }
        }};
        if (!isVarLenType(attrs_[col_id].type)) {
          if (block_span->HasPreAgg()) {
            ret = block_span->GetPreMin(col_id, min_addr);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            ret = block_span->GetPreMax(col_id, max_addr);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
          } else {
            ret = getBlockSpanMinMaxValue(block_span, col_id, attrs_[col_id].type, min_addr, max_addr);
            is_new = true;
          }
          if (!min_addr || !max_addr) continue;

          switch (attrs_[col_id].type) {
            case DATATYPE::BYTE:
            case DATATYPE::BOOL: {
              min.data = static_cast<char*>(min_addr);
              max.data = static_cast<char*>(max_addr);
              min.len = max.len = attrs_[col_id].size;
              break;
            }
            case DATATYPE::BINARY:
            case DATATYPE::CHAR:
            case DATATYPE::STRING: {
              min.data = static_cast<char*>(min_addr);
              max.data = static_cast<char*>(max_addr);
              min.len = strlen(min.data);
              max.len = strlen(max.data);
              break;
            }
            case DATATYPE::INT8: {
              min.ival = (k_int64)(*(static_cast<k_int8*>(min_addr)));
              max.ival = (k_int64)(*(static_cast<k_int8*>(max_addr)));
              break;
            }
            case DATATYPE::INT16: {
              min.ival = (k_int64)(*(static_cast<k_int16*>(min_addr)));
              max.ival = (k_int64)(*(static_cast<k_int16*>(max_addr)));
              break;
            }
            case DATATYPE::INT32:
            case DATATYPE::TIMESTAMP: {
              min.ival = (k_int64)(*(static_cast<k_int32*>(min_addr)));
              max.ival = (k_int64)(*(static_cast<k_int32*>(max_addr)));
              break;
            }
            case DATATYPE::INT64:
            case DATATYPE::TIMESTAMP64:
            case DATATYPE::TIMESTAMP64_MICRO:
            case DATATYPE::TIMESTAMP64_NANO: {
              min.ival = *static_cast<k_int64*>(min_addr);
              max.ival = *static_cast<k_int64*>(max_addr);
              break;
            }
            case DATATYPE::FLOAT: {
              min.dval = static_cast<double>(*static_cast<float*>(min_addr));
              max.dval = static_cast<double>(*static_cast<float*>(max_addr));
              break;
            }
            case DATATYPE::DOUBLE: {
              min.dval = *static_cast<double*>(min_addr);
              max.dval = *static_cast<double*>(max_addr);
              break;
            }
            default:
              break;
          }
        } else {
          bool is_var_string = (attrs_[col_id].type == DATATYPE::VARSTRING);
          if (block_span->HasPreAgg()) {
            TSSlice var_pre_min{nullptr, 0};
            ret = block_span->GetVarPreMin(col_id, var_pre_min);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("GetVarPreMin failed.");
              return KStatus::FAIL;
            }
            TSSlice var_pre_max{nullptr, 0};
            ret = block_span->GetVarPreMax(col_id, var_pre_max);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("GetVarPreMax failed.");
              return KStatus::FAIL;
            }
            if (!var_pre_min.data || !var_pre_max.data) continue;

            min.len = is_var_string ? (var_pre_min.len - 1) : var_pre_min.len;
            min.data = var_pre_min.data;
            max.len = is_var_string ? (var_pre_max.len - 1) : var_pre_max.len;
            max.data = var_pre_max.data;
          } else {
            TSSlice var_pre_min{nullptr, 0};
            TSSlice var_pre_max{nullptr, 0};
            ret = getBlockSpanVarMinMaxValue(block_span, col_id, attrs_[col_id].type, var_pre_min, var_pre_max);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("getBlockSpanVarMinValue failed.");
              return KStatus::FAIL;
            }
            if (!var_pre_min.data || !var_pre_max.data) continue;

            min.len = is_var_string ? (var_pre_min.len - 1) : var_pre_min.len;
            min.data = var_pre_min.data;
            max.len = is_var_string ? (var_pre_max.len - 1) : var_pre_max.len;
            max.data = var_pre_max.data;

            is_new = true;
          }
        }
        if (!matchesFilterRange(filter, min, max, (DATATYPE)attrs_[col_id].type)) {
          is_filtered = true;
          return KStatus::SUCCESS;
        }
        break;
      }
      default:
        break;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::ScanEntityBlockSpans(timestamp64 ts) {
  ts_block_spans_.clear();
  UpdateTsSpans(ts);
  filter_->entity_id_ = entity_ids_[cur_entity_index_];
  for (auto& partition_version : ts_partitions_) {
    if (ts != INVALID_TS && IsFilteredOut(partition_version->GetTsColTypeStartTime(ts_col_type_),
                                          partition_version->GetTsColTypeEndTime(ts_col_type_), ts))  {
      continue;
    }
    auto s = partition_version->GetBlockSpans(*filter_, &ts_block_spans_, table_schema_mgr_, schema_);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition_version GetBlockSpan failed.");
      return s;
    }
    if (!block_filter_.empty()) {
      uint32_t size = ts_block_spans_.size();
      for (uint32_t i = 0; i < size; ++i) {
        auto block_span = std::move(ts_block_spans_.front());
        ts_block_spans_.pop_front();
        if (!block_span->GetRowNum()) {
          continue;
        }
        bool is_filtered = false;
        s = isBlockFiltered(block_span, is_filtered);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("isBlockFiltered failed, entityid is %lu", block_span->GetEntityID());
          return KStatus::FAIL;
        }
        if (is_filtered) continue;
        ts_block_spans_.push_back(std::move(block_span));
      }
    }
  }

  return KStatus::SUCCESS;
}

TsSortedRawDataIteratorV2Impl::TsSortedRawDataIteratorV2Impl(const std::shared_ptr<TsVGroup>& vgroup,
                                                              vector<uint32_t>& entity_ids,
                                                              std::vector<KwTsSpan>& ts_spans,
                                                              std::vector<BlockFilter>& block_filter,
                                                              std::vector<k_uint32>& kw_scan_cols,
                                                              std::vector<k_uint32>& ts_scan_cols,
                                                              std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                                              std::shared_ptr<MMapMetricsTable>& schema,
                                                              SortOrder order_type) :
                          TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(vgroup, entity_ids, ts_spans, block_filter,
                                                                           kw_scan_cols, ts_scan_cols,
                                                                           table_schema_mgr, schema) {
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

bool TsSortedRawDataIteratorV2Impl::IsDisordered() {
  return false;
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
      if (!is_done && (ts != INVALID_TS && IsFilteredOut(block_span->GetFirstTS(), block_span->GetLastTS(), ts))) {
        is_done = true;
      }
      if (!is_done) {
        // Found a block span which might contain satisfied rows.
        ret = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, block_span, res, count);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        if (*count > 0) {
          // We are returning memory address inside TsBlockSpan, so we need to keep it until iterator is destroyed
          ts_block_spans_.push_back(block_span);
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

TsAggIteratorV2Impl::TsAggIteratorV2Impl(const std::shared_ptr<TsVGroup>& vgroup, vector<uint32_t>& entity_ids,
                                         std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                                         std::vector<k_uint32>& kw_scan_cols,
                                         std::vector<k_uint32>& ts_scan_cols, std::vector<k_int32>& agg_extend_cols,
                                         std::vector<Sumfunctype>& scan_agg_types,
                                         const std::vector<timestamp64>& ts_points,
                                         std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                         std::shared_ptr<MMapMetricsTable>& schema)
    : TsStorageIteratorV2Impl::TsStorageIteratorV2Impl(vgroup, entity_ids, ts_spans, block_filter,
                                                       kw_scan_cols, ts_scan_cols, table_schema_mgr, schema),
      scan_agg_types_(scan_agg_types),
      last_ts_points_(ts_points),
      agg_extend_cols_{agg_extend_cols} {}

TsAggIteratorV2Impl::~TsAggIteratorV2Impl() {}

inline bool PartitionLessThan(std::shared_ptr<const TsPartitionVersion>& a, std::shared_ptr<const TsPartitionVersion>& b) {
  return a->GetStartTime() < b->GetEndTime();
}

KStatus TsAggIteratorV2Impl::Init(bool is_reversed) {
  KStatus s = TsStorageIteratorV2Impl::Init(is_reversed);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  final_agg_data_.resize(kw_scan_cols_.size());
  final_agg_buffer_is_new_.resize(kw_scan_cols_.size(), true);
  is_overflow_.resize(kw_scan_cols_.size());

  has_first_row_col_ = false;
  has_last_row_col_ = false;
  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    switch (scan_agg_types_[i]) {
      case Sumfunctype::LAST:
      case Sumfunctype::LASTTS: {
          final_agg_buffer_is_new_[i] = (scan_agg_types_[i] == Sumfunctype::LAST &&
                                        isVarLenType(attrs_[kw_scan_cols_[i]].type)) ? true : false;
          if ((last_ts_points_.empty() || last_ts_points_[i] == TsMaxMilliTimestamp ||
               last_ts_points_[i] == TsMaxMicroTimestamp) && attrs_[kw_scan_cols_[i]].isFlag(AINFO_NOT_NULL)) {
            if (scan_agg_types_[i] == Sumfunctype::LAST) {
              scan_agg_types_[i] = Sumfunctype::LAST_ROW;
            } else {
              scan_agg_types_[i] = Sumfunctype::LASTROWTS;
            }
            has_last_row_col_ = true;
          } else {
            if (last_ts_points_.empty()) {
              if (last_map_.find(kw_scan_cols_[i]) == last_map_.end()) {
                last_col_idxs_.push_back(i);
                last_map_[kw_scan_cols_[i]] = i;
              }
            } else {
              last_col_idxs_.push_back(i);
            }
          }
        }
        break;
      case Sumfunctype::FIRST:
      case Sumfunctype::FIRSTTS: {
          final_agg_buffer_is_new_[i] = (scan_agg_types_[i] == Sumfunctype::FIRST &&
                                        isVarLenType(attrs_[kw_scan_cols_[i]].type)) ? true : false;
          if (attrs_[kw_scan_cols_[i]].isFlag(AINFO_NOT_NULL)) {
            if (scan_agg_types_[i] == Sumfunctype::FIRST) {
              scan_agg_types_[i] = Sumfunctype::FIRST_ROW;
            } else {
              scan_agg_types_[i] = Sumfunctype::FIRSTROWTS;
            }
            has_first_row_col_ = true;
          } else {
            if (first_map_.find(kw_scan_cols_[i]) == first_map_.end()) {
              first_col_idxs_.push_back(i);
              first_map_[kw_scan_cols_[i]] = i;
            }
          }
        }
        break;
      case Sumfunctype::COUNT:
        count_col_idxs_.push_back(i);
        break;
      case Sumfunctype::SUM:
        sum_col_idxs_.push_back(i);
        break;
      case Sumfunctype::MAX:
        if (max_map_.find(kw_scan_cols_[i]) == max_map_.end()) {
          max_col_idxs_.push_back(i);
          max_map_[kw_scan_cols_[i]] = i;
        }
        break;
      case Sumfunctype::MIN:
        if (min_map_.find(kw_scan_cols_[i]) == min_map_.end()) {
          min_col_idxs_.push_back(i);
          min_map_[kw_scan_cols_[i]] = i;
        }
        break;
      case Sumfunctype::LAST_ROW:
        has_last_row_col_ = true;
        final_agg_buffer_is_new_[i] = isVarLenType(attrs_[kw_scan_cols_[i]].type) ? true : false;
        break;
      case Sumfunctype::LASTROWTS:
        has_last_row_col_ = true;
        final_agg_buffer_is_new_[i] = false;
        break;
      case Sumfunctype::FIRST_ROW:
        has_first_row_col_ = true;
        final_agg_buffer_is_new_[i] = isVarLenType(attrs_[kw_scan_cols_[i]].type) ? true : false;
        break;
      case Sumfunctype::FIRSTROWTS:
        has_first_row_col_ = true;
        final_agg_buffer_is_new_[i] = false;
        break;
      case Sumfunctype::MAX_EXTEND:
      case Sumfunctype::MIN_EXTEND:
        final_agg_buffer_is_new_[i] = isVarLenType(attrs_[agg_extend_cols_[i]].type) ? true : false;
        break;
      default:
        LOG_ERROR("Agg function type is not supported in storage engine: %d.", scan_agg_types_[i]);
        return KStatus::FAIL;
        break;
    }
  }
  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    switch (scan_agg_types_[i]) {
      case Sumfunctype::MAX_EXTEND:
        if (max_map_.find(kw_scan_cols_[i]) == max_map_.end()) {
          max_col_idxs_.push_back(i);
          max_map_[kw_scan_cols_[i]] = i;
        } else {
          if (agg_extend_cols_[max_map_[kw_scan_cols_[i]]] < 0) {
            agg_extend_cols_[max_map_[kw_scan_cols_[i]]] = kw_scan_cols_[i];
          }
        }
        break;
      case Sumfunctype::MIN_EXTEND:
        if (min_map_.find(kw_scan_cols_[i]) == min_map_.end()) {
          min_col_idxs_.push_back(i);
          min_map_[kw_scan_cols_[i]] = i;
        } else {
          if (agg_extend_cols_[min_map_[kw_scan_cols_[i]]] < 0) {
            agg_extend_cols_[min_map_[kw_scan_cols_[i]]] = kw_scan_cols_[i];
          }
        }
        break;
      default:
        break;
    }
  }
  candidates_.resize(kw_scan_cols_.size());

  first_last_only_agg_ = (count_col_idxs_.size() + sum_col_idxs_.size() + max_col_idxs_.size() + min_col_idxs_.size() == 0);

  // This partition sort can be removed if the partitions got from ts version manager are sorted.
  if (first_col_idxs_.size() > 0 || last_col_idxs_.size() > 0 || has_first_row_col_ || has_last_row_col_) {
    std::sort(ts_partitions_.begin(), ts_partitions_.end(), PartitionLessThan);
  }

  only_count_ts_ = (CLUSTER_SETTING_COUNT_USE_STATISTICS && scan_agg_types_.size() == 1
        && scan_agg_types_[0] == Sumfunctype::COUNT && kw_scan_cols_.size() == 1 && kw_scan_cols_[0] == 0);

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    if (scan_agg_types_[i] != LAST_ROW && scan_agg_types_[i] != LASTROWTS) {
      if ((scan_agg_types_[i] == LAST || scan_agg_types_[i] == LASTTS) &&
           attrs_[kw_scan_cols_[i]].isFlag(AINFO_NOT_NULL)) {
        continue;
      }
      only_last_row_ = false;
    }
  }

  cur_entity_index_ = 0;

  return KStatus::SUCCESS;
}

bool TsAggIteratorV2Impl::IsDisordered() {
  return false;
}

KStatus TsAggIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  *count = 0;
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  std::fill(final_agg_data_.begin(), final_agg_data_.end(), TSSlice{nullptr, 0});

  cur_first_col_idxs_ = first_col_idxs_;
  cur_last_col_idxs_ = last_col_idxs_;
  for (auto first_col_idx : cur_first_col_idxs_) {
    candidates_[first_col_idx].blk_span = nullptr;
    candidates_[first_col_idx].ts = INT64_MAX;
  }
  for (auto last_col_idx : cur_last_col_idxs_) {
    candidates_[last_col_idx].blk_span = nullptr;
    candidates_[last_col_idx].ts = INT64_MIN;
  }

  std::fill(is_overflow_.begin(), is_overflow_.end(), false);

  for (auto count_col_idx : count_col_idxs_) {
    final_agg_data_[count_col_idx].len = sizeof(uint64_t);
    final_agg_data_[count_col_idx].data = static_cast<char*>(malloc(final_agg_data_[count_col_idx].len));
    memset(final_agg_data_[count_col_idx].data, 0, final_agg_data_[count_col_idx].len);
  }

  if (has_first_row_col_) {
    first_row_candidate_.blk_span = nullptr;
    first_row_candidate_.ts = INT64_MAX;
  }
  if (has_last_row_col_) {
    last_row_candidate_.blk_span = nullptr;
    last_row_candidate_.ts = INT64_MIN;
  }

  KStatus ret;
  timestamp64 entity_last_ts = INVALID_TS;
  std::vector<KwTsSpan> ts_spans_bkup;
  std::vector<std::shared_ptr<const TsPartitionVersion>> ts_partitions_bkup;
  if (only_last_row_) {
    ret = vgroup_->GetEntityLastRow(table_schema_mgr_, entity_ids_[cur_entity_index_], ts_spans_, entity_last_ts);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetEntityLastRow failed.");
      return ret;
    }
    if (entity_last_ts != INVALID_TS && (last_ts_points_.empty() || entity_last_ts <=
                                         *min_element(last_ts_points_.begin(), last_ts_points_.end()))) {
      ts_spans_bkup.swap(ts_spans_);
      ts_partitions_bkup.swap(ts_partitions_);
      ts_spans_.clear();
      ts_spans_.push_back({entity_last_ts, entity_last_ts});
      auto current = vgroup_->CurrentVersion();
      ts_partitions_ = current->GetPartitions(db_id_, ts_spans_, ts_col_type_);
    }
  }

  if (only_count_ts_) {
    ret = CountAggregate();
  } else {
    ret = Aggregate();
  }
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  res->clear();
  if (only_count_ts_ && (KInt64(final_agg_data_[0].data) == 0)) {
    free(final_agg_data_[0].data);
    final_agg_data_[0].data = nullptr;
    *count = 0;
    *is_finished = false;
    ++cur_entity_index_;
    return KStatus::SUCCESS;
  }
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    TSSlice& slice = final_agg_data_[i];
    Batch* b;
    uint32_t col_idx = (scan_agg_types_[i] == Sumfunctype::MAX_EXTEND || scan_agg_types_[i] == Sumfunctype::MIN_EXTEND) ?
                       agg_extend_cols_[i] : kw_scan_cols_[i];
    if (slice.data == nullptr) {
      b = new AggBatch(nullptr, 0, nullptr);
    } else if (!isVarLenType(attrs_[col_idx].type) || scan_agg_types_[i] == Sumfunctype::COUNT) {
      b = new AggBatch(slice.data, 1, nullptr);
      b->is_new = final_agg_buffer_is_new_[i];
      b->is_overflow = is_overflow_[i];
    } else {
      std::shared_ptr<void> ptr(slice.data, free);
      b = new AggBatch(ptr, 1, nullptr);
    }
    res->push_back(i, b);
  }

  res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};
  res->col_num_ = kw_scan_cols_.size();
  *count = 1;

  *is_finished = false;
  ++cur_entity_index_;
  if (only_last_row_ && entity_last_ts != INVALID_TS) {
    ts_spans_.swap(ts_spans_bkup);
    ts_partitions_.swap(ts_partitions_bkup);
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::Aggregate() {
  // Scan forwards to aggrate first col along with other agg functions
  int first_partition_idx = 0;
  for (; first_partition_idx < ts_partitions_.size(); ++first_partition_idx) {
    if (cur_first_col_idxs_.empty() && !has_first_row_col_) {
      break;
    }
    cur_partition_index_ = first_partition_idx;
    TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_lsn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_];
    ts_block_spans_.clear();
    auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
    ret = UpdateAggregation(false);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }

  // Scan backwards to aggrate last col along with other agg functions
  int last_partition_idx = ts_partitions_.size() - 1;
  for (; last_partition_idx >= first_partition_idx; --last_partition_idx) {
    if (cur_last_col_idxs_.empty() && !has_last_row_col_) {
      break;
    }
    cur_partition_index_ = last_partition_idx;
    TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_lsn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_];
    ts_block_spans_.clear();
    auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
    ret = UpdateAggregation(true);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }

  if (!first_last_only_agg_) {
    // first and last col aggregations are done, so remove them.
    cur_first_col_idxs_.clear();
    cur_last_col_idxs_.clear();
    for (; first_partition_idx <= last_partition_idx; ++first_partition_idx) {
      cur_partition_index_ = first_partition_idx;
      TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                                entity_ids_[cur_entity_index_], ts_col_type_, scan_lsn_, ts_spans_};
      auto partition_version = ts_partitions_[cur_partition_index_];
      ts_block_spans_.clear();
      auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      ret = UpdateAggregation(true);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    Sumfunctype agg_type = scan_agg_types_[i];
    if (agg_type == Sumfunctype::COUNT || agg_type == Sumfunctype::SUM) {
      continue;
    }
    if (agg_type == Sumfunctype::MAX || agg_type == Sumfunctype::MIN) {
      if ((agg_type == Sumfunctype::MAX && max_map_[kw_scan_cols_[i]] != i)
          || (agg_type ==Sumfunctype::MIN && min_map_[kw_scan_cols_[i]] != i)) {
        final_agg_data_[i].len = final_agg_data_[min_map_[kw_scan_cols_[i]]].len;
        final_agg_data_[i].data = static_cast<char*>(malloc(final_agg_data_[i].len));
        memcpy(final_agg_data_[i].data, final_agg_data_[min_map_[kw_scan_cols_[i]]].data, final_agg_data_[i].len);
      }
      continue;
    }
    auto& c = ((agg_type == Sumfunctype::LAST_ROW || agg_type == Sumfunctype::LASTROWTS) ?
                last_row_candidate_ :
                  (agg_type == Sumfunctype::LAST || agg_type == Sumfunctype::LASTTS) ?
                  candidates_[last_ts_points_.empty() ? last_map_[kw_scan_cols_[i]] : i] :
                    (agg_type == Sumfunctype::FIRST_ROW || agg_type == Sumfunctype::FIRSTROWTS) ?
                    first_row_candidate_ :
                      (agg_type == Sumfunctype::FIRST || agg_type == Sumfunctype::FIRSTTS) ?
                      candidates_[first_map_[kw_scan_cols_[i]]] :
                        (agg_type == Sumfunctype::MAX_EXTEND) ?
                        candidates_[max_map_[kw_scan_cols_[i]]] : candidates_[min_map_[kw_scan_cols_[i]]]);
    const k_uint32 col_idx = (agg_type == Sumfunctype::MAX_EXTEND || agg_type == Sumfunctype::MIN_EXTEND) ?
                             agg_extend_cols_[i] : kw_scan_cols_[i];
    if (final_agg_data_[i].data) {
      free(final_agg_data_[i].data);
      final_agg_data_[i].data = nullptr;
    }
    if (c.blk_span == nullptr) {
      final_agg_data_[i] = {nullptr, 0};
    } else if (agg_type == Sumfunctype::FIRSTTS || agg_type == Sumfunctype::LASTTS
              || agg_type == Sumfunctype::FIRSTROWTS || agg_type == Sumfunctype::LASTROWTS) {
      final_agg_data_[i].data = static_cast<char*>(malloc(sizeof(timestamp64)));
      memcpy(final_agg_data_[i].data, &c.ts, sizeof(timestamp64));
      final_agg_data_[i].len = sizeof(timestamp64);
      final_agg_buffer_is_new_[i] = true;
      /* crash with following code to avoid malloc and memcpy.
      char* value = nullptr;
      TsBitmap bitmap;
      auto ret = c.blk_span->GetFixLenColAddr(0, &value, bitmap, false);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }

      final_agg_data_[i].len = c.blk_span->GetColSize(0);
      final_agg_data_[i].data = value + c.row_idx * final_agg_data_[i].len;
      */
    } else {
      if (!c.blk_span->IsColExist(col_idx)) {
        if (agg_type == Sumfunctype::FIRST_ROW || agg_type == Sumfunctype::LAST_ROW) {
          final_agg_data_[i] = {nullptr, 0};
        } else {
          LOG_ERROR("Something is wrong here since column doesn't exist and we should not have any candidates.")
          return KStatus::FAIL;
        }
      } else {
        if (!c.blk_span->IsVarLenType(col_idx)) {
          char* value = nullptr;
          TsBitmap bitmap;
          auto ret = c.blk_span->GetFixLenColAddr(col_idx, &value, bitmap, false);
          if (ret != KStatus::SUCCESS) {
            return ret;
          }

          if (!attrs_[col_idx].isFlag(AINFO_NOT_NULL) && bitmap[c.row_idx] != DataFlags::kValid) {
            final_agg_data_[i] = {nullptr, 0};
          } else {
            final_agg_data_[i].len = c.blk_span->GetColSize(col_idx);
            final_agg_data_[i].data = value + c.row_idx * final_agg_data_[i].len;
          }
        } else {
          TSSlice slice;
          DataFlags flag;
          auto ret = c.blk_span->GetVarLenTypeColAddr(c.row_idx, col_idx, flag, slice);
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
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::CountAggregate() {
  KStatus ret;
  for (int idx = 0; idx < ts_partitions_.size(); idx++) {
    cur_partition_index_ = idx;
    TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_lsn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_];
    auto count_manager = partition_version->GetCountManager();
    TsEntityCountHeader count_header{};
    count_header.entity_id = entity_ids_[cur_entity_index_];
    count_manager->GetEntityCountHeader(&count_header);
    if (count_header.is_count_valid && checkTimestampWithSpans(ts_spans_, count_header.min_ts, count_header.max_ts) ==
    TimestampCheckResult::FullyContained) {
      std::list<shared_ptr<TsBlockSpan>> mem_block_spans;
      ret = partition_version->GetBlockSpans(filter, &mem_block_spans, table_schema_mgr_, schema_,
                                            false, true, true);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("partition_version get mem block span failed.");
        return ret;
      }
      if (mem_block_spans.empty()) {
        KUint64(final_agg_data_[0].data) += count_header.valid_count;
      } else {
        uint64_t mem_count = 0;
        std::vector<KwTsSpan> mem_ts_spans;
        TsBlockSpanSortedIterator iter(mem_block_spans, EngineOptions::g_dedup_rule);
        iter.Init();
        std::shared_ptr<TsBlockSpan> mem_block;
        bool is_finished = false;
        while (iter.Next(mem_block, &is_finished) == KStatus::SUCCESS && !is_finished) {
          mem_ts_spans.push_back({mem_block->GetFirstTS(), mem_block->GetLastTS()});
          mem_count += mem_block->GetRowNum();
        }
        if (EngineOptions::g_dedup_rule == DedupRule::KEEP ||
        (checkTimestampWithSpans(mem_ts_spans, count_header.min_ts, count_header.max_ts) ==
        TimestampCheckResult::NonOverlapping)) {
          KUint64(final_agg_data_[0].data) += count_header.valid_count + mem_count;
        } else {
          ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("e_paritition GetBlockSpan failed.");
            return ret;
          }
        }
      }
    } else {
      ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      if (!count_header.is_count_valid && checkTimestampWithSpans(ts_spans_, count_header.min_ts, count_header.max_ts) ==
                                          TimestampCheckResult::FullyContained) {
        ret = RecalculateCountInfo(partition_version, count_manager);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("RecalculateCountInfo entity[%lu] failed.", count_header.entity_id);
        }
      }
    }
    ret = UpdateAggregation(false);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorV2Impl::RecalculateCountInfo(std::shared_ptr<const TsPartitionVersion> partition,
                                                  shared_ptr<TsPartitionEntityCountManager> count_manager) {
  KStatus ret;
  ret = count_manager->PrepareEntityCountValid(entity_ids_[cur_entity_index_]);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("PrepareEntityCountValid entity[%u] failed.", entity_ids_[cur_entity_index_]);
    return ret;
  }
  auto partition_id = partition->GetPartitionIdentifier();
  auto latest_partition = vgroup_->CurrentVersion()->GetPartition(std::get<0>(partition_id), std::get<1>(partition_id));
  std::list<shared_ptr<TsBlockSpan>> count_block_spans;
  std::vector<KwTsSpan> ts_spans = {{latest_partition->GetTsColTypeStartTime(ts_col_type_),
                                     latest_partition->GetTsColTypeEndTime(ts_col_type_)}};
  TsScanFilterParams count_filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                                  entity_ids_[cur_entity_index_], ts_col_type_, UINT64_MAX, ts_spans};
  ret = latest_partition->GetBlockSpans(count_filter, &count_block_spans, table_schema_mgr_, schema_, true);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("RecalculateCountInfo get mem block span failed.");
    return ret;
  }
  TsBlockSpanSortedIterator iter(count_block_spans, EngineOptions::g_dedup_rule);
  iter.Init();
  std::shared_ptr<TsBlockSpan> dedup_block_span;
  bool is_finished = false;
  TsEntityFlushInfo flush_info{entity_ids_[cur_entity_index_], INVALID_TS, INVALID_TS, 0, ""};
  while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
    if (flush_info.min_ts > dedup_block_span->GetFirstTS()) {
      flush_info.min_ts = dedup_block_span->GetFirstTS();
    }
    if (flush_info.max_ts == INVALID_TS || flush_info.max_ts < dedup_block_span->GetLastTS()) {
      flush_info.max_ts = dedup_block_span->GetLastTS();
    }
    flush_info.deduplicate_count += dedup_block_span->GetRowNum();
  }
  if (flush_info.deduplicate_count > 0) {
    ret = count_manager->SetEntityCountValid(flush_info.entity_id, &flush_info);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("SetEntityCountValid entity[%lu] failed.", flush_info.entity_id);
      return ret;
    }
  }
  return KStatus::SUCCESS;
}

inline bool FirstTSLessThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetFirstTS() < b->GetFirstTS();
}

inline bool LastTSLessThan(shared_ptr<TsBlockSpan>& a, shared_ptr<TsBlockSpan>& b) {
  return a->GetLastTS() < b->GetLastTS();
}

KStatus TsAggIteratorV2Impl::UpdateAggregation(bool can_remove_last_candidate) {
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
    ts_block_spans.push_back(std::move(dedup_block_span));
  }
  ts_block_spans_.clear();
  if (ts_block_spans.empty()) {
    return KStatus::SUCCESS;
  }

  int block_span_idx = 0;
  if (!cur_first_col_idxs_.empty() || has_first_row_col_) {
    if (has_first_row_col_) {
      if (first_row_candidate_.ts > ts_block_spans[0]->GetFirstTS()) {
        first_row_candidate_.blk_span = ts_block_spans[0];
        first_row_candidate_.ts = first_row_candidate_.blk_span->GetFirstTS();
        first_row_candidate_.row_idx = 0;
      }
    }
    while (block_span_idx < ts_block_spans.size() && !cur_first_col_idxs_.empty()) {
      shared_ptr<TsBlockSpan>& blk_span = ts_block_spans[block_span_idx];
      ret = UpdateAggregation(blk_span, true, false);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      ++block_span_idx;
    }
  }

  int block_span_backward_idx = ts_block_spans.size() - 1;
  if (!cur_last_col_idxs_.empty() || has_last_row_col_) {
    if (has_last_row_col_) {
      if (last_row_candidate_.ts < ts_block_spans[block_span_backward_idx]->GetLastTS()) {
        last_row_candidate_.blk_span = ts_block_spans[block_span_backward_idx];
        last_row_candidate_.ts = last_row_candidate_.blk_span->GetLastTS();
        last_row_candidate_.row_idx = last_row_candidate_.blk_span->GetRowNum() - 1;
      }
    }
    while (block_span_idx <= block_span_backward_idx && !cur_last_col_idxs_.empty()) {
      shared_ptr<TsBlockSpan>& blk_span = ts_block_spans[block_span_backward_idx];
      ret = UpdateAggregation(blk_span, true, can_remove_last_candidate);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      --block_span_backward_idx;
    }
  }

  if (!first_last_only_agg_) {
    for (; block_span_idx <= block_span_backward_idx; ++block_span_idx) {
      shared_ptr<TsBlockSpan>& blk_span = ts_block_spans[block_span_idx];
      ret = UpdateAggregation(blk_span, false, can_remove_last_candidate);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }
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
                                                bool aggregate_first_last_cols,
                                                bool can_remove_last_candidate) {
  KStatus ret;
  int row_num = block_span->GetRowNum();

  if (aggregate_first_last_cols) {
    // Aggregate first col
    if (!cur_first_col_idxs_.empty()) {
      int i = cur_first_col_idxs_.size() - 1;
      while (i >= 0) {
        k_uint32 idx = cur_first_col_idxs_[i];
        auto kw_col_idx = kw_scan_cols_[idx];
        if (!block_span->IsColExist(kw_col_idx)) {
          // No data for this column in this block span, so just move on to the next first col.
          --i;
          continue;
        }
        AggCandidate& candidate = candidates_[idx];
        TsBitmap bitmap;
        ret = block_span->GetColBitmap(kw_col_idx, bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          int64_t ts = block_span->GetTS(row_idx);
          if (!candidate.blk_span || candidate.ts > ts) {
            candidate.blk_span = block_span;
            candidate.ts = ts;
            candidate.row_idx = row_idx;
            // Found the first candidate, so remove it from cur_first_col_idxs_
            cur_first_col_idxs_.erase(cur_first_col_idxs_.begin() + i);
            break;
          }
        }
        --i;
      }
    }

    // Aggregate last col
    if (!cur_last_col_idxs_.empty()) {
      int i = cur_last_col_idxs_.size() - 1;
      while (i >= 0) {
        k_uint32 idx = cur_last_col_idxs_[i];
        auto kw_col_idx = kw_scan_cols_[idx];
        if (!block_span->IsColExist(kw_col_idx)) {
          // No data for this column in this block span, so just move on to the next last col.
          --i;
          continue;
        }
        AggCandidate& candidate = candidates_[idx];
        TsBitmap bitmap;
        ret = block_span->GetColBitmap(kw_col_idx, bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        for (int row_idx = row_num - 1; row_idx >= 0; --row_idx) {
          if (bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          int64_t ts = block_span->GetTS(row_idx);
          if ((last_ts_points_.empty() || ts <= last_ts_points_[idx])
              && (!candidate.blk_span || candidate.ts < ts)) {
            candidate.blk_span = block_span;
            candidate.ts = ts;
            candidate.row_idx = row_idx;
            if (can_remove_last_candidate) {
              /* We are seachhing last cndidate backward, so it can be removed
              * from cur_last_col_idxs_ if last candidate is found.
              */
              cur_last_col_idxs_.erase(cur_last_col_idxs_.begin() + i);
            }
            break;
          }
        }
        --i;
      }
    }
  }

  // Aggregate count col
  for (auto idx : count_col_idxs_) {
    auto kw_col_idx = kw_scan_cols_[idx];
    if (!block_span->IsColExist(kw_col_idx)) {
      // No data for this column in this block span, so just move on to the next last col.
      continue;
    }
    if (block_span->IsColNotNull(kw_col_idx)) {
      KUint64(final_agg_data_[idx].data) += block_span->GetRowNum();
    } else {
      if (block_span->HasPreAgg()) {
        // Use pre agg to calculate count
        uint16_t pre_count{0};
        ret = block_span->GetPreCount(kw_col_idx, pre_count);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        KUint64(final_agg_data_[idx].data) += pre_count;
      } else {
        uint32_t col_count{0};
        ret = block_span->GetCount(kw_col_idx, col_count);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        KUint64(final_agg_data_[idx].data) += col_count;
      }
    }
  }

  // Aggregate sum col
  for (auto idx : sum_col_idxs_) {
    auto kw_col_idx = kw_scan_cols_[idx];
    if (!block_span->IsColExist(kw_col_idx)) {
      // No data for this column in this block span, so just move on to the next last col.
      continue;
    }
    auto type = block_span->GetColType(kw_col_idx);
    if (block_span->HasPreAgg()) {
      // Use pre agg to calculate sum
      void* pre_sum{nullptr};
      bool pre_sum_is_overflow{false};
      ret = block_span->GetPreSum(kw_col_idx, pre_sum, pre_sum_is_overflow);
      if (ret != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
      if (!pre_sum) {
        continue;
      }
      TSSlice& agg_data = final_agg_data_[idx];
      if (agg_data.data == nullptr) {
        agg_data.len = sizeof(int64_t);
        InitAggData(agg_data);
        InitSumValue(agg_data.data, type);
      }
      if (!is_overflow_[idx]) {
        if (!pre_sum_is_overflow) {
          ret = AddSumNotOverflowYet(idx, type, pre_sum, agg_data);
        } else {
          ret = AddSumNotOverflowYet(idx, DATATYPE::DOUBLE, pre_sum, agg_data);
        }
      } else {
        if (!pre_sum_is_overflow) {
          ret = AddSumOverflow(type, pre_sum, agg_data);
        } else {
          ret = AddSumOverflow(DATATYPE::DOUBLE, pre_sum, agg_data);
        }
      }
      if (ret != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    } else {
      char* value = nullptr;
      TsBitmap bitmap;
      auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, bitmap, false);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetFixLenColAddr failed.");
        return s;
      }

      int32_t size = block_span->GetColSize(kw_col_idx);
      for (int row_idx = 0; row_idx < row_num; ++row_idx) {
        if (!attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL) && bitmap[row_idx] != DataFlags::kValid) {
          continue;
        }
        void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
        TSSlice& agg_data = final_agg_data_[idx];
        if (agg_data.data == nullptr) {
          agg_data.len = sizeof(int64_t);
          InitAggData(agg_data);
          InitSumValue(agg_data.data, type);
        }
        if (!is_overflow_[idx]) {
          ret = AddSumNotOverflowYet(idx, type, current, agg_data);
          if (ret != KStatus::SUCCESS) {
            return KStatus::FAIL;
          }
        }
        if (is_overflow_[idx]) {
          ret = AddSumOverflow(type, current, agg_data);
          if (ret != KStatus::SUCCESS) {
            return KStatus::FAIL;
          }
        }
      }
    }
  }

  // Aggregate max col
  for (auto idx : max_col_idxs_) {
    auto kw_col_idx = kw_scan_cols_[idx];
    if (!block_span->IsColExist(kw_col_idx)) {
      // No data for this column in this block span, so just move on to the next last col.
      continue;
    }
    TSSlice& agg_data = final_agg_data_[idx];
    auto type = block_span->GetColType(kw_col_idx);
    if (agg_extend_cols_[idx] < 0 && block_span->IsSameType(kw_col_idx)
        && block_span->HasPreAgg()) {
      // Use pre agg to calculate max
      if (!block_span->IsVarLenType(kw_col_idx)) {
        void* pre_max{nullptr};
        int32_t size = kw_col_idx == 0 ? 8 : block_span->GetColSize(kw_col_idx);
        ret = block_span->GetPreMax(kw_col_idx, pre_max);  // pre agg max(timestamp) use 8 bytes
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (!pre_max) {
          continue;
        }
        bool need_copy{false};
        if (agg_data.data == nullptr) {
          agg_data.len = size;
          InitAggData(agg_data);
          need_copy = true;
        } else if (cmp(pre_max, agg_data.data, type, kw_col_idx == 0 ? 8 : size) > 0) {
          need_copy = true;
        }
        if (need_copy) {
          memcpy(agg_data.data, pre_max, kw_col_idx == 0 ? 8 : size);
        }
      } else {
        TSSlice pre_max{nullptr, 0};
        ret = block_span->GetVarPreMax(kw_col_idx, pre_max);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (pre_max.data) {
          string pre_max_val(pre_max.data, pre_max.len);
          if (agg_data.data) {
            string current_max({agg_data.data + kStringLenLen, agg_data.len - kStringLenLen});
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
      if (!block_span->IsVarLenType(kw_col_idx)) {
        char* value = nullptr;
        TsBitmap bitmap;
        auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, bitmap, false);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }

        int32_t size = block_span->GetColSize(kw_col_idx);
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (!attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL) && bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
          if (agg_data.data == nullptr) {
            agg_data.len = size;
            InitAggData(agg_data);
            memcpy(agg_data.data, current, size);
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idx, block_span};
            }
          } else if (cmp(current, agg_data.data, type, size) > 0) {
            memcpy(agg_data.data, current, size);
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idx, block_span};
            }
          }
        }
      } else {
        std::vector<string> var_rows;
        std::vector<int> row_idxs;
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          TSSlice slice;
          DataFlags flag;
          ret = block_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, flag, slice);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("GetVarLenTypeColAddr failed.");
            return ret;
          }
          if (flag == DataFlags::kValid) {
            var_rows.emplace_back(slice.data, slice.len);
            row_idxs.push_back(row_idx);
          }
        }
        if (!var_rows.empty()) {
          auto max_it = std::max_element(var_rows.begin(), var_rows.end());
          if (agg_data.data) {
            string current_max({agg_data.data + kStringLenLen, agg_data.len - kStringLenLen});
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
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idxs[max_it - var_rows.begin()], block_span};
            }
          }
        }
      }
    }
  }

  // Aggregate min col
  for (auto idx : min_col_idxs_) {
    auto kw_col_idx = kw_scan_cols_[idx];
    if (!block_span->IsColExist(kw_col_idx)) {
      // No data for this column in this block span, so just move on to the next last col.
      continue;
    }
    TSSlice& agg_data = final_agg_data_[idx];
    auto type = block_span->GetColType(kw_col_idx);
    // TODO(zqh): both integer or both char can use pre agg
    if (agg_extend_cols_[idx] < 0 && block_span->IsSameType(kw_col_idx)
        && block_span->HasPreAgg()) {
      // Use pre agg to calculate min
      if (!block_span->IsVarLenType(kw_col_idx)) {
        void* pre_min{nullptr};
        int32_t size = block_span->GetColSize(kw_col_idx);
        ret = block_span->GetPreMin(kw_col_idx, pre_min);  // pre agg min(timestamp) use 8 bytes
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (!pre_min) {
          continue;
        }
        bool need_copy{false};
        if (agg_data.data == nullptr) {
          agg_data.len = size;
          InitAggData(agg_data);
          need_copy = true;
        } else if (cmp(pre_min, agg_data.data, type, kw_col_idx == 0 ? 8 : size) < 0) {
          need_copy = true;
        }
        if (need_copy) {
          memcpy(agg_data.data, pre_min, kw_col_idx == 0 ? 8 : size);
        }
      } else {
        TSSlice pre_min{nullptr, 0};
        ret = block_span->GetVarPreMin(kw_col_idx, pre_min);
        if (ret != KStatus::SUCCESS) {
          return KStatus::FAIL;
        }
        if (pre_min.data) {
          string pre_min_val(pre_min.data, pre_min.len);
          if (agg_data.data) {
            string current_min({agg_data.data + kStringLenLen, agg_data.len - kStringLenLen});
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
      if (!block_span->IsVarLenType(kw_col_idx)) {
        char* value = nullptr;
        TsBitmap bitmap;
        auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, bitmap, false);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }

        int32_t size = block_span->GetColSize(kw_col_idx);
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (!attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL) && bitmap[row_idx] != DataFlags::kValid) {
            continue;
          }
          void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
          if (agg_data.data == nullptr) {
            agg_data.len = size;
            InitAggData(agg_data);
            memcpy(agg_data.data, current, size);
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idx, block_span};
            }
          } else if (cmp(current, agg_data.data, type, size) < 0) {
            memcpy(agg_data.data, current, size);
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idx, block_span};
            }
          }
        }
      } else {
        std::vector<string> var_rows;
        std::vector<int> row_idxs;
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          TSSlice slice;
          DataFlags flag;
          ret = block_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, flag, slice);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("GetVarLenTypeColAddr failed.");
            return ret;
          }
          if (flag == DataFlags::kValid) {
            var_rows.emplace_back(slice.data, slice.len);
            row_idxs.push_back(row_idx);
          }
        }
        if (!var_rows.empty()) {
          auto min_it = std::min_element(var_rows.begin(), var_rows.end());
          if (agg_data.data) {
            string current_min({agg_data.data + kStringLenLen, agg_data.len - kStringLenLen});
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
            if (agg_extend_cols_[idx] >= 0) {
              candidates_[idx] = {-1, row_idxs[min_it - var_rows.begin()], block_span};
            }
          }
        }
      }
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::divideBlockSpans(timestamp64 begin_ts, timestamp64 end_ts, uint32_t* lower_cnt,
                                                 deque<std::shared_ptr<TsBlockSpan>>& lower_block_span) {
  uint32_t size = filter_block_spans_.size();
  timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
  for (uint32_t i = 0; i < size; ++i) {
    std::shared_ptr<TsBlockSpan> block_span = filter_block_spans_.front();
    filter_block_spans_.pop_front();
    uint32_t vgroup_id = block_span->GetVGroupID();
    uint32_t entity_id = block_span->GetEntityID();
    std::shared_ptr<TsBlock> block = block_span->GetTsBlock();
    timestamp64 min_ts, max_ts;
    block_span->GetTSRange(&min_ts, &max_ts);
    if ((is_reversed_ && min_ts > mid_ts) || (!is_reversed_ && max_ts <= mid_ts)) {
      *lower_cnt += block_span->GetRowNum();
      lower_block_span.push_back(block_span);
    } else if ((is_reversed_ && max_ts <= min_ts) || (!is_reversed_ && min_ts > mid_ts)) {
      filter_block_spans_.push_back(block_span);
    } else {
      // TODO(lmz): code review here, is_lower_part is uninitialized. It may cause a bug.
      //  is_lower_part = true to avoid compile error.
      bool is_lower_part = true;
      int first_row = block_span->GetStartRow(), start_row = block_span->GetStartRow();
      uint32_t row_num = block_span->GetRowNum();
      for (int j = start_row; j < start_row + row_num; ++j) {
        timestamp64 cur_ts = block_span->GetTS(j - start_row);
        if (j == start_row) {
          is_lower_part = (is_reversed_ == (cur_ts > mid_ts));
          min_ts = max_ts = cur_ts;
        } else if (is_lower_part && ((is_reversed_ && cur_ts <= mid_ts) || (!is_reversed_ && cur_ts > mid_ts))) {
          *lower_cnt += (j - first_row);
          lower_block_span.push_back(make_shared<TsBlockSpan>(*block_span, block, first_row, j - first_row));
          first_row = j;
          is_lower_part = false;
          min_ts = max_ts = cur_ts;
        } else if (!is_lower_part && ((is_reversed_ && cur_ts > mid_ts) || (!is_reversed_ && cur_ts <= mid_ts))) {
          filter_block_spans_.push_back(make_shared<TsBlockSpan>(*block_span, block, first_row, j - first_row));
          first_row = j;
          is_lower_part = true;
          min_ts = max_ts = cur_ts;
        } else {
          min_ts = min(min_ts, cur_ts);
          max_ts = max(max_ts, cur_ts);
        }
      }
      if (first_row < start_row + row_num) {
        if (is_lower_part) {
          *lower_cnt += (start_row + row_num - first_row);
          lower_block_span.push_back(make_shared<TsBlockSpan>(*block_span, block, first_row,
                                                              start_row + row_num - first_row));
        } else {
          filter_block_spans_.push_back(make_shared<TsBlockSpan>(*block_span, block, first_row,
                                                                 start_row + row_num - first_row));
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::filterLower(uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetStartTime(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetEndTime(), ts_col_type_);
  while (!filter_end_) {
    uint32_t lower_cnt = 0;
    deque<std::shared_ptr<TsBlockSpan>> lower_block_span;
    if (divideBlockSpans(begin_ts, end_ts, &lower_cnt, lower_block_span) != KStatus::SUCCESS) {
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
        *cnt += filter_block_spans_.front()->GetRowNum();
        filter_block_spans_.pop_front();
      }
      while (!lower_block_span.empty()) {
        filter_block_spans_.push_back(lower_block_span.front());
        lower_block_span.pop_front();
      }
    }
    filter_end_ = (filter_cnt_ > (offset_ - deviation_)) || (end_ts - begin_ts < t_time_);
  }
  while (!filter_block_spans_.empty()) {
    block_spans_.push_back(filter_block_spans_.front());
    *cnt += filter_block_spans_.front()->GetRowNum();
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::filterUpper(uint32_t filter_num, uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetStartTime(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetEndTime(), ts_col_type_);
  bool filter_end = false;
  while (!filter_end) {
    uint32_t lower_cnt = 0;
    deque<std::shared_ptr<TsBlockSpan>> lower_block_span;
    timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
    if (divideBlockSpans(begin_ts, end_ts, &lower_cnt, lower_block_span) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    if (lower_cnt >= filter_num) {
      is_reversed_ ? begin_ts = mid_ts : end_ts = mid_ts;
      std::deque<std::shared_ptr<TsBlockSpan>>().swap(filter_block_spans_);
      while (!lower_block_span.empty()) {
        filter_block_spans_.push_back(lower_block_span.front());
        lower_block_span.pop_front();
      }
    } else {
      is_reversed_ ? end_ts = mid_ts : begin_ts = mid_ts;
      while (!lower_block_span.empty()) {
        block_spans_.push_back(lower_block_span.front());
        lower_block_span.pop_front();
      }
      *cnt += lower_cnt;
      filter_num -= lower_cnt;
    }
    filter_end = (filter_num <= 0) || (end_ts - begin_ts < t_time_);
  }
  while (!filter_block_spans_.empty()) {
    *cnt += filter_block_spans_.front()->GetRowNum();
    block_spans_.push_back(filter_block_spans_.front());
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::ScanPartitionBlockSpans(uint32_t* cnt) {
  *cnt = 0;
  KStatus ret;
  for (const auto& it : p_time_it_->second) {
    uint32_t vgroup_id = it.first;
    std::shared_ptr<const TsPartitionVersion> partition_version = it.second;
    std::shared_ptr<TsVGroup> vgroup = vgroups_[vgroup_id];
    std::vector<EntityID> entity_ids = vgroup_ids_[vgroup_id];
    // TODO(liumengzhen) filter参数能否支持多设备
    for (auto entity_id : entity_ids) {
      TsScanFilterParams filter{db_id_, table_id_, vgroup_id, entity_id, ts_col_type_, scan_lsn_, ts_spans_};
      ts_block_spans_.clear();
      ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpan failed.");
        return KStatus::FAIL;
      }
      for (const auto& block_span : ts_block_spans_) {
        *cnt += block_span->GetRowNum();
        filter_block_spans_.push_back(block_span);
      }
    }
  }
  return ret;
}

KStatus TsOffsetIteratorV2Impl::Init(bool is_reversed) {
  GetTerminationTime();
  is_reversed_ = is_reversed;
  comparator_.is_reversed = is_reversed_;
  decltype(p_times_) t_map(comparator_);
  p_times_.swap(t_map);
  attrs_ = schema_->getSchemaInfoExcludeDropped();
  db_id_ = table_schema_mgr_->GetDbID();
  table_id_ = table_schema_mgr_->GetTableId();

  for (const auto& it : vgroup_ids_) {
    uint32_t vgroup_id = it.first;
    std::shared_ptr<TsVGroup> vgroup = vgroups_[vgroup_id];
    auto current = vgroup->CurrentVersion();
    auto ts_partitions = current->GetPartitions(db_id_, ts_spans_, ts_col_type_);

    for (const auto& partition : ts_partitions) {
      p_times_[partition->GetStartTime()].push_back({vgroup_id, partition});
    }
  }
  p_time_it_ = p_times_.begin();
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::filterBlockSpan() {
  Defer defer{[&]() { ts_block_spans_.clear(); }};
  uint32_t row_cnt = 0;
  KStatus ret = ScanPartitionBlockSpans(&row_cnt);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("call ScanPartitionBlockSpans failed.");
    return KStatus::FAIL;
  }
  if (0 == row_cnt) {
    return KStatus::SUCCESS;
  }
  filter_end_ = (filter_cnt_ > (offset_ - deviation_));
  if (!filter_end_) {
    if (filter_cnt_ + row_cnt <= offset_) {
      filter_cnt_ += row_cnt;
      row_cnt = 0;
      std::deque<std::shared_ptr<TsBlockSpan>>().swap(filter_block_spans_);
      filter_end_ = (filter_cnt_ > (offset_ - deviation_));
      return KStatus::SUCCESS;
    } else {
      if (filterLower(&row_cnt) != KStatus::SUCCESS) {
        return KStatus::FAIL;
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
      return KStatus::FAIL;
    }
  }
  queried_cnt += row_cnt;
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorV2Impl::Next(ResultSet* res, k_uint32* count, timestamp64 ts) {
  *count = 0;
  KStatus ret;
  while (block_spans_.empty()) {
    // scan over
    if (p_time_it_ == p_times_.end() || queried_cnt >= offset_ + limit_ - filter_cnt_) {
      return KStatus::SUCCESS;
    }
    ret = filterBlockSpan();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("call filterBlockSpan failed.");
      return KStatus::FAIL;
    }
    ++p_time_it_;
  }
  // Return one block span data each time.
  shared_ptr<TsBlockSpan> ts_block = block_spans_.front();
  block_spans_.pop_front();
  ret = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, ts_block, res, count);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get next block span for current partition: %ld.", p_time_it_->first);
    return KStatus::FAIL;
  }
  // We are returning memory address inside TsBlockSpan, so we need to keep it until iterator is destroyed
  ts_block_spans_with_data_.push_back(ts_block);
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
