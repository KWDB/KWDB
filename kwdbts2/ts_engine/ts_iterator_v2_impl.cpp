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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "ee_global.h"
#include "engine.h"
#include "ts_iterator_v2_impl.h"
#include "ts_ts_lsn_span_utils.h"
#include "ts_vgroup.h"
#include "ts_split_block_spans.h"

namespace kwdbts {
int64_t TsMaxMilliTimestamp = 31556995200000;  // be associated with 'kwbase/pkg/sql/sem/tree/type_check.go'
int64_t TsMaxMicroTimestamp = 31556995200000000;

KStatus ConvertBlockSpanToResultSet(const std::vector<k_uint32>& kw_scan_cols, const vector<AttributeInfo>& attrs,
                                    shared_ptr<TsBlockSpan>& ts_blk_span, ResultSet* res, k_uint32* count,
                                    TsScanStats* ts_scan_stats) {
  *count = ts_blk_span->GetRowNum();
  KStatus ret;
  std::unique_ptr<TsBitmapBase> ts_bitmap;
  for (int i = 0; i < kw_scan_cols.size(); ++i) {
    auto kw_col_idx = kw_scan_cols[i];
    Batch* batch;
    if (!ts_blk_span->IsColExist(kw_col_idx)) {
      // column is dropped at block version.
      char* bitmap = nullptr;
      batch = new Batch(bitmap, *count, bitmap, 1);
    } else {
      bool col_not_null = attrs[kw_scan_cols[i]].isFlag(AINFO_NOT_NULL);
      unsigned char* bitmap = nullptr;
      if (!col_not_null) {
        bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(*count)));
        if (bitmap == nullptr) {
          return KStatus::FAIL;
        }
        memset(bitmap, 0x00, KW_BITMAP_SIZE(*count));
      }
      if (!ts_blk_span->IsVarLenType(kw_col_idx)) {
        char* value;
        ret = ts_blk_span->GetFixLenColAddr(kw_col_idx, &value, &ts_bitmap, ts_scan_stats);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          free(bitmap);
          return ret;
        }
        if (bitmap != nullptr && !ts_bitmap->IsAllValid()) {
          for (int row_idx = 0; row_idx < *count; ++row_idx) {
            if (ts_bitmap->At(row_idx) != DataFlags::kValid) {
              set_null_bitmap(bitmap, row_idx);
            }
          }
        }
        batch = new Batch(value, *count, reinterpret_cast<char *>(bitmap), 1);
        batch->is_new = false;
      } else {
        batch = new VarColumnBatch(*count, reinterpret_cast<char *>(bitmap), 1);
        auto s = ts_blk_span->GetColBitmap(kw_col_idx, &ts_bitmap, ts_scan_stats);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("ts_blk_span->GetColBitmap failed.");
          delete batch;
          free(bitmap);
          return s;
        }
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          if (ts_bitmap->At(row_idx) != DataFlags::kValid) {
            set_null_bitmap(bitmap, row_idx);
            batch->push_back(nullptr);
          } else {
            TSSlice var_data;
            s = ts_blk_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, var_data, ts_scan_stats);
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("GetVarLenTypeColAddr failed.");
              return s;
            }
            char* buffer = static_cast<char*>(malloc(var_data.len + kStringLenLen));
            if (buffer == nullptr) {
              LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", var_data.len + kStringLenLen);
              return KStatus::FAIL;
            }
            KUint16(buffer) = var_data.len;
            memcpy(buffer + kStringLenLen, var_data.data, var_data.len);
            std::shared_ptr<char> ptr(buffer, free);
            batch->push_back(ptr);
          }
        }
      }
      if (bitmap != nullptr) {
        batch->need_free_bitmap = true;
      }
    }
    res->push_back(i, batch);
  }
  res->entity_index = {1, (uint32_t)ts_blk_span->GetEntityID(), ts_blk_span->GetVGroupID()};
  res->block_span = std::move(ts_blk_span);

  return KStatus::SUCCESS;
}

TsStorageIteratorImpl::TsStorageIteratorImpl() {
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
    merged_spans.emplace_back(merged_span);
    merged_span = sorted_spans[i];
  }
  merged_spans.emplace_back(merged_span);
  return merged_spans;
}

TsStorageIteratorImpl::TsStorageIteratorImpl(const std::shared_ptr<TsVGroup>& vgroup, uint32_t version,
                                                 vector<uint32_t>& entity_ids,
                                                 std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                                                 std::vector<k_uint32>& kw_scan_cols,
                                                 std::vector<k_uint32>& ts_scan_cols,
                                                 const std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                                 const std::shared_ptr<MMapMetricsTable>& schema,
                                                 TS_OSN scan_osn) {
  vgroup_ = vgroup;
  table_version_ = version;
  entity_ids_ = entity_ids;
  ts_spans_ = SortAndMergeSpan(ts_spans);
  block_filter_ = block_filter;
  ts_col_type_ = schema->GetTsColDataType();
  ts_scan_cols_ = ts_scan_cols;
  kw_scan_cols_ = kw_scan_cols;
  table_schema_mgr_ = table_schema_mgr;
  scan_schema_ = schema;
  scan_osn_ = scan_osn;
}

TsStorageIteratorImpl::~TsStorageIteratorImpl() {
}

KStatus TsStorageIteratorImpl::Init(bool is_reversed) {
  is_reversed_ = is_reversed;
  attrs_ = *scan_schema_->getSchemaInfoExcludeDroppedPtr();
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = scan_schema_->metaData()->db_id;
  vgroup_current_version_ = vgroup_->CurrentVersion();
  ts_partitions_ = vgroup_current_version_->GetPartitions(db_id_, ts_spans_, ts_col_type_);
  filter_ = std::make_shared<TsScanFilterParams>(db_id_, table_id_, vgroup_->GetVGroupID(),
                                                  0, ts_col_type_, scan_osn_, ts_spans_);
  return KStatus::SUCCESS;
}

inline void TsStorageIteratorImpl::UpdateTsSpans(timestamp64 ts) {
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

void TsStorageIteratorImpl::SwitchEntity() {
  if (++cur_entity_index_ < entity_ids_.size()) {
    if (TsMemSegment::IsApproachingLimit()) {
      for (auto &partition : ts_partitions_) {
        partition = vgroup_->GetCurrentPartitionVersion(partition->GetPartitionIdentifier());
      }
    }
  }
}

inline bool TsStorageIteratorImpl::IsFilteredOut(timestamp64 begin_ts, timestamp64 end_ts, timestamp64 ts) {
  return  (!is_reversed_ && begin_ts > ts) || (is_reversed_ && end_ts < ts);
}

KStatus TsStorageIteratorImpl::getBlockSpanMinMaxValue(std::shared_ptr<TsBlockSpan>& block_span, uint32_t col_id,
                                                         uint32_t type, TsScanStats* ts_scan_stats,
                                                         void*& min, void*& max) {
  std::unique_ptr<TsBitmapBase> bitmap;
  char* value = nullptr;
  auto s = block_span->GetFixLenColAddr(col_id, &value, &bitmap, ts_scan_stats);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetFixLenColAddr failed.");
    return s;
  }
  uint32_t row_num = block_span->GetRowNum();
  int32_t size = block_span->GetColSize(col_id);
  void* cur_max = nullptr;
  void* cur_min = nullptr;
  void* current;
  bool need_check_bt = !bitmap->IsAllValid();
  for (int row_idx = 0; row_idx < row_num; ++row_idx) {
    if (need_check_bt && bitmap->At(row_idx) != DataFlags::kValid) {
      continue;
    }
    current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
    if (cur_max != nullptr) {
      auto ret = cmpWithSpan(cur_min, cur_max, current, type, size);
      if (ret < 0) {
        cur_min = current;
      } else if (ret > 0) {
        cur_max = current;
      }
    } else {
      cur_max = current;
      cur_min = current;
    }
  }
  if (cur_max != nullptr) {
    min = malloc(size);
    memcpy(min, cur_min, size);
    max = malloc(size);
    memcpy(max, cur_max, size);
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorImpl::getBlockSpanVarMinMaxValue(std::shared_ptr<TsBlockSpan>& block_span,
                                                            uint32_t col_id, uint32_t type,
                                                            TsScanStats* ts_scan_stats,
                                                            TSSlice& min, TSSlice& max) {
  KStatus ret;
  std::vector<string> var_rows;
  uint32_t row_num = block_span->GetRowNum();
  for (int row_idx = 0; row_idx < row_num; ++row_idx) {
    TSSlice slice;
    DataFlags flag;
    ret = block_span->GetVarLenTypeColAddr(row_idx, col_id, flag, slice, ts_scan_stats);
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

KStatus TsStorageIteratorImpl::isBlockFiltered(std::shared_ptr<TsBlockSpan>& block_span,
                                                  TsScanStats* ts_scan_stats, bool& is_filtered) {
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
            ret = block_span->GetPreCount(col_id, ts_scan_stats, pre_count);
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
            ret = block_span->GetPreCount(col_id, ts_scan_stats, pre_count);
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
            ret = block_span->GetPreMin(col_id, ts_scan_stats, min_addr);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
            ret = block_span->GetPreMax(col_id, ts_scan_stats, max_addr);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
          } else {
            ret = getBlockSpanMinMaxValue(block_span, col_id, attrs_[col_id].type,
                                          ts_scan_stats, min_addr, max_addr);
            if (ret != KStatus::SUCCESS) {
              return KStatus::FAIL;
            }
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
            ret = block_span->GetVarPreMin(col_id, ts_scan_stats, var_pre_min);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("GetVarPreMin failed.");
              return KStatus::FAIL;
            }
            TSSlice var_pre_max{nullptr, 0};
            ret = block_span->GetVarPreMax(col_id, ts_scan_stats, var_pre_max);
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
            ret = getBlockSpanVarMinMaxValue(block_span, col_id, attrs_[col_id].type,
                                              ts_scan_stats, var_pre_min, var_pre_max);
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

KStatus TsStorageIteratorImpl::ScanEntityBlockSpans(timestamp64 ts, TsScanStats* ts_scan_stats) {
  assert(ts_block_spans_.empty());
  UpdateTsSpans(ts);
  if (cur_partition_index_ < ts_partitions_.size()) {
    filter_->entity_id_ = entity_ids_[cur_entity_index_];
    auto partition_version = ts_partitions_[cur_partition_index_].get();
    if (ts != INVALID_TS && IsFilteredOut(partition_version->GetTsColTypeStartTime(ts_col_type_),
                                          partition_version->GetTsColTypeEndTime(ts_col_type_), ts))  {
      return KStatus::SUCCESS;
    }
    auto s = partition_version->GetBlockSpans(*filter_, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition_version GetBlockSpan failed.");
      return s;
    }
    if (!block_filter_.empty() && !ts_block_spans_.empty()) {
      if (1 == ts_block_spans_.size()) {
        bool is_filtered = false;
        s = isBlockFiltered(ts_block_spans_.front(), ts_scan_stats, is_filtered);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("isBlockFiltered failed, entityid is %lu", ts_block_spans_.front()->GetEntityID());
          return KStatus::FAIL;
        }
        if (is_filtered) {
          ts_block_spans_.clear();
        }
        return KStatus::SUCCESS;
      }
      const size_t kMinSamplingCount = 5;
      size_t sampling_count = ceil(EngineOptions::block_filter_sampling_ratio * ts_block_spans_.size());
      if (sampling_count >= kMinSamplingCount) {
        size_t count = 0, filtered_count = 0;
        auto it = ts_block_spans_.begin();
        while (count < sampling_count && it != ts_block_spans_.end()) {
          bool is_filtered = false;
          s = isBlockFiltered(*it, ts_scan_stats, is_filtered);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("isBlockFiltered failed, entityid is %lu", (*it)->GetEntityID());
            return KStatus::FAIL;
          }
          if (is_filtered) {
            ++filtered_count;
          }
          ++it;
          ++count;
        }
        double filter_ratio = static_cast<double>(filtered_count) / count;
        if (filter_ratio < 0.5) {
          return KStatus::SUCCESS;
        }
      }

      vector<pair<timestamp64, timestamp64>> intervals;
      std::map<pair<timestamp64, timestamp64>, std::vector<std::shared_ptr<TsBlockSpan>>> interval_block_span_map;
      while (!ts_block_spans_.empty()) {
        auto block_span = std::move(ts_block_spans_.front());
        ts_block_spans_.pop_front();
        timestamp64 begin_ts = block_span->GetFirstTS(), end_ts = block_span->GetLastTS();
        if (!block_span->GetRowNum() || checkTimestampWithSpans(ts_spans_, begin_ts, end_ts) ==
                                        TimestampCheckResult::NonOverlapping) {
          continue;
        }
        if (!interval_block_span_map.count({begin_ts, end_ts})) {
          intervals.push_back({begin_ts, end_ts});
        }
        interval_block_span_map[{begin_ts, end_ts}].emplace_back(block_span);
      }

      sort(intervals.begin(), intervals.end());
      std::vector<pair<pair<timestamp64, timestamp64>, std::vector<std::shared_ptr<TsBlockSpan>>>> sorted_block_spans;
      for (auto interval : intervals) {
        if (sorted_block_spans.empty() || interval.first > sorted_block_spans.back().first.second) {
          sorted_block_spans.push_back({{interval.first, interval.second},
                                       interval_block_span_map[interval]});
        } else {
          if (interval.second > sorted_block_spans.back().first.second) {
            sorted_block_spans.back().first.second = interval.second;
          }
          sorted_block_spans.back().second.insert(sorted_block_spans.back().second.end(),
                                                  interval_block_span_map[interval].begin(),
                                                  interval_block_span_map[interval].end());
        }
      }
      intervals.clear();
      interval_block_span_map.clear();

      for (auto& cur_block_spans : sorted_block_spans) {
        if (cur_block_spans.second.size() > 1) {
          ts_block_spans_.insert(ts_block_spans_.end(), cur_block_spans.second.begin(), cur_block_spans.second.end());
        } else {
          bool is_filtered = false;
          auto cur_block_span = cur_block_spans.second.front();
          s = isBlockFiltered(cur_block_span, ts_scan_stats, is_filtered);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("isBlockFiltered failed, entityid is %lu", cur_block_span->GetEntityID());
            return KStatus::FAIL;
          }
          if (is_filtered) {
            continue;
          }
          ts_block_spans_.emplace_back(cur_block_span);
        }
      }
    }
  }

  return KStatus::SUCCESS;
}

TsSortedRawDataIteratorImpl::TsSortedRawDataIteratorImpl(const std::shared_ptr<TsVGroup>& vgroup,
                                                              uint32_t version,
                                                              vector<uint32_t>& entity_ids,
                                                              std::vector<KwTsSpan>& ts_spans,
                                                              std::vector<BlockFilter>& block_filter,
                                                              std::vector<k_uint32>& kw_scan_cols,
                                                              std::vector<k_uint32>& ts_scan_cols,
                                                              const std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                                              const std::shared_ptr<MMapMetricsTable>& schema,
                                                              TS_OSN scan_osn, SortOrder order_type) :
                          TsStorageIteratorImpl::TsStorageIteratorImpl(vgroup, version, entity_ids, ts_spans,
                                                                           block_filter, kw_scan_cols,
                                                                           ts_scan_cols, table_schema_mgr,
                                                                           schema, scan_osn) {
}

TsSortedRawDataIteratorImpl::~TsSortedRawDataIteratorImpl() {
}

NextBlockStatus TsSortedRawDataIteratorImpl::NextBlockSpan(timestamp64 ts, TsScanStats* ts_scan_stats) {
  KStatus s;
  while (true) {
    if (cur_entity_index_ >= entity_ids_.size()) {
      return NextBlockStatus::SCAN_OVER;
    }
    if (block_span_sorted_iterator_ != nullptr) {
      bool is_done = false;
      s = block_span_sorted_iterator_->Next(cur_block_span_, &is_done);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NextBlockSpan failed. vgroup_id: %u, entity_id: %u",
                  vgroup_->GetVGroupID(), entity_ids_[cur_partition_index_]);
        return NextBlockStatus::SCAN_ERROR;
      }
      if (!is_done && (ts != INVALID_TS && IsFilteredOut(cur_block_span_->GetFirstTS(),
                                                         cur_block_span_->GetLastTS(), ts))) {
        is_done = true;
        cur_block_span_ = nullptr;
        cur_partition_index_ = ts_partitions_.size();
      }
      if (!is_done) {
        return NextBlockStatus::FIND_ONE;
      }
    }
    bool is_entity_changed = false;
    if (++cur_partition_index_ >= ts_partitions_.size()) {
      SwitchEntity();
      cur_partition_index_ = 0;
      is_entity_changed = true;
    }
    s = ScanAndSortEntityData(ts, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("ScanAndSortEntityData failed. vgroup_id: %u, entity_id: %u",
                vgroup_->GetVGroupID(), entity_ids_[cur_partition_index_]);
      return NextBlockStatus::SCAN_ERROR;
    }
    if (!is_entity_changed) {
      continue;
    }
    return NextBlockStatus::SCAN_OVER;
  }
}

KStatus TsSortedRawDataIteratorImpl::ScanAndSortEntityData(timestamp64 ts, TsScanStats* ts_scan_stats) {
  if (cur_entity_index_ < entity_ids_.size()) {
    // scan row data for current entity
    KStatus ret = ScanEntityBlockSpans(ts, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to scan block spans for entity(%d).", entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }
    if (ts_block_spans_.empty()) {
      block_span_sorted_iterator_ = nullptr;
    } else {
      // sort the block span data
      block_span_sorted_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(ts_block_spans_, vgroup_->GetSchemaMgr(),
                                                                                EngineOptions::g_dedup_rule, is_reversed_);
      ret = block_span_sorted_iterator_->Init();
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to init block span sorted iterator for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

bool TsSortedRawDataIteratorImpl::IsDisordered() {
  return false;
}

KStatus TsSortedRawDataIteratorImpl::Init(bool is_reversed) {
  KStatus ret = TsStorageIteratorImpl::Init(is_reversed);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  cur_entity_index_ = 0;
  if (is_reversed) {
    reverse(ts_partitions_.begin(), ts_partitions_.end());
  }
  return KStatus::SUCCESS;
}

KStatus TsSortedRawDataIteratorImpl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts,
                                            TsScanStats* ts_scan_stats) {
  *count = 0;
  KStatus ret;
  if (cur_entity_index_ >= entity_ids_.size() || ts_partitions_.empty()) {
    // All entities are scanned.
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  if (nullptr == cur_block_span_) {
    NextBlockStatus status = NextBlockSpan(ts, ts_scan_stats);
    if (NextBlockStatus::SCAN_ERROR == status) {
      LOG_ERROR("Next failed. vgroup_id: %u, entity_id: %u",
                vgroup_->GetVGroupID(), entity_ids_[cur_partition_index_]);
      return KStatus::FAIL;
    } else if (NextBlockStatus::SCAN_OVER == status) {
      if (cur_entity_index_ >= entity_ids_.size()) {
        *is_finished = true;
      }
      return KStatus::SUCCESS;
    }
  }
  assert(cur_block_span_ != nullptr);
  ret = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, cur_block_span_, res, count, ts_scan_stats);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  assert(*count > 0);
  cur_block_span_ = nullptr;
  // Return the result set.
  return KStatus::SUCCESS;
}

TsFillRawDataIteratorImpl::TsFillRawDataIteratorImpl(const std::shared_ptr<TsVGroup>& vgroup, uint32_t version,
                                                     vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans,
                                                     std::vector<BlockFilter>& block_filter,
                                                     std::vector<k_uint32>& kw_scan_cols,
                                                     std::vector<k_uint32>& ts_scan_cols,
                                                     const std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                                     const std::shared_ptr<MMapMetricsTable>& schema,
                                                     const FillParams fill_params) :
              TsSortedRawDataIteratorImpl(vgroup, version, entity_ids, ts_spans,
                                          block_filter, kw_scan_cols,
                                          ts_scan_cols, table_schema_mgr,
                                          schema, UINT64_MAX),
              fill_type_(fill_params.fill_type), varbytes_cols_(fill_params.varbytes_col_ids),
              before_range_(fill_params.before_range), after_range_(fill_params.after_range),
              const_fill_type_(fill_params.const_data_type), const_fill_value_(fill_params.const_data_value),
              const_data_length_(fill_params.const_data_length), const_n_data_length_(fill_params.const_n_data_length),
              int_value_flag_(fill_params.int_value_flag), const_int_value_(fill_params.const_int_value) {
  assert(ts_spans_.size() == 1);
  assert(ts_spans_.front().begin == ts_spans_.front().end);
  fill_ts_point_ = ts_spans_.front().begin;
  if (before_range_ != 0) {
    before_range_ = convertMSToPrecisionTS(before_range_, ts_col_type_);
  }
  if (after_range_ != 0) {
    after_range_ = convertMSToPrecisionTS(after_range_, ts_col_type_);
  }
}

KStatus TsFillRawDataIteratorImpl::Init(bool is_reversed) {
  if (FillType::PREVIOUS == fill_type_ || FillType::CLOSER == fill_type_ || FillType::LINEAR == fill_type_) {
    if (before_range_ != 0) {
      before_ts_span_ = {{fill_ts_point_ - before_range_, fill_ts_point_}};
    } else {
      before_ts_span_ = {{INT64_MIN, fill_ts_point_}};
    }
  }

  if (FillType::NEXT == fill_type_ || FillType::CLOSER == fill_type_ || FillType::LINEAR == fill_type_) {
    if (after_range_ != 0) {
      after_ts_span_ = {{fill_ts_point_, fill_ts_point_ + after_range_}};
    } else {
      after_ts_span_ = {{fill_ts_point_, INT64_MAX}};
    }
  }
  db_id_ = scan_schema_->metaData()->db_id;
  table_id_ = table_schema_mgr_->GetTableId();
  attrs_ = *scan_schema_->getSchemaInfoExcludeDroppedPtr();
  vgroup_current_version_ = vgroup_->CurrentVersion();
  ts_partitions_ = vgroup_current_version_->GetPartitions(db_id_, ts_spans_, ts_col_type_);
  filter_ = std::make_shared<TsScanFilterParams>(db_id_, table_id_, vgroup_->GetVGroupID(),
                                                 0, ts_col_type_, scan_osn_, ts_spans_);
  switchEntity();
  return KStatus::SUCCESS;
}

bool TsFillRawDataIteratorImpl::IsDisordered() {
  return false;
}

void TsFillRawDataIteratorImpl::switchEntity() {
  if (++cur_entity_index_ < entity_ids_.size()) {
    cur_entity_id_ = entity_ids_[cur_entity_index_];
    if (TsMemSegment::IsApproachingLimit()) {
      for (auto &partition : ts_partitions_) {
        partition = vgroup_->GetCurrentPartitionVersion(partition->GetPartitionIdentifier());
      }
    }
    fill_kw_scan_cols_.clear();
    fill_ts_scan_cols_.clear();
    fill_cols_map_.clear();
  }
}

bool TsFillRawDataIteratorImpl::isConstFillTypeIllegal(uint32_t col_idx) {
    DATATYPE col_type = (DATATYPE) attrs_[col_idx].type;
    switch (const_fill_type_) {
    case DATATYPE::NO_TYPE: {
      return true;
    }
    case DATATYPE::BOOL:
    case DATATYPE::BYTE: {
      if (col_type == DATATYPE::INT8 || col_type == DATATYPE::INT16 || col_type == DATATYPE::INT32 ||
          col_type == DATATYPE::INT64 || col_type == DATATYPE::FLOAT || col_type == DATATYPE::DOUBLE ||
          col_type == DATATYPE::TIMESTAMP || col_type == DATATYPE::TIMESTAMP64 ||
          col_type == DATATYPE::TIMESTAMP64_MICRO || col_type == DATATYPE::TIMESTAMP64_NANO) {
        return true;
      }
      break;
    }
    case DATATYPE::INT16: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE || col_type == DATATYPE::INT8) {
        return true;
      }
      break;
    }
    case DATATYPE::INT32: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE ||
          col_type == DATATYPE::INT8 || col_type == DATATYPE::INT16) {
        return true;
      }
      break;
    }
    case DATATYPE::INT64: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE ||
          col_type == DATATYPE::INT8 || col_type == DATATYPE::INT16 || col_type == DATATYPE::INT32 ||
          col_type == DATATYPE::TIMESTAMP) {
        return true;
      }
      break;
    }
    case DATATYPE::FLOAT: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE || col_type == DATATYPE::INT8 ||
          col_type == DATATYPE::INT16 || col_type == DATATYPE::INT32 || col_type == DATATYPE::INT64 ||
          col_type == DATATYPE::TIMESTAMP || col_type == DATATYPE::TIMESTAMP64 ||
          col_type == DATATYPE::TIMESTAMP64_MICRO || col_type == DATATYPE::TIMESTAMP64_NANO) {
        return true;
      }
      break;
    }
    case DATATYPE::DOUBLE: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE || col_type == DATATYPE::INT8 ||
          col_type == DATATYPE::INT16 || col_type == DATATYPE::INT32 || col_type == DATATYPE::INT64 ||
          col_type == DATATYPE::TIMESTAMP || col_type == DATATYPE::TIMESTAMP64 ||
          col_type == DATATYPE::TIMESTAMP64_MICRO || col_type == DATATYPE::TIMESTAMP64_NANO ||
          col_type == DATATYPE::FLOAT) {
        return true;
      }
      break;
    }
    case DATATYPE::CHAR:
    case DATATYPE::BINARY:
    case DATATYPE::VARSTRING:
    case DATATYPE::VARBINARY: {
      if (col_type == DATATYPE::BOOL || col_type == DATATYPE::BYTE ||
          col_type == DATATYPE::INT8 || col_type == DATATYPE::INT16 || col_type == DATATYPE::INT32 ||
          col_type == DATATYPE::INT64 || col_type == DATATYPE::FLOAT || col_type == DATATYPE::DOUBLE) {
        return true;
      }
      if (col_type == DATATYPE::TIMESTAMP || col_type == DATATYPE::TIMESTAMP64 ||
          col_type == DATATYPE::TIMESTAMP64_MICRO || col_type == DATATYPE::TIMESTAMP64_NANO) {
        return !int_value_flag_;
      }
      break;
    }
    default:
      break;
  }
  if (col_type == DATATYPE::CHAR || col_type == DATATYPE::BINARY ||
      col_type == DATATYPE::VARSTRING || col_type == DATATYPE::VARBINARY) {
    uint32_t col_size = attrs_[col_idx].length;
    if (varbytes_cols_.count(col_idx)) {
      return const_data_length_ > col_size;
    }
    col_size -= 1;
    if (col_type == DATATYPE::CHAR || col_type == DATATYPE::VARSTRING) {
      return const_data_length_ > col_size;
    } else {
      return const_n_data_length_ > (col_size / 4);
    }
  }
  return false;
}

KStatus TsFillRawDataIteratorImpl::copyBatch(uint32_t col_idx, const Batch* old_batch, Batch** new_batch) {
  if (old_batch->count > 0) {
    bool is_var_type = isVarLenType(attrs_[col_idx].type);
    int32_t alloc_size = is_var_type ? KUint16(old_batch->mem) + kStringLenLen : attrs_[col_idx].size;
    char* value = static_cast<char*>(malloc(alloc_size));
    if (value == nullptr) {
      LOG_ERROR("malloc failed, cannot allocate memory, size: %u", alloc_size);
      return KStatus::FAIL;
    }
    memcpy(value, old_batch->mem, alloc_size);
    if (!is_var_type) {
      *new_batch = new AggBatch(value, 1);
      (*new_batch)->is_new = true;
    } else {
      std::shared_ptr<char> ptr(value, free);
      *new_batch = new AggVarBatch(ptr, 1);
    }
  } else {
    *new_batch = new AggBatch(nullptr, 0);
  }
  delete old_batch;
  old_batch = nullptr;
  return KStatus::SUCCESS;
}

KStatus TsFillRawDataIteratorImpl::fillColumnResultSet(ResultSet* res, uint32_t idx, const std::string& data_value) {
  char* value = nullptr;
  k_uint32 col_idx = fill_kw_scan_cols_[idx];
  if (!isConstFillTypeIllegal(col_idx)) {
    switch (attrs_[col_idx].type) {
      case DATATYPE::BYTE:
      case DATATYPE::BOOL: {
        value = static_cast<char*>(malloc(sizeof(k_bool)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_bool));
          return KStatus::FAIL;
        }
        KBool(value) = (data_value == "1") || (data_value == "true") ? 1 : 0;
        break;
      }
      case DATATYPE::INT8: {
        value = static_cast<char*>(malloc(sizeof(k_int8)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int8));
          return KStatus::FAIL;
        }
        try {
          KInt8(value) = std::stoi(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int8 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::INT16: {
        value = static_cast<char*>(malloc(sizeof(k_int16)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int16));
          return KStatus::FAIL;
        }
        try {
          KInt16(value) = std::stoi(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int16 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::INT32: {
        value = static_cast<char*>(malloc(sizeof(k_int32)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int32));
          return KStatus::FAIL;
        }
        try {
          KInt32(value) = std::stoi(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int32 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::TIMESTAMP: {
        value = static_cast<char*>(malloc(sizeof(k_int32)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int32));
          return KStatus::FAIL;
        }
        try {
          KInt32(value) = int_value_flag_
                          ? convertNanoToPrecisionTS(const_int_value_, ts_col_type_)
                          : std::stoi(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int32 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::INT64: {
        value = static_cast<char*>(malloc(sizeof(k_int64)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int64));
          return KStatus::FAIL;
        }
        try {
          KInt64(value) = std::stoll(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int64 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::TIMESTAMP64:
      case DATATYPE::TIMESTAMP64_MICRO:
      case DATATYPE::TIMESTAMP64_NANO: {
        value = static_cast<char*>(malloc(sizeof(k_int64)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int64));
          return KStatus::FAIL;
        }
        try {
          KInt64(value) = int_value_flag_
                          ? convertNanoToPrecisionTS(const_int_value_, ts_col_type_)
                          : std::stoll(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to int64 type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::FLOAT: {
        value = static_cast<char*>(malloc(sizeof(k_float32)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_float32));
          return KStatus::FAIL;
        }
        try {
          KFloat32(value) = std::stof(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to float type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::DOUBLE: {
        value = static_cast<char*>(malloc(sizeof(k_double64)));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_double64));
          return KStatus::FAIL;
        }
        try {
          KDouble64(value) = std::stod(data_value);
        } catch (const std::out_of_range& e) {
          free(value);
          value = nullptr;
          LOG_DEBUG("cast to double type out of range - %s", e.what());
        }
        break;
      }
      case DATATYPE::BINARY:
      case DATATYPE::CHAR:
      case DATATYPE::STRING: {
        size_t alloc_size = attrs_[col_idx].size;
        value = static_cast<char*>(malloc(alloc_size));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", alloc_size);
          return KStatus::FAIL;
        }
        memset(value, 0, alloc_size);
        memcpy(value, data_value.c_str(), data_value.length());
        break;
      }
      case DATATYPE::VARBINARY: {
        size_t data_size = data_value.length();
        size_t alloc_size = data_size + kStringLenLen;
        value = static_cast<char*>(malloc(alloc_size));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", alloc_size);
          return KStatus::FAIL;
        }
        KUint16(value) = data_size;
        memcpy(value + kStringLenLen, data_value.c_str(), data_size);
        break;
      }
      case DATATYPE::VARSTRING: {
        size_t data_size = data_value.length();
        size_t alloc_size = data_size + kStringLenLen + 1;
        value = static_cast<char*>(malloc(alloc_size));
        if (value == nullptr) {
          LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", alloc_size);
          return KStatus::FAIL;
        }
        KUint16(value) = data_size + 1;
        memcpy(value + kStringLenLen, data_value.c_str(), data_size);
        value[alloc_size - 1] = '\0';
        break;
      }
      default:
        break;
    }
  }
  Batch* batch = nullptr;
  if (nullptr == value) {
    batch = new AggBatch(nullptr, 0);
  } else {
    if (!isVarLenType(attrs_[col_idx].type)) {
      batch = new AggBatch(value, 1);
      batch->is_new = true;
    } else {
      std::shared_ptr<char> ptr(value, free);
      batch = new AggVarBatch(ptr, 1);
    }
  }
  res->push_back(fill_cols_map_[idx], batch);
  return KStatus::SUCCESS;
}

KStatus TsFillRawDataIteratorImpl::getExactFillTsPointResultSet(ResultSet* res, k_uint32* count,
                                                                TsScanStats* ts_scan_stats) {
  KStatus s;
  auto partition = vgroup_current_version_->GetPartitions(db_id_, ts_spans_, ts_col_type_);
  if (!partition.empty()) {
    assert(partition.size() == 1);
    auto filter = std::make_shared<TsScanFilterParams>(db_id_, table_id_, vgroup_->GetVGroupID(),
                                                       cur_entity_id_, ts_col_type_, scan_osn_, ts_spans_);
    s = partition.front()->GetBlockSpans(*filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Partition GetBlockSpans failed");
      return KStatus::FAIL;
    }
    if (!ts_block_spans_.empty()) {
      // sort the block span data
      block_span_sorted_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(ts_block_spans_,
                                                                                vgroup_->GetSchemaMgr(),
                                                                                EngineOptions::g_dedup_rule,
                                                                                is_reversed_);
      s = block_span_sorted_iterator_->Init();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Failed to init block span sorted iterator for entity(%d).", cur_entity_id_);
        return KStatus::FAIL;
      }
      bool is_done = false;
      // TODO(liumengzhen): duplicate data
      s = block_span_sorted_iterator_->Next(cur_block_span_, &is_done);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NextBlockSpan failed. vgroup_id: %u, entity_id: %u",
                  vgroup_->GetVGroupID(), cur_entity_id_);
        return KStatus::FAIL;
      }
      s = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, cur_block_span_, res, count, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("ConvertBlockSpanToResultSet failed.");
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsFillRawDataIteratorImpl::calculateLinearResultSet(ResultSet* res, uint32_t idx,
                                                            timestamp64 t1, timestamp64 t2,
                                                            void* before_value, void* after_value) {
  assert(t1 != t2);
  char* value = nullptr;
  k_uint32 col_idx = fill_kw_scan_cols_[idx];
  double ratio = static_cast<double>(fill_ts_point_ - t1) / (t2 - t1);
  switch (attrs_[col_idx].type) {
    case DATATYPE::INT8: {
      k_int8 v1 = KInt8(before_value), v2 = KInt8(after_value);
      k_int8 v = static_cast<k_int8>(v1 + (v2 - v1) * ratio);
      value = static_cast<char*>(malloc(sizeof(k_int8)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int8));
        return KStatus::FAIL;
      }
      KInt8(value) = v;
      break;
    }
    case DATATYPE::INT16: {
      k_int16 v1 = KInt16(before_value), v2 = KInt16(after_value);
      k_int16 v = static_cast<k_int16>(v1 + (v2 - v1) * ratio);
      value = static_cast<char*>(malloc(sizeof(k_int16)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int16));
        return KStatus::FAIL;
      }
      KInt16(value) = v;
      break;
    }
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP: {
      k_int32 v1 = KInt32(before_value), v2 = KInt32(after_value);
      k_int32 v = static_cast<k_int32>(v1 + (v2 - v1) * ratio);
      value = static_cast<char*>(malloc(sizeof(k_int32)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int32));
        return KStatus::FAIL;
      }
      KInt32(value) = v;
      break;
    }
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO: {
      k_int64 v1 = KInt64(before_value), v2 = KInt64(after_value);
      k_int64 v = static_cast<k_int64>(v1 + (v2 - v1) * ratio);
      value = static_cast<char*>(malloc(sizeof(k_int64)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int64));
        return KStatus::FAIL;
      }
      KInt64(value) = v;
      break;
    }
    case DATATYPE::FLOAT: {
      k_float32 v1 = KFloat32(before_value), v2 = KFloat32(after_value);
      k_float32 v = v1 + (v2 - v1) * ratio;
      value = static_cast<char*>(malloc(sizeof(k_float32)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_float32));
        return KStatus::FAIL;
      }
      KFloat32(value) = round_to_n_decimals(v, 2);
      break;
    }
    case DATATYPE::DOUBLE: {
      k_double64 v1 = KDouble64(before_value), v2 = KDouble64(after_value);
      k_double64 v = v1 + (v2 - v1) * ratio;
      value = static_cast<char*>(malloc(sizeof(k_double64)));
      if (value == nullptr) {
        LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_double64));
        return KStatus::FAIL;
      }
      KDouble64(value) = round_to_n_decimals(v, 2);
      break;
    }
    default:
      break;
  }
  Batch* batch = new AggBatch(value, 1);
  batch->is_new = true;
  res->push_back(fill_cols_map_[idx], batch);
  return KStatus::SUCCESS;
}

KStatus TsFillRawDataIteratorImpl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts,
                                        TsScanStats* ts_scan_stats) {
  *count = 0;
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  KStatus s = getExactFillTsPointResultSet(res, count, ts_scan_stats);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getExactFillTsPointResultSet failed.");
    return KStatus::FAIL;
  }
  if (*count > 0) {
    uint32_t null_cols = 0;
    for (int i = 1; fill_type_ != FillType::EXACT && i < res->data.size(); ++i) {
      const Batch* b = res->data[i][0];
      assert(b != nullptr);
      bool is_null = false;
      b->isNull(0, &is_null);
      if (is_null) {
        fill_kw_scan_cols_.emplace_back(kw_scan_cols_[i]);
        fill_ts_scan_cols_.emplace_back(ts_scan_cols_[i]);
        fill_cols_map_[null_cols] = i;
        ++null_cols;
        delete b;
        b = nullptr;
        res->data[i].clear();
      }
    }
    if (0 == null_cols) {
      switchEntity();
      return KStatus::SUCCESS;
    }
  } else {
    fill_kw_scan_cols_.assign(kw_scan_cols_.begin() + 1, kw_scan_cols_.end());
    fill_ts_scan_cols_.assign(ts_scan_cols_.begin() + 1, ts_scan_cols_.end());
    for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
      fill_cols_map_[i] = i + 1;
    }
  }
  *count = 1;
  switch (fill_type_) {
    case FillType::EXACT: {
      for (int i = 1; i < kw_scan_cols_.size(); ++i) {
        Batch* batch = new AggBatch(nullptr, 0);
        res->push_back(i, batch);
      }
      break;
    }
    case FillType::PREVIOUS: {
      std::vector<uint32_t> entity_ids = {cur_entity_id_};
      std::vector<k_int32> agg_extend_cols;
      uint32_t col_num = fill_kw_scan_cols_.size();
      std::vector<Sumfunctype> scan_agg_type(col_num, Sumfunctype::LAST);
      auto agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, before_ts_span_, block_filter_,
                                            fill_kw_scan_cols_, fill_ts_scan_cols_, agg_extend_cols, scan_agg_type,
                                            {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      ResultSet temp_res{col_num};
      s = agg_iter->Next(&temp_res, count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }
      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        Batch* new_batch = nullptr;
        const Batch* batch = temp_res.data[i][0];
        s = copyBatch(fill_kw_scan_cols_[i], batch, &new_batch);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("copyBatch failed");
          return KStatus::FAIL;
        }
        temp_res.data[i].clear();
        res->push_back(fill_cols_map_[i], new_batch);
      }
      delete agg_iter;
      agg_iter = nullptr;
      break;
    }
    case FillType::NEXT: {
      std::vector<uint32_t> entity_ids = {cur_entity_id_};
      std::vector<k_int32> agg_extend_cols;
      uint32_t col_num = fill_kw_scan_cols_.size();
      std::vector<Sumfunctype> scan_agg_type(col_num, Sumfunctype::FIRST);
      auto agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, after_ts_span_, block_filter_,
                                            fill_kw_scan_cols_, fill_ts_scan_cols_, agg_extend_cols, scan_agg_type,
                                            {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      ResultSet temp_res{col_num};
      s = agg_iter->Next(&temp_res, count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }
      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        Batch* new_batch = nullptr;
        const Batch* batch = temp_res.data[i][0];
        s = copyBatch(fill_kw_scan_cols_[i], batch, &new_batch);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("copyBatch failed");
          return KStatus::FAIL;
        }
        temp_res.data[i].clear();
        res->push_back(fill_cols_map_[i], new_batch);
      }
      delete agg_iter;
      agg_iter = nullptr;
      break;
    }
    case FillType::CLOSER: {
      std::vector<uint32_t> entity_ids = {cur_entity_id_};
      std::vector<k_int32> agg_extend_cols;
      uint32_t col_num = 2 * fill_kw_scan_cols_.size();
      std::vector<k_uint32> kw_scan_cols(col_num), ts_scan_cols(col_num);
      std::vector<Sumfunctype> previous_agg_type(col_num), after_agg_type(col_num);
      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        previous_agg_type[2 * i] = Sumfunctype::LASTTS;
        previous_agg_type[2 * i + 1] = Sumfunctype::LAST;
        after_agg_type[2 * i] = Sumfunctype::FIRSTTS;
        after_agg_type[2 * i + 1] = Sumfunctype::FIRST;
        kw_scan_cols[2 * i] = kw_scan_cols[2 * i + 1] = fill_kw_scan_cols_[i];
        ts_scan_cols[2 * i] = ts_scan_cols[2 * i + 1] = fill_ts_scan_cols_[i];
      }

      auto previous_agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, before_ts_span_, block_filter_,
                                            kw_scan_cols, ts_scan_cols, agg_extend_cols, previous_agg_type,
                                            {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = previous_agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      k_uint32 previous_count = 0;
      ResultSet previous_res{col_num};
      s = previous_agg_iter->Next(&previous_res, &previous_count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }

      auto after_agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, after_ts_span_, block_filter_,
                                         kw_scan_cols, ts_scan_cols, agg_extend_cols, after_agg_type,
                                          {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = after_agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      k_uint32 after_count = 0;
      ResultSet after_res{col_num};
      s = after_agg_iter->Next(&after_res, &after_count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }

      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        k_uint32 kw_col_idx = fill_kw_scan_cols_[i];
        bool is_before_null = false, is_after_null = false;
        previous_res.data[2 * i][0]->isNull(0, &is_before_null);
        after_res.data[2 * i][0]->isNull(0, &is_after_null);
        Batch* batch = nullptr;
        if (is_before_null && is_after_null) {
          batch = new AggBatch(nullptr, 0);
        } else {
          void* mem = nullptr;
          char* value = nullptr;
          if (is_before_null) {
            mem = after_res.data[2 * i + 1][0]->mem;
          } else if (is_after_null) {
            mem = previous_res.data[2 * i + 1][0]->mem;
          } else {
            timestamp64 before_fill_ts = KTimestamp(previous_res.data[2 * i][0]->mem);
            timestamp64 after_fill_ts = KTimestamp(after_res.data[2 * i][0]->mem);
            timestamp64 before_interval = fill_ts_point_ - before_fill_ts;
            timestamp64 after_interval = after_fill_ts - fill_ts_point_;
            mem = before_interval <= after_interval
                    ? previous_res.data[2 * i + 1][0]->mem
                    : after_res.data[2 * i + 1][0]->mem;
          }
          bool is_var_type = isVarLenType(attrs_[kw_col_idx].type);
          size_t alloc_size = is_var_type ? (KUint16(mem) + kStringLenLen) : attrs_[kw_col_idx].size;
          value = static_cast<char*>(malloc(alloc_size));
          if (value == nullptr) {
            LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", alloc_size);
            return KStatus::FAIL;
          }
          memcpy(value, mem, alloc_size);
          if (!is_var_type) {
            batch = new AggBatch(value, 1);
            batch->is_new = true;
          } else {
            std::shared_ptr<char> ptr(value, free);
            batch = new AggVarBatch(ptr, 1);
          }
        }
        res->push_back(fill_cols_map_[i], batch);
      }
      delete previous_agg_iter;
      delete after_agg_iter;
      break;
    }
    case FillType::CONSTANT: {
      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        fillColumnResultSet(res, i, const_fill_value_);
      }
      break;
    }
    case FillType::LINEAR: {
      std::vector<uint32_t> entity_ids = {cur_entity_id_};
      std::vector<k_int32> agg_extend_cols;
      uint32_t col_num = 2 * fill_kw_scan_cols_.size();
      std::vector<k_uint32> kw_scan_cols(col_num), ts_scan_cols(col_num);
      std::vector<Sumfunctype> previous_agg_type(col_num), after_agg_type(col_num);
      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        previous_agg_type[2 * i] = Sumfunctype::LASTTS;
        previous_agg_type[2 * i + 1] = Sumfunctype::LAST;
        after_agg_type[2 * i] = Sumfunctype::FIRSTTS;
        after_agg_type[2 * i + 1] = Sumfunctype::FIRST;
        kw_scan_cols[2 * i] = kw_scan_cols[2 * i + 1] = fill_kw_scan_cols_[i];
        ts_scan_cols[2 * i] = ts_scan_cols[2 * i + 1] = fill_ts_scan_cols_[i];
      }

      auto previous_agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, before_ts_span_,
                                                     block_filter_, kw_scan_cols, ts_scan_cols, agg_extend_cols,
                                                     previous_agg_type, {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = previous_agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      k_uint32 previous_count = 0;
      ResultSet previous_res{col_num};
      s = previous_agg_iter->Next(&previous_res, &previous_count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }

      auto after_agg_iter = new TsAggIteratorImpl(vgroup_, table_version_, entity_ids, after_ts_span_, block_filter_,
                                                  kw_scan_cols, ts_scan_cols, agg_extend_cols, after_agg_type,
                                                  {}, table_schema_mgr_, scan_schema_, UINT64_MAX);
      s = after_agg_iter->Init(is_reversed_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Init failed.")
        return KStatus::FAIL;
      }
      k_uint32 after_count = 0;
      ResultSet after_res{col_num};
      s = after_agg_iter->Next(&after_res, &after_count, is_finished, ts, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AggIterator Next failed.")
        return KStatus::FAIL;
      }

      for (int i = 0; i < fill_kw_scan_cols_.size(); ++i) {
        k_uint32 kw_col_idx = fill_kw_scan_cols_[i];
        if (!isSumType((DATATYPE) attrs_[kw_col_idx].type)) {
          Batch* batch = new AggBatch(nullptr, 0);
          res->push_back(fill_cols_map_[i], batch);
          continue;
        }
        bool is_before_null = false, is_after_null = false;
        previous_res.data[2 * i][0]->isNull(0, &is_before_null);
        after_res.data[2 * i][0]->isNull(0, &is_after_null);
        if (is_before_null || is_after_null) {
          Batch* batch = new AggBatch(nullptr, 0);
          res->push_back(fill_cols_map_[i], batch);
        } else {
          timestamp64 t1 = KTimestamp(previous_res.data[2 * i][0]->mem);
          timestamp64 t2 = KTimestamp(after_res.data[2 * i][0]->mem);
          calculateLinearResultSet(res, i, t1, t2, previous_res.data[2 * i + 1][0]->mem,
                                   after_res.data[2 * i + 1][0]->mem);
        }
      }
      delete previous_agg_iter;
      delete after_agg_iter;
      break;
    }
    default:
      break;
  }
  if (res->data[0].empty()) {
    char* value = static_cast<char*>(malloc(sizeof(k_int64)));
    if (value == nullptr) {
      LOG_ERROR("malloc failed, cannot allocate memory, size: %lu", sizeof(k_int64));
      return KStatus::FAIL;
    }
    KInt64(value) = fill_ts_point_;
    Batch* ts_batch = new AggBatch(value, 1);
    ts_batch->is_new = true;
    res->push_back(0, ts_batch);
  }
  res->entity_index = {1, cur_entity_id_, vgroup_->GetVGroupID()};
  switchEntity();
  return KStatus::SUCCESS;
}

TsAggIteratorImpl::TsAggIteratorImpl(const std::shared_ptr<TsVGroup>& vgroup, uint32_t version,
                                         vector<uint32_t>& entity_ids,
                                         std::vector<KwTsSpan>& ts_spans, std::vector<BlockFilter>& block_filter,
                                         std::vector<k_uint32>& kw_scan_cols,
                                         std::vector<k_uint32>& ts_scan_cols, std::vector<k_int32>& agg_extend_cols,
                                         std::vector<Sumfunctype>& scan_agg_types,
                                         const std::vector<timestamp64>& ts_points,
                                         const std::shared_ptr<TsTableSchemaManager>& table_schema_mgr,
                                         const std::shared_ptr<MMapMetricsTable>& schema, TS_OSN scan_osn,
                                         const TimeBucketInfo time_bucket_info)
    : TsStorageIteratorImpl::TsStorageIteratorImpl(vgroup, version, entity_ids, ts_spans, block_filter,
                                                       kw_scan_cols, ts_scan_cols, table_schema_mgr, schema, scan_osn),
      scan_agg_types_(scan_agg_types),
      last_ts_points_(ts_points),
      agg_extend_cols_{agg_extend_cols},
      time_bucket_info_(time_bucket_info) {}

TsAggIteratorImpl::~TsAggIteratorImpl() {}

inline bool PartitionLessThan(std::shared_ptr<const TsPartitionVersion>& a, std::shared_ptr<const TsPartitionVersion>& b) {
  return a->GetStartTime() < b->GetEndTime();
}

KStatus TsAggIteratorImpl::Init(bool is_reversed) {
  KStatus s = TsStorageIteratorImpl::Init(is_reversed);
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
      case Sumfunctype::TIME_BUCKET:
        // Don't need to do anything
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

  only_count_ts_ = (scan_agg_types_.size() == 1
        && scan_agg_types_[0] == Sumfunctype::COUNT && kw_scan_cols_.size() == 1 && kw_scan_cols_[0] == 0);

  for (const auto& type : scan_agg_types_) {
    if (type != COUNT && type != MAX && type != MIN && type != SUM) {
      only_partition_agg_type_ = false;
      break;
    }
  }

  for (int i = 0; i < scan_agg_types_.size(); ++i) {
    if (scan_agg_types_[i] != LAST && scan_agg_types_[i] != LASTTS &&
        scan_agg_types_[i] != LAST_ROW && scan_agg_types_[i] != LASTROWTS) {
      only_last_ = false;
      only_last_row_ = false;
      break;
    }
    if (scan_agg_types_[i] == LAST_ROW || scan_agg_types_[i] == LAST) {
      kw_last_scan_cols_.emplace_back(kw_scan_cols_[i]);
    }
    if (scan_agg_types_[i] == LASTROWTS || scan_agg_types_[i] == LASTTS) {
      kw_last_scan_cols_.emplace_back(0);
    }
    if ((scan_agg_types_[i] == LAST_ROW) ||
        (scan_agg_types_[i] == LAST && attrs_[kw_scan_cols_[i]].isFlag(AINFO_NOT_NULL)) ||
        (scan_agg_types_[i] == LASTROWTS) ||
        (scan_agg_types_[i] == LASTTS && attrs_[kw_scan_cols_[i]].isFlag(AINFO_NOT_NULL))) {
      continue;
    }
    only_last_row_ = false;
    if (!only_last_ && !only_last_row_) {
      break;
    }
  }

  for (auto idx : count_col_idxs_) {
    agg_col_idxs_.insert(idx);
  }
  for (auto idx : max_col_idxs_) {
    agg_col_idxs_.insert(idx);
  }
  for (auto idx : min_col_idxs_) {
    agg_col_idxs_.insert(idx);
  }
  for (auto idx : sum_col_idxs_) {
    agg_col_idxs_.insert(idx);
  }

  cur_entity_index_ = 0;
  return KStatus::SUCCESS;
}

bool TsAggIteratorImpl::IsDisordered() {
  return false;
}

void TsAggIteratorImpl::SwitchEntity() {
  TsStorageIteratorImpl::SwitchEntity();
  time_bucket_block_spans_.clear();
}

void TsAggIteratorImpl::Reset() {
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
}

inline void TsAggIteratorImpl::AddAggResult(ResultSet* res) {
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    TSSlice& slice = final_agg_data_[i];
    Batch* b;
    uint32_t col_idx = (scan_agg_types_[i] == Sumfunctype::MAX_EXTEND || scan_agg_types_[i] == Sumfunctype::MIN_EXTEND) ?
                      agg_extend_cols_[i] : kw_scan_cols_[i];
    if (slice.data == nullptr) {
      b = new AggBatch(nullptr, 0);
    } else if (!isVarLenType(attrs_[col_idx].type) || scan_agg_types_[i] == Sumfunctype::COUNT) {
      b = new AggBatch(slice.data, 1);
      b->is_new = final_agg_buffer_is_new_[i];
      b->is_overflow = is_overflow_[i];
    } else {
      std::shared_ptr<char> ptr(slice.data, free);
      b = new AggVarBatch(ptr, 1);
    }
    res->push_back(i, b);
  }
}

KStatus TsAggIteratorImpl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts,
                                  TsScanStats* ts_scan_stats) {
  *count = 0;
  if (cur_entity_index_ >= entity_ids_.size()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  EntityID entity_id = entity_ids_[cur_entity_index_];
  if (time_bucket_info_.interval == 0) {
    Reset();
    KStatus ret;
    bool last_payload_valid = false;
    timestamp64 entity_last_ts = INVALID_TS;

    std::vector<KwTsSpan> ts_spans_bkup;
    std::vector<std::shared_ptr<const TsPartitionVersion>> ts_partitions_bkup;
    if (only_last_ || only_last_row_) {
      if (EngineOptions::last_cache_max_size) {
        ret = vgroup_->GetEntityLastRowBatch(entity_id, table_version_, table_schema_mgr_, scan_schema_, parser_,
                                            ts_spans_, kw_last_scan_cols_, entity_last_ts, last_payload_valid, res);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("GetEntityLastRowBatch failed.");
          return KStatus::FAIL;
        }
        if (last_payload_valid) {
          if (entity_last_ts != INVALID_TS && (last_ts_points_.empty() || entity_last_ts <=
                                              *min_element(last_ts_points_.begin(), last_ts_points_.end()))) {
            bool has_null = false;
            if (only_last_ && !only_last_row_) {
              for (int i = 0; i < scan_agg_types_.size() && !has_null; ++i) {
                res->data[i][0]->isNull(0, &has_null);
              }
            }
            if (!has_null) {
              *count = 1;
              res->col_num_ = kw_scan_cols_.size();
              res->entity_index = {1, entity_id, vgroup_->GetVGroupID()};
              SwitchEntity();
              return KStatus::SUCCESS;
            }
          }
          res->clear();
        }
      }
      if (!last_payload_valid && only_last_row_) {
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
          ts_partitions_ = vgroup_current_version_->GetPartitions(db_id_, ts_spans_, ts_col_type_);
        }
      }
    }

    if (CLUSTER_SETTING_COUNT_USE_STATISTICS && only_count_ts_) {
      ret = CountAggregate(ts_scan_stats);
    } else if (CLUSTER_SETTING_PARTITION_AGG && only_partition_agg_type_ && !calc_partition_agg_invoke_) {
      ret = PartitionAggregate(ts_scan_stats);
    } else {
      ret = AggregateWithoutBucket(ts_scan_stats);
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
      SwitchEntity();
      return KStatus::SUCCESS;
    }

    AddAggResult(res);

    res->entity_index = {1, entity_id, vgroup_->GetVGroupID()};
    res->col_num_ = kw_scan_cols_.size();
    *count = 1;

    *is_finished = false;
    if (only_last_row_ && !last_payload_valid && entity_last_ts != INVALID_TS) {
      ts_spans_.swap(ts_spans_bkup);
      ts_partitions_.swap(ts_partitions_bkup);
    }
    SwitchEntity();
  } else {
    // Time bucket push down to storage engine.
    auto ret = GetBlockSpans(ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans failed.");
      return ret;
    }

    // Split block spans to multiple time buckets.
    std::vector<shared_ptr<TsBlockSpan>> ts_block_spans = SortBlockSpans(ts_block_spans_);

    std::vector<uint64_t> block_spans_index;
    if (!ts_block_spans.empty()) {
      time_bucket_begin_ts_ = CALCULATE_TIME_BUCKET_VALUE(ts_block_spans[0]->GetFirstTS(), time_bucket_info_.diff,
                                              time_bucket_info_.interval);
      TsBlockSpanSplitter block_span_splitter(time_bucket_begin_ts_, time_bucket_info_.interval, ts_block_spans);

      ret = block_span_splitter.SplitBlockSpans(time_bucket_block_spans_, block_spans_index);
      if (ret == KStatus::FAIL) {
        LOG_ERROR("block_span_splitter SplitBlockSpans failed.");
        return ret;
      }
    }

    // Loop all the time buckets to calculate agg data.
    for (time_bucket_block_spans_index_ = 0; time_bucket_block_spans_index_ < time_bucket_block_spans_.size();
              ++time_bucket_block_spans_index_) {
      time_bucket_index_ = block_spans_index[time_bucket_block_spans_index_];
      Reset();
      ret = AggregateWithBucket(ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("AggregateWithBucket failed.");
        return ret;
      }
      // We need to add all rows as a bulk to result for better performance.
      AddAggResult(res);
      ++(*count);
    }

    res->entity_index = {1, entity_id, vgroup_->GetVGroupID()};
    res->col_num_ = kw_scan_cols_.size();

    *is_finished = false;
    SwitchEntity();
  }
  return KStatus::SUCCESS;
}

// This is only for agg push down storage engine without time_bucket
KStatus TsAggIteratorImpl::AggregateWithoutBucket(TsScanStats* ts_scan_stats) {
  // Scan forwards to aggrate first col along with other agg functions
  int first_partition_idx = 0;
  for (; first_partition_idx < ts_partitions_.size(); ++first_partition_idx) {
    if (cur_first_col_idxs_.empty() && !has_first_row_col_) {
      break;
    }
    cur_partition_index_ = first_partition_idx;
    TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].get();
    ts_block_spans_.clear();
    auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
    std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
    ret = UpdateAggregation(sorted_block_spans, false, ts_scan_stats);
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
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].get();
    ts_block_spans_.clear();
    auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
    std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
    ret = UpdateAggregation(sorted_block_spans, true, ts_scan_stats);
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
                                entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};
      auto partition_version = ts_partitions_[cur_partition_index_].get();
      ts_block_spans_.clear();
      auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("e_paritition GetBlockSpan failed.");
        return ret;
      }
      std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
      ret = UpdateAggregation(sorted_block_spans, true, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }
  return GenerateAggData();
}

KStatus TsAggIteratorImpl::GetBlockSpans(TsScanStats* ts_scan_stats) {
  // Scan partitions to collect all block spans
  ts_block_spans_.clear();
  int first_partition_idx = 0;
  TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                            entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};
  for (auto partition : ts_partitions_) {
    auto partition_version = partition.get();
    auto ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("e_paritition GetBlockSpan failed.");
      return ret;
    }
  }
  return KStatus::SUCCESS;
}

// This is only for agg push down storage engine with time_bucket
KStatus TsAggIteratorImpl::AggregateWithBucket(TsScanStats* ts_scan_stats) {
  KStatus ret;
  // Scan forwards to aggrate first col along with other agg functions
  if (cur_first_col_idxs_.empty() && !has_first_row_col_) {
    // Scan backwards to aggrate last col along with other agg functions
    if (cur_last_col_idxs_.empty() && !has_last_row_col_) {
      if (!first_last_only_agg_) {
        // first and last col aggregations are done, so remove them.
        cur_first_col_idxs_.clear();
        cur_last_col_idxs_.clear();
        ret = UpdateAggregation(time_bucket_block_spans_[time_bucket_block_spans_index_], true, ts_scan_stats);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
      }
    } else {
      ret = UpdateAggregation(time_bucket_block_spans_[time_bucket_block_spans_index_], true, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  } else {
    ret = UpdateAggregation(time_bucket_block_spans_[time_bucket_block_spans_index_], false, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      return ret;
    }
  }
  return GenerateAggData();
}

KStatus TsAggIteratorImpl::GenerateAggData() {
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
    if (agg_type == Sumfunctype::TIME_BUCKET) {
      final_agg_data_[i].len = sizeof(timestamp64);
      final_agg_data_[i].data = static_cast<char*>(malloc(final_agg_data_[i].len));
      KTimestamp(final_agg_data_[i].data) = time_bucket_begin_ts_
                  + time_bucket_info_.interval * time_bucket_index_;
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
          std::unique_ptr<TsBitmapBase> bitmap;
          auto ret = c.blk_span->GetFixLenColAddr(col_idx, &value, &bitmap);
          if (ret != KStatus::SUCCESS) {
            return ret;
          }

          if (!attrs_[col_idx].isFlag(AINFO_NOT_NULL) && bitmap->At(c.row_idx) != DataFlags::kValid) {
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

KStatus TsAggIteratorImpl::CountAggregate(TsScanStats* ts_scan_stats) {
  KStatus ret;
  for (int idx = 0; idx < ts_partitions_.size(); idx++) {
    cur_partition_index_ = idx;
    TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};
    auto partition_version = ts_partitions_[cur_partition_index_].get();
    auto count_manager = partition_version->GetCountManager();
    TsCountStatsFileHeader count_header{};
    TsEntityCountStats count_stats{};
    if (count_manager) {
      count_stats.entity_id = entity_ids_[cur_entity_index_];
      count_manager->GetCountStatsHeader(count_header);
      count_manager->GetEntityCountStats(count_stats);
    } else {
      count_stats.entity_id = 0;
      count_stats.is_count_valid = false;
    }
    TS_OSN del_osn;
    auto s = partition_version->GetDelMaxOSN(count_stats.entity_id, del_osn);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetDelRange failed.");
      return s;
    }
    if (count_stats.is_count_valid && checkTimestampWithSpans(ts_spans_, count_stats.min_ts, count_stats.max_ts) ==
      TimestampCheckResult::FullyContained && (count_header.max_osn > del_osn)) {
      std::list<shared_ptr<TsBlockSpan>> mem_block_spans;
      ret = partition_version->GetBlockSpans(filter, &mem_block_spans, table_schema_mgr_, scan_schema_,
                                              ts_scan_stats, false, true, true);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("partition_version get mem block span failed.");
        return ret;
      }
      if (mem_block_spans.empty()) {
        KUint64(final_agg_data_[0].data) += count_stats.valid_count;
        if (ts_scan_stats != nullptr) {
          ts_scan_stats->partition_agg_count++;
        }
      } else {
        uint64_t mem_count = 0;
        std::vector<KwTsSpan> mem_ts_spans;
        TsBlockSpanSortedIterator iter(mem_block_spans, vgroup_->GetSchemaMgr(), EngineOptions::g_dedup_rule);
        iter.Init();
        std::shared_ptr<TsBlockSpan> mem_block;
        bool is_finished = false;
        while (iter.Next(mem_block, &is_finished) == KStatus::SUCCESS && !is_finished) {
          mem_ts_spans.push_back({mem_block->GetFirstTS(), mem_block->GetLastTS()});
          mem_count += mem_block->GetRowNum();
        }
        if (EngineOptions::g_dedup_rule == DedupRule::KEEP_EXPERIMENTAL ||
            (checkTimestampWithSpans(mem_ts_spans, count_stats.min_ts, count_stats.max_ts) ==
        TimestampCheckResult::NonOverlapping)) {
          KUint64(final_agg_data_[0].data) += count_stats.valid_count + mem_count;
          if (ts_scan_stats != nullptr) {
            ts_scan_stats->partition_agg_count++;
          }
        } else {
          ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("e_paritition GetBlockSpan failed.");
            return ret;
          }
          std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
          ret = UpdateAggregation(sorted_block_spans, false, ts_scan_stats);
          if (ret != KStatus::SUCCESS) {
            return ret;
          }
        }
      }
    } else {
      if (!count_stats.is_count_valid && count_stats.entity_id != 0 && EngineOptions::agg_stats_recalc_cycle != 0) {
        ret = vgroup_->AddRecalcEntity(partition_version->GetPartitionIdentifier(), table_id_, count_stats.entity_id);
        if (ret != KStatus::SUCCESS) {
          LOG_WARN("AddRecalcEntity partition[%s] table[%lu} entity[%lu] failed.",
            partition_version->GetPartitionIdentifierStr().c_str(), table_id_, count_stats.entity_id);
        }
      }
      if (CLUSTER_SETTING_PARTITION_AGG && only_partition_agg_type_ && !calc_partition_agg_invoke_) {
        ret = partitionAggImpl(ts_scan_stats);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("partitionAggImpl failed");
          return ret;
        }
      } else {
        ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
        if (ret != KStatus::SUCCESS) {
          LOG_ERROR("e_paritition GetBlockSpan failed.");
          return ret;
        }
        std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
        ret = UpdateAggregation(sorted_block_spans, false, ts_scan_stats);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorImpl::PartitionAggregate(TsScanStats* ts_scan_stats) {
  for (int i = 0; i < ts_partitions_.size(); i++) {
    cur_partition_index_ = i;
    auto s = partitionAggImpl(ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorImpl::partitionAggImpl(TsScanStats* ts_scan_stats) {
  uint32_t agg_header_size = attrs_.size() * sizeof(uint32_t);
  TsScanFilterParams filter{db_id_, table_id_, vgroup_->GetVGroupID(),
                              entity_ids_[cur_entity_index_], ts_col_type_, scan_osn_, ts_spans_};

  auto partition = ts_partitions_[cur_partition_index_];
  auto path = partition->GetPartitionPath();

  TS_OSN max_osn;
  auto s = partition->GetMaxOSN(db_id_, table_id_, entity_ids_[cur_entity_index_], ts_col_type_, max_osn);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelMaxOSN failed");
    return s;
  }

  auto agg_reader = partition->GetAggReader();
  TsEntityPartitionAggIndex agg_index;
  if (agg_reader) {
    agg_index.entity_id = entity_ids_[cur_entity_index_];
    s = agg_reader->GetPartitionAggIndex(agg_index);
    if (s != KStatus::SUCCESS) {
      LOG_INFO("table %lu entity %u has no partition aggregation result, will goto general query", table_id_,
        entity_ids_[cur_entity_index_]);
      s = partition->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("partition [%s] GetBlockSpan failed", partition->GetPartitionPath().c_str());
        return s;
      }
      std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
      s = UpdateAggregation(sorted_block_spans, false, ts_scan_stats);
      return s;
    }
  }

  if (agg_reader && agg_index.max_osn >= max_osn && agg_index.table_version == table_version_ &&
    checkTimestampWithSpans(ts_spans_, agg_index.min_ts,
                            agg_index.max_ts) == TimestampCheckResult::FullyContained) {
    TsSliceGuard entity_agg;
    s = agg_reader->GetPartitionAgg(agg_index.agg_offset, agg_index.agg_len, entity_agg);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetPartitionAgg failed");
      return s;
    }

    std::unordered_map<uint32_t, TsSliceGuard> col_aggs;
    for (auto col_idx : agg_col_idxs_) {
      auto kw_col_idx = kw_scan_cols_[col_idx];
      TsSliceGuard col_agg;
      uint32_t start_offset = 0;
      if (kw_col_idx != 0) {
        start_offset = *reinterpret_cast<uint32_t*>(entity_agg.data() + (kw_col_idx - 1) * sizeof(uint32_t));
      }
      uint32_t end_offset = *reinterpret_cast<uint32_t*>(entity_agg.data() + (kw_col_idx) * sizeof(uint32_t));
      assert(end_offset >= start_offset);
      uint32_t len = end_offset - start_offset;
      if (len) {
        col_agg = TsSliceGuard(entity_agg.data() + agg_header_size + start_offset, len);
      }
      col_aggs[kw_col_idx] = std::move(col_agg);
    }

    // Aggregate count col
    for (auto idx : count_col_idxs_) {
      auto kw_col_idx = kw_scan_cols_[idx];
      auto& col_agg = col_aggs[kw_col_idx];
      if (!col_agg.empty()) {
        KUint64(final_agg_data_[idx].data) += *reinterpret_cast<uint64_t*>(col_agg.data());
      }
    }
    // Aggregate max col
    for (auto idx : max_col_idxs_) {
      auto kw_col_idx = kw_scan_cols_[idx];
      auto& col_agg = col_aggs[kw_col_idx];
      if (col_agg.empty()) {
        continue;
      }

      auto type = attrs_[kw_col_idx].type;
      TSSlice& agg_data = final_agg_data_[idx];

      if (!isVarLenType(type)) {
        void* pre_max = col_agg.data() + sizeof(uint64_t);
        int32_t size = kw_col_idx == 0 ? 8 : attrs_[kw_col_idx].size;
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
        auto max_len = *reinterpret_cast<const uint16_t*>(col_agg.data() + sizeof(uint64_t));
        string pre_max_val(col_agg.data() + sizeof(uint64_t) + sizeof(uint16_t) * 2, max_len);
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
    // Aggregate min col
    for (auto idx : min_col_idxs_) {
      auto kw_col_idx = kw_scan_cols_[idx];
      auto& col_agg = col_aggs[kw_col_idx];
      if (col_agg.empty()) {
        continue;
      }

      auto type = attrs_[kw_col_idx].type;
      TSSlice& agg_data = final_agg_data_[idx];

      if (!isVarLenType(type)) {
        int32_t size = kw_col_idx == 0 ? 8 : attrs_[kw_col_idx].size;
        void* pre_min = col_agg.data() + sizeof(uint64_t) + size;
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
        auto max_len = *reinterpret_cast<const uint16_t*>(col_agg.data() + sizeof(uint64_t));
        auto min_len = *reinterpret_cast<const uint16_t*>(col_agg.data() + sizeof(uint64_t) + sizeof(uint16_t));
        void* pre_min = col_agg.data() + sizeof(uint64_t) + sizeof(uint16_t) * 2 + max_len;
        string pre_min_val(static_cast<char*>(pre_min), min_len);
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
    // Aggregate sum col
    for (auto idx : sum_col_idxs_) {
      auto kw_col_idx = kw_scan_cols_[idx];
      auto& col_agg = col_aggs[kw_col_idx];
      if (col_agg.empty()) {
        continue;
      }

      int32_t size = kw_col_idx == 0 ? 8 : attrs_[kw_col_idx].size;
      auto type = attrs_[kw_col_idx].type;
      bool pre_sum_is_overflow = *reinterpret_cast<bool*>(col_agg.data() + sizeof(uint64_t) + size * 2);
      void* pre_sum = col_agg.data() + sizeof(uint64_t) + size * 2 + 1;
      TSSlice& agg_data = final_agg_data_[idx];
      if (agg_data.data == nullptr) {
        agg_data.len = sizeof(int64_t);
        InitAggData(agg_data);
        InitSumValue(agg_data.data, type);
      }
      KStatus ret = KStatus::SUCCESS;
      if (!is_overflow_[idx]) {
        ret = AddSumNotOverflowYetByPreSum(idx, type, pre_sum, agg_data, pre_sum_is_overflow);
      } else {
        ret = AddSumOverflowByPreSum(type, pre_sum, agg_data, pre_sum_is_overflow);
      }
      if (ret != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    }
    if (ts_scan_stats != nullptr) {
      ts_scan_stats->partition_agg_count++;
    }
  } else {
    s = partition->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition [%s] GetBlockSpan failed", partition->GetPartitionPath().c_str());
      return s;
    }
    std::vector<std::shared_ptr<TsBlockSpan>> sorted_block_spans = SortBlockSpans(ts_block_spans_);
    s = UpdateAggregation(sorted_block_spans, false, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      return s;
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

std::vector<shared_ptr<TsBlockSpan>> TsAggIteratorImpl::SortBlockSpans(
                                                        std::list<std::shared_ptr<TsBlockSpan>>& ts_block_spans) {
  std::vector<shared_ptr<TsBlockSpan>> sorted_block_spans;
  if (!ts_block_spans.empty()) {
    TsBlockSpanSortedIterator iter(ts_block_spans, vgroup_->GetSchemaMgr(), EngineOptions::g_dedup_rule);
    iter.Init();
    std::shared_ptr<TsBlockSpan> dedup_block_span;
    bool is_finished = false;
    while (iter.Next(dedup_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
      sorted_block_spans.push_back(std::move(dedup_block_span));
    }
  }
  return sorted_block_spans;
}

KStatus TsAggIteratorImpl::UpdateAggregation(std::vector<std::shared_ptr<TsBlockSpan>>& ts_block_spans,
                                              bool can_remove_last_candidate, TsScanStats* ts_scan_stats) {
  if (ts_block_spans.empty()) {
    return KStatus::SUCCESS;
  }
  KStatus ret;
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
      ret = UpdateAggregation(blk_span, true, false, ts_scan_stats);
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
      ret = UpdateAggregation(blk_span, true, can_remove_last_candidate, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
      --block_span_backward_idx;
    }
  }

  if (!first_last_only_agg_) {
    for (; block_span_idx <= block_span_backward_idx; ++block_span_idx) {
      shared_ptr<TsBlockSpan>& blk_span = ts_block_spans[block_span_idx];
      ret = UpdateAggregation(blk_span, false, can_remove_last_candidate, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }
  return KStatus::SUCCESS;
}

inline void TsAggIteratorImpl::InitAggData(TSSlice& agg_data) {
  agg_data.data = static_cast<char*>(malloc(agg_data.len));
  memset(agg_data.data, 0, agg_data.len);
}

inline void TsAggIteratorImpl::InitSumValue(void* data, int32_t type) {
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

inline void TsAggIteratorImpl::ConvertToDoubleIfOverflow(uint32_t col_idx, bool over_flow, TSSlice& agg_data) {
  if (over_flow) {
    *reinterpret_cast<double*>(agg_data.data) = *reinterpret_cast<int64_t*>(agg_data.data);
    is_overflow_[col_idx] = true;
  }
}

inline KStatus TsAggIteratorImpl::AddSumNotOverflowYet(uint32_t col_idx,
                                                          int32_t type,
                                                          void* current,
                                                          TSSlice& agg_data) {
  bool over_flow = false;
  switch (type) {
    case DATATYPE::INT8:
      over_flow = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int8_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, over_flow, agg_data);
      break;
    case DATATYPE::INT16:
      over_flow = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int16_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, over_flow, agg_data);
      break;
    case DATATYPE::INT32:
      over_flow = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int32_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, over_flow, agg_data);
      break;
    case DATATYPE::INT64:
      over_flow = AddAggInteger<int64_t>(
          *reinterpret_cast<int64_t*>(agg_data.data),
          *reinterpret_cast<int64_t*>(current));
      ConvertToDoubleIfOverflow(col_idx, over_flow, agg_data);
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
      LOG_ERROR("Not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
      break;
  }
  return KStatus::SUCCESS;
}


inline KStatus TsAggIteratorImpl::AddSumNotOverflowYetByPreSum(uint32_t col_idx,
                                                                 int32_t type,
                                                                 void* current,
                                                                 TSSlice& agg_data,
                                                                 bool is_overflow) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::INT16:
    case DATATYPE::INT32:
    case DATATYPE::INT64: {
      if (!is_overflow) {
        if (AddAggInteger<int64_t>(*reinterpret_cast<int64_t*>(agg_data.data),
                                   *reinterpret_cast<int64_t*>(current))) {
          ConvertToDoubleIfOverflow(col_idx, true, agg_data);
          AddAggFloat<double, int64_t>(*reinterpret_cast<double*>(agg_data.data),
                                          *reinterpret_cast<int64_t*>(current));
        }
      } else {
        ConvertToDoubleIfOverflow(col_idx, true, agg_data);
        AddAggFloat<double, double>(*reinterpret_cast<double*>(agg_data.data),
                                    *reinterpret_cast<double*>(current));
      }
      break;
    }
    case DATATYPE::FLOAT:
    case DATATYPE::DOUBLE: {
      if (!is_overflow) {
        AddAggFloat<double, double>(*reinterpret_cast<double*>(agg_data.data), *reinterpret_cast<double*>(current));
      } else {
        LOG_ERROR("Overflow not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
        return KStatus::FAIL;
      }
      break;
    }
    default:
      LOG_ERROR("Not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsAggIteratorImpl::AddSumOverflow(int32_t type,
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
      LOG_ERROR("Overflow not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
      break;
    default:
      LOG_ERROR("Not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
      break;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsAggIteratorImpl::AddSumOverflowByPreSum(int32_t type,
                                                           void* current,
                                                           TSSlice& agg_data,
                                                           bool is_overflow) {
  switch (type) {
    case DATATYPE::INT8:
    case DATATYPE::INT16:
    case DATATYPE::INT32:
    case DATATYPE::INT64: {
      if (!is_overflow) {
        AddAggFloat<double, int64_t>(*reinterpret_cast<double*>(agg_data.data), *reinterpret_cast<int64_t*>(current));
      } else {
        AddAggFloat<double, double>(*reinterpret_cast<double*>(agg_data.data), *reinterpret_cast<double*>(current));
      }
      break;
    }
    case DATATYPE::FLOAT:
    case DATATYPE::DOUBLE:
      LOG_ERROR("Overflow not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
    default:
      LOG_ERROR("Not supported for sum, datatype: %d  table_id: %lu", type, table_id_);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsAggIteratorImpl::UpdateAggregation(std::shared_ptr<TsBlockSpan>& block_span,
                                                bool aggregate_first_last_cols,
                                                bool can_remove_last_candidate,
                                                TsScanStats* ts_scan_stats) {
  KStatus ret;
  std::unique_ptr<TsBitmapBase> bitmap;
  TsBitmapBase *pbitmap = nullptr;
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
        ret = block_span->GetColBitmap(kw_col_idx, &bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        pbitmap = bitmap.get();
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (pbitmap->At(row_idx) != DataFlags::kValid) {
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
        ret = block_span->GetColBitmap(kw_col_idx, &bitmap);
        if (ret != KStatus::SUCCESS) {
          return ret;
        }
        pbitmap = bitmap.get();
        for (int row_idx = row_num - 1; row_idx >= 0; --row_idx) {
          if (pbitmap->At(row_idx) != DataFlags::kValid) {
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
      // No data for this column in this block span, so just move on to the next count col.
      continue;
    }
    if (block_span->IsColNotNull(kw_col_idx)) {
      KUint64(final_agg_data_[idx].data) += block_span->GetRowNum();
    } else {
      if (block_span->HasPreAgg()) {
        // Use pre agg to calculate count
        uint16_t pre_count{0};
        ret = block_span->GetPreCount(kw_col_idx, ts_scan_stats, pre_count);
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
      // No data for this column in this block span, so just move on to the next sum col.
      continue;
    }
    auto type = block_span->GetColType(kw_col_idx);
    if (block_span->HasPreAgg()) {
      // Use pre agg to calculate sum
      void* pre_sum{nullptr};
      bool pre_sum_is_overflow{false};
      ret = block_span->GetPreSum(kw_col_idx, ts_scan_stats, pre_sum, pre_sum_is_overflow);
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
        ret = AddSumNotOverflowYetByPreSum(idx, type, pre_sum, agg_data, pre_sum_is_overflow);
      } else {
        ret = AddSumOverflowByPreSum(type, pre_sum, agg_data, pre_sum_is_overflow);
      }
      if (ret != KStatus::SUCCESS) {
        return KStatus::FAIL;
      }
    } else {
      char* value = nullptr;
      auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, &bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetFixLenColAddr failed.");
        return s;
      }
      pbitmap = bitmap.get();
      int32_t size = block_span->GetColSize(kw_col_idx);
      bool col_not_null = attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL);
      bool all_valid = pbitmap->IsAllValid();
      for (int row_idx = 0; row_idx < row_num; ++row_idx) {
        if (!col_not_null && !all_valid && pbitmap->At(row_idx) != DataFlags::kValid) {
          continue;
        }
        void* current = reinterpret_cast<void*>((intptr_t)(value + row_idx * size));
        TSSlice& agg_data = final_agg_data_[idx];
        if (agg_data.data == nullptr) {
          agg_data.len = sizeof(int64_t);
          InitAggData(agg_data);
          InitSumValue(agg_data.data, type);
        }
        std::vector<bool>::reference cur_overflow = is_overflow_[idx];
        if (!cur_overflow) {
          ret = AddSumNotOverflowYet(idx, type, current, agg_data);
          if (ret != KStatus::SUCCESS) {
            return KStatus::FAIL;
          }
        }
        if (cur_overflow) {
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
      // No data for this column in this block span, so just move on to the next max col.
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
        ret = block_span->GetPreMax(kw_col_idx, ts_scan_stats, pre_max);  // pre agg max(timestamp) use 8 bytes
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
        ret = block_span->GetVarPreMax(kw_col_idx, ts_scan_stats, pre_max);
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
        auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, &bitmap);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }
        pbitmap = bitmap.get();
        int32_t size = block_span->GetColSize(kw_col_idx);
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (!attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL) && pbitmap->At(row_idx) != DataFlags::kValid) {
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
          ret = block_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, flag, slice, ts_scan_stats);
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
      // No data for this column in this block span, so just move on to the next min col.
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
        ret = block_span->GetPreMin(kw_col_idx, ts_scan_stats, pre_min);  // pre agg min(timestamp) use 8 bytes
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
        ret = block_span->GetVarPreMin(kw_col_idx, ts_scan_stats, pre_min);
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
        auto s = block_span->GetFixLenColAddr(kw_col_idx, &value, &bitmap);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetFixLenColAddr failed.");
          return s;
        }
        pbitmap = bitmap.get();
        int32_t size = block_span->GetColSize(kw_col_idx);
        for (int row_idx = 0; row_idx < row_num; ++row_idx) {
          if (!attrs_[kw_col_idx].isFlag(AINFO_NOT_NULL) && pbitmap->At(row_idx) != DataFlags::kValid) {
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
          ret = block_span->GetVarLenTypeColAddr(row_idx, kw_col_idx, flag, slice, ts_scan_stats);
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

KStatus TsOffsetIteratorImpl::divideBlockSpans(timestamp64 begin_ts, timestamp64 end_ts, uint32_t* lower_cnt,
                                                 deque<pair<pair<timestamp64, timestamp64>,
                                                 std::shared_ptr<TsBlockSpan>>>& lower_block_span) {
  uint32_t size = filter_block_spans_.size();
  timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
  for (uint32_t i = 0; i < size; ++i) {
    timestamp64 min_ts = filter_block_spans_.front().first.first;
    timestamp64 max_ts = filter_block_spans_.front().first.second;
    std::shared_ptr<TsBlockSpan> block_span = filter_block_spans_.front().second;
    filter_block_spans_.pop_front();
    std::shared_ptr<TsBlock> block = block_span->GetTsBlock();
    if ((is_reversed_ && min_ts > mid_ts) || (!is_reversed_ && max_ts < mid_ts)) {
      *lower_cnt += block_span->GetRowNum();
      lower_block_span.push_back({{min_ts, max_ts}, block_span});
    } else if ((is_reversed_ && max_ts <= mid_ts) || (!is_reversed_ && min_ts >= mid_ts)) {
      filter_block_spans_.push_back({{min_ts, max_ts}, block_span});
    } else {
      int row_num = block_span->GetRowNum();
      int begin_row = block_span->GetStartRow(), end_row = begin_row + row_num - 1;
      int left = begin_row, right = end_row;
      int split_row = -1;
      timestamp64 split_ts = INVALID_TS;
      while (left <= right) {
        int mid = left + (right - left) / 2;
        timestamp64 blk_mid_ts = block_span->GetTS(mid - begin_row);
        bool is_lower = ((is_reversed_ && (blk_mid_ts > mid_ts)) || (!is_reversed_ && (blk_mid_ts < mid_ts)));
        if (is_lower) {
          split_row = mid;
          split_ts = blk_mid_ts;
          is_reversed_ ? right = mid - 1 : left = mid + 1;
        } else {
          is_reversed_ ? left = mid + 1 : right = mid - 1;
        }
      }
      assert((is_reversed_ && split_row != begin_row) || (!is_reversed_ && split_row != end_row));
      if (!is_reversed_) {
        lower_block_span.push_back({
          {min_ts, split_ts},
          make_shared<TsBlockSpan>(*block_span, block, begin_row, split_row - begin_row + 1)
        });
        filter_block_spans_.push_back({
          {block_span->GetTS(split_row + 1 - begin_row), max_ts},
          make_shared<TsBlockSpan>(*block_span, block, split_row + 1, end_row - split_row)
        });
      } else {
        lower_block_span.push_back({
          {split_ts, max_ts},
          make_shared<TsBlockSpan>(*block_span, block, split_row, end_row - split_row + 1)
        });
        filter_block_spans_.push_back({
          {min_ts, block_span->GetTS(split_row - 1 - begin_row)},
          make_shared<TsBlockSpan>(*block_span, block, begin_row,  split_row - begin_row)
        });
      }
      *lower_cnt += is_reversed_ ? (end_row - split_row + 1) : (split_row - begin_row + 1);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorImpl::filterLower(uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetStartTime(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetEndTime(), ts_col_type_);
  while (!filter_end_) {
    uint32_t lower_cnt = 0;
    deque<pair<pair<timestamp64, timestamp64>, std::shared_ptr<TsBlockSpan>>> lower_block_span;
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
        *cnt += filter_block_spans_.front().second->GetRowNum();
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
    *cnt += filter_block_spans_.front().second->GetRowNum();
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorImpl::filterUpper(uint32_t filter_num, uint32_t* cnt) {
  *cnt = 0;
  timestamp64 begin_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetStartTime(), ts_col_type_);
  timestamp64 end_ts = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetEndTime(), ts_col_type_);
  bool filter_end = false;
  while (!filter_end) {
    uint32_t lower_cnt = 0;
    deque<pair<pair<timestamp64, timestamp64>, std::shared_ptr<TsBlockSpan>>> lower_block_span;
    timestamp64 mid_ts = begin_ts + (end_ts - begin_ts) / 2;
    if (divideBlockSpans(begin_ts, end_ts, &lower_cnt, lower_block_span) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    if (lower_cnt >= filter_num) {
      is_reversed_ ? begin_ts = mid_ts : end_ts = mid_ts;
      deque<pair<pair<timestamp64, timestamp64>, std::shared_ptr<TsBlockSpan>>>().swap(filter_block_spans_);
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
    *cnt += filter_block_spans_.front().second->GetRowNum();
    block_spans_.push_back(filter_block_spans_.front());
    filter_block_spans_.pop_front();
  }
  return KStatus::SUCCESS;
}

KStatus TsOffsetIteratorImpl::ScanPartitionBlockSpans(uint32_t* cnt, TsScanStats* ts_scan_stats) {
  *cnt = 0;
  KStatus ret = KStatus::SUCCESS;
  for (const auto& it : p_time_it_->second) {
    uint32_t vgroup_id = it.first;
    std::shared_ptr<const TsPartitionVersion> partition_version = it.second;
    std::shared_ptr<TsVGroup> vgroup = vgroups_[vgroup_id];
    std::vector<EntityID> entity_ids = vgroup_ids_[vgroup_id];
    // TODO(liumengzhen) Can the "filter" parameter support multiple devices
    for (auto entity_id : entity_ids) {
      ts_block_spans_.clear();
      TsScanFilterParams filter{db_id_, table_id_, vgroup_id, entity_id, ts_col_type_, scan_osn_, ts_spans_};
      ret = partition_version->GetBlockSpans(filter, &ts_block_spans_, table_schema_mgr_, schema_, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpan failed.");
        return KStatus::FAIL;
      }
      // dedup
      vector<pair<timestamp64, timestamp64>> intervals;
      std::map<pair<timestamp64, timestamp64>, std::vector<std::shared_ptr<TsBlockSpan>>> interval_block_span_map;
      while (!ts_block_spans_.empty()) {
        auto block_span = std::move(ts_block_spans_.front());
        ts_block_spans_.pop_front();
        timestamp64 begin_ts = block_span->GetFirstTS(), end_ts = block_span->GetLastTS();
        if (!block_span->GetRowNum() || checkTimestampWithSpans(ts_spans_, begin_ts, end_ts) ==
                                        TimestampCheckResult::NonOverlapping) {
          continue;
        }
        if (!interval_block_span_map.count({begin_ts, end_ts})) {
          intervals.push_back({begin_ts, end_ts});
        }
        interval_block_span_map[{begin_ts, end_ts}].emplace_back(block_span);
      }

      sort(intervals.begin(), intervals.end());
      std::vector<pair<pair<timestamp64, timestamp64>, std::vector<std::shared_ptr<TsBlockSpan>>>> sorted_block_spans;
      for (auto interval : intervals) {
        if (sorted_block_spans.empty() || interval.first > sorted_block_spans.back().first.second) {
          sorted_block_spans.push_back({{interval.first, interval.second},
                                       interval_block_span_map[interval]});
        } else {
          if (interval.second > sorted_block_spans.back().first.second) {
            sorted_block_spans.back().first.second = interval.second;
          }
          sorted_block_spans.back().second.insert(sorted_block_spans.back().second.end(),
                                                  interval_block_span_map[interval].begin(),
                                                  interval_block_span_map[interval].end());
        }
      }
      intervals.clear();
      interval_block_span_map.clear();
      std::list<std::shared_ptr<TsBlockSpan>> dedup_block_spans;
      for (auto& cur_block_spans : sorted_block_spans) {
        if (cur_block_spans.second.size() > 1) {
          dedup_block_spans.insert(dedup_block_spans.end(), cur_block_spans.second.begin(), cur_block_spans.second.end());
        } else {
          auto cur_ts_range = cur_block_spans.first;
          auto cur_block_span = cur_block_spans.second.front();
          filter_block_spans_.push_back({cur_ts_range, cur_block_span});
          *cnt += cur_block_span->GetRowNum();
        }
      }

      TsBlockSpanSortedIterator sorted_iter(dedup_block_spans, vgroup->GetSchemaMgr(),
                                            EngineOptions::g_dedup_rule, is_reversed_);
      ret = sorted_iter.Init();
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("TsOffsetIteratorImpl failed to init block span sorted iterator for partition: %lu.", p_time_it_->first);
        return KStatus::FAIL;
      }
      bool is_finished = false;
      std::shared_ptr<TsBlockSpan> cur_block_span;
      while (sorted_iter.Next(cur_block_span, &is_finished) == KStatus::SUCCESS && !is_finished) {
        auto cur_ts_range = std::make_pair(cur_block_span->GetFirstTS(), cur_block_span->GetLastTS());
        filter_block_spans_.push_back({cur_ts_range, cur_block_span});
        *cnt += cur_block_span->GetRowNum();
      }
    }
  }
  return ret;
}

KStatus TsOffsetIteratorImpl::Init(bool is_reversed) {
  GetTerminationTime();
  is_reversed_ = is_reversed;
  comparator_.is_reversed = is_reversed_;
  decltype(p_times_) t_map(comparator_);
  p_times_.swap(t_map);
  attrs_ = *schema_->getSchemaInfoExcludeDroppedPtr();
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

KStatus TsOffsetIteratorImpl::filterBlockSpan(TsScanStats* ts_scan_stats) {
  Defer defer{[&]() { ts_block_spans_.clear(); }};
  uint32_t row_cnt = 0;
  KStatus ret = ScanPartitionBlockSpans(&row_cnt, ts_scan_stats);
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
      std::deque<pair<pair<timestamp64, timestamp64>, std::shared_ptr<TsBlockSpan>>>().swap(filter_block_spans_);
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

KStatus TsOffsetIteratorImpl::Next(ResultSet* res, k_uint32* count, timestamp64 ts, TsScanStats* ts_scan_stats) {
  *count = 0;
  KStatus ret;
  while (block_spans_.empty()) {
    // scan over
    if (p_time_it_ == p_times_.end() || queried_cnt >= offset_ + limit_ - filter_cnt_) {
      return KStatus::SUCCESS;
    }
    if (ts != INVALID_TS) {
      timestamp64 p_begin = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetStartTime(), ts_col_type_);
      timestamp64 p_end = convertSecondToPrecisionTS(p_time_it_->second.begin()->second->GetEndTime(), ts_col_type_);
      if ((is_reversed_ && p_end <= ts) || (!is_reversed_ && p_begin >= ts)) {
        return KStatus::SUCCESS;
      }
    }
    ret = filterBlockSpan(ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("call filterBlockSpan failed.");
      return KStatus::FAIL;
    }
    ++p_time_it_;
  }
  // Return one block span data each time.
  std::shared_ptr<TsBlockSpan> ts_block = nullptr;
  while (nullptr == ts_block && !block_spans_.empty()) {
    if (ts != INVALID_TS) {
      timestamp64 min_ts = block_spans_.front().first.first;
      timestamp64 max_ts = block_spans_.front().first.second;
      if ((is_reversed_ && max_ts <= ts) || (!is_reversed_ && min_ts >= ts)) {
        block_spans_.pop_front();
        continue;
      }
    }
    ts_block = block_spans_.front().second;
    block_spans_.pop_front();
  }
  if (nullptr == ts_block) {
    return KStatus::SUCCESS;
  }
  ret = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, ts_block, res, count, ts_scan_stats);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get next block span for current partition: %ld.", p_time_it_->first);
    return KStatus::FAIL;
  }
  // We are returning memory address inside TsBlockSpan, so we need to keep it until iterator is destroyed
  ts_block_spans_with_data_.push_back(ts_block);
  return KStatus::SUCCESS;
}

TsRawDataIteratorImplByOSN::TsRawDataIteratorImplByOSN(const std::shared_ptr<TsVGroup>& vgroup,
  uint32_t version, vector<EntityResultIndex>& entity_ids,
  std::vector<k_uint32>& scan_cols, std::vector<k_uint32>& ts_scan_cols,
  std::vector<KwOSNSpan>& osn_spans, std::vector<KwTsSpan>& ts_spans,
  const std::shared_ptr<TsTableSchemaManager>& table_schema_mgr) : osn_span_(osn_spans), entitys_(entity_ids) {
  table_schema_mgr_ = table_schema_mgr;
  table_version_ = version;
  kw_scan_cols_ = scan_cols;
  ts_scan_cols_ = ts_scan_cols;
  vgroup_ = vgroup;
  ts_spans_ = SortAndMergeSpan(ts_spans);
}

KStatus TsRawDataIteratorImplByOSN::Init() {
  auto s = table_schema_mgr_->GetMetricSchema(table_version_, &scan_schema_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("get table version[%u] schema failed.", table_version_);
    return s;
  }
  attrs_ = *scan_schema_->getSchemaInfoExcludeDroppedPtr();
  ts_spans_.push_back({INT64_MIN, INT64_MAX});
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = scan_schema_->metaData()->db_id;
  ts_col_type_ = scan_schema_->GetTsColDataType();
  vgroup_current_version_ = vgroup_->CurrentVersion();
  ts_partitions_ = vgroup_current_version_->GetPartitions(db_id_, ts_spans_, ts_col_type_);
  filter_ = std::make_shared<TsScanFilterParams>(db_id_, table_id_, vgroup_->GetVGroupID(), 0,
              ts_col_type_, ts_spans_, osn_span_);
  return KStatus::SUCCESS;
}

KStatus TsRawDataIteratorImplByOSN::MoveToNextEntity(bool* is_finished, TsScanStats* ts_scan_stats) {
  ts_block_spans_.clear();
  cur_entity_index_++;
  if (cur_entity_index_ >= entitys_.size()) {
    // All entities are scanned.
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  filter_->entity_id_ = entitys_[cur_entity_index_].entityId;
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entitys_[cur_entity_index_].op_with_osn.get());
  if (op_osn->type != OperatorTypeOfRecord::OP_TYPE_TAG_DELETE) {
    for (auto& partition_version : ts_partitions_) {
      auto s = partition_version->GetBlockSpans(*filter_, &ts_block_spans_, table_schema_mgr_, scan_schema_, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("partition_version GetBlockSpan failed.");
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsRawDataIteratorImplByOSN::Next(ResultSet* res, k_uint32* count, bool* is_finished,
  timestamp64 ts, TsScanStats* ts_scan_stats) {
  *count = 0;
  *is_finished = false;
  while (true) {
    if (ts_block_spans_.size() == 0 && SendingStatus::SENDING_METRIC_ROWS == cur_entity_status_) {
      auto ret = MoveToNextEntity(is_finished, ts_scan_stats);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("MoveToNextEntity failed.");
        return ret;
      }
      if (*is_finished) {
        return KStatus::SUCCESS;
      }
      cur_entity_status_ = SendingStatus::SENDING_DEL_INFO;
    }
    switch (cur_entity_status_) {
      case SendingStatus::SENDING_DEL_INFO: {
        auto s = GetMetricDelRows(res, count);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetMetricDelRows failed.");
          return s;
        }
        cur_entity_status_ = SendingStatus::SENDING_EMPTY_ROW;
        break;
      }
      case SendingStatus::SENDING_EMPTY_ROW: {
        // if only tag, no metric data, we need create empty row.
        auto oper_info = reinterpret_cast<OperatorInfoOfRecord*>(entitys_[cur_entity_index_].op_with_osn.get());
        assert(oper_info != nullptr);
        if (oper_info->type == OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE ||
            oper_info->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE ||
            (oper_info->type == OperatorTypeOfRecord::OP_TYPE_INSERT && ts_block_spans_.size() == 0)) {
          auto s = FillEmptyMetricRow(res, 1, oper_info->osn, oper_info->type);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("FillEmptyMetricRow failed.");
            return s;
          }
          *count = 1;
        }
        cur_entity_status_ = SendingStatus::SENDING_METRIC_ROWS;
        break;
      }
      case SendingStatus::SENDING_METRIC_ROWS: {
        auto s = GetMetricInsertRows(res, count, ts_scan_stats);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetMetricInsertRows failed.");
          return s;
        }
        break;
      }
      default:
        LOG_ERROR("sending status cannot be this. sending status is %d", cur_entity_status_);
        return KStatus::FAIL;
    }
    if (*count > 0) {
      return KStatus::SUCCESS;
    }
  }
  LOG_WARN("should not exec here.");
  return KStatus::FAIL;
}

KStatus TsRawDataIteratorImplByOSN::GetMetricDelRows(ResultSet* res, k_uint32* count) {
  *count = 0;
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entitys_[cur_entity_index_].op_with_osn.get());
  assert(op_osn != nullptr);
  if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE) {
    // if tag is deleted, no need search metric delete operation.
    return KStatus::SUCCESS;
  }
  std::list<STDelRange> del_spans;
  auto s = vgroup_->GetDelInfoWithOSN(nullptr, table_id_, entitys_[cur_entity_index_].entityId, &del_spans);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelInfoByOSN failed.");
    return s;
  }
  std::vector<STDelRange> del_ranges;
  for (auto& del : del_spans) {
    if (IsOsnInSpans(del.osn_span.end, osn_span_)) {
      del_ranges.push_back(del);
    }
  }
  if (del_ranges.empty()) {
    return KStatus::SUCCESS;
  }
  *count = del_ranges.size();
  s = FillEmptyMetricRow(res, del_ranges.size(), 1, OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("FillEmptyMetricRow failed.");
    return s;
  }
  size_t ext_event_idx = kw_scan_cols_.size() + 2;
  size_t cur_idx = res->data[0].size() - 1;
  for (size_t i = 0; i < del_ranges.size(); i++) {
    STDelRange& cur_del = del_ranges[i];
    KUint64(reinterpret_cast<char*>(res->data[ext_event_idx - 2][cur_idx]->mem) + i * 8) = cur_del.osn_span.end;
    KUint64(reinterpret_cast<char*>(res->data[ext_event_idx][cur_idx]->mem) + i * 16) = cur_del.ts_span.begin;
    KUint64(reinterpret_cast<char*>(res->data[ext_event_idx][cur_idx]->mem) + i * 16 + 8) = cur_del.ts_span.end;
  }
  return KStatus::SUCCESS;
}

KStatus TsRawDataIteratorImplByOSN::AppendExtendColSpace(ResultSet* res, uint32_t count) {
  size_t bitmap_len = (count + 7) / 8;
  char* value = reinterpret_cast<char*>(malloc(count * (8 + 1 + 16) + 3 * bitmap_len));
  memset(value, 0, count * (8 + 1 + 16) + 3 * bitmap_len);
  Batch* batch = new Batch(value + 3 * bitmap_len, count, value, 1);
  batch->need_free_bitmap = true;  // free memory for value.
  size_t extend_col_idx = kw_scan_cols_.size();
  res->push_back(extend_col_idx, batch);
  batch = new Batch(value + 3 * bitmap_len + 8 * count, count, value + bitmap_len, 1);
  res->push_back(extend_col_idx + 1, batch);
  batch = new Batch(value + 3 * bitmap_len + (8 + 1) * count, count, value + 2 * bitmap_len, 1);
  res->push_back(extend_col_idx + 2, batch);
  return KStatus::SUCCESS;
}

KStatus TsRawDataIteratorImplByOSN::FillEmptyMetricRow(ResultSet* res, uint32_t count,
  TS_OSN osn, OperatorTypeOfRecord type) {
  for (int i = 0; i < kw_scan_cols_.size(); ++i) {
    Batch* batch = nullptr;
    // just as all column is dropped at block version.
    batch = new Batch(nullptr, count, nullptr);
    res->push_back(i, batch);
  }
  res->entity_index = entitys_[cur_entity_index_];
  // add  osn | type | event  columns.
  AppendExtendColSpace(res, count);
  size_t ext_idx = kw_scan_cols_.size();
  size_t vector_idx = res->data[0].size() - 1;
  for (size_t i = 0; i < count; i++) {
    KUint64(reinterpret_cast<char*>(res->data[ext_idx + 0][vector_idx]->mem) + i * 8) = osn;
    KUint8(reinterpret_cast<char*>(res->data[ext_idx + 1][vector_idx]->mem) + i * 1) = type;
  }
  if (type == OperatorTypeOfRecord::OP_TYPE_INSERT) {
    for (size_t i = 0; i < count; i++) {
      KUint8(reinterpret_cast<char*>(res->data[ext_idx + 2][vector_idx]->mem) + i * 16) = type;
    }
  } else if (type == OperatorTypeOfRecord::OP_TYPE_METRIC_DELETE) {
    // delete will insert this column.
  } else {
    setBatchDeleted(reinterpret_cast<char*>(res->data[ext_idx + 2][vector_idx]->bitmap), 1, count);
  }
  return KStatus::SUCCESS;
}

KStatus TsRawDataIteratorImplByOSN::GetMetricInsertRows(ResultSet* res, k_uint32* count, TsScanStats* ts_scan_stats) {
  *count = 0;
  if (ts_block_spans_.size() > 0) {
    auto block_span = ts_block_spans_.front();
    ts_block_spans_.pop_front();
    // no need remove repeat data, just send all.
    auto ret = ConvertBlockSpanToResultSet(kw_scan_cols_, attrs_, block_span, res, count, ts_scan_stats);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("ConvertBlockSpanToResultSet failed.");
      return ret;
    }
    // append extern column: osn | processed_type | processed_event_info.
    AppendExtendColSpace(res, *count);
    size_t ext_idx = kw_scan_cols_.size();
    assert(res->block_span->GetRowNum() == *count);
    size_t vector_idx = res->data[0].size() - 1;
    for (size_t i = 0; i < *count; i++) {
      KUint64(reinterpret_cast<char*>(res->data[ext_idx + 0][vector_idx]->mem) + 8 * i) =
        *(res->block_span->GetOSNAddr(i));
      KUint8(reinterpret_cast<char*>(res->data[ext_idx + 1][vector_idx]->mem) + 1 * i) =
        OperatorTypeOfRecord::OP_TYPE_INSERT;
    }
    setBatchDeleted(reinterpret_cast<char*>(res->data[ext_idx + 2][vector_idx]->bitmap), 1, *count);
  }
  return KStatus::SUCCESS;
}

TsRawDataIteratorImplByOSN::~TsRawDataIteratorImplByOSN() {
}

}  //  namespace kwdbts
