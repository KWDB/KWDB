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
  ret = table_schema_mgr_->GetMetricMeta(table_version_, attrs_);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  table_id_ = table_schema_mgr_->GetTableId();
  db_id_ = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id_);

  cur_entity_index_ = 0;

  auto schema_mgr = vgroup_->GetEngineSchemaMgr();
  auto& partition_managers = vgroup_->GetPartitionManagers();
  auto it = partition_managers.find(db_id_);

  if (it != partition_managers.end() && it->second) {
    auto* partition_manager = it->second.get();
    const auto& partitions = partition_manager->GetPartitions();

    if (!partitions.empty()) {
      std::unordered_set<TsVGroupPartition*> seen;
      for (const auto& [idx, partition_ptr] : partitions) {
        if (!partition_ptr) continue;
        timestamp64 p_start = convertSecondToPrecisionTS(partition_ptr->StartTs(), ts_col_type_);
        timestamp64 p_end = convertSecondToPrecisionTS(partition_ptr->EndTs(), ts_col_type_);
        if (isTimestampInSpans(ts_spans_, p_start, p_end)) {
          if (seen.insert(partition_ptr.get()).second) {
            ts_partitions_.emplace_back(
              std::shared_ptr<TsVGroupPartition>(partition_ptr.get(), [](TsVGroupPartition*) {}));
          }
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

KStatus TsStorageIteratorV2Impl::ConvertBlockSpanToResultSet(const TsBlockSpan& ts_blk_span,
                                                              ResultSet* res, k_uint32* count) {
  *count = ts_blk_span.nrow;
  KStatus ret;
  for (auto col_idx : kw_scan_cols_) {
    Batch* batch;
    if (col_idx >= 0 && col_idx < attrs_.size()) {
      unsigned char* bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(*count)));
      if (bitmap == nullptr) {
        return KStatus::FAIL;
      }
      memset(bitmap, 0x00, KW_BITMAP_SIZE(*count));
      TSSlice col_data;
      if (!isVarLenType(attrs_[col_idx].type)) {
        char* value = static_cast<char*>(malloc(attrs_[col_idx].size * (*count)));
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          if (ts_blk_span.block->IsColNull(ts_blk_span.start_row + row_idx, col_idx, attrs_)) {
            set_null_bitmap(bitmap, row_idx);
          } else {
            ret = ts_blk_span.block->GetValueSlice(ts_blk_span.start_row + row_idx, col_idx, attrs_, col_data);
            if (ret != KStatus::SUCCESS) {
              return ret;
            }
            memcpy(value + row_idx * attrs_[col_idx].size,
                    col_data.data,
                    attrs_[col_idx].size);
          }
        }
        batch = new Batch(static_cast<void *>(value), *count, bitmap, 1, nullptr);
        batch->is_new = true;
        batch->need_free_bitmap = true;
      } else {
        batch = new VarColumnBatch(*count, bitmap, 1, nullptr);
        for (int row_idx = 0; row_idx < *count; ++row_idx) {
          if (ts_blk_span.block->IsColNull(ts_blk_span.start_row + row_idx, col_idx, attrs_)) {
            set_null_bitmap(bitmap, row_idx);
            batch->push_back(nullptr);
          } else {
            ret = ts_blk_span.block->GetValueSlice(ts_blk_span.start_row + row_idx, col_idx, attrs_, col_data);
            if (ret != KStatus::SUCCESS) {
              return ret;
            }
            char* buffer = static_cast<char*>(malloc(col_data.len + 2 + 1));
            KUint16(buffer) = col_data.len;
            memcpy(buffer + 2, col_data.data, col_data.len);
            *(buffer + col_data.len + 2) = 0;
            std::shared_ptr<void> ptr(buffer, free);
            batch->push_back(ptr);
          }
        }
        batch->is_new = true;
        batch->need_free_bitmap = true;
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      batch = new Batch(bitmap, *count, bitmap, 1, nullptr);
    }
    res->push_back(col_idx, batch);
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

KStatus TsRawDataIteratorV2Impl::Init(bool is_reversed) {
  KStatus ret = TsStorageIteratorV2Impl::Init(is_reversed);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  return MoveToMemSegment();
}

KStatus TsRawDataIteratorV2Impl::NextBlockSpan(ResultSet* res, k_uint32* count) {
  if (ts_block_spans_.empty()) {
    return KStatus::FAIL;
  }
  TsBlockSpan ts_block = ts_block_spans_.front();
  ts_block_spans_.pop_front();
  return ConvertBlockSpanToResultSet(ts_block, res, count);
}

KStatus TsRawDataIteratorV2Impl::MoveToMemSegment() {
  status_ = STORAGE_SCAN_STATUS::SCAN_MEM_SEGMENT;
  return AddMemSegmentBlockSpans();
}

KStatus TsRawDataIteratorV2Impl::MoveToLastSegment() {
  status_ = STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT;
  return AddLastSegmentBlockSpans();
}

KStatus TsRawDataIteratorV2Impl::MoveToEntitySegment() {
  status_ = STORAGE_SCAN_STATUS::SCAN_ENTITY_SEGMENT;
  return AddEntitySegmentBlockSpans();
}

KStatus TsRawDataIteratorV2Impl::MoveToNextEntity() {
  ++cur_entity_index_;
  if (cur_entity_index_ >= entity_ids_.size()) {
    // All entities are scanned
    status_ = STORAGE_SCAN_STATUS::SCAN_STATUS_DONE;
    return KStatus::SUCCESS;
  } else {
    // Start from mem segment
    return MoveToMemSegment();
  }
}

KStatus TsRawDataIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  if (cur_entity_index_ >= entity_ids_.size()) {
    *count = 0;
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  *count = 0;
  KStatus ret;
  while (status_ != STORAGE_SCAN_STATUS::SCAN_STATUS_DONE && *count == 0) {
    if (ts_block_spans_.size() > 0) {
      ret = NextBlockSpan(res, count);
      if (ret != KStatus::SUCCESS) {
        LOG_ERROR("Failed to get next block span for entity(%d).", entity_ids_[cur_entity_index_]);
        return KStatus::FAIL;
      }
    } else {
      switch (status_) {
        case STORAGE_SCAN_STATUS::SCAN_MEM_SEGMENT: {
            // Move to scan partition
            cur_partition_index_ = 0;
            if (cur_partition_index_ >= ts_partitions_.size()) {
              // Move to next entity
              ret = MoveToNextEntity();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to move to next entity(%d).", entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            } else {
              ret = MoveToLastSegment();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to move to last segment of current entity(%d).", entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            }
          }
          break;
        case STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT: {
            ret = MoveToEntitySegment();
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("Failed to move to entity segment of current entity(%d).", entity_ids_[cur_entity_index_]);
              return KStatus::FAIL;
            }
          }
          break;
        case STORAGE_SCAN_STATUS::SCAN_ENTITY_SEGMENT: {
            if (cur_partition_index_ >= ts_partitions_.size()) {
              // Move to next entity
              ret = MoveToNextEntity();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to move to next entity(%d).", entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            } else {
              // Move to next partition
              ++cur_partition_index_;
              // Start from last segment
              ret = MoveToLastSegment();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to move to last segment of current entity(%d).", entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            }
          }
          break;
        default: {
            // internal error
            LOG_ERROR("Internal error: Unknown status: %d", status_);
            return KStatus::FAIL;
          };
      }
    }
  }
  *is_finished = (status_ == STORAGE_SCAN_STATUS::SCAN_STATUS_DONE);
  return KStatus::SUCCESS;
}

TsSortedRowDataIteratorV2Impl::TsSortedRowDataIteratorV2Impl(std::shared_ptr<TsVGroup>& vgroup,
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

TsSortedRowDataIteratorV2Impl::~TsSortedRowDataIteratorV2Impl() {
}

KStatus TsSortedRowDataIteratorV2Impl::Init(bool is_reversed) {
  // TODO(Yongyan): initialization
  return KStatus::FAIL;
}

KStatus TsSortedRowDataIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  // TODO(Yongyan): scan next batch
  return KStatus::FAIL;
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

  k_uint64 total_row_count = 0;
  for (const auto& block : ts_block_spans_) {
    total_row_count += block.nrow;
  }

  if (total_row_count > 0) {
    res->clear();
    for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
      switch (scan_agg_types_[i]) {
        case Sumfunctype::COUNT: {
          char* value = static_cast<char*>(malloc(sizeof(k_uint64)));
          *reinterpret_cast<k_uint64*>(value) = total_row_count;

          unsigned char* bitmap = static_cast<unsigned char*>(malloc(KW_BITMAP_SIZE(1)));
          memset(bitmap, 0x00, KW_BITMAP_SIZE(1));

          Batch* b = new Batch(value, 1, bitmap, 1, nullptr);
          b->is_new = true;
          b->need_free_bitmap = true;
          res->push_back(i, b);
          break;
        }
        default: {
          LOG_ERROR("Unsupported aggregation type: %d", static_cast<int>(scan_agg_types_[i]));
          return KStatus::FAIL;
        }
      }
    }

    res->entity_index = {1, entity_ids_[cur_entity_index_], vgroup_->GetVGroupID()};
    res->col_num_ = kw_scan_cols_.size();
    *count = 1;
    *is_finished = false;
    total_row_count = 0;
    ++cur_entity_index_;
    return KStatus::SUCCESS;
  }

  *is_finished = true;
  ++cur_entity_index_;
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
