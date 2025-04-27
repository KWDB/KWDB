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
  ret = table_schema_mgr_->GetMetricAttr(attrs_, table_version_);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }

  cur_entity_index_ = 0;
  InitializeMemSegmentIterator();

  TSTableID table_id = table_schema_mgr_->GetTableId();
  auto schema_mgr = vgroup_->GetEngineSchemaMgr();
  uint32_t database_id = schema_mgr->GetDBIDByTableID(table_id);
  auto& partition_managers = vgroup_->GetPartitionManagers();
  auto it = partition_managers.find(database_id);

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

inline KStatus TsStorageIteratorV2Impl::InitializeLastSegmentIterator() {
  if (cur_partition_index_ < ts_partitions_.size()) {
    last_segment_iterator_ = std::make_unique<TsLastSegmentIterator>(vgroup_, ts_partitions_[cur_partition_index_],
                                                                     entity_ids_[cur_entity_index_], ts_spans_,
                                                                     ts_col_type_, kw_scan_cols_, ts_scan_cols_,
                                                                     table_schema_mgr_, table_version_);
    if (last_segment_iterator_ == nullptr) {
      LOG_ERROR("Failed to create TsLastSegmentIterator.");
      return KStatus::FAIL;
    }
    return last_segment_iterator_->Init();
  } else {
    last_segment_iterator_ = nullptr;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsStorageIteratorV2Impl::InitializeBlockSegmentIterator() {
  if (cur_partition_index_ < ts_partitions_.size()) {
    block_segment_iterator_ = std::make_unique<TsBlockSegmentIterator>(vgroup_, ts_partitions_[cur_partition_index_],
                                                                     entity_ids_[cur_entity_index_], ts_spans_,
                                                                     ts_col_type_, kw_scan_cols_, ts_scan_cols_,
                                                                     table_schema_mgr_, table_version_);
    if (block_segment_iterator_ == nullptr) {
      LOG_ERROR("Failed to create TsBlockSegmentIterator.");
      return KStatus::FAIL;
    }
    return block_segment_iterator_->Init();
  } else {
    block_segment_iterator_ = nullptr;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsStorageIteratorV2Impl::InitializeMemSegmentIterator() {
  if (cur_entity_index_ < entity_ids_.size()) {
    mem_segment_iterator_ = std::make_unique<TsMemSegmentIterator>(vgroup_, entity_ids_[cur_entity_index_], ts_spans_,
                                                                    ts_col_type_, kw_scan_cols_, ts_scan_cols_,
                                                                    table_schema_mgr_, table_version_);
    if (mem_segment_iterator_ == nullptr) {
      LOG_ERROR("Failed to create TsMemSegmentIterator.");
      return KStatus::FAIL;
    }
    return mem_segment_iterator_->Init();
  } else {
    mem_segment_iterator_ = nullptr;
  }
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
  status_ = STORAGE_SCAN_STATUS::SCAN_MEM_TABLE;
  return KStatus::SUCCESS;
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
    switch (status_) {
      case STORAGE_SCAN_STATUS::SCAN_MEM_TABLE: {
          // Scan mem tables
          bool is_done = false;
          ret = mem_segment_iterator_->Next(res, count, &is_done);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("Failed to scan mem table for entity(%d).", entity_ids_[cur_entity_index_]);
            return KStatus::FAIL;
          }
          if (is_done) {
            cur_partition_index_ = 0;
            status_ = STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT;
            ret = InitializeLastSegmentIterator();
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("Failed to move to last segment for entity(%d).", entity_ids_[cur_entity_index_]);
              return KStatus::FAIL;
            }
          }
        }
        break;
      case STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT: {
          // Scan last segment
          if (cur_partition_index_ >= ts_partitions_.size()) {
            status_ = STORAGE_SCAN_STATUS::SCAN_BLOCK_SEGMENT;
            cur_partition_index_ = 0;
            ret = InitializeBlockSegmentIterator();
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("Failed to initialize block segment iterator of current partition(%d) for current entity(%d).",
                        cur_partition_index_, entity_ids_[cur_entity_index_]);
              return KStatus::FAIL;
            }
          } else {
            // Scan last segment of current partition
            bool is_done = false;
            ret = last_segment_iterator_->Next(res, count, &is_done);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("Failed to scan last segment of partition(%d).", cur_partition_index_);
              return KStatus::FAIL;
            }
            if (is_done) {
              ++cur_partition_index_;
              ret = InitializeLastSegmentIterator();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to initialize last segment iterator of current partition(%d) for current entity(%d).",
                          cur_partition_index_, entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            }
          }
        }
        break;
      case STORAGE_SCAN_STATUS::SCAN_BLOCK_SEGMENT: {
          // Scan block segment
          if (cur_partition_index_ >= ts_partitions_.size()) {
            ++cur_entity_index_;
            if (cur_entity_index_ >= entity_ids_.size()) {
              status_ = STORAGE_SCAN_STATUS::SCAN_STATUS_DONE;
            } else {
              InitializeMemSegmentIterator();
              status_ = STORAGE_SCAN_STATUS::SCAN_MEM_TABLE;
            }
          } else {
            // Scan block segment of current partition
            bool is_done = false;
            ret = block_segment_iterator_->Next(res, count, &is_done);
            if (ret != KStatus::SUCCESS) {
              LOG_ERROR("Failed to scan block segment of partition(%d).", cur_partition_index_);
              return KStatus::FAIL;
            }
            if (is_done) {
              ++cur_partition_index_;
              ret = InitializeBlockSegmentIterator();
              if (ret != KStatus::SUCCESS) {
                LOG_ERROR("Failed to initialize block segment iterator of current partition(%d) for current entity(%d).",
                          cur_partition_index_, entity_ids_[cur_entity_index_]);
                return KStatus::FAIL;
              }
            }
          }
        }
        break;
      default: {
          // internal error
          return KStatus::FAIL;
        };
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
  if (cur_entity_index_ >= entity_ids_.size()) {
    *count = 0;
    *is_finished = true;
    return KStatus::SUCCESS;
  }

  k_uint64 total_row_count = 0;
  *count = 0;
  KStatus ret;
  ret = InitializeMemSegmentIterator();
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("Failed to initialize mem segment iterator of current partition(%d) for current entity(%d).",
              cur_partition_index_, entity_ids_[cur_entity_index_]);
    return KStatus::FAIL;
  }
  ret = mem_segment_iterator_->ScanCount(count);
  total_row_count += *count;

  for (cur_partition_index_=0; cur_partition_index_ < ts_partitions_.size(); ++cur_partition_index_) {
    ret = InitializeLastSegmentIterator();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize last segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return KStatus::FAIL;
    }
    ret = last_segment_iterator_->ScanCount(count);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to scan last segment for partition(%d) of entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return ret;
    }
    total_row_count += *count;

    // Initialize and scan block segment
    ret = InitializeBlockSegmentIterator();
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to initialize block segment iterator of partition(%d) for entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return ret;
    }

    k_uint32 block_seg_count = 0;
    ret = block_segment_iterator_->ScanCount(&block_seg_count);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("Failed to scan block segment for partition(%d) of entity(%d).",
                cur_partition_index_, entity_ids_[cur_entity_index_]);
      return ret;
    }
    total_row_count += block_seg_count;
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

TsSegmentIterator::TsSegmentIterator(std::shared_ptr<TsVGroup>& vgroup,
                                      std::shared_ptr<TsVGroupPartition> ts_partition,
                                      uint32_t entity_id,
                                      std::vector<KwTsSpan>& ts_spans,
                                      DATATYPE ts_col_type,
                                      std::vector<k_uint32>& kw_scan_cols,
                                      std::vector<k_uint32>& ts_scan_cols,
                                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                      uint32_t table_version) {
  vgroup_ = vgroup;
  ts_partition_ = ts_partition;
  entity_id_ = entity_id;
  ts_spans_ = ts_spans;
  kw_scan_cols_ = kw_scan_cols;
  ts_scan_cols_ = ts_scan_cols;
  table_schema_mgr_ = table_schema_mgr;
  table_version_ = table_version;
}

TsSegmentIterator::~TsSegmentIterator() {
}

KStatus TsSegmentIterator::Init() {
  return table_schema_mgr_->GetMetricAttr(attrs_, table_version_);
}

inline KStatus TsSegmentIterator::AddBlockData(std::shared_ptr<TsBlock> ts_block, ResultSet* res, k_uint32* count) {
  *count = ts_block->GetRowNum();
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
        for (int row_idx = 0; row_idx < ts_block->GetRowNum(); ++row_idx) {
          if (ts_block->IsColNull(row_idx, col_idx, attrs_)) {
            set_null_bitmap(bitmap, row_idx);
          } else {
            ret = ts_block->GetValueSlice(row_idx, col_idx, attrs_, col_data);
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
        for (int row_idx = 0; row_idx < ts_block->GetRowNum(); ++row_idx) {
          if (ts_block->IsColNull(row_idx, col_idx, attrs_)) {
            set_null_bitmap(bitmap, row_idx);
            batch->push_back(nullptr);
          } else {
            ret = ts_block->GetValueSlice(row_idx, col_idx, attrs_, col_data);
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
  res->entity_index = {1, entity_id_, vgroup_->GetVGroupID()};

  return KStatus::SUCCESS;
}

KStatus TsSegmentIterator::AddBlockSpanData(const TsBlockSpan& ts_blk_span, ResultSet* res, k_uint32* count) {
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
  res->entity_index = {1, entity_id_, vgroup_->GetVGroupID()};

  return KStatus::SUCCESS;
}

KStatus TsSegmentIterator::Next(ResultSet* res, k_uint32* count, bool* is_finished) {
  if (ts_block_spans_.empty()) {
    *is_finished = true;
    return KStatus::SUCCESS;
  }
  TsBlockSpan ts_block = ts_block_spans_.front();
  ts_block_spans_.pop_front();
  return AddBlockSpanData(ts_block, res, count);
}

KStatus TsSegmentIterator::ScanCount(k_uint32* count) {
  *count = 0;
  for (const auto& block : ts_block_spans_) {
    *count += block.nrow;
  }

  return KStatus::SUCCESS;
}

TsMemSegmentIterator::TsMemSegmentIterator(std::shared_ptr<TsVGroup>& vgroup,
                                            uint32_t entity_id,
                                            std::vector<KwTsSpan>& ts_spans,
                                            DATATYPE ts_col_type,
                                            std::vector<k_uint32>& kw_scan_cols,
                                            std::vector<k_uint32>& ts_scan_cols,
                                            std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                            uint32_t table_version) :
                  TsSegmentIterator::TsSegmentIterator(vgroup, nullptr, entity_id, ts_spans, ts_col_type,
                                                        kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                        table_version) {
}

TsMemSegmentIterator::~TsMemSegmentIterator() {
}

KStatus TsMemSegmentIterator::Init() {
  KStatus ret = TsSegmentIterator::Init();
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  auto table_id = table_schema_mgr_->GetTableId();
  auto db_id = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  TsBlockItemFilterParams filter{db_id, table_id, entity_id_, ts_spans_};
  return vgroup_->GetMemSegmentMgr()->GetBlockSpans(filter, &ts_block_spans_);
}

TsLastSegmentIterator::TsLastSegmentIterator(std::shared_ptr<TsVGroup>& vgroup,
                                              std::shared_ptr<TsVGroupPartition> ts_partition,
                                              uint32_t entity_id,
                                              std::vector<KwTsSpan>& ts_spans,
                                              DATATYPE ts_col_type,
                                              std::vector<k_uint32>& kw_scan_cols,
                                              std::vector<k_uint32>& ts_scan_cols,
                                              std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                              uint32_t table_version) :
                  TsSegmentIterator::TsSegmentIterator(vgroup, ts_partition, entity_id, ts_spans, ts_col_type,
                                                        kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                        table_version) {
}

TsLastSegmentIterator::~TsLastSegmentIterator() {
}

KStatus TsLastSegmentIterator::Init() {
  KStatus ret = TsSegmentIterator::Init();
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  std::vector<std::shared_ptr<TsLastSegment>> last_segments =
      ts_partition_->GetLastSegmentMgr()->GetAllLastSegments();
  auto table_id = table_schema_mgr_->GetTableId();
  auto db_id = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  TsBlockItemFilterParams filter{db_id, table_id, entity_id_, ts_spans_};
  for (std::shared_ptr<TsLastSegment> last_segment : last_segments) {
    last_segment->GetBlockSpans(filter, &ts_block_spans_);
  }
  return KStatus::SUCCESS;
}

TsBlockSegmentIterator::TsBlockSegmentIterator(std::shared_ptr<TsVGroup>& vgroup,
                                              std::shared_ptr<TsVGroupPartition> ts_partition,
                                              uint32_t entity_id,
                                              std::vector<KwTsSpan>& ts_spans,
                                              DATATYPE ts_col_type,
                                              std::vector<k_uint32>& kw_scan_cols,
                                              std::vector<k_uint32>& ts_scan_cols,
                                              std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                                              uint32_t table_version) :
                  TsSegmentIterator::TsSegmentIterator(vgroup, ts_partition, entity_id, ts_spans, ts_col_type,
                                                        kw_scan_cols, ts_scan_cols, table_schema_mgr,
                                                        table_version) {
}

TsBlockSegmentIterator::~TsBlockSegmentIterator() {
}

KStatus TsBlockSegmentIterator::Init() {
  KStatus ret = TsSegmentIterator::Init();
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  auto table_id = table_schema_mgr_->GetTableId();
  auto db_id = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  TsBlockItemFilterParams filter{db_id, table_id, entity_id_, ts_spans_};
  return ts_partition_->GetBlockSegment()->GetBlockSpans(filter, &ts_block_spans_);
}

}  //  namespace kwdbts
