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

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "ts_vgroup.h"
#include "ts_format.h"
#include "ts_iterator_v2_impl.h"

namespace kwdbts {

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
  // TODO(Yongyan): initialization
  KStatus ret = table_schema_mgr_->GetColumnsIncludeDropped(attrs_, table_version_);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  return KStatus::SUCCESS;
}

KStatus TsStorageIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  // TODO(Yongyan): scan next batch
  return KStatus::FAIL;
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
    return ret;
  }

  cur_entity_index_ = 0;
  status_ = STORAGE_SCAN_STATUS::SCAN_MEM_TABLE;

  mem_table_scanner_ = std::make_unique<TsMemTableScanner>(vgroup_, entity_ids_, ts_spans_, ts_col_type_,
                                                                    kw_scan_cols_, ts_scan_cols_, table_schema_mgr_,
                                                                    table_version_);
  if (mem_table_scanner_ == nullptr) {
    return KStatus::FAIL;
  }
  if (mem_table_scanner_->Init(is_reversed) != KStatus::SUCCESS) {
    mem_table_scanner_ = nullptr;
    return KStatus::FAIL;
  }

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
          ret = mem_table_scanner_->Scan(entity_ids_[cur_entity_index_], res, count, ts);
          if (ret != KStatus::SUCCESS) {
            LOG_ERROR("Failed to scan mem table for entity(%d).", entity_ids_[cur_entity_index_]);
            return KStatus::FAIL;
          }
          status_ = STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT;
          cur_partition_index_ = 0;
        }
        break;
      case STORAGE_SCAN_STATUS::SCAN_LAST_SEGMENT: {
          // Scan last segment
          if (cur_partition_index_ >= ts_partitions_.size()) {
            status_ = STORAGE_SCAN_STATUS::SCAN_BLOCK_SEGMENT;
            cur_partition_index_ = 0;
          } else {
            // Scan last segment of current partition
            ++cur_partition_index_;
          }
        }
        break;
      case STORAGE_SCAN_STATUS::SCAN_BLOCK_SEGMENT: {
          // Scan block segment
          if (cur_partition_index_ >= ts_partitions_.size()) {
            ++cur_entity_index_;
            cur_partition_index_ = 0;
            if (cur_entity_index_ >= entity_ids_.size()) {
              status_ = STORAGE_SCAN_STATUS::SCAN_STATUS_DONE;
            } else {
              status_ = STORAGE_SCAN_STATUS::SCAN_MEM_TABLE;
            }
          } else {
            // Scan block segment of current partition
            ++cur_partition_index_;
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
                                                                            table_version) {
}

TsAggIteratorV2Impl::~TsAggIteratorV2Impl() {
}

KStatus TsAggIteratorV2Impl::Init(bool is_reversed) {
  // TODO(Yongyan): initialization
  return KStatus::FAIL;
}

KStatus TsAggIteratorV2Impl::Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts) {
  // TODO(Yongyan): scan next batch
  return KStatus::FAIL;
}

TsMemTableScanner::TsMemTableScanner(std::shared_ptr<TsVGroup>& vgroup,
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

TsMemTableScanner::~TsMemTableScanner() {
}

KStatus TsMemTableScanner::Init(bool is_reversed) {
  KStatus ret = TsStorageIteratorV2Impl::Init(is_reversed);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
}

KStatus TsMemTableScanner::Scan(uint32_t entity_id, ResultSet* res, k_uint32* count, timestamp64 ts) {
  KStatus ret;
  std::list<std::shared_ptr<TsBlockItemInfo>> blocks;
  ret = vgroup_->GetMemSegmentMgr()->GetBlockItems(0, table_schema_mgr_->GetTableID(),
                                                    entity_id, &blocks);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }

  for (auto block : blocks) {
    *count += block->GetRowNum();
  }
  for (int i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 col_idx = ts_scan_cols_[i];
    Batch* batch;
    if (col_idx >= 0 && col_idx < attrs_.size()) {
      void* bitmap = malloc(KW_BITMAP_SIZE(*count));
      if (bitmap == nullptr) {
        return KStatus::FAIL;
      }
      memset(bitmap, 0xFF, KW_BITMAP_SIZE(*count));
      TSSlice col_data;
      if (!isVarLenType(attrs_[col_idx].type)) {
        char* value = static_cast<char*>(malloc(attrs_[col_idx].size * (*count)));
        int row = 0;
        for (auto block : blocks) {
          for (int i = 0; i < block->GetRowNum(); ++i) {
            ret = block->GetValueSlice(i, col_idx, attrs_, col_data);
            if (ret != KStatus::SUCCESS) {
              return ret;
            }
            memcpy(value + row * attrs_[col_idx].size,
                    col_data.data,
                    attrs_[col_idx].size);
            ++row;
          }
        }
        batch = new Batch(static_cast<void *>(value), *count, bitmap, 0, nullptr);
        batch->is_new = true;
      } else {
        batch = new VarColumnBatch(*count, bitmap, 0, nullptr);
        for (auto block : blocks) {
          for (int i = 0; i < block->GetRowNum(); ++i) {
            ret = block->GetValueSlice(i, col_idx, attrs_, col_data);
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
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      batch = new Batch(bitmap, *count, bitmap, 0, nullptr);
    }
    res->push_back(i, batch);
  }

  res->entity_index = {1, entity_id, vgroup_->GetVGroupID()};
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
