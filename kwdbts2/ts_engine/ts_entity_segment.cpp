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

#include "ts_entity_segment.h"

#include <cstdint>
#include <utility>
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_agg.h"
#include "ts_bitmap.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_compressor.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lastsegment_builder.h"
#include "ts_sliceguard.h"
#include "ts_version.h"
#include "ts_lru_block_cache.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {

KStatus TsEntitySegmentEntityItemFile::Open() {
  if (io_env_->NewRandomReadFile(file_path_, &r_file_) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentEntityItemFile NewRandomReadFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (r_file_->GetFileSize() < sizeof(TsEntityItemFileHeader)) {
    LOG_ERROR("TsEntitySegmentEntityItemFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  KStatus s = r_file_->Read(r_file_->GetFileSize() - sizeof(TsEntityItemFileHeader), sizeof(TsEntityItemFileHeader),
                            &header_guard_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentEntityItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = reinterpret_cast<TsEntityItemFileHeader*>(header_guard_.data());
  if (header_->status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentEntityItemFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}
KStatus TsEntitySegmentEntityItemFile::SetEntityItemDropped(uint64_t entity_id) {
  // todo(liangbo01) readonly_file ,cannot modify.
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentEntityItemFile::GetEntityItem(uint64_t entity_id, TsEntityItem& entity_item,
                                                     bool& is_exist) {
  if (entity_id > header_->entity_num) {
    is_exist = false;
    return KStatus::FAIL;
  }
  is_exist = true;
  assert(entity_id * sizeof(TsEntityItem) <= r_file_->GetFileSize() - sizeof(TsEntityItemFileHeader));
  TsSliceGuard result;
  KStatus s = r_file_->Read((entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                            &result);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("read entity item[id=%lu] failed.", entity_id);
    return s;
  }
  if (is_exist) {
    entity_item = *(reinterpret_cast<TsEntityItem *>(result.data()));
  }
  if (entity_item.table_id == 0) {
    is_exist = false;
    return SUCCESS;
  }
  return KStatus::SUCCESS;
}

uint32_t TsEntitySegmentEntityItemFile::GetFileNum() {
  return std::atoi(file_path_.data() + file_path_.size() - 12);
}

uint64_t TsEntitySegmentEntityItemFile::GetEntityNum() {
  return header_->entity_num;
}

bool TsEntitySegmentEntityItemFile::IsReady() {
  return header_->status == TsFileStatus::READY;
}

KStatus TsEntitySegmentBlockItemFile::Open() {
  if (io_env_->NewRandomReadFile(file_path_, &r_file_, file_size_) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile NewRandomReadFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (r_file_->GetFileSize() < sizeof(TsBlockItemFileHeader)) {
    LOG_ERROR("TsEntitySegmentBlockItemFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  KStatus s = r_file_->Read(0, sizeof(TsBlockItemFileHeader), &header_guard_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = reinterpret_cast<TsBlockItemFileHeader*>(header_guard_.data());
  if (header_->status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentBlockItemFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}

KStatus TsEntitySegmentBlockItemFile::GetBlockItem(uint64_t blk_id, TsEntitySegmentBlockItem** blk_item,
                                                   TsSliceGuard* blk_item_guard, TsScanStats* ts_scan_stats) {
  KStatus s = r_file_->Read(sizeof(TsBlockItemFileHeader) + (blk_id - 1) * sizeof(TsEntitySegmentBlockItem),
                            sizeof(TsEntitySegmentBlockItem), blk_item_guard);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  *blk_item = reinterpret_cast<TsEntitySegmentBlockItem*>(blk_item_guard->data());
  if ((*blk_item)->block_version == INVALID_BLOCK_VERSION) {
    LOG_ERROR("TsEntitySegmentBlockItemFile block version is invalid, file_path=%s, block_id=%lu", file_path_.c_str(),
              blk_id);
    return FAIL;
  }
  if (ts_scan_stats) {
    ts_scan_stats->header_bytes += blk_item_guard->size();
  }
  return KStatus::SUCCESS;
}

TsEntitySegmentMetaManager::TsEntitySegmentMetaManager(TsIOEnv* env, const string& dir_path, EntitySegmentMetaInfo info)
    : dir_path_(dir_path),
      entity_header_(env, dir_path_ / EntityHeaderFileName(info.header_e_file_number)),
      block_header_(env, dir_path_ / BlockHeaderFileName(info.header_b_info.file_number), info.header_b_info.length) {}

KStatus TsEntitySegmentMetaManager::Open() {
  // Attempt to access the directory
  if (access(dir_path_.c_str(), F_OK)) {
    LOG_ERROR("cannot open directory [%s].", dir_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = entity_header_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_header_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentMetaManager::GetAllBlockItems(TSEntityID entity_id,
                                                    std::vector<TsEntitySegmentBlockItemWithData>* blk_items) {
  TsEntityItem entity_item{entity_id};
  bool is_exist;
  KStatus s = entity_header_.GetEntityItem(entity_id, entity_item, is_exist);
  if (s != KStatus::SUCCESS && is_exist) {
    return s;
  }
  uint64_t last_blk_id = entity_item.cur_block_id;

  while (last_blk_id > 0) {
    TsEntitySegmentBlockItemWithData block_item_data;
    s = block_header_.GetBlockItem(last_blk_id, &block_item_data.block_item, &block_item_data.data);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    last_blk_id = block_item_data.block_item->prev_block_id;
    blk_items->push_back(std::move(block_item_data));
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentMetaManager::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                                  std::shared_ptr<TsEntitySegment> entity_segment,
                                                  std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                                  std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                                  std::shared_ptr<MMapMetricsTable>& scan_schema,
                                                  TsScanStats* ts_scan_stats) {
  TsEntityItem entity_item{filter.entity_id};
  bool is_exist;
  KStatus s = entity_header_.GetEntityItem(filter.entity_id, entity_item, is_exist);
  if (s != KStatus::SUCCESS && is_exist) {
    return s;
  }
  if (filter.table_id != entity_item.table_id) {
    return SUCCESS;
  }
  uint64_t last_blk_id = entity_item.cur_block_id;

  TsEntitySegmentBlockItem* cur_blk_item;
  TsSliceGuard block_item_guard;
  TsBlockSpan* template_blk_span = nullptr;
  while (last_blk_id > 0) {
    s = block_header_.GetBlockItem(last_blk_id, &cur_blk_item, &block_item_guard, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get block item failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
      return s;
    }

    if (IsTsLsnSpanInSpans(filter.spans_, {cur_blk_item->min_ts, cur_blk_item->max_ts},
                          {cur_blk_item->min_osn, cur_blk_item->max_osn})) {
      std::shared_ptr<TsEntityBlock> block = nullptr;
      if (ts_scan_stats) {
        ++ts_scan_stats->entity_block_count;
      }
      if (EngineOptions::block_cache_max_size > 0) {
         block = entity_segment->GetEntityBlock(cur_blk_item->block_id);
        if (block == nullptr) {
          block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, entity_segment);
          entity_segment->AddEntityBlock(cur_blk_item->block_id, block);
          TsLRUBlockCache::GetInstance().Add(block);
        } else {
          TsLRUBlockCache::GetInstance().Access(block);
          if (ts_scan_stats) {
            ++ts_scan_stats->block_cache_hit_count;
          }
        }
      } else {
        block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, entity_segment);
      }
      std::shared_ptr<TsBlockSpan> cur_blk_span;
      s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, block, 0, block->GetRowNum(),
        scan_schema->GetVersion(), scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_blk_span);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("MakeNewBlockSpan failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
        return s;
      }
      template_blk_span = cur_blk_span.get();
      block_spans.push_front(std::move(cur_blk_span));
    } else if (IsTsLsnSpanCrossSpans(filter.spans_, {cur_blk_item->min_ts, cur_blk_item->max_ts},
                              {cur_blk_item->min_osn, cur_blk_item->max_osn})) {
      std::shared_ptr<TsEntityBlock> block = nullptr;
      if (ts_scan_stats) {
        ++ts_scan_stats->entity_block_count;
      }
      if (EngineOptions::block_cache_max_size > 0) {
        block = entity_segment->GetEntityBlock(cur_blk_item->block_id);
        if (block == nullptr) {
          block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, entity_segment);
          entity_segment->AddEntityBlock(cur_blk_item->block_id, block);
          TsLRUBlockCache::GetInstance().Add(block);
        } else {
          TsLRUBlockCache::GetInstance().Access(block);
          if (ts_scan_stats) {
            ++ts_scan_stats->block_cache_hit_count;
          }
        }
      } else {
        block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, entity_segment);
      }
      // std::vector<std::pair<start_row, row_num>>
      std::vector<std::pair<int, int>> row_spans;
      s = block->GetRowSpans(filter.spans_, row_spans, ts_scan_stats);
      if (s != KStatus::SUCCESS) {
        return s;
      }
      for (int i = row_spans.size() - 1; i >= 0; --i) {
        if (row_spans[i].second <= 0) {
          continue;
        }
        std::shared_ptr<TsBlockSpan> cur_blk_span;
        s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, block,
          row_spans[i].first, row_spans[i].second,
          scan_schema->GetVersion(), scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_blk_span);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("MakeNewBlockSpan failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
          return s;
        }
        template_blk_span = cur_blk_span.get();
        block_spans.push_front(std::move(cur_blk_span));
        // Because block item traverses from back to front, use push_front
      }
    }
    last_blk_id = cur_blk_item->prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsEntityBlock::TsEntityBlock(uint32_t table_id, TsEntitySegmentBlockItem* block_item,
                             std::shared_ptr<TsEntitySegment>& block_segment)
    : rw_latch_(RWLATCH_ID_ENTITY_BLOCK_RWLOCK) {
  table_id_ = table_id;
  table_version_ = block_item->table_version;
  entity_id_ = block_item->entity_id;
  n_rows_ = block_item->n_rows;
  n_cols_ = block_item->n_cols;
  first_ts_ = block_item->min_ts;
  last_ts_ = block_item->max_ts;
  first_osn_ = block_item->first_osn;
  last_osn_ = block_item->last_osn;
  min_osn_ = block_item->min_osn;
  max_osn_ = block_item->max_osn;
  block_offset_ = block_item->block_offset;
  block_length_ = block_item->block_len;
  agg_offset_ = block_item->agg_offset;
  agg_length_ = block_item->agg_len;
  block_id_ = block_item->block_id;
  entity_segment_ = block_segment;
  block_version_ = block_item->block_version;
  // reserve two columns for timestamp and OSN
  column_blocks_.resize(n_cols_);
}

char* TsEntityBlock::GetMetricColAddr(uint32_t col_idx) {
  assert(col_idx < column_blocks_.size() - 1);
  return column_blocks_[col_idx + 1]->buffer.data();
}

KStatus TsEntityBlock::GetMetricColValue(uint32_t row_idx, uint32_t col_idx, TSSlice& value) {
  assert(col_idx < column_blocks_.size() - 1);
  assert(row_idx < n_rows_);

  if (metric_schema_ != nullptr && isVarLenType((*metric_schema_)[col_idx].type)) {
    char* ptr = column_blocks_[col_idx + 1]->buffer.data();
    uint32_t offset = 0;
    if (row_idx != 0) {
      offset = *reinterpret_cast<uint32_t *>(ptr + (row_idx - 1) * sizeof(uint32_t));
    }
    uint32_t next_row_offset = *reinterpret_cast<uint32_t*>(ptr + row_idx * sizeof(uint32_t));
    uint32_t var_offsets_len = n_rows_ * sizeof(uint32_t);
    value.data = column_blocks_[col_idx + 1]->buffer.data() + var_offsets_len + offset;
    value.len = next_row_offset - offset;
  } else {
    size_t d_size = col_idx == 0 ? 8 : static_cast<DATATYPE>((*metric_schema_)[col_idx].size);
    value.data = column_blocks_[col_idx + 1]->buffer.data() + row_idx * d_size;
    value.len = d_size;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadColData(int32_t col_idx, const std::vector<AttributeInfo>* metric_schema,
                                   TsSliceGuard&& data) {
  bool is_var_type = col_idx > 0 && isVarLenType((*metric_schema)[col_idx].type);
  bool is_not_null = col_idx <= 0 || (*metric_schema)[col_idx].isFlag(AINFO_NOT_NULL);
  const auto& mgr = CompressorManager::GetInstance();
  if (metric_schema_ == nullptr) {
    metric_schema_ = metric_schema;
  }

  size_t bitmap_len = 0;
  if (column_blocks_[col_idx + 1] == nullptr) {
    column_blocks_[col_idx + 1] = std::make_shared<TsEntitySegmentColumnBlock>();
  }
#ifdef WITH_TESTS
  if (col_idx == -1 && TsLRUBlockCache::GetInstance().unit_test_enabled) {
    // Initializing osn column block, timestamp column block has been initialized at this point.
    if (TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_NONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_FIRST_INITIALIZING;
      while (TsLRUBlockCache::GetInstance().unit_test_phase != TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_SECOND_ACCESS_DONE
            && (TsLRUBlockCache::GetInstance().unit_test_phase !=
                TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_SECOND_GOING_TO_INITIALIZE)) {
        usleep(1000);
      }
    }
  }
#endif
  if (block_version_ < 1) {
    if (col_idx >= 1) {
      bitmap_len = TsBitmap::GetBitmapLen(n_rows_);
      column_blocks_[col_idx + 1]->bitmap = std::make_unique<TsBitmap>(data.SubSlice(0, bitmap_len), n_rows_);
    } else if (col_idx == 0) {
      // Timestamp Column Assign Default Value kValid
      column_blocks_[col_idx + 1]->bitmap = std::make_unique<TsUniformBitmap<kValid>>(n_rows_);
    }
  } else if (block_version_ >= 1 && block_version_ < BLOCK_VERSION_LIMIT) {
    bool has_bitmap = col_idx >= 1;  //&& !(*metric_schema_)[col_idx].isFlag(AINFO_NOT_NULL);
    if (has_bitmap) {
      std::unique_ptr<TsBitmapBase> bitmap;
      TSSlice bitmap_data = data.AsSlice();
      bool ok = mgr.DecompressBitmap(bitmap_data, &bitmap, n_rows_, &bitmap_len);
      if (!ok) {
        LOG_ERROR("block segment column[%u] bitmap decompress failed", col_idx + 1);
        return KStatus::FAIL;
      }
      column_blocks_[col_idx + 1]->bitmap = std::move(bitmap);
    } else {
      column_blocks_[col_idx + 1]->bitmap = std::make_unique<TsUniformBitmap<kValid>>(n_rows_);
    }
  } else {
    LOG_ERROR("unexpected block version %u", block_version_);
    return FAIL;
  }
  data.RemovePrefix(bitmap_len);
  if (!is_var_type) {
    TsSliceGuard plain;
    TsBitmapBase* bitmap = is_not_null ? nullptr : column_blocks_[col_idx + 1]->bitmap.get();
    bool ok = mgr.DecompressData(std::move(data), bitmap, n_rows_, &plain);
    if (!ok) {
      LOG_ERROR("block segment column[%u] data decompress failed, entity segment is [%s], handle info %s", col_idx + 1,
                GetEntitySegmentPath().c_str(), GetHandleInfoStr().c_str());
      return KStatus::FAIL;
    }
    // save decompressed col block data
    column_blocks_[col_idx + 1]->buffer = std::move(plain);
  } else {
    uint32_t var_offsets_len = *reinterpret_cast<uint32_t*>(data.data());
    data.RemovePrefix(sizeof(uint32_t));
    TsSliceGuard compressed_var_offsets = data.SubSliceGuard(0, var_offsets_len);
    TsSliceGuard var_offsets;
    bool ok = mgr.DecompressData(std::move(compressed_var_offsets), nullptr, n_rows_, &var_offsets);
    if (!ok) {
      LOG_ERROR("Decompress var offsets failed");
      return KStatus::FAIL;
    }
    assert(var_offsets.size() == n_rows_ * sizeof(uint32_t));
    TsBufferBuilder var_buffer(var_offsets);
#ifdef WITH_TESTS
  if (TsLRUBlockCache::GetInstance().unit_test_enabled &&
      TsLRUBlockCache::GetInstance().unit_test_phase ==
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_NONE) {
    TsLRUBlockCache::GetInstance().unit_test_phase =
      TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_ONE_DONE;
    while (TsLRUBlockCache::GetInstance().unit_test_phase !=
            TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_GET_VAR_COL_ADDR_DONE
          && TsLRUBlockCache::GetInstance().unit_test_phase !=
            TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_TRY_GETTING_VAR_COLUMN_BLOCK) {
      usleep(1000);
    }
  }
#endif
    data.RemovePrefix(var_offsets_len);
    TsSliceGuard var_data;
    ok = mgr.DecompressVarchar(std::move(data), &var_data);
    if (!ok) {
      LOG_ERROR("Decompress varchar failed");
      return KStatus::FAIL;
    }
    var_buffer.append(var_data.AsStringView());
    column_blocks_[col_idx + 1]->buffer = var_buffer.GetBuffer();
#ifdef WITH_TESTS
  if (TsLRUBlockCache::GetInstance().unit_test_enabled) {
    if (TsLRUBlockCache::GetInstance().unit_test_phase ==
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_GET_VAR_COL_ADDR_DONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase =
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE;
      while (TsLRUBlockCache::GetInstance().unit_test_phase !=
              TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE) {
        usleep(1000);
      }
    } else if (TsLRUBlockCache::GetInstance().unit_test_phase ==
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_TRY_GETTING_VAR_COLUMN_BLOCK) {
      TsLRUBlockCache::GetInstance().unit_test_phase =
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_TWO_DONE;
    }
  }
#endif
    assert(*reinterpret_cast<uint32_t*>(var_offsets.data() + var_offsets.size() - sizeof(uint32_t)) == var_data.size());
  }
  TsLRUBlockCache::GetInstance().AddMemory(this, bitmap_len + column_blocks_[col_idx + 1]->buffer.size());
#ifdef WITH_TESTS
  if (TsLRUBlockCache::GetInstance().unit_test_enabled &&
      TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_NONE) {
    TsLRUBlockCache::GetInstance().unit_test_phase =
      TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_FIRST_INITIALIZING;
    while (TsLRUBlockCache::GetInstance().unit_test_phase !=
            TsLRUBlockCache::UNIT_TEST_PHASE::COLUMN_BLOCK_CRASH_PHASE_SECOND_ACCESS_DONE) {
      usleep(1000);
    }
  }
#endif
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadAggData(int32_t col_idx, TsSliceGuard&& buffer) {
  if (column_blocks_[col_idx + 1] == nullptr) {
    column_blocks_[col_idx + 1] = std::make_shared<TsEntitySegmentColumnBlock>();
  }
  size_t buffer_len = buffer.size();
  if (buffer_len > 0) {
    column_blocks_[col_idx + 1]->agg = std::move(buffer);
    TsLRUBlockCache::GetInstance().AddMemory(this, buffer_len);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadBlockInfo(TsSliceGuard&& buffer) {
  block_info_.col_block_offset = std::move(buffer);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadAggInfo(TsSliceGuard&& buffer) {
  block_info_.col_agg_offset = std::move(buffer);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetRowSpans(const std::vector<STScanRange>& spans,
                      std::vector<std::pair<int, int>>& row_spans, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(0)) {
    KStatus s = entity_segment_->GetColumnBlock(0, {}, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[ts] data load failed");
      return s;
    }
  }

  if (!HasDataCached(-1)) {
#ifdef WITH_TESTS
    if (TsLRUBlockCache::GetInstance().unit_test_enabled) {
      if (TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_FIRST_INITIALIZING) {
        TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_SECOND_GOING_TO_INITIALIZE;
      }
    }
#endif
    KStatus s = entity_segment_->GetColumnBlock(-1, {}, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[osn] data load failed");
      return s;
    }
  }
  const timestamp64* ts_col = reinterpret_cast<const timestamp64*>(column_blocks_[1]->buffer.data());
  const TS_OSN* osn_col = reinterpret_cast<const TS_OSN*>(column_blocks_[0]->buffer.data());
  assert(n_rows_ * 8 == column_blocks_[1]->buffer.size());
  assert(n_rows_ * 8 == column_blocks_[0]->buffer.size());

  for (const auto& span : spans) {
    if (!IsTsLsnSpanCrossSpans({span}, {first_ts_, last_ts_}, {min_osn_, max_osn_})) {
      continue;
    }
    // binary search to find the start and end index of the time range
    auto ts_start = std::lower_bound(ts_col, ts_col + n_rows_, span.ts_span.begin);
    auto ts_end = std::upper_bound(ts_col, ts_col + n_rows_, span.ts_span.end);
    // osn_span filter
    int start_idx = ts_start - ts_col;
    int end_idx = ts_end - ts_col;
    bool match_found = false;
    int span_start = 0;
    for (int i = start_idx; i < end_idx; i++) {
      if (osn_col[i] >= span.osn_span.begin && osn_col[i] <= span.osn_span.end) {
        if (!match_found) {
          span_start = i;
          match_found = true;
        }
      } else {
        if (match_found) {
          match_found = false;
          row_spans.push_back({span_start, i - span_start});
        }
      }
    }
    if (match_found) {
      row_spans.push_back({span_start, end_idx - span_start});
    }
  }
#ifdef WITH_TESTS
  if (TsLRUBlockCache::GetInstance().unit_test_enabled) {
    if (TsLRUBlockCache::GetInstance().unit_test_phase == TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_FIRST_INITIALIZING) {
      TsLRUBlockCache::GetInstance().unit_test_phase = TsLRUBlockCache::UNIT_TEST_PHASE::PHASE_SECOND_ACCESS_DONE;
    }
  }
#endif
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                                  char** value, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  *value = GetMetricColAddr(col_id);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                                    std::unique_ptr<TsBitmapBase>* bitmap, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }

  *bitmap = column_blocks_[col_id + 1]->bitmap->AsView();
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                                           TSSlice& value, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  return GetMetricColValue(row_num, col_id, value);
}

bool TsEntityBlock::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                              TsScanStats* ts_scan_stats) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  // assert(col_id < column_blocks_.size() - 1);
  assert(row_num < n_rows_);
  return column_blocks_[col_id + 1]->bitmap->At(row_num) == DataFlags::kNull;
}

timestamp64 TsEntityBlock::GetTS(int row_num, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(0)) {
    KStatus s = entity_segment_->GetColumnBlock(0, {}, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      // Should not return s as timestamp64, we need to refactor the code later
      return s;
    }
  }
  return *reinterpret_cast<const timestamp64*>(column_blocks_[1]->buffer.data() + row_num * sizeof(timestamp64));
}

inline timestamp64 TsEntityBlock::GetFirstTS() {
  return first_ts_;
}

inline timestamp64 TsEntityBlock::GetLastTS() {
  return last_ts_;
}

inline void TsEntityBlock::GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) {
  min_osn = min_osn_;
  max_osn = max_osn_;
}

inline uint64_t TsEntityBlock::GetFirstOSN() {
  return first_osn_;
}

inline uint64_t TsEntityBlock::GetLastOSN() {
  return last_osn_;
}

inline const uint64_t* TsEntityBlock::GetOSNAddr(int row_num, TsScanStats* ts_scan_stats) {
  if (!HasDataCached(-1)) {
    KStatus s = entity_segment_->GetColumnBlock(-1, {}, this, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[osn] data load failed");
      return nullptr;
    }
  }
  return reinterpret_cast<const uint64_t*>(column_blocks_[0]->buffer.data() + row_num * sizeof(uint64_t));
}

KStatus TsEntityBlock::GetCompressDataFromFile(uint32_t table_version, int32_t nrow, TsBufferBuilder* data) {
  if (nrow != n_rows_ || table_version != table_version_) {
    return KStatus::FAIL;
  }
  TsSliceGuard tmp_data;
  KStatus s = entity_segment_->GetBlockData(this, &tmp_data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetBlockData failed");
    return s;
  }
  data->append(tmp_data);
  s = entity_segment_->GetAggData(this, &tmp_data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAggData failed");
    return s;
  }
  data->append(tmp_data);
  return KStatus::SUCCESS;
}

inline bool TsEntityBlock::HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) {
  return 0 == begin_row_idx && row_num == n_rows_;
}

inline KStatus TsEntityBlock::GetPreCount(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, uint16_t& count) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    count = 0;
  } else {
    count = *reinterpret_cast<const uint16_t*>(col_blk->agg.data());
  }
  return KStatus::SUCCESS;
}

inline KStatus TsEntityBlock::GetPreSum(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats,
                                        void* &pre_sum, bool& is_overflow) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    return KStatus::SUCCESS;
  }
  char* pre_agg = col_blk->agg.data();
  is_overflow = *reinterpret_cast<bool*>(pre_agg + sizeof(uint16_t) + size * 2);
  pre_sum = pre_agg + sizeof(uint16_t) + size * 2 + 1;
  return KStatus::SUCCESS;
}

inline KStatus TsEntityBlock::GetPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, void* &pre_max) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    return KStatus::SUCCESS;
  }
  pre_max = static_cast<void*>(col_blk->agg.data() + sizeof(uint16_t));

  return KStatus::SUCCESS;
}

inline KStatus TsEntityBlock::GetPreMin(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats,
                                        void* &pre_min) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    return KStatus::SUCCESS;
  }
  if (blk_col_idx == 0) {
    size = 8;
  }
  pre_min = static_cast<void*>(col_blk->agg.data() + sizeof(uint16_t) + size);

  return KStatus::SUCCESS;
}

inline KStatus TsEntityBlock::GetVarPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_max) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    return KStatus::SUCCESS;
  }
  const char* pre_agg = col_blk->agg.data();
  auto len = *reinterpret_cast<const uint32_t*>(pre_agg + sizeof(uint16_t));
  pre_max = col_blk->agg.SubSlice(sizeof(uint16_t) + sizeof(uint32_t) * 2, len);
  return KStatus::SUCCESS;
}

inline KStatus TsEntityBlock::GetVarPreMin(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_min) {
  if (!HasAggData(blk_col_idx)) {
    auto s = entity_segment_->GetColumnAgg(blk_col_idx, this, ts_scan_stats);
    if (s != SUCCESS) {
      return s;
    }
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk->agg.empty()) {
    return KStatus::SUCCESS;
  }
  const char* pre_agg = col_blk->agg.data();
  uint32_t max_len = *reinterpret_cast<const uint32_t*>(pre_agg + sizeof(uint16_t));
  auto len = *reinterpret_cast<const uint32_t*>(pre_agg + sizeof(uint16_t) + sizeof(uint32_t));
  pre_min = col_blk->agg.SubSlice(sizeof(uint16_t) + sizeof(uint32_t) * 2 + max_len, len);
  return KStatus::SUCCESS;
}

std::string TsEntityBlock::GetEntitySegmentPath() {
  return entity_segment_->GetPath();
}

std::string TsEntityBlock::GetHandleInfoStr() {
  return entity_segment_->GetHandleInfoStr();
}

void TsEntityBlock::RemoveFromSegment() {
  entity_segment_->RemoveEntityBlock(block_id_);
}

TsEntitySegment::TsEntitySegment(EntitySegmentIOEnvSet env_set, const fs::path& root, EntitySegmentMetaInfo info)
    : dir_path_(root),
      meta_mgr_(env_set.header_io_env, root, info),
      block_file_(env_set.block_agg_io_env, root, info),
      agg_file_(env_set.block_agg_io_env, root, info),
      info_(info),
      entity_blocks_rw_latch_(RWLATCH_ID_ENTITY_BLOCKS_RWLOCK) {
  KStatus s = Open();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Open entity segment failed");
    return;
  }
  entity_blocks_.resize(meta_mgr_.GetBlockNum());
}

TsEntitySegment::TsEntitySegment(uint32_t max_blocks) : entity_blocks_rw_latch_(RWLATCH_ID_ENTITY_BLOCKS_RWLOCK) {
  entity_blocks_.resize(max_blocks);
}

KStatus TsEntitySegment::Open() {
  KStatus s = meta_mgr_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_file_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = agg_file_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

inline KStatus TsEntitySegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                        std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                        std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                        std::shared_ptr<MMapMetricsTable>& scan_schema,
                                        TsScanStats* ts_scan_stats) {
  if (filter.entity_id > meta_mgr_.GetEntityNum()) {
    // LOG_WARN("entity id [%lu] > entity number [%lu]", filter.entity_id, meta_mgr_.GetEntityNum());
    return KStatus::SUCCESS;
  }
  return meta_mgr_.GetBlockSpans(filter, shared_from_this(), block_spans, tbl_schema_mgr, scan_schema, ts_scan_stats);
}

inline KStatus TsEntitySegment::GetBlockData(TsEntityBlock* block, TsSliceGuard* data) {
  // get compressed data
  KStatus s = block_file_.ReadData(block->GetBlockOffset(), data, block->GetBlockLength());
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegment::GetBlockData read block data[offset=%lu, length=%d] failed",
              block->GetBlockOffset(), block->GetBlockLength());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegment::GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>* metric_schema,
                                       TsEntityBlock* block, TsScanStats* ts_scan_stats) {
#ifdef WITH_TESTS
  if (TsLRUBlockCache::GetInstance().unit_test_enabled) {
    if (TsLRUBlockCache::GetInstance().unit_test_phase ==
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_FIRST_APPEND_ONE_DONE) {
      TsLRUBlockCache::GetInstance().unit_test_phase =
        TsLRUBlockCache::UNIT_TEST_PHASE::VAR_COLUMN_BLOCK_CRASH_PHASE_SECOND_TRY_GETTING_VAR_COLUMN_BLOCK;
    }
  }
#endif
  block->WrLock();
  Defer defer([&]() { block->Unlock(); });
  // init block info
  if (block->GetBlockInfo().col_block_offset.empty()) {
    TsSliceGuard buffer;
    KStatus s = block_file_.ReadData(block->GetBlockOffset(), &buffer, sizeof(uint32_t) * block->GetNCols());
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read block info data failed")
      return s;
    }
    size_t buffer_size = buffer.size();
    s = block->LoadBlockInfo(std::move(buffer));
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock block info init failed")
      return s;
    }
     if (ts_scan_stats) {
      ts_scan_stats->block_bytes += buffer_size;
    }
  }
  // init column block
  if (!block->HasDataCachedNoLock(col_idx)) {
    uint32_t col_offsets_len = sizeof(uint32_t) * block->GetNCols();
    uint32_t start_offset = 0;
    const TsEntitySegmentBlockInfo& block_info = block->GetBlockInfo();
    if (col_idx != -1) {
      start_offset = reinterpret_cast<const uint32_t*>(block_info.col_block_offset.data())[col_idx];
    }
    uint32_t end_offset = reinterpret_cast<const uint32_t*>(block_info.col_block_offset.data())[col_idx + 1];
    TsSliceGuard buffer;
    KStatus s = block_file_.ReadData(block->GetBlockOffset() + col_offsets_len + start_offset,
                                     &buffer, end_offset - start_offset);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }

    if (ts_scan_stats) {
      ts_scan_stats->block_bytes += buffer.size();
    }

    s = block->LoadColData(col_idx, metric_schema, std::move(buffer));
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock column[%u] block init failed", col_idx + 1);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

inline KStatus TsEntitySegment::GetAggData(TsEntityBlock* block, TsSliceGuard* data) {
  // get agg data
  KStatus s = agg_file_.ReadAggData(block->GetAggOffset(), data, block->GetAggLength());
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegment::GetAggData read agg block[offset=%lu, length=%d] data failed",
              block->GetAggOffset(), block->GetAggLength());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegment::GetColumnAgg(int32_t col_idx, TsEntityBlock *block, TsScanStats* ts_scan_stats) {
  block->WrLock();
  Defer defer([&]() { block->Unlock(); });
  if (block->GetBlockInfo().col_agg_offset.empty()) {
    TsSliceGuard agg_offsets;
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset(), &agg_offsets, sizeof(uint32_t) * (block->GetNCols() - 1));
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read agg data failed")
      return s;
    }
    size_t agg_offsets_size = agg_offsets.size();
    s = block->LoadAggInfo(std::move(agg_offsets));
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock agg info init failed")
      return s;
    }
    if (ts_scan_stats) {
      ts_scan_stats->agg_bytes += agg_offsets_size;
    }
  }
  if (!block->HasAggDataNoLock(col_idx)) {
    uint32_t agg_offsets_len = sizeof(uint32_t) * (block->GetNCols() - 1);
    uint32_t start_offset = 0;
    if (col_idx != 0) {
      start_offset = reinterpret_cast<const uint32_t*>(block->GetBlockInfo().col_agg_offset.data())[col_idx - 1];
    }
    uint32_t end_offset = reinterpret_cast<const uint32_t*>(block->GetBlockInfo().col_agg_offset.data())[col_idx];
    TsSliceGuard col_agg_buffer;
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset() + agg_offsets_len + start_offset,
                                      &col_agg_buffer, end_offset - start_offset);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }
    size_t col_agg_buffer_size = col_agg_buffer.size();
    block->LoadAggData(col_idx, std::move(col_agg_buffer));
    if (ts_scan_stats) {
      ts_scan_stats->agg_bytes += col_agg_buffer_size;
    }
  }

  return KStatus::SUCCESS;
}

std::string TsEntitySegment::GetHandleInfoStr() {
  string str = "{" + std::to_string(info_.header_e_file_number) + ", "
                + std::to_string(info_.header_b_info.file_number) + ", "
                + std::to_string(info_.datablock_info.file_number) + ", "
                + std::to_string(info_.agg_info.file_number) + "}";
  return str;
}

}  //  namespace kwdbts
