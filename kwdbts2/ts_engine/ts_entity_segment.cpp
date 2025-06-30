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

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_agg.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_coding.h"
#include "ts_compressor.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lastsegment_builder.h"

namespace kwdbts {

KStatus TsEntitySegmentEntityItemFile::Open() {
  if (r_file_->GetFileSize() < sizeof(TsEntityItemFileHeader)) {
    LOG_ERROR("TsEntitySegmentEntityItemFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  TSSlice result;
  KStatus s = r_file_->Read(r_file_->GetFileSize() - sizeof(TsEntityItemFileHeader), sizeof(TsEntityItemFileHeader),
                            &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentEntityItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = reinterpret_cast<TsEntityItemFileHeader*>(result.data);
  if (header_->status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentEntityItemFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}

KStatus TsEntitySegmentEntityItemFile::GetEntityItem(uint64_t entity_id, TsEntityItem& entity_item, bool& is_exist) {
  if (entity_id > header_->entity_num) {
    is_exist = false;
    entity_item = TsEntityItem{};
    entity_item.entity_id = entity_id;
    LOG_WARN("entity item[id=%lu] not exist.", entity_id);
    return KStatus::SUCCESS;
  }
  assert(entity_id * sizeof(TsEntityItem) <= r_file_->GetFileSize() - sizeof(TsEntityItemFileHeader));
  TSSlice result;
  KStatus s = r_file_->Read((entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                            &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("read entity item[id=%lu] failed.", entity_id);
    return s;
  }
  entity_item = *reinterpret_cast<TsEntityItem *>(result.data);
  return s;
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
  if (r_file_->GetFileSize() < sizeof(TsBlockItemFileHeader)) {
    LOG_ERROR("TsEntitySegmentBlockItemFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  TSSlice result;
  KStatus s = r_file_->Read(0, sizeof(TsBlockItemFileHeader), &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = reinterpret_cast<TsBlockItemFileHeader*>(result.data);
  if (header_->status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentBlockItemFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}

KStatus TsEntitySegmentBlockItemFile::GetBlockItem(uint64_t blk_id, TsEntitySegmentBlockItem** blk_item) {
  TSSlice result;
  KStatus s = r_file_->Read(sizeof(TsBlockItemFileHeader) + (blk_id - 1) * sizeof(TsEntitySegmentBlockItem),
                            sizeof(TsEntitySegmentBlockItem), &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  *blk_item = reinterpret_cast<TsEntitySegmentBlockItem*>(result.data);
  return KStatus::SUCCESS;
}

TsEntitySegmentMetaManager::TsEntitySegmentMetaManager(const string& dir_path, uint64_t entity_header_file_num) :
  entity_header_(dir_path + "/" + EntityHeaderFileName(entity_header_file_num)),
  block_header_(dir_path + "/" + block_item_file_name), dir_path_(dir_path) {
}

KStatus TsEntitySegmentMetaManager::Open() {
  // Attempt to access the directory
  if (access(dir_path_.c_str(), 0)) {
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
                                                    std::vector<TsEntitySegmentBlockItem*>* blk_items) {
  TsEntityItem entity_item;
  bool is_exist;
  KStatus s = entity_header_.GetEntityItem(entity_id, entity_item, is_exist);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint64_t last_blk_id = entity_item.cur_block_id;

  TsEntitySegmentBlockItem* cur_blk_item;
  while (last_blk_id > 0) {
    s = block_header_.GetBlockItem(last_blk_id, &cur_blk_item);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    blk_items->push_back(cur_blk_item);
    last_blk_id = cur_blk_item->prev_block_id;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentMetaManager::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                                  std::shared_ptr<TsEntitySegment> blk_segment,
                                                  std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                                  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                                  uint32_t scan_version) {
  TsEntityItem entity_item;
  bool is_exist;
  KStatus s = entity_header_.GetEntityItem(filter.entity_id, entity_item, is_exist);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  uint64_t last_blk_id = entity_item.cur_block_id;

  TsEntitySegmentBlockItem* cur_blk_item;
  while (last_blk_id > 0) {
    s = block_header_.GetBlockItem(last_blk_id, &cur_blk_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get block item failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
      return s;
    }
    // todo(liangbo)  change lsn range if block item store.
    // todo(limeng) opts: Because block item traverses from back to front, use push_front
    if (IsTsLsnSpanCrossSpans(filter.spans_, {cur_blk_item->min_ts, cur_blk_item->max_ts}, {0, UINT64_MAX})) {
      std::shared_ptr<TsEntityBlock> block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item,
                                                                             blk_segment);
      // std::vector<std::pair<start_row, row_num>>
      std::vector<std::pair<int, int>> row_spans;
      s = block->GetRowSpans(filter.spans_, row_spans);
      if (s != KStatus::SUCCESS) {
        return s;
      }
      for (int i = row_spans.size() - 1; i >= 0; --i) {
        if (row_spans[i].second <= 0) {
          continue;
        }
        // Because block item traverses from back to front, use push_front
        block_spans.push_front(make_shared<TsBlockSpan>(filter.entity_id, block, row_spans[i].first,
                                                        row_spans[i].second, tbl_schema_mgr,
                                                        scan_version));
      }
    }
    last_blk_id = cur_blk_item->prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsEntityBlock::TsEntityBlock(uint32_t table_id, TsEntitySegmentBlockItem* block_item,
                             std::shared_ptr<TsEntitySegment> block_segment) {
  table_id_ = table_id;
  table_version_ = block_item->table_version;
  entity_id_ = block_item->entity_id;
  n_rows_ = block_item->n_rows;
  n_cols_ = block_item->n_cols;
  block_offset_ = block_item->block_offset;
  block_length_ = block_item->block_len;
  agg_offset_ = block_item->agg_offset;
  agg_length_ = block_item->agg_len;
  entity_segment_ = block_segment;
  // column blocks
  column_blocks_.resize(block_item->n_cols);
}

TsEntityBlock::TsEntityBlock(const TsEntityBlock& other) {
  table_id_ = other.table_id_;
  table_version_ = other.table_version_;
  entity_id_ = other.entity_id_;
  metric_schema_ = other.metric_schema_;
  block_info_ = other.block_info_;
  column_blocks_ = other.column_blocks_;
  n_rows_ = other.n_rows_;
  n_cols_ = other.n_cols_;
}

char* TsEntityBlock::GetMetricColAddr(uint32_t col_idx) {
  assert(col_idx < column_blocks_.size() - 1);
  if (col_idx == 0) {
    if (extra_buffer_.empty()) {
      extra_buffer_.resize(n_rows_ * 16);
      for (int i = 0; i < n_rows_; ++i) {
        memcpy(extra_buffer_.data() + i * 16, column_blocks_[1].buffer.data() + i * 8, 8);
      }
    }
    return extra_buffer_.data();
  }
  return column_blocks_[col_idx + 1].buffer.data();
}

KStatus TsEntityBlock::GetMetricColValue(uint32_t row_idx, uint32_t col_idx, TSSlice& value) {
  assert(col_idx < column_blocks_.size() - 1);
  assert(row_idx < n_rows_);

  if (!metric_schema_.empty() && isVarLenType(metric_schema_[col_idx].type)) {
    char* ptr = column_blocks_[col_idx + 1].buffer.data();
    uint32_t offset = *reinterpret_cast<uint32_t*>(ptr + row_idx * sizeof(uint32_t));
    uint32_t next_row_offset = *reinterpret_cast<uint32_t*>(ptr + (row_idx + 1) * sizeof(uint32_t));
    value.data = column_blocks_[col_idx + 1].buffer.data() + offset;
    value.len = next_row_offset - offset;
  } else {
    size_t d_size = col_idx == 0 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx].size);
    value.data = column_blocks_[col_idx + 1].buffer.data() + row_idx * d_size;
    value.len = d_size;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadLSNColData(TSSlice buffer) {
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  assert(column_blocks_.size() == n_cols_);
  uint32_t start_offset = block_info_.col_block_offset[0];
  uint32_t end_offset = block_info_.col_block_offset[1];
  // decompress
  TSSlice data{buffer.data, end_offset - start_offset};
  std::string plain;
  const auto& mgr = CompressorManager::GetInstance();
  bool ok = mgr.DecompressData(data, nullptr, n_rows_, &plain);
  if (!ok) {
    LOG_ERROR("block segment column[0] data decompress failed");
    return KStatus::FAIL;
  }
  // save decompressed col block data
  column_blocks_[0].buffer = std::move(plain);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadColData(int32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                                         TSSlice buffer) {
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  assert(column_blocks_.size() == n_cols_);
  assert(column_blocks_.size() > col_idx + 1);
  const auto& mgr = CompressorManager::GetInstance();
  if (metric_schema_.empty()) {
    metric_schema_ = metric_schema;
  }
  uint32_t start_offset = block_info_.col_block_offset[col_idx + 1];
  uint32_t end_offset = block_info_.col_block_offset[col_idx + 2];
  assert(buffer.len == end_offset - start_offset);

  TSSlice data{buffer.data, end_offset - start_offset};
  size_t bitmap_len = 0;
  if (col_idx >= 1) {
    bitmap_len = TsBitmap::GetBitmapLen(n_rows_);
    column_blocks_[col_idx + 1].bitmap.Map({data.data, bitmap_len}, n_rows_);
  } else if (col_idx == 0) {
    // Timestamp Column Assign Default Value kValid
    column_blocks_[col_idx + 1].bitmap.SetCount(n_rows_);
  }
  RemovePrefix(&data, bitmap_len);
  std::string plain;
  bool ok = mgr.DecompressData(data, &column_blocks_[col_idx + 1].bitmap, n_rows_, &plain);
  if (!ok) {
    LOG_ERROR("block segment column[%u] data decompress failed", col_idx + 1);
    return KStatus::FAIL;
  }
  // save decompressed col block data
  column_blocks_[col_idx + 1].buffer = std::move(plain);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadAggData(int32_t col_idx, TSSlice buffer) {
  if (buffer.len > 0) {
    column_blocks_[col_idx + 1].agg.assign(buffer.data, buffer.len);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadBlockInfo(TSSlice buffer) {
  for (int i = 0; i < n_cols_ + 1; ++i) {
    block_info_.col_block_offset.push_back(*reinterpret_cast<uint32_t*>(buffer.data + sizeof(uint32_t) * i));
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadAggInfo(TSSlice buffer) {
  for (int i = 0; i < n_cols_; ++i) {
    block_info_.col_agg_offset.push_back(*reinterpret_cast<uint32_t*>(buffer.data + sizeof(uint32_t) * i));
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::LoadAllData(const std::vector<AttributeInfo>& metric_schema, TSSlice buffer) {
  metric_schema_ = metric_schema;
  assert(n_cols_ == metric_schema.size() + 1);
  // block info(col offsets)
  LoadBlockInfo(buffer);
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  // lsn column block
  uint32_t start_offset = block_info_.col_block_offset[0];
  KStatus s = LoadLSNColData({buffer.data + start_offset, buffer.len - start_offset});
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("block segment column[0] data load failed");
    return s;
  }
  // metric column blocks
  for (int i = 0; i < n_cols_ - 1; ++i) {
    start_offset = block_info_.col_block_offset[i + 1];
    s = LoadColData(i, metric_schema, {buffer.data + start_offset, buffer.len - start_offset});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", i + 1);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetRowSpans(const std::vector<STScanRange>& spans,
                      std::vector<std::pair<int, int>>& row_spans) {
  if (!HasDataCached(0)) {
    KStatus s = entity_segment_->GetColumnBlock(0, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      return s;
    }
  }

  if (!HasDataCached(-1)) {
    KStatus s = entity_segment_->GetColumnBlock(-1, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      return s;
    }
  }
  timestamp64* ts_col = reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data());
  TS_LSN* lsn_col = reinterpret_cast<TS_LSN*>(column_blocks_[0].buffer.data());
  int start_idx = 0;
  bool match_found = false;
  assert(n_rows_ * 8 == column_blocks_[1].buffer.length());
  assert(n_rows_ * 8 == column_blocks_[0].buffer.length());
  // todo(liangbo) scan all rows, can we scan faster.
  for (int i = 0; i < n_rows_; i++) {
    if (IsTsLsnInSpans(ts_col[i], lsn_col[i], spans)) {
      if (!match_found) {
        start_idx = i;
        match_found = true;
      }
    } else {
      if (match_found) {
        match_found = false;
        row_spans.push_back({start_idx, i - start_idx});
      }
    }
  }
  if (match_found) {
    row_spans.push_back({start_idx, n_rows_ - start_idx});
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetRowSpans(const std::vector<KwTsSpan>& ts_spans,
                      std::vector<std::pair<int, int>>& row_spans) {
  if (!HasDataCached(0)) {
    KStatus s = entity_segment_->GetColumnBlock(0, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      return s;
    }
  }

  timestamp64* ts_col = reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data());
  timestamp64 max_ts = ts_col[n_rows_ - 1];
  timestamp64 min_ts = ts_col[0];
  int start_idx = 0;
  for (const KwTsSpan& span : ts_spans) {
    if (span.begin > max_ts || span.end < min_ts) {
      continue;
    }
    int begin_offset = 0, end_offset = 0;
    if (span.begin > ts_col[start_idx]) {
      timestamp64* ts = lower_bound(ts_col + start_idx, ts_col + n_rows_, span.begin);
      begin_offset = distance(ts_col + start_idx, ts);
    }
    start_idx += begin_offset;
    if (span.end >= max_ts) {
      end_offset = n_rows_ - start_idx - 1;
    } else {
      timestamp64* ts = upper_bound(ts_col + start_idx, ts_col + n_rows_, span.end);
      end_offset = distance(ts_col + start_idx, ts) - 1;
    }
    row_spans.push_back({start_idx, end_offset + 1});
    start_idx += end_offset;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                     char** value) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  *value = GetMetricColAddr(col_id);
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                                          TsBitmap& bitmap) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  bitmap = column_blocks_[col_id + 1].bitmap;
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                                           TSSlice& value) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  return GetMetricColValue(row_num, col_id, value);
}

bool TsEntityBlock::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) {
  if (!HasDataCached(col_id)) {
    KStatus s = entity_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  assert(col_id < column_blocks_.size() - 1);
  assert(row_num < n_rows_);
  return column_blocks_[col_id + 1].bitmap[row_num] == DataFlags::kNull;
}

timestamp64 TsEntityBlock::GetTS(int row_num) {
  if (!HasDataCached(0)) {
    KStatus s = entity_segment_->GetColumnBlock(0, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      return s;
    }
  }
  return *reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data() + row_num * sizeof(timestamp64));
}

uint64_t* TsEntityBlock::GetLSNAddr(int row_num) {
  if (!HasDataCached(-1)) {
    KStatus s = entity_segment_->GetColumnBlock(-1, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[lsn] data load failed");
      return nullptr;
    }
  }
  return reinterpret_cast<uint64_t*>(column_blocks_[0].buffer.data() + row_num * sizeof(uint64_t));
}

bool TsEntityBlock::HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) {
  return 0 == begin_row_idx && row_num == n_rows_;
}

KStatus TsEntityBlock::GetPreCount(uint32_t blk_col_idx, uint16_t& count) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    count = 0;
  } else {
    count = *reinterpret_cast<uint16_t*>(col_blk.agg.data());
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreSum(uint32_t blk_col_idx, int32_t size, void* &pre_sum, bool& is_overflow) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    return KStatus::SUCCESS;
  }
  void* pre_agg = static_cast<void*>(col_blk.agg.data());
  is_overflow = *static_cast<bool*>(pre_agg + sizeof(uint16_t) + size * 2);
  pre_sum = pre_agg + sizeof(uint16_t) + size * 2 + 1;
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreMax(uint32_t blk_col_idx, void* &pre_max) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    return KStatus::SUCCESS;
  }
  pre_max = static_cast<void*>(col_blk.agg.data() + sizeof(uint16_t));

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreMin(uint32_t blk_col_idx, int32_t size, void* &pre_min) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    return KStatus::SUCCESS;
  }
  if (blk_col_idx == 0) {
    size = 8;
  }
  pre_min = static_cast<void*>(col_blk.agg.data() + sizeof(uint16_t) + size);

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetVarPreMax(uint32_t blk_col_idx, TSSlice& pre_max) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    return KStatus::SUCCESS;
  }
  void* pre_agg = static_cast<void*>(col_blk.agg.data());
  pre_max.len = *static_cast<uint32_t *>(pre_agg + sizeof(uint16_t));
  pre_max.data = static_cast<char*>(pre_agg + sizeof(uint16_t) + sizeof(uint32_t) * 2);

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetVarPreMin(uint32_t blk_col_idx, TSSlice& pre_min) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  if (col_blk.agg.empty()) {
    return KStatus::SUCCESS;
  }
  void* pre_agg = static_cast<void*>(col_blk.agg.data());
  uint32_t max_len = *static_cast<uint32_t *>(pre_agg + sizeof(uint16_t));
  pre_min.len = *static_cast<uint32_t *>(pre_agg+ sizeof(uint16_t) + sizeof(uint32_t));
  pre_min.data = static_cast<char*>(pre_agg + sizeof(uint16_t) + sizeof(uint32_t) * 2 + max_len);
  return KStatus::SUCCESS;
}

TsEntitySegment::TsEntitySegment(const std::filesystem::path& root, uint64_t entity_header_file_num) :
                                 dir_path_(root), meta_mgr_(root, entity_header_file_num),
                                 block_file_(root / block_data_file_name),
                                 agg_file_(root / block_agg_file_name) {
  Open();
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

KStatus TsEntitySegment::GetAllBlockItems(TSEntityID entity_id,
                                         std::vector<TsEntitySegmentBlockItem*>* blk_items) {
  return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
}

KStatus TsEntitySegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                       std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                       std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                       uint32_t scan_version) {
  return meta_mgr_.GetBlockSpans(filter, shared_from_this(), block_spans, tbl_schema_mgr, scan_version);
}

KStatus TsEntitySegment::GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                                       TsEntityBlock* block) {
  // init block info
  if (block->GetBlockInfo().col_block_offset.empty()) {
    TSSlice buffer;
    buffer.len = sizeof(uint32_t) * (block->GetNCols() + 1);
    KStatus s = block_file_.ReadData(block->GetBlockOffset(), &buffer.data, buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read block info data failed")
      return s;
    }
    s = block->LoadBlockInfo(buffer);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock block info init failed")
      return s;
    }
  }
  // init column block
  if (!block->HasDataCached(col_idx)) {
    uint32_t start_offset = block->GetBlockInfo().col_block_offset[col_idx + 1];
    uint32_t end_offset = block->GetBlockInfo().col_block_offset[col_idx + 2];
    TSSlice buffer;
    buffer.len = end_offset - start_offset;
    KStatus s = block_file_.ReadData(block->GetBlockOffset() + start_offset, &buffer.data, buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }
    s = block->LoadColData(col_idx, metric_schema, {buffer.data, buffer.len});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock column[%u] block init failed", col_idx + 1);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegment::GetColumnAgg(int32_t col_idx, TsEntityBlock *block) {
  if (block->GetBlockInfo().col_agg_offset.empty()) {
    TSSlice agg_offsets;
    agg_offsets.len = sizeof(uint32_t) * block->GetNCols();
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset(), &agg_offsets.data, agg_offsets.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read agg data failed")
      return s;
    }
    s = block->LoadAggInfo(agg_offsets);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock agg info init failed")
      return s;
    }
  }
  if (!block->HasAggData(col_idx)) {
    uint32_t start_offset = block->GetBlockInfo().col_agg_offset[col_idx];
    uint32_t end_offset = block->GetBlockInfo().col_agg_offset[col_idx + 1];
    TSSlice col_agg_buffer;
    col_agg_buffer.len = end_offset - start_offset;
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset() + start_offset, &col_agg_buffer.data, col_agg_buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }
    block->LoadAggData(col_idx, {col_agg_buffer.data, col_agg_buffer.len});
  }

  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
