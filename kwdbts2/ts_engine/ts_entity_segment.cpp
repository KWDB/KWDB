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
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_coding.h"
#include "ts_lastsegment_builder.h"
#include "ts_timsort.h"
#include "ts_vgroup_partition.h"
#include "ts_compressor.h"
#include "ts_agg.h"

namespace kwdbts {

const char entity_item_meta_file_name[] = "header.e";
const char block_item_meta_file_name[] = "header.b";
const char block_data_file_name[] = "block";
const char block_agg_file_name[] = "agg";

KStatus TsEntitySegmentEntityItemFile::Open() {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsEntityItemFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.magic = TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
    header_.status = TsFileStatus::READY;
    header_.entity_num = 0;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsEntityItemFileHeader)});
  }
  return s;
}

void TsEntitySegmentEntityItemFile::WrLock() {
  RW_LATCH_X_LOCK(&rw_latch_);
}

void TsEntitySegmentEntityItemFile::RdLock() {
  RW_LATCH_S_LOCK(&rw_latch_);
}

void TsEntitySegmentEntityItemFile::UnLock() {
  RW_LATCH_UNLOCK(&rw_latch_);
}

KStatus TsEntitySegmentEntityItemFile::UpdateEntityItem(uint64_t entity_id,
                                                       const TsEntitySegmentBlockItem& block_item_info,
                                                       bool lock) {
  TsEntityItem entity_item{};
  TSSlice result;
  if (lock) {
    WrLock();
  }
  Defer defer([&]() {
    if (lock) {
      UnLock();
    }
  });
  KStatus s = file_->Read(sizeof(TsEntityItemFileHeader) + (entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                          &result, reinterpret_cast<char *>(&entity_item));
  bool new_entity = false;
  if (entity_item.entity_id == 0) {
    entity_item.entity_id = entity_id;
    new_entity = true;
  }
  entity_item.cur_block_id = block_item_info.block_id;
  if (entity_item.max_ts < block_item_info.max_ts) {
    entity_item.max_ts = block_item_info.max_ts;
  }
  if (entity_item.min_ts > block_item_info.min_ts) {
    entity_item.min_ts = block_item_info.min_ts;
  }
  entity_item.row_written += block_item_info.n_rows;
  s = file_->Write(sizeof(TsEntityItemFileHeader) + (entity_id - 1) * sizeof(TsEntityItem),
                   TSSlice{reinterpret_cast<char *>(&entity_item), sizeof(entity_item)});
  if (s == KStatus::SUCCESS) {
    if (new_entity) {
      ++header_.entity_num;
      s = file_->Write(0, TSSlice{reinterpret_cast<char *>(&header_), sizeof(header_)});
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("write entity header failed.")
        return s;
      }
    }
  } else {
    LOG_ERROR("write entity item[id=%lu] failed.", entity_id);
    return s;
  }
  return s;
}

KStatus TsEntitySegmentEntityItemFile::GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id, bool lock) {
  TsEntityItem entity_item{};
  TSSlice result;
  if (lock) {
    RdLock();
  }
  KStatus s = file_->Read(sizeof(TsEntityItemFileHeader) + (entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                          &result, reinterpret_cast<char *>(&entity_item));
  if (lock) {
    UnLock();
  }

  cur_block_id = entity_item.cur_block_id;
  return s;
}

KStatus TsEntitySegmentBlockItemFile::Open() {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsBlockItemFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.status = TsFileStatus::READY;
    header_.magic = TS_ENTITY_SEGMENT_BLOCK_ITEM_FILE_MAGIC;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsBlockItemFileHeader)});
  }
  return s;
}

KStatus TsEntitySegmentBlockItemFile::AllocateBlockItem(uint64_t entity_id, TsEntitySegmentBlockItem& block_item_info) {
  RW_LATCH_X_LOCK(block_item_mtx_);
  // file header
  header_.block_num += 1;
  KStatus s = writeFileMeta(header_);
  // block item info
  block_item_info.block_id = header_.block_num;
  size_t offset = sizeof(TsBlockItemFileHeader) + (block_item_info.block_id - 1) * sizeof(TsEntitySegmentBlockItem);
  file_->Write(offset, TSSlice{reinterpret_cast<char *>(&block_item_info), sizeof(TsEntitySegmentBlockItem)});
  RW_LATCH_UNLOCK(block_item_mtx_);
  return s;
}

KStatus TsEntitySegmentBlockItemFile::GetBlockItem(uint64_t entity_id, uint64_t blk_id, TsEntitySegmentBlockItem& blk_item) {
  RW_LATCH_S_LOCK(block_item_mtx_);
  TSSlice result;
  file_->Read(sizeof(TsBlockItemFileHeader) + (blk_id - 1) * sizeof(TsEntitySegmentBlockItem),
              sizeof(TsEntitySegmentBlockItem), &result, reinterpret_cast<char *>(&(blk_item)));
  RW_LATCH_UNLOCK(block_item_mtx_);
  if (result.len != sizeof(TsEntitySegmentBlockItem)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}


KStatus TsEntitySegmentBlockItemFile::readFileHeader(TsBlockItemFileHeader& block_meta) {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsBlockItemFileHeader), &result, reinterpret_cast<char *>(&block_meta));
  return s;
}

KStatus TsEntitySegmentBlockItemFile::writeFileMeta(TsBlockItemFileHeader& block_meta) {
  KStatus s = file_->Write(0, TSSlice{reinterpret_cast<char *>(&block_meta), sizeof(TsBlockItemFileHeader)});
  return s;
}

TsEntitySegmentMetaManager::TsEntitySegmentMetaManager(const string& path) :
  path_(path), entity_header_(path + "/" + entity_item_meta_file_name),
  block_header_(path + "/" + block_item_meta_file_name) {
}

KStatus TsEntitySegmentMetaManager::Open() {
  // Attempt to access the directory
  if (access(path_.c_str(), 0)) {
    LOG_ERROR("cannot open directory [%s].", path_.c_str());
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

KStatus TsEntitySegmentMetaManager::AppendBlockItem(TsEntitySegmentBlockItem& blk_item) {
  uint64_t entity_id = blk_item.entity_id;
  entity_header_.WrLock();
  Defer defer([&]() { entity_header_.UnLock(); });
  // get last block id
  uint64_t last_blk_id;
  KStatus s = entity_header_.GetEntityCurBlockId(entity_id, last_blk_id, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // allocate&add block item
  blk_item.prev_block_id = last_blk_id;
  s = block_header_.AllocateBlockItem(entity_id, blk_item);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // update entity item
  s = entity_header_.UpdateEntityItem(entity_id, blk_item, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentMetaManager::GetAllBlockItems(TSEntityID entity_id,
                                                    std::vector<TsEntitySegmentBlockItem>* blk_items) {
  uint64_t last_blk_id;
  KStatus s = entity_header_.GetEntityCurBlockId(entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  TsEntitySegmentBlockItem cur_blk_item;
  while (last_blk_id > 0) {
    s = block_header_.GetBlockItem(entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    blk_items->push_back(cur_blk_item);
    last_blk_id = cur_blk_item.prev_block_id;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentMetaManager::GetBlockSpans(const TsBlockItemFilterParams& filter, TsEntitySegment* blk_segment,
                                                  std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                                  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                                  uint32_t scan_version,
                                                  const std::vector<uint32_t>& ts_scan_cols) {
  uint64_t last_blk_id;
  KStatus s = entity_header_.GetEntityCurBlockId(filter.entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  TsEntitySegmentBlockItem cur_blk_item;
  while (last_blk_id > 0) {
    s = block_header_.GetBlockItem(filter.entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get block item failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
      return s;
    }

    if (isTimestampWithinSpans(filter.ts_spans_, cur_blk_item.min_ts, cur_blk_item.max_ts)) {
      std::shared_ptr<TsEntityBlock> block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, blk_segment);
      // Because block item traverses from back to front, use push_front
      block_spans.push_front(make_shared<TsBlockSpan>(filter.entity_id, block, 0, cur_blk_item.n_rows,
                                                      tbl_schema_mgr, scan_version, ts_scan_cols));
    } else if (isTimestampInSpans(filter.ts_spans_, cur_blk_item.min_ts, cur_blk_item.max_ts)) {
      std::shared_ptr<TsEntityBlock> block = std::make_shared<TsEntityBlock>(filter.table_id, cur_blk_item, blk_segment);
      // std::vector<std::pair<start_row, row_num>>
      std::vector<std::pair<int, int>> row_spans;
      s = block->GetRowSpans(filter.ts_spans_, row_spans);
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
                                                        scan_version, ts_scan_cols));
      }
    }
    last_blk_id = cur_blk_item.prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsEntityBlock::TsEntityBlock(uint32_t table_id, const TsEntitySegmentBlockItem& block_item,
                                         TsEntitySegment* block_segment) {
  table_id_ = table_id;
  table_version_ = block_item.table_version;
  entity_id_ = block_item.entity_id;
  n_rows_ = block_item.n_rows;
  n_cols_ = block_item.n_cols;
  block_offset_ = block_item.block_offset;
  block_length_ = block_item.block_len;
  agg_offset_ = block_item.agg_offset;
  agg_length_ = block_item.agg_len;
  entity_segment_ = block_segment;
  // column blocks
  column_blocks_.resize(block_item.n_cols);
}

TsEntityBlock::TsEntityBlock(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                                         std::vector<AttributeInfo>& metric_schema) :
  table_id_(table_id), table_version_(table_version),
  entity_id_(entity_id), metric_schema_(metric_schema) {
  n_cols_ = metric_schema.size() + 1;
  column_blocks_.resize(n_cols_);
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsEntitySegmentColumnBlock& column_block = column_blocks_[col_idx];
    column_block.bitmap.Reset(EngineOptions::max_rows_per_block);
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize((EngineOptions::max_rows_per_block + 1) * sizeof(uint32_t));
    }
  }
  block_info_.col_block_offset.resize(n_cols_ + 1);
  block_info_.col_agg_offset.resize(n_cols_);
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

uint64_t TsEntityBlock::GetLSN(uint32_t row_idx) {
  return *reinterpret_cast<uint64_t*>(column_blocks_[0].buffer.data() + row_idx * sizeof(uint64_t));
}

timestamp64 TsEntityBlock::GetTimestamp(uint32_t row_idx) {
  return *reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data() + row_idx * sizeof(timestamp64));
}

KStatus TsEntityBlock::GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value, std::vector<DataFlags>& data_flags) {
  for (int col_idx = 1; col_idx < n_cols_; ++col_idx) {
    char* ptr = column_blocks_[col_idx].buffer.data();
    if (isVarLenType(metric_schema_[col_idx - 1].type)) {
      uint32_t start_offset = *reinterpret_cast<uint32_t*>(ptr + row_idx * sizeof(uint32_t));
      uint32_t end_offset = *reinterpret_cast<uint32_t*>(ptr + (row_idx + 1) * sizeof(uint32_t));
      value.push_back({ptr + start_offset, end_offset - start_offset});
    } else {
      size_t d_size = col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
      value.push_back({ptr + row_idx * d_size, d_size});
    }
    data_flags.push_back(column_blocks_[col_idx].bitmap[row_idx]);
  }
  return KStatus::SUCCESS;
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

KStatus TsEntityBlock::Append(shared_ptr<TsBlockSpan> span, bool& is_full) {
  size_t written_rows = span->GetRowNum() + n_rows_ > EngineOptions::max_rows_per_block ?
                   EngineOptions::max_rows_per_block - n_rows_ : span->GetRowNum();
  assert(span->GetRowNum() >= written_rows);
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    size_t d_size = col_idx == 0 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
    // lsn column do not contain bitmaps
    bool has_bitmap = col_idx != 0;

    bool is_var_col = isVarLenType(d_type);
    TsEntitySegmentColumnBlock& block = column_blocks_[col_idx];
    size_t row_idx_in_block = n_rows_;
    char* col_val = nullptr;
    TsBitmap bitmap;
    if (!is_var_col && has_bitmap) {
      KStatus s = span->GetFixLenColAddr(col_idx - 1, &col_val, bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetColBitmap failed");
        return s;
      }
    }
    for (size_t span_row_idx = 0; span_row_idx < written_rows; ++span_row_idx) {
      if (!is_var_col && has_bitmap) {
        block.bitmap[row_idx_in_block] = bitmap[span_row_idx];
      }
      if (is_var_col) {
        DataFlags data_flag;
        TSSlice value;
        KStatus s = span->GetVarLenTypeColAddr(span_row_idx, col_idx - 1, data_flag, value);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetValueSlice failed");
          return s;
        }
        block.bitmap[row_idx_in_block] = data_flag;
        uint32_t var_offset = block.buffer.size();
        memcpy(block.buffer.data() + row_idx_in_block * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
        block.buffer.append(value.data, value.len);
        block.var_rows.emplace_back(value.data, value.len);
      } else if (col_idx == 1) {
        block.buffer.append(col_val + span_row_idx * d_size, sizeof(timestamp64));
      }
      row_idx_in_block++;
    }
    if (is_var_col) {
      uint32_t var_offset = block.buffer.size();
      memcpy(block.buffer.data() + row_idx_in_block * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
    } else {
      if (col_idx == 0) {
        char* lsn_col_value = reinterpret_cast<char *>(span->GetLSNAddr(0));
        block.buffer.append(lsn_col_value, written_rows * d_size);
      } else if (col_idx != 1) {
        block.buffer.append(col_val, written_rows * d_size);
      }
    }
  }
  n_rows_ += written_rows;
  span->Truncate(written_rows);
  is_full = n_rows_ == EngineOptions::max_rows_per_block;
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::Flush(TsVGroupPartition* partition) {
  // compressor manager
  const auto& mgr = CompressorManager::GetInstance();
  // init col data offsets to data buffer
  string data_buffer;
  data_buffer.resize((n_cols_ + 1) * sizeof(uint32_t));
  // init col agg offsets to agg buffer, exclude lsn col
  string agg_buffer;
  agg_buffer.resize(n_cols_ * sizeof(uint32_t));

  // write column block data and column agg
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    bool has_bitmap = col_idx > 1;
    bool is_var_col = isVarLenType(d_type);

    // record col offset
    block_info_.col_block_offset[col_idx] = data_buffer.size();

    TsEntitySegmentColumnBlock& block = column_blocks_[col_idx];
    // compress
    // compress bitmap
    if (has_bitmap) {
      block.bitmap.Truncate(n_rows_);
      TSSlice bitmap_data = block.bitmap.GetData();
      // TODO(limeng04): compress bitmap
      char bitmap_compress_type = 0;
      data_buffer.append(&bitmap_compress_type);
      data_buffer.append(bitmap_data.data, bitmap_data.len);
    }
    TsBitmap* b = has_bitmap ? &block.bitmap : nullptr;
    // compress col data & write to buffer
    std::string compressed;
    auto [first, second] = mgr.GetDefaultAlgorithm(d_type);
    TSSlice plain{block.buffer.data(), block.buffer.size()};
    mgr.CompressData(plain, b, n_rows_, &compressed, first, second);
    data_buffer.append(compressed);
    // calculate aggregate
    if (0 == col_idx) {
      continue;
    }
    string col_agg;
    if (!is_var_col) {
      TsBitmap* bitmap = nullptr;
      if (has_bitmap) {
        bitmap = &block.bitmap;
      }
      uint16_t count = 0;
      string max, min, sum;
      int32_t col_size =
        (metric_schema_[col_idx - 1].type == DATATYPE::TIMESTAMP64_LSN ? 8 : metric_schema_[col_idx - 1].size);
      max.resize(col_size, '\0');
      min.resize(col_size, '\0');
      // count: 2 bytes
      // max/min: col size
      // sum: 1 byte is_overflow + 8 byte result (int64_t or double)
      sum.resize(9, '\0');

      AggCalculatorV2 aggCalc(block.buffer.data(), bitmap, DATATYPE(metric_schema_[col_idx - 1].type),
                              metric_schema_[col_idx - 1].size, n_rows_);
      *reinterpret_cast<bool *>(sum.data()) =  aggCalc.CalcAggForFlush(count, max.data(), min.data(), sum.data() + 1);
      if (0 == count) {
        continue;
      }
      col_agg.resize(sizeof(uint16_t) + 2 * col_size + 9, '\0');
      memcpy(col_agg.data(), &count, sizeof(uint16_t));
      memcpy(col_agg.data() + sizeof(uint16_t), max.data(), col_size);
      memcpy(col_agg.data() + sizeof(uint16_t) + col_size, min.data(), col_size);
      memcpy(col_agg.data() + sizeof(uint16_t) + col_size * 2, sum.data(), 9);
    } else {
      VarColAggCalculatorV2 aggCalc(block.var_rows);
      string max;
      string min;
      uint64_t count = 0;
      aggCalc.CalcAggForFlush(max, min, count);
      if (0 == count) {
        continue;
      }
      col_agg.resize(sizeof(uint16_t) + 2 * sizeof(uint32_t), '\0');
      memcpy(col_agg.data(), &count, sizeof(uint16_t));
      col_agg.append(max);
      col_agg.append(min);
      *reinterpret_cast<uint32_t *>(col_agg.data() + sizeof(uint16_t)) = max.size();
      *reinterpret_cast<uint32_t *>(col_agg.data() + sizeof(uint16_t) + sizeof(uint32_t)) = min.size();
    }
    uint32_t offset = agg_buffer.size();
    memcpy(agg_buffer.data() + (col_idx - 1) * sizeof(uint32_t), &offset, sizeof(uint32_t));
    agg_buffer.append(col_agg);
  }
  uint32_t offset = agg_buffer.size();
  memcpy(agg_buffer.data() + (n_cols_ - 1) * sizeof(uint32_t), &offset, sizeof(uint32_t));

  // record last col dataoffset
  block_info_.col_block_offset[n_cols_] = data_buffer.size();
  // write col data offset
  for (int i = 0; i < n_cols_ + 1; ++i) {
    memcpy(data_buffer.data() + i * sizeof(uint32_t), &(block_info_.col_block_offset[i]), sizeof(uint32_t));
  }

  // flush
  timestamp64 min_ts = GetTimestamp(0);
  timestamp64 max_ts = GetTimestamp(n_rows_ - 1);
  KStatus s = partition->AppendToBlockSegment(table_id_, entity_id_, table_version_, n_cols_, n_rows_, max_ts, min_ts,
                                    {data_buffer.data(), data_buffer.size()}, {agg_buffer.data(), agg_buffer.size()});
  if (s != KStatus::SUCCESS) {
    return s;
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
  return GetTimestamp(row_num);
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

void TsEntityBlock::Clear() {
  n_rows_ = 0;
  if (n_cols_ > 0) {
    column_blocks_[0].buffer.clear();
  }
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsEntitySegmentColumnBlock& column_block = column_blocks_[col_idx];
    column_block.bitmap.Reset(EngineOptions::max_rows_per_block);
    column_block.buffer.clear();
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize((EngineOptions::max_rows_per_block + 1) * sizeof(uint32_t));
    }
    column_block.agg.clear();
    column_block.var_rows.clear();
  }
  block_info_.col_agg_offset.clear();
  block_info_.col_agg_offset.resize(n_cols_);
  block_info_.col_block_offset.clear();
  block_info_.col_block_offset.resize(n_cols_ + 1);
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
  count = *reinterpret_cast<uint16_t*>(col_blk.agg.data());
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreSum(uint32_t blk_col_idx, int32_t size, void* &pre_sum, bool& is_overflow) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  void* pre_agg_ = static_cast<void*>(col_blk.agg.data());
  is_overflow = *static_cast<bool*>(pre_agg_ + sizeof(uint16_t) + size * 2);
  pre_sum = pre_agg_ + sizeof(uint16_t) + size * 2 + 1;
  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreMax(uint32_t blk_col_idx, void* &pre_max) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  pre_max = static_cast<void*>(col_blk.agg.data() + sizeof(uint16_t));

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetPreMin(uint32_t blk_col_idx, int32_t size, void* &pre_min) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  pre_min = static_cast<void*>(col_blk.agg.data() + sizeof(uint16_t) + size);

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetVarPreMax(uint32_t blk_col_idx, TSSlice& pre_max) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  void* pre_agg_ = static_cast<void*>(col_blk.agg.data());
  pre_max.len = *static_cast<uint32_t *>(pre_agg_ + sizeof(uint16_t));
  pre_max.data = static_cast<char*>(pre_agg_ + sizeof(uint16_t) + sizeof(uint32_t) * 2);

  return KStatus::SUCCESS;
}

KStatus TsEntityBlock::GetVarPreMin(uint32_t blk_col_idx, TSSlice& pre_min) {
  auto s = entity_segment_->GetColumnAgg(blk_col_idx, this);
  if (s != SUCCESS) {
    return s;
  }
  auto& col_blk = column_blocks_[blk_col_idx + 1];
  void* pre_agg_ = static_cast<void*>(col_blk.agg.data());
  uint32_t max_len = *static_cast<uint32_t *>(pre_agg_ + sizeof(uint16_t));
  pre_min.len = *static_cast<uint32_t *>(pre_agg_+ sizeof(uint16_t) + sizeof(uint32_t));
  pre_min.data = static_cast<char*>(pre_agg_ + sizeof(uint16_t) + sizeof(uint32_t) * 2 + max_len);
  return KStatus::SUCCESS;
}

TsEntitySegment::TsEntitySegment(const std::filesystem::path& root)
  : dir_path_(root), meta_mgr_(root), block_file_(root / block_data_file_name),
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

KStatus TsEntitySegment::AppendBlockData(TsEntitySegmentBlockItem& blk_item, const TSSlice& data, const TSSlice& agg) {
  uint64_t blk_offset = 0;
  KStatus s = block_file_.AppendBlock(data, &blk_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to block file failed. data len: %lu.", data.len);
    return s;
  }
  uint64_t agg_offset = 0;
  s = agg_file_.AppendAggBlock(agg, &agg_offset);
  if (s != SUCCESS) {
    LOG_ERROR("append to agg file failed. agg len: %lu.", agg.len);
    return s;
  }
  blk_item.block_offset = blk_offset;
  blk_item.block_len = data.len;
  blk_item.agg_offset = agg_offset;
  blk_item.agg_len = agg.len;
  s = meta_mgr_.AppendBlockItem(blk_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to meta file failed. data len: %lu.", data.len);
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegment::GetAllBlockItems(TSEntityID entity_id,
                                         std::vector<TsEntitySegmentBlockItem>* blk_items) {
  return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
}

KStatus TsEntitySegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                       std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                       std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                       uint32_t scan_version,
                                       const std::vector<uint32_t>& ts_scan_cols) {
  return meta_mgr_.GetBlockSpans(filter, this, block_spans, tbl_schema_mgr, scan_version, ts_scan_cols);
}

KStatus TsEntitySegment::GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                                       TsEntityBlock* block) {
  // init block info
  if (block->GetBlockInfo().col_block_offset.empty()) {
    TSSlice buffer;
    buffer.len = sizeof(uint32_t) * (block->GetNCols() + 1);
    buffer.data = new char[buffer.len];
    Defer defer {[&]() { delete[] buffer.data; }};
    KStatus s = block_file_.ReadData(block->GetBlockOffset(), buffer.data, buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read block info data failed")
      return s;
    }
    s = block->LoadBlockInfo({buffer.data, buffer.len});
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
    buffer.data = new char[buffer.len];
    Defer defer {[&]() { delete[] buffer.data; }};
    KStatus s = block_file_.ReadData(block->GetBlockOffset() + start_offset, buffer.data, buffer.len);
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
    agg_offsets.data = new char[agg_offsets.len];
    Defer defer {[&]() { delete[] agg_offsets.data; }};
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset(), agg_offsets.data, agg_offsets.len);
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
    col_agg_buffer.data = new char[col_agg_buffer.len];
    Defer defer {[&]() { delete[] col_agg_buffer.data; }};
    KStatus s = agg_file_.ReadAggData(block->GetAggOffset() + start_offset, col_agg_buffer.data, col_agg_buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }
    block->LoadAggData(col_idx, {col_agg_buffer.data, col_agg_buffer.len});
  }

  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBuilder::BuildAndFlush() {
  KStatus s;
  // 1. The iterator will be used to read MAX_COMPACT_NUM last segment data
  shared_ptr<TsBlockSpan> block_span{nullptr};
  bool is_finished = false;
  TsEngineSchemaManager* schema_mgr = partition_->GetSchemaMgr();
  // 2. new last segment
  std::unique_ptr<TsFile> last_segment = nullptr;
  std::unique_ptr<TsLastSegmentBuilder> builder = nullptr;
  // 3. Traverse the last segment data and write the data to the block segment
  std::vector<std::list<shared_ptr<TsBlockSpan>>> block_spans;
  block_spans.resize(last_segments_.size());
  for (int i = 0; i < last_segments_.size(); ++i) {
    s = last_segments_[i]->GetBlockSpans(block_spans[i]);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, get block spans failed.")
      return s;
    }
  }
  TsBlockSpanSortedIterator iter(block_spans, EngineOptions::g_dedup_rule);
  iter.Init();
  TsEntityKey entity_key;
  std::shared_ptr<TsEntityBlock> block = nullptr;
  std::vector<std::shared_ptr<TsEntityBlock>> cached_blocks;
  while (true) {
    if (!block_span || block_span->GetRowNum() == 0) {
      s = iter.Next(block_span, &is_finished);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, iterate last segments failed.")
        return s;
      }
      std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr = {nullptr};
      s = schema_mgr->GetTableSchemaMgr(block_span->GetTableID(), tbl_schema_mgr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get table schema manager failed. table id: %lu", block_span->GetTableID());
        return s;
      }
      if (is_finished) {
        break;
      }
      block_span->SetConvertVersion(tbl_schema_mgr);
    }
    TsEntityKey cur_entity_key = {block_span->GetTableID(), block_span->GetTableVersion(), block_span->GetEntityID()};
    if (entity_key != cur_entity_key) {
      if (block && block->HasData()) {
        if (block->GetRowNum() >= EngineOptions::min_rows_per_block) {
          s = block->Flush(partition_);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, flush block failed.")
            return s;
          }
          block->Clear();
        } else {
          // Create new last segment
          if (builder == nullptr && block->GetRowNum() > 0) {
            uint32_t file_number;
            s = partition_->NewLastSegmentFile(&last_segment, &file_number);
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, new last segment failed.")
              return s;
            }
            builder = std::make_unique<TsLastSegmentBuilder>(schema_mgr, std::move(last_segment), file_number);
          }
          // Writes the incomplete data back to the last segment
          for (uint32_t row_idx = 0; row_idx < block->GetRowNum(); ++row_idx) {
            uint64_t lsn = block->GetLSN(row_idx);
            std::vector<TSSlice> metric_value;
            std::vector<DataFlags> data_flags;
            block->GetMetricValue(row_idx, metric_value, data_flags);
            s = builder->PutColData(entity_key.table_id, entity_key.table_version, entity_key.entity_id, lsn,
                                   metric_value, data_flags);
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder put failed.")
              return s;
            }
          }
          if (builder != nullptr) {
            cached_blocks.push_back(block);
            if (entity_key.table_id != cur_entity_key.table_id ||
                entity_key.table_version != cur_entity_key.table_version) {
              s = builder->FlushBuffer();
              if (s != KStatus::SUCCESS) {
                LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder flush buffer failed.")
                return s;
              }
              cached_blocks.clear();
            }
          }
        }
      }
      // Get the metric schema
      std::vector<AttributeInfo> metric_schema;
      if (block == nullptr || entity_key.table_id != cur_entity_key.table_id ||
          entity_key.table_version != cur_entity_key.table_version) {
        std::shared_ptr<MMapMetricsTable> table_schema_;
        s = schema_mgr->GetTableMetricSchema({}, block_span->GetTableID(), block_span->GetTableVersion(), &table_schema_);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("get table schema failed. table id: %lu, table version: %u.",
                    block_span->GetTableID(), block_span->GetTableVersion());
          return s;
        }
        metric_schema = table_schema_->getSchemaInfoExcludeDropped();
      } else {
        metric_schema = block->GetMetricSchema();
      }
      // init the block segment block
      block = std::make_shared<TsEntityBlock>(block_span->GetTableID(), block_span->GetTableVersion(),
                                              block_span->GetEntityID(), metric_schema);
      entity_key = cur_entity_key;
    }

    // write data to block buffer
    bool is_full = false;
    s = block->Append(block_span, is_full);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, append block failed.")
      return s;
    }
    // flush block if full
    if (is_full) {
      s = block->Flush(partition_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, flush block failed.")
        return s;
      }
      block->Clear();
    }
  }
  // 4. Writes the incomplete data back to the last segment
  if (block && block->HasData()) {
    // Create new last segment
    if (builder == nullptr && block->GetRowNum() > 0) {
      uint32_t file_number;
      s = partition_->NewLastSegmentFile(&last_segment, &file_number);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, new last segment failed.")
        return s;
      }
      builder = std::make_unique<TsLastSegmentBuilder>(schema_mgr, std::move(last_segment), file_number);
    }
    // Writes the incomplete data back to the last segment
    for (uint32_t row_idx = 0; row_idx < block->GetRowNum(); ++row_idx) {
      uint64_t lsn = block->GetLSN(row_idx);
      std::vector<TSSlice> metric_value;
      std::vector<DataFlags> data_flags;
      block->GetMetricValue(row_idx, metric_value, data_flags);
      s = builder->PutColData(entity_key.table_id, entity_key.table_version, entity_key.entity_id,
                             lsn, metric_value, data_flags);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder put failed.")
        return s;
      }
    }
  }
  // 5. flush the last segment block
  if (builder != nullptr) {
    s = builder->Finalize();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder finalize failed.")
      return s;
    }
    partition_->PublicLastSegment(builder->GetFileNumber());
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
