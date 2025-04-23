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

#include "ts_block_segment.h"
#include "kwdb_type.h"
#include "ts_lastsegment_builder.h"
#include "ts_timsort.h"
#include "ts_vgroup_partition.h"
#include "ts_compressor.h"

namespace kwdbts {

const char entity_item_meta_file_name[] = "header.e";
const char block_item_meta_file_name[] = "header.b";
const char block_data_file_name[] = "block";

KStatus TsBlockSegmentEntityItemFile::Open() {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsEntityItemFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.magic = TS_BLOCK_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
    header_.status = TsFileStatus::READY;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsEntityItemFileHeader)});
  }
  return s;
}

void TsBlockSegmentEntityItemFile::WrLock() {
  RW_LATCH_X_LOCK(&rw_latch_);
}

void TsBlockSegmentEntityItemFile::RdLock() {
  RW_LATCH_S_LOCK(&rw_latch_);
}

void TsBlockSegmentEntityItemFile::UnLock() {
  RW_LATCH_UNLOCK(&rw_latch_);
}

KStatus TsBlockSegmentEntityItemFile::UpdateEntityItem(uint64_t entity_id,
                                                       const TsBlockSegmentBlockItem& block_item_info,
                                                       bool lock) {
  TsEntityItem entity_item{};
  TSSlice result;
  if (lock) {
    WrLock();
  }
  KStatus s = file_->Read(sizeof(TsEntityItemFileHeader) + (entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                          &result, reinterpret_cast<char *>(&entity_item));
  if (entity_item.entity_id == 0) {
    entity_item.entity_id = entity_id;
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
  if (lock) {
    UnLock();
  }
  return s;
}

KStatus TsBlockSegmentEntityItemFile::GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id, bool lock) {
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

KStatus TsBlockSegmentBlockItemFile::Open() {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsBlockItemFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.status = TsFileStatus::READY;
    header_.magic = TS_BLOCK_SEGMENT_BLOCK_ITEM_FILE_MAGIC;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsBlockItemFileHeader)});
  }
  return s;
}

KStatus TsBlockSegmentBlockItemFile::AllocateBlockItem(uint64_t entity_id, TsBlockSegmentBlockItem& block_item_info) {
  RW_LATCH_X_LOCK(block_item_mtx_);
  // file header
  header_.block_num += 1;
  KStatus s = writeFileMeta(header_);
  // block item info
  block_item_info.block_id = header_.block_num;
  size_t offset = sizeof(TsBlockItemFileHeader) + (block_item_info.block_id - 1) * sizeof(TsBlockSegmentBlockItem);
  file_->Write(offset, TSSlice{reinterpret_cast<char *>(&block_item_info), sizeof(TsBlockSegmentBlockItem)});
  RW_LATCH_UNLOCK(block_item_mtx_);
  return s;
}

KStatus TsBlockSegmentBlockItemFile::GetBlockItem(uint64_t entity_id, uint64_t blk_id, TsBlockSegmentBlockItem& blk_item) {
  RW_LATCH_S_LOCK(block_item_mtx_);
  TSSlice result;
  file_->Read(sizeof(TsBlockItemFileHeader) + (blk_id - 1) * sizeof(TsBlockSegmentBlockItem),
              sizeof(TsBlockSegmentBlockItem), &result, reinterpret_cast<char *>(&(blk_item)));
  RW_LATCH_UNLOCK(block_item_mtx_);
  if (result.len != sizeof(TsBlockSegmentBlockItem)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}


KStatus TsBlockSegmentBlockItemFile::readFileHeader(TsBlockItemFileHeader& block_meta) {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsBlockItemFileHeader), &result, reinterpret_cast<char *>(&block_meta));
  return s;
}

KStatus TsBlockSegmentBlockItemFile::writeFileMeta(TsBlockItemFileHeader& block_meta) {
  KStatus s = file_->Write(0, TSSlice{reinterpret_cast<char *>(&block_meta), sizeof(TsBlockItemFileHeader)});
  return s;
}

TsBlockSegmentMetaManager::TsBlockSegmentMetaManager(const string& path) :
  path_(path), entity_meta_(path + "/" + entity_item_meta_file_name),
  block_meta_(path + "/" + block_item_meta_file_name) {
}

KStatus TsBlockSegmentMetaManager::Open() {
  // Attempt to access the directory
  if (access(path_.c_str(), 0)) {
    LOG_ERROR("cannot open directory [%s].", path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = entity_meta_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_meta_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentMetaManager::AppendBlockItem(TsBlockSegmentBlockItem& blk_item) {
  uint64_t entity_id = blk_item.entity_id;
  entity_meta_.WrLock();
  Defer defer([&]() { entity_meta_.UnLock(); });
  // get last block id
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // allocate&add block item
  blk_item.prev_block_id = last_blk_id;
  s = block_meta_.AllocateBlockItem(entity_id, blk_item);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // update entity item
  s = entity_meta_.UpdateEntityItem(entity_id, blk_item, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentMetaManager::GetAllBlockItems(TSEntityID entity_id,
                                                    std::vector<TsBlockSegmentBlockItem>* blk_items) {
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  TsBlockSegmentBlockItem cur_blk_item;
  while (last_blk_id > 0) {
    s = block_meta_.GetBlockItem(entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    blk_items->push_back(cur_blk_item);
    last_blk_id = cur_blk_item.prev_block_id;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentMetaManager::GetBlockSpans(const TsBlockItemFilterParams& filter, TsBlockSegment* blk_segment,
                                                 std::vector<TsBlockSpan>* block_spans) {
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(filter.entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  TsBlockSegmentBlockItem cur_blk_item;
  while (last_blk_id > 0) {
    s = block_meta_.GetBlockItem(filter.entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get block item failed, entity_id=%lu, blk_id=%lu", filter.entity_id, last_blk_id);
      return s;
    }

    if (isTimestampInSpans(filter.ts_spans_, cur_blk_item.min_ts, cur_blk_item.max_ts)) {
      std::shared_ptr<TsBlockSegmentBlock> block = std::make_shared<TsBlockSegmentBlock>(filter.table_id, cur_blk_item,
                                                                                         blk_segment);
      std::vector<std::pair<int, int>> row_spans;
      s = block->GetRowSpans(filter.ts_spans_, row_spans);
      if (s != KStatus::SUCCESS) {
        return s;
      }
      for (int i = row_spans.size() - 1; i >= 0; --i) {
        TsBlockSpan block_span(filter.table_id, cur_blk_item.table_version, filter.entity_id, block,
                               row_spans[i].first, row_spans[i].second);
        block_spans->insert(block_spans->begin(), block_span);
      }
    }
    last_blk_id = cur_blk_item.prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsBlockSegmentBlock::TsBlockSegmentBlock(uint32_t table_id, const TsBlockSegmentBlockItem& block_item,
                                         TsBlockSegment* block_segment) {
  table_id_ = table_id;
  table_version_ = block_item.table_version;
  entity_id_ = block_item.entity_id;
  n_rows_ = block_item.n_rows;
  n_cols_ = block_item.n_cols;
  block_offset_ = block_item.block_offset;
  block_length_ = block_item.block_len;
  block_segment_ = block_segment;
  // column blocks
  column_blocks_.resize(block_item.n_cols);
}

TsBlockSegmentBlock::TsBlockSegmentBlock(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                                         std::vector<AttributeInfo>& metric_schema) :
  table_id_(table_id), table_version_(table_version),
  entity_id_(entity_id), metric_schema_(metric_schema) {
  n_cols_ = metric_schema.size() + 1;
  column_blocks_.resize(n_cols_);
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsBlockSegmentColumnBlock& column_block = column_blocks_[col_idx];
    column_block.bitmap.Reset(EngineOptions::max_rows_per_block);
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize((EngineOptions::max_rows_per_block + 1) * sizeof(uint32_t));
    }
  }
  block_info_.col_block_offset.resize(n_cols_ + 1);
}
TsBlockSegmentBlock::TsBlockSegmentBlock(const TsBlockSegmentBlock& other) {
  table_id_ = other.table_id_;
  table_version_ = other.table_version_;
  entity_id_ = other.entity_id_;
  metric_schema_ = other.metric_schema_;
  block_info_ = other.block_info_;
  column_blocks_ = other.column_blocks_;
  n_rows_ = other.n_rows_;
  n_cols_ = other.n_cols_;
}

uint64_t TsBlockSegmentBlock::GetSeqNo(uint32_t row_idx) {
  return *reinterpret_cast<uint64_t*>(column_blocks_[0].buffer.data() + row_idx * sizeof(uint64_t));
}

timestamp64 TsBlockSegmentBlock::GetTimestamp(uint32_t row_idx) {
  return *reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data() + row_idx * sizeof(timestamp64));
}

KStatus TsBlockSegmentBlock::GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value) {
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
  }
  return KStatus::SUCCESS;
}

char* TsBlockSegmentBlock::GetMetricColAddr(uint32_t col_idx) {
  assert(col_idx < column_blocks_.size() - 1);
  return column_blocks_[col_idx + 1].buffer.data();
}

KStatus TsBlockSegmentBlock::GetMetricColValue(uint32_t row_idx, uint32_t col_idx, TSSlice& value) {
  assert(col_idx < column_blocks_.size() - 1);
  assert(row_idx < n_rows_);
  if (isVarLenType(metric_schema_[col_idx].type)) {
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

KStatus TsBlockSegmentBlock::Append(TsLastSegmentBlockSpan& span, bool& is_full) {
  size_t end_row = span.end_row - span.start_row + n_rows_ > EngineOptions::max_rows_per_block ?
                   span.start_row + EngineOptions::max_rows_per_block - n_rows_ : span.end_row;
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    size_t d_size = col_idx == 0 ? 8 : col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
    bool has_bitmap = col_idx != 0;

    bool is_var_col = isVarLenType(d_type);
    TsBlockSegmentColumnBlock& block = column_blocks_[col_idx];
    size_t row_idx_in_block = n_rows_;
    for (size_t span_row_idx = span.start_row; span_row_idx < end_row; ++span_row_idx) {
      if (has_bitmap) {
        block.bitmap[row_idx_in_block] = span.block->GetBitmap(col_idx + 1, span_row_idx);
      }
      TSSlice value;
      if (is_var_col) {
        value = span.block->GetData(col_idx + 1, span_row_idx, d_type, d_size);
        uint32_t var_offset = block.buffer.size();
        memcpy(block.buffer.data() + row_idx_in_block * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
        block.buffer.append(value.data, value.len);
      }
      row_idx_in_block++;
    }
    if (is_var_col) {
      uint32_t var_offset = block.buffer.size();
      memcpy(block.buffer.data() + row_idx_in_block * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
    } else {
      TSSlice value = span.block->GetNotVarBatchData(col_idx + 1, span.start_row, end_row - span.start_row, d_size);
      block.buffer.append(value.data, value.len);
    }
  }
  n_rows_ += end_row - span.start_row;
  span.start_row = end_row;
  is_full = n_rows_ == EngineOptions::max_rows_per_block;
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::Flush(TsVGroupPartition* partition) {
  // compressor manager
  const auto& mgr = CompressorManager::GetInstance();
  // init col offsets to buffer
  string buffer;
  buffer.resize((n_cols_ + 1) * sizeof(uint32_t));
  // write column block data to buffer
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    size_t d_size = col_idx == 0 ? 8 : col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
    bool has_bitmap = col_idx != 0;
    bool is_var_col = isVarLenType(d_type);

    // record col offset
    block_info_.col_block_offset[col_idx] = buffer.size();

    TsBlockSegmentColumnBlock& block = column_blocks_[col_idx];
    // compress
    // compress bitmap
    if (has_bitmap) {
      TSSlice bitmap_data = block.bitmap.GetData();
      // TODO(limeng04): compress bitmap
      char bitmap_compress_type = 0;
      buffer.append(&bitmap_compress_type);
      buffer.append(bitmap_data.data, bitmap_data.len);
    }
    // compress col data & write to buffer
    std::string compressed;
    auto compressor = mgr.GetDefaultCompressor(d_type);
    TSSlice plain{const_cast<char *>(block.buffer.data()), block.buffer.size()};
    TsBitmap* b = has_bitmap ? &block.bitmap : nullptr;
    bool ok = compressor.Compress(plain, b, n_rows_, &compressed);
    if (ok) {
      auto [first, second] = compressor.GetAlgorithms();
      buffer.push_back(static_cast<char>(first));
      buffer.push_back(static_cast<char>(second));
      buffer.append(compressed);
    } else {
      buffer.push_back(static_cast<char>(TsCompAlg::kPlain));
      buffer.push_back(static_cast<char>(GenCompAlg::kPlain));
      buffer.append(block.buffer);
    }
  }

  // record col offset
  block_info_.col_block_offset[n_cols_] = buffer.size();
  // write last col data end offset
  for (int i = 0; i < n_cols_ + 1; ++i) {
    memcpy(buffer.data() + i * sizeof(uint32_t), &(block_info_.col_block_offset[i]), sizeof(uint32_t));
  }

  // flush
  timestamp64 min_ts = GetTimestamp(0);
  timestamp64 max_ts = GetTimestamp(n_rows_ - 1);
  KStatus s = partition->AppendToBlockSegment(table_id_, entity_id_, table_version_, n_cols_, n_rows_, max_ts, min_ts,
                                              {buffer.data(), buffer.size()}, {});
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::LoadSeqNo(TSSlice buffer) {
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  assert(column_blocks_.size() == n_cols_);
  char* ptr = buffer.data;
  TsCompAlg first = static_cast<TsCompAlg>(*ptr);
  ptr++;
  GenCompAlg second = static_cast<GenCompAlg>(*ptr);
  ptr++;
  assert(first < TsCompAlg::TS_COMP_ALG_LAST && second < GenCompAlg::GEN_COMP_ALG_LAST);
  auto compressor = CompressorManager::GetInstance().GetCompressor(first, second);
  // decompress
  uint32_t start_offset = block_info_.col_block_offset[0];
  uint32_t end_offset = block_info_.col_block_offset[1];
  std::string_view plain_sv{ptr, end_offset - start_offset - 2};
  std::string plain;
  if (compressor.IsPlain()) {
  } else {
    bool ok = compressor.Decompress({ptr, plain_sv.size()}, nullptr, n_rows_, &plain);
    if (!ok) {
      LOG_ERROR("block segment column[0] data decompress failed");
      return KStatus::FAIL;
    }
    plain_sv = plain;
  }
  // save decompressed col block data
  column_blocks_[0].buffer.assign(plain_sv);
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::LoadColData(uint32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                                         TSSlice buffer) {
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  assert(column_blocks_.size() == n_cols_);
  assert(column_blocks_.size() > col_idx + 1);
  if (metric_schema_.empty()) {
    metric_schema_ = metric_schema;
  }
  size_t bitmap_len = TsBitmap::GetBitmapLen(n_rows_);
  column_blocks_[col_idx + 1].bitmap.Map({buffer.data, bitmap_len}, n_rows_);
  char* ptr = buffer.data + bitmap_len;
  TsCompAlg first = static_cast<TsCompAlg>(*ptr);
  ptr++;
  GenCompAlg second = static_cast<GenCompAlg>(*ptr);
  ptr++;
  assert(first < TsCompAlg::TS_COMP_ALG_LAST && second < GenCompAlg::GEN_COMP_ALG_LAST);
  auto compressor = CompressorManager::GetInstance().GetCompressor(first, second);
  // decompress
  uint32_t start_offset = block_info_.col_block_offset[col_idx + 1];
  uint32_t end_offset = block_info_.col_block_offset[col_idx + 2];
  std::string_view plain_sv{ptr, end_offset - start_offset - bitmap_len - 2};
  std::string plain;
  if (compressor.IsPlain()) {
  } else {
    bool ok = compressor.Decompress({ptr, plain_sv.size()}, &column_blocks_[col_idx + 1].bitmap, n_rows_, &plain);
    if (!ok) {
      LOG_ERROR("block segment column[%u] data decompress failed", col_idx + 1);
      return KStatus::FAIL;
    }
    plain_sv = plain;
  }
  // save decompressed col block data
  column_blocks_[col_idx + 1].buffer.assign(plain_sv);
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::LoadBlockInfo(TSSlice buffer) {
  for (int i = 0; i < n_cols_ + 1; ++i) {
    block_info_.col_block_offset.push_back(*reinterpret_cast<uint32_t*>(buffer.data + sizeof(uint32_t) * i));
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::LoadAllData(const std::vector<AttributeInfo>& metric_schema, TSSlice buffer) {
  metric_schema_ = metric_schema;
  assert(n_cols_ == metric_schema.size() + 1);
  // block info(col offsets)
  LoadBlockInfo(buffer);
  assert(block_info_.col_block_offset.size() == n_cols_ + 1);
  // seq no column block
  uint32_t start_offset = block_info_.col_block_offset[0];
  KStatus s = LoadSeqNo({buffer.data + start_offset, buffer.len - start_offset});
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

KStatus TsBlockSegmentBlock::GetRowSpans(const std::vector<KwTsSpan>& ts_spans, std::vector<std::pair<int, int>>& row_spans) {
  if (!HasColumnData(0)) {
    KStatus s = block_segment_->GetColumnBlock(0, {}, this);
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

KStatus TsBlockSegmentBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                     char** value) {
  if (!HasColumnData(col_id)) {
    KStatus s = block_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  *value = GetMetricColAddr(col_id);
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                                          TsBitmap& bitmap) {
  if (!HasColumnData(col_id)) {
    KStatus s = block_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  bitmap = column_blocks_[col_id + 1].bitmap;
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                                           TSSlice& value) {
  if (!HasColumnData(col_id)) {
    KStatus s = block_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  return GetMetricColValue(row_num, col_id, value);
}

bool TsBlockSegmentBlock::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) {
  if (!HasColumnData(col_id)) {
    KStatus s = block_segment_->GetColumnBlock(col_id, schema, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[%u] data load failed", col_id);
      return s;
    }
  }
  assert(col_id < column_blocks_.size() - 1);
  assert(row_num < n_rows_);
  return column_blocks_[col_id + 1].bitmap[row_num] == DataFlags::kNull;
}

timestamp64 TsBlockSegmentBlock::GetTS(int row_num) {
  if (!HasColumnData(0)) {
    KStatus s = block_segment_->GetColumnBlock(0, {}, this);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("block segment column[0] data load failed");
      return s;
    }
  }
  return GetTimestamp(row_num);
}

void TsBlockSegmentBlock::Clear() {
  n_rows_ = 0;
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsBlockSegmentColumnBlock& column_block = column_blocks_[col_idx];
    column_block.bitmap.Reset(EngineOptions::max_rows_per_block);
    column_block.buffer.clear();
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize((EngineOptions::max_rows_per_block + 1) * sizeof(uint32_t));
    }
  }
  block_info_.col_block_offset.clear();
  block_info_.col_block_offset.resize(n_cols_ + 1);
}

TsBlockSegment::TsBlockSegment(const std::filesystem::path& root)
    : dir_path_(root), meta_mgr_(root), block_file_(root / block_data_file_name) {
  Open();
}

KStatus TsBlockSegment::Open() {
  KStatus s = meta_mgr_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_file_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::AppendBlockData(TsBlockSegmentBlockItem& blk_item, const TSSlice& data, const TSSlice& agg) {
  uint64_t blk_offset = 0;
  KStatus s = block_file_.AppendBlock(data, &blk_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to block file failed. data len: %lu.", data.len);
    return s;
  }
  blk_item.block_offset = blk_offset;
  blk_item.block_len = data.len;
  s = meta_mgr_.AppendBlockItem(blk_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to meta file failed. data len: %lu.", data.len);
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::GetAllBlockItems(TSEntityID entity_id,
                                         std::vector<TsBlockSegmentBlockItem>* blk_items) {
  return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
}

KStatus TsBlockSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                      std::list<std::shared_ptr<TsSegmentBlockSpan>>* blocks) {
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                      std::vector<TsBlockSpan>* blocks) {
  return meta_mgr_.GetBlockSpans(filter, this, blocks);
}

KStatus TsBlockSegment::GetColumnBlock(uint32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                                       TsBlockSegmentBlock* block) {
  // init block info
  if (block->GetBlockInfo().col_block_offset.empty()) {
    TSSlice buffer;
    buffer.len = sizeof(uint32_t) * (block->GetNCols() + 1);
    buffer.data = new char[buffer.len];
    Defer defer {[&]() { delete[] buffer.data; }};
    KStatus s = block_file_.ReadData(block->GetBlockOffset(), buffer.data, buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSegment::GetColumnBlock read block info data failed")
      return s;
    }
    s = block->LoadBlockInfo({buffer.data, buffer.len});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSegment::GetColumnBlock block info init failed")
      return s;
    }
  }
  // init column block
  if (!block->HasColumnData(col_idx)) {
    uint32_t start_offset = block->GetBlockInfo().col_block_offset[col_idx + 1];
    uint32_t end_offset = block->GetBlockInfo().col_block_offset[col_idx + 2];
    TSSlice buffer;
    buffer.len = end_offset - start_offset;
    buffer.data = new char[buffer.len];
    Defer defer {[&]() { delete[] buffer.data; }};
    KStatus s = block_file_.ReadData(block->GetBlockOffset() + start_offset, buffer.data, buffer.len);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSegment::GetColumnBlock read column[%u] block data failed", col_idx + 1);
      return s;
    }
    s = block->LoadColData(col_idx, metric_schema, {buffer.data, buffer.len});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSegment::GetColumnBlock column[%u] block init failed", col_idx + 1);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBuilder::BuildAndFlush() {
  KStatus s;
  // 1. The iterator will be used to read MAX_COMPACT_NUM last segment data
  TsLastSegmentBlockSpan block_span;
  bool is_finished = false;
  TsEngineSchemaManager* schema_mgr = partition_->GetSchemaMgr();
  // 2. Create a new last segment
  std::unique_ptr<TsFile> last_segment;
  uint32_t file_number;
  s = partition_->NewLastSegmentFile(&last_segment, &file_number);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, new last segment failed.")
    return s;
  }
  TsLastSegmentBuilder builder(schema_mgr, std::move(last_segment), file_number);
  // 3. Traverse the last segment data and write the data to the block segment
  TsLastSegmentsMergeIterator iter(last_segments_);
  iter.Init();
  TsEntityKey entity_key;
  std::shared_ptr<TsBlockSegmentBlock> block = nullptr;
  while (true) {
    if (block_span.end_row <= block_span.start_row) {
      s = iter.Next(&block_span, is_finished);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, iterate last segments failed.")
        return s;
      }
      if (is_finished) {
        break;
      }
    }
    TsEntityKey cur_entity_key = {block_span.table_id, block_span.table_version, block_span.entity_id};
    if (entity_key != cur_entity_key) {
      if (block && block->HasData()) {
        if (block->GetRowNum() >= EngineOptions::min_rows_per_block) {
          s = block->Flush(partition_);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, flush block failed.")
            return s;
          }
          block->Clear();
        } else {
          // Writes the incomplete data back to the last segment
          for (uint32_t row_idx = 0; row_idx < block->GetRowNum(); ++row_idx) {
            uint64_t seq_no = block->GetSeqNo(row_idx);
            std::vector<TSSlice> metric_value;
            block->GetMetricValue(row_idx, metric_value);
            s = builder.PutColData(entity_key.table_id, entity_key.table_version, entity_key.entity_id, seq_no,
                                   metric_value);
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder put failed.")
              return s;
            }
          }
          s = builder.FlushBuffer();
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder flush buffer failed.")
            return s;
          }
        }
      }
      // Get the metric schema
      std::vector<AttributeInfo> metric_schema;
      if (!block || entity_key.table_id != cur_entity_key.table_id ||
          entity_key.table_version != cur_entity_key.table_version) {
        std::shared_ptr<MMapMetricsTable> table_schema_;
        s = schema_mgr->GetTableMetricSchema({}, block_span.table_id, block_span.table_version, &table_schema_);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("get table schema failed. table id: %u, table version: %u.",
                    block_span.table_id, block_span.table_version);
          return s;
        }
        metric_schema = table_schema_->getSchemaInfoExcludeDropped();
      } else {
        metric_schema = block->GetMetricSchema();
      }
      // init the block segment block
      block = std::make_shared<TsBlockSegmentBlock>(block_span.table_id, block_span.table_version,
                                                      block_span.entity_id, metric_schema);
      entity_key = cur_entity_key;
    }

    // write data to block buffer
    bool is_full = false;
    s = block->Append(block_span, is_full);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, append block failed.")
      return s;
    }
    // flush block if full
    if (is_full) {
      s = block->Flush(partition_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, flush block failed.")
        return s;
      }
      block->Clear();
    }
  }
  // 4. Writes the incomplete data back to the last segment
  if (block && block->HasData()) {
    for (uint32_t row_idx = 0; row_idx < block->GetRowNum(); ++row_idx) {
      uint64_t seq_no = block->GetSeqNo(row_idx);
      std::vector<TSSlice> metric_value;
      block->GetMetricValue(row_idx, metric_value);
      s = builder.PutColData(entity_key.table_id, entity_key.table_version, entity_key.entity_id, seq_no, metric_value);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder put failed.")
        return s;
      }
    }
  }
  // 5. flush the last segment block
  s = builder.Finalize();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder finalize failed.")
    return s;
  }
  partition_->PublicLastSegment(builder.GetFileNumber());
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
