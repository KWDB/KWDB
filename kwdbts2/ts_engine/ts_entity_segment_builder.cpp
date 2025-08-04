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

#include "ts_entity_segment_builder.h"
#include <utility>
#include "settings.h"
#include "ts_agg.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_filename.h"
#include "ts_batch_data_worker.h"
#include "ts_version.h"

namespace kwdbts {

KStatus TsEntitySegmentEntityItemFileBuilder::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  if (env->NewAppendOnlyFile(file_path_, &w_file_, true, -1) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentEntityItemFileBuilder NewAppendOnlyFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentEntityItemFileBuilder::AppendEntityItem(TsEntityItem& entity_item) {
  assert(w_file_->GetFileSize() == (entity_item.entity_id - 1) * sizeof(TsEntityItem));
  KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char *>(&entity_item), sizeof(entity_item)});
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("write entity item[id=%lu] failed.", entity_item.entity_id);
  }
  assert(w_file_->GetFileSize() == entity_item.entity_id * sizeof(TsEntityItem));
  return s;
}

KStatus TsEntitySegmentBlockItemFileBuilder::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  if (env->NewAppendOnlyFile(file_path_, &w_file_, override_, -1) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFile NewAppendOnlyFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (w_file_->GetFileSize() == 0) {
    header_.status = TsFileStatus::READY;
    header_.magic = TS_ENTITY_SEGMENT_BLOCK_ITEM_FILE_MAGIC;
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsBlockItemFileHeader)});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBlockItemFileBuilder append failed, file_path=%s", file_path_.c_str())
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBlockItemFileBuilder::AppendBlockItem(TsEntitySegmentBlockItem& block_item) {
  assert((w_file_->GetFileSize() - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  block_item.block_id = (w_file_->GetFileSize() - sizeof(TsBlockItemFileHeader)) / sizeof(TsEntitySegmentBlockItem) + 1;
  KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char *>(&block_item), sizeof(TsEntitySegmentBlockItem)});
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockItemFileBuilder append failed, file_path=%s", file_path_.c_str())
  }
  assert((w_file_->GetFileSize() - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  return s;
}

KStatus TsEntitySegmentBlockFileBuilder::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  if (env->NewAppendOnlyFile(file_path_, &w_file_, override_, -1) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockFileBuilder NewAppendOnlyFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (w_file_->GetFileSize() == 0) {
    header_.status = TsFileStatus::READY;
    header_.magic = TS_ENTITY_SEGMENT_BLOCK_FILE_MAGIC;
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsAggAndBlockFileHeader)});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBlockFileBuilder append failed, file_path=%s", file_path_.c_str())
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBlockFileBuilder::AppendBlock(const TSSlice& block, uint64_t* offset) {
  *offset = w_file_->GetFileSize();
  w_file_->Append(block);
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentAggFileBuilder::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  if (env->NewAppendOnlyFile(file_path_, &w_file_, override_, -1) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentAggFile NewAppendOnlyFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (w_file_->GetFileSize() == 0) {
    header_.status = TsFileStatus::READY;
    header_.magic = TS_ENTITY_SEGMENT_AGG_FILE_MAGIC;
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsAggAndBlockFileHeader)});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentAggFile append failed, file_path=%s", file_path_.c_str())
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentAggFileBuilder::AppendAggBlock(const TSSlice& agg, uint64_t* offset) {
  *offset = w_file_->GetFileSize();
  w_file_->Append(agg);
  return SUCCESS;
}

TsEntityBlockBuilder::TsEntityBlockBuilder(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                                           std::vector<AttributeInfo>& metric_schema)
                                           : table_id_(table_id), table_version_(table_version), entity_id_(entity_id),
                                           metric_schema_(metric_schema) {
  n_cols_ = metric_schema.size() + 1;
  column_blocks_.resize(n_cols_);
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsEntitySegmentColumnBlock& column_block = column_blocks_[col_idx];
    column_block.bitmap.Reset(EngineOptions::max_rows_per_block);
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize(EngineOptions::max_rows_per_block * sizeof(uint32_t));
    }
  }
  block_info_.col_block_offset.resize(n_cols_);
  block_info_.col_agg_offset.resize(n_cols_ - 1);
}

uint64_t TsEntityBlockBuilder::GetLSN(uint32_t row_idx) {
  return *reinterpret_cast<uint64_t*>(column_blocks_[0].buffer.data() + row_idx * sizeof(uint64_t));
}

timestamp64 TsEntityBlockBuilder::GetTimestamp(uint32_t row_idx) {
  return *reinterpret_cast<timestamp64*>(column_blocks_[1].buffer.data() + row_idx * sizeof(timestamp64));
}

KStatus TsEntityBlockBuilder::GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value,
                                             std::vector<DataFlags>& data_flags) {
  for (int col_idx = 1; col_idx < n_cols_; ++col_idx) {
    char* ptr = column_blocks_[col_idx].buffer.data();
    if (isVarLenType(metric_schema_[col_idx - 1].type)) {
      uint32_t start_offset = 0;
      if (row_idx != 0) {
        start_offset = *reinterpret_cast<uint32_t *>(ptr + (row_idx - 1) * sizeof(uint32_t));
      }
      uint32_t end_offset = *reinterpret_cast<uint32_t*>(ptr + row_idx * sizeof(uint32_t));
      uint32_t var_data_offset = EngineOptions::max_rows_per_block * sizeof(uint32_t);
      value.push_back({ptr + var_data_offset + start_offset, end_offset - start_offset});
    } else {
      size_t d_size = col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
      value.push_back({ptr + row_idx * d_size, d_size});
    }
    data_flags.push_back(column_blocks_[col_idx].bitmap[row_idx]);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityBlockBuilder::Append(shared_ptr<TsBlockSpan> span, bool& is_full) {
  size_t written_rows = span->GetRowNum() + n_rows_ > EngineOptions::max_rows_per_block ?
                        EngineOptions::max_rows_per_block - n_rows_ : span->GetRowNum();
  assert(span->GetRowNum() >= written_rows);
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    size_t d_size = col_idx == 0 ? 8 : metric_schema_[col_idx - 1].size;
    // lsn column do not contain bitmaps
    bool has_bitmap = col_idx != 0;

    bool is_var_col = isVarLenType(d_type);
    TsEntitySegmentColumnBlock& block = column_blocks_[col_idx];
    std::string var_offsets_data;
    uint32_t var_offsets_len = EngineOptions::max_rows_per_block * sizeof(uint32_t);
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
        if (data_flag == kValid) {
          block.buffer.append(value.data, value.len);
          block.var_rows.emplace_back(value.data, value.len);
        }
        uint32_t var_offset = block.buffer.size() - var_offsets_len;
        memcpy(block.buffer.data() + row_idx_in_block * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
      } else if (col_idx == 1) {
        block.buffer.append(col_val + span_row_idx * d_size, sizeof(timestamp64));
      }
      row_idx_in_block++;
    }
    if (!is_var_col) {
      if (col_idx == 0) {
        char* lsn_col_value = reinterpret_cast<char *>(span->GetLSNAddr(0));
        block.buffer.append(lsn_col_value, written_rows * d_size);
      } else if (col_idx != 1) {
        block.buffer.append(col_val, written_rows * d_size);
      }
    }
  }
  n_rows_ += written_rows;
  span->TrimFront(written_rows);
  is_full = n_rows_ == EngineOptions::max_rows_per_block;
  return KStatus::SUCCESS;
}

KStatus TsEntityBlockBuilder::GetCompressData(TsEntitySegmentBlockItem& blk_item, string& data_buffer, string& agg_buffer) {
  // compressor manager
  const auto& mgr = CompressorManager::GetInstance();
  // init col data offsets to data buffer
  uint32_t block_header_size = n_cols_ * sizeof(uint32_t);
  data_buffer.resize(block_header_size);
  // init col agg offsets to agg buffer, exclude lsn col
  uint32_t agg_header_size = (n_cols_ - 1) * sizeof(uint32_t);
  agg_buffer.resize(agg_header_size);
  // min lsn && max lsn
  TS_LSN min_lsn = UINT64_MAX;
  TS_LSN max_lsn = 0;
  TS_LSN first_lsn = 0;
  TS_LSN last_lsn = 0;

  // write column block data and column agg
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    size_t d_size = col_idx == 0 ? 8 : metric_schema_[col_idx - 1].size;
    bool has_bitmap = col_idx > 1;
    bool is_var_col = isVarLenType(d_type);

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
    auto [first, second] = mgr.GetDefaultAlgorithm(d_type);
    if (is_var_col) {
      // varchar use Gorilla algorithm
      first = TsCompAlg::kChimp_32;
      // var offset data
      std::string compressed;
      TSSlice var_offsets = {block.buffer.data(), n_rows_ * sizeof(uint32_t)};
      bool ok = mgr.CompressData(var_offsets, nullptr, n_rows_, &compressed, first, second);
      if (!ok) {
        LOG_ERROR("Compress var offset data failed");
        return KStatus::SUCCESS;
      }
      uint32_t compressed_len = compressed.size();
      data_buffer.append(reinterpret_cast<const char *>(&compressed_len), sizeof(uint32_t));
      data_buffer.append(compressed);
      // var data
      compressed.clear();
      uint32_t var_data_offset = EngineOptions::max_rows_per_block * sizeof(uint32_t);
      ok = mgr.CompressVarchar({block.buffer.data() + var_data_offset, block.buffer.size() - var_data_offset},
                               &compressed, GenCompAlg::kSnappy);
      if (!ok) {
        LOG_ERROR("Compress var data failed");
        return KStatus::SUCCESS;
      }
      data_buffer.append(compressed);
    } else {
      std::string compressed;
      TSSlice plain{block.buffer.data(), block.buffer.size()};
      mgr.CompressData(plain, b, n_rows_, &compressed, first, second);
      data_buffer.append(compressed);
    }
    // record col offset
    block_info_.col_block_offset[col_idx] = data_buffer.size() - block_header_size;
    // write col data offset
    memcpy(data_buffer.data() + col_idx * sizeof(uint32_t), &(block_info_.col_block_offset[col_idx]), sizeof(uint32_t));
    // calculate aggregate
    if (0 == col_idx) {
      for (int row_idx = 0; row_idx < n_rows_; ++row_idx) {
        TS_LSN* lsn = reinterpret_cast<TS_LSN *>(block.buffer.data() + row_idx * sizeof(TS_LSN));
        if (min_lsn > *lsn) {
          min_lsn = *lsn;
        }
        if (max_lsn < *lsn) {
          max_lsn = *lsn;
        }
      }
      first_lsn = *reinterpret_cast<TS_LSN *>(block.buffer.data());
      last_lsn = *reinterpret_cast<TS_LSN *>(block.buffer.data() + (n_rows_ - 1) * sizeof(TS_LSN));
      continue;
    }
    string col_agg;
    Defer defer {[&]() {
      agg_buffer.append(col_agg);
      uint32_t offset = agg_buffer.size() - agg_header_size;
      memcpy(agg_buffer.data() + (col_idx - 1) * sizeof(uint32_t), &offset, sizeof(uint32_t));
    }};
    if (!is_var_col) {
      TsBitmap* bitmap = nullptr;
      if (has_bitmap) {
        bitmap = &block.bitmap;
      }
      uint16_t count = 0;
      string max, min, sum;
      int32_t col_size = metric_schema_[col_idx - 1].size;
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
  }

  // flush
  timestamp64 min_ts = GetTimestamp(0);
  timestamp64 max_ts = GetTimestamp(n_rows_ - 1);

  blk_item.entity_id = entity_id_;
  blk_item.table_version = table_version_;
  blk_item.n_cols = n_cols_;
  blk_item.n_rows = n_rows_;
  blk_item.max_ts = max_ts;
  blk_item.min_ts = min_ts;
  blk_item.max_lsn = max_lsn;
  blk_item.min_lsn = min_lsn;
  blk_item.first_lsn = first_lsn;
  blk_item.last_lsn = last_lsn;
  blk_item.block_len = data_buffer.size();
  blk_item.agg_len = agg_buffer.size();

  return KStatus::SUCCESS;
}

void TsEntityBlockBuilder::Clear() {
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
      column_block.buffer.resize(EngineOptions::max_rows_per_block * sizeof(uint32_t));
    }
    column_block.agg.clear();
    column_block.var_rows.clear();
  }
  block_info_.col_agg_offset.clear();
  block_info_.col_agg_offset.resize(n_cols_ - 1);
  block_info_.col_block_offset.clear();
  block_info_.col_block_offset.resize(n_cols_);
  min_lsn_ = UINT64_MAX;
  max_lsn_ = 0;
}

KStatus TsEntitySegmentBuilder::NewLastSegmentFile(std::unique_ptr<TsAppendOnlyFile>* file,
                                                   uint64_t* file_number) {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  *file_number = version_manager_->NewFileNumber();
  auto filepath = root_path_ / LastSegmentFileName(*file_number);
  LOG_INFO("Last Segment %s created by Compaction", filepath.string().c_str());
  return env->NewAppendOnlyFile(filepath, file);
}

KStatus TsEntitySegmentBuilder::UpdateEntityItem(TsEntityKey& entity_key, TsEntitySegmentBlockItem& block_item) {
  KStatus s = KStatus::SUCCESS;
  if (cur_entity_item_.entity_id != entity_key.entity_id) {
    if (cur_entity_item_.entity_id != 0) {
      s = entity_item_builder_->AppendEntityItem(cur_entity_item_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append entity item failed.")
        return s;
      }
    }
    for (uint64_t entity_id = cur_entity_item_.entity_id + 1; entity_id < entity_key.entity_id; ++entity_id) {
      cur_entity_item_ = {entity_id};
      if (cur_entity_segment_) {
        bool is_exist = true;
        s = cur_entity_segment_->GetEntityItem(entity_id, cur_entity_item_, is_exist);
        if (s != KStatus::SUCCESS && is_exist) {
          LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
          return s;
        }
      }
      s = entity_item_builder_->AppendEntityItem(cur_entity_item_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append entity item failed.")
        return s;
      }
    }
    cur_entity_item_ = {entity_key.entity_id};
    if (cur_entity_segment_) {
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_key.entity_id, cur_entity_item_, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
        return s;
      }
    }
  }
  block_item.prev_block_id = cur_entity_item_.cur_block_id;
  s = block_item_builder_->AppendBlockItem(block_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append block item failed.")
    return s;
  }
  cur_entity_item_.cur_block_id = block_item.block_id;
  cur_entity_item_.row_written += block_item.n_rows;
  if (block_item.max_ts > cur_entity_item_.max_ts) {
    cur_entity_item_.max_ts = block_item.max_ts;
  }
  if (block_item.min_ts < cur_entity_item_.min_ts) {
    cur_entity_item_.min_ts = block_item.min_ts;
  }
  return s;
}

KStatus TsEntitySegmentBuilder::WriteBlock(TsEntityKey& entity_key) {
  string data_buffer;
  string agg_buffer;
  TsEntitySegmentBlockItem block_item;
  KStatus s = block_->GetCompressData(block_item, data_buffer, agg_buffer);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get block compress data failed.")
    return s;
  }
  s = block_file_builder_->AppendBlock({data_buffer.data(), data_buffer.size()}, &block_item.block_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append block failed.")
    return s;
  }
  s = agg_file_builder_->AppendAggBlock({agg_buffer.data(), agg_buffer.size()}, &block_item.agg_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append agg block failed.")
  }
  s = UpdateEntityItem(entity_key, block_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, update entity item failed.")
  }
  return s;
}

KStatus TsEntitySegmentBuilder::Open() {
  KStatus s = entity_item_builder_->Open();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Open Entity Item File Failed");
    return s;
  }
  s = block_item_builder_->Open();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Open Block Item File Failed");
    return s;
  }
  s = block_file_builder_->Open();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Open Block File Failed");
    return s;
  }
  s = agg_file_builder_->Open();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Open Agg File Failed");
  }
  return s;
}

KStatus TsEntitySegmentBuilder::WriteCachedBlockSpan(TsEntityKey& entity_key) {
  KStatus s = KStatus::SUCCESS;
  while (!cached_spans_.empty()) {
    if (cached_spans_.front()->GetRowNum() == 0) {
      cached_spans_.pop_front();
      continue;
    }

    if (cached_count_ < EngineOptions::min_rows_per_block) {
      if (builder_ == nullptr) {
        uint64_t file_number;
        std::unique_ptr<TsAppendOnlyFile> last_segment;
        s = NewLastSegmentFile(&last_segment, &file_number);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("TsEntitySegmentBuilder::Compact failed, new last segment failed.")
          return s;
        }
        builder_ = std::make_unique<TsLastSegmentBuilder>(schema_manager_, std::move(last_segment), file_number);
      }
      // Writes the incomplete data back to the last segment
      auto row_num = cached_spans_.front()->GetRowNum();
      s = builder_->PutBlockSpan(cached_spans_.front());
      cached_count_ -= row_num;
      cached_spans_.pop_front();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, TsLastSegmentBuilder put failed.")
        return s;
      }
      continue;
    }

    bool is_full = false;
    s = block_->Append(cached_spans_.front(), is_full);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append block failed.")
      return s;
    }
    if (is_full) {
      auto row_num = block_->GetRowNum();
      s = WriteBlock(entity_key);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write block failed.")
        return s;
      }
      cached_count_ -= row_num;
      block_->Clear();
    }
  }
  if (block_->HasData()) {
    auto row_num = block_->GetRowNum();
    s = WriteBlock(entity_key);
    if (s == FAIL) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write block failed.")
      return s;
    }
    cached_count_ -= row_num;
    block_->Clear();
  }
  assert(cached_count_ == 0);
  return s;
}

KStatus TsEntitySegmentBuilder::Compact(TsVersionUpdate* update) {
  // assert(EngineOptions::min_rows_per_block < EngineOptions::max_rows_per_block);
  std::unique_lock lock{mutex_};
  KStatus s;
  shared_ptr<TsBlockSpan> block_span{nullptr};
  bool is_finished = false;
  // 1. Traverse the last segment data and write the data to the block segment
  std::vector<std::list<shared_ptr<TsBlockSpan>>> block_spans;
  block_spans.resize(last_segments_.size());
  for (int i = 0; i < last_segments_.size(); ++i) {
    s = last_segments_[i]->GetBlockSpans(block_spans[i], schema_manager_);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get block spans failed.")
      return s;
    }
  }
  TsBlockSpanSortedIterator iter(block_spans, EngineOptions::g_dedup_rule);
  block_spans.clear();
  iter.Init();
  TsEntityKey entity_key;
  while (true) {
    s = iter.Next(block_span, &is_finished);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, iterate last segments failed.")
      return s;
    }
    if (is_finished) {
      break;
    }
    TsEntityKey cur_entity_key = {block_span->GetTableID(), block_span->GetTableVersion(), block_span->GetEntityID()};
    if (entity_key == TsEntityKey{}) {
      entity_key = cur_entity_key;
      std::shared_ptr<MMapMetricsTable> table_schema_;
      s = schema_manager_->GetTableMetricSchema({}, entity_key.table_id, entity_key.table_version, &table_schema_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get table schema failed. table id: %lu, table version: %u.", block_span->GetTableID(),
                  block_span->GetTableVersion());
        return s;
      }
      std::vector<AttributeInfo> metric_schema = table_schema_->getSchemaInfoExcludeDropped();
      block_ = std::make_shared<TsEntityBlockBuilder>(entity_key.table_id, entity_key.table_version,
                                                     entity_key.entity_id, metric_schema);
    }
    if (cur_entity_key == entity_key) {
      cached_count_ += block_span->GetRowNum();
      cached_spans_.push_back(std::move(block_span));
      continue;
    }

    s = WriteCachedBlockSpan(entity_key);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write cached block span failed.")
      return s;
    }

    std::vector<AttributeInfo> metric_schema;
    if (entity_key.table_id != cur_entity_key.table_id ||
        entity_key.table_version != cur_entity_key.table_version) {
      std::shared_ptr<MMapMetricsTable> table_schema_;
      s = schema_manager_->GetTableMetricSchema({}, cur_entity_key.table_id, cur_entity_key.table_version,
                                                &table_schema_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get table schema failed. table id: %lu, table version: %u.", cur_entity_key.table_id,
                  cur_entity_key.table_version);
        return s;
      }
      metric_schema = table_schema_->getSchemaInfoExcludeDropped();
    } else {
      metric_schema = block_->GetMetricSchema();
    }

    block_ = std::make_shared<TsEntityBlockBuilder>(cur_entity_key.table_id, cur_entity_key.table_version,
                                                   cur_entity_key.entity_id, metric_schema);
    entity_key = cur_entity_key;
    cached_count_ += block_span->GetRowNum();
    cached_spans_.push_back(std::move(block_span));
  }

  s = WriteCachedBlockSpan(entity_key);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write cached block span failed.")
    return s;
  }

  if (cur_entity_item_.entity_id != 0) {
    entity_item_builder_->AppendEntityItem(cur_entity_item_);
  }
  if (cur_entity_segment_) {
    uint64_t max_entity_id = cur_entity_segment_->GetEntityNum();
    for (uint64_t entity_id = cur_entity_item_.entity_id + 1; entity_id <= max_entity_id; ++entity_id) {
      cur_entity_item_ = {entity_id};
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_id, cur_entity_item_, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
        return s;
      }
      s = entity_item_builder_->AppendEntityItem(cur_entity_item_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append entity item failed.")
        return s;
      }
    }
  }

  // 4. flush the last segment block
  if (builder_ != nullptr) {
    s = builder_->Finalize();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR(
        "TsEntitySegmentBuilder::Compact failed, TsLastSegmentBuilder finalize failed.")
      return s;
    }
    update->AddLastSegment(partition_id_, builder_->GetFileNumber());
  }

  TsVersionUpdate::EntitySegmentVersionInfo info;
  info.agg_file_size = agg_file_builder_->GetFileSize();
  info.block_file_size = block_file_builder_->GetFileSize();
  info.header_b_size = block_item_builder_->GetFileSize();
  info.header_e_file_number = entity_item_builder_->GetFileNumber();
  assert((info.header_b_size - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  update->SetEntitySegment(partition_id_, info);
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBuilder::WriteBatch(uint32_t entity_id, uint32_t table_version, TS_LSN lsn, TSSlice block_data) {
  std::unique_lock lock{mutex_};
  auto it = entity_items_.find(entity_id);
  if (it == entity_items_.end()) {
    TsEntityItem entity_item{entity_id};
    if (cur_entity_segment_) {
      bool is_exist = true;
      KStatus s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("entity item[%d] not found", entity_id);
        return s;
      }
    }
    entity_items_[entity_id] = entity_item;
  }

  uint32_t block_data_header_size = TsBatchData::block_span_data_header_size_;
  uint32_t n_cols = *reinterpret_cast<uint32_t*>(block_data.data + TsBatchData::n_cols_offset_in_span_data_);
  uint32_t n_rows = *reinterpret_cast<uint32_t*>(block_data.data +  + TsBatchData::n_rows_offset_in_span_data_);

  TsEntitySegmentBlockItem block_item;
  block_item.entity_id = entity_id;
  block_item.prev_block_id = entity_items_[entity_id].cur_block_id;  // pre block item id
  block_item.table_version = table_version;
  block_item.n_cols = n_cols;
  block_item.n_rows = n_rows;
  block_item.block_len = *reinterpret_cast<uint32_t*>(block_data.data + block_data_header_size
                         + (n_cols - 1) * sizeof(uint32_t)) + sizeof(uint32_t) * n_cols;
  block_item.min_ts = *reinterpret_cast<timestamp64*>(block_data.data + TsBatchData::min_ts_offset_in_span_data_);
  block_item.max_ts = *reinterpret_cast<timestamp64*>(block_data.data + TsBatchData::max_ts_offset_in_span_data_);
  block_item.min_lsn = lsn;
  block_item.max_lsn = lsn;
  block_item.first_lsn = lsn;
  block_item.last_lsn = lsn;
  block_item.agg_len = *reinterpret_cast<uint32_t*>(block_data.data + block_data_header_size + block_item.block_len
                       + (n_cols - 2) * sizeof(uint32_t))  + sizeof(uint32_t) * (n_cols - 1);

  TSSlice data_buffer = {block_data.data + block_data_header_size, block_item.block_len};
  KStatus s = block_file_builder_->AppendBlock(data_buffer, &block_item.block_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::WriteBatch failed, append block failed.")
    return s;
  }

  TSSlice agg_buffer = {block_data.data + block_data_header_size + block_item.block_len, block_item.agg_len};
  assert(block_data.len - (block_data_header_size + block_item.block_len) == block_item.agg_len);
  s = agg_file_builder_->AppendAggBlock(agg_buffer, &block_item.agg_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::WriteBatch failed, append agg block failed.")
  }

  s = block_item_builder_->AppendBlockItem(block_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::WriteBatch failed, append block item failed.")
    return s;
  }

  entity_items_[entity_id].cur_block_id = block_item.block_id;
  entity_items_[entity_id].row_written += block_item.n_rows;
  if (block_item.max_ts > entity_items_[entity_id].max_ts) {
    entity_items_[entity_id].max_ts = block_item.max_ts;
  }
  if (block_item.min_ts < entity_items_[entity_id].min_ts) {
    entity_items_[entity_id].min_ts = block_item.min_ts;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBuilder::WriteBatchFinish(TsVersionUpdate *update) {
  std::unique_lock lock{mutex_};
  // write entity header
  KStatus s = KStatus::SUCCESS;
  uint32_t cur_entity_id = 0;
  for (auto kv : entity_items_) {
    for (uint32_t entity_id = cur_entity_id + 1; entity_id < kv.first; ++entity_id) {
      TsEntityItem entity_item{entity_id};
      if (cur_entity_segment_) {
        bool is_exist = true;
        s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
        if (s != KStatus::SUCCESS && is_exist) {
          LOG_ERROR("GetEntityItem[entity_id=%d] failed", entity_id)
          return s;
        }
      }
      s = entity_item_builder_->AppendEntityItem(entity_item);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AppendEntityItem[entity_id=%d] failed", entity_id)
        return s;
      }
    }
    s = entity_item_builder_->AppendEntityItem(kv.second);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("AppendEntityItem[entity_id=%d] failed", kv.first)
      return s;
    }
    cur_entity_id = kv.first;
  }
  if (cur_entity_segment_) {
    for (uint32_t entity_id = cur_entity_id + 1; entity_id < cur_entity_segment_->GetEntityNum(); ++entity_id) {
      TsEntityItem entity_item{entity_id};
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("GetEntityItem[entity_id=%d] failed", entity_id)
        return s;
      }
      s = entity_item_builder_->AppendEntityItem(entity_item);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AppendEntityItem[entity_id=%d] failed", entity_id)
        return s;
      }
    }
  }

  TsVersionUpdate::EntitySegmentVersionInfo info;
  info.agg_file_size = agg_file_builder_->GetFileSize();
  info.block_file_size = block_file_builder_->GetFileSize();
  info.header_b_size = block_item_builder_->GetFileSize();
  info.header_e_file_number = entity_item_builder_->GetFileNumber();
  assert((info.header_b_size - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  update->SetEntitySegment(partition_id_, info);
  return KStatus::SUCCESS;
}

void TsEntitySegmentBuilder::MarkDelete() {
  entity_item_builder_->MarkDelete();
}

}  //  namespace kwdbts
