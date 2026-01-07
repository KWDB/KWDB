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
#include <sys/types.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <utility>


#include "data_type.h"
#include "kwdb_type.h"
#include "settings.h"
#include "ts_agg.h"
#include "ts_batch_data_worker.h"
#include "ts_block_span_sorted_iterator.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_compatibility.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_data.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lastsegment_builder.h"
#include "ts_sliceguard.h"
#include "ts_version.h"

namespace kwdbts {

KStatus TsEntitySegmentEntityItemFileBuilder::Open() {
  if (io_env_->NewAppendOnlyFile(file_path_, &w_file_, true, -1) != KStatus::SUCCESS) {
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
  if (io_env_->NewAppendOnlyFile(file_path_, &w_file_, false, file_size_) != KStatus::SUCCESS) {
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
  if (io_env_->NewAppendOnlyFile(file_path_, &w_file_, false, file_size_) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockFileBuilder NewAppendOnlyFile failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  if (w_file_->GetFileSize() == 0) {
    header_.status = TsFileStatus::READY;
    header_.magic = TS_ENTITY_SEGMENT_BLOCK_FILE_MAGIC;
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char*>(&header_), sizeof(TsAggAndBlockFileHeader)});
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
  if (io_env_->NewAppendOnlyFile(file_path_, &w_file_, false, file_size_) != KStatus::SUCCESS) {
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
    TsEntitySegmentColumnBlockBuilder& column_block = column_blocks_[col_idx];
    column_block.bitmap = std::make_unique<TsBitmap>(EngineOptions::max_rows_per_block);
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize(EngineOptions::max_rows_per_block * sizeof(uint32_t));
    }
  }
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
    data_flags.push_back(column_blocks_[col_idx].bitmap->At(row_idx));
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
    // osn column do not contain bitmaps
    bool has_bitmap = col_idx != 0;

    bool is_var_col = isVarLenType(d_type);
    TsEntitySegmentColumnBlockBuilder& block = column_blocks_[col_idx];
    std::string var_offsets_data;
    uint32_t var_offsets_len = EngineOptions::max_rows_per_block * sizeof(uint32_t);
    size_t row_idx_in_block = n_rows_;
    char* col_val = nullptr;
    std::unique_ptr<TsBitmapBase> bitmap;
    if (!is_var_col && has_bitmap) {
      KStatus s = span->GetFixLenColAddr(col_idx - 1, &col_val, &bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetColBitmap failed");
        return s;
      }
    }
    for (size_t span_row_idx = 0; span_row_idx < written_rows; ++span_row_idx) {
      TsBitmap* loc_bitmap = static_cast<TsBitmap*>(block.bitmap.get());
      if (!is_var_col && has_bitmap) {
        (*loc_bitmap)[row_idx_in_block] = bitmap->At(span_row_idx);
      }
      if (is_var_col) {
        DataFlags data_flag;
        TSSlice value;
        KStatus s = span->GetVarLenTypeColAddr(span_row_idx, col_idx - 1, data_flag, value);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetValueSlice failed");
          return s;
        }
        (*loc_bitmap)[row_idx_in_block] = data_flag;
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
        const char* osn_col_value = reinterpret_cast<const char*>(span->GetOSNAddr(0));
        block.buffer.append(osn_col_value, written_rows * d_size);
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

KStatus TsEntityBlockBuilder::GetCompressData(TsEntitySegmentBlockItem& blk_item, TsBufferBuilder* data_buffer,
                                              TsBufferBuilder* agg_buffer) {
  // compressor manager
  const auto& mgr = CompressorManager::GetInstance();
  // init col data offsets to data buffer
  uint32_t block_header_size = n_cols_ * sizeof(uint32_t);
  data_buffer->resize(block_header_size);
  // init col agg offsets to agg buffer, exclude osn col
  uint32_t agg_header_size = (n_cols_ - 1) * sizeof(uint32_t);
  agg_buffer->resize(agg_header_size);
  // min osn && max osn
  uint64_t min_osn = UINT64_MAX;
  uint64_t max_osn = 0;
  uint64_t first_osn = 0;
  uint64_t last_osn = 0;

  // write column block data and column agg
  assert(n_cols_ == metric_schema_.size() + 1);
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    size_t d_size = col_idx == 0 ? 8 : metric_schema_[col_idx - 1].size;
    bool has_bitmap = col_idx > 1;  // && !metric_schema_[col_idx - 1].isFlag(AINFO_NOT_NULL);
    bool is_var_col = isVarLenType(d_type);

    TsEntitySegmentColumnBlockBuilder& block = column_blocks_[col_idx];
    // compress
    // compress bitmap
    if (has_bitmap) {
      TsBitmap* loc_bitmap = static_cast<TsBitmap*>(block.bitmap.get());
      loc_bitmap->Truncate(n_rows_);
      mgr.CompressBitmap(loc_bitmap, data_buffer);
    }
    TsBitmapBase* b = has_bitmap ? block.bitmap.get() : nullptr;
    // compress col data & write to buffer
    auto [first, second] = mgr.GetDefaultAlgorithm(d_type);
    if (is_var_col) {
      // varchar offset use simple8b algorithm
      first = TsCompAlg::kSimple8B_V2_u32;
      // var offset data
      TsBufferBuilder compressed;
      TSSlice var_offsets = {block.buffer.data(), n_rows_ * sizeof(uint32_t)};
      bool ok = mgr.CompressData(var_offsets, nullptr, n_rows_, &compressed, first, second);
      if (!ok) {
        LOG_ERROR("Compress var offset data failed");
        return KStatus::SUCCESS;
      }
      uint32_t compressed_len = compressed.size();
      PutFixed32(data_buffer, compressed_len);
      data_buffer->append(compressed);
      // var data
      compressed.clear();
      uint32_t var_data_offset = EngineOptions::max_rows_per_block * sizeof(uint32_t);
      ok = mgr.CompressVarchar({block.buffer.data() + var_data_offset, block.buffer.size() - var_data_offset},
                               &compressed, GenCompAlg::kSnappy);
      if (!ok) {
        LOG_ERROR("Compress var data failed");
        return KStatus::SUCCESS;
      }
      data_buffer->append(compressed);
    } else {
      TsBufferBuilder compressed;
      TSSlice plain{block.buffer.data(), block.buffer.size()};
      mgr.CompressData(plain, b, n_rows_, &compressed, first, second);
      data_buffer->append(compressed);
    }
    // col offset
    uint32_t col_offset = data_buffer->size() - block_header_size;
    // write col data offset
    memcpy(data_buffer->data() + col_idx * sizeof(uint32_t), &col_offset, sizeof(uint32_t));
    // calculate aggregate
    if (0 == col_idx) {
      for (int row_idx = 0; row_idx < n_rows_; ++row_idx) {
        uint64_t* osn = reinterpret_cast<uint64_t *>(block.buffer.data() + row_idx * sizeof(uint64_t));
        if (min_osn > *osn) {
          min_osn = *osn;
        }
        if (max_osn < *osn) {
          max_osn = *osn;
        }
      }
      first_osn = *reinterpret_cast<uint64_t *>(block.buffer.data());
      last_osn = *reinterpret_cast<uint64_t *>(block.buffer.data() + (n_rows_ - 1) * sizeof(uint64_t));
      continue;
    }
    string col_agg;
    Defer defer {[&]() {
      agg_buffer->append(col_agg);
      uint32_t offset = agg_buffer->size() - agg_header_size;
      memcpy(agg_buffer->data() + (col_idx - 1) * sizeof(uint32_t), &offset, sizeof(uint32_t));
    }};
    if (!is_var_col) {
      TsBitmapBase* bitmap = nullptr;
      if (has_bitmap) {
        bitmap = block.bitmap.get();
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
      auto is_not_null = metric_schema_[col_idx - 1].isFlag(AINFO_NOT_NULL);
      *reinterpret_cast<bool *>(sum.data()) =  aggCalc.CalcAggForFlush(is_not_null, count, max.data(),
                                                                       min.data(), sum.data() + 1);
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
  blk_item.max_osn = max_osn;
  blk_item.min_osn = min_osn;
  blk_item.first_osn = first_osn;
  blk_item.last_osn = last_osn;
  blk_item.block_len = data_buffer->size();
  blk_item.agg_len = agg_buffer->size();

  return KStatus::SUCCESS;
}

void TsEntityBlockBuilder::Clear() {
  n_rows_ = 0;
  if (n_cols_ > 0) {
    column_blocks_[0].buffer.clear();
  }
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsEntitySegmentColumnBlockBuilder& column_block = column_blocks_[col_idx];
    column_block.bitmap = std::make_unique<TsBitmap>(EngineOptions::max_rows_per_block);
    column_block.buffer.clear();
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize(EngineOptions::max_rows_per_block * sizeof(uint32_t));
    }
    column_block.var_rows.clear();
  }
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
      cur_entity_item_ = {};
      if (cur_entity_segment_) {
        bool is_exist = true;
        s = cur_entity_segment_->GetEntityItem(entity_id, cur_entity_item_, is_exist);
        if (s != KStatus::SUCCESS && is_exist) {
          LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
          return s;
        }
      }
      if (cur_entity_item_.entity_id == 0) {
        cur_entity_item_.entity_id = entity_id;
      }
      s = entity_item_builder_->AppendEntityItem(cur_entity_item_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append entity item failed.")
        return s;
      }
    }
    cur_entity_item_ = {};
    if (cur_entity_segment_) {
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_key.entity_id, cur_entity_item_, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
        return s;
      }
    }
    if (cur_entity_item_.entity_id == 0) {
      cur_entity_item_.entity_id = entity_key.entity_id;
    }
    if (cur_entity_item_.table_id == 0) {
      cur_entity_item_.table_id = entity_key.table_id;
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

KStatus TsEntitySegmentBuilder::WriteBlock(TsEntityKey& entity_key, TsSegmentWriteStats* stats) {
  stats->written_blocks++;
  stats->written_rows += block_->GetRowNum();
  TsBufferBuilder data_buffer;
  TsBufferBuilder agg_buffer;
  TsEntitySegmentBlockItem block_item;
  block_item.block_version = CURRENT_BLOCK_VERSION;
  KStatus s = block_->GetCompressData(block_item, &data_buffer, &agg_buffer);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get block compress data failed.")
    return s;
  }
  stats->written_bytes += data_buffer.size();
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

KStatus TsEntitySegmentBuilder::WriteCachedBlockSpan(bool call_by_vacuum, TsEntityKey& entity_key,
                                                     TsSegmentWriteStats* stats) {
  KStatus s = KStatus::SUCCESS;
  while (!cached_spans_.empty()) {
    if (cached_spans_.front()->GetRowNum() == 0) {
      cached_spans_.pop_front();
      continue;
    }

    if (!call_by_vacuum && cached_count_ < EngineOptions::min_rows_per_block) {
      // Writes the incomplete data back to the last segment
      auto row_num = cached_spans_.front()->GetRowNum();
      lastsegment_block_spans_.push_back(std::move(cached_spans_.front()));
      cached_count_ -= row_num;
      cached_spans_.pop_front();
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
      s = WriteBlock(entity_key, stats);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write block failed.")
        return s;
      }
      cached_count_ -= row_num;
      block_->Clear();
    }
  }
  if (block_ && block_->HasData()) {
    auto row_num = block_->GetRowNum();
    s = WriteBlock(entity_key, stats);
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

void TsEntitySegmentBuilder::ReleaseBuilders() {
  entity_item_builder_.reset();
  block_item_builder_.reset();
  block_file_builder_.reset();
  agg_file_builder_.reset();
}

KStatus TsEntitySegmentBuilder::Compact(bool call_by_vacuum, TsVersionUpdate* update,
                                        std::vector<std::shared_ptr<TsBlockSpan>>* residual_spans,
                                        TsSegmentWriteStats* stats) {
  std::unique_lock lock{mutex_};
  KStatus s;
  shared_ptr<TsBlockSpan> block_span{nullptr};
  bool is_finished = false;

  assert(schema_manager_ != nullptr);
  TsBlockSpanSortedIterator iter(std::move(block_spans_), schema_manager_, EngineOptions::g_dedup_rule);
  block_spans_.clear();
  iter.Init();
  TsEntityKey entity_key;
  while (true) {
    s = iter.Next(block_span, &is_finished);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Compact failed, iterate last segments failed.")
      return s;
    }
    if (is_finished) {
      break;
    }
    TsEntityKey cur_entity_key = {block_span->GetTableID(), block_span->GetTableVersion(), block_span->GetEntityID()};
    if (entity_key == TsEntityKey{}) {
      entity_key = cur_entity_key;
      stats->written_devices++;
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

    s = WriteCachedBlockSpan(call_by_vacuum, entity_key, stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentBuilder::Compact failed, write cached block span failed.")
      return s;
    }

    std::vector<AttributeInfo> metric_schema;
    if (entity_key.table_id != cur_entity_key.table_id || entity_key.table_version != cur_entity_key.table_version) {
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
    stats->written_devices++;
    cached_count_ += block_span->GetRowNum();
    cached_spans_.push_back(std::move(block_span));
  }

  s = WriteCachedBlockSpan(call_by_vacuum, entity_key, stats);
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
      cur_entity_item_ = {};
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_id, cur_entity_item_, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, get entity item failed.")
        return s;
      }
      if (cur_entity_item_.entity_id == 0) {
        cur_entity_item_.entity_id = entity_id;
      }
      s = entity_item_builder_->AppendEntityItem(cur_entity_item_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsEntitySegmentBuilder::Compact failed, append entity item failed.")
        return s;
      }
    }
  }

  residual_spans->clear();
  residual_spans->swap(lastsegment_block_spans_);

  EntitySegmentMetaInfo info;
  info.agg_info = agg_file_builder_->GetFileInfo();
  info.datablock_info = block_file_builder_->GetFileInfo();
  info.header_b_info = block_item_builder_->GetFileInfo();
  info.header_e_file_number = entity_item_builder_->GetFileNumber();
  assert((info.header_b_info.length - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  update->SetEntitySegment(partition_id_, info, false);
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBuilder::WriteBatch(TSTableID tbl_id, uint32_t entity_id, uint32_t table_version,
                                           uint32_t batch_version, TSSlice block_data) {
  std::unique_lock lock{mutex_};
  LOG_DEBUG("TsEntitySegmentBuilder WriteBatch begin, root_path: %s, entity_header_file_num: %lu", root_path_.c_str(),
           entity_item_file_number_);
  Defer defer([this]() {
    LOG_DEBUG("TsEntitySegmentBuilder WriteBatch end, root_path: %s, entity_header_file_num: %lu", root_path_.c_str(),
             entity_item_file_number_);
  });
  if (write_batch_finished_) {
    LOG_WARN("TsEntitySegmentBuilder::WriteBatch skip, builder has already finished.");
    return KStatus::SUCCESS;
  }
  auto it = entity_items_.find(entity_id);
  if (it == entity_items_.end()) {
    TsEntityItem entity_item{};
    if (cur_entity_segment_) {
      bool is_exist = true;
      KStatus s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("entity item[%d] not found", entity_id);
        return s;
      }
    }
    if (entity_item.entity_id == 0) {
      entity_item.entity_id = entity_id;
    }
    if (entity_item.table_id == 0) {
      entity_item.table_id = tbl_id;
    }
    entity_items_[entity_id] = entity_item;
  }

  TsEntitySegmentBlockItem block_item;
  uint32_t block_data_header_size = TsBatchData::block_span_data_header_size_;
  if (batch_version == 0) {
    block_data_header_size = 60;
    block_item.block_version = 0;
  } else if (batch_version >= 1 && batch_version < BATCH_VERSION_LIMIT) {
    block_item.block_version =
        *reinterpret_cast<uint32_t*>(block_data.data + TsBatchData::block_version_offset_in_span_data_);
  } else {
    LOG_ERROR("TsEntitySegmentBuilder::WriteBatch failed, invalid batch version: %u", batch_version);
    return FAIL;
  }

  uint32_t n_cols = *reinterpret_cast<uint32_t*>(block_data.data + TsBatchData::n_cols_offset_in_span_data_);
  uint32_t n_rows = *reinterpret_cast<uint32_t*>(block_data.data +  + TsBatchData::n_rows_offset_in_span_data_);

  block_item.entity_id = entity_id;
  block_item.prev_block_id = entity_items_[entity_id].cur_block_id;  // pre block item id
  block_item.table_version = table_version;
  block_item.n_cols = n_cols;
  block_item.n_rows = n_rows;
  block_item.block_len = *reinterpret_cast<uint32_t*>(block_data.data + block_data_header_size
                         + (n_cols - 1) * sizeof(uint32_t)) + sizeof(uint32_t) * n_cols;
  block_item.min_ts = *reinterpret_cast<timestamp64*>(block_data.data + TsBatchData::min_ts_offset_in_span_data_);
  block_item.max_ts = *reinterpret_cast<timestamp64*>(block_data.data + TsBatchData::max_ts_offset_in_span_data_);
  block_item.min_osn = *reinterpret_cast<uint64_t*>(block_data.data + TsBatchData::min_osn_offset_in_span_data_);
  block_item.max_osn = *reinterpret_cast<uint64_t*>(block_data.data + TsBatchData::max_osn_offset_in_span_data_);
  block_item.first_osn = *reinterpret_cast<uint64_t*>(block_data.data + TsBatchData::first_osn_offset_in_span_data_);
  block_item.last_osn = *reinterpret_cast<uint64_t*>(block_data.data + TsBatchData::last_osn_offset_in_span_data_);

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
  TsEntityCountStats info = {tbl_id, entity_id, block_item.min_ts, block_item.max_ts, block_item.n_rows, true, ""};
  flush_infos_.emplace_back(info);
  return KStatus::SUCCESS;
}

KStatus TsEntitySegmentBuilder::WriteBatchFinish(TsVersionUpdate *update) {
  write_batch_finished_ = true;
  std::unique_lock lock{mutex_};
  LOG_DEBUG("TsEntitySegmentBuilder WriteBatchFinish begin, root_path: %s, entity_header_file_num: %lu", root_path_.c_str(),
           entity_item_file_number_);
  Defer defer([&]() {
    LOG_DEBUG("TsEntitySegmentBuilder WriteBatchFinish end, root_path: %s, update info: %s",
             root_path_.c_str(), update->DebugStr().c_str());
    ReleaseBuilders();
  });
  // write entity header
  KStatus s = KStatus::SUCCESS;
  uint32_t cur_entity_id = 0;
  for (auto kv : entity_items_) {
    for (uint32_t entity_id = cur_entity_id + 1; entity_id < kv.first; ++entity_id) {
      TsEntityItem entity_item{};
      if (cur_entity_segment_) {
        bool is_exist = true;
        s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
        if (s != KStatus::SUCCESS && is_exist) {
          LOG_ERROR("GetEntityItem[entity_id=%d] failed", entity_id)
          return s;
        }
      }
      if (entity_item.entity_id == 0) {
        entity_item.entity_id = entity_id;
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
      TsEntityItem entity_item{};
      bool is_exist = true;
      s = cur_entity_segment_->GetEntityItem(entity_id, entity_item, is_exist);
      if (s != KStatus::SUCCESS && is_exist) {
        LOG_ERROR("GetEntityItem[entity_id=%d] failed", entity_id)
        return s;
      }
      if (entity_item.entity_id == 0) {
        entity_item.entity_id = entity_id;
      }
      s = entity_item_builder_->AppendEntityItem(entity_item);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("AppendEntityItem[entity_id=%d] failed", entity_id)
        return s;
      }
    }
  }

  EntitySegmentMetaInfo info;
  info.agg_info = agg_file_builder_->GetFileInfo();
  info.datablock_info = block_file_builder_->GetFileInfo();
  info.header_b_info = block_item_builder_->GetFileInfo();
  info.header_e_file_number = entity_item_builder_->GetFileNumber();
  assert((info.header_b_info.length - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
  update->SetEntitySegment(partition_id_, info, false);
  return KStatus::SUCCESS;
}

void TsEntitySegmentBuilder::WriteBatchCancel() {
  write_batch_finished_ = true;
  std::unique_lock lock{mutex_};
  LOG_INFO("TsEntitySegmentBuilder WriteBatchCancel begin, root_path: %s, entity_header_file_num: %lu", root_path_.c_str(),
           entity_item_file_number_);
  Defer defer([this]() {
    LOG_INFO("TsEntitySegmentBuilder WriteBatchCancel end, root_path: %s, entity_header_file_num: %lu",
             root_path_.c_str(), entity_item_file_number_);
    ReleaseBuilders();
  });
  entity_item_builder_->MarkDelete();
}

TsEntitySegmentVacuumer::TsEntitySegmentVacuumer(const std::string& root_path, TsVersionManager* version_manager)
    : root_path_(root_path), version_manager_(version_manager) {
  // entity header file
  TsIOEnv* env = &TsIOEnv::GetInstance();
  fs::path root(root_path);
  uint64_t entity_header_file_number = version_manager->NewFileNumber();
  std::string entity_header_file_path = root / EntityHeaderFileName(entity_header_file_number);
  entity_item_builder_ =
      std::make_unique<TsEntitySegmentEntityItemFileBuilder>(env, entity_header_file_path, entity_header_file_number);

  // block header file
  uint64_t block_item_file_number = version_manager->NewFileNumber();
  std::string block_header_file_path = root / BlockHeaderFileName(block_item_file_number);
  block_item_builder_ =
      std::make_unique<TsEntitySegmentBlockItemFileBuilder>(env, block_header_file_path, block_item_file_number, false);

  // block data file
  uint64_t block_data_file_number = version_manager->NewFileNumber();
  std::string block_file_path = root / DataBlockFileName(block_data_file_number);
  block_file_builder_ =
      std::make_unique<TsEntitySegmentBlockFileBuilder>(env, block_file_path, block_data_file_number, false);

  // block agg file
  uint64_t agg_file_number = version_manager->NewFileNumber();
  std::string agg_file_path = root / EntityAggFileName(agg_file_number);
  agg_file_builder_ = std::make_unique<TsEntitySegmentAggFileBuilder>(env, agg_file_path, agg_file_number, false);
}

KStatus TsEntitySegmentVacuumer::Open() {
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

void TsEntitySegmentVacuumer::Cancel() {
  entity_item_builder_->MarkDelete();
  block_item_builder_->MarkDelete();
  block_file_builder_->MarkDelete();
  agg_file_builder_->MarkDelete();
}

KStatus TsEntitySegmentVacuumer::AppendEntityItem(TsEntityItem& entity_item) {
  return entity_item_builder_->AppendEntityItem(entity_item);
}

KStatus TsEntitySegmentVacuumer::AppendBlock(const TSSlice& block, uint64_t* offset) {
  return block_file_builder_->AppendBlock(block, offset);
}

KStatus TsEntitySegmentVacuumer::AppendAgg(const TSSlice& agg, uint64_t* offset) {
  return agg_file_builder_->AppendAggBlock(agg, offset);
}

KStatus TsEntitySegmentVacuumer::AppendBlockItem(TsEntitySegmentBlockItem& block_item) {
  return block_item_builder_->AppendBlockItem(block_item);
}

EntitySegmentMetaInfo TsEntitySegmentVacuumer::GetHandleInfo() {
  EntitySegmentMetaInfo info;
  info.datablock_info = block_file_builder_->GetFileInfo();
  info.agg_info = agg_file_builder_->GetFileInfo();
  info.header_b_info = block_item_builder_->GetFileInfo();
  info.header_e_file_number = entity_item_builder_->GetFileNumber();
  return info;
}

TsSliceGuard TsMemEntitySegmentModifier::GetEntityItems(TsEntitySegment* target) {
  TsRandomReadFile* r_file = target->GetSegmentFile()->meta_mgr_.entity_header_.r_file_.get();
  size_t header_size = sizeof(TsEntityItemFileHeader);
  TsSliceGuard result;
  auto s = r_file->Read(0, r_file->GetFileSize() - header_size, &result);
  assert(s == KStatus::SUCCESS);
  return result;
}

TsSliceGuard TsMemEntitySegmentModifier::GetBlockItems(TsEntitySegment* target) {
  TsRandomReadFile* r_file = target->GetSegmentFile()->meta_mgr_.block_header_.r_file_.get();
  size_t header_size = sizeof(TsBlockItemFileHeader);
  TsSliceGuard result;
  auto s = r_file->Read(header_size, r_file->GetFileSize() - header_size, &result);
  assert(s == KStatus::SUCCESS);
  return result;
}

auto TsMemEntitySegmentModifier::GetEntityAndBlockItems(TsEntitySegment* target) {
  struct HeaderResult {
    TsEntityItem* entity_items;
    size_t entity_num;

    TsEntitySegmentBlockItem* block_items;
    size_t block_num;

    TsSliceGuard entity_items_guard_;
    TsSliceGuard block_items_guard_;
  };

  HeaderResult result;
  result.entity_items_guard_ = GetEntityItems(target);
  result.entity_items = reinterpret_cast<TsEntityItem*>(result.entity_items_guard_.data());

  result.entity_num = target->GetEntityNum();
  assert(result.entity_num * sizeof(TsEntityItem) == result.entity_items_guard_.size());

  result.block_items_guard_ = GetBlockItems(target);
  result.block_items = reinterpret_cast<TsEntitySegmentBlockItem*>(result.block_items_guard_.data());
  result.block_num = result.block_items_guard_.size() / sizeof(TsEntitySegmentBlockItem);
  assert(result.block_num * sizeof(TsEntitySegmentBlockItem) == result.block_items_guard_.size());

  return result;
}

auto TsMemEntitySegmentModifier::Modify(TsEntitySegment* base) {
  auto mem_entity_meta = GetEntityAndBlockItems(entity_segment_);
  auto dsk_entity_meta = GetEntityAndBlockItems(base);
  auto dsk_block_num = dsk_entity_meta.block_num;

  size_t max_mem_eid = UINT64_MAX;
  if (mem_entity_meta.entity_num < dsk_entity_meta.entity_num) {
    auto len = dsk_entity_meta.entity_num * sizeof(TsEntityItem);
    TsBufferBuilder builder(len);
    auto guard = builder.GetBuffer();
    std::copy_n(mem_entity_meta.entity_items, mem_entity_meta.entity_num,
                reinterpret_cast<TsEntityItem*>(guard.data()));
    max_mem_eid = mem_entity_meta.entity_num;
    mem_entity_meta.entity_num = dsk_entity_meta.entity_num;
    mem_entity_meta.entity_items_guard_ = std::move(guard);
    mem_entity_meta.entity_items = reinterpret_cast<TsEntityItem*>(mem_entity_meta.entity_items_guard_.data());
  }
  // modify:
  for (size_t i = 0; i < mem_entity_meta.entity_num; ++i) {
    bool mem_exist = mem_entity_meta.entity_items[i].cur_block_id != 0 && i < max_mem_eid;
    bool dsk_exist = i < dsk_entity_meta.entity_num && dsk_entity_meta.entity_items[i].cur_block_id != 0;

    if (!mem_exist) {
      if (dsk_exist) {
        mem_entity_meta.entity_items[i] = dsk_entity_meta.entity_items[i];
      } else {
        mem_entity_meta.entity_items[i] = TsEntityItem();
      }
      continue;
    }
    if (!dsk_exist) {
      mem_entity_meta.entity_items[i].cur_block_id += dsk_block_num;
      continue;
    }

    timestamp64 min_ts = std::min(mem_entity_meta.entity_items[i].min_ts, dsk_entity_meta.entity_items[i].min_ts);
    timestamp64 max_ts = std::max(mem_entity_meta.entity_items[i].max_ts, dsk_entity_meta.entity_items[i].max_ts);
    size_t row_written = mem_entity_meta.entity_items[i].row_written + dsk_entity_meta.entity_items[i].row_written;

    mem_entity_meta.entity_items[i].min_ts = min_ts;
    mem_entity_meta.entity_items[i].max_ts = max_ts;
    mem_entity_meta.entity_items[i].row_written = row_written;
    mem_entity_meta.entity_items[i].cur_block_id += dsk_block_num;
  }

  // modify: block_id prev_block_id block_offset agg_offset;
  size_t block_offset = base->GetSegmentFile()->block_file_.r_file_->GetFileSize();
  size_t agg_offset = base->GetSegmentFile()->agg_file_.r_file_->GetFileSize();
  for (int i = 0; i < mem_entity_meta.block_num; ++i) {
    mem_entity_meta.block_items[i].block_id += dsk_block_num;
    mem_entity_meta.block_items[i].block_offset += block_offset - sizeof(TsAggAndBlockFileHeader);
    mem_entity_meta.block_items[i].agg_offset += agg_offset - sizeof(TsAggAndBlockFileHeader);

    if (mem_entity_meta.block_items[i].prev_block_id != 0) {
      mem_entity_meta.block_items[i].prev_block_id += dsk_block_num;
    } else {
      assert(mem_entity_meta.block_items[i].entity_id > 0);
      auto entity_idx = mem_entity_meta.block_items[i].entity_id - 1;
      if (entity_idx < dsk_entity_meta.entity_num) {
        mem_entity_meta.block_items[i].prev_block_id = dsk_entity_meta.entity_items[entity_idx].cur_block_id;
      }
    }
  }

  return mem_entity_meta;
}

static KStatus CopyHelper(TsRandomReadFile* src) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  auto path = src->GetFilePath();
  std::unique_ptr<TsAppendOnlyFile> w_file;
  auto s = env->NewAppendOnlyFile(path, &w_file);
  if (s == FAIL) {
    return s;
  }
  return FileRangeCopy(src, w_file.get());
}

static KStatus AppendHelper(const std::string& dst_path, size_t offset, TsRandomReadFile* src, size_t start = 0,
                            size_t end = -1) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  std::unique_ptr<TsAppendOnlyFile> w_file;
  // std::printf("append to %s, offset %lu, start %lu, end %lu\n", dst_path.c_str(), offset, start, end);
  auto s = env->NewAppendOnlyFile(dst_path, &w_file, false, offset);
  if (s == FAIL) {
    return s;
  }
  return FileRangeCopy(src, w_file.get(), start, end);
}

static KStatus DumpToEntityHeader(const std::string& dst_path, const TsSliceGuard& entity_items) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  std::unique_ptr<TsAppendOnlyFile> w_file;
  auto s = env->NewAppendOnlyFile(dst_path, &w_file);
  if (s == FAIL) {
    return s;
  }
  auto s1 = w_file->Append(entity_items.AsSlice());
  TsEntityItemFileHeader header;
  std::memset(&header, 0, sizeof(header));
  header.magic = TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
  header.status = TsFileStatus::READY;
  header.entity_num = w_file->GetFileSize() / sizeof(TsEntityItem);
  auto s2 = w_file->Append(TSSlice{reinterpret_cast<char*>(&header), sizeof(header)});
  return s1 == SUCCESS && s2 == SUCCESS ? SUCCESS : FAIL;
}

KStatus TsMemEntitySegmentModifier::FirstFlushBypass(EntitySegmentMetaInfo* info) {
  auto s = CopyHelper(entity_segment_->GetSegmentFile()->meta_mgr_.entity_header_.r_file_.get());
  if (s != SUCCESS) return s;
  s = CopyHelper(entity_segment_->GetSegmentFile()->meta_mgr_.block_header_.r_file_.get());
  if (s != SUCCESS) return s;

  s = CopyHelper(entity_segment_->GetSegmentFile()->block_file_.r_file_.get());
  if (s != SUCCESS) return s;
  s = CopyHelper(entity_segment_->GetSegmentFile()->agg_file_.r_file_.get());
  if (s != SUCCESS) return s;
  *info = entity_segment_->GetSegmentFile()->info_;
  return SUCCESS;
}

KStatus TsMemEntitySegmentModifier::NormalFlush(TsEntitySegment* base, EntitySegmentMetaInfo* info) {
  auto mem_entity_meta = Modify(base);
  auto s = DumpToEntityHeader(entity_segment_->GetSegmentFile()->meta_mgr_.entity_header_.r_file_->GetFilePath(),
                              mem_entity_meta.entity_items_guard_);
  // auto s = CopyHelper(entity_segment_->meta_mgr_.entity_header_.r_file_.get());
  if (s != SUCCESS) return s;
  std::string path = base->GetSegmentFile()->meta_mgr_.block_header_.r_file_->GetFilePath();
  s = AppendHelper(path, base->GetSegmentFile()->info_.header_b_info.length,
                    entity_segment_->GetSegmentFile()->meta_mgr_.block_header_.r_file_.get(),
                    sizeof(TsBlockItemFileHeader));
  if (s != SUCCESS) return s;

  path = base->GetSegmentFile()->block_file_.r_file_->GetFilePath();
  s = AppendHelper(path, base->GetSegmentFile()->info_.datablock_info.length,
                    entity_segment_->GetSegmentFile()->block_file_.r_file_.get(),
                    sizeof(TsAggAndBlockFileHeader));
  if (s != SUCCESS) return s;

  path = base->GetSegmentFile()->agg_file_.r_file_->GetFilePath();
  s = AppendHelper(path, base->GetSegmentFile()->info_.agg_info.length,
                    entity_segment_->GetSegmentFile()->agg_file_.r_file_.get(),
                    sizeof(TsAggAndBlockFileHeader));
  if (s != SUCCESS) return s;


  *info = base->GetSegmentFile()->info_;
  info->header_e_file_number = entity_segment_->GetSegmentFile()->info_.header_e_file_number;
  info->header_b_info.length +=
        entity_segment_->GetSegmentFile()->info_.header_b_info.length - sizeof(TsBlockItemFileHeader);
  info->datablock_info.length +=
        entity_segment_->GetSegmentFile()->info_.datablock_info.length - sizeof(TsAggAndBlockFileHeader);
  info->agg_info.length +=
        entity_segment_->GetSegmentFile()->info_.agg_info.length - sizeof(TsAggAndBlockFileHeader);
  return SUCCESS;
}

KStatus TsMemEntitySegmentModifier::PersistToDisk(TsEntitySegment* base, EntitySegmentMetaInfo* info) {
  if (base) {
    return NormalFlush(base, info);
  }
  return FirstFlushBypass(info);
}
}  //  namespace kwdbts
