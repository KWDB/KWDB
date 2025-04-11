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
                                                       const TsBlockSegmentBlockItemInfo& block_item_info,
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
  entity_item.row_written += block_item_info.rows;
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

KStatus TsBlockSegmentBlockItemFile::AllocateBlockItem(uint64_t entity_id, TsBlockSegmentBlockItemInfo& block_item_info) {
  RW_LATCH_X_LOCK(block_item_mtx_);
  // file header
  header_.block_num += 1;
  KStatus s = writeFileMeta(header_);
  // block item info
  block_item_info.block_id = header_.block_num;
  size_t offset = sizeof(TsBlockItemFileHeader) + (block_item_info.block_id - 1) * sizeof(TsBlockSegmentBlockItemInfo);
  file_->Write(offset, TSSlice{reinterpret_cast<char *>(&block_item_info), sizeof(TsBlockSegmentBlockItemInfo)});
  RW_LATCH_UNLOCK(block_item_mtx_);
  return s;
}

KStatus TsBlockSegmentBlockItemFile::GetBlockItem(uint64_t entity_id, uint64_t blk_id,
                                                  std::shared_ptr<TsBlockSegmentBlockItem>& blk_item) {
  RW_LATCH_S_LOCK(block_item_mtx_);
  TSSlice result;
  file_->Read(sizeof(TsBlockItemFileHeader) + (blk_id - 1) * sizeof(TsBlockSegmentBlockItemInfo),
              sizeof(TsBlockSegmentBlockItemInfo), &result, reinterpret_cast<char *>(&(blk_item->Info())));
  RW_LATCH_UNLOCK(block_item_mtx_);
  if (result.len != sizeof(TsBlockSegmentBlockItemInfo)) {
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

KStatus TsBlockSegmentMetaManager::AppendBlockItem(TsBlockSegmentBlockItem* blk_item) {
  uint64_t entity_id = blk_item->Info().entity_id;
  entity_meta_.WrLock();
  Defer defer([&]() { entity_meta_.UnLock(); });
  // get last block id
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // allocate&add block item
  blk_item->Info().prev_block_id = last_blk_id;
  s = block_meta_.AllocateBlockItem(entity_id, blk_item->Info());
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // update entity item
  s = entity_meta_.UpdateEntityItem(entity_id, blk_item->Info(), false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentMetaManager::GetAllBlockItems(TSEntityID entity_id,
                                                    std::vector<std::shared_ptr<TsBlockSegmentBlockItem>>* blk_items) {
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  std::shared_ptr<TsBlockSegmentBlockItem> cur_blk_item = std::make_shared<TsBlockSegmentBlockItem>();
  while (last_blk_id > 0) {
    s = block_meta_.GetBlockItem(entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    blk_items->push_back(cur_blk_item);
    last_blk_id = cur_blk_item->Info().prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsBlockSegment::TsBlockSegment(const std::filesystem::path& root)
    : dir_path_(root), meta_mgr_(root), block_file_(root / block_data_file_name) {}

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

KStatus TsBlockSegment::AppendBlockData(TsBlockSegmentBlockItem* blk_item, const TSSlice& data, const TSSlice& agg) {
  uint64_t blk_offset = 0;
  KStatus s = block_file_.AppendBlock(data, &blk_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to block file failed. data len: %lu.", data.len);
    return s;
  }
  blk_item->Info().block_offset = blk_offset;
  blk_item->Info().block_len = data.len;
  s = meta_mgr_.AppendBlockItem(blk_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to meta file failed. data len: %lu.", data.len);
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::GetAllBlockItems(TSEntityID entity_id,
                                         std::vector<std::shared_ptr<TsBlockSegmentBlockItem>>* blk_items) {
  return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
}

KStatus TsBlockSegment::GetBlockData(TsBlockSegmentBlockItem* blk_item, char* buff) {
  return block_file_.ReadBlock(blk_item->Info().block_offset, buff, blk_item->Info().block_len);
}

TsBlockSegmentBlock::TsBlockSegmentBlock(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                                    std::vector<AttributeInfo>& metric_schema) :
                                    table_id_(table_id), table_version_(table_version),
                                    entity_id_(entity_id), metric_schema_(metric_schema) {
  n_cols_ = metric_schema.size() + 1;
  column_blocks.resize(n_cols_);
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsBlockSegmentColumnBlock& column_block = column_blocks[col_idx];
    column_block.bitmap.Reset(MAX_ROWS_PER_BLOCK);
    DATATYPE d_type = static_cast<DATATYPE>(metric_schema_[col_idx - 1].type);
    if (isVarLenType(d_type)) {
      column_block.buffer.resize((MAX_ROWS_PER_BLOCK + 1) * sizeof(uint32_t));
    }
  }
  block_info.col_block_offset.resize(n_cols_ + 1);
}
TsBlockSegmentBlock::TsBlockSegmentBlock(const TsBlockSegmentBlock& other) {
  table_id_ = other.table_id_;
  table_version_ = other.table_version_;
  entity_id_ = other.entity_id_;
  metric_schema_ = other.metric_schema_;
  block_info = other.block_info;
  column_blocks = other.column_blocks;
  n_rows_ = other.n_rows_;
  n_cols_ = other.n_cols_;
}

uint64_t TsBlockSegmentBlock::GetSeqNo(uint32_t row_idx) {
  return *reinterpret_cast<uint64_t*>(column_blocks[0].buffer.data() + row_idx * sizeof(uint64_t));
}

timestamp64 TsBlockSegmentBlock::GetTimestamp(uint32_t row_idx) {
  return *reinterpret_cast<timestamp64*>(column_blocks[1].buffer.data() + row_idx * sizeof(timestamp64));
}

KStatus TsBlockSegmentBlock::GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value) {
  for (int col_idx = 1; col_idx < n_cols_; ++col_idx) {
    if (isVarLenType(metric_schema_[col_idx - 1].type)) {
      char* ptr = column_blocks[col_idx].buffer.data();
      uint32_t start_offset = *reinterpret_cast<uint32_t*>(ptr + row_idx * sizeof(uint32_t));
      uint32_t end_offset = *reinterpret_cast<uint32_t*>(ptr + (row_idx + 1) * sizeof(uint32_t));
      value.push_back({column_blocks[col_idx].buffer.data() + start_offset, end_offset - start_offset});
    } else {
      size_t d_size = col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
      value.push_back({column_blocks[col_idx].buffer.data() + row_idx * d_size, d_size});
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlock::Append(TsLastSegmentBlockSpan& span, bool& is_full) {
  size_t end_row = span.end_row - span.start_row + n_rows_ > MAX_ROWS_PER_BLOCK ?
                   span.start_row + MAX_ROWS_PER_BLOCK - n_rows_ : span.end_row;
  for (int col_idx = 0; col_idx < n_cols_; ++col_idx) {
    DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
                      static_cast<DATATYPE>(metric_schema_[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
    size_t d_size = col_idx == 0 ? 8 : col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema_[col_idx - 1].size);
    bool has_bitmap = col_idx != 0;

    bool is_var_col = isVarLenType(d_type);
    TsBlockSegmentColumnBlock& block = column_blocks[col_idx];
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
  is_full = n_rows_ == MAX_ROWS_PER_BLOCK;
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
    block_info.col_block_offset.push_back(buffer.size());

    TsBlockSegmentColumnBlock& block = column_blocks[col_idx];
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
  block_info.col_block_offset.push_back(buffer.size());
  // write last col data end offset
  memcpy(buffer.data(), &(block_info.col_block_offset), (n_cols_ + 1) * sizeof(uint32_t));

  // flush
  timestamp64 min_ts = GetTimestamp(0);
  timestamp64 max_ts = GetTimestamp(n_rows_ - 1);
  KStatus s = partition->AppendToBlockSegment(table_id_, entity_id_, table_version_, max_ts, min_ts,
                                              {buffer.data(), buffer.size()}, {}, n_rows_);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

void TsBlockSegmentBlock::Clear() {
  n_rows_ = 0;
  for (size_t col_idx = 1; col_idx < n_cols_; ++col_idx) {
    TsBlockSegmentColumnBlock& column_block = column_blocks[col_idx];
    column_block.bitmap.Reset(MAX_ROWS_PER_BLOCK);
    column_block.buffer.clear();
  }
  block_info.col_block_offset.clear();
  block_info.col_block_offset.resize(n_cols_ + 1);
}

KStatus TsBlockSegmentBuilder::BuildAndFlush() {
  KStatus s;
  // 1. The iterator will be used to read MAX_COMPACT_NUM last segment data
  TsLastSegmentBlockSpan block_span;
  bool is_finished = false;
  TsEngineSchemaManager* schema_mgr = partition_->GetSchemaMgr();
  // 2. Create a new last segment
  std::unique_ptr<TsLastSegment> last_segment;
  s = partition_->NewLastSegment(&last_segment);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, new last segment failed.")
    return s;
  }
  TsLastSegmentBuilder builder(schema_mgr, last_segment);
  // 3. Traverse the last segment data and write the data to the block segment
  TsLastSegmentsMergeIterator iter(last_segments_);
  iter.Init();
  TsEntityKey entity_key;
  std::shared_ptr<TsBlockSegmentBlock> block = nullptr;
  while (true) {
    if (block_span.end_row - block_span.start_row <= 0) {
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
        // Writes the incomplete data back to the last segment
        for (uint32_t row_idx = 0; row_idx < block->GetNRows(); ++row_idx) {
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
    for (uint32_t row_idx = 0; row_idx < block->GetNRows(); ++row_idx) {
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
  s = builder.Flush();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsBlockSegmentBuilder::BuildAndFlush failed, TsLastSegmentBuilder flush failed.")
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
