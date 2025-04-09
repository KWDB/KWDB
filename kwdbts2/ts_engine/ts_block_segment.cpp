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
#include "ts_timsort.h"
#include "ts_vgroup_partition.h"

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
  if (entity_item.max_ts < block_item_info.max_ts_in_block) {
    entity_item.max_ts = block_item_info.max_ts_in_block;
  }
  if (entity_item.min_ts > block_item_info.min_ts_in_block) {
    entity_item.min_ts = block_item_info.min_ts_in_block;
  }
  entity_item.row_written += block_item_info.row_count;
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

KStatus TsBlockSegmentBuilder::buildColData(std::vector<TsLastSegmentBlockRowInfo>& row_values,
                                            int col_idx, size_t row_offset, size_t row_count,
                                            bool has_bitmap, DATATYPE d_type, size_t d_size,
                                            string& col_data, TsBitmap& bitmap) {
  bitmap.Reset(row_count);
  uint32_t var_offset = 0;
  bool is_var_col = isVarLenType(d_type);
  if (is_var_col) {
    col_data.resize((row_count + 1) * sizeof(uint32_t));
  }
  for (size_t row_idx = row_offset; row_idx < row_offset + row_count; ++row_idx) {
    uint32_t last_segment_idx = row_values[row_idx].last_segment_idx;
    uint32_t block_idx = row_values[row_idx].block_idx;
    std::shared_ptr<TsLastSegmentBlock> &metric_block = blocks_[last_segment_idx][block_idx];
    if (has_bitmap) {
      bitmap[row_idx - row_offset] = metric_block->GetBitmap(col_idx + 1, row_values[row_idx].row_idx);
    }
    TSSlice value = metric_block->GetData(col_idx + 1, row_values[row_idx].row_idx, d_type, d_size);
    if (is_var_col) {
      memcpy(col_data.data() + (row_idx - row_offset) * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
      var_offset += value.len;
    }
    col_data.append(value.data, value.len);
  }
  if (is_var_col) {
    memcpy(col_data.data() + row_count * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBuilder::compress(const std::string& col_data, TsBitmap* bitmap, DATATYPE d_type, size_t row_count,
                                        std::string& buffer) {
  // write bitmap to buffer
  bool has_bitmap = bitmap != nullptr;
  if (has_bitmap) {
    TSSlice bitmap_data = bitmap->GetData();
    // TODO(limeng04): compress bitmap
    char bitmap_compress_type = 0;
    buffer.append(&bitmap_compress_type);
    buffer.append(bitmap_data.data, bitmap_data.len);
  }

  // compress col data & write to buffer
  const auto& mgr = CompressorManager::GetInstance();
  auto compressor = mgr.GetDefaultCompressor(d_type);
  TSSlice plain{const_cast<char *>(col_data.data()), col_data.size()};
  std::string compressed;
  bool ok = compressor.Compress(plain, bitmap, row_count, &compressed);
  std::string tmp;
  if (ok) {
    auto [first, second] = compressor.GetAlgorithms();
    buffer.push_back(static_cast<char>(first));
    buffer.push_back(static_cast<char>(second));
    buffer.append(compressed);
  } else {
    buffer.push_back(static_cast<char>(TsCompAlg::kPlain));
    buffer.push_back(static_cast<char>(GenCompAlg::kPlain));
    buffer.append(col_data);
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBuilder::BuildAndFlush(uint32_t thread_num) {
  blocks_.resize(last_segments_.size());
  std::vector<std::thread> workers;
  KStatus global_status = KStatus::SUCCESS;
#ifdef K_DEBUG
  std::atomic<size_t> read_row_num;
  std::atomic<size_t> write_row_num;
#endif
  // Parallel reading of last segments
  {
    std::mutex entity_values_mutex;

    auto read_last_segment = [&](std::shared_ptr<TsLastSegment>& last_segment, uint32_t last_segment_idx) {
      // get block indexes
      std::vector<TsLastSegmentBlockIndex> block_indexes;
      KStatus s = last_segment->GetAllBlockIndex(&block_indexes);
      if (s != KStatus::SUCCESS || global_status != KStatus::SUCCESS) {
        return s;
      }

      // read entity row values
      std::map<TsEntityKey, std::vector<TsLastSegmentBlockRowInfo>> local_entity_row_values;
      for (TsLastSegmentBlockIndex& block_index : block_indexes) {
        TsLastSegmentBlockInfo block_info;
        s = last_segment->GetBlockInfo(block_index, &block_info);
        if (s != KStatus::SUCCESS || global_status != KStatus::SUCCESS) {
          return s;
        }

#ifdef K_DEBUG
        read_row_num.fetch_add(block_info.nrow);
#endif
        auto block = std::make_shared<TsLastSegmentBlock>();
        s = last_segment->GetBlock(block_info, block.get());
        if (s != KStatus::SUCCESS || global_status != KStatus::SUCCESS) {
          return s;
        }

        for (uint32_t row_idx = 0; row_idx < block_info.nrow; row_idx++) {
          TsEntityKey entity{block->GetEntityId(row_idx), block_index.table_id, block_index.table_version};
          TsLastSegmentBlockRowInfo row_info{block->GetTimestamp(row_idx), block->GetSeqNo(row_idx), last_segment_idx,
                                             static_cast<uint32_t>(blocks_[last_segment_idx].size()), row_idx};
          local_entity_row_values[entity].push_back(row_info);
        }

        blocks_[last_segment_idx].push_back(block);
      }

      // Merge local results into global structures
      {
        std::lock_guard<std::mutex> lock_entity(entity_values_mutex);
        entity_row_values_.merge(local_entity_row_values);
      }

      return KStatus::SUCCESS;
    };

    // Distribute segments across threads
    uint32_t compact_num_per_thread = (last_segments_.size() + thread_num - 1) / thread_num;
    for (uint32_t thread_idx = 0; thread_idx < thread_num; ++thread_idx) {
      workers.emplace_back([&, thread_idx]() {
        uint32_t end_idx = (thread_idx + 1) * compact_num_per_thread;
        for (size_t idx = thread_idx * compact_num_per_thread; idx < end_idx && idx < last_segments_.size(); ++idx) {
          KStatus s = read_last_segment(last_segments_[idx], idx);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("read last segment[%u] failed, thread idx: %u", last_segments_[idx]->GetVersion(), thread_idx);
            global_status = s;
            return;
          }
        }
      });
    }

    for (auto& worker : workers) {
      worker.join();
    }

    if (global_status != KStatus::SUCCESS) {
      return global_status;
    }
  }

  // Parallel processing of entity row values
  {
    TsEngineSchemaManager* schema_mgr = partition_->GetSchemaMgr();

    workers.clear();
    global_status = KStatus::SUCCESS;

    // Convert entity_row_values_ to vector for parallel processing
    std::vector<std::pair<TsEntityKey, std::vector<TsLastSegmentBlockRowInfo>>> entities_vec(
      entity_row_values_.begin(), entity_row_values_.end());

    auto write_block_segment = [&](TsEntityKey& entity_key, std::vector<TsLastSegmentBlockRowInfo>& row_values) {
      std::shared_ptr<MMapMetricsTable> table_schema_;
      KStatus s = schema_mgr->GetTableMetricSchema({}, entity_key.table_id, entity_key.table_version, &table_schema_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get table schema failed. table id: %lu, table version: %u.",
                  entity_key.table_id, entity_key.table_version);
        return s;
      }
      std::vector<AttributeInfo> metric_schema = table_schema_->getSchemaInfoExcludeDropped();

      timsort(row_values.begin(), row_values.end());

      size_t row_offset = 0;
      size_t row_count = row_values.size() > max_rows_per_block_ ? max_rows_per_block_ : row_values.size();
      size_t col_count = metric_schema.size() + 1;

      while (row_count != 0) {
        std::string buffer;
        buffer.resize((col_count + 1) * sizeof(uint32_t));

        for (int col_idx = 0; col_idx < col_count; ++col_idx) {
          DATATYPE d_type = col_idx == 0 ? DATATYPE::INT64 : col_idx != 1 ?
            static_cast<DATATYPE>(metric_schema[col_idx - 1].type) : DATATYPE::TIMESTAMP64;
          size_t d_size = col_idx == 0 ? 8 : col_idx == 1 ? 8 : static_cast<DATATYPE>(metric_schema[col_idx - 1].size);
          bool has_bitmap = col_idx != 0;

          // write col offset to buffer
          uint32_t offset = buffer.size();
          memcpy(buffer.data() + col_idx * sizeof(uint32_t), &offset, sizeof(uint32_t));

          // build column data
          TsBitmap bitmap;
          string col_data;
          buildColData(row_values, col_idx, row_offset, row_count, has_bitmap, d_type, d_size, col_data, bitmap);

#ifdef K_DEBUG
          size_t offset_len = isVarLenType(d_type) ? (row_count + 1) * 4 : 0;
          assert(col_data.size() == d_size * row_count + offset_len);
#endif
          // compress
          if (has_bitmap) {
            compress(col_data, &bitmap, d_type, row_count, buffer);
          } else {
            compress(col_data, nullptr, d_type, row_count, buffer);
          }
        }
#ifdef K_DEBUG
        write_row_num.fetch_add(row_count);
#endif
        // write last col data end offset
        uint32_t offset = buffer.size();
        memcpy(buffer.data() + col_count * sizeof(uint32_t), &offset, sizeof(uint32_t));

        // flush
        s = partition_->AppendToBlockSegment(entity_key.table_id, entity_key.entity_id, entity_key.table_version,
                                             {buffer.data(), buffer.size()}, {}, row_count);
        if (s != KStatus::SUCCESS) {
          return s;
        }

        row_offset += row_count;
        row_count = row_values.size() - row_offset > max_rows_per_block_ ?
                    max_rows_per_block_ : row_values.size() - row_offset;
      }
      return KStatus::SUCCESS;
    };

    for (uint32_t thread_idx = 0; thread_idx < thread_num; thread_idx++) {
      workers.emplace_back([&, thread_idx]() {
        for (size_t idx = thread_idx; idx < entities_vec.size(); idx += thread_num) {
          KStatus s = write_block_segment(entities_vec[idx].first, entities_vec[idx].second);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("write block segment failed, thread idx: %u", thread_idx);
            global_status = s;
            return;
          }
          if (global_status == KStatus::FAIL) {
            return;
          }
        }
      });
    }

    for (auto& worker : workers) {
      worker.join();
    }

    if (global_status != KStatus::SUCCESS) {
      return global_status;
    }
  }
#ifdef KDEBUG
  assert(read_row_num.load() == write_row_num.load());
#endif
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts

