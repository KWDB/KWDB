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

#include "ts_lastsegment.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <list>
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_block.h"
#include "ts_coding.h"
#include "ts_column_block.h"
#include "ts_common.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_lastsegment_endec.h"
#include "ts_segment.h"
#include "ts_std_utils.h"
#include "ts_table_schema_manager.h"
namespace kwdbts {

int TsLastSegment::kNRowPerBlock = 4096;

static KStatus LoadBlockInfo(TsRandomReadFile* file, const TsLastSegmentBlockIndex& index,
                             TsLastSegmentBlockInfo* info) {
  assert(info != nullptr);
  TSSlice result;
  auto s = file->Read(index.info_offset, index.length, &result, nullptr);
  if (s != SUCCESS) {
    return s;
  }
  return DecodeBlockInfo(result, info);
}

KStatus TsLastSegment::TsLastSegBlockCache::BlockIndexCache::GetBlockIndices(
    std::vector<TsLastSegmentBlockIndex>** block_indices) {
  if (block_indices_ != nullptr) {
    *block_indices = block_indices_.get();
    return SUCCESS;
  }
  std::unique_lock lk{mu_};
  if (block_indices_ != nullptr) {
    *block_indices = block_indices_.get();
    return SUCCESS;
  }
  auto indices = std::make_unique<std::vector<TsLastSegmentBlockIndex>>();
  auto s = lastseg_->GetAllBlockIndex(indices.get());
  if (s == FAIL) {
    LOG_ERROR("cannot get block index from last segment");
  }
  // indices is ready to use after GetAllBlockIndex.
  block_indices_ = std::move(indices);
  *block_indices = block_indices_.get();
  return SUCCESS;
}

KStatus TsLastSegment::TsLastSegBlockCache::BlockInfoCache::GetBlockInfo(int block_id, TsLastSegmentBlockInfo** info) {
  {
    if (cache_flag_[block_id] == 1) {
      *info = &block_infos_[block_id];
      return SUCCESS;
    }
  }
  std::unique_lock lk{mu_};
  if (cache_flag_[block_id] == 1) {
    *info = &block_infos_[block_id];
    return SUCCESS;
  }
  TsLastSegmentBlockIndex* index;
  auto s = lastseg_cache_->GetBlockIndex(block_id, &index);
  if (s == FAIL) {
    LOG_ERROR("cannot load block index from last segment");
  }
  TsLastSegmentBlockInfo tmp_info;
  s = LoadBlockInfo(lastseg_cache_->segment_->file_.get(), *index, &tmp_info);
  if (s == FAIL) {
    LOG_ERROR("cannot load block info from last segment");
  }
  block_infos_[block_id] = std::move(tmp_info);
  cache_flag_[block_id] = 1;
  *info = &block_infos_[block_id];
  return SUCCESS;
}

KStatus TsLastSegment::GetFooter(TsLastSegmentFooter* footer) const {
  TSSlice result;
  size_t offset = file_->GetFileSize() - sizeof(TsLastSegmentFooter);
  auto s = file_->Read(offset, sizeof(TsLastSegmentFooter), &result, reinterpret_cast<char*>(footer));
  if (s == FAIL || result.len != sizeof(TsLastSegmentFooter)) {
    LOG_ERROR("last segment[%s] GetFooter failed.", file_->GetFilePath().c_str());
    return s;
  }
  // important, Read function may not fill the buffer;
  return DecodeFooter(result, footer);
}

KStatus TsLastSegment::GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indices) {
  assert(footer_.magic_number == FOOTER_MAGIC);
  std::vector<TsLastSegmentBlockIndex> tmp_indices;
  tmp_indices.resize(footer_.n_data_block);
  TSSlice result;
  auto buf = std::make_unique<char[]>(footer_.n_data_block * sizeof(TsLastSegmentBlockIndex));
  auto s = file_->Read(footer_.block_info_idx_offset, tmp_indices.size() * sizeof(TsLastSegmentBlockIndex), &result,
                       buf.get());
  if (s == FAIL) {
    LOG_ERROR("cannot read data from file");
    return s;
  }
  constexpr size_t kIndexSize = sizeof(TsLastSegmentBlockIndex);
  if (result.len != footer_.n_data_block * kIndexSize) {
    LOG_ERROR("last segment[%s] GetAllBlockIndex failed, result.len[%ld] != footer_.n_data_block * kIndexSize[%ld]",
              file_->GetFilePath().c_str(), result.len, footer_.n_data_block * kIndexSize);
    return FAIL;
  }
  for (int i = 0; i < tmp_indices.size(); ++i) {
    TSSlice slice{result.data, kIndexSize};
    DecodeBlockIndex(slice, &tmp_indices[i]);
    RemovePrefix(&result, kIndexSize);
  }
  assert(result.len == 0);
  block_indices->swap(tmp_indices);
  return SUCCESS;
}

class TsLastBlock : public TsBlock {
 private:
  const TsLastSegment* lastsegment_;

  int block_id_;

  TsLastSegmentBlockIndex block_index_;
  TsLastSegmentBlockInfo block_info_;

  class ColumnCache {
   private:
    TsRandomReadFile* file_;
    TsLastSegmentBlockInfo* block_info_;

    std::vector<std::unique_ptr<TsColumnBlock>> column_blocks_;

    TsSliceGuard entity_ids_;
    TsSliceGuard timestamps_;
    TsSliceGuard osn_;

   public:
    ColumnCache(TsRandomReadFile* file, TsLastSegmentBlockInfo* block_info)
        : file_(file), block_info_(block_info), column_blocks_(block_info->ncol) {}
    KStatus GetColumnBlock(int col_id, TsColumnBlock** block, const std::vector<AttributeInfo>* schema) {
      if (column_blocks_[col_id] != nullptr) {
        *block = column_blocks_[col_id].get();
        return SUCCESS;
      }
      auto offset = block_info_->block_offset + block_info_->entity_id_len;
      offset += block_info_->col_infos[col_id].offset;

      const auto& col_info = block_info_->col_infos[col_id];
      size_t length = col_info.bitmap_len + col_info.fixdata_len + col_info.vardata_len;

      TSSlice result{nullptr, 0};
      auto s = file_->Read(offset, length, &result, nullptr);
      if (s == FAIL || result.len != length) {
        LOG_ERROR("cannot read column data from file, expect %lu, result %lu", length, result.len);
        return s;
      }
      TsColumnCompressInfo info;
      info.bitmap_len = col_info.bitmap_len;
      info.fixdata_len = col_info.fixdata_len;
      info.vardata_len = col_info.vardata_len;
      info.row_count = block_info_->nrow;

      std::unique_ptr<TsColumnBlock> colblock;
      s = TsColumnBlock::ParseColumnData((*schema)[col_id], result, info, &colblock);
      if (s == FAIL) {
        LOG_ERROR("can not parse column data, col_id %d", col_id);
        return FAIL;
      }

      column_blocks_[col_id].swap(colblock);
      *block = column_blocks_[col_id].get();
      return SUCCESS;
    }

    KStatus GetEntityIDs(TSEntityID** entity_ids) {
      if (!entity_ids_.empty()) {
        *entity_ids = reinterpret_cast<TSEntityID*>(entity_ids_.data());
        return SUCCESS;
      }

      const auto& mgr = CompressorManager::GetInstance();
      auto offset = block_info_->block_offset;
      size_t length = block_info_->entity_id_len;
      TSSlice result;
      auto s = file_->Read(offset, length, &result, nullptr);
      if (s == FAIL) {
        return FAIL;
      }
      bool ok = mgr.DecompressData(result, nullptr, block_info_->nrow, &entity_ids_);
      *entity_ids = reinterpret_cast<TSEntityID*>(entity_ids_.data());
      return ok ? SUCCESS : FAIL;
    }

    KStatus GetOSN(uint64_t** osn) {
      if (!osn_.empty()) {
        *osn = reinterpret_cast<uint64_t*>(osn_.data());
        return SUCCESS;
      }

      const auto& mgr = CompressorManager::GetInstance();
      auto offset = block_info_->block_offset + block_info_->entity_id_len;
      size_t length = block_info_->entity_id_len;
      TSSlice result;
      auto s = file_->Read(offset, length, &result, nullptr);
      if (s == FAIL) {
        return FAIL;
      }
      bool ok = mgr.DecompressData(result, nullptr, block_info_->nrow, &osn_);
      *osn = reinterpret_cast<TS_LSN*>(osn_.data());
      return ok ? SUCCESS : FAIL;
    }

    KStatus GetTimestamps(timestamp64** timestamps) {
      if (!timestamps_.empty()) {
        *timestamps = reinterpret_cast<timestamp64*>(timestamps_.data());
        return SUCCESS;
      }

      auto offset = block_info_->block_offset + block_info_->entity_id_len;
      offset += block_info_->col_infos[0].offset + block_info_->col_infos[0].bitmap_len;
      auto length = block_info_->col_infos[0].fixdata_len;
      const auto& mgr = CompressorManager::GetInstance();
      TSSlice result;
      auto s = file_->Read(offset, length, &result, nullptr);
      if (s == FAIL) {
        return FAIL;
      }
      bool ok = mgr.DecompressData(result, nullptr, block_info_->nrow, &timestamps_);
      *timestamps = reinterpret_cast<timestamp64*>(timestamps_.data());
      return ok ? SUCCESS : FAIL;
    }
  };

  std::unique_ptr<ColumnCache> column_block_cache_;

 public:
  TsLastBlock(const TsLastSegment* lastseg, int block_id, TsLastSegmentBlockIndex block_index,
              TsLastSegmentBlockInfo block_info)
      : lastsegment_(lastseg),
        block_id_(block_id),
        block_index_(block_index),
        block_info_(std::move(block_info)),
        column_block_cache_(std::make_unique<ColumnCache>(lastsegment_->file_.get(), &block_info_)) {}
  ~TsLastBlock() = default;
  TSTableID GetTableId() override { return block_index_.table_id; }
  uint32_t GetTableVersion() override { return block_index_.table_version; }
  size_t GetRowNum() override { return block_info_.nrow; }

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                       std::unique_ptr<TsBitmapBase>* bitmap) override {
    TsColumnBlock* col_block = nullptr;
    auto s = column_block_cache_->GetColumnBlock(col_id, &col_block, schema);
    if (s == FAIL) {
      LOG_ERROR("load column from %s failed block_id %d, col_id %d", lastsegment_->GetFilePath().c_str(), block_id_,
                col_id);
      return FAIL;
    }
    return col_block->GetColBitmap(bitmap);
  }
  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema, char** value) override {
    TsColumnBlock* col_block = nullptr;
    auto s = column_block_cache_->GetColumnBlock(col_id, &col_block, schema);
    if (s == FAIL) {
      LOG_ERROR("load column from %s failed block_id %d, col_id %d", lastsegment_->GetFilePath().c_str(), block_id_,
                col_id);
      return FAIL;
    }
    *value = col_block->GetColAddr();
    return SUCCESS;
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema, TSSlice& value) override {
    TsColumnBlock* col_block = nullptr;
    auto s = column_block_cache_->GetColumnBlock(col_id, &col_block, schema);
    if (s == FAIL) {
      LOG_ERROR("load column from %s failed block_id %d, col_id %d", lastsegment_->GetFilePath().c_str(), block_id_,
                col_id);
      return FAIL;
    }
    return col_block->GetValueSlice(row_num, value);
  }

  inline bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema) override {
    std::unique_ptr<TsBitmapBase> bitmap;
    auto s = GetColBitmap(col_id, schema, &bitmap);
    if (s == FAIL) {
      return false;
    }
    return bitmap->At(row_num) == DataFlags::kNull;
  }

  // if just get timestamp , this function return fast.
  inline timestamp64 GetTS(int row_num) override {
    auto ts = GetTimestamps();
    if (ts == nullptr) {
      return INVALID_TS;
    }
    return ts[row_num];
  }

  inline timestamp64 GetFirstTS() override {
    return block_index_.first_ts;
  }

  inline timestamp64 GetLastTS() override {
    return block_index_.last_ts;
  }

  inline void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) override {
    min_osn = block_index_.min_osn;
    max_osn = block_index_.max_osn;
  }

  inline uint64_t GetFirstOSN() override {
    return block_index_.first_osn;
  }

  inline uint64_t GetLastOSN() override {
    return block_index_.last_osn;
  }

  inline const uint64_t* GetOSNAddr(int row_num) override {
    auto osn = GetOSN();
    if (osn == nullptr) {
      LOG_ERROR("cannot get osn addr");
      return nullptr;
    }
    return &osn[row_num];
  }

  inline KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, std::string& data) override {
    return KStatus::FAIL;
  }

  inline int GetBlockID() const { return block_id_; }

 private:
  friend class TsLastSegment;

  const TSEntityID* GetEntities() {
    TSEntityID* entity_ids = nullptr;
    auto s = column_block_cache_->GetEntityIDs(&entity_ids);
    if (s == FAIL) {
      LOG_ERROR("cannot load entitiy column");
      return nullptr;
    }
    return entity_ids;
  }

  inline const uint64_t* GetOSN() {
    uint64_t* osn = nullptr;
    auto s = column_block_cache_->GetOSN(&osn);
    if (s == FAIL) {
      LOG_ERROR("cannot load osn column");
      return nullptr;
    }
    return osn;
  }

  inline const timestamp64* GetTimestamps() {
    timestamp64* timestamps = nullptr;
    auto s = column_block_cache_->GetTimestamps(&timestamps);
    if (s == FAIL) {
      LOG_ERROR("cannot load timestamp column");
      return nullptr;
    }
    return timestamps;
  }
};

KStatus TsLastSegment::GetBlock(int block_id, std::shared_ptr<TsLastBlock>* block) const {
  TsLastSegmentBlockIndex* index;
  auto s = block_cache_->GetBlockIndex(block_id, &index);
  if (s == FAIL) {
    LOG_ERROR("cannot get block index");
    return s;
  }

  TsLastSegmentBlockInfo* info;
  s = block_cache_->GetBlockInfo(block_id, &info);
  if (s == FAIL) {
    LOG_ERROR("cannot get block info");
    return s;
  }

  *block = std::make_unique<TsLastBlock>(this, block_id, *index, *info);
  return SUCCESS;
}

TsLastSegment::TsLastSegBlockCache::TsLastSegBlockCache(TsLastSegment* last, int nblock)
    : segment_(last),
      block_index_cache_(std::make_unique<BlockIndexCache>(last)),
      block_info_cache_(std::make_unique<BlockInfoCache>(this, nblock)) {}

KStatus TsLastSegment::TsLastSegBlockCache::GetAllBlockIndex(
    std::vector<TsLastSegmentBlockIndex>** block_indices) const {
  return block_index_cache_->GetBlockIndices(block_indices);
}

KStatus TsLastSegment::TsLastSegBlockCache::GetBlockIndex(int block_id, TsLastSegmentBlockIndex** index) const {
  std::vector<TsLastSegmentBlockIndex>* block_indices;
  auto s = block_index_cache_->GetBlockIndices(&block_indices);
  if (s == FAIL) {
    return s;
  }
  *index = &(*block_indices)[block_id];
  return SUCCESS;
}

inline KStatus TsLastSegment::TsLastSegBlockCache::GetBlockInfo(int block_id, TsLastSegmentBlockInfo** info) const {
  return block_info_cache_->GetBlockInfo(block_id, info);
}

KStatus TsLastSegment::Open() {
  // just check the magic number;
  auto sz = file_->GetFileSize();
  if (sz < sizeof(TsLastSegmentFooter)) {
    LOG_ERROR("lastsegment file corrupted");
    return FAIL;
  }
  auto s = GetFooter(&footer_);
  if (s == FAIL) {
    return s;
  }
  if (footer_.magic_number != FOOTER_MAGIC) {
    LOG_ERROR("lastsegment %s: footer magic mismatch", file_->GetFilePath().c_str());
    return FAIL;
  }

  // load necessary meta block to memory.
  // NOTICE: maybe we will support lazy loading later. For now, just load all meta blocks in
  // Open()
  int nmeta = footer_.n_meta_block;
  if (nmeta != 0) {
    TSSlice result;
    s = file_->Read(footer_.meta_block_idx_offset, nmeta * 16, &result, nullptr);
    if (s == FAIL) {
      return s;
    }
    std::vector<size_t> meta_offset(nmeta);
    std::vector<size_t> meta_len(nmeta);
    for (int i = 0; i < nmeta; ++i) {
      GetFixed64(&result, &meta_offset[i]);
      GetFixed64(&result, &meta_len[i]);
    }

    for (int i = 0; i < nmeta; ++i) {
      s = file_->Read(meta_offset[i], meta_len[i], &result, nullptr);
      if (s == FAIL) {
        return FAIL;
      }
      uint8_t len = static_cast<uint8_t>(result.data[0]);
      std::string_view sv{result.data + 1, len};
      result.data += len + 1;
      result.len -= len + 1;
      if (sv == LastSegmentBloomFilter::Name()) {
        s = TsBloomFilter::FromData(result, &bloom_filter_);
      } else {
        assert(false);
      }
      if (s == FAIL) {
        return FAIL;
      }
    }
  }

  int nblock = footer_.n_data_block;
  assert(nblock >= 0);  // TODO(zzr) the case nblock == 0 may exist in UT.
  block_cache_ = std::make_unique<TsLastSegBlockCache>(this, nblock);
  return SUCCESS;
}

KStatus TsLastSegment::GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     TsEngineSchemaManager* schema_mgr) {
  assert(block_cache_ != nullptr);

  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr = nullptr;
  for (int idx = 0; idx < footer_.n_data_block; ++idx) {
    std::shared_ptr<TsLastBlock> block;
    this->GetBlock(idx, &block);

    // auto block = std::make_shared<TsLastBlock>(shared_from_this(), idx, block_indices[idx], *info);

    // split current block to several span;
    int prev_end = 0;
    auto entities = block->GetEntities();
    if (entities == nullptr) {
      LOG_ERROR("cannot load entity column");
      return FAIL;
    }
    auto ts = block->GetTimestamps();
    if (ts == nullptr) {
      LOG_ERROR("cannot load timestamp column");
      return FAIL;
    }

    if (tbl_schema_mgr == nullptr || tbl_schema_mgr->GetTableId() != block->GetTableId()) {
      auto s = schema_mgr->GetTableSchemaMgr(block->GetTableId(), tbl_schema_mgr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get table schema manager failed. table id: %lu", block->GetTableId());
        return s;
      }
    }
    std::shared_ptr<MMapMetricsTable> scan_metric = nullptr;
    TsBlockSpan* template_blk_span = nullptr;
    int nrow = block->GetRowNum();
    while (prev_end < block->GetRowNum()) {
      int start = prev_end;
      auto current_entity = entities[start];
      auto uppder_idx = *std::upper_bound(IndexRange{start}, IndexRange{nrow}, current_entity,
                                          [&](TSEntityID val, int idx) { return val < entities[idx]; });

      if (scan_metric == nullptr || scan_metric->GetVersion() != block->GetTableVersion()) {
        auto s = tbl_schema_mgr->GetMetricSchema(block->GetTableVersion(), &scan_metric);
        if (s != SUCCESS) {
          LOG_ERROR("GetMetricSchema failed. table id [%u], table version [%lu]",
            block->GetTableVersion(), block->GetTableId());
        }
      }
      std::shared_ptr<TsBlockSpan> cur_blk_span;
      auto s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, 0, current_entity, block, start, uppder_idx - start,
        scan_metric->GetVersion(), &(scan_metric->getSchemaInfoExcludeDropped()), tbl_schema_mgr, cur_blk_span);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("MakeNewBlockSpan failed, entity_id=%lu", current_entity);
        return s;
      }
      template_blk_span = cur_blk_span.get();
      block_spans.emplace_back(std::move(cur_blk_span));
      prev_end = uppder_idx;
    }
  }
  return SUCCESS;
}

using EntityTsPoint = std::tuple<TSEntityID, timestamp64>;
KStatus TsLastSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                     std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                      std::shared_ptr<MMapMetricsTable>& scan_schema) {
  assert(block_cache_ != nullptr);

  // if filter is empty, no need to do anything.
  if (filter.spans_.empty()) {
    return SUCCESS;
  }

  std::vector<TsLastSegmentBlockIndex>* p_block_indices;
  auto s = block_cache_->GetAllBlockIndex(&p_block_indices);
  if (s == FAIL) {
    return FAIL;
  }
  const std::vector<TsLastSegmentBlockIndex>& block_indices = *p_block_indices;
  assert(block_indices.size() == footer_.n_data_block);

  // find the first block which satisfies block.max_entity_id >= filter.entity_id
  auto begin_it = std::upper_bound(
      block_indices.begin(), block_indices.end(), filter.entity_id,
      [](TSEntityID entity_id, const TsLastSegmentBlockIndex& element) { return element.max_entity_id >= entity_id; });
  if (begin_it == block_indices.end()) {
    return SUCCESS;
  }

  // find the first block which satisfies block.min_entity_id > filter.entity_id
  auto end_it = std::upper_bound(
      block_indices.begin(), block_indices.end(), filter.entity_id,
      [](TSEntityID entity_id, const TsLastSegmentBlockIndex& element) { return element.min_entity_id > entity_id; });
  if (begin_it == end_it) {
    return SUCCESS;
  }
  assert(end_it > begin_it);

  std::shared_ptr<TsLastBlock> block = nullptr;
  TsBlockSpan* template_blk_span = nullptr;
  for (const auto& span : filter.spans_) {
    if (span.ts_span.begin > span.ts_span.end) {
      // invalid span, move to the next.
      continue;
    }

    EntityTsPoint filter_ts_span_start{filter.entity_id, span.ts_span.begin};
    EntityTsPoint filter_ts_span_end{filter.entity_id, span.ts_span.end};

    for (auto it = begin_it; it != end_it; ++it) {
      assert(it->max_entity_id >= filter.entity_id && it->min_entity_id <= filter.entity_id);
      if (it->table_id != filter.table_id) {
        continue;
      }
      if (it->max_ts < span.ts_span.begin || it->min_ts > span.ts_span.end) {
        continue;
      }
      if (it->max_osn < span.lsn_span.begin || it->min_osn > span.lsn_span.end) {
        continue;
      }
      //  we need to read the block to do further filtering.
      int block_idx = it - block_indices.begin();

      if (block == nullptr || block->GetBlockID() != block_idx) {
        s = this->GetBlock(block_idx, &block);
        if (s == FAIL) {
          return s;
        }
      }
      auto ts = block->GetTimestamps();
      auto entities = block->GetEntities();
      auto osn = block->GetOSN();
      if (ts == nullptr || entities == nullptr || osn == nullptr) {
        return FAIL;
      }

      // find the first row in the block that matches (eid, ts) >= (filter.eid, filter.start_ts).
      auto start_idx = *std::upper_bound(IndexRange{0}, IndexRange(block->GetRowNum()), filter_ts_span_start,
                                         [&](const EntityTsPoint& val, int idx) {
                                           EntityTsPoint data_point{entities[idx], ts[idx]};
                                           return val <= data_point;
                                         });
      if (start_idx == block->GetRowNum()) {
        // move to the next block
        continue;
      }

      // find the first row in the block that (eid, ts) > (filter.eid, filter.end_ts).
      auto end_idx = *std::upper_bound(IndexRange{start_idx}, IndexRange(block->GetRowNum()), filter_ts_span_end,
                                       [&](const EntityTsPoint& val, int idx) {
                                         EntityTsPoint data_point{entities[idx], ts[idx]};
                                         return data_point > val;
                                       });

      // no need to check whether idx_it == end(), the caculation are consistent no matter idx_it is valid or not.
      assert(end_idx >= start_idx);

      if (it->max_osn <= span.lsn_span.end && span.lsn_span.begin <= it->min_osn) {
        // all osn in the block is in the span, we can directly use the end_idx;
        if (end_idx - start_idx > 0) {
          std::shared_ptr<TsBlockSpan> cur_span;
          auto s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, block, start_idx,
                                        end_idx - start_idx, scan_schema->GetVersion(),
                                        scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_span);
          if (s != KStatus::SUCCESS) {
             LOG_ERROR("TsBlockSpan::GenDataConvertfailed, entity_id=%lu.", filter.entity_id);
              return s;
          }
          template_blk_span = cur_span.get();
          block_spans.push_back(std::move(cur_span));
        }
      } else {
        // we must filter OSN row-by-row
        int prev_idx = -1;  // invalide index
        for (int i = start_idx; i < end_idx; ++i) {
          if (span.lsn_span.begin <= osn[i] && osn[i] <= span.lsn_span.end) {
            prev_idx = prev_idx == -1 ? i : prev_idx;
            continue;
          }

          if (prev_idx != -1 && i - prev_idx > 0) {
            // we need to split the block into spans.
            std::shared_ptr<TsBlockSpan> cur_span;
            auto s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, block, prev_idx,
                                          i - prev_idx, scan_schema->GetVersion(),
                                          scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_span);
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("TsBlockSpan::GenDataConvertfailed, entity_id=%lu.", filter.entity_id);
                return s;
            }
            template_blk_span = cur_span.get();
            block_spans.push_back(std::move(cur_span));
          }
          prev_idx = -1;
        }

        if (prev_idx != -1 && end_idx - prev_idx > 0) {
          std::shared_ptr<TsBlockSpan> cur_span;
          auto s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, block, prev_idx,
                                        end_idx - prev_idx, scan_schema->GetVersion(),
                                        scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_span);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("TsBlockSpan::GenDataConvertfailed, entity_id=%lu.", filter.entity_id);
              return s;
          }
          template_blk_span = cur_span.get();
          block_spans.push_back(std::move(cur_span));
        }
      }
    }
  }
  return SUCCESS;
}

}  // namespace kwdbts
