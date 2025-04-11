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
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_coding.h"
#include "ts_compressor.h"
#include "ts_compressor_impl.h"
#include "ts_io.h"
#include "ts_lastsegment_manager.h"
#include "utils/big_table_utils.h"
namespace kwdbts {

int TsLastSegment::kNRowPerBlock = 4096;

KStatus TsLastSegment::Append(const TSSlice& data) { return file_->Append(data); }

KStatus TsLastSegment::Flush() { return file_->Flush(); }

size_t TsLastSegment::GetFileSize() const { return file_->GetFileSize(); }

TsFile* TsLastSegment::GetFilePtr() const { return file_.get(); }

uint32_t TsLastSegment::GetVersion() const { return ver_; }

KStatus TsLastSegment::GetFooter(TsLastSegmentFooter* footer) const {
  TSSlice result;
  size_t offset = file_->GetFileSize() - sizeof(TsLastSegmentFooter);
  file_->Read(offset, sizeof(TsLastSegmentFooter), &result, reinterpret_cast<char*>(footer));
  if (result.len != sizeof(TsLastSegmentFooter) || footer->magic_number != FOOTER_MAGIC) {
    LOG_ERROR("last segment[%s] GetFooter failed.", file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(TsLastSegmentFooter& footer,
                                        std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TSSlice result;
  uint64_t nblock = footer.n_data_block;
  block_indexes->resize(nblock);
  for (uint64_t i = 0; i < nblock; ++i) {
    file_->Read(footer.block_info_idx_offset + i * sizeof(TsLastSegmentBlockIndex),
                sizeof(TsLastSegmentBlockIndex), &result,
                reinterpret_cast<char*>(&(*block_indexes)[i]));
    if (result.len != sizeof(TsLastSegmentBlockIndex)) {
      LOG_ERROR("last segment[%s] GetAllBlockIndex failed.", file_->GetFilePath().c_str());
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TsLastSegmentFooter last_segment_footer;
  KStatus s = GetFooter(&last_segment_footer);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = GetAllBlockIndex(last_segment_footer, block_indexes);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlockInfo(TsLastSegmentBlockIndex& block_index,
                                    TsLastSegmentBlockInfo* block_info) {
  TSSlice result;
  // block info header
  auto buf = std::make_unique<char[]>(block_index.length);
  file_->Read(block_index.offset, block_index.length, &result, buf.get());
  if (result.len != block_index.length) {
    LOG_ERROR(
        "last segment[%s] GetBlockInfo failed, read header failed. "
        "table id: %lu, table version: %u, block info offset: %lu.",
        file_->GetFilePath().c_str(), block_index.table_id, block_index.table_version,
        block_index.offset);
    return KStatus::FAIL;
  }
  const char* ptr = buf.get();
  // TODO(zzr): endian problems
  memcpy(block_info, ptr, LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE);
  ptr += LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE;

  // block info column offset
  block_info->col_infos.resize(block_info->ncol);
  for (size_t col_idx = 0; col_idx < block_info->ncol; ++col_idx) {
    block_info->col_infos[col_idx].offset = DecodeFixed32(ptr);
    ptr += 4;
    block_info->col_infos[col_idx].bitmap_len = DecodeFixed16(ptr);
    ptr += 2;
    block_info->col_infos[col_idx].data_len = DecodeFixed32(ptr);
    ptr += 4;
  }
  assert(ptr == result.data + result.len);
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block) {
  block->column_blocks.resize(block_info.ncol);
  // column block
  TSSlice result;
  size_t offset = block_info.block_offset;
  for (uint32_t i = 0; i < block_info.ncol; ++i) {
    size_t col_block_len = block_info.col_infos[i].bitmap_len + block_info.col_infos[i].data_len;
    // read col block data
    auto col_block_buf = std::make_unique<char[]>(col_block_len);
    file_->Read(offset, col_block_len, &result, col_block_buf.get());
    if (result.len != col_block_len) {
      LOG_ERROR("last segment[%s] GetBlock failed, read column block[%u] failed.",
                file_->GetFilePath().c_str(), i);
      return KStatus::FAIL;
    }

    // Decompress:

    // parse TsBitmap;
    const TsBitmap* p_bitmap = nullptr;
    TsBitmap tmp;
    bool has_bitmap = block_info.col_infos[i].bitmap_len != 0;
    if (has_bitmap) {
      // TODO(zzr) decompress bitmap first, compression for bitmap is not implemented yet.
      BitmapCompAlg comp_type = static_cast<BitmapCompAlg>(*col_block_buf.get());
      if (comp_type == BitmapCompAlg::kPlain) {
        size_t raw_bitmap_len = block_info.col_infos[i].bitmap_len - 1;
        TSSlice raw_bitmap{col_block_buf.get() + 1, raw_bitmap_len};
        tmp.Map(raw_bitmap, block_info.nrow);
        p_bitmap = &tmp;
      } else {
        assert(false);  // bitmap compression not implemented
      }
    }

    // parse Data
    char* ptr = col_block_buf.get() + block_info.col_infos[i].bitmap_len;
    TsCompAlg first = static_cast<TsCompAlg>(*ptr);
    ptr++;
    GenCompAlg second = static_cast<GenCompAlg>(*ptr);
    ptr++;
    assert(first < TsCompAlg::TS_COMP_ALG_LAST && second < GenCompAlg::GEN_COMP_ALG_LAST);
    auto compressor = CompressorManager::GetInstance().GetCompressor(first, second);

    std::string_view plain_sv{ptr, block_info.col_infos[i].data_len - 2};
    std::string plain;
    if (compressor.IsPlain()) {
    } else {
      bool ok = compressor.Decompress({ptr, plain_sv.size()}, p_bitmap, block_info.nrow, &plain);
      if (!ok) {
        LOG_ERROR("last segment[%s] GetBlock failed, decode column block[%u] failed.",
                  file_->GetFilePath().c_str(), i);
        return KStatus::FAIL;
      }
      plain_sv = plain;
    }

    // save decompressed col block data
    block->column_blocks[i].buffer.assign(plain_sv);
    if (has_bitmap) {
      block->column_blocks[i].bitmap = *p_bitmap;  // copy
    }
    offset += col_block_len;
  }
  // read var data
  char* var_buf = new char[block_info.var_len];
  file_->Read(offset, block_info.var_len, &result, var_buf);
  if (result.len != block_info.var_len) {
    delete[] var_buf;
    LOG_ERROR("last segment[%s] GetBlock failed, read var data failed.",
              file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  // save var data
  block->var_buffer.assign(var_buf, block_info.var_len);
  delete[] var_buf;

  return KStatus::SUCCESS;
}

KStatus TsLastSegmentManager::NewLastSegment(std::unique_ptr<TsLastSegment>* last_segment) {
  char buffer[64];
  ver_.fetch_add(1, std::memory_order_relaxed);
  std::snprintf(buffer, sizeof(buffer), "last.ver-%04u", ver_.load(std::memory_order_relaxed));
  auto filename = dir_path_ / buffer;
  *last_segment =
      std::make_unique<TsLastSegment>(ver_, new TsMMapFile(filename, false /*read_only*/));
  return KStatus::SUCCESS;
}

void TsLastSegmentManager::TakeLastSegmentOwnership(std::unique_ptr<TsLastSegment>&& last_segment) {
  wrLock();
  last_segments_.emplace_back(std::move(last_segment));
  n_lastsegment_.fetch_add(1, std::memory_order_relaxed);
  unLock();
}

// TODO(zzr) get last segments from VersionManager, this method must be atomic
void TsLastSegmentManager::GetCompactLastSegments(std::vector<std::shared_ptr<TsLastSegment>>& result) {
  rdLock();
  size_t compact_num = std::min(last_segments_.size(), static_cast<size_t>(MAX_COMPACT_NUM));
  result.assign(last_segments_.begin(), last_segments_.begin() + compact_num);
  unLock();
}

bool TsLastSegmentManager::NeedCompact() {
  return n_lastsegment_.load(std::memory_order_relaxed) > MAX_LAST_SEGMENT_NUM;
}

void TsLastSegmentManager::ClearLastSegments(uint32_t ver) {
  wrLock();
  for (auto it = last_segments_.begin(); it != last_segments_.end();) {
    if ((*it)->GetVersion() <= ver) {
      (*it)->GetFilePtr()->MarkDelete();
      it = last_segments_.erase(it);
      n_lastsegment_.fetch_sub(1, std::memory_order_relaxed);
    } else {
      ++it;
    }
  }
  unLock();
}

std::unique_ptr<TsLastSegmentBlockIterator> TsLastSegment::NewIterator(Allocator* alloc) const {
  auto iter =
      std::unique_ptr<TsLastSegmentBlockIterator>(new TsLastSegmentBlockIterator(alloc, this));
  return iter;
}

std::unique_ptr<TsLastSegmentBlockIterator> TsLastSegment::NewIterator(Allocator* alloc,
                                                                       TSTableID table_id,
                                                                       TSEntityID entity_id,
                                                                       timestamp64 min_ts,
                                                                       timestamp64 max_ts) const {
  auto iter = std::unique_ptr<TsLastSegmentBlockIterator>(
      new TsLastSegmentBlockIterator(alloc, this, table_id, entity_id, min_ts, max_ts));
  return iter;
}

static void ParseBlockInfo(TSSlice data, TsLastSegmentBlockInfo* info) {
  GetFixed64(&data, &info->block_offset);
  GetFixed32(&data, &info->nrow);
  GetFixed32(&data, &info->ncol);
  GetFixed32(&data, &info->var_offset);
  GetFixed32(&data, &info->var_len);
  info->col_infos.resize(info->ncol);
  for (int i = 0; i < info->ncol; ++i) {
    GetFixed32(&data, &info->col_infos[i].offset);
    GetFixed16(&data, &info->col_infos[i].bitmap_len);
    GetFixed32(&data, &info->col_infos[i].data_len);
  }
  assert(data.len == 0);
}

TsLastSegmentBlockIterator::TsLastSegmentBlockIterator(Allocator* alloc, const TsLastSegment* last)
    : alloc_(alloc),
      lastsegment_(last),
      specfic_scan_(false),
      min_ts_(INT64_MIN),
      max_ts_(INT64_MAX),
      entityblock_buffer_(std::make_unique<CurrentBlockCache>()) {
  LoadBlockIndex();
  curr_idx_ = 0;
  if (!Valid()) return;
  LoadToCurrentBlockCache();
  entityblock_buffer_->row_start = 0;  // begin from the first;
  entity_id_ = entityblock_buffer_->entities[0];
  LocateUpperBound();
}

TsLastSegmentBlockIterator::TsLastSegmentBlockIterator(Allocator* alloc, const TsLastSegment* last,
                                                       TSTableID table_id, TSEntityID entity_id,
                                                       timestamp64 min_ts, timestamp64 max_ts)
    : alloc_(alloc),
      lastsegment_(last),
      specfic_scan_(true),
      table_id_(table_id),
      entity_id_(entity_id),
      min_ts_(min_ts),
      max_ts_(max_ts),
      entityblock_buffer_(std::make_unique<CurrentBlockCache>()) {
  LoadBlockIndex();
  curr_idx_ = 0;
  for (; curr_idx_ < block_idx_.size(); ++curr_idx_) {
    const auto& cur_block = block_idx_[curr_idx_];
    if (cur_block.table_id == table_id && cur_block.min_entity_id <= entity_id &&
        cur_block.max_entity_id >= entity_id &&
        !(max_ts < cur_block.min_ts && min_ts > cur_block.max_ts))
      break;
  }
  if (!Valid()) return;
  LoadToCurrentBlockCache();
  LocateLowerBound();
  if (entityblock_buffer_->row_start == entityblock_buffer_->info.nrow) {
    // cannot find entity in this table, invalidate the iterator;
    Invalidate();
    return;
  }
  LocateUpperBound();
  if (entityblock_buffer_->row_end <= entityblock_buffer_->row_start) {
    Invalidate();
  }
}

void TsLastSegmentBlockIterator::NextBlock() {
  ++curr_idx_;
  column_block_cache_.clear();
  if (!Valid()) return;
  LoadToCurrentBlockCache();
}

void TsLastSegmentBlockIterator::NextEntityBlock() {
  if (!specfic_scan_) {
    if (entityblock_buffer_->row_end == entityblock_buffer_->info.nrow) {
      // move to the next lastsegment block
      NextBlock();
      if (!Valid()) return;
      entityblock_buffer_->row_start = 0;
    } else {
      entityblock_buffer_->row_start = entityblock_buffer_->row_end;
    }
    entity_id_ = entityblock_buffer_->entities[entityblock_buffer_->row_start];
    LocateUpperBound();
  } else {
    if (entityblock_buffer_->row_end < entityblock_buffer_->info.nrow) {
      // finish, invalidate the iterator
      Invalidate();
      return;
    }
    NextBlock();
    if (!Valid()) return;
    entityblock_buffer_->row_start = 0;
    if (entity_id_ != entityblock_buffer_->entities[entityblock_buffer_->row_start]) {
      Invalidate();
      return;
    }
    LocateUpperBound();
  }
}

void TsLastSegmentBlockIterator::LoadBlockIndex() {
  TsLastSegmentFooter footer;
  auto s = lastsegment_->GetFooter(&footer);
  assert(s == SUCCESS);
  block_idx_.resize(footer.n_data_block);
  TSSlice result;
  lastsegment_->file_->Read(footer.block_info_idx_offset,
                            block_idx_.size() * sizeof(TsLastSegmentBlockIndex), &result,
                            reinterpret_cast<char*>(block_idx_.data()));
  assert(result.len == block_idx_.size() * sizeof(TsLastSegmentBlockIndex));
}

static KStatus ReadColumnBlock(TsFile* file, const TsLastSegmentBlockInfo& info, int col_id,
                               std::string* out) {
  size_t offset = info.block_offset + info.col_infos[col_id].offset;
  size_t len = info.col_infos[col_id].bitmap_len + info.col_infos[col_id].data_len;
  bool has_bitmap = info.col_infos[col_id].bitmap_len != 0;
  TSSlice result;
  auto buf = std::make_unique<char[]>(len);
  auto s = file->Read(offset, len, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }
  TsBitmap* p_bitmap = nullptr;
  TsBitmap bitmap;
  if (has_bitmap) {
    BitmapCompAlg alg = static_cast<BitmapCompAlg>(buf[0]);
    switch (alg) {
      case BitmapCompAlg::kPlain: {
        size_t len = info.col_infos[col_id].bitmap_len - 1;
        bitmap = TsBitmap({buf.get() + 1, len}, info.nrow);
        p_bitmap = &bitmap;
        break;
      }
      case BitmapCompAlg::kCompressed:
        assert(false);
      default:
        assert(false);
    }
  }

  // Metric
  RemovePrefix(&result, info.col_infos[col_id].bitmap_len);
  assert(result.len >= 2);
  auto first = static_cast<TsCompAlg>(result.data[0]);
  auto second = static_cast<GenCompAlg>(result.data[1]);
  const auto& compressor = CompressorManager::GetInstance().GetCompressor(first, second);
  RemovePrefix(&result, 2);
  bool ok = true;
  out->clear();
  if (compressor.IsPlain()) {
    out->assign(result.data, result.len);
  } else {
    ok = compressor.Decompress(result, p_bitmap, info.nrow, out);
  }

  return ok ? SUCCESS : FAIL;
}

static KStatus ReadVarcharBlock(TsFile* file, const TsLastSegmentBlockInfo& info,
                                std::string* out) {
  bool has_varchar = info.var_offset != 0;
  if (!has_varchar) {
    LOG_ERROR("no varcha block to read");
    return FAIL;
  }
  size_t offset = info.block_offset + info.var_offset;
  size_t len = info.var_len;
  auto buf = std::make_unique<char[]>(len);
  TSSlice result;

  assert(len > 0);
  file->Read(offset, len, &result, buf.get());
  GenCompAlg type = static_cast<GenCompAlg>(buf[0]);
  RemovePrefix(&result, 1);
  int ok = true;
  switch (type) {
    case GenCompAlg::kPlain: {
      out->assign(result.data, result.len);
      break;
    }
    case GenCompAlg::kSnappy: {
      const auto& snappy = SnappyString::GetInstance();
      ok = snappy.Decompress(result, 0, out);
      break;
    }
    default:
      assert(false);
  }
  return ok ? SUCCESS : FAIL;
}

KStatus TsLastSegmentBlockIterator::LoadToCurrentBlockCache() {
  if (curr_idx_ == entityblock_buffer_->block_id) {
    return SUCCESS;
  }
  const auto& cur_block = block_idx_[curr_idx_];
  TSSlice result;
  auto buf = std::make_unique<char[]>(cur_block.length);
  lastsegment_->file_->Read(cur_block.offset, cur_block.length, &result, buf.get());
  ParseBlockInfo(result, &entityblock_buffer_->info);
  entityblock_buffer_->block_id = curr_idx_;

  auto s = LoadColumnToCache(0);  // 0 for entity id
  if (s == FAIL) {
    return FAIL;
  }
  entityblock_buffer_->entities = reinterpret_cast<TSEntityID*>(column_block_cache_[0]->data());

  // need to load ts column;
  if (specfic_scan_) {
    s = LoadColumnToCache(2);
    if (s == FAIL) {
      return FAIL;
    }
    entityblock_buffer_->ts = reinterpret_cast<timestamp64*>(column_block_cache_[2]->data());
  }
  return SUCCESS;
}

KStatus TsLastSegmentBlockIterator::LoadColumnToCache(int col_id) {
  if (column_block_cache_[col_id] == nullptr) {
    std::string out;
    auto s = ReadColumnBlock(lastsegment_->file_.get(), entityblock_buffer_->info, col_id, &out);
    if (s == FAIL) {
      LOG_ERROR("cannot read column block from lastsegment");
      return FAIL;
    }
    column_block_cache_[col_id] = std::make_unique<std::string>(std::move(out));
  }
  return SUCCESS;
}

KStatus TsLastSegmentBlockIterator::LoadVarcharToCache() {
  if (column_block_cache_[-1] == nullptr) {
    std::string out;
    auto s = ReadVarcharBlock(lastsegment_->file_.get(), entityblock_buffer_->info, &out);
    if (s == FAIL) {
      LOG_ERROR("cannot read varchar block from lastsegment");
      return FAIL;
    }
    column_block_cache_[-1] = std::make_unique<std::string>(std::move(out));
  }
  return SUCCESS;
}

void TsLastSegmentBlockIterator::LocateUpperBound() {
  // TODO(zzr) can optimize the complicity to O(logn)
  int idx = entityblock_buffer_->row_start;
  for (; idx < entityblock_buffer_->info.nrow; ++idx) {
    if (entityblock_buffer_->entities[idx] != entity_id_) break;
  }
  entityblock_buffer_->row_end = idx;
  if (!specfic_scan_) return;
  idx = entityblock_buffer_->row_start;
  for (; idx < entityblock_buffer_->row_end; ++idx) {
    if (entityblock_buffer_->ts[idx] > max_ts_) break;
  }
  entityblock_buffer_->row_end = idx;
}

void TsLastSegmentBlockIterator::LocateLowerBound() {
  // TODO(zzr) can optimize the complicity to O(logn)
  int idx = 0;
  for (; idx < entityblock_buffer_->info.nrow; ++idx) {
    if (entityblock_buffer_->entities[idx] == entity_id_) break;
  }
  entityblock_buffer_->row_start = idx;
  if (!specfic_scan_) return;

  for (; idx < entityblock_buffer_->info.nrow; ++idx) {
    if (entityblock_buffer_->ts[idx] >= min_ts_) break;
  }
  entityblock_buffer_->row_start = idx;
}

auto TsLastSegmentBlockIterator::GetEntityBlock() -> EntityBlock* {
  assert(Valid());
  auto ptr = alloc_->AllocateAligned(sizeof(EntityBlock));
  auto res = new (ptr) EntityBlock(this);
  return res;
}

void TsLastSegmentBlockIterator::EntityBlock::GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
  parent_iter_->LoadColumnToCache(2);
  int begin = parent_iter_->entityblock_buffer_->row_start;
  int end = parent_iter_->entityblock_buffer_->row_end;
  timestamp64* ptr = reinterpret_cast<timestamp64*>(parent_iter_->column_block_cache_[2]->data());
  *min_ts = ptr[begin];

  assert(end >= 1 && end < parent_iter_->entityblock_buffer_->info.nrow);
  *max_ts = ptr[end - 1];
}

KStatus TsLastSegmentBlockIterator::EntityBlock::GetValueSlice(
    int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) {
  parent_iter_->LoadColumnToCache(col_id + 2);  // Skip entity id & LSN;
  char* ptr = parent_iter_->column_block_cache_[col_id + 2]->data();
  int dtype = schema[col_id].type;
  int dsize = 0;
  if (isVarLenType(dtype)) {
    parent_iter_->LoadVarcharToCache();
    const size_t* data = reinterpret_cast<const size_t*>(ptr);
    size_t offset = data[row_num + parent_iter_->entityblock_buffer_->row_start];
    TSSlice result{parent_iter_->column_block_cache_[-1]->data() + offset, 2};
    uint16_t len;
    GetFixed16(&result, &len);
    value.data = result.data;
    value.len = len;
  } else {
    if (dtype == TIMESTAMP64_LSN_MICRO || dtype == TIMESTAMP64_LSN ||
        dtype == TIMESTAMP64_LSN_NANO) {
      // discard LSN
      dsize = 8;
    } else {
      dsize = getDataTypeSize(dtype);
    }
    value.len = dsize;
    value.data = ptr + dsize * (row_num + parent_iter_->entityblock_buffer_->row_start);
  }
  return SUCCESS;
}

timestamp64 TsLastSegmentBlockIterator::EntityBlock::GetTS(int row_num, const std::vector<AttributeInfo>& schema) {
  parent_iter_->LoadColumnToCache(2);
  timestamp64* ptr = reinterpret_cast<timestamp64*>(parent_iter_->column_block_cache_[2]->data());
  return ptr[row_num + parent_iter_->entityblock_buffer_->row_start];
}

}  // namespace kwdbts
