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

  char* ptr = var_buf;
  GenCompAlg alg = static_cast<GenCompAlg>(*ptr);
  ptr++;
  const auto& snappy = SnappyString::GetInstance();
  std::string tmp;
  std::string out;
  if (alg == GenCompAlg::kSnappy) {
    if (snappy.Decompress({ptr, block_info.var_len - 1}, 0, &out)) {
      tmp.append(out);
    } else {
      assert(false);
    }
  } else {
    tmp.append({ptr, block_info.var_len - 1});
  }
  // save var data
  block->var_buffer.assign(tmp);
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
void TsLastSegmentManager::GetCompactLastSegments(
    std::vector<std::shared_ptr<TsLastSegment>>& result) {
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

std::unique_ptr<TsLastSegmentEntityBlockIteratorBase> TsLastSegment::NewIterator() const {
  auto iter = std::unique_ptr<TsLastSegmentFullEntityBlockIterator>(
      new TsLastSegmentFullEntityBlockIterator(this));
  iter->Init();
  return iter;
}

std::unique_ptr<TsLastSegmentEntityBlockIteratorBase> TsLastSegment::NewIterator(
    TSTableID table_id, TSEntityID entity_id, const std::vector<KwTsSpan>& spans) const {
  auto iter = std::unique_ptr<TsLastSegmentPartialEntityBlockIterator>(
      new TsLastSegmentPartialEntityBlockIterator(this, table_id, entity_id, spans));
  iter->Init();
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

static KStatus ReadColumnBlock(TsFile* file, const TsLastSegmentBlockInfo& info, int col_id,
                               std::string* col_data, std::unique_ptr<TsBitmap>* bitmap) {
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
  if (has_bitmap) {
    *bitmap = std::make_unique<TsBitmap>();
    BitmapCompAlg alg = static_cast<BitmapCompAlg>(buf[0]);
    switch (alg) {
      case BitmapCompAlg::kPlain: {
        size_t len = info.col_infos[col_id].bitmap_len - 1;
        **bitmap = TsBitmap({buf.get() + 1, len}, info.nrow);
        p_bitmap = bitmap->get();
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
  col_data->clear();
  if (compressor.IsPlain()) {
    col_data->assign(result.data, result.len);
  } else {
    ok = compressor.Decompress(result, p_bitmap, info.nrow, col_data);
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

void TsLastSegmentEntityBlockIteratorBase::EntityBlock::GetTSRange(timestamp64* min_ts,
                                                                   timestamp64* max_ts) {
  int row_start = current_.GetRowStart();
  int row_end = current_.GetRowEnd();
  int block_id = current_.block_id;
  parent_iter_->LoadColumnToCache(block_id, 2);
  timestamp64* ptr = reinterpret_cast<timestamp64*>(
      parent_iter_->cache_.GetColumnBlock(block_id, 2)->data->data());
  *min_ts = ptr[row_start];

  assert(row_end >= 1 && row_end <= parent_iter_->current_.info.nrow);
  *max_ts = ptr[row_end - 1];
}

KStatus TsLastSegmentEntityBlockIteratorBase::EntityBlock::GetValueSlice(
    int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) {
  int block_id = current_.block_id;
  parent_iter_->LoadColumnToCache(block_id, col_id + 2);  // Skip entity id & LSN;
  char* ptr = parent_iter_->cache_.GetColumnBlock(block_id, col_id + 2)->data->data();
  int dtype = schema[col_id].type;
  int dsize = 0;
  if (isVarLenType(dtype)) {
    parent_iter_->LoadVarcharToCache(block_id);
    const size_t* data = reinterpret_cast<const size_t*>(ptr);
    size_t offset = data[row_num + current_.GetRowStart()];
    TSSlice result{parent_iter_->cache_.GetColumnBlock(block_id, -1)->data->data() + offset, 2};
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
    value.data = ptr + dsize * (row_num + current_.GetRowStart());
  }
  return SUCCESS;
}

timestamp64 TsLastSegmentEntityBlockIteratorBase::EntityBlock::GetTS(
    int row_num, const std::vector<AttributeInfo>& schema) {
  parent_iter_->LoadColumnToCache(current_.GetRowStart(), 2);
  timestamp64* ptr = reinterpret_cast<timestamp64*>(
      parent_iter_->cache_.GetColumnBlock(current_.block_id, 2)->data->data());
  return ptr[row_num + current_.GetRowStart()];
}

bool TsLastSegmentEntityBlockIteratorBase::EntityBlock::IsColNull(
    int row_num, int col_id, const std::vector<AttributeInfo>& schema) {
  int block_id = current_.block_id;
  parent_iter_->LoadColumnToCache(block_id, col_id + 2);
  auto col_block = parent_iter_->cache_.GetColumnBlock(block_id, col_id + 2);
  if (!col_block->HasBitmap()) {
    return false;
  }
  return (*col_block->bitmap)[row_num + current_.GetRowStart()] == kNull;
}

KStatus TsLastSegmentEntityBlockIteratorBase::EntityBlock::GetColAddr(
    uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value, TsBitmap& bitmap) {
  return KStatus::FAIL;
}

KStatus TsLastSegmentEntityBlockIteratorBase::LoadBlockIndex() {
  TsLastSegmentFooter footer;
  auto s = lastsegment_->GetFooter(&footer);
  if (s != SUCCESS) {
    return FAIL;
  }
  block_indices_.resize(footer.n_data_block);
  TSSlice result;
  auto buf = std::make_unique<char[]>(footer.n_data_block * sizeof(TsLastSegmentBlockIndex));
  lastsegment_->file_->Read(footer.block_info_idx_offset,
                            block_indices_.size() * sizeof(TsLastSegmentBlockIndex), &result,
                            buf.get());
  assert(result.len == block_indices_.size() * sizeof(TsLastSegmentBlockIndex));
  for (int i = 0; i < block_indices_.size(); ++i) {
    GetFixed64(&result, &block_indices_[i].offset);
    GetFixed64(&result, &block_indices_[i].length);
    GetFixed64(&result, &block_indices_[i].table_id);
    GetFixed32(&result, &block_indices_[i].table_version);
    GetFixed32(&result, &block_indices_[i].n_entity);
    uint64_t v;
    GetFixed64(&result, &v);
    block_indices_[i].min_ts = v;
    GetFixed64(&result, &v);
    block_indices_[i].max_ts = v;
    GetFixed64(&result, &block_indices_[i].min_entity_id);
    GetFixed64(&result, &block_indices_[i].max_entity_id);
  }
  assert(result.len == 0);
  return SUCCESS;
}

auto TsLastSegmentEntityBlockIteratorBase::GetEntityBlock() -> std::unique_ptr<EntityBlock> {
  assert(Valid());
  return std::make_unique<EntityBlock>(this);
}

KStatus TsLastSegmentEntityBlockIteratorBase::LoadColumnToCache(int block_id, int col_id) {
  if (!cache_.HasCached(block_id, col_id)) {
    std::string out;
    std::unique_ptr<TsBitmap> bitmap;
    auto s = ReadColumnBlock(lastsegment_->file_.get(), current_.info, col_id, &out, &bitmap);
    if (s == FAIL) {
      LOG_ERROR("cannot read column block from lastsegment");
      return FAIL;
    }
    cache_.PutData(block_id, col_id, std::move(out));
    if (bitmap != nullptr) {
      cache_.PutBitmap(block_id, col_id, std::move(bitmap));
    }
  }
  return SUCCESS;
}

KStatus TsLastSegmentEntityBlockIteratorBase::LoadVarcharToCache(int block_id) {
  if (!cache_.HasCached(block_id, -1)) {
    std::string out;
    auto s = ReadVarcharBlock(lastsegment_->file_.get(), current_.info, &out);
    if (s == FAIL) {
      LOG_ERROR("cannot read varchar block from lastsegment");
      return FAIL;
    }
    cache_.PutData(block_id, -1, std::move(out));
  }
  return SUCCESS;
}

KStatus TsLastSegmentEntityBlockIteratorBase::LoadToCurrentBlockCache() {
  if (idx_ == current_.block_id) {
    return SUCCESS;
  }
  const auto& cur_block = block_indices_[idx_];
  TSSlice result;
  auto buf = std::make_unique<char[]>(cur_block.length);
  lastsegment_->file_->Read(cur_block.offset, cur_block.length, &result, buf.get());
  ParseBlockInfo(result, &current_.info);
  current_.block_id = idx_;

  auto s = LoadColumnToCache(idx_, 0);  // 0 for entity id
  if (s == FAIL) {
    return FAIL;
  }
  current_.entities = reinterpret_cast<TSEntityID*>(cache_.GetColumnBlock(idx_, 0)->data->data());
  s = LoadColumnToCache(idx_, 2);
  if (s == FAIL) {
    return FAIL;
  }
  current_.timestamps =
      reinterpret_cast<timestamp64*>(cache_.GetColumnBlock(idx_, 2)->data->data());
  return SUCCESS;
}

auto TsLastSegmentEntityBlockIteratorBase::GetAllEntityBlocks()
    -> std::vector<std::unique_ptr<EntityBlock>> {
  std::vector<std::unique_ptr<EntityBlock>> result;
  SeekToFirst();
  while (Valid()) {
    result.push_back(GetEntityBlock());
    NextEntityBlock();
  }
  return result;
}

KStatus TsLastSegmentFullEntityBlockIterator::Init() {
  LoadBlockIndex();
  return SeekToFirst();
}

KStatus TsLastSegmentFullEntityBlockIterator::SeekToFirst() {
  idx_ = 0;
  auto s = LoadToCurrentBlockCache();
  if (s == FAIL) {
    Invalidate();
    return FAIL;
  }
  current_.SetRowStart(0);
  current_.FindUpperBound(current_.GetEntityID(), INT64_MAX);
  return SUCCESS;
}

void TsLastSegmentFullEntityBlockIterator::NextEntityBlock() {
  if (current_.Finished()) {
    NextBlock();
    if (!Valid()) return;
    current_.SetRowStart(0);
    current_.FindUpperBound(current_.GetEntityID(), INT64_MAX);
  } else {
    current_.SetRowStart(current_.GetRowEnd());
    current_.FindUpperBound(current_.GetEntityID(), INT64_MAX);
  }
}

void TsLastSegmentPartialEntityBlockIterator::LocateToSpan(const KwTsSpan& span) {
  int64_t min_ts = span.begin;
  int64_t max_ts = span.end;
  if (max_ts < min_ts) {
    Invalidate();
    return;
  }
  int i = idx_;

  for (; i < block_indices_.size(); ++i) {
    const auto& cur_block = block_indices_[i];
    if (cur_block.table_id == table_id_ && cur_block.min_entity_id <= entity_id_ &&
        cur_block.max_entity_id >= entity_id_ &&
        !(max_ts < cur_block.min_ts && min_ts > cur_block.max_ts))
      break;
  }
  if (i == block_indices_.size()) {
    Invalidate();
    return;
  }
  idx_ = i;
}

KStatus TsLastSegmentPartialEntityBlockIterator::Init() {
  if (spans_.empty()) {
    return FAIL;
  }
  LoadBlockIndex();
  SeekToFirst();
  return SUCCESS;
}

KStatus TsLastSegmentPartialEntityBlockIterator::SeekToFirst() {
  span_idx_ = -1;
  idx_ = 0;
  MoveToNextSpan();
  return SUCCESS;
}

void TsLastSegmentPartialEntityBlockIterator::MoveToNextSpan() {
  ++span_idx_;
  if (span_idx_ >= spans_.size()) {
    Invalidate();
    return;
  }
  LocateToSpan(CurrentSpan());
  if (!Valid()) return;

  LoadToCurrentBlockCache();
  current_.FindLowerBound(entity_id_, CurrentSpan().begin);
  if (current_.GetRowStart() >= current_.info.nrow) {
    Invalidate();
    return;
  }

  current_.FindUpperBound(entity_id_, CurrentSpan().end);
  if (current_.GetRowCount() == 0) {
    Invalidate();
  }
}

void TsLastSegmentPartialEntityBlockIterator::NextEntityBlock() {
  if (current_.Finished()) {
    NextBlock();
    if (!Valid()) return;
    current_.SetRowStart(0);
    if (current_.GetEntityID() != entity_id_ || current_.GetStartTs() > CurrentSpan().end) {
      MoveToNextSpan();
    } else {
      current_.FindUpperBound(entity_id_, CurrentSpan().end);
    }
  } else {
    MoveToNextSpan();
  }
}

struct Element_ {
  TSEntityID e_id;
  timestamp64 ts;
  bool operator==(const Element_& rhs) const { return e_id == rhs.e_id && ts == rhs.ts; }
};

static inline bool CompareLess(const Element_& lhs, const Element_& rhs) {
  return lhs.e_id < rhs.e_id || (lhs.e_id == rhs.e_id && lhs.ts < rhs.ts);
}

void TsLastSegmentEntityBlockIteratorBase::CurrentBlock::FindLowerBound(TSEntityID e_id,
                                                                        timestamp64 ts) {
  int l = 0, r = info.nrow;
  Element_ target{e_id, ts};
  while (r - l > 0) {
    int m = (l + r) / 2;
    Element_ current{entities[m], timestamps[m]};
    if (CompareLess(current, target)) {
      l = m + 1;
      continue;
    }
    r = m;
  }
  row_start = r;
  if (row_start < info.nrow) {
    entity_id_ = entities[r];
  }
}

void TsLastSegmentEntityBlockIteratorBase::CurrentBlock::FindUpperBound(TSEntityID e_id,
                                                                        timestamp64 ts) {
  int l = row_start, r = info.nrow;
  Element_ target{e_id, ts};
  while (r - l > 0) {
    int m = (l + r) / 2;
    Element_ current{entities[m], timestamps[m]};
    if (CompareLess(current, target) || current == target) {
      l = m + 1;
      continue;
    }
    r = m;
  }
  row_end = r;
}

}  // namespace kwdbts
