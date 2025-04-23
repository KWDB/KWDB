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
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_block.h"
#include "ts_coding.h"
#include "ts_compressor.h"
#include "ts_compressor_impl.h"
#include "ts_io.h"
#include "ts_lastsegment_manager.h"
#include "ts_segment.h"
#include "utils/big_table_utils.h"
namespace kwdbts {

int TsLastSegment::kNRowPerBlock = 4096;

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

static KStatus LoadBlockInfo(TsFile* file, const TsLastSegmentBlockIndex& index,
                             TsLastSegmentBlockInfo* info) {
  assert(info != nullptr);
  TSSlice result;
  auto buf = std::make_unique<char[]>(index.length);
  auto s = file->Read(index.offset, index.length, &result, buf.get());
  if (s != SUCCESS) {
    return s;
  }
  ParseBlockInfo(result, info);
  return SUCCESS;
}

static KStatus ReadColumnBitmap(TsFile* file, const TsLastSegmentBlockInfo& info, int col_id,
                                std::unique_ptr<TsBitmap>* bitmap) {
  size_t offset = info.block_offset + info.col_infos[col_id].offset;
  size_t len = info.col_infos[col_id].bitmap_len;
  bool has_bitmap = info.col_infos[col_id].bitmap_len != 0;
  TSSlice result;
  auto buf = std::make_unique<char[]>(len);
  auto s = file->Read(offset, len, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }

  bitmap->reset();
  if (has_bitmap) {
    BitmapCompAlg alg = static_cast<BitmapCompAlg>(buf[0]);
    switch (alg) {
      case BitmapCompAlg::kPlain: {
        size_t len = info.col_infos[col_id].bitmap_len - 1;
        *bitmap = std::make_unique<TsBitmap>(TSSlice{buf.get() + 1, len}, info.nrow);
        break;
      }
      case BitmapCompAlg::kCompressed:
        assert(false);
      default:
        assert(false);
    }
  }
  return SUCCESS;
}

static KStatus ReadColumnBlock(TsFile* file, const TsLastSegmentBlockInfo& info, int col_id,
                               std::string* col_data, std::unique_ptr<TsBitmap>* bitmap) {
  bitmap->reset();
  ReadColumnBitmap(file, info, col_id, bitmap);
  size_t offset = info.block_offset + info.col_infos[col_id].offset;
  offset += info.col_infos[col_id].bitmap_len;
  size_t len = info.col_infos[col_id].data_len;
  TSSlice result;
  auto buf = std::make_unique<char[]>(len);
  auto s = file->Read(offset, len, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }

  // Metric
  assert(result.len >= 2);
  auto first = static_cast<TsCompAlg>(result.data[0]);
  auto second = static_cast<GenCompAlg>(result.data[1]);
  assert(first < TsCompAlg::TS_COMP_ALG_LAST && second < GenCompAlg::GEN_COMP_ALG_LAST);
  const auto& compressor = CompressorManager::GetInstance().GetCompressor(first, second);
  RemovePrefix(&result, 2);
  bool ok = true;
  col_data->clear();
  if (compressor.IsPlain()) {
    col_data->assign(result.data, result.len);
  } else {
    ok = compressor.Decompress(result, bitmap->get(), info.nrow, col_data);
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

std::string TsLastSegmentManager::LastSegmentFileName(uint32_t file_number) const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "last.ver-%012u", file_number);
  auto filename = dir_path_ / buffer;
  return filename;
}

KStatus TsLastSegmentManager::NewLastSegmentFile(std::unique_ptr<TsFile>* last_segment,
                                                 uint32_t* file_number) {
  *file_number = current_file_number_.fetch_add(1, std::memory_order_relaxed);
  auto filename = LastSegmentFileName(*file_number);
  *last_segment = std::make_unique<TsMMapFile>(filename, false /*read_only*/);
  return KStatus::SUCCESS;
}

KStatus TsLastSegmentManager::OpenLastSegmentFile(uint32_t file_number,
                                                  std::shared_ptr<TsLastSegment>* lastsegment) {
  // 1. find from cache.
  {
    std::shared_lock lk{s_mutex_};
    auto it = last_segments_.find(file_number);
    if (it != last_segments_.end()) {
      // found, assign and return
      *lastsegment = it->second;
      return SUCCESS;
    }
  }

  // 2. open from disk.
  auto file = TsLastSegment::Create(file_number, LastSegmentFileName(file_number));
  auto s = file->Open();
  if (s == FAIL) {
    LOG_ERROR("can not open file %s", LastSegmentFileName(file_number).c_str());
    return FAIL;
  }
  {
    std::unique_lock lk{s_mutex_};
    // find again before insert
    auto it = last_segments_.find(file_number);
    if (it != last_segments_.end()) {
      *lastsegment = it->second;
      return SUCCESS;
    }

    // now we can really insert it to cache
    *lastsegment = file;
    auto [iter, ok] = last_segments_.insert_or_assign(file_number, std::move(file));
    assert(ok);
  }
  n_lastsegment_.fetch_add(1, std::memory_order_relaxed);
  return SUCCESS;
}

// TODO(zzr) get last segments from VersionManager, this method must be atomic
void TsLastSegmentManager::GetCompactLastSegments(
    std::vector<std::shared_ptr<TsLastSegment>>& result) {
  {
    std::shared_lock lk{s_mutex_};
    size_t compact_num = std::min<size_t>(last_segments_.size(), EngineOptions::max_compact_num);
    result.reserve(compact_num);
    auto it = last_segments_.begin();
    for (int i = 0; i < compact_num; ++i, ++it) {
      result.push_back(it->second);
    }
  }
}

bool TsLastSegmentManager::NeedCompact() {
  return n_lastsegment_.load(std::memory_order_relaxed) > EngineOptions::max_last_segment_num;
}

void TsLastSegmentManager::ClearLastSegments(uint32_t ver) {
  {
    std::unique_lock lk{s_mutex_};
    for (auto it = last_segments_.begin(); it != last_segments_.end();) {
      assert(it->first == it->second->GetVersion());
      if (it->second->GetVersion() <= ver) {
        it->second->MarkDelete();
        it = last_segments_.erase(it);
        n_lastsegment_.fetch_sub(1, std::memory_order_relaxed);
      } else {
        ++it;
      }
    }
  }
}

KStatus TsLastSegment::GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indices) {
  TsLastSegmentFooter footer;
  auto s = GetFooter(&footer);
  if (s != SUCCESS) {
    return FAIL;
  }
  std::vector<TsLastSegmentBlockIndex> tmp_indices;
  tmp_indices.resize(footer.n_data_block);
  TSSlice result;
  auto buf = std::make_unique<char[]>(footer.n_data_block * sizeof(TsLastSegmentBlockIndex));
  file_->Read(footer.block_info_idx_offset, tmp_indices.size() * sizeof(TsLastSegmentBlockIndex),
              &result, buf.get());
  assert(result.len == tmp_indices.size() * sizeof(TsLastSegmentBlockIndex));
  for (int i = 0; i < tmp_indices.size(); ++i) {
    GetFixed64(&result, &tmp_indices[i].offset);
    GetFixed64(&result, &tmp_indices[i].length);
    GetFixed64(&result, &tmp_indices[i].table_id);
    GetFixed32(&result, &tmp_indices[i].table_version);
    GetFixed32(&result, &tmp_indices[i].n_entity);
    uint64_t v;
    GetFixed64(&result, &v);
    tmp_indices[i].min_ts = v;
    GetFixed64(&result, &v);
    tmp_indices[i].max_ts = v;
    GetFixed64(&result, &tmp_indices[i].min_entity_id);
    GetFixed64(&result, &tmp_indices[i].max_entity_id);
  }
  assert(result.len == 0);
  block_indices->swap(tmp_indices);
  return SUCCESS;
}

using DataBlock = std::unique_ptr<std::string>;
using BitmapBlock = std::unique_ptr<TsBitmap>;
struct ColumnBlockV2 {
  bool bitmap_cached = false, data_cached = false;
  BitmapBlock bitmap;
  DataBlock data;
  bool HasBitmap() const { return bitmap != nullptr; }
};

struct ColumnBlockCacheV2 {
  std::unordered_map<int, std::unique_ptr<ColumnBlockV2>> cache_;
  void PutData(int col_id, std::string&& data) {
    if (cache_[col_id] == nullptr) {
      cache_[col_id] = std::make_unique<ColumnBlockV2>();
    }
    assert(!cache_[col_id]->data_cached);
    cache_[col_id]->data = std::make_unique<std::string>(std::move(data));
    cache_[col_id]->data_cached = true;
  }
  void PutBitmap(int col_id, std::unique_ptr<TsBitmap>&& bitmap) {
    if (cache_[col_id] == nullptr) {
      cache_[col_id] = std::make_unique<ColumnBlockV2>();
    }
    assert(!cache_[col_id]->bitmap_cached);
    cache_[col_id]->bitmap = std::move(bitmap);
    cache_[col_id]->bitmap_cached = true;
  }
  ColumnBlockV2* GetColumnBlock(int col_id) { return cache_[col_id].get(); }
  bool HasDataCached(int col_id) {
    auto p = GetColumnBlock(col_id);
    return p != nullptr && p->data_cached;
  }
  bool HasBitmapCached(int col_id) {
    auto p = GetColumnBlock(col_id);
    return p != nullptr && p->bitmap_cached;
  }
};

class TsLastBlock : public TsBlock {
 private:
  std::shared_ptr<TsLastSegment> lastsegment_;

  int block_id_;

  TsLastSegmentBlockIndex block_index_;
  TsLastSegmentBlockInfo block_info_;

  std::unique_ptr<ColumnBlockCacheV2> column_cache_;

 public:
  TsLastBlock(std::shared_ptr<TsLastSegment> lastseg, int block_id,
              TsLastSegmentBlockIndex block_index, TsLastSegmentBlockInfo block_info)
      : lastsegment_(std::move(lastseg)),
        block_id_(block_id),
        block_index_(block_index),
        block_info_(std::move(block_info)),
        column_cache_(std::make_unique<ColumnBlockCacheV2>()) {}
  TSTableID GetTableId() override { return block_index_.table_id; }
  uint32_t GetTableVersion() override { return block_index_.table_version; }
  size_t GetRowNum() override { return block_info_.nrow; }

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                       TsBitmap& bitmap) override {
    int actual_colid = col_id + 2;
    auto s = LoadBitmapToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }

    // should we avoid copy and just return the pointer?
    bitmap = *column_cache_->GetColumnBlock(actual_colid)->bitmap;
    return SUCCESS;
  }
  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                     char** value) override {
    int actual_colid = col_id + 2;
    auto s = LoadAllDataToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }
    if (isVarLenType(schema[col_id].type)) {
      *value = nullptr;
      return FAIL;
    }
    *value = column_cache_->GetColumnBlock(actual_colid)->data->data();
    return SUCCESS;
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                        TSSlice& value) override {
    int actual_colid = col_id + 2;
    auto s = LoadAllDataToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }
    char* ptr = column_cache_->GetColumnBlock(actual_colid)->data->data();
    int dtype = schema[col_id].type;
    int dsize = 0;
    if (isVarLenType(dtype)) {
      s = LoadVarcharToCache();
      if (s == FAIL) {
        return FAIL;
      }
      const size_t* data = reinterpret_cast<const size_t*>(ptr);
      size_t offset = data[row_num];
      TSSlice result{column_cache_->GetColumnBlock(-1)->data->data() + offset, 2};
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
      value.data = ptr + dsize * row_num;
    }
    return SUCCESS;
  }
  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) override {
    int actual_colid = col_id + 2;
    LoadBitmapToCache(actual_colid);
    ColumnBlockV2* p = column_cache_->GetColumnBlock(actual_colid);
    if (!p->HasBitmap()) {
      // this column is not nullable
      return false;
    }
    const TsBitmap& bitmap = *column_cache_->GetColumnBlock(actual_colid)->bitmap;
    return bitmap[row_num] == kNull;
  }
  // if just get timestamp , this function return fast.
  timestamp64 GetTS(int row_num) override {
    assert(block_info_.ncol > 2);
    auto ts = GetTimestamps();
    return ts[row_num];
  }

  int GetBlockID() const { return block_id_; }

 private:
  friend class TsLastSegment;
  KStatus LoadBitmapToCache(int actual_colid) {
    if (!column_cache_->HasBitmapCached(actual_colid)) {
      std::unique_ptr<TsBitmap> bitmap;
      auto s = ReadColumnBitmap(lastsegment_->file_.get(), block_info_, actual_colid, &bitmap);
      if (s == FAIL) {
        return FAIL;
      }
      column_cache_->PutBitmap(actual_colid, std::move(bitmap));
    }
    return SUCCESS;
  }
  KStatus LoadAllDataToCache(int actual_colid) {
    if (!column_cache_->HasDataCached(actual_colid)) {
      std::unique_ptr<TsBitmap> bitmap;
      std::string data;
      auto s =
          ReadColumnBlock(lastsegment_->file_.get(), block_info_, actual_colid, &data, &bitmap);
      if (s == FAIL) {
        return FAIL;
      }
      column_cache_->PutData(actual_colid, std::move(data));
      if (bitmap != nullptr && !column_cache_->HasBitmapCached(actual_colid)) {
        column_cache_->PutBitmap(actual_colid, std::move(bitmap));
      }
    }
    return SUCCESS;
  }
  KStatus LoadVarcharToCache() {
    // we use idx -1 to cache varchar block
    if (!column_cache_->HasDataCached(-1)) {
      std::string data;
      auto s = ReadVarcharBlock(lastsegment_->file_.get(), block_info_, &data);
      if (s == FAIL) {
        return FAIL;
      }
      column_cache_->PutData(-1, std::move(data));
    }
    return SUCCESS;
  }
  const TSEntityID* GetEntities() {
    LoadAllDataToCache(0);
    const std::string& data = *column_cache_->GetColumnBlock(0)->data;
    const TSEntityID* entities = reinterpret_cast<const TSEntityID*>(data.data());
    return entities;
  }

  const timestamp64* GetTimestamps() {
    LoadAllDataToCache(2);
    const std::string& data = *column_cache_->GetColumnBlock(2)->data;
    const timestamp64* ts = reinterpret_cast<const timestamp64*>(data.data());
    return ts;
  }
};

struct Element_ {
  TSEntityID e_id;
  timestamp64 ts;
  bool operator==(const Element_& rhs) const { return e_id == rhs.e_id && ts == rhs.ts; }
  bool operator<(const Element_& rhs) const {
    return e_id < rhs.e_id || (e_id == rhs.e_id && ts < rhs.ts);
  }
};

int FindUpperBound(const Element_& target, const TSEntityID* entities, const timestamp64* tss,
                   int start, int end) {
  int l = start, r = end;
  while (r - l > 0) {
    int m = (l + r) / 2;
    Element_ current{entities[m], tss[m]};
    if (current < target || current == target) {
      l = m + 1;
      continue;
    }
    r = m;
  }
  return r;
}

int FindLowerBound(const Element_& target, const TSEntityID* entities, const timestamp64* tss,
                   int start, int end) {
  int l = start, r = end;
  while (r - l > 0) {
    int m = (l + r) / 2;
    Element_ current{entities[m], tss[m]};
    if (current < target) {
      l = m + 1;
      continue;
    }
    r = m;
  }
  return r;
}

KStatus TsLastSegment::GetBlockSpans(std::vector<TsBlockSpan>* spans) {
  std::vector<TsLastSegmentBlockIndex> block_indices;
  auto s = GetAllBlockIndex(&block_indices);
  if (s == FAIL) {
    return FAIL;
  }

  spans->clear();
  for (int idx = 0; idx < block_indices.size(); ++idx) {
    TsLastSegmentBlockInfo info;
    s = LoadBlockInfo(file_.get(), block_indices[idx], &info);
    if (s == FAIL) {
      return FAIL;
    }
    auto self = shared_from_this();
    auto block = std::make_shared<TsLastBlock>(self, idx, block_indices[idx], info);

    // split current block to several span;
    int prev_end = 0;
    auto entities = block->GetEntities();
    auto ts = block->GetTimestamps();
    while (prev_end < info.nrow) {
      int start = prev_end;
      auto current_eneity = entities[start];
      auto upper_bound =
          FindUpperBound({current_eneity, INT64_MAX}, entities, ts, start, info.nrow);
      spans->emplace_back(block->GetTableId(), block->GetTableVersion(), current_eneity, block,
                          start, upper_bound - start);
      prev_end = upper_bound;
    }
  }
  return SUCCESS;
}

KStatus TsLastSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                     std::vector<TsBlockSpan>* spans) {
  spans->clear();
  if (filter.ts_spans_.empty()) {
    return SUCCESS;
  }
  std::vector<TsLastSegmentBlockIndex> block_indices;
  auto s = GetAllBlockIndex(&block_indices);
  if (s == FAIL) {
    return FAIL;
  }

  TSEntityID entity_id = filter.entity_id;
  const std::vector<KwTsSpan>& ts_spans = filter.ts_spans_;
  int block_idx = 0;
  int span_idx = 0;
  for (; block_idx < block_indices.size(); ++block_idx) {
    const TsLastSegmentBlockIndex& cur_block = block_indices[block_idx];
    // Filter table, entity first
    // TODO(zzr) use binary search here?
    if (cur_block.table_id == filter.table_id && entity_id >= cur_block.min_entity_id &&
        entity_id <= cur_block.max_entity_id) {
      break;
    }
    if (cur_block.table_id > filter.table_id) {
      return SUCCESS;
    }
  }
  if (block_idx == block_indices.size()) {
    return SUCCESS;
  }

  for (; block_idx < block_indices.size(); ++block_idx) {
    const TsLastSegmentBlockIndex& cur_block = block_indices[block_idx];
    if (cur_block.table_id != filter.table_id || entity_id < cur_block.min_entity_id) {
      // no need to read following blocks
      break;
    }
    std::shared_ptr<TsLastBlock> block = nullptr;
    while (span_idx < ts_spans.size()) {
      const KwTsSpan& current_span = ts_spans[span_idx];
      if (current_span.end < cur_block.min_ts) {
        ++span_idx;
        continue;
      }
      if (current_span.begin > cur_block.max_ts) {
        break;
      }
      if (block == nullptr) {
        TsLastSegmentBlockInfo info;
        s = LoadBlockInfo(file_.get(), cur_block, &info);
        if (s == FAIL) {
          return FAIL;
        }
        block = std::make_shared<TsLastBlock>(shared_from_this(), block_idx,
                                              block_indices[block_idx], info);
      }
      auto entities = block->GetEntities();
      auto ts = block->GetTimestamps();
      const TsLastSegmentBlockInfo& info = block->block_info_;
      int start = FindLowerBound({entity_id, current_span.begin}, entities, ts, 0, info.nrow);

      if (start >= info.nrow || entities[start] != entity_id) {
        // The entity cannot be found within this block. At this stage, we already know that
        // cur_block.max_entity_id >= entity_id >= cur_block.min_entity_id. If the entity with this
        // entity_id is not present in the current block, it cannot be present in the subsequent
        // block either. Otherwise, we would reach the conclusion that entity_id >=
        // next_block.min_entity_id >= cur_block.max_entity_id >= entity_id. In other words,
        // next_block.min_entity_id would be equal to cur_block.max_entity_id. Given that the
        // minimum and maximum entity_ids definitely exist within their corresponding blocks, this
        // would conflict with the previous conclusion. Therefore, if the entity cannot be found in
        // the current block, we can simply terminate the search.
        return SUCCESS;
      }
      int end = FindUpperBound({entity_id, current_span.end}, entities, ts, start, info.nrow);
      spans->emplace_back(block->GetTableId(), block->GetTableVersion(), entity_id, block, start,
                          end - start);

      if (end == info.nrow) {
        // We reach the end of current block
        break;
      } else {
        // we reach the end of the span, just move to the next span
        ++span_idx;
      }
    }
  }

  return SUCCESS;
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

timestamp64 TsLastSegmentEntityBlockIteratorBase::EntityBlock::GetTS(int row_num) {
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
  LoadBlockInfo(lastsegment_->file_.get(), cur_block, &current_.info);
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
        !(max_ts < cur_block.min_ts && min_ts > cur_block.max_ts)) {
      break;
    }
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

void TsLastSegmentEntityBlockIteratorBase::CurrentBlock::FindLowerBound(TSEntityID e_id,
                                                                        timestamp64 ts) {
  int l = 0, r = info.nrow;
  Element_ target{e_id, ts};
  while (r - l > 0) {
    int m = (l + r) / 2;
    Element_ current{entities[m], timestamps[m]};
    if (current < target) {
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
    if (current < target || current == target) {
      l = m + 1;
      continue;
    }
    r = m;
  }
  row_end = r;
}

}  // namespace kwdbts
