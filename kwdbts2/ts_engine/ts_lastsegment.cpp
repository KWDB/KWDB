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
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "mmap/mmap_entity_block_meta.h"
#include "ts_bitmap.h"
#include "ts_block.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compressor.h"
#include "ts_compressor_impl.h"
#include "ts_io.h"
#include "ts_segment.h"
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

static KStatus LoadBlockInfo(TsRandomReadFile* file, const TsLastSegmentBlockIndex& index,
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

static KStatus ReadColumnBitmap(TsRandomReadFile* file, const TsLastSegmentBlockInfo& info, int col_id,
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
  char* ptr = result.data;
  if (has_bitmap) {
    BitmapCompAlg alg = static_cast<BitmapCompAlg>(ptr[0]);
    switch (alg) {
      case BitmapCompAlg::kPlain: {
        size_t len = info.col_infos[col_id].bitmap_len - 1;
        *bitmap = std::make_unique<TsBitmap>(TSSlice{ptr + 1, len}, info.nrow);
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

static KStatus ReadColumnBlock(TsRandomReadFile* file, const TsLastSegmentBlockInfo& info, int col_id,
                               std::string* col_data, std::unique_ptr<TsBitmap>* bitmap) {
  bitmap->reset();
  auto s = ReadColumnBitmap(file, info, col_id, bitmap);
  if (s == FAIL) {
    LOG_ERROR("cannot read column bitmap");
    return s;
  }
  size_t offset = info.block_offset + info.col_infos[col_id].offset;
  offset += info.col_infos[col_id].bitmap_len;
  size_t len = info.col_infos[col_id].data_len;
  TSSlice result;
  auto buf = std::make_unique<char[]>(len);
  s = file->Read(offset, len, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }

  // Metric
  const auto& mgr = CompressorManager::GetInstance();
  bool ok = mgr.DecompressData(result, bitmap->get(), info.nrow, col_data);
  if (!ok) {
    LOG_ERROR("cannot decompress data");
  }
  return ok ? SUCCESS : FAIL;
}

static KStatus ReadVarcharBlock(TsRandomReadFile* file, const TsLastSegmentBlockInfo& info, std::string* out) {
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
  char* ptr = result.data;
  GenCompAlg type = static_cast<GenCompAlg>(ptr[0]);
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

KStatus TsLastSegment::TsLastSegBlockCache::BlockIndexCache::GetBlockIndices(
    std::vector<TsLastSegmentBlockIndex>** block_indices) {
  {
    std::shared_lock lk{mu_};
    if (cached_) {
      *block_indices = &block_indices_;
      return SUCCESS;
    }
  }
  std::unique_lock lk{mu_};
  if (cached_) {
    *block_indices = &block_indices_;
    return SUCCESS;
  }
  auto s = lastseg_->GetAllBlockIndex(&block_indices_);
  if (s == FAIL) {
    LOG_ERROR("cannot get block index from last segment");
  }
  cached_ = true;
  *block_indices = &block_indices_;
  return SUCCESS;
}

KStatus TsLastSegment::TsLastSegBlockCache::BlockInfoCache::GetBlockInfo(int block_id, TsLastSegmentBlockInfo** info) {
  {
    std::shared_lock lk{mu_};
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
  if (s == FAIL) {
    return s;
  }
  // important, Read function may not fill the buffer;
  *footer = *reinterpret_cast<TsLastSegmentFooter*>(result.data);
  if (result.len != sizeof(TsLastSegmentFooter) || footer->magic_number != FOOTER_MAGIC) {
    LOG_ERROR("last segment[%s] GetFooter failed.", file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
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
  s = file_->Read(footer.block_info_idx_offset, tmp_indices.size() * sizeof(TsLastSegmentBlockIndex), &result,
                  buf.get());
  if (s == FAIL) {
    LOG_ERROR("cannot read data from file");
    return s;
  }
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
    GetFixed64(&result, &tmp_indices[i].min_lsn);
    GetFixed64(&result, &tmp_indices[i].max_lsn);
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


constexpr static int VARCHAR_CACHE_ID = -1;
constexpr static int TIMESTAMP8_CACHE_ID = -2;

class TsLastBlock;
class ColumnBlockCache {
 private:
  std::shared_mutex mu_;
  TsRandomReadFile* underlying_file_;
  TsLastSegmentBlockInfo* block_info_;

  std::vector<ColumnBlockV2> column_block_cache_;

 public:
  ColumnBlockCache(TsRandomReadFile* underlying_file, TsLastSegmentBlockInfo* block_info);
  KStatus GetColBitmap(int actual_colid, const std::vector<AttributeInfo>& schema, TsBitmap** bitmap);
  KStatus GetColAddr(int actual_colid, const std::vector<AttributeInfo>& schema, char** value);
  KStatus GetValueSlice(int row_num, int actural_colid, const std::vector<AttributeInfo>& schema, TSSlice& value);

 private:
  KStatus LoadColumnDataToCache(int actual_colid, const std::vector<AttributeInfo>& schema);
  KStatus LoadVarcharDataToCache();
  KStatus FormatValueSlice(int row_num, int actural_colid, const std::vector<AttributeInfo>& schema, TSSlice& value);
};

static inline bool need_convert_ts(int dtype) {
  return (dtype == TIMESTAMP64_LSN_MICRO || dtype == TIMESTAMP64_LSN || dtype == TIMESTAMP64_LSN_NANO);
}


class TsLastBlock : public TsBlock {
 private:
  TsLastSegment* lastsegment_;

  int block_id_;

  TsLastSegmentBlockIndex block_index_;
  TsLastSegmentBlockInfo block_info_;

  std::unique_ptr<ColumnBlockCacheV2> column_cache_;
  std::unique_ptr<ColumnBlockCache> column_block_cache_;

 public:
  TsLastBlock(TsLastSegment* lastseg, int block_id, TsLastSegmentBlockIndex block_index,
              TsLastSegmentBlockInfo block_info)
      : lastsegment_(lastseg),
        block_id_(block_id),
        block_index_(block_index),
        block_info_(std::move(block_info)),
        column_cache_(std::make_unique<ColumnBlockCacheV2>()) {}
  ~TsLastBlock() = default;
  TSTableID GetTableId() override { return block_index_.table_id; }
  uint32_t GetTableVersion() override { return block_index_.table_version; }
  size_t GetRowNum() override { return block_info_.nrow; }

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap) override {
    int actual_colid = col_id + 2;
    auto s = LoadBitmapToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }

    // TODO(zzr): optimize, avoid copy and just return the pointer
    bitmap = *column_cache_->GetColumnBlock(actual_colid)->bitmap;
    return SUCCESS;
  }
  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value) override {
    int dtype = schema[col_id].type;
    if (isVarLenType(dtype)) {
      *value = nullptr;
      return FAIL;
    }

    int actual_colid = col_id + 2;
    auto s = LoadAllDataToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }
    if (need_convert_ts(dtype)) {
      ConvertTS8to16(actual_colid);
      actual_colid = TIMESTAMP8_CACHE_ID;
    }
    *value = column_cache_->GetColumnBlock(actual_colid)->data->data();
    return SUCCESS;
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) override {
    int dtype = schema[col_id].type;
    int actual_colid = col_id + 2;
    auto s = LoadAllDataToCache(actual_colid);
    if (s == FAIL) {
      return FAIL;
    }

    if (need_convert_ts(dtype)) {
      ConvertTS8to16(actual_colid);
      actual_colid = TIMESTAMP8_CACHE_ID;
    }
    char* ptr = column_cache_->GetColumnBlock(actual_colid)->data->data();

    if (isVarLenType(dtype)) {
      s = LoadVarcharToCache();
      if (s == FAIL) {
        return FAIL;
      }
      const uint32_t* data = reinterpret_cast<const uint32_t*>(ptr);
      size_t offset = data[row_num];
      TSSlice result{column_cache_->GetColumnBlock(VARCHAR_CACHE_ID)->data->data() + offset, 2};
      uint16_t len;
      GetFixed16(&result, &len);
      value.data = result.data;
      value.len = len;
      return SUCCESS;
    }
    int dsize = schema[col_id].size;
    if (need_convert_ts(dtype)) {
      dsize = 16;
    }
    value.len = dsize;
    value.data = ptr + dsize * row_num;

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
    if (ts == nullptr) {
      return INVALID_TS;
    }
    return ts[row_num];
  }

  uint64_t* GetLSNAddr(int row_num) override {
    assert(block_info_.ncol > 2);
    auto seq_nos = GetLSN();
    if (seq_nos == nullptr) {
      LOG_ERROR("cannot get lsn addr");
      return nullptr;
    }
    return const_cast<uint64_t*>(&seq_nos[row_num]);
  }

  KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, std::string& data) override {
    return KStatus::FAIL;
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

  void ConvertTS8to16(int ts_col_id) {
    if (column_cache_->HasDataCached(TIMESTAMP8_CACHE_ID)) {
      return;
    }
    assert(column_cache_->HasDataCached(ts_col_id));
    const std::string& cached = *column_cache_->GetColumnBlock(ts_col_id)->data;
    auto sz = cached.size();
    assert(sz % 8 == 0);
    auto count = sz / 8;
    std::string tmp;
    tmp.resize(count * 16);
    struct TsWithLSN {
      timestamp64 ts;
      TS_LSN lsn;
    };
    TsWithLSN* ptr = reinterpret_cast<TsWithLSN*>(tmp.data());
    const timestamp64* p_ts = reinterpret_cast<const timestamp64*>(cached.data());
    for (int i = 0; i < count; ++i) {
      ptr[i].ts = p_ts[i];
    }
    column_cache_->PutData(TIMESTAMP8_CACHE_ID, std::move(tmp));
  }

  KStatus LoadAllDataToCache(int actual_colid) {
    if (!column_cache_->HasDataCached(actual_colid)) {
      std::unique_ptr<TsBitmap> bitmap;
      std::string data;
      auto s = ReadColumnBlock(lastsegment_->file_.get(), block_info_, actual_colid, &data, &bitmap);
      if (s == FAIL) {
        LOG_ERROR("cannot read column block %d", actual_colid);
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
    if (!column_cache_->HasDataCached(VARCHAR_CACHE_ID)) {
      std::string data;
      auto s = ReadVarcharBlock(lastsegment_->file_.get(), block_info_, &data);
      if (s == FAIL) {
        return FAIL;
      }
      column_cache_->PutData(VARCHAR_CACHE_ID, std::move(data));
    }
    return SUCCESS;
  }
  const TSEntityID* GetEntities() {
    auto s = LoadAllDataToCache(0);
    if (s == FAIL) {
      LOG_ERROR("cannot load entitiy column");
      return nullptr;
    }
    const std::string& data = *column_cache_->GetColumnBlock(0)->data;
    const TSEntityID* entities = reinterpret_cast<const TSEntityID*>(data.data());
    return entities;
  }

  const uint64_t* GetLSN() {
    auto s = LoadAllDataToCache(1);
    if (s == FAIL) {
      LOG_ERROR("cannot load lsn column");
      return nullptr;
    }
    const std::string& data = *column_cache_->GetColumnBlock(1)->data;
    const uint64_t* seq_nos = reinterpret_cast<const uint64_t*>(data.data());
    return seq_nos;
  }

  const timestamp64* GetTimestamps() {
    auto s = LoadAllDataToCache(2);
    if (s == FAIL) {
      LOG_ERROR("cannot load timestamp column");
      return nullptr;
    }
    const std::string& data = *column_cache_->GetColumnBlock(2)->data;
    const timestamp64* ts = reinterpret_cast<const timestamp64*>(data.data());
    return ts;
  }
};

ColumnBlockCache::ColumnBlockCache(TsRandomReadFile* underlying_file, TsLastSegmentBlockInfo* block_info)
    : underlying_file_(underlying_file),
      block_info_(block_info),
      column_block_cache_(block_info->ncol + 1 /* the extra 1 is reserved for varchar cache*/) {}

// entity_id, lsn
constexpr int kColIDShift = 2;

KStatus ColumnBlockCache::LoadColumnDataToCache(int actual_colid, const std::vector<AttributeInfo>& schema) {
  assert(column_block_cache_[actual_colid].data_cached == false);

  TsBitmap* bitmap = nullptr;
  if (!column_block_cache_[actual_colid].bitmap_cached) {
    auto s = ReadColumnBitmap(underlying_file_, *block_info_, actual_colid, &column_block_cache_[actual_colid].bitmap);
    if (s == FAIL) {
      return FAIL;
    }
    column_block_cache_[actual_colid].bitmap_cached = true;
  }
  bitmap = column_block_cache_[actual_colid].bitmap.get();

  size_t offset = block_info_->block_offset + block_info_->col_infos[actual_colid].offset;
  offset += block_info_->col_infos[actual_colid].bitmap_len;
  size_t len = block_info_->col_infos[actual_colid].data_len;
  TSSlice result;
  auto buf = std::make_unique<char[]>(len);
  auto s = underlying_file_->Read(offset, len, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }

  // Metric

  column_block_cache_[actual_colid].data = std::make_unique<std::string>();
  const auto& mgr = CompressorManager::GetInstance();
  bool ok = mgr.DecompressData(result, bitmap, block_info_->nrow, column_block_cache_[actual_colid].data.get());
  if (!ok) {
    LOG_ERROR("cannot decompress data");
    return FAIL;
  }
  column_block_cache_[actual_colid].data_cached = true;

  int col_id = actual_colid - kColIDShift;
  if (col_id == 0) {  // timestamp
    // convert timestamp from size 8 to 16

    const std::string& data = *column_block_cache_[actual_colid].data;

    std::string tmp;
    tmp.resize(block_info_->nrow * 16);
    struct TsWithLSN {
      timestamp64 ts;
      TS_LSN lsn;
    };
    auto dstptr = reinterpret_cast<TsWithLSN*>(tmp.data());
    auto srcptr = reinterpret_cast<const timestamp64*>(data.data());
    for (int i = 0; i < block_info_->nrow; ++i) {
      dstptr[i].ts = srcptr[i];
    }

    column_block_cache_[actual_colid].data->swap(tmp);
  }

  return SUCCESS;
}

KStatus ColumnBlockCache::LoadVarcharDataToCache() {
  bool has_varchar = block_info_->var_offset != 0;
  if (!has_varchar) {
    LOG_ERROR("no varcha block to read");
    return FAIL;
  }
  size_t offset = block_info_->block_offset + block_info_->var_offset;
  size_t len = block_info_->var_len;
  auto buf = std::make_unique<char[]>(len);
  TSSlice result;

  assert(len > 0);
  underlying_file_->Read(offset, len, &result, buf.get());
  char* ptr = result.data;
  GenCompAlg type = static_cast<GenCompAlg>(ptr[0]);
  RemovePrefix(&result, 1);
  int ok = true;
  column_block_cache_.back().data = std::make_unique<std::string>();
  switch (type) {
    case GenCompAlg::kPlain: {
      column_block_cache_.back().data->assign(result.data, result.len);
      break;
    }
    case GenCompAlg::kSnappy: {
      const auto& snappy = SnappyString::GetInstance();
      ok = snappy.Decompress(result, 0, column_block_cache_.back().data.get());
      break;
    }
    default:
      assert(false);
  }
  if (!ok) {
    LOG_ERROR("cannot decompress varchar data");
    return FAIL;
  }

  column_block_cache_.back().data_cached = true;
  return SUCCESS;
}

KStatus ColumnBlockCache::GetColBitmap(int actual_colid, const std::vector<AttributeInfo>& schema, TsBitmap** bitmap) {
  {
    std::shared_lock lk{mu_};
    if (column_block_cache_[actual_colid].bitmap_cached) {
      *bitmap = column_block_cache_[actual_colid].bitmap.get();
      return SUCCESS;
    }
  }
  {
    std::unique_lock lk{mu_};
    if (column_block_cache_[actual_colid].bitmap_cached) {
      *bitmap = column_block_cache_[actual_colid].bitmap.get();
      return SUCCESS;
    }
    auto s = ReadColumnBitmap(underlying_file_, *block_info_, actual_colid, &column_block_cache_[actual_colid].bitmap);
    if (s == FAIL) {
      return FAIL;
    }
    column_block_cache_[actual_colid].bitmap_cached = true;
  }
  *bitmap = column_block_cache_[actual_colid].bitmap.get();
  return SUCCESS;
}

KStatus ColumnBlockCache::GetColAddr(int actual_colid, const std::vector<AttributeInfo>& schema, char** value) {
  {
    std::shared_lock lk{mu_};
    if (column_block_cache_[actual_colid].data_cached) {
      *value = column_block_cache_[actual_colid].data->data();
      return SUCCESS;
    }
  }
  {
    std::unique_lock lk{mu_};
    if (column_block_cache_[actual_colid].data_cached) {
      *value = column_block_cache_[actual_colid].data->data();
      return SUCCESS;
    }
    auto s = LoadColumnDataToCache(actual_colid, schema);
    if (s == FAIL) {
      return FAIL;
    }
  }
  *value = this->column_block_cache_[actual_colid].data->data();
  return SUCCESS;
}

KStatus ColumnBlockCache::FormatValueSlice(int row_num, int actual_colid, const std::vector<AttributeInfo>& schema,
                                           TSSlice& value) {
  int col_id = actual_colid - kColIDShift;
  assert(col_id >= 0);
  if (isVarLenType(schema[col_id].type)) {
    const uint32_t* data = reinterpret_cast<const uint32_t*>(column_block_cache_[actual_colid].data->data());
    size_t offset = data[row_num];
    TSSlice result{column_block_cache_.back().data->data() + offset, 2};
    uint16_t len;
    GetFixed16(&result, &len);
    value.data = result.data;
    value.len = len;
    return SUCCESS;
  }
  char* ptr = column_block_cache_[actual_colid].data->data();
  auto dsize = need_convert_ts(schema[col_id].type) ? 16 : schema[col_id].size;
  value.len = dsize;
  value.data = ptr + dsize * row_num;
  return SUCCESS;
}

KStatus ColumnBlockCache::GetValueSlice(int row_num, int actual_colid, const std::vector<AttributeInfo>& schema,
                                        TSSlice& value) {
  int col_id = actual_colid - kColIDShift;
  assert(col_id >= 0);
  {
    std::shared_lock lk{mu_};
    if (column_block_cache_[actual_colid].data_cached) {
      return FormatValueSlice(row_num, actual_colid, schema, value);
    }
  }
  {
    std::unique_lock lk{mu_};
    if (column_block_cache_[actual_colid].data_cached) {
      return FormatValueSlice(row_num, actual_colid, schema, value);
    }

    auto s = LoadColumnDataToCache(actual_colid, schema);
    if (s == FAIL) {
      return FAIL;
    }
    if (isVarLenType(schema[col_id].type)) {
      s = LoadVarcharDataToCache();
      if (s == FAIL) {
        return FAIL;
      }
    }
  }
  return FormatValueSlice(row_num, actual_colid, schema, value);
}

KStatus TsLastSegment::TsLastSegBlockCache::BlockCache::GetBlock(int block_id, std::shared_ptr<TsBlock>* block) {
  {
    std::shared_lock lk{mu_};
    if (cache_flag_[block_id] == 1) {
      *block = block_infos_[block_id];
      return SUCCESS;
    }
  }
  std::unique_lock lk{mu_};
  if (cache_flag_[block_id] == 1) {
    *block = block_infos_[block_id];
    return SUCCESS;
  }
  // std::shared_ptr<TsLastSegment> lastseg, int block_id,
  //           TsLastSegmentBlockIndex block_index, TsLastSegmentBlockInfo block_info
  TsLastSegmentBlockIndex* index;
  auto s = lastseg_cache_->GetBlockIndex(block_id, &index);
  if (s == FAIL) {
    LOG_ERROR("cannot get block index");
    return s;
  }

  TsLastSegmentBlockInfo* info;
  s = lastseg_cache_->GetBlockInfo(block_id, &info);
  if (s == FAIL) {
    LOG_ERROR("cannot get block info");
    return s;
  }

  auto tmp_block = std::make_unique<TsLastBlock>(lastseg_cache_->segment_, block_id, *index, *info);
  cache_flag_[block_id] = 1;
  block_infos_[block_id] = std::move(tmp_block);
  *block = block_infos_[block_id];
  return SUCCESS;
}

TsLastSegment::TsLastSegBlockCache::TsLastSegBlockCache(TsLastSegment* last, int nblock)
    : segment_(last),
      block_index_cache_(std::make_unique<BlockIndexCache>(last)),
      block_info_cache_(std::make_unique<BlockInfoCache>(this, nblock)),
      block_cache_(std::make_unique<BlockCache>(this, nblock)) {}

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

KStatus TsLastSegment::TsLastSegBlockCache::GetBlockInfo(int block_id, TsLastSegmentBlockInfo** info) const {
  return block_info_cache_->GetBlockInfo(block_id, info);
}

KStatus TsLastSegment::TsLastSegBlockCache::GetBlock(int block_id, std::shared_ptr<TsBlock>* block) const {
  return block_cache_->GetBlock(block_id, block);
}

struct Element_ {
  TSEntityID e_id;
  timestamp64 ts;
  bool operator==(const Element_& rhs) const { return e_id == rhs.e_id && ts == rhs.ts; }
  bool operator<(const Element_& rhs) const { return e_id < rhs.e_id || (e_id == rhs.e_id && ts < rhs.ts); }
};

int FindUpperBound(const Element_& target, const TSEntityID* entities, const timestamp64* tss, int start, int end) {
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

int FindLowerBound(const Element_& target, const TSEntityID* entities, const timestamp64* tss, int start, int end) {
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
    LOG_ERROR("magic mismatch");
    return FAIL;
  }

  // load necessary meta block to memory.
  // NOTICE: maybe we will support lazy loading later. For now, just load all meta blocks in
  // Open()
  int nmeta = footer_.n_meta_block;
  if (nmeta != 0) {
    Arena arena;
    TSSlice result;
    char* buf = arena.Allocate(nmeta * 16);
    s = file_->Read(footer_.meta_block_idx_offset, nmeta * 16, &result, buf);
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
      char* buf2 = arena.Allocate(meta_len[i]);
      s = file_->Read(meta_offset[i], meta_len[i], &result, buf2);
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

  std::vector<TsLastSegmentBlockIndex>* p_block_indices;
  auto s = block_cache_->GetAllBlockIndex(&p_block_indices);
  if (s == FAIL) {
    LOG_ERROR("cannot get block indices");
    return s;
  }
  const auto& block_indices = *p_block_indices;

  for (int idx = 0; idx < footer_.n_data_block; ++idx) {
    TsLastSegmentBlockInfo* info;
    s = block_cache_->GetBlockInfo(idx, &info);
    if (s == FAIL) {
      LOG_ERROR("cannot get block info");
      return s;
    }

    std::shared_ptr<TsBlock> tmp_block;
    block_cache_->GetBlock(idx, &tmp_block);
    auto block = std::static_pointer_cast<TsLastBlock>(tmp_block);

    // auto block = std::make_shared<TsLastBlock>(shared_from_this(), idx, block_indices[idx], *info);

    // split current block to several span;
    int prev_end = 0;
    auto entities = block->GetEntities();
    if (entities == nullptr) {
      LOG_ERROR("cannot load entity column");
    }
    auto ts = block->GetTimestamps();
    if (ts == nullptr) {
      LOG_ERROR("cannot load timestamp column");
    }
    std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr = {nullptr};
    s = schema_mgr->GetTableSchemaMgr(block->GetTableId(), tbl_schema_mgr);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("get table schema manager failed. table id: %lu", block->GetTableId());
      return s;
    }
    while (prev_end < block->GetRowNum()) {
      int start = prev_end;
      auto current_entity = entities[start];
      auto upper_bound = FindUpperBound({current_entity, INT64_MAX}, entities, ts, start, block->GetRowNum());
      block_spans.emplace_back(
          make_shared<TsBlockSpan>(current_entity, block, start, upper_bound - start, tbl_schema_mgr, 0));
      prev_end = upper_bound;
    }
  }
  return SUCCESS;
}

struct ValuePoint {
  TSTableID table_id;
  TSEntityID entity_id;
  timestamp64 ts;
  TS_LSN lsn;
};

static inline bool Compare(const ValuePoint& lhs, const ValuePoint& rhs) {
  using Helper = std::tuple<TSTableID, TSEntityID, timestamp64, TS_LSN>;
  return Helper(lhs.table_id, lhs.entity_id, lhs.ts, lhs.lsn) <= Helper(rhs.table_id, rhs.entity_id, rhs.ts, rhs.lsn);
}

KStatus TsLastSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                     std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version) {
  assert(block_cache_ != nullptr);

  // if filter is empty, no need to do anything.
  if (filter.spans_.empty()) {
    return SUCCESS;
  }

  // check bloom filter first
  if (!MayExistEntity(filter.entity_id)) {
    return SUCCESS;
  }
  
  std::vector<TsLastSegmentBlockIndex>* p_block_indices;
  auto s = block_cache_->GetAllBlockIndex(&p_block_indices);
  if (s == FAIL) {
    return FAIL;
  }
  const std::vector<TsLastSegmentBlockIndex>& block_indices = *p_block_indices;
  assert(block_indices.size() == footer_.n_data_block);

  auto begin_it = block_indices.begin();

  std::vector<int> iota_vector;

  std::shared_ptr<TsLastBlock> block = nullptr;

  for (const auto& span : filter.spans_) {
    if (begin_it == block_indices.end()) {
      break;
    }

    ValuePoint filter_span_start{filter.table_id, filter.entity_id, span.ts_span.begin, span.lsn_span.begin};
    ValuePoint filter_span_end{filter.table_id, filter.entity_id, span.ts_span.end, span.lsn_span.end};

    auto it = std::upper_bound(
        begin_it, block_indices.end(), filter_span_start,
        [](const ValuePoint& val, const TsLastSegmentBlockIndex& element) {
          return Compare(val, {element.table_id, element.max_entity_id, element.max_ts, element.max_lsn});
        });

    bool use_binary_search_for_start_row = true;
    for (; it != block_indices.end(); ++it) {
      ValuePoint block_span_start{it->table_id, it->min_entity_id, it->min_ts, it->min_lsn};
      if (!Compare(block_span_start, filter_span_end)) {
        break;
      }

      //  we need to read the block to do futher filtering.
      int block_idx = it - block_indices.begin();
      TsLastSegmentBlockInfo* info;
      s = block_cache_->GetBlockInfo(block_idx, &info);
      if (s == FAIL) {
        LOG_ERROR("cannot get block info");
        return s;
      }
      if (block == nullptr || block->GetBlockID() != block_idx) {
        std::shared_ptr<TsBlock> tmp_block;
        auto s = block_cache_->GetBlock(block_idx, &tmp_block);
        if (s == FAIL) {
          return s;
        }
        block = std::static_pointer_cast<TsLastBlock>(tmp_block);
        // block = std::make_shared<TsLastBlock>(shared_from_this(), block_idx, *it, *info);
      }
      auto ts = block->GetTimestamps();
      auto entities = block->GetEntities();
      auto lsn = block->GetLSN();
      if (ts == nullptr || entities == nullptr || lsn == nullptr) {
        return FAIL;
      }
      iota_vector.resize(block->GetRowNum());
      std::iota(iota_vector.begin(), iota_vector.end(), 0);

      int start_idx = 0;
      if (use_binary_search_for_start_row) {
        // find the first row int the block that matches the filter.
        auto idx_it = std::upper_bound(iota_vector.begin(), iota_vector.end(), filter_span_start,
                                       [&](const ValuePoint& val, int idx) {
                                         ValuePoint data_point{block->GetTableId(), entities[idx], ts[idx], lsn[idx]};
                                         return Compare(val, data_point);
                                       });
        if (idx_it == iota_vector.end()) {
          // cannot found in this block, move to the next.
          continue;
        }
        start_idx = *idx_it;
      } else {
        ValuePoint data_point{block->GetTableId(), entities[0], ts[0], lsn[0]};
        if (!Compare(data_point, filter_span_end)) {
          // which means data_point > filter_span_end, the filter is end.
          break;
        }
      }

      // find the last row int the block that matches the filter.
      auto idx_it = std::lower_bound(iota_vector.begin(), iota_vector.end(), filter_span_end,
                                     [&](int idx, const ValuePoint& val) {
                                       ValuePoint data_point{block->GetTableId(), entities[idx], ts[idx], lsn[idx]};
                                       return Compare(data_point, val);
                                     });

      // no need to check wheather idx_it == end(), the caculation are consistent no matter idx_it is valid or not.
      int end_idx = idx_it - iota_vector.begin();

      if (end_idx - start_idx > 0) {
        block_spans.emplace_back(std::make_shared<TsBlockSpan>(filter.vgroup_id, filter.entity_id, block, start_idx,
                                                               end_idx - start_idx, tbl_schema_mgr, scan_version));
      }

      if (idx_it != iota_vector.end()) {
        // filter spans end before this block, move to the next filter
        break;
      } else {
        // we reach the end of the block, move to the next. And no need to use binary search for start row in the next
        // block. just check the first row;
        use_binary_search_for_start_row = false;
      }
    }

    begin_it = it;
  }
  return SUCCESS;
}

}  // namespace kwdbts
