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

static KStatus ReadVarcharBlock(TsRandomReadFile* file, const TsLastSegmentBlockInfo& info,
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
  char *ptr = result.data;
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

KStatus TsLastSegment::GetFooter(TsLastSegmentFooter* footer) const {
  TSSlice result;
  size_t offset = file_->GetFileSize() - sizeof(TsLastSegmentFooter);
  auto s =
      file_->Read(offset, sizeof(TsLastSegmentFooter), &result, reinterpret_cast<char*>(footer));
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

static inline bool need_convert_ts(int dtype) {
  return (dtype == TIMESTAMP64_LSN_MICRO || dtype == TIMESTAMP64_LSN ||
          dtype == TIMESTAMP64_LSN_NANO);
}

constexpr static int VARCHAR_CACHE_ID = -1;
constexpr static int TIMESTAMP8_CACHE_ID = -2;

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
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                        TSSlice& value) override {
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
    auto seq_nos = GetSeqNos();
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
      auto s =
          ReadColumnBlock(lastsegment_->file_.get(), block_info_, actual_colid, &data, &bitmap);
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

  const uint64_t* GetSeqNos() {
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

KStatus TsLastSegment::GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     TsEngineSchemaManager* schema_mgr) {
  std::vector<TsLastSegmentBlockIndex> block_indices;
  auto s = GetAllBlockIndex(&block_indices);
  if (s == FAIL) {
    return FAIL;
  }

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
    while (prev_end < info.nrow) {
      int start = prev_end;
      auto current_entity = entities[start];
      auto upper_bound =
          FindUpperBound({current_entity, INT64_MAX}, entities, ts, start, info.nrow);
      block_spans.emplace_back(
          make_shared<TsBlockSpan>(current_entity, block, start, upper_bound - start, tbl_schema_mgr, 0));
      prev_end = upper_bound;
    }
  }
  return SUCCESS;
}

KStatus TsLastSegment::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                     std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                     std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                     uint32_t scan_version) {
  if (!MayExistEntity(filter.entity_id)) {
    return SUCCESS;
  }
  // spans->clear();
  if (filter.spans_.empty()) {
    return SUCCESS;
  }
  std::vector<TsLastSegmentBlockIndex> block_indices;
  auto s = GetAllBlockIndex(&block_indices);
  if (s == FAIL) {
    return FAIL;
  }

  TSEntityID entity_id = filter.entity_id;
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
    // todo(liangbo)  store lsn info.
    if (!IsTsLsnSpanCrossSpans(filter.spans_, {cur_block.min_ts, cur_block.max_ts}, {0, UINT64_MAX})) {
      continue;
    }
    // todo(zhangzirui)  code review.
    TsLastSegmentBlockInfo info;
    s = LoadBlockInfo(file_.get(), cur_block, &info);
    if (s == FAIL) {
      return FAIL;
    }
    auto block = std::make_shared<TsLastBlock>(shared_from_this(), block_idx,
                                          block_indices[block_idx], info);

    auto row_num = block->GetRowNum();
    auto ts = block->GetTimestamps();
    auto lsn = block->GetSeqNos();
    auto entities = block->GetEntities();
    int start_idx = 0;
    int end_idx = 0;
    bool match_found = false;
    for (size_t i = 0; i < row_num; i++) {
      if (entities[i] > entity_id) {
        end_idx = i;
        break;
      }
      if (entities[i] != entity_id) {
        continue;
      }
      if (IsTsLsnInSpans(ts[i], lsn[i], filter.spans_)) {
        if (!match_found) {
          start_idx = i;
          match_found = true;
        }
      } else {
        if (match_found) {
          match_found = false;
          block_spans.emplace_back(make_shared<TsBlockSpan>(filter.vgroup_id, entity_id, block, start_idx, i - start_idx,
                                                            tbl_schema_mgr, scan_version));
        }
      }
    }
    if (match_found) {
      match_found = false;
      block_spans.emplace_back(make_shared<TsBlockSpan>(filter.vgroup_id, entity_id, block, start_idx,
                                end_idx == 0 ? row_num - start_idx : end_idx - start_idx,
                                tbl_schema_mgr, scan_version));
    }
  }

  return SUCCESS;
}

}  // namespace kwdbts
