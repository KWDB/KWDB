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

#pragma once
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_arena.h"
#include "ts_bitmap.h"
#include "ts_io.h"
#include "ts_segment.h"

namespace kwdbts {

class MetaBlockBase {
 public:
  virtual char* GetName() const = 0;
  virtual void Serialize(std::string* dst) = 0;
};

// first 8 byte of `md5 -s kwdbts::TsLastSegment`
// TODO(zzr) fix endian
static constexpr uint64_t FOOTER_MAGIC = 0xcb2ffe9321847271;

struct TsLastSegmentFooter {
  uint64_t block_info_idx_offset, n_data_block;
  uint64_t meta_block_idx_offset, n_meta_block;
  uint8_t padding[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t file_version;
  const uint64_t magic_number = FOOTER_MAGIC;
};
static_assert(sizeof(TsLastSegmentFooter) == 64);

struct TsLastSegmentBlockIndex {
  uint64_t offset, length;
  uint64_t table_id;
  uint32_t table_version, n_entity;
  int64_t min_ts, max_ts;
  uint64_t min_entity_id, max_entity_id;
};

struct TsLastSegmentBlockInfo {
  uint64_t block_offset;
  uint32_t nrow;
  uint32_t ncol;
  uint32_t var_offset;
  uint32_t var_len;
  struct ColInfo {
    uint32_t offset;
    uint16_t bitmap_len;
    uint32_t data_len;
  };
  std::vector<ColInfo> col_infos;
};
const size_t LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE = sizeof(uint64_t) + 4 * sizeof(uint32_t);
const size_t LAST_SEGMENT_BLOCK_INFO_COL_INFO_SIZE =
    sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint32_t);

struct TsLastSegmentColumnBlock {
  TsBitmap bitmap;
  std::string buffer;
};

struct TsLastSegmentBlock {
  std::vector<TsLastSegmentColumnBlock> column_blocks;  // entity id, seq number and metric columns
  std::string var_buffer;

  DataFlags GetBitmap(uint32_t col_idx, uint32_t row_idx) {
    return column_blocks[col_idx].bitmap[row_idx];
  }

  uint64_t GetEntityId(uint32_t row_idx) {
    return *reinterpret_cast<uint64_t*>(&column_blocks[0].buffer[row_idx * sizeof(uint64_t)]);
  }

  uint32_t GetSeqNo(uint32_t row_idx) {
    return *reinterpret_cast<uint32_t*>(&column_blocks[1].buffer[row_idx * sizeof(uint32_t)]);
  }

  timestamp64 GetTimestamp(uint32_t row_idx) {
    return *reinterpret_cast<timestamp64*>(&column_blocks[2].buffer[row_idx * sizeof(timestamp64)]);
  }

  TSSlice GetData(uint32_t col_idx, uint32_t row_idx, DATATYPE type, size_t d_size) {
    TSSlice value;
    if (type != DATATYPE::VARSTRING && type != DATATYPE::VARBINARY) {
      value.data = &column_blocks[col_idx].buffer[row_idx * d_size];
      value.len = d_size;
    } else {
      size_t offset = *reinterpret_cast<size_t*>(&column_blocks[col_idx].buffer[row_idx * d_size]);
      value.len = *reinterpret_cast<uint16_t*>(&var_buffer[offset]);
      value.data = &var_buffer[offset + sizeof(uint16_t)];
    }
    return value;
  }

  TSSlice GetNotVarBatchData(uint32_t col_idx, uint32_t row_idx, uint32_t row_count,
                             size_t d_size) {
    TSSlice value;
    value.data = &column_blocks[col_idx].buffer[row_idx * d_size];
    value.len = d_size * row_count;
    return value;
  }
};

class TsLastSegmentBlockIterator;
class TsLastSegmentEntityBlockIteratorBase;
class TsLastSegment : public TsSegmentBase, public std::enable_shared_from_this<TsLastSegment> {
 public:
  static int kNRowPerBlock;
  friend class TsLastSegmentBlockIterator;
  friend class TsLastSegmentEntityBlockIteratorBase;
  friend class TsLastBlock;

 private:
  uint32_t file_number_;
  std::unique_ptr<TsFile> file_;

  explicit TsLastSegment(uint32_t file_number, const std::string& path)
      : file_number_(file_number), file_(std::make_unique<TsMMapFile>(path, true)) {}

 public:
  template <class... Args>
  static std::shared_ptr<TsLastSegment> Create(Args&&... args) {
    return std::shared_ptr<TsLastSegment>(new TsLastSegment(std::forward<Args>(args)...));
  }

  ~TsLastSegment() = default;

  KStatus Open() {
    // just check the magic number;
    auto sz = file_->GetFileSize();
    if (sz < sizeof(TsLastSegmentFooter)) {
      LOG_ERROR("lastsegment file corrupted");
      return FAIL;
    }
    uint64_t magic;
    TSSlice result;
    file_->Read(sz - 8, sz, &result, reinterpret_cast<char*>(&magic));
    if (magic != FOOTER_MAGIC) {
      LOG_ERROR("magic mismatch, expect: %lx, found: %lx", FOOTER_MAGIC, magic);
      return FAIL;
    }
    return SUCCESS;
  }

  uint32_t GetVersion() const { return file_number_; }

  void MarkDelete() { file_->MarkDelete(); }

  KStatus GetFooter(TsLastSegmentFooter* footer) const;

  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes);

  KStatus GetBlockInfo(TsLastSegmentBlockIndex& block_index, TsLastSegmentBlockInfo* block_info);

  KStatus GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block);

  KStatus GetBlockSpans(std::vector<TsBlockSpan>* spans);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter,
                        std::vector<TsBlockSpan>* spans) override;

  std::unique_ptr<TsLastSegmentEntityBlockIteratorBase> NewIterator() const;

  std::unique_ptr<TsLastSegmentEntityBlockIteratorBase> NewIterator(
      TSTableID table_id, TSEntityID entity_id, const std::vector<KwTsSpan>& spans) const;

 private:
  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>*) const;
};

class TsLastSegmentEntityBlockIteratorBase {
  friend class TsLastSegment;

 protected:
  bool valid_ = true;
  const TsLastSegment* lastsegment_;

  struct CurrentBlock {
   private:
    int row_start, row_end;
    TSEntityID entity_id_;

   public:
    int block_id = -1;
    TsLastSegmentBlockInfo info;
    TSEntityID* entities;
    timestamp64* timestamps;
    void SetRowStart(int start) {
      assert(start < info.nrow);
      row_start = start;
      entity_id_ = entities[start];
    }
    void SetRowEnd(int end) { row_end = end; }
    int GetRowStart() const { return row_start; }
    int GetRowEnd() const { return row_end; }
    int GetRowCount() const { return row_end - row_start; }
    bool Finished() const { return row_end == info.nrow; }
    TSEntityID GetEntityID() const { return entity_id_; }
    timestamp64 GetStartTs() const { return timestamps[row_start]; }
    void FindLowerBound(TSEntityID e_id, timestamp64 ts);
    void FindUpperBound(TSEntityID e_id, timestamp64 ts);
  } current_;
  std::vector<TsLastSegmentBlockIndex> block_indices_;
  int idx_ = 0;

  using DataBlock = std::unique_ptr<std::string>;
  using BitmapBlock = std::unique_ptr<TsBitmap>;
  struct ColumnBlock {
    BitmapBlock bitmap;
    DataBlock data;
    bool HasBitmap() const { return bitmap != nullptr; }
  };

  // Store decompressed data to this class
  struct BlockCache {
    std::unordered_map<int, std::unordered_map<int, std::unique_ptr<ColumnBlock>>> cache_;
    void PutData(int block_id, int col_id, std::string&& data) {
      if (cache_[block_id][col_id] == nullptr) {
        cache_[block_id][col_id] = std::make_unique<ColumnBlock>();
      }
      cache_[block_id][col_id]->data = std::make_unique<std::string>(std::move(data));
    }
    void PutBitmap(int block_id, int col_id, std::unique_ptr<TsBitmap>&& bitmap) {
      if (cache_[block_id][col_id] == nullptr) {
        cache_[block_id][col_id] = std::make_unique<ColumnBlock>();
      }
      cache_[block_id][col_id]->bitmap = std::move(bitmap);
    }
    ColumnBlock* GetColumnBlock(int block_id, int col_id) { return cache_[block_id][col_id].get(); }
    bool HasCached(int block_id, int col_id) {
      auto p = GetColumnBlock(block_id, col_id);
      return p != nullptr && p->data != nullptr;
    }
  } cache_;

  class EntityBlock : public TsSegmentBlockSpan {
   private:
    TsLastSegmentEntityBlockIteratorBase* parent_iter_;
    const CurrentBlock current_;

   public:
    explicit EntityBlock(TsLastSegmentEntityBlockIteratorBase* piter)
        : parent_iter_(piter), current_(parent_iter_->current_) {}
    ~EntityBlock() {}
    TSEntityID GetEntityId() override { return current_.GetEntityID(); }
    TSTableID GetTableId() override {
      return parent_iter_->block_indices_[current_.block_id].table_id;
    }
    uint32_t GetTableVersion() override {
      return parent_iter_->block_indices_[current_.block_id].table_version;
    }
    void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) override;
    size_t GetRowNum() override { return current_.GetRowCount(); }
    KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                          TSSlice& value) override;
    timestamp64 GetTS(int row_num, const std::vector<AttributeInfo>& schema) override;
    bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) override;
    KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value,
                       TsBitmap& bitmap) override;
  };

  void Invalidate() { valid_ = false; }
  KStatus LoadBlockIndex();
  void NextBlock() {
    idx_++;
    if (Valid()) LoadToCurrentBlockCache();
  }
  KStatus LoadColumnToCache(int block_id, int col_id);
  KStatus LoadVarcharToCache(int block_id);
  KStatus LoadToCurrentBlockCache();

 public:
  explicit TsLastSegmentEntityBlockIteratorBase(const TsLastSegment* last) : lastsegment_(last) {}
  virtual ~TsLastSegmentEntityBlockIteratorBase() = default;

  bool Valid() const { return valid_ && idx_ < block_indices_.size(); }

  virtual KStatus Init() = 0;
  virtual KStatus SeekToFirst() = 0;
  virtual void NextEntityBlock() = 0;
  std::unique_ptr<EntityBlock> GetEntityBlock();
  std::vector<std::unique_ptr<EntityBlock>> GetAllEntityBlocks();
};

class TsLastSegmentFullEntityBlockIterator : public TsLastSegmentEntityBlockIteratorBase {
 public:
  explicit TsLastSegmentFullEntityBlockIterator(const TsLastSegment* last)
      : TsLastSegmentEntityBlockIteratorBase(last) {}
  KStatus Init() override;
  KStatus SeekToFirst() override;
  void NextEntityBlock() override;
};

class TsLastSegmentPartialEntityBlockIterator : public TsLastSegmentEntityBlockIteratorBase {
 private:
  const TSEntityID entity_id_;
  const TSTableID table_id_;

  const std::vector<KwTsSpan> spans_;
  int span_idx_ = -1;

  void LocateToSpan(const KwTsSpan& span);
  void MoveToNextSpan();
  const KwTsSpan& CurrentSpan() const { return spans_[span_idx_]; }

 public:
  // REQUIRE: span has no intersection and must be ordered, check should be done by caller
  TsLastSegmentPartialEntityBlockIterator(const TsLastSegment* last, TSTableID table_id,
                                          TSEntityID entity_id, const std::vector<KwTsSpan>& span)
      : TsLastSegmentEntityBlockIteratorBase(last),
        entity_id_(entity_id),
        table_id_(table_id),
        spans_(span) {}
  KStatus Init() override;
  KStatus SeekToFirst() override;
  void NextEntityBlock() override;
};

struct TsLastSegmentBlockSpan {
  TsLastSegmentBlock* block = nullptr;
  uint32_t table_id = 0;
  uint32_t table_version = 0;
  uint64_t entity_id = 0;

  uint32_t start_row = 0;
  uint32_t end_row = 0;
};

class TsLastSegmentsMergeIterator {
 private:
  struct TsIteratorRowInfo {
    uint64_t entity_id;
    timestamp64 ts;
    uint64_t seq_no;

    uint64_t last_segment_idx;
    uint64_t block_idx;
    uint32_t row_idx;

    inline bool operator<(const TsIteratorRowInfo& other) const {
      return entity_id != other.entity_id ? entity_id < other.entity_id
                                          : ts != other.ts ? ts < other.ts : seq_no > other.seq_no;
    }
  };

  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  std::vector<std::vector<TsLastSegmentBlockIndex>> block_indexes_;
  std::vector<std::vector<TsLastSegmentBlockInfo>> block_infos_;

  std::vector<uint64_t> block_idx_in_last_seg_;

  std::vector<TsLastSegmentBlock*> prev_blocks_;
  std::vector<TsLastSegmentBlock*> cur_blocks_;
  std::list<TsIteratorRowInfo> row_infos_;

  void insertRowInfo(uint64_t last_segment_idx, uint64_t block_idx, uint32_t row_idx) {
    uint64_t entity_id = cur_blocks_[last_segment_idx]->GetEntityId(row_idx);
    timestamp64 ts = cur_blocks_[last_segment_idx]->GetTimestamp(row_idx);
    uint64_t seq_no = cur_blocks_[last_segment_idx]->GetSeqNo(row_idx);
    TsIteratorRowInfo row_info = {entity_id, ts, seq_no, last_segment_idx, block_idx, row_idx};
    insertRowInfo(row_info);
  }

  void insertRowInfo(TsIteratorRowInfo& row_info) {
    auto it = row_infos_.begin();
    while (it != row_infos_.end() && *it < row_info) {
      ++it;
    }
    row_infos_.insert(it, row_info);
  }

  KStatus nextBlock(size_t segment_idx) {
    uint64_t block_idx = block_idx_in_last_seg_[segment_idx]++;
    if (prev_blocks_[segment_idx]) {
      delete prev_blocks_[segment_idx];
      prev_blocks_[segment_idx] = nullptr;
    }
    if (cur_blocks_[segment_idx]) {
      prev_blocks_[segment_idx] = cur_blocks_[segment_idx];
      cur_blocks_[segment_idx] = nullptr;
    }
    if (block_idx >= block_infos_[segment_idx].size()) {
      return KStatus::SUCCESS;
    }
    if (!cur_blocks_[segment_idx]) {
      cur_blocks_[segment_idx] = new TsLastSegmentBlock();
    }
    return last_segments_[segment_idx]->GetBlock(block_infos_[segment_idx][block_idx],
                                                 cur_blocks_[segment_idx]);
  }

 public:
  explicit TsLastSegmentsMergeIterator(std::vector<std::shared_ptr<TsLastSegment>>& last_segments)
      : last_segments_(last_segments) {}
  ~TsLastSegmentsMergeIterator() {
    for (int i = 0; i < cur_blocks_.size(); ++i) {
      if (cur_blocks_[i]) {
        delete cur_blocks_[i];
        cur_blocks_[i] = nullptr;
      }
      if (prev_blocks_[i]) {
        delete prev_blocks_[i];
        prev_blocks_[i] = nullptr;
      }
    }
  }

  KStatus Init() {
    block_indexes_.resize(last_segments_.size());
    block_infos_.resize(last_segments_.size());
    for (size_t i = 0; i < last_segments_.size(); ++i) {
      KStatus s = last_segments_[i]->GetAllBlockIndex(&block_indexes_[i]);
      if (s == KStatus::FAIL) {
        return s;
      }
      block_infos_[i].resize(block_indexes_[i].size());
      for (size_t j = 0; j < block_indexes_[i].size(); ++j) {
        s = last_segments_[i]->GetBlockInfo(block_indexes_[i][j], &(block_infos_[i][j]));
        if (s == KStatus::FAIL) {
          return s;
        }
      }
    }

    cur_blocks_.resize(last_segments_.size());
    prev_blocks_.resize(last_segments_.size());
    block_idx_in_last_seg_.resize(last_segments_.size());
    for (size_t i = 0; i < last_segments_.size(); ++i) {
      KStatus s = nextBlock(i);
      if (s == KStatus::FAIL) {
        return s;
      }
      if (cur_blocks_[i]) {
        insertRowInfo(i, block_idx_in_last_seg_[i] - 1, 0);
      }
    }
    return KStatus::SUCCESS;
  }

  KStatus Next(TsLastSegmentBlockSpan* block_span, bool& is_finished) {
    if (row_infos_.empty()) {
      is_finished = true;
      return KStatus::SUCCESS;
    }

    TsIteratorRowInfo first_row_info = row_infos_.front();
    row_infos_.pop_front();
    TsIteratorRowInfo second_row_info{UINT64_MAX, INT64_MAX, UINT64_MAX};
    if (!row_infos_.empty()) {
      second_row_info = row_infos_.front();
    }

    uint32_t n_rows_in_block =
        block_infos_[first_row_info.last_segment_idx][first_row_info.block_idx].nrow;
    uint32_t row_idx = first_row_info.row_idx;
    TsLastSegmentBlock* block = cur_blocks_[first_row_info.last_segment_idx];
    for (; row_idx < n_rows_in_block; ++row_idx) {
      TsIteratorRowInfo new_row_info = {block->GetEntityId(row_idx), block->GetTimestamp(row_idx),
                                        block->GetSeqNo(row_idx)};
      if (second_row_info < new_row_info) {
        new_row_info.last_segment_idx = first_row_info.last_segment_idx;
        new_row_info.block_idx = first_row_info.block_idx;
        new_row_info.row_idx = row_idx;
        insertRowInfo(new_row_info);
        break;
      }
    }

    TsLastSegmentBlockIndex block_index =
        block_indexes_[first_row_info.last_segment_idx][first_row_info.block_idx];
    block_span->block = block;
    block_span->table_id = block_index.table_id;
    block_span->table_version = block_index.table_version;
    block_span->entity_id = first_row_info.entity_id;
    block_span->start_row = first_row_info.row_idx;
    block_span->end_row = row_idx;

    if (row_idx == n_rows_in_block) {
      KStatus s = nextBlock(first_row_info.last_segment_idx);
      if (s == KStatus::FAIL) {
        return s;
      }
      uint64_t last_seg_idx = first_row_info.last_segment_idx;
      if (cur_blocks_[last_seg_idx]) {
        insertRowInfo(last_seg_idx, block_idx_in_last_seg_[last_seg_idx] - 1, 0);
      }
    }
    return KStatus::SUCCESS;
  }
};

struct TsLastSegmentSlice {
  TsLastSegment* last_seg_;
  uint32_t offset;
  uint32_t count;
};
}  // namespace kwdbts
