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

#include <filesystem>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include "ts_block.h"
#include "ts_entity_segment_data.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_lastsegment_manager.h"
#include "ts_lastsegment.h"


namespace kwdbts {

struct TsEntitySegmentBlockItem {
  uint64_t block_id = 0;          // block item id
  uint64_t entity_id = 0;
  uint64_t prev_block_id = 0;     // pre block item id
  uint64_t block_offset = 0;
  uint32_t block_len = 0;
  uint32_t table_version = 0;
  uint32_t n_cols = 0;
  uint32_t n_rows = 0;
  timestamp64 min_ts = INT64_MAX;
  timestamp64 max_ts = INT64_MIN;
  uint64_t agg_offset = 0;
  uint32_t agg_len = 0;
  uint16_t non_null_row_count = 0;  // the number of non-null rows
  bool is_overflow = false;
  bool is_agg_res_available = false;  //  agg for block is valid.
  char reserved[48] = {0};      // reserved for user-defined information.
};
static_assert(sizeof(TsEntitySegmentBlockItem) == 128,
              "wrong size of TsEntitySegmentBlockItem, please check compatibility.");

static constexpr uint64_t TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC = 0xcb2ffe9321847272;
static constexpr uint64_t TS_ENTITY_SEGMENT_BLOCK_ITEM_FILE_MAGIC = 0xcb2ffe9321847273;

/**
 * TsEntitySegmentEntityItemFile used for managing entity_item file.
 * index of block items.
 */
class TsEntitySegmentEntityItemFile {
 private:
  struct TsEntityItemFileHeader {
    uint64_t magic;               // Magic number for block.e file.
    int32_t encoding;             // Encoding scheme.
    int32_t status;               // status flag.
    uint64_t entity_num;          // entity num
    char reserved[40];           // reserved for user-defined meta data information.
  };
  static_assert(sizeof(TsEntityItemFileHeader) == 64, "wrong size of TsBlockFileHeader, please check compatibility.");

  struct TsEntityItem {
    uint64_t entity_id = 0;
    uint64_t cur_block_id = 0;        // block id that is allocating space for writing.
    int64_t max_ts = INT64_MIN;       // max ts of current entity in this Partition
    int64_t min_ts = INT64_MAX;       // min ts of current entity in this Partition
    uint64_t row_written = 0;         // row num that has written into file.
    char reserved[88] = {0};          // reserved for user-defined information.
  };
  static_assert(sizeof(TsEntityItem) == 128, "wrong size of TsEntityItem, please check compatibility.");

  string file_path_;
  std::unique_ptr<TsFile> file_;

  KRWLatch rw_latch_;

  TsEntityItemFileHeader header_;

 public:
  explicit TsEntitySegmentEntityItemFile(const string& file_path) :
           file_path_(file_path), rw_latch_(RWLATCH_ID_ENTITY_ITEM_RWLOCK) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
    memset(&header_, 0, sizeof(TsEntityItemFileHeader));
  }

  ~TsEntitySegmentEntityItemFile() {}

  KStatus Open();

  void WrLock();

  void RdLock();

  void UnLock();

  KStatus UpdateEntityItem(uint64_t entity_id, const TsEntitySegmentBlockItem& block_item_info, bool lock = true);

  KStatus GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id, bool lock = true);
};

class TsEntitySegmentBlockItemFile {
 private:
  string file_path_;
  std::unique_ptr<TsFile> file_;

  KRWLatch* block_item_mtx_{nullptr};

  struct TsBlockItemFileHeader {
    uint64_t magic;               // Magic number for block.e file.
    int32_t encoding;             // Encoding scheme.
    int32_t status;               // status flag.
    uint64_t block_num;
    char user_defined[40];       // reserved for user-defined meta data information.
  };
  static_assert(sizeof(TsBlockItemFileHeader) == 64,
                "wrong size of TsBlockItemFileHeader, please check compatibility.");

  TsBlockItemFileHeader header_;

 public:
  explicit TsEntitySegmentBlockItemFile(const string& file_path) : file_path_(file_path) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
    block_item_mtx_ = new KRWLatch(RWLATCH_ID_MMAP_BLOCK_META_RWLOCK);
    memset(&header_, 0, sizeof(TsBlockItemFileHeader));
  }

  ~TsEntitySegmentBlockItemFile() {
    if (block_item_mtx_) {
      delete block_item_mtx_;
      block_item_mtx_ = nullptr;
    }
  }

  inline void ReadLock() {
    RW_LATCH_X_LOCK(block_item_mtx_);
  }

  inline void UnLock() {
    RW_LATCH_UNLOCK(block_item_mtx_);
  }

  KStatus Open();

  KStatus AllocateBlockItem(uint64_t entity_id, TsEntitySegmentBlockItem& block_item_info);

  KStatus GetBlockItem(uint64_t entity_id, uint64_t blk_offset, TsEntitySegmentBlockItem& blk_item);

 protected:
  KStatus readFileHeader(TsBlockItemFileHeader& block_meta);

  KStatus writeFileMeta(TsBlockItemFileHeader& block_meta);
};

class TsEntitySegment;
class TsEntitySegmentMetaManager {
 private:
  string path_;
  TsEntitySegmentEntityItemFile entity_header_;
  TsEntitySegmentBlockItemFile block_header_;

 public:
  explicit TsEntitySegmentMetaManager(const string& path);

  ~TsEntitySegmentMetaManager() {}

  KStatus Open();

  KStatus AppendBlockItem(TsEntitySegmentBlockItem& blk_item);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<TsEntitySegmentBlockItem>* blk_items);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, TsEntitySegment* blk_segment,
                        std::list<TsBlockSpan>* block_spans);
};

struct TsEntitySegmentBlockInfo {
  std::vector<uint32_t> col_block_offset;
};

struct TsEntitySegmentColumnBlock {
  TsBitmap bitmap;
  std::string buffer;
  std::vector<std::string> var_rows;
};

class TsVGroupPartition;
class TsEntityBlock : public TsBlock {
 private:
  uint32_t table_id_ = 0;
  uint32_t table_version_ = 0;
  uint64_t entity_id_ = 0;
  std::vector<AttributeInfo> metric_schema_;

  TsEntitySegmentBlockInfo block_info_;
  std::vector<TsEntitySegmentColumnBlock> column_blocks_;
  std::string extra_buffer_;

  uint32_t n_rows_ = 0;
  uint32_t n_cols_ = 0;

  uint64_t block_offset_ = 0;
  uint32_t block_length_ = 0;

  TsEntitySegment* entity_segment_ = nullptr;

 public:
  TsEntityBlock() = delete;
  // for read
  TsEntityBlock(uint32_t table_id, const TsEntitySegmentBlockItem& block_item, TsEntitySegment* block_segment);
  // for write
  TsEntityBlock(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                std::vector<AttributeInfo>& metric_schema);
  TsEntityBlock(const TsEntityBlock& other);
  ~TsEntityBlock() {}

  bool HasData() { return n_rows_ > 0; }

  size_t GetRowNum() { return n_rows_; }

  uint32_t GetNCols() { return n_cols_; }

  TSTableID GetTableId() { return table_id_; }

  uint32_t GetTableVersion() { return table_version_; }

  uint64_t GetEntityId() { return entity_id_; }

  std::vector<AttributeInfo> GetMetricSchema() { return metric_schema_; }

  const TsEntitySegmentBlockInfo& GetBlockInfo() const { return block_info_; }

  uint64_t GetBlockOffset() const { return block_offset_; }

  uint32_t GetBlockLength() const { return block_length_; }

  inline bool HasDataCached(int32_t col_idx) {
    assert(col_idx >= -1);
    return n_cols_ > 0 && column_blocks_.size() == n_cols_ && !column_blocks_[col_idx + 1].buffer.empty();
  }

  uint64_t GetLSN(uint32_t row_idx);

  timestamp64 GetTimestamp(uint32_t row_idx);

  KStatus GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value, std::vector<DataFlags>& data_flags);

  char* GetMetricColAddr(uint32_t col_idx);

  KStatus GetMetricColValue(uint32_t row_idx, uint32_t col_idx, TSSlice& value);

  KStatus Append(TsBlockSpan& span, bool& is_full);

  KStatus Flush(TsVGroupPartition* partition);

  KStatus LoadLSNColData(TSSlice buffer);

  KStatus LoadColData(int32_t col_idx, const std::vector<AttributeInfo>& metric_schema, TSSlice buffer);

  KStatus LoadBlockInfo(TSSlice buffer);

  KStatus LoadAllData(const std::vector<AttributeInfo>& metric_schema, TSSlice buffer);

  KStatus GetRowSpans(const std::vector<KwTsSpan>& ts_spans, std::vector<std::pair<int, int>>& row_spans);

  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                     char** value);

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema,
                       TsBitmap& bitmap);

  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                        TSSlice& value);

  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema);

  timestamp64 GetTS(int row_num);

  uint64_t* GetLSNAddr(int row_num);

  void Clear();
};

class TsEntitySegment : public TsSegmentBase, public enable_shared_from_this<TsEntitySegment> {
 private:
  string dir_path_;
  TsEntitySegmentMetaManager meta_mgr_;
  TsEntitySegmentBlockFile block_file_;
  TsEntitySegmentAggFile agg_file_;

 public:
  TsEntitySegment() = delete;

  explicit TsEntitySegment(const std::filesystem::path& root);

  ~TsEntitySegment() {}

  KStatus Open();

  KStatus AppendBlockData(TsEntitySegmentBlockItem& blk_item, const TSSlice& data, const TSSlice& agg);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<TsEntitySegmentBlockItem>* blk_items);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<TsBlockSpan>* blocks) override;

  KStatus GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>& metric_schema,
                         TsEntityBlock* block);
};

class TsEntitySegmentBuilder {
 private:
  struct TsEntityKey {
    TSTableID table_id = 0;
    uint32_t table_version = 0;
    uint64_t entity_id = 0;

    bool operator==(const TsEntityKey& other) const {
      return entity_id == other.entity_id && table_version == other.table_version && table_id == other.table_id;
    }
    bool operator!=(const TsEntityKey& other) const {
      return !(*this == other);
    }
  };

  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  TsVGroupPartition* partition_;

 public:
  explicit TsEntitySegmentBuilder(std::vector<std::shared_ptr<TsLastSegment>> last_segments,
                                 TsVGroupPartition* partition = nullptr) :
                                 last_segments_(last_segments), partition_(partition) {}
  ~TsEntitySegmentBuilder() {}

  KStatus BuildAndFlush();
};

}  // namespace kwdbts
