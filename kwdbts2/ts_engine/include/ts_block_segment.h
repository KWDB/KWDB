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
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "ts_block_segment_data.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_lastsegment_manager.h"
#include "ts_lastsegment.h"


namespace kwdbts {

const size_t MAX_ROWS_PER_BLOCK = 1000;

struct TsBlockSegmentBlockItemInfo {
  uint64_t block_id = 0;          // block item id
  uint64_t entity_id = 0;
  uint64_t prev_block_id = 0;     // pre block item id
  uint64_t block_offset = 0;
  uint32_t block_len = 0;
  uint32_t table_version = 0;
  uint32_t cols = 0;
  uint32_t rows = 0;
  timestamp64 min_ts = INT64_MAX;
  timestamp64 max_ts = INT64_MIN;
  char reserved[64] = {0};      // reserved for user-defined information.
};
static_assert(sizeof(TsBlockSegmentBlockItemInfo) == 128,
              "wrong size of TsBlockSegmentBlockItemInfo, please check compatibility.");

class TsBlockSegmentBlockItem {
 private:
  TsBlockSegmentBlockItemInfo info_;

 public:
  TsBlockSegmentBlockItem() {
    memset(&info_, 0, sizeof(TsBlockSegmentBlockItemInfo));
  }
  ~TsBlockSegmentBlockItem() {}

  TsBlockSegmentBlockItemInfo& Info() {
    return info_;
  }

  ostream& to_string(ostream& os) {
    os << " entity_id:" << info_.entity_id
       << " data_block_id:" << info_.block_id
       << " prev_block_id:" << info_.prev_block_id
       << std::endl;
    return os;
  }
};

static constexpr uint64_t TS_BLOCK_SEGMENT_ENTITY_ITEM_FILE_MAGIC = 0xcb2ffe9321847272;
static constexpr uint64_t TS_BLOCK_SEGMENT_BLOCK_ITEM_FILE_MAGIC = 0xcb2ffe9321847273;

/**
 * TsBlockSegmentEntityItemFile used for managing entity_item file.
 * index of block items.
 */
class TsBlockSegmentEntityItemFile {
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
  explicit TsBlockSegmentEntityItemFile(const string& file_path) :
           file_path_(file_path), rw_latch_(RWLATCH_ID_ENTITY_ITEM_RWLOCK) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
    memset(&header_, 0, sizeof(TsEntityItemFileHeader));
  }

  ~TsBlockSegmentEntityItemFile() {}

  KStatus Open();

  void WrLock();

  void RdLock();

  void UnLock();

  KStatus UpdateEntityItem(uint64_t entity_id, const TsBlockSegmentBlockItemInfo& block_item_info, bool lock = true);

  KStatus GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id, bool lock = true);
};

class TsBlockSegmentBlockItemFile {
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
  explicit TsBlockSegmentBlockItemFile(const string& file_path) : file_path_(file_path) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
    block_item_mtx_ = new KRWLatch(RWLATCH_ID_MMAP_BLOCK_META_RWLOCK);
    memset(&header_, 0, sizeof(TsBlockItemFileHeader));
  }

  ~TsBlockSegmentBlockItemFile() {
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

  KStatus AllocateBlockItem(uint64_t entity_id, TsBlockSegmentBlockItemInfo& block_item_info);

  KStatus GetBlockItem(uint64_t entity_id, uint64_t blk_offset, std::shared_ptr<TsBlockSegmentBlockItem>& blk_item);

 protected:
  KStatus readFileHeader(TsBlockItemFileHeader& block_meta);

  KStatus writeFileMeta(TsBlockItemFileHeader& block_meta);
};

class TsBlockSegmentMetaManager {
 private:
  string path_;
  TsBlockSegmentEntityItemFile entity_meta_;
  TsBlockSegmentBlockItemFile block_meta_;

 public:
  explicit TsBlockSegmentMetaManager(const string& path);

  ~TsBlockSegmentMetaManager() {}

  KStatus Open();

  KStatus AppendBlockItem(TsBlockSegmentBlockItem* blk_item);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<std::shared_ptr<TsBlockSegmentBlockItem>>* blk_items);
};

class TsBlockSegment {
 private:
  string dir_path_;
  TsBlockSegmentMetaManager meta_mgr_;
  TsBlockSegmentBlockFile block_file_;

 public:
  TsBlockSegment() = delete;

  explicit TsBlockSegment(const std::filesystem::path& root);

  ~TsBlockSegment() {}

  KStatus Open();

  KStatus AppendBlockData(TsBlockSegmentBlockItem* blk_item, const TSSlice& data, const TSSlice& agg);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<std::shared_ptr<TsBlockSegmentBlockItem>>* blk_items);

  KStatus GetBlockData(TsBlockSegmentBlockItem* blk_item, char* buff);
};

class TsVGroupPartition;

struct TsBlockSegmentBlockInfo {
  std::vector<uint32_t> col_block_offset;
};

struct TsBlockSegmentColumnBlock {
  TsBitmap bitmap;
  std::string buffer;
};

class TsBlockSegmentBlock {
 private:
  uint32_t table_id_;
  uint32_t table_version_;
  uint64_t entity_id_;
  std::vector<AttributeInfo> metric_schema_;

  TsBlockSegmentBlockInfo block_info;
  std::vector<TsBlockSegmentColumnBlock> column_blocks;

  uint32_t n_rows_ = 0;
  uint32_t n_cols_ = 0;

 public:
  TsBlockSegmentBlock(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                      std::vector<AttributeInfo>& metric_schema);
  TsBlockSegmentBlock(const TsBlockSegmentBlock& other);
  ~TsBlockSegmentBlock() {}

  bool HasData() { return n_rows_ > 0; }

  uint32_t GetNRows() const { return n_rows_; }

  uint32_t GetTableId() const { return table_id_; }

  uint32_t GetTableVersion() const { return table_version_; }

  uint64_t GetEntityId() const { return entity_id_; }

  std::vector<AttributeInfo> GetMetricSchema() { return metric_schema_; }

  uint64_t GetSeqNo(uint32_t row_idx);

  timestamp64 GetTimestamp(uint32_t row_idx);

  KStatus GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value);

  KStatus Append(TsLastSegmentBlockSpan& span, bool& is_full);

  KStatus Flush(TsVGroupPartition* partition);

  void Clear();
};

class TsBlockSegmentBuilder {
 private:
  struct TsEntityKey {
    uint32_t table_id = 0;
    uint32_t table_version = 0;
    uint64_t entity_id = 0;

    bool operator==(const TsEntityKey& other) const {
      return table_id == other.table_id && table_version == other.table_version && entity_id == other.entity_id;
    }
    bool operator!=(const TsEntityKey& other) const {
      return !(*this == other);
    }
    bool operator<(const TsEntityKey& other) const {
      if (table_id != other.table_id) {
        return table_id < other.table_id;
      }
      if (table_version != other.table_version) {
        return table_version < other.table_version;
      }
      return entity_id < other.entity_id;
    }
  };

  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  TsVGroupPartition* partition_;

 public:
  explicit TsBlockSegmentBuilder(std::vector<std::shared_ptr<TsLastSegment>> last_segments,
                                 TsVGroupPartition* partition = nullptr) :
                                 last_segments_(last_segments), partition_(partition) {}
  ~TsBlockSegmentBuilder() {}

  KStatus BuildAndFlush();
};

}  // namespace kwdbts
