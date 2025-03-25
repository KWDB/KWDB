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
#include <utility>
#include <list>
#include <set>
#include <unordered_map>
#include <string>
#include <vector>
#include "ts_common.h"
#include "ts_block_segment_data.h"
#include "ts_hash_latch.h"
#include "ts_io.h"


namespace kwdbts {

/**
 * @brief  BlockItem managing BLock status
 * 
*/
struct TsBlockItemInfo {
  uint64_t block_id;          // block id
  uint64_t entity_id;
  uint64_t prev_block_id;     // pre BlockItem
  uint32_t schema_version;
  uint32_t row_count;
  timestamp64 min_ts_in_block;
  timestamp64 max_ts_in_block;
  uint64_t block_offset;
  char reserved[136];      // reserved for user-defined information.
};
static_assert(sizeof(TsBlockItemInfo) == 192, "wrong size of TsBlockItemInfo, please check compatibility.");


class TsBlockItem {
 private:
  TsBlockItemInfo info_;

 public:
  TsBlockItem() {}
  ~TsBlockItem() {}

  TsBlockItemInfo& Info() {
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

enum TsFileStatus {
  NOT_READY = 0,
  READY = 1,
};


static constexpr uint64_t TS_SEGMENT_ENTITY_META_MAGIC = 0xcb2ffe9321847272;
static constexpr uint64_t TS_SEGMENT_BLOCK_META_MAGIC = 0xcb2ffe9321847273;

#define ENTITY_ITEM_META_LATCH_BUCKET_NUM 100

/**
 * TsSegmentEntityMetaFile used for managing entity_item.meta file.
 * index of block items.
 */
class TsSegmentEntityMetaFile {
 private:
  struct TsEntityMetaFileHeader {
    uint64_t magic;               // Magic number for block.e file.
    int32_t encoding;             // Encoding scheme.
    int32_t status;               // status flag.
    uint64_t entity_num;          // entity num
    char reserved[104];           // reserved for user-defined meta data information.
  };
  static_assert(sizeof(TsEntityMetaFileHeader) == 128, "wrong size of TsEntityMetaFileHeader, please check compatibility.");

  struct TsEntityItem {
    uint64_t entity_id;
    uint64_t cur_block_id;            // block id that is allocating space for writing.
    int64_t max_ts = INT64_MIN;       // max ts of current entity in this Partition
    int64_t min_ts = INT64_MAX;       // min ts of current entity in this Partition
    uint64_t row_allocated;           // allocated row num for writing.
    uint64_t row_written;             // row num that has written into file.
    bool is_initialized;
    char reserved[79];                // reserved for user-defined information.
  };
  static_assert(sizeof(TsEntityItem) == 128, "wrong size of TsEntityItem, please check compatibility.");

  string file_path_;
  std::unique_ptr<TsFile> file_;

  TsHashRWLatch entity_hash_latch_;

  TsEntityMetaFileHeader header_;

 public:
  explicit TsSegmentEntityMetaFile(const string& file_path) :
           file_path_(file_path), entity_hash_latch_(ENTITY_ITEM_META_LATCH_BUCKET_NUM, RWLATCH_ID_ENTITY_ITEM_RWLOCK) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
  }

  ~TsSegmentEntityMetaFile() {}

  KStatus Open();

  // update entityItem, if need update more, we can add paramters.
  KStatus UpdateEntityItem(uint64_t entity_id, const TsBlockItemInfo& block_item_info);

  KStatus GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id);
};

class TsSegmentBlockMetaFile {
 private:
  string file_path_;
  std::unique_ptr<TsFile> file_;

  KRWLatch* block_item_mtx_{nullptr};

  struct TsBlockFileMetaFileHeader {
    uint64_t magic;               // Magic number for block.e file.
    int32_t encoding;             // Encoding scheme.
    int32_t status;               // status flag.
    uint64_t block_num;
    char user_defined[104];       // reserved for user-defined meta data information.
  };
  static_assert(sizeof(TsBlockFileMetaFileHeader) == 128,
                "wrong size of TsBlockFileMetaFileHeader, please check compatibility.");

  TsBlockFileMetaFileHeader header_;

 public:
  explicit TsSegmentBlockMetaFile(const string& file_path) : file_path_(file_path) {
    file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
    block_item_mtx_ = new KRWLatch(RWLATCH_ID_MMAP_BLOCK_META_RWLOCK);
  }

  ~TsSegmentBlockMetaFile() {
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

  inline KStatus readFileMeta(TsBlockFileMetaFileHeader& block_meta) {
    TSSlice result;
    TsStatus s = file_->Read(0, sizeof(TsBlockFileMetaFileHeader), &result, reinterpret_cast<char *>(&block_meta));
    return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
  }

  inline KStatus writeFileMeta(TsBlockFileMetaFileHeader& block_meta) {
    TsStatus s = file_->Write(0, TSSlice{reinterpret_cast<char *>(&block_meta), sizeof(TsBlockFileMetaFileHeader)});
    return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
  }

  KStatus Open();
  KStatus AllocateBlockItem(uint64_t entity_id, TsBlockItemInfo& block_item_info);
  KStatus GetBlockItem(uint64_t entity_id, uint64_t blk_offset, std::shared_ptr<TsBlockItem>& blk_item);
};

class TsBlkSegmentMetaManager {
 private:
  string path_;
  TsSegmentEntityMetaFile entity_meta_;
  TsSegmentBlockMetaFile block_meta_;

 public:
  explicit TsBlkSegmentMetaManager(const string& path);

  ~TsBlkSegmentMetaManager() {}

  KStatus Open();

  KStatus AppendBlockItem(TsBlockItem* blk_item);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<std::shared_ptr<TsBlockItem>>* blk_items);
};


class TsBlockSegment {
 private:
  string dir_path_;
  TsBlkSegmentMetaManager meta_mgr_;
  TsBlockFile block_file_;

 public:
  TsBlockSegment() = delete;

  explicit TsBlockSegment(const std::filesystem::path& root);

  ~TsBlockSegment() {}

  KStatus Open();

  KStatus AppendBlockData(TsBlockItem* blk_item, const TSSlice& data, const TSSlice& agg);

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<std::shared_ptr<TsBlockItem>>* blk_items);

  KStatus GetBlockData(TsBlockItem* blk_item, char* buff);
};

}  // namespace kwdbts
