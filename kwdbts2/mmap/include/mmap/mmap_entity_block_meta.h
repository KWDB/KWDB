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

#include <assert.h>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <cstring>
#include <vector>
#include <queue>
#include <utility>
#include "mmap/mmap_file.h"
#include "ts_common.h"
#include "utils/compress_utils.h"
#include "libkwdbts2.h"
#include "lt_rw_latch.h"
#include "bitmap_utils.h"

const int METRIC_VERSION = 1;                       // metric version
const size_t BLOCK_AGG_NUM = 3;                     // Data Block has agg num.
const size_t BLOCK_AGG_COUNT_SIZE = 2;              // COUNT agg value has bytes.
extern size_t META_BLOCK_ITEM_MAX;                  // blockitem num in one .meta file
const size_t BLOCK_ITEM_MAX = 1000000;              // max block item number in each segment
const size_t BLOCK_ROWS_MAX = 1000;                 // max rows number in each block item, pay attention to the delete bitmap
const size_t BLOCK_ROWS_MIN = 10;                   // min rows number in each block
const int64_t INVALID_TS = INT64_MAX;

#define BLOCK_ITEM_BITMAP_SIZE (1000 + 7) / 8

typedef uint32_t BLOCK_ID;

/**
 * @brief check if all rows is null for a column.
 * @param[in] bitmap  bitmap addr.
 * @param[in] start_row     row num of bitmap. start from 1.
 * @param[in] rows_count    check count.
 *
*/
bool isAllNull(char* bitmap, size_t start_row, size_t rows_count);

// row info in current partition.
struct MetricRowID {
  BLOCK_ID block_id = 0;
  uint32_t offset_row = 0;  // the first row is 1.

  MetricRowID operator+(int offset) const {
    return MetricRowID{block_id, offset_row + offset};
  }

  MetricRowID operator-(int offset) const {
    return MetricRowID{block_id, offset_row - offset};
  }

  MetricRowID operator++() {
    offset_row++;
    return *this;
  }

  MetricRowID operator++(int) {
    offset_row++;
    return *this;
  }

  MetricRowID operator--() {
    offset_row--;
    return *this;
  }

  MetricRowID operator--(int) {
    offset_row--;
    return *this;
  }

  size_t operator()(const MetricRowID& rowId) const {
    return std::hash<BLOCK_ID>()(rowId.block_id) ^ std::hash<uint32_t>()(rowId.offset_row);
  }

  bool operator==(const MetricRowID& rowId) const {
    return (block_id == rowId.block_id) && (offset_row == rowId.offset_row);
  }

  bool operator!=(const MetricRowID& rowId) const {
    return (offset_row != rowId.offset_row) || (block_id != rowId.block_id);
  }

  bool operator<(const MetricRowID& rowId) const {
    return (block_id < rowId.block_id) || (block_id == rowId.block_id && offset_row < rowId.offset_row);
  }

  bool operator>(const MetricRowID& rowId) const {
    return (block_id > rowId.block_id) || (block_id == rowId.block_id && offset_row > rowId.offset_row);
  }
};

/**
 * @brief EntityHeader records  entities status in current partition.
 *        
 *       id = 0 means is initialing.
 */
struct EntityHeader {
  int magic;
  int version;
  uint16_t compressed_datafile_id;   // max data file that already compressed.
  uint16_t cur_entity_id;            // max entity id in using.
  BLOCK_ID cur_datafile_id;          // max data file id in using.
  BLOCK_ID cur_block_id;             // max block id in using.
  uint16_t partition_interval;       // configure item
  uint16_t max_rows_per_block;       // configure item
  uint16_t max_entities_per_subgroup;   // configure item
  int64_t minTimestamp;              // max ts and min ts in current partition.
  int64_t maxTimestamp;
  uint32_t max_blocks_per_segment;   // configure item
  bool partition_deleted = false;
  bool data_disordered = false;
  bool data_deleted = false;
  bool is_initialized = false;
  char user_defined[28];  // < reserved for user-defined information.

  ostream& to_string(ostream& os) {
    std::cout << "EntityHeader:"
              << " compressed_datafile_id:" << compressed_datafile_id
              << " cur_entity_id:" << cur_entity_id
              << " cur_datafile_id:" << cur_datafile_id
              << " cur_block_id:" << cur_block_id
              << " cur_meta_block_id:" << cur_block_id
              << " partition_interval:" << partition_interval
              << " max_rows_per_block:" << max_rows_per_block
              << std::endl;
    return os;
  }
};

static_assert(sizeof(EntityHeader) == 88, "wrong size of EntityHeader, please check compatibility.");

/**
 * @brief EntityItem records one entity status in current paritition.
 *  
 */
struct EntityItem {
  uint64_t columns_varchar_offset;  // .s file writing offset. all columns use one .s file.
  uint64_t row_allocated;           // allocated row num for writing.
  int64_t row_written;             // row num that has writen into file.
  uint64_t lasted_checkpoint;       // WAL check point.
  uint32_t entity_id;
  BLOCK_ID block_count;             // block count for current entity.
  BLOCK_ID cur_block_id;            // block id that is allocating space for writing.
  int64_t max_ts;                   // max ts of current entity in this Partition
  int64_t min_ts;                   // min ts of current entity in this Partition
  bool is_deleted;                  // entity delete flag.
  bool is_disordered = false;
  BLOCK_ID count_block_id = 0;  // block should to calculate count.
  char user_defined[28];  // reserved for user-defined information.

  ostream& to_string(ostream& os) {
    os << " entity_id:" << entity_id
       << " block_count:" << block_count
       << " cur_block_id:" << cur_block_id
       << " cloumns_varchar_offset:" << columns_varchar_offset
       << " row_written:" << row_written
       << " lasted_checkpoint:" << lasted_checkpoint
       << std::endl;
    return os;
  }
};

static_assert(sizeof(EntityItem) == 104, "wrong size of EntityItem, please check compatibility.");

/**
 * @brief  BlockItem managing BLock status, block has fixed row num used for store entity data.
 * 
*/
struct BlockItem {
  uint32_t crc;
  bool unordered_flag;        // block data is disorder?
  bool is_overflow;
  bool is_agg_res_available;  //  agg for block is valid.
  bool read_only = false;     // mark the blockitem is read only
  uint16_t non_null_row_count;  // the number of non-null rows
  uint16_t useless;           // no implementation
  uint32_t entity_id;
  BLOCK_ID block_id;          // block ID
  BLOCK_ID prev_block_id;     // pre BlockItem ID
  uint32_t publish_row_count; // rows that already writen.
  uint32_t alloc_row_count;   // row space that already allocated.
  char rows_delete_flags[BLOCK_ITEM_BITMAP_SIZE];  // Block bitmap that mark if row is deleted.
  uint32_t max_rows_in_block;  // max rows that can be stored in a block
  timestamp64 min_ts_in_block;
  timestamp64 max_ts_in_block;
  char user_defined[4];      // reserved for user-defined information.

  MetricRowID getRowID(uint32_t offset) {
    return std::move(MetricRowID{block_id, offset});
  }

  void CopyMetricMetaInfo(const BlockItem& blk_item) {
    uint32_t entity_id_s = entity_id;
    BLOCK_ID block_id_s = block_id;
    BLOCK_ID prev_block_id_s = prev_block_id;
    memcpy(this, &blk_item, sizeof(blk_item));
    entity_id = entity_id_s;
    block_id = block_id_s;
    prev_block_id = prev_block_id_s;
  }

  // count deleted rows in current block.
  size_t getDeletedCount() {
    size_t count = 0;
    for (int i = 0; i < alloc_row_count;) {
      size_t byte = i >> 3;
      size_t bit = 1 << (i & 7);
      if (0 == rows_delete_flags[byte]) {
        i += 8;
      } else {
        if (rows_delete_flags[byte] & bit) {
          ++count;
        }
        ++i;
      }
    }
    return count;
  }

  inline bool isBlockEmpty() {
    return isAllDeleted(rows_delete_flags, 1, alloc_row_count);
  }

  int isDeleted(size_t row_idx, bool* is_deleted) {
    if (row_idx > alloc_row_count) {
      return -1;
    }
    size_t byte = (row_idx - 1) >> 3;
    size_t bit = 1 << ((row_idx - 1) & 7);
    *is_deleted = rows_delete_flags[byte] & bit;
    return 0;
  }

  int setDeleted(size_t row_idx) {
    if (row_idx > alloc_row_count) {
      LOG_ERROR("BlockItem setDeleted failed, block_item id: %d, alloc_row_count: %d, row_idx: %zu",
                block_id, alloc_row_count, row_idx)
      return -1;
    }
    if (is_agg_res_available) {
      is_agg_res_available = false;
    }
    setRowDeleted(rows_delete_flags, row_idx);
    return 0;
  }

  // reset blockitem for recycling this blockitem.
  void recycle() {
    unordered_flag = false;
    publish_row_count = 0;
    alloc_row_count = 0;
    memset(rows_delete_flags, 0, sizeof(rows_delete_flags));
  }

  void clear() {
    memset(this, 0, sizeof(BlockItem));
  }

  uint32_t getNonNullRowCount() {
    return (is_agg_res_available && non_null_row_count > 0) ? non_null_row_count : (publish_row_count - getDeletedCount());
  }

  ostream& to_string(ostream& os) {
    os << " entity_id:" << entity_id
       << " unordered_flag:" << unordered_flag
       << " is_agg_res_available:" << is_agg_res_available
       << " read_only:" << read_only
       << " data_block_id:" << block_id
       << " prev_block_id:" << prev_block_id
       << " publish_row_count:" << publish_row_count
       << " alloc_row_count:" << alloc_row_count
       << std::endl;
    return os;
  }
};

static_assert(sizeof(BlockItem) == 192, "wrong size of BlockItem, please check compatibility.");

// record one continous span in block.
struct BlockSpan {
  BlockItem* block_item = nullptr;
  uint32_t start_row;             // first row is 0
  uint32_t row_num = 0;               // continuous num
  uint32_t subgroup_id = 0;
  timestamp64 min_ts = INT64_MAX;
  timestamp64 max_ts = INT64_MIN;
};

/**
 * @brief  MMapEntityBlockMeta used for managing entity block for current time partition.
 *          EntityBlockMetaManager is managing MMapBlockMeta objects.
*/
class MMapEntityBlockMeta : public MMapFile {
  friend class EntityBlockMetaManager;

 private:
  using MMapEntityMetaLatch = KLatch;
  using MMapEntityMetaRWLatch = KRWLatch;
  MMapEntityMetaLatch* obj_mutex_;
  MMapEntityMetaRWLatch* entity_blockitem_mutex_;   // control entity item / block item
 protected:

  size_t block_item_offset_;  // the first blockitem offset in meta file.

  EntityHeader* entity_header_ = nullptr;
  EntityItem* entity_item_offset_ = nullptr;
  uint16_t block_agg_num_ = 4;
  uint32_t block_item_offset_num_ = 0; // min blockitem id in current meta file.
  bool first_meta_ = true;
  bool is_et_ = false;
  uint16_t max_entities_per_subgroup_ = 500;

  // assuming mutex locked
  int incSize_(size_t len);

 public:
  MMapEntityBlockMeta();

  MMapEntityBlockMeta(bool is_et, bool first_meta);

  virtual ~MMapEntityBlockMeta();

  int init(const string& file_path, const std::string& db_path, const string& tbl_sub_path, int flags,
           bool alloc_block_item, uint16_t config_subgroup_entities);

  // 32bit system: 4-bytes for size & pointers
  // 64bit system: 8-bytes
  size_t& size() { return ((reinterpret_cast<size_t*>(mem_))[1]); }

  inline int startLoc() const { return 16; }

  inline EntityHeader* getEntityHeader() {
    return entity_header_;
  }

  inline void initAddr() {
    if (first_meta_) {
      entity_header_ = reinterpret_cast<EntityHeader*>((intptr_t) memAddr() + startLoc());
      entity_item_offset_ = reinterpret_cast<EntityItem*>((intptr_t) memAddr() + startLoc() + sizeof(EntityHeader));
    } else {
      entity_header_ = nullptr;
      entity_item_offset_ = nullptr;
    }
  }

  inline EntityItem* getEntityItem(uint entity_id) {
    assert(entity_id > 0);
    EntityItem* ent_ptr = &entity_item_offset_[(entity_id - 1) % entity_header_->max_entities_per_subgroup];
    return ent_ptr;
  }

  /**
  * @brief  search one free entityitem
  * 
  * @return 0: not found . >0 :search succese, and return is allocated entityid.
  */
  uint16_t addEntity() {
    MUTEX_LOCK(obj_mutex_);
    Defer defer{[&]() { MUTEX_UNLOCK(obj_mutex_); }};

    for (int i = entity_header_->cur_entity_id + 1; i <= entity_header_->max_entities_per_subgroup; i++) {
      EntityItem* entity_item = getEntityItem(i);
      if (entity_item->entity_id == 0) {
        entity_item->entity_id = i;
        entity_header_->cur_entity_id++;
        return i;
      }
    }
    return 0;
  }

  void deleteEntity(uint32_t entity_id) {
    EntityItem* entity_item = getEntityItem(entity_id);
    entity_item->is_deleted = true;
  }

  std::vector<uint32_t> getEntities() {
    MUTEX_LOCK(obj_mutex_);
    std::vector<uint32_t> entities;
    for (int i = 1; i <= entity_header_->max_entities_per_subgroup; i++) {
      EntityItem* entity_item = getEntityItem(i);
      if (entity_item->entity_id != 0) {
        entities.push_back(entity_item->entity_id);
      }
    }
    MUTEX_UNLOCK(obj_mutex_);
    return entities;
  }

  inline void* getBlockItemOffset() {
    return reinterpret_cast<void*>((intptr_t) memAddr() + block_item_offset_);
  }

  inline BlockItem* GetBlockItem(BLOCK_ID item_id) {
    assert(item_id > 0);
    return reinterpret_cast<BlockItem*>((intptr_t) getBlockItemOffset() +
                                        (item_id - block_item_offset_num_ - 1) * sizeof(BlockItem));
  }

  inline uint32_t GetMinBlockId() {
    return block_item_offset_num_ + 1;
  }

  inline uint32_t GetMaxBlockId() {
    return entity_header_->cur_block_id;
  }

  void to_string();

  void to_string(uint entity_id);

  void lock() { MUTEX_LOCK(obj_mutex_); }

  void unlock() { MUTEX_UNLOCK(obj_mutex_); }

  uint16_t getBlockAggNum() {
    return block_agg_num_;
  }

  int GetBlockItem(uint meta_block_id, uint item_id, BlockItem* blk_item);

  int64_t& minTimestamp() { return entity_header_->minTimestamp; }

  int64_t& maxTimestamp() { return entity_header_->maxTimestamp; }

  bool& isInitialized() { return entity_header_->is_initialized; }

  void SetEntityItemOffset(EntityItem* entity_item_offset) {
    entity_item_offset_ = entity_item_offset;
  };

  inline uint16_t GetConfigSubgroupEntities() {
    if (entity_header_) {
      return entity_header_->max_entities_per_subgroup;
    } else {
      return max_entities_per_subgroup_;
    }
  }
};
