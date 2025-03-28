// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
#include "mmap/mmap_entity_block_meta.h"

/**
 * @brief EntityBlockMetaManager used for managing entity block data in TsTimePartition.
 *        Entity block meta data store in  xxxx.meta.n files. n starts from 0, increasing.
 *        each meta file has fixed size, and cannot remap.
 *        if meta space not enough, new meta file created.
*/
class EntityBlockMetaManager {
 public:
  EntityBlockMetaManager();
  ~EntityBlockMetaManager();

  // opening meta files, and generating meta data struct in memory, using mmap mechanism.
  int Open(const string &file_path, const std::string &db_path, const string &tbl_sub_path, bool alloc_block_item);

  // mark entity deleted by entityMeta object.
  inline void deleteEntity(uint32_t entity_id) { return entity_block_metas_[0]->deleteEntity(entity_id);}

  // get entityItem object in current partition.
  inline EntityItem* getEntityItem(uint entity_id) const { return entity_block_metas_[0]->getEntityItem(entity_id);}

  inline int64_t& minTimestamp() { return entity_block_metas_[0]->minTimestamp(); }

  inline int64_t& maxTimestamp() { return entity_block_metas_[0]->maxTimestamp(); }

  inline bool& isInitialized() { return entity_block_metas_[0]->isInitialized(); }

  inline EntityHeader* getEntityHeader() { return entity_block_metas_[0]->getEntityHeader(); }

  BlockItem* GetBlockItem(BLOCK_ID item_id);

  void lock() { pthread_mutex_lock(&obj_mutex_); }
  void unlock() { pthread_mutex_unlock(&obj_mutex_);}

  inline uint64_t GetTableId() const { return table_id_; }

  // if current block item is full, new block item and add to entity.
  int AddBlockItem(uint entity_id, BlockItem** blk_item);

  // update  entity struct info with block item.
  void UpdateEntityItem(uint entity_id, BlockItem* blk_item);

  // sync data to file.
  int sync(int flags);

  std::vector<uint32_t> getEntities() { return entity_block_metas_[0]->getEntities(); }
  MMapEntityBlockMeta* GetFirstMeta() { return entity_block_metas_[0]; }

  int GetAllBlockItems(vector<uint32_t>& entity_ids, std::deque<BlockItem*>& block_items);

  /**
   * @brief	get all blockitem objects of entity.
   * @return	0 succeed, otherwise -1.
   */
  int GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse = false);

  int remove();
  void release();

  /**
 * @brief	 caculate block span start location in  partition row.
 *
 * @param in span
 * @return	0 succeed, otherwise -1.
 */
  MetricRowID GetFileStartRow(const BlockSpan& span) {
    return MetricRowID{span.block_item->block_id, span.start_row + 1};
  }

  inline int GetMetaIndex(BLOCK_ID block_item_id){
    int idx = (block_item_id -1) / META_BLOCK_ITEM_MAX;
    assert(idx < entity_block_metas_.size());
    assert(idx < meta_num_);
    assert(entity_block_metas_[idx] != nullptr);
    return idx;
  }

  inline uint32_t GetMetaFileNum() {
   return meta_num_;
  }

  /**
 * @brief	 mark blockspan rows deleted.
 *
 * @param in span 	  block span
 * @return	0 succeed, otherwise -1.
 */
  int MarkSpaceDeleted(uint32_t entity_id, BlockSpan* span);

  // resize vector space that storing meta file names.
  void resizeMeta();

  int64_t GetModifyTime();

  uint16_t max_entities_per_subgroup = 500;   // configure item: entity max num in subgroup

 protected:
  std::vector<MMapEntityBlockMeta*> entity_block_metas_;
  std::vector<MMapEntityBlockMeta*> entity_block_metas_swap_;
  uint64_t table_id_;
  std::string file_path_base_;
  std::string db_path_;
  std::string tbl_sub_path_;
  uint16_t block_null_bitmap_size_;
  pthread_mutex_t obj_mutex_;
  std::shared_mutex entity_block_item_mutex_;  // control entity item / block item
  uint32_t meta_num_ = 0;  // number of meta, not meta's amount
};