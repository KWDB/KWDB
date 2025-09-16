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

#include <thread>
#include "utils/big_table_utils.h"
#include <mmap/mmap_entity_block_meta.h>
#include <mmap/mmap_segment_table.h>
#include "utils/string_utils.h"
#include "ts_common.h"
#include "st_config.h"
#include "sys_utils.h"

size_t META_BLOCK_ITEM_MAX = 1000;  // max blockitem in one meta file

MMapEntityBlockMeta::MMapEntityBlockMeta(): MMapFile() {
  obj_mutex_ = new MMapEntityMetaLatch(LATCH_ID_MMAP_ENTITY_META_MUTEX);
  entity_blockitem_mutex_ = new MMapEntityMetaRWLatch(RWLATCH_ID_MMAP_ENTITY_META_RWLOCK);
}

MMapEntityBlockMeta::MMapEntityBlockMeta(bool is_et, bool first_meta): MMapFile() {
  is_et_ = is_et;
  first_meta_ = first_meta;
  obj_mutex_ = new MMapEntityMetaLatch(LATCH_ID_MMAP_ENTITY_META_MUTEX);
  entity_blockitem_mutex_ = new MMapEntityMetaRWLatch(RWLATCH_ID_MMAP_ENTITY_META_RWLOCK);
}

MMapEntityBlockMeta::~MMapEntityBlockMeta() {
  if (obj_mutex_) {
    delete obj_mutex_;
    obj_mutex_ = nullptr;
  }
  if (entity_blockitem_mutex_) {
    delete entity_blockitem_mutex_;
    entity_blockitem_mutex_ = nullptr;
  }
}


int MMapEntityBlockMeta::init(const string &file_path, const std::string &db_path, const string &tbl_sub_path,
                              int flags, bool alloc_block_item, uint16_t config_subgroup_entities) {
  int err_code = MMapFile::open(file_path, db_path + tbl_sub_path + file_path, flags);
  if (err_code < 0)
    return err_code;

  bool new_file = false;
  size_t header_new_len = startLoc();

  uint16_t max_entities = config_subgroup_entities;
  if (fileLen() > 0) {
    initAddr();
    if (entity_header_) {  // adapt history files
      max_entities = entity_header_->max_entities_per_subgroup == 0 ?
                     max_entities : entity_header_->max_entities_per_subgroup;
    }
  }

  if (first_meta_) {
    header_new_len += sizeof(EntityHeader) + sizeof(EntityItem) * max_entities;
  }
  block_item_offset_ = getPageOffset(header_new_len);

  if (fileLen() <= 0) {
    size_t new_len = block_item_offset_;
    if (alloc_block_item) {
      new_len += META_BLOCK_ITEM_MAX * sizeof(BlockItem);
    }

    err_code = mremap(getPageOffset(new_len));
    if (err_code < 0) {
      Remove(db_path + tbl_sub_path + file_path);
      return err_code;
    }
    size() = block_item_offset_;  // skip size & first element.
    new_file = true;
  }
  if (size() > fileLen()) {  // meta file is corrupted.
    return KWECORR;
  }

  initAddr();

  if (entity_header_ && entity_header_->max_entities_per_subgroup == 0) {
    entity_header_->max_entities_per_subgroup = max_entities;
  }

  if (new_file && first_meta_) {
    for (int i = 1; i <= entity_header_->max_entities_per_subgroup; i++) {
      EntityItem* entityItem = getEntityItem(i);
      entityItem->max_ts = INVALID_TS;
      entityItem->min_ts = INVALID_TS;
    }
    entity_header_->cur_datafile_id = 0;
    entity_header_->version = METRIC_VERSION;
  }

  max_entities_per_subgroup_ = max_entities;

  return 0;
}

int MMapEntityBlockMeta::incSize_(size_t len) {
  if (len > static_cast<size_t>(fileLen()) - size()) {
    return -1;
  }
  size() += len;
  return 0;
}

void MMapEntityBlockMeta::to_string() {
  printf("Meta File Size:%ld\n", size());
  if (!first_meta_) return;
  printf("=== EntityItem Begin ===\n");
  for (int i = 1; i <= entity_header_->max_entities_per_subgroup; i++) {
    auto entityItem = getEntityItem(i);
    entityItem->to_string(std::cout);
  }
  printf("=== EntityItem End ===\n");

  printf("=== BlockItem Begin ===\n");
  for (int i = 1; i <= getEntityHeader()->cur_block_id; i++) {
    auto blockItem = GetBlockItem(i);
    blockItem->to_string(std::cout);
  }
  printf("=== BlockItem End ===\n");
}

void MMapEntityBlockMeta::to_string(uint entity_id) {
  assert(entity_id > 0);

  printf("Meta File Size:%ld\n", size());
  if (!first_meta_) return;
  printf("=== EntityItem Begin ===\n");
  for (int i = 1; i <= entity_header_->max_entities_per_subgroup; i++) {
    auto entityItem = getEntityItem(i);
    if (entityItem->entity_id == entity_id) {
      entityItem->to_string(std::cout);
    }
  }
  printf("=== EntityItem End ===\n");

  printf("=== BlockItem Begin ===\n");
  for (int i=1; i<= getEntityHeader()->cur_block_id && i <= META_BLOCK_ITEM_MAX; i++) {
    auto blockItem = GetBlockItem(i);
    if (blockItem->entity_id == entity_id) {
      blockItem->to_string(std::cout);
    }
  }
  printf("=== BlockItem End ===\n");
}

int MMapEntityBlockMeta::GetBlockItem(uint meta_block_id, uint item_id, BlockItem* blk_item) {
  RW_LATCH_S_LOCK(entity_blockitem_mutex_);
  BlockItem* b_item = GetBlockItem(item_id);
  *blk_item = *b_item;
  RW_LATCH_UNLOCK(entity_blockitem_mutex_);
  return 0;
}

bool isAllNull(char* bitmap, size_t start_row, size_t rows_count) {
  size_t byte_start = (start_row - 1) >> 3;
  size_t bit_start = (start_row - 1) & 7;
  size_t byte_end = (start_row - 1 + rows_count - 1) >> 3;
  size_t bit_end = (start_row - 1 + rows_count - 1) & 7;
  uint8_t del_flag = 0;
  // in same byte
  if (byte_start == byte_end) {
    del_flag = 0;
    for (size_t i = bit_start; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((bitmap[byte_start] >> bit_start) & del_flag) != del_flag) {
      return false;
    }
    return true;
  }

  size_t bytes_start = byte_start + 1;
  size_t bytes_length = byte_end - byte_start - 1;
  // check first byte
  if (bit_start == 0) {
    bytes_start = byte_start;
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = bit_start; i < 8; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((bitmap[byte_start] >> bit_start) & del_flag) != del_flag) {
      return false;
    }
  }
  // check last byte
  if (bit_end == 7) {
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = 0; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if ((bitmap[byte_end] & del_flag) != del_flag) {
      return false;
    }
  }
  // check other bytes
  for (size_t i = 0; i < bytes_length; i++) {
    if (bitmap[bytes_start + i] != static_cast<char>(0xFF)) {
      return false;
    }
  }
  return true;
}
