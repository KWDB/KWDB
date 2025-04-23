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


#include <thread>
#include "dirent.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "entity_block_meta_manager.h"
#include "utils/big_table_utils.h"
#include "st_config.h"
#include "sys_utils.h"

EntityBlockMetaManager::EntityBlockMetaManager() {
  pthread_mutex_init(&obj_mutex_, NULL);
  entity_block_metas_.resize(1000, nullptr);
}

EntityBlockMetaManager::~EntityBlockMetaManager() {
  for (auto& ptr : entity_block_metas_) {
    if (ptr) delete ptr;
  }
}

int EntityBlockMetaManager::Open(const string& file_path, const std::string& db_path, const string& tbl_sub_path,
                                 bool alloc_block_item) {
  meta_num_ = 0;  // reset meta number
  table_id_ = std::atoi(getTsObjectName(file_path).c_str());
  file_path_base_ = file_path;
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  //  After finding all the metas first, then use map to ensure the order,
  //  at least meta.0 should be the first, and then calculate the offset will not be wrong.
  std::map<uint64_t, string> meta_map;
  int err_code = 0;
  DIR* dir_ptr = opendir((db_path_ + tbl_sub_path).c_str());
  if (!dir_ptr) {
    return KWENOOBJ;
  }

  struct dirent* entry;
  while ((entry = readdir(dir_ptr)) != nullptr) {
    std::string full_path = db_path_ + tbl_sub_path + entry->d_name;
    struct stat file_stat{};
    if (stat(full_path.c_str(), &file_stat) != 0) {
      LOG_ERROR("stat[%s] failed", full_path.c_str());
      closedir(dir_ptr);
      return KWENFILE;
    }
    if (!S_ISREG(file_stat.st_mode)) {
      continue;
    }
    string meta_prefix = file_path + ".";
    //  entity->d_name: 78.meta.XX
    //  meta_prefix: 78.meta
    if (strncmp(entry->d_name, meta_prefix.c_str(), meta_prefix.length()) == 0) {
      char* endptr;
      errno = 0;
      uint64_t num = std::strtoull(entry->d_name + meta_prefix.length(), &endptr, 10);
      if ((errno == ERANGE && num == ULLONG_MAX) || (errno != 0 && num == 0)) {
        continue;
        // There is a file name that starts with "78.meta.", but it is not followed by a number, ignore it
      }
      meta_map[num] = entry->d_name;
    }
  }
  closedir(dir_ptr);

  if (meta_map.empty()) {
    // There is no meta file in the path, initialize and create a meta.0
    auto entity_meta = new MMapEntityBlockMeta(false, true);
    int flags = MMAP_CREAT_EXCL;
    err_code = entity_meta->init(file_path + ".0", db_path, tbl_sub_path, flags, alloc_block_item,
                                 max_entities_per_subgroup);
    if (err_code < 0) {
      return err_code;
    }
    entity_block_metas_[0] = entity_meta;
    meta_num_ = 1;
  } else if (meta_map.find(0) == meta_map.end()) {
    // There is a meta file but not meta.0
    return KWENOOBJ;
  } else {
    meta_num_ = meta_map.size();
    bool success = true;
    for (auto& it: meta_map) {
      const uint64_t& idx = it.first;
      string& file_name = it.second;
      int flags = MMAP_OPEN_NORECURSIVE;
      auto entity_meta = new MMapEntityBlockMeta(false, idx == 0);
      err_code = entity_meta->init(file_name, db_path, tbl_sub_path, flags, alloc_block_item, max_entities_per_subgroup);
      if (err_code < 0) {
        success = false;
        break;
      }
      if (idx != 0) {
        entity_meta->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
        // We will take entity_metas_[0] to get offset here, so be sure to do it in order
        entity_meta->block_item_offset_num_ = idx * META_BLOCK_ITEM_MAX;
      }
      resizeMeta();
      // Since num is not necessarily continuous, we should check whether resize is required first,
      // otherwise, the num is too large and the read entity_metas_ will crash
      entity_block_metas_[idx] = entity_meta;
    }
    if (!success) {
      for (auto& ptr: entity_block_metas_) {
        if (ptr) delete ptr;
      }
      return err_code;
    }
  }

  if (entity_block_metas_[0]->entity_header_->version != METRIC_VERSION) {
    LOG_ERROR("The [%s] versions don't match: Code Metric Version[%d], Data Metric Version[%d]",
              file_path.c_str(), METRIC_VERSION, entity_block_metas_[0]->entity_header_->version);
    char* version_validate = getenv("KW_DATA_VERSION_VALIDATE");
    if (version_validate != nullptr && strcmp(version_validate, "true") == 0) {
      exit(1);
    }
  }
  return 0;
}

BlockItem* EntityBlockMetaManager::GetBlockItem(BLOCK_ID item_id) {
  return entity_block_metas_.at(GetMetaIndex(item_id))->GetBlockItem(item_id);
}

int EntityBlockMetaManager::sync(int flags) {
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    if (!entity_block_metas_[i]) {
      break;
    }
    int error_code = entity_block_metas_[i]->sync(flags);
    if (error_code < 0) return error_code;
  }

  return 0;
}

int EntityBlockMetaManager::GetAllBlockItems(vector<uint32_t>& entity_ids, std::deque<BlockItem*>& block_items) {
  if (entity_ids.empty()) {
    BLOCK_ID max_block_id = getEntityHeader()->cur_block_id;
    for (uint32_t id = 1; id <= max_block_id; ++id) {
      block_items.emplace_back(GetBlockItem(id));
    }
  } else {
    for (auto entity_id : entity_ids) {
      GetAllBlockItems(entity_id, block_items, false);
    }
  }
  return 0;
}

int EntityBlockMetaManager::GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  while (block_item_id != 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item == nullptr) {
      LOG_ERROR("GetBlockItem error: block item is nullptr, entity id[%u], block id[%u]", entity_id, block_item_id)
      return KWENOOBJ;
    }
    if (block_item->entity_id != entity_id) {
      LOG_ERROR("entity id in block item is inconsistent: entity_id[%u], block_item->entity_id[%u]",
                entity_id, block_item->entity_id)
      return KWENOOBJ;
    }
    reverse ? block_items.push_back(block_item) : block_items.push_front(block_item);
    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

int EntityBlockMetaManager::GetCountBlockItems(uint32_t entity_id, BLOCK_ID block_id, std::deque<BlockItem*>& block_items) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  while (block_item_id != 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item == nullptr) {
      LOG_ERROR("GetBlockItem error: block item is nullptr, entity id[%u], block id[%u]", entity_id, block_item_id)
      return KWENOOBJ;
    }
    if (block_item->entity_id != entity_id) {
      LOG_ERROR("entity id in block item is inconsistent: entity_id[%u], block_item->entity_id[%u]",
                entity_id, block_item->entity_id)
      return KWENOOBJ;
    }
    if (block_item_id <= block_id) {
      break;
    }
    block_items.push_front(block_item);
    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

// delete meta files and structs in memory.
int EntityBlockMetaManager::remove() {
  int error_code = 0;
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_metas_[i]) {
      error_code = entity_block_metas_[i]->remove();
      if (error_code < 0) return -1;
      delete entity_block_metas_[i];
    }
  }
  entity_block_metas_.clear();
  return error_code;
}

void EntityBlockMetaManager::release() {
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_metas_[i]) delete entity_block_metas_[i];
  }
  entity_block_metas_.clear();
}

int EntityBlockMetaManager::MarkSpaceDeleted(uint32_t entity_id, BlockSpan* span) {
  std::lock_guard<std::shared_mutex> lk(entity_block_item_mutex_);
  BlockItem* block_item = span->block_item;
  assert(block_item != nullptr);
  if (hasDeleted(block_item->rows_delete_flags, span->start_row + 1, span->row_num)) {
    for (int i = 0; i < span->row_num; i++) {
      bool is_deleted = false;
      if (block_item->isDeleted(span->start_row + i + 1, &is_deleted) == 0) {
        if (!is_deleted) {
          block_item->setDeleted(span->start_row + i + 1);
        }
      }
    }
  } else {
    setBatchDeleted(block_item->rows_delete_flags, span->start_row + 1, span->row_num);
  }
  return 0;
}

void EntityBlockMetaManager::resizeMeta() {
  if (meta_num_ > entity_block_metas_.size() * 0.8) {
    lock();
    entity_block_metas_swap_ = entity_block_metas_;
    entity_block_metas_swap_.resize(meta_num_ * 2);
    entity_block_metas_.swap(entity_block_metas_swap_);
    unlock();
  }
}

int EntityBlockMetaManager::AddBlockItem(uint entity_id, BlockItem** blk_item) {
  EntityHeader* header = getEntityHeader();
  BLOCK_ID new_block_item_id = header->cur_block_id + 1;

  int err_code = 0;
  if (new_block_item_id > meta_num_ * META_BLOCK_ITEM_MAX) {
    // meta file has no space to store a new block item, we need create new meta file.
    int i = (new_block_item_id - 1) / META_BLOCK_ITEM_MAX;
    // The meta number may not be consecutive, and should correspond to the largest block_id
    std::string meta_file = file_path_base_ + "." + std::to_string(i);

    MMapEntityBlockMeta* entity_block_meta = new MMapEntityBlockMeta(false, false);
    err_code = entity_block_meta->init(meta_file, db_path_, tbl_sub_path_, MMAP_CREAT_EXCL, true, max_entities_per_subgroup);
    if (err_code < 0) {
      LOG_ERROR("Create meta file %s fail. error_code: %d", meta_file.c_str(), err_code);
      pthread_mutex_unlock(&obj_mutex_);
      return err_code;
    }

    entity_block_meta->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
    entity_block_meta->block_item_offset_num_ = i * META_BLOCK_ITEM_MAX;
    entity_block_metas_[i] = entity_block_meta;
    meta_num_ = i + 1;
    resizeMeta();
  }

  header->cur_block_id++;
  auto entity_item = getEntityItem(entity_id);
  BLOCK_ID latest_block_item_id = entity_item->cur_block_id;

  // get new block item from meta file,and initialize it.
  auto block_item = GetBlockItem(new_block_item_id);
  memset(block_item, 0, sizeof(BlockItem));
  block_item->prev_block_id = latest_block_item_id;
  block_item->entity_id = entity_id;
  block_item->block_id = new_block_item_id;

  *blk_item = block_item;
  return err_code;
}

void EntityBlockMetaManager::UpdateEntityItem(uint entity_id, BlockItem* blk_item) {
  auto entity_item = getEntityItem(entity_id);
  entity_item->block_count++;
  entity_item->cur_block_id = blk_item->block_id;
}

int64_t EntityBlockMetaManager::GetModifyTime() {
  int64_t newest_modify_time = INT64_MIN;
  for (auto meta : entity_block_metas_) {
    if (meta == nullptr) {
      break;
    }
    auto meta_file_path = meta->realFilePath();
    int64_t modify_time = ModifyTime(meta_file_path);
    if (newest_modify_time < modify_time) {
      newest_modify_time = modify_time;
    }
  }
  return newest_modify_time;
}
