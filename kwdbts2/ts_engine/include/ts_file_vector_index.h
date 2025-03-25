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

#include <vector>
#include <cmath>
#include "ts_common.h"

namespace kwdbts {

#define INVALID_POSITION          0
#define NUM_PER_INDEX_BLOCK       1000
#define INDIRECT_INDEX_LEVEL_MAX  4           // Max entity number can be 1G
#define INDIRECT_INDEX_LEVEL_RATIO 0.1

class FileWithIndex {
 public:
  virtual uint64_t AllocateAssigned(size_t size, uint8_t fill_number) = 0;
  virtual char* GetAddrForOffset(uint64_t offset, uint32_t reading_bytes) = 0;
};

/**
 *  first node for file index struct.
 * 
 *  direct1,direct2,,,direct900,indirect1,indirect2,,,indirect90,indirect2_1,indirect2_2,,,indirect2_10
 * 
 */
template<typename T>
class VectorIndexForFile {
 private:
  struct IndexLevelInfo {
    uint32_t offset_in_first_node;
    uint32_t num_in_first_node;
    uint32_t level;
    uint32_t obj_num;
    uint64_t start_id;
    uint64_t end_id;
  };
  FileWithIndex* file_;
  uint64_t start_offset_;
  std::vector<IndexLevelInfo> level_;
  std::mutex mutex_;

 public:
  explicit VectorIndexForFile(FileWithIndex* file, uint64_t offset = INVALID_POSITION) :
    file_(file), start_offset_(offset) {
    initLevelInfo();
  }

  void initLevelInfo() {
    level_.resize(INDIRECT_INDEX_LEVEL_MAX);
    for (size_t i = 0; i < INDIRECT_INDEX_LEVEL_MAX; i++) {
      level_[i].level = i;
      level_[i].num_in_first_node = NUM_PER_INDEX_BLOCK * pow(INDIRECT_INDEX_LEVEL_RATIO, i);
      if (i != INDIRECT_INDEX_LEVEL_MAX - 1) {
        level_[i].num_in_first_node -= NUM_PER_INDEX_BLOCK * pow(INDIRECT_INDEX_LEVEL_RATIO, i + 1);
      }
      if (i == 0) {
        level_[i].start_id = 0;
      } else {
        level_[i].start_id = level_[i - 1].end_id + 1;
      }
      level_[i].end_id = level_[i].start_id + level_[i].num_in_first_node * pow(NUM_PER_INDEX_BLOCK, i) - 1;
      level_[i].offset_in_first_node =
        (NUM_PER_INDEX_BLOCK - NUM_PER_INDEX_BLOCK * pow(INDIRECT_INDEX_LEVEL_RATIO, i)) * sizeof(uint64_t);
    }
  }

  uint64_t GetMaxId() {
    return level_.back().end_id;
  }

  uint64_t newNode(uint64_t offset, uint32_t node_length) {
    mutex_.lock();
    Defer defer([&]() { mutex_.unlock(); });
    auto node_ref = file_->GetAddrForOffset(offset, sizeof(uint64_t));
    if (node_ref == nullptr) {
      LOG_ERROR("can not get data from file postition[%lu]", offset);
      return INVALID_POSITION;
    }
    uint64_t& new_node_offset = KUint64(node_ref);
    if (new_node_offset == INVALID_POSITION) {
      auto offset = file_->AllocateAssigned(node_length, 0);
      if (offset == INVALID_POSITION) {
        LOG_ERROR("allocate space for node from file failed.[%lu]", sizeof(uint64_t) * NUM_PER_INDEX_BLOCK);
        return INVALID_POSITION;
      }
      new_node_offset = offset;
    }
    return new_node_offset;
  }

  T* GetIndexObject(uint32_t obj_idx, bool create_if_noexist) {
    if (start_offset_ == INVALID_POSITION) {
      if (!create_if_noexist) {
        return nullptr;
      }
      mutex_.lock();
      Defer defer([&]() { mutex_.unlock(); });
      if (start_offset_ == INVALID_POSITION) {
        auto offset = file_->AllocateAssigned(sizeof(uint64_t) * NUM_PER_INDEX_BLOCK, 0);
        if (offset == INVALID_POSITION) {
          LOG_ERROR("allocate space for node from file failed.[%lu]", sizeof(uint64_t) * NUM_PER_INDEX_BLOCK);
          return INVALID_POSITION;
        }
        start_offset_ = offset;
      }
    }
    for (size_t i = 0; i < INDIRECT_INDEX_LEVEL_MAX; i++) {
      if (level_[i].end_id >= obj_idx) {
        auto level_offset_in_file = start_offset_ + level_[i].offset_in_first_node;
        auto level_idx = obj_idx - level_[i].start_id;
        uint32_t num_per_position = pow(NUM_PER_INDEX_BLOCK, level_[i].level);
        char* next_level_node;
        uint64_t next_node_offset;
        for (size_t j = 0; j <= level_[i].level; j++) {
          auto idx_in_cur_level = level_idx / num_per_position;
          auto idx_in_next_level = level_idx % num_per_position;
          next_node_offset = level_offset_in_file + idx_in_cur_level * sizeof(uint64_t);
          next_level_node = file_->GetAddrForOffset(next_node_offset, sizeof(uint64_t));
          if (next_level_node == nullptr) {
            LOG_ERROR("can not get data from file postition[%lu]", next_node_offset);
            return nullptr;
          }
          level_offset_in_file = KUint64(next_level_node);
          // if not leaf node.
          if (level_[i].level - j > 0) {
            if (level_offset_in_file == INVALID_POSITION) {
              if (!create_if_noexist) {
                return nullptr;
              }
              auto offset = newNode(next_node_offset, sizeof(uint64_t) * NUM_PER_INDEX_BLOCK);
              if (offset == INVALID_POSITION) {
                LOG_ERROR("allocate level node failed.");
                return nullptr;
              }
              level_offset_in_file = offset;
            }
          }
          level_idx = idx_in_next_level;
          num_per_position = num_per_position / NUM_PER_INDEX_BLOCK;
        }
        // if leaf node no exist.
        if (level_offset_in_file == INVALID_POSITION) {
          if (!create_if_noexist) {
            return nullptr;
          }
          auto offset = newNode(next_node_offset, sizeof(T));
          if (offset == INVALID_POSITION) {
            LOG_ERROR("allocate leaf node from file failed.");
            return nullptr;
          }
          level_offset_in_file = offset;
        }
        char* object_addr = file_->GetAddrForOffset(level_offset_in_file, sizeof(T));
        if (object_addr == nullptr) {
          LOG_ERROR("can not get data from file postition[%lu]", next_node_offset);
          return nullptr;
        }
        return reinterpret_cast<T*>(object_addr);
      }
    }
    if (create_if_noexist) {
      LOG_ERROR("cannot create object for id[%u]", obj_idx);
    }
    return nullptr;
  }
};


}  // namespace kwdbts
