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
#include <string>
#include <list>
#include "libkwdbts2.h"
#include "ts_io.h"
#include "ts_const.h"
#include "ts_file_vector_index.h"
#include "ts_hash_latch.h"

namespace kwdbts {

const char PARTTITON_META_FILE_NAME[] = "entity_meta.item";

struct TsPartitionMetaHeader {
  uint64_t min_osn;
  uint64_t max_osn;
  uint64_t max_entity_id;
  char reserverd[104];
};
static_assert(sizeof(TsPartitionMetaHeader) == 128, "wrong size of TsPartitionMetaHeader.");


struct TsPartitionEntityMetaHeader {
  uint64_t entity_id;
  timestamp64 min_ts;
  timestamp64 max_ts;
  uint64_t min_osn;
  uint64_t max_osn;
  char reserverd[88];
};
static_assert(sizeof(TsPartitionEntityMetaHeader) == 128, "wrong size of TsPartitionEntityMetaHeader.");

class TsPartitionEntityMetaManager {
 private:
  std::string path_;
  TsMMapAllocFile mmap_alloc_;
  // get offset of first index node.
  VectorIndexForFile<uint64_t> index_;
  KRWLatch* rw_lock_{nullptr};
  TsPartitionMetaHeader* header_;

 public:
  explicit TsPartitionEntityMetaManager(std::string path) :
  path_(path + "/" + PARTTITON_META_FILE_NAME), mmap_alloc_(path_) {
    rw_lock_ = new KRWLatch(RWLATCH_ID_MMAP_PARTITION_META_RWLOCK);
  }

  ~TsPartitionEntityMetaManager() {
    if (rw_lock_) {
      delete rw_lock_;
    }
  }

  KStatus Open() {
    if (mmap_alloc_.Open() == KStatus::SUCCESS) {
      if (mmap_alloc_.GetAllocSize() >= sizeof(TsPartitionMetaHeader)) {
      header_ = reinterpret_cast<TsPartitionMetaHeader*>(mmap_alloc_.addr(mmap_alloc_.GetStartPos()));
      } else {
        auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsPartitionMetaHeader), 0);
        header_ = reinterpret_cast<TsPartitionMetaHeader*>(mmap_alloc_.addr(offset));
        header_->min_osn = INVALID_OSN;
        header_->max_osn = INVALID_OSN;
        header_->max_entity_id = 0;
      }
      KStatus s = index_.Init(&mmap_alloc_, &(mmap_alloc_.getHeader()->index_header_offset));
      if (s == KStatus::SUCCESS) {
        return s;
      }
    }
    LOG_ERROR("entity_count.item open failed.");
    return KStatus::FAIL;
  }

  TsPartitionEntityMetaHeader* GetEntityMeta(TSEntityID e_id, bool create_if_no_exist) {
    auto node = index_.GetIndexObject(e_id, create_if_no_exist);
    if (node == nullptr) {
      LOG_ERROR("get node from index file failed. entity [%lu].", e_id);
      return nullptr;
    }
    bool new_header = false;
    if (*node == INVALID_POSITION) {
      auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsPartitionEntityMetaHeader), 0);
      if (offset == INVALID_POSITION) {
        LOG_ERROR("get node from index file failed. entity [%lu].", e_id);
        return nullptr;
      }
      *node = offset;
      new_header = true;
    }
    TsPartitionEntityMetaHeader* header = reinterpret_cast<TsPartitionEntityMetaHeader*>(mmap_alloc_.addr(*node));
    if (new_header) {
      header->min_ts = INVALID_TS;
      header->max_ts = INVALID_TS;
      header->entity_id = e_id;
      header->min_osn = INVALID_OSN;
      header->max_osn = INVALID_OSN;
    }
    return header;
  }

  void UpdateOSNRange(TSEntityID entity_id, uint64_t osn) {
    auto entity_header = GetEntityMeta(entity_id, true);
    assert(entity_header != nullptr);
    RW_LATCH_X_LOCK(rw_lock_);
    if (header_->max_osn < osn || header_->max_osn == INVALID_OSN) {
      header_->max_osn = osn;
    }
    if (header_->min_osn > osn || header_->min_osn == INVALID_OSN) {
      header_->min_osn = osn;
    }
    if (header_->max_entity_id < entity_id) {
      header_->max_entity_id = entity_id;
    }
    if (entity_header->max_osn < osn || entity_header->max_osn == INVALID_OSN) {
      entity_header->max_osn = osn;
    }
    if (entity_header->min_osn > osn || entity_header->min_osn == INVALID_OSN) {
      entity_header->min_osn = osn;
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }

  void UpdateTsRange(TSEntityID entity_id, timestamp64 ts) {
    auto entity_header = GetEntityMeta(entity_id, true);
    assert(entity_header != nullptr);
    RW_LATCH_X_LOCK(rw_lock_);
    if (entity_header->min_ts > ts || entity_header->min_ts == INVALID_TS) {
      entity_header->min_ts = ts;
    }
    if (entity_header->max_ts > ts || entity_header->max_ts == INVALID_TS) {
      entity_header->max_ts = ts;
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  KStatus Reset();
};


}  // namespace kwdbts
