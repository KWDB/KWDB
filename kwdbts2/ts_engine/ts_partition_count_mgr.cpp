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

#include <assert.h>
#include <string>
#include "data_type.h"
#include "ts_partition_count_mgr.h"
#include "settings.h"

namespace kwdbts {
const int64_t INVALID_TS = INT64_MAX;

TsPartitionEntityCountManager::TsPartitionEntityCountManager(std::string path) :
  path_(path + "/" + COUNT_FILE_NAME), mmap_alloc_(path_) {
  rw_lock_ = new KRWLatch(RWLATCH_ID_MMAP_ENTITY_COUNT_MGR_RWLOCK);
}

TsPartitionEntityCountManager::~TsPartitionEntityCountManager() {
  if (rw_lock_) {
    delete rw_lock_;
  }
}

KStatus TsPartitionEntityCountManager::Open() {
  if (mmap_alloc_.Open() == KStatus::SUCCESS) {
    KStatus s = index_.Init(&mmap_alloc_, &(mmap_alloc_.getHeader()->index_header_offset));
    if (s == KStatus::SUCCESS) {
      return s;
    }
  }
  LOG_ERROR("entity_count.item open failed.");
  return KStatus::FAIL;
}

KStatus TsPartitionEntityCountManager::updateEntityCount(TsEntityCountHeader* header,
  TsEntityFlushInfo* info, bool update_ts) {
  if (!header->is_count_valid) {
    // no need do anything.
    return KStatus::SUCCESS;
  } else if (EngineOptions::g_dedup_rule == DedupRule::KEEP && update_ts) {
    header->valid_count += info->deduplicate_count;
  } else {
    // ts span no cross with history ts span.
    if (info->min_ts > header->max_ts || info->max_ts < header->min_ts || header->min_ts == INVALID_TS) {
      header->valid_count += info->deduplicate_count;
    } else {
      // if ts span crossed, set count invalid.
      header->valid_count = 0;
      header->is_count_valid = false;
    }
  }
  if (update_ts) {
    // update min ts and max ts
    if (info->min_ts < header->min_ts || header->min_ts == INVALID_TS) {
      header->min_ts = info->min_ts;
    }
    if (info->max_ts > header->max_ts || header->max_ts == INVALID_TS) {
      header->max_ts = info->max_ts;
    }
  }
  header->changed_aft_prepare = true;
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::AddFlushEntityAgg(TsEntityFlushInfo& info) {
  auto node = index_.GetIndexObject(info.entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu].", info.entity_id);
    return KStatus::FAIL;
  }
  {
    RW_LATCH_X_LOCK(rw_lock_);
    if (*node == INVALID_POSITION) {
      auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsEntityCountHeader), 0);
      if (offset == INVALID_POSITION) {
        LOG_ERROR("get node from index file failed. entity [%lu].", info.entity_id);
        RW_LATCH_UNLOCK(rw_lock_);
        return FAIL;
      }
      TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(offset));
      header->min_ts = INVALID_TS;
      header->max_ts = INVALID_TS;
      header->entity_id = info.entity_id;
      header->valid_count = 0;
      header->changed_aft_prepare = false;
      header->is_count_valid = true;
      *node = offset;
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  KStatus s = KStatus::SUCCESS;
  {
    RW_LATCH_X_LOCK(rw_lock_);  // TODO(zyc): HASH LOCK
    TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    s = updateEntityCount(header, &info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("updateEntityCount failed.");
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return s;
}

KStatus TsPartitionEntityCountManager::SetEntityCountInValid(TSEntityID e_id, const KwTsSpan& ts_range) {
  auto node = index_.GetIndexObject(e_id, false);
  if (node == nullptr) {
    LOG_DEBUG("no found entity[%lu] from %s. no need set invalid.", e_id, COUNT_FILE_NAME);
    return KStatus::SUCCESS;
  }
  KStatus s = KStatus::SUCCESS;
  TsEntityFlushInfo info;
  info.deduplicate_count = 0;
  info.entity_id = e_id;
  info.min_ts = ts_range.begin;
  info.max_ts = ts_range.end;
  {
    RW_LATCH_X_LOCK(rw_lock_);
    TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    s = updateEntityCount(header, &info, false);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("updateEntityCount failed.");
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return s;
}

KStatus TsPartitionEntityCountManager::PrepareEntityCountValid(TSEntityID e_id) {
  auto node = index_.GetIndexObject(e_id, false);
  if (node == nullptr) {
    LOG_DEBUG("no found entity[%lu] from %s. no need prepare valid.", e_id, COUNT_FILE_NAME);
    return KStatus::FAIL;
  }
  {
    RW_LATCH_X_LOCK(rw_lock_);
    TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    header->changed_aft_prepare = false;
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::SetEntityCountValid(TSEntityID e_id, TsEntityFlushInfo* info) {
  auto node = index_.GetIndexObject(e_id, false);
  if (node == nullptr) {
    LOG_DEBUG("no found entity[%lu] from %s. no need set valid.", e_id, COUNT_FILE_NAME);
    return KStatus::FAIL;
  }
  {
    RW_LATCH_X_LOCK(rw_lock_);
    TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    if (!header->changed_aft_prepare && !header->is_count_valid) {
      header->is_count_valid = true;
      header->min_ts = info->min_ts;
      header->max_ts = info->max_ts;
      header->valid_count = info->deduplicate_count;
      header->changed_aft_prepare = true;
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::GetEntityCountHeader(TsEntityCountHeader* count_header) {
  auto node = index_.GetIndexObject(count_header->entity_id, false);
  if (node == nullptr) {
    LOG_DEBUG("get node from index file. not found entity [%lu].", count_header->entity_id);
    count_header->is_count_valid = true;
    count_header->valid_count = 0;
    count_header->min_ts = INVALID_TS;
    count_header->max_ts = INVALID_TS;
    return KStatus::SUCCESS;
  }
  {
    RW_LATCH_S_LOCK(rw_lock_);
    TsEntityCountHeader* header = reinterpret_cast<TsEntityCountHeader*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    count_header->min_ts = header->min_ts;
    count_header->max_ts = header->max_ts;
    count_header->is_count_valid = header->is_count_valid;
    count_header->valid_count = header->valid_count;
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
