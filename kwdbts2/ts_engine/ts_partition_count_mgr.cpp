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
#define ENTITY_COUNT_LATCH_BUCKET_NUM 10

TsPartitionEntityCountManager::TsPartitionEntityCountManager(std::string path) :
  path_(path), mmap_alloc_(path_) {}

TsPartitionEntityCountManager::~TsPartitionEntityCountManager() {
  mmap_alloc_.Close();
  if (delete_after_free) {
    unlink(path_.c_str());
  }
}

KStatus TsPartitionEntityCountManager::Open() {
  if (mmap_alloc_.Open() == KStatus::SUCCESS) {
    if (mmap_alloc_.GetAllocSize() >= sizeof(TsCountStatsFileHeader)) {
      header_ = reinterpret_cast<TsCountStatsFileHeader*>(mmap_alloc_.addr(mmap_alloc_.GetStartPos()));
    } else {
      auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsCountStatsFileHeader), 0);
      header_ = reinterpret_cast<TsCountStatsFileHeader*>(mmap_alloc_.addr(offset));
    }
    KStatus s = index_.Init(&mmap_alloc_, &(mmap_alloc_.getHeader()->index_header_offset));
    if (s == KStatus::SUCCESS) {
      return s;
    }
  }
  LOG_ERROR("count.stat open failed.");
  return KStatus::FAIL;
}

KStatus TsPartitionEntityCountManager::SetCountStatsHeader(uint8_t file_version, uint64_t version_num, TS_OSN max_osn) {
  header_->file_version = file_version;
  header_->version_num = version_num;
  header_->max_osn = max_osn;
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::updateEntityCount(TsEntityCountStats* header, TsEntityCountStats* info) {
  Defer defer([&]() {
    // update min ts and max ts
    if (info->min_ts < header->min_ts) {
      header->min_ts = info->min_ts;
    }
    if (info->max_ts > header->max_ts) {
      header->max_ts = info->max_ts;
    }
  });
  if (!header->is_count_valid) {
    // no need do anything.
    return KStatus::SUCCESS;
  } else if (!info->is_count_valid) {
    header->valid_count = 0;
    header->is_count_valid = false;
  } else if (EngineOptions::g_dedup_rule == DedupRule::KEEP) {
    header->valid_count += info->valid_count;
  } else {
    // ts span no cross with history ts span.
    if (info->min_ts > header->max_ts || info->max_ts < header->min_ts) {
      header->valid_count += info->valid_count;
    } else {
      // if ts span crossed, set count invalid.
      header->valid_count = 0;
      header->is_count_valid = false;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::AddEntityCountStats(TsEntityCountStats& info) {
  auto node = index_.GetIndexObject(info.entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", info.entity_id, path_.c_str());
    return KStatus::FAIL;
  }
  if (*node == INVALID_POSITION) {
    auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsEntityCountStats), 0);
    if (offset == INVALID_POSITION) {
      LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", info.entity_id, path_.c_str());
      return FAIL;
    }
    auto* stats = reinterpret_cast<TsEntityCountStats*>(mmap_alloc_.addr(offset));
    stats->min_ts = info.min_ts;
    stats->max_ts = info.max_ts;
    stats->table_id = info.table_id;
    stats->entity_id = info.entity_id;
    stats->valid_count = info.valid_count;
    stats->is_count_valid = info.is_count_valid;
    *node = offset;
    return KStatus::SUCCESS;
  }
  {
    auto* stats = reinterpret_cast<TsEntityCountStats*>(mmap_alloc_.addr(*node));
    assert(stats != nullptr);
    KStatus s = updateEntityCount(stats, &info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("updateEntityCount failed. entity [%lu] path [%s].", info.entity_id, path_.c_str());
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::SetEntityCountStats(TsEntityCountStats& info) {
  auto node = index_.GetIndexObject(info.entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", info.entity_id, path_.c_str());
    return KStatus::FAIL;
  }
  if (*node == INVALID_POSITION) {
    auto offset = mmap_alloc_.AllocateAssigned(sizeof(TsEntityCountStats), 0);
    if (offset == INVALID_POSITION) {
      LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", info.entity_id, path_.c_str());
      return FAIL;
    }
    *node = offset;
  }
  {
    auto* stats = reinterpret_cast<TsEntityCountStats*>(mmap_alloc_.addr(*node));
    assert(stats != nullptr);
    stats->min_ts = info.min_ts;
    stats->max_ts = info.max_ts;
    stats->table_id = info.table_id;
    stats->entity_id = info.entity_id;
    stats->valid_count = info.valid_count;
    stats->is_count_valid = info.is_count_valid;;
  }

  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::GetCountStatsHeader(TsCountStatsFileHeader& file_header) {
  file_header.file_version = header_->file_version;
  file_header.version_num = header_->version_num;
  file_header.max_osn = header_->max_osn;
  return KStatus::SUCCESS;
}

KStatus TsPartitionEntityCountManager::GetEntityCountStats(TsEntityCountStats& stats) {
  auto node = index_.GetIndexObject(stats.entity_id, false);
  if (node == nullptr) {
    // LOG_DEBUG("not found node from index file. entity [%lu] path [%s].", stats.entity_id, path_.c_str());
    stats.is_count_valid = true;
    stats.valid_count = 0;
    stats.min_ts = INVALID_TS;
    stats.max_ts = INVALID_TS;
    return KStatus::SUCCESS;
  }
  {
    TsEntityCountStats* header = reinterpret_cast<TsEntityCountStats*>(mmap_alloc_.addr(*node));
    assert(header != nullptr);
    stats.table_id = header->table_id;
    stats.min_ts = header->min_ts;
    stats.max_ts = header->max_ts;
    stats.valid_count = header->valid_count;
    stats.is_count_valid = header->is_count_valid;
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
