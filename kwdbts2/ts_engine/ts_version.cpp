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

#include "ts_version.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "ts_entity_segment.h"
#include "ts_filename.h"
#include "ts_io.h"

namespace kwdbts {
static const int64_t interval = 3600 * 24 * 10;  // 10 days.

// Note: we expect this function always return lower bound of both positive and negative timestamp
// e.g. interval = 3,
// timestamp = 0, return 0;
// timestamp = 1, return 0
// timestamp = 2, return 0
// timestamp = 3, return 3
// timestamp = -1, return -3
// timestamp = -2, return -3
// timestamp = -3, return -3
// timestamp = -4, return -6
static int64_t GetPartitionStartTime(timestamp64 timestamp) {
  bool negative = timestamp < 0;
  timestamp64 tmp = timestamp + negative;
  int64_t index = tmp / interval;
  index -= negative;
  return index * interval;
}

void TsVersionManager::AddPartition(DatabaseID dbid, timestamp64 ptime) {
  timestamp64 start = GetPartitionStartTime(ptime);
  PartitionIdentifier partition_id{dbid, start};
  if (partition_id == this->last_created_partition_) {
    return;
  }
  {
    auto current = Current();
    auto it = current->partitions_.find(partition_id);
    if (it != current->partitions_.end()) {
      last_created_partition_ = partition_id;
      return;
    }
  }
  // find partition again under exclusive lock
  std::unique_lock lk{mu_};
  auto it = current_->partitions_.find(partition_id);
  if (it != current_->partitions_.end()) {
    last_created_partition_ = partition_id;
    return;
  }

  // create new partition under exclusive lock
  auto new_version = std::make_unique<TsVGroupVersion>(*current_);
  std::unique_ptr<TsPartitionVersion> partition(new TsPartitionVersion{start, start + interval, partition_id});
  partition->valid_memseg_ = new_version->valid_memseg_;

  {
    // create directory for new partition
    // TODO(zzr): optimization: create the directory only when flushing
    // this logic is only for deletion and will be removed later after optimize delete
    std::filesystem::path db_path = options_.db_path;
    auto partition_dir = db_path / VGroupDirName(vgroup_id_) / PartitionDirName(partition_id);
    options_.io_env->NewDirectory(partition_dir);
    LOG_INFO("Partition directory created: %s", partition_dir.string().c_str());
    partition->directory_created_ = true;

    partition->del_info_ = std::make_shared<TsDelItemManager>(partition_dir);
    partition->del_info_->Open();
  }
  new_version->partitions_.insert({partition_id, std::move(partition)});

  // update current version
  current_ = std::move(new_version);
  last_created_partition_ = partition_id;
}

KStatus TsVersionManager::ApplyUpdate(const TsVersionUpdate &update) {
  if (update.empty) {
    // empty update, do nothing
    return SUCCESS;
  }
  TsIOEnv *env = options_.io_env;


  // TODO(zzr): thinking concurrency control carefully.
  std::unique_lock lk{mu_};

  // Create a new vgroup version based on current version
  auto new_vgroup_version = std::make_unique<TsVGroupVersion>(*current_);

  if (update.mem_segments_updated_) {
    new_vgroup_version->valid_memseg_ = std::make_shared<MemSegList>(std::move(update.valid_memseg_));
  }
  // looping over all partitions
  for (auto [par_id, par] : new_vgroup_version->partitions_) {
    auto new_partition_version = std::make_unique<TsPartitionVersion>(*par);
    if (update.mem_segments_updated_) {
      new_partition_version->valid_memseg_ = new_vgroup_version->valid_memseg_;
    }

    if (update.partitions_created_.find(par_id) != update.partitions_created_.end()) {
      new_partition_version->directory_created_ = true;
    }

    // Process lastsegment deletion, used by Compact()
    {
      auto it = update.delete_lastsegs_.find(par_id);
      if (it != update.delete_lastsegs_.end()) {
        std::vector<std::shared_ptr<TsLastSegment>> tmp;
        for (auto last_segment : new_partition_version->last_segments_) {
          if (it->second.find(last_segment->GetFileNumber()) == it->second.end()) {
            tmp.push_back(last_segment);
          } else {
            last_segment->MarkDelete();
          }
        }
        new_partition_version->last_segments_.swap(tmp);
      }
    }

    std::filesystem::path db_path = options_.db_path;
    auto partition_dir = db_path / VGroupDirName(vgroup_id_) / PartitionDirName(par_id);
    // Process lastsegment creation, used by Flush() and Compact()
    {
      // TODO(zzr): Lazy open? add something like `TsLastSegmentHandler` to do this.
      auto it = update.new_lastsegs_.find(par_id);
      if (it != update.new_lastsegs_.end()) {
        for (auto file_number : it->second) {
          std::unique_ptr<TsRandomReadFile> rfile;
          auto filepath = partition_dir / LastSegmentFileName(file_number);
          auto s = env->NewRandomReadFile(filepath, &rfile);
          if (s == FAIL) {
            return FAIL;
          }

          auto last_segment = TsLastSegment::Create(file_number, std::move(rfile));
          s = last_segment->Open();
          if (s == FAIL) {
            LOG_ERROR("can not open file %s", LastSegmentFileName(file_number).c_str());
            return FAIL;
          }
          // LOG_INFO("Load :%s", filepath.c_str());
          new_partition_version->last_segments_.push_back(std::move(last_segment));
        }
      }
    }

    // Process entity segment update, used by Compact()
    auto it = update.entity_segment_.find(par_id);
    if (it != update.entity_segment_.end()) {
      new_partition_version->entity_segment_ = it->second;
    }

    // VGroupVersion accepts the new partition version
    new_vgroup_version->partitions_[par_id] = std::move(new_partition_version);
  }

  current_ = std::move(new_vgroup_version);
  LOG_INFO("%s", update.DebugStr().c_str());
  return SUCCESS;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetPartitions(uint32_t target_dbid) const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    const auto &[dbid, _] = k;
    if (dbid == target_dbid) {
      result.push_back(v);
    }
  }
  return result;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetPartitionsToCompact() const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    if (v->NeedCompact()) {
      result.push_back(v);
    }
  }
  return result;
}

std::shared_ptr<const TsPartitionVersion> TsVGroupVersion::GetPartition(uint32_t target_dbid,
                                                                        timestamp64 target_time) const {
  timestamp64 start = GetPartitionStartTime(target_time);
  auto it = partitions_.find({target_dbid, start});
  if (it == partitions_.end()) {
    return nullptr;
  }
  return it->second;
}

std::vector<std::shared_ptr<TsLastSegment>> TsPartitionVersion::GetCompactLastSegments() const {
  // TODO(zzr): There is room for optimization
  // Maybe we can pre-compute which lastsegments can be compacted, and just return them.
  size_t compact_num = std::min<size_t>(last_segments_.size(), EngineOptions::max_compact_num);
  std::vector<std::shared_ptr<TsLastSegment>> result;
  result.reserve(compact_num);
  auto it = last_segments_.begin();
  for (int i = 0; i < compact_num; ++i, ++it) {
    result.push_back(*it);
  }
  return result;
}

std::list<std::shared_ptr<TsMemSegment>> TsPartitionVersion::GetAllMemSegments() const {
  if (valid_memseg_) {
    return *valid_memseg_;
  }
  return {};
}

std::vector<std::shared_ptr<TsSegmentBase>> TsPartitionVersion::GetAllSegments() const {
  std::vector<std::shared_ptr<TsSegmentBase>> result;
  auto mem_segs = GetAllMemSegments();

  result.reserve(mem_segs.size() + last_segments_.size() + 1);
  result.insert(result.end(), mem_segs.begin(), mem_segs.end());
  result.insert(result.end(), last_segments_.begin(), last_segments_.end());
  result.push_back(entity_segment_);
  return result;
}

KStatus TsPartitionVersion::DeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans,
                                       const KwLSNSpan &lsn) const {
  kwdbts::TsEntityDelItem del_item(ts_spans[0], lsn, e_id);
  for (auto &ts_span : ts_spans) {
    assert(ts_span.begin <= ts_span.end);
    del_item.range.ts_span = ts_span;
    auto s = del_info_->AddDelItem(e_id, del_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("AddDelItem failed. for entity[%lu]", e_id);
      return s;
    }
  }
  return KStatus::SUCCESS;
}
}  // namespace kwdbts
