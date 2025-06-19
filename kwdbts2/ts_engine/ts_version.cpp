#include "ts_version.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_entity_segment.h"

namespace kwdbts {
static const int64_t interval = 3600 * 24 * 30;  // 30 days.

static int64_t GetPartitionStartTime(timestamp64 timestamp) {
  int64_t index = timestamp / interval;
  index = timestamp < 0 ? index - 1 : index;
  return index * interval;
}

void TsVersionManager::AddPartition(DatabaseID dbid, timestamp64 ptime) {
  auto current = Current();
  timestamp64 start = GetPartitionStartTime(ptime);
  auto it = current->partitions_.find({dbid, start});
  if (it != current->partitions_.end()) {
    return;
  }

  PartitionIdentifier partition_id{dbid, start};
  std::unique_ptr<TsPartitionVersion> partition(
      new TsPartitionVersion{start, start + interval, partition_id});
  auto new_version = std::make_unique<TsVGroupVersion>(*current);
  new_version->partitions_.insert({partition_id, std::move(partition)});

  std::unique_lock lk{mu_};
  // find partition again under unique lock
  it = current_->partitions_.find(partition_id);
  if (it != current_->partitions_.end()) {
    return;
  }

  // update current version
  current_ = std::move(new_version);
}

KStatus TsVersionManager::ApplyUpdate(const TsVersionUpdate &update) {
  if (update.empty_) {
    return SUCCESS;
  }

  TsIOEnv *env = options_.io_env;


  // TODO(zzr): thinking concurrency control carefully.
  std::unique_lock lk{mu_};

  // Create a new vgroup version based on current version
  auto new_vgroup_version = std::make_unique<TsVGroupVersion>(*current_);

  // looping over all updated partitions
  for (auto par : update.updated_partitions_) {
    auto partition_iter = current_->partitions_.find(par);
    if (partition_iter == current_->partitions_.end()) {
      LOG_ERROR("ApplyUpdate failed, partition not found");
      return FAIL;
    }
    auto [database_id, start] = partition_iter->first;
    auto new_partition_version = std::make_unique<TsPartitionVersion>(*partition_iter->second);
    std::filesystem::path db_path = options_.db_path;
    auto partition_dir = db_path / VGroupDirName(vgroup_id_) / PartitionDirName(database_id, start);

    if (update.partitions_to_create_.find(par) != update.partitions_to_create_.end()) {
      new_partition_version->directory_created_ = true;
    }

    // Process lastsegment deletion, used by Compact()
    {
      auto it = update.delete_lastsegs_.find(par);
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

    // Process lastsegment creation, used by Flush() and Compact()
    {
      // TODO(zzr): Lazy open? add something like `TsLastSegmentHandler` to do this.
      auto it = update.new_lastsegs_.find(par);
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
          LOG_INFO("Load :%s", filepath.c_str());
          new_partition_version->last_segments_.push_back(std::move(last_segment));
        }
      }
    }

    { new_partition_version->mem_segments_ = update.valid_memseg_; }


    new_vgroup_version->partitions_[par] = std::move(new_partition_version);
  }

  current_ = std::move(new_vgroup_version);
  return SUCCESS;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetAllPartitions() const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    result.push_back(v);
  }
  return result;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetAllPartitions(
    uint32_t target_dbid) const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    const auto &[dbid, _] = k;
    if (dbid == target_dbid) {
      result.push_back(v);
    }
  }
  return result;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetPartitionsToCompact()
    const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    if (v->NeedCompact()) {
      result.push_back(v);
    }
  }
  return result;
}

std::shared_ptr<const TsPartitionVersion> TsVGroupVersion::GetPartition(
    uint32_t target_dbid, timestamp64 target_time) const {
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

std::vector<std::shared_ptr<TsSegmentBase>> TsPartitionVersion::GetAllSegments() const {
  std::vector<std::shared_ptr<TsSegmentBase>> result;
  result.reserve(mem_segments_.size() + last_segments_.size() + 1);
  result.insert(result.end(), mem_segments_.begin(), mem_segments_.end());
  result.insert(result.end(), last_segments_.begin(), last_segments_.end());
  result.push_back(entity_segment_);
  return result;
}
}  // namespace kwdbts