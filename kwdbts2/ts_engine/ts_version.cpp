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
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "lg_impl.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lru_block_cache.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {
// static const int64_t interval = 3600 * 24 * 10;  // 10 days.
static const int64_t vacuum_minutes = 24 * 60;  // 24 hours.
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
static int64_t GetPartitionStartTime(timestamp64 timestamp, int64_t ts_interval) {
  bool negative = timestamp < 0;
  timestamp64 tmp = timestamp + negative;
  int64_t index = tmp / ts_interval;
  index -= negative;
  return index * ts_interval;
}

KStatus TsVersionManager::AddPartition(DatabaseID dbid, timestamp64 ptime) {
  int64_t interval = PartitionIntervalRecorder::GetInstance()->GetInterval(dbid);
  timestamp64 start = GetPartitionStartTime(ptime, interval);
  PartitionIdentifier partition_id{dbid, start, start + interval};
  if (partition_id == this->last_created_partition_) {
    return KStatus::SUCCESS;
  }
  {
    auto current = Current();
    assert(current!= nullptr);
    auto it = current->partitions_.find(partition_id);
    if (it != current->partitions_.end()) {
      last_created_partition_ = partition_id;
      return KStatus::SUCCESS;
    }
  }
  // find partition again under exclusive lock
  std::unique_lock lk{mu_};
  auto it = current_->partitions_.find(partition_id);
  if (it != current_->partitions_.end()) {
    last_created_partition_ = partition_id;
    return KStatus::SUCCESS;
  }

  // create new partition under exclusive lock
  auto partition_dir = root_path_ / PartitionDirName(partition_id);
  auto new_version = std::make_unique<TsVGroupVersion>(*current_);
  std::unique_ptr<TsPartitionVersion> partition(new TsPartitionVersion{partition_dir, partition_id});
  partition->valid_memseg_ = new_version->valid_memseg_;

  {
    // create directory for new partition
    // TODO(zzr): optimization: create the directory only when flushing
    // this logic is only for deletion and will be removed later after optimize delete
    env_->DeleteDir(partition_dir);
    env_->NewDirectory(partition_dir);
    LOG_INFO("Partition directory created: %s", partition_dir.string().c_str());
    partition->directory_created_ = true;

    partition->del_info_ = std::make_shared<TsDelItemManager>(partition_dir);
    KStatus s = partition->del_info_->Open();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Partition DelItemManager open failed: partition_dir[%s].", partition_dir.string().c_str());
      return KStatus::FAIL;
    }
  }
  new_version->partitions_.insert({partition_id, std::move(partition)});

  // update current version
  current_ = std::move(new_version);
  last_created_partition_ = partition_id;
  return KStatus::SUCCESS;
}

static KStatus TruncateFile(const fs::path path, size_t size) {
  int fd = open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    LOG_ERROR("can not open file: %s, reason: %s", path.c_str(), std::strerror(errno));
    return FAIL;
  }

  KStatus ret = SUCCESS;
  size_t actual_size = lseek(fd, 0, SEEK_END);
  if (actual_size > size) {
    LOG_INFO("unexpected file size: %s, expected: %lu, actual: %lu, truncate it", path.c_str(), size, actual_size);
    int ok = ftruncate(fd, size);
    if (ok != 0) {
      LOG_ERROR("can not truncate file: %s, reason: %s", path.c_str(), std::strerror(errno));
      ret = FAIL;
    }
  }
  close(fd);
  return ret;
}

KStatus TsVersionManager::Recover(bool force_recover) {
  // based on empty version, recover from log file
  current_ = std::make_shared<TsVGroupVersion>();

  auto current_path = root_path_ / CurrentVersionName();
  KStatus s;
  if (!fs::exists(current_path)) {
    //  Brand new database, create current version
    uint64_t log_file_number = 0;
    auto update_path = root_path_ / VersionUpdateName(log_file_number);
    {
      std::unique_ptr<TsAppendOnlyFile> current_file;
      s = env_->NewAppendOnlyFile(current_path, &current_file);
      if (s == FAIL) {
        LOG_ERROR("can not create current version file");
        return FAIL;
      }
      s = current_file->Append(update_path.filename().string() + "\n");
      if (s == FAIL) {
        LOG_ERROR("can not write to current version file");
        return FAIL;
      }
    }
    std::unique_ptr<TsAppendOnlyFile> update_log_file;
    s = env_->NewAppendOnlyFile(update_path, &update_log_file);
    if (s == FAIL) {
      LOG_ERROR("can not create update log file");
      return FAIL;
    }

    assert(logger_ == nullptr);
    logger_ = std::make_unique<Logger>(std::move(update_log_file));
    return SUCCESS;
  }

  // Database exists, recover from current version
  std::unique_ptr<TsRandomReadFile> rfile;
  s = env_->NewRandomReadFile(root_path_ / CurrentVersionName(), &rfile);
  if (s == FAIL) {
    LOG_ERROR("can not open current version file");
    return FAIL;
  }
  size_t size = rfile->GetFileSize();
  TsSliceGuard result;
  s = rfile->Read(0, size, &result);
  if (s == FAIL) {
    LOG_ERROR("can not read current version file");
    return FAIL;
  }
  std::string_view content(result.data(), result.size());
  if (content.back() != '\n') {
    LOG_ERROR("current version file content is not ended with newline");
    return FAIL;
  }

  std::unique_ptr<RecordReader> reader;
  std::string log_filename{content.substr(0, content.size() - 1)};
  uint64_t next_logfile_number = 0;
  {
    std::regex re("TSVERSION-([0-9]{12})");
    std::smatch res;
    bool ok = std::regex_match(log_filename, res, re);
    if (!ok) {
      LOG_ERROR("CURRENT file corruption, wrong log file name format");
      return FAIL;
    }

    next_logfile_number = std::stoull(res.str(1)) + 1;
    std::unique_ptr<TsSequentialReadFile> log_file;
    s = env_->NewSequentialReadFile(root_path_ / log_filename, &log_file);
    if (s == FAIL) {
      LOG_ERROR("can not open update log file");
      return FAIL;
    }
    reader = std::make_unique<RecordReader>(std::move(log_file));
  }

  // construct a new logger_
  {
    std::unique_ptr<TsAppendOnlyFile> new_log_file;
    s =
        env_->NewAppendOnlyFile(root_path_ / VersionUpdateName(next_logfile_number), &new_log_file, true /*overwrite*/);
    if (s == FAIL) {
      LOG_ERROR("can not create new update log file");
      return FAIL;
    }
    assert(logger_ == nullptr);
    logger_ = std::make_unique<Logger>(std::move(new_log_file));
  }

  bool eof = false;
  VersionBuilder builder;
  do {
    std::string record;
    s = reader->ReadRecord(&record, &eof);
    if (s == FAIL) {
      LOG_ERROR("can not read update log file");
      return FAIL;
    }
    TsVersionUpdate update;
    TSSlice record_slice{record.data(), record.size()};
    s = update.DecodeFromSlice(record_slice);
    if (s == FAIL) {
      LOG_ERROR("can not decode update log record");
      return FAIL;
    }
    builder.AddUpdate(update);
  } while (!eof);
  TsVersionUpdate update;
  builder.Finalize(&update);
  assert(logger_ != nullptr);
  for (const auto &ipar : update.partitions_created_) {
    auto [dbid, start, end] = ipar;
    int64_t interval = end - start;
    recorder_->RecordInterval(dbid, interval);
  }
  s = ApplyUpdate(&update, force_recover);
  LOG_INFO("recovered update: %s", update.DebugStr().c_str());
  if (s == FAIL) {
    return FAIL;
  }

  // safe write to current file
  {
    std::unique_ptr<TsAppendOnlyFile> tmp_current_file;
    s = env_->NewAppendOnlyFile(root_path_ / TempFileName(CurrentVersionName()), &tmp_current_file, true /*overwrite*/);
    if (s == FAIL) {
      LOG_ERROR("can not create temp current version file");
      return FAIL;
    }
    s = tmp_current_file->Append(VersionUpdateName(next_logfile_number) + "\n");
    if (s == FAIL) {
      LOG_ERROR("can not write to temp current version file");
      return FAIL;
    }
    tmp_current_file.reset();
    std::error_code ec;
    fs::rename(root_path_ / TempFileName(CurrentVersionName()), root_path_ / CurrentVersionName(), ec);
    if (ec.value() != 0) {
      LOG_ERROR("can not rename temp current version file, reason: %s", ec.message().c_str());
      return FAIL;
    }

    // the older log file is no longer needed, delete it
    // no need to check return value
    env_->DeleteFile(root_path_ / log_filename);
  }

  // delete unexpected files
  std::set<fs::path> partition_name;
  for (const auto &partition_id : update.partitions_created_) {
    partition_name.insert(PartitionDirName(partition_id));
  }
  {
    // 1. delete unflushed partition dir
    std::error_code ec;
    fs::directory_iterator iter{root_path_, ec};
    if (ec.value() != 0) {
      LOG_ERROR("cannot iterate directory %s, reason: %s", root_path_.c_str(), ec.message().c_str());
      return FAIL;
    }
    for (auto const &dir_entry : iter) {
      if (!fs::is_directory(dir_entry)) {
        continue;
      }
      fs::path path{dir_entry};
      if (partition_name.find(path.filename()) == partition_name.end()) {
        LOG_INFO("unexpected partition directory: %s, remove it", path.c_str());
        s = env_->DeleteDir(path);
        if (s == FAIL) {
          return s;
        }
      }
    }
  }
  {
    // 2. delete datafiles
    for (auto par_id : update.partitions_created_) {
      auto root = root_path_ / PartitionDirName(par_id);
      std::set<fs::path> expected{DEL_FILE_NAME};
      {
        auto it = update.entity_segment_.find(par_id);
        if (it != update.entity_segment_.end()) {
          auto &info = it->second;
          expected.insert(EntityHeaderFileName(info.header_e_file_number));

          auto header_b_file = BlockHeaderFileName(info.header_b_info.file_number);
          TruncateFile(root / header_b_file, info.header_b_info.length);
          expected.insert(header_b_file);

          auto datablock_file = DataBlockFileName(info.datablock_info.file_number);
          TruncateFile(root / datablock_file, info.datablock_info.length);
          expected.insert(datablock_file);

          auto agg_file = EntityAggFileName(info.agg_info.file_number);
          TruncateFile(root / agg_file, info.agg_info.length);
          expected.insert(agg_file);
        }
      }
      {
        auto it = update.new_lastsegs_.find(par_id);
        if (it != update.new_lastsegs_.end()) {
          for (auto n : it->second) {
            expected.insert(LastSegmentFileName(n.file_number));
          }
        }
      }
      {
        auto it = update.count_flush_infos_.find(par_id);
        if (it != update.count_flush_infos_.end()) {
          expected.insert(CountStatFileName(it->second.file_number));
        }
      }

      std::error_code ec;
      fs::directory_iterator iter{root, ec};
      if (ec.value() != 0) {
        LOG_ERROR("cannot iterate directory %s, reason: %s", root.c_str(), ec.message().c_str());
        return FAIL;
      }

      for (auto const &dir_entry : iter) {
        fs::path p{dir_entry};
        if (expected.find(p.filename()) == expected.end()) {
          LOG_INFO("unexpected file: %s, remove it", p.c_str());
          s = env_->DeleteFile(p);
          if (s == FAIL) {
            return s;
          }
        }
      }
    }
  }

  assert(current_ != nullptr);
  return SUCCESS;
}

KStatus TsVersionManager::ApplyUpdate(TsVersionUpdate *update, bool force_apply) {
  if (update->Empty()) {
    // empty update, do nothing
    return SUCCESS;
  }

  if (update->MemSegmentsOnly()) {
    // switch memsegment operation will not persist to disk, process it as fast as possible
    std::unique_lock lk{mu_};
    auto new_vgroup_version = std::make_unique<TsVGroupVersion>(*current_);
    new_vgroup_version->valid_memseg_ = std::make_unique<MemSegList>(*current_->valid_memseg_);
    if (update->has_del_mem_segments_) {
      new_vgroup_version->valid_memseg_->remove(update->del_memseg_);
    }
    if (update->has_new_mem_segments_) {
      new_vgroup_version->valid_memseg_->push_back(update->new_memseg_);
    }
    for (auto [par_id, par] : new_vgroup_version->partitions_) {
      auto new_partition_version = std::make_unique<TsPartitionVersion>(*par);
      new_partition_version->valid_memseg_ = new_vgroup_version->valid_memseg_;
      new_vgroup_version->partitions_[par_id] = std::move(new_partition_version);
    }
    current_ = std::move(new_vgroup_version);
    return SUCCESS;
  }

  if (update->has_next_file_number_) {
    assert(this->next_file_number_.load(std::memory_order_relaxed) == 0);
    this->next_file_number_.store(update->next_file_number_, std::memory_order_relaxed);
  }

  if (update->NeedRecordFileNumber()) {
    update->SetNextFileNumber(this->next_file_number_.load(std::memory_order_relaxed));
  }

  TsBufferBuilder encoded_update;
  if (update->NeedRecord()) {
    encoded_update = update->EncodeToString();
  }

  TsIOEnv *header_env = nullptr;
  if (EngineOptions::g_io_mode >= TsIOMode::FIO_AND_MMAP) {
    header_env = &TsMMapIOEnv::GetInstance();
  } else {
    header_env = &TsFIOEnv::GetInstance();
  }
  EntitySegmentIOEnvSet env_set{&TsIOEnv::GetInstance(), header_env};

  // TODO(zzr): thinking concurrency control carefully.
  std::unique_lock lk{mu_};

  // Create a new vgroup version based on current version
  auto new_vgroup_version = std::make_unique<TsVGroupVersion>(*current_);
  if (update->has_max_lsn_) {
    new_vgroup_version->max_osn_ = std::max(new_vgroup_version->max_osn_, update->max_lsn_);
  }

  if (update->has_count_stats_ && update->count_stats_status_ == CountStatsStatus::FlushImmOrWriteBatch) {
    update->has_new_version_number_ = true;
    update->version_num_ = version_num_.fetch_add(1, std::memory_order_relaxed);
    encoded_update.push_back(static_cast<char>(VersionUpdateType::kNewVersionNumber));
    PutVarint64(&encoded_update, update->version_num_);
    new_vgroup_version->version_num_ = update->version_num_;
  }

  if (update->has_new_partition_) {
    for (const auto &p : update->partitions_created_) {
      if (new_vgroup_version->partitions_.find(p) != new_vgroup_version->partitions_.end()) {
        continue;
      }
      auto partition_dir = root_path_ / PartitionDirName(p);
      auto partition = std::unique_ptr<TsPartitionVersion>(new TsPartitionVersion{partition_dir, p});
      partition->del_info_ = std::make_shared<TsDelItemManager>(partition_dir);
      partition->del_info_->Open();
      // partition->meta_info_ = std::make_shared<TsPartitionEntityMetaManager>(root_path_ / PartitionDirName(p));
      // partition->meta_info_->Open();
      new_vgroup_version->partitions_.insert({p, std::move(partition)});
    }
  }

  if (update->has_new_mem_segments_ || update->has_del_mem_segments_) {
    new_vgroup_version->valid_memseg_ = std::make_unique<MemSegList>(*current_->valid_memseg_);
    if (update->has_del_mem_segments_) {
      new_vgroup_version->valid_memseg_->remove(update->del_memseg_);
    }
    if (update->has_new_mem_segments_) {
      new_vgroup_version->valid_memseg_->push_back(update->new_memseg_);
    }
  }
  // looping over all partitions
  TsVersionUpdate clean_up_update;
  for (auto [par_id, par] : new_vgroup_version->partitions_) {
    auto new_partition_version = std::make_unique<TsPartitionVersion>(*par);

    if (update->partitions_created_.find(par_id) != update->partitions_created_.end()) {
      new_partition_version->directory_created_ = true;
      new_partition_version->flushed_ = true;
    }

    if (update->has_new_mem_segments_ || update->has_del_mem_segments_) {
      new_partition_version->valid_memseg_ = new_vgroup_version->valid_memseg_;
    }

    // Process lastsegment deletion, used by Compact()
    {
      auto it = update->delete_lastsegs_.find(par_id);
      if (it != update->delete_lastsegs_.end()) {
        TsPartitionVersion::LastSegmentContainer tmp_container;
        std::vector<std::vector<std::shared_ptr<TsLastSegment>>> tmp;

        const auto &src_last_segments = new_partition_version->leveled_last_segments_;

        for (int level = 0; level < src_last_segments.GetMaxLevel(); ++level) {
          for (int group = 0; group < src_last_segments.GetGroupSize(level); ++group) {
            const auto &last_segments = src_last_segments.GetLastSegments(level, group);
            for (auto &l : last_segments) {
              if (it->second.find(l->GetFileNumber()) == it->second.end()) {
                tmp_container.AddLastSegment(level, group, l);
              } else {
                l->MarkDelete();
              }
            }
          }
        }
        new_partition_version->leveled_last_segments_ = std::move(tmp_container);
      }
    }

    auto partition_dir = root_path_ / PartitionDirName(par_id);
    // Process lastsegment creation, used by Flush() and Compact()
    {
      // TODO(zzr): Lazy open? add something like `TsLastSegmentHandler` to do this.
      auto it = update->new_lastsegs_.find(par_id);
      if (it != update->new_lastsegs_.end()) {
        for (auto meta : it->second) {
          std::unique_ptr<TsRandomReadFile> rfile;
          auto filepath = partition_dir / LastSegmentFileName(meta.file_number);
          TsIOEnv* last_segment_env;
          if (EngineOptions::g_io_mode >= TsIOMode::FIO_AND_MMAP) {
            last_segment_env = &TsMMapIOEnv::GetInstance();
          } else {
            last_segment_env = &TsFIOEnv::GetInstance();
          }
          auto s = last_segment_env->NewRandomReadFile(filepath, &rfile);
          if (s == FAIL && !force_apply) {
            return FAIL;
          }

          if (s != FAIL) {
            auto last_segment = TsLastSegment::Create(meta.file_number, std::move(rfile));
            s = last_segment->Open();
            if (s == FAIL && !force_apply) {
              LOG_ERROR("can not open file %s", LastSegmentFileName(meta.file_number).c_str());
              return FAIL;
            }
            if (s != FAIL) {
              new_partition_version->leveled_last_segments_.AddLastSegment(meta.level, meta.group,
                                                                           std::move(last_segment));
            }
          }

          if (s == FAIL) {
            clean_up_update.DeleteLastSegment(par_id, meta.file_number);
          }
        }
      }
    }

    // Process entity segment update, used by Compact()
    auto it = update->entity_segment_.find(par_id);
    if (it != update->entity_segment_.end()) {
      std::string root = root_path_ / PartitionDirName(par_id);
      if (new_partition_version->entity_segment_) {
        new_partition_version->entity_segment_->MarkDeleteEntityHeader();
        if (update->delete_all_prev_entity_segment_) {
          new_partition_version->entity_segment_->MarkDeleteAll();
        }
      }
      new_partition_version->entity_segment_ = std::make_unique<TsEntitySegment>(env_set, root, it->second,
                                                          new_vgroup_version->partitions_[par_id]->GetEntitySegment());
    }

    // Process count stats, used by Flush() and FinishWriteBatchData()
    {
      auto count_meta = update->count_flush_infos_.find(par_id);
      if (count_meta != update->count_flush_infos_.end()) {
        std::string count_path = root_path_ / PartitionDirName(par_id) / CountStatFileName(count_meta->second.file_number);
        auto new_count_file = std::make_shared<TsPartitionEntityCountManager>(count_path);
        switch (update->count_stats_status_) {
          case CountStatsStatus::FlushImmOrWriteBatch: {
            if (new_partition_version->count_info_) {
              if (!CopyFile(new_partition_version->count_info_->FilePath(), count_path)) {
                LOG_ERROR("copy count.stat file failed! source path [%s], dest path [%s]",
                  new_partition_version->count_info_->FilePath().c_str(), count_path.c_str())
                return KStatus::FAIL;
              }
            }
            auto s = new_count_file->Open();
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("count.stat open failed! path [%s]", count_path.c_str())
              return KStatus::FAIL;
            }
            new_count_file->SetCountStatsHeader(1, update->version_num_, new_vgroup_version->max_osn_);
            for (auto& v : count_meta->second.flush_infos) {
              if (new_partition_version->ShouldSetCountStatsInvalid(v.entity_id)) {
                v.is_count_valid = false;
              }
              new_count_file->AddEntityCountStats(v);
            }
            break;
          }
          case CountStatsStatus::Recalculate: {
            if (new_partition_version->count_info_) {
              TsCountStatsFileHeader header {};
              new_partition_version->count_info_->GetCountStatsHeader(header);
              if (header.version_num > update->version_num_) {
                return KStatus::FAIL;
              }
              if (!CopyFile(new_partition_version->count_info_->FilePath(), count_path)) {
                LOG_ERROR("copy count.stat file failed! source path [%s], dest path [%s]",
                  new_partition_version->count_info_->FilePath().c_str(), count_path.c_str())
                return KStatus::FAIL;
              }
            }
            auto s = new_count_file->Open();
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("count.stat open failed! path [%s]", count_path.c_str())
              return KStatus::FAIL;
            }
            new_count_file->SetCountStatsHeader(1, update->version_num_, update->max_lsn_);
            for (auto& v : count_meta->second.flush_infos) {
              if (new_partition_version->count_info_) {
                TsEntityCountStats stats {};
                stats.entity_id = v.entity_id;
                new_partition_version->count_info_->GetEntityCountStats(stats);
                if (!stats.is_count_valid) {
                  new_count_file->SetEntityCountStats(v);
                }
              } else {
                new_count_file->SetEntityCountStats(v);
              }
            }
            break;
          }
          case CountStatsStatus::Recover: {
            auto s = new_count_file->Open();
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("count.stat open failed! path [%s]", count_path.c_str())
              return KStatus::FAIL;
            }
            break;
          }
          case CountStatsStatus::UpgradeRecover: {
            auto s = new_count_file->Open();
            if (s != KStatus::SUCCESS) {
              LOG_ERROR("count.stat open failed! path [%s]", count_path.c_str())
              return KStatus::FAIL;
            }
            new_count_file->SetCountStatsHeader(1, version_num_.load(memory_order_relaxed),
              new_vgroup_version->max_osn_);
            for (auto& v : count_meta->second.flush_infos) {
              new_count_file->AddEntityCountStats(v);
            }
            break;
          }
        }
        KStatus s = new_count_file->Sync();
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("count.stat sync failed! path [%s]", count_path.c_str())
          return KStatus::FAIL;
        }
        if (new_partition_version->count_info_) {
          new_partition_version->count_info_->MarkDelete();
        }
        new_partition_version->count_info_ = std::move(new_count_file);
      }
    }

    // VGroupVersion accepts the new partition version
    new_vgroup_version->partitions_[par_id] = std::move(new_partition_version);
  }

  if (update->count_stats_status_ == CountStatsStatus::Recover && update->has_new_version_number_) {
    this->version_num_.store(update->version_num_, std::memory_order_relaxed);
  }

  if (update->NeedRecord()) {
    logger_->AddRecord(encoded_update.AsStringView());
  }
  if (clean_up_update.NeedRecord()) {
    auto cleanup_encode = clean_up_update.EncodeToString();
    logger_->AddRecord(cleanup_encode.AsStringView());
  }
  current_ = std::move(new_vgroup_version);

  // release LRU cache if the update is call by Vacuum
  // TODO(zzr): optimize later in 3.1
  if (update->delete_all_prev_entity_segment_) {
    TsLRUBlockCache::GetInstance().EvictAll();
  }
  // LOG_DEBUG("%s: %s", this->root_path_.filename().c_str(), update->DebugStr().c_str());
  return SUCCESS;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetPartitions(uint32_t target_dbid,
  const std::vector<KwTsSpan>& ts_spans, DATATYPE ts_type) const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    const auto &[dbid, _, __] = k;
    if (dbid == target_dbid) {
      // check if current partition is cross with ts_spans.
      if (checkTimestampWithSpans(ts_spans, v->GetTsColTypeStartTime(ts_type), v->GetTsColTypeEndTime(ts_type))
           < TimestampCheckResult::NonOverlapping) {
        result.push_back(v);
      }
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
  int64_t interval = recorder_->GetInterval(target_dbid);
  timestamp64 start = GetPartitionStartTime(target_time, interval);
  auto it = partitions_.find({target_dbid, start, start + interval});
  if (it == partitions_.end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<const TsPartitionVersion> TsVGroupVersion::GetPartition(PartitionIdentifier par_id) const {
  auto it = partitions_.find(par_id);
  if (it == partitions_.end()) {
    return nullptr;
  }
  return it->second;
}

std::map<uint32_t, std::vector<std::shared_ptr<const TsPartitionVersion>>> TsVGroupVersion::GetPartitions() const {
  std::map<uint32_t, std::vector<std::shared_ptr<const TsPartitionVersion>>> result;
  for (const auto &[k, v] : partitions_) {
      result[std::get<0>(k)].push_back(v);
  }
  return result;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetDBAllPartitions(uint32_t target_dbid) const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    const auto &[dbid, _, __] = k;
    if (dbid == target_dbid) {
        result.push_back(v);
    }
  }
  return result;
}

std::vector<std::shared_ptr<TsLastSegment>> TsPartitionVersion::GetCompactLastSegments(int *level, int *group) const {
  // NOTE: check will from bottom to top
  auto [l, g] = leveled_last_segments_.GetLevelGroupToCompact();
  if (l < 0) {
    return {};
  }
  const auto &lasts = leveled_last_segments_.GetLastSegments(l, g);
  *level = l;
  *group = g;
  if (lasts.size() <= EngineOptions::max_compact_num) {
    return lasts;
  }
  std::vector<std::shared_ptr<TsLastSegment>> result;
  result.reserve(EngineOptions::max_compact_num);
  std::move(lasts.begin(), lasts.begin() + EngineOptions::max_compact_num, std::back_inserter(result));
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
  auto last_segs = GetAllLastSegments();
  result.reserve(mem_segs.size() + last_segs.size() + 1);

  std::move(mem_segs.begin(), mem_segs.end(), std::back_inserter(result));
  std::move(last_segs.begin(), last_segs.end(), std::back_inserter(result));
  result.push_back(entity_segment_);
  return result;
}

KStatus TsPartitionVersion::DeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans,
                                       const KwOSNSpan &lsn, bool user_del) const {
  kwdbts::TsEntityDelItem del_item(ts_spans[0], lsn, e_id,
    user_del ? TsEntityDelItemType::DEL_ITEM_TYPE_USER : TsEntityDelItemType::DEL_ITEM_TYPE_OTHER);
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

KStatus TsPartitionVersion::UndoDeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans,
                                           const KwOSNSpan &lsn) const {
  auto s = del_info_->RollBackDelItem(e_id, lsn);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBackDelItem failed. for entity[%lu]", e_id);
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionVersion::HasDeleteItem(bool& has_delete_info, const KwOSNSpan &lsn) const {
  return del_info_->HasValidDelItem(lsn, has_delete_info);
}

KStatus TsPartitionVersion::RmDeleteItems(const std::list<std::pair<TSEntityID, TS_OSN>>& entity_max_lsn) const {
  for (auto entity : entity_max_lsn) {
    auto s = del_info_->RmDeleteItems(entity.first, {0, entity.second});
    if (s != KStatus::SUCCESS) {
      // failed not cause any function err, but scan may slow a little.
      LOG_WARN("RmDeleteItems failed. entity_id[%lu], lsn[%lu]", entity.first, entity.second);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionVersion::DropEntity(TSEntityID e_id) const {
  // todo(liangbo01) add metric data clearing function
  if (entity_segment_.get() != nullptr) {
    auto s = entity_segment_->SetEntityItemDropped(e_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("SetEntityItemDropped failed.");
      return s;
    }
  }
  return del_info_->DropEntity(e_id);
}

KStatus TsPartitionVersion::getFilter(const TsScanFilterParams& filter, TsBlockItemFilterParams& block_data_filter) const {
  std::list<STDelRange> del_range;
  auto s = GetDelRange(filter.entity_id_, del_range);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelRange failed.");
    return s;
  }
  KwTsSpan partition_span;
  partition_span.begin = convertSecondToPrecisionTS(GetStartTime(), filter.table_ts_type_);
  partition_span.end = convertSecondToPrecisionTS(GetEndTime(), filter.table_ts_type_) - 1;
  std::vector<STScanRange> cur_scan_range;
  for (auto& scan : filter.ts_spans_) {
    KwTsSpan cross_part;
    cross_part.begin = std::max(partition_span.begin, scan.begin);
    cross_part.end = std::min(partition_span.end, scan.end);
    if (cross_part.begin <= cross_part.end) {
      for (const KwOSNSpan& osn_span : filter.osn_spans_) {
        cur_scan_range.push_back(STScanRange{cross_part, osn_span});
      }
    }
  }
  for (auto& del : del_range) {
    cur_scan_range = LSNRangeUtil::MergeScanAndDelRange(cur_scan_range, del);
  }

  // TODO(zzr, lb): optimize: cur_scan_range should be sorted, implement a O(m+n) algorithm to do this.
  // for now, MergeScanAndDelRange in loop is O(m*n)
  std::sort(cur_scan_range.begin(), cur_scan_range.end(), [](const STScanRange &a, const STScanRange &b) {
    using HelperTuple = std::tuple<timestamp64, TS_OSN>;
    return HelperTuple{a.ts_span.begin, a.osn_span.begin} < HelperTuple{b.ts_span.begin, b.osn_span.begin};
  });

  block_data_filter.spans_ = std::move(cur_scan_range);
  block_data_filter.db_id = filter.db_id_;
  block_data_filter.entity_id = filter.entity_id_;
  block_data_filter.vgroup_id = filter.vgroup_id_;
  block_data_filter.table_id = filter.table_id_;
  return KStatus::SUCCESS;
}

KStatus TsPartitionVersion::GetBlockSpans(const TsScanFilterParams& filter,
                                          std::list<shared_ptr<TsBlockSpan>>* ts_block_spans,
                                          const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                          const std::shared_ptr<MMapMetricsTable>& scan_schema,
                                          TsScanStats* ts_scan_stats,
                                          bool skip_mem, bool skip_last, bool skip_entity) const {
  TsBlockItemFilterParams block_data_filter;
  auto s = getFilter(filter, block_data_filter);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getFilter failed.");
    return s;
  }

  if (!skip_mem) {
    // get block span in mem segment
    if (valid_memseg_ != nullptr) {
      for (auto &mem : *valid_memseg_) {
        s = mem->GetBlockSpans(block_data_filter, *ts_block_spans, tbl_schema_mgr, scan_schema, ts_scan_stats);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetBlockSpans of mem segment failed.");
          return s;
        }
      }
    }
  }
  if (!skip_last) {
    // get block span in last segment
    for (int level = 0; level < leveled_last_segments_.GetMaxLevel(); ++level) {
      int group = LastSegmentContainer::GetGroupByEntityID(level, block_data_filter.entity_id);
      const auto &last_segments = leveled_last_segments_.GetLastSegments(level, group);
      for (const auto &last_seg : last_segments) {
        s = last_seg->GetBlockSpans(block_data_filter, *ts_block_spans, tbl_schema_mgr, scan_schema, ts_scan_stats);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("GetBlockSpans of last segment failed.");
          return s;
        }
      }
    }
  }
  if (!skip_entity) {
    // get block span in entity segment
    if (entity_segment_ == nullptr) {
      // entity segment not exist
      return KStatus::SUCCESS;
    }
    s = entity_segment_->GetBlockSpans(entity_segment_, block_data_filter, *ts_block_spans,
             tbl_schema_mgr, scan_schema, ts_scan_stats);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans of entity segment failed.");
      return s;
    }
  }
  // LOG_DEBUG("reading block span num [%lu]", ts_block_spans->size());
  return KStatus::SUCCESS;
}

bool TsPartitionVersion::TrySetBusy(PartitionStatus desired) const {
  PartitionStatus expected = PartitionStatus::None;
  return exclusive_status_->compare_exchange_strong(expected, desired);
}

void TsPartitionVersion::ResetStatus() const {
  exclusive_status_->store(PartitionStatus::None);
}

KStatus TsPartitionVersion::NeedVacuumEntitySegment(const fs::path& root_path, TsEngineSchemaManager* schema_manager,
                                                    bool force, bool& need_vacuum) const {
  need_vacuum = false;
  int nlastseg = leveled_last_segments_.Size();
  if (nlastseg == 0 && entity_segment_ == nullptr) {
    return SUCCESS;
  }
  timestamp64 latest_mtime = 0;
  bool has_files = false;

  std::error_code ec;
  fs::directory_iterator dir_iter{root_path, ec};
  if (ec.value() != 0) {
    LOG_ERROR("fs::directory_iterator error:%s", ec.message().c_str());
    return FAIL;
  }
  for (const auto& entry : dir_iter) {
    std::error_code file_ec;
    if (fs::is_regular_file(entry, file_ec) && !file_ec && entry.path().filename() != DEL_FILE_NAME) {
      auto mtime = ModifyTime(entry.path());
      if (!has_files || mtime > latest_mtime) {
        latest_mtime = mtime;
        has_files = true;
      }
    }
  }
  if (!has_files) {
    LOG_WARN("No regular files found in directory [%s]", root_path.c_str());
    return SUCCESS;
  }
  auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
  if (!force) {
    // Temporarily add environment variables to control vacuum
    uint32_t vacuum_interval = vacuum_minutes;
    const char *vacuum_minutes_char = getenv("KW_VACUUM_TIME");
    if (vacuum_minutes_char) {
      char* endptr;
      vacuum_interval = strtol(vacuum_minutes_char, &endptr, 10);
      assert(*endptr == '\0');
    }
    float diff_latest_now = (now.time_since_epoch().count() - latest_mtime) / 60;
    if (diff_latest_now < vacuum_interval) {
      return SUCCESS;
    }
  }
  bool has_del_info;
  // todo(liangbo01) get entity segment min and max lsn.
  KwOSNSpan span{0, UINT64_MAX};
  auto s = del_info_->HasValidDelItem(span, has_del_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("HasValidDelItem failed");
    return s;
  }
  need_vacuum = has_del_info;
  if (!need_vacuum && entity_segment_ != nullptr) {
    uint64_t max_entity_id = entity_segment_->GetEntityNum();
    std::unordered_map<TSTableID, bool> traversed_table;
    for (int entity_id = 1; entity_id <= max_entity_id; entity_id++) {
      TsEntityItem entity_item;
      bool found = false;
      s = entity_segment_->GetEntityItem(entity_id, entity_item, found);
      if (s != KStatus::SUCCESS) {
        LOG_WARN("GetEntityItem failed, entity id [%u]", entity_id);
        continue;
      }
      if (!found || 0 == entity_item.cur_block_id) {
        continue;
      }

      if (force) {
        // CheckDeviceContinuity
        std::vector<TsEntitySegmentBlockItemWithData> block_datas;
        s = entity_segment_->GetAllBlockItems(entity_id, &block_datas);
        if (s != KStatus::SUCCESS) {
          LOG_WARN("GetAllBlockItems failed, entity id [%u]", entity_id);
          continue;
        }
        for (const auto& block_data : block_datas) {
          if (block_data.block_item->block_id - 1 != block_data.block_item->prev_block_id) {
            need_vacuum = true;
            return SUCCESS;
          }
        }
      }
      if (traversed_table.count(entity_item.table_id) == 0) {
        std::shared_ptr<TsTableSchemaManager> tb_schema_mgr{nullptr};
        s = schema_manager->GetTableSchemaMgr(entity_item.table_id, tb_schema_mgr);
        if (s != SUCCESS) {
          if (tb_schema_mgr == nullptr) {
            need_vacuum = true;
            return SUCCESS;
          }
          return s;
        }
        auto life_time = tb_schema_mgr->GetLifeTime();

        bool has_lifetime = life_time.ts != 0;
        if (tb_schema_mgr->IsDropped()) {
          need_vacuum = true;
          return SUCCESS;
        }
        if (has_lifetime) {
          auto start_ts = (now.time_since_epoch().count() - life_time.ts) * life_time.precision;
          auto end_time = GetTsColTypeEndTime(tb_schema_mgr->GetTsColDataType());
          if (end_time < start_ts) {
            need_vacuum = true;
            return SUCCESS;
          }
        }
        traversed_table[entity_item.table_id] = true;
      }
    }
  }
  return KStatus::SUCCESS;
}

bool TsPartitionVersion::ShouldSetCountStatsInvalid(TSEntityID e_id) {
  TS_OSN del_osn = 0;
  auto s = del_info_->GetDelMaxOSN(e_id, del_osn);
  if (s != KStatus::SUCCESS) {
    return false;
  }
  if (del_osn == 0) {
    return false;
  }
  if (count_info_) {
    TsCountStatsFileHeader header {};
    s = count_info_->GetCountStatsHeader(header);
    if (del_osn > header.max_osn) {
      return true;
    }
  }
  return true;
}

// version update

inline void EncodePartitionID(TsBufferBuilder *result, const PartitionIdentifier &partition_id) {
  auto [dbid, start_time, end_time] = partition_id;
  PutVarint32(result, dbid);
  PutVarint64(result, start_time);
  PutVarint64(result, end_time);
}

const char *DecodePartitionID(const char *ptr, const char *limit, PartitionIdentifier *partition_id) {
  uint32_t dbid = 0;
  ptr = DecodeVarint32(ptr, limit, &dbid);
  if (ptr == nullptr) {
    return nullptr;
  }
  uint64_t start_time = 0;
  ptr = DecodeVarint64(ptr, limit, &start_time);
  if (ptr == nullptr) {
    return nullptr;
  }
  uint64_t end_time = 0;
  ptr = DecodeVarint64(ptr, limit, &end_time);
  if (ptr == nullptr) {
    return nullptr;
  }
  *partition_id = {dbid, start_time, end_time};
  return ptr;
}

inline void EncodeLastSegmentMetas(
    TsBufferBuilder *result, const std::map<PartitionIdentifier, std::vector<LastSegmentMetaInfo>> &last_segment_metas) {
  uint32_t npartition = last_segment_metas.size();
  PutVarint32(result, npartition);
  for (const auto &[par_id, meta_vec] : last_segment_metas) {
    EncodePartitionID(result, par_id);
    uint32_t nfile = meta_vec.size();
    PutVarint32(result, nfile);
    result->push_back(3);  // nmeta
    result->push_back(static_cast<char>(LastSegmentMetaType::kFileNumber));
    for (auto meta : meta_vec) {
      PutVarint64(result, meta.file_number);
    }

    result->push_back(static_cast<char>(LastSegmentMetaType::kLevel));
    for (auto meta : meta_vec) {
      PutVarint32(result, meta.level);
    }

    result->push_back(static_cast<char>(LastSegmentMetaType::kGroup));
    for (auto meta : meta_vec) {
      PutVarint32(result, meta.group);
    }
  }
}

inline const char *DecodeFileNumber(const char *ptr, const char *limit, int n,
                                    std::vector<LastSegmentMetaInfo> *last_seg_meta_vec) {
  for (int i = 0; i < n; ++i) {
    uint64_t file_number = 0;
    ptr = DecodeVarint64(ptr, limit, &file_number);
    if (ptr == nullptr) {
      return nullptr;
    }

    (*last_seg_meta_vec)[i].file_number = file_number;
  }
  return ptr;
}

inline const char *DecodeLevel(const char *ptr, const char *limit, int n,
                               std::vector<LastSegmentMetaInfo> *last_seg_meta_vec) {
  for (int i = 0; i < n; ++i) {
    uint32_t level = 0;
    ptr = DecodeVarint32(ptr, limit, &level);
    if (ptr == nullptr) {
      return nullptr;
    }

    (*last_seg_meta_vec)[i].level = level;
  }
  return ptr;
}

inline const char *DecodeGroup(const char *ptr, const char *limit, int n,
                               std::vector<LastSegmentMetaInfo> *last_seg_meta_vec) {
  for (int i = 0; i < n; ++i) {
    uint32_t group = 0;
    ptr = DecodeVarint32(ptr, limit, &group);
    if (ptr == nullptr) {
      return nullptr;
    }

    (*last_seg_meta_vec)[i].group = group;
  }
  return ptr;
}

static constexpr std::array<decltype(&DecodeFileNumber), 4> decode_funcs = {nullptr, DecodeFileNumber, DecodeLevel,
                                                                            DecodeGroup};

inline const char *DecodeLastSegmentMetas(
    const char *ptr, const char *limit,
    std::map<PartitionIdentifier, std::vector<LastSegmentMetaInfo>> *last_segment_metas) {
  uint32_t npartition = 0;
  ptr = DecodeVarint32(ptr, limit, &npartition);
  if (ptr == nullptr) {
    return nullptr;
  }

  for (uint32_t i = 0; i < npartition; ++i) {
    PartitionIdentifier par_id;
    ptr = DecodePartitionID(ptr, limit, &par_id);
    if (ptr == nullptr) {
      return nullptr;
    }
    uint32_t nfiles = 0;
    ptr = DecodeVarint32(ptr, limit, &nfiles);
    if (ptr == nullptr) {
      return nullptr;
    }
    std::vector<LastSegmentMetaInfo> last_seg_meta_vec(nfiles);
    auto n_meta_section = static_cast<uint8_t>(*ptr);
    ++ptr;

    for (int j = 0; j < n_meta_section; ++j) {
      auto type = static_cast<uint8_t>(*ptr);
      ptr++;
      ptr = decode_funcs[type](ptr, limit, nfiles, &last_seg_meta_vec);

      if (ptr == nullptr) {
        return nullptr;
      }
    }

    (*last_segment_metas)[par_id] = std::move(last_seg_meta_vec);
  }
  return ptr;
}

inline void EncodePartitionFiles(TsBufferBuilder *result, const std::map<PartitionIdentifier, std::set<uint64_t>> &files) {
  uint32_t npartition = files.size();
  PutVarint32(result, npartition);
  for (const auto &[par_id, file_numbers] : files) {
    EncodePartitionID(result, par_id);
    uint32_t nfile = file_numbers.size();
    PutVarint32(result, nfile);
    for (uint64_t file_number : file_numbers) {
      PutVarint64(result, file_number);
    }
  }
}

inline const char *DecodePartitionFiles(const char *ptr, const char *limit,
                                       std::map<PartitionIdentifier, std::set<uint64_t>> *files) {
  uint32_t npartition = 0;
  ptr = DecodeVarint32(ptr, limit, &npartition);
  if (ptr == nullptr) {
    return nullptr;
  }
  for (uint32_t i = 0; i < npartition; ++i) {
    PartitionIdentifier par_id;
    ptr = DecodePartitionID(ptr, limit, &par_id);
    if (ptr == nullptr) {
      return nullptr;
    }
    uint32_t nfile = 0;
    ptr = DecodeVarint32(ptr, limit, &nfile);
    if (ptr == nullptr) {
      return nullptr;
    }
    std::set<uint64_t> file_numbers;
    for (uint32_t j = 0; j < nfile; ++j) {
      uint64_t file_number = 0;
      ptr = DecodeVarint64(ptr, limit, &file_number);
      if (ptr == nullptr) {
        return nullptr;
      }
      file_numbers.insert(file_number);
    }
    (*files)[par_id] = std::move(file_numbers);
  }
  return ptr;
}

static inline void EncodeMetaInfo(TsBufferBuilder *result, const MetaFileInfo &meta_info) {
  PutVarint64(result, meta_info.file_number);
  PutVarint64(result, meta_info.length);
}

static inline const char *DecodeMetaInfo(const char *ptr, const char *limit, MetaFileInfo *meta_info) {
  ptr = DecodeVarint64(ptr, limit, &meta_info->file_number);
  ptr = DecodeVarint64(ptr, limit, &meta_info->length);
  return ptr;
}

inline void EncodeEntitySegment(TsBufferBuilder *result,
                                const std::map<PartitionIdentifier, EntitySegmentMetaInfo> &entity_segments) {
  uint32_t npartition = entity_segments.size();
  PutVarint32(result, npartition);
  for (const auto &[par_id, info] : entity_segments) {
    EncodePartitionID(result, par_id);

    EncodeMetaInfo(result, info.datablock_info);
    EncodeMetaInfo(result, info.header_b_info);
    PutVarint64(result, info.header_e_file_number);
    EncodeMetaInfo(result, info.agg_info);
  }
}

const char *DecodeEntitySegment(const char *ptr, const char *limit,
                                std::map<PartitionIdentifier, EntitySegmentMetaInfo> *entity_segments) {
  uint32_t npartition = 0;
  ptr = DecodeVarint32(ptr, limit, &npartition);
  if (ptr == nullptr) {
    return nullptr;
  }

  for (uint32_t i = 0; i < npartition; ++i) {
    PartitionIdentifier par_id;
    ptr = DecodePartitionID(ptr, limit, &par_id);
    if (ptr == nullptr) {
      return nullptr;
    }
    EntitySegmentMetaInfo info;
    ptr = DecodeMetaInfo(ptr, limit, &info.datablock_info);
    ptr = DecodeMetaInfo(ptr, limit, &info.header_b_info);
    ptr = DecodeVarint64(ptr, limit, &info.header_e_file_number);
    ptr = DecodeMetaInfo(ptr, limit, &info.agg_info);
    if (ptr == nullptr) {
      return nullptr;
    }
    entity_segments->insert_or_assign(par_id, info);
  }
  return ptr;
}

inline void EncodeCountStatFile(TsBufferBuilder *result,
                                const std::map<PartitionIdentifier, CountStatMetaInfo> &count_stats) {
  uint32_t npartition = count_stats.size();
  PutVarint32(result, npartition);
  for (const auto &[par_id, info] : count_stats) {
    EncodePartitionID(result, par_id);
    PutVarint64(result, info.file_number);
  }
}

const char *DecodeCountStatFile(const char *ptr, const char *limit,
                                std::map<PartitionIdentifier, CountStatMetaInfo> *count_stats) {
  uint32_t npartition = 0;
  ptr = DecodeVarint32(ptr, limit, &npartition);
  if (ptr == nullptr) {
    return nullptr;
  }

  for (uint32_t i = 0; i < npartition; ++i) {
    PartitionIdentifier par_id;
    ptr = DecodePartitionID(ptr, limit, &par_id);
    if (ptr == nullptr) {
      return nullptr;
    }
    CountStatMetaInfo info;
    ptr = DecodeVarint64(ptr, limit, &info.file_number);
    if (ptr == nullptr) {
      return nullptr;
    }
    count_stats->insert_or_assign(par_id, info);
  }
  return ptr;
}

TsBufferBuilder TsVersionUpdate::EncodeToString() const {
  TsBufferBuilder result;
  if (has_new_partition_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNewPartition));
    uint32_t npartition = partitions_created_.size();
    PutVarint32(&result, npartition);
    for (const auto &par_id : partitions_created_) {
      EncodePartitionID(&result, par_id);
    }
  }
  if (has_new_lastseg_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNewLastSegmentWithMeta));
    EncodeLastSegmentMetas(&result, new_lastsegs_);
  }

  if (has_delete_lastseg_) {
    result.push_back(static_cast<char>(VersionUpdateType::kDeleteLastSegment));
    EncodePartitionFiles(&result, delete_lastsegs_);
  }

  // TODO(zzr): encode entity segment update
  if (has_entity_segment_) {
    result.push_back(static_cast<char>(VersionUpdateType::kSetEntitySegment));
    EncodeEntitySegment(&result, entity_segment_);
  }

  if (has_next_file_number_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNextFileNumber));
    PutVarint64(&result, next_file_number_);
  }

  if (has_max_lsn_) {
    result.push_back(static_cast<char>(VersionUpdateType::kMaxLSN));
    PutVarint64(&result, max_lsn_);
  }

  if (has_count_stats_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNewCountStatFile));
    EncodeCountStatFile(&result, count_flush_infos_);
  }

  return result;
}

KStatus TsVersionUpdate::DecodeFromSlice(TSSlice input) {
  const char *ptr = input.data;
  const char *end = input.data + input.len;
  while (ptr < end) {
    VersionUpdateType type = static_cast<VersionUpdateType>(*ptr);
    ++ptr;
    switch (type) {
      case VersionUpdateType::kNewPartition: {
        uint32_t npartition = 0;
        ptr = DecodeVarint32(ptr, end, &npartition);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        for (uint32_t i = 0; i < npartition; ++i) {
          PartitionIdentifier par_id;
          ptr = DecodePartitionID(ptr, end, &par_id);
          if (ptr == nullptr) {
            LOG_ERROR("Corrupted version update slice");
            return FAIL;
          }
          this->partitions_created_.insert(par_id);
        }
        this->has_new_partition_ = true;
        break;
      }

      // deprecated branch, just for compatibility
      case VersionUpdateType::kNewLastSegment: {
        std::map<PartitionIdentifier, std::set<uint64_t>> file_nums;
        ptr = DecodePartitionFiles(ptr, end, &file_nums);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        for (const auto &[par_id, file_numbers] : file_nums) {
          std::vector<LastSegmentMetaInfo> meta_vec;
          meta_vec.reserve(file_numbers.size());
          for (uint64_t file_number : file_numbers) {
            meta_vec.push_back({file_number, 0, 0});
          }
          this->new_lastsegs_.insert_or_assign(par_id, std::move(meta_vec));
        }
        this->has_new_lastseg_ = true;
        break;
      }

      case VersionUpdateType::kNewLastSegmentWithMeta: {
        ptr = DecodeLastSegmentMetas(ptr, end, &this->new_lastsegs_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_new_lastseg_ = true;
        break;
      }
      case VersionUpdateType::kDeleteLastSegment: {
        ptr = DecodePartitionFiles(ptr, end, &this->delete_lastsegs_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_delete_lastseg_ = true;
        break;
      }

      case VersionUpdateType::kSetEntitySegment: {
        // TODO(zzr): decode entity segment update
        ptr = DecodeEntitySegment(ptr, end, &this->entity_segment_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_entity_segment_ = true;
        break;
      }

      case VersionUpdateType::kNextFileNumber: {
        ptr = DecodeVarint64(ptr, end, &this->next_file_number_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_next_file_number_ = true;
        break;
      }

      case VersionUpdateType::kMaxLSN: {
        ptr = DecodeVarint64(ptr, end, &this->max_lsn_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_max_lsn_ = true;
        break;
      }

      case VersionUpdateType::kNewCountStatFile: {
        ptr = DecodeCountStatFile(ptr, end, &this->count_flush_infos_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_count_stats_ = true;
        break;
      }

      case VersionUpdateType::kNewVersionNumber: {
        ptr = DecodeVarint64(ptr, end, &this->version_num_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_new_version_number_ = true;
        break;
      }
      default:
        LOG_ERROR("Unknown version update type: %d", static_cast<int>(type));
        return FAIL;
    }
  }
  if (ptr != end) {
    LOG_ERROR("unexpected end of version update slice");
    return FAIL;
  }
  return SUCCESS;
}

constexpr static uint32_t kTsVersionMagicNumber = 0x54535654;

KStatus TsVersionManager::Logger::AddRecord(std::string_view record) {
  uint16_t checksum = 0;
  for (uint32_t i = 0; i < record.size(); ++i) {
    checksum = checksum + static_cast<uint8_t>(record[i]);
  }
  TsBufferBuilder data;
  PutFixed32(&data, kTsVersionMagicNumber);
  PutFixed16(&data, checksum);
  PutFixed32(&data, record.size());
  data.append(record);
  auto s = file_->Append(data.AsStringView());
  if (s != SUCCESS) {
    return FAIL;
  }
  return file_->Sync();
}

KStatus TsVersionManager::RecordReader::ReadRecord(std::string *record, bool *eof) {
  static constexpr size_t kHeaderSize = sizeof(uint16_t) + sizeof(uint32_t);
  *eof = false;

  if (file_->IsEOF()) {
    *eof = true;
    return SUCCESS;
  }

  TsSliceGuard result;
  auto s = file_->Read(sizeof(kTsVersionMagicNumber), &result);
  if (s == FAIL) {
    return FAIL;
  }
  if (result.size() != sizeof(kTsVersionMagicNumber)) {
    *eof = true;
    return SUCCESS;
  }

  uint32_t magic_number = DecodeFixed32(result.data());
  if (magic_number != kTsVersionMagicNumber) {
    LOG_WARN("Invalid magic number, expect %x, actual %x, ignore following records", kTsVersionMagicNumber,
             magic_number);
    *eof = true;
    return SUCCESS;
  }

  s = file_->Read(kHeaderSize, &result);
  if (s == FAIL) {
    return FAIL;
  }
  if (result.size() != kHeaderSize) {
    LOG_WARN("Failed to read a full record header, ignore following records");
    *eof = true;
    return SUCCESS;
  }
  const char *ptr = result.data();
  uint32_t checksum = DecodeFixed16(ptr);
  ptr += sizeof(uint16_t);
  uint32_t size = DecodeFixed32(ptr);
  ptr += sizeof(uint32_t);

  file_->Read(size, &result);
  if (result.size() != size) {
    *eof = true;
    LOG_WARN("Failed to read a full record data, expect %u, actual %lu, ignore following records", size, result.size());
    return SUCCESS;
  }

  uint16_t tmp = 0;
  for (uint32_t i = 0; i < size; ++i) {
    tmp += static_cast<uint8_t>(result.data()[i]);
  }
  if (checksum != tmp) {
    LOG_ERROR("Checksum mismatch, expect %u, actual %u, ignore following records", checksum, tmp);
    *eof = true;
    return SUCCESS;
  }
  record->assign(result.data(), size);
  return SUCCESS;
}

KStatus TsVersionManager::VersionBuilder::AddUpdate(const TsVersionUpdate &update) {
  if (update.has_new_partition_) {
    all_updates_.has_new_partition_ = true;
    all_updates_.partitions_created_.insert(update.partitions_created_.begin(), update.partitions_created_.end());
  }

  if (update.has_new_lastseg_) {
    all_updates_.has_new_lastseg_ = true;
    for (const auto &[par_id, metas] : update.new_lastsegs_) {
      std::copy(metas.begin(), metas.end(), std::back_inserter(all_updates_.new_lastsegs_[par_id]));
    }
  }

  if (update.has_delete_lastseg_) {
    for (const auto &pair : update.delete_lastsegs_) {
      auto &par_id = pair.first;
      auto &file_number = pair.second;
      auto it = all_updates_.new_lastsegs_.find(par_id);
      assert(it != all_updates_.new_lastsegs_.end());
      it->second.erase(std::remove_if(it->second.begin(), it->second.end(),
                                      [&](const LastSegmentMetaInfo &meta) {
                                        return file_number.find(meta.file_number) != file_number.end();
                                      }),
                       it->second.end());
    }
  }

  if (update.has_entity_segment_) {
    all_updates_.has_entity_segment_ = true;
    for (auto [par_id, info] : update.entity_segment_) {
      all_updates_.entity_segment_[par_id] = info;
    }
  }

  if (update.has_next_file_number_) {
    all_updates_.has_next_file_number_ = true;
    all_updates_.next_file_number_ = update.next_file_number_;
  }

  if (update.has_max_lsn_) {
    all_updates_.has_max_lsn_ = true;
    all_updates_.max_lsn_ = std::max(all_updates_.max_lsn_, update.max_lsn_);
  }

  if (update.has_count_stats_) {
    all_updates_.has_count_stats_ = true;
    all_updates_.count_stats_status_ = CountStatsStatus::Recover;
    for (auto [par_id, info] : update.count_flush_infos_) {
      all_updates_.count_flush_infos_[par_id] = info;
    }
  }

  if (update.has_new_version_number_) {
    all_updates_.has_new_version_number_ = true;
    all_updates_.version_num_ = std::max(all_updates_.version_num_, update.version_num_);
  }
  return SUCCESS;
}

void TsVersionManager::VersionBuilder::Finalize(TsVersionUpdate *update) {
  update->has_new_partition_ = all_updates_.has_new_partition_;
  update->partitions_created_ = std::move(all_updates_.partitions_created_);

  update->has_new_lastseg_ = all_updates_.has_new_lastseg_;
  update->new_lastsegs_ = std::move(all_updates_.new_lastsegs_);

  update->has_delete_lastseg_ = all_updates_.has_delete_lastseg_;
  update->delete_lastsegs_ = std::move(all_updates_.delete_lastsegs_);

  update->has_entity_segment_ = all_updates_.has_entity_segment_;
  update->entity_segment_ = std::move(all_updates_.entity_segment_);

  update->has_next_file_number_ = all_updates_.has_next_file_number_;
  update->next_file_number_ = all_updates_.next_file_number_;

  update->has_max_lsn_ = all_updates_.has_max_lsn_;
  update->max_lsn_ = all_updates_.max_lsn_;

  update->has_count_stats_ = all_updates_.has_count_stats_;
  update->count_stats_status_ = all_updates_.count_stats_status_;
  update->count_flush_infos_ = std::move(all_updates_.count_flush_infos_);
  update->has_new_version_number_ = all_updates_.has_new_version_number_;
  update->version_num_ = all_updates_.version_num_;

  update->need_record_ = true;
}

static std::ostream &operator<<(std::ostream &os, const PartitionIdentifier &p) {
  auto [dbid, start_time, end_time] = p;
  os << "{" << dbid << "," << start_time << "}";
  return os;
}

static std::ostream &operator<<(std::ostream &os, const std::vector<LastSegmentMetaInfo> &info) {
  os << "(";
  for (auto it = info.begin(); it != info.end(); ++it) {
    os << it->file_number << ":" << it->level;
    if (std::next(it) != info.end()) {
      os << ",";
    }
  }
  os << ")";
  return os;
}

static std::ostream &operator<<(std::ostream &os, const std::set<uint64_t> &info) {
  os << "(";
  for (auto it = info.begin(); it != info.end(); ++it) {
    os << *it;
    if (std::next(it) != info.end()) {
      os << ",";
    }
  }
  os << ")";
  return os;
}

static std::ostream &operator<<(std::ostream &os, const EntitySegmentMetaInfo &info) {
  os << "[" << info.datablock_info.length << "," << info.header_b_info.length << "," << info.header_e_file_number << ","
     << info.agg_info.length << "]";
  return os;
}

std::string TsVersionUpdate::DebugStr() const {
  std::stringstream ss;
  ss << "update:";
  // if (has_mem_segments_) {
  //   ss << "mem_segments(" << valid_memseg_.size() << "):{";
  //   for (const auto &mem_segment : valid_memseg_) {
  //     ss << mem_segment.get() << " ";
  //   }
  //   ss << "};";
  // }
  for (auto par_id : updated_partitions_) {
    if (partitions_created_.find(par_id) != partitions_created_.end()) {
      ss << "+";
    }
    ss << par_id << ":{";
    {
      auto it = new_lastsegs_.find(par_id);
      if (it != new_lastsegs_.end()) {
        ss << "+" << it->second << ";";
      }
    }

    {
      auto it = delete_lastsegs_.find(par_id);
      if (it != delete_lastsegs_.end()) {
        ss << "-" << it->second << ";";
      }
    }

    {
      auto it = entity_segment_.find(par_id);
      if (it != entity_segment_.end()) {
        ss << it->second << ";";
      }
    }
    ss << "}";
  }
  return ss.str();
}

}  // namespace kwdbts
