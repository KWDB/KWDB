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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_common.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_mem_segment_mgr.h"
#include "ts_partition_interval_recorder.h"
#include "ts_segment.h"
#include "ts_entity_segment_handle.h"
#include "ts_partition_count_mgr.h"
#include "ts_partition_meta_mgr.h"

namespace kwdbts {
using DatabaseID = uint32_t;
using PartitionIdx = int64_t;
using PartitionIdentifier = std::tuple<DatabaseID, timestamp64, timestamp64>;  // (dbid, start_time, end_time);
using MemSegList = std::list<std::shared_ptr<TsMemSegment>>;

class TsVGroupVersion;
class TsEntitySegment;

enum class PartitionStatus : uint32_t {
  None = 0,
  Vacuuming,
  Compacting,
  BatchDataWriting,
};

enum class CountStatsStatus {
  FlushImmOrWriteBatch = 0,
  Recalculate,
  Recover,
  UpgradeRecover,
};

class TsPartitionVersion {
  friend class TsVersionManager;
  friend class TsVGroupVersion;

 public:
  class LastSegmentContainer {
   public:
    constexpr static int kMaxLevel = 3;
    static_assert(kMaxLevel >= 1);

   private:
    using Ptr = std::shared_ptr<TsLastSegment>;
    using Group = std::vector<Ptr>;
    std::array<std::vector<Group>, kMaxLevel> last_segments_;

    size_t size_ = 0;

    int level_to_compact_ = -1;
    int group_to_compact_ = -1;
    int current_candidate_size_ = 0;

   public:
    static uint32_t GetGroupSize(int level) {
      if (level == 0) {
        return 1;
      }
      if (level >= kMaxLevel) {
        return 0;
      }
      return 1 << (level + 1);
    }

    static uint32_t GetGroupByEntityID(int level, TSEntityID entity_id) {
      if (level == 0) {
        return 0;
      }
      if (level >= kMaxLevel) {
        return -1;
      }
      return (entity_id >> 9) % GetGroupSize(level);
    }

    LastSegmentContainer() {
      for (int level = 0; level < kMaxLevel; ++level) {
        last_segments_[level].resize(GetGroupSize(level));
      }
    }

    size_t GetLastSegmentsCount(int level, int group) const {
      if (level >= last_segments_.size() || group > GetGroupSize(level)) {
        return 0;
      }
      return last_segments_[level][group].size();
    }

    size_t Size() const { return size_; }

    void AddLastSegment(int level, int group, Ptr last_segment) {
      if (level >= kMaxLevel || group > GetGroupSize(level)) {
        return;
      }
      last_segments_[level][group].push_back(last_segment);
      size_++;

      if (level < level_to_compact_) {
        return;
      }

      int sz = last_segments_[level][group].size();
      if (level > 0 && sz > 1 && sz > current_candidate_size_) {
        level_to_compact_ = level;
        group_to_compact_ = group;
        current_candidate_size_ = sz;
      }

      if (level == 0 && sz > EngineOptions::max_last_segment_num) {
        level_to_compact_ = level;
        group_to_compact_ = group;
      }
    }

    auto GetLevelGroupToCompact() const { return std::make_tuple(level_to_compact_, group_to_compact_); }

    const std::vector<std::shared_ptr<TsLastSegment>> &GetLastSegments(int level, int group) const {
      return last_segments_[level][group];
    }

    std::vector<std::shared_ptr<TsLastSegment>> GetAllLastSegments() const {
      Group result;
      for (int level = 0; level < kMaxLevel; ++level) {
        for (int group = 0; group < GetGroupSize(level); ++group) {
          result.insert(result.end(), last_segments_[level][group].begin(), last_segments_[level][group].end());
        }
      }
      return result;
    }

    std::vector<uint32_t> GetAllLevelLastSegmentsCount() const {
      std::vector<uint32_t> result(kMaxLevel, 0);
      for (int level = 0; level < kMaxLevel; ++level) {
        for (int group = 0; group < GetGroupSize(level); ++group) {
          result[level] += last_segments_[level][group].size();
        }
      }
      return result;
    }

    int GetMaxLevel() const { return kMaxLevel; }
  };

 private:
  std::shared_ptr<MemSegList> valid_memseg_;
  std::shared_ptr<TsEntitySegment> entity_segment_;
  LastSegmentContainer leveled_last_segments_;

  fs::path partition_path_;
  PartitionIdentifier partition_info_;

  bool directory_created_ = false;
  bool flushed_ = false;  // TODO(zzr): remove this field later
  std::shared_ptr<std::atomic<PartitionStatus>> exclusive_status_;

  std::shared_ptr<TsDelItemManager> del_info_;
  std::shared_ptr<TsPartitionEntityCountManager> count_info_;
  // std::shared_ptr<TsPartitionEntityMetaManager> meta_info_;

  // Only TsVersionManager can create TsPartitionVersion
  explicit TsPartitionVersion(fs::path partition_path, PartitionIdentifier partition_info)
      : partition_path_(std::move(partition_path)),
        partition_info_(partition_info),
        exclusive_status_(std::make_shared<std::atomic<PartitionStatus>>(PartitionStatus::None)) {}

 public:
  TsPartitionVersion(const TsPartitionVersion &) = default;
  TsPartitionVersion &operator=(const TsPartitionVersion &) = default;
  TsPartitionVersion(TsPartitionVersion &&) = default;

  std::vector<std::shared_ptr<TsSegmentBase>> GetAllSegments() const;

  bool HasDirectoryCreated() const { return directory_created_; }
  bool HasFlushed() const { return flushed_; }

  DatabaseID GetDatabaseID() const { return std::get<0>(partition_info_); }

  inline timestamp64 GetStartTime() const { return std::get<1>(partition_info_); }
  inline timestamp64 GetEndTime() const { return std::get<2>(partition_info_); }
  inline timestamp64 GetTsColTypeStartTime(DATATYPE ts_type) const {
    return convertSecondToPrecisionTS(GetStartTime(), ts_type);
  }
  inline timestamp64 GetTsColTypeEndTime(DATATYPE ts_type) const {
    return convertSecondToPrecisionTS(GetEndTime(), ts_type) - 1;
  }

  fs::path GetPartitionPath() const { return partition_path_; }
  PartitionIdentifier GetPartitionIdentifier() const { return partition_info_; }

  std::string GetPartitionIdentifierStr() const {
    return intToString(std::get<1>(partition_info_)) + "_" + intToString(std::get<2>(partition_info_));
  }

  bool NeedCompact() const {
    auto [l, g] = leveled_last_segments_.GetLevelGroupToCompact();
    return l != -1;
  }
  std::vector<std::shared_ptr<TsLastSegment>> GetCompactLastSegments(int *level, int *group) const;

  std::vector<std::shared_ptr<TsLastSegment>> GetAllLastSegments() const {
    return leveled_last_segments_.GetAllLastSegments();
  }

  std::vector<uint32_t> GetAllLevelLastSegmentsCount() const {
    return leveled_last_segments_.GetAllLevelLastSegmentsCount();
  }

  int32_t GetLastSegmentsCount() const { return GetAllLastSegments().size(); }

  std::shared_ptr<TsEntitySegment> GetEntitySegment() const { return entity_segment_; }
  std::list<std::shared_ptr<TsMemSegment>> GetAllMemSegments() const;
  shared_ptr<TsPartitionEntityCountManager> GetCountManager() const { return count_info_; }

  // TODO(zzr): optimize the following function ralate to deletions, deletion should also be atomic in future, this is
  // just a temporary solution
  KStatus DeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans,
    const KwOSNSpan &lsn, bool user_del = true) const;
  KStatus UndoDeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans, const KwOSNSpan &lsn) const;
  KStatus HasDeleteItem(bool& has_delete_info, const KwOSNSpan &lsn) const;
  KStatus RmDeleteItems(const std::list<std::pair<TSEntityID, TS_OSN>>& entity_max_lsn) const;
  KStatus DropEntity(TSEntityID e_id) const;
  KStatus GetDelRange(TSEntityID e_id, std::list<STDelRange> &del_items) const {
    return del_info_->GetDelRange(e_id, del_items);
  }

  KStatus GetDelRangeByOSN(TSEntityID e_id, std::vector<KwOSNSpan>& osn_span, std::list<KwTsSpan>& del_range) const {
    return del_info_->GetDelRangeByOSN(e_id, osn_span, del_range);
  }

  KStatus GetDelRangeWithOSN(TSEntityID e_id, std::vector<KwOSNSpan>& osn_span,
      std::list<STDelRange>& del_range) const {
    return del_info_->GetDelRangeWithOSN(e_id, osn_span, del_range);
  }

  KStatus getFilter(const TsScanFilterParams& filter, TsBlockItemFilterParams& block_data_filter) const;
  KStatus GetBlockSpans(const TsScanFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>* ts_block_spans,
                       const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                       const std::shared_ptr<MMapMetricsTable>& scan_schema,
                       TsScanStats* ts_scan_stats = nullptr,
                       bool skip_mem = false, bool skip_last = false, bool skip_entity = false) const;

  bool TrySetBusy(PartitionStatus desired) const;
  void ResetStatus() const;
  KStatus NeedVacuumEntitySegment(const fs::path& root_path, TsEngineSchemaManager* schema_manager,
                                  bool force, bool& need_vacuum) const;

  KStatus GetDelMaxOSN(TSEntityID e_id, TS_OSN& max_osn) const {
    return del_info_->GetDelMaxOSN(e_id, max_osn);
  }
  bool ShouldSetCountStatsInvalid(TSEntityID e_id);
};

class TsVGroupVersion {
  friend class TsVersionManager;
  friend class TsPartitionVersion;

 private:
  const PartitionIntervalRecorder* recorder_;
  std::map<PartitionIdentifier, std::shared_ptr<const TsPartitionVersion>> partitions_;
  std::shared_ptr<MemSegList> valid_memseg_;

  uint64_t max_osn_ = 0;
  uint64_t version_num_ = 0;

 public:
  TsVGroupVersion()
      : valid_memseg_(std::make_shared<MemSegList>()) {
    recorder_ = PartitionIntervalRecorder::GetInstance();
  }

  ~TsVGroupVersion() {}

  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitions(uint32_t dbid,
                                                                       const std::vector<KwTsSpan> &ts_spans,
                                                                       DATATYPE ts_type) const;

  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitionsToCompact() const;

  // timestamp is in ptime
  std::shared_ptr<const TsPartitionVersion> GetPartition(uint32_t dbid, timestamp64 timestamp) const;

  std::shared_ptr<const TsPartitionVersion> GetPartition(PartitionIdentifier par_id) const;

  std::map<uint32_t, std::vector<std::shared_ptr<const TsPartitionVersion>>> GetPartitions() const;

  std::map<PartitionIdentifier, std::shared_ptr<const TsPartitionVersion>> GetAllPartitions() const {
    return partitions_;
  }
  std::vector<std::shared_ptr<const TsPartitionVersion>> GetDBAllPartitions(uint32_t target_dbid) const;

  TS_OSN GetMaxOSN() const { return max_osn_; }

  uint64_t GetVersionNumber() const { return version_num_; }
};

enum class VersionUpdateType : uint8_t {
  kNewPartition = 1,
  kDeletePartition = 2,

  kNewLastSegment = 3,  // deprecated, just for compatibility
  kDeleteLastSegment = 4,

  kSetEntitySegment = 5,
  kNextFileNumber = 6,

  kMaxLSN = 7,

  kNewLastSegmentWithMeta = 8,

  kNewCountStatFile = 9,
  kNewVersionNumber = 10,
};

enum class LastSegmentMetaType : uint8_t {
  kNone = 0,
  kFileNumber = 1,
  kLevel = 2,
  kGroup = 3,
};

class TsVersionUpdate {
  friend class TsVersionManager;

 private:
  bool has_new_partition_ = false;
  std::set<PartitionIdentifier> partitions_created_;

  bool has_new_lastseg_ = false;
  std::map<PartitionIdentifier, std::vector<LastSegmentMetaInfo>> new_lastsegs_;

  bool has_delete_lastseg_ = false;
  std::map<PartitionIdentifier, std::set<uint64_t>> delete_lastsegs_;

  // std::list<std::shared_ptr<TsMemSegment>> valid_memseg_;
  bool has_new_mem_segments_ = false;
  std::shared_ptr<TsMemSegment> new_memseg_;
  bool has_del_mem_segments_ = false;
  std::shared_ptr<TsMemSegment> del_memseg_;

  bool has_entity_segment_ = false;
  bool delete_all_prev_entity_segment_ = false;
  std::map<PartitionIdentifier, EntitySegmentMetaInfo> entity_segment_;

  bool has_next_file_number_ = false;
  uint64_t next_file_number_ = 0;

  bool has_max_lsn_ = false;
  TS_OSN max_lsn_ = 0;

  bool need_record_ = false;

  std::set<PartitionIdentifier> updated_partitions_;

  bool has_count_stats_ = false;
  CountStatsStatus count_stats_status_ = CountStatsStatus::FlushImmOrWriteBatch;
  bool has_new_version_number_ = false;
  uint64_t version_num_ = 0;
  std::map<PartitionIdentifier, CountStatMetaInfo> count_flush_infos_;

  bool NeedRecordFileNumber() const {
    return has_new_lastseg_ || has_entity_segment_ || has_delete_lastseg_ || has_count_stats_;
  }
  bool NeedRecord() const { return need_record_; }
  bool MemSegmentsOnly() const {
    return (has_new_mem_segments_ || has_del_mem_segments_) && !has_new_partition_ && !has_new_lastseg_ &&
           !has_delete_lastseg_ && !has_entity_segment_ && !has_next_file_number_ && !has_count_stats_;
  }

 public:
  bool Empty() const {
    return !(has_new_partition_ || has_new_lastseg_ || has_delete_lastseg_ || has_new_mem_segments_ ||
             has_del_mem_segments_ || has_entity_segment_ || has_max_lsn_ || has_count_stats_);
  }

  void PartitionDirCreated(const PartitionIdentifier &partition_id) {
    partitions_created_.insert(partition_id);
    updated_partitions_.insert(partition_id);
    has_new_partition_ = true;
    need_record_ = true;
  }
  void AddLastSegment(const PartitionIdentifier &partition_id, LastSegmentMetaInfo meta) {
    updated_partitions_.insert(partition_id);
    new_lastsegs_[partition_id].push_back(meta);
    has_new_lastseg_ = true;
    need_record_ = true;
  }

  void SetMaxLSN(TS_OSN lsn) {
    max_lsn_ = std::max(max_lsn_, lsn);
    has_max_lsn_ = true;
    need_record_ = true;
  }

  void DeleteLastSegment(const PartitionIdentifier &partition_id, uint64_t file_number) {
    updated_partitions_.insert(partition_id);
    delete_lastsegs_[partition_id].insert(file_number);
    has_delete_lastseg_ = true;
    need_record_ = true;
  }

  void RemoveMemSegment(std::shared_ptr<TsMemSegment> mem) {
    has_del_mem_segments_ = true;
    del_memseg_ = std::move(mem);
  }

  void AddMemSegment(std::shared_ptr<TsMemSegment> mem) {
    has_new_mem_segments_ = true;
    new_memseg_ = std::move(mem);
  }

  void SetEntitySegment(const PartitionIdentifier &partition_id, EntitySegmentMetaInfo info, bool delete_all_prev_files) {
    updated_partitions_.insert(partition_id);
    entity_segment_[partition_id] = info;
    has_entity_segment_ = true;
    delete_all_prev_entity_segment_ = delete_all_prev_files;
    need_record_ = true;
  }

  void GetEntitySegmentInfo(const PartitionIdentifier &partition_id, EntitySegmentMetaInfo *info) {
    assert(entity_segment_.count(partition_id) == 1);
    *info = entity_segment_[partition_id];
  }

  void SetNextFileNumber(uint64_t file_number) {
    next_file_number_ = file_number;
    has_next_file_number_ = true;
    need_record_ = true;
  }

  void AddCountFile(const PartitionIdentifier& partition_id, CountStatMetaInfo info) {
    updated_partitions_.insert(partition_id);
    count_flush_infos_[partition_id] = info;
    has_count_stats_ = true;
    need_record_ = true;
  }

  void SetCountStatsType(CountStatsStatus status) {
    count_stats_status_ = status;
  }

  void SetVersionNum(uint64_t version_num) {
    version_num_ = version_num;
  }

  TsBufferBuilder EncodeToString() const;
  KStatus DecodeFromSlice(TSSlice input);

  std::string DebugStr() const;
};

class TsVersionManager {
 private:
  TsIOEnv *env_;
  mutable std::shared_mutex mu_;
  mutable PartitionIdentifier last_created_partition_{-1, INVALID_TS, INVALID_TS};  // Initial as invalid

  std::shared_ptr<const TsVGroupVersion> current_;

  std::atomic<uint64_t> next_file_number_ = 0;
  std::atomic<uint64_t> version_num_ = 0;

  fs::path root_path_;

  class Logger;
  std::unique_ptr<Logger> logger_;
  PartitionIntervalRecorder* recorder_;

  class RecordReader;
  class VersionBuilder;

 public:
  explicit TsVersionManager(TsIOEnv *env, const std::string &root_path)
      : env_(env), root_path_(root_path) {
    recorder_ = PartitionIntervalRecorder::GetInstance();
  }
  KStatus Recover(bool force_recover);
  KStatus ApplyUpdate(TsVersionUpdate *update, bool force_apply = false);

  std::shared_ptr<const TsVGroupVersion> Current() const {
    std::shared_lock lk{mu_};
    return current_;
  }

  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1, std::memory_order_relaxed); }
  uint64_t CurrentVersionNum() { return version_num_.load(std::memory_order_relaxed); }
  KStatus AddPartition(DatabaseID dbid, timestamp64 start);
};

class TsVersionManager::Logger {
 private:
  std::unique_ptr<TsAppendOnlyFile> file_;

 public:
  explicit Logger(std::unique_ptr<TsAppendOnlyFile> &&file) : file_(std::move(file)) {}
  KStatus AddRecord(std::string_view);
};

class TsVersionManager::RecordReader {
 private:
  std::unique_ptr<TsSequentialReadFile> file_;

 public:
  explicit RecordReader(std::unique_ptr<TsSequentialReadFile> &&file) : file_(std::move(file)) {}
  KStatus ReadRecord(std::string *record, bool *eof);
};

class TsVersionManager::VersionBuilder {
 private:
  TsVersionUpdate all_updates_;

 public:
  KStatus AddUpdate(const TsVersionUpdate &update);
  void Finalize(TsVersionUpdate *update);
};

}  // namespace kwdbts
