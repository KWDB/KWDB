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
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "settings.h"
#include "ts_common.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_mem_segment_mgr.h"
#include "ts_segment.h"
#include "ts_entity_segment_handle.h"

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

class TsPartitionVersion {
  friend class TsVersionManager;
  friend class TsVGroupVersion;

 private:
  std::shared_ptr<MemSegList> valid_memseg_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;
  std::shared_ptr<TsEntitySegment> entity_segment_;

  PartitionIdentifier partition_info_;

  bool directory_created_ = false;
  bool memory_only_ = true;  // TODO(zzr): remove this field later
  std::shared_ptr<std::atomic<PartitionStatus>> exclusive_status_;

  std::shared_ptr<TsDelItemManager> del_info_;

  // Only TsVersionManager can create TsPartitionVersion
  explicit TsPartitionVersion(PartitionIdentifier partition_info)
      : partition_info_(partition_info),
        exclusive_status_(std::make_shared<std::atomic<PartitionStatus>>(PartitionStatus::None)) {}

 public:
  TsPartitionVersion(const TsPartitionVersion &) = default;
  TsPartitionVersion &operator=(const TsPartitionVersion &) = default;
  TsPartitionVersion(TsPartitionVersion &&) = default;

  std::vector<std::shared_ptr<TsSegmentBase>> GetAllSegments() const;

  bool HasDirectoryCreated() const { return directory_created_; }
  bool IsMemoryOnly() const { return memory_only_; }

  DatabaseID GetDatabaseID() const { return std::get<0>(partition_info_); }

  inline timestamp64 GetStartTime() const { return std::get<1>(partition_info_); }
  inline timestamp64 GetEndTime() const { return std::get<2>(partition_info_); }
  inline timestamp64 GetTsColTypeStartTime(DATATYPE ts_type) const {
    return convertSecondToPrecisionTS(GetStartTime(), ts_type);
  }
  inline timestamp64 GetTsColTypeEndTime(DATATYPE ts_type) const {
    return convertSecondToPrecisionTS(GetEndTime(), ts_type) - 1;
  }

  PartitionIdentifier GetPartitionIdentifier() const { return partition_info_; }

  std::string GetPartitionIdentifierStr() const {
    return intToString(std::get<0>(partition_info_)) + "_" + intToString(std::get<0>(partition_info_));
  }

  bool NeedCompact() const { return last_segments_.size() > EngineOptions::max_last_segment_num; }
  std::vector<std::shared_ptr<TsLastSegment>> GetCompactLastSegments() const;

  std::vector<std::shared_ptr<TsLastSegment>> GetAllLastSegments() const { return last_segments_; }
  std::shared_ptr<TsEntitySegment> GetEntitySegment() const { return entity_segment_; }
  std::list<std::shared_ptr<TsMemSegment>> GetAllMemSegments() const;

  // TODO(zzr): optimize the following function ralate to deletions, deletion should also be atomic in future, this is
  // just a temporary solution
  KStatus DeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans, const KwLSNSpan &lsn) const;
  KStatus UndoDeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans, const KwLSNSpan &lsn) const;
  KStatus HasDeleteItem(bool& has_delete_info, const KwLSNSpan &lsn) const;
  KStatus RmDeleteItems(const std::list<std::pair<TSEntityID, TS_LSN>>& entity_max_lsn) const;
  KStatus DropEntity(TSEntityID e_id) const;
  KStatus GetDelRange(TSEntityID e_id, std::list<STDelRange> &del_items) const {
    return del_info_->GetDelRange(e_id, del_items);
  }
  KStatus getFilter(const TsScanFilterParams& filter, TsBlockItemFilterParams& block_data_filter) const;
  KStatus GetBlockSpans(const TsScanFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>* ts_block_spans,
                       std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version,
                       bool skip_last = false, bool skip_entity = false) const;

  bool TrySetBusy(PartitionStatus desired) const;
  void ResetStatus() const;
  KStatus NeedVacuumEntitySegment(const std::filesystem::path& root_path, bool& need_vacuum) const;
};

class TsVGroupVersion {
  friend class TsVersionManager;
  friend class TsPartitionVersion;

 private:
  std::map<PartitionIdentifier, std::shared_ptr<const TsPartitionVersion>> partitions_;
  std::shared_ptr<MemSegList> valid_memseg_;

  TS_LSN max_lsn_ = 0;

 public:
  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitions(uint32_t dbid,
    const std::vector<KwTsSpan>& ts_spans, DATATYPE ts_type) const;

  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitionsToCompact() const;

  std::vector<std::shared_ptr<const TsPartitionVersion>> GetPartitionsToVacuum() const;

  // timestamp is in ptime
  std::shared_ptr<const TsPartitionVersion> GetPartition(uint32_t dbid, timestamp64 timestamp) const;

  std::map<uint32_t, std::vector<std::shared_ptr<const TsPartitionVersion>>> GetPartitions() const;

  TS_LSN GetMaxLSN() const { return max_lsn_; }
};

enum class VersionUpdateType : uint8_t {
  kNewPartition = 1,
  kDeletePartition = 2,

  kNewLastSegment = 3,
  kDeleteLastSegment = 4,

  kSetEntitySegment = 5,
  kNextFileNumber = 6,

  kMaxLSN = 7,
};

class TsVersionUpdate {
  friend class TsVersionManager;

 private:
  bool has_new_partition_ = false;
  std::set<PartitionIdentifier> partitions_created_;

  bool has_new_lastseg_ = false;
  std::map<PartitionIdentifier, std::set<uint64_t>> new_lastsegs_;

  bool has_delete_lastseg_ = false;
  std::map<PartitionIdentifier, std::set<uint64_t>> delete_lastsegs_;

  bool has_mem_segments_ = false;
  std::list<std::shared_ptr<TsMemSegment>> valid_memseg_;

  bool has_entity_segment_ = false;
  bool delete_all_prev_entity_segment_ = false;
  std::map<PartitionIdentifier, EntitySegmentHandleInfo> entity_segment_;

  bool has_next_file_number_ = false;
  uint64_t next_file_number_ = 0;

  bool has_max_lsn_ = false;
  TS_LSN max_lsn_ = 0;

  bool need_record_ = false;

  std::set<PartitionIdentifier> updated_partitions_;

  std::mutex mu_;

  bool NeedRecordFileNumber() const { return has_new_lastseg_ || has_entity_segment_ || has_delete_lastseg_; }
  bool NeedRecord() const { return need_record_; }
  bool MemSegmentsOnly() const {
    return has_mem_segments_ && !has_new_partition_ && !has_new_lastseg_ && !has_delete_lastseg_ &&
           !has_entity_segment_ && !has_next_file_number_;
  }

 public:
  bool Empty() const {
    return !(has_new_partition_ || has_new_lastseg_ || has_delete_lastseg_ || has_mem_segments_ ||
             has_entity_segment_ || has_max_lsn_);
  }

  void PartitionDirCreated(const PartitionIdentifier &partition_id) {
    partitions_created_.insert(partition_id);
    updated_partitions_.insert(partition_id);
    has_new_partition_ = true;
    need_record_ = true;
  }
  void AddLastSegment(const PartitionIdentifier &partition_id, uint64_t file_number) {
    std::unique_lock lk{mu_};
    updated_partitions_.insert(partition_id);
    new_lastsegs_[partition_id].insert(file_number);
    has_new_lastseg_ = true;
    need_record_ = true;
  }

  void SetMaxLSN(TS_LSN lsn) {
    max_lsn_ = std::max(max_lsn_, lsn);
    has_max_lsn_ = true;
    need_record_ = true;
  }

  void DeleteLastSegment(const PartitionIdentifier &partition_id, uint64_t file_number) {
    std::unique_lock lk{mu_};
    updated_partitions_.insert(partition_id);
    delete_lastsegs_[partition_id].insert(file_number);
    has_delete_lastseg_ = true;
    need_record_ = true;
  }

  void SetValidMemSegments(const std::list<std::shared_ptr<TsMemSegment>> &mem) {
    valid_memseg_ = mem;
    has_mem_segments_ = true;
  }

  void SetEntitySegment(const PartitionIdentifier &partition_id, EntitySegmentHandleInfo info, bool delete_all_prev_files) {
    std::unique_lock lk{mu_};
    updated_partitions_.insert(partition_id);
    entity_segment_[partition_id] = info;
    has_entity_segment_ = true;
    delete_all_prev_entity_segment_ = delete_all_prev_files;
    need_record_ = true;
  }

  void SetNextFileNumber(uint64_t file_number) {
    next_file_number_ = file_number;
    has_next_file_number_ = true;
    need_record_ = true;
  }

  std::string EncodeToString() const;
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

  std::filesystem::path root_path_;

  class Logger;
  std::unique_ptr<Logger> logger_;

  class RecordReader;
  class VersionBuilder;

 public:
  explicit TsVersionManager(TsIOEnv *env, const std::string &root_path) : env_(env), root_path_(root_path) {}
  KStatus Recover();
  KStatus ApplyUpdate(TsVersionUpdate *update);

  std::shared_ptr<const TsVGroupVersion> Current() const {
    std::shared_lock lk{mu_};
    return current_;
  }

  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1, std::memory_order_relaxed); }
  void AddPartition(DatabaseID dbid, timestamp64 start);
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
