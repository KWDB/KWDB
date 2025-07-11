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
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <utility>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <list>
#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "lg_impl.h"
#include "libkwdbts2.h"
#include "ts_coding.h"
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
static int64_t GetPartitionStartTime(timestamp64 timestamp, int64_t interval) {
  bool negative = timestamp < 0;
  timestamp64 tmp = timestamp + negative;
  int64_t index = tmp / interval;
  index -= negative;
  return index * interval;
}

void TsVersionManager::AddPartition(DatabaseID dbid, timestamp64 ptime) {
  timestamp64 start = GetPartitionStartTime(ptime, interval);
  PartitionIdentifier partition_id{dbid, start, start + interval};
  if (partition_id == this->last_created_partition_) {
    return;
  }
  {
    auto current = Current();
    assert(current!= nullptr);
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
  std::unique_ptr<TsPartitionVersion> partition(new TsPartitionVersion{partition_id});
  partition->valid_memseg_ = new_version->valid_memseg_;

  {
    // create directory for new partition
    // TODO(zzr): optimization: create the directory only when flushing
    // this logic is only for deletion and will be removed later after optimize delete
    auto partition_dir = root_path_ / PartitionDirName(partition_id);
    env_->NewDirectory(partition_dir);
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

KStatus TsVersionManager::Recover() {
  // based on empty version, recover from log file
  current_ = std::make_shared<TsVGroupVersion>();

  auto current_path = root_path_ / CurrentVersionName();
  if (!std::filesystem::exists(current_path)) {
    //  Brand new database, create current version
    uint64_t log_file_number = 0;
    auto update_path = root_path_ / VersionUpdateName(log_file_number);
    {
      std::unique_ptr<TsAppendOnlyFile> current_file;
      auto s = env_->NewAppendOnlyFile(current_path, &current_file);
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
    auto s = env_->NewAppendOnlyFile(update_path, &update_log_file);
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
  auto s = env_->NewRandomReadFile(root_path_ / CurrentVersionName(), &rfile);
  if (s == FAIL) {
    LOG_ERROR("can not open current version file");
    return FAIL;
  }
  size_t size = rfile->GetFileSize();
  TSSlice result;
  auto buf = std::make_unique<char[]>(size);
  s = rfile->Read(0, size, &result, buf.get());
  if (s == FAIL) {
    LOG_ERROR("can not read current version file");
    return FAIL;
  }
  std::string_view content(result.data, result.len);
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
    auto s =
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
  s = ApplyUpdate(&update);
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
    std::filesystem::rename(root_path_ / TempFileName(CurrentVersionName()), root_path_ / CurrentVersionName(), ec);
    if (ec.value() != 0) {
      LOG_ERROR("can not rename temp current version file, reason: %s", ec.message().c_str());
      return FAIL;
    }

    // the older log file is no longer needed, delete it
    // no need to check return value
    env_->DeleteFile(root_path_ / log_filename);
  }

  assert(current_ != nullptr);
  return SUCCESS;
}

KStatus TsVersionManager::ApplyUpdate(TsVersionUpdate *update) {
  if (update->Empty()) {
    // empty update, do nothing
    return SUCCESS;
  }

  // TODO(zzr): thinking concurrency control carefully.
  std::unique_lock lk{mu_};

  // Create a new vgroup version based on current version
  auto new_vgroup_version = std::make_unique<TsVGroupVersion>(*current_);

  if (update->has_next_file_number_) {
    assert(this->next_file_number_.load(std::memory_order_relaxed) == 0);
    this->next_file_number_.store(update->next_file_number_, std::memory_order_relaxed);
  }

  if (update->has_new_partition_) {
    for (const auto &p : update->partitions_created_) {
      if (new_vgroup_version->partitions_.find(p) != new_vgroup_version->partitions_.end()) {
        continue;
      }
      auto partition = std::unique_ptr<TsPartitionVersion>(new TsPartitionVersion{p});
      partition->del_info_ = std::make_shared<TsDelItemManager>(root_path_ / PartitionDirName(p));
      partition->del_info_->Open();
      new_vgroup_version->partitions_.insert({p, std::move(partition)});
    }
  }

  if (update->has_mem_segments_) {
    new_vgroup_version->valid_memseg_ = std::make_shared<MemSegList>(update->valid_memseg_);
  }
  // looping over all partitions
  for (auto [par_id, par] : new_vgroup_version->partitions_) {
    auto new_partition_version = std::make_unique<TsPartitionVersion>(*par);

    if (update->partitions_created_.find(par_id) != update->partitions_created_.end()) {
      new_partition_version->memory_only_ = false;
    }

    if (update->has_mem_segments_) {
      new_partition_version->valid_memseg_ = new_vgroup_version->valid_memseg_;
    }

    if (update->partitions_created_.find(par_id) != update->partitions_created_.end()) {
      new_partition_version->directory_created_ = true;
    }

    // Process lastsegment deletion, used by Compact()
    {
      auto it = update->delete_lastsegs_.find(par_id);
      if (it != update->delete_lastsegs_.end()) {
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

    auto partition_dir = root_path_ / PartitionDirName(par_id);
    // Process lastsegment creation, used by Flush() and Compact()
    {
      // TODO(zzr): Lazy open? add something like `TsLastSegmentHandler` to do this.
      auto it = update->new_lastsegs_.find(par_id);
      if (it != update->new_lastsegs_.end()) {
        for (auto file_number : it->second) {
          std::unique_ptr<TsRandomReadFile> rfile;
          auto filepath = partition_dir / LastSegmentFileName(file_number);
          auto s = env_->NewRandomReadFile(filepath, &rfile);
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
    auto it = update->entity_segment_.find(par_id);
    if (it != update->entity_segment_.end()) {
      std::string root = root_path_ / PartitionDirName(par_id);
      if (new_partition_version->entity_segment_) {
        new_partition_version->entity_segment_->MarkDeleteEntityHeader();
      }
      new_partition_version->entity_segment_ = std::make_unique<TsEntitySegment>(root, it->second);
    }

    // VGroupVersion accepts the new partition version
    new_vgroup_version->partitions_[par_id] = std::move(new_partition_version);
  }

  if (update->NeedRecordFileNumber()) {
    update->SetNextFileNumber(this->next_file_number_.load(std::memory_order_relaxed));
  }

  std::string encoded_update = update->EncodeToString();
  if (!encoded_update.empty()) {
    logger_->AddRecord(encoded_update);
  }
  current_ = std::move(new_vgroup_version);
  LOG_INFO("%s: %s", this->root_path_.filename().c_str(), update->DebugStr().c_str());
  return SUCCESS;
}

std::vector<std::shared_ptr<const TsPartitionVersion>> TsVGroupVersion::GetPartitions(uint32_t target_dbid,
  const std::vector<KwTsSpan>& ts_spans, DATATYPE ts_type) const {
  std::vector<std::shared_ptr<const TsPartitionVersion>> result;
  for (const auto &[k, v] : partitions_) {
    const auto &[dbid, _, __] = k;
    if (dbid == target_dbid) {
      // check if current partition is cross with ts_spans.
      if (isTimestampInSpans(ts_spans, v->GetTsColTypeStartTime(ts_type), v->GetTsColTypeEndTime(ts_type))) {
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
  timestamp64 start = GetPartitionStartTime(target_time, interval);
  auto it = partitions_.find({target_dbid, start, start + interval});
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
KStatus TsPartitionVersion::UndoDeleteData(TSEntityID e_id, const std::vector<KwTsSpan> &ts_spans,
                                           const KwLSNSpan &lsn) const {
  auto s = del_info_->RollBackDelItem(e_id, lsn);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBackDelItem failed. for entity[%lu]", e_id);
    return s;
  }
  return KStatus::SUCCESS;
}
KStatus TsPartitionVersion::getFilter(const TsScanFilterParams& filter, TsBlockItemFilterParams& block_data_filter) const {
  std::list<STDelRange> del_range_all;
  auto s = GetDelRange(filter.entity_id, del_range_all);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelRange failed.");
    return s;
  }
  // filter delitems that insert after scannig.
  std::list<STDelRange> del_range;
  for (STDelRange& d_item : del_range_all) {
    if (d_item.lsn_span.end <= filter.end_lsn) {
      del_range.push_back(d_item);
    }
  }
  KwTsSpan partition_span;
  partition_span.begin = convertSecondToPrecisionTS(GetStartTime(), filter.table_ts_type);
  partition_span.end = convertSecondToPrecisionTS(GetEndTime(), filter.table_ts_type) - 1;
  std::vector<STScanRange> cur_scan_range;
  for (auto& scan : filter.ts_spans_) {
    KwTsSpan cross_part;
    cross_part.begin = std::max(partition_span.begin, scan.begin);
    cross_part.end = std::min(partition_span.end, scan.end);
    if (cross_part.begin <= cross_part.end) {
      cur_scan_range.push_back(STScanRange(cross_part, {0, filter.end_lsn}));
    }
  }
  for (auto& del : del_range) {
    cur_scan_range = LSNRangeUtil::MergeScanAndDelRange(cur_scan_range, del);
  }
  block_data_filter.spans_ = std::move(cur_scan_range);
  block_data_filter.db_id = filter.db_id;
  block_data_filter.entity_id = filter.entity_id;
  block_data_filter.vgroup_id = filter.vgroup_id;
  block_data_filter.table_id = filter.table_id;
  return KStatus::SUCCESS;
}

KStatus TsPartitionVersion::GetBlockSpan(const TsScanFilterParams& filter,
std::list<shared_ptr<TsBlockSpan>>* ts_block_spans,
std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version, bool skip_last, bool skip_entity) const {
  TsBlockItemFilterParams block_data_filter;
  auto s = getFilter(filter, block_data_filter);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getFilter failed.");
    return s;
  }

  ts_block_spans->clear();
  // get block span in mem segment
  for (auto& mem : GetAllMemSegments()) {
    auto s = mem->GetBlockSpans(block_data_filter, *ts_block_spans, tbl_schema_mgr, scan_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans of mem segment failed.");
      return s;
    }
  }
  if (!skip_last) {
    // get block span in last segment
    std::vector<std::shared_ptr<TsLastSegment>> last_segs = GetAllLastSegments();
    for (auto& last_seg : last_segs) {
      auto s = last_seg->GetBlockSpans(block_data_filter, *ts_block_spans, tbl_schema_mgr, scan_version);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpans of mem segment failed.");
        return s;
      }
    }
  }
  if (!skip_entity) {
    // get block span in entity segment
    auto entity_segment = GetEntitySegment();
    if (entity_segment == nullptr) {
      // entity segment not exist
      return KStatus::SUCCESS;
    }
    auto s = entity_segment->GetBlockSpans(block_data_filter, *ts_block_spans,
             tbl_schema_mgr, scan_version);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans of mem segment failed.");
      return s;
    }
  }
  LOG_DEBUG("reading block span num [%lu]", ts_block_spans->size());
  return KStatus::SUCCESS;
}


// version update

inline void EncodePartitionID(std::string *result, const PartitionIdentifier &partition_id) {
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

inline void EncodePartitonFiles(std::string *result, const std::map<PartitionIdentifier, std::set<uint64_t>> &files) {
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

inline const char *DecodePartitonFiles(const char *ptr, const char *limit,
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

inline void EncodeEntitySegment(
    std::string *result,
    const std::map<PartitionIdentifier, TsVersionUpdate::EntitySegmentVersionInfo> &entity_segments) {
  uint32_t npartition = entity_segments.size();
  PutVarint32(result, npartition);
  for (const auto &[par_id, info] : entity_segments) {
    EncodePartitionID(result, par_id);
    PutVarint64(result, info.block_file_size);
    PutVarint64(result, info.header_b_size);
    PutVarint64(result, info.header_e_file_number);
    PutVarint64(result, info.agg_file_size);
  }
}

const char *DecodeEntitySegment(const char *ptr, const char *limit,
                                std::map<PartitionIdentifier, TsVersionUpdate::EntitySegmentVersionInfo> *entity_segments) {
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
    TsVersionUpdate::EntitySegmentVersionInfo info;
    ptr = DecodeVarint64(ptr, limit, &info.block_file_size);
    ptr = DecodeVarint64(ptr, limit, &info.header_b_size);
    ptr = DecodeVarint64(ptr, limit, &info.header_e_file_number);
    ptr = DecodeVarint64(ptr, limit, &info.agg_file_size);
    if (ptr == nullptr) {
      return nullptr;
    }
    entity_segments->insert_or_assign(par_id, info);
  }
  return ptr;
}

std::string TsVersionUpdate::EncodeToString() const {
  std::string result;
  if (has_new_partition_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNewPartition));
    uint32_t npartition = partitions_created_.size();
    PutVarint32(&result, npartition);
    for (const auto &par_id : partitions_created_) {
      EncodePartitionID(&result, par_id);
    }
  }
  if (has_new_lastseg_) {
    result.push_back(static_cast<char>(VersionUpdateType::kNewLastSegment));
    EncodePartitonFiles(&result, new_lastsegs_);
  }

  if (has_delete_lastseg_) {
    result.push_back(static_cast<char>(VersionUpdateType::kDeleteLastSegment));
    EncodePartitonFiles(&result, delete_lastsegs_);
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

      case VersionUpdateType::kNewLastSegment: {
        ptr = DecodePartitonFiles(ptr, end, &this->new_lastsegs_);
        if (ptr == nullptr) {
          LOG_ERROR("Corrupted version update slice");
          return FAIL;
        }
        this->has_new_lastseg_ = true;
        break;
      }
      case VersionUpdateType::kDeleteLastSegment: {
        ptr = DecodePartitonFiles(ptr, end, &this->delete_lastsegs_);
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
  std::string data;
  PutFixed32(&data, kTsVersionMagicNumber);
  PutFixed16(&data, checksum);
  PutFixed32(&data, record.size());
  data.append(record);
  auto s = file_->Append(data);
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

  TSSlice result;
  auto buf = std::make_unique<char[]>(sizeof(kTsVersionMagicNumber));
  auto s = file_->Read(sizeof(kTsVersionMagicNumber), &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }
  if (result.len != sizeof(kTsVersionMagicNumber)) {
    *eof = true;
    return SUCCESS;
  }

  uint32_t magic_number = DecodeFixed32(result.data);
  if (magic_number != kTsVersionMagicNumber) {
    LOG_WARN("Invalid magic number, expect %x, actual %x, ignore following records", kTsVersionMagicNumber,
             magic_number);
    *eof = true;
    return SUCCESS;
  }

  buf = std::make_unique<char[]>(kHeaderSize);
  s = file_->Read(kHeaderSize, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }
  if (result.len != kHeaderSize) {
    LOG_WARN("Failed to read a full record header, ignore following records");
    *eof = true;
    return SUCCESS;
  }
  const char *ptr = result.data;
  uint32_t checksum = DecodeFixed16(ptr);
  ptr += sizeof(uint16_t);
  uint32_t size = DecodeFixed32(ptr);
  ptr += sizeof(uint32_t);

  buf = std::make_unique<char[]>(size);
  s = file_->Read(size, &result, buf.get());
  if (s == FAIL) {
    return FAIL;
  }
  if (result.len != size) {
    *eof = true;
    LOG_WARN("Failed to read a full record data, expect %u, actual %lu, ignore following records", size, result.len);
    return SUCCESS;
  }

  uint16_t tmp = 0;
  for (uint32_t i = 0; i < size; ++i) {
    tmp += static_cast<uint8_t>(result.data[i]);
  }
  if (checksum != tmp) {
    LOG_ERROR("Checksum mismatch, expect %u, actual %u, ignore following records", checksum, tmp);
    *eof = true;
    return SUCCESS;
  }
  record->assign(result.data, size);
  return SUCCESS;
}

KStatus TsVersionManager::VersionBuilder::AddUpdate(const TsVersionUpdate &update) {
  if (update.has_new_partition_) {
    all_updates_.has_new_partition_ = true;
    all_updates_.partitions_created_.insert(update.partitions_created_.begin(), update.partitions_created_.end());
  }

  if (update.has_new_lastseg_) {
    all_updates_.has_new_lastseg_ = true;
    for (const auto &[par_id, file_numbers] : update.new_lastsegs_) {
      all_updates_.new_lastsegs_[par_id].insert(file_numbers.begin(), file_numbers.end());
    }
  }

  if (update.has_delete_lastseg_) {
    for (const auto &[par_id, file_numbers] : update.delete_lastsegs_) {
      auto it = all_updates_.new_lastsegs_.find(par_id);
      assert(it != all_updates_.new_lastsegs_.end());
      for (uint64_t file_number : file_numbers) {
        assert(it->second.find(file_number) != it->second.end());
        it->second.erase(file_number);
      }
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
}

static std::ostream &operator<<(std::ostream &os, const PartitionIdentifier &p) {
  auto [dbid, start_time, end_time] = p;
  os << "{" << dbid << "," << start_time << "}";
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

static std::ostream &operator<<(std::ostream &os, const TsVersionUpdate::EntitySegmentVersionInfo &info) {
  os << "[" << info.block_file_size << "," << info.header_b_size << "," << info.header_e_file_number << ","
     << info.agg_file_size << "]";
  return os;
}

std::string TsVersionUpdate::DebugStr() const {
  std::stringstream ss;
  ss << "update:";
  if (has_mem_segments_) {
    ss << "mem_segments(" << valid_memseg_.size() << "):{";
    for (const auto &mem_segment : valid_memseg_) {
      ss << mem_segment.get() << " ";
    }
    ss << "};";
  }
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
