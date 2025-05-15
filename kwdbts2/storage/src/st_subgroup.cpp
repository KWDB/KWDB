// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <lg_api.h>
#include <dirent.h>
#include <map>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include "st_subgroup.h"
#include "st_config.h"
#include "sys_utils.h"
#include "mmap/mmap_metrics_table.h"
#include "lt_rw_latch.h"
#include "st_tier_manager.h"

namespace kwdbts {

TsSubEntityGroup::TsSubEntityGroup(MMapRootTableManager*& root_tbl_manager)
    : TSObject(), root_tbl_manager_(root_tbl_manager) {
  table_name_ = root_tbl_manager->GetTableName();
  // pthread_rwlock_init(&partition_lk_, NULL);

  sub_entity_group_mutex_ = new TsSubEntityGroupLatch(LATCH_ID_TSSUBENTITY_GROUP_MUTEX);
  sub_entity_group_rwlock_ = new TsSubEntityGroupRWLatch(RWLATCH_ID_TS_SUB_ENTITY_GROUP_RWLOCK);
  subgroup_id_ = 0;
}

TsSubEntityGroup::~TsSubEntityGroup() {
  partition_cache_.Clear();
  // pthread_rwlock_destroy(&partition_lk_);
  if (entity_block_meta_ != nullptr) {
    delete entity_block_meta_;
    entity_block_meta_ = nullptr;
  }

  for (auto& mtx : entity_mutexes_) {
    delete mtx;
  }
  if (sub_entity_group_mutex_ != nullptr) {
    delete sub_entity_group_mutex_;
    sub_entity_group_mutex_ = nullptr;
  }
  if (sub_entity_group_rwlock_ != nullptr) {
    delete sub_entity_group_rwlock_;
    sub_entity_group_rwlock_ = nullptr;
  }
}

int TsSubEntityGroup::rdLock() {
  return RW_LATCH_S_LOCK(sub_entity_group_rwlock_);
}

int TsSubEntityGroup::wrLock() {
  return RW_LATCH_X_LOCK(sub_entity_group_rwlock_);
}

int TsSubEntityGroup::unLock() {
  return RW_LATCH_UNLOCK(sub_entity_group_rwlock_);
}


std::vector <timestamp64> TsSubEntityGroup::GetPartitionTsInSpan(KwTsSpan ts_span) {
  vector<timestamp64> parts;
  rdLock();
  Defer defer{[&]() { unLock(); }};
  timestamp64 tmp_max_ts;
  auto type = root_tbl_manager_->GetTsColDataType();
  auto span_begin_second = convertTsToPTime(ts_span.begin, type);
  auto span_end_second = convertTsToPTime(ts_span.end, type);

  auto start_it = partitions_ts_.find(PartitionTime(span_begin_second, tmp_max_ts));
  if (start_it == partitions_ts_.end()) {
    start_it = partitions_ts_.begin();
  }

  // To include ts_span.end, so the next partition must be the endpoint
  auto end_it = partitions_ts_.find(PartitionTime(span_end_second, tmp_max_ts));
  if (end_it != partitions_ts_.end()) {
    // ts_span.end fall into partition, so we take the next partition as endpoint
    end_it++;
  }
  for (auto it = start_it; it != end_it ; it++) {
    parts.push_back(it->first);
  }
  return parts;
}

int TsSubEntityGroup::ClearPartitionCache() {
  partition_cache_.Clear();
  return 0;
}

int TsSubEntityGroup::ErasePartitionCache(timestamp64 pt_ts) {
  partition_cache_.Erase(pt_ts);
  return 0;
}

int TsSubEntityGroup::ReOpenInit(ErrorInfo& err_info) {
  uint16_t max_entities_per_subgroup = GetMaxEntitiesPerSubgroup();
  if (entity_block_meta_ != nullptr) {
    max_entities_per_subgroup = entity_block_meta_->GetConfigSubgroupEntities();
    delete entity_block_meta_;
    entity_block_meta_ = nullptr;
  }
  partitions_ts_.clear();
  return OpenInit(subgroup_id_, db_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, max_entities_per_subgroup, err_info);
}

int TsSubEntityGroup::OpenInit(SubGroupID subgroup_id, const std::string& db_path, const string& tbl_sub_path,
                               int flags, uint16_t max_entities_per_subgroup, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (entity_block_meta_ != nullptr) {
    // err_info.setError(BOEPERM, tbl_sub_path + " already opened.");
    LOG_WARN("SubEntityGroup[%s] already opened.", tbl_sub_path.c_str());
    return 0;
  }
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  real_path_ = db_path_ + tbl_sub_path_;

  struct stat buffer;
  if (stat(real_path_.c_str(), &buffer) != 0) {
    if (flags & O_CREAT) {
      // If it is CREATE but the absolute path does not exist, create it
      if (!MakeDirectory(real_path_, err_info)) {
        return err_info.errcode;
      }
    } else {
      err_info.setError(KWENOOBJ, real_path_);
      return err_info.errcode;
    }
  }
  subgroup_id_ = subgroup_id;

  entity_block_meta_ = new MMapEntityBlockMeta(true, true);
  string meta_path = table_name_ + ".et";
  int ret = entity_block_meta_->init(meta_path, db_path_, tbl_sub_path_, flags, false, max_entities_per_subgroup);
  if (ret < 0) {
    err_info.setError(ret, tbl_sub_path);
    return ret;
  }
  // Load all partition directories
  DIR* dir_ptr = opendir(real_path_.c_str());
  if (dir_ptr) {
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      std::string full_path = real_path_ + entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        err_info.setError(KWENFILE, "stat[" + full_path + "] failed");
        closedir(dir_ptr);
        return err_info.errcode;
      }
      if (!S_ISDIR(file_stat.st_mode)) {
        continue;
      }
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
      }
      // Check if there is a directory with partition time
      std::string partition_dir = entry->d_name;
      int64_t p_ts = convertToTimestamp(partition_dir);
      if (strcmp(partition_dir.c_str(), std::to_string(p_ts).c_str()) == 0) {
        // Load the partition table and obtain the minimum and maximum timestamps for the partition
        TsTimePartition* p_table = getPartitionTable(p_ts, p_ts, err_info, false);
        if (err_info.errcode == KWEDROPPEDOBJ || err_info.errcode == KWEUNINIT) {
          err_info.clear();
          continue;
        }
        if (err_info.errcode < 0) {
          delete p_table;
          break;
        }
        ReleaseTable(p_table);
      }
    }
    closedir(dir_ptr);
  }

  if (err_info.errcode < 0) {
    partitions_ts_.clear();
  }

  for (int i = 0; i <= entity_block_meta_->GetConfigSubgroupEntities(); i++) {
    entity_mutexes_.push_back(std::move(new TsSubEntityGroupEntityLatch(LATCH_ID_TSSUBENTITY_GROUP_ENTITYS_MUTEX)));
  }

  return err_info.errcode;
}

EntityID TsSubEntityGroup::AllocateEntityID(const string& primary_tag, ErrorInfo& err_info) {
  assert(entity_block_meta_ != nullptr);
  return entity_block_meta_->addEntity();
}

std::vector<uint32_t> TsSubEntityGroup::GetEntities() {
  assert(entity_block_meta_ != nullptr);
  return entity_block_meta_->getEntities();
}

uint16_t TsSubEntityGroup::GetSubgroupEntities() {
  return entity_block_meta_->GetConfigSubgroupEntities();
}

int TsSubEntityGroup::SetEntityNum(uint32_t entity_num) {
  return -1;
}

timestamp64 TsSubEntityGroup::PartitionTime(timestamp64 ts, timestamp64& max_ts) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  int64_t interval = root_tbl_manager_->GetPartitionInterval();
  if (!partitions_ts_.empty()) {
    auto it = --partitions_ts_.end();
    if (ts >= it->first && ts <= it->second) {  // Latest partition
      max_ts = it->second;
      return it->first;
    } else if (ts > it->second) {  // Create a new partition
      timestamp64 min_ts;
      partitionTime(ts, it->second + 1, interval, min_ts, max_ts);
      return min_ts;
    }
  } else {  // Create a new partition
    timestamp64 min_ts;
    partitionTime(ts, 0, interval, min_ts, max_ts);
    return min_ts;
  }
  // Found the first partition larger than ts
  auto it = partitions_ts_.upper_bound(ts);
  int64_t begin_ts = it->first;
  int64_t min_start_ts = 0;
  if (it != partitions_ts_.begin()) {
    --it;
    if (it->second >= ts) {
      max_ts = it->second;
      return it->first;
    } else {
      min_start_ts = it->second + 1;
    }
  }
  // The previous partition does not contain ts,
  // create a new partition and ensure that the min_ts of the partition is not less than the max_ts+1
  // of the previous partition
  timestamp64 min_ts;
  partitionTime(ts, begin_ts, interval, min_ts, max_ts);
  if (min_start_ts != 0 && min_ts < min_start_ts) {
    min_ts = min_start_ts;
  }
  return min_ts;
}

timestamp64 TsSubEntityGroup::MinPartitionTime() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.size() > 0) {
    return partitions_ts_.begin()->first;
  }
  return 0;
}

timestamp64 TsSubEntityGroup::MaxPartitionTime() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.size() > 0) {
    return (--partitions_ts_.end())->first;
  }
  return INVALID_TS;
}

vector<timestamp64> TsSubEntityGroup::GetPartitions(const KwTsSpan& ts_span) {
  std::vector<int64_t> p_times;
  timestamp64 max_ts;
  timestamp64 pts_begin = PartitionTime(ts_span.begin, max_ts);
  timestamp64 pts_end = PartitionTime(ts_span.end, max_ts);
  rdLock();
  Defer defer{[&]() { unLock(); }};
  for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
    if (!(it->first > pts_end || it->second <= pts_begin)) {
      p_times.emplace_back(it->first);
    }
  }
  return p_times;
}

TsTimePartition* TsSubEntityGroup::GetPartitionTable(timestamp64 ts, ErrorInfo& err_info,
                                                     bool create_if_not_exist, bool lru_push_back) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  TsTimePartition* mt_table = partition_cache_.Get(p_time);
  if (mt_table != nullptr) {
    return mt_table;
  }

  if (deleted_partitions_.find(p_time) != deleted_partitions_.end()) {
    LOG_WARN("Partition is deleted.");
    err_info.setError(KWEDROPPEDOBJ, "No such partition:" + db_path_ + partitionTblSubPath(ts));
    return nullptr;
  }

  wrLock();
  Defer defer{[&]() { unLock(); }};
  // After locking, call the internal method to obtain the table instance
  err_info.clear();
  mt_table = getPartitionTable(p_time, max_ts, err_info, create_if_not_exist, lru_push_back);
  return mt_table;
}

vector<TsTimePartition*> TsSubEntityGroup::GetPartitionTables(const KwTsSpan& ts_span, ErrorInfo& err_info) {
  std::vector<int64_t> p_times;
  auto type = root_tbl_manager_->GetTsColDataType();
  {
    // First, traverse the partitions that meet the criteria: the start and end ranges of the partitions are within ts_span
    timestamp64 max_ts;
    timestamp64 pts_begin = PartitionTime(convertTsToPTime(ts_span.begin, type), max_ts);
    timestamp64 pts_end = PartitionTime(convertTsToPTime(ts_span.end, type), max_ts);
    rdLock();
    Defer defer{[&]() { unLock(); }};
    for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
      if (!(it->first > pts_end || it->second <= pts_begin)) {
        p_times.emplace_back(it->first);
      }
    }
  }
  vector<TsTimePartition*> results;
  for (int i = 0 ; i < p_times.size() ; i++) {
    wrLock();
    Defer defer{[&]() { unLock(); }};
    TsTimePartition* p_table = getPartitionTable(p_times[i], 0, err_info, false, false);
    if (!err_info.isOK() && err_info.errcode != KWEDROPPEDOBJ) {
      break;
    }
    results.emplace_back(p_table);
    err_info.clear();
  }

  if (!err_info.isOK()) {
    // If there is an error in obtaining the partition table, release the already successful partition table instance
    for (int i = 0 ; i < results.size() ; i++) {
      ReleaseTable(results[i]);
    }
    results.clear();
  }
  return std::move(results);
}

std::shared_ptr<TsSubGroupPTIterator> TsSubEntityGroup::GetPTIterator(const std::vector<KwTsSpan>& ts_spans) {
  std::vector<timestamp64> p_times;
  {
    std::vector<AttributeInfo> schema;
    auto ts_type = root_tbl_manager_->GetTsColDataType();
    // filter all partition time that match ts_spans.
    timestamp64 max_ts, pts_begin, pts_end;
    rdLock();
    for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
      for (auto& ts_span : ts_spans) {
        pts_begin = convertTsToPTime(ts_span.begin, ts_type);
        pts_end = convertTsToPTime(ts_span.end, ts_type);
        if (!(it->first > pts_end || it->second <= pts_begin)) {
          p_times.emplace_back(it->first);
          break;
        }
      }
    }
    unLock();
  }
  std::shared_ptr<TsSubGroupPTIterator> ret = std::make_shared<TsSubGroupPTIterator>(this, p_times);
  return std::move(ret);
}

KStatus TsSubGroupPTIterator::Next(TsTimePartition** p_table) {
  *p_table = nullptr;
  while (true) {
    if (cur_p_table_ != nullptr) {
      cur_p_table_->unLock();
      ReleaseTable(cur_p_table_);
      cur_p_table_ = nullptr;
    }
    timestamp64 cur_partition;
    if (reverse_traverse_) {
      if (cur_partition_idx_ < 0) {
        // scan over.
        return KStatus::SUCCESS;
      }
      cur_partition = partitions_[cur_partition_idx_--];
    } else {
      if (cur_partition_idx_ >= partitions_.size()) {
        // scan over.
        return KStatus::SUCCESS;
      }
      cur_partition = partitions_[cur_partition_idx_++];
    }
    ErrorInfo err_info;
    cur_p_table_ = sub_eg_->GetPartitionTable(cur_partition, err_info);
    if (err_info.errcode == KWEDROPPEDOBJ) {
      cur_partition_idx_++;
      err_info.clear();
      continue;
    }
    if (!err_info.isOK()) {
      LOG_ERROR("can not parse partition [%lu] in subgroup [%u].", cur_partition, sub_eg_->GetID());
      return KStatus::FAIL;
    }
    break;
  }
  *p_table = cur_p_table_;
  cur_p_table_->rdLock();
  return KStatus::SUCCESS;
}

KStatus TsEntityGroupPTIterator::Next(uint32_t subgroup_id, TsTimePartition** p_table) {
  KStatus s = partition_table_iter_[subgroup_id]->Next(p_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("failed get next partition");
    return KStatus::FAIL;
  }
  cur_p_table_[subgroup_id] = *p_table;
  return KStatus::SUCCESS;
}

TsTimePartition* TsSubEntityGroup::CreateTmpPartitionTable(string p_name, size_t version, bool& created) {
  string pt_tbl_sub_path = tbl_sub_path_ + p_name;
  ErrorInfo err_info;
  auto mt_table = new TsTimePartition(root_tbl_manager_, entity_block_meta_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode >= 0) {
    LOG_DEBUG("open temp partiton table [%s] success.", pt_tbl_sub_path.c_str());
    created = false;
    return mt_table;
  }
  err_info.clear();
  delete mt_table;
    // Create partition directory
  if (!MakeDirectory(db_path_ + pt_tbl_sub_path, err_info)) {
    LOG_ERROR("can not mkdir for %s", (db_path_ + pt_tbl_sub_path).c_str());
    return nullptr;
  }
  mt_table = new TsTimePartition(root_tbl_manager_, entity_block_meta_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  if (!err_info.isOK()) {
    LOG_ERROR("can not open partition for %s", (db_path_ + pt_tbl_sub_path).c_str());
    delete mt_table;
    return nullptr;
  }
  created = true;
  return mt_table;
}

TsTimePartition* TsSubEntityGroup::getPartitionTable(timestamp64 p_time, timestamp64 max_ts, ErrorInfo& err_info,
                                                      bool create_if_not_exist, bool lru_push_back) {
  TsTimePartition* mt_table = partition_cache_.Get(p_time);
  if (mt_table != nullptr) {
    return mt_table;
  }
  string pt_tbl_sub_path = partitionTblSubPath(p_time);
  mt_table = new TsTimePartition(root_tbl_manager_, entity_block_meta_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode == KWEDROPPEDOBJ || err_info.errcode == KWEUNINIT) {
    delete mt_table;
    mt_table = nullptr;
    removePartitionDir(db_path_, pt_tbl_sub_path);
    if (!create_if_not_exist) {
      return nullptr;
    }
  }
  if (err_info.errcode < 0 && create_if_not_exist) {
    err_info.clear();
    delete mt_table;
    mt_table = createPartitionTable(pt_tbl_sub_path, p_time, max_ts, err_info);
  }
  if (err_info.errcode < 0) {
    delete mt_table;
    return nullptr;
  }

  // Add cache after adding references to prevent being eliminated by cache
  mt_table->incRefCount();
  partitions_ts_[mt_table->minTimestamp()] = mt_table->maxTimestamp();
  partition_cache_.Put(p_time, mt_table, lru_push_back);
  return mt_table;
}

TsTimePartition* TsSubEntityGroup::CreatePartitionTable(timestamp64 ts, ErrorInfo& err_info) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  string pt_tbl_sub_path = partitionTblSubPath(p_time);

  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.find(p_time) != partitions_ts_.end()) {
    err_info.setError(KWEEXIST, pt_tbl_sub_path);
    return nullptr;
  }
  TsTimePartition* mt_table = createPartitionTable(pt_tbl_sub_path, p_time, max_ts, err_info);
  if (mt_table != nullptr) {
    mt_table->incRefCount();
    partitions_ts_[mt_table->minTimestamp()] = mt_table->maxTimestamp();
    partition_cache_.Put(p_time, mt_table);
  }
  return mt_table;
}

TsTimePartition* TsSubEntityGroup::createPartitionTable(string& pt_tbl_sub_path, timestamp64 p_time, timestamp64 max_ts,
                                                        ErrorInfo& err_info) {
  LOG_INFO("Create partition [%s] begin", (db_path_ + pt_tbl_sub_path).c_str());
  // Create partition directory
  int p_level = 0;
  calcPartitionTierLevel(max_ts, &p_level);
  auto status = TsTierPartitionManager::GetInstance().MakePartitionDir(db_path_ + pt_tbl_sub_path, p_level, err_info);
  if (status != KStatus::SUCCESS) {
    LOG_ERROR("create partition [%s] failed.", pt_tbl_sub_path.c_str());
    return nullptr;
  }
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_tbl_manager_, entity_block_meta_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  if (!err_info.isOK()) {
    delete mt_table;
    return nullptr;
  }
  // min and max timsstamp precision is second.
  mt_table->minTimestamp() = p_time;
  mt_table->maxTimestamp() = max_ts;
  mt_table->isInitialized() = true;
  LOG_INFO("Create partition [%s] succeeded", (db_path_ + pt_tbl_sub_path).c_str());
  return mt_table;
}

int TsSubEntityGroup::RemovePartitionTable(timestamp64 ts, ErrorInfo& err_info, bool skip_busy, bool with_lock) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  if (with_lock) {
    wrLock();
  }
  Defer defer{[&]() { if (with_lock) { unLock(); } }};
  if (!skip_busy && partitions_ts_.find(p_time) == partitions_ts_.end()) {
    // If the skip_busy is set to true, when the p_time is deleted for the first time,
    // if it is skipped, the p_time will be deleted in the partitions_ts_,
    // and the next time step to here, if the skip_busy is not judged, will return directly.
    LOG_WARN("Partition[%ld] not exist.", p_time);
    return 0;
  }
  TsTimePartition* mt_table = getPartitionTable(p_time, max_ts, err_info, false);
  if (mt_table == nullptr) {
    return err_info.errcode;
  }
  // set partition table's deleted flag, if remove failed this time, the iterator will not query the table
  // and will try to delete it again when the next lifecycle is scheduled
  mt_table->DeleteFlag() = true;
  partitions_ts_.erase(p_time);
  if (removePartitionTable(mt_table, false, err_info, skip_busy) >= 0) {
    // remove refcount of cache
    partition_cache_.Erase(p_time, false);
  } else {
    // If removePartitionTable fails, we need to release mt_table obtained from getPartitionTable.
    ReleaseTable(mt_table);
  }
  return err_info.errcode;
}

int TsSubEntityGroup::RemoveExpiredPartition(int64_t end_ts, ErrorInfo& err_info) {
  {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    for (auto& pts : partitions_ts_) {
      // If other threads are using the partition, the deletion operation will be delayed
      // until the next scheduling. In order to delete the delayed partitions, we need to
      // delete all the data up to the end_ts. So, put all the partitions
      // which end timestamp older than end_ts into p_times
      if (pts.second < end_ts) {
        deleted_partitions_[pts.first] = pts.second;
      }
    }
  }
  err_info.clear();
  // delete all partitions in p_times.
  for (auto iter = deleted_partitions_.begin(); iter != deleted_partitions_.end();) {
    string pt_tbl_sub_path = partitionTblSubPath(iter->first);
    LOG_INFO("RemoveExpiredPartitionTable[%s] start", pt_tbl_sub_path.c_str());
    int ret = RemovePartitionTable(iter->first, err_info, true);
    if (ret < 0) {
      if (ret != KWERSRCBUSY) {
        LOG_ERROR("RemoveExpiredPartitionTable[%s] failed", pt_tbl_sub_path.c_str());
        break;
      }
      // If error_code = KWERSRCBUSY, don't break, skip and remove the partition next time.
      // Clear err_info, avoid error reporting due to KWERSRCBUSY returned.
      err_info.clear();
      LOG_WARN("RemoveExpiredPartitionTable[%s] skipped", pt_tbl_sub_path.c_str());
      iter++;
      continue;
    }
    deleted_partitions_.erase(iter++);
    LOG_INFO("RemoveExpiredPartitionTable[%s] succeeded", pt_tbl_sub_path.c_str());
  }
  return err_info.errcode;
}

int TsSubEntityGroup::removePartitionTable(TsTimePartition* mt_table, bool is_force, ErrorInfo& err_info,
                                           bool skip_busy) {
  mt_table->setObjectStatus(OBJ_READY_TO_CHANGE);
  string pt_path = db_path_ + partitionTblSubPath(mt_table->minTimestamp());
  if (!is_force) {
    MUTEX_LOCK(mt_table->m_ref_cnt_mtx_);
    // check if there are any other refcount
    while (mt_table->isUsedWithCache()) {
      if (skip_busy) {
        // skip_busy is true, we can defer deletion
        LOG_WARN("Partition is in use, defer deletion until the next scheduling. partition:%s", pt_path.c_str());
        // error_code < 0, partition_ts_ and partition_cache_ will not erase this partition,
        // but partition's delete flag is set to true, so will not query this partition by iterator later.
        err_info.setError(KWERSRCBUSY, "Partition is in use");
        MUTEX_UNLOCK(mt_table->m_ref_cnt_mtx_);
        return err_info.errcode;
      }
      // skip_busy is false, need to wait until partition not be used
      KW_COND_WAIT(mt_table->m_ref_cnt_cv_, mt_table->m_ref_cnt_mtx_);
    }
    MUTEX_UNLOCK(mt_table->m_ref_cnt_mtx_);
  }

  // unmap and remove segments
  err_info.errcode = mt_table->remove();
  if (err_info.errcode >= 0) {
    // remove partition directory
    TsTierPartitionManager::GetInstance().RMPartitionDir(pt_path, err_info);
  }
  delete mt_table;
  mt_table = nullptr;
  return err_info.errcode;
}

int TsSubEntityGroup::removePartitionDir(const std::string& db_path, const std::string& pt_tbl_sub_path) {
  std::string real_partition_path = db_path + pt_tbl_sub_path;
  DIR* dir_ptr = opendir(real_partition_path.c_str());
  if (dir_ptr) {
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      std::string full_path = real_partition_path + entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        closedir(dir_ptr);
        return -1;
      }
      if (S_ISREG(file_stat.st_mode)) {
        int dn_len = strlen(entry->d_name);
        dn_len = dn_len - 5;
        if (strcmp(entry->d_name + dn_len, ".sqfs") == 0) {
          string part_name = string(entry->d_name, dn_len);
          ErrorInfo err_info;
          umount(db_path, pt_tbl_sub_path + part_name + '/', err_info);
          if (err_info.errcode < 0) {
            closedir(dir_ptr);
            return err_info.errcode;
          }
        }
      }
    }
    closedir(dir_ptr);
  }
  ErrorInfo err_info;
  TsTierPartitionManager::GetInstance().RMPartitionDir(real_partition_path, err_info);
  return 0;
}

map<int64_t, int64_t> TsSubEntityGroup::allPartitions() {
  map<int64_t, int64_t> parts;
  rdLock();
  Defer defer{[&]() { unLock(); }};
  // Load all partition tables
  // std::vector<MMapMetricsTable*> tables;
  for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
    parts[it->first] = it->second;
  }
  return std::move(parts);
}

int TsSubEntityGroup::RemoveAll(bool is_force, ErrorInfo& err_info) {
  int error_code = 0;
  wrLock();
  Defer defer{[&]() { unLock(); }};
  // Load all partition tables
  std::vector<TsTimePartition*> tables;
  for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
    err_info.clear();
    TsTimePartition* mt_table = getPartitionTable(it->first, it->second, err_info, false);
    if (err_info.errcode == KWEDROPPEDOBJ) {  // Ignoring non-existent partition tables (deleted or file corrupted)
      err_info.clear();
      continue;
    }
    if (err_info.errcode < 0) {
      break;
    }
    tables.emplace_back(mt_table);
  }

  for (auto it = deleted_partitions_.begin() ; it != deleted_partitions_.end() ; it++) {
    err_info.clear();
    TsTimePartition* mt_table = getPartitionTable(it->first, it->second, err_info, false);
    if (err_info.errcode == KWEDROPPEDOBJ) {  // Ignoring non-existent partition tables (deleted or file corrupted)
      err_info.clear();
      continue;
    }
    if (err_info.errcode < 0) {
      break;
    }
    tables.emplace_back(mt_table);
  }

  if (err_info.errcode >= 0) {
    // Release all partition tables in cache
    partition_cache_.Clear();
    for (int i = 0 ; i < tables.size() ; i++) {
      if (removePartitionTable(tables[i], is_force, err_info) < 0) {
        LOG_ERROR("remove partition [%s] failed.", tables[i]->GetPath().c_str());
        break;
      }
      tables[i] = nullptr;
    }
    if (err_info.errcode >= 0) {
      // Delete metadata and subgroup directory
      err_info.errcode = entity_block_meta_->remove();
      delete entity_block_meta_;
      entity_block_meta_ = nullptr;
      partitions_ts_.clear();
      deleted_partitions_.clear();

      err_info.errcode = fs::remove_all(real_path_.c_str());
      if (err_info.errcode < 0) {
        LOG_ERROR("remove[%s] error : %s", real_path_.c_str(), strerror(errno));
      }
    }
  }

  // Release the obtained table
  for (int i = 0 ; i < tables.size() ; i++) {
    if (tables[i] != nullptr) ReleaseTable(tables[i]);
  }

  return err_info.errcode;
}

void TsSubEntityGroup::PartitionCacheEvict() {
  partition_cache_.Clear(0);
}

int TsSubEntityGroup::DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  // Firstly, delete the datablock of Entity in each partition table
  map<int64_t, int64_t> partitions = allPartitions();
  for (auto it = partitions.begin() ; it != partitions.end() ; it++) {
    uint64_t num = 0;
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      p_table->DeleteEntity(entity_id, lsn, &num, err_info);
    }
    ReleaseTable(p_table);
    if (err_info.errcode < 0 && err_info.errcode != KWEDROPPEDOBJ) {
      break;
    }
    *count = *count + num;
  }
  deleteEntityItem(entity_id);

  return err_info.errcode;
}

int TsSubEntityGroup::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  // Firstly, undo the deleted Entity in each partition table
  map<int64_t, int64_t> partitions = allPartitions();
  for (auto it = partitions.begin(); it != partitions.end(); it++) {
    uint64_t num = 0;
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      p_table->UndoDeleteEntity(entity_id, lsn, &num, err_info);
    }
    ReleaseTable(p_table);
    if (err_info.errcode < 0 && err_info.errcode != KWEDROPPEDOBJ) {
      break;
    }
    *count = *count + num;
  }
  EntityItem* entity_item = entity_block_meta_->getEntityItem(entity_id);
  entity_item->is_deleted = false;

  return err_info.errcode;
}

void TsSubEntityGroup::sync(int flags) {
  map<int64_t, int64_t> partitions = allPartitions();
  ErrorInfo err_info;
  for (auto it = partitions.begin() ; it != partitions.end() ; it++) {
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      if (p_table->isValid()) {
        p_table->rdLock();
        p_table->sync(flags);
        p_table->unLock();
      }
      ReleaseTable(p_table);
    }
  }
}

inline void TsSubEntityGroup::partitionTime(timestamp64 target_ts, timestamp64 begin_ts, timestamp64 interval,
                                            timestamp64& min_ts, timestamp64& max_ts) {
  // begin_ts = 0 or other partition min_ts
  if (target_ts >= begin_ts) {
    // Starting from begin_ts with a time interval of interval, retrieve the partition time of cur_ts backwards
    min_ts = begin_ts + uint64_t(target_ts - begin_ts) / interval * interval;
    if (min_ts < INT64_MAX - interval) {
      max_ts = min_ts + interval - 1;
    } else {
      max_ts = INT64_MAX;
    }
  } else {
    // Starting from begin_ts with a time interval of interval, retrieve the partition time where cur_ts is located forward
    uint64_t offset = uint64_t(begin_ts - target_ts + interval - 1) / interval * interval;
    if (begin_ts >= (timestamp64)(INT64_MIN + offset)) {
      min_ts = begin_ts - offset;
      max_ts = min_ts + interval - 1;
    } else {
      min_ts = INT64_MIN;
      max_ts = begin_ts + interval - offset - 1;
    }
  }
}

void TsSubEntityGroup::calcPartitionTierLevel(KTimestamp partition_max_ts, int* to_level) {
  *to_level = TsTierPartitionManager::CalculateTierLevelByTimestamp(partition_max_ts);
}

void TsSubEntityGroup::PartitionsTierMigrate() {
}

int TsSubEntityGroup::GetLastRowEntity(pair<timestamp64, uint32_t>& last_row_entity) {
  if (last_row_checked_ && last_row_entity_.second != 0) {
    last_row_entity = last_row_entity_;
    return 0;
  }
  rdLock();
  Defer defer{[&]() { unLock(); }};

  if (partitions_ts_.size() == 0) {
    last_row_entity = {INT64_MIN, 0};
    last_row_checked_ = true;
    return 0;
  }
  bool last_row_found = false;
  auto it = partitions_ts_.rbegin();
  while (!last_row_found && it != partitions_ts_.rend()) {
    timestamp64 partition = it->first;
    ++it;
    ErrorInfo err_info;
    TsTimePartition* p_table = GetPartitionTable(partition, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTable failed");
      return err_info.errcode;
    }
    p_table->rdLock();
    Defer defer2{[&]() {
      p_table->unLock();
      ReleaseTable(p_table);
    }};

    vector<uint32_t> entity_ids;
    std::deque<BlockItem*> block_items;
    p_table->GetAllBlockItems(entity_ids, block_items);

    class cmp {
     public:
      bool operator()(const pair<timestamp64, BlockItem*>& a, const pair<timestamp64, BlockItem*>& b) {
        return a.first < b.first;
      }
    };
    std::priority_queue<pair<timestamp64, BlockItem*>, vector<pair<timestamp64, BlockItem*>>, cmp> pque;
    while (!block_items.empty()) {
      BlockItem* block_item = block_items.front();
      block_items.pop_front();
      std::shared_ptr<MMapSegmentTable> segment_tbl = p_table->getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, p_table->GetPath().c_str());
        return -1;
      }
      timestamp64 min_ts, max_ts;
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
      pque.push({max_ts, block_item});
    }

    last_row_entity = {INT64_MIN, 0};
    while (!pque.empty() && last_row_entity.first < pque.top().first) {
      timestamp64 ts = pque.top().first;
      BlockItem* blk = pque.top().second;
      pque.pop();

      std::shared_ptr<MMapSegmentTable> segment_tbl = p_table->getSegmentTable(blk->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", blk->block_id, p_table->GetPath().c_str());
        return -1;
      }
      timestamp64 blk_max_ts = INT64_MIN;
      for (uint32_t i = 1; i <= blk->publish_row_count; ++i) {
        if (!segment_tbl->IsRowVaild(blk, i)) continue;
        MetricRowID row_id = blk->getRowID(i);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
        blk_max_ts = max(blk_max_ts, cur_ts);
      }
      if (blk_max_ts > last_row_entity.first) {
        last_row_entity = {blk_max_ts, blk->entity_id};
        last_row_found = true;
      }
    }
  }

  UpdateEntityAndMaxTs(last_row_entity.first, last_row_entity.second);
  last_row_checked_ = true;
  last_row_entity = last_row_entity_;
  return 0;
}

}  // namespace kwdbts
