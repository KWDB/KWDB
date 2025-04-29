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

#include <dirent.h>
#include <iostream>
#include "lg_api.h"
#include "ts_time_partition.h"
#include "st_group_manager.h"
#include "utils/big_table_utils.h"
#include "utils/compress_utils.h"
#include "perf_stat.h"
#include "sys_utils.h"
#include "st_config.h"

namespace kwdbts {

SubEntityGroupManager::~SubEntityGroupManager() {
  wrLock();
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; it++) {
    delete it->second;
  }
  subgroups_.clear();
  unLock();
  delete subgroup_mgr_mutex_;
  subgroup_mgr_mutex_ = nullptr;
  delete subgroup_mgr_rwlock_;
  subgroup_mgr_rwlock_ = nullptr;
}

int SubEntityGroupManager::OpenInit(const std::string& db_path, const std::string& tbl_sub_path,
                                    uint64_t table_id, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (subgroups_.size() > 0) {
    err_info.setError(KWEPERM, tbl_sub_path + " already opened.");
    return err_info.errcode;
  }
  db_path_ = db_path;
  table_id_ = table_id;
  // root_table->incRefCount();

  if (db_path_.back() != '/') {
    db_path_ = db_path_ + "/";
  }
  tbl_sub_path_ = normalizePath(tbl_sub_path);

  // max_sub_group_id
  DIR* dir_ptr = opendir((db_path_ + tbl_sub_path_).c_str());
  if (dir_ptr) {
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      std::string full_path = db_path_ + tbl_sub_path_ + entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        err_info.setError(KWENFILE, "stat[" + full_path + "] failed");
        closedir(dir_ptr);
        return err_info.errcode;
      }
      if (S_ISDIR(file_stat.st_mode)) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
            || entry->d_name[0] == '_' || strcmp(entry->d_name, "wal") == 0) {
          continue;
        }
        string dir_name = entry->d_name;
        size_t idx = dir_name.find_last_of('_');
        if (idx == string::npos) {
          continue;
        }
        string table_id_str = dir_name.substr(0, idx);
        string sub_group_str = dir_name.substr(idx + 1);
        if (table_id_str == std::to_string(table_id)) {
          char* endptr;
          int64_t sub_group_id = std::strtol(sub_group_str.c_str(), &endptr, 10);
          if (*endptr == '\0' && max_subgroup_id_ < sub_group_id) {
            max_subgroup_id_ = sub_group_id;
          }
          TsSubEntityGroup* subgroup = openSubGroup(sub_group_id, err_info, false);
          if (err_info.errcode < 0) {
            LOG_ERROR("GetSubGroup error : %s", err_info.errmsg.c_str());
            break;
          }
          subgroups_[sub_group_id] = subgroup;
        }
      }
    }
    closedir(dir_ptr);
  }

  if (err_info.errcode < 0) {
    // Clean up the loaded subgroups
    subgroups_.clear();
  }
  return err_info.errcode;
}

TsTimePartition* SubEntityGroupManager::CreatePartitionTable(timestamp64 ts, SubGroupID subgroup_id,
                                                              ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return nullptr;
  }
  return sub_group->CreatePartitionTable(ts, err_info);
}

TsSubEntityGroup* SubEntityGroupManager::CreateSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info) {
  assert(root_table_manager_ != nullptr);
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto it = subgroups_.find(subgroup_id);
  if (it != subgroups_.end()) {
    err_info.setError(KWEEXIST, "SubEntityGroup : " + std::to_string(subgroup_id));
    return it->second;
  }

  root_table_manager_->rdLock();
  TsSubEntityGroup* sub_group = new TsSubEntityGroup(root_table_manager_);
  root_table_manager_->unLock();
  string group_sanbox = GetSubGroupTblSubPath(subgroup_id);
  uint16_t max_entities_of_prev_subgroup = 0;
  if (subgroups_.find(subgroup_id - 1) != subgroups_.end()) {
    max_entities_of_prev_subgroup = subgroups_[subgroup_id-1]->GetSubgroupEntities();
  }
  uint16_t max_entities_per_subgroup = GetMaxEntitiesPerSubgroup(max_entities_of_prev_subgroup);
  if (sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_CREAT_EXCL, max_entities_per_subgroup, err_info) < 0) {
    delete sub_group;
    return nullptr;
  }
  subgroups_[subgroup_id] = sub_group;
  return sub_group;
}

TsSubEntityGroup* SubEntityGroupManager::GetSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info,
                                                     bool create_not_exist) {
  {
    // Initially, attempt to acquire the subgroup using a read lock. If the subgroup is not found, subsequently,
    // acquire a write lock and perform another search to ensure no other threads have created it.
    // If not found yet, create a new subgroup
    rdLock();
    Defer defer{[&]() { unLock(); }};
    auto it = subgroups_.find(subgroup_id);
    if (it != subgroups_.end()) {
      return it->second;
    }
  }

  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto it = subgroups_.find(subgroup_id);
  if (it != subgroups_.end()) {
    return it->second;
  }

  TsSubEntityGroup* sub_group = openSubGroup(subgroup_id, err_info, create_not_exist);
  if (err_info.errcode < 0) {
    return nullptr;
  }
  if (subgroup_id > max_subgroup_id_) {
    max_subgroup_id_ = subgroup_id;
  }
  subgroups_[subgroup_id] = sub_group;
  return sub_group;
}

TsSubEntityGroup* SubEntityGroupManager::openSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info,
                                                      bool create_not_exist) {
  root_table_manager_->rdLock();
  TsSubEntityGroup* sub_group = new TsSubEntityGroup(root_table_manager_);
  root_table_manager_->unLock();
  string group_sanbox = GetSubGroupTblSubPath(subgroup_id);
  uint16_t max_entities_per_subgroup = GetMaxEntitiesPerSubgroup();
  if (sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_OPEN_NORECURSIVE,
                          max_entities_per_subgroup, err_info) < 0) {
    delete sub_group;
    sub_group = nullptr;
  }
  if (err_info.errcode < 0 && create_not_exist) {
    err_info.clear();
    root_table_manager_->rdLock();
    sub_group = new TsSubEntityGroup(root_table_manager_);
    uint16_t max_entities_of_prev_subgroup = 0;
    if (subgroups_.find(subgroup_id - 1) != subgroups_.end()) {
      max_entities_of_prev_subgroup = subgroups_[subgroup_id-1]->GetSubgroupEntities();
    }
    max_entities_per_subgroup = GetMaxEntitiesPerSubgroup(max_entities_of_prev_subgroup);
    sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_CREAT_EXCL, max_entities_per_subgroup, err_info);
    root_table_manager_->unLock();
  }
  if (err_info.errcode < 0) {
    delete sub_group;
    sub_group = nullptr;
  }
  return sub_group;
}

vector<timestamp64> SubEntityGroupManager::GetPartitions(const KwTsSpan& ts_span, SubGroupID subgroup_id,
                                                           ErrorInfo& err_info) {
  KWDB_DURATION(StStatistics::Get().get_partition);
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    LOG_WARN(" cannot found subgroup %u at sandbox %s.", subgroup_id, this->tbl_sub_path_.c_str());
    return {};
  }
  return sub_group->GetPartitions(ts_span);
}

TsTimePartition* SubEntityGroupManager::GetPartitionTable(timestamp64 ts, SubGroupID sub_group_id,
                                                          ErrorInfo& err_info, bool for_new) {
  KWDB_DURATION(StStatistics::Get().get_partition);
  TsSubEntityGroup* sub_group = GetSubGroup(sub_group_id, err_info, false);
  if (sub_group == nullptr) {
    return nullptr;
  }
  return sub_group->GetPartitionTable(ts, err_info, for_new);
}

vector<TsTimePartition*> SubEntityGroupManager::GetPartitionTables(const KwTsSpan& ts_span, SubGroupID subgroup_id,
                                                                   ErrorInfo& err_info) {
  KWDB_DURATION(StStatistics::Get().get_partitions);
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return {};
  }
  return sub_group->GetPartitionTables(ts_span, err_info);
}

void SubEntityGroupManager::ReleasePartitionTable(TsTimePartition* e_bt, bool is_force) {
  ReleaseTable(e_bt);
}

int SubEntityGroupManager::DropSubGroup(SubGroupID subgroup_id, bool if_exist,
                                        ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return KWENOOBJ;
  }
  err_info.clear();
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (sub_group->RemoveAll(false, err_info) >= 0) {
    delete sub_group;
    subgroups_.erase(subgroup_id);
  }
  return err_info.errcode;
}

int SubEntityGroupManager::DropPartitionTable(timestamp64 p_time, SubGroupID subgroup_id, bool if_exist,
                                              ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    err_info.setError(KWENOOBJ, tbl_sub_path_ + std::to_string(p_time));
    return err_info.errcode;
  }
  return sub_group->RemovePartitionTable(p_time, err_info);
}

int SubEntityGroupManager::DeleteExpiredData(int64_t end_ts, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  // Traverse through all subgroups to delete expired data
  for (auto& subgroup : subgroups_) {
    if (subgroup.second->RemoveExpiredPartition(end_ts, err_info) < 0) {
      break;
    }
  }
  return err_info.errcode;
}

int SubEntityGroupManager::DropAll(bool is_force, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; ) {
    if (it->second->RemoveAll(is_force, err_info) < 0) {
      return err_info.errcode;
    }
    delete it->second;
    it = subgroups_.erase(it);
  }

  return 0;
}

void SubEntityGroupManager::Compress(kwdbContext_p ctx, const timestamp64& compress_ts,
                                     uint32_t& compressed_num, ErrorInfo& err_info) {
  std::unordered_map<TsTimePartition*, TsSubEntityGroup*> compress_tables;
  // Gets all the compressible partitions in the [INT64_MIN, ts] time range under subgroup
  rdLock();
  for (auto & subgroup : subgroups_) {
    vector<TsTimePartition*> p_tables = subgroup.second->GetPartitionTables({INT64_MIN, compress_ts}, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("SubEntityGroupManager GetPartitionTable error : %s", err_info.errmsg.c_str());
      break;
    }
    for (auto p_table : p_tables) {
      compress_tables.insert({p_table, subgroup.second});
    }
  }
  unLock();

  bool compress_error = false;
  for (auto it = compress_tables.begin(); it != compress_tables.end();) {
    // detect ctrl+C first
    if (ctx->relation_ctx != 0 && isCanceledCtx(ctx->relation_ctx)) {
      err_info.setError(KWEOTHER, "interrupted");
      return;
    }
    TsTimePartition* p_table = it->first;
    if (p_table != nullptr && !compress_error) {
      p_table->Compress(compress_ts, compressed_num, err_info);
      if (err_info.errcode < 0) {
        LOG_ERROR("MMapPartitionTable[%s] compress error : %s", p_table->path().c_str(), err_info.errmsg.c_str());
        compress_error = true;
      }
    }
    ReleaseTable(p_table);
    // Call partition table lru cache to eliminate after compression
    it->second->PartitionCacheEvict();
    ++it;
  }
}

void SubEntityGroupManager::Vacuum(kwdbContext_p ctx, uint32_t ts_version, ErrorInfo &err_info) {
  std::unordered_map<TsTimePartition*, TsSubEntityGroup*> vacuum_tables;
  rdLock();
  for (auto& [fst, snd] : subgroups_) {
    vector<TsTimePartition*> p_tables = snd->GetPartitionTables({INT64_MIN, INT64_MAX}, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("SubEntityGroupManager GetPartitionTable error : %s", err_info.errmsg.c_str());
      break;
    }
    for (auto p_table : p_tables) {
      vacuum_tables.insert({p_table, snd});
    }
  }
  unLock();

  bool vacuum_error = false;
  for (auto it = vacuum_tables.begin(); it != vacuum_tables.end();) {
    TsTimePartition* p_table = it->first;
    if (p_table != nullptr && !vacuum_error) {
      // data vacuum
      std::string partition_path = p_table->GetPath();
      VacuumStatus vacuum_status{VacuumStatus::NOT_BEGIN};
      KStatus s = p_table->Vacuum(ts_version, vacuum_status);
      if (s != SUCCESS) {
        LOG_ERROR("Failed Vacuum in partition [%s]", partition_path.c_str());
        vacuum_error = true;
      } else {
        if (vacuum_status == VacuumStatus::CANCEL) {
          LOG_INFO("Cancel vacuum in partition [%s]", partition_path.c_str());
        } else if (vacuum_status == VacuumStatus::FINISH) {
          LOG_INFO("Finish vacuum in partition [%s]", partition_path.c_str());
        } else if (vacuum_status == VacuumStatus::FAILED) {
          LOG_ERROR("Failed Vacuum in partition [%s]", partition_path.c_str());
        }
      }
    }
    ReleaseTable(p_table);
    // Call partition table lru cache to eliminate after compression
    it->second->PartitionCacheEvict();
    ++it;
  }
}

int SubEntityGroupManager::removeDir(string path) {
  int ret = -1;
  if (!path.empty()) {
    if (Remove(path)) {
      return 0;
    }
  }
  return ret;
}

int SubEntityGroupManager::AllocateEntity(std::string& primary_tags, uint64_t tag_hash, SubGroupID* group_id,
                                          EntityID* entity_id) {
  // Calculate the number of group and try to meet the hash id range of range
  SubGroupID find_group = max_active_subgroup_id_;
  EntityID find_entity = 0;

  ErrorInfo err_info;
  while (find_entity == 0) {
    TsSubEntityGroup* g_bt = GetSubGroup(find_group, err_info, true);
    if (err_info.errcode < 0) {
      return err_info.errcode;
    }
    // Call the MMapEntityBigTable method to query whether there are any free entity IDs in g_bt
    // If 0 is returned, it means that this group has no available entity
    find_entity = g_bt->AllocateEntityID(primary_tags, err_info);
    if (find_entity > 0) {
      break;
    }
    // Calculate the next group to assign entity id
    find_group++;
    max_active_subgroup_id_ = find_group;
  }

  *group_id = find_group;
  *entity_id = find_entity;
  return 0;
}

int SubEntityGroupManager::ReuseEntities(SubGroupID group_id, std::vector<EntityID>& entity_ids) {
  ErrorInfo err_info;
  TsSubEntityGroup* g_bt = GetSubGroup(group_id, err_info);
  if (!g_bt) {
    return err_info.errcode;
  }
  for (int i = 0 ; i < entity_ids.size() ; i++) {
    // Reuse the entity, which means marking the entity as deleted
    // g_bt->deleteEntityItem(entity_ids[i]);
  }
  return 0;
}

void SubEntityGroupManager::sync(int flags) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; it++) {
    it->second->sync(flags);
  }
}

void SubEntityGroupManager::TierMigrate() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  for (auto& subgroup : subgroups_) {
    subgroup.second->PartitionsTierMigrate();
  }
}

void SubEntityGroupManager::Count(kwdbContext_p ctx, ErrorInfo& err_info) {
  std::unordered_map<TsTimePartition*, TsSubEntityGroup*> count_tables;
  rdLock();
  for (auto& [fst, snd] : subgroups_) {
    vector<TsTimePartition*> p_tables = snd->GetPartitionTables({INT64_MIN, INT64_MAX}, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("SubEntityGroupManager GetPartitionTable error : %s", err_info.errmsg.c_str());
      break;
    }
    for (auto p_table : p_tables) {
      count_tables.insert({p_table, snd});
    }
  }
  unLock();

  bool count_error = false;
  for (auto it = count_tables.begin(); it != count_tables.end();) {
    TsTimePartition* p_table = it->first;
    if (p_table != nullptr && !count_error) {
      p_table->Count();
    }
    ReleaseTable(p_table);
    // Call partition table lru cache to eliminate after compression
    it->second->PartitionCacheEvict();
    ++it;
  }
}
}  // namespace kwdbts
