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

#pragma once

#include <climits>
#include <string>
#include <vector>
#include <utility>
#include <unordered_map>
#include <memory>
#include "lru_cache.h"
#include "mmap/mmap_root_table_manager.h"
#include "ts_time_partition.h"
#include "libkwdbts2.h"
#include "mmap/mmap_tag_column_table.h"
#include "map"
#include "deque"

namespace kwdbts {
typedef uint32_t SubGroupID;
typedef uint32_t EntityID;
class TsSubGroupPTIterator;

class TsSubEntityGroup : public TSObject {
 public:
  explicit TsSubEntityGroup(MMapRootTableManager*& root_tbl_manager);

  virtual ~TsSubEntityGroup();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  /**
   * @brief	Open and initialize SubEntityGroup. Mainly for initializing meta files
   *        If flags=CREATE, an already existing error will be returned when the meta file exists.
   *
   * @param 	tbl_name
   * @param 	schema		root table schema
   * @param 	subgroup_id		subgroup id
   * @param 	db_path
   * @param 	tbl_sub_path		subgroup sub path
   * @param 	flags option to open a file; O_CREAT to create new file.
   * @param 	max_entities_per_subgroup  max entities in subgroup
   * @return	0 succeed, otherwise < 0.
   */
  int OpenInit(SubGroupID subgroup_id, const std::string& db_path, const string& tbl_sub_path,
               int flags, uint16_t max_entities_per_subgroup, ErrorInfo& err_info);

  /**
  * @brief subgroup ID
  */
  SubGroupID GetID() { return subgroup_id_; }

  /**
 * @brief Assign an EntityID when adding a new entity.
 * @param[in] primary_tag   Entity primary tag
 *
 * @return The assigned EntityID, if there are no available IDs, returns 0
  */
  EntityID AllocateEntityID(const string& primary_tag, ErrorInfo& err_info);

  /**
  * @brief SubGroup has already been assigned an EntityID collection.
  *
  * @return Set of EntityID vectors assigned
   */
  std::vector<uint32_t> GetEntities();

  /**
   * @brief Get the number of entities that SubEntityGroup can store
   * @return uint16_t
   */
  uint16_t GetSubgroupEntities();

  /**
* @brief delete entity
 * @param[in] entity_id   Entity ID
*
* @return error code
 */
  int DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  int UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  /**
* @brief Modify the number of entities that SubEntityGroup can store.
*         When creating a new time partition MetricsTable, use the modified values without affecting the existing partition.
* @param[in] entity_num   entity num
*
* @return error code
 */
  int SetEntityNum(uint32_t entity_num);

/**
 * @brief Calculate the partition time based on the data timestamp
 * @param[in] ts   time in seconds
 * @param[out] max_ts   The maximum time of this partition
 *
 * @return Return the partition time in seconds
 */
  timestamp64 PartitionTime(timestamp64 ts, timestamp64& max_ts);

  /**
 * @brief Query minimum partition time
 *
 * @return
 */
  timestamp64 MinPartitionTime();

  /**
 * @brief Query maximum partition time
 *
 * @return
 */
  timestamp64 MaxPartitionTime();

/**
 * @brief Retrieve MMapPartition times
 * @param[in] ts_span   The time span in seconds.
 *
 * @return partiton start time.
 */
  vector<timestamp64> GetPartitions(const KwTsSpan& ts_span);

  /**
   * @brief create temp MMapPartition table
   * @param[in] ts_span   The time span in seconds.
   *
   * @return partiton start time.
   */
  TsTimePartition* CreateTmpPartitionTable(string p_name, size_t version, bool& created);

/**
 * @brief Retrieve MMapPartitionTable from the time partition directory
 * @param[in] ts   The time in seconds is internally converted to the time of the partition it belongs to.
 * @param[in] create_if_not_exist  True: If the group table does not exist, call the internal createPartitionTable to create it.
 * @param[in] lru_push_back  False: Insert lru_cache put into the header; True: Insert to the end.
 *
 * @return
 */
  TsTimePartition* GetPartitionTable(timestamp64 ts, ErrorInfo& err_info,
                                     bool create_if_not_exist = false,
                                     bool lru_push_back = false);

  /**
 * @brief  Filter partition table instances based on ts span
 * @param[in] ts_span   timestamp using precision of schema.
 *
 * @return
 */
  vector<TsTimePartition*> GetPartitionTables(const KwTsSpan& ts_span, ErrorInfo& err_info);

  /**
  * @brief  get iterator for partition tables in current subgroup.
   * @param[in] ts_spans   time spans
   *
   * @return
   */
  std::shared_ptr<TsSubGroupPTIterator> GetPTIterator(const std::vector<KwTsSpan>& ts_spans);


  /**
 * @brief Create MMapPartitionTable in the time partition directory
 * @param[in] ts   The time in seconds is internally converted to the time of the partition it belongs to.
 *
 * @return MMapPartitionTable
 */
  TsTimePartition* CreatePartitionTable(timestamp64 ts, ErrorInfo& err_info);

/**
 * @brief Remove partition table within the ts.
 * @param[in] ts   The time in seconds is internally converted to the time of the partition to which it belongs.
 * @param[in] skip_busy if set true, the deletion will be delayed until the next
 *                      scheduling when partition is busy, default false
 * @return error code
 */
  int RemovePartitionTable(timestamp64 ts, ErrorInfo& err_info, bool skip_busy = false, bool with_lock = true);

/**
 * @brief Delete expired partition data
 * @param[in] end_ts  end timestamps of expired data
 *
 * @return error code
 */
  int RemoveExpiredPartition(int64_t end_ts, ErrorInfo& err_info);

/**
 * @brief delete all partition in subgroup
 *
 * @return error code
 */
  int RemoveAll(bool is_force, ErrorInfo& err_info);

/**
 * @brief Partition table cache elimination
 *
 * @return error code
 */
  void PartitionCacheEvict();

  /**
 * @brief Add new column information
 * @param[in] attr_info  Newly added columns
 *
 * @return error code
 */

  virtual void sync(int flags);

  inline void MutexLockEntity(int32_t entity_id) { MUTEX_LOCK(entity_mutexes_[entity_id]); }

  inline void MutexUnLockEntity(int32_t entity_id) { MUTEX_UNLOCK(entity_mutexes_[entity_id]); }

  int ReOpenInit(ErrorInfo& err_info);

  std::vector <timestamp64> GetPartitionTsInSpan(KwTsSpan ts_span);
  int ClearPartitionCache();
  int ErasePartitionCache(timestamp64 pt_ts);

  /**
   * @brief Hot and cold data tiering migration, loop all partitions
   * @return void
   */
  void PartitionsTierMigrate();

  void UpdateEntityAndMaxTs(timestamp64 max_ts, EntityID entity_id) {
    MUTEX_LOCK(sub_entity_group_mutex_);
    if (max_ts >= last_row_entity_.first) {
      last_row_entity_ = {max_ts, entity_id};
    }
    MUTEX_UNLOCK(sub_entity_group_mutex_);
  }

  void ResetEntityMaxTs(timestamp64 max_ts, const vector<EntityID>& entity_ids) {
    MUTEX_LOCK(sub_entity_group_mutex_);
    if (max_ts >= last_row_entity_.first) {
      for (auto entity_id : entity_ids) {
        if (entity_id == last_row_entity_.second) {
          last_row_entity_ = {INT64_MIN, 0};
          last_row_checked_ = false;
          break;
        }
      }
    }
    MUTEX_UNLOCK(sub_entity_group_mutex_);
  }

  int GetLastRowEntity(pair<timestamp64, uint32_t>& last_row_entity);

 private:
  std::string db_path_;
  std::string tbl_sub_path_;
  // The absolute path to the subgroup directory
  std::string real_path_;
  // Referenced from the root table manager of SubEntityGroupManager
  MMapRootTableManager*& root_tbl_manager_;
  std::string table_name_;
  SubGroupID subgroup_id_;
  // The entity block meta of subgroup is mainly used to manage the allocation of EntityIDs
  MMapEntityBlockMeta* entity_block_meta_{nullptr};
  // The set of all partition times: key=minimum partition time, value=maximum partition time
  map<timestamp64, timestamp64> partitions_ts_;
  // deleted but skipped partitions
  map<timestamp64, timestamp64> deleted_partitions_;
  // Using LRU to cache commonly used partition tables, key=partition time
  PartitionLRUCache partition_cache_;
  bool last_row_checked_ = false;
  pair<timestamp64, uint32_t> last_row_entity_ = {INT64_MIN, 0};

  using TsSubEntityGroupEntityLatch = KLatch;
  std::vector<TsSubEntityGroupEntityLatch* > entity_mutexes_;

  using TsSubEntityGroupLatch = KLatch;
  TsSubEntityGroupLatch* sub_entity_group_mutex_;

  using TsSubEntityGroupRWLatch = KRWLatch;
  TsSubEntityGroupRWLatch* sub_entity_group_rwlock_;

  inline void partitionTime(timestamp64 target_ts, timestamp64 begin_ts, timestamp64 interval,
                            timestamp64& min_ts, timestamp64& max_ts);

  /**
 * @brief Internal method to obtain MMapPartitionTable, unlocked
 *
 * @return
 */
  TsTimePartition* getPartitionTable(timestamp64 p_time, timestamp64 max_ts, ErrorInfo& err_info,
                                     bool create_if_not_exist = false, bool lru_push_back = false);
  /**
  * @brief Internal method for creating MMapPartitionTable
  *
  * @return
  */
  TsTimePartition* createPartitionTable(string& pt_tbl_sub_path, timestamp64 p_time, timestamp64 max_ts,
                                        ErrorInfo& err_info);

  /**
  * @brief internal functions of RemoveMMapPartitionTable
  * if the partition is in use and skip_busy is true, defer deletion until the next scheduling
  *
  * @return error code
  */
  int removePartitionTable(TsTimePartition* mt_table, bool is_force, ErrorInfo& err_info, bool skip_busy = false);

  /**
  * @brief delete the file directory directly
  * @param db_path database path
  * @param pt_tbl_sub_path partition table sub path
  *
  * @return error code
  */
  int removePartitionDir(const std::string& db_path, const std::string& pt_tbl_sub_path);

  void calcPartitionTierLevel(KTimestamp partition_max_ts, int* to_level);

  inline string partitionTblSubPath(timestamp64 p_time) {
    if (p_time >= 0) {
      return tbl_sub_path_ + std::to_string(p_time) + "/";
    } else {
      return tbl_sub_path_ + "m"+ std::to_string(abs(p_time)) + "/";
    }
  }

  //
  void deleteEntityItem(uint entity_id) {
    entity_block_meta_->deleteEntity(entity_id);
  }

  // Internal method to obtain all partition information: Lock, copy partitions.ts_, and return
  map<int64_t, int64_t> allPartitions();

  void mutexLock() override {
    MUTEX_LOCK(sub_entity_group_mutex_);
  }
  void mutexUnlock() override {
    MUTEX_UNLOCK(sub_entity_group_mutex_);
  }
};

class TsSubGroupPTIterator {
 public:
  TsSubGroupPTIterator(TsSubEntityGroup* sub_eg, const std::vector<timestamp64>& partitions) :
                      sub_eg_(sub_eg), partitions_(partitions) {
  }

  explicit TsSubGroupPTIterator(TsSubGroupPTIterator* other) :
    sub_eg_(other->sub_eg_), partitions_(other->partitions_) {
    cur_p_table_ = nullptr;
    sub_eg_ = other->sub_eg_;
    cur_partition_idx_ = 0;
  }

  ~TsSubGroupPTIterator() {
    if (cur_p_table_ != nullptr) {
      cur_p_table_->unLock();
      ReleaseTable(cur_p_table_);
      cur_p_table_ = nullptr;
    }
  }

  KStatus Next(TsTimePartition** p_table);

  void Reset(bool reverse_traverse = false) {
    if (cur_p_table_ != nullptr) {
      cur_p_table_->unLock();
      ReleaseTable(cur_p_table_);
      cur_p_table_ = nullptr;
    }
    reverse_traverse_ = reverse_traverse;
    if (reverse_traverse_) {
      cur_partition_idx_ = partitions_.size() - 1;
    } else {
      cur_partition_idx_ = 0;
    }
  }

  bool Valid() {
    if (reverse_traverse_) {
      if (cur_partition_idx_ < 0) {
        // scan over.
        return false;
      }
    } else {
      if (cur_partition_idx_ >= partitions_.size()) {
        // scan over.
        return false;
      }
    }
    return true;
  }

  inline const std::vector<timestamp64> GetPartitions() {
    return partitions_;
  }

 private:
  TsSubEntityGroup* sub_eg_{nullptr};
  std::vector<timestamp64> partitions_;
  bool reverse_traverse_{false};
  int cur_partition_idx_{0};
  TsTimePartition* cur_p_table_{nullptr};
};

class TsEntityGroupPTIterator {
 public:
  TsEntityGroupPTIterator() {}

  ~TsEntityGroupPTIterator() {
    cur_p_table_.clear();
    partition_table_iter_.clear();
  }

  void push_back(uint32_t subgroup_id, std::shared_ptr<TsSubGroupPTIterator>& p_iter) {
    partition_table_iter_[subgroup_id] = p_iter;
  }

  KStatus Next(uint32_t subgroup_id, TsTimePartition** p_table);

 private:
  unordered_map<uint32_t, std::shared_ptr<TsSubGroupPTIterator>> partition_table_iter_;
  unordered_map<uint32_t, TsTimePartition*> cur_p_table_;
};

}  // namespace kwdbts
