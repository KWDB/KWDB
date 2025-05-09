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
#include <list>
#include <string>
#include <vector>
#include "libkwdbts2.h"
#include "mmap/mmap_root_table_manager.h"
#include "st_subgroup.h"
#include "lt_rw_latch.h"

namespace kwdbts {
#define MAX_HASH_ID ULONG_MAX

/**
 * @brief EntityBigTableManager manages an EntityBigtable for a time series table
 *   One EntityBigTableManager instance for each chronology table
 */
class SubEntityGroupManager : public TSObject {
 public:
  explicit SubEntityGroupManager(MMapRootTableManager*& root_table_manager)
      : TSObject(), root_table_manager_(root_table_manager) {
    subgroup_mgr_mutex_ = new SubEntityGroupManagerLatch(LATCH_ID_SUBENTITY_GROUP_MANAGER_MUTEX);
    subgroup_mgr_rwlock_ = new SubEntityGroupManagerRWLatch(RWLATCH_ID_SUBENTITY_GROUP_MANAGER_RWLOCK);
  }

  virtual ~SubEntityGroupManager();

  /**
   * @brief Initialize SubEntityGroupManager
   * @param[in] db_path   Absolute path of the database root directory
   * @param[in] tbl_sub_path   Relative path of EntityGroup
   * @param[in] table_id   Time series table ID
   * @param[in] root_table  MMapMetricsTable instance of the time series table
   *
   * @return KStatus
   */
  int OpenInit(const std::string& db_path, const std::string& tbl_sub_path,
                        uint64_t table_id, ErrorInfo& err_info);

  // MMapMetricsTable* GetRootTable(ErrorInfo& err_info);

  /**
   * @brief Create a PartitionTable for the specified time partition under the SubGroup directory.
   * @param[in] ts   time in seconds
   * @param[in] subgroup_id   subgroup number
   *
   * @return KStatus
   */
  TsTimePartition* CreatePartitionTable(timestamp64 ts, SubGroupID subgroup_id,
                                        ErrorInfo& err_info = getDummyErrorInfo());

  /**
   * @brief Create a SubGroupTable with the specified id.
   * @param[in] subgroup_id   subgroup number
   *
   * @return KStatus
   */
  TsSubEntityGroup* CreateSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info = getDummyErrorInfo());

  /**
   * @brief Query the SubGroupTable with the specified id.
   * @param[in] ts: time in seconds
   * @param[in] subgroup_id   subgroup number
   * @param[in] create_not_exist   Whether to create a new subgroup if it does not exist
   *
   * @return KStatus
   */
  TsSubEntityGroup* GetSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info, bool create_not_exist = false);

  /**
   * @brief Alter SubGroup column
   * @param[in] attr_info Attribute information
   * @param[out] err_info error information
   * @return
   */
  int AlterSubGroupColumn(AttributeInfo& attr_info, ErrorInfo& err_info);

  /**
   * @brief Query the Partition time of time span in the specified SubGroup directory.
   * @param[in] ts_span  time span
   * @param[in] subgroup_id   subgroup number
   * @param[in] for_new Whether to create a new partition table if it does not exist
   *
   * @return vector<timestamp64>  partition time vector
   */
  vector<timestamp64> GetPartitions(const KwTsSpan& ts_span, SubGroupID subgroup_id, ErrorInfo& err_info);

  /**
   * @brief Query the PartitionTable of the specified partition time in the specified SubGroup directory.
   * @param[in] ts  time in seconds
   * @param[in] subgroup_id   subgroup number
   * @param[in] for_new Whether to create a new partition table if it does not exist
   *
   * @return KStatus
   */
  TsTimePartition* GetPartitionTable(timestamp64 ts, SubGroupID sub_group_id, ErrorInfo& err_info, bool for_new = false);

  /**
   * @brief Filter partition table instances that meet the conditions based on ts span
   * @param[in] ts_span   time span.
   * @param[in] subgroup_id   subgroup number
   *
   * @return the collection of MMapPartitionTable instances that meet the conditions
   */
  vector<TsTimePartition*> GetPartitionTables(const KwTsSpan& ts_span, SubGroupID subgroup_id, ErrorInfo& err_info);

  int DropSubGroup(SubGroupID subgroup_id, bool if_exist, ErrorInfo &err_info);

  int DropPartitionTable(timestamp64 p_time, SubGroupID subgroup_id, bool if_exist, ErrorInfo& err_info);

  /**
   * @brief Delete expired partition whose timestamp is older than the end_ts in all subgroups
   * @param[in] end_ts end timestamp of expired data
   * @param[out] err_info error info
   * @return error code
   */
  int DeleteExpiredData(int64_t end_ts, ErrorInfo& err_info);

  int DropAll(bool is_force, ErrorInfo& err_info);

  /**
   * @brief Compress the segment whose maximum timestamp is smaller than ts in all subgroups of entity group
   * @param[in] compress_ts A timestamp that needs to be compressed
   * @param[out] err_info error info
   *
   * @return void
   */
  void Compress(kwdbContext_p ctx, const timestamp64& compress_ts, uint32_t& compressed_num, ErrorInfo& err_info);

  void Vacuum(kwdbContext_p ctx, uint32_t ts_version, ErrorInfo& err_info);

  void ReleasePartitionTable(TsTimePartition* e_bt, bool is_force = false);

  /**
   * @brief Obtain the next available groupId and EntityID
   * @param[in] tag_hash The hash id calculated based on the primary tag entity
   * @param[out] group_id available group id
   * @param[out] entity_id Available entity id
   */
  int AllocateEntity(std::string& primary_tags, uint64_t tag_hash, SubGroupID* group_id, EntityID* entity_id);

  /**
  * @brief Recycle and reuse entity id
  * @param[in] primary The main tag of the tag entity
  * @param[in] group_id  group number
  * @param[in] entity_ids The entity number that can be reused within the group
  */
  int ReuseEntities(SubGroupID group_id, std::vector<EntityID>& entity_ids);

  inline std::string GetSubGroupTblSubPath(SubGroupID subgroup_id) {
    return tbl_sub_path_ + std::to_string(table_id_) + "_" + std::to_string(subgroup_id) + "/";
  }

  inline SubGroupID GetMaxSubGroupId() {
    return max_subgroup_id_;
  }

  void sync(int flags) override;

  /**
   * @brief Hot and cold data tiering migration
   * @return void
   */
  void TierMigrate();

  void GetAllSubGroups(std::list<TsSubEntityGroup*>& sub_grps) {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    for (auto& kv : subgroups_) {
      sub_grps.push_back(kv.second);
    }
  }

  /**
   * @brief Count the segment in all subgroups of entity group
   * @param[out] err_info error info
   *
   * @return void
   */
  void Count(kwdbContext_p ctx, ErrorInfo& err_info);

 private:
  std::string db_path_;
  std::string tbl_sub_path_;
  uint64_t table_id_;
  // Referenced from the root table manager under the directory of the time series table
  MMapRootTableManager*& root_table_manager_;

  SubGroupID max_subgroup_id_ = 0;
  // Record the maximum addable device subgroup id
  SubGroupID max_active_subgroup_id_ = 1;
  map<SubGroupID , TsSubEntityGroup*> subgroups_;

  using SubEntityGroupManagerLatch = KLatch;
  using SubEntityGroupManagerRWLatch = KRWLatch;

  SubEntityGroupManagerLatch* subgroup_mgr_mutex_;
  SubEntityGroupManagerRWLatch* subgroup_mgr_rwlock_;

  void mutexLock() override {
    MUTEX_LOCK(subgroup_mgr_mutex_);
  }

  void mutexUnlock() override {
    MUTEX_UNLOCK(subgroup_mgr_mutex_);
  }

  int rdLock() override {
    return RW_LATCH_S_LOCK(subgroup_mgr_rwlock_);
  }
  int wrLock() override {
    return RW_LATCH_X_LOCK(subgroup_mgr_rwlock_);
  }
  int unLock() override {
    return RW_LATCH_UNLOCK(subgroup_mgr_rwlock_);
  }
  int removeDir(string path);

  // Load the internal methods of TsSubEntityGroup without locks
  TsSubEntityGroup* openSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info, bool create_not_exist);
};

}  // namespace kwdbts
