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

#include <sys/time.h>

#include "st_byrl_table.h"
#include "sys_utils.h"

namespace kwdbts {

RaftLoggedTsTable::RaftLoggedTsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id)
    : TsTable(ctx, db_path, table_id) {}

RaftLoggedTsTable::~RaftLoggedTsTable() {}

KStatus RaftLoggedTsTable::Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups, ErrorInfo& err_info) {
  return TsTable::Init(ctx, range_groups, err_info);
}

KStatus RaftLoggedTsTable::CreateCheckpoint(kwdbContext_p ctx) {
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  auto entity_groups_repl = entity_groups_;
  RW_LATCH_UNLOCK(entity_groups_mtx_);
  for (const auto& tbl_range : entity_groups_repl) {
    KStatus s = tbl_range.second->CreateCheckpoint(ctx);
    if (s == FAIL) {
      // ignore the checkpoint error, record it into ts engine log.
      LOG_ERROR("Failed to create a checkpoint for entity group %lu of table id %lu",
                tbl_range.second->HashRange().range_group_id, table_id_)
    }
  }
  return SUCCESS;
}

RaftLoggedTsEntityGroup::RaftLoggedTsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_bt_manager,
                                         const string& db_path, const KTableKey& table_id, const RangeGroup& range,
                                         const string& tbl_sub_path, uint64_t hash_num)
    : TsEntityGroup(ctx, root_bt_manager, db_path, table_id, range, tbl_sub_path, hash_num) {}

RaftLoggedTsEntityGroup::~RaftLoggedTsEntityGroup() {}

KStatus RaftLoggedTsEntityGroup::CreateCheckpoint(kwdbContext_p ctx) {
  EnterFunc()
  LOG_INFO("create checkpoint for table %lu", table_id_);
  if (new_tag_bt_ == nullptr || root_bt_manager_ == nullptr) {
    LOG_ERROR("new_tag_bt_ is %p, root_bt_manager_ is %p", new_tag_bt_, root_bt_manager_)
    Return(FAIL)
  }
  // wal is not created, use timestamp as lsn
  struct timeval tv;
  gettimeofday(&tv, NULL);
  TS_LSN checkpoint_lsn = tv.tv_sec * (uint64_t)1000000 + tv.tv_usec;

  // call Tag.flush, pass Checkpoint_LSN as parameter
  new_tag_bt_->sync_with_lsn(checkpoint_lsn);

  /**
   * Call the Flush method of the Metrics table with the input parameter Current LSN
   * During the recovery process, check the dat aof each Entity based on Checkpoint LSN to determine the offset of
   * each Entity at the Checkpoint
   * The Checkpoint log of this solution is smaller, but the recovery time is longer.
   *
   * Another solution is to record the offset of each Entity at Checkpoint moment in the Checkpoint logs
   * The offset is used to reset the current write offset of the corresponding Entity while recovery
   * In this solution, the Checkpoint log is large, but the recovery time is reduced.
   *
   * At present, solution 1 is used, and later performance evaluation is needed to determine the final scheme.
  **/
  ErrorInfo err_info;
  if (root_bt_manager_->Sync(checkpoint_lsn, err_info) != 0) {
    LOG_ERROR("Failed to flush the Metrics table.")
    Return(FAIL)
  }
  // force sync metric data into files.
  ebt_manager_->sync(0);

  Return(SUCCESS)
}

}  //  namespace kwdbts
