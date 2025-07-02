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

#include <string>
#include <utility>
#include <shared_mutex>
#include <map>
#include "st_wal_mgr.h"

namespace kwdbts {

class WALMgr;
/**
 * TimeSeries table transaction Management class, with each EntityGroup maintaining a separate TSxMgr instance.
 * there is two type:
 * 1. TS Mini-transaction(mini-trans): The TS Mini-transaction guarantees the atomicity of a RaftLog write
 * at the replica layer.
 * 2. TS Transaction(TSx): TS transaction, which ensure the atomicity of user DML operations, correspond to
 * KaiwuDB distributed transactions.
 *
 * The life cycle of TSxMgr is the same as that of EntityGroup.
*/
class TSxMgr {
 public:
  explicit TSxMgr(WALMgr*);

  ~TSxMgr();

  /**
   * Start a transaction.
   * @param ctx TS Engine context
   * @param ts_trans_id distributed txn ID, provided by the distributed layer
   * @return
   */
  KStatus TSxBegin(kwdbContext_p ctx, const char* ts_trans_id);


  /**
  * Commit a transaction.
  * @param ctx TS Engine context
  * @param ts_trans_id distributed txn ID, provided by the distributed layer
  * @return
  */
  KStatus TSxCommit(kwdbContext_p ctx, const char* ts_trans_id);

  /**
   * Rollback a transaction.
   * @param ctx TS Engine context
   * @param ts_trans_id distributed txn ID, provided by the distributed layer.
   * @return
   */
  KStatus TSxRollback(kwdbContext_p ctx, const char* ts_trans_id);

  /**
   * Start a Mini-transaction.
   * @param ctx[in] TS Engine context
   * @param mini_trans_id[out] Mini-Transaction ID, the value is the LSN of the WAL log entry
   * @return
   */
  KStatus MtrBegin(kwdbContext_p ctx, uint64_t range_id, uint64_t index, uint64_t& mini_trans_id,
                   const char* tsx_id = nullptr);

  /**
   * Commit a Mini-transaction.
   * @param ctx[in] TS Engine context
   * @param mini_trans_id[out] Mini-Transaction ID, the value is the LSN of the WAL log entry
   * @return
   */
  KStatus MtrCommit(kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id = nullptr);

  /**
   * Rollback a Mini-transaction.
   * @param ctx TS Engine context
   * @param mini_trans_id Mini-Transaction ID, the value is the LSN of the WAL log entry
   * @return
   */
  KStatus MtrRollback(kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id = nullptr);

  uint64_t getMtrID(const std::string& uuid) {
    uint64_t x_id = 0;
    std::shared_lock<std::shared_mutex> lock(map_mutex_);
    std::map<std::string, uint64_t>::iterator iter = ts_trans_ids_.find(uuid);
    if (iter != ts_trans_ids_.end()) {
      x_id = iter->second;
    }
    return x_id;
  }

  uint64_t getMtrID(const char* ts_trans_id) {
    return getMtrID(TTREntry::constructUUID(ts_trans_id));
  }

  void insertMtrID(const char* ts_trans_id, uint64_t mini_trans_id) {
    std::string uuid = TTREntry::constructUUID(ts_trans_id);
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    ts_trans_ids_.insert(std::make_pair(uuid, mini_trans_id));
  }

  void eraseMtrID(const uint64_t mini_trans_id) {
    for (auto it = ts_trans_ids_.begin(); it != ts_trans_ids_.end(); ) {
      if (it->second == mini_trans_id) {
        ts_trans_ids_.erase(it->first);
        break;
      } else {
        ++it;
      }
    }
  }

 private:
  std::shared_mutex map_mutex_;
  WALMgr* wal_mgr_{nullptr};
  std::map<std::string, uint64_t> ts_trans_ids_{};
};

}  // namespace kwdbts
