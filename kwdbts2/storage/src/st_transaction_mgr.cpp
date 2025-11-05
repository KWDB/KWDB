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
#include "kwdb_type.h"
#include "st_transaction_mgr.h"
#include "st_wal_mgr.h"

namespace kwdbts {
TSxMgr::TSxMgr(WALMgr* wal_mgr) : wal_mgr_(wal_mgr) {}

TSxMgr::~TSxMgr() = default;

KStatus TSxMgr::TSxBegin(kwdbContext_p ctx, const char* ts_trans_id) {
  TS_OSN mini_trans_id = 0;
  char* wal_log = MTRBeginEntry::construct(WALLogType::TS_BEGIN, mini_trans_id, ts_trans_id, 0, 0);
  size_t wal_len = MTRBeginEntry::fixed_length;
  KStatus s = wal_mgr_->WriteWAL(ctx, wal_log, wal_len, mini_trans_id);
  delete[] wal_log;

  std::string uuid = TTREntry::constructUUID(ts_trans_id);
  std::unique_lock<std::shared_mutex> lock(map_mutex_);
  ts_trans_ids_.insert(std::make_pair(uuid, mini_trans_id));
  return s;
}

KStatus TSxMgr::TSxCommit(kwdbContext_p ctx, const char* ts_trans_id) {
  std::string uuid = TTREntry::constructUUID(ts_trans_id);
  KStatus status = wal_mgr_->WriteTSxWAL(ctx, getMtrID(uuid), ts_trans_id, WALLogType::TS_COMMIT);

  if (status == FAIL) {
    return status;
  }
  std::unique_lock<std::shared_mutex> lock(map_mutex_);
  ts_trans_ids_.erase(uuid);
  return SUCCESS;
}

KStatus TSxMgr::TSxRollback(kwdbContext_p ctx, const char* ts_trans_id) {
  std::string uuid = TTREntry::constructUUID(ts_trans_id);
  KStatus status = wal_mgr_->WriteTSxWAL(ctx, getMtrID(uuid), ts_trans_id, WALLogType::TS_ROLLBACK);

  if (status == FAIL) {
    return status;
  }
  std::unique_lock<std::shared_mutex> lock(map_mutex_);
  ts_trans_ids_.erase(uuid);
  return SUCCESS;
}

KStatus TSxMgr::MtrBegin(kwdbts::kwdbContext_p ctx, uint64_t range_id, uint64_t index, TS_OSN& mini_trans_id,
                         const char* tsx_id) {
  if (tsx_id == nullptr) {
    tsx_id = LogEntry::DEFAULT_TS_TRANS_ID;
  }
  char* wal_log = MTRBeginEntry::construct(WALLogType::MTR_BEGIN, mini_trans_id, tsx_id,
                                           range_id, index);
  size_t wal_len = MTRBeginEntry::fixed_length;
  KStatus s;
  map_mutex_.lock();
  if (ts_trans_ids_.find(tsx_id) == ts_trans_ids_.end()) {
    s = wal_mgr_->WriteWAL(ctx, wal_log, wal_len, mini_trans_id);
  } else {
    map_mutex_.unlock();
    delete [] wal_log;
    return SUCCESS;
  }
  map_mutex_.unlock();

  if (tsx_id != LogEntry::DEFAULT_TS_TRANS_ID) {
    map_mutex_.lock();
    ts_trans_ids_.emplace(std::make_pair(tsx_id, mini_trans_id));
    map_mutex_.unlock();
  }
  delete[] wal_log;
  return s;
}

KStatus TSxMgr::MtrCommit(kwdbts::kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id) {
  if (tsx_id != nullptr) {
    map_mutex_.lock();
    if (ts_trans_ids_.find(tsx_id) != ts_trans_ids_.end()) {
      ts_trans_ids_.erase(tsx_id);
    } else {
      map_mutex_.unlock();
      return SUCCESS;
    }
    map_mutex_.unlock();
  }
  return wal_mgr_->WriteMTRWAL(ctx, mini_trans_id, LogEntry::DEFAULT_TS_TRANS_ID, WALLogType::MTR_COMMIT);
}

KStatus TSxMgr::MtrRollback(kwdbts::kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id) {
  if (tsx_id != nullptr) {
    map_mutex_.lock();
    if (ts_trans_ids_.find(tsx_id) != ts_trans_ids_.end()) {
      ts_trans_ids_.erase(tsx_id);
    } else {
      map_mutex_.unlock();
      return SUCCESS;
    }
    map_mutex_.unlock();
  }
  return wal_mgr_->WriteMTRWAL(ctx, mini_trans_id, LogEntry::DEFAULT_TS_TRANS_ID, WALLogType::MTR_ROLLBACK);
}

}  // namespace kwdbts
