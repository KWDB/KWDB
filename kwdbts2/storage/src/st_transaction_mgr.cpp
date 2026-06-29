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
#include "ts_io.h"

namespace kwdbts {

static constexpr size_t kTransIdRecordSize = LogEntry::TS_TRANS_ID_LEN + sizeof(uint64_t);

TSxMgr::TSxMgr(WALMgr* wal_mgr, const std::string& file_path)
    : wal_mgr_(wal_mgr), file_path_(file_path) {
  recoverFromFile();
}

TSxMgr::~TSxMgr() = default;

KStatus TSxMgr::recoverFromFile() {
  if (file_path_.empty()) {
    return SUCCESS;
  }
  std::ifstream ifs(file_path_, std::ios::in | std::ios::binary);
  if (!ifs.is_open()) {
    return SUCCESS;
  }
  ifs.seekg(0, std::ios::end);
  auto file_size = ifs.tellg();
  if (file_size <= 0 || (file_size % kTransIdRecordSize) != 0) {
    LOG_WARN("ts_trans_ids file size invalid: %ld, path: %s", static_cast<uint64_t>(file_size), file_path_.c_str());
    ifs.close();
    return SUCCESS;
  }
  ifs.seekg(0, std::ios::beg);
  size_t count = file_size / kTransIdRecordSize;
  for (size_t i = 0; i < count; ++i) {
    char uuid_buf[LogEntry::TS_TRANS_ID_LEN];
    uint64_t mini_trans_id = 0;
    ifs.read(uuid_buf, LogEntry::TS_TRANS_ID_LEN);
    ifs.read(reinterpret_cast<char*>(&mini_trans_id), sizeof(uint64_t));
    if (!ifs.good()) {
      LOG_WARN("read ts_trans_ids file failed at record %zu", i);
      break;
    }
    std::string uuid(uuid_buf, LogEntry::TS_TRANS_ID_LEN);
    ts_trans_ids_[uuid] = mini_trans_id;
    mtr_to_tsx_[mini_trans_id] = uuid;
  }
  ifs.close();
  return SUCCESS;
}

KStatus TSxMgr::persistToFile() {
  if (file_path_.empty()) {
    return SUCCESS;
  }
  std::ofstream ofs(file_path_, std::ios::out | std::ios::binary | std::ios::trunc);
  if (!ofs.is_open()) {
    LOG_ERROR("failed to open ts_trans_ids file for writing: %s", file_path_.c_str());
    return FAIL;
  }
  for (const auto& entry : ts_trans_ids_) {
    ofs.write(entry.first.data(), LogEntry::TS_TRANS_ID_LEN);
    ofs.write(reinterpret_cast<const char*>(&entry.second), sizeof(uint64_t));
  }
  ofs.flush();
  // fsync to ensure data is on disk
  auto helper = [](std::filebuf* fb) -> int {
    struct Helper : public std::filebuf {
     public:
      int handle() { return _M_file.fd(); }
    };
    return static_cast<Helper*>(fb)->handle();
  };
  if (fsync(helper(ofs.rdbuf())) < 0) {
    auto ec = MakeErrorCode(errno);
    LOG_ERROR("fsync ts_trans_ids file failed, reason: %s", ec.message().c_str());
    ofs.close();
    return FAIL;
  }
  ofs.close();
  return SUCCESS;
}

KStatus TSxMgr::TSxBegin(kwdbContext_p ctx, const char* ts_trans_id) {
  TS_OSN mini_trans_id = 0;
  char* wal_log = TTREntry::construct(WALLogType::TS_BEGIN, mini_trans_id, ts_trans_id);
  size_t wal_len = TTREntry::fixed_length;
  KStatus s = wal_mgr_->WriteWAL(ctx, wal_log, wal_len, mini_trans_id);
  delete[] wal_log;
  return s;
}

KStatus TSxMgr::TSxCommit(kwdbContext_p ctx, const char* ts_trans_id) {
  std::string uuid = TTREntry::constructUUID(ts_trans_id);
  KStatus status = wal_mgr_->WriteTSxWAL(ctx, getMtrID(uuid), ts_trans_id, WALLogType::TS_COMMIT);

  if (status == FAIL) {
    return status;
  }
  return SUCCESS;
}

KStatus TSxMgr::TSxRollback(kwdbContext_p ctx, const char* ts_trans_id) {
  std::string uuid = TTREntry::constructUUID(ts_trans_id);
  KStatus status = wal_mgr_->WriteTSxWAL(ctx, getMtrID(uuid), ts_trans_id, WALLogType::TS_ROLLBACK);

  if (status == FAIL) {
    return status;
  }
  return SUCCESS;
}

KStatus TSxMgr::MtrBegin(kwdbts::kwdbContext_p ctx, uint64_t range_id, uint64_t index, TS_OSN& mini_trans_id,
                         const char* tsx_id) {
  if (tsx_id == nullptr) {
    tsx_id = LogEntry::DEFAULT_TS_TRANS_ID;
  } else {
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    std::string key(tsx_id, LogEntry::TS_TRANS_ID_LEN);
    auto it = ts_trans_ids_.find(key);
    if (it != ts_trans_ids_.end()) {
      mini_trans_id = it->second;
      return SUCCESS;
    }
    char* wal_log = MTRBeginEntry::construct(WALLogType::MTR_BEGIN, mini_trans_id, tsx_id,
                                             range_id, index);
    size_t wal_len = MTRBeginEntry::fixed_length;
    KStatus s = wal_mgr_->WriteWAL(ctx, wal_log, wal_len, mini_trans_id);
    if (s == KStatus::FAIL) {
      delete[] wal_log;
      return s;
    }
    std::string uuid = TTREntry::constructUUID(tsx_id);
    ts_trans_ids_.insert(std::make_pair(uuid, mini_trans_id));
    mtr_to_tsx_[mini_trans_id] = uuid;
    persistToFile();
    delete[] wal_log;
    return s;
  }
  char* wal_log = MTRBeginEntry::construct(WALLogType::MTR_BEGIN, mini_trans_id, tsx_id,
                                           range_id, index);
  size_t wal_len = MTRBeginEntry::fixed_length;
  KStatus s = wal_mgr_->WriteWAL(ctx, wal_log, wal_len, mini_trans_id);
  delete[] wal_log;
  return s;
}

KStatus TSxMgr::MtrCommit(kwdbts::kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id) {
  if (tsx_id != nullptr) {
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    std::string key(tsx_id, LogEntry::TS_TRANS_ID_LEN);
    auto it = ts_trans_ids_.find(key);
    if (it != ts_trans_ids_.end()) {
      mtr_to_tsx_.erase(it->second);
      ts_trans_ids_.erase(it);
      persistToFile();
    } else {
      return SUCCESS;
    }
  }
  return wal_mgr_->WriteMTRWAL(ctx, mini_trans_id, LogEntry::DEFAULT_TS_TRANS_ID, WALLogType::MTR_COMMIT);
}

KStatus TSxMgr::MtrRollback(kwdbts::kwdbContext_p ctx, uint64_t mini_trans_id, const char* tsx_id) {
  if (tsx_id != nullptr) {
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    std::string key(tsx_id, LogEntry::TS_TRANS_ID_LEN);
    auto it = ts_trans_ids_.find(key);
    if (it != ts_trans_ids_.end()) {
      mtr_to_tsx_.erase(it->second);
      ts_trans_ids_.erase(it);
      persistToFile();
    } else {
      return SUCCESS;
    }
  }
  return wal_mgr_->WriteMTRWAL(ctx, mini_trans_id, LogEntry::DEFAULT_TS_TRANS_ID, WALLogType::MTR_ROLLBACK);
}

}  // namespace kwdbts
