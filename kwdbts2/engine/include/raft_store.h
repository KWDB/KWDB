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

#include <unistd.h>
#include <memory>
#include <unordered_map>
#include <map>
#include <string>
#include <condition_variable>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "lt_rw_latch.h"
#include "cm_kwdb_context.h"

using namespace kwdbts; // NOLINT
enum RaftKeyType {
  PUT = 1,
  DELSPAN = 2,
  DELSTATE = 3,
  CLEAR = 4,
  TRUNCATE = 5,
};

struct FileHandle {
  int file;
  std::string path;
  explicit FileHandle(int file_id = -1, std::string file_path = "") : file(file_id), path(file_path) {}
};

#pragma pack(1)
struct RaftLogHeader {
  char type;
  uint64_t rangeID;
  uint64_t index;
  union {uint64_t size; uint64_t endIndex;};
};
#pragma pack()

struct RaftValueOffset {
  uint64_t index_id;
  int file_id;
  size_t offset;
  size_t len;
  TSSlice value_ptr;
  std::shared_ptr<RaftValueOffset> next;
 public:
  RaftValueOffset(uint64_t index_id, int file_id, size_t offset, size_t len):
  index_id(index_id), file_id(file_id), offset(offset), len(len), next(nullptr) {
    value_ptr = {nullptr, 0};
  }
  ~RaftValueOffset() {
    if (value_ptr.data != nullptr) {
      free(value_ptr.data);
      value_ptr.data = nullptr;
    }
  }
};

struct RaftStore {
 public:
  RaftStore() {
    rwlock_ = new KRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_MANAGER_RWLOCK);
  }
  explicit RaftStore(std::string engine_root_path) {
    rwlock_ = new KRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_MANAGER_RWLOCK);
  }
  ~RaftStore() {
    Close();
  }

  KStatus Open();
  KStatus init(std::string engine_root_path);

  KStatus Get(kwdbContext_p ctx, uint64_t range_id, uint64_t start, uint64_t end, TSSlice* value);
  KStatus GetFirst(kwdbContext_p ctx, uint64_t range_id, TSSlice* value);
  KStatus GetFirstIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t *index_id);
  KStatus GetLastIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t *index_id);

  KStatus WriteRaftLog(kwdbContext_p ctx, int cnt, TSRaftlog *raftlog, bool sync);
  KStatus Sync(kwdbContext_p ctx);
  KStatus HasRange(kwdbContext_p ctx, uint64_t rangeID);
  KStatus Close();

#ifdef WITH_TESTS
  KStatus resizeMax() {
    file_max_size = 100;
    return KStatus::SUCCESS;
  }
  KStatus reStart() {
    wrLock();
    close(file_);
    stopCompact();
    // Close the file handle that is already filled with raftlog.
    for (auto file : previous_files_) {
      if (file.second.file != -1) {
        close(file.second.file);
      }
    }
    previous_files_.clear();
    ranges_.clear();
    for (auto indexes : state_) {
      if (indexes.second->value_ptr.data != nullptr) {
        free(indexes.second->value_ptr.data);
        indexes.second->value_ptr.data = nullptr;
      }
    }
    state_.clear();
    current_id_ = 1;
    unLock();
    return KStatus::SUCCESS;
  }
#endif

 private:
  // The following three functions encapsulate read-write locks to simplify lock operations.
  inline void rdLock() { RW_LATCH_S_LOCK(rwlock_); }
  inline void wrLock() { RW_LATCH_X_LOCK(rwlock_); }
  inline void unLock() { RW_LATCH_UNLOCK(rwlock_); }
  // Read-write lock
  KRWLatch* rwlock_;
  std::string file_path_;
  int current_id_ = 1;
  int file_{-1};
  uint64_t file_len_{0};

  int compact_file_{-1};
  uint64_t compact_len_{0};
  std::atomic<bool> isCompact = false;
  size_t file_max_size = 1024 * 1024 * 512;
  int files_max_num = 6;
  std::unordered_map<uint64_t, std::shared_ptr<RaftValueOffset>> state_;
  std::unordered_map<uint64_t, std::shared_ptr<RaftValueOffset>> ranges_;
  std::map<int, FileHandle> previous_files_;

  // compact asynchronous thread scheduled tasks.
  std::atomic<bool> running_{false};
  std::thread compactThread_;
  std::condition_variable cv_;
  std::mutex mutex_;
  const std::chrono::minutes interval_{30};
  void startCompact();
  void stopCompact();

  KStatus putIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id, TSSlice &value, std::string &mem);
  KStatus put(kwdbContext_p ctx, TSRaftlog *raftlog, std::string &mem);
  KStatus getDiskValue(kwdbContext_p ctx, std::shared_ptr<RaftValueOffset>& index_id, TSSlice* value, bool isCompact);

  KStatus multDelete(kwdbContext_p ctx, uint64_t range_id, uint64_t start_id,
                     uint64_t end_id, std::string &mem, bool write_disk);
  KStatus deleteFromDisk(kwdbContext_p ctx, uint64_t range_id, uint64_t start, uint64_t end);
  KStatus clearRange(kwdbContext_p ctx, uint64_t range_id, std::string &mem);
  KStatus deleteState(kwdbContext_p ctx, uint64_t range_id, std::string &mem, bool write_disk);
  KStatus truncate(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id, std::string &mem, bool write_disk);

  KStatus compact();
  uint64_t compactPut(uint64_t range_id, uint64_t index_id, TSSlice& value, std::string &mem);
  KStatus loadFile(kwdbContext_p ctx, FileHandle& file_handle, int file_id);
  KStatus loadIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id, int file_id, uint64_t offset, size_t len);
  KStatus createNewFile();
  bool checkIsCompact(int file1, int file2);

  uint64_t buildRaftLog(kwdbContext_p ctx, uint64_t range_id, uint64_t index,
                        TSSlice *raftlog, std::string &mem, uint64_t &val_len);
};
