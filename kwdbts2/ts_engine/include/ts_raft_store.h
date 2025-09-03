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
#include <unordered_map>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "lt_rw_latch.h"

namespace kwdbts {

enum RaftKeyType {
  PUT = 1,
  DEL = 2,
};

struct RaftStoreOptions {};

struct RaftValueOffset {
  size_t offset;
  size_t len;
};

class RaftStore {
 public:
  RaftStore(const std::string& file_path, RaftStoreOptions& options) : options_(options), file_path_(file_path) {
    rwlock_ = new KRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_MANAGER_RWLOCK);
  }
  ~RaftStore() { delete rwlock_; }

  KStatus Open();

  KStatus Put(const TSSlice& key, const TSSlice& value);

  KStatus Get(const TSSlice& key, std::string& value);

  KStatus Delete(const TSSlice& key);

  KStatus Recovery();
  KStatus Compact();

  KStatus Close();

 private:
  // The following three functions encapsulate read-write locks to simplify lock operations.
  inline void rdLock() { RW_LATCH_S_LOCK(rwlock_); }
  inline void wrLock() { RW_LATCH_X_LOCK(rwlock_); }
  inline void unLock() { RW_LATCH_UNLOCK(rwlock_); }
  // Read-write lock
  KRWLatch* rwlock_;

  RaftStoreOptions options_;
  std::string file_path_;
  FILE* file_{nullptr};
  uint64_t file_len_{0};
  bool at_end_file_ = false;

  std::unordered_map<std::string, RaftValueOffset> table_;
};
}  // namespace kwdbts
