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

#include "ts_raft_store.h"
#include "lg_api.h"
#include "ts_common.h"

namespace kwdbts {

KStatus RaftStore::Open() {
  file_ = std::fopen(file_path_.c_str(), "wb+");
  if (file_ == NULL) {
    LOG_ERROR("open file [%s] failed. errno: %d.", file_path_.c_str(), errno);
    return KStatus::FAIL;
  }
  std::fseek(file_, 0, SEEK_END);
  file_len_ = ftell(file_);
  LOG_DEBUG("open file [%s] success. file length: %lu", file_path_.c_str(), file_len_);
  return KStatus::SUCCESS;
}

KStatus RaftStore::Put(const TSSlice& key, const TSSlice& value) {
  std::string mem;
  const auto type = RaftKeyType::PUT;
  mem.append(reinterpret_cast<const char*>(&type), 1);
  mem.append(reinterpret_cast<const char*>(&key.len), sizeof(size_t));
  mem.append(key.data, key.len);
  mem.append(reinterpret_cast<const char*>(&value.len), sizeof(size_t));
  mem.append(value.data, value.len);

  wrLock();
  if (!at_end_file_) {
    std::fseek(file_, 0, SEEK_END);
    at_end_file_ = true;
  }
  assert(file_len_ == ftell(file_));

  size_t value_offset = file_len_ + 1 + sizeof(size_t) + key.len + sizeof(size_t);
  table_[std::string(key.data, key.len)] = RaftValueOffset{value_offset, value.len};
  fwrite(mem.c_str(), mem.size(), 1, file_);
  file_len_ += mem.size();
  unLock();

  return KStatus::SUCCESS;
}

KStatus RaftStore::Get(const TSSlice& key, std::string& value) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  KStatus s = KStatus::SUCCESS;
  auto iter = table_.find(std::string(key.data, key.len));
  if (iter != table_.end()) {
    auto value_offset = iter->second;
    at_end_file_ = false;
    if (std::fseek(file_, value_offset.offset, SEEK_SET) == 0) {
      value.resize(value_offset.len);
      auto read_num = fread(&value[0], 1, value_offset.len, file_);
      if (read_num != value_offset.len) {
        LOG_ERROR("only read[%lu] from file [%s] not same as [%lu].", read_num, file_path_.c_str(), value_offset.len);
        s = KStatus::SUCCESS;
      }
    } else {
      LOG_ERROR("cannot seek location[%lu] of file [%s] .", value_offset.offset, file_path_.c_str());
      s = KStatus::FAIL;
    }

  } else {
    s = KStatus::FAIL;
  }

  return s;
}

KStatus RaftStore::Close() {
  fclose(file_);
  return KStatus::SUCCESS;
}
}  // namespace kwdbts
