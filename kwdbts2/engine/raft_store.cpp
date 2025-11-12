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

#include <cstdlib>
#include "snappy.h"
#include "raft_store.h"
#include "lg_api.h"
#include "ts_common.h"
#include "sys_utils.h"

KStatus RaftStore::Open() {
  file_ = open(file_path_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
  if (file_ == -1) {
    LOG_ERROR("open file [%s] failed. errno: %d.", file_path_.c_str(), errno);
    return KStatus::FAIL;
  }
  file_len_ = lseek(file_, 0, SEEK_END);
  return KStatus::SUCCESS;
}

KStatus RaftStore::putIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id, TSSlice &value, string &mem) {
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    uint64_t val_len;
    uint64_t off = buildRaftLog(ctx, range_id, index_id, &value, mem, val_len);
    std::shared_ptr<RaftValueOffset> index = std::make_shared<RaftValueOffset>(index_id, current_id_, off, val_len);
    index->next = nullptr;
    ranges_.insert(std::pair(range_id, index));
  } else {
    auto tmp_index = indexes->second;
    uint64_t max_index_id = tmp_index->index_id;
    // Whether insertion into the linked list header is allowed.
    if (index_id > tmp_index->index_id && index_id != tmp_index->index_id + 1) {
      LOG_WARN("raft log is not continuous, [%lu, %lu], rangeID[%lu]", tmp_index->index_id, index_id, range_id);
      // The migration of the copy occurs, delete all Raftlogs of range_id and insert a new raftLog.
      RaftLogHeader header;
      header.type = RaftKeyType::TRUNCATE;
      header.rangeID = range_id;
      header.index = max_index_id + 1;
      mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
      uint64_t val_len;
      uint64_t off = buildRaftLog(ctx, range_id, index_id, &value, mem, val_len);
      std::shared_ptr<RaftValueOffset> index = std::make_shared<RaftValueOffset>(index_id, current_id_, off, val_len);
      ranges_.erase(range_id);
      ranges_.insert(std::pair(range_id, index));
      return KStatus::SUCCESS;
    }

    while (tmp_index->next != nullptr && index_id < tmp_index->index_id) {
      tmp_index = tmp_index->next;
    }
    // A conflict occurred when inserting raftlog.
    if (tmp_index->index_id == index_id) {
      if (index_id != 0) {
        LOG_INFO("raft meets conflict at index %lu, rangeID:%lu", index_id, range_id);
      }
      RaftLogHeader header;
      header.type = RaftKeyType::DELSPAN;
      header.rangeID = range_id;
      header.index = index_id;
      header.endIndex = max_index_id + 1;
      mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
      tmp_index = tmp_index->next;
    }
    uint64_t val_len;
    uint64_t off = buildRaftLog(ctx, range_id, index_id, &value, mem, val_len);
    std::shared_ptr<RaftValueOffset> index = std::make_shared<RaftValueOffset>(index_id, current_id_, off, val_len);
    index->next = tmp_index;
    indexes->second = index;
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::loadIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id,
                             int file_id, uint64_t offset, size_t len) {
  std::shared_ptr<RaftValueOffset> index = std::make_shared<RaftValueOffset>(index_id, file_id, offset, len);
  wrLock();
  Defer defer([&]() { unLock(); });
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    index->next = nullptr;
    ranges_.insert(std::pair(range_id, index));
  } else {
    if (index_id > indexes->second->index_id) {
      index->next = indexes->second;
      ranges_[range_id] = index;
    } else {
      std::shared_ptr<RaftValueOffset> tmp_index = indexes->second;
      std::shared_ptr<RaftValueOffset> next_index = tmp_index->next;
      while (next_index != nullptr && index_id < next_index->index_id) {
        tmp_index = next_index;
        next_index = next_index->next;
      }
      if (next_index != nullptr && next_index->index_id == index_id) {
        if (index_id != 0) {
          LOG_WARN("A raftLog with the same index already exists, rangeID:%lu, index: %lu", range_id, index_id);
        }
        tmp_index->next = index;
        index->next = next_index->next;
        return KStatus::SUCCESS;
      }
      tmp_index->next = index;
      index->next = next_index;
    }
  }
  return KStatus::SUCCESS;
}

uint64_t RaftStore::buildRaftLog(kwdbContext_p ctx, uint64_t range_id, uint64_t index,
                                 TSSlice *raftlog, std::string &mem, uint64_t& val_len) {
    uint64_t off = file_len_ + mem.size();
    RaftLogHeader header;
    header.type = RaftKeyType::PUT;
    header.rangeID = range_id;
    header.index = index;
    // If the value is not empty, compress the value.
    if (raftlog->len != 0) {
      std::string val;
      snappy::Compress(raftlog->data, raftlog->len, &val);
      val_len = val.size();
      header.size = val.size();
      mem.append(reinterpret_cast<char*>(&header), sizeof(RaftLogHeader));
      mem.append(val.c_str(), val.size());
    } else {
      header.size = raftlog->len;
      mem.append(reinterpret_cast<char*>(&header), sizeof(RaftLogHeader));
    }
    return off + sizeof(RaftLogHeader);
}

KStatus RaftStore::put(kwdbContext_p ctx, TSRaftlog *raftlog, std::string &mem) {
  uint64_t range_id = raftlog->range_id;
  for (int i = 0; i < raftlog->index_cnt; i++) {
    TSSlice log{.data = raftlog->data + raftlog->offs[i], .len = raftlog->offs[i + 1] - raftlog->offs[i]};
    if (raftlog->indexes[i] == 0) {  // Write the status raftlog
      // Save the TSSlice before compression.
      char* value = reinterpret_cast<char *>(malloc(log.len));
      if (value == nullptr) {
        LOG_ERROR("failed malloc %lu bytes, rangeID:%lu, index:%lu.", log.len, range_id, raftlog->indexes[i]);
        return KStatus::FAIL;
      }
      memcpy(value, log.data, log.len);
      // Compress and calculate the offset of the value.
      uint64_t val_len;
      uint64_t off = buildRaftLog(ctx, range_id, raftlog->indexes[i], &log, mem, val_len);
      state_[range_id] = std::make_shared<RaftValueOffset>(0, current_id_, off, val_len);
      // store cache
      state_[range_id]->value_ptr.data = value;
      state_[range_id]->value_ptr.len = log.len;
    } else {  // Write data raftlog
      if (putIndex(ctx, range_id, raftlog->indexes[i], log, mem) == KStatus::FAIL) {
        return KStatus::FAIL;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::createNewFile() {
  std::string previous_id = std::to_string(current_id_);
  fs::path file_path = file_path_;
  std::string previous_file = file_path.parent_path().string() + "/previous_" + previous_id + ".raftlog";
  fs::rename(file_path_.c_str(), previous_file.c_str());
  FileHandle previous_handle = FileHandle(file_, previous_file);
  previous_files_.insert(std::pair(current_id_, previous_handle));
  // Create a new file for writing raftlog.
  current_id_++;
  std::string current_file =
      file_path.parent_path().string() + "/current_" + std::to_string(current_id_) + ".raftlog";
  file_path_ = current_file;
  Open();
  return KStatus::SUCCESS;
}

KStatus RaftStore::WriteRaftLog(kwdbContext_p ctx, int cnt, TSRaftlog *raftlog, bool sync) {
  std::string mem;
  KStatus s = KStatus::SUCCESS;
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (int i = 0; i < cnt && s == KStatus::SUCCESS; i++) {
    if (raftlog[i].data != nullptr) {  // The value is not empty, and the write process is executed.
      s = put(ctx, &raftlog[i], mem);
    } else {
      // When the value is empty, the deletion process is executed.
      switch (raftlog[i].index_cnt) {
        case 0:  // Delete all Raftlogs of the range, including status raftlogs and data raftlogs.
          s = clearRange(ctx, raftlog[i].range_id, mem);
          break;
        case 1:  // Delete the data raftLog that is less than the specified index.
          s = truncate(ctx, raftlog[i].range_id, raftlog[i].indexes[0], mem, true);
          break;
        case 2:  // Delete the status raftLog.
          if (raftlog[i].indexes[0] == 0) {
            s = deleteState(ctx, raftlog[i].range_id, mem, true);
          } else {
            // Delete the data raftLog within the specified range, where the interval is left-closed and right-open.
            s = multDelete(ctx, raftlog[i].range_id, raftlog[i].indexes[0], raftlog[i].indexes[1], mem, true);
          }
          break;
        default:
          LOG_WARN("Unknown operation types of raftlog, rangeID[%lu].",  raftlog[i].range_id);
          s = KStatus::FAIL;
          break;
      }
    }
  }
  if (s == KStatus::FAIL) {
    return s;
  }

  ssize_t write_num = write(file_, mem.c_str(), mem.size());
  if (write_num != mem.size()) {
    LOG_ERROR("Raftlog cannot be written normally.");
    return KStatus::FAIL;
  }
  file_len_ += mem.size();
  if (sync) {
    fsync(file_);
  }
  if (file_len_ > file_max_size) {
    createNewFile();
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::Get(kwdbContext_p ctx, uint64_t range_id, uint64_t start, uint64_t end, TSSlice* value) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (start == 0) {  // Query status raftlog
    auto index = state_[range_id];
    if (index != nullptr) {
      if (index->value_ptr.len != 0 && index->value_ptr.data != nullptr) {
        value->len = index->value_ptr.len;
        value->data = reinterpret_cast<char *>(malloc(index->value_ptr.len));
        if (value->data == nullptr) {
          LOG_ERROR("failed malloc %lu bytes, rangeID:%lu, index:%lu.", index->len, range_id, index->index_id);
        }
        memcpy(value->data, index->value_ptr.data, value->len);
        return KStatus::SUCCESS;
      } else {
        return getDiskValue(ctx, index, value, false);
      }
    }
    return KStatus::FAIL;
  }
  // Query the data raftLog.
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    LOG_ERROR("cannot find range %lu", range_id);
    return KStatus::FAIL;
  }
  auto freeData = [value](uint64_t start, uint64_t end) {
    for (auto i = start; i < end; ++i) {
      free(value[i].data);
    }
  };
  std::shared_ptr<RaftValueOffset> index = indexes->second;
  for (auto index_id = end - 1; index_id >= start; index_id--) {
    while (index != nullptr && index_id < index->index_id) {
      index = index->next;
    }
    if (index == nullptr || index->index_id != index_id) {
      LOG_ERROR("Could not find raftlog with same index[%lu] of rangeID[%lu] .", index_id, range_id);
      if (index_id != end - 1) {
        freeData(index_id + 1 - start, end - start);
      }
      return KStatus::FAIL;
    }
    if (getDiskValue(ctx, index, &value[index_id - start], false) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::GetFirst(kwdbContext_p ctx, uint64_t range_id, TSSlice* value) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    LOG_ERROR("The first value of the range[%lu] cannot be found.", range_id);
    return KStatus::FAIL;
  }
  auto first_index = indexes->second;
  while (first_index->next != nullptr) {
    first_index = first_index->next;
  }
  if (getDiskValue(ctx, first_index, value, false) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::GetFirstIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t* index_id) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    LOG_ERROR("The first index of the range[%lu] cannot be found.", range_id);
    return KStatus::FAIL;
  }
  auto first_index = indexes->second;
  while (first_index->next != nullptr) {
    first_index = first_index->next;
  }
  *index_id = first_index->index_id;
  return KStatus::SUCCESS;
}

KStatus RaftStore::GetLastIndex(kwdbContext_p ctx, uint64_t range_id, uint64_t* index_id) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    LOG_ERROR("The last index of the range[%lu] cannot be found.", range_id);
    return KStatus::FAIL;
  }
  *index_id = indexes->second->index_id;
  return KStatus::SUCCESS;
}

KStatus RaftStore::getDiskValue(kwdbContext_p ctx, std::shared_ptr<RaftValueOffset>& index_id,
                                TSSlice* value, bool isCompact) {
  if (index_id->len == 0) {
    value->len = index_id->len;
    value->data = nullptr;
    return KStatus::SUCCESS;
  }
  int get_file = {-1};
  std::string file_name;
  if (current_id_ == index_id->file_id) {
    get_file = file_;
    file_name = file_path_;
  } else {
    auto file = previous_files_.find(index_id->file_id);
    if (file != previous_files_.end()) {
      file_name = file->second.path;
      get_file = file->second.file;
      if (get_file == -1) {
        LOG_ERROR("Failed to obtain the file descriptor: [%s].", file_name.c_str());
        return KStatus::FAIL;
      }
    }
  }
  if (lseek(get_file, index_id->offset, SEEK_SET) != index_id->offset) {
    LOG_ERROR("cannot seek location[%lu] of file [%s] .", index_id->offset, file_name.c_str());
    return KStatus::FAIL;
  }
  char* data = reinterpret_cast<char *>(malloc(index_id->len));
  if (data == nullptr) {
    LOG_ERROR("failed malloc %lu bytes, index:%lu.", index_id->len, index_id->index_id);
  }
  size_t read_num = read(get_file, data, index_id->len);
  if (read_num != index_id->len) {
    LOG_ERROR("only read[%lu] from file [%s] not same as [%u].", read_num, file_name.c_str(), index_id->len);
    free(value->data);
    return KStatus::FAIL;
  }
  if (isCompact) {
    value->len = index_id->len;
    value->data = data;
  } else {
    std::string val;
    if (!snappy::Uncompress(data, index_id->len, &val)) {
      LOG_ERROR("Decompression failed! index:%lu.", index_id->index_id);
      free(data);
      return KStatus::FAIL;
    }
    value->data = reinterpret_cast<char *>(malloc(val.size()));
    if (value->data == nullptr) {
      LOG_ERROR("failed malloc %lu bytes, index:%lu.", val.size(), index_id->index_id);
    }
    value->len = val.size();
    memcpy(value->data, val.c_str(), val.size());
    free(data);
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::multDelete(kwdbContext_p ctx, uint64_t range_id, uint64_t start_id, uint64_t end_id,
                              std::string &mem, bool write_disk) {
  auto indexes = ranges_.find(range_id);
  if (indexes == ranges_.end()) {
    LOG_WARN("Could not find raftlog of rangeID[%lu], start:[%lu], end:[%lu].", range_id, start_id, end_id);
    return KStatus::SUCCESS;
  }
  std::shared_ptr<RaftValueOffset> tmp_index = indexes->second;
  if (tmp_index->index_id < start_id) {
    LOG_WARN("deleting much newer raft logs, lastIndex: %lu, span[%lu, %lu], rangeID[%lu]", tmp_index->index_id,
             start_id, end_id, range_id);
    return KStatus::SUCCESS;
  }
  // Search for the largest raftlog that needs to be deleted.
  while (tmp_index != nullptr && tmp_index->index_id > end_id) {
    tmp_index = tmp_index->next;
  }
  if (tmp_index == nullptr) {
    LOG_WARN("deleting much older raft log, index [%lu], rangeID[%lu].", end_id, range_id);
    return KStatus::SUCCESS;
  }
  auto end_index = tmp_index;
  if (tmp_index->index_id < end_id) {
    LOG_INFO("deleting end %lu higher than last index %lu", end_id, tmp_index->index_id);
    // Record the id of the index that was actually deleted.
    end_id = tmp_index->index_id + 1;
    end_index = nullptr;
  }
  auto start_index = tmp_index->next;
  // Search for the smallest raftlog that needs to be deleted.
  while (start_index != nullptr && start_id < start_index->index_id) {
    tmp_index = start_index;
    start_index = start_index->next;
  }
  if (start_index == nullptr) {
    start_id = tmp_index->index_id;
  } else {
    start_index = start_index->next;
  }

  if (end_index != nullptr) {
    end_index->next = start_index;
  } else {
    if (start_index != nullptr) {
      ranges_[range_id] = start_index;
    } else {
      // all cleared
      ranges_.erase(range_id);
    }
  }
  // Avoid the last node of the linked list being the largest node to be deleted.
  if (write_disk && start_id != end_id) {
    RaftLogHeader header;
    header.type = RaftKeyType::DELSPAN;
    header.rangeID = range_id;
    header.index = start_id;
    header.endIndex = end_id;
    mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::truncate(kwdbContext_p ctx, uint64_t range_id, uint64_t index_id, string &mem, bool write_disk) {
  // The actual deleted raftlog.
  uint64_t truncate_id = index_id;
  auto indexes = ranges_.find(range_id);
  if (indexes != ranges_.end()) {
    std::shared_ptr<RaftValueOffset> tmp_index = indexes->second;
    std::shared_ptr<RaftValueOffset> next_index = tmp_index->next;
    // Clear all data raftlog.
    if (tmp_index->index_id < index_id) {
      truncate_id = tmp_index->index_id;
      ranges_.erase(range_id);
    } else if (tmp_index->index_id == index_id) {
      // Delete starting from the second node in the linked list.
      tmp_index->next = nullptr;
    } else {
      while (next_index != nullptr && index_id < next_index->index_id) {
        tmp_index = next_index;
        next_index = next_index->next;
      }
      if (next_index == nullptr) {
        LOG_WARN("Could not find raftlog with same index[%lu] of rangeID[%lu] .", index_id, range_id);
        return KStatus::SUCCESS;
      }
      if (next_index->index_id == index_id) {
        next_index->next = nullptr;
      } else {
        truncate_id = tmp_index->index_id;
        tmp_index->next = nullptr;
      }
    }
    if (write_disk) {
      RaftLogHeader header;
      header.type = RaftKeyType::TRUNCATE;
      header.rangeID = range_id;
      header.index = truncate_id;
      mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
    }
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::clearRange(kwdbContext_p ctx, uint64_t range_id, std::string &mem) {
  KStatus status = KStatus::SUCCESS;
  auto state = state_.find(range_id);
  // Avoid crashing caused by an empty value in the state.
  if (state != state_.end() && state->second != NULL && state->second->value_ptr.data != nullptr) {
    free(state->second->value_ptr.data);
    state->second->value_ptr.data = nullptr;
  }
  if (state_.erase(range_id) == 0 && ranges_.erase(range_id) == 0) {
    LOG_INFO("no raft log and state for rangeID[%lu]", range_id);
    return KStatus::SUCCESS;
  }
  const auto type = RaftKeyType::CLEAR;
  mem.append(reinterpret_cast<const char *>(&type), 1);
  mem.append(reinterpret_cast<const char *>(&range_id), sizeof(uint64_t));
  return KStatus::SUCCESS;
}

KStatus RaftStore::deleteFromDisk(kwdbContext_p ctx, uint64_t range_id, uint64_t start, uint64_t end) {
  RaftLogHeader header;
  header.type = RaftKeyType::DELSPAN;
  header.rangeID = range_id;
  header.index = start;
  header.endIndex = end;
  std::string mem;
  mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
  write(file_, mem.c_str(), mem.size());
  file_len_ += mem.size();
  return KStatus::SUCCESS;
}

KStatus RaftStore::deleteState(kwdbContext_p ctx, uint64_t range_id, std::string &mem, bool write_disk) {
  KStatus status = KStatus::SUCCESS;
  auto state = state_.find(range_id);
  if (state != state_.end() && state->second != NULL) {
    if (state->second->value_ptr.data != nullptr) {
      free(state->second->value_ptr.data);
      state->second->value_ptr.data = nullptr;
    }
  }
  if (state_.erase(range_id) == 0) {
    LOG_WARN("Could not find state raftlog, rangeID[%lu].", range_id);
    return KStatus::SUCCESS;
  }
  if (write_disk) {
    const auto type = RaftKeyType::DELSTATE;
    mem.append(reinterpret_cast<const char *>(&type), 1);
    mem.append(reinterpret_cast<const char *>(&range_id), sizeof(uint64_t));
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::loadFile(kwdbContext_p ctx, FileHandle& file_handle, int file_id, bool is_cur_file) {
  file_handle.file = open(file_handle.path.c_str(), O_RDWR | O_APPEND);
  if (file_id == current_id_) {
    file_ = file_handle.file;
  }
  if (file_handle.file == -1) {
    LOG_ERROR("open file [%s] failed. errno: %d.", file_handle.path.c_str(), errno);
    return KStatus::FAIL;
  }
  uint64_t len = lseek(file_handle.file, 0, SEEK_END);
  if (len == 0) {
    LOG_WARN("The content of the file[%s] is empty.", file_handle.path.c_str());
    return KStatus::FAIL;
  }
  char* buffer = reinterpret_cast<char*>(malloc(len));
  if (buffer == nullptr) {
    LOG_ERROR("failed malloc %lu bytes", len);
    return KStatus::FAIL;
  }
  Defer defer([&]() {free(buffer);});
  lseek(file_handle.file, 0, SEEK_SET);
  uint64_t read_num = read(file_handle.file, buffer, len);
  if (read_num != len) {
    LOG_ERROR("The content reading failed. The actual length of the read content: %d, and the file length: %d",
              read_num, len);
    return KStatus::FAIL;
  }
  uint64_t off = 0;
  while (off < len) {
    auto header = reinterpret_cast<RaftLogHeader*>(buffer + off);
    uint64_t value_off = 0;
    switch (header->type) {
      case RaftKeyType::PUT:
        value_off = off + sizeof(RaftLogHeader);
        if (is_cur_file && (value_off + header->size > len)) {
          LOG_WARN("data missing, type=%d, rangeID=%lu, index=%lu", header->type, header->rangeID, header->index);
          break;
        }
        if (header->index == 0) {
          state_[header->rangeID] = std::make_shared<RaftValueOffset>(0, file_id, value_off, header->size);
        } else {
          loadIndex(ctx, header->rangeID, header->index, file_id, value_off, header->size);
        }
        off = value_off + header->size;
        break;
      case RaftKeyType::DELSPAN:
      {
        if (is_cur_file && (off + sizeof(RaftLogHeader) > len)) {
          LOG_WARN("data missing, type=%d", header->type);
          break;
        }
        std::string mem;
        multDelete(ctx, header->rangeID, header->index, header->endIndex, mem, false);
        off += sizeof(RaftLogHeader);
      }
        break;
      case RaftKeyType::DELSTATE:
      {
        if (is_cur_file && (off + sizeof(header->type) + sizeof(header->rangeID) > len)) {
          LOG_WARN("data missing, type=%d", header->type);
          break;
        }
        state_.erase(header->rangeID);
        off += sizeof(header->type) + sizeof(header->rangeID);
      }
        break;
      case RaftKeyType::CLEAR:
      {
        if (is_cur_file && (off + sizeof(header->type) + sizeof(header->rangeID) > len)) {
          LOG_WARN("data missing, type=%d", header->type);
          break;
        }
        ranges_.erase(header->rangeID);
        state_.erase(header->rangeID);
        off += sizeof(header->type) + sizeof(header->rangeID);
      }
        break;
      case RaftKeyType::TRUNCATE:
      {
        if (is_cur_file && (off + sizeof(RaftLogHeader) > len)) {
          LOG_WARN("data missing, type=%d", header->type);
          break;
        }
        std::string mem;
        truncate(ctx, header->rangeID, header->index, mem, false);
        off += sizeof(RaftLogHeader);
      }
       break;
      default:
        LOG_ERROR("invalid type %d", header->type);
        return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus RaftStore::init(std::string engine_root_path) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("InitServerKWDBContext Error!");
    return s;
  }
  try {
    if (!fs::exists(engine_root_path)) {
      fs::create_directories(engine_root_path);
    }
  } catch (fs::filesystem_error& e) {
    std::cout <<"Failed to create the directory: " << engine_root_path << " , error:" << e.what() << std::endl;
  }
  for (const auto& entry : fs::directory_iterator(engine_root_path)) {
    const std::string filename = entry.path().filename();
    if (filename.find("current_") != std::string::npos) {
      file_path_ = engine_root_path + "/" + filename;
      Open();
      current_id_ = std::stoul(filename.substr(filename.find("current_") + 8, filename.find(".")));
    }
  }
  if (!file_path_.empty()) {
    for (int i = 1; i < current_id_; i++) {
      std::string file_path = engine_root_path + "/previous_" + std::to_string(i) + ".raftlog";
      std::string tmp_path = engine_root_path + "/previous_" + std::to_string(-i) + ".raftlog";
      if (fs::exists(file_path)) {
        previous_files_[i] = FileHandle(-1, file_path);
      }
      if (fs::exists(tmp_path)) {
        previous_files_[-i] = FileHandle(-1, tmp_path);
      }
    }
    // Discover the new files that are being merged and delete them.
    std::map<int, FileHandle> removeFiles;
    for (auto& file : previous_files_) {
      if (file.first > 0) {
        auto removeFile = previous_files_.find(-file.first);
        if (removeFile != previous_files_.end()) {
          removeFiles.insert(file);
        }
      }
    }
    for (auto& removeFile : removeFiles) {
      fs::remove(removeFile.second.path);
      previous_files_.erase(removeFile.first);
    }

    std::vector<int> files_id;
    // Sort the file ids by absolute value.
    for (int i = 0; i < current_id_; i++) {
      if (previous_files_.find(i) != previous_files_.end()) {
        files_id.push_back(i);
      } else if (previous_files_.find(-i) != previous_files_.end()) {
        files_id.push_back(-i);
      }
    }

    if (files_id.size() != previous_files_.size()) {
      LOG_ERROR("The number[%zu] of files found is inconsistent with the previous number[%zu] of files.",
                files_id.size(), previous_files_.size());
      return KStatus::FAIL;
    }
    // Delete the new files that are being merged.
    for (int i = 0; (i + 2) < previous_files_.size(); i++) {
      // Avoid consecutively numbered files.
      if (files_id[i + 1] == files_id[i] + 1 && files_id[i + 2] == files_id[i] + 2) {
        continue;
      }
      if (files_id[i + 1] == (std::abs(files_id[i]) + std::abs(files_id[i + 2])) / 2) {
        auto removeFile = previous_files_.find(files_id[i + 1]);
        fs::remove(removeFile->second.path);
        previous_files_.erase(files_id[i + 1]);
      }
    }
    // Read the raftlog recorded in the file.
    for (int i = 1; i < current_id_; i++) {
      if (previous_files_.find(i) != previous_files_.end()) {
        loadFile(ctx_p, previous_files_[i], i, false);
      }
      if (previous_files_.find(-i) != previous_files_.end()) {
        loadFile(ctx_p, previous_files_[-i], -i, false);
      }
    }
    // Load the raftlog of the courrend_id_ file.
    FileHandle current_file = FileHandle(-1, file_path_);
    loadFile(ctx_p, current_file, current_id_, true);
  } else {
    std::string current_file = engine_root_path + "/current_" + std::to_string(current_id_) + ".raftlog";
    file_path_ = current_file;
    Open();
  }
  startCompact();
  return KStatus::SUCCESS;
}

uint64_t RaftStore::compactPut(uint64_t range_id, uint64_t index_id, TSSlice& value, std::string &mem) {
  RaftLogHeader header;
  header.type = RaftKeyType::PUT;
  header.rangeID = range_id;
  header.index = index_id;
  header.size = value.len;
  mem.append(reinterpret_cast<const char *>(&header), sizeof(RaftLogHeader));
  mem.append(value.data, value.len);
  size_t value_offset = compact_len_ + sizeof(RaftLogHeader);
  compact_len_ += sizeof(RaftLogHeader) + value.len;
  if (value.data != nullptr) {
    free(value.data);
    value.data = nullptr;
  }
  return value_offset;
}

KStatus RaftStore::compact() {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  int files_size;
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (previous_files_.size() % 2 == 1) {
    files_size = previous_files_.size() -1;
  } else {
    files_size = previous_files_.size();
  }
  std::vector<int> files_id;
  int tmp = 0;
  // Select the earliest files_max_num files before merging.
  for (int i = 1; i < current_id_; i++) {
    if (previous_files_.find(i) != previous_files_.end()) {
      files_id.push_back(i);
      tmp++;
    } else if (previous_files_.find(-i) != previous_files_.end()) {
      files_id.push_back(-i);
      tmp++;
    }
    if (tmp == files_size) {
      break;
    }
  }

  for (int i = 0 ; i+1 < files_size; i+=2) {
    if (!checkIsCompact(files_id[i], files_id[i+1])) {
      continue;
    }
    int previous_id;
    // File numbers are adjacent.
    if (files_id[i] + 1 == files_id[i + 1]) {
      previous_id = -files_id[i];
    } else {
      previous_id = (std::abs(files_id[i]) + std::abs(files_id[i + 1])) / 2;
    }
    fs::path file = file_path_;
    std::string new_file = file.parent_path().string() + "/previous_" + std::to_string(previous_id) + ".raftlog";
    compact_file_ = open(new_file.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    if (compact_file_ == -1) {
      LOG_ERROR("open file [%s] failed. errno: %d.", new_file.c_str(), errno);
      return KStatus::FAIL;
    }
    compact_len_ = lseek(compact_file_, 0, SEEK_END);
    FileHandle new_handle = FileHandle(compact_file_, new_file);
    previous_files_.insert(std::pair(previous_id, new_handle));
    std::string mem;
    for (auto indexes : ranges_) {
      auto index = indexes.second;
      while (true) {
        if (index->file_id == files_id[i] || index->file_id == files_id[i + 1]) {
          TSSlice value;
          getDiskValue(ctx_p, index, &value, true);
          index->file_id = previous_id;
          index->offset = compactPut(indexes.first, index->index_id, value, mem);
        }
        if (index->next == nullptr) {
          break;
        }
        index = index->next;
      }
    }
    for (auto indexes : state_) {
      auto index = indexes.second;
      if (index != NULL) {
        if (index->file_id == files_id[i] || index->file_id == files_id[i + 1]) {
          TSSlice value;
          getDiskValue(ctx_p, index, &value, true);
          index->file_id = previous_id;
          index->offset = compactPut(indexes.first, index->index_id, value, mem);
        }
      }
    }
    ssize_t write_num = write(compact_file_, mem.c_str(), mem.size());
    assert(write_num == mem.size());
    FileHandle remove_file = previous_files_[files_id[i]];
    previous_files_.erase(files_id[i]);
    if (fs::exists(remove_file.path)) {
      if (remove_file.file != -1) {
        close(remove_file.file);
      }
      fs::remove(remove_file.path);
    }
    remove_file = previous_files_[files_id[i + 1]];
    previous_files_.erase(files_id[i + 1]);
    if (fs::exists(remove_file.path)) {
      if (remove_file.file != -1) {
        close(remove_file.file);
      }
      fs::remove(remove_file.path);
    }
  }
  isCompact = false;
  return KStatus::SUCCESS;
}

KStatus RaftStore::Close() {
  stopCompact();
  close(file_);
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
  delete rwlock_;
  return KStatus::SUCCESS;
}

KStatus RaftStore::Sync(kwdbContext_p ctx) {
  fsync(file_);
  return KStatus::SUCCESS;
}

KStatus RaftStore::HasRange(kwdbContext_p ctx, uint64_t rangeID) {
  wrLock();
  Defer defer([&]() { unLock(); });
  if (state_.find(rangeID) != state_.end()) {
    return KStatus::SUCCESS;
  }
  return KStatus::FAIL;
}

bool RaftStore::checkIsCompact(int file1, int file2) {
  uint64_t len = 0;
  for (auto indexes : ranges_) {
    auto index = indexes.second;
    while (true) {
      if (index->file_id == file1 || index->file_id == file2) {
        len += index->len;
      }
      if (index->next == nullptr) {
        break;
      }
      index = index->next;
    }
  }
  for (auto indexes : state_) {
    auto index = indexes.second;
    if (index != NULL) {
      if (index->file_id == file1 || index->file_id == file2) {
        len += index->len;
      }
    }
  }
  if (len < file_max_size) {
    return true;
  }
  return false;
}

void RaftStore::startCompact() {
  if (running_) return;
  running_ = true;
  compactThread_ = std::thread([this]() {
    while (running_) {
      if (!isCompact) {
        isCompact = true;
        compact();
      }
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, interval_, [this] { return !running_; });
    }
  });
}

void RaftStore::stopCompact() {
  if (!running_) return;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
  }
  cv_.notify_one();
  if (compactThread_.joinable()) {
    compactThread_.join();
  }
}
