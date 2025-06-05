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

#include "ee_io_cache_handler.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <sstream>
#include <cstdio>

#include "ee_exec_pool.h"

namespace kwdbts {
IOCacheHandler::IOCacheHandler(k_uint64 max_file_size)
    : current_file_id_(0),
      max_file_size_(std::min(max_file_size, MAX_FILE_SIZE)) {
  read_buffer_ = static_cast<char *>(malloc(read_buffer_size_));
}
IOCacheHandler::~IOCacheHandler() {
  for (auto &info : io_info_) {
    if (info.fd_ != -1) {
      close(info.fd_);
      info.fd_ = -1;
    }
    if (info.path_ != nullptr) {
      unlink(info.path_);  // delete file
      free(info.path_);    // free memory
      info.path_ = nullptr;
    }
  }
  SafeFreePointer(read_buffer_);
}

void IOCacheHandler::update_lru(k_uint32 file_id) {
  auto it = lru_map_.find(file_id);
  if (it != lru_map_.end()) {
    lru_list_.erase(it->second);
  }
  lru_list_.push_front(file_id);
  lru_map_[file_id] = lru_list_.begin();
}

void IOCacheHandler::remove_lru() {
  if (!lru_list_.empty()) {
    k_uint32 lru_file_id = lru_list_.back();
    lru_list_.pop_back();
    lru_map_.erase(lru_file_id);

    if (io_info_[lru_file_id].fd_ != -1) {
      close(io_info_[lru_file_id].fd_);
      io_info_[lru_file_id].fd_ = -1;
    }
  }
}
KStatus IOCacheHandler::Open(const k_uint32 file_id, cache_type type) {
  if (file_id >= io_info_.size()) {
    IO_INFO info;
    static std::atomic<size_t> ts_inc{0};
    std::ostringstream oss;
    int64_t t = ts_inc.fetch_add(1);
    oss << ExecPool::GetInstance().db_path_ << "/cache_" << t;
    info.path_ = static_cast<k_char *>(malloc(oss.str().size() + 1));
    snprintf(info.path_, oss.str().size() + 1, "%s", oss.str().c_str());
    io_info_.push_back(info);
  }
  if (max_file_size_ != 0) {
    io_info_[file_id].last_access_ = std::chrono::steady_clock::now();
    update_lru(file_id);
  }

  if (io_info_[file_id].fd_ != -1 && io_info_[file_id].type_ == type) {
    return KStatus::SUCCESS;
  }

  if (lru_map_.size() >= MAX_OPEN_FILE_NUM) {
    remove_lru();
  }

  io_info_[file_id].type_ = type;
  int Flags = 0;
  switch (type) {
    case cache_type::CACHE_READ: {
      Flags = O_RDWR;
      break;
    }
    default: {
      Flags = O_RDWR | O_CREAT | O_TRUNC;
      break;
    }
  }

  io_info_[file_id].fd_ = open(io_info_[file_id].path_, Flags, 0600);
  return KStatus::SUCCESS;
}

KStatus IOCacheHandler::Reset() {
  current_file_id_ = 0;
  for (auto &info : io_info_) {
    info.size_ = 0;
    info.type_ = cache_type::CACHE_UNKNOW;
  }
  total_size_ = 0;
  return KStatus::SUCCESS;
}

KStatus IOCacheHandler::Write(const k_char *buf, k_uint64 len) {
  auto write_buf = buf;
  while (len > 0) {
    Open(current_file_id_, cache_type::CACHE_WRITE);
    if (max_file_size_ == 0) {
      if (write(io_info_[current_file_id_].fd_, write_buf, len) == -1) {
        return KStatus::FAIL;
      }
      break;
    }
    auto append_size =
        std::min(len, max_file_size_ - io_info_[current_file_id_].size_);
    if (write(io_info_[current_file_id_].fd_, write_buf, append_size) == -1) {
      return KStatus::FAIL;
    }
    io_info_[current_file_id_].size_ += append_size;
    len -= append_size;
    write_buf += append_size;
    if (len > 0) {
      ++current_file_id_;
    }
  }
  total_size_ += len;
  return KStatus::SUCCESS;
}

KStatus IOCacheHandler::ReadFromBuffer(k_char *buf, k_uint64 offset,
                                       k_uint64 len) {
  if (offset < read_buffer_offset_ ||
      offset >= read_buffer_offset_ + read_buffer_size_) {
    return KStatus::FAIL;
  }
  if (offset + len > read_buffer_offset_ + read_buffer_size_) {
    return KStatus::FAIL;
  }
  memcpy(buf, read_buffer_ + offset - read_buffer_offset_, len);
  return KStatus::SUCCESS;
}

KStatus IOCacheHandler::ReadFromFile(k_char *buf, k_uint64 offset,
                                     k_uint64 len) {
  if (max_file_size_ == 0) {
    read_buffer_offset_ = offset;
    Open(0, cache_type::CACHE_READ);
    if (offset + len > total_size_) {
      return KStatus::FAIL;
    }
    if (offset + read_buffer_size_ > total_size_) {
      if (read_buffer_size_ < total_size_) {
        read_buffer_offset_ = total_size_ - read_buffer_size_;
      } else {
        read_buffer_offset_ = 0;
      }
    }
    if (lseek(io_info_[0].fd_, read_buffer_offset_, SEEK_SET) == -1) {
      return FAIL;
    }
    if (len > read_buffer_size_) {
      if (read(io_info_[0].fd_, buf, len) == -1) {
        return FAIL;
      }
      memcpy(read_buffer_, buf + len - read_buffer_size_, read_buffer_size_);
    } else {
      if (read(io_info_[0].fd_, read_buffer_,
               std::min(read_buffer_size_, total_size_)) == -1) {
        return FAIL;
      }
      memcpy(buf, read_buffer_ + offset - read_buffer_offset_, len);
    }

    return SUCCESS;
  }

  auto file_index = offset / max_file_size_;
  auto offset_in_file = offset % max_file_size_;
  k_char *read_buf = buf;

  while (len > 0) {
    Open(file_index, cache_type::CACHE_READ);
    auto read_size = std::min(len, io_info_[file_index].size_ - offset_in_file);
    if (lseek(io_info_[file_index].fd_, offset_in_file, SEEK_SET) == -1) {
      return KStatus::FAIL;
    }
    if (read(io_info_[file_index].fd_, read_buf, read_size) == -1) {
      return KStatus::FAIL;
    }
    len -= read_size;
    read_buf += read_size;
    offset_in_file = 0;
    file_index++;
  }

  return KStatus::SUCCESS;
}
KStatus IOCacheHandler::Read(k_char *buf, k_uint64 offset, k_uint64 len) {
  KStatus ret = ReadFromBuffer(buf, offset, len);
  if (ret == KStatus::SUCCESS) {
    return KStatus::SUCCESS;
  }
  ret = ReadFromFile(buf, offset, len);
  return ret;
}
}  // namespace kwdbts
