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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

#include "libkwdbts2.h"
#include "rocksdb/status.h"
#include "ts_slice.h"
#include "ts_status.h"
#include "lg_api.h"

namespace kwdbts {

class TsFile {
 protected:
  std::atomic_bool delete_after_free = false;
  std::filesystem::path filename_;

 public:
  explicit TsFile(const std::string& path) : filename_(path) {}
  virtual ~TsFile() {
    if (delete_after_free) {
      unlink(filename_.c_str());
    }
  }

  virtual size_t GetFileSize() const = 0;

  virtual TsStatus Sync() = 0;
  virtual TsStatus Flush() = 0;

  virtual TsStatus Append(const TSSlice&) = 0;
  virtual TsStatus Append(const std::string& data) {
    return this->Append(TSSlice{const_cast<char*>(data.data()), data.size()});
  }

  virtual TsStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) = 0;

  virtual TsStatus Write(size_t offset, const TSSlice& data) = 0;

  virtual TsStatus Reset() = 0;

  virtual TsStatus Close() = 0;

  void MarkDelete() { delete_after_free.store(true); }
};

class TsMMapFile final : public TsFile {
 private:
  int fd_ = -1;
  char* addr_ = nullptr;

  size_t len_ = 0;
  size_t size_ = 0;

 public:
  TsMMapFile(const std::string& path, bool read_only) : TsFile(path) {
    bool exists = std::filesystem::exists(path);
    void* base = nullptr;
    if (exists) {
      int oflag = read_only ? O_RDONLY : O_RDWR;
      fd_ = open(path.c_str(), oflag);
      len_ = lseek(fd_, 0, SEEK_END);
      size_ = len_;

      int prot = PROT_READ | (read_only ? 0 : PROT_WRITE);
      base = mmap(nullptr, len_, prot, MAP_SHARED, fd_, 0);
    } else {
      assert(!read_only);
      fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
      len_ = getpagesize();
      ftruncate(fd_, len_);
      base = mmap(nullptr, len_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    }
    addr_ = reinterpret_cast<char*>(base);
  }

  ~TsMMapFile() override { Close(); }

  TsStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) override {
    size_t size = size_;

    if (offset >= size) {
      result->len = 0;
      return TsStatus::OK();
    }

    size_t nread = std::min(size - offset, n);
    memcpy(buffer, addr_ + offset, nread);
    result->len = nread;
    result->data = buffer;
    return TsStatus::OK();
  }

  TsStatus Write(size_t offset, const TSSlice& data) override {
    size_t new_len = len_;
    while (new_len < offset + data.len) {
      new_len *= 2;
    }
    int err_code;
    if (new_len >= len_) {
      if ((err_code = posix_fallocate(fd_, 0, new_len)) != 0) {
        close(fd_);
        LOG_ERROR("resize file failed, error code:%d", err_code);
        return TsStatus::SpaceLimit();
      }
    } else {
      err_code = ftruncate(fd_, new_len);
      if (err_code < 0) {
        close(fd_);
        LOG_ERROR("resize file failed, error code:%d", err_code);
        return TsStatus::SpaceLimit();
      }
    }
    if (new_len != len_) {
      void* base = mremap(addr_, len_, new_len, MREMAP_MAYMOVE);
      if (base == MAP_FAILED) {
        err_code = errno;
        char *error_msg = strerror(err_code);
        LOG_ERROR("mremap failed, error: %s, old length:%lu, new length:%lu", error_msg, len_, new_len);
        munmap(addr_, len_);
        base = mmap(nullptr, new_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (base == MAP_FAILED) {
          err_code = errno;
          error_msg = strerror(err_code);
          LOG_ERROR("mmap failed, error: %s, old length:%lu, new length:%lu", error_msg, len_, new_len);
        }
      }
      assert(base != MAP_FAILED);
      addr_ = reinterpret_cast<char*>(base);
      len_ = new_len;
    }
    memcpy(addr_ + offset, data.data, data.len);
    if (offset + data.len > size_) {
      size_ += data.len;
    }
    return TsStatus::OK();
  }

  using TsFile::Append;
  TsStatus Append(const TSSlice& data) override {
    size_t newlen = len_;
    while (newlen - size_ < data.len) {
      newlen *= 2;
    }

    if (newlen != len_) {
      if (ftruncate(fd_, newlen) == -1) {
        return TsStatus::IOError("ftruncate error ", strerror(errno));
      }
      void* base = mremap(addr_, len_, newlen, MREMAP_MAYMOVE);
      if (base == MAP_FAILED) {
        switch (errno) {
          case EAGAIN:
          case EFAULT:
            munmap(addr_, len_);
            base = mmap(nullptr, newlen, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
            if (base == MAP_FAILED) {
              return TsStatus::IOError("mmap error ", strerror(errno));
            }
          case EINVAL:
            assert(false);
          case ENOMEM:
            return TsStatus::MemoryLimit("mremap error ", strerror(errno));
        }
      }
      assert(base != MAP_FAILED);
      addr_ = reinterpret_cast<char*>(base);
      int pgsz = getpagesize();
      size_t offset = size_ / pgsz * pgsz;
      madvise(addr_, offset, MADV_DONTNEED);
      madvise(addr_ + offset, newlen - offset, MADV_SEQUENTIAL);
      len_ = newlen;
    }
    memcpy(addr_ + size_, data.data, data.len);
    size_ += data.len;
    return TsStatus::OK();
  }

  TsStatus Close() override {
    if (fd_ != -1) {
      Sync();
      ftruncate(fd_, size_);
      close(fd_);
      munmap(addr_, len_);
      fd_ = -1;
      addr_ = nullptr;
    }
    return TsStatus::OK();
  }

  TsStatus Reset() override {
    munmap(addr_, len_);
    size_ = 0;
    len_ = getpagesize();
    ftruncate(fd_, len_);
    void* base = mmap(nullptr, len_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (base == MAP_FAILED) {
      int err_code = errno;
      char* error_msg = strerror(err_code);
      std::string err_info = "mmap[" + filename_.string() + "] failed, error_no: " + std::to_string(err_code)
                             + ", error_msg: " + error_msg + ", length:" + std::to_string(len_);
      LOG_ERROR("%s", err_info.c_str());
      return TsStatus::Corruption(err_info);
    }
    addr_ = reinterpret_cast<char*>(base);
    return TsStatus::OK();
  }

  TsStatus Sync() override {
    int err = msync(addr_, len_, MS_SYNC);
    if (err != 0) {
      return TsStatus::IOError();
    }
    return TsStatus::OK();
  }
  TsStatus Flush() override { return rocksdb::Status::OK(); }

  size_t GetFileSize() const override { return size_; }
};
}  // namespace kwdbts
