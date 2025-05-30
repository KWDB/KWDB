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

#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_file_vector_index.h"

namespace kwdbts {

enum TsFileStatus {
  NOT_READY = 0,
  READY = 1,
};

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

  virtual KStatus Sync() = 0;
  virtual KStatus Flush() = 0;

  virtual KStatus Append(const TSSlice&) = 0;
  virtual KStatus Append(const std::string& data) {
    return this->Append(TSSlice{const_cast<char*>(data.data()), data.size()});
  }

  virtual KStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) = 0;

  virtual KStatus Write(size_t offset, const TSSlice& data) = 0;

  virtual KStatus Reset() = 0;

  virtual KStatus Close() = 0;

  void MarkDelete() { delete_after_free.store(true); }

  std::string GetFilePath() { return filename_.string(); }
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

  KStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) override {
    size_t size = size_;

    if (offset >= size) {
      result->len = 0;
      return KStatus::SUCCESS;
    }

    size_t nread = std::min(size - offset, n);
    memcpy(buffer, addr_ + offset, nread);
    result->len = nread;
    result->data = buffer;
    return KStatus::SUCCESS;
  }

  KStatus Write(size_t offset, const TSSlice& data) override {
    size_t new_len = len_;
    while (new_len < offset + data.len) {
      new_len *= 2;
    }
    int err_code;
    if (new_len != len_) {
      err_code = ftruncate(fd_, new_len);
      if (err_code < 0) {
        close(fd_);
        LOG_ERROR("resize file failed, error code:%d", err_code);
        return KStatus::FAIL;
      }
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
      size_ = offset + data.len;
    }
    return KStatus::SUCCESS;
  }

  using TsFile::Append;
  KStatus Append(const TSSlice& data) override {
    size_t newlen = len_;
    while (newlen - size_ < data.len) {
      uint64_t threshold = 4UL << 20;
      if (len_ < threshold) {
        newlen *= 2;
      } else {
        newlen += threshold;
      }
    }

    if (newlen != len_) {
      if (ftruncate(fd_, newlen) == -1) {
        LOG_ERROR("ftruncate error %s.", strerror(errno));
        return KStatus::FAIL;
      }
      void* base = mremap(addr_, len_, newlen, MREMAP_MAYMOVE);
      if (base == MAP_FAILED) {
        switch (errno) {
          case EAGAIN:
          case EFAULT:
            munmap(addr_, len_);
            base = mmap(nullptr, newlen, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
            if (base == MAP_FAILED) {
              LOG_ERROR("mmap error %s.", strerror(errno))
              return KStatus::FAIL;
            }
            break;
          case EINVAL:
            assert(false);
          case ENOMEM:
            LOG_ERROR("mremap error %s.", strerror(errno));
            return KStatus::FAIL;
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
    return KStatus::SUCCESS;
  }

  KStatus Close() override {
    if (fd_ != -1) {
      Sync();
      ftruncate(fd_, size_);
      close(fd_);
      munmap(addr_, len_);
      fd_ = -1;
      addr_ = nullptr;
    }
    return KStatus::SUCCESS;
  }

  KStatus Reset() override {
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
      return KStatus::FAIL;
    }
    addr_ = reinterpret_cast<char*>(base);
    return KStatus::SUCCESS;
  }

  KStatus Sync() override {
    int err = msync(addr_, len_, MS_SYNC);
    if (err != 0) {
      LOG_ERROR("msync failed. err: %d", err);
      return KStatus::FAIL;
    }
    return KStatus::SUCCESS;
  }
  KStatus Flush() override { return KStatus::SUCCESS; }

  size_t GetFileSize() const override { return size_; }
};

class TsMMapAllocFile : public FileWithIndex {
 private:
  struct FileHeader {
    uint64_t file_len;
    uint64_t alloc_offset;
    char reserved[112];
  };
static_assert(sizeof(FileHeader) == 128, "wrong size of FileHeader, please check compatibility.");

  std::string path_;
  int fd_ = -1;
  std::vector<TSSlice> addrs_;
  std::mutex mutex_;
  KRWLatch* rw_lock_;

 public:
  TsMMapAllocFile(const std::string& path) : path_(path) {
    bool exists = std::filesystem::exists(path_);
    void* base = nullptr;
    size_t file_len;
    if (exists) {
      int oflag = O_RDWR;
      fd_ = open(path.c_str(), oflag);
      file_len = lseek(fd_, 0, SEEK_END);
      int prot = PROT_READ | PROT_WRITE;
      base = mmap(nullptr, file_len, prot, MAP_SHARED, fd_, 0);
    } else {
      fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
      file_len = getpagesize();
      ftruncate(fd_, file_len);
      base = mmap(nullptr, file_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    }
    addrs_.push_back({reinterpret_cast<char*>(base), file_len});
    if (!exists) {
      auto header = getHeader();
      header->alloc_offset = sizeof(FileHeader);
      header->file_len = file_len;
    }
    assert(file_len == getHeader()->file_len);
    rw_lock_ = new KRWLatch(RWLATCH_ID_MMAP_DEL_ITEM_RWLOCK);
  }

  ~TsMMapAllocFile() {
    Close();
    if (rw_lock_) {
      delete rw_lock_;
    }
  }

  uint64_t GetStartPos() {
    return sizeof(FileHeader);
  }

  FileHeader* getHeader() {
    return reinterpret_cast<FileHeader*>(addrs_[0].data);
  }

  KStatus resize(uint64_t add_size) {
    size_t new_len = getHeader()->file_len;
    while (add_size > (new_len - getHeader()->file_len)) {
      uint64_t threshold = 4UL << 20;
      if (new_len < threshold) {
        new_len *= 2;
      } else {
        new_len += threshold;
      }
    }
    if (ftruncate(fd_, new_len) == -1) {
      LOG_ERROR("ftruncate size[%lu] error %s.", new_len, strerror(errno));
      return KStatus::FAIL;
    }

    auto cur_mmap_len = new_len - getHeader()->file_len;
    auto base = mmap(nullptr, cur_mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, getHeader()->file_len);
    if (base == MAP_FAILED) {
      LOG_ERROR("mmap [%lu] error %s.", cur_mmap_len, strerror(errno));
      return KStatus::FAIL;
    }
    addrs_.push_back({reinterpret_cast<char*>(base), cur_mmap_len});
    getHeader()->alloc_offset = getHeader()->file_len;
    getHeader()->file_len = new_len;
    return KStatus::SUCCESS;
  }

  void offsetAssigned() {
    auto header = getHeader();
    auto left = (header->alloc_offset + 7) % 8;
    header->alloc_offset += 7 - left;
  }

  char* addr(size_t offset) {
    uint64_t cur_offset = 0;
    for (int i = 0; i < addrs_.size(); i++) {
      if (cur_offset + addrs_[i].len > offset) {
        return addrs_[i].data + (offset - cur_offset);
      } else {
        cur_offset += addrs_[i].len;
      }
    }
    return nullptr;
  }

  uint64_t AllocateAssigned(size_t size, uint8_t fill_number) override {
    RW_LATCH_X_LOCK(rw_lock_);
    offsetAssigned();
    auto header = getHeader();
    uint64_t ret;
    bool ok = true;
    if (header->alloc_offset + size >= header->file_len) {
      if (resize(size) != KStatus::SUCCESS) {
        LOG_ERROR("resize failed.");
        ok = false;
      }
    }
    if (ok) {
      ret = header->alloc_offset;
      header->alloc_offset += size;
      memset(addr(ret), fill_number, size);
    } else {
      ret = INVALID_POSITION;
    }
    RW_LATCH_UNLOCK(rw_lock_);
    return ret;
  }
  char* GetAddrForOffset(uint64_t offset, uint32_t reading_bytes) override {
    RW_LATCH_S_LOCK(rw_lock_);
    auto ret = addr(offset);
    RW_LATCH_UNLOCK(rw_lock_);
    return ret;
  }

  KStatus Close() {
    if (fd_ != -1) {
      Sync();
      close(fd_);
      for (auto addr : addrs_) {
        munmap(addr.data, addr.len);
      }
      fd_ = -1;
      addrs_.clear();
    }
    return KStatus::SUCCESS;
  }

  KStatus Sync() {
    RW_LATCH_X_LOCK(rw_lock_);
    KStatus s = KStatus::SUCCESS;
    for (auto addr : addrs_) {
      int err = msync(addr.data, addr.len, MS_SYNC);
      if (err != 0) {
        LOG_ERROR("msync failed. err: %d", err);
        s = KStatus::FAIL;
        break;
      }
    }
    RW_LATCH_UNLOCK(rw_lock_);
    return s;
  }
};

}  // namespace kwdbts
