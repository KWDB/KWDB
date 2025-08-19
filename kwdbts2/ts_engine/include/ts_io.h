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

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "sys_utils.h"
#include "ts_file_vector_index.h"

namespace kwdbts {

enum TsFileStatus {
  UNREADY = 0,
  READY = 1,
};

class TsAppendOnlyFile {
 protected:
  bool delete_after_free = false;  // remove it later
  const std::string path_;

 public:
  explicit TsAppendOnlyFile(const std::string& path) : path_(path) {}
  virtual ~TsAppendOnlyFile() {
    if (delete_after_free) {
      unlink(path_.c_str());
    }
  }

  virtual KStatus Append(std::string_view) = 0;
  KStatus Append(TSSlice slice) { return this->Append(std::string_view{slice.data, slice.len}); }

  virtual size_t GetFileSize() const = 0;
  std::string GetFilePath() const { return path_; }

  virtual KStatus Sync() = 0;
  virtual KStatus Flush() = 0;

  virtual KStatus Close() = 0;

  void MarkDelete() { delete_after_free = true; }
};

class TsRandomReadFile {
 protected:
  bool delete_after_free = false;  // remove it later
  const std::string path_;

 public:
  explicit TsRandomReadFile(const std::string& path) : path_(path) {}
  virtual ~TsRandomReadFile() {
    if (delete_after_free) {
      unlink(path_.c_str());
    }
  }

  virtual KStatus Prefetch(size_t offset, size_t n) = 0;
  virtual KStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) const = 0;

  virtual size_t GetFileSize() const = 0;
  std::string GetFilePath() const { return path_; }

  void MarkDelete() { delete_after_free = true; }
};

class TsSequentialReadFile {
 protected:
  bool delete_after_free = false;  // remove it later
  const std::string path_;

  size_t offset_ = 0;
  size_t file_size_ = 0;

 public:
  explicit TsSequentialReadFile(const std::string& path, size_t file_size) : path_(path), file_size_(file_size) {}
  virtual ~TsSequentialReadFile() {
    if (delete_after_free) {
      unlink(path_.c_str());
    }
  }

  virtual KStatus Read(size_t n, TSSlice* slice, char* buf) = 0;
  virtual KStatus Skip(size_t n) {
    offset_ += n;
    return SUCCESS;
  }
  virtual KStatus Seek(size_t offset) {
    offset_ = offset;
    return SUCCESS;
  }

  size_t GetFileSize() const { return file_size_; }
  const std::string& GetFilePath() const { return path_; }
  bool IsEOF() const { return offset_ >= file_size_; }

  void MarkDelete() { delete_after_free = true; }
};

class TsMMapAppendOnlyFile : public TsAppendOnlyFile {
 private:
  int fd_;

  char* mmap_start_ = nullptr;
  char* mmap_end_ = nullptr;
  char* dest_ = nullptr;
  char* synced_ = nullptr;  // address before synced_ are synced by msync

  const size_t page_size_;
  size_t mmap_size_;
  size_t file_size_;

  KStatus UnmapCurrent();
  KStatus MMapNew();

 public:
  TsMMapAppendOnlyFile(const std::string& path, int fd, size_t offset /*append from offset*/)
      : TsAppendOnlyFile(path), fd_(fd), page_size_(getpagesize()), mmap_size_(16 * page_size_), file_size_(offset) {}
  ~TsMMapAppendOnlyFile();

  KStatus Append(std::string_view data) override;
  size_t GetFileSize() const override { return file_size_; }
  KStatus Sync() override;
  KStatus Flush() override { return SUCCESS; }

  KStatus Close() override;
};

class TsMMapRandomReadFile : public TsRandomReadFile {
  int fd_;
  char* mmap_start_;
  size_t file_size_;
  size_t page_size_;

 public:
  TsMMapRandomReadFile(const std::string& path, int fd, char* addr, size_t filesize)
      : TsRandomReadFile(path), fd_(fd), mmap_start_(addr), file_size_(filesize), page_size_(getpagesize()) {
    assert(fd > 0);
    if (filesize != 0) {
      assert(mmap_start_ != nullptr);
    } else {
      assert(mmap_start_ == nullptr);
    }
  }
  ~TsMMapRandomReadFile() {
    if (mmap_start_ != nullptr) {
      munmap(mmap_start_, file_size_);
    }
    close(fd_);
  }
  KStatus Prefetch(size_t offset, size_t n) override;
  KStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) const override;

  size_t GetFileSize() const override { return file_size_; }
};

class TsMMapSequentialReadFile : public TsSequentialReadFile {
 private:
  int fd_;
  char* mmap_start_;
  size_t page_size_;

 public:
  TsMMapSequentialReadFile(const std::string& path, int fd, char* addr, size_t filesize)
      : TsSequentialReadFile(path, filesize), fd_(fd), mmap_start_(addr), page_size_(getpagesize()) {
    assert(fd > 0);
    if (filesize != 0) {
      assert(mmap_start_ != nullptr);
    } else {
      assert(mmap_start_ == nullptr);
    }
  }
  KStatus Read(size_t n, TSSlice* slice, char* buf) override;
  ~TsMMapSequentialReadFile() {
    if (mmap_start_ != nullptr) {
      munmap(mmap_start_, file_size_);
    }
    close(fd_);
  }
};

class TsIOEnv {
 public:
  virtual ~TsIOEnv() {}
  virtual KStatus NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                                    bool overwrite = true, size_t offset = -1) = 0;
  virtual KStatus NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                                    size_t file_size = -1) = 0;
  virtual KStatus NewSequentialReadFile(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                        size_t file_size = -1) = 0;
  virtual KStatus NewDirectory(const std::string& path) = 0;
  virtual KStatus DeleteDir(const std::string& path) = 0;
  virtual KStatus DeleteFile(const std::string& path) = 0;
};

class TsMMapIOEnv : public TsIOEnv {
 public:
  static TsIOEnv& GetInstance();
  KStatus NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file, bool overrite = true,
                            size_t offset = -1) override;
  KStatus NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                            size_t file_size = -1) override;
  KStatus NewSequentialReadFile(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                size_t file_size = -1) override;
  KStatus NewDirectory(const std::string& path) override;
  KStatus DeleteFile(const std::string& path) override;
  KStatus DeleteDir(const std::string& path) override;
};

class TsMMapAllocFile : public FileWithIndex {
 private:
  struct FileHeader {
    uint64_t file_len;
    uint64_t alloc_offset;
    uint64_t index_header_offset;
    char reserved[104];
  };
  static_assert(sizeof(FileHeader) == 128, "wrong size of FileHeader, please check compatibility.");
  static_assert(std::has_unique_object_representations_v<FileHeader>, "padding in struct FileHeader");

  std::string path_;
  int fd_ = -1;
  std::vector<TSSlice> addrs_;
  std::mutex mutex_;
  KRWLatch* rw_lock_ = nullptr;

 public:
  explicit TsMMapAllocFile(const std::string& path) : path_(path) {}
  KStatus Open() {
    bool exists = fs::exists(path_);
    void* base = nullptr;
    size_t file_len;
    if (exists) {
      int oflag = O_RDWR;
      fd_ = open(path_.c_str(), oflag);
      file_len = lseek(fd_, 0, SEEK_END);
      int prot = PROT_READ | PROT_WRITE;
      base = mmap(nullptr, file_len, prot, MAP_SHARED, fd_, 0);
    } else {
      fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0644);
      if (fd_ == -1) {
        return KStatus::FAIL;
      }
      file_len = getpagesize();
      fallocate(fd_, 0, 0, file_len);
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
    return KStatus::SUCCESS;
  }

  ~TsMMapAllocFile() {
    Close();
    if (rw_lock_) {
      delete rw_lock_;
    }
  }

  uint64_t GetStartPos() { return sizeof(FileHeader); }

  uint64_t GetAllocSize() { return getHeader()->alloc_offset - GetStartPos(); }

  FileHeader* getHeader() { return reinterpret_cast<FileHeader*>(addrs_[0].data); }

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
    if (fallocate(fd_, 0, 0, new_len) == -1) {
      LOG_ERROR("fallocate size[%lu] error %s.", new_len, strerror(errno));
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

  KStatus DropAll() {
    if (Close()) {
      std::string cmd = "rm -rf " + path_;
      if (System(cmd)) {
        return KStatus::SUCCESS;
      }
    }
    return KStatus::FAIL;
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
