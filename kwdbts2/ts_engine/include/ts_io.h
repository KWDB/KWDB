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
#include <string_view>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"


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


// Prepare for TsVersion

class TsAppendOnlyFile {
 protected:
  const std::string path_;

 public:
  explicit TsAppendOnlyFile(const std::string& path) : path_(path) {}
  virtual ~TsAppendOnlyFile() {}

  virtual KStatus Append(std::string_view) = 0;
  virtual KStatus Append(TSSlice slice) {
    return this->Append(std::string_view{slice.data, slice.len});
  }

  virtual size_t GetFileSize() const = 0;
  std::string GetFilePath() const { return path_; }

  virtual KStatus Sync() = 0;
  virtual KStatus Flush() = 0;

  virtual KStatus Close() = 0;
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

 public:
  explicit TsSequentialReadFile(const std::string& path) : path_(path) {}
  virtual ~TsSequentialReadFile() {
    if (delete_after_free) {
      unlink(path_.c_str());
    }
  }

  virtual KStatus Read(size_t n, TSSlice* slice, char* buf) = 0;
  virtual KStatus Skip(size_t n) = 0;
};

class TsMMapAppendOnlyFile : public TsAppendOnlyFile {
 private:
  int fd_;

  char* mmap_start_ = nullptr;
  char* mmap_end_ = nullptr;
  char* dest_ = nullptr;
  char* synced_ = nullptr; // address before synced_ are synced by msync

  const size_t page_size_;
  size_t mmap_size_;
  size_t file_size_;

  KStatus UnmapCurrent();
  KStatus MMapNew();

 public:
  TsMMapAppendOnlyFile(const std::string& path, int fd, size_t offset /*append from offset*/)
      : TsAppendOnlyFile(path),
        fd_(fd),
        page_size_(getpagesize()),
        mmap_size_(16 * page_size_),
        file_size_(offset) {}
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
      : TsRandomReadFile(path),
        fd_(fd),
        mmap_start_(addr),
        file_size_(filesize),
        page_size_(getpagesize()) {
    assert(mmap_start_);
    assert(file_size_ > 0);
  }
  ~TsMMapRandomReadFile() {
    munmap(mmap_start_, file_size_);
    close(fd_);
  }
  KStatus Prefetch(size_t offset, size_t n) override;
  KStatus Read(size_t offset, size_t n, TSSlice* result, char* buffer) const override;

  size_t GetFileSize() const override { return file_size_; }
};

class TsIOEnv {
 public:
  virtual ~TsIOEnv() {}
  virtual KStatus NewAppendOnlyFile(const std::string& filepath,
                                    std::unique_ptr<TsAppendOnlyFile>* file, bool overwrite = true,
                                    size_t offset = -1) = 0;
  virtual KStatus NewRandomReadFile(const std::string& filepath,
                                    std::unique_ptr<TsRandomReadFile>* file,
                                    size_t file_size = -1) = 0;
  virtual KStatus NewDirectory(const std::string& path) = 0;
  virtual KStatus DeleteDir(const std::string& path) = 0;
  virtual KStatus DeleteFile(const std::string& path) = 0;
};

class TsMMapIOEnv : public TsIOEnv {
 public:
  static TsIOEnv& GetInstance();
  KStatus NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                            bool overrite = true, size_t offset = -1) override;
  KStatus NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                            size_t file_size = -1) override;
  KStatus NewDirectory(const std::string& path) override;
  KStatus DeleteFile(const std::string& path) override;
  KStatus DeleteDir(const std::string& path) override;
};
}  // namespace kwdbts
