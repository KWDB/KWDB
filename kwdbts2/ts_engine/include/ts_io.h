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
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "sys_utils.h"
#include "ts_bufferbuilder.h"
#include "ts_file_vector_index.h"
#include "ts_sliceguard.h"

namespace kwdbts {

inline std::error_code MakeErrorCode(int err) { return std::error_code(err, std::system_category()); }

enum TsFileStatus {
  UNREADY = 0,
  READY = 1,
};

bool CreateDirSymLink(const fs::path& target_path, const fs::path& symbol_path);

inline size_t TruncateToPage(size_t offset, size_t page_size) {
  assert((page_size & (page_size - 1)) == 0);
  offset -= offset & (page_size - 1);
  return offset;
}

class TsAppendOnlyFile {
 protected:
  bool delete_after_free = false;  // remove it later
  const std::string path_;
  virtual KStatus AppendImpl(std::string_view) = 0;
  virtual KStatus SyncImpl() = 0;
  virtual KStatus CloseImpl() = 0;

 public:
  explicit TsAppendOnlyFile(const std::string& path) : path_(path) {}
  virtual ~TsAppendOnlyFile() = default;

  [[nodiscard]] KStatus Append(std::string_view data) { return this->AppendImpl(data); }
  [[nodiscard]] KStatus Append(TSSlice slice) { return this->AppendImpl(std::string_view{slice.data, slice.len}); }

  virtual size_t GetFileSize() const = 0;
  std::string GetFilePath() const { return path_; }

  [[nodiscard]] KStatus Sync() { return this->SyncImpl(); }
  [[nodiscard]] KStatus Close() { return this->CloseImpl(); }

  void MarkDelete() { delete_after_free = true; }
};

class TsRandomReadFile {
 protected:
  bool delete_after_free = false;  // remove it later
  const std::string path_;

  virtual KStatus PrefetchImpl(size_t offset, size_t n) = 0;
  virtual KStatus ReadImpl(size_t offset, size_t n, TsSliceGuard* result) const = 0;

 public:
  explicit TsRandomReadFile(const std::string& path) : path_(path) {}
  virtual ~TsRandomReadFile() = default;

  KStatus Prefetch(size_t offset, size_t n) { return this->PrefetchImpl(offset, n); }
  [[nodiscard]] KStatus Read(size_t offset, size_t n, TsSliceGuard* result) const {
    return this->ReadImpl(offset, n, result);
  }

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

  virtual KStatus ReadImpl(size_t n, TsSliceGuard* slice) = 0;

 public:
  explicit TsSequentialReadFile(const std::string& path, size_t file_size) : path_(path), file_size_(file_size) {}
  virtual ~TsSequentialReadFile() = default;

  [[nodiscard]] KStatus Read(size_t n, TsSliceGuard* slice) { return this->ReadImpl(n, slice); }

  void Skip(size_t n) { offset_ += n; }
  void Seek(size_t offset) { offset_ = offset; }
  size_t Tell() const { return offset_; }

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

 protected:
  KStatus AppendImpl(std::string_view data) override;
  size_t GetFileSize() const override { return file_size_; }
  KStatus SyncImpl() override;
  KStatus CloseImpl() override;

 public:
  TsMMapAppendOnlyFile(const std::string& path, int fd, size_t offset /*append from offset*/)
      : TsAppendOnlyFile(path), fd_(fd), page_size_(getpagesize()), mmap_size_(16 * page_size_), file_size_(offset) {}
  ~TsMMapAppendOnlyFile() {
    if (fd_ >= 0) {
      auto s = this->Close();
      if (s == FAIL) {
        LOG_ERROR("close file %s failed", path_.c_str());
      }
    }
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }
};

class TsMMapRandomReadFile : public TsRandomReadFile {
 private:
  int fd_;
  char* mmap_start_;
  size_t file_size_;
  size_t page_size_;

 protected:
  KStatus PrefetchImpl(size_t offset, size_t n) override;
  KStatus ReadImpl(size_t offset, size_t n, TsSliceGuard* result) const override;

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
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }

  size_t GetFileSize() const override { return file_size_; }
};

class TsMMapSequentialReadFile : public TsSequentialReadFile {
 private:
  int fd_;
  char* mmap_start_;
  size_t page_size_;

 protected:
  KStatus ReadImpl(size_t n, TsSliceGuard* slice) override;

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
  ~TsMMapSequentialReadFile() {
    if (mmap_start_ != nullptr) {
      munmap(mmap_start_, file_size_);
    }
    close(fd_);
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }
};

class TsFIOAppendOnlyFile : public TsAppendOnlyFile {
 private:
  FILE* fp_;
  size_t file_size_;

 protected:
  KStatus AppendImpl(std::string_view data) override;
  KStatus SyncImpl() override;
  KStatus CloseImpl() override;

 public:
  TsFIOAppendOnlyFile(const std::string& path, FILE* fp, size_t offset = 0)
    : TsAppendOnlyFile(path),
      fp_(fp),
      file_size_(offset) {}

  ~TsFIOAppendOnlyFile() {
    auto s = this->Close();
    if (s == FAIL) {
      LOG_ERROR("close file %s failed", path_.c_str());
    }
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }


  size_t GetFileSize() const override { return file_size_; }
};

class TsFIORandomReadFile : public TsRandomReadFile {
 private:
  int fd_;
  size_t file_size_;

 protected:
  KStatus PrefetchImpl(size_t offset, size_t n) override {
    int err = posix_fadvise(fd_, offset, n, POSIX_FADV_WILLNEED);
    if (err != 0) {
      auto ec = MakeErrorCode(err);
      LOG_ERROR("file prefetch error, file path: %s, file size: %zu, offset: %zu, length: %zu, error: %s",
                path_.c_str(), file_size_, offset, n, ec.message().c_str());
    }
    return SUCCESS;
  }
  KStatus ReadImpl(size_t offset, size_t n, TsSliceGuard* result) const override;

 public:
  TsFIORandomReadFile(const std::string& path, int fd, size_t filesize)
      : TsRandomReadFile(path), fd_(fd), file_size_(filesize) {}

  ~TsFIORandomReadFile() {
    if (fd_ >= 0) {
      close(fd_);
    }
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }
  size_t GetFileSize() const override { return file_size_; }
};

class TsFIOSequentialReadFile : public TsSequentialReadFile {
 private:
  int fd_;

 protected:
  KStatus ReadImpl(size_t n, TsSliceGuard* result) override;

 public:
  TsFIOSequentialReadFile(const std::string& path, int fd, size_t file_size)
      : TsSequentialReadFile(path, file_size), fd_(fd) {}

  ~TsFIOSequentialReadFile() {
    if (fd_ >= 0) {
      close(fd_);
    }
    if (delete_after_free) {
      LOG_INFO("delete file %s", path_.c_str());
      unlink(path_.c_str());
    }
  }
};

class TsMemoryAppendOnlyFile : public TsAppendOnlyFile {
 private:
  TsBufferBuilder buffer_;

 protected:
  KStatus SyncImpl() override { return SUCCESS; }
  KStatus CloseImpl() override { return SUCCESS; }
  KStatus AppendImpl(std::string_view sv) override {
    buffer_.append(sv);
    return SUCCESS;
  }

 public:
  explicit TsMemoryAppendOnlyFile(const std::string& path, TsBufferBuilder&& buffer)
      : TsAppendOnlyFile(path), buffer_(std::move(buffer)) {}
  virtual ~TsMemoryAppendOnlyFile();

  KStatus Append(TSSlice slice) { return this->AppendImpl(std::string_view{slice.data, slice.len}); }

  size_t GetFileSize() const override { return buffer_.size(); }
  std::string GetFilePath() const { return path_; }
};

class TsMemoryRandomReadFile : public TsRandomReadFile {
 private:
  std::shared_ptr<TsSliceGuard> data_;
  size_t file_size_;

 protected:
  KStatus PrefetchImpl(size_t offset, size_t n) override { return SUCCESS; }

  KStatus ReadImpl(size_t offset, size_t n, TsSliceGuard* result) const override {
    if (offset + n > file_size_) {
      LOG_ERROR("file access overflow, file path: %s, file size: %zu, offset: %zu, length: %zu", path_.c_str(),
                file_size_, offset, n);
      return FAIL;
    }
    *result = TsSliceGuard(TSSlice{data_->data() + offset, n});
    return SUCCESS;
  }

 public:
  explicit TsMemoryRandomReadFile(const std::string& path, std::shared_ptr<TsSliceGuard> data, size_t file_size)
      : TsRandomReadFile(path), data_(std::move(data)), file_size_(file_size) {}
  virtual ~TsMemoryRandomReadFile();

  size_t GetFileSize() const override { return data_->size(); }
  std::string GetFilePath() const { return path_; }

  void MarkDelete() { delete_after_free = true; }
};

class TsMemorySequentialReadFile : public TsSequentialReadFile {
 private:
  std::shared_ptr<TsSliceGuard> data_;

 protected:
  KStatus ReadImpl(size_t n, TsSliceGuard* slice) override {
    if (offset_ + n > file_size_) {
      LOG_ERROR("file access overflow, file path: %s, file size: %zu, offset: %zu, length: %zu", path_.c_str(),
                file_size_, offset_, n);
      return FAIL;
    }
    *slice = TsSliceGuard(TSSlice{data_->data() + offset_, n});
    offset_ += n;
    return SUCCESS;
  }

 public:
  TsMemorySequentialReadFile(const std::string& path, std::shared_ptr<TsSliceGuard> data, size_t file_size)
      : TsSequentialReadFile(path, file_size), data_(std::move(data)) {}
};

class TsIOEnv {
 protected:
  virtual KStatus NewAppendOnlyFileImpl(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                                        bool overwrite = true, size_t offset = -1) = 0;
  virtual KStatus NewRandomReadFileImpl(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                                        size_t file_size = -1) = 0;
  virtual KStatus NewSequentialReadFileImpl(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                            size_t file_size = -1) = 0;
  virtual KStatus NewDirectoryImpl(const std::string& path) = 0;
  virtual KStatus DeleteDirImpl(const std::string& path) = 0;
  virtual KStatus DeleteFileImpl(const std::string& path) = 0;

 public:
  virtual ~TsIOEnv() {}
  static TsIOEnv& GetInstance();
  [[nodiscard]] KStatus NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                                          bool overwrite = true, size_t offset = -1) {
    return this->NewAppendOnlyFileImpl(filepath, file, overwrite, offset);
  }
  [[nodiscard]] KStatus NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                                          size_t file_size = -1) {
    return this->NewRandomReadFileImpl(filepath, file, file_size);
  }
  [[nodiscard]] KStatus NewSequentialReadFile(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                              size_t file_size = -1) {
    return this->NewSequentialReadFileImpl(filepath, file, file_size);
  }
  [[nodiscard]] KStatus NewDirectory(const std::string& path) { return this->NewDirectoryImpl(path); }
  [[nodiscard]] KStatus DeleteDir(const std::string& path) { return this->DeleteDirImpl(path); }
  [[nodiscard]] KStatus DeleteFile(const std::string& path) { return this->DeleteFileImpl(path); }
  virtual bool IsFIOMode() = 0;
};

class TsFIOEnv : public TsIOEnv {
 public:
  static TsIOEnv& GetInstance();
  KStatus NewAppendOnlyFileImpl(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file, bool overwrite = true,
                            size_t offset = -1) override;
  KStatus NewRandomReadFileImpl(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                            size_t file_size = -1) override;
  KStatus NewSequentialReadFileImpl(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                size_t file_size = -1) override;
  KStatus NewDirectoryImpl(const std::string& path) override;
  KStatus DeleteFileImpl(const std::string& path) override;
  KStatus DeleteDirImpl(const std::string& path) override;
  bool  IsFIOMode() override { return true; }
};

class TsMMapIOEnv : public TsIOEnv {
 public:
  static TsIOEnv& GetInstance();
  KStatus NewAppendOnlyFileImpl(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file, bool overwrite = true,
                            size_t offset = -1) override;
  KStatus NewRandomReadFileImpl(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                            size_t file_size = -1) override;
  KStatus NewSequentialReadFileImpl(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                size_t file_size = -1) override;
  KStatus NewDirectoryImpl(const std::string& path) override;
  KStatus DeleteFileImpl(const std::string& path) override;
  KStatus DeleteDirImpl(const std::string& path) override;
  bool  IsFIOMode() override { return false; }
};

class TsMemoryIOEnv : public TsIOEnv {
  friend class TsMemoryAppendOnlyFile;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<TsSliceGuard>> files_;
  KStatus RegisterFile(const std::string& path, TsBufferBuilder&& file) {
    {
      std::shared_lock s_lock(mutex_);
      if (files_.find(path) != files_.end()) {
        LOG_ERROR("file %s already exists.", path.c_str());
        return KStatus::FAIL;
      }
    }
    std::unique_lock lock(mutex_);
    files_[path] = std::make_shared<TsSliceGuard>(file.GetBuffer());
    return KStatus::SUCCESS;
  }

 public:
  static TsMemoryIOEnv& GetInstance() {
    static TsMemoryIOEnv instance;
    return instance;
  }
  KStatus NewAppendOnlyFileImpl(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file, bool overwrite = true,
                            size_t offset = -1) override;
  KStatus NewRandomReadFileImpl(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                            size_t file_size = -1) override;
  KStatus NewSequentialReadFileImpl(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                size_t file_size = -1) override;

  KStatus NewDirectoryImpl(const std::string& path) override { return SUCCESS; }
  KStatus DeleteFileImpl(const std::string& path) override;
  KStatus DeleteDirImpl(const std::string& path) override { return SUCCESS; }
  bool  IsFIOMode() override { return false; }
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
  // static_assert(std::has_unique_object_representations_v<FileHeader>, "padding in struct FileHeader");

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

  ~TsMMapAllocFile() override {
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
      auto ec = MakeErrorCode(errno);
      LOG_ERROR("fallocate size[%lu] error %s.", new_len, ec.message().c_str());
      return KStatus::FAIL;
    }

    auto cur_mmap_len = new_len - getHeader()->file_len;
    auto base = mmap(nullptr, cur_mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, getHeader()->file_len);
    if (base == MAP_FAILED) {
      auto ec = MakeErrorCode(errno);
      LOG_ERROR("mmap [%lu] error %s.", cur_mmap_len, ec.message().c_str());
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

  KStatus Sync() override {
    if (!EngineOptions::force_sync_file) {
      return SUCCESS;
    }
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

KStatus FileRangeCopy(TsRandomReadFile* src, TsAppendOnlyFile* dst, size_t start = 0, size_t end = -1);
// KStatus FileRangeCopy(TsSequentialReadFile* src, TsAppendOnlyFile* dst, size_t start, size_t end);

}  // namespace kwdbts
