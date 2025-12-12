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

#include "ts_io.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <system_error>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"

namespace kwdbts {

KStatus TsMMapAppendOnlyFile::UnmapCurrent() {
  if (mmap_start_ == nullptr) {
    return SUCCESS;
  }
  // int ok = msync(mmap_start_, mmap_end_ - mmap_start_, MS_SYNC);
  // if (ok < 0) {
  //   LOG_ERROR("msync failed, reason: %s", strerror(errno));
  //   return FAIL;
  // }
  int ok = munmap(mmap_start_, mmap_end_ - mmap_start_);
  if (ok < 0) {
    LOG_ERROR("munmap failed, reason: %s", strerror(errno));
    return FAIL;
  }
  mmap_start_ = mmap_end_ = dest_ = synced_ = nullptr;

  // double the size if mmap_size_ < 1MB
  if (mmap_size_ < (1 << 20)) {
    mmap_size_ *= 2;
  }
  return SUCCESS;
}

KStatus TsMMapAppendOnlyFile::MMapNew() {
  assert(mmap_start_ == nullptr);
  // int ok = posix_fallocate(fd_, file_size_, mmap_size_);
  int ok = fallocate(fd_, 0, file_size_, mmap_size_);
  if (ok < 0) {
    LOG_ERROR("can not allocate space %lu on file %s, reason: %s", mmap_size_, path_.c_str(), strerror(errno));
    return FAIL;
  }
  size_t mmap_offset = TruncateToPage(file_size_, page_size_);
  void* ptr = mmap(nullptr, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, mmap_offset);
  if (ptr == MAP_FAILED) {
    LOG_ERROR("mmap failed on %s, reason: %s", path_.c_str(), strerror(errno));
    return FAIL;
  }
  ok = madvise(ptr, mmap_size_, MADV_SEQUENTIAL);
  if (ok < 0) {
    LOG_WARN("madvise failed, reason %s", strerror(errno));
  }
  mmap_start_ = static_cast<char*>(ptr);
  mmap_end_ = mmap_start_ + mmap_size_;
  dest_ = mmap_start_ + (file_size_ - mmap_offset);
  synced_ = mmap_start_;
  return SUCCESS;
}

KStatus TsMMapAppendOnlyFile::Append(std::string_view data) {
  size_t nleft = data.size();
  const char* src = data.data();
  while (nleft != 0) {
    size_t avail_space = mmap_end_ - dest_;
    if (avail_space == 0) {
      auto s = UnmapCurrent();
      if (s == FAIL) {
        LOG_ERROR("Append UnmapCurrent failed.");
        return FAIL;
      }
      s = MMapNew();
      if (s == FAIL) {
        LOG_ERROR("Append MMapNew failed.");
        return FAIL;
      }
    }
    // See the reason why copy_n is used here
    // https://stackoverflow.com/questions/4707012/is-it-better-to-use-stdmemcpy-or-stdcopy-in-terms-to-performance
    size_t ncopy = std::min(avail_space, nleft);
    dest_ = std::copy_n(src, ncopy, dest_);
    nleft -= ncopy;
    src += ncopy;
    file_size_ += ncopy;
  }
  return SUCCESS;
}

KStatus TsMMapAppendOnlyFile::Sync() {
  if (synced_ == nullptr) {
    return SUCCESS;
  }
  if (dest_ == synced_) {
    return SUCCESS;
  }
  char* p1 = mmap_start_ + TruncateToPage(synced_ - mmap_start_, page_size_);
  char* p2 = mmap_start_ + TruncateToPage(dest_ - mmap_start_ - 1, page_size_) + page_size_;
  int ok = msync(p1, p2 - p1, MS_SYNC);
  if (ok < 0) {
    LOG_ERROR("msync failed, reason: %s", strerror(errno));
    return FAIL;
  }
  synced_ = dest_;
  return SUCCESS;
}

KStatus TsMMapAppendOnlyFile::Close() {
  if (fd_ == -1) {
    return SUCCESS;
  }
  auto s = UnmapCurrent();
  if (s == FAIL) {
    return s;
  }
  int ok = ftruncate(fd_, file_size_);
  if (ok < 0) {
    LOG_ERROR("ftruncate file %s failed, reason: %s", path_.c_str(), strerror(errno));
    return FAIL;
  }
  ok = flock(fd_, LOCK_UN);
  if (ok < 0) {
    LOG_ERROR("flock file %s failed, reason: %s", path_.c_str(), strerror(errno));
    return FAIL;
  }
  ok = close(fd_);
  if (ok < 0) {
    LOG_ERROR("close file %s failed, reason: %s", path_.c_str(), strerror(errno));
    return FAIL;
  }

  fd_ = -1;
  mmap_start_ = mmap_end_ = dest_ = synced_ = nullptr;
  return SUCCESS;
}

TsMMapAppendOnlyFile::~TsMMapAppendOnlyFile() {
  if (fd_ >= 0) {
    this->Close();
  }
}

KStatus TsMMapRandomReadFile::Prefetch(size_t offset, size_t n) {
  if (offset + n > file_size_) {
    n = offset > file_size_ ? 0 : file_size_ - offset;
  }
  if (n == 0) {
    return SUCCESS;
  }

  size_t page_offset = TruncateToPage(offset, page_size_);
  char* p1 = mmap_start_ + page_offset;
  int ok = madvise(p1, n + (offset - page_offset), MADV_SEQUENTIAL);
  if (ok < 0) {
    LOG_WARN("madvise failed, reason %s", strerror(errno));
  }
  return SUCCESS;
}

KStatus TsMMapRandomReadFile::Read(size_t offset, size_t n, TsSliceGuard* result) const {
  if (offset >= file_size_ || offset + n > file_size_) {
    LOG_ERROR("file access overflow, file path: %s, file size: %zu, offset: %zu, length: %zu",
              path_.c_str(), file_size_, offset, n);
    return FAIL;
  }
  *result = TsSliceGuard(TSSlice{mmap_start_ + offset, n});
  return SUCCESS;
}

KStatus TsMMapSequentialReadFile::Read(size_t n, TsSliceGuard* slice) {
  if (offset_ >= file_size_ || offset_ + n > file_size_) {
    LOG_ERROR("file access overflow, file path: %s, file size: %zu, offset: %zu, length: %zu",
              path_.c_str(), file_size_, offset_, n);
    return FAIL;
  }
  *slice = TsSliceGuard(TSSlice{mmap_start_ + offset_, n});
  offset_ += n;
  return SUCCESS;
}

KStatus TsMMapIOEnv::NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                                       bool overwrite, size_t offset) {
  int fd = -1;
  int flag = O_RDWR | O_CREAT;
  if (overwrite) {
    flag |= O_TRUNC;
    offset = -1;
  }
  do {
    fd = open(filepath.c_str(), flag, 0644);
  } while (fd < 0 && errno == EINTR);
  if (fd < 0) {
    LOG_ERROR("can not open file %s, reason: %s", filepath.c_str(), strerror(errno));
    return FAIL;
  }
  size_t append_offset = lseek(fd, 0, SEEK_END);
  if (append_offset == -1) {
    LOG_ERROR("can not seek the end of %s, reason: %s", filepath.c_str(), strerror(errno));
    return FAIL;
  }
  if (offset != -1) {
    if (offset > append_offset) {
      LOG_ERROR("the given offset is larger than file size");
      return FAIL;
    }
    append_offset = offset;
  }

  int ok = flock(fd, LOCK_EX);
  if (ok < 0) {
    LOG_ERROR("flock failed, reason: %s", strerror(errno));
    return FAIL;
  }
  auto new_file = std::make_unique<TsMMapAppendOnlyFile>(filepath, fd, append_offset);
  *file = std::move(new_file);
  return SUCCESS;
}

static std::pair<int, size_t> OpenReadOnlyFile(const std::string& filepath, size_t file_size) {
  int fd = -1;
  do {
    fd = open(filepath.c_str(), O_RDONLY);
  } while (fd < 0 && errno == EINTR);
  if (fd < 0) {
    LOG_ERROR("cannot open file %s, reason: %s", filepath.c_str(), strerror(errno));
    return {-1, -1};
  }
  size_t actual_size = lseek(fd, 0, SEEK_END);
  if (actual_size == -1) {
    LOG_ERROR("lseek failed on file %s, reason: %s", filepath.c_str(), strerror(errno));
    return {-1, -1};
  }

  if (file_size == -1) {
    file_size = actual_size;
  }
  if (file_size > actual_size) {
    LOG_ERROR("error on file %s, the input file size %lu is larger than actual size %lu", filepath.c_str(), file_size,
              actual_size);
    return {-1, -1};
  }
  assert(file_size <= actual_size);
  return {fd, file_size};
}

KStatus TsMMapIOEnv::NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                                       size_t file_size) {
  auto [fd, readable_size] = OpenReadOnlyFile(filepath, file_size);
  if (fd < 0) {
    return FAIL;
  }
  void* ptr = nullptr;
  if (readable_size != 0) {
    ptr = mmap(nullptr, readable_size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
      LOG_ERROR("mmap failed on file %s, reason: %s", filepath.c_str(), strerror(errno));
      return FAIL;
    }
  }
  auto new_file = std::make_unique<TsMMapRandomReadFile>(filepath, fd, static_cast<char*>(ptr), readable_size);
  *file = std::move(new_file);
  return SUCCESS;
}

KStatus TsMMapIOEnv::NewSequentialReadFile(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                           size_t file_size) {
  auto [fd, readable_size] = OpenReadOnlyFile(filepath, file_size);
  if (fd < 0) {
    return FAIL;
  }

  void* ptr = nullptr;
  if (readable_size != 0) {
    ptr = mmap(nullptr, readable_size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
      LOG_ERROR("mmap failed on file %s, reason: %s", filepath.c_str(), strerror(errno));
      return FAIL;
    }
    int ok = madvise(ptr, readable_size, MADV_SEQUENTIAL);
    if (ok < 0) {
      LOG_WARN("madvise failed, reason %s", strerror(errno));
    }
  }

  auto new_file = std::make_unique<TsMMapSequentialReadFile>(filepath, fd, static_cast<char*>(ptr), readable_size);
  *file = std::move(new_file);
  return SUCCESS;
}

KStatus TsMMapIOEnv::NewDirectory(const std::string& path) {
  std::error_code ec;
  bool ok = fs::create_directories(path, ec);
  if (!ok) {
    if (fs::exists(path)) {
      return SUCCESS;
    }
    LOG_ERROR("create directory %s failed, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsMMapIOEnv::DeleteDir(const std::string& path) {
  std::error_code ec;
  uintmax_t n_removed = fs::remove_all(path, ec);
  if (n_removed == -1) {
    LOG_ERROR("cannot delete directory %s, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsMMapIOEnv::DeleteFile(const std::string& path) {
  std::error_code ec;
  bool ok = fs::remove(path, ec);
  if (!ok) {
    LOG_ERROR("cannot delete directory %s, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

TsIOEnv& TsMMapIOEnv::GetInstance() {
  static TsMMapIOEnv inst;
  return inst;
}

KStatus TsFIOEnv::NewAppendOnlyFile(const std::string& filepath, std::unique_ptr<TsAppendOnlyFile>* file,
                                   bool overwrite, size_t offset) {
  FILE* fp;
  if (overwrite) {
    offset = 0;
  }
  if (offset != -1 && fs::exists(filepath) && truncate(filepath.c_str(), offset) != 0) {
    LOG_ERROR("truncate failed on file %s, offset = %zu", filepath.c_str(), offset);
    return FAIL;
  }

  fp = fopen(filepath.c_str(), "ab");
  if (fp == nullptr) {
    LOG_ERROR("can not open file %s", filepath.c_str());
    return FAIL;
  }

  if (fseek(fp, 0, SEEK_END) !=0) {
    LOG_ERROR("fseek failed on file %s", filepath.c_str());
    return FAIL;
  }
  size_t append_offset = ftell(fp);
  auto new_file = std::make_unique<TsFIOAppendOnlyFile>(filepath, fp, append_offset);
  *file = std::move(new_file);
  return SUCCESS;
}

KStatus TsFIOEnv::NewRandomReadFile(const std::string& filepath, std::unique_ptr<TsRandomReadFile>* file,
                                   size_t file_size) {
  auto [fd, readable_size] = OpenReadOnlyFile(filepath, file_size);
  if (fd < 0) {
    return FAIL;
  }
  auto new_file = std::make_unique<TsFIORandomReadFile>(filepath, fd, readable_size);
  *file = std::move(new_file);
  return SUCCESS;
}

KStatus TsFIOEnv::NewSequentialReadFile(const std::string& filepath, std::unique_ptr<TsSequentialReadFile>* file,
                                       size_t file_size) {
  auto [fd, readable_size] = OpenReadOnlyFile(filepath, file_size);
  if (fd < 0) {
    return FAIL;
  }

  auto new_file = std::make_unique<TsFIOSequentialReadFile>(filepath, fd, readable_size);
  *file = std::move(new_file);
  return SUCCESS;
}

KStatus TsFIOEnv::NewDirectory(const std::string& path) {
  std::error_code ec;
  bool ok = fs::create_directories(path, ec);
  if (!ok) {
    if (fs::exists(path)) {
      return SUCCESS;
    }
    LOG_ERROR("create directory %s failed, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsFIOEnv::DeleteDir(const std::string& path) {
  std::error_code ec;
  uintmax_t n_removed = fs::remove_all(path, ec);
  if (n_removed == -1) {
    LOG_ERROR("cannot delete directory %s, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsFIOEnv::DeleteFile(const std::string& path) {
  std::error_code ec;
  bool ok = fs::remove(path, ec);
  if (!ok) {
    LOG_ERROR("cannot delete directory %s, reason: %s", path.c_str(), ec.message().c_str());
    return FAIL;
  }
  return SUCCESS;
}

TsIOEnv& TsFIOEnv::GetInstance() {
  static TsFIOEnv inst;
  return inst;
}

TsIOEnv& TsIOEnv::GetInstance() {
  if (EngineOptions::g_io_mode <= TsIOMode::FIO_AND_MMAP) {
    return TsFIOEnv::GetInstance();
  }
  return TsMMapIOEnv::GetInstance();
}

}  // namespace kwdbts
