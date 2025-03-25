// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <unistd.h>
#include <sys/mman.h>
#include <iostream>
#include "mmap/mmap_file.h"
#include "ts_object_error.h"
#include "utils/big_table_utils.h"
#include "lg_api.h"

MMapFile::MMapFile() {
  mem_ = 0;
  file_length_ = 0;
  new_length_ = 1;
  flags_ = 0;
}

MMapFile::~MMapFile() { munmap(); }

int MMapFile::reportError() {
  int err_code = errnoToErrorCode();
  if (err_code == KWEOTHER)
    return KWEMMAP;
  return err_code;
}

void MMapFile::LogMMapFileError(const std::string &op) {
  LOG_ERROR("%lx [KWFTL] %s failed: os system err_no %d, \"%s\", len: %lu", pthread_self(),
            op.c_str(), errno, absolute_file_path_.c_str(), file_length_);
}

int MMapFile::open() {
  int fd;
  if (flags_ & (O_ANONYMOUS|O_MATERIALIZATION))
    return 0;

  if (absolute_file_path_ == "") {
    // create a temporary file for mmap file since absolute_file_path_ is empty
    char file_name_template[] = "/tmp/kwdb_XXXXXX";
    if ((fd = ::mkstemp(file_name_template)) < 0) {
      LOG_ERROR("open() temporary file failed: errno[%d]", errno);
      return reportError();
    }
    absolute_file_path_ = file_name_template;
  } else {
    if ((fd = ::open(absolute_file_path_.c_str(), flags_,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
      if (errno == EROFS) {
        flags_ = (flags_ & ~O_RDWR);
        if ((fd = ::open(absolute_file_path_.c_str(), flags_,
          S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
  open_error:
          mem_ = 0;
          LOG_ERROR("open() failed: errno[%d], file[%s]", errno, absolute_file_path_.c_str());
          return reportError();
        }
      } else {
        goto open_error;
      }
    }
  }
  file_length_ = lseek(fd, 0, SEEK_END);

  if (file_length_ != 0 && file_length_ != (off_t)-1) {
    int mmap_flags = (readOnly()) ?
      PROT_READ : PROT_READ | PROT_WRITE;
//    mmap_mtx.lock();
    mem_ = mmap(0, file_length_, mmap_flags, MAP_SHARED, fd, 0);
//    mmap_mtx.unlock();
  }
  close(fd);
  if (mem_ == MAP_FAILED) {
    mem_ = nullptr;
    LogMMapFileError("mmap");
    checkError();
    return reportError();
  }
  new_length_ = file_length_;
  return 0;
}

int MMapFile::openTemp() {
  // set file_path_ to empty and let it create a temporary file.
  file_path_ = "";
  absolute_file_path_ = file_path_;
  flags_ = O_CREAT;
  return open();
}

int MMapFile::open(const std::string &file_path, int flags) {
  file_path_ = file_path;
  absolute_file_path_ = file_path_;
  flags_ = flags;
  return open();
}

int MMapFile::open(const std::string &file_path, const std::string &absolute_file_path, int flags) {
  file_path_ = file_path;
  absolute_file_path_ = absolute_file_path;
  flags_ = flags;
  return open();
}

int MMapFile::open(const std::string &file_path, const std::string &absolute_file_path, int flags, size_t init_sz, ErrorInfo &err_info) {
  err_info.errcode = open(file_path, absolute_file_path, flags);
  if (err_info.errcode < 0)
    return err_info.errcode;
  if (file_length_ < (off_t)init_sz) {
    err_info.errcode = mremap(getPageOffset(init_sz));
  }
  return err_info.errcode;
}

int MMapFile::munmap() {
  int err_code = 0;
  if (mem_) {
    err_code = ::munmap(mem_, file_length_);
     if (flags_ & O_ANONYMOUS) {
      kwdbts::kw_used_anon_memory_size.fetch_sub(file_length_);
     }
    mem_ = 0;
    file_length_ = 0;
    new_length_ = 1;
  }
  return err_code;
}

int MMapFile::mremap(size_t length) {
    if (length && (length < static_cast<size_t>(file_length_)))
        return 0;
    int err_code = resize(length);
    if (err_code == KWENOMEM) {
        ErrorInfo err_info;
        err_info.setError(KWENOMEM);
        LOG_FATAL("remmap file faild. %s", this->file_path_.c_str());
    }
    return err_code;
}

int MMapFile::sync(int flags) {
    if (mem_ && !(flags_ & O_ANONYMOUS)) {
        return msync(mem_, file_length_, flags);
    }
    return 0;
}


int MMapFile::resize(size_t length) {
  // TODO(zhuderun): remove in the future
  if ((flags_ & O_ANONYMOUS) && (kwdbts::kw_used_anon_memory_size.load() + length) > kwdbts::EngineOptions::max_anon_memory_size()) {
    // check if ANON memory reach limit.
    LogMMapFileError("resize new "+ std::to_string(length) + " bytes");
    return KWENOMEM;
  }
  new_length_ = length;
  void *mem_tmp = nullptr;
  if (!(flags_ & O_ANONYMOUS)) {
    int fd;
    if ((fd = ::open(absolute_file_path_.c_str(), O_RDWR | O_CREAT,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
      // mem_ = 0;  failed, reuse original address, make sure no core.
      LogMMapFileError("open file");
      return reportError();
    }
    // posix_fallocate is able to detect disk full.
    int err_code;
#if defined (__ANDROID__)
    if (ftruncate(fd, length) < 0) {
#else
    if (length >= file_length_) {
      if ((err_code = posix_fallocate(fd, 0, length)) != 0) {
        close(fd);
        LogMMapFileError("resize file");
        return errnumToErrorCode(err_code);
      }
    } else {
      if ((err_code = ftruncate(fd, length)) < 0) {
        close(fd);
        LogMMapFileError("resize file");
        return errnumToErrorCode(err_code);
      }
    }
#endif
    if (length) {
#if !defined(__x86_64__) && !defined(_M_X64)
      munmap();
#endif
//  mmap_mtx.lock();
      mem_tmp = (!mem_) ?
          mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) :
          ::mremap(mem_, file_length_, length, MREMAP_MAYMOVE);
//  mmap_mtx.unlock();
    }
    close(fd);
  } else {  // anonymous memory
//    mmap_mtx.lock();
    mem_tmp = (!mem_) ? mmap(0, length, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) :
      ::mremap(mem_, file_length_, length, MREMAP_MAYMOVE);
//    mmap_mtx.unlock();
  }
  if (mem_tmp == MAP_FAILED) {
    // mem_ = 0;
    LogMMapFileError("remap");
    checkError();
    return reportError();
  }
  if (flags_ & O_ANONYMOUS) {
    // Only incremental can be recorded
    kwdbts::kw_used_anon_memory_size.fetch_add(length - file_length_);
  }
  mem_ = mem_tmp;
  file_length_ = length;
  return 0;
}

int MMapFile::remove() {
  int err_code = 0;
  munmap();
  if (!absolute_file_path_.empty()) {
    err_code = ::remove(absolute_file_path_.c_str());
    absolute_file_path_.clear();
  }
  return err_code;
}

int MMapFile::rename(const string &new_fp) {
  int err_code = 0;
  if (flags_ & O_ANONYMOUS) {
    absolute_file_path_ = new_fp;
  } else {
    munmap();
    if (!absolute_file_path_.empty()) {
      err_code = ::rename(absolute_file_path_.c_str(), new_fp.c_str());
      if (err_code != 0) {
        err_code = errnoToErrorCode();
      } else {
        absolute_file_path_ = new_fp;
      }

    }
  }
  return err_code;
}

void MMapFile::copyMember(MMapFile& other) {
  absolute_file_path_ = other.absolute_file_path_;
  file_length_ = other.file_length_;
  new_length_ = other.new_length_;
  file_path_ = other.file_path_;
  flags_ = other.flags_;
}

void MMapFile::checkError() {
  FILE* maps_file = fopen("/proc/self/maps", "r");
  if (maps_file) {
    int map_count = 0;
    char buffer[256];
    while (fgets(buffer, sizeof(buffer), maps_file)) {
      map_count++;
    }
    fclose(maps_file);

    int max_map_count = 0;
    FILE* sysctl_file = fopen("/proc/sys/vm/max_map_count", "r");
    if (sysctl_file) {
      memset(buffer, 0, sizeof(buffer));
      if (fgets(buffer, sizeof(buffer), sysctl_file)) {
        std::string line = buffer;
        std::istringstream iss(line);
        iss >> max_map_count;
      }
      fclose(sysctl_file);
      if (map_count >= max_map_count * 0.9) {
        LOG_WARN("Likely vm.max_map_count limit: %d/%d maps used", map_count, max_map_count);
      } else {
        LOG_WARN("Likely out of memory: %d/%d maps used", map_count, max_map_count);
      }
    }
  }
}
