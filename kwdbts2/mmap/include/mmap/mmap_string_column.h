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

#pragma once

#include <mutex>
#include <string>
#include "mmap/mmap_file.h"
#include "lt_rw_latch.h"

// data starts at [16]
// 32-bit: [4-bytes reserved][4-byte size][8-byte reserved][data]
// 64-bit: [8-bytes reserved][8-byte size] [data...]
// first location starts with empty data (row 0).

using MMapStrFileMutex = KLatch;

using MMapStrFileRWLock = KRWLatch;

class MMapStringColumn {
 protected:
  MMapStrFileMutex*   m_strfile_mutex_;
  MMapStrFileRWLock*  m_strfile_rwlock_;
  // assuming mutex locked
  int incSize_(size_t len);
  MMapFile str_file_;

 public:
  static const int kStringLenLen = sizeof(uint16_t);
  static const int kEndCharacterLen = sizeof(char);
  MMapStringColumn() = delete;
  MMapStringColumn(latch_id_t latch_id, rwlatch_id_t rwlatch_id);

  virtual ~MMapStringColumn();

  int open(const string &file_path, const std::string &real_file_path, int flags);

  static int startLoc() { return 32; }

  // @return: location in string file.
  //          -1: no space left
  size_t push_back(const void *str, int len);

  // @return: location in string file.
  //          -1: no space left
  size_t push_back_binary(const void *data, int len);

  // @return: location in string file.
  //          -1: no space left or invalid hex string
  size_t push_back_hexbinary(const void *data, int len);

  // 32bit system: 4-bytes for size & pointers
  // 64bit system: 8-bytes
  size_t & size() { return ((reinterpret_cast<size_t *>(str_file_.memAddr()))[2]); }

  int incSize(size_t len);

  int retryMap();

  void* memAddr();

  int sync(int flags) { return str_file_.sync(flags); }

  std::string realFilePath() const { return str_file_.realFilePath(); }

  off_t fileLen() const { return str_file_.fileLen(); }

  int remove() { return str_file_.remove(); }

  int munmap() { return str_file_.munmap(); }

  int reserve(size_t new_row_num, int str_len);

  int reserve(size_t old_row_size, size_t new_row_size, int max_len);

  size_t stringToAddr(const string &str);

  char * getStringAddr(size_t loc);

  int trim(size_t loc);

  // @return:  0: success.
  //          -1: no space left
  int push_back_nolock(const void *str, int len);

  void mutexLock() {
    MUTEX_LOCK(m_strfile_mutex_);
  }
  void mutexUnlock() {
    MUTEX_UNLOCK(m_strfile_mutex_);
  }
  void wrLock() {
    RW_LATCH_X_LOCK(m_strfile_rwlock_);
  }
  void rdLock() {
    RW_LATCH_S_LOCK(m_strfile_rwlock_);
  }
  void unLock() {
    RW_LATCH_UNLOCK(m_strfile_rwlock_);
  }

  MMapFile& strFile() { return str_file_; }

  int rename(const string &new_fp) { return str_file_.rename(new_fp); }

};
