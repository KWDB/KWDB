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

#include "utils/big_table_utils.h"
#include "mmap/mmap_string_column.h"
#include "utils/string_utils.h"
#include "lt_rw_latch.h"
#include "lg_api.h"

MMapStringColumn::MMapStringColumn(latch_id_t latch_id, rwlatch_id_t rwlatch_id) {
  m_strfile_mutex_ = new KLatch(latch_id);
  m_strfile_rwlock_ = new KRWLatch(rwlatch_id);
}

MMapStringColumn::~MMapStringColumn() {
  delete m_strfile_mutex_;
  delete m_strfile_rwlock_;
}

int MMapStringColumn::open(const string &file_path, const std::string &real_file_path, int flags) {
  int err_code = str_file_.open(file_path, real_file_path, flags);
  if (err_code < 0)
    return err_code;
  if (str_file_.fileLen() <= 0) {
    size_t new_len = getPageOffset(8);
    err_code = str_file_.mremap(new_len);
    if ( err_code < 0 ) {
      return err_code;
    }
    size() = MMapStringColumn::startLoc();    // skip size & first element.
  }
  if (size() > str_file_.fileLen()) {  // string file is corrupted.
    LOG_ERROR("string file is corrupted. size: %lu > filelen: %lu ", size(), str_file_.fileLen());
    return KWECORR;
  }
  return 0;
}


int MMapStringColumn::incSize_(size_t len) {
  int err_code = 0;
  if (len > static_cast<size_t>(str_file_.fileLen()) - size()) {
    size_t new_len = getPageOffset(size() + len);
    size_t min_inc = getPageOffset(size() * 1.5);
    min_inc = std::max(min_inc, static_cast<size_t>(1048576));
    new_len = std::max(new_len, min_inc);
    wrLock();
    err_code = str_file_.mremap(new_len);
    unLock();
  }
  return err_code;
}

int MMapStringColumn::retryMap() {
  if (str_file_.fileLen() == str_file_.newLen()) {
    return 0;
  }
  int err_code = 0;
  wrLock();
  err_code = str_file_.mremap(str_file_.newLen());
  unLock();
  return err_code;
}

void* MMapStringColumn::memAddr() {
  return str_file_.memAddr();
}

int MMapStringColumn::incSize(size_t len) {
  MUTEX_LOCK(m_strfile_mutex_);
  int err_code = incSize_(len);
  MUTEX_UNLOCK(m_strfile_mutex_);
  return err_code;
}

size_t MMapStringColumn::push_back(const void *str, int len) {
  MUTEX_LOCK(m_strfile_mutex_);
  if (retryMap() < 0) {
    MUTEX_UNLOCK(m_strfile_mutex_);
    return -1;
  }
  size_t loc = size();
  if (incSize_(len + kStringLenLen) == 0) {
    unsigned char *vsp = reinterpret_cast<unsigned char *>(offsetAddr(str_file_.memAddr(), loc));
    size() += mmap_strlcpy(reinterpret_cast<char*>((intptr_t) vsp + kStringLenLen),
                           reinterpret_cast<const char*>(str), len);
    size() += kStringLenLen + 1;  // 1: char end character.
    *(reinterpret_cast<uint16_t *>(vsp)) = len;
  } else {
    loc = static_cast<size_t>(-1);
  }
  MUTEX_UNLOCK(m_strfile_mutex_);
  return loc;
}

size_t MMapStringColumn::push_back_binary(const void *data, int len) {
  MUTEX_LOCK(m_strfile_mutex_);
  if (retryMap() < 0) {
    MUTEX_UNLOCK(m_strfile_mutex_);
    return -1;
  }
  size_t loc = size();
  if (incSize_(len + kStringLenLen) == 0) {
    unsigned char *vsp = (unsigned char *)offsetAddr(str_file_.memAddr(), loc);
    memcpy(vsp + kStringLenLen, data, len);
    size() += len + kStringLenLen;
    *(reinterpret_cast<uint16_t *>(vsp)) = len;
  } else {
    loc = static_cast<size_t>(-1);
  }
  MUTEX_UNLOCK(m_strfile_mutex_);
  return loc;
}

size_t MMapStringColumn::push_back_hexbinary(const void *data, int len) {
  MUTEX_LOCK(m_strfile_mutex_);
  if (retryMap() < 0) {
    MUTEX_UNLOCK(m_strfile_mutex_);
    return -1;
  }
  size_t loc = size();
  if (incSize_(len) == 0) {
    unsigned char *vsp = (unsigned char *)offsetAddr(str_file_.memAddr(), loc);
    int hex_len = unHex(reinterpret_cast<const char *>(data), vsp + sizeof(int32_t),
      len - sizeof(int32_t));
    if (hex_len >= 0) {
      hex_len += sizeof(int32_t);
      *(reinterpret_cast<uint32_t *>(vsp)) = hex_len;
      size() += hex_len;
    } else {
      loc = static_cast<size_t>(-1);
    }
  } else {
    loc = static_cast<size_t>(-1);
  }
  MUTEX_UNLOCK(m_strfile_mutex_);
  return loc;
}

int MMapStringColumn::reserve(size_t new_row_num, int str_len) {
  MUTEX_LOCK(m_strfile_mutex_);
  size_t cur_sz = size();
  size_t new_sz = new_row_num * static_cast<size_t>(str_len);
  size_t ext_sz = (new_sz < cur_sz) ? 0 : (new_sz - cur_sz);

  // NOTE: avoid size_t underflow for '-' operation.
  int err_code = (ext_sz > 0) ? incSize_(ext_sz) : 0;
  MUTEX_UNLOCK(m_strfile_mutex_);
  return err_code;
}


int MMapStringColumn::reserve(size_t old_row_size, size_t new_row_size,
                              int max_len) {
  MUTEX_LOCK(m_strfile_mutex_);
  size_t cur_sz = size();
  size_t max_sz = max_len * new_row_size;
  size_t new_sz;

  if (old_row_size != 0) {
    new_sz = (cur_sz * new_row_size) / old_row_size;
    if (new_sz < cur_sz) {
      new_sz = std::max(max_sz, cur_sz + 8388608);
    }
  } else {
    new_sz = max_sz;
  }

    // NOTE: avoid size_t underflow for '-' operation.
  int err_code = (new_sz > cur_sz) ? incSize_(new_sz - cur_sz) : 0;
  MUTEX_UNLOCK(m_strfile_mutex_);
  return err_code;
}

size_t MMapStringColumn::stringToAddr(const string &str) {
  MUTEX_LOCK(m_strfile_mutex_);
  if (retryMap() < 0) {
    MUTEX_UNLOCK(m_strfile_mutex_);
    return -1;
  }
  size_t loc = size();
  size_t len = str.size();
  if (incSize_(len + kStringLenLen) == 0) {
    unsigned char *vsp = (unsigned char *)offsetAddr(str_file_.memAddr(), loc);
    memcpy(reinterpret_cast<char *>((intptr_t)vsp + kStringLenLen), str.c_str(), len);
    *(reinterpret_cast<uint32_t *>(vsp)) = len;
    size() += len + kStringLenLen + 1;
  } else {
    loc = static_cast<size_t>(-1);
  }
  // don't update string file
  MUTEX_UNLOCK(m_strfile_mutex_);
  return loc;
}

char * MMapStringColumn::getStringAddr(size_t loc)
{ return reinterpret_cast<char *>((intptr_t)str_file_.memAddr() + (loc)); }

int MMapStringColumn::trim(size_t loc) {
  size() = loc;
  size_t new_len = getPageOffset(loc);
  str_file_.resize(new_len);
  return 0;
}

int MMapStringColumn::push_back_nolock(const void *str, int len) {
  MUTEX_LOCK(m_strfile_mutex_);
  size_t start_offset = size();
  int err_code = incSize_(len + kStringLenLen);
  if (err_code < 0) {
    MUTEX_UNLOCK(m_strfile_mutex_);
    return -1;
  }
  size() = start_offset + len + kStringLenLen;
  MUTEX_UNLOCK(m_strfile_mutex_);
  rdLock();
  unsigned char *vsp = reinterpret_cast<unsigned char *>(offsetAddr(str_file_.memAddr(), start_offset));
  if (len > 0) {
    memcpy(reinterpret_cast<char *>((intptr_t) vsp + kStringLenLen),
           reinterpret_cast<const char *>(str), len);
  }
  *(reinterpret_cast<uint16_t *>(vsp)) = len;
  unLock();
  return start_offset;
}
