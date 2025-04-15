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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <array>
#include <utility>
#include "ts_object.h"
#include "data_type.h"
#include "mmap_file.h"
#include "mmap_index.h"
#include "ts_object_error.h"
#include "lg_api.h"
#include "lt_rw_latch.h"

typedef uint32_t HashIndexRowID;
typedef uint64_t HashCode;
typedef uint64_t (*HashFunc) (const char *data, int len);

#define INVALID_TABLE_VERSION_ID 0

using TagHashIndexMutex = KLatch;
using TagHashIndexRWLock = KRWLatch;
using TagHashBucketRWLock = KRWLatch;

// data of index file
struct HashIndexData {
  HashCode         hash_val;
  TableVersionID   tb_version;
  TagPartitionTableRowID    bt_row;
  HashIndexRowID   next_row;
};

class  HashBucket {
 private:
  HashIndexRowID* m_mem_bucket_;
  TagHashBucketRWLock* m_bucket_rwlock_;
  size_t m_bucket_count_;

 public:
  explicit HashBucket(size_t  bucket_count = 8);
  virtual ~HashBucket();
  size_t get_bucket_index(const size_t& key);
  void resize(size_t new_bucket_count);
  HashIndexRowID& bucketValue(size_t n) {
    return *(m_mem_bucket_ + n);
  }
  inline int Rlock() {
    RW_LATCH_S_LOCK(m_bucket_rwlock_);
    return 0;
    //return pthread_rwlock_rdlock(&rwlock_);
  }
  inline int Wlock() {
    RW_LATCH_X_LOCK(m_bucket_rwlock_);
    return 0;
    // return pthread_rwlock_wrlock(&rwlock_);
  }
  inline int Unlock() {
    RW_LATCH_UNLOCK(m_bucket_rwlock_);
    return 0;
    // return pthread_rwlock_unlock(&rwlock_);
  }
};

struct HashIndexMetaData {
  size_t    m_file_size;  // Hash index file size
  size_t    m_record_size;  // Hash index record size
  uint32_t  m_row_count;  // Hash index row count
  size_t    m_bucket_count;
  uint64_t  m_lsn;
  bool      m_droped;
  std::array<int64_t , 10> tag_col_ids;
};

// please keep lsn and drop together and relative order
constexpr int lsnOffsetInHashIndex() {
  return offsetof(struct HashIndexMetaData, m_lsn);
}

class MMapHashIndex: public MMapIndex {
 protected:
  const size_t k_Hash_Default_Row_Count = 1024;
  TagHashIndexMutex*  m_rehash_mutex_;
  TagHashIndexRWLock* m_file_rwlock_;
  HashIndexData *mem_hash_;
  HashFunc hash_func_;

  std::vector<HashBucket* > buckets_;
  size_t n_bkt_instances_;
  size_t m_element_count_;
  size_t m_bucket_count_;
  size_t m_record_size_{0};

  void* addr(off_t offset) const { return reinterpret_cast<void*>((intptr_t) mem_ + offset); }

  void resizeBucket(size_t  new_bucket_count);

  inline HashIndexData* row(size_t n) const { 
    return reinterpret_cast<HashIndexData *>(offsetAddr(addr(kHashMetaDataSize), n * metaData().m_record_size));
  }

  inline char* keyvalue(size_t n) const {
    return reinterpret_cast<char *>((intptr_t) mem_ + kHashMetaDataSize + n * metaData().m_record_size + sizeof(HashIndexData));
  }

  std::pair<bool, size_t> is_need_rehash();

  int rehash(size_t new_size);

  void mutexLock() { MUTEX_LOCK(m_rehash_mutex_); }

  void mutexUnlock() { MUTEX_UNLOCK(m_rehash_mutex_); }

  void bucketsRlock();

  void bucketsUnlock();

  void bucketsWlock();

  void dataRlock();

  void dataWlock();

  void dataUnlock();

  void loadRecord(size_t start, size_t end);

  void getHashValue(const char *s, int len, size_t& hashcode);

  inline HashIndexData* addrHash() const {
    return reinterpret_cast<HashIndexData *>((intptr_t) mem_ + kHashMetaDataSize);
  }

  inline bool compare(const char* key, size_t src_row) {
    return (memcmp(keyvalue(src_row), key, m_key_len_) == 0);
  }

  const off_t kHashMetaDataSize = 1024;
 public:
  explicit MMapHashIndex(int key_len, size_t bkt_instances = 1, size_t per_bkt_count = 1024);

  MMapHashIndex();

  virtual ~MMapHashIndex();

  HashIndexMetaData& metaData() const { return *(reinterpret_cast<HashIndexMetaData *>(mem_)); }

  virtual int open(const string &path, const std::string &db_path, const string &tbl_sub_path, int flag,
                   ErrorInfo &err_info);

  int reserve(size_t n);

  int keySize() const { return m_key_len_; }

  int clear();

  void printHashTable();

  inline uint64_t getLSN() const { return metaData().m_lsn; }

  inline void setLSN(uint64_t lsn) {
    metaData().m_lsn = lsn;
    return;
  }

  inline void setDrop() {
    metaData().m_droped = true;
    return;
  }

  inline size_t getElementCount() { return m_element_count_; }

  inline bool isDroped() const { return metaData().m_droped; }

  int size() const;
};
