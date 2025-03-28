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

#include "mmap_file.h"
#include "mmap_hash_index.h"

class MMapEntityRowHashIndex: public MMapFile {
  const off_t kHashMetaDataSize = 1024;
  const size_t k_Hash_Default_Row_Count = 1024;

 protected:

  TagHashIndexMutex*  m_rehash_mutex_;
  TagHashIndexRWLock* m_file_rwlock_;
  HashIndexData *mem_hash_;
  HashFunc hash_func_;

  std::vector<HashBucket* > buckets_;
  int    type_;
  size_t n_bkt_instances_;
  size_t m_element_count_;
  size_t m_bucket_count_;
  int    m_key_len_;
  size_t m_record_size_{0};

  void* addr(off_t offset) const
  { return reinterpret_cast<void*>((intptr_t) mem_ + offset); }

  void resizeBucket(size_t  new_bucket_count);

  inline HashIndexData* row(size_t n) const {
    return reinterpret_cast<HashIndexData *>(offsetAddr(addr(kHashMetaDataSize), n * metaData().m_record_size));
  }

  inline char* keyvalue(size_t n) const {
    return reinterpret_cast<char *>((intptr_t) mem_ + kHashMetaDataSize + n * metaData().m_record_size + sizeof(HashIndexData));
  }

  std::pair<bool, size_t> is_need_rehash();

  int rehash(size_t new_size);

  std::pair<TableVersionID, TagPartitionTableRowID> read_first(const char* key, int len = sizeof(uint64_t));

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

 public:
  explicit MMapEntityRowHashIndex(int key_len = sizeof(uint64_t), size_t bkt_instances = 1, size_t per_bkt_count = 1024);
  virtual ~MMapEntityRowHashIndex();

  HashIndexMetaData& metaData() const
  { return *(reinterpret_cast<HashIndexMetaData *>(mem_)); }

  int open(const string &path, const std::string &db_path, const string &tbl_sub_path, int flag,
           ErrorInfo &err_info);

  inline uint8_t type() const  { return type_; }

  int size() const;

  int reserve(size_t n);

  int keySize() const { return m_key_len_; }

  int put(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid);

  std::pair<TableVersionID, TagPartitionTableRowID>  delete_data(const char *key, int len = sizeof(uint64_t));

  /**
   * @brief	find the value stored in hash table for a given key.
   *
   * @param 	key			key to be found.
   * @param 	len			length of the key.
   * @return	the stored value in the hash table if key is found; 0 otherwise.
   */
  std::pair<TableVersionID, TagPartitionTableRowID> get(const char *s, int len = sizeof(uint64_t));

  int remove();

  void printHashTable();
  inline uint64_t getLSN() const {
    return metaData().m_lsn;
  }

  inline void setLSN(uint64_t lsn) {
    metaData().m_lsn = lsn;
    return;
  }

  inline void setDrop() {
    metaData().m_droped = true;
    return;
  }

  inline bool isDroped() const {
    return metaData().m_droped;
  }
};
