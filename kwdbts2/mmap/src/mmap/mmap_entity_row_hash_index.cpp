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

#include "mmap/mmap_entity_row_hash_index.h"
#include "lib/t1ha.h"

MMapEntityRowHashIndex::MMapEntityRowHashIndex(int key_len, size_t bkt_instances, size_t per_bkt_count) : m_key_len_(key_len) {
  m_record_size_ = m_key_len_ + sizeof(HashIndexData);
  mem_hash_ = nullptr;
  hash_func_ = t1ha1_le;
  // Ensure correct values
  n_bkt_instances_ = bkt_instances < 1 ? 1:bkt_instances;
  // m_bucket_count_ = n_bkt_instances_ * (per_bkt_count< 8 ? 8: per_bkt_count);
  m_bucket_count_ = k_Hash_Default_Row_Count;
  per_bkt_count = m_bucket_count_ / n_bkt_instances_;
  m_element_count_ = 0;

  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    buckets_.push_back(std::move(new HashBucket(per_bkt_count)));
  }
  m_rehash_mutex_ = new KLatch(LATCH_ID_HASH_INDEX_MUTEX);
  m_file_rwlock_  = new KRWLatch(RWLATCH_ID_HASH_INDEX_FILE_RWLOCK);
  // pthread_mutex_init(&rehash_mutex_, NULL);
}

MMapEntityRowHashIndex::~MMapEntityRowHashIndex() {
  // delete []mem_bucket_;
  // mem_bucket_ = nullptr;
  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    delete buckets_[i];
    buckets_[i] = nullptr;
  }
  delete m_rehash_mutex_;
  delete m_file_rwlock_;
  m_rehash_mutex_ = nullptr;
  m_file_rwlock_ = nullptr;
  // pthread_mutex_destroy(&rehash_mutex_);
  // munmap
  MMapFile::munmap();
}

void MMapEntityRowHashIndex::bucketsRlock() {
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->Rlock();
  }
}

void MMapEntityRowHashIndex::bucketsUnlock() {
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->Unlock();
  }
}

void MMapEntityRowHashIndex::bucketsWlock() {
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->Wlock();
  }
}

void MMapEntityRowHashIndex::dataRlock() {
  RW_LATCH_S_LOCK(m_file_rwlock_);
}

void MMapEntityRowHashIndex::dataWlock() {
  RW_LATCH_X_LOCK(m_file_rwlock_);
}

void MMapEntityRowHashIndex::dataUnlock() {
  RW_LATCH_UNLOCK(m_file_rwlock_);
}

void MMapEntityRowHashIndex::loadRecord(size_t start, size_t end) {
  // uint32_t offset ;
  int bkt_idx = 0;
  int bkt_ins_idx = 0;
  // HashIndexData* rec = addrHash();
  m_bucket_count_ = metaData().m_bucket_count;
  for (size_t idx = start; idx <= end; ++idx) {
    bkt_ins_idx = (row(idx)->hash_val >> 56) & (n_bkt_instances_ - 1);
    bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(row(idx)->hash_val);
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = idx;
  }
}

void MMapEntityRowHashIndex::resizeBucket(size_t  new_bucket_count) {
  if (new_bucket_count <= m_bucket_count_) {
    return;
  }
  mutexLock();
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
  }
  mutexUnlock();
}

int MMapEntityRowHashIndex::open(const std::string &path, const std::string &db_path, const std::string &tbl_sub_path,
                        int flags, ErrorInfo &err_info) {
  if (flags & O_CREAT) {
    size_t new_file_size = (k_Hash_Default_Row_Count + 1) * m_record_size_ + kHashMetaDataSize;
    err_info.errcode = MMapFile::open(path, db_path + tbl_sub_path + path, flags, new_file_size, err_info);
  } else {
    err_info.errcode = MMapFile::open(path, db_path + tbl_sub_path + path, flags);
  }
  if (err_info.errcode < 0)
    return err_info.errcode;
  if (file_length_ < kHashMetaDataSize)
    err_info.errcode = mremap(kHashMetaDataSize);
  if (err_info.errcode < 0)
    return err_info.errcode;
  if (file_length_ >= kHashMetaDataSize) {
    mem_hash_ = addrHash();
    if (metaData().m_row_count) {
      resizeBucket(metaData().m_bucket_count);
      loadRecord(1, metaData().m_row_count);
      m_element_count_ = metaData().m_row_count;
    }
  }
  metaData().m_bucket_count = m_bucket_count_;
  metaData().m_record_size = m_record_size_;
  return err_info.errcode;
}

std::pair<bool, size_t> MMapEntityRowHashIndex::is_need_rehash() {
  if (metaData().m_row_count + 2 > metaData().m_bucket_count) {
    return std::make_pair(true, metaData().m_bucket_count * 2);
  }
  return std::make_pair(false, 0);
}

int MMapEntityRowHashIndex::rehash(size_t new_size) {
  size_t new_bucket_count = new_size;
  if (new_bucket_count <= m_bucket_count_) {
    // do nothing
    return 0;
  }
  LOG_INFO("Hash Index %s rehash, new_size: %lu", filePath().c_str(), new_size);
  auto start = std::chrono::high_resolution_clock::now();
  bucketsWlock();
  dataWlock();
  // extend size
  size_t new_file_size = (new_bucket_count + 1) * metaData().m_record_size + kHashMetaDataSize;
  if (file_length_ < new_file_size) {
    int err_code = mremap(new_file_size);
    if (err_code < 0) {
      dataUnlock();
      bucketsUnlock();
      return err_code;
    }
    mem_hash_ = addrHash();
    metaData().m_file_size = new_file_size;
  }
  // resize buckets
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
  }
  // rehash record
  int bkt_ins_idx = 0;
  int bkt_idx = 0;
  for (uint32_t rownum = 1; rownum <= metaData().m_row_count; ++rownum) {
    bkt_ins_idx = (row(rownum)->hash_val >> 56) & (n_bkt_instances_ - 1);
    bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(row(rownum)->hash_val);
    row(rownum)->next_row = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = rownum;
  }
  m_bucket_count_ = new_bucket_count;
  metaData().m_bucket_count = m_bucket_count_;
  dataUnlock();
  bucketsUnlock();
  return 0;
}

int MMapEntityRowHashIndex::size() const {
  if (mem_) {
    return metaData().m_file_size;
  }
  return 0;
}

int MMapEntityRowHashIndex::reserve(size_t n) {
  dataWlock();
  size_t new_file_size = (n+1) * metaData().m_record_size + kHashMetaDataSize;
  if (file_length_ < new_file_size) {
    int err_code = mremap(new_file_size);
    if (err_code < 0) {
      dataUnlock();
      return err_code;
    }
    mem_hash_ = addrHash();
    metaData().m_file_size = new_file_size;
  }
  dataUnlock();
  return 0;
}


int MMapEntityRowHashIndex::put(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) {
  HashCode hash_val = (*hash_func_)(s, len);
  mutexLock();
  std::pair<bool, size_t> do_rehash = is_need_rehash();
  if (do_rehash.first && rehash(do_rehash.second) < 0) {
    LOG_ERROR("rehash failed.");
    return -1;
  }
  // index remap need check here?
  size_t rownum = metaData().m_row_count + 1;  // change metaData
  metaData().m_row_count++;
  ++m_element_count_;
  mutexUnlock();

  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ -1);
  buckets_[bkt_ins_idx]->Wlock();
  size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);

  dataRlock();
  // HashIndexData* rec = addrHash();
  // write to .ht file
  row(rownum)->hash_val = hash_val;
  row(rownum)->bt_row = tag_table_rowid;
  row(rownum)->tb_version = table_version;
  memcpy(keyvalue(rownum), s, len);

  row(rownum)->next_row = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
  buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = rownum;
  dataUnlock();

  buckets_[bkt_ins_idx]->Unlock();

  return 0;
}

void MMapEntityRowHashIndex::getHashValue(const char *s, int len, size_t& hashcode) {
  hashcode = (*hash_func_)(s, len);
}

std::pair<TableVersionID, TagPartitionTableRowID> MMapEntityRowHashIndex::read_first(const char *key, int len) {
  HashCode hash_val = (*hash_func_)(key, len);
  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  buckets_[bkt_ins_idx]->Rlock();
  size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);

  dataRlock();
  // HashIndexData* rec = addrHash();
  while (rownum) {
    // if (h_value->hash_val_ == rec[rownum].hash_val_) {
    if (this->compare(key, rownum)) {
      TableVersionID tmp_version = row(rownum)->tb_version;
      TagPartitionTableRowID tmp_part_rowid = row(rownum)->bt_row;
      dataUnlock();
      buckets_[bkt_ins_idx]->Unlock();
      return std::make_pair(tmp_version, tmp_part_rowid);
    }
    rownum = row(rownum)->next_row;
  }
  dataUnlock();
  buckets_[bkt_ins_idx]->Unlock();
  return std::make_pair(INVALID_TABLE_VERSION_ID, INVALID_TABLE_VERSION_ID);
}

int MMapEntityRowHashIndex::remove() {
  // mem_bucket_ = nullptr;
  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    delete buckets_[i];
    buckets_[i] = nullptr;
  }
  delete m_rehash_mutex_;
  delete m_file_rwlock_;
  m_rehash_mutex_ = nullptr;
  m_file_rwlock_ = nullptr;
  return MMapFile::remove();
}

std::pair<TableVersionID, TagPartitionTableRowID>  MMapEntityRowHashIndex::delete_data(const char *key, int len) {
  size_t hash_val_ = (*hash_func_)(key, len);
  size_t bkt_ins_idx = (hash_val_ >> 56) & (n_bkt_instances_ - 1);
  buckets_[bkt_ins_idx]->Wlock();
  size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val_);

  dataWlock();
  size_t delete_count = 0;
  // HashIndexData* rec = addrHash();
  size_t pre_rownum = 0;
  size_t tmp_rownum = 0;
  TagPartitionTableRowID ret_row = INVALID_TABLE_VERSION_ID;
  TableVersionID ret_tbl_version = INVALID_TABLE_VERSION_ID;
  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
  if (rownum && (hash_val_ == row(rownum)->hash_val &&
                 this->compare(key, rownum))) {
    // matched bucketValue
    pre_rownum = rownum;
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = row(rownum)->next_row;
    ret_tbl_version = row(rownum)->tb_version;
    ret_row = row(rownum)->bt_row;
    memset(row(rownum), 0x00, metaData().m_record_size);
    ++delete_count;
    goto end_success;
  }
  if (!rownum) {
    // list empty
    m_element_count_ -= delete_count;
    dataUnlock();
    buckets_[bkt_ins_idx]->Unlock();
    std::string key_str;
    // toHexString(key, len, key_str);
    LOG_WARN("failed to delete key: %s, hash: %lu from hash index %s, "
             "not find the key",
             key_str.c_str(), hash_val_, filePath().c_str());
    return std::make_pair(INVALID_TABLE_VERSION_ID, INVALID_TABLE_VERSION_ID);
  }
  // found list node
  pre_rownum = rownum;
  rownum = row(rownum)->next_row;
  while (rownum) {
    tmp_rownum = row(rownum)->next_row;
    if (hash_val_ == row(rownum)->hash_val &&
        this->compare(key, rownum) ) {
      // match node
      row(pre_rownum)->next_row = row(rownum)->next_row;
      ret_tbl_version = row(rownum)->tb_version;
      ret_row = row(rownum)->bt_row;
      ++delete_count;
      memset(row(rownum), 0x00, metaData().m_record_size);
      break;
    }
    pre_rownum = rownum;
    rownum = tmp_rownum;
  }
  end_success:
  m_element_count_ -= delete_count;
  dataUnlock();
  buckets_[bkt_ins_idx]->Unlock();
  return std::make_pair(ret_tbl_version, ret_row);
}

std::pair<TableVersionID, TagPartitionTableRowID> MMapEntityRowHashIndex::get(const char *s, int len) {
  return read_first(s, len);
}
