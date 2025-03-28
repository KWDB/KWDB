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

#include <cmath>
#include <cstring>
#include <thread>
#include "mmap/mmap_hash_index.h"
#include "ts_object_error.h"
#include "lib/t1ha.h"

HashBucket::HashBucket(size_t  bucket_count) {
  // pthread_rwlock_init(&rwlock_, NULL);
  m_bucket_rwlock_ = new KRWLatch(RWLATCH_ID_HASH_INDEX_BUCKET_RWLOCK);
  m_bucket_count_ = bucket_count < 8 ? 8 : bucket_count;
  m_mem_bucket_ = reinterpret_cast<HashIndexRowID*>(new char[m_bucket_count_*sizeof(HashIndexRowID)]);
  memset(m_mem_bucket_, 0x00, sizeof(HashIndexRowID)*m_bucket_count_);
}
HashBucket::~HashBucket() {
  // pthread_rwlock_destroy(&rwlock_);
  delete m_bucket_rwlock_;
  delete [] m_mem_bucket_;
}
size_t HashBucket::get_bucket_index(const size_t& key) {
  return key % m_bucket_count_;
}
void HashBucket::resize(size_t new_bucket_count) {
  if (new_bucket_count <= m_bucket_count_) {
    // do nothing
    return;
  }
  HashIndexRowID* tmp_bucket = reinterpret_cast<HashIndexRowID*>(new char[new_bucket_count*sizeof(HashIndexRowID)]);
  memset(tmp_bucket, 0x00, sizeof(HashIndexRowID)*new_bucket_count);
  m_bucket_count_ = new_bucket_count;
  delete []m_mem_bucket_;
  m_mem_bucket_ = tmp_bucket;
}

MMapHashIndex::MMapHashIndex() {}

MMapHashIndex::MMapHashIndex(int key_len, size_t bkt_instances, size_t per_bkt_count) : MMapIndex(key_len) {
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

MMapHashIndex::~MMapHashIndex() {
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

int MMapHashIndex::open(const std::string &path, const std::string &db_path, const std::string &tbl_sub_path,
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
    return err_info.errcode;
}

std::pair<bool, size_t> MMapHashIndex::is_need_rehash() {
    if (metaData().m_row_count + 2 > metaData().m_bucket_count) {
        return std::make_pair(true, metaData().m_bucket_count * 2);
    }
    return std::make_pair(false, 0);
}

int MMapHashIndex::rehash(size_t new_size) {
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
    auto end = std::chrono::high_resolution_clock::now();
    auto ins_dur = std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    return 0;
}

void MMapHashIndex::bucketsRlock() {
    for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
        buckets_[idx]->Rlock();
    }
}

void MMapHashIndex::bucketsUnlock() {
    for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
        buckets_[idx]->Unlock();
    }
}

void MMapHashIndex::bucketsWlock() {
    for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
        buckets_[idx]->Wlock();
    }
}

void MMapHashIndex::dataRlock() {
    RW_LATCH_S_LOCK(m_file_rwlock_);
}

void MMapHashIndex::dataWlock() {
    RW_LATCH_X_LOCK(m_file_rwlock_);
}

void MMapHashIndex::dataUnlock() {
    RW_LATCH_UNLOCK(m_file_rwlock_);
}

void MMapHashIndex::loadRecord(size_t start, size_t end) {
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

void MMapHashIndex::resizeBucket(size_t  new_bucket_count) {
    if (new_bucket_count <= m_bucket_count_) {
        return;
    }
    mutexLock();
    for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
        buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
    }
    mutexUnlock();
}

int MMapHashIndex::size() const {
    if (mem_) {
        return metaData().m_file_size;
    }
    return 0;
}

int MMapHashIndex::reserve(size_t n) {
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

int MMapHashIndex::clear() {
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

void MMapHashIndex::getHashValue(const char *s, int len, size_t& hashcode) {
    hashcode = (*hash_func_)(s, len);
}

void MMapHashIndex::printHashTable() {
}
