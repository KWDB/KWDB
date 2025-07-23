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
#include <cstdio>
#include <cstring>
#include <limits>
#include <thread>
#include <stdexcept>
#include "utils/big_table_utils.h"
#include "ts_object_error.h"
#include "lib/t1ha.h"
#include "ee_dynamic_hash_index.h"
#include "mmap/mmap_file.h"
#include "ee_global.h"

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

// construct a hash bucket with at least 8 buckets, init the mem for buckets
DynamicHashBucket::DynamicHashBucket() {
}

// destructor, release the bucket mem
DynamicHashBucket::~DynamicHashBucket() {
  if (m_mem_bucket_) {
    delete[] m_mem_bucket_;
  }
}

// get bucket index
k_int32 DynamicHashBucket::init(size_t bucket_count) {
  m_bucket_count_ = bucket_count < 8 ? 8 : bucket_count;
  // allocate mem for count * DynamicHashIdnexRowID objects
  m_mem_bucket_ = reinterpret_cast<DynamicHashIndexRowID*>(new char[m_bucket_count_ * sizeof(DynamicHashIndexRowID)]);
  if (nullptr == m_mem_bucket_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("m_mem_bucket_ new failed\n");
    return KWENOMEM;
  }
  memset(m_mem_bucket_, 0x00, sizeof(DynamicHashIndexRowID) * m_bucket_count_);
  return 0;
}

// get bucket index
size_t DynamicHashBucket::get_bucket_index(const size_t& key) const {
  return key % m_bucket_count_;
}

// adjust bucket size when new_bucket count > prev bucket count
k_int32 DynamicHashBucket::resize(size_t new_bucket_count) {
  if (new_bucket_count <= m_bucket_count_) {
      return 0;
  }
  DynamicHashIndexRowID* tmp_bucket =
          reinterpret_cast<DynamicHashIndexRowID*>(new char[new_bucket_count * sizeof(DynamicHashIndexRowID)]);
  if (nullptr == tmp_bucket) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("tmp_bucket new failed\n");
    return KWENOMEM;
  }
  memset(tmp_bucket, 0x00, sizeof(DynamicHashIndexRowID) * new_bucket_count);
  m_bucket_count_ = new_bucket_count;
  delete[] m_mem_bucket_;
  m_mem_bucket_ = tmp_bucket;
  return 0;
}

DynamicHashIndex::DynamicHashIndex() {
  // all moved to init()
}

// destructor, release bucket instances
DynamicHashIndex::~DynamicHashIndex() {
  // check mode
  if (mode_ == 0) {
    // release in-memory resources
    for (size_t i = 0; i < n_bkt_instances_; ++i) {
      delete buckets_[i];
      buckets_[i] = nullptr;
    }
    if (mem_hash_) {
      delete[] mem_hash_;
      mem_hash_ = nullptr;
    }
  } else {
    // release mmap resources
    remove();
  }
}

// create index with mmap file for rel hash
k_int32 DynamicHashIndex::init(k_int32 key_len, size_t bkt_instances, size_t per_bkt_count,
                            int64_t data_size_threshold_in_memory) {
  m_record_size_ = m_key_len_ + sizeof(DynamicHashIndexData);
  mem_hash_ = nullptr;
  hash_func_ = t1ha1_le;
  n_bkt_instances_ = bkt_instances < 1 ? 1 : bkt_instances;
  m_bucket_count_ = n_bkt_instances_ * (per_bkt_count < 8 ? 8 : per_bkt_count);
  m_element_count_ = 0;
  data_size_threshold_in_memory_ = data_size_threshold_in_memory;
  // initialize buckets_
  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    DynamicHashBucket* bucket = new DynamicHashBucket();
    if (nullptr == bucket) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("bucket new failed\n");
      return KWENOMEM;
    }
    k_int32 ret = bucket->init(per_bkt_count);
    if (ret != 0) {
      delete bucket;
      return ret;
    }
    buckets_.push_back(bucket);
  }

  m_key_len_ = key_len;
  // calculate index data size
  m_record_size_ = m_key_len_ + sizeof(DynamicHashIndexData);
  // check mem size, if < threshold, use in-memory, otherwise use mmap
  mode_ = (m_record_size_ < data_size_threshold_in_memory_) ? 0 : 1;
  if (mode_ == 0) {
    // in-memory method
    size_t initial_size = m_bucket_count_ * m_record_size_;
    mem_hash_ = new char[initial_size]();
    if (nullptr == mem_hash_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("mem_hash_ new failed\n");
      return KWENOMEM;
    }

    meta_data.m_bucket_count = m_bucket_count_;
    meta_data.m_record_size = m_record_size_;
    meta_data.m_capacity = initial_size;

    return 0;
  } else {
    size_t new_file_size = m_bucket_count_ * m_record_size_;
    ErrorInfo err_info;
    // use simple open
    err_info.errcode = MMapFile::openTemp();

    if (err_info.errcode < 0) {
      return err_info.errcode;
    }
    // allocate file size
    err_info.errcode = mremap(new_file_size);
    if (err_info.errcode < 0) {
      return err_info.errcode;
    }
    // update mem hash
    mem_hash_ = addrHash();

    // 更新元数据中的桶数量和记录大小
    meta_data.m_bucket_count = m_bucket_count_;
    meta_data.m_record_size = m_record_size_;
    meta_data.m_file_size = new_file_size;

    return err_info.errcode;
  }
}

// rehash wrt new bucket count
k_int32 DynamicHashIndex::rehash(size_t new_size) {
  size_t new_bucket_count = new_size;
  if (new_bucket_count <= m_bucket_count_) {
    // do nothing
    return 0;
  }
  auto start = std::chrono::high_resolution_clock::now();
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    k_int32 ret = buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
    if (ret != 0) {
      return ret;
    }
  }
  k_int32 error_code = reserve(new_bucket_count);
  if (error_code != 0) {
    LOG_ERROR("Failed to reserve memory: %ld", new_bucket_count);
    return error_code;
  }
  for (uint32_t rownum = 1; rownum <= meta_data.m_row_count; ++rownum) {
    k_int32 bkt_ins_idx = (row(rownum)->hash_val >> 56) & (n_bkt_instances_ - 1);
    k_int32 bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(row(rownum)->hash_val);
    row(rownum)->next_row = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = rownum;
  }
  m_bucket_count_ = new_bucket_count;
  meta_data.m_bucket_count = m_bucket_count_;
  auto end = std::chrono::high_resolution_clock::now();
  auto ins_dur = std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  return 0;
}

// mmap use file
k_int32 DynamicHashIndex::reserve(size_t n) {
  if (mode_ == 0) {
    // in-memory reserve
    size_t new_capacity = n * meta_data.m_record_size;
    if (meta_data.m_capacity < new_capacity) {
      // check if it's still < threshold
      if (new_capacity < data_size_threshold_in_memory_) {
        // still use mode 0, simulate memory allocation increase
        char* new_mem_hash = new char[new_capacity]();
        if (nullptr == new_mem_hash) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          LOG_ERROR("new_mem_hash new failed\n");
          return KWENOMEM;
        } else {
          LOG_INFO("in-memory reservation succeeded, new size is %ld.", n);
        }
        memcpy(new_mem_hash, mem_hash_, meta_data.m_capacity);
        delete[] mem_hash_;
        mem_hash_ = new_mem_hash;
        meta_data.m_capacity = new_capacity;
      } else {
        // create mmap, copy in-memory data into mmap files, then delete orginal in-memory data
        mode_ = 1;

        size_t new_file_size = n * meta_data.m_record_size;  // m_bucket_count_ * m_record_size_;
        ErrorInfo err_info;
        // use simple open
        err_info.errcode = MMapFile::openTemp();
        if (err_info.errcode < 0) {
          return err_info.errcode;
        }
        // allocate file size
        err_info.errcode = mremap(new_file_size);
        if (err_info.errcode < 0) {
          return err_info.errcode;
        } else {
          LOG_INFO("a mmap file creation succeeded, new size is %ld.", n);
        }
        // get new mem addr of mmap file
        char* mem_hash_new = addrHash();
        // update meta data
        meta_data.m_file_size = new_file_size;
        // copy in-memory data into mmap 文件
        std::memcpy(mem_hash_new, mem_hash_, meta_data.m_capacity);
        // delete in-memory-data
        delete[] mem_hash_;
        // update mem_hash_
        mem_hash_ = mem_hash_new;
      }
    }
  } else {
    // mmap reserve
    size_t new_file_size = n * meta_data.m_record_size;  // calculate new file size
    if (file_length_ < new_file_size) {
      k_int32 err_code = mremap(new_file_size);
      if (err_code < 0) {
        return err_code;
      } else {
        LOG_INFO("mmap reservation succeeded, new size is %ld.", n);
      }
      mem_hash_ = addrHash();  // update hash mem address
      meta_data.m_file_size = new_file_size;  // update file size in metadata
    }
  }
  return 0;
}

// get bucket value
DynamicHashIndexRowID& DynamicHashBucket::bucketValue(size_t n) {
  return m_mem_bucket_[n];
}

// put is used for unittest
k_int32 DynamicHashIndex::put(const char* key, k_int32 len, const RowIndice& row_indice) {
    DynamicHashCode hash_val = (*hash_func_)(key, len);
    std::pair<bool, size_t> do_rehash = is_need_rehash();
    if (do_rehash.first) {
      k_int32 error_code = rehash(do_rehash.second);
        if (error_code != 0) {
          return error_code;
        }
    }
    // get row num and update row count
    size_t rownum = meta_data.m_row_count + 1;
    meta_data.m_row_count++;
    ++m_element_count_;
    // get bucket instance and index
    size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
    size_t bucket_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
    // find the row position in mmap file and insert data
    DynamicHashIndexData* current_data = row(rownum);
    current_data->hash_val = hash_val;
    current_data->rel_row_indice = row_indice;
    memcpy(keyvalue(rownum), key, len);
    current_data->next_row = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
    // update bucket, point to the new row
    buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
    return 0;
}

// add or update data to hash index
k_int32 DynamicHashIndex::addOrUpdate(const char *key, k_int32 len,
                                      const RowIndice& row_indice,
                                      RowIndice& last_row_indice) {
    DynamicHashCode hash_val = (*hash_func_)(key, len);
    std::pair<bool, size_t> do_rehash = is_need_rehash();
    if (do_rehash.first) {
      k_int32 error_code = rehash(do_rehash.second);
      if (error_code != 0) {
        return error_code;
      }
    }
    // get bucket instance and index of hash table
    size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
    size_t bucket_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
    size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
    // check if data exists in bucket
    if (rownum == 0) {
        rownum = meta_data.m_row_count + 1;
        meta_data.m_row_count++;
        ++m_element_count_;
        // write new data record in mmap
        DynamicHashIndexData* current_data = row(rownum);
        current_data->hash_val = hash_val;
        current_data->next_row = 0;  // new key without linkedlist
        current_data->rel_row_indice = row_indice;
        memcpy(keyvalue(rownum), key, len);
        // update bucket pointer
        buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
        last_row_indice = {0, 0};
    } else {
        size_t first_rownum = rownum;
        // check if key exists
        while (rownum) {
            if (rownum > m_bucket_count_) {
              string log_error;
              size_t i = first_rownum;
              LOG_ERROR("Something bad is happening, m_bucket_count_ is: %ld.", m_bucket_count_);
              while (i && i <= m_bucket_count_) {
                log_error += to_string(i) + ", ";
                i = row(i)->next_row;
                LOG_ERROR("Something bad is happening: %s", log_error.c_str());
              }
              LOG_ERROR("End of error log");
            }
            if (row(rownum)->hash_val == hash_val && this->compare(key, rownum)) {
                last_row_indice = row(rownum)->rel_row_indice;
                row(rownum)->rel_row_indice = row_indice;
                return 0;
            }
            rownum = row(rownum)->next_row;
        }
        // insert new data if there's no matched key
        rownum = meta_data.m_row_count + 1;
        meta_data.m_row_count++;
        ++m_element_count_;
        DynamicHashIndexData* current_data = row(rownum);
        last_row_indice = current_data->rel_row_indice;
        current_data->hash_val = hash_val;
        current_data->rel_row_indice = row_indice;
        memcpy(keyvalue(rownum), key, len);
        current_data->next_row = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
        buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
        last_row_indice = {0, 0};
    }
    return 0;
}

k_int32 DynamicHashIndex::remove() {
  // clean and release bucket list
  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    if (buckets_[i]) {
      delete buckets_[i];
      buckets_[i] = nullptr;
    }
  }
  // call mmap func to remove
  return MMapFile::remove();
}

// get row index from key
k_int32 DynamicHashIndex::get(const char *key, k_int32 len, RowIndice& last_row_indice) {
  DynamicHashCode hash_val = (*hash_func_)(key, len);
  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
  // get init row num from bucket
  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
  // traverse the linkedlist to find key
  while (rownum) {
    // check if cur hash val is matched and compare keys
    if (row(rownum)->hash_val == hash_val && this->compare(key, rownum)) {
      last_row_indice = row(rownum)->rel_row_indice;
      return 0;
    }
    // find next
    rownum = row(rownum)->next_row;
  }
  return -1;
}

void DynamicHashIndex::printHashTable() {
  // Implementation for debugging
}

uint64_t DynamicHashIndex::getLSN() const {
  return meta_data.m_lsn;
}

void DynamicHashIndex::setLSN(uint64_t lsn) {
  meta_data.m_lsn = lsn;
}

std::pair<bool, size_t> DynamicHashIndex::is_need_rehash() {
  if (m_element_count_ >= m_bucket_count_) {
    return std::make_pair(true, m_bucket_count_*2);
  }
  return std::make_pair(false, 0);
}
