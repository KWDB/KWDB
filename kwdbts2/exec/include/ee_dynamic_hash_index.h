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
#include <utility>
#include "kwdb_type.h"
#include "ts_object.h"
#include "data_type.h"
#include "mmap/mmap_file.h"
#include "ts_object_error.h"
#include "lg_api.h"
#include "lt_rw_latch.h"

namespace kwdbts {

typedef uint32_t DynamicHashIndexRowID;
typedef uint64_t DynamicHashCode;

#define INVALID_TABLE_VERSION_ID 0
// data size threshold to be kept in memory, otherwise the data will be flush to disk
#define DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY 256 * 1024 * 1024  // 256M

struct RowIndice {
  uint16_t batch_no;
  uint16_t offset_in_batch;
};

// Data of index file for multiple model processing
struct DynamicHashIndexData {
  DynamicHashCode hash_val;
  DynamicHashIndexRowID next_row;
  RowIndice rel_row_indice;
};

struct DynamicHashIndexMetaData {
  size_t m_file_size;  // Hash index file size
  size_t m_record_size;  // Hash index record size
  uint32_t m_row_count;  // Hash index row count
  size_t m_bucket_count;
  uint64_t m_lsn;
  size_t m_capacity;     // Capacity of mem_bash_ for in-memo mode
};

// HashBucket for multiple model processing
class DynamicHashBucket {
 private:
  DynamicHashIndexRowID* m_mem_bucket_{nullptr};
  size_t m_bucket_count_;

 public:
  DynamicHashBucket();
  ~DynamicHashBucket();

  k_int32 init(size_t bucket_count = 8);
  size_t get_bucket_index(const size_t& key) const;
  k_int32 resize(size_t new_bucket_count);
  DynamicHashIndexRowID& bucketValue(size_t n);
};

typedef k_uint64 (*DynamicHashFunc) (const char *data, k_int32 len);

// hash index for HashTagScan to build dynamic hash index for multiple model processing
class DynamicHashIndex : public MMapFile {
 protected:
  DynamicHashFunc hash_func_;

 private:
  char* mem_hash_;  // mmap mapping data
  std::vector<DynamicHashBucket*> buckets_;
  size_t n_bkt_instances_;
  size_t m_element_count_;
  size_t m_bucket_count_;
  k_int32 m_key_len_;
  DynamicHashIndexMetaData meta_data{0};
  size_t m_record_size_{0};
  int64_t data_size_threshold_in_memory_;
  k_int32 mode_{0};  // 0: in-memory, 1: mmap

  inline DynamicHashIndexData* row(size_t n) const {
    return reinterpret_cast<DynamicHashIndexData*>(
        mem_hash_ + (n - 1) * meta_data.m_record_size);
  }

  inline char* keyvalue(size_t n) const {
    return mem_hash_ + (n - 1) * meta_data.m_record_size +
           sizeof(DynamicHashIndexData);
  }

  inline char* addrHash() const {
    return reinterpret_cast<char *>((intptr_t) mem_);
  }

  std::pair<bool, size_t> is_need_rehash();
  k_int32 rehash(size_t new_size);

 public:
  DynamicHashIndex();
  ~DynamicHashIndex();

  k_int32 open(const std::string &path);

  size_t size() const;
  k_int32 reserve(size_t n);
  k_int32 put(const char *key, k_int32 len, const RowIndice& row_indice);
  k_int32 addOrUpdate(const char *key, k_int32 len, const RowIndice& row_indice, RowIndice& last_row_indice);

  k_int32 remove();
  k_int32 init(k_int32 key_len, size_t bkt_instances = 1, size_t per_bkt_count = 8,
  k_int64 data_size_threshold_in_memory = DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY);

  inline bool compare(const char* key, size_t src_row) {
    return (memcmp(keyvalue(src_row), key, m_key_len_) == 0);
  }

  k_int32 get(const char *key, k_int32 len, RowIndice& last_row_indice);
  void printHashTable();

  k_uint64 getLSN() const;
  void setLSN(uint64_t lsn);
  void setDrop();
  bool isDroped() const;

  // Public interfaces for testing
  void publicRehash(size_t new_bucket_count) {
    rehash(new_bucket_count);
  }

  DynamicHashIndexData* publicRow(size_t n) const {
    return row(n);
  }
};

};  // namespace kwdbts
