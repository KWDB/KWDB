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
#include<iostream>
#include <mutex>
#include <atomic>
#include <stdio.h>
#include <thread>
// #include "ts_common.h"

#define likely(cond) __builtin_expect(!!(cond), 1)
#define unlikely(cond) __builtin_expect(!!(cond), 0)

#define LATCH_TYPES                                 \
ENUM_MEM(LATCH_ID_NONE),                            \
ENUM_MEM(LATCH_ID_TAG_TABLE_MUTEX),                 \
ENUM_MEM(LATCH_ID_HASH_INDEX_MUTEX),                \
ENUM_MEM(LATCH_ID_TAG_STRING_FILE_MUTEX),           \
ENUM_MEM(LATCH_ID_METRICS_STRING_FILE_MUTEX),       \
ENUM_MEM(LATCH_ID_TEMP_TABLE_STRING_FILE_MUTEX),    \
ENUM_MEM(LATCH_ID_TSTABLE_ITERATOR_MUTEX),          \
ENUM_MEM(LATCH_ID_MMAP_ENTITY_META_MUTEX),          \
ENUM_MEM(LATCH_ID_MMAP_PARTITION_TABLE_SEGMENTS_MUTEX), \
ENUM_MEM(LATCH_ID_PARTITION_TABLE_MUTEX), \
ENUM_MEM(LATCH_ID_ERROR_INFO_MUTEX),                \
ENUM_MEM(LATCH_ID_OBJECT_MANAGER_FACTORY_MUTEX),    \
ENUM_MEM(LATCH_ID_CREATE_TABLE_MUTEX),              \
ENUM_MEM(LATCH_ID_BIGOBJECT_CONFIG_MUTEX),          \
ENUM_MEM(LATCH_ID_TSENTITY_GROUP_MUTEX),            \
ENUM_MEM(LATCH_ID_TSTABLE_SNAPSHOT_MUTEX),          \
ENUM_MEM(LATCH_ID_WALMGR_META_MUTEX),               \
ENUM_MEM(LATCH_ID_WALFILEMGR_FILE_MUTEX),           \
ENUM_MEM(LATCH_ID_WALBUFMGR_BUF_MUTEX),             \
ENUM_MEM(LATCH_ID_TSSUBENTITY_GROUP_ENTITYS_MUTEX), \
ENUM_MEM(LATCH_ID_TSSUBENTITY_GROUP_MUTEX),         \
ENUM_MEM(LATCH_ID_LOGGED_TSSUBENTITY_GROUP_MUTEX),  \
ENUM_MEM(LATCH_ID_SUBENTITY_GROUP_MANAGER_MUTEX),   \
ENUM_MEM(LATCH_ID_TAG_REF_COUNT_MUTEX),             \
ENUM_MEM(LATCH_ID_PARTITION_REF_COUNT_MUTEX),       \
ENUM_MEM(LATCH_ID_TSTABLE_CACHE_LOCK),              \
ENUM_MEM(LATCH_ID_PARTITION_LRU_CACHE_MANAGER_CAPACITY_THREAD_LOCK), \
ENUM_MEM(LATCH_ID_PARTITION_LRU_CACHE_MANAGER_CAPACITY_QUEUE_LOCK), \
ENUM_MEM(LATCH_ID_TSTABLE_DELETE_DATA_LOCK),        \
ENUM_MEM(LATCH_ID_TAG_TABLE_METADATA_MUTEX),        \
ENUM_MEM(LATCH_ID_PARTITION_VACUUM_WRITE_MUTEX),    \
ENUM_MEM(LATCH_ID_PARTITION_VACUUM_DELETE_MUTEX),   \
ENUM_MEM(LATCH_ID_LAST_MUTEX),                      \
ENUM_MEM(LATCH_ID_ENTITY_ITEM_MUTEX),               \
ENUM_MEM(LATCH_ID_COMPRESS_INFO_INIT_MUTEX),        \

enum latch_id_t {
#define ENUM_MEM(x) x
  LATCH_TYPES
#undef ENUM_MEM
  LATCH_ID_MAX = LATCH_ID_LAST_MUTEX
};

#define LATCH_INFO_KEY(N) { #N }


struct KLatchUseage {
  std::atomic<uint64_t> m_count;  // Count of values

  std::atomic<uint64_t> m_sum;  // Sum of values

  std::atomic<uint64_t> m_min;  // Minimum value

  std::atomic<uint64_t> m_max;  // Maximum value
  
  KLatchUseage();
  void reset(void);
  void aggregate(uint64_t value);
  std::string print();
};

struct Klatch_stat {
  KLatchUseage m_wait_stat;
  KLatchUseage m_lock_stat;
};

using KTimestamp = int64_t;

struct KLS_mutex {
  latch_id_t m_id;

  Klatch_stat m_latch_stat;

  KTimestamp   m_last_locked;
public:
  KLS_mutex() = delete;
  KLS_mutex(latch_id_t id) : m_id(id) {
    m_last_locked = 0;
    m_latch_stat.m_lock_stat.reset();
    m_latch_stat.m_wait_stat.reset();
  }
  ~KLS_mutex() {
  }
};

struct  KLatch {
  pthread_mutex_t m_native_mutex;

  KLS_mutex*  m_kls;

public:
  KLatch() = delete;

  KLatch(latch_id_t id);

  ~KLatch() {
    pthread_mutex_destroy(&m_native_mutex);
    delete m_kls;
  }

  int Lock(const char* file_name, uint32_t line);

  int Unlock(const char* file_name, uint32_t line);
};

#define MUTEX_LOCK(M) (M)->Lock(   \
                           __FILE__, __LINE__)

#define MUTEX_UNLOCK(M) (M)->Unlock(   \
                           __FILE__, __LINE__)


extern KTimestamp get_timevalue();

void debug_latch_print(FILE* file);