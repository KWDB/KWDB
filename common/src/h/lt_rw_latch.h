#pragma once
#include<iostream>
#include <mutex>
#include <atomic>
#include "lt_latch.h"

#define RW_LATCH_TYPES                                \
ENUM_MEM(RWLATCH_ID_NONE),                            \
ENUM_MEM(RWLATCH_ID_HASH_INDEX_FILE_RWLOCK),          \
ENUM_MEM(RWLATCH_ID_HASH_INDEX_BUCKET_RWLOCK),        \
ENUM_MEM(RWLATCH_ID_TAG_TABLE_RWLOCK),                \
ENUM_MEM(RWLATCH_ID_TAG_STRING_FILE_RWLOCK),          \
ENUM_MEM(RWLATCH_ID_METRICS_STRING_FILE_RWLOCK),      \
ENUM_MEM(RWLATCH_ID_TEMP_TABLE_STRING_FILE_RWLOCK),   \
ENUM_MEM(RWLATCH_ID_MMAP_COLUMN_BIGTABLE_RWLOCK),     \
ENUM_MEM(RWLATCH_ID_MMAP_ENTITY_META_RWLOCK),         \
ENUM_MEM(RWLATCH_ID_MMAP_BIGTABLE_RWLOCK),            \
ENUM_MEM(RWLATCH_ID_BIGTABLE_RWLOCK),                 \
ENUM_MEM(RWLATCH_ID_MMAP_SEGMENT_TABLE_RWLOCK),         \
ENUM_MEM(RWLATCH_ID_TS_TABLE_ENTITYGRPS_RWLOCK),      \
ENUM_MEM(RWLATCH_ID_TS_ENTITY_GROUP_DROP_RWLOCK),     \
ENUM_MEM(RWLATCH_ID_LOGGED_TSTABLE_RWLOCK),           \
ENUM_MEM(RWLATCH_ID_TS_SUB_ENTITY_GROUP_RWLOCK),      \
ENUM_MEM(RWLATCH_ID_MMAP_PARTITION_TABLE_RWLOCK),     \
ENUM_MEM(RWLATCH_ID_SUB_ENTITY_GROUP_MANAGER_RWLOCK), \
ENUM_MEM(RWLATCH_ID_MMAP_METRICS_TABLE_RWLOCK),       \
ENUM_MEM(RWLATCH_ID_SUBENTITY_GROUP_MANAGER_RWLOCK),  \
ENUM_MEM(RWLATCH_ID_PARTITION_LRU_CACHE_RWLOCK),      \
ENUM_MEM(RWLATCH_ID_TSTABLE_LRU_CACHE_RWLOCK),        \
ENUM_MEM(RWLATCH_ID_PARTITION_LRU_CACHE_MANAGER_RWLOCK), \
ENUM_MEM(RWLATCH_ID_MMAP_ROOT_TABLE_RWLOCK),          \
ENUM_MEM(RWLATCH_ID_TAG_TABLE_VERSION_MGR_RWLOCK),    \
ENUM_MEM(RWLATCH_ID_TAG_TABLE_PARTITION_MGR_RWLOCK),  \
ENUM_MEM(RWLATCH_ID_LAST_RWLATCH),                    \
ENUM_MEM(RWLATCH_ID_NTAG_INDEXES_RWLATCH),            \

enum rwlatch_id_t {
#define ENUM_MEM(x) x
  RW_LATCH_TYPES
#undef ENUM_MEM
  RWLATCH_ID_MAX = RWLATCH_ID_LAST_RWLATCH
};

#define RWLATCH_INFO_KEY(N) { #N }

struct KRWLatch_stat {
  KLatchUseage   m_wait_stat;
  KLatchUseage   m_rdlock_stat;
  KLatchUseage   m_wrlock_stat;
};

struct KLS_rwlock {
  rwlatch_id_t m_rw_id;

  KRWLatch_stat m_rwlatch_stat;

  KTimestamp     m_last_written;

  KTimestamp     m_last_read;

  uint16_t       m_readers;

  bool           m_writer;
public:
  KLS_rwlock() = delete;
  KLS_rwlock(rwlatch_id_t id) : m_rw_id(id) {
    m_last_written = 0;
    m_last_read = 0;
    m_readers = 0;
    m_writer = false;
    m_rwlatch_stat.m_rdlock_stat.reset();
    m_rwlatch_stat.m_wait_stat.reset();
    m_rwlatch_stat.m_wrlock_stat.reset();
  }
  ~KLS_rwlock() {
  }
};

struct  KRWLatch {

  pthread_rwlock_t  m_rw_lock;

  KLS_rwlock*  m_kls;

public:
  KRWLatch() = delete;
  KRWLatch(rwlatch_id_t id);

  ~KRWLatch() {
    pthread_rwlock_destroy(&m_rw_lock);
    delete m_kls;
  }

  int rdLock(const char* file_name, uint32_t line);

  int wrLock(const char* file_name, uint32_t line);

  int Unlock(const char* file_name, uint32_t line);
};

#define RW_LATCH_S_LOCK(M) (M)->rdLock(  \
                        __FILE__, __LINE__)

#define RW_LATCH_X_LOCK(M) (M)->wrLock(  \
                        __FILE__, __LINE__)

#define RW_LATCH_UNLOCK(M) (M)->Unlock(  \
                        __FILE__, __LINE__)

void debug_rwlock_print(FILE* file);
