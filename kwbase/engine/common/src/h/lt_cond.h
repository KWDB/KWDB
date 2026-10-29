#pragma once
#include<iostream>
#include <mutex>
#include <atomic>
#include "lt_latch.h"

#define COND_TYPES                              \
ENUM_MEM(COND_ID_NONE),                         \
ENUM_MEM(COND_ID_TAG_REF_COUNT_COND),           \
ENUM_MEM(COND_ID_PARTITION_REF_COUNT_COND),     \
ENUM_MEM(COND_ID_LAST_COND),                    \

enum cond_id_t {
#define ENUM_MEM(x) x
  COND_TYPES
#undef ENUM_MEM
  COND_ID_MAX = COND_ID_LAST_COND
};

struct Kcond_stat {
  KLatchUseage m_wait_stat;
  uint64_t     m_signal_count;
  uint64_t     m_broadcast_count;
public:
  inline void reset(void) {
    m_wait_stat.reset();
    m_signal_count = 0;
    m_broadcast_count = 0;
  }
};


struct KLS_cond {
  cond_id_t m_cnd_id;

  Kcond_stat m_cond_stat;

public:
  KLS_cond() = delete;
  KLS_cond(cond_id_t id) : m_cnd_id(id) {
    m_cond_stat.reset();
  }
  ~KLS_cond() {
  }
};

struct  KCond_t {

  pthread_cond_t  m_cond;

  KLS_cond*  m_kls;

public:
  KCond_t() = delete;
  KCond_t(cond_id_t id) {
    pthread_cond_init(&m_cond, NULL);
#ifdef ENABLE_LATCH_DEBUG
    m_kls = new KLS_cond(id);
#else
    m_kls = nullptr;
#endif
  }

  ~KCond_t() {
    pthread_cond_destroy(&m_cond);
    delete m_kls;
  }
};

int kw_cond_wait(KCond_t* cond, KLatch* mutex, const char* call_file,const uint32_t line);
int kw_cond_timedwait(KCond_t* cond, KLatch* mutex, const struct timespec *abstime, const char* call_file, const uint32_t line);
int kw_cond_signal(KCond_t* cond);
int kw_cond_broadcast(KCond_t* cond);

#define KW_COND_WAIT(C,M) \
    kw_cond_wait(C, M, __FILE__, __LINE__)

#define KW_COND_TIMEDWAIT(C,M,W) \
    kw_cond_timedwait(C, M, W, __FILE__, __LINE__)

#define KW_COND_SIGNAL(C)  kw_cond_signal(C)

#define KW_COND_BROADCAST(C)  kw_cond_broadcast(C)

void debug_condwait_print(FILE* file);