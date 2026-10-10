#include <assert.h>
#include <chrono>
#include "lg_api.h"
#include "lt_rw_latch.h"


KRWLatch_stat global_rw_lock_stats[RWLATCH_ID_LAST_RWLATCH];

static const char* all_rw_lock_info[] = {
#define ENUM_MEM(x) #x
    RW_LATCH_TYPES
#undef ENUM_MEM
};

struct KRWLatch_locker_state {

  std::thread::id m_thread;  // Current thread id

  KTimestamp m_start;  // Timer start

  KLS_rwlock*  m_rw_lock;  // rw latch

};

void startRWLockRdWait(KRWLatch_locker_state* rwlock_state, KLS_rwlock* rwlock);

void endRWLockRdWait(KRWLatch_locker_state* rwlock_state);

void startRWLockWRWait(KRWLatch_locker_state* rwlock_state, KLS_rwlock* rwlock);

void endRWLockWRWait(KRWLatch_locker_state* rwlock_state);

void startRWLockUnlock(KLS_rwlock* rw_lock);

KRWLatch::KRWLatch(rwlatch_id_t id) {
  pthread_rwlock_init(&m_rw_lock, NULL);
#ifdef ENABLE_LATCH_DEBUG
  m_kls = new KLS_rwlock(id);
#else
  m_kls = nullptr;
#endif  
}

int KRWLatch::rdLock(const char* file_name, uint32_t line) {

  if (m_kls != nullptr) {
    KRWLatch_locker_state rwlock_state;
    startRWLockRdWait(&rwlock_state, m_kls);
  
    // rd lock
    auto r = pthread_rwlock_rdlock(&m_rw_lock);
    endRWLockRdWait(&rwlock_state);
    return r;
  }
  return pthread_rwlock_rdlock(&m_rw_lock);
}

int KRWLatch::wrLock(const char* file_name, uint32_t line) {

  if (m_kls != nullptr) {
    KRWLatch_locker_state rwlock_state;
    startRWLockWRWait(&rwlock_state, m_kls);
  
    // write lock
    auto r = pthread_rwlock_wrlock(&m_rw_lock);
    endRWLockWRWait(&rwlock_state);
    return r;
  }
  return pthread_rwlock_wrlock(&m_rw_lock);
}

int KRWLatch::Unlock(const char* file_name, uint32_t line) {

  if (m_kls != nullptr) {
    startRWLockUnlock(m_kls);
  }
  // unlock
  return pthread_rwlock_unlock(&m_rw_lock);
}

void startRWLockRdWait(KRWLatch_locker_state* rwlock_state, KLS_rwlock* rwlock) {
  assert(rwlock_state != nullptr);
  rwlock_state->m_start = get_timevalue();
  rwlock_state->m_thread = std::this_thread::get_id();
  rwlock_state->m_rw_lock = rwlock;
}

void endRWLockRdWait(KRWLatch_locker_state* rwlock_state) {
  assert(rwlock_state != nullptr);

  if (!rwlock_state->m_rw_lock) {
    return;
  }
  KLS_rwlock* rwlock = rwlock_state->m_rw_lock;
  auto end_time = get_timevalue();
  
  KRWLatch_stat *wait = &global_rw_lock_stats[rwlock->m_rw_id];
  auto wait_time = (end_time >= rwlock_state->m_start) ? (end_time - rwlock_state->m_start) : 0;
  wait->m_wait_stat.aggregate(wait_time);
  rwlock->m_rwlatch_stat.m_wait_stat.aggregate(wait_time);
  if (rwlock->m_readers == 0) {
    rwlock->m_last_read = end_time;
  }
  rwlock->m_writer = false;
  rwlock->m_readers++;

}

void startRWLockWRWait(KRWLatch_locker_state* rwlock_state, KLS_rwlock* rwlock) {
  assert(rwlock_state != nullptr);
  rwlock_state->m_start = get_timevalue();
  rwlock_state->m_thread = std::this_thread::get_id();
  rwlock_state->m_rw_lock = rwlock;
}

void endRWLockWRWait(KRWLatch_locker_state* rwlock_state) {
  assert(rwlock_state != nullptr);

  if (!rwlock_state->m_rw_lock) {
    return;
  }
  KLS_rwlock* rwlock = rwlock_state->m_rw_lock;
  auto end_time = get_timevalue();
  
  KRWLatch_stat *wait = &global_rw_lock_stats[rwlock->m_rw_id];
  auto wait_time = (end_time >= rwlock_state->m_start) ? (end_time - rwlock_state->m_start) : 0;
  wait->m_wait_stat.aggregate(wait_time);
  rwlock->m_rwlatch_stat.m_wait_stat.aggregate(wait_time);

  rwlock->m_writer = true;
  rwlock->m_last_written = end_time;
  rwlock->m_readers = 0;
  rwlock->m_last_read = 0;
}

void startRWLockUnlock(KLS_rwlock* rw_lock) {

  bool last_writer= false;
  bool last_reader= false;

  if (!rw_lock) {
    return;
  }

  if (rw_lock->m_writer) {
    // thread safe
    last_writer = true;
    rw_lock->m_writer = false;
    rw_lock->m_readers = 0;
  }else if (likely(rw_lock->m_readers > 0)) {
    if (--(rw_lock->m_readers) == 0) {
      last_reader = true;
    }
  }
  uint64_t locked_time;
  KRWLatch_stat *wait = &global_rw_lock_stats[rw_lock->m_rw_id];
  if (last_writer) {
    locked_time = get_timevalue() - rw_lock->m_last_written;
    wait->m_wrlock_stat.aggregate(locked_time);
    rw_lock->m_rwlatch_stat.m_wrlock_stat.aggregate(locked_time);
  }else if (last_reader) {
    locked_time = get_timevalue() - rw_lock->m_last_read;
    wait->m_rdlock_stat.aggregate(locked_time);
    rw_lock->m_rwlatch_stat.m_rdlock_stat.aggregate(locked_time);
  } else {
    wait->m_rdlock_stat.m_count.fetch_add(1);
  }
}

void debug_rwlock_print(FILE* file) {
#ifdef ENABLE_LATCH_DEBUG
  uint32_t count = static_cast<uint32_t>(RWLATCH_ID_MAX);
  for(uint32_t idx = 1; idx < count; ++idx) {
    fprintf(file, "\n%s\n",all_rw_lock_info[idx]);
    fprintf(file, " 1)waited stats:\n");
    fprintf(file, "%s", global_rw_lock_stats[idx].m_wait_stat.print().c_str());
    fprintf(file, " 2)wrlocked stats:\n");
    fprintf(file, "%s", global_rw_lock_stats[idx].m_wrlock_stat.print().c_str());
    fprintf(file, " 3)rdlocked stats:\n");
    fprintf(file, "%s", global_rw_lock_stats[idx].m_rdlock_stat.print().c_str());
  }
  fprintf(file, "\n");
#else
  fprintf(file, "not define ENABLE_LATCH_DEBUG \n");
#endif
}