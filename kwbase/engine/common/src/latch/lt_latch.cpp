#include <assert.h>
#include <chrono>
#include <sstream>
#include <climits>
#include "lg_api.h"
#include "lt_latch.h"

Klatch_stat global_mutex_stats[LATCH_ID_LAST_MUTEX];

static const char* all_mutex_info[] = {
#define ENUM_MEM(x) #x
    LATCH_TYPES
#undef ENUM_MEM
};

struct KMutex_locker_state {

  std::thread::id m_thread;  // Current thread id

  KTimestamp m_start;  // Timer start

  KLS_mutex*  m_mutex;  // mutex

};

KLatchUseage::KLatchUseage() {
  m_count= 0;
  m_sum= 0;
  m_min= ULLONG_MAX;
  m_max= 0;
}

void KLatchUseage::reset(void) {
  m_count= 0;
  m_sum= 0;
  m_min= ULLONG_MAX;
  m_max= 0;
}

void KLatchUseage::aggregate(uint64_t value) {
  m_count.fetch_add(1);
  m_sum.fetch_add(value);
  if (unlikely(m_min.load() > value)) {
    m_min.exchange(value);
  }
  if (unlikely(m_max.load() < value)) {
    m_max.exchange(value);
  }
}

std::string KLatchUseage::print() {
  std::stringstream ss;
  ss << "  m_count: " << m_count.load() << " times\n";
  ss << "  m_sum:   " << m_sum.load()   << " us\n";
  ss << "  m_min:   " << m_min.load()   << " us\n";
  ss << "  m_max:   " << m_max.load()   << " us\n";
  return ss.str();
}

KTimestamp get_timevalue() {
  // return (std::chrono::high_resolution_clock::now());
  return (std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count());
}

void startMutexWait(KMutex_locker_state* latch_state, KLS_mutex* mutex) {
  assert(latch_state != nullptr);
  latch_state->m_start = get_timevalue();
  latch_state->m_thread = std::this_thread::get_id();
  latch_state->m_mutex = mutex;
}

void endMutexWait(KMutex_locker_state* latch_state) {
  assert(latch_state != nullptr);
  if (latch_state->m_mutex == nullptr) {
    return;
  }
  KLS_mutex* kmutex = latch_state->m_mutex;
  auto end_time = get_timevalue();
  kmutex->m_last_locked = end_time;

  Klatch_stat *wait = &global_mutex_stats[kmutex->m_id];
  auto wait_time = (end_time >= latch_state->m_start) ? (end_time - latch_state->m_start) : 0;
  wait->m_wait_stat.aggregate(wait_time);
  kmutex->m_latch_stat.m_wait_stat.aggregate(wait_time);
}

void startMutexUnlock(KLS_mutex* kmutex) {
  assert(kmutex);
  auto end_time = get_timevalue();
  Klatch_stat *wait = &global_mutex_stats[kmutex->m_id];
  auto locked_time = (end_time >= kmutex->m_last_locked) ? (end_time - kmutex->m_last_locked) : 0;
  wait->m_lock_stat.aggregate(locked_time);
  kmutex->m_latch_stat.m_lock_stat.aggregate(locked_time);
}

KLatch::KLatch(latch_id_t id) {
  pthread_mutex_init(&m_native_mutex, nullptr);
#ifdef ENABLE_LATCH_DEBUG
  m_kls = new KLS_mutex(id);
#else
  m_kls = nullptr;
#endif
}

int KLatch::Lock(const char* file_name, uint32_t line) {

  if (m_kls != nullptr) {
    KMutex_locker_state latch_state;
    startMutexWait(&latch_state, m_kls);
    // mutex lock
    auto r = pthread_mutex_lock(&m_native_mutex);
    endMutexWait(&latch_state);
    return r;
  }
  return pthread_mutex_lock(&m_native_mutex);
}

int KLatch::Unlock(const char* file_name, uint32_t line) {
  if (m_kls != nullptr) {
    startMutexUnlock(m_kls);
  }
  return pthread_mutex_unlock(&m_native_mutex);
}

void debug_latch_print(FILE* file) {
#ifdef ENABLE_LATCH_DEBUG
  uint32_t count = static_cast<uint32_t>(LATCH_ID_MAX);
  // fprintf(file, "\n mutex waited stats: \n");
  for(uint32_t idx = 1; idx < count; ++idx) {
    fprintf(file, "\n%s\n",all_mutex_info[idx]);
    fprintf(file, " 1) waited stats:\n");
    fprintf(file, "%s", global_mutex_stats[idx].m_wait_stat.print().c_str());
    fprintf(file, " 2) locked stats:\n");
    fprintf(file, "%s", global_mutex_stats[idx].m_lock_stat.print().c_str());
  }
  fprintf(file, "\n");
#else
  fprintf(file, "not define ENABLE_LATCH_DEBUG \n");
#endif
}