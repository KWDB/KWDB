#include <assert.h>
#include <chrono>
#include "lg_api.h"
#include "lt_cond.h"

Kcond_stat global_cond_stats[COND_ID_LAST_COND];

static const char* all_cond_info[] = {
#define ENUM_MEM(x) #x
    COND_TYPES
#undef ENUM_MEM
};

struct KCond_locker_state {

  std::thread::id m_thread;  // Current thread id

  KTimestamp m_start;  // Timer start

  KLS_cond*  m_cond;  // cond

  KLS_mutex* m_mutex;  // mutex

};

void startCondWait(KCond_locker_state* cond_state, KLS_cond* cond, KLS_mutex* mutex) {
  assert(cond_state != nullptr);
  cond_state->m_start = get_timevalue();
  cond_state->m_thread = std::this_thread::get_id();
  cond_state->m_mutex = mutex;
  cond_state->m_cond = cond;
}

void endCondWait(KCond_locker_state* cond_state) {
  assert(cond_state != nullptr);
  if (cond_state->m_cond == nullptr) {
    return;
  }
  KLS_cond* kcond = cond_state->m_cond;
  auto end_time = get_timevalue();

  Kcond_stat *wait = &global_cond_stats[kcond->m_cnd_id];
  auto wait_time = (end_time >= cond_state->m_start) ? (end_time - cond_state->m_start) : 0;
  wait->m_wait_stat.aggregate(wait_time);
  kcond->m_cond_stat.m_wait_stat.aggregate(wait_time);
}

void startCondSignal(KLS_cond* cond) {
  assert(cond);
  Kcond_stat *wait = &global_cond_stats[cond->m_cnd_id];
  wait->m_signal_count++;
  cond->m_cond_stat.m_signal_count++;
}

void startCondBroadcast(KLS_cond* cond) {
  assert(cond);
  Kcond_stat *wait = &global_cond_stats[cond->m_cnd_id];
  wait->m_broadcast_count++;
  cond->m_cond_stat.m_broadcast_count++;
}

int kw_cond_wait(KCond_t* cond, KLatch* mutex, const char* call_file, const uint32_t line) {
  if (cond->m_kls != nullptr) {
    KCond_locker_state cond_state;
    startCondWait(&cond_state, cond->m_kls, mutex->m_kls);

    auto ret = pthread_cond_wait(&cond->m_cond, &mutex->m_native_mutex);

   endCondWait(&cond_state);
   return ret;
  }
  return pthread_cond_wait(&cond->m_cond, &mutex->m_native_mutex);
}

int kw_cond_timedwait(KCond_t* cond, KLatch* mutex, const struct timespec *abstime, const char* call_file, const uint32_t line) {
  if (cond->m_kls != nullptr) {
    KCond_locker_state cond_state;
    startCondWait(&cond_state, cond->m_kls, mutex->m_kls);

    auto ret = pthread_cond_timedwait(&cond->m_cond, &mutex->m_native_mutex, abstime);
    endCondWait(&cond_state);
    return ret;
  }
  return pthread_cond_timedwait(&cond->m_cond, &mutex->m_native_mutex, abstime);
}

int kw_cond_signal(KCond_t* cond) {
  if (cond->m_kls != nullptr) {
    startCondSignal(cond->m_kls);
  }
  return pthread_cond_signal(&cond->m_cond);
}

int kw_cond_broadcast(KCond_t* cond) {
  if (cond->m_kls != nullptr) {
    startCondBroadcast(cond->m_kls);
  }
  return pthread_cond_broadcast(&cond->m_cond);
}

void debug_condwait_print(FILE* file) {
#ifdef ENABLE_LATCH_DEBUG
  uint32_t count = static_cast<uint32_t>(COND_ID_MAX);
  // fprintf(file, "\n mutex waited stats: \n");
  for(uint32_t idx = 1; idx < count; ++idx) {
    fprintf(file, "\n%s\n",all_cond_info[idx]);
    fprintf(file, " signal count:%lu  broadcast cnt:%lu \n",
            global_cond_stats[idx].m_signal_count, global_cond_stats[idx].m_broadcast_count);
    fprintf(file, " waited stats:\n");
    fprintf(file, "%s", global_cond_stats[idx].m_wait_stat.print().c_str());

  }
  fprintf(file, "\n");
#else
  fprintf(file, "not define ENABLE_LATCH_DEBUG \n");
#endif
}