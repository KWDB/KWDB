// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#include "ee_timer_event.h"

#include <unistd.h>

namespace kwdbts {

k_bool TimerEvent::StopEvent() {
  k_bool expect = KFALSE;
  // atomic operation
  return stop_.compare_exchange_strong(expect, KTRUE);
}

k_bool TimerEvent::IsStop() { return stop_; }

KStatus TimerEvent::OnExpire(const k_bool& pool_exit) {
  // if stop ,return false
  if (IsStop()) {
    return KStatus::FAIL;
  }
  // exit
  if (pool_exit) {
    return KStatus::FAIL;
  }
  if (end_time_ > 0 && time_point_ > end_time_) {
    return KStatus::FAIL;
  }
  SetComputingTime(time_point_);
  TimeRun();
  return KStatus::SUCCESS;
}

KStatus TimerEvent::UpdateTimePoint() {
  if (GetType() != TimerEventType::TE_DURATION) {
    return KStatus::FAIL;
  }
  time_point_ = time_point_ + duration_;
  // end_time_ = 0 has no endtime;>0 has endtime,judge timeout
  if (end_time_ > 0 && time_point_ > end_time_) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

void TimerEvent::SetEndTime(const k_time_point& end_time) {
  end_time_ = end_time;
}

void TimerEvent::SetTimePoint(const k_time_point& time_point) {
  if (time_point > time_point_) {
    time_point_ = time_point;
  }
}

void TimerEvent::SecondsAfter(const k_uint64& sec, k_time_point* time_point) {
  *time_point = TimerEvent::GetMonotonicMs() / 1000 + sec;
}

void TimerEvent::GetNowTimestamp(k_time_point* time_point) {
  *time_point = TimerEvent::GetMonotonicMs() / 1000;
}

k_time_point TimerEvent::GetMonotonicMs() {
  /* clock_gettime() is specified in POSIX.1b (1993).  Even so, some systems
   * did not support this until much later.  CLOCK_MONOTONIC is technically
   * optional and may not be supported - but it appears to be universal.
   * If this is not supported, provide a system-specific alternate version.  
   * */
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ((k_uint64)ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

k_time_point TimerEvent::GetMonotonicNs() {
  /* clock_gettime() is specified in POSIX.1b (1993).  Even so, some systems
   * did not support this until much later.  CLOCK_MONOTONIC is technically
   * optional and may not be supported - but it appears to be universal.
   * If this is not supported, provide a system-specific alternate version.
   * */
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ((k_uint64)ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

TimerEventPool::TimerEventPool(const k_uint32& max_count)
    : max_time_event_count_(max_count), stop_(KTRUE) {}

KStatus TimerEventPool::Init() {
  KStatus ret = KStatus::FAIL;
  k_uint64 now = TimerEvent::GetMonotonicMs();
  for (int i = 0; i < TIMER_TICK_MAX_LEVEL; i++) {
    timer_tick* tick = ticks_ + i;
    if (pthread_mutex_init(&tick->mutex, NULL) != 0) {
      return ret;
    }
    tick->next_scan_time = now + tick->ms;
    tick->index = 0;
    tick->slots = KNEW TimerEventPtr[tick->size];
    if (tick->slots == nullptr) {
      return ret;
    }

    for (k_uint16 idx = 0; idx < tick->size; idx++) {
      tick->slots[idx] = nullptr;
    }
  }
  ret = KStatus::SUCCESS;
  return ret;
}

void TimerEventPool::PushTimeEvent(const TimerEventPtr& te) {
  {
    std::unique_lock<std::mutex> guard(mut_);
    // wait
    not_fill_cv_.wait(guard, [this]() -> bool {
      return this->current_size_ < this->max_time_event_count_;
    });
  }
  k_int32 delay = static_cast<k_int32>((te->GetTimePoint() - TimerEvent::GetMonotonicMs()));
  te->tick = TIMER_TICK_MAX_LEVEL - 1;
  for (uint8_t i = 0; i < TIMER_TICK_MAX_LEVEL; i++) {
    timer_tick* tick = ticks_ + i;
    if (delay < static_cast<k_int32>(tick->ms * tick->size)) {
      te->tick = i;
      break;
    }
  }

  timer_tick* tick = ticks_ + te->tick;
  te->prev = nullptr;
  pthread_mutex_lock(&tick->mutex);

  uint32_t idx = 0;
  if (te->GetTimePoint() > tick->next_scan_time) {
    // adjust delay according to next scan time of this tick
    // so that the timer is not fired earlier than desired.
    delay = (k_int32)(te->GetTimePoint() - tick->next_scan_time);
    idx = (delay + tick->ms - 1) / tick->ms;
  }

  te->slot = (k_uint16)((tick->index + idx + 1) % tick->size);
  TimerEventPtr p = tick->slots[te->slot];
  tick->slots[te->slot] = te;
  te->next = p;
  if (p != nullptr) {
    p->prev = te;
  }
  current_size_.fetch_add(1);
  pthread_mutex_unlock(&tick->mutex);
  at_least_one_cv_.notify_all();
}

void TimerEventPool::Run() {
  try {
    while (true) {
      if (stop_) {
        break;
      }
      k_bool stop = KFALSE;
      // get ready task
      TimerEventPtr events;
      GetReady(&stop, &events);

      while (events && !stop_) {
        TimerEventPtr next = events->next;
        events->prev = nullptr;
        events->next = nullptr;
        KStatus more = events->OnExpire(stop);
        // if true,update timepoint and put it into event pool.
        if (more && events->UpdateTimePoint()) {
          PushTimeEvent(events);
        }
        events = next;
        if (events) {
          events->prev = nullptr;
        }
      }
    }
  } catch (...) {
  }
}
void TimerEventPool::Stop() {
  // wake one time
  if (stop_) {
    return;
  }
  stop_ = KTRUE;
  // wake run
  at_least_one_cv_.notify_all();
  thr_.join();
  CleanAllEvent();
}

TimerEventPool::~TimerEventPool() { Stop(); }
void TimerEventPool::Start() {
  thr_ = std::thread([this]() {
    this->Run();
  });
  stop_ = KFALSE;
}

KStatus TimerEventPool::GetReady(k_bool* stop,
                                 TimerEventPtr *events) {
  k_time_point now = TimerEvent::GetMonotonicMs();
  for (int i = 0; i < TIMER_TICK_MAX_LEVEL; ++i) {
    timer_tick* tick = ticks_ + i;
    while (now >= tick->next_scan_time) {
      pthread_mutex_lock(&tick->mutex);
      tick->index = (tick->index + 1) % tick->size;
      TimerEventPtr te = tick->slots[tick->index];
      while (te) {
        TimerEventPtr next = te->next;
        if (now < te->GetTimePoint()) {
          te = next;
          continue;
        }

        if (te->prev == nullptr) {
          tick->slots[tick->index] = next;
          if (next != nullptr) {
            next->prev = nullptr;
          }
        } else {
          te->prev->next = next;
          if (next != nullptr) {
            next->prev = te->prev;
          }
        }
        te->tick = TIMER_TICK_MAX_LEVEL;
        // add to temporary expire list
        te->next = *events;
        te->prev = nullptr;
        if ((*events) != nullptr) {
          (*events)->prev = te;
        }
        *events = te;
        te = next;
        current_size_.fetch_sub(1);
      }
      tick->next_scan_time += tick->ms;
      pthread_mutex_unlock(&tick->mutex);
    }
  }
  *stop = stop_;
  // three cases
  // case1 :
  if ((*events) != nullptr) {
    not_fill_cv_.notify_all();
    return KStatus::SUCCESS;
  }
  if (stop_) {
    return KStatus::FAIL;
  }
  std::unique_lock guard(mut_);
  // case 2 ： wait until at least one time_event enters the queue
  if (current_size_ < 1) {
    at_least_one_cv_.wait_for(guard, std::chrono::seconds(2));
  } else {
    // case3 ：
    // The maximum waiting time for the most recent time_event that needs to be
    // awakened is, otherwise it will be awakened midway (with other time_events
    // entering the queue)
    std::chrono::milliseconds dt =
        std::chrono::milliseconds(TIMER_TICK_MSECONDS_LEVEL1);
    at_least_one_cv_.wait_for(guard, dt);
  }
  *stop = stop_;
  return KStatus::SUCCESS;
}

void TimerEventPool::CleanAllEvent() {
  for (k_int32 i = 0; i < TIMER_TICK_MAX_LEVEL; i++) {
    timer_tick* tick = ticks_ + i;
    if (tick->slots == nullptr) {
      continue;
    }
    pthread_mutex_lock(&tick->mutex);
    for (k_int32 j = 0; j < tick->size; ++j) {
      TimerEventPtr slot = tick->slots[j];
      while (slot) {
        TimerEventPtr next = slot->next;
        if (slot->next) {
          slot->next = nullptr;
        }
        slot->OnExpire(KTRUE);
        slot = next;
        if (slot) {
          slot->prev = nullptr;
        }
      }
      if (tick->slots[j]) {
        tick->slots[j] = nullptr;
      }
    }
    pthread_mutex_unlock(&tick->mutex);
    pthread_mutex_destroy(&tick->mutex);
    if (tick->slots) {
      delete[] tick->slots;
      tick->slots = nullptr;
    }
  }
}
}  // namespace kwdbts
