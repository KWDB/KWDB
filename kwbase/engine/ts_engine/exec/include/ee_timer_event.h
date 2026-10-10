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
#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "kwdb_type.h"

#define TIMER_TICK_MAX_LEVEL 3
#define TIMER_TICK_MSECONDS_LEVEL1 5
#define TIMER_TICK_MSECONDS_LEVEL2 1000
#define TIMER_TICK_MSECONDS_LEVEL3 60000
#define TIMER_TICK_SIZE_LEVEL1 4096
#define TIMER_TICK_SIZE_LEVEL2 1024
#define TIMER_TICK_SIZE_LEVEL3 1024


namespace kwdbts {

class TimerEvent;
typedef std::shared_ptr<TimerEvent> TimerEventPtr;
/**
 * @brief TimerEventType timer type
 */
enum TimerEventType {
  TE_TIME_POINT,  // Point-in-time tasks
  TE_DURATION,    // Interval tasks
  TE_CRON         // cron plan
};

typedef k_int64 k_time_point;

/**
 * @brief Timed events
 *
 */
class TimerEvent {
  friend class TimerEventPool;

 public:
  TimerEvent() = default;

  /**
   * @brief Construct a new Timer Event object
   * @param time_point
   */
  explicit TimerEvent(k_time_point time_point, TimerEventType type)
      : time_point_(time_point), type_(type), end_time_(0), stop_(KFALSE) {}
  /**
   * @brief Construct a new Timer Event object
   * @param duration
   */
  explicit TimerEvent(k_uint32 duration)
      : time_point_(0),
        type_(TimerEventType::TE_DURATION),
        end_time_(0),
        duration_(duration),
        stop_(KFALSE) {
    // next time  cur + interval
    time_point_ = TimerEvent::GetMonotonicMs();
    time_point_ = time_point_ + duration_;
  }
  explicit TimerEvent(k_uint32 duration, k_time_point time_point)
      : time_point_(0),
        type_(TimerEventType::TE_DURATION),
        end_time_(0),
        duration_(duration),
        stop_(KFALSE) {
    // next time  cur + interval
    time_point_ = time_point;
    if (time_point_ <= 0) {
      time_point_ = TimerEvent::GetMonotonicMs();
    }
    time_point_ = time_point_ + duration_;
  }

  /**
   * @brief Construct a new Timer Event object
   *
   */
  TimerEvent(const TimerEvent&) = default;

  /**
   * @brief op=
   *
   * @return TimerEvent&
   */
  TimerEvent& operator=(const TimerEvent&) = default;

  /**
   * @brief Construct a new Timer Event object
   *
   */
  TimerEvent(TimerEvent&&) = default;

  /**
   * @brief op=
   *
   * @return TimerEvent&
   */
  TimerEvent& operator=(TimerEvent&&) = default;

  /**
   * @brief Destroy the Timer Event object
   *
   */
  virtual ~TimerEvent() {
    stop_ = KTRUE;
  }

  /**
   * @brief expire
   * @param[in] pool_exit Whether to stop
   * @return KStatus
   */
  inline KStatus OnExpire(const k_bool& pool_exit);

  const k_time_point& GetTimePoint() const { return time_point_; }

  const k_uint32& GetDuration() const { return duration_; }

  inline TimerEventType GetType() const { return type_; }
  inline void SetType(const TimerEventType &type) {  type_ = type; }
  /**
   * @brief update time
   * If there is no point in time available, an error is returned
   */
  inline KStatus UpdateTimePoint();

  /**
   * @brief now+sec
   *
   * @param[in] sec
   * @param[out] time_point
   */
  static void SecondsAfter(const uint64_t& sec, k_time_point* time_point);

  /**
   * @brief get timestamp
   * @param[out] time_point
   */
  static void GetNowTimestamp(k_time_point* time_point);

  /**
   * @brief ms
  */
  static k_time_point GetMonotonicMs();

  /**
   * @brief ns
  */
  static k_time_point GetMonotonicNs();

  /**
   * @brief Triggers a task execution function
   * @return true
   * @return false
   */
  virtual KStatus TimeRun() { return KStatus::SUCCESS; }

  /**
   * @brief stop task
   * @return true  Multiple calls only return true the first time, and false
   * after that
   * @return false
   */
  k_bool StopEvent();

  /**
   * @brief Whether to stop
   * @return true  stoped
   * @return false
   */
  k_bool IsStop();

  void SetEndTime(const k_time_point& end_time);

  void SetTimePoint(const k_time_point& time_point);

  inline void SetComputingTime(k_time_point computing_time) { computing_time_ = computing_time; }
  k_time_point GetComputingTime() { return computing_time_; }

 private:
  /**
   * @brief Timed time
   *
   */
  k_time_point time_point_{0};
  k_time_point computing_time_{0};
  /**
   * @brief type：interval
   *
   */
  TimerEventType type_{TimerEventType::TE_TIME_POINT};
  /**
   * @brief end time,You can set an end time for interval tasks
   *
   */
  k_time_point end_time_{0};
  /**
   * @brief interval time 
   *
   */
  k_uint32 duration_{0};
  /**
   * @brief Whether to stop
   *
   */
  std::atomic<k_bool> stop_{KFALSE};

 public:
  TimerEventPtr next;
  TimerEventPtr prev;
  k_uint16 tick;
  k_uint16 slot;
};


/**
 * @brief time event pool
 *
 */
class TimerEventPool {
 public:
  /**
   * @brief Construct a new Timer Event Pool object
   *
   * @param max_count length 
   */
  explicit TimerEventPool(const k_uint32& max_count);

  /**
   * @brief Init function
  */
  KStatus Init();
  /**
   * @brief add timed task
   *
   * @param te event 
   * @return
   */
  void PushTimeEvent(const TimerEventPtr& te);
  /**
   * @brief execute func
   *
   */
  void Run();
  /**
   * @brief stop
   *
   */
  void Stop();
  /**
   * @brief Destroy the Timer Event Pool object
   *
   */
  ~TimerEventPool();
  /**
   * @brief start
   *
   */
  void Start();

 private:
  /**
   * @brief Get the tasks at the current point in time
   *
   * @param[in] stop whether to stop
   * @param[in] events Expired tasks
   * @return KStatus
   */
  KStatus GetReady(k_bool* stop, TimerEventPtr *events);
  /**
   * @brief clear event
   *
   */
  void CleanAllEvent();

 private:
  struct timer_tick {
    pthread_mutex_t mutex;
    k_uint64 next_scan_time;
    k_uint32 ms;
    k_uint32 index;
    k_uint16 size;
    TimerEventPtr* slots{nullptr};

#if defined(__GNUC__) && (__GNUC__ < 8)
    timer_tick(k_uint32 ms_val, k_uint16 size_val)
        : mutex(PTHREAD_MUTEX_INITIALIZER), next_scan_time(0), ms(ms_val), index(0), size(size_val), slots(nullptr) {}
#endif
  };
  timer_tick ticks_[TIMER_TICK_MAX_LEVEL] = {
    {.ms = TIMER_TICK_MSECONDS_LEVEL1, .size = TIMER_TICK_SIZE_LEVEL1},
    {.ms = TIMER_TICK_MSECONDS_LEVEL2, .size = TIMER_TICK_SIZE_LEVEL2},
    {.ms = TIMER_TICK_MSECONDS_LEVEL3, .size = TIMER_TICK_SIZE_LEVEL3},
  };

  /**
   * @brief constrant lock
   *
   */
  mutable std::mutex mut_;
  /**
   * @brief max
   *
   */
  k_uint32 max_time_event_count_{0};

  std::atomic<k_uint32> current_size_{0};
  /**
   * @brief condition var，You need to wait when the queue reaches its upper
   * limit
   *
   */
  std::condition_variable not_fill_cv_;
  /**
   * @brief condition var
   *
   */
  std::condition_variable at_least_one_cv_;
  /**
   * @brief stop
   *
   */
  bool stop_{false};
  /**
   * @brief thread
   *
   */
  std::thread thr_;
};
};  // namespace kwdbts
