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

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "cm_assert.h"
#include "ee_global.h"
#include "ee_task.h"

#define EE_TIMER_EVENT_POOL_SIZE 10240
#define EE_ENABLE_PARALLEL 1
// wait for about 10 minutes before exit for temporary thread
#define TEMPORARY_THREAD_MAX_WAIT_TIMES       300

namespace kwdbts {

class TimerEventPool;
class PipelineTaskPoller;
/**
 * @brief the scheduling class
 */
class ExecPool {
 private:
  /**
   * @brief time_event_pool_ timer
   *
   */
  TimerEventPool *timer_event_pool_{nullptr};

  PipelineTaskPoller *pipeline_task_poller_{nullptr};

  typedef std::deque<ExecTaskPtr> TaskQueue;
  /**
   * @brief task_queue_ task queue
   */
  TaskQueue task_queue_;
  /**
   * @brief tq_num_ queue length limit
   */
  k_uint32 tq_num_;
  /**
   * @brief Concurrency lock, which controls the concurrency of thread pool
   * tasks
   */
  mutable std::mutex lock_;
  /**
   * @brief Condition variable, which needs to wait after the queue reaches the
   * upper limit
   *
   */
  std::condition_variable not_fill_cv_;
  /*
   * @brief Conditional variables, when the task queue is empty waiting, when
   * there is a task to send a signal
   */
  std::condition_variable wait_cond_;
  /*
   * @brief Conditional variables, Waiters wake up when
   * threads_num_ and threads_starting_ are both 0. 
   */
  std::condition_variable no_threads_cond_;
  /*
   * @brief Stop Thread Pool
   */
  bool is_tp_stop_;
  /*
   * @brief The flag of initialization, prevent duplicate initialization
   */
  bool is_init_{false};
  /*
   * @brief Number of threads
   *
   */
  k_uint32 threads_num_{0};
  /*
   * @brief Number of threads which are starting.
   */
  k_uint32 threads_starting_{0};
  /*
   * @brief Min Number of threads.
   */
  k_uint32 min_threads_{0};
  /*
   * @brief Max Number of threads.
   */
  k_uint32 max_threads_{0};
  /*
   * @brief Number of active threads.
   */
  k_uint32 active_threads_{0};

  std::atomic<k_int32> total_threads_{0};

  /**
   * @brief Number of wait times before exit for temporary thread
   */
  int temporary_thread_wait_times_{TEMPORARY_THREAD_MAX_WAIT_TIMES};

 public:
  /**
   * @brief Constructor
   * @param[in]  tq_num         the max length of the tq
   * @param[in]  tp_size        the thread num
   * @return
   */
  ExecPool(k_uint32 tq_num, k_uint32 tp_size);

  /**
   * @brief Destructors
   */
  ~ExecPool();

  ExecPool(const ExecPool &) = delete;
  ExecPool& operator=(const ExecPool &) = delete;

  /**
   * @brief add task
   *
   * @param task_ptr
   * @param no_check : if true, don't check if the queue is filled
   */
  KStatus PushTask(ExecTaskPtr task_ptr, bool no_check = false);

  /**
   * @brief set the number of wait times before exit for temporary thread
   */
  void SetTemporaryThreadWaitTimes(int temporary_thread_wait_times) {
    temporary_thread_wait_times_ = temporary_thread_wait_times;
  }

  KStatus PushBlockedTask(ExecTaskPtr task_ptr);
  /**
  * @brief add timed tasks
  *
  * @param event_ptr
  */
  void PushTimeEvent(TimerEventPtr event_ptr);
  /**
   * @brief Threads are scheduled to execute tasks
   */
  void Routine(void *);
  inline void CreateThread();
  /**
   * @brief gets the number of idle threads
   */
  k_uint32 GetWaitThreadNum() const;
  k_uint32 GetTotalThreadNum() const {
    std::unique_lock l(lock_);
    return threads_num_ + threads_starting_;
  }
  k_uint32 GetWaitThreadNum(k_uint32 dop);
  k_uint32 ReleaseThreadNum(k_uint32 dop);
  bool IsActive() { return active_threads_ > 0 || task_queue_.size() > 0; }
  KStatus Init(kwdbContext_p ctx);
  void Stop();
  static ExecPool &GetInstance(k_uint32 tq_num = 1024, k_uint32 tp_size = 10) {
    static ExecPool instance(tq_num, tp_size);
    return instance;
  }
  k_bool IsFull();
  k_bool IsInited();
  k_bool IsStopped();

  k_bool IsEmpty() { return task_queue_.empty(); }

 public:
  std::string db_path_;
};  // ExecPool
};  // namespace kwdbts
