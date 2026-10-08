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

#include "th_kwdb_dynamic_thread_pool.h"
#include <memory>
#include <string>
#include <iostream>
#include "lg_api.h"
#include "cm_assert.h"
#include "cm_config.h"
#include "cm_fault_injection.h"
#include "er_api.h"

#ifndef K_DO_NOT_SHIP
  using kwdbts::FaultInjectorRunTime;
  using kwdbts::FaultInjector;
  using kwdbts::InjectorType;
  using kwdbts::FAULT_TH_FAULT_INJECTION_NUM;
  using kwdbts::FAULT_TH_FAULT_INJECTION_DELAY;
#endif

#ifdef KMALLOC_DEBUGGER
    extern kwdbts::KStatus StopMemUsageServer();
#endif

namespace kwdbts {

#ifdef WITH_TESTS
KWDBDynamicThreadPool::Ptr KWDBDynamicThreadPool::instance_ = nullptr;
std::mutex KWDBDynamicThreadPool::mutex_;
#endif

KWDBWrappedThread::KWDBWrappedThread()
    : user_routine(nullptr),
      arg(nullptr),
      pool(nullptr),
      m_info(),
      m_thread(nullptr),
      m_id(0),
      m_cond_var(),
      m_mutex(),
      m_thread_stop(false),
      m_is_cancel(false),
      thread_status(ThreadStatus::INIT) {}

//
// After testing, it is found that the number of single-process threads should not exceed 31975,
// otherwise the system will force the process to end.
//
KWDBDynamicThreadPool::KWDBDynamicThreadPool(k_uint16 max_threads_num, k_uint16 min_threads_num)
    : threads_num_(0), max_threads_num_(max_threads_num < 31000 ? max_threads_num : 31000),
      min_threads_num_(min_threads_num < max_threads_num ? min_threads_num : max_threads_num),
      error_threads_(), wait_threads_(), all_threads_(max_threads_num_) {
  for (k_int32 i = 0; i < max_threads_num_; ++i) {
    all_threads_[i].m_id = i + thread_id_offset_;
    all_threads_[i].pool = this;
    all_threads_[i].m_thread_stop = true;
  }
  pool_status_ = UNINIT;
}

KWDBDynamicThreadPool::~KWDBDynamicThreadPool() {
  Assert(this->IsStop());
}

// to be removed.
KStatus KWDBDynamicThreadPool::Init(k_uint16 min_threads_num, kwdbContext_p ctx) {
  return KStatus::SUCCESS;
}

KStatus KWDBDynamicThreadPool::InitImplicitly() {
  kwdbContext_p ctx = ContextManager::GetThreadContext();
  ctx->owner = ContextOwner::ThreadPool;
  ctx->label = ThreadLabel::Master;
  TH_TRACE_1("init the thread pool.");
  if (pool_status_ != PoolStatus::UNINIT) {
    LOG_ERROR("thread pool cannot init twice!");
    TH_TRACE_4("thread pool cannot init twice!");
    PUSH_ERR_0(SY_TH_THREAD_INIT_ERR);
    return KStatus::FAIL;
  }
  INJECT_DATA_FAULT(FAULT_TH_FAULT_INJECTION_NUM, min_threads_num_, 65535, nullptr);
  if (min_threads_num_ > max_threads_num_) {
    LOG_WARN("The number of threads(:%d) initialized exceeded the maximum allowed value(:%d).",
        min_threads_num_, max_threads_num_);
  }
  threads_num_.store(min_threads_num_);
  for (k_int32 i = 0; i < max_threads_num_; ++i) {
    KWDBWrappedThread &kwdb_thread = all_threads_[i];
    auto id = i + thread_id_offset_;
    kwdb_thread.m_context.thread_id = id;
    kwdb_thread.m_context.owner = ContextOwner::ThreadPool;
    kwdb_thread.m_context.label = ThreadLabel::Slave;

    if (i < min_threads_num_) {
      if (startThread(kwdb_thread) == KStatus::FAIL) {
        error_mutex_.lock();
        kwdb_thread.pool->error_threads_.push_back(kwdb_thread.m_id);
        error_mutex_.unlock();
        LOG_ERROR("failed to start thread during INIT!");
        TH_TRACE_4("failed to start thread(id: %ld) during INIT!", kwdb_thread.m_id);
        PUSH_ERR_1(SY_TH_THREAD_START_ERR, kwdb_thread.m_id);
      } else {
        kwdb_thread.pool->wait_mutex_.lock();
        kwdb_thread.pool->wait_threads_.push_back(kwdb_thread.m_id);
        kwdb_thread.pool->wait_mutex_.unlock();
      }
    }
  }

  pool_status_.store(PoolStatus::RUNNING);
  TH_TRACE_1("The thread pool has been successfully initialized.");

  return KStatus::SUCCESS;
}

KStatus KWDBDynamicThreadPool::JoinThread(KThreadID thread_id, k_uint16 wait_microseconds) {
  INJECT_DATA_FAULT(FAULT_TH_FAULT_INJECTION_NUM, thread_id, 1, nullptr);
  k_int32 ind = thread_id - thread_id_offset_;
  auto ctx = ContextManager::GetThreadContext();
  TH_TRACE_3("The thread(id:%ld) is about to join. wait: %d us", thread_id, wait_microseconds);

  if (ind < 0 || ind >= threads_num_.load()) {
    LOG_ERROR("The thread id of the join is out of range.");
    TH_TRACE_4("The thread id of the join is out of range.");
    PUSH_ERR_0(SY_TH_THREAD_ID_ERR);
    return KStatus::FAIL;
  }
  KWDBWrappedThread &kwdb_thread = all_threads_[ind];
  std::unique_lock<std::mutex> locker(kwdb_thread.m_mutex);
  if (kwdb_thread.thread_status.load() == KWDBWrappedThread::ERROR) {
    LOG_ERROR("this thread(id: %ld) failed during startup, cannot join.", kwdb_thread.m_id);
    TH_TRACE_4("this thread(id: %ld) failed during startup, cannot join.", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else if (kwdb_thread.m_thread_stop) {
    LOG_ERROR("this thread is stopped, cannot join!");
    TH_TRACE_4("this thread is stopped, cannot join!");
    PUSH_ERR_1(SY_TH_THREAD_STATUS_STOP, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else if (kwdb_thread.thread_status.load() == KWDBWrappedThread::WAIT) {
    TH_TRACE_4("The task executed by the thread(id:%ld) has completed.", thread_id);
    return KStatus::SUCCESS;
  } else {
    if (0 == wait_microseconds) {
      kwdb_thread.m_cond_var.wait(locker);
    } else {
      kwdb_thread.m_cond_var.wait_for(locker, std::chrono::milliseconds(wait_microseconds));
    }
    TH_TRACE_4("Thread(id:%ld) join succeeded.", thread_id);
    return KStatus::SUCCESS;
  }
}

KStatus KWDBDynamicThreadPool::CancelThread(KThreadID thread_id) {
  INJECT_DATA_FAULT(FAULT_TH_FAULT_INJECTION_NUM, thread_id, 1, nullptr);
  k_int32 ind = thread_id - thread_id_offset_;
  auto ctx = ContextManager::GetThreadContext();

  TH_TRACE_3("The thread(id:%ld) is about to be canceled.", thread_id);
  if (ind < 0 || ind >= threads_num_.load()) {
    LOG_ERROR("The cancelled thread id out of range.");
    TH_TRACE_4("The cancelled thread id(%ld) is out of range!", thread_id);
    PUSH_ERR_0(SY_TH_THREAD_ID_ERR);
    return KStatus::FAIL;
  }
  KWDBWrappedThread &kwdb_thread = all_threads_[ind];
  if (kwdb_thread.thread_status.load() == KWDBWrappedThread::ERROR) {
    LOG_ERROR("this thread(id: %ld) failed during startup, cannot cancel.", kwdb_thread.m_id);
    TH_TRACE_4("this thread(id: %ld) failed during startup, cannot cancel.", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else if (kwdb_thread.m_thread_stop) {
    LOG_ERROR("this thread is stopped, cannot cancel!");
    TH_TRACE_4("this thread(id: %ld) is stopped, cannot cancel!", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_STOP, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else {
    kwdb_thread.m_is_cancel.store(true);
    TH_TRACE_4("Canceling thread(id: %ld) succeeded.", kwdb_thread.m_id);
    return KStatus::SUCCESS;
  }
}

k_bool KWDBDynamicThreadPool::IsCancel() {
  if (pool_status_.load() == PoolStatus::STOPPING || pool_status_.load() == PoolStatus::STOPPED) {
    return KTRUE;
  }
  return isThreadCancelOrStop();
}

k_bool KWDBDynamicThreadPool::isThreadCancelOrStop() {
  auto ctx = ContextManager::GetThreadContext();
  Assert(ctx->owner == ContextOwner::ThreadPool);
  k_uint64 thread_id = ContextManager::GetThreadId();
  k_int32 ind = thread_id - thread_id_offset_;
  if (ind < 0 || ind >= threads_num_.load()) {
    LOG_ERROR("the current thread id is out of range.");
    TH_TRACE_4("The current thread was not created by the thread pool!");
    PUSH_ERR_0(SY_TH_THREAD_ID_ERR);
    return KTRUE;
  }
  KWDBWrappedThread &kwdb_thread = all_threads_[thread_id - thread_id_offset_];
  k_bool is_true = {false};

  is_true = kwdb_thread.m_is_cancel || kwdb_thread.m_thread_stop;
  if (is_true) {
    TH_TRACE_4("Terminates an ongoing task, thread id: %ld.", kwdb_thread.m_id);
    kwdb_thread.thread_status.store(KWDBWrappedThread::CANCELLING);
  }
  return is_true;
}

KStatus KWDBDynamicThreadPool::Stop() {
  auto ctx = ContextManager::GetThreadContext();
  TH_TRACE_1("stop the thread pool.");
  if (this->IsStop()) {
    LOG_ERROR("thread pool is stopped, cannot stop twice!");
    TH_TRACE_4("thread pool is stopped, cannot stop twice!");
    PUSH_ERR_0(SY_TH_POOL_STOP_ERR)
    return KStatus::FAIL;
  }
  pool_status_.store(PoolStatus::STOPPING);
  // TODO(liyongqiang): k_free Whether thread pool pool_context can be used.
  for (k_int32 i = 0; i < max_threads_num_; ++i) {
    KWDBWrappedThread &kwdb_thread = all_threads_[i];
    if (!kwdb_thread.m_thread_stop.load()) {
      if (stopThread(kwdb_thread) == KStatus::FAIL) {
        LOG_ERROR("failed to stop thread!");
        TH_TRACE_4("failed to stop thread! id = %ld", kwdb_thread.m_id);
        PUSH_ERR_1(SY_TH_THREAD_STOP_ERR, kwdb_thread.m_id);
      }
    }
  }
  if (clear() == KStatus::SUCCESS) {
    TH_TRACE_1("The thread pool stopped successfully.");
    pool_status_.store(PoolStatus::STOPPED);
    return KStatus::SUCCESS;
  }
  TH_TRACE_1("Failed to stop the thread pool.");
  return KStatus::FAIL;
}

KStatus KWDBDynamicThreadPool::clear() {
  wait_threads_.clear();
  error_threads_.clear();
  all_threads_.clear();
  return KStatus::SUCCESS;
}

k_bool KWDBDynamicThreadPool::IsStop() {
  return pool_status_.load() == PoolStatus::STOPPING || pool_status_.load() == PoolStatus::STOPPED;
}

KThreadID
KWDBDynamicThreadPool::ApplyThread(std::function<void(void *)> &&job,
                                  void *arg,
                                  KWDBOperatorInfo *kwdb_operator_info) {
  KThreadID thread_id = 0;
  Assert(pool_status_ != PoolStatus::UNINIT);
  auto ctx = ContextManager::GetThreadContext();
  Assert(ctx != nullptr);
  INJECT_DELAY_FAULT(FAULT_TH_FAULT_INJECTION_DELAY, 0);
  while (true) {
    // wait_threads_ is not empty, in this case there is a free thread, just return the thread id
    wait_mutex_.lock();
    if (wait_threads_.size() != 0) {
      k_uint16 thread_id_point = wait_threads_.back();
      wait_threads_.remove(thread_id_point);
      thread_id = thread_id_point;
      wait_mutex_.unlock();
      break;
    }
    wait_mutex_.unlock();
    // Prevent threads_num_ from exceeding the maximum number of threads when applying for
    // thread resources concurrently.
    k_uint16 cur_threads_num = threads_num_.load();
    k_bool exchanged = false;
    while (cur_threads_num < max_threads_num_ && !exchanged) {
      // Increments the number of existing threads in the thread pool so that
      // threads are created in startThread traversal
      exchanged = threads_num_.compare_exchange_strong(cur_threads_num, cur_threads_num + 1);
      if (!exchanged) {
        cur_threads_num = threads_num_.load();
      }
    }
    if (exchanged) {
      KWDBWrappedThread &kwdb_thread = all_threads_[cur_threads_num];
      if (startThread(kwdb_thread) == KStatus::SUCCESS) {
        thread_id = cur_threads_num + thread_id_offset_;
      } else {
        error_mutex_.lock();
        error_threads_.push_back(cur_threads_num + thread_id_offset_);
        error_mutex_.unlock();
      }
    }

    break;
  }

  if (thread_id == 0) {
    error_mutex_.lock();
    if (error_threads_.size() > 0) {
      k_uint16 error_id_point = error_threads_.back();
      error_threads_.remove(error_id_point);
      if (checkThreadStatus(error_id_point - thread_id_offset_)) {
        thread_id = error_id_point;
      }

      if (thread_id == 0) {
        error_threads_.push_back(error_id_point);
      }
    }
    error_mutex_.unlock();
  }

  if (thread_id == 0 && threads_num_.load() == max_threads_num_) {
    // If wait_threads_ is empty and the number of threads in the thread pool
    // reaches the max_thread_num, thread acquisition fails and 0 is returned
    PUSH_ERR_0(OT_TH_THREAD_POOL_FULL_ERR);
    LOG_INFO("all threads are busy!");
    TH_TRACE_4("all threads are busy!");
    return 0;
  }

  // The thread id is subtracted from thread_id_offset_ to get its footer in all_threads_
  k_int32 ind = thread_id - thread_id_offset_;
  if (ind < 0 || ind > threads_num_) {
    LOG_ERROR("get wrong thread id!");
    TH_TRACE_4("get wrong thread id! id = %ld", thread_id);
    PUSH_ERR_0(SY_TH_THREAD_ID_ERR);
    return 0;
  }

  KWDBWrappedThread &kwdb_thread = all_threads_[ind];
  if (kwdb_thread.m_thread_stop.load()) {
    LOG_ERROR("thread is stopped!");
    TH_TRACE_4("thread(id: %ld) is stopped!", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_STOP, kwdb_thread.m_id);
    return 0;
  } else if (kwdb_thread.thread_status.load() == KWDBWrappedThread::ERROR) {
    LOG_ERROR("this thread(id: %ld) failed during startup, cannot apply.", kwdb_thread.m_id);
    TH_TRACE_4("this thread(id: %ld) failed during startup, cannot apply.", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return 0;
  } else {
    kwdb_thread.m_info.SetOperatorName(kwdb_operator_info->GetOperatorName());
    kwdb_thread.m_info.SetOperatorOwner(kwdb_operator_info->GetOperatorOwner());
    kwdb_thread.m_info.SetOperatorStatus(kwdb_operator_info->GetOperatorStatus());
    kwdb_thread.m_info.SetOperatorStartTime(
        kwdb_operator_info->GetOperatorStartTime());
    kwdb_thread.m_info.SetProxyThreadInfo(
        kwdb_operator_info->GetProxyThreadInfo());
    // set user function
    kwdb_thread.user_routine = job;
    // set user function required parameters
    kwdb_thread.arg = arg;
    TH_TRACE_4("apply thread success! id = %ld", kwdb_thread.m_id);
    // lock and then unlock to ensure that the thread state is set to running
    kwdb_thread.m_mutex.lock();
    kwdb_thread.thread_status.store(KWDBWrappedThread::RUNNING);
    kwdb_thread.m_mutex.unlock();
    // Free all threads to start executing tasks
    kwdb_thread.m_cond_var.notify_all();
    return kwdb_thread.m_id;
  }
}

k_bool KWDBDynamicThreadPool::checkThreadStatus(k_int32 thread_index) {
  Assert(thread_index >= 0 && thread_index + 1 <= max_threads_num_);

  auto ctx = ContextManager::GetThreadContext();
  TH_TRACE_3("Check thread status.");
  if (pool_status_.load() == PoolStatus::STOPPING || pool_status_.load() == PoolStatus::STOPPED) {
    LOG_ERROR("thread pool is stopped, cannot check thread status!");
    TH_TRACE_4("thread pool is stopped, cannot check thread status!");
    PUSH_ERR_0(SY_TH_THREAD_POOL_STOP);
    return KFALSE;
  }

  k_bool result = KTRUE;

  KWDBWrappedThread &kwdb_thread = all_threads_[thread_index];
  if (thread_index < threads_num_.load()) {
    // In multi-user concurrency scenarios, keep the thread state consistent.
    if (kwdb_thread.thread_status.load() == KWDBWrappedThread::INIT) {
      if (startThread(kwdb_thread) == KStatus::FAIL) {
        LOG_ERROR("failed to start thread in INIT state!");
        TH_TRACE_4("failed to start thread(id: %ld) in INIT state!", kwdb_thread.m_id);
        PUSH_ERR_1(SY_TH_THREAD_START_ERR, kwdb_thread.m_id);
        result = KFALSE;
      }
    } else if (kwdb_thread.thread_status.load() == KWDBWrappedThread::ERROR) {
      auto tmp_status = KWDBWrappedThread::ERROR;
      if (kwdb_thread.thread_status.compare_exchange_strong(tmp_status, KWDBWrappedThread::INIT)) {
        if (startThread(kwdb_thread) == KStatus::FAIL) {
          LOG_ERROR("failed to start thread in ERROR state!");
          TH_TRACE_4("failed to start thread(id: %ld) in ERROR state!", kwdb_thread.m_id);
          PUSH_ERR_1(SY_TH_THREAD_START_ERR, kwdb_thread.m_id);
          result = KFALSE;
        }
      }
    }
  } else {
    if (kwdb_thread.thread_status.load() == KWDBWrappedThread::WAIT) {
      if (stopThread(kwdb_thread) == KStatus::FAIL) {
        LOG_ERROR("failed to stop thread!");
        TH_TRACE_4("failed to stop thread! id = %ld", kwdb_thread.m_id);
        PUSH_ERR_1(SY_TH_THREAD_STOP_ERR, kwdb_thread.m_id);
        result = KFALSE;
      }
    }
  }

  TH_TRACE_4("The thread status check is complete.");

  return result;
}

KStatus KWDBDynamicThreadPool::stopThread(KWDBWrappedThread &kwdb_thread) {
  auto ctx = ContextManager::GetThreadContext();
  TH_TRACE_3("The thread is about to end, id = %ld.", kwdb_thread.m_id);
  if (pool_status_.load() != PoolStatus::STOPPING && pool_status_.load() != PoolStatus::STOPPED) {
    LOG_ERROR("thread pool is not stopped, cannot stop thread!");
    TH_TRACE_4("thread pool is not stopped, cannot stop thread! id = %ld", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else if (kwdb_thread.thread_status.load() == KWDBWrappedThread::INIT) {
    LOG_ERROR("thread is uninitialized, cannot stop!");
    TH_TRACE_4("thread is uninitialized, cannot stop! id = %ld", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return KStatus::FAIL;
  } else if (kwdb_thread.thread_status.load() == KWDBWrappedThread::ERROR) {
    LOG_ERROR("thread is in error status without running of any system thread.");
    TH_TRACE_4("thread is in error status without running of any system thread. id = %ld", kwdb_thread.m_id);
    PUSH_ERR_1(SY_TH_THREAD_STATUS_ERR, kwdb_thread.m_id);
    return KStatus::SUCCESS;
  }

  kwdb_thread.m_thread_stop.store(true);
  kwdb_thread.pool->wait_mutex_.lock();
  kwdb_thread.pool->wait_threads_.remove(kwdb_thread.m_id);
  kwdb_thread.pool->wait_mutex_.unlock();
  kwdb_thread.m_cond_var.notify_all();
  if (kwdb_thread.m_thread->joinable()) {
    TH_TRACE_4("Thread(id: %ld) is joining.", kwdb_thread.m_id);
    kwdb_thread.m_thread->join();
  } else {
    LOG_ERROR("failed to stop thread!");
    TH_TRACE_4("failed to stop thread! id = %ld", kwdb_thread.m_id);
    return KStatus::FAIL;
  }
  TH_TRACE_4("Stop the thread, id = %ld.", kwdb_thread.m_id);

  return KStatus::SUCCESS;
}

void *Routine(void *arg) {
  Assert(arg != nullptr);
  KWDBWrappedThread *kwdb_thread = static_cast<KWDBWrappedThread *>(arg);

  Assert(nullptr != kwdb_thread);
#ifdef WITH_TESTS
  ++kwdb_thread->m_telemetry.enter_routine_count;
#endif

  std::unique_lock<std::mutex> locker_init(kwdb_thread->m_mutex);
  while (!kwdb_thread->m_thread_stop.load() && kwdb_thread->thread_status.load() == KWDBWrappedThread::WAIT) {
    kwdb_thread->m_cond_var.wait(locker_init);
  }
  locker_init.unlock();
  // this attribute is to be removed from context
#ifdef WITH_TESTS
  auto id = ContextManager::GetThreadId();
  Assert(id == 0);
  auto ctx = ContextManager::GetThreadCache();
  Assert(ctx == nullptr);
#endif
  ContextManager::SetThreadId((kwdb_thread->m_context).thread_id);
  ContextManager::SetThreadCache(&(kwdb_thread->m_context));

  while (!kwdb_thread->m_thread_stop.load()) {
    if (kwdb_thread->user_routine != nullptr) {
    #ifdef WITH_TESTS
      ++kwdb_thread->m_telemetry.hook_user_routine_count;
    #endif

      // Passing parameters for user_routine
      INJECT_DELAY_FAULT(FAULT_TH_DELAY_EXEC, 0);
      kwdb_thread->user_routine(kwdb_thread->arg);
      kwdb_thread->user_routine = nullptr;
      kwdb_thread->arg = nullptr;
      kwdb_thread->m_info.Reset();
      kwdb_thread->m_context.err_stack.Reset();
      kwdb_thread->m_is_cancel.store(false);
    }

    //
    // this is an approximation to sync with thread application in ApplyThread.
    //
    std::unique_lock<std::mutex> locker_wait(kwdb_thread->m_mutex);
    kwdb_thread->thread_status.store(KWDBWrappedThread::WAIT);
    Assert(nullptr != kwdb_thread && nullptr != kwdb_thread->pool);
    kwdb_thread->pool->wait_mutex_.lock();
    kwdb_thread->pool->wait_threads_.push_back(kwdb_thread->m_id);
    kwdb_thread->pool->wait_mutex_.unlock();
    kwdb_thread->m_cond_var.notify_all();

    // When a thread does not execute a task and is in Wait state, a lock is
    // added to the thread to avoid thread exhaustion

    while (!kwdb_thread->m_thread_stop.load() && kwdb_thread->thread_status.load() == KWDBWrappedThread::WAIT) {
      kwdb_thread->m_cond_var.wait(locker_wait);
    }
    locker_wait.unlock();
  }

  return nullptr;
}

KStatus KWDBDynamicThreadPool::startThread(KWDBWrappedThread &kwdb_thread) {
#ifdef WITH_TESTS
  ++kwdb_thread.m_telemetry.enter_start_thread_count;
#endif

  auto ctx = ContextManager::GetThreadContext();
  TH_TRACE_3("A new thread is about to be created, id = %ld.", kwdb_thread.m_id);
  if (pool_status_.load() == PoolStatus::STOPPING || pool_status_.load() == PoolStatus::STOPPED) {
    kwdb_thread.thread_status.store(KWDBWrappedThread::ERROR);
    LOG_ERROR("thread pool is stopped, cannot start thread!");
    TH_TRACE_4("thread pool is stopped, cannot start thread! id = %ld", kwdb_thread.m_id);
    PUSH_ERR_0(SY_TH_THREAD_POOL_STOP);
    return KStatus::FAIL;
  } else {
    kwdb_thread.m_thread_stop.store(false);
    Assert(kwdb_thread.thread_status.load() == KWDBWrappedThread::INIT);
    kwdb_thread.thread_status.store(KWDBWrappedThread::WAIT);
    try {
      kwdb_thread.m_thread = std::make_shared<std::thread>(&Routine, &kwdb_thread);
      } catch (std::system_error) {
      LOG_WARN("An system exception occurred while creating a new thread！");
      TH_TRACE_4("An system exception occurred while creating a new thread! id = %ld", kwdb_thread.m_id);
      kwdb_thread.thread_status.store(KWDBWrappedThread::ERROR);
      return KStatus::FAIL;
    } catch (...) {
      LOG_WARN("An unknown exception occurred while creating a new thread！");
      TH_TRACE_4("An unknown exception occurred while creating a new thread! id = %ld", kwdb_thread.m_id);
      kwdb_thread.thread_status.store(KWDBWrappedThread::ERROR);
      return KStatus::FAIL;
    }

    if (nullptr != kwdb_thread.m_thread) {
      Assert(kwdb_thread.thread_status.load() == KWDBWrappedThread::WAIT);
      TH_TRACE_4("Create a new thread, id = %ld.", kwdb_thread.m_id);
      return KStatus::SUCCESS;
    } else {
      kwdb_thread.thread_status.store(KWDBWrappedThread::ERROR);
      LOG_ERROR("failed to create thread!");
      TH_TRACE_4("failed to create thread!");
      return KStatus::FAIL;
    }
  }
}

void KWDBDynamicThreadPool::GetOperatorInfo(KWDBOperatorInfo *kwdb_operator_info) {
  Assert(ContextManager::GetThreadCache() != nullptr &&
      ContextManager::GetThreadCache()->owner == ContextOwner::ThreadPool);
  auto thread_id = ContextManager::GetThreadId();
  k_int32 ind = thread_id - thread_id_offset_;
  Assert(ind >= 0 && ind < threads_num_.load());
  KWDBWrappedThread &kwdb_thread = all_threads_[ind];
  kwdb_operator_info->SetOperatorName(kwdb_thread.m_info.GetOperatorName());
  kwdb_operator_info->SetOperatorOwner(kwdb_thread.m_info.GetOperatorOwner());
  kwdb_operator_info->SetOperatorStatus(kwdb_thread.m_info.GetOperatorStatus());
  kwdb_operator_info->SetOperatorStartTime(
      kwdb_thread.m_info.GetOperatorStartTime());
  kwdb_operator_info->SetProxyThreadInfo(kwdb_thread.m_info.GetProxyThreadInfo());
}

void KWDBDynamicThreadPool::SetOperatorInfo(KWDBOperatorInfo *kwdb_operator_info) {
  auto ctx = ContextManager::GetThreadCache();
  Assert(ctx->owner == ContextOwner::ThreadPool);
  if (ctx->label == ThreadLabel::Master) {
    return;
  }
  k_uint64 thread_id = ContextManager::GetThreadId();
  k_int32 ind = thread_id - thread_id_offset_;
  Assert(ind >= 0 && ind < threads_num_.load());
  KWDBWrappedThread &kwdb_thread = all_threads_[ind];
  kwdb_thread.m_info.SetOperatorName(kwdb_operator_info->GetOperatorName());
  kwdb_thread.m_info.SetOperatorOwner(kwdb_operator_info->GetOperatorOwner());
  kwdb_thread.m_info.SetOperatorStatus(kwdb_operator_info->GetOperatorStatus());
  kwdb_thread.m_info.SetOperatorStartTime(kwdb_operator_info->GetOperatorStartTime());
  kwdb_thread.m_info.SetProxyThreadInfo(kwdb_operator_info->GetProxyThreadInfo());
}

kwdbContext_p KWDBDynamicThreadPool::GetOperatorContext() {
  if (pool_status_ == STOPPED || pool_status_ == STOPPING) {
    return nullptr;
  }

  return ContextManager::GetThreadContext();
}

void KWDBDynamicThreadPool::SetOperatorContext(kwdbContext_p kwdb_operator_context) {
  // to delete this function later
}

#ifdef WITH_TESTS
uint KWDBDynamicThreadPool::DebugGetEnterStartThreadCount() {
  uint result = 0;

  for (auto &t : all_threads_) {
    result += t.m_telemetry.enter_start_thread_count;
  }
  return result;
}

uint KWDBDynamicThreadPool::DebugGetEnterRoutineCount() {
  uint result = 0;

  for (auto &t : all_threads_) {
    result += t.m_telemetry.enter_routine_count;
  }
  return result;
}

uint KWDBDynamicThreadPool::DebugGetUserRoutineCount() {
  uint result = 0;

  for (auto &t : all_threads_) {
    result += t.m_telemetry.hook_user_routine_count;
  }
  return result;
}
#endif

std::atomic<KWDBDynamicThreadPool::PoolStatus> KWDBDynamicThreadPool::pool_status_
    = KWDBDynamicThreadPool::PoolStatus::UNINIT;

thread_local k_uint16 ContextManager::s_thread_id_ = {0};
thread_local kwdbContext_p ContextManager::s_thread_ctx_cache_ = {nullptr};

kwdbContext_p ContextManager::GetThreadContext(const ContextOwner& owner) {
  if (s_thread_ctx_cache_ != nullptr) {
    return s_thread_ctx_cache_;
  }

  kwdbContext_p result = nullptr;

  switch (owner) {
    case ContextOwner::UnClassified:
      result = UnClassifiedContextManager::GetThreadContext();
      break;

    case ContextOwner::GoCoRoutine:
      result = CGoContextManager::GetThreadContext();
      break;

    default:
      Assert(false);
  }
  s_thread_ctx_cache_ = result;
  return result;
}

void ContextManager::RemoveThreadContext() {
  if (s_thread_ctx_cache_ == nullptr) {
    return;
  }
  // The process has ended and ctx will not be reclaimed.
  if (KWDBDynamicThreadPool::GetThreadPool().IsStop()) {
    return;
  }
  switch (s_thread_ctx_cache_->owner) {
    case ContextOwner::UnClassified:
      UnClassifiedContextManager::RemoveThreadContext();
      break;

    case ContextOwner::GoCoRoutine:
      CGoContextManager::RemoveThreadContext();
      break;
  }
}

k_uint16 ContextManager::GetThreadId() {
  return s_thread_id_;
}

inline void ContextManager::SetThreadId(k_uint16 id) {
  s_thread_id_ = id;
}

void ContextManager::SetThreadCache(const kwdbContext_p cache) {
  Assert(s_thread_ctx_cache_ == nullptr);
  s_thread_ctx_cache_ = cache;
}

std::mutex UnClassifiedContextManager::s_mutex_ = {};
k_int32 UnClassifiedContextManager::s_last_thread_idx_ = {-1};
std::vector<std::shared_ptr<kwdbContext_t>> UnClassifiedContextManager::s_contexts_ = {};
std::vector<k_uint32> UnClassifiedContextManager::s_to_thread_index_(1, 0);
std::vector<k_uint32> UnClassifiedContextManager::s_to_thread_id_ = {};

//
// the state of data structures
// without any elements removing
//
//--------------|-------|-------|-------|-------|-------|-------|-------|
// Id           |  N/A  |   1   |   2   |   3   |  ...  |   i   |  i+1  |
//--------------|-------|-------|-------|-------|-------|-------|-------|
// Id to index  |  N/A  |   0   |   1   |   2   |  ...  |   i-1 |   i   |
//--------------|-------|-------|-------|-------|-------|-------|-------|
//                    _____|  _____|  _____|  _____|  _____|  _____|
//                   |       |       |       |       |       |
//--------------|-------|-------|-------|-------|-------|-------|
// ctx at index | ctx_0 | ctx_1 | ctx_2 |  ...  |ctx_i-1| ctx_i |
//--------------|-------|-------|-------|-------|-------|-------|
//
//
//--------------|-------|-------|-------|-------|-------|-------|
// index to Id  |  1    |   2   |   3   |  ...  |   i   |  i+1  |
//--------------|-------|-------|-------|-------|-------|-------|
// index        |  0    |   1   |   2   |  ...  |  i-1  |   i   |
//--------------|-------|-------|-------|-------|-------|-------|
//                                                          |
//                                                           ----------- last_thread_idx
//
kwdbContext_p UnClassifiedContextManager::GetThreadContext() {
  std::lock_guard<std::mutex> lock(s_mutex_);

  auto id = ContextManager::GetThreadId();
  if (id <= 0) {
    SetThreadDestroyer();
    ++s_last_thread_idx_;
    if (s_last_thread_idx_ == s_contexts_.size()) {
      try {
        kwdbContext_p ctx_p = new kwdbContext_t(ContextOwner::UnClassified);
        s_contexts_.push_back(std::shared_ptr<kwdbContext_t>(ctx_p));
      } catch (...) {
        LOG_TH_WARN("UnClassifiedContextManager: Unable to create a new context!");
        return nullptr;
      }
      s_to_thread_index_.push_back(s_last_thread_idx_);
      id = s_last_thread_idx_ + 1;
      s_to_thread_id_.push_back(id);
      Assert(id == s_to_thread_id_.size());
    } else {
      id = s_to_thread_id_[s_last_thread_idx_];
    }

    ContextManager::SetThreadId(id);
  }

  auto idx = s_to_thread_index_[id];
  s_contexts_[idx].get()->owner = ContextOwner::UnClassified;
  return s_contexts_[idx].get();
}

//
// the state of data structures
// when a ctx object of Id 2 is being removed
//
//
//--------------|-------|-------|-------|-------|-------|-------|-------|
// Id           |  N/A  |   1   |\  2  \|   3   |  ...  |   i   |\ i+1 \|
//--------------|-------|-------|-------|-------|-------|-------|-------|
// Id to index  |  N/A  |   0   |\  i  \|   2   |  ...  |   i-1 |\  1  \|
//--------------|-------|-------|-------|-------|-------|-------|-------|
//                         |        |      |       |       |       |
//                         |        |_____/|\_____/|\_____/|\_     |
//                    _____|          _____|  _____|  _____|  |    |
//                   |               |       |       |        |    |
//--------------|-------|-------|-------|-------|-------|-------|  |
// ctx at index | ctx_0 |\ctx_i\| ctx_2 |  ...  |ctx_i-1|\ctx_1\|  |
//--------------|-------|-------|-------|-------|-------|-------|  |
//                          |______________________________________|
//
//--------------|-------|-------|-------|-------|-------|-------|
// index to Id  |  1    |\ i+1 \|   3   |  ...  |   i   |\  2  \|
//--------------|-------|-------|-------|-------|-------|-------|
// index        |  0    |   1   |   2   |  ...  |  i-1  |   i   |
//--------------|-------|-------|-------|-------|-------|-------|
//                                                  |
//                                                   ----------- last_thread_idx
//
void UnClassifiedContextManager::RemoveThreadContext() {
  std::lock_guard<std::mutex> lock(s_mutex_);
  if (s_last_thread_idx_ < 0) {
    return;
  }
  auto id = ContextManager::GetThreadId();
  if (id == 0) {
    return;
  }
  Assert(id < s_to_thread_index_.size());
  auto del_idx = s_to_thread_index_[id];
  if (del_idx < 0 || del_idx > s_last_thread_idx_) {
    return;
  }

  std::swap(s_contexts_[del_idx], s_contexts_[s_last_thread_idx_]);
  auto sub_id = s_to_thread_id_[s_last_thread_idx_];
  std::swap(s_to_thread_index_[id], s_to_thread_index_[sub_id]);
  std::swap(s_to_thread_id_[del_idx], s_to_thread_id_[s_last_thread_idx_]);
  s_contexts_[s_last_thread_idx_]->reset();
  --s_last_thread_idx_;
  ContextManager::ResetThreadId();
  ContextManager::ResetThreadContextCache();
}

void UnClassifiedContextManager::SetThreadDestroyer() {
  class ThreadDestroyer {
   public:
    ~ThreadDestroyer() {
      RemoveThreadContext();
    }
  };
  static thread_local ThreadDestroyer destroyer;
}

//
// class CGoContextManager - we need a small program to test this algorithm
//
std::mutex CGoContextManager::s_mutex_ = {};
k_int32 CGoContextManager::s_last_thread_idx_ = {-1};
k_int32 CGoContextManager::s_max_connection_ = {0};
std::vector<std::shared_ptr<kwdbContext_t>> CGoContextManager::s_contexts_;
std::vector<k_uint32> CGoContextManager::s_to_thread_index_;
std::vector<k_uint32> CGoContextManager::s_to_thread_id_;

kwdbContext_p CGoContextManager::GetThreadContext() {
  std::lock_guard<std::mutex> lock(s_mutex_);

  if (0 == s_max_connection_) {
#ifdef WITH_TESTS
    s_max_connection_ = 100;
#else
    s_max_connection_ = std::stoi(GetKwdbConfig("max_connection"));
#endif
    s_contexts_.resize(s_max_connection_, nullptr);
    s_to_thread_index_.resize(s_max_connection_ + 1, 0);
    s_to_thread_id_.resize(s_max_connection_, 0);
    for (int id = 1; id < s_to_thread_index_.size(); ++id) {
      s_to_thread_index_[id] = id - 1;
    }
    for (int i = 0; i < s_to_thread_id_.size(); ++i) {
      s_to_thread_id_[i] = i + 1;
    }
  }

  auto id = ContextManager::GetThreadId();
  if (id <= 0) {
    ++s_last_thread_idx_;
    if (s_last_thread_idx_ == s_contexts_.size()) {
      s_contexts_.push_back(nullptr);
      s_to_thread_index_.push_back(s_last_thread_idx_);
      s_to_thread_id_.push_back(s_last_thread_idx_ + 1);
    } else {
      id = s_to_thread_id_[s_last_thread_idx_];
    }
    id = s_to_thread_id_[s_last_thread_idx_];
    ContextManager::SetThreadId(id);
  }

  auto idx = s_to_thread_index_[id];
  if (nullptr == s_contexts_[idx]) {
    try {
      kwdbContext_p ctx_p = new kwdbContext_t(ContextOwner::GoCoRoutine);
      s_contexts_[idx] = std::shared_ptr<kwdbContext_t>(ctx_p);
    } catch (...) {
      LOG_TH_WARN("CGoContextManager: Unable to create a new context!");
      return nullptr;
    }
  }

  return s_contexts_[idx].get();
}

void CGoContextManager::RemoveThreadContext() {
  std::lock_guard<std::mutex> lock(s_mutex_);
  if (s_last_thread_idx_ < 0) {
    return;
  }
  auto id = ContextManager::GetThreadId();
  if (id == 0) {
    return;
  }
  Assert(id < s_to_thread_index_.size());
  auto del_idx = s_to_thread_index_[id];
  if (del_idx > s_last_thread_idx_) {
    return;
  }

  std::swap(s_contexts_[del_idx], s_contexts_[s_last_thread_idx_]);
  auto sub_id = s_to_thread_id_[s_last_thread_idx_];
  std::swap(s_to_thread_index_[id], s_to_thread_index_[sub_id]);
  std::swap(s_to_thread_id_[del_idx], s_to_thread_id_[s_last_thread_idx_]);
  if (s_contexts_[s_last_thread_idx_] != nullptr) {
    // need to understand why nullptr happens
    s_contexts_[s_last_thread_idx_]->reset();
  }
  --s_last_thread_idx_;
  ContextManager::ResetThreadId();
  ContextManager::ResetThreadContextCache();
}
}  // namespace kwdbts

