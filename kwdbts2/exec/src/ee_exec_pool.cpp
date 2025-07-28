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
#include "ee_exec_pool.h"

#include "ee_task.h"
#include "er_api.h"
#include "lg_api.h"
#include "kwdb_type.h"
#include "cm_fault_injection.h"
#include "ee_cpuinfo.h"
#include "ee_pipeline_task_poller.h"

namespace kwdbts {

#define DEFAULT_MAX_THREADS_NUM 10
#define MIN_DOP_LIMIT 2

ExecPool::ExecPool(k_uint32 tq_num, k_uint32 tp_size) {
  tq_num_ = tq_num;
  min_threads_ = tp_size;
  max_threads_ = DEFAULT_MAX_THREADS_NUM;
  is_tp_stop_ = true;
}

void ExecPool::CreateThread() {
  std::thread thr([this]() {
    this->Routine(this);
  });
  thr.detach();
}

KStatus ExecPool::Init(kwdbContext_p ctx) {
  EnterFunc();
  if (is_init_) {
    Return(SUCCESS);
  }
  is_init_ = true;
  if (EE_ENABLE_PARALLEL > 0) {
    CpuInfo::Init();
    max_threads_ = CpuInfo::Get_Num_Cores();
    total_threads_.store(max_threads_);
  }
  is_tp_stop_ = false;
  timer_event_pool_ = KNEW TimerEventPool(EE_TIMER_EVENT_POOL_SIZE);
  if (timer_event_pool_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  pipeline_task_poller_ = new PipelineTaskPoller(this);
  if (pipeline_task_poller_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  pipeline_task_poller_->Init();

  for (k_int32 i = 0; i < min_threads_; i++) {
    {
      std::unique_lock unique_lock(lock_);
      threads_starting_++;
    }
    CreateThread();
  }

  Return(KStatus::SUCCESS);
}

void ExecPool::Stop() {
  if (nullptr != pipeline_task_poller_) {
    pipeline_task_poller_->Stop();
    SafeDeletePointer(pipeline_task_poller_);
  }

  is_tp_stop_ = true;
  wait_cond_.notify_all();
  not_fill_cv_.notify_all();
  if (timer_event_pool_) {
    timer_event_pool_->Stop();
    SafeDeletePointer(timer_event_pool_);
  }
  std::unique_lock l(lock_);
  no_threads_cond_.wait(
      l, [&]() { return threads_num_ + threads_starting_ <= 0; });
  sleep(1);
  task_queue_.clear();
}

ExecPool::~ExecPool() {
  if (!is_tp_stop_) {
    Stop();
  }
}
/*
 * execute tasks in the task queue
 */
void ExecPool::Routine(void *arg) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);
  std::unique_lock l(lock_);
  threads_num_++;
  threads_starting_--;
  bool permanent = threads_num_ <= min_threads_;
  bool wait_once = false;
  while (true) {
    if (is_tp_stop_) {
      break;
    }
    // task_queue_ null
    if (task_queue_.empty()) {
      if (!permanent && wait_once) {
        break;
      }
      wait_cond_.wait_for(l, std::chrono::seconds(2));
      wait_once = true;
      continue;
    }
    wait_once = false;
    // get task
    ExecTaskPtr task_ptr = task_queue_.front();
    task_queue_.pop_front();
    not_fill_cv_.notify_one();
    if (task_ptr) {
      ++active_threads_;
      l.unlock();
      try {
        task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_RUNNING);
        // execute task
        task_ptr->Run(ctx);
        task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_IDLE);
      } catch (...) {
        constexpr char ERROR_MESSAGE[128] =
            "some unknown exception was thrown in execution of the thread pool!";
        LOG_ERROR(ERROR_MESSAGE);
      }
      l.lock();
      --active_threads_;
    }
  }
  threads_num_--;
  if (threads_num_ + threads_starting_ == 0) {
    no_threads_cond_.notify_all();
  }
}

KStatus ExecPool::PushTask(ExecTaskPtr task_ptr, bool no_check) {
  std::unique_lock unique_lock(lock_);
  if (!no_check) {
    not_fill_cv_.wait(unique_lock, [this]() -> bool {
      return ((task_queue_.size() < tq_num_) || is_tp_stop_);
    });
  }
  if (is_tp_stop_) {
    return KStatus::FAIL;
  }
  k_int32 idle = threads_num_ + threads_starting_ - active_threads_;
  k_int32 need_threads = task_queue_.size() + 1 - idle;
  bool need = false;
  if (need_threads > 0 &&
      threads_num_ + threads_starting_ < max_threads_) {
    need = true;
    threads_starting_++;
  }
  // insert task queue
  if (task_ptr->is_can_schedule()) {
    task_queue_.push_back(task_ptr);
    task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_WAITING);
  } else {
    pipeline_task_poller_->add_blocked_driver(task_ptr);
  }

  // notify
  wait_cond_.notify_one();
  unique_lock.unlock();
  if (need) {
    CreateThread();
  }
  return KStatus::SUCCESS;
}

KStatus ExecPool::PushBlockedTask(ExecTaskPtr task_ptr) {
  pipeline_task_poller_->add_blocked_driver(task_ptr);

  return KStatus::SUCCESS;
}

void ExecPool::PushTimeEvent(TimerEventPtr event_ptr) {
  return timer_event_pool_->PushTimeEvent(event_ptr);
}

k_bool ExecPool::IsFull() { return task_queue_.size() >= tq_num_; }

k_uint32 ExecPool::GetWaitThreadNum() const {
  k_int32 idle = threads_num_ + threads_starting_ - active_threads_;
  return idle > 8 ? 8 : idle;
}

k_uint32 ExecPool::GetWaitThreadNum(k_uint32 dop) {
  k_int32 availableThreads = total_threads_.fetch_sub(dop);
  // LOG_ERROR("avai:%d, dop:%d",availableThreads, dop);
  if (availableThreads <= 1) {
    total_threads_.fetch_add(dop);
    return 1;
  }
  if (availableThreads < dop) {
    total_threads_.fetch_add(dop - availableThreads);
    return availableThreads;
  }
  return dop;
}

k_uint32 ExecPool::ReleaseThreadNum(k_uint32 dop) {
  // LOG_ERROR("add dop:%d", dop);
  total_threads_.fetch_add(dop);
  return 1;
}

}  // namespace kwdbts
