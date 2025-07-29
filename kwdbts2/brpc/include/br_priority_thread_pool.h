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

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "br_blocking_priority_queue.h"
#include "kwdb_type.h"
#include "lg_api.h"

namespace kwdbts {

// Simple threadpool which processes items (of type T) in parallel which were
// placed on a blocking queue by Offer(). Each item is processed by a single
// user-supplied method.
class PriorityThreadPool {
 public:
  // Signature of a work-processing function. Takes the integer id of the thread
  // which is calling it (ids run from 0 to num_threads - 1) and a reference to
  // the item to process.
  typedef std::function<void()> WorkFunction;

  struct Task {
   public:
    k_int32 priority = 0;
    WorkFunction work_function;
    k_bool operator<(const Task& o) const { return priority < o.priority; }

    Task& operator++() {
      priority += 2;
      return *this;
    }
  };

  // Creates a new thread pool and start num_threads threads.
  //  -- num_threads: how many threads are part of this pool
  //  -- queue_size: the maximum size of the queue on which work items are
  //  offered. If the
  //     queue exceeds this size, subsequent calls to Offer will block until
  //     there is capacity available.
  //  -- work_function: the function to run every time an item is consumed from
  //  the queue
  PriorityThreadPool(std::string name, k_uint32 num_threads, k_uint32 queue_size)
      : name_(std::move(name)), work_queue_(queue_size), shutdown_(false) {
    for (k_int32 i = 0; i < num_threads; ++i) {
      NewThread(++current_thread_id_);
    }
  }

  // Destructor ensures that all threads are terminated before this object is
  // freed (otherwise they may continue to run and reference member variables)
  ~PriorityThreadPool() noexcept {
    Shutdown();
    Join();
  }

  // Blocking operation that puts a work item on the queue. If the queue is
  // full, blocks until there is capacity available.
  //
  // 'work' is copied into the work queue, but may be referenced at any time in
  // the future. Therefore the caller needs to ensure that any data referenced
  // by work (if T is, e.g., a pointer type) remains valid until work has been
  // processed, and it's up to the caller to provide their own signalling
  // mechanism to detect this (or to wait until after DrainAndshutdown returns).
  //
  // Returns true if the work item was successfully added to the queue, false
  // otherwise (which typically means that the thread pool has already been shut
  // down).
  k_bool Offer(const Task& task) { return work_queue_.BlockingPut(task); }

  k_bool TryOffer(const Task& task) { return work_queue_.TryPut(task); }

  k_bool Offer(WorkFunction func) {
    PriorityThreadPool::Task task = {0, std::move(func)};
    return work_queue_.BlockingPut(std::move(task));
  }

  k_bool TryOffer(WorkFunction func) {
    PriorityThreadPool::Task task = {0, std::move(func)};
    return work_queue_.TryPut(std::move(task));
  }

  // Shuts the thread pool down, causing the work queue to cease accepting
  // offered work and the worker threads to terminate once they have processed
  // their current work item. Returns once the shutdown flag has been set, does
  // not wait for the threads to terminate.
  void Shutdown() {
    {
      std::lock_guard<std::mutex> l(lock_);
      shutdown_ = true;
    }
    work_queue_.Shutdown();
  }

  // Blocks until all threads are finished. shutdown does not need to have been
  // called, since it may be called on a separate thread.
  void Join() {
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  k_size_t GetQueueCapacity() const { return work_queue_.GetCapacity(); }

  k_uint32 GetQueueSize() const { return work_queue_.GetSize(); }

  // Blocks until the work queue is empty, and then calls shutdown to stop the
  // worker threads and Join to wait until they are finished. Any work
  // Offer()'ed during DrainAndshutdown may or may not be processed.
  void DrainAndShutdown() {
    {
      std::unique_lock<std::mutex> l(lock_);
      while (work_queue_.GetSize() != 0) {
        empty_cv_.wait(l);
      }
    }
    Shutdown();
    Join();
  }

  void SetNumThread(k_int32 num_thread) {
    k_size_t num_thread_in_pool = threads_.size();
    if (num_thread > num_thread_in_pool) {
      IncreaseThr(num_thread - num_thread_in_pool);
    } else if (num_thread < num_thread_in_pool) {
      DecreaseThr(num_thread_in_pool - num_thread);
    }
  }

 private:
  void IncreaseThr(k_int32 num_thread) {
    std::lock_guard<std::mutex> l(lock_);
    for (k_int32 i = 0; i < num_thread; ++i) {
      NewThread(++current_thread_id_);
    }
  }

  void DecreaseThr(k_int32 num_thread) {
    should_decrease_ += num_thread;

    for (k_int32 i = 0; i < num_thread; ++i) {
      PriorityThreadPool::Task empty_task = {0, []() {}};
      work_queue_.TryPut(empty_task);
    }
  }

  void SetThreadName(pthread_t t, const std::string& name) {
    k_int32 ret;
    if (name.length() < 16) {
      ret = pthread_setname_np(t, name.data());
    } else {
      std::string truncated_name = name.substr(0, 15);
      ret = pthread_setname_np(t, truncated_name.c_str());
    }
    if (ret) {
      LOG_ERROR("Failed to set thread name: %s", name.c_str());
    }
  }

  void NewThread(k_int32 tid) {
    threads_.emplace_back(std::bind(&PriorityThreadPool::WorkThread, this, tid));
    SetThreadName(threads_.back().native_handle(), name_);
    threads_id_.emplace_back(tid);
  }

  void RemoveThread(k_int32 tid) {
    std::lock_guard<std::mutex> l(lock_);
    auto it = std::find(threads_id_.begin(), threads_id_.end(), tid);
    if (it != threads_id_.end()) {
      k_size_t index = std::distance(threads_id_.begin(), it);
      if (index < threads_.size()) {
        threads_.erase(threads_.begin() + index);
        threads_id_.erase(it);
      } else {
        LOG_ERROR("Invalid thread index: %zu, threads_.size(): %zu", index, threads_.size());
      }
    }
  }

  void WorkThread(k_int32 thread_id) {
    while (!IsShutdown()) {
      Task task;
      if (work_queue_.BlockingGet(&task)) {
        task.work_function();
      }
      if (work_queue_.GetSize() == 0) {
        empty_cv_.notify_all();
      }
      if (should_decrease_) {
        k_bool need_destroy = true;
        k_int32 expect;
        k_int32 target;
        do {
          expect = should_decrease_;
          target = expect - 1;
          if (expect == 0) {
            need_destroy = false;
            break;
          }
        } while (!should_decrease_.compare_exchange_weak(expect, target));
        if (need_destroy) {
          RemoveThread(thread_id);
          break;
        }
      }
    }
  }

  k_bool IsShutdown() {
    std::lock_guard<std::mutex> l(lock_);
    return shutdown_;
  }

  const std::string name_;
  std::vector<k_int32> threads_id_;
  BlockingPriorityQueue<Task> work_queue_;
  std::vector<std::thread> threads_;
  std::mutex lock_;
  k_bool shutdown_;
  std::condition_variable empty_cv_;
  std::atomic<k_int32> should_decrease_{0};
  std::atomic<k_int32> current_thread_id_{0};
};

}  // namespace kwdbts
