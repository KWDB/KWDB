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
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "kwdb_type.h"
#include "settings.h"
#include "ts_lastsegment_builder.h"
#include "ts_vgroup.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <condition_variable>
#include <mutex>
#endif

namespace kwdbts {

class TsMemSegment;
#ifdef _WIN32

class WinMutex {
private:
    CRITICAL_SECTION cs_;
public:
    WinMutex() { InitializeCriticalSection(&cs_); }
    ~WinMutex() { DeleteCriticalSection(&cs_); }
    void lock() { EnterCriticalSection(&cs_); }
    void unlock() { LeaveCriticalSection(&cs_); }
    CRITICAL_SECTION* native_handle() { return &cs_; }
    WinMutex(const WinMutex&) = delete;
    WinMutex& operator=(const WinMutex&) = delete;
};

class WinCondVar {
private:
    CONDITION_VARIABLE cv_;
public:
    WinCondVar() { InitializeConditionVariable(&cv_); }
    ~WinCondVar() {}

    // Accept std::unique_lock to keep its internal state in sync with the
    // underlying CRITICAL_SECTION.  SleepConditionVariableCS atomically
    // releases the CS and re-acquires it before returning, mirroring what
    // std::condition_variable does with std::unique_lock<std::mutex>.
    void wait(std::unique_lock<WinMutex>& lk) {
        // unique_lock must own the lock before we hand control to the OS.
        assert(lk.owns_lock());
        // Temporarily release ownership so unique_lock's destructor does not
        // double-unlock after SleepConditionVariableCS re-acquires the CS.
        WinMutex* m = lk.release();
        SleepConditionVariableCS(&cv_, m->native_handle(), INFINITE);
        // Re-establish ownership so callers (and the destructor) see a
        // consistently locked unique_lock.
        lk = std::unique_lock<WinMutex>(*m, std::adopt_lock);
    }

    template<typename Predicate>
    void wait(std::unique_lock<WinMutex>& lk, Predicate pred) {
        while (!pred()) {
            wait(lk);
        }
    }

    void notify_one() { WakeConditionVariable(&cv_); }
    void notify_all() { WakeAllConditionVariable(&cv_); }

    WinCondVar(const WinCondVar&) = delete;
    WinCondVar& operator=(const WinCondVar&) = delete;
};

#endif

class TsFlushJobPool {
 private:
  TsFlushJobPool() {
    kMaxConcurrentJobNum = kMaxConcurrentJobNum < 8 ? 8 : kMaxConcurrentJobNum;
    for (int i = 0; i < kMaxConcurrentJobNum; i++) {
      workers_.emplace_back([this] { this->WorkerFunc(); });
    }
  }
  ~TsFlushJobPool() {
    // cancel all unrun jobs
    {
      std::lock_guard lk{job_mutex_};
      jobs_.erase(
          std::remove_if(jobs_.begin(), jobs_.end(), [](const JobInfo& job) { return job.status == JOB_CREATED; }),
          jobs_.end());

      // add sentinel
      for (int i = 0; i < kMaxConcurrentJobNum; i++) {
        jobs_.push_back({JOB_CANCELED, nullptr, nullptr});
      }
    }

    // wait all threads
    queue_non_empty_cv_.notify_all();
    for (auto& worker : workers_) {
      worker.join();
    }
  }
  TsFlushJobPool(const TsFlushJobPool&) = delete;
  TsFlushJobPool& operator=(const TsFlushJobPool&) = delete;

  enum JobStatus : uint8_t {
    JOB_CREATED = 1,
    JOB_RUNNING = 2,
    JOB_FINISHED = 3,
    JOB_CANCELED = 4,
    JOB_FAILED = 5,
  };

  struct JobInfo {
    JobStatus status;
    TsVGroup* vgroup;
    std::shared_ptr<TsMemSegment> imm_segment;
    int retry_times = 0;
  };

  std::deque<JobInfo> jobs_;

  int kMaxConcurrentJobNum = EngineOptions::vgroup_max_num * 2;
  std::vector<std::thread> workers_;

  #ifdef _WIN32
    WinMutex job_mutex_;
    WinCondVar queue_non_empty_cv_;
    WinCondVar queue_non_full_cv_;
    WinCondVar stop_cv_;
  #else
    std::mutex job_mutex_;
    std::condition_variable queue_non_empty_cv_;
    std::condition_variable queue_non_full_cv_;
    std::condition_variable stop_cv_;
  #endif
  std::atomic_int bg_running_jobs_ = 0;

  std::atomic<k_bool> stop_flag_{false};
  KStatus job_status_ = KStatus::SUCCESS;

  void WorkerFunc() {
    while (true) {
      std::unique_lock lk{job_mutex_};
      queue_non_empty_cv_.wait(lk, [this]() { return !jobs_.empty(); });
      auto job = jobs_.front();
      bg_running_jobs_++;
      jobs_.pop_front();
      lk.unlock();
      queue_non_full_cv_.notify_one();
      if (job.vgroup == nullptr) {
        // found sentinel, just break;
        break;
      }

      BackgroundFlushJob(&job);
      bg_running_jobs_--;
      stop_cv_.notify_one();
      if (job.status == JOB_CREATED) {
        // retry again
        AddFlushJob(job.vgroup, job.imm_segment);
      } else if (job.status == JOB_FAILED) {
        job_status_ = KStatus::FAIL;
      }
    }
  }

  static void BackgroundFlushJob(JobInfo* job) {
    assert(job->status == JOB_CREATED);
    job->status = JOB_RUNNING;
    thread_local std::unique_ptr<TsLastSegmentBuilder> last_segment_builder;
    auto s = job->vgroup->FlushImmSegment(last_segment_builder, job->imm_segment);
    if (s == FAIL) {
      if (job->retry_times < 2) {
        job->retry_times++;
        job->status = JOB_CREATED;
      } else {
        job->status = JOB_FAILED;
      }
    } else {
      job->status = JOB_FINISHED;
    }
  }

 public:
  static TsFlushJobPool& GetInstance() {
    static TsFlushJobPool instance;
    return instance;
  }

  void AddFlushJob(TsVGroup* vgroup, std::shared_ptr<TsMemSegment> mem_segment) {
    JobInfo job_info;
    job_info.status = JOB_CREATED;
    job_info.vgroup = vgroup;
    job_info.imm_segment = mem_segment;

    {
      std::unique_lock lk{job_mutex_};
      if (jobs_.size() >= kMaxConcurrentJobNum) {
        LOG_INFO("Flush job queue is full, wait for a job to finish");
        queue_non_full_cv_.wait(lk, [this]() { return jobs_.size() < kMaxConcurrentJobNum; });
      }
      if (stop_flag_.load() != true) {
        jobs_.push_back(job_info);
      }
    }
    queue_non_empty_cv_.notify_one();
  }

  void StopAndWait() {
    stop_flag_.store(true);
    std::unique_lock lk{job_mutex_};
    stop_cv_.wait(lk, [this]() { return jobs_.empty() && bg_running_jobs_.load() == 0; });
  }

  void Start() {
    stop_flag_.store(false);
    job_status_ = KStatus::SUCCESS;
    jobs_.clear();
  }

  KStatus GetBackGroundStatus() const { return job_status_; }
};

}  //  namespace kwdbts
