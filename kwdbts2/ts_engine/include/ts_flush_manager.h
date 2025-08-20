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
#include <condition_variable>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "settings.h"
#include "ts_common.h"
#include "ts_vgroup.h"

namespace kwdbts {

class TsMemSegment;
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
    cv_.notify_all();
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

  std::condition_variable cv_;
  std::mutex job_mutex_;

  KStatus job_status_ = KStatus::SUCCESS;

  void WorkerFunc() {
    while (true) {
      std::unique_lock lk{job_mutex_};
      cv_.wait(lk, [this]() { return !jobs_.empty(); });
      auto job = jobs_.front();
      jobs_.pop_front();
      lk.unlock();
      if (job.vgroup == nullptr) {
        // found sentinel, just break;
        break;
      }

      BackgroundFlushJob(&job);
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
    auto s = job->vgroup->FlushImmSegment(job->imm_segment);
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

    while (true) {
      size_t job_size = 0;
      {
        std::unique_lock lk{job_mutex_};
        job_size = jobs_.size();
      }
      if (job_size >= kMaxConcurrentJobNum) {
        LOG_INFO("Flush job queue is full, wait for a job to finish");
        std::this_thread::sleep_for(1s);
      } else {
        break;
      }
    }
    {
      std::unique_lock lk{job_mutex_};
      jobs_.push_back(job_info);
    }
    cv_.notify_one();
  }

  KStatus GetBackGroundStatus() const { return job_status_; }
};

}  //  namespace kwdbts
