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

#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <deque>
#include <vector>
#include <utility>
#include "settings.h"
#include "ts_common.h"
#include "ts_vgroup.h"

namespace kwdbts {

class TsLSNFlushManager {
 private:
  enum JobStatus : uint8_t {
    JOB_CREATED = 1,
    JOB_RUNNING = 2,
    JOB_FINISHED = 3,
    JOB_CANCELED = 4,
    JOB_FAILED = 5,
  };
  struct SwithJobInfo {
    JobStatus status;
    TS_LSN lsn;
    KThreadID thread_id;
    std::vector<TsVGroup*> vgroups;
  };
  const std::vector<std::shared_ptr<TsVGroup>>& vgrps_;
  std::mutex job_mutex_;
  std::deque<SwithJobInfo> jobs_;
  TS_LSN flushed_lsn_{0};
  std::atomic<size_t> mem_size_{0};
  std::atomic<size_t> mem_total_size_{0};

 public:
  explicit TsLSNFlushManager(std::vector<std::shared_ptr<TsVGroup>>& vgrps) : vgrps_(vgrps) {}

  TS_LSN GetFinishedLSN() {
    return flushed_lsn_;
  }

  void Count(size_t data_size) {
    auto total_size = mem_total_size_.fetch_add(data_size);
    mem_size_.fetch_add(data_size);
    auto now_size = mem_size_.load();
    if (now_size > EngineOptions::mem_segment_max_size) {
      if (mem_size_.compare_exchange_strong(now_size, 0)) {
        // TODO(zzr, liangbo): total_size input as LSN?
        FlushMemSegment(total_size);
      }
    }
  }

  KStatus FlushMemSegment(TS_LSN lsn) {
    for (auto& job : jobs_) {
      if (job.lsn == lsn) {
        LOG_ERROR("wal file [%lu] already in jobs.", lsn);
        return KStatus::FAIL;
      }
    }
    while (jobs_.size() > 4) {
      LOG_INFO("current flush job number [%lu] sleep 1 second.", jobs_.size());
      sleep(1);
    }
    SwithJobInfo flush_job;
    flush_job.status = JOB_CREATED;
    flush_job.lsn = lsn;
    std::shared_ptr<TsMemSegment> cur_mem;
    for (auto& grp : vgrps_) {
      flush_job.vgroups.push_back(grp.get());
    }
    kwdbts::KWDBOperatorInfo kwdb_operator_info;
    kwdb_operator_info.SetOperatorName("flushMemSegment");
    kwdb_operator_info.SetOperatorOwner("TsLSNFlushManager");
    time_t now;
    // Record the start time of the operation
    kwdb_operator_info.SetOperatorStartTime((uint64_t) time(&now));
    job_mutex_.lock();
    jobs_.push_back(std::move(flush_job));
    SwithJobInfo& flush_job_in_list = jobs_.back();
    job_mutex_.unlock();
    flush_job_in_list.thread_id = kwdbts::KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&TsLSNFlushManager::FlushThreadFunc, this, std::placeholders::_1), &flush_job_in_list,
      &kwdb_operator_info);
    if (flush_job_in_list.thread_id < 1) {
      flush_job_in_list.status = JOB_FAILED;
      LOG_ERROR("TsLSNFlushManager flush thread create failed");
    }
    return KStatus::SUCCESS;
  }

  void updateFinishLSN() {
    job_mutex_.lock();
    while (jobs_.size() > 0) {
      SwithJobInfo& cur_job = jobs_.front();
      if (cur_job.status == JOB_FINISHED) {
        if (flushed_lsn_ < cur_job.lsn) {
          flushed_lsn_ = cur_job.lsn;
        }
        jobs_.pop_front();
      } else if (cur_job.status == JOB_FAILED) {
        cur_job.status = JOB_CREATED;
        kwdbts::KWDBOperatorInfo kwdb_operator_info;
        kwdb_operator_info.SetOperatorName("flushMemSegment");
        kwdb_operator_info.SetOperatorOwner("TsLSNFlushManager");
        time_t now;
        // Record the start time of the operation
        kwdb_operator_info.SetOperatorStartTime((uint64_t) time(&now));
        cur_job.thread_id = kwdbts::KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
          std::bind(&TsLSNFlushManager::FlushThreadFunc, this, std::placeholders::_1), &cur_job,
          &kwdb_operator_info);
        if (cur_job.thread_id < 1) {
          cur_job.status = JOB_FAILED;
          LOG_ERROR("TsLSNFlushManager flush thread create failed");
        }
        break;
      } else {
        break;
      }
    }
    job_mutex_.unlock();
  }

  void FlushThreadFunc(void* args) {
    SwithJobInfo* job_info = reinterpret_cast<SwithJobInfo*>(args);
    assert(job_info->status == JOB_CREATED);
    job_info->status = JOB_RUNNING;
    bool flush_success = true;
    for (auto& vgroup : job_info->vgroups) {
      if (kwdbts::KWDBDynamicThreadPool::GetThreadPool().IsCancel()) {
        job_info->status = JOB_CANCELED;
        break;
      }
      if (vgroup == nullptr) {
        continue;
      }
      auto s = vgroup->Flush();
      if (s == KStatus::SUCCESS) {
        // mem segment flush success. no need flush anymore.
        vgroup = nullptr;
      } else {
        flush_success = false;
      }
    }
    if (flush_success) {
      job_info->status = JOB_FINISHED;
    } else {
      job_info->status = JOB_FAILED;
    }
    updateFinishLSN();
  }
};

}  //  namespace kwdbts
