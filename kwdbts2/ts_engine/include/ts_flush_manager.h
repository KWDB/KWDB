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

#include <map>
#include <mutex>
#include <deque>
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
    std::list<std::pair<TsVGroup*, std::shared_ptr<TsMemSegment>>> vgrp_mem_segs;
  };
  const std::vector<std::shared_ptr<TsVGroup>>& vgrps_;
  std::mutex job_mutex_;
  std::deque<SwithJobInfo> jobs_;
  TS_LSN flushed_lsn_{0};

 public:
  TsLSNFlushManager(std::vector<std::shared_ptr<TsVGroup>>& vgrps) : vgrps_(vgrps) {}

  TS_LSN GetFinishedLSN() {
    return flushed_lsn_;
  }

  KStatus FlashMemSegment(TS_LSN lsn) {
    for(auto& job : jobs_) {
      if (job.lsn == lsn) {
        LOG_ERROR("wal file [%lu] already in jobs.", lsn);
        return KStatus::FAIL;
      }
    }
    SwithJobInfo flush_job;
    flush_job.status = JOB_CREATED;
    flush_job.lsn = lsn;
    std::shared_ptr<TsMemSegment> cur_mem;
    for (auto& grp : vgrps_) {
      grp->SwitchMemSegment(&cur_mem);
      if (cur_mem != nullptr) {
        flush_job.vgrp_mem_segs.push_back(std::make_pair(grp.get(), cur_mem));
      }
    }
    kwdbts::KWDBOperatorInfo kwdb_operator_info;
    kwdb_operator_info.SetOperatorName("flushMemSegment");
    kwdb_operator_info.SetOperatorOwner("TsLSNFlushManager");
    time_t now;
    // Record the start time of the operation
    kwdb_operator_info.SetOperatorStartTime((uint64_t) time(&now));
    flush_job.thread_id = kwdbts::KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&TsLSNFlushManager::FlushThreadFunc, this, std::placeholders::_1), &flush_job,
      &kwdb_operator_info);
    if (flush_job.thread_id < 1) {
      flush_job.status = JOB_FAILED;
      LOG_ERROR("TsLSNFlushManager flush thread create failed");
    }
    job_mutex_.lock();
    jobs_.push_back(std::move(flush_job));
    job_mutex_.unlock();
    return KStatus::SUCCESS;
  }

  void updateFinishLSN () {
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
    for (auto& mem : job_info->vgrp_mem_segs) {
      if (kwdbts::KWDBDynamicThreadPool::GetThreadPool().IsCancel()) {
        job_info->status = JOB_CANCELED;
        break;
      }
      if (mem.second == nullptr || mem.first == nullptr) {
        continue;
      }
      auto s = mem.first->FlushImmSegment(mem.second);
      if (s == KStatus::SUCCESS) {
        // mem segment flush success. no need flush anymore.
        mem.first = nullptr;
        mem.second.reset();
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
