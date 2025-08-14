// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//

// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_pipeline_task_poller.h"

#include <chrono>
#include <thread>

#include "ee_exec_pool.h"
#include "ee_pipeline_task.h"
#include "lg_api.h"

namespace kwdbts {
PipelineTaskPoller::PipelineTaskPoller(ExecPool *exec_pool) {
  exec_pool_ = exec_pool;
}

PipelineTaskPoller::~PipelineTaskPoller() {}

KStatus PipelineTaskPoller::Init() {
  poller_thread_ = std::thread([this]() { this->run_internal(this); });

  return KStatus::SUCCESS;
}

void PipelineTaskPoller::Stop() {
  is_poller_stop_ = true;
  blocked_drivers_cv_.notify_all();
  poller_thread_.join();
}

void PipelineTaskPoller::add_blocked_driver(ExecTaskPtr task) {
  {
    std::unique_lock<std::mutex> lock(blocked_drivers_mutex_);
    blocked_drivers_.push_back(task);
  }

  blocked_drivers_cv_.notify_one();
}

void PipelineTaskPoller::run_internal(void *arg) {
  TaskList tmp_blocked_tasks;
  while (!is_poller_stop_) {
    {
      std::unique_lock<std::mutex> lock(blocked_drivers_mutex_);
      tmp_blocked_tasks.splice(tmp_blocked_tasks.end(), blocked_drivers_);
      if (tmp_blocked_tasks.empty()) {
        blocked_drivers_cv_.wait_for(lock, std::chrono::seconds(2));
        continue;
      }
    }

    auto task_it = tmp_blocked_tasks.begin();
    while (task_it != tmp_blocked_tasks.end()) {
      auto task = *task_it;
      std::shared_ptr<PipelineTask> pipeline_task =
          std::dynamic_pointer_cast<PipelineTask>(task);
      if (pipeline_task->is_can_schedule()) {
        tmp_blocked_tasks.erase(task_it++);
        // LOG_ERROR("block task is active %p", task.get());
        exec_pool_->PushTask(task);
      } else {
        task_it++;
      }
    }
    std::this_thread::sleep_for(std::chrono::microseconds(2));
  }
}

}  // namespace kwdbts
