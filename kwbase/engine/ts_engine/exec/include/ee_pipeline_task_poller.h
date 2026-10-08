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
#include "ee_task.h"
#include "kwdb_type.h"


namespace kwdbts {

class ExecPool;

class PipelineTaskPoller {
  using TaskList = std::list<ExecTaskPtr>;

 public:
  explicit PipelineTaskPoller(ExecPool *exec_pool);

  ~PipelineTaskPoller();

  KStatus Init();

  void Stop();

  void add_blocked_driver(ExecTaskPtr task);

 protected:
  void run_internal(void *arg);


 private:
  std::atomic_bool is_poller_stop_{false};
  TaskList blocked_drivers_;
  ExecPool *exec_pool_{nullptr};
  std::thread poller_thread_;
  std::mutex blocked_drivers_mutex_;
  std::condition_variable blocked_drivers_cv_;
};

}  // namespace kwdbts
