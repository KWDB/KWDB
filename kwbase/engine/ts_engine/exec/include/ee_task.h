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

#include <memory>
#include <string>

#include "cm_kwdb_context.h"
#include "ee_timer_event.h"

namespace kwdbts {

enum ExecTaskState {
  EXEC_TASK_STATE_IDLE,
  EXEC_TASK_STATE_WAITING,
  EXEC_TASK_STATE_RUNNING
};

/**
 * @brief ExecTask task
 */
class ExecTask : public TimerEvent, public std::enable_shared_from_this<ExecTask> {
 public:
  virtual ~ExecTask() {}

  ExecTask(const ExecTask &) = delete;
  ExecTask& operator=(const ExecTask &) = delete;
  ExecTask(ExecTask &&) = delete;
  ExecTask& operator=(ExecTask &&) = delete;
  std::shared_ptr<ExecTask> GetPtr() { return shared_from_this(); }
  /**
   * @brief op interface
   */
  virtual void Run(kwdbContext_p ctx) {}

  virtual bool is_can_schedule() { return false; }
  /**
   * @brief Construct a new ExecTask object
   *
   */
  ExecTask() : state_(EXEC_TASK_STATE_IDLE) {}

  void SetState(ExecTaskState state) { state_ = state; }
  ExecTaskState GetState() { return state_; }

 private:
  ExecTaskState state_;
};

typedef std::shared_ptr<ExecTask> ExecTaskPtr;
};  // namespace kwdbts
