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

#include <functional>

#include "cm_task.h"
#include "lg_api.h"
#include "cm_func.h"
#include "th_kwdb_operator_info.h"
#include "th_kwdb_dynamic_thread_pool.h"

namespace kwdbts {

KStatus CreateTask(kwdbContext_p ctx, void* args, const std::string& task_nm,
                   const std::string& owner, std::function<void(void*)>&& job) {
  EnterFunc();
  KWDBOperatorInfo op_info;
  op_info.SetOperatorName(task_nm);
  op_info.SetOperatorOwner(owner);
  time_t now;
  op_info.SetOperatorStartTime((k_uint64)time(&now));
  // TODO(xiabohan): replace task_args where with ctx.
  KThreadID thread_id = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::move(job), args, &op_info);
  if (thread_id == 0) {
    Return(FAIL);
  }
  Return(SUCCESS);
}

}  // namespace kwdbts
