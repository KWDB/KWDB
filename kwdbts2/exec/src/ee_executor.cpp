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

#include "ee_executor.h"

#include <fstream>

#include "ee_exec_pool.h"

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

namespace kwdbts {

void DeleteDirectory(const std::string& path) {
  if (!fs::exists(path)) {
    return;
  }
  for (const auto& entry : fs::directory_iterator(path)) {
    if (!fs::is_directory(entry)) {
      fs::remove(entry.path());  // delete file
    }
  }
}

KStatus InitExecutor(kwdbContext_p ctx, const EngineOptions &options) {
  EnterFunc();
  k_int32 thread_num = options.thread_pool_size;
  if (ExecPool::GetInstance(options.task_queue_size, thread_num).IsInited()) {
    LOG_INFO("no need init again.");
    return KStatus::SUCCESS;
  }
  ExecPool::GetInstance(options.task_queue_size, thread_num).Init(ctx);
  ExecPool::GetInstance().db_path_ = options.db_path + "/temp_db_/";
  if (access(ExecPool::GetInstance().db_path_.c_str(), 0)) {
    fs::create_directories(ExecPool::GetInstance().db_path_);
  } else {
    DeleteDirectory(ExecPool::GetInstance().db_path_);
  }
  k_uint32 bufferpool_size = options.buffer_pool_size;
  g_pstBufferPoolInfo = kwdbts::EE_MemPoolInit(bufferpool_size, ROW_BUFFER_SIZE);
  Return(SUCCESS);
}
KStatus DestoryExecutor() {
  if (ExecPool::GetInstance().IsStopped()) {
    LOG_INFO("no need stop again.");
    return KStatus::SUCCESS;
  }
  ExecPool::GetInstance().Stop();
  kwdbts::KStatus status = kwdbts::EE_MemPoolCleanUp(g_pstBufferPoolInfo);
  g_pstBufferPoolInfo = nullptr;
  return SUCCESS;
}

KStatus GetWaitThreadNum(kwdbContext_p ctx, void *resp) {
  // Get wait thread num
  k_uint32 wait_threads = ExecPool::GetInstance().GetWaitThreadNum();
  // Prepare response
  auto *return_info = static_cast<ThreadInfo *>(resp);
  if (return_info == nullptr) {
    LOG_ERROR("invalid resp pointer")
    return KStatus::FAIL;
  }
  return_info->wait_threads = wait_threads;
  return KStatus::SUCCESS;
}
}  // namespace kwdbts
