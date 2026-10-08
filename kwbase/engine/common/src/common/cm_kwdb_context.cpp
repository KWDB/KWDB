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

#include <memory>
#include "cm_kwdb_context.h"
#include "cm_assert.h"
#include "th_kwdb_dynamic_thread_pool.h"
namespace kwdbts {

// initialize all memebers inside KContext
KStatus InitKContext(KContextP ctx) {
  ctx->frame_level = 0;
  for (int i = 0; i < CTX_MAX_FRAME_LEVEL; i++) {
    ctx->file_name[i][0] = '\0';
    ctx->func_name[i][0] = '\0';
    ctx->func_start_time[i] = 0;
    ctx->func_end_time[i] = 0;
  }

  ctx->max_stack_size = 0;
  ctx->timezone = 8;  // The default time zone is East 8
  ctx->relation_ctx = 0;
  return KStatus::SUCCESS;
}

// In order to avoid a lot of code changes, we decided to create a new InitServerKWDBContext() function
// with the same internal logic as the original InitKWDBContext(). The InitServerKWDBContext() function
// and SetOperatorContext() function of the thread pool are called in the InitKWDBContext function,
// and the initialized context is stored in the KWDBOperatorInfo of this thread.
// That is, InitKWDBContext() and InitServerKWDBContext() are applied to threads in the thread pool
// and the main thread respectively.

// initialize all members inside kwdbContext_t
KStatus InitKWDBContext(kwdbContext_p ctx) {
#ifdef WITH_TESTS
    Assert(ctx != nullptr);
#endif
  InitServerKWDBContext(ctx);
  KWDBDynamicThreadPool &kwdb_thread_pool_ctx = KWDBDynamicThreadPool::GetThreadPool();
  kwdb_thread_pool_ctx.SetOperatorContext(ctx);
  return KStatus::SUCCESS;
}

// initialize all members inside kwdbContext_t in the main thread
KStatus InitServerKWDBContext(kwdbContext_p ctx) {
  if (InitKContext(ctx) == KStatus::FAIL) {
    return KStatus::FAIL;
  }

  // ctx->conn = nullptr;
  // ctx->connArray->conn = nullptr;
  // init your thread_id, connection_id, and so on
  ctx->thread_id = 0;
  ctx->connection_id = 0;
  ctx->logNestCount = 0;
  return KStatus::SUCCESS;
}

KStatus DestroyKWDBContext(kwdbContext_p ctx) {
  return KStatus::SUCCESS;
}
}  //  namespace kwdbts
