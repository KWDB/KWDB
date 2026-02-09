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

#ifndef COMMON_SRC_H_CM_KWDB_CONTEXT_H_
#define COMMON_SRC_H_CM_KWDB_CONTEXT_H_

#include <assert.h>
#include <string.h>
#include <shared_mutex>
#include <memory>
#include <vector>
#include "kwdb_type.h"
#include "er_stack.h"

namespace kwdbts {

enum class ContextOwner:unsigned int{
  UnClassified = 0,
  GoCoRoutine,
  ThreadPool
};

enum class ThreadLabel {
  None = 0,   // no label
  Master,     // for a main thread in a ThreadPool
  Slave       // for pool threads in a ThreadPool
};

class DmlExec;

typedef struct _kwdbContext_t: public _KContext_t {
  _kwdbContext_t(
    ContextOwner thread_owner = ContextOwner::UnClassified) :
    _KContext_t(),
    thread_id(0),
    logNestCount(0),
    owner(thread_owner),
    label(ThreadLabel::None) {
    err_stack.Reset();
  }

  ~_kwdbContext_t() {
    thread_id = 0;
    err_stack.Reset();
  }

  void reset() {
    max_stack_size = {0};
    frame_level = {0};
    connection_id = {0};
    timezone = {8};
    b_is_cancel = nullptr;
    for (int i = 0; i < CTX_MAX_FRAME_LEVEL; i++) {
      memset(file_name[i], '\0', CTX_MAX_FILE_NAME_LEN);
      memset(func_name[i], '\0', CTX_MAX_FILE_NAME_LEN);
      func_start_time[i] = 0;
      func_end_time[i] = 0;
    }
    memset(msg_buf, '\0', 256);
    snprintf(assert_file_name, sizeof(assert_file_name), "./kwdb_assert.txt");
    thread_id = {0};
    logNestCount = {0};
    err_stack.Reset();
    owner = ContextOwner::UnClassified;
    label = ThreadLabel::None;
  }


  // for concurrency control in log file handling
  std::shared_mutex mutex;
  KThreadID thread_id;      // thread_id in threadpool
  k_uint32 logNestCount;    // the num of entry log(), avoid nesting call log().
  KwdbErrorStack err_stack;
  ContextOwner owner;        // indicating the main category
  ThreadLabel label;         // indicating associated thread category
  void *ts_engine{nullptr};
  void *fetcher{nullptr};
  DmlExec* dml_exec_handle{nullptr};  // handle in DmlExec for multiple model processing
  k_bool *b_is_cancel{nullptr};
  k_bool wait_for_output{false};
} kwdbContext_t;
// Defines the context structure pointer type
typedef kwdbContext_t* kwdbContext_p;

// initialize context struct
KStatus InitKWDBContext(kwdbContext_p ctx);

// initialize main thread context struct
KStatus InitServerKWDBContext(kwdbContext_p ctx);

//
KStatus DestroyKWDBContext(kwdbContext_p ctx);

// define the way to find kwdbheader
#define KWDBHeader(ctx)   {ctx->DBHdr}

class ContextManager {
 public:
  static kwdbContext_p GetThreadContext(const ContextOwner& owner = ContextOwner::UnClassified);
  static void RemoveThreadContext();

  inline static void ResetThreadId() {
    s_thread_id_ = 0;
  }

  inline static void ResetThreadContextCache() {
    s_thread_ctx_cache_ = nullptr;
  }

  inline static void SetThreadId(k_uint16 id);
  static void SetThreadCache(const kwdbContext_p cache);

  static k_uint16 GetThreadId();
  inline static kwdbContext_p GetThreadCache() {
    return s_thread_ctx_cache_;
  }

 private:
  static thread_local k_uint16 s_thread_id_;
  static thread_local kwdbContext_p s_thread_ctx_cache_;
};

//
// class: UnClassifiedContextManager
//     manage thread contexts from k_free / k_malloc or main thread context which has no
//     owner context managers to be associated with.
//
class UnClassifiedContextManager {
  friend class ContextManager;

 private:
  ~UnClassifiedContextManager() {
    if (GetThreadContext() != nullptr) {
      RemoveThreadContext();
    }
  }
  static kwdbContext_p GetThreadContext();
  static void RemoveThreadContext();
  static void SetThreadDestroyer();

 private:
  static std::mutex s_mutex_;
  static k_int32 s_last_thread_idx_;
  static std::vector<std::shared_ptr<kwdbContext_t>> s_contexts_;
  static std::vector<k_uint32> s_to_thread_index_;
  static std::vector<k_uint32> s_to_thread_id_;
};

//
// class: CGoContextManager
//     manage a thread context regarding to a thread from a cgo caller.
//
class CGoContextManager {
  friend class ContextManager;

 private:
  static void RemoveThreadContext();
  static kwdbContext_p GetThreadContext();

 private:
  static std::mutex s_mutex_;
  static k_int32 s_max_connection_;
  static k_int32 s_last_thread_idx_;
  static std::vector<std::shared_ptr<kwdbContext_t>> s_contexts_;
  static std::vector<k_uint32> s_to_thread_index_;
  static std::vector<k_uint32> s_to_thread_id_;
};
}  //  namespace kwdbts

#endif  // COMMON_SRC_H_CM_KWDB_CONTEXT_H_

