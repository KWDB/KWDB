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

#ifndef COMMON_SRC_H_ER_CFG_H_
#define COMMON_SRC_H_ER_CFG_H_

#include "cm_module.h"
#include "kwdb_type.h"

namespace kwdbts {

// Error code will be classified in different classes based on the type of error
// users see. each class will use one contiguous section of positive integers which
// later will be translated into distinct user error code.

typedef enum KwdbErrCode {
  // success code 0
  SUCCESS_CODE = 0,

  // each section start with first two letters of error class

  /*class CN - connection error*/
  CN_FIRST_ERR = 1000,              // Preserved
  CN_INVALID_MESSAGE_TYPE,          // pgcode 08P01
  CN_INVALID_STARTUP_PACKET,        // pgcode 08P01
  CN_PROTOCOL_NOT_SUPPORTED,        // pgcode 0A000
  CN_INVALID_PASSWORD,              // pgcode 28P01
  CN_LAST_ERR,

  /*class SY - system exception*/
  SY_FIRST_ERR = 1300,
  SY_TH_THREAD_ID_ERR,                //  Invalid thread ID
  SY_TH_THREAD_ID_NOT_FIND,           //  A thread created outside the thread pool
  SY_TH_THREAD_ID_HASH_ERR,           //  A thread created outside the thread pool
  SY_TH_THREAD_INIT_ERR,              //  Description Failed to initialize the thread pool
  SY_TH_THREAD_STATUS_ERR,            //  Thread state exception
  SY_TH_POOL_STOP_ERR,                //  Error ending thread pool
  SY_TH_THREAD_POOL_STOP,             //  The thread pool has been stopped, and the thread cannot continue further operations
  SY_TH_THREAD_START_ERR,             //  Thread creation error
  SY_TH_THREAD_STATUS_STOP,           //  The thread is in the stop state, and no further operations can be performed
  SY_TH_THREAD_STOP_ERR,              //  End thread error
  SY_TH_MAP_INSERT_ERR,               //  The thread pool could not record threads
  SY_LAST_ERR,

  /*class IN - internal error*/
  IN_FIRST_ERR = 1800,
  IN_INIT_ERR,                 // Internal initialization failed
  IN_EXEC_ERR,                 // An internal execution error was reported
  IN_LAST_ERR,

  /*class OT - other error*/
  OT_FIRST_ERR = 2000,
  OT_TH_THREAD_POOL_FULL_ERR,

  KWDB_ERROR_LAST = 3000,
} KwdbErrCode;

/*class CN - connection error msg*/
#define _CHECKER_CN_INVALID_MESSAGE_TYPE "invalid request (unknown message type %d)"
#define _CHECKER_CN_INVALID_STARTUP_PACKET \
"invalid startup packet layout: expected terminator as last byte"
#define _CHECKER_CN_PROTOCOL_NOT_SUPPORTED "protocol %u.%u is not supported"
#define _CHECKER_CN_INVALID_PASSWORD "authentication failed for user %s"

/*class SY - system exception msg*/
#define _CHECKER_SY_TH_THREAD_ID_ERR "thread id out of range!"
#define _CHECKER_SY_TH_THREAD_ID_NOT_FIND "The current thread is not recorded!"
#define _CHECKER_SY_TH_THREAD_ID_HASH_ERR "The hash value of the thread is incorrect."
#define _CHECKER_SY_TH_THREAD_INIT_ERR "thread pool cannot init twice!"
#define _CHECKER_SY_TH_THREAD_STATUS_ERR "The thread(id:%ld) is in an abnormal state."
#define _CHECKER_SY_TH_POOL_STOP_ERR "Close the thread pool repeatedly!"
#define _CHECKER_SY_TH_THREAD_START_ERR "The thread(id:%ld) is in an abnormal state."
#define _CHECKER_SY_TH_THREAD_STATUS_STOP "The thread(id:%ld) is already in the stop state."
#define _CHECKER_SY_TH_THREAD_STOP_ERR "Failed to terminate the thread(id:%ld)."
#define _CHECKER_SY_TH_MAP_INSERT_ERR "The thread pool could not log a new thread, id = %ld."
#define _CHECKER_SY_TH_THREAD_POOL_STOP "The thread pool has stopped and the thread is about to stop."

/*class IN - internal error msg*/
#define _CHECKER_IN_INIT_ERR "Failed to initialize object(%s)\n"
#define _CHECKER_IN_EXEC_ERR "internal execution error (%s)\n"

/*thread pool error msg*/
#define  _CHECKER_OT_TH_THREAD_POOL_FULL_ERR "all threads are busy!"

#define _CHECKER_UN_TYPE_ERR "Unknown type"

/**
 * code：        Error code corresponding to the error.
 * err_msg：     Error message corresponding to the error code.
 * num_tokens：  The number of placeholders in a predefined error message.
 * action：      Action corresponding to this error.
 * reason：      Cause of this error.
 * internal_actions_taken：The action taken internally by the system to handle this error.
 */
typedef struct KWDBErrorInfo {
  KwdbErrCode code;
  char *err_msg;
  k_uint8 num_tokens;
  char *action;
  char *reason;
  char *internal_actions_taken;
} KWDBErrorInfo_t;

#define ERROR_INFO_DEF(err_code, num_tokens, action, reason, internal_actions_taken) \
  {                                                                                           \
    err_code, { err_code, const_cast<char*>(_CHECKER_##err_code), num_tokens, \
    const_cast<char*>(action), \
    const_cast<char*>(reason), const_cast<char*>(internal_actions_taken) }       \
  }

#define PG_CODE_DEF(err_code, pg_code) \
  {                                                                                           \
    err_code, pg_code       \
  }

extern const char *GetFmtStrByErrCode(KwdbErrCode err_code);
extern KStatus ErrCode2PgCode(KwdbErrCode err_code, char *buf, k_uint32 size);
extern const KWDBErrorInfo_t &GetErrInfoByErrCode(KwdbErrCode err_code);

}  // namespace kwdbts
#endif  // COMMON_SRC_H_ER_CFG_H_
