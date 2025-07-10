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

#include "er_cfg.h"

#include <map>

namespace kwdbts {

/**!
 * code：        Error code corresponding to the error
 * err_msg：     Error message corresponding to the error code.
 * num_tokens：  The number of placeholders in a predefined error message.
 * action：      Action corresponding to this error.
 * reason：      Cause of this error.
 * internal_actions_taken：The action taken internally by the system to handle this error.
 */
static std::map<KwdbErrCode, KWDBErrorInfo_t> kSErrorInfoMap = {
    ERROR_INFO_DEF(CN_INVALID_MESSAGE_TYPE,
                   1, "use legal method to create your message", "unsupported message type", ""),
    ERROR_INFO_DEF(
        CN_INVALID_STARTUP_PACKET, 0,
        "use legal method to connect server please", "invalid startup packet layout", ""),
    ERROR_INFO_DEF(CN_PROTOCOL_NOT_SUPPORTED, 2, "contacting the developer",
                   "unsupported frontend protocol", ""),
    ERROR_INFO_DEF(CN_INVALID_PASSWORD, 1,
                   "try again", "username, token or password dosen't match", ""),

    /*class SY - system exception info*/
    ERROR_INFO_DEF(SY_TH_THREAD_ID_ERR, 0, "no action required",
                   "The thread_id is not used by the thread pool.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_ID_NOT_FIND, 0, "The thread pool is faulty.",
                   "The current thread was not created by the thread pool.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_ID_HASH_ERR, 0, "no action required",
                   "The hash value of the current thread is incorrect.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_INIT_ERR, 0, "no action required",
                   "Initialize the thread pool repeatedly.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_STATUS_ERR, 1, "no action required",
                   "he thread is in an abnormal state.", ""),
    ERROR_INFO_DEF(SY_TH_POOL_STOP_ERR, 0, "no action required",
                   "The thread pool ended repeatedly.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_POOL_STOP, 0, "no action required",
                   "The thread pool stopped and the thread could not continue operations.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_START_ERR, 1, "try again",
                   "Failed to create a new thread.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_STATUS_STOP, 1, "no action required",
                   "The thread is stopped.", ""),
    ERROR_INFO_DEF(SY_TH_THREAD_STOP_ERR, 1, "no action required",
                   "Failed to stop the thread.", ""),
    ERROR_INFO_DEF(SY_TH_MAP_INSERT_ERR, 1, "no action required",
                   "The thread pool could not record threads created.", ""),

    /*class IN - internal error info*/
    ERROR_INFO_DEF(IN_INIT_ERR, 1,
                   "try again", "Failed to initialize object", ""),
    ERROR_INFO_DEF(IN_EXEC_ERR, 1,
                   "try again", "internal execution error", ""),
    ERROR_INFO_DEF(OT_TH_THREAD_POOL_FULL_ERR, 0, "", "out of memory", "try again"),

};

/**!
 * code：    Error code corresponding to the error.
 * pg_code： PgCode corresponding to Error code.
 */
static std::map<KwdbErrCode, const char *> kPgCodeMap = {
    /*KwdbErrCode maps to pgCode*/
    PG_CODE_DEF(CN_INVALID_MESSAGE_TYPE, "08P01"),
    PG_CODE_DEF(CN_INVALID_STARTUP_PACKET, "08P01"),
    PG_CODE_DEF(CN_PROTOCOL_NOT_SUPPORTED, "0A000"),
    PG_CODE_DEF(CN_INVALID_PASSWORD, "28P01"),
};


/**
 * @brief This function is used to get the defined err_msg from kSErrorInfoMap according to KwdbErrCode.
 * If err_msg corresponding to KwdbErrCode is not retrieved in kSErrorInfoMap, then a "" is returned.
 * @param[in] err_code : Error code defined in KwdbErrCode.
 * @return[out] Return the retrieved err_msg corresponding to KwdbErrCode or return a "".
 */
extern const char *GetFmtStrByErrCode(KwdbErrCode err_code) {
  auto it = kSErrorInfoMap.find(err_code);
  if (it == kSErrorInfoMap.end()) {
    // No error fmt is defined
    return "";
  }
  return it->second.err_msg;
}

/**
 * @brief ErrCode2PgCode
 * @param[in] err_code : Error code defined in KwdbErrCode.
 * @param[in] buf : Holds the output buf of the formatted KwdbError Error Code string.
 * @param[in] size : The maximum capacity of the output buf.
 * @return[out] Returns a KStatus that checks whether the error code was successfully converted to PgCode.
 */
extern KStatus ErrCode2PgCode(KwdbErrCode err_code, char *buf,
                                    k_uint32 size) {
  k_int32 len = 0;
  auto it = kPgCodeMap.find(err_code);
  if (it == kPgCodeMap.end()) {
    // Return an error code starting with KW
    len = snprintf(buf, size, "%s%03d", "KW", err_code % 1000);
  } else {
    // Return PgCode
    len = snprintf(buf, size, "%s", it->second);
  }
  return len <= size ? SUCCESS : FAIL;
}

/**
 * @brief Get ErrInfo By ErrCode.
 * @param[in] err_code : Error code defined in KwdbErrCode.
 * @return[out]  The KWDBErrorInfo_t defined in kSErrorInfoMap.
 */
extern const KWDBErrorInfo_t &GetErrInfoByErrCode(KwdbErrCode err_code) {
  return kSErrorInfoMap.find(err_code)->second;
}

}  // namespace kwdbts
