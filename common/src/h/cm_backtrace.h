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

#ifndef COMMON_SRC_H_CM_BACKTRACE_H_
#define COMMON_SRC_H_CM_BACKTRACE_H_

namespace kwdbts {
/**
 * @brief Dump all thread backtrace to file when receive signal SIGUSER1.
 * @param[in]  folder         dump folder path
 * @param[in]  nowTimeStamp   file generate timestamp
 * @return  void
*/
bool DumpAllThreadBacktrace(char* folder, char* nowTimeStamp);

/**
 * @brief Register dump stack backtrace signal SIGUSER1 handler.
 * @return  void
*/
void RegisterBacktraceSignalHandler();

}  // namespace kwdbts

#endif  // COMMON_SRC_H_CM_BACKTRACE_H_
