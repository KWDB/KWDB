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

#ifndef COMMON_SRC_H_LG_ITEM_H_
#define COMMON_SRC_H_LG_ITEM_H_

#include "cm_module.h"
#include "lg_severity.h"
#define DEVICE_LENGTH 16
namespace kwdbts {
enum ItemType{
  TYPE_LOG = 1 ,
  TYPE_TRACE
};
/*！
 * @class logItem
 * The three attributes of module, level and statement used to store the log are initialized by the constructor and then called by other log modules
 */
typedef struct LogItem {
  // The module in which log is triggered
  KwdbModule module;
  // log level
  LogSeverity severity;
  // The filename of the process from which the log was triggered
  char file_name[CTX_MAX_FILE_NAME_LEN];
  // The process name to which log belongs
  char device[DEVICE_LENGTH];
  // log the timestamp when it was fired
  KTimestamp time_stamp;
  // id of the thread in which the log is currently running, defaulting to 0 if not triggered in the thread
  KThreadID thread_id;
  // log The number of lines in the file when triggered
  k_uint16 line;
  // Single log size
  k_uint16 buf_size;
  // Connection id
  KConnId connID;
  // Used to distinguish between LOG information and TRACE information
  ItemType itemType;
  // log statement for the input
  char buf[0];
} LogItem;

}  // namespace kwdbts

#endif  // COMMON_SRC_H_LG_ITEM_H_
