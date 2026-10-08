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

#ifndef COMMON_SRC_H_LG_CONFIG_H_
#define COMMON_SRC_H_LG_CONFIG_H_

#include "kwdb_type.h"
#include <string>

namespace kwdbts {

typedef struct LogConfig {
  std::string path;
  int64_t file_max;
  int64_t path_max;
  LogSeverity level;
} LogConf;

}  // namespace kwdbts
#endif  // COMMON_SRC_H_LG_CONFIG_H_
