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

#ifndef COMMON_SRC_H_CM_MODULE_H_
#define COMMON_SRC_H_CM_MODULE_H_

#include "kwdb_type.h"
namespace kwdbts {
enum KwdbModule {
  SU = 1, /*START UP MODULE*/
  CN, /*CONNECTION MODULE*/
  MM, /*MEMORY MODULE*/
  TH, /*THREAD POOL MODULE*/
  ST, /*STORAGE MODULE*/
  EE, /*EXECUTION MODULE*/
  SV, /*SERVER MODULE*/
  PS, /*PUB/SUB MODULE*/
  UN, /*UNKNOWN MODULE*/
  ME, /*METADATA MODULE*/
  CM, /*COMMON MODULE*/
  NW, /*NETWORK MODULE*/
  MA, /*MA THREAD MODULE*/
  KWSA, /*KWSA MODULE*/
  MQ, /*MESSAGE QUEUE MODULE*/
  MAX_MODULE
};

extern const char *KwdbModuleToStr(KwdbModule module);
extern KStatus GetModuleByName(const char *name, KwdbModule *module);
extern KStatus GetModuleByFileName(const char *file, KwdbModule *module);

#define ERR_MODULE_DEF(err_module_name, err_module) \
  {                                                                                           \
    err_module_name, err_module       \
  }
}  // namespace kwdbts

#endif  // COMMON_SRC_H_CM_MODULE_H_
