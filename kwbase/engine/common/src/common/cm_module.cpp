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

#include <map>
#include "cm_module.h"
#include "cm_func.h"

namespace kwdbts {

static const char *module_to_str[MAX_MODULE] = {"su", "cn", "mm", "th", "st", "ee",
                                      "sv", "ps", "un", "me", "cm",
                                      "nw", "ma", "kwsa", "mq"};

static std::map<KString, KwdbModule> kSErrModuleNameMap = {
    ERR_MODULE_DEF("su", SU),
    ERR_MODULE_DEF("cn", CN),
    ERR_MODULE_DEF("mm", MM),
    ERR_MODULE_DEF("mq", MQ),
    ERR_MODULE_DEF("th", TH),
    ERR_MODULE_DEF("st", ST),
    ERR_MODULE_DEF("ee", EE),
    ERR_MODULE_DEF("sv", SV),
    ERR_MODULE_DEF("ps", PS),
    ERR_MODULE_DEF("un", UN),
    ERR_MODULE_DEF("me", ME),
    ERR_MODULE_DEF("cm", CM),
    ERR_MODULE_DEF("nw", NW),
    ERR_MODULE_DEF("ma", MA),
    ERR_MODULE_DEF("kwsa", KWSA),
};

extern const char *KwdbModuleToStr(KwdbModule module) {
  return module_to_str[module - SU];
}

extern KStatus GetModuleByName(const char *name, KwdbModule *module) {
  if (nullptr == name || nullptr == module) {
    return KStatus::FAIL;
  }
  if (kSErrModuleNameMap.find(name) == kSErrModuleNameMap.end()) {
    return KStatus::FAIL;
  }
  *module = kSErrModuleNameMap.find(name)->second;
  return KStatus::SUCCESS;
}

KStatus GetModuleNameByFileName(const char *file_with_path, char *buf,
                                k_int32 size) {
  const char *file_name = GetFileName(file_with_path);
  if (nullptr == file_name || nullptr == buf || size < 5) {
    return KStatus::FAIL;
  }
  // Temporary fix to the issue that the abbreviation of the module name
  // cannot be recognized after bo is changed to kwsa
  if (file_name[0] == 'k' && file_name[1] == 'w' && file_name[2] == 's') {
    buf[0] = 'k';
    buf[1] = 'w';
    buf[2] = 's';
    buf[3] = 'a';
    buf[4] = 0;
    return KStatus ::SUCCESS;
  }
  if ('_' != file_name[2]) {
    buf[0] = 'u';
    buf[1] = 'n';
    buf[2] = 0;
  } else {
    buf[0] = *(file_name + 0);
    buf[1] = *(file_name + 1);
    buf[2] = 0;
  }
  return KStatus::SUCCESS;
}

extern KStatus GetModuleByFileName(const char *file, KwdbModule *module) {
  if (nullptr == file || nullptr == module) {
    return KStatus::FAIL;
  }
  // Remove path
  char temp_buf[8] = {0};
  if (KStatus::FAIL ==
      GetModuleNameByFileName(file, temp_buf, sizeof(temp_buf))) {
    return KStatus::FAIL;
  }
  return GetModuleByName(temp_buf, module);
}

}  // namespace kwdbts
