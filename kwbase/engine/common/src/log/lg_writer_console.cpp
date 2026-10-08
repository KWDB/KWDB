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

#include "lg_writer_console.h"
#include <sys/msg.h>
#include "cm_func.h"
#include "cm_config.h"

namespace kwdbts {
KStatus LogWriterConsole::Write(const LogItem *pItem) {
  if (nullptr == pItem) {
    return FAIL;
  }
  KStatus ret = FormatAndOut(pItem);
  return ret;
}
KStatus LogWriterConsole::Init() {
  p_file_ = stdout;
  inited_ = true;
  return (SUCCESS);
}
KStatus LogWriterConsole::Destroy() {
  return KStatus::SUCCESS;
}
}  // namespace kwdbts
