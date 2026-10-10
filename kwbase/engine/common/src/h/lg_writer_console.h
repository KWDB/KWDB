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

#ifndef COMMON_SRC_H_LG_WRITER_CONSOLE_H_
#define COMMON_SRC_H_LG_WRITER_CONSOLE_H_

#include "cm_kwdb_context.h"
#include "lg_writer_file.h"

namespace kwdbts {
/**!
 * @brief show log on console
 */
class LogWriterConsole : public LogWriterFile {
 public:
  KStatus Write(const LogItem *pItem) override;
  KStatus Init() override;
  KStatus Destroy() override;
  LogWriterConsole() = default;
  explicit LogWriterConsole(LogConf *cfg) : LogWriterFile(cfg) {}
  ~LogWriterConsole() override = default;
 private:
  int pid_ = 0;
};
}  // namespace kwdbts

#endif  // COMMON_SRC_H_LG_WRITER_CONSOLE_H_
