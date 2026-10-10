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

#ifndef COMMON_SRC_H_LG_WRITER_H_
#define COMMON_SRC_H_LG_WRITER_H_

#include "cm_kwdb_context.h"
#include "lg_item.h"
#include "lg_config.h"
#include "kwdb_type.h"
namespace kwdbts {
/**!
 * @class An abstract class for logging output
 */
class LogWriter {
 public:
  /**!
   * @brief Output logging interface function
   * @param[in] pItem The output day entry needs to free the memory space pointed to by pItem within write
   * @return SUCCESS or FAIL
   */
  explicit LogWriter(LogConf *cfg) : cfg(cfg) {}
  virtual KStatus Write(const LogItem *pItem) = 0;
  /**!
   * @brief Init writer
   * @param[in] ctx
   * @return SUCCESS or FAIL
   */
  virtual KStatus Init() = 0;
  /**!
   * @brief destroy
   * @return SUCCESS or FAIL
   */
  virtual KStatus Destroy() = 0;
  virtual ~LogWriter() = default;
  k_bool inited_ = false;
  LogConf *cfg;
};
}  // namespace kwdbts

#endif  // COMMON_SRC_H_LG_WRITER_H_
