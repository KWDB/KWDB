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

#ifndef COMMON_SRC_H_LG_API_H_
#define COMMON_SRC_H_LG_API_H_

#include <unistd.h>
#include "cm_module.h"
#include "kwdb_compatible.h"
#include "lg_impl.h"
namespace kwdbts {

#define LOG_INIT(...) kwdbts::LOGGER.Init(__VA_ARGS__);
#define LOG_UPDATE(...) kwdbts::LOGGER.UpdateSettings(__VA_ARGS__); LOG_UPDATE_SPAN(__VA_ARGS__);
#define LOG_DESTORY() kwdbts::LOGGER.Destroy();
/*!
 * @brief Log output at INFO level
 * @param ... sunch as input for printf（fmt,...）
 */
#define LOG_INFO(...)                                                     \
  kwdbts::LOGGER.Log(kwdbts::INFO, gettid(), __FILE__, __LINE__, __VA_ARGS__); \
  LOG_INFO_SPAN(__VA_ARGS__);

/*!
 * @brief Log output at WARN level
 * @param ... sunch as input for printf（fmt,...）
 */
#define LOG_WARN(...)                                                     \
  kwdbts::LOGGER.Log(kwdbts::WARN, gettid(), __FILE__, __LINE__, __VA_ARGS__); \
  LOG_WARN_SPAN(__VA_ARGS__);

/*!
 * @brief Log output at ERROR level
 * @param ... sunch as input for printf（fmt,...）
 */
#define LOG_ERROR(...)                                                     \
  kwdbts::LOGGER.Log(kwdbts::ERROR, gettid(), __FILE__, __LINE__, __VA_ARGS__); \
  LOG_ERROR_SPAN(__VA_ARGS__);

/*!
 * @brief
 * Log output at FATAL level，Note that calling FATAL does not cause the process to exit, unlike the usual logging modules.
* This is mainly due to the fact that kwdb has no signal handling mechanism and exiting the process directly may cause unknown conditions. Consider adding this functionality in the future.
 * @param ... sunch as input for printf（fmt,...）
 */
#define LOG_FATAL(...)                                                     \
  kwdbts::LOGGER.Log(kwdbts::FATAL, gettid(), __FILE__, __LINE__, __VA_ARGS__); \
  LOG_ERROR_SPAN(__VA_ARGS__);

/*!
 * @brief Log output at DEBUG level, And in release mode does not output the log information in this mode
 * @param ... sunch as input for printf（fmt,...）
 */
#ifdef K_DEBUG
#define LOG_DEBUG(...) kwdbts::LOGGER.Log(kwdbts::DEBUG, gettid(), __FILE__, __LINE__, __VA_ARGS__)
#else
#define LOG_DEBUG(...)
#endif

}  // namespace kwdbts

#endif  // COMMON_SRC_H_LG_API_H_
