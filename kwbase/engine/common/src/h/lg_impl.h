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

#ifndef COMMON_SRC_H_LG_IMPL_H_
#define COMMON_SRC_H_LG_IMPL_H_
#include <atomic>
#include <memory>
#include <unordered_map>
#include <string>
#include "cm_kwdb_context.h"
#include "cm_module.h"
#include "cm_trace_plugin.h"
#include "kwdb_type.h"
#include "lg_severity.h"
#include "lg_writer.h"
#include "lg_config.h"
enum writerType {
  OFF = 0,
  FILEWRITER,
  CONSOLE,
};

namespace kwdbts {
/*!
 * @brief Logger Log management module
 */
extern std::unordered_map<std::string, LogSeverity> LogLevelMap;

class Logger {
 public:
  Logger(Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  /*!
   * @brief Sets the logging level at which the module is enabled
   * @param[in] module
   * @param[in] severity
   * @return Log level before modification by the set module
   */
  void SetModuleSeverity(KwdbModule mod, LogSeverity severity);

  /*!
   * @brief
   * Get an instance of the logging module. The logging module uses singleton mode, all calls to this function get the same Logger instance
   * @return Logger Instance
   */
  inline static Logger &GetInstance() {
    static Logger instance;
    return instance;
  }

  /*!
   * @brief
   * Initialize the logging module, which must be called before it can be used and can only be called once during the lifetime of the process.
   * @param[in] ctx
   * @return Returns KTRUE on success and KFALSE otherwise
   */
  KStatus Init();

  /*!
   * @brief
   * initial of LogModule @static Logger::GetInstance()
   * called in engine/engine.cpp
   * use in constructor function, do not new/malloc writer!!!
   * @param[in] cfg LogConf{
      * @param[in] path log file output path. which need created before called Init
      * @param[in] file_max single log file max size. just passed on kwbase start options. noused now
      * @param[in] path_max  combine log files size. just passed on kwbase start options. noused now
      * @param[in] level log to file only LOG_XXX's severity greater than level
   * }
   * @return Returns KTRUE on success and KFALSE otherwise
   */
  KStatus Init(LogConf cfg);
  /*!
   * @brief
   * TODO(lsy):
   * if cluster settings worked. UpdateSettings can be extended for usage. no used now.
   * also can be extended when cluster settings OnChanged registry
   * maybe log_file changed. need reopen or KAP... or extend dir max size .finish this function
   * @param[in] args want kv params or map: [cluster_settings]settings_value
   * @return Returns KTRUE on success and KFALSE otherwise
   */
  KStatus UpdateSettings(void* args);
  // Destroy It cleans up resources and closes links
  KStatus Destroy();
  /*!
   * @brief
   * The output level and output module of the log are limited
   * @param kwdb_module
   * @param severity
   * @return Returns KTRUE on success and KFALSE otherwise
   */
  k_bool IsEnableFor(KwdbModule kwdb_module, LogSeverity severity) {
    return severity >= m_log_serverity_;
    //return severity >= severity_of_module_[kwdb_module];
  }
  /**
   * @brief combine msg info to LogItem and use writer to write. if writer not inital,init it
   * @param severity this log msg's level
   * @param file  call LOGXXX's file name
   * @param line  call LOGXXX's line number
   * @param fmt   call LOGXXX's format info,maybe null
   * @param ...   call LOGXXX's msg contents
   */
  void Log(LogSeverity severity, pid_t tid, const char *file, k_uint16 line, const char *fmt, ...)
      __attribute__((format(printf, 6, 7)));
  /*!
   * @brief
   * reused for TRACE. 
   * @param[in] item trace combined logItem. only format Item to logfile
   * @return Returns KTRUE on success and KFALSE otherwise
   */
  KStatus LogItemToFile(LogItem *item);

  /*!
   * @brief
   * get log dir path. 
   * @param[in] item trace combined logItem. only format Item to logfile
   * @return return path value
   */
  std::string LogRealPath() const {
    return cfg_.path;
  }
#ifndef WITH_TESTS

 private:
#endif
  Logger() = default;
  ~Logger();
  std::mutex log_mtx_;
  enum LoggerStatus : k_int32 {
    INIT = 0,
    RUN,
    STOP,
  };
  std::atomic<LoggerStatus> LOGGER_STATUS_ = INIT;
  k_int32 writer_type_ = OFF;
  KString device_;
  LogSeverity m_log_serverity_ = LogSeverity::DEBUG;
  LogSeverity severity_of_module_[MAX_MODULE + 1];
  LogWriter* log_writer_ = nullptr;
  k_bool writer_inited_ = false;
  LogConf cfg_;
  void logItemFill(const KwdbModule &mod, const LogSeverity &severity, int tid, const char *file,
                   k_uint16 line, const char *fmt, va_list args, k_uint16 len, LogItem *pItem) const;
  KStatus log(KwdbModule mod, pid_t tid, LogSeverity severity, const char *file, k_uint16 line, const char *fmt,
              va_list args);
  k_uint16 getFmtSize(const char *fmt, va_list args) const;
  LogSeverity getLogLevelByMod(KwdbModule mod, KString *str);
  LogSeverity getLogLevelByName(std::string&& log_level);
  void writerInit();
  void writerDestroy();

#ifdef WITH_TESTS

 public:
#endif
  k_bool SetLogModLevel();
};

#define LOGGER Logger::GetInstance()

}  // namespace kwdbts
#endif  // COMMON_SRC_H_LG_IMPL_H_
