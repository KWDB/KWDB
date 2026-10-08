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

#include "lg_impl.h"

#include <lg_item.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <cstdarg>
#include <cstring>
#include <mutex>
#include <algorithm>

#include "cm_config.h"
#include "cm_fault_injection.h"
#include "cm_func.h"
#include "lg_writer_console.h"

namespace kwdbts {

std::unordered_map<std::string, LogSeverity> LogLevelMap = {
  {"DEBUG", LogSeverity::DEBUG},
  {"INFO", LogSeverity::INFO},
  {"WARN", LogSeverity::WARN},
  {"ERROR", LogSeverity::ERROR},
  {"FATAL", LogSeverity::FATAL}
};

KStatus Logger::Init() { return KStatus::SUCCESS; }

KStatus Logger::Init(LogConf cfg) {
  cfg_.file_max = cfg.file_max <= 0 ? LOG_FILE_DEFAULT_SIZE : cfg.file_max;
  cfg_.path_max = cfg.path_max <= 0 ? LOG_FILE_DEFAULT_SIZE << 5 : cfg.path_max;
  cfg_.path = cfg.path;
  cfg_.level = cfg.level;
  if (LOGGER_STATUS_ != INIT) {
    return (FAIL);
  }
  if (std::string(cfg_.path) == "") {
    writer_type_ = CONSOLE;
  } else {
    writer_type_ = FILEWRITER;
  }
  m_log_serverity_ = cfg.level;
  LOGGER_STATUS_ = RUN;
  return (SUCCESS);
}

KStatus Logger::UpdateSettings(void* args) {
  // 1. reopen(file)
  // 2. set_Log_Kap
  return (SUCCESS);
}

KStatus Logger::Destroy() {
  writerDestroy();
  LOGGER_STATUS_ = STOP;
  return (SUCCESS);
}
Logger::~Logger() = default;

void Logger::writerInit() {
  // if first log call in diffrent thread quikckly,
  // maybe new twice. try lock. get lock failed then write
  if (log_mtx_.try_lock()) {
    switch (writer_type_) {
    case FILEWRITER:
      log_writer_ = new LogWriterFile(&cfg_);
      break;
    default:
      log_writer_ = new LogWriterConsole(&cfg_);
      break;
    }

    log_writer_->Init();
    writer_inited_ = true;
    log_mtx_.unlock();
  } else {  // wait for init writer finished
    log_mtx_.lock();
    log_mtx_.unlock();
  }
}
void Logger::writerDestroy() {
  if (writer_inited_ == true) {
    if (log_mtx_.try_lock()) {
      if (log_writer_ != nullptr) {
        log_writer_->Destroy();
        delete log_writer_;
        log_writer_ = nullptr;
      }
      log_mtx_.unlock();
    } else {
      log_mtx_.lock();
      log_mtx_.unlock();
    }
  }
}

void Logger::SetModuleSeverity(KwdbModule mod, LogSeverity severity) { severity_of_module_[mod] = severity; }

/**
   * @brief combine msg info to LogItem and use writer to write. if writer not inital,init it
   * @param severity this log msg's level
   * @param file  call LOGXXX's file name
   * @param line  call LOGXXX's line number
   * @param fmt   call LOGXXX's format info,maybe null
   * @param ...   call LOGXXX's msg contents
 */
void Logger::Log(LogSeverity severity, pid_t tid, const char *file, k_uint16 line, const char *fmt, ...) {
  // Init() routine try to modify isInit_, so double check isInit_ here.
  if (LOGGER_STATUS_ == INIT || LOGGER_STATUS_ == STOP) {
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    printf("\n");
    return;
  }
  if (log_writer_ == nullptr) {
    writerInit();
  }
  if (nullptr == file || nullptr == fmt) {
    return;
  }

  KwdbModule mod = UN;

  if (FAIL == GetModuleByFileName(file, &mod)) {
    mod = UN;  // even unknown file module need to LOG whatever
  }
  va_list args;
  va_start(args, fmt);
  if (KStatus::FAIL == log(mod, tid, severity, GetFileName(file), line, fmt, args)) {
    vprintf(fmt, args);
    printf("\n");
  }
  va_end(args);
}

KStatus Logger::log(KwdbModule mod, pid_t tid, LogSeverity severity, const char *file, k_uint16 line, const char *fmt,
              va_list args) {
  if (!IsEnableFor(mod, severity)) {
    return KStatus::SUCCESS;
  }

  k_uint16 len = getFmtSize(fmt, args);
  auto pItem = static_cast<LogItem *>(malloc(sizeof(LogItem) + len));
  INJECT_DATA_FAULT(FAULT_LOG_MALLOC_FAIL, pItem, nullptr, nullptr);
  if (nullptr == pItem) {
    return KStatus::FAIL;
  }
  logItemFill(mod, severity, tid, file, line, fmt, args, len, pItem);
  KStatus status = KStatus::SUCCESS;
  log_mtx_.lock();
  status = log_writer_->Write(pItem);
  log_mtx_.unlock();
  free(pItem);
  return status;
}

/*!
   * @brief
   * reused for TRACE. 
   * @param[in] item trace combined logItem. only format Item to logfile
   * @return Returns KTRUE on success and KFALSE otherwise
 */
KStatus Logger::LogItemToFile(LogItem *pItem) {
  KStatus status = KStatus::SUCCESS;
  if (nullptr != log_writer_) {
    log_mtx_.lock();
    log_writer_->Write(pItem);
    log_mtx_.unlock();
    if (KStatus::FAIL == status) {
      return SUCCESS;
    }
  }
  return status;
}

k_uint16 Logger::getFmtSize(const char *fmt, va_list args) const {
  va_list tmp_args;
  va_copy(tmp_args, args);
  k_uint32 len = vsnprintf(nullptr, 0, fmt, tmp_args) + 1;
  va_end(tmp_args);
  return static_cast<k_uint16>(len);
}

void Logger::logItemFill(const KwdbModule &mod, const LogSeverity &severity, int tid, const char *file,
                         k_uint16 line, const char *fmt, va_list args, k_uint16 len, LogItem *pItem) const {
  memset(pItem, 0, sizeof(LogItem) + len);

  pItem->module = mod;
  pItem->severity = severity;
  snprintf(pItem->file_name, sizeof(pItem->file_name), "%s", file);
  pItem->line = line;
  pItem->thread_id = tid;
  struct timeval time_secs;
  gettimeofday(&time_secs, nullptr);
  pItem->time_stamp = time_secs.tv_sec * 1000000 + time_secs.tv_usec;
#ifdef WITH_TESTS
  pItem->time_stamp = 0;
#endif
  pItem->buf_size = len;
  pItem->itemType = TYPE_LOG;
  snprintf(pItem->device, sizeof(pItem->device), "%s", device_.c_str());
  vsnprintf(pItem->buf, pItem->buf_size, fmt, args);
}

LogSeverity Logger::getLogLevelByName(std::string&& log_level) {
  auto it = LogLevelMap.find(log_level);
  if (it != LogLevelMap.end()) {
    return it->second;
  }
  return LogSeverity::DEBUG;
}

LogSeverity Logger::getLogLevelByMod(KwdbModule mod, KString *str) {
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "%s", str->c_str());

  char level[2] = {};
  const char *sep1 = ",";
  const char *sep2 = " ";
  char *modAndLevel, *brkt, *brkb, *subMod;
  kwdbts::k_int32 count = 0;
  for (modAndLevel = strtok_r(tmp, sep1, &brkt); modAndLevel != nullptr; modAndLevel = strtok_r(nullptr, sep1, &brkt)) {
    if (nullptr != strstr(modAndLevel, KwdbModuleToStr(mod))) {
      count = 0;
      for (subMod = strtok_r(modAndLevel, sep2, &brkb); subMod != nullptr; subMod = strtok_r(nullptr, sep2, &brkb)) {
        if (count == 1) {
          if (strlen(subMod) > 1) {
            return WARN;
          }
          snprintf(level, sizeof(level), "%s", subMod);
        }
        count++;
      }
      k_int32 ret = atoi(level);
      if (ret > 4 || ret < 0) {
        return WARN;
      }
      switch (ret) {
        case 1:
          return INFO;
        case 2:
          return WARN;
        case 3:
          return ERROR;
        case 4:
          return FATAL;
        default:
          return DEBUG;
      }
    }
  }
  return WARN;
}

k_bool Logger::SetLogModLevel() {
  return true;
}

}  // namespace kwdbts
