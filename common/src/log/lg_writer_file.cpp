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

#include "lg_writer_file.h"

#include <sys/stat.h>
#include <sys/time.h>

#include <cstdio>

#include "cm_config.h"
#include "cm_func.h"
#include "cm_fault_injection.h"

namespace kwdbts {

#define FILE_MAX_NUM 20
#define FILE_MAX_SIZE 50
#define FILE_NAME_LEN 100

KStatus LogWriterFile::Write(const LogItem *pItem) {
  INJECT_DATA_FAULT(FAULT_LOG_PITEM_NOT_NULL_FAIL, pItem, nullptr, nullptr);
  if (nullptr == pItem) {
    return (FAIL);
  }
  INJECT_DATA_FAULT(FAULT_LOG_FILE_EXIT_FAIL, p_file_, nullptr, nullptr);
  if (nullptr != p_file_ && ftell(p_file_) > cfg->file_max) {
    if (!RolloverLogfile()) {
      return (FAIL);
    }
  }
  KStatus ret = FormatAndOut(pItem);
  return (ret);
}

KStatus LogWriterFile::FormatAndOut(const LogItem *pItem) {
  if (nullptr == p_file_) {
    p_file_ = stdout;
    return FAIL;
  } else if (nullptr == pItem) {
    return FAIL;
  }

  // Add module and level identification to the message
  struct tm tm{};
  time_t sec = pItem->time_stamp / 1000000;
  k_int32 usec = pItem->time_stamp % 1000000;
  gmtime_r(&sec, &tm);
#ifdef WITH_TESTS
  ToGMT(sec, tm);
#endif
  // [D/I/W/E/F][YYMMDD HH:MM:SS.usec] [gettid()] [file:line] LogMsg
  fprintf(p_file_, "%s%02d%02d%2d %02d:%02d:%02d.%06d %ld %s:%d %s\n",
              SeverityToStr(pItem->severity),
              tm.tm_year%100, tm.tm_mon + 1, tm.tm_mday,
              tm.tm_hour, tm.tm_min, tm.tm_sec, usec,
              pItem->thread_id,
              pItem->file_name, pItem->line,
              pItem->buf);

  fflush(p_file_);
  return SUCCESS;
}

void LogWriterFile :: InitLogFile() {
  rto_f_.lock();
  log_symlink_name_ = LOG_SYMLINK_NAME(cfg->path);
  char fname[FILE_NAME_LEN];
  MakeLogFileName(fname, FILE_NAME_LEN);
  snprintf(log_dir_, sizeof(log_dir_), "%s/%s", cfg->path.c_str(), fname);
  std::ignore = symlink(fname, log_symlink_name_.c_str());
  p_file_ = fopen(log_symlink_name_.c_str(), "a+");
  if (p_file_ == nullptr) {
    KStatus ret = CreateLogDir(cfg->path.c_str());
    if (ret == FAIL) {
      p_file_ = stdout;
      inited_ = false;
      rto_f_.unlock();
      return;
    }
    std::ignore = symlink(fname, log_symlink_name_.c_str());
    p_file_ = fopen(log_symlink_name_.c_str(), "a+");
  }
  rto_f_.unlock();
}

KStatus LogWriterFile::Init() {
  inited_ = true;
  InitLogFile();
  LogDirCheck();
  return SUCCESS;
}

KStatus LogWriterFile::Destroy() {
  if (inited_ == false) {
    return (SUCCESS);
  }
  if (nullptr != p_file_) {
    fclose(p_file_);
    p_file_ = nullptr;
  }
  return (SUCCESS);
}

KStatus LogWriterFile::CreateLogDir(const char *path) {
  if (path[0] == '\0') {
    return FAIL;
  }
  if (mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
    return FAIL;
  }
  return SUCCESS;
}

void LogWriterFile::RelinkLogFile(char *fname) {
  int ret;
  rto_f_.lock();
  fclose(p_file_);
  ret = unlink(log_symlink_name_.c_str());
  if (ret != 0) {
    std::cerr << "remove log file failed!\n";
  }
  // whatever symlink will create ok.maybe write failed.it's another problem
  std::ignore = symlink(fname, log_symlink_name_.c_str());
  // reopen pfile_ in upper
  p_file_ = fopen(log_symlink_name_.c_str(), "a+");
  rto_f_.unlock();
}

KStatus LogWriterFile::LogDirCheck() {
  struct stat info {};
  // we check linked targed file's size.
  stat(log_dir_, &info);
  if (cfg->file_max <= 0) cfg->file_max = LOG_FILE_DEFAULT_SIZE;
  if (info.st_size > cfg->file_max) {
    RolloverLogfile();
    return FAIL;
  }
  if (info.st_mode & S_IFDIR) {
    return SUCCESS;
  }
  return FAIL;
}

void LogWriterFile::MakeLogFileName(char *src, int max_size) {
  time_t now = time(0);
  tm* localTime = localtime(&now);
  char timestamp[20] = {0};
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H_%M_%S", localTime);
  snprintf(src, max_size, "TsEngine.%s.log", timestamp);
}

void LogWriterFile::GetTimeStamp(char *buff, k_int32 size) {
  struct tm tm {};
  struct timeval time_secs {};
  time_t cur_time;
  gettimeofday(&time_secs, nullptr);
  cur_time = time_secs.tv_sec;
  localtime_r(&cur_time, &tm);
  snprintf(buff, size, "%d-%02d-%02dT%02d_%02d_%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
           tm.tm_min, tm.tm_sec);
}

KStatus LogWriterFile::RolloverLogfile() {
    char fname[FILE_NAME_LEN];
    MakeLogFileName(fname, FILE_NAME_LEN);
    snprintf(log_dir_, sizeof(log_dir_), "%s/%s", cfg->path.c_str(), fname);
    RelinkLogFile(fname);
    return (SUCCESS);
}

char *LogWriterFile::GetLogFileName() { return filename_with_path_; }

}  // namespace kwdbts
