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

#ifndef COMMON_SRC_H_LG_WRITER_FILE_H_
#define COMMON_SRC_H_LG_WRITER_FILE_H_

#include "cm_kwdb_context.h"
#include "lg_writer.h"
#include "lg_config.h"
#include <string>

namespace kwdbts {
/**!
 * @brief Outputs log statements to a log file
 */
// @since kwbase/pkg/util/log/file.go LogFileMaxSize 10M
#define LOG_FILE_DEFAULT_SIZE 10<<20
#define LOG_SYMLINK_NAME(path) path+"/TsEngine.log";
class LogWriterFile : public LogWriter {
 public:
  KStatus Write(const LogItem *pItem) override;
  KStatus Init() override;
  KStatus Destroy() override;
  // Used for unit tests to get log file names
  char *GetLogFileName();
  LogWriterFile() = default;
  ~LogWriterFile() override = default;
  explicit LogWriterFile(LogConf *cfg) : LogWriter(cfg) {}

 protected:
  FILE *p_file_ = nullptr;
  char filename_with_path_[LOG_FILE_NAME_MAX_LEN];
  char log_dir_[LOG_DIR_MAX_LEN];
  /*!
   * Format and write log statements to a file
   * @param pItem The log statement class, which contains the basic information of the log statement
   * @return Success or not
   */
  KStatus FormatAndOut(const LogItem *pItem);
  // TODO(yuhangqing): roll rules for creating log files when a new day is added
  /*!
   * Create new log files when they exceed the specified size
   * @param
   * @return Success or not
   */
  KStatus RolloverLogfile();
  /*!
   * Create a log directory with the configured log_dir if the log directory does not exist
   * @param
   * @return Success or not
   */
  KStatus CreateLogDir(const char *path);
  /*!
   * Used to determine if a directory exists
   * @param
   * @return Success or not
   */
  KStatus LogDirCheck();
  // Log file naming format
  // TsEngine.[start timestamp in UTC].log
  // TsEngine.2024-02-19T11_07_35.log
  /*!
   * Create a default name for the log file
   * @param src[out] filename generated
   * @param max_size[in] filename to generate maxLen
   */
  void MakeLogFileName(char *src, int max_size);
  /*!
   * Gets the current timestamp
   * @param 
   * @return Success or not
   */
  static void GetTimeStamp(char *buff, k_int32 size);

 private:
  /* 
  * @brief link log_symlink_name_ to this fname
  * @param fname link target file name
  */
  void RelinkLogFile(char *fname);
  /*
  @brief init log_symlink_name_ and open
  */
  void InitLogFile();
  std::string log_symlink_name_;
  std::mutex rto_f_;
};

}  // namespace kwdbts

#endif  // COMMON_SRC_H_LG_WRITER_FILE_H_
