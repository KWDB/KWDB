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


#pragma once

#include <fcntl.h>
#include <cstdlib>
#include <string>
#include <utility>
#include "ts_object_error.h"
#include "sys_utils.h"

#define O_NORECURSIVE   0x01000000
#define O_MATERIALIZATION  0x02000000
#define O_ANONYMOUS     0x08000000

#define MMAP_OPEN             (O_RDWR)
#define MMAP_OPEN_NORECURSIVE (O_RDWR | O_NORECURSIVE)
#define MMAP_CREATOPEN        (O_RDWR | O_CREAT)
#define MMAP_CREATTRUNC       (O_RDWR | O_CREAT | O_TRUNC)
#define MMAP_CREAT_ANONYMOUS  (O_RDWR | O_CREAT | O_EXCL | O_TRUNC | O_ANONYMOUS)
#define MMAP_CREAT_EXCL       (O_RDWR | O_CREAT | O_EXCL)

/**
 * A memory mapped object class.
 * a file is mapped into memory via memory mapped function.
 */
class MMapFile {
 protected:
  void *mem_;  // memory mapped address.
  off_t file_length_;
  off_t new_length_;
  std::string file_name_;
  std::string absolute_file_path_;
  int flags_;

  void LogMMapFileError(const string &op);

  int open();

  int reportError();

  inline bool isMapFailed() {  return file_length_ != new_length_;}

 public:
  /**
   * @brief	a default constructor.
   */
  MMapFile();

  /**
   * @brief	a default dstructor.
   */
  virtual ~MMapFile();

  /**
   * @brief open temporary file for mmap file.
   * @return >= 0 if succeed, otherwise -1.
   */
  int openTemp();

  // open file in the home directory
  /**
   * @brief	open and memory map a file.
   * @param	flag		O_CREAT to create file or 0.
   * @return	>= 0 if succeed, otherwise -1.
   */
  int open(const std::string &file_name, const std::string &absolute_file_path, int flags);

  int open(const std::string &file_name, const std::string &absolute_file_path, int flags, size_t init_sz, ErrorInfo
   &err_info);

  /**
   * @brief	open and memory map a file.
   *
   * @param	fd		file descriptor
   * @return	returns zero on success.  On error, -1 is returned.
   */
  int munmap();

  /**
   * @brief	memory remap a file.
   * @param	length	the requested file/mmap length.
   * @return	mapped memory address if succeeds.
   * 			-1 otherwise.
   */
  int mremap(size_t length);

  int sync(int flags);

  int resize(size_t length);

  inline void* memAddr() const { return mem_; }

  off_t fileLen() const { return file_length_; }

  off_t newLen() const { return new_length_; }

  std::string filePath() const { return file_name_; }

  std::string& filePath() { return file_name_; }

  std::string realFilePath() const { return absolute_file_path_; }

  std::string& realFilePath() { return absolute_file_path_; }

  int remove();

  int rename(const string &new_fp);

  int flags() const { return flags_; }

  void setFlags(int flags) { flags_ = flags; }

  bool readOnly() const { return !(flags_ & O_RDWR); }

  void copyMember(MMapFile& other);

  void checkError();
};
